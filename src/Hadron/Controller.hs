{-# LANGUAGE BangPatterns               #-}
{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE EmptyDataDecls             #-}
{-# LANGUAGE ExistentialQuantification  #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE NoMonomorphismRestriction  #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TemplateHaskell            #-}
{-# LANGUAGE UndecidableInstances       #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Hadron.Controller
-- Copyright   :  Soostone Inc
-- License     :  BSD3
--
-- Maintainer  :  Ozgun Ataman
-- Stability   :  experimental
--
-- High level flow-control of Hadoop programs with ability to define a
-- sequence of Map-Reduce operations in a Monad, have strongly typed
-- data locations.
----------------------------------------------------------------------------

module Hadron.Controller
    (


    -- * Hadoop Program Construction
      Controller

    , connect
    , connect'
    , io
    , orchIO
    , nodeIO
    , setVal
    , getVal
    , runOnce

    , MapReduce (..)
    , mrOptions
    , Mapper
    , Reducer
    , (>.>)
    , (<.<)

    , MRKey (..)
    , CompositeKey
    , SingleKey (..)
    , WrapSerialize (..)
    , WrapSafeCopy (..)

    -- * Data Sources
    , Tap (..)
    , proto, location
    , tap
    , taps
    , mergeTaps
    , binaryDirTap
    , setupBinaryDir
    , fileListTap
    , readTap
    , readHdfsFile

    -- * Command Line Entry Point
    , hadoopMain
    , HadoopEnv (..)

    -- * Settings for MapReduce Jobs
    , MROptions
    , mroPart
    , mroNumMap
    , mroNumReduce
    , mroCompress
    , mroOutSep
    , PartitionStrategy (..)
    , Comparator (..)
    , RerunStrategy (..)

    -- * Hadoop Utilities
    , emitCounter
    , hsEmitCounter
    , emitStatus
    , getFileName


    -- * MapReduce Combinators

    , mapReduce
    , firstBy
    , mapMR
    , oneSnap
    , joinMR

    , joinStep
    , JoinType (..)
    , JoinKey

    -- * Data Serialization Utilities
    , module Hadron.Protocol
    , module Hadron.Run

    ) where

-------------------------------------------------------------------------------
import           Control.Applicative
import           Control.Arrow
import           Control.Error
import           Control.Exception.Lens
import           Control.Lens
import           Control.Monad.Catch

import           Control.Monad.Operational hiding (view)
import qualified Control.Monad.Operational as O
import           Control.Monad.State

import qualified Data.ByteString.Char8     as B
import           Data.ByteString.Search    as B
import           Data.Default

import           Data.List
import qualified Data.Map.Strict           as M
import           Data.Monoid
import           Data.RNG
import           Data.SafeCopy
import           Data.Serialize
import qualified Data.Text                 as T
import           Data.Text.Encoding
import           Data.Time
import           Data.Typeable

import           System.Environment
import           System.FilePath.Lens
import           System.IO
import           System.IO.Streams         (InputStream, OutputStream)
import qualified System.IO.Streams         as S
import           System.Locale
import           Text.Parsec
-------------------------------------------------------------------------------
import           Hadron.Basic              hiding (mapReduce)
import           Hadron.Join
import           Hadron.Logger
import           Hadron.Protocol
import           Hadron.Run
import           Hadron.Streams
import           Hadron.Types
-------------------------------------------------------------------------------


newtype SingleKey a = SingleKey { unKey :: a }
    deriving (Eq,Show,Read,Ord,Serialize)

newtype WrapSerialize a = WrapSerialize { _getSerialized :: a }
    deriving (Eq,Show,Read,Ord,Serialize)

newtype WrapSafeCopy a = WrapSafeCopy { _getSafeCopy :: a }
    deriving (Eq,Show,Read,Ord,SafeCopy)


-- mrKeyError :: Int -> Int -> a
-- mrKeyError i n =
--     error $ "Expected MapReduce key to have "
--          <> show i <> " parts. Instead received "
--          <> show n <> " parts."


type Parser = Parsec [B.ByteString] ()


keyToken :: Parser B.ByteString
keyToken = tokenPrim B.unpack (\pos _ _ -> incSourceColumn pos 1) Just


fromCompKey :: MRKey a => [B.ByteString] -> Either ParseError a
fromCompKey s = runParser keyParser () "Key Input" s


class MRKey k where
    toCompKey :: k -> CompositeKey
    keyParser :: Parser k
    numKeys :: k -> Int

instance MRKey ()  where
    toCompKey _ = [""]
    keyParser = keyToken >> return ()
    numKeys _ = 1

instance MRKey B.ByteString where
    toCompKey k = [k]
    keyParser = keyToken
    numKeys _ = 1

instance MRKey CompositeKey where
    toCompKey ks = ks
    keyParser = many1 keyToken
    numKeys ks = length ks

instance MRKey String where
    toCompKey = toCompKey . B.pack
    keyParser = B.unpack <$> keyToken
    numKeys _ = 1

instance MRKey T.Text where
    toCompKey = toCompKey . encodeUtf8
    keyParser = decodeUtf8 <$> keyToken
    numKeys _ = 1

instance MRKey Int where
    toCompKey = toCompKey . B.pack . show
    keyParser = keyParser >>=
        maybe (fail "Can't read Int MRKey") return . readMay
    numKeys _ = 1

instance Serialize a => MRKey (WrapSerialize a) where
    toCompKey = toCompKey . (^. re pSerialize) . _getSerialized
    keyParser = do
        a <- (^? pSerialize) <$> keyParser
        maybe (fail "Can't decode WrapSerialize") (return . WrapSerialize) a
    numKeys _ = 1

instance SafeCopy a => MRKey (WrapSafeCopy a) where
    toCompKey = toCompKey . (^. re pSafeCopy) . _getSafeCopy
    keyParser = do
        a <- (^? pSafeCopy) <$> keyParser
        maybe (fail "Can't decode WrapSerialize") (return . WrapSafeCopy) a
    numKeys _ = 1

utcFormat :: String
utcFormat = "%Y-%m-%d %H:%M:%S.%q"

instance MRKey UTCTime where
    toCompKey = toCompKey . formatTime defaultTimeLocale utcFormat
    keyParser = do
        res <- parseTime defaultTimeLocale utcFormat <$> keyParser
        maybe (fail "Can't parse value as UTCTime") return res
    numKeys _ = 1

instance (MRKey a, MRKey b) => MRKey (a,b) where
    toCompKey (a,b) = toCompKey a ++ toCompKey b
    keyParser = (,) <$> keyParser <*> keyParser
    numKeys (a,b) = numKeys a + numKeys b

instance (MRKey a, MRKey b, MRKey c) => MRKey (a,b,c) where
    toCompKey (a,b,c) = toCompKey a ++ toCompKey b ++ toCompKey c
    keyParser = (,,) <$> keyParser <*> keyParser <*> keyParser
    numKeys (a,b,c) = numKeys a + numKeys b + numKeys c

instance (MRKey a, MRKey b, MRKey c, MRKey d) => MRKey (a,b,c,d) where
    toCompKey (a,b,c,d) = toCompKey a ++ toCompKey b ++ toCompKey c ++ toCompKey d
    keyParser = (,,,) <$> keyParser <*> keyParser <*> keyParser <*> keyParser
    numKeys (a,b,c,d) = numKeys a + numKeys b + numKeys c + numKeys d

-- instance (MRKey a, MRKey b) => MRKey (a,b) where
--     toCompKey (a,b) = toCompKey a ++ toCompKey b
--     fromCompKey xs = (,) <$> fromCompKey na <*> fromCompKey nb
--         where
--           (na, nb) = splitAt (numKeys (undefined :: a)) xs

--     numKeys (a,b) = numKeys a + numKeys b

-- instance Serialize a => MRKey (SingleKey a) where
--     toCompKey a = [review pSerialize a]
--     fromCompKey [a] = preview pSerialize a
--     fromCompKey xs  = mrKeyError 1 (length xs)
--     numKeys _ = 1


-- instance (Serialize a, Serialize b) => MRKey (a,b) where
--     toCompKey (a,b) = [review pSerialize a, review pSerialize b]
--     fromCompKey [a,b] = (,) <$> preview pSerialize a <*> preview pSerialize b
--     fromCompKey xs  = mrKeyError 2 (length xs)
--     numKeys _ = 2

-- instance (Serialize a, Serialize b, Serialize c) => MRKey (a,b,c) where
--     toCompKey (a,b,c) = [review pSerialize a, review pSerialize b, review pSerialize c]
--     fromCompKey [a,b,c] = (,,)
--         <$> preview pSerialize a
--         <*> preview pSerialize b
--         <*> preview pSerialize c
--     fromCompKey xs  = mrKeyError 3 (length xs)
--     numKeys _ = 3

-- instance (Serialize a, Serialize b, Serialize c, Serialize d) => MRKey (a,b,c,d) where
--     toCompKey (a,b,c,d) = [review pSerialize a, review pSerialize b, review pSerialize c, review pSerialize d]
--     fromCompKey [a,b,c,d] = (,,,)
--         <$> preview pSerialize a
--         <*> preview pSerialize b
--         <*> preview pSerialize c
--         <*> preview pSerialize d
--     fromCompKey xs  = mrKeyError 4 (length xs)
--     numKeys _ = 4

-- instance (Serialize a, Serialize b, Serialize c, Serialize d, Serialize e) => MRKey (a,b,c,d,e) where
--     toCompKey (a,b,c,d,e) =
--         [ review pSerialize a, review pSerialize b, review pSerialize c
--         , review pSerialize d, review pSerialize e ]
--     fromCompKey [a,b,c,d,e] = (,,,,)
--         <$> preview pSerialize a
--         <*> preview pSerialize b
--         <*> preview pSerialize c
--         <*> preview pSerialize d
--         <*> preview pSerialize e
--     fromCompKey xs  = mrKeyError 5 (length xs)
--     numKeys _ = 5

-- instance (Serialize a, Serialize b, Serialize c, Serialize d, Serialize e, Serialize f) => MRKey (a,b,c,d,e,f) where
--     toCompKey (a,b,c,d,e,f) =
--         [ review pSerialize a, review pSerialize b, review pSerialize c
--         , review pSerialize d, review pSerialize e, review pSerialize f]
--     fromCompKey [a,b,c,d,e,f] = (,,,,,)
--         <$> preview pSerialize a
--         <*> preview pSerialize b
--         <*> preview pSerialize c
--         <*> preview pSerialize d
--         <*> preview pSerialize e
--         <*> preview pSerialize f
--     fromCompKey xs  = mrKeyError 6 (length xs)
--     numKeys _ = 6





-------------------------------------------------------------------------------
-- | Map MR output.
(>.>) :: MapReduce a b -> (OutputStream c -> IO (OutputStream b)) -> MapReduce a c
(MapReduce o p m c r) >.> f = MapReduce o p m c r'
    where
      r' i o = f o >>= r i


-------------------------------------------------------------------------------
-- | Contramap MR input; i.e. do something before MR.
(<.<) :: (InputStream a -> IO (InputStream b)) -> MapReduce b c -> MapReduce a c
f <.< (MapReduce o p m c r) = MapReduce o p m' c r
    where
      m' i o = f i >>= \ i' -> m i' o

-------------------------------------------------------------------------------
-- | A packaged MapReduce step. Make one of these for each distinct
-- map-reduce step in your overall 'Controller' flow.
data MapReduce a b = forall k v. MRKey k => MapReduce {
      _mrOptions  :: MROptions
    -- ^ Hadoop and MapReduce options affecting only this specific
    -- job.
    , _mrInPrism  :: Prism' B.ByteString v
    -- ^ A serialization scheme for values between the map-reduce
    -- steps.
    , _mrMapper   :: Mapper a k v
    , _mrCombiner :: Maybe (Reducer k v (k,v))
    , _mrReducer  :: Reducer k v b
    }

--------------------------------------------------------------------------------
mrOptions :: Lens' (MapReduce a b) MROptions
mrOptions f (MapReduce o p m c r) = (\ o' -> MapReduce o' p m c r) <$> f o


-- | Tap is a data source/sink definition that *knows* how to serve
-- records of type 'a'.
--
-- It comes with knowledge on how to decode ByteString to target type
-- and can be used both as a sink (to save data form MR output) or
-- source (to feed MR programs).
--
-- Usually, you just define the various data sources and destinations
-- your MapReduce program is going to need:
--
-- > customers = 'tap' "s3n://my-bucket/customers" (csvProtocol def)
data Tap a = Tap
    { _location :: [FilePath]
    , _proto    :: Protocol' a
    }
makeLenses ''Tap


-- | If two 'location's are the same, we consider two Taps equal.
instance Eq (Tap a) where
    a == b = _location a == _location b


-- | Construct a 'DataDef'
tap :: FilePath -> Protocol' a -> Tap a
tap fp p = Tap [fp] p

taps :: [FilePath] -> Protocol' a -> Tap a
taps fp p = Tap fp p


-------------------------------------------------------------------------------
-- | Given a tap directory, enumerate and load all files inside.
-- Caution: This is meant only as a way to load small files, or else
-- you'll fill up your memory.
readTap :: RunContext -> Tap a -> IO [a]
readTap rc t = do
    fs <- liftIO $ concat <$> forM (_location t) (hdfsLs rc)
    let fs' = filter (\ fp -> not  $ elem (fp ^. filename) [".", ".."]) fs
    S.toList =<< liftIO . (t ^. proto . protoDec) =<< inp fs'
    where
      inp fs = forM fs (hdfsCat rc) >>= S.concatInputStreams


------------------------------------------------------------------------------
-- | Conduit that takes in hdfs filenames and outputs the file contents.
readHdfsFile
    :: RunContext
    -> S.InputStream B.ByteString
    -> IO (S.InputStream B.ByteString)
readHdfsFile settings is = bindStream is go
    where
      go s3Uri = do
        let uriStr = B.unpack s3Uri
            getFile = hdfsLocalStream settings uriStr
        if isSuffixOf "gz" uriStr
          then getFile >>= S.gunzip
          else getFile


------------------------------------------------------------------------------
-- | Tap for handling file lists.  Hadoop can't process raw binary data
-- because it splits on newlines.  This tap allows you to get around that
-- limitation by instead making your input a list of file paths that contain
-- binary data.  Then the file names get split by hadoop and each map job
-- reads from those files as its first step.
fileListTap :: RunContext
            -> FilePath
            -- ^ A file containing a list of files to be used as input
            -> Tap B.ByteString
fileListTap settings loc = tap loc (Protocol enc dec)
  where
    enc = error "You should never use a fileListTap as output!"
    dec is = streamLines is >>= readHdfsFile settings


data ContState = ContState {
      _csMRCount      :: ! Int
    -- ^ MR run count; one for each 'connect'.
    , _csMRVars       :: ! (M.Map String B.ByteString)
    -- ^ Arbitrary key-val store that's communicated to nodes.
    , _csDynId        :: ! Int
    -- ^ Keeps increasing count of dynamic taps in the order they are
    -- created in the Controller monad. Needed so we can communicate
    -- the tap locations to MR nodes.
    , _csRunOnceId    :: ! Int
    -- ^ Increasing count of run-once cache items so we can
    -- communicate to remote nodes.
    , _csShortCircuit :: Bool
    -- ^ Used by the remote nodes. When they hit their primary target
    -- (the mapper, combiner or the reducer), they should stop
    -- executing.
    }

instance Default ContState where
    def = ContState 0 M.empty 0 0 False


makeLenses ''ContState



-------------------------------------------------------------------------------
data ConI a where
    Connect :: forall i o. MapReduce i o
            -> [Tap i] -> Tap o
            -> Maybe String
            -> ConI ()

    MakeTap :: Protocol' a -> ConI (Tap a)

    BinaryDirTap :: FilePath -> (FilePath -> Bool) -> ConI (Tap B.ByteString)

    ConIO :: IO a -> ConI a
    -- ^ General IO action; both orchestrator and nodes perform the action

    OrchIO :: IO a -> ConI ()
    -- ^ Only the orchestrator performs action

    NodeIO :: IO a -> ConI a
    -- ^ Only the nodes perform action

    SetVal :: String -> B.ByteString -> ConI ()

    GetVal :: String -> ConI (Maybe B.ByteString)

    RunOnce :: Serialize a => IO a -> ConI a
    -- ^ Only run on orchestrator, then make available to all the
    -- nodes via HDFS.


-- | All MapReduce steps are integrated in the 'Controller' monad.
--
-- Warning: We do have an 'io' combinator as an escape valve for you
-- to use. However, you need to be careful how you use the result of
-- an IO computation. Remember that the same 'main' function will run
-- on both the main orchestrator process and on each and every
-- map/reduce node.
newtype Controller a = Controller (Program ConI a)
    deriving (Functor, Applicative, Monad)



-------------------------------------------------------------------------------
-- | Connect a MapReduce program to a set of inputs, returning the
-- output tap that was implicity generated (on hdfs) in the process.
connect'
    :: MapReduce a b
    -- ^ MapReduce step to run
    -> [Tap a]
    -- ^ Input files
    -> Protocol' b
    -- ^ Serialization protocol to be used on the output
    -> Maybe String
    -- ^ A custom name for the job
    -> Controller (Tap b)
connect' mr inp proto nm = do
    out <- makeTap proto
    connect mr inp out nm
    return out


-------------------------------------------------------------------------------
-- | Connect a typed MapReduce program you supply with a list of
-- sources and a destination.
connect :: MapReduce a b -> [Tap a] -> Tap b -> Maybe String -> Controller ()
connect mr inp outp nm = Controller $ singleton $ Connect mr inp outp nm


-------------------------------------------------------------------------------
makeTap :: Protocol' a -> Controller (Tap a)
makeTap proto = Controller $ singleton $ MakeTap proto


-------------------------------------------------------------------------------
-- | Set a persistent variable in Controller state. This variable will
-- be set during main M-R job controller loop and communicated to all
-- the map and reduce nodes and will be available there.
setVal :: String -> B.ByteString -> Controller ()
setVal k v = Controller $ singleton $ SetVal k v


-------------------------------------------------------------------------------
-- | Get varible from Controller state
getVal :: String -> Controller (Maybe B.ByteString)
getVal k = Controller $ singleton $ GetVal k


-------------------------------------------------------------------------------
-- | Creates a tap for a directory of binary files.
binaryDirTap
    :: FilePath
    -- ^ A root location to list files under
    -> (FilePath -> Bool)
    -- ^ A filter condition to refine the listing
    -> Controller (Tap B.ByteString)
binaryDirTap loc filt = Controller $ singleton $ BinaryDirTap loc filt


-------------------------------------------------------------------------------
-- | Perform an IO operation both on the orchestrator and on the worker nodes.
io :: IO a -> Controller a
io f = Controller $ singleton $ ConIO f


-------------------------------------------------------------------------------
-- | Perform an IO operation only on the orchestrator
orchIO :: IO a -> Controller ()
orchIO = Controller . singleton . OrchIO


-- | Perform an IO action to obtain value, then cache it on HDFS and
-- magically make it available to nodes during their runtime.
runOnce :: Serialize a => IO a -> Controller a
runOnce = Controller . singleton . RunOnce


-------------------------------------------------------------------------------
-- | Perform an IO operation only on the worker nodes.
nodeIO :: IO a -> Controller a
nodeIO = Controller . singleton . NodeIO


-------------------------------------------------------------------------------
newMRKey :: MonadState ContState m => m String
newMRKey = do
    i <- gets _csMRCount
    csMRCount %= (+1)
    return $! show i


-------------------------------------------------------------------------------
-- | Grab list of files in destination, write into a file, put file on
-- HDFS so it is shared and return the (local, hdfs) paths.
setupBinaryDir :: RunContext -> FilePath -> (FilePath -> Bool) -> IO (LocalFile, FilePath)
setupBinaryDir settings loc chk = do
    localFile <- randomLocalFile
    hdfsFile <- randomRemoteFile settings

    files <- hdfsLs settings loc
    let files' = filter chk files
    withLocalFile settings localFile $ \ f -> writeFile f (unlines files')

    hdfsPut settings localFile hdfsFile

    return (localFile, hdfsFile)


tapLens :: Int -> Lens' ContState (Maybe B.ByteString)
tapLens curId = csMRVars.at ("tap_" <> show curId)

runCacheLens :: Int -> Lens' ContState (Maybe B.ByteString)
runCacheLens curId = csMRVars.at ("runOnce_" <> show curId)


pickTapId :: MonadState ContState m => m Int
pickTapId = pickIdWith csDynId


pickRunCacheId :: MonadState ContState m => m Int
pickRunCacheId = pickIdWith csRunOnceId


-------------------------------------------------------------------------------
-- | Monotinically increasing counter.
pickIdWith :: MonadState ContState m => Lens' ContState Int -> m Int
pickIdWith l = do
    curId <- use l
    l %= (+1)
    return curId


-------------------------------------------------------------------------------
-- | Interpreter for the central job control process
orchestrate
    :: (MonadMask m, MonadIO m)
    => Controller a
    -> RunContext
    -> RerunStrategy
    -> ContState
    -> m (Either String a)
orchestrate (Controller p) settings rr s = do
    bracket
      (liftIO $ openFile "hadron.log" AppendMode)
      (liftIO . hClose)
      (\ h -> do liftIO $ do
                   enableDebugLog
                   hSetBuffering h LineBuffering
                   setLogHandle "Hadron" h INFO
                   infoM "Hadron.Controller" "Initiating orchestration..."
                 evalStateT (runEitherT (go p)) s)
    where
      go = eval . O.view

      eval (Return a) = return a
      eval (i :>>= f) = eval' i >>= go . f

      eval' :: (MonadIO m) => ConI a -> EitherT String (StateT ContState m) a

      eval' (ConIO f) = liftIO f

      eval' (OrchIO f) = void $ liftIO f

      eval' (NodeIO _) = return (error "NodeIO can't be used in the orchestrator decision path")

      -- evaluate the function, write its result into HDFS for later retrieval
      eval' (RunOnce f) = do
          a <- liftIO f

          curId <- pickRunCacheId

          runCacheLens curId .= Just (encode a)

          -- loc <- liftIO $ randomRemoteFile settings
          -- curId <- pickRunCacheId
          -- runCacheLens curId .= Just (B.pack loc)

          -- tmp <- randomFileName
          -- liftIO $ withLocalFile settings tmp $ \ fn ->
          --   B.writeFile fn (encode a)
          -- liftIO $ hdfsPut settings tmp loc

          return a


      eval' (MakeTap proto) = do
          loc <- liftIO $ randomRemoteFile settings

          curId <- pickTapId
          tapLens curId .= Just (B.pack loc)

          return $ Tap [loc] proto

      eval' (BinaryDirTap loc filt) = do
          (_, hdfsFile) <- liftIO $ setupBinaryDir settings loc filt

          -- remember location of the file from the original loc
          -- string
          curId <- pickTapId
          tapLens curId .= Just (B.pack hdfsFile)

          return $ fileListTap settings hdfsFile


      eval' (SetVal k v) = csMRVars . at k .= Just v
      eval' (GetVal k) = use (csMRVars . at k)

      eval' (Connect (MapReduce mro _ _ _ _) inp outp nm) = go'
          where
            go' = do
                mrKey <- newMRKey

                liftIO $ infoM "Hadron.Controller"
                  ("Launching MR job with key: " ++ show mrKey)

                chk <- liftIO $ mapM (hdfsFileExists settings) (_location outp)
                case any id chk of
                  False -> do
                      liftIO $ infoM "Hadron.Controller"
                        "Destination file does not exist. Proceeding."
                      go'' mrKey
                  True ->
                    case rr of
                      RSFail -> liftIO $ errorM "Hadron.Controller" $
                        "Destination exists: " <> head (_location outp)
                      RSSkip -> liftIO $ infoM "Hadron.Controller" $
                        "Destination exists. Skipping " <> intercalate ", " (_location outp)
                      RSReRun -> do
                        liftIO $ infoM "Hadron.Controller" $
                          "Destination file exists, will delete and rerun: " <>
                          head (_location outp)
                        _ <- liftIO $ mapM_ (hdfsDeletePath settings) (_location outp)
                        go'' mrKey

                        liftIO $ infoM "Hadron.Controller" ("MR job complete: " ++ show mrKey)


            go'' mrKey = do

              -- serialize current state to HDFS, to be read by
              -- individual mappers reducers of this step.
              runToken <- liftIO $ (mkRNG >>= randomToken 64) <&> B.unpack
              remote <- hdfsTempFilePath settings runToken
              let local = LocalFile runToken


              st <- use csMRVars

              withLocalFile settings local $ \ lfp ->
                liftIO $ writeFile lfp (show st)

              -- put settings file into a file named after the
              -- randomly generated token.
              liftIO $ hdfsPut settings local remote

              let mrs = mrOptsToRunOpts mro
              launchMapReduce settings mrKey runToken
                mrs { mrsInput = concatMap _location inp
                    , mrsOutput = head (_location outp)
                    , mrsJobName = nm
                    }



data Phase = Map | Combine | Reduce


-------------------------------------------------------------------------------
-- | What to do when we notice that a destination file already exists.
data RerunStrategy
    = RSFail
    -- ^ Fail and log the problem.
    | RSReRun
    -- ^ Delete the file and rerun the analysis
    | RSSkip
    -- ^ Consider the analaysis already done and skip.
    deriving (Eq,Show,Read,Ord)

instance Default RerunStrategy where
    def = RSFail


-------------------------------------------------------------------------------
-- | Decode key produced by the Map stage. Errors are simply raised as
-- key marshalling errors are unacceptable.
decodeKey :: MRKey k => (CompositeKey, v) -> (k, v)
decodeKey (k,v) = (k', v)
  where
    k' = either mkErr id $ fromCompKey k
    mkErr e = error ("Stage could not decode Map's output: " ++ show e)


encodeKey :: MRKey k => (k, v) -> (CompositeKey, v)
encodeKey = first toCompKey



data NodeError
    = NodeRunComplete
    -- ^ Single short circuiting in node workers; map/reduce/combine
    -- has been completed.
    deriving (Eq,Show,Read,Ord,Typeable)
makePrisms ''NodeError

instance Exception NodeError


-------------------------------------------------------------------------------
-- | The main entry point. Use this function to produce a command line
-- program that encapsulates everything.
--
-- When run without arguments, the program will orchestrate the entire
-- MapReduce job flow. The same program also doubles as the actual
-- mapper/reducer executable when called with right arguments, though
-- you don't have to worry about that.
hadoopMain
    :: forall m a. (MonadThrow m, MonadMask m, MonadIO m, Functor m)
    => Controller a
    -- ^ The Hadoop streaming application to run.
    -> RunContext
    -- ^ Hadoop environment info.
    -> RerunStrategy
    -- ^ What to do if destination files already exist.
    -> m ()
hadoopMain cont settings rr = do
    args <- liftIO getArgs
    case args of
      [] -> do
        res <- orchestrate cont settings rr def
        liftIO $ either print (const $ putStrLn "Success.") res
      [runToken, arg] -> void $ workNode settings cont runToken arg
      _ -> error "Usage: No arguments for job control or a phase name."




-------------------------------------------------------------------------------
mkArgs mrKey = [ (Map, "mapper_" ++ mrKey)
               , (Reduce, "reducer_" ++ mrKey)
               , (Combine, "combiner_" ++ mrKey) ]


-------------------------------------------------------------------------------
-- | load controller varibles back up
loadState settings runToken = do
    fn <- hdfsTempFilePath settings runToken
    tmp <- liftIO $ hdfsGet settings fn
    st <- liftIO $ withLocalFile settings tmp $ \ local -> do
      !st <- readFile local <&> read
      -- removeFile local
      return st
    csMRVars %= M.union st


-------------------------------------------------------------------------------
-- | Interpret the Controller in the context of a Hadoop worker node.
-- In this mode, the objective is to find the mapper, combiner or the
-- reducer that we are supposed to be executing as.
workNode
    :: forall m a. (MonadIO m, MonadThrow m, MonadMask m, Functor m)
    => RunContext
    -> Controller a
    -> String
    -> String
    -> m ()
workNode settings (Controller p) runToken arg = do
    handling (exception._NodeRunComplete) (const $ return ()) $ do
      void $ flip evalStateT def $ do
        loadState settings runToken
        interpretWithMonad go' p
  where

      -- A short-circuiting wrapper for go. We hijack the exception
      -- system to implement shortcircuting here. It may be a better
      -- idea to use ContT.
      go' :: ConI b -> StateT ContState m b
      go' c = do
          chk <- use csShortCircuit
          case chk of
            True -> throwM NodeRunComplete
            False -> go c

      go :: ConI b -> StateT ContState m b

      go (ConIO f) = liftIO f

      go (OrchIO _) = return ()

      go (NodeIO f) = liftIO f

      go (MakeTap proto) = do
          curId <- pickTapId
          dynLoc <- use $ tapLens curId
          case dynLoc of
            Nothing -> error $ "Dynamic location can't be determined for MakTap at index " <> show curId
            Just loc' -> return $ Tap ([B.unpack loc']) proto

      go (BinaryDirTap loc _) = do

          -- remember location of the file from the original loc
          -- string
          curId <- pickTapId
          dynLoc <- use $ tapLens curId
          case dynLoc of
            Nothing -> error $
              "Dynamic location can't be determined for BinaryDirTap at: " <> loc
            Just loc' -> return $ fileListTap settings $ B.unpack loc'

      -- setting in map-reduce phase is a no-op... There's nobody to
      -- communicate it to.
      go (SetVal _ _) = return ()
      go (GetVal k) = use (csMRVars . at k)

      go (RunOnce _) = do
          curId <- pickRunCacheId
          bs <- use (runCacheLens curId)

          either error return $
            note "RunOnce cache missing on remote node" bs >>= decode

      go (Connect (MapReduce mro mrInPrism mp comb rd) inp outp nm) = do
          mrKey <- newMRKey

          let dec = head inp ^. proto . protoDec
              enc = outp ^. proto . protoEnc

              mp' = mapperWith mrInPrism $ \ is os -> do
                is' <- dec is
                os' <- S.contramap encodeKey os
                mp is' os'

              red = reducer mro mrInPrism $ \ is os -> do
                is' <- S.map decodeKey is
                os' <- enc os
                rd is' os'

              comb' = case comb of
                  Nothing -> error "Unexpected: No combiner supplied."
                  Just c -> combiner mro mrInPrism $ \ is os -> do
                    is' <- S.map decodeKey is
                    os' <- S.contramap encodeKey os
                    c is' os'


              -- error message maker for caught exceptions
              mkErr :: Maybe FilePath -> String -> SomeException -> b
              mkErr file stage e = error $
                "Exception raised during " <> stage <>
                " in MR Job #" <> mrKey <>
                maybe "" (\nm' -> " (" <> nm' <> ") ") nm <>
                maybe "" (" while processing file " <>) file <>
                ": " <> show e


          case find ((== arg) . snd) $ mkArgs mrKey of

            Just (Map, _) -> do
              liftIO $ do
                curFile <- getFileName
                catching exception mp' (mkErr (Just curFile) "mapper")
              csShortCircuit .= True


            Just (Reduce, _) -> do
              liftIO $ catching exception red (mkErr Nothing "reducer")
              csShortCircuit .= True


            Just (Combine, _) -> do
              liftIO $ catching exception comb' (mkErr Nothing "combiner")
              csShortCircuit .= True

            Nothing -> return ()


-- -- | TODO: See if this works. Objective is to increase type safety of
-- -- join inputs. Notice how we have an existential on a.
-- --
-- -- A join definition that ultimately produces objects of type b.
-- data JoinDef b = forall a. JoinDef {
--       joinTap  :: Tap a
--     , joinType :: JoinType
--     , joinMap  :: Conduit a IO (JoinKey, b)
--     }


-------------------------------------------------------------------------------
-- | A convenient way to express map-sde multi-way join operations
-- into a single data type. All you need to supply is the map
-- operation for each tap, the reduce step is assumed to be the
-- Monoidal 'mconcat'.
--
-- 'joinMR' is probably easier to use if you can get by with an inner
-- join.
joinStep
    :: forall k b a.
       (Show b, Monoid b, Serialize b,
        MRKey k)
    => [(Tap a, JoinType, Mapper a k b)]
    -- ^ Dataset definitions and how to map each dataset.
    -> MapReduce a b
joinStep fs = MapReduce mro pSerialize mp Nothing rd
    where
      showBS = B.pack . show
      n = numKeys (undefined :: k)

      mro = joinOpts { _mroPart = Partition (n+1) n }

      locations :: [FilePath]
      locations = concatMap (view (_1 . location)) fs

      taps' :: [Tap a]
      taps' = concatMap ((\t -> replicate (length (_location t)) t) . view _1) fs

      locations' = map B.pack locations

      dataSets :: [(FilePath, DataSet)]
      dataSets = map (\ (loc, i) -> (loc, DataSet (showBS i))) $
                 zip locations ([0..] :: [Int])

      dsIx :: M.Map FilePath DataSet
      dsIx = M.fromList dataSets

      tapIx :: M.Map DataSet (Tap a)
      tapIx = M.fromList $ zip (map snd dataSets) taps'

      getTapDS :: Tap a -> [DataSet]
      getTapDS t = mapMaybe (flip M.lookup dsIx) (_location t)


      fs' :: [(DataSet, JoinType)]
      fs' = concatMap (\ (t, jt, _) -> for (getTapDS t) $ \ ds -> (ds, jt) ) fs
      for = flip map


      -- | get dataset name from a given input filename
      getDS nm = fromMaybe (error "Can't identify current tap from filename.") $ do
        let nm' = B.pack nm
        curLoc <- find (\l -> length (B.indices l nm') > 0) locations'
        M.lookup (B.unpack curLoc) dsIx


      -- | get the conduit for given dataset name
      mkMap' ds = fromMaybe (error "Can't identify current tap in IX.") $ do
          t <- M.lookup ds tapIx
          cond <- find ((== t) . view _1) fs
          return $ \ is os -> do
            os' <- S.contramap (_1 %~ toCompKey) os
            (view _3 cond) is os'

      mp = joinMapper getDS mkMap'

      rd =  joinReducer fs'




-------------------------------------------------------------------------------
-- | Combine two taps intelligently into the Either sum type.
--
-- Matches on the prefix path given as part of each tap. It would
-- therefore fail to work properly on self joins where the same data
-- location is used in both taps.
mergeTaps :: Tap a -> Tap b -> Tap (Either a b)
mergeTaps ta tb = Tap (_location ta ++ _location tb) newP
    where
      newP = Protocol enc dec

      dec is = do
          fn <- getFileName
          if (any (flip isInfixOf fn) (map (takeWhile (/= '*')) $ _location ta))
            then (ta ^. proto . protoDec) is >>= S.map Left
            else (tb ^. proto . protoDec) is >>= S.map Right

      enc os = do
          as <- (ta ^. (proto . protoEnc)) os
          bs <- (tb ^. (proto . protoEnc)) os

          S.makeOutputStream $ \ i ->
            case i of
              Nothing -> S.write Nothing os
              Just (Left a) -> S.write (Just a) as
              Just (Right a) -> S.write (Just a) bs



mapReduce = undefined
-- -------------------------------------------------------------------------------
-- -- | A generic map-reduce function that should be good enough for most
-- -- cases.
-- mapReduce
--     :: forall a k v b. (MRKey k, Serialize v)
--     => (a -> MaybeT IO [(k, v)])
--     -- ^ Common map key
--     -> (k -> b -> v -> IO b)
--     -- ^ Left fold in reduce stage
--     -> b
--     -- ^ A starting point for fold
--     -> MapReduce a (k,b)
-- mapReduce mp rd a0 = MapReduce mro pSerialize m  Nothing r
--     where
--       n = numKeys (undefined :: k)
--       mro = def { _mroPart = Partition n n }

--       m :: Mapper a k v
--       m = awaitForever $ \ a -> runMaybeT $ hoist (lift . lift) (mp a) >>= lift . C.sourceList

--       r :: Reducer k v (k,b)
--       r = do
--           (k, b) <- C.foldM step (Nothing, a0)
--           case k of
--             Nothing -> return ()
--             Just k' -> yield (k', b)


--       step (_, acc) (k, v) = do
--           !b <- liftIO $ rd k acc v
--           return (Just k, b)


firstBy = undefined
-- -------------------------------------------------------------------------------
-- -- | Deduplicate input objects that have the same key value; the first
-- -- object seen for each key will be kept.
-- firstBy
--     :: forall a k. (Serialize a, MRKey k)
--     => (a -> MaybeT IO [k])
--     -- ^ Key making function
--     -> MapReduce a a
-- firstBy f = mapReduce mp rd Nothing >.> (C.map snd =$= C.catMaybes)
--     where
--       mp :: a -> MaybeT IO [(k, a)]
--       mp a = do
--           k <- f a
--           return $ zip k (repeat a)

--       rd :: k -> Maybe a -> a -> IO (Maybe a)
--       rd _ Nothing a = return $! Just a
--       rd _ acc _ = return $! acc


mapMR = undefined
-- -------------------------------------------------------------------------------
-- -- | A generic map-only MR step.
-- mapMR :: (Serialize b) => (v -> IO [b]) -> MapReduce v b
-- mapMR f = MapReduce def pSerialize mp Nothing rd
--     where
--       mp = do
--           rng <- liftIO mkRNG
--           awaitForever $ \ a -> do
--               t <- liftIO $ randomToken 2 rng
--               res <- liftIO $ f a
--               mapM_ (\x -> yield (t, x)) res
--       rd = C.map snd


oneSnap = undefined
-- -------------------------------------------------------------------------------
-- -- | Do somthing with only the first row we see, putting the result in
-- -- the given HDFS destination.
-- oneSnap
--     :: RunContext
--     -> FilePath
--     -> (a -> B.ByteString)
--     -> Conduit a IO a
-- oneSnap settings s3fp f = do
--     h <- await
--     case h of
--       Nothing -> return ()
--       Just h' -> do
--           liftIO $ putHeaders (f h')
--           yield h'
--           awaitForever yield
--   where
--     putHeaders x = do
--         tmp <- randomFileName
--         withLocalFile settings tmp $ \ fn -> B.writeFile fn x
--         chk <- hdfsFileExists settings s3fp
--         when (not chk) $ void $ hdfsPut settings tmp s3fp
--         withLocalFile settings tmp removeFile


-------------------------------------------------------------------------------
-- | Monoidal inner (map-side) join for two types. Each type is mapped
-- into the common monoid, which is then collapsed during reduce.
--
-- Make sure an incoming 'Left' stays 'Left' and a 'Right' stays a
-- 'Right'.
--
-- TODO: Wrap around this with a better API so the user doesn't have
-- to care.
joinMR
    :: forall a b k v. (MRKey k, Monoid v, Serialize v)
    => Mapper (Either a b) k (Either v v)
    -- ^ Mapper for the input
    -> MapReduce (Either a b) v
joinMR mp = MapReduce mro pSerialize mp' Nothing red
    where
      mro = def { _mroPart = Partition (n+1) n }
      n = numKeys (undefined :: k)

      -- add to key so we know for sure all Lefts arrive before
      -- Rights.

      mp' :: Mapper (Either a b) CompositeKey (Either v v)
      mp' is os = do
        os' <- S.contramap modMap os
        mp is os'

      modMap (k, Left v) = (toCompKey k ++ ["1"], Left v)
      modMap (k, Right v) = (toCompKey k ++ ["2"], Right v)

      -- cache lefts, start emitting upon seeing the first right.
      red is os = go []
        where
          go acc = do
            inc <- S.read is
            case inc of
              Nothing -> return ()

              Just (_, Left r) -> go $! (r:acc)

              Just (_, Right b) -> do
                let xs = [mappend a b | a <- acc]
                forM_ xs $ \ x -> S.write (Just x) os
                go acc


