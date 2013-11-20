{-# LANGUAGE BangPatterns               #-}
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
-- Module      :  Hadoop.Streaming.Controller
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

module Hadoop.Streaming.Controller
    (


    -- * Hadoop Program Construction
      Controller

    , connect
    , connect'
    , io

    , MapReduce (..)
    , mrOptions
    , Mapper
    , Reducer
    , (>.>)
    , (<.<)

    , MRKey (..)
    , CompositeKey
    , SingleKey (..)

    -- * Data Sources
    , Tap (..)
    , tap
    , taps
    , mergeTaps
    , binaryDirTap
    , setupBinaryDir
    , fileListTap
    , readHdfsFile

    -- * Command Line Entry Point
    , hadoopMain
    , HadoopEnv (..)
    , clouderaDemo
    , amazonEMR

    -- * Settings for MapReduce Jobs
    , MROptions
    , mroEq
    , mroPart
    , mroNumMap
    , mroNumReduce
    , mroCompress
    , mroOutSep
    , gzipCodec
    , snappyCodec
    , PartitionStrategy (..)
    , RerunStrategy (..)

    -- * Hadoop Utilities
    , emitCounter
    , hsEmitCounter
    , emitStatus
    , getFileName


    -- * Logging Related
    , logTo


    -- * MapReduce Combinators

    , joinStep
    , JoinType (..)
    , JoinKey

    , mapReduce
    , firstBy
    , mapMR
    , oneSnap
    , joinMR

    -- * Data Serialization Utilities
    , module Hadoop.Streaming.Protocol

    ) where

-------------------------------------------------------------------------------
import           Control.Applicative
import           Control.Error
import           Control.Exception
import           Control.Exception.Lens
import           Control.Lens
import           Control.Monad.Morph
import           Control.Monad.Operational hiding (view)
import qualified Control.Monad.Operational as O
import           Control.Monad.State
import qualified Data.ByteString.Char8     as B
import           Data.ByteString.Search    as B
import           Data.Conduit              as C
import qualified Data.Conduit.List         as C
import           Data.Conduit.Zlib
import           Data.Default
import           Data.List
import qualified Data.Map                  as M
import           Data.Monoid
import           Data.RNG
import           Data.Serialize
import qualified Data.Text                 as T
import           Data.Text.Encoding
import           System.Directory
import           System.Environment
import           System.FilePath
import           System.IO
-------------------------------------------------------------------------------
import           Hadoop.Streaming          hiding (mapReduce)
import           Hadoop.Streaming.Hadoop
import           Hadoop.Streaming.Join
import           Hadoop.Streaming.Logger
import           Hadoop.Streaming.Protocol
import           Hadoop.Streaming.Types
-------------------------------------------------------------------------------


newtype SingleKey a = SingleKey { unKey :: a }
    deriving (Eq,Show,Read,Ord,Serialize)


mrKeyError :: Int -> Int -> a
mrKeyError i n =
    error $ "Expected MapReduce key to have "
         <> show i <> " parts. Instead received "
         <> show n <> " parts."

class MRKey k where
    toCompKey :: k -> CompositeKey
    fromCompKey :: CompositeKey -> Maybe k
    numKeys :: k -> Int

instance MRKey B.ByteString where
    toCompKey k = [k]
    fromCompKey [k] = Just k
    fromCompKey xs  = mrKeyError 1 (length xs)
    numKeys _ = 1

instance MRKey CompositeKey where
    toCompKey ks = ks
    fromCompKey ks = Just ks
    numKeys ks = length ks

instance MRKey String where
    toCompKey = toCompKey . B.pack
    fromCompKey = fmap B.unpack . fromCompKey
    numKeys _ = 1


instance MRKey T.Text where
    toCompKey = toCompKey . encodeUtf8
    fromCompKey = fmap decodeUtf8 . fromCompKey
    numKeys _ = 1


instance Serialize a => MRKey (SingleKey a) where
    toCompKey a = [review pSerialize a]
    fromCompKey [a] = preview pSerialize a
    fromCompKey xs  = mrKeyError 1 (length xs)
    numKeys _ = 1


instance (Serialize a, Serialize b) => MRKey (a,b) where
    toCompKey (a,b) = [review pSerialize a, review pSerialize b]
    fromCompKey [a,b] = (,) <$> preview pSerialize a <*> preview pSerialize b
    fromCompKey xs  = mrKeyError 2 (length xs)
    numKeys _ = 2

instance (Serialize a, Serialize b, Serialize c) => MRKey (a,b,c) where
    toCompKey (a,b,c) = [review pSerialize a, review pSerialize b, review pSerialize c]
    fromCompKey [a,b,c] = (,,)
        <$> preview pSerialize a
        <*> preview pSerialize b
        <*> preview pSerialize c
    fromCompKey xs  = mrKeyError 3 (length xs)
    numKeys _ = 3

instance (Serialize a, Serialize b, Serialize c, Serialize d) => MRKey (a,b,c,d) where
    toCompKey (a,b,c,d) = [review pSerialize a, review pSerialize b, review pSerialize c, review pSerialize d]
    fromCompKey [a,b,c,d] = (,,,)
        <$> preview pSerialize a
        <*> preview pSerialize b
        <*> preview pSerialize c
        <*> preview pSerialize d
    fromCompKey xs  = mrKeyError 4 (length xs)
    numKeys _ = 4

instance (Serialize a, Serialize b, Serialize c, Serialize d, Serialize e) => MRKey (a,b,c,d,e) where
    toCompKey (a,b,c,d,e) =
        [ review pSerialize a, review pSerialize b, review pSerialize c
        , review pSerialize d, review pSerialize e ]
    fromCompKey [a,b,c,d,e] = (,,,,)
        <$> preview pSerialize a
        <*> preview pSerialize b
        <*> preview pSerialize c
        <*> preview pSerialize d
        <*> preview pSerialize e
    fromCompKey xs  = mrKeyError 5 (length xs)
    numKeys _ = 5

instance (Serialize a, Serialize b, Serialize c, Serialize d, Serialize e, Serialize f) => MRKey (a,b,c,d,e,f) where
    toCompKey (a,b,c,d,e,f) =
        [ review pSerialize a, review pSerialize b, review pSerialize c
        , review pSerialize d, review pSerialize e, review pSerialize f]
    fromCompKey [a,b,c,d,e,f] = (,,,,,)
        <$> preview pSerialize a
        <*> preview pSerialize b
        <*> preview pSerialize c
        <*> preview pSerialize d
        <*> preview pSerialize e
        <*> preview pSerialize f
    fromCompKey xs  = mrKeyError 6 (length xs)
    numKeys _ = 6





-------------------------------------------------------------------------------
-- | Do something with m-r output before writing it to a tap.
(>.>) :: MapReduce a b -> Conduit b IO c -> MapReduce a c
(MapReduce o p m r) >.> f = MapReduce o p m (r =$= f)


-------------------------------------------------------------------------------
-- | Dome something with the m-r input before starting the map stage.
(<.<) :: Conduit c IO a -> MapReduce a b -> MapReduce c b
f <.< (MapReduce o p m r) = MapReduce o p (f =$= m) r

-------------------------------------------------------------------------------
-- | A packaged MapReduce step. Make one of these for each distinct
-- map-reduce step in your overall 'Controller' flow.
data MapReduce a b = forall k v. MRKey k => MapReduce {
      _mrOptions :: MROptions
    -- ^ Hadoop and MapReduce options affecting only this specific
    -- job.
    , _mrInPrism :: Prism' B.ByteString v
    -- ^ A serialization scheme for values between the map-reduce
    -- steps.
    , _mrMapper  :: Mapper a k v
    , _mrReducer :: Reducer k v b
    }


-------------------------------------------------------------------------------
mrOptions :: Lens' (MapReduce a b) MROptions
mrOptions f (MapReduce o p m r) = (\ o' -> MapReduce o' p m r) <$> f o

makeLenses ''MapReduce

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
    { location :: [FilePath]
    , proto    :: Protocol' a
    }


-- | If two 'location's are the same, we consider two Taps equal.
instance Eq (Tap a) where
    a == b = location a == location b


-- | Construct a 'DataDef'
tap :: FilePath -> Protocol' a -> Tap a
tap fp p = Tap [fp] p

taps :: [FilePath] -> Protocol' a -> Tap a
taps fp p = Tap fp p


------------------------------------------------------------------------------
-- | Conduit that takes in hdfs filenames and outputs the file contents.
readHdfsFile :: HadoopEnv -> Conduit B.ByteString IO B.ByteString
readHdfsFile settings = awaitForever $ \s3Uri -> do
    let uriStr = B.unpack s3Uri
        getFile = hdfsLocalStream settings uriStr
    if isSuffixOf "gz" uriStr
      then getFile =$= ungzip
      else getFile


------------------------------------------------------------------------------
-- | Tap for handling file lists.  Hadoop can't process raw binary data
-- because it splits on newlines.  This tap allows you to get around that
-- limitation by instead making your input a list of file paths that contain
-- binary data.  Then the file names get split by hadoop and each map job
-- reads from those files as its first step.
fileListTap :: HadoopEnv
            -> FilePath
            -- ^ A file containing a list of files to be used as input
            -> Tap B.ByteString
fileListTap settings loc = tap loc (Protocol enc dec)
  where
    enc = error "You should never use a fileListTap as output!"
    dec = linesConduit =$= readHdfsFile settings


data ContState = ContState {
      _csMRCount :: Int
    -- ^ MR run count; one for each 'connect'.
    , _csMRVars  :: M.Map String B.ByteString
    -- ^ Arbitrary key-val store that's communicated to nodes.
    , _csDynId   :: Int
    -- ^ Keeps increasing count of dynamic taps in the order they are
    -- created in the Controller monad. Needed so we can communicate
    -- the tap locations to MR nodes.
    }

instance Default ContState where
    def = ContState 0 M.empty 0


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
    SetVal :: String -> B.ByteString -> ConI ()
    GetVal :: String -> ConI (Maybe B.ByteString)


-- | All MapReduce steps are integrated in the 'Controller' monad.
--
-- Warning: We do have an 'io' combinator as an escape valve for you
-- to use. However, you need to be careful how you use the result of
-- an IO computation. Remember that the same 'main' function will run
-- on both the main orchestrator process and on each and every
-- map/reduce node.
newtype Controller a = Controller { unController :: Program ConI a }
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


-- | LIft IO into 'Controller'. Note that this is a NOOP for when the
-- Mappers/Reducers are running; it only executes in the main
-- controller application during job-flow orchestration.
--
-- If you try to construct a 'MapReduce' step that depends on the
-- result of an 'io' call, you'll get a runtime error when running
-- your job.
io :: IO a -> Controller a
io f = Controller $ singleton $ ConIO f


-------------------------------------------------------------------------------
newMRKey :: MonadState ContState m => m String
newMRKey = do
    i <- gets _csMRCount
    csMRCount %= (+1)
    return $! show i


-------------------------------------------------------------------------------
-- | Grab list of files in destination, write into a file, put file on
-- HDFS so it is shared and return the local/HDFS path, they are the
-- same, as output.
setupBinaryDir :: HadoopEnv -> FilePath -> (FilePath -> Bool) -> IO FilePath
setupBinaryDir settings loc chk = do
    localFile <- randomFilename
    let root = dropFileName localFile
    createDirectoryIfMissing True root
    hdfsMkdir settings root
    files <- hdfsLs settings loc
    let files' = filter chk files
    writeFile localFile $ unlines files'
    hdfsPut settings localFile localFile
    return localFile


tapLens curId = csMRVars.at ("tap_" <> show curId)
pickId = do
    curId <- use csDynId
    csDynId %= (+1)
    return curId


-------------------------------------------------------------------------------
-- | Interpreter for the central job control process
orchestrate
    :: (MonadIO m, MonadLogger m)
    => Controller a
    -> HadoopEnv
    -> RerunStrategy
    -> ContState
    -> m (Either String a)
orchestrate (Controller p) settings rr s = evalStateT (runEitherT (go p)) s
    where
      go = eval . O.view

      eval (Return a) = return a
      eval (i :>>= f) = eval' i >>= go . f

      eval' :: (MonadLogger m, MonadIO m) => ConI a -> EitherT String (StateT ContState m) a

      eval' (ConIO f) = liftIO f

      eval' (MakeTap proto) = do
          loc <- liftIO randomFilename

          curId <- pickId
          tapLens curId .= Just (B.pack loc)

          return $ Tap [loc] proto

      eval' (BinaryDirTap loc filt) = do
          localFile <- liftIO $ setupBinaryDir settings loc filt

          -- remember location of the file from the original loc
          -- string
          curId <- pickId
          tapLens curId .= Just (B.pack localFile)

          return $ fileListTap settings localFile


      eval' (SetVal k v) = csMRVars . at k .= Just v
      eval' (GetVal k) = use (csMRVars . at k)

      eval' (Connect mr@(MapReduce mro mrInPrism _ _) inp outp nm) = go'
          where
            go' = do
                mrKey <- newMRKey

                chk <- liftIO $ mapM (hdfsFileExists settings) (location outp)
                case any id chk of
                  False -> go'' mrKey
                  True ->
                    case rr of
                      RSFail -> lift $ $(logError) $ T.concat
                        [ "Destination exists: ", T.pack $ head (location outp)]
                      RSSkip -> lift $ $(logInfo) $ T.concat
                        [ "Destination exists. Skipping ", T.intercalate ", " $
                          map T.pack (location outp)]
                      RSReRun -> do
                        lift $ $(logInfo) $ T.pack $
                          "Destination file exists, will delete and rerun: " ++
                          head (location outp)
                        _ <- liftIO $ mapM_ (hdfsDeletePath settings) (location outp)
                        go'' mrKey


            go'' mrKey = do

              -- serialize current state to HDFS, to be read by
              -- individual mappers reducers of this step.
              runToken <- liftIO $ (mkRNG >>= randomToken 64) <&> B.unpack
              let fn = tmpRoot <> runToken
              st <- use csMRVars
              liftIO $ createDirectoryIfMissing True tmpRoot
              liftIO $ writeFile fn (show st)
              liftIO $ hdfsPut settings fn fn

              let mrs = mrOptsToRunOpts mro
              launchMapReduce settings mrKey runToken
                mrs { mrsInput = concatMap location inp
                    , mrsOutput = head (location outp)
                    , mrsJobName = nm
                    }



data Phase = Map | Reduce


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
-- | The main entry point. Use this function to produce a command line
-- program that encapsulates everything.
--
-- When run without arguments, the program will orchestrate the entire
-- MapReduce job flow. The same program also doubles as the actual
-- mapper/reducer executable when called with right arguments, though
-- you don't have to worry about that.
hadoopMain
    :: forall m a. (MonadThrow m, MonadIO m)
    => Controller a
    -- ^ The Hadoop streaming application to run.
    -> HadoopEnv
    -- ^ Hadoop environment info.
    -> RerunStrategy
    -- ^ What to do if destination files already exist.
    -> m ()
hadoopMain c@(Controller p) hs rr = logTo stdout $ do
    args <- liftIO getArgs
    case args of
      [] -> do
        res <- orchestrate c hs rr def
        liftIO $ either print (const $ putStrLn "Success.") res
      [runToken, arg] -> do
        _ <- evalStateT (loadState runToken >>
                         interpretWithMonad (go runToken arg) p) def
        return ()
      _ -> error "Usage: No arguments for job control or a phase name."
    where

      mkArgs mrKey = [ (Map, "map_" ++ mrKey)
                     , (Reduce, "reduce_" ++ mrKey) ]


      -- load controller varibles back up
      loadState runToken = do
          let fn = tmpRoot <> runToken
          tmp <- liftIO $ hdfsGet hs fn
          st <- liftIO $ readFile tmp <&> read
          csMRVars %= M.union st
          liftIO $ removeFile tmp


      go :: String -> String -> ConI b -> StateT ContState (LoggingT m) b

      go _ _ (ConIO f) = liftIO f

      go _ _ (MakeTap proto) = do
          curId <- pickId
          dynLoc <- use $ tapLens curId
          case dynLoc of
            Nothing -> error $ "Dynamic location can't be determined for MakTap at index " <> show curId
            Just loc' -> return $ Tap ([B.unpack loc']) proto

      go _ _ (BinaryDirTap loc _) = do

          -- remember location of the file from the original loc
          -- string
          curId <- pickId
          dynLoc <- use $ tapLens curId
          case dynLoc of
            Nothing -> error $
              "Dynamic location can't be determined for BinaryDirTap at: " <> loc
            Just loc' -> return $ fileListTap hs $ B.unpack loc'

      -- setting in map-reduce phase is a no-op... There's nobody to
      -- communicate it to.
      go _ _ (SetVal _ _) = return ()
      go _ _ (GetVal k) = use (csMRVars . at k)

      go _ arg (Connect (MapReduce mro mrInPrism mp rd) inp outp _) = do
          mrKey <- newMRKey

          let dec = protoDec . proto $ head inp
              enc = protoEnc  $ proto outp

          let mp' = (mapperWith mrInPrism $
                     dec =$=
                     mp =$=
                     C.map (\ (!k, !v) -> (toCompKey k, v)))

          let red = do
                  let conv (k,v) = do
                          !k' <- fromCompKey k
                          return (k', v)
                      rd' = C.mapMaybe conv =$= rd =$= enc
                  liftIO $ (reducerMain mro mrInPrism rd')


          case find ((== arg) . snd) $ mkArgs mrKey of

            Just (Map, _) -> liftIO $ do
              curFile <- getFileName
              catching exception mp'
                (\ (e :: SomeException) ->
                     error $ "Exception raised during Map in stage " <>
                             show mrKey <>
                             " while processing file " <> curFile <>
                             ": " <> show e)

            Just (Reduce, _) -> liftIO $ do
              catching exception red
                (\ (e :: SomeException) ->
                     error $ "Exception raised during Reduce in stage " <>
                             show mrKey <> ": " <> show e)

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
joinStep
    :: forall k b a.
       (Show b, Monoid b, Serialize b,
        MRKey k)
    => [(Tap a, JoinType, Mapper a k b)]
    -- ^ Dataset definitions and how to map each dataset.
    -> MapReduce a b
joinStep fs = MapReduce mro pSerialize mp rd
    where
      showBS = B.pack . show
      n = numKeys (undefined :: k)

      mro = joinOpts { _mroPart = Partition (n+1) n }

      locations :: [FilePath]
      locations = concatMap (location . view _1) fs

      taps' :: [Tap a]
      taps' = concatMap ((\t -> replicate (length (location t)) t) . view _1) fs

      locations' = map B.pack locations

      dataSets :: [(FilePath, DataSet)]
      dataSets = map (\ (loc, i) -> (loc, DataSet (showBS i))) $
                 zip locations [0..]

      dsIx :: M.Map FilePath DataSet
      dsIx = M.fromList dataSets

      tapIx :: M.Map DataSet (Tap a)
      tapIx = M.fromList $ zip (map snd dataSets) taps'

      getTapDS :: Tap a -> [DataSet]
      getTapDS t = mapMaybe (flip M.lookup dsIx) (location t)


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
                      return $ view _3 cond =$= C.map (over _1 toCompKey)

      mp = joinMapper getDS mkMap'

      rd =  joinReducer fs'




-------------------------------------------------------------------------------
-- | Combine two taps intelligently into the Either sum type.
mergeTaps :: Tap a -> Tap b -> Tap (Either a b)
mergeTaps ta tb = Tap (location ta ++ location tb) newP
    where
      newP = Protocol enc dec

      dec = do
          fn <- lift getFileName
          if (any (flip isInfixOf fn) (map (takeWhile (/= '*')) $ location ta))
            then (protoDec . proto) ta =$= C.map Left
            else (protoDec . proto) tb =$= C.map Right

      enc =
          awaitForever $ \ res ->
              case res of
                Left a -> yield a =$= (protoEnc . proto) ta
                Right b -> yield b =$= (protoEnc . proto) tb





-------------------------------------------------------------------------------
-- | A generic map-reduce function that should be good enough for most
-- cases.
mapReduce
    :: forall a k v b. (MRKey k, Serialize v)
    => (a -> MaybeT IO [(k, v)])
    -- ^ Common map key
    -> (k -> b -> v -> IO b)
    -- ^ Left fold in reduce stage
    -> b
    -- ^ A starting point for fold
    -> MapReduce a (k,b)
mapReduce mp rd a0 = MapReduce mro pSerialize m r
    where
      n = numKeys (undefined :: k)
      mro = def { _mroPart = Partition n n }

      m :: Mapper a k v
      m = awaitForever $ \ a -> runMaybeT $ hoist lift (mp a) >>= lift . C.sourceList

      r :: Reducer k v (k,b)
      r = do
          (k, b) <- C.foldM step (Nothing, a0)
          case k of
            Nothing -> return ()
            Just k' -> yield (k', b)


      step (_, acc) (k, v) = do
          !b <- rd k acc v
          return (Just k, b)




-------------------------------------------------------------------------------
-- | Deduplicate input objects that have the same key value; the first
-- object seen for each key will be kept.
firstBy
    :: forall a k. (Serialize a, MRKey k)
    => (a -> MaybeT IO [k])
    -- ^ Key making function
    -> MapReduce a a
firstBy f = mapReduce mp rd Nothing >.> (C.map snd =$= C.catMaybes)
    where
      mp :: a -> MaybeT IO [(k, a)]
      mp a = do
          k <- f a
          return $ zip k (repeat a)

      rd :: k -> Maybe a -> a -> IO (Maybe a)
      rd _ Nothing a = return $! Just a
      rd _ acc _ = return $! acc


-------------------------------------------------------------------------------
-- | A generic map-only MR step.
mapMR :: (Serialize v) => (v -> b) -> MapReduce v b
mapMR f = MapReduce def pSerialize mp rd
    where
      mp = do
          rng <- liftIO mkRNG
          let send a = do
                  t <- liftIO $ randomToken 2 rng
                  return (t, a)
          C.mapM send
      rd = C.map (f . snd)


-------------------------------------------------------------------------------
-- | Do somthing with only the first row we see, putting the result in
-- the given HDFS destination.
oneSnap
    :: FilePath
    -> (a -> B.ByteString)
    -> Conduit a IO a
oneSnap s3fp f = do
    h <- await
    case h of
      Nothing -> return ()
      Just h' -> do
          liftIO $ putHeaders (f h')
          yield h'
          awaitForever yield
  where
    putHeaders x = do
        tmp <- randomFilename
        B.writeFile tmp x
        chk <- hdfsFileExists amazonEMR s3fp
        when (not chk) $ hdfsPut amazonEMR tmp s3fp >> return ()
        removeFile tmp



-------------------------------------------------------------------------------
-- | Monoidal inner (map-side) join for two types. Each type is mapped
-- into the common monoid, which is then collapsed during reduce.
joinMR
    :: forall a b k v. (MRKey k, Monoid v, Serialize v)
    => Conduit (Either a b) IO (k, Either v v)
    -- ^ Mapper for the input
    -> MapReduce (Either a b) v
joinMR mp = MapReduce mro pSerialize mp red
    where
      mro = def { _mroPart = Partition n n }
      n = numKeys (undefined :: k)

      red = do
          xs <- C.consume <&> map snd
          let (ls, rs) = partition isLeft xs & over (both.traverse) (view chosen)
          mapM_ yield [mappend a b | a <- ls, b <- rs]

