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
    , Mapper
    , Reducer
    , (>.>)
    , (<.<)
    , MRKey (..)
    , SingleKey (..)
    , Tap (..)
    , Tap'
    , tap
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

    , RerunStrategy (..)

    -- * Logging Related
    , logTo

    -- * Joining Multiple Datasets
    , joinStep
    , JoinType (..)
    , JoinKey

    ) where

-------------------------------------------------------------------------------
import           Control.Applicative
import           Control.Error
import           Control.Exception
import           Control.Exception.Lens
import           Control.Lens
import           Control.Monad.Operational   hiding (view)
import qualified Control.Monad.Operational   as O
import           Control.Monad.State
import qualified Data.ByteString.Char8       as B
import           Data.Conduit                as C
import qualified Data.Conduit.List           as C
import           Data.Conduit.Zlib
import           Data.Default
import           Data.Hashable
import           Data.List
import           Data.List.LCS.HuntSzymanski
import qualified Data.Map                    as M
import           Data.Monoid
import           Data.Ord
import           Data.Serialize
import qualified Data.Text                   as T
import           System.Directory
import           System.Environment
import           System.FilePath
import           System.IO
-------------------------------------------------------------------------------
import           Hadoop.Streaming
import           Hadoop.Streaming.Hadoop
import           Hadoop.Streaming.Join
import           Hadoop.Streaming.Logger
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
(>.>) :: Monad m => MapReduce a m b -> Conduit b m c -> MapReduce a m c
(MapReduce o p m r) >.> f = MapReduce o p m (r =$= f)


-------------------------------------------------------------------------------
-- | Dome something with the m-r input before starting the map stage.
(<.<) :: Monad m => Conduit c m a -> MapReduce a m b -> MapReduce c m b
f <.< (MapReduce o p m r) = MapReduce o p (f =$= m) r

-------------------------------------------------------------------------------
-- | A packaged MapReduce step. Make one of these for each distinct
-- map-reduce step in your overall 'Controller' flow.
data MapReduce a m b = forall k v. MRKey k => MapReduce {
      mrOptions :: MROptions
    -- ^ Hadoop and MapReduce options affecting only this specific
    -- job.
    , mrInPrism :: Prism' B.ByteString v
    -- ^ A serialization scheme for values between the map-reduce
    -- steps.
    , mrMapper  :: Mapper a m k v
    , mrReducer :: Reducer k v m b
    }


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
data Tap m a = Tap
    { location :: FilePath
    , proto    :: Protocol' m a
    }


-- | If two 'location's are the same, we consider two Taps equal.
instance Eq (Tap m a) where
    a == b = location a == location b


-- | It is often just fine to use IO as the base monad for MapReduce ops.
type Tap' a = Tap IO a


-- | Construct a 'DataDef'
tap :: FilePath -> Protocol' m a -> Tap m a
tap = Tap


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
            -> Tap IO B.ByteString
fileListTap settings loc = tap loc (Protocol enc dec)
  where
    enc = error "You should never use a fileListTap as output!"
    dec = linesConduit =$= readHdfsFile settings


data ContState = ContState {
      _csMRCount :: Int
    }

instance Default ContState where
    def = ContState 0


makeLenses ''ContState



-------------------------------------------------------------------------------
data ConI a where
    Connect :: forall i o. MapReduce i IO o
            -> [Tap IO i] -> Tap IO o
            -> ConI ()

    MakeTap :: Protocol' IO a -> ConI (Tap IO a)
    BinaryDirTap :: FilePath -> (FilePath -> Bool) -> ConI (Tap IO B.ByteString)

    ConIO :: IO a -> ConI a



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
    :: MapReduce a IO b
    -- ^ MapReduce step to run
    -> [Tap IO a]
    -- ^ Input files
    -> Protocol' IO b
    -- ^ Serialization protocol to be used on the output
    -> Controller (Tap IO b)
connect' mr inp proto = do
    out <- makeTap proto
    connect mr inp out
    return out


-------------------------------------------------------------------------------
-- | Connect a typed MapReduce program you supply with a list of
-- sources and a destination.
connect :: MapReduce a IO b -> [Tap IO a] -> Tap IO b -> Controller ()
connect mr inp outp = Controller $ singleton $ Connect mr inp outp


-------------------------------------------------------------------------------
makeTap :: Protocol' IO a -> Controller (Tap IO a)
makeTap proto = Controller $ singleton $ MakeTap proto


-------------------------------------------------------------------------------
-- | Creates a tap for a directory of binary files.
binaryDirTap
    :: FilePath
    -- ^ A root location to list files under
    -> (FilePath -> Bool)
    -- ^ A filter condition to refine the listing
    -> Controller (Tap IO B.ByteString)
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
          return $ Tap loc proto

      eval' (BinaryDirTap loc filt) = liftIO $ do
          localFile <- setupBinaryDir settings loc filt

          return $ fileListTap settings localFile

      eval' (Connect mr@(MapReduce mro mrInPrism _ _) inp outp) = go'
          where
            go' = do
                chk <- liftIO $ hdfsFileExists settings (location outp)
                case chk of
                  False -> go''
                  True ->
                    case rr of
                      RSFail -> lift $ $(logError) $ T.concat
                        ["Destination file exists: ", T.pack (location outp)]
                      RSSkip -> go''
                      RSReRun -> do
                        lift $ $(logInfo) $ T.pack $
                          "Destination file exists, will delete and rerun: " ++
                          location outp
                        _ <- liftIO $ hdfsDeletePath settings (location outp)
                        go''
            go'' = do
              mrKey <- newMRKey
              let mrs = mrOptsToRunOpts mro
              launchMapReduce settings mrKey
                mrs { mrsInput = map location inp
                    , mrsOutput = location outp }



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
      [arg] -> do
        _ <- evalStateT (interpretWithMonad (go arg) p) def
        return ()
      _ -> error "Usage: No arguments for job control or a phase name."
    where

      mkArgs mrKey = [ (Map, "map_" ++ mrKey)
                     , (Reduce, "reduce_" ++ mrKey) ]


      go :: String -> ConI b -> StateT ContState (LoggingT m) b

      go _ (ConIO f) = liftIO f

      go _ (MakeTap proto) = return $ Tap "---" proto

      go _ (BinaryDirTap _ _) = return $ fileListTap hs "---"

      go arg (Connect (MapReduce mro mrInPrism mp rd) inp outp) = do
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
              fn <- getFileName
              catching exception mp'
                (\ (e :: SomeException) ->
                     error $ "Exception raised during Map in stage " <> show mrKey <>
                             " while processing file " <> fn <> ": " <> show e)

            Just (Reduce, _) -> liftIO $ do
              fn <- getFileName
              catching exception red
                (\ (e :: SomeException) ->
                     error $ "Exception raised during Reduce in stage " <> show mrKey <>
                             " while processing file " <> fn <> ": " <> show e)

            Nothing -> return ()


-- | TODO: See if this works. Objective is to increase type safety of
-- join inputs. Notice how we have an existential on a.
--
-- A join definition that ultimately produces objects of type b.
data JoinDef m b = forall a. JoinDef {
      joinTap  :: Tap m a
    , joinType :: JoinType
    , joinMap  :: Conduit a m (JoinKey, b)
    }


-------------------------------------------------------------------------------
-- | A convenient way to express multi-way join operations into a
-- single data type. All you need to supply is the map operation for
-- each tap, the reduce step is assumed to be the Monoidal 'mconcat'.
joinStep
    :: forall m k b a.
       (MonadIO m, MonadThrow m,
        Show b, Monoid b, Serialize b,
        MRKey k)
    => [(Tap m a, JoinType, Mapper a m k b)]
    -- ^ Dataset definitions and how to map each dataset.
    -> MapReduce a m b
joinStep fs = MapReduce mro pSerialize mp rd
    where
      salt = 0
      showBS = B.pack . show
      n = numKeys (undefined :: k)

      mro = joinOpts { mroPart = Partition (n+1) n }

      locations :: [FilePath]
      locations = map (location . view _1) fs

      dataSets :: [(FilePath, DataSet)]
      dataSets = map (\ loc -> (loc, DataSet (showBS $ hashWithSalt salt loc))) locations

      dsIx :: M.Map FilePath DataSet
      dsIx = M.fromList dataSets

      tapIx :: M.Map DataSet (Tap m a)
      tapIx = M.fromList $ zip (map snd dataSets) (map (view _1) fs)

      getTapDS :: Tap m a -> DataSet
      getTapDS t =
          fromMaybe (error "Can't identify dataset name for given location") $
          M.lookup (location t) dsIx


      fs' :: [(DataSet, JoinType)]
      fs' = map (\ (t, jt, _) -> (getTapDS t, jt)) fs


      -- | get dataset name from a given input filename
      getDS nm = fromMaybe (error "Can't identify current tap from filename.") $ do

        curLoc <- fmap fst . lastMay . sortBy (comparing snd) .
                  zip locations $
                  map (length . lcs nm) locations

        M.lookup curLoc dsIx


      -- | get the conduit for given dataset name
      mkMap' ds = fromMaybe (error "Can't identify current tap in IX.") $ do
                      t <- M.lookup ds tapIx
                      cond <- find ((== t) . view _1) fs
                      return $ view _3 cond =$= C.map (over _1 toCompKey)

      mp = joinMapper getDS mkMap'

      rd =  joinReducer fs'


