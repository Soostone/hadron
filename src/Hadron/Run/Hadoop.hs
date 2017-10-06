{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE TemplateHaskell       #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Hadron.Run.Hadoop
-- Copyright   :  Soostone Inc
-- License     :  BSD3
--
-- Maintainer  :  Ozgun Ataman
-- Stability   :  experimental
--
-- Deal with the hadoop command line program.
----------------------------------------------------------------------------


module Hadron.Run.Hadoop
    ( HadoopEnv (..)
    , hsBin, hsJar
    , clouderaDemo
    , amazonEMR

    , PartitionStrategy (..)
    , numSegs
    , eqSegs
    , Comparator (..)

    , HadoopRunOpts (..)
    , mrSettings
    , mrsInput, mrsOutput, mrsPart, mrsNumMap, mrsNumReduce
    , mrsCombine, mrsCompress, mrsOutSep, mrsJobName, mrsComparator
    , mrsTaskTimeout

    , Codec
    , gzipCodec
    , snappyCodec

    -- * Hadoop Command Line Wrappers
    , hadoopMapReduce
    , hdfsFileExists
    , hdfsDeletePath
    , hdfsLs, parseLS
    , hdfsPut
    , hdfsFanOut
    , hdfsFanOutStream
    , hdfsMkdir
    , tmpRoot
    , hdfsCat
    , hdfsGet
    , hdfsChmod
    , randomFilename

    ) where

-------------------------------------------------------------------------------
import           Control.Applicative          as A
import           Control.Error
import           Control.Lens
import           Control.Monad
import           Control.Monad.Trans
import           Control.Monad.Trans.Resource
import           Crypto.Hash.MD5
import qualified Data.ByteString.Base16       as Base16
import           Data.ByteString.Char8        (ByteString)
import qualified Data.ByteString.Char8        as B
import           Data.Conduit
import           Data.Conduit.Binary          (sourceHandle)
import           Data.Default
import           Data.List
import           Data.List.LCS.HuntSzymanski
import           Data.Monoid
import           Data.String.Conv
import qualified Data.Text                    as T
import           System.Directory
import           System.Environment
import           System.Exit
import           System.FilePath
import           System.FilePath.Lens
import           System.IO
import           System.Process
-------------------------------------------------------------------------------
import           Hadron.Logger
import           Hadron.Run.FanOut
import           Hadron.Utils
-------------------------------------------------------------------------------


-------------------------------------------------------------------------------
echo :: (A.Applicative m, MonadIO m) => Severity -> LogStr -> m ()
echo sev msg = runLog $ logMsg "Run.Hadoop" sev msg


-------------------------------------------------------------------------------
echoInfo :: (Applicative m, MonadIO m) => LogStr -> m ()
echoInfo = echo InfoS



data HadoopEnv = HadoopEnv {
      _hsBin :: String
    , _hsJar :: String
    }


-- | Settings for the cloudera demo VM.
clouderaDemo :: HadoopEnv
clouderaDemo = HadoopEnv {
            _hsBin = "hadoop"
          , _hsJar = "/usr/lib/hadoop-0.20-mapreduce/contrib/streaming/hadoop-streaming-2.0.0-mr1-cdh4.2.0.jar"
          }


-- | Settings for Amazon's EMR instances.
amazonEMR :: HadoopEnv
amazonEMR = HadoopEnv {
              _hsBin = "/home/hadoop/bin/hadoop"
            , _hsJar = "/home/hadoop/contrib/streaming/hadoop-streaming.jar"
            }


instance Default HadoopEnv where
    def = clouderaDemo


data PartitionStrategy
    = NoPartition
    -- ^ Expect a single key segment emitted from the 'Mapper'.
    | Partition {
        keySegs  :: Int
      -- ^ Total segments comprising a key.
      , partSegs :: Int
      -- ^ First N key segments used for partitioning. All keys that
      -- share these segments will be routed to the same reducer.
      }
    -- ^ Expect a composite key emitted from the 'Mapper'.


instance Default PartitionStrategy where def = NoPartition


data Comparator
    = RegularComp
    -- ^ Regular sorting
    | NumericComp Int Int Bool
    -- ^ Numeric sorting spanning fields i to j, True=reversed

instance Default Comparator where def = RegularComp


-------------------------------------------------------------------------------
-- | Number of total key segments.
numSegs :: PartitionStrategy -> Int
numSegs NoPartition = 1
numSegs Partition{..} = keySegs


-------------------------------------------------------------------------------
-- | Number of key segments that constitute input object equality, for
-- hadoop partitions.
eqSegs :: PartitionStrategy -> Int
eqSegs NoPartition = 1
eqSegs Partition{..} = partSegs


type Codec = String

-------------------------------------------------------------------------------
gzipCodec :: Codec
gzipCodec = "org.apache.hadoop.io.compress.GzipCodec"

snappyCodec :: Codec
snappyCodec = "org.apache.hadoop.io.compress.SnappyCodec"


type MapReduceKey = String
type RunToken = String


-------------------------------------------------------------------------------
-- | Useful reference for hadoop flags:
--
-- @http://hadoop.apache.org/docs/r2.4.1/hadoop-mapreduce-client/hadoop-mapreduce-client-core/mapred-default.xml@
data HadoopRunOpts = HadoopRunOpts {
      _mrsInput       :: [String]
    , _mrsOutput      :: String
    , _mrsPart        :: PartitionStrategy
    , _mrsNumMap      :: Maybe Int
    , _mrsNumReduce   :: Maybe Int
    , _mrsTaskTimeout :: Maybe Int
    , _mrsCombine     :: Bool
    , _mrsCompress    :: Maybe Codec
    , _mrsOutSep      :: Maybe Char
    -- ^ A separator to be used in reduce output. It is sometimes
    -- useful to specify one to trick Hadoop.
    , _mrsJobName     :: Maybe String
    , _mrsComparator  :: Comparator
    }
makeLenses ''HadoopRunOpts

instance Default HadoopRunOpts where
    def = HadoopRunOpts [] "" def Nothing Nothing Nothing False Nothing Nothing Nothing def

-- | A simple starting point to defining 'HadoopRunOpts'
mrSettings
    :: [String]
    -- ^ Input files
    -> String
    -- ^ Output files
    -> HadoopRunOpts
mrSettings ins out = def { _mrsInput = ins, _mrsOutput = out }



-------------------------------------------------------------------------------
hadoopMapReduce
    :: (Functor m, MonadIO m)
    => HadoopEnv
    -> MapReduceKey
    -> RunToken
    -> HadoopRunOpts
    -> ExceptT T.Text m ()
hadoopMapReduce HadoopEnv{..} mrKey runToken HadoopRunOpts{..} = do
    exec <- scriptIO getExecutablePath
    prog <- scriptIO getProgName
    echoInfo $ "Launching Hadoop job for MR key: " <> ls mrKey

    let args = mkArgs exec prog

    echoInfo $ "Hadoop arguments: " <> ls (intercalate " " args)

    (code, out, eout) <- scriptIO $ readProcessWithExitCode _hsBin args ""
    case code of
      ExitSuccess -> return ()
      e -> do
        echo ErrorS $ ls $ intercalate "\n"
          [ "Hadoop job failed.", "StdOut:"
          , out, "", "StdErr:", eout]

        hoistEither $ Left $ T.pack $ "MR job failed with: " ++ show e
    where
      mkArgs exec prog =
            [ "jar", _hsJar] ++
            comp ++ numMap ++ numRed ++ timeout ++ outSep ++ jobName ++
            comparator ++ part ++
            inputs ++
            [ "-output", _mrsOutput] ++
            mkStage prog "mapper" ++
            mkStage prog "reducer" ++
            if _mrsCombine then mkStage prog "combiner" else [] ++
            [ "-file", exec ]


      mkStage prog stage =
          [ "-" ++ stage
          , "\"" ++ prog ++ " " ++ runToken ++ " " ++ stage ++ "_" ++ mrKey ++ "\""
          ]

      jobName = maybe [] (\nm -> ["-D", "mapreduce.job.name=\"" <> nm <>"\""])
                _mrsJobName


      inputs = concatMap mkInput _mrsInput
      mkInput i = ["-input", i]

      numMap = maybe [] (\x -> ["-D", "mapreduce.job.maps=" ++ show x]) _mrsNumMap
      numRed = maybe [] (\x -> ["-D", "mapreduce.job.reduces=" ++ show x]) _mrsNumReduce

      timeout = maybe [] (\x -> ["-D", "mapreduce.task.timeout=" ++ show x]) _mrsTaskTimeout

      comp =
        case _mrsCompress of
          Just codec -> [ "-D", "mapreduce.output.fileoutputformat.compress=true"
                        , "-D", "mapreduce.output.fileoutputformat.compress.codec=" ++ codec
                        -- , "-D", "mapred.compress.map.output=true"
                        -- , "-D", "mapred.map.output.compression.codec=" ++ mrsCodec
                        ]
          Nothing -> []

      part = case _mrsPart of
               NoPartition -> []
               Partition{..} ->
                 [ "-D", "stream.num.map.output.key.fields=" ++ show keySegs
                 , "-D", "mapreduce.partition.keypartitioner.options=-k1," ++ show partSegs
                 , "-partitioner", "org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner"
                 ]


      comparator = case _mrsComparator of
                     RegularComp -> []
                     NumericComp st end rev ->
                       [ "-D", "mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator"
                       , "-D", "mapred.text.key.comparator.options=-k" <>
                               show st <> "," <> show end <> "n" <>
                               if rev then "r" else ""
                       ]

      outSep = case _mrsOutSep of
                 Nothing -> []
                 Just sep ->
                   [ "-D", "stream.reduce.output.field.separator=" ++ [sep]
                   , "-D", "mapred.textoutputformat.separator=" ++ [sep] ]
                   ++ (if _mrsNumReduce == Just 0
                       then [ "-D", "stream.map.output.field.separator=" ++ [sep]]
                       else [])


-------------------------------------------------------------------------------
-- | Check if the target file is present.
hdfsFileExists :: HadoopEnv -> FilePath -> IO Bool
hdfsFileExists HadoopEnv{..} p = do
    res <- rawSystem _hsBin ["fs", "-stat", p]
    return $ case res of
      ExitSuccess -> True
      ExitFailure{} -> False



-------------------------------------------------------------------------------
-- | Check if the target file is present.
hdfsDeletePath :: HadoopEnv -> FilePath -> IO ()
hdfsDeletePath HadoopEnv{..} p = void $
    rawSystem _hsBin ["fs", "-rmr", "-skipTrash", p]




-------------------------------------------------------------------------------
-- | List a directory's contents
hdfsLs :: HadoopEnv -> FilePath -> IO [File]
hdfsLs HadoopEnv{..} p = do
    (res,out,_) <- readProcessWithExitCode _hsBin ["fs", "-lsr", p] ""
    return $ case res of
      ExitSuccess -> parseLS p out
      ExitFailure{} -> []


-------------------------------------------------------------------------------
-- | TODO: The lcs function does not guarantee contiguous-common
-- regions, so this function may behave strangely. We should figure
-- out a way to use longest-common-prefix like semantics.
parseLS :: String -> String -> [File]
parseLS pat out = filter isOK $ map clean $ mapMaybe parseLs $ lines out
  where
    pat' = T.pack pat
    prefix = takeWhile (/= '*') pat
    isOK x = x ^. filePath . to (isPrefixOf prefix)
    clean f = f & filePath %~ (T.unpack begin <>)
        where
          shared = T.pack $ lcs pat (f ^. filePath)
          (begin, _) = T.breakOn shared pat'


-------------------------------------------------------------------------------
-- | Copy file from a location to a location
hdfsPut :: HadoopEnv -> FilePath -> FilePath -> IO ()
hdfsPut HadoopEnv{..} localPath hdfsPath = void $
    rawSystem _hsBin ["fs", "-put", localPath, hdfsPath]


-------------------------------------------------------------------------------
-- | Create a new multiple output file manager.
hdfsFanOutStream :: HadoopEnv -> FilePath -> IO FanOut
hdfsFanOutStream env@HadoopEnv{..} tmp = mkFanOut mkP
    where

      mkTmp fp = tmp </> fp ^. filename

      mkP fp = do
        (Just h, _, _, ph) <- createProcess $ (proc _hsBin ["fs", "-put", "-", mkTmp fp])
          { std_in = CreatePipe }
        hSetBuffering h LineBuffering
        let fin = do void $ waitForProcess ph
                     hdfsMkdir env (fp ^. directory)
                     void $ rawSystem _hsBin ["fs", "-mv", mkTmp fp, fp]
        return (h, fin)


-------------------------------------------------------------------------------
hdfsFanOut :: HadoopEnv -> FilePath -> IO FanOut
hdfsFanOut env@HadoopEnv{..} tmp = mkFanOut mkHandle
    where

      mkTmp fp = tmp </> (toS . Base16.encode . toS . hash . toS $ fp)

      -- write into a temp file loc until we know the stage is
      -- complete without failure
      mkHandle fp = do
        let fp' = mkTmp fp
        createDirectoryIfMissing True (fp' ^. directory)
        h <- openFile fp' AppendMode
        let fin = runResourceT $ do
              a <- register $ removeFile fp'
              liftIO $ hdfsMkdir env (fp ^. directory)
              liftIO $ hdfsPut env fp' fp
              release a
        return (h, fin)



-------------------------------------------------------------------------------
-- | Create HDFS directory if missing
hdfsMkdir :: HadoopEnv -> String -> IO ()
hdfsMkdir HadoopEnv{..} fp = void $ rawSystem _hsBin ["fs", "-mkdir", "-p", fp]


-------------------------------------------------------------------------------
-- | Apply recursive permissions to given
hdfsChmod
    :: HadoopEnv
    -> String
    -- ^ Target path
    -> String
    -- ^ Permissions string
    -> IO ExitCode
hdfsChmod HadoopEnv{..} fp mode = rawSystem _hsBin ["fs", "-chmod", "-R", mode, fp]


-------------------------------------------------------------------------------
-- | Stream data directly from HDFS using @hdfs cat@.
--
-- NOTE: It appears that this function may output a header before the file
-- contents.  Be careful!
hdfsCat :: MonadIO m => HadoopEnv -> FilePath -> Producer m ByteString
hdfsCat HadoopEnv{..} p = do
    (inH, outH, _, _) <- liftIO $ do
      let cp = (proc _hsBin ["fs", "-cat", p]) { std_in = CreatePipe
                                              , std_out = CreatePipe
                                              , std_err = Inherit }
      createProcess cp
    maybe (return ()) (liftIO . hClose) inH
    maybe exit sourceHandle outH
  where
    exit = error $ concat ["Could not open file ", p, ".  Skipping...."]



tmpRoot :: FilePath
tmpRoot = "/tmp/hadron/"


------------------------------------------------------------------------------
-- | Generates a random filename in the /tmp/hadron directory.
randomFilename :: HadoopEnv -> IO FilePath
randomFilename settings = do
    tk <- randomToken 64
    hdfsMkdir settings tmpRoot
    return $ B.unpack $ B.concat [B.pack tmpRoot, B.pack tk]


-------------------------------------------------------------------------------
-- | Copy file from HDFS to a temporary local file whose name is returned.
hdfsGet :: HadoopEnv -> FilePath -> FilePath -> IO ()
hdfsGet HadoopEnv{..} p local = do
    (res,out,e) <- readProcessWithExitCode _hsBin ["fs", "-get", p, local]  ""
    case res of
      ExitFailure i -> error $ "hdfsGet failed: " <> show i <> ".\n" <> out <> "\n" <> e
      ExitSuccess -> return ()




-------------------------------------------------------------------------------
makeLenses ''HadoopEnv
-------------------------------------------------------------------------------
