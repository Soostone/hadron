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

    , Codec
    , gzipCodec
    , snappyCodec

    -- * Hadoop Command Line Wrappers
    , hadoopMapReduce
    , hdfsFileExists
    , hdfsDeletePath
    , hdfsLs
    , hdfsPut
    , hdfsMkdir
    , tmpRoot
    , hdfsCat
    , hdfsGet
    , hdfsChmod
    , randomFilename

    ) where

-------------------------------------------------------------------------------
import           Control.Error
import           Control.Lens
import           Control.Monad
import           Control.Monad.Trans
import           Data.ByteString.Char8       (ByteString)
import qualified Data.ByteString.Char8       as B
import           Data.Conduit
import           Data.Conduit.Binary         (sourceHandle)
import           Data.Default
import           Data.List
import           Data.List.LCS.HuntSzymanski
import           Data.Monoid
import           Data.RNG
import qualified Data.Text                   as T
import           System.Environment
import           System.Exit
import           System.IO
import           System.Process
-------------------------------------------------------------------------------
import           Hadron.Logger
-------------------------------------------------------------------------------


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
      -- ^ Total segments comprising a key
      , partSegs :: Int
      -- ^ First N key segments used for partitioning.
      }
    -- ^ Expect a composite key emitted form the 'Mapper'.


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


data HadoopRunOpts = HadoopRunOpts {
      mrsInput      :: [String]
    , mrsOutput     :: String
    , mrsPart       :: PartitionStrategy
    , mrsNumMap     :: Maybe Int
    , mrsNumReduce  :: Maybe Int
    , mrsCombine    :: Bool
    , mrsCompress   :: Maybe Codec
    , mrsOutSep     :: Maybe Char
    -- ^ A separator to be used in reduce output. It is sometimes
    -- useful to specify one to trick Hadoop.
    , mrsJobName    :: Maybe String
    , mrsComparator :: Comparator
    }


instance Default HadoopRunOpts where
    def = HadoopRunOpts [] "" def Nothing Nothing False Nothing Nothing Nothing def

-- | A simple starting point to defining 'HadoopRunOpts'
mrSettings
    :: [String]
    -- ^ Input files
    -> String
    -- ^ Output files
    -> HadoopRunOpts
mrSettings ins out = def { mrsInput = ins, mrsOutput = out }


type Codec = String

-------------------------------------------------------------------------------
gzipCodec :: Codec
gzipCodec = "org.apache.hadoop.io.compress.GzipCodec"

snappyCodec :: Codec
snappyCodec = "org.apache.hadoop.io.compress.SnappyCodec"


type MapReduceKey = String
type RunToken = String


-------------------------------------------------------------------------------
hadoopMapReduce
    :: (MonadIO m)
    => HadoopEnv
    -> MapReduceKey
    -> RunToken
    -> HadoopRunOpts
    -> EitherT String m ()
hadoopMapReduce HadoopEnv{..} mrKey runToken HadoopRunOpts{..} = do
    exec <- scriptIO getExecutablePath
    prog <- scriptIO getProgName
    liftIO $ infoM "Hadron.Hadoop" $ "Launching Hadoop job for MR key: " <> mrKey

    let args = mkArgs exec prog

    liftIO . infoM "Hadron.Hadoop" $ "Hadoop arguments: " <> (intercalate " " args)

    (code, out, eout) <- scriptIO $ readProcessWithExitCode _hsBin args ""
    case code of
      ExitSuccess -> return ()
      e -> do
        liftIO . errorM "Hadron.Hadoop" $ intercalate "\n"
          [ "Hadoop job failed.", "StdOut:"
          , out, "", "StdErr:", eout]
        hoistEither $ Left $ "MR job failed with: " ++ show e
    where
      mkArgs exec prog =
            [ "jar", _hsJar] ++
            comp ++ numMap ++ numRed ++ outSep ++ jobName ++
            comparator ++ part ++
            inputs ++
            [ "-output", mrsOutput] ++
            mkStage prog "mapper" ++
            mkStage prog "reducer" ++
            if mrsCombine then mkStage prog "combiner" else [] ++
            [ "-file", exec ]


      mkStage prog stage =
          [ "-" ++ stage
          , "\"" ++ prog ++ " " ++ runToken ++ " " ++ stage ++ "_" ++ mrKey ++ "\""
          ]

      jobName = maybe [] (\nm -> ["-D", "mapred.job.name=\"" <> nm <>"\""])
                mrsJobName


      inputs = concatMap mkInput mrsInput
      mkInput i = ["-input", i]

      numMap = maybe [] (\x -> ["-D", "mapred.map.tasks=" ++ show x]) mrsNumMap
      numRed = maybe [] (\x -> ["-D", "mapred.reduce.tasks=" ++ show x]) mrsNumReduce

      comp =
        case mrsCompress of
          Just codec -> [ "-D", "mapred.output.compress=true"
                        , "-D", "mapred.output.compression.codec=" ++ codec
                        -- , "-D", "mapred.compress.map.output=true"
                        -- , "-D", "mapred.map.output.compression.codec=" ++ mrsCodec
                        ]
          Nothing -> []

      part = case mrsPart of
               NoPartition -> []
               Partition{..} ->
                 [ "-D", "stream.num.map.output.key.fields=" ++ show keySegs
                 , "-D", "mapred.text.key.partitioner.options=-k1," ++ show partSegs
                 , "-partitioner", "org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner"
                 ]


      comparator = case mrsComparator of
                     RegularComp -> []
                     NumericComp st end rev ->
                       [ "-D", "mapred.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator"
                       , "-D", "mapred.text.key.comparator.options=-k" <>
                               show st <> "," <> show end <> "n" <>
                               if rev then "r" else ""
                       ]

      outSep = case mrsOutSep of
                 Nothing -> []
                 Just sep -> [ "-D", "stream.reduce.output.field.separator=" ++ [sep]
                             , "-D", "mapred.textoutputformat.separator=" ++ [sep]
                             ]


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
hdfsLs :: HadoopEnv -> FilePath -> IO [FilePath]
hdfsLs HadoopEnv{..} p = do
    (res,out,_) <- readProcessWithExitCode _hsBin ["fs", "-lsr", p] ""
    return $ case res of
      ExitSuccess -> parseLS p out
      ExitFailure{} -> []


-------------------------------------------------------------------------------
-- | TODO: The lcs function does not guarantee contiguous-common
-- regions, so this function may behave strangely. We should figure
-- out a way to use longest-common-prefix like semantics.
parseLS :: [Char] -> String -> [[Char]]
parseLS pat out = filter isOK $ map clean $ lines out
  where
    pat' = T.pack pat
    prefix = takeWhile (/= '*') pat
    isOK x = isPrefixOf prefix x
    clean x = T.unpack $ begin `T.append` T.pack path
        where
          path = last (words x)
          shared = T.pack $ lcs pat path
          (begin, _) = T.breakOn shared pat'


-------------------------------------------------------------------------------
-- | Copy file from a location to a location
hdfsPut :: HadoopEnv -> FilePath -> FilePath -> IO ()
hdfsPut HadoopEnv{..} localPath hdfsPath = void $
    rawSystem _hsBin ["fs", "-put", localPath, hdfsPath]



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
    exit = liftIO $
      hPutStrLn stderr $ concat ["Could not open file ", p, ".  Skipping...."]



tmpRoot :: FilePath
tmpRoot = "/tmp/hadron/"


------------------------------------------------------------------------------
-- | Generates a random filename in the /tmp/hadron directory.
randomFilename :: HadoopEnv -> IO FilePath
randomFilename settings = do
    tk <- mkRNG >>= randomToken 64
    hdfsMkdir settings tmpRoot
    return $ B.unpack $ B.concat [B.pack tmpRoot, tk]


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
