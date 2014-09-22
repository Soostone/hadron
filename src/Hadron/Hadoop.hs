{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE TemplateHaskell       #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Hadron.Hadoop
-- Copyright   :  Soostone Inc
-- License     :  BSD3
--
-- Maintainer  :  Ozgun Ataman
-- Stability   :  experimental
--
-- Deal with the hadoop command line program.
----------------------------------------------------------------------------


module Hadron.Hadoop
    ( HadoopEnv (..)
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
    , randomFilename

    -- * Hadoop Command Line Wrappers
    , launchMapReduce
    , hdfsFileExists
    , hdfsDeletePath
    , hdfsLs
    , hdfsPut
    , hdfsMkdir
    , tmpRoot
    , hdfsCat
    , hdfsGet
    , hdfsLocalStream
    , hdfsLocalStreamMulti
    , hdfsChmod
    ) where

-------------------------------------------------------------------------------
import           Control.Concurrent.Async
import           Control.Error
import           Control.Exception.Lens
import           Control.Lens
import           Control.Monad
import           Control.Monad.Base
import           Control.Monad.Catch
import           Control.Monad.Primitive
import           Control.Monad.Trans
import           Data.ByteString.Char8       (ByteString)
import qualified Data.ByteString.Char8       as B
import           Data.Conduit
import           Data.Conduit.Binary         (sourceHandle)
import           Data.Conduit.Zlib
import           Data.Default
import           Data.List
import           Data.List.LCS.HuntSzymanski
import           Data.Monoid
import           Data.RNG
import qualified Data.Text                   as T
import           System.Directory
import           System.Environment
import           System.Exit
import           System.IO
import           System.Process
-------------------------------------------------------------------------------
import           Hadron.Logger
-------------------------------------------------------------------------------


data HadoopEnv = HadoopEnv {
      hsBin :: String
    , hsJar :: String
    }


-- | Settings for the cloudera demo VM.
clouderaDemo :: HadoopEnv
clouderaDemo = HadoopEnv {
            hsBin = "hadoop"
          , hsJar = "/usr/lib/hadoop-0.20-mapreduce/contrib/streaming/hadoop-streaming-2.0.0-mr1-cdh4.2.0.jar"
          }


-- | Settings for Amazon's EMR instances.
amazonEMR :: HadoopEnv
amazonEMR = HadoopEnv {
              hsBin = "/home/hadoop/bin/hadoop"
            , hsJar = "/home/hadoop/contrib/streaming/hadoop-streaming.jar"
            }


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
    -- ^ Numeric sorting spoanning fields i to j, True=reversed

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
    , mrsCompress   :: Maybe String
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
launchMapReduce
    :: (MonadIO m)
    => HadoopEnv
    -> MapReduceKey
    -> RunToken
    -> HadoopRunOpts
    -> EitherT String m ()
launchMapReduce HadoopEnv{..} mrKey runToken HadoopRunOpts{..} = do
    exec <- scriptIO getExecutablePath
    prog <- scriptIO getProgName
    liftIO $ infoM "Hadron.Hadoop" $ "Launching Hadoop job for MR key: " <> mrKey

    let args = mkArgs exec prog

    liftIO . infoM "Hadron.Hadoop" $ "Hadoop arguments: " <> (intercalate " " args)

    (code, out, eout) <- scriptIO $ readProcessWithExitCode hsBin args ""
    case code of
      ExitSuccess -> return ()
      e -> do
        liftIO . errorM "Hadron.Hadoop" $ intercalate "\n"
          [ "Hadoop job failed.", "StdOut:"
          , out, "", "StdErr:", eout]
        hoistEither $ Left $ "MR job failed with: " ++ show e
    where
      mkArgs exec prog =
            [ "jar", hsJar] ++
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
    res <- rawSystem hsBin ["fs", "-stat", p]
    return $ case res of
      ExitSuccess -> True
      ExitFailure{} -> False



-------------------------------------------------------------------------------
-- | Check if the target file is present.
hdfsDeletePath :: HadoopEnv -> FilePath -> IO ExitCode
hdfsDeletePath HadoopEnv{..} p =
    rawSystem hsBin ["fs", "-rmr", "-skipTrash", p]



-------------------------------------------------------------------------------
-- | Check if the target file is present.
hdfsLs :: HadoopEnv -> FilePath -> IO [FilePath]
hdfsLs HadoopEnv{..} p = do
    (res,out,_) <- readProcessWithExitCode hsBin ["fs", "-lsr", p] ""
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
-- | Check if the target file is present.
hdfsPut :: HadoopEnv -> FilePath -> FilePath -> IO ExitCode
hdfsPut HadoopEnv{..} localPath hdfsPath =
    rawSystem hsBin ["fs", "-put", localPath, hdfsPath]



-------------------------------------------------------------------------------
-- | Create HDFS directory if missing
hdfsMkdir :: HadoopEnv -> String -> IO ExitCode
hdfsMkdir HadoopEnv{..} fp = rawSystem hsBin ["fs", "-mkdir", "-p", fp]


-------------------------------------------------------------------------------
-- | Apply recursive permissions to given
hdfsChmod
    :: HadoopEnv
    -> String
    -- ^ Target path
    -> String
    -- ^ Permissions string
    -> IO ExitCode
hdfsChmod HadoopEnv{..} fp mode = rawSystem hsBin ["fs", "-chmod", "-R", mode, fp]


-------------------------------------------------------------------------------
-- | Stream data directly from HDFS using @hdfs cat@.
--
-- NOTE: It appears that this function may output a header before the file
-- contents.  Be careful!
hdfsCat :: MonadIO m => HadoopEnv -> FilePath -> Producer m ByteString
hdfsCat HadoopEnv{..} p = do
    (inH, outH, _, _) <- liftIO $ do
      let cp = (proc hsBin ["fs", "-cat", p]) { std_in = CreatePipe
                                              , std_out = CreatePipe
                                              , std_err = Inherit }
      createProcess cp
    maybe (return ()) (liftIO . hClose) inH
    maybe exit sourceHandle outH
  where
    exit = liftIO $
      hPutStrLn stderr $ concat ["Could not open file ", p, ".  Skipping...."]


------------------------------------------------------------------------------
-- | Generates a random filename in the /tmp/hadron directory.
randomFilename :: IO FilePath
randomFilename = do
    tk <- mkRNG >>= randomToken 64
    return $ B.unpack $ B.concat [B.pack tmpRoot, tk]


tmpRoot :: FilePath
tmpRoot = "/tmp/hadron/"


-------------------------------------------------------------------------------
-- | Copy file from HDFS to a temporary local file whose name is returned.
hdfsGet :: HadoopEnv -> FilePath -> IO FilePath
hdfsGet HadoopEnv{..} p = do
    tmpFile <- randomFilename
    createDirectoryIfMissing True tmpRoot
    -- rawSystem hsBin ["chmod", "a+rw", tmpRoot]
    (res,out,e) <- readProcessWithExitCode hsBin ["fs", "-get", p, tmpFile]  ""
    case res of
      ExitFailure i -> error $ "hdfsGet failed: " <> show i <> ".\n" <> out <> "\n" <> e
      ExitSuccess -> return tmpFile


-------------------------------------------------------------------------------
-- | Copy a file down to local FS, then stream its content.
hdfsLocalStream :: MonadIO m => HadoopEnv -> FilePath -> Producer m ByteString
hdfsLocalStream hs fp = do
    random <- liftIO $ hdfsGet hs fp
    h <- liftIO $ catching _IOException
           (openFile random ReadMode)
           (\e ->  error $ "hdfsLocalStream failed with open file: " <> show e)
    sourceHandle h
    liftIO $ hClose h
    liftIO $ removeFile random


-------------------------------------------------------------------------------
-- | Stream contents of a folder one by one from HDFS.
hdfsLocalStreamMulti
    :: (MonadIO m, MonadThrow m, MonadBase base m, PrimMonad base)
    => HadoopEnv
    -> FilePath
    -- ^ Location / glob pattern
    -> (FilePath -> Bool)
    -- ^ Fire filter based on name
    -> Source m ByteString
hdfsLocalStreamMulti hs loc chk = do
    fs <- liftIO $ hdfsLs hs loc <&> filter chk
    lfs <- liftIO $ mapConcurrently (hdfsGet hs) fs
    forM_ (zip lfs fs) $ \ (local, fp) -> do
        h <- liftIO $ catching _IOException
             (openFile local ReadMode)
             (\e ->  error $ "hdfsLocalStream failed with open file: " <> show e)
        let getFile = sourceHandle h
        if isSuffixOf "gz" fp
          then getFile =$= ungzip
          else getFile
        liftIO $ do
          hClose h
          removeFile local


