{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE TemplateHaskell   #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Hadoop.Streaming.Hadoop
-- Copyright   :  Soostone Inc
-- License     :  BSD3
--
-- Maintainer  :  Ozgun Ataman
-- Stability   :  experimental
--
-- Deal with the hadoop command line program.
----------------------------------------------------------------------------


module Hadoop.Streaming.Hadoop
    ( HadoopEnv (..)
    , clouderaDemo
    , amazonEMR

    , PartitionStrategy (..)
    , numSegs

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
    ) where

-------------------------------------------------------------------------------
import           Control.Error
import           Control.Monad.Logger
import           Control.Monad.Trans
import           Data.ByteString.Char8 (ByteString)
import qualified Data.ByteString.Char8 as B
import           Data.Conduit
import           Data.Conduit.Binary   (sourceHandle)
import           Data.Default
import           Data.List
import           Data.List
import           Data.RNG
import qualified Data.Text             as T
import           System.Directory
import           System.Environment
import           System.Exit
import           System.IO
import           System.Process
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

instance Default PartitionStrategy where
    def = NoPartition

numSegs NoPartition = 1
numSegs Partition{..} = keySegs



data HadoopRunOpts = HadoopRunOpts {
      mrsInput     :: [String]
    , mrsOutput    :: String
    , mrsPart      :: PartitionStrategy
    , mrsNumMap    :: Maybe Int
    , mrsNumReduce :: Maybe Int
    , mrsCompress  :: Maybe String
    , mrsOutSep    :: Maybe Char
    -- ^ A separator to be used in reduce output. It is sometimes
    -- useful to specify one to trick Hadoop.
    }


instance Default HadoopRunOpts where
    def = HadoopRunOpts [] "" def Nothing Nothing Nothing Nothing

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


-------------------------------------------------------------------------------
launchMapReduce
    :: (MonadIO m, MonadLogger m)
    => HadoopEnv
    -> MapReduceKey
    -> HadoopRunOpts
    -> EitherT String m ()
launchMapReduce HadoopEnv{..} mrKey HadoopRunOpts{..} = do
    exec <- scriptIO getExecutablePath
    prog <- scriptIO getProgName
    lift $ $(logInfo) $ T.concat ["Launching Hadoop job for MR key: ", T.pack mrKey]

    let args = mkArgs exec prog

    lift $ $(logInfo) $ T.concat ["Hadoop arguments: ", T.pack (intercalate " " args)]

    (code, out, eout) <- scriptIO $ readProcessWithExitCode hsBin args ""
    case code of
      ExitSuccess -> return ()
      e -> do
        lift $ $(logError) $ T.intercalate "\n" ["Hadoop job failed.", "StdOut:", T.pack out, "", "StdErr:", T.pack eout]
        hoistEither $ Left $ "MR job failed with: " ++ show e
    where
      mkArgs exec prog =
            [ "jar", hsJar] ++
            compress ++ numMap ++ numRed ++ part ++ outSep ++
            inputs ++
            [ "-output", mrsOutput
            , "-mapper", "\"" ++ prog ++ " map_" ++ mrKey ++ "\""
            , "-reducer", "\"" ++ prog ++ " reduce_" ++ mrKey ++ "\""
            , "-file", exec
            ]

      inputs = concatMap mkInput mrsInput
      mkInput i = ["-input", i]

      numMap = maybe [] (\x -> ["-D", "mapred.map.tasks=" ++ show x]) mrsNumMap
      numRed = maybe [] (\x -> ["-D", "mapred.reduce.tasks=" ++ show x]) mrsNumReduce

      compress =
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
                 , "-D", "mapred.text.key.partitioner.options=-k" ++ intercalate "," (map show [1..partSegs])
                 , "-partitioner", "org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner"
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
    (res,out,_) <- readProcessWithExitCode hsBin ["fs", "-ls", p] ""
    return $ case res of
      ExitSuccess -> filter isOK $ map clean $ lines out
      ExitFailure{} -> []
  where
    prefix = takeWhile (/= '*') p
    isOK x = isPrefixOf prefix x
    clean xs = last (words xs)


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
    rawSystem hsBin ["fs", "-get", p, tmpFile]
    return tmpFile


-------------------------------------------------------------------------------
-- | Copy a file down to local FS, then stream its content.
hdfsLocalStream :: MonadIO m => HadoopEnv -> FilePath -> Producer m ByteString
hdfsLocalStream set fp = do
    random <- liftIO $ hdfsGet set fp
    h <- liftIO $ openFile random ReadMode
    sourceHandle h
    liftIO $ hClose h
    liftIO $ removeFile random



