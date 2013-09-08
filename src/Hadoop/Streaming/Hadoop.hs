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
    ( HadoopSettings (..)
    , clouderaDemo
    , amazonEMR

    , PartitionStrategy (..)
    , numSegs

    , MRSettings (..)
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
import           Data.RNG
import qualified Data.Text             as T
import           System.Directory
import           System.Environment
import           System.Exit
import           System.IO
import           System.Process
-------------------------------------------------------------------------------


data HadoopSettings = HadoopSettings {
      hsBin :: String
    , hsJar :: String
    }


-- | Settings for the cloudera demo VM.
clouderaDemo :: HadoopSettings
clouderaDemo = HadoopSettings {
            hsBin = "hadoop"
          , hsJar = "/usr/lib/hadoop-0.20-mapreduce/contrib/streaming/hadoop-streaming-2.0.0-mr1-cdh4.2.0.jar"
          }


-- | Settings for Amazon's EMR instances.
amazonEMR :: HadoopSettings
amazonEMR = HadoopSettings {
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


data MRSettings = MRSettings {
      mrsInput     :: [String]
    , mrsOutput    :: String
    , mrsPart      :: PartitionStrategy
    , mrsNumMap    :: Maybe Int
    , mrsNumReduce :: Maybe Int
    , mrsCompress  :: Bool
    , mrsCodec     :: String
    }

instance Default MRSettings where
    def = MRSettings [] "" def Nothing Nothing False snappyCodec

-- | A simple starting point to defining 'MRSettings'
mrSettings
    :: [String]
    -- ^ Input files
    -> String
    -- ^ Output files
    -> MRSettings
mrSettings ins out = MRSettings ins out NoPartition Nothing Nothing False gzipCodec


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
    => HadoopSettings
    -> MapReduceKey
    -> MRSettings
    -> EitherT String m ()
launchMapReduce HadoopSettings{..} mrKey MRSettings{..} = do
    exec <- scriptIO getExecutablePath
    prog <- scriptIO getProgName
    lift $ $(logInfo) $ T.concat ["Launching Hadoop job for MR key: ", T.pack mrKey]

    let args = mkArgs exec prog

    lift $ $(logInfo) $ T.concat ["Hadoop arguments: ", T.pack (intercalate " " args)]

    (code, out, err) <- scriptIO $ readProcessWithExitCode hsBin args ""
    case code of
      ExitSuccess -> return ()
      e -> do
        lift $ $(logError) $ T.intercalate "\n" ["Hadoop job failed.", "StdOut:", T.pack out, "", "StdErr:", T.pack err]
        hoistEither $ Left $ "MR job failed with: " ++ show e
    where
      mkArgs exec prog =
            [ "jar", hsJar] ++
            compress ++ numMap ++ numRed ++ part ++
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
        if mrsCompress
          then [ "-D", "mapred.output.compress=true"
               , "-D", "mapred.output.compression.codec=" ++ mrsCodec
               -- , "-D", "mapred.compress.map.output=true"
               -- , "-D", "mapred.map.output.compression.codec=" ++ mrsCodec
               ]
          else []

      part = case mrsPart of
               NoPartition -> []
               Partition{..} ->
                 [ "-D", "stream.num.map.output.key.fields=" ++ show keySegs
                 , "-D", "mapred.text.key.partitioner.options=-k" ++ intercalate "," (map show [1..partSegs])
                 , "-partitioner", "org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner"
                 ]



-------------------------------------------------------------------------------
-- | Check if the target file is present.
hdfsFileExists :: HadoopSettings -> FilePath -> IO Bool
hdfsFileExists HadoopSettings{..} p = do
    res <- rawSystem hsBin ["fs", "-stat", p]
    return $ case res of
      ExitSuccess -> True
      ExitFailure{} -> False



-------------------------------------------------------------------------------
-- | Check if the target file is present.
hdfsDeletePath :: HadoopSettings -> FilePath -> IO ExitCode
hdfsDeletePath HadoopSettings{..} p =
    rawSystem hsBin ["fs", "-rmr", "-skipTrash", p]



-------------------------------------------------------------------------------
-- | Check if the target file is present.
hdfsLs :: HadoopSettings -> FilePath -> IO [FilePath]
hdfsLs HadoopSettings{..} p = do
    (res,out,_) <- readProcessWithExitCode hsBin ["fs", "-ls", p] ""
    return $ case res of
      ExitSuccess -> filter (not . null) $ map (drop 43) $ lines out
      ExitFailure{} -> []


-------------------------------------------------------------------------------
-- | Check if the target file is present.
hdfsPut :: HadoopSettings -> FilePath -> FilePath -> IO ExitCode
hdfsPut HadoopSettings{..} localPath hdfsPath =
    rawSystem hsBin ["fs", "-put", localPath, hdfsPath]


-------------------------------------------------------------------------------
-- | Stream data directly from HDFS using @hdfs cat@.
--
-- NOTE: It appears that this function may output a header before the file
-- contents.  Be careful!
hdfsCat :: MonadIO m => HadoopSettings -> FilePath -> Producer m ByteString
hdfsCat HadoopSettings{..} p = do
    (inH, outH, errH, ph) <- liftIO $ do
      let cp = (proc hsBin ["fs", "-cat", p]) { std_in = CreatePipe
                                              , std_out = CreatePipe
                                              , std_err = Inherit }
      createProcess cp
    maybe (return ()) (liftIO . hClose) inH
    maybe err sourceHandle outH
  where
    err = liftIO $
      hPutStrLn stderr $ concat ["Could not open file ", p, ".  Skipping...."]


------------------------------------------------------------------------------
-- | Generates a random filename in the /tmp/hadoop-streaming directory.
randomFilename :: IO FilePath
randomFilename = do
    tk <- mkRNG >>= randomToken 64
    return $ B.unpack $ B.concat [B.pack tmpRoot, tk]


tmpRoot :: FilePath
tmpRoot = "/tmp/hadoop-streaming/"


-------------------------------------------------------------------------------
-- | Copy file from HDFS to a temporary local file whose name is returned.
hdfsGet :: HadoopSettings -> FilePath -> IO FilePath
hdfsGet HadoopSettings{..} p = do
    tmpFile <- randomFilename
    createDirectoryIfMissing True tmpRoot
    rawSystem hsBin ["fs", "-get", p, tmpFile]
    return tmpFile


-------------------------------------------------------------------------------
-- | Copy a file down to local FS, then stream its content.
hdfsLocalStream :: MonadIO m => HadoopSettings -> FilePath -> Producer m ByteString
hdfsLocalStream set fp = do
    random <- liftIO $ hdfsGet set fp
    h <- liftIO $ openFile random ReadMode
    sourceHandle h



