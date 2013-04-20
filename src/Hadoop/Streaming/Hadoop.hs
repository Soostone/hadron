{-# LANGUAGE OverloadedStrings #-}
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

    -- * Hadoop Command Line Wrappers
    , launchMapReduce
    , hdfsFileExists
    , hdfsDeletePath
    ) where

-------------------------------------------------------------------------------
import           Control.Error
import           Control.Monad.Logger
import           Control.Monad.Trans
import           Data.Default
import           Data.List
import qualified Data.Text            as T
import           System.Environment
import           System.Exit
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


instance Default HadoopSettings where
    def = HadoopSettings {
            hsBin = "hadoop"
          , hsJar = "/usr/lib/hadoop-0.20-mapreduce/contrib/streaming/hadoop-streaming-2.0.0-mr1-cdh4.2.0.jar"
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

snappyCodec :: String
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
hdfsFileExists :: HadoopSettings -> String -> IO Bool
hdfsFileExists HadoopSettings{..} p = do
    res <- rawSystem hsBin ["fs", "-stat", p]
    return $ case res of
      ExitSuccess -> True
      ExitFailure{} -> False



-------------------------------------------------------------------------------
-- | Check if the target file is present.
hdfsDeletePath :: HadoopSettings -> String -> IO ExitCode
hdfsDeletePath HadoopSettings{..} p =
    rawSystem hsBin ["fs", "-rmr", "-skipTrash", p]






