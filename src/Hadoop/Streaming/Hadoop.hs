{-# LANGUAGE RecordWildCards #-}
-----------------------------------------------------------------------------
-- |
-- Module      :
-- Copyright   :  Soostone Inc
-- License     :  BSD3
--
-- Maintainer  :  Ozgun Ataman
-- Stability   :  experimental
--
-- Deal with the hadoop command line program.
----------------------------------------------------------------------------


module Hadoop.Streaming.Hadoop where

-------------------------------------------------------------------------------
import           Control.Error
import           Control.Monad.Trans
import           Data.Default
import           Data.List
import           System.Environment
import           System.Exit
import           System.Process
-------------------------------------------------------------------------------



data HadoopSettings = HadoopSettings {
      hsBin :: String
    , hsJar :: String
    }



instance Default HadoopSettings where
    def = HadoopSettings {
            hsBin = "hadoop"
          , hsJar = "/usr/lib/hadoop-0.20-mapreduce/contrib/streaming/hadoop-streaming-2.0.0-mr1-cdh4.2.0.jar"
          }


data PartitionStrategy
    = NoPartition
    | Partition {
        keySegs  :: Int
      -- ^ Total segments comprising a key
      , partSegs :: Int
      -- ^ First N key segments used for partitioning
      }

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
    }


-- | A simple starting point to defining 'MRSettings'
mrSettings
    :: [String]
    -- ^ Input files
    -> String
    -- ^ Output files
    -> MRSettings
mrSettings ins out = MRSettings ins out NoPartition Nothing Nothing


type MapReduceKey = String



-------------------------------------------------------------------------------
launchMapReduce
    :: MonadIO m
    => HadoopSettings
    -> MapReduceKey
    -> MRSettings
    -> EitherT String m ()
launchMapReduce HadoopSettings{..} mrKey MRSettings{..} = do
    exec <- scriptIO getExecutablePath
    prog <- scriptIO getProgName
    (code, out, err) <- scriptIO $ readProcessWithExitCode hsBin (args exec prog) ""
    case code of
      ExitSuccess -> return ()
      err -> hoistEither $ Left $ "MR job failed with: " ++ show err
    where
      args exec prog =
            [ "jar", hsJar
            , "-output", mrsOutput
            , "-mapper", "\"" ++ prog ++ " map_" ++ mrKey ++ "\""
            , "-reducer", "\"" ++ prog ++ " reduce_" ++ mrKey ++ "\""
            , "-file", exec
            ] ++ inputs ++ part ++ numMap ++ numRed

      inputs = concatMap mkInput mrsInput
      mkInput i = ["-input", i]

      numMap = maybe [] (\x -> ["-D", "mapred.map.tasks=" ++ show x]) mrsNumMap
      numRed = maybe [] (\x -> ["-D", "mapred.reduce.tasks=" ++ show x]) mrsNumReduce

      part = case mrsPart of
               NoPartition -> []
               Partition{..} ->
                 [ "-D", "stream.num.map.output.key.fields=" ++ show keySegs
                 , "-D", "mapred.text.key.partitioner.options=-k1,2" ++ intercalate "," (map show [1..partSegs])
                 , "-partitioner", "org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner"
                 ]







