{-# LANGUAGE BangPatterns              #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE TupleSections             #-}

module Main where

-------------------------------------------------------------------------------
import qualified Data.ByteString.Char8       as B
import           Data.Conduit
import qualified Data.Conduit.List           as C
import           Data.CSV.Conduit
import           Data.Default
import           System.IO
-------------------------------------------------------------------------------
import           Hadoop.Streaming
import           Hadoop.Streaming.Controller
import           Hadoop.Streaming.Logger
-------------------------------------------------------------------------------

main :: IO ()
main = do
    h <- openFile "hadoop.log" AppendMode
    logTo h $ hadoopMain app def def RSReRun


source = tap "hdfs://localhost/user/cloudera/full_meta_4.csv.gz" idProtocol

target = tap "hdfs://localhost/user/cloudera/wcOut1" (csvProtocol def)


mr1 :: MapReduce B.ByteString IO (Row B.ByteString)
mr1 = MapReduce mro mapper' reducer'


mro = MROptions (==) def pSerialize


mapper' = intoCSV def =$= C.concatMap f
    where
      f ln = concatMap (map (\w -> ([w], 1)) . B.words) ln


reducer' :: Monad m => Reducer Int m (Row B.ByteString)
reducer' = do
  (!w, !cnt) <- C.fold (\ (_, !cnt) ([k], !x) -> (k, cnt + x)) ("", 0)
  yield $ [w, B.pack . show $ cnt]



app = connect mr1 [source] target


