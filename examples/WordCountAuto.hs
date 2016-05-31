{-# LANGUAGE BangPatterns              #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE TupleSections             #-}

module Main where

-------------------------------------------------------------------------------
import qualified Data.ByteString.Char8       as B
import           Data.Conduit ((=$=), yield)
import qualified Data.Conduit.List           as C
import           Data.CSV.Conduit
import           Data.Default
-------------------------------------------------------------------------------
import           Hadron.Controller
-------------------------------------------------------------------------------

main :: IO ()
main = hadoopMain [("count", app)] (HadoopRun clouderaDemo def) RSReRun


source :: Tap B.ByteString
source = tap "hdfs://localhost/user/cloudera/full_meta_4.csv.gz" idProtocol

target :: CSV B.ByteString a => Tap a
target = tap "hdfs://localhost/user/cloudera/wcOut1" (csvProtocol def)


mr1 :: MapReduce B.ByteString (Row B.ByteString)
mr1 = MapReduce def pSerialize mapper' Nothing (Left reducer')


mapper' :: Mapper B.ByteString CompositeKey Int
mapper' = intoCSV def =$= C.concatMap f
    where
      f :: [B.ByteString] -> [([B.ByteString], Int)]
      f ln = concatMap (map (\w -> ([w], 1 :: Int)) . B.words) ln


reducer' :: Reducer CompositeKey Int (Row B.ByteString)
reducer' = do
  (!w, !cnt) <- C.fold (\ (_, !cnt) ([k], !x) -> (k, cnt + x)) ("", 0)
  yield $ [w, B.pack . show $ cnt]


app :: Controller ()
app = connect mr1 [source] target (Just "Counting words")
