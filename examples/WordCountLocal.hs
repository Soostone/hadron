{-# LANGUAGE BangPatterns              #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE TupleSections             #-}

module Main where

-------------------------------------------------------------------------------
import qualified Data.ByteString.Char8 as B
import           Data.Conduit
import qualified Data.Conduit.List     as C
import           Data.CSV.Conduit
import           Data.Default
-------------------------------------------------------------------------------
import           Hadron.Controller
-------------------------------------------------------------------------------


main :: IO ()
main = hadoopMain app (LocalRun def) RSReRun


-- notice how path is a file
source = tap "data/sample.csv" (csvProtocol def)


-- notice how path is a folder
target = tap "data/wordFrequency" (csvProtocol def)


-- notice how output is a file
wordCountTarget = tap "data/wordCount.csv" (csvProtocol def)


mr1 :: MapReduce (Row B.ByteString) (Row B.ByteString)
mr1 = MapReduce def pSerialize mapper' Nothing reducer'


mapper':: Monad m => Conduit (Row B.ByteString) m (CompositeKey, Int)
mapper' = C.concatMap f
    where
      f ln = concatMap (map (\w -> ([w], 1 :: Int)) . B.words) ln


reducer' :: Reducer CompositeKey Int (Row B.ByteString)
reducer' = do
  (!w, !cnt) <- C.fold (\ (_, !cnt) ([k], !x) -> (k, cnt + x)) ("", 0)
  yield $ [w, B.pack . show $ cnt]


app :: Controller ()
app = do
    connect mr1 [source] target (Just "Counting word frequency")
    connect mr2 [target] wordCountTarget (Just "Counting words")


-------------------------------------------------------------------------------
-- | Count the number of words in mr1 output
mr2 :: MapReduce (Row B.ByteString) (Row B.ByteString)
mr2 = MapReduce def pSerialize m Nothing r
    where
      m :: Mapper (Row B.ByteString) (SingleKey String) Int
      m = C.map (const $ (SingleKey "count", 1))

      r :: Reducer (SingleKey String) Int (Row B.ByteString)
      r = do
          cnt <- C.fold (\ !m (_, !i) -> m + i) 0
          yield $ ["Total Count", (B.pack . show) cnt]

