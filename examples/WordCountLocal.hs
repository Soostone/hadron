{-# LANGUAGE BangPatterns              #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE TupleSections             #-}

module Main where

-------------------------------------------------------------------------------
import           Control.Category
import           Control.Lens
import qualified Data.ByteString.Char8 as B
import qualified Data.Conduit          as C
import qualified Data.Conduit.List     as C
import           Data.CSV.Conduit
import           Data.Default
import           Prelude               hiding (id, (.))
-------------------------------------------------------------------------------
import           Hadron.Controller
-------------------------------------------------------------------------------


main :: IO ()
main = hadoopMain [("app", app)] (LocalRun def) RSReRun


-- notice how path is a file
source :: CSV B.ByteString a => Tap a
source = tap "data/sample.csv" (csvProtocol def)


-- notice how path is a folder
target :: CSV B.ByteString a => Tap a
target = tap "data/wordFrequency" (csvProtocol def)


truncated :: CSV B.ByteString a => Tap a
truncated = tap "data/truncated.csv" (csvProtocol def)


-- notice how output is a file
wordCountTarget :: CSV B.ByteString a => Tap a
wordCountTarget = tap "data/wordCount.csv" (csvProtocol def)


mr1 :: MapReduce (Row B.ByteString) (Row B.ByteString)
mr1 = MapReduce def pSerialize mapper' Nothing (Left reducer')


-------------------------------------------------------------------------------
mapper':: Mapper (Row B.ByteString) B.ByteString Int
mapper' = C.concatMap (map (\w -> (w, 1 :: Int)) . concatMap B.words)


reducer' :: Reducer B.ByteString Int (Row B.ByteString)
reducer'  = do
  (!w, !cnt) <- C.fold (\ (_, !cnt) (k, !x) -> (k, cnt + x)) ("", 0)
  C.yield [w, B.pack . show $ cnt]


-------------------------------------------------------------------------------
-- | Count the number of words in mr1 output
mr2 :: MapReduce (Row B.ByteString) (Row B.ByteString)
mr2 = MapReduce def pSerialize mapper Nothing (Left r)
    where
      mapper :: Mapper (Row B.ByteString) String Int
      mapper = C.map (const $ ("count", 1))

      r :: Reducer (String) Int (Row B.ByteString)
      r = do
          cnt <- C.fold (\ !m (_, !i) -> m + i) 0
          C.yield ["Total Count", (B.pack . show) cnt]


mr3 :: MapReduce (Row B.ByteString) (Row B.ByteString)
mr3 = MapReduce opts pSerialize mapper Nothing r
  where
    opts = def & mroNumReduce .~ Just 0

    mapper = C.map (\ v -> ((), map (B.take 5) v) )

    r = Right (C.map id)



app :: Controller ()
app = do
    let src = source
    connect mr1 [src] target (Just "Counting word frequency")
    connect mr2 [target] wordCountTarget (Just "Counting words")
    connect mr3 [target] truncated (Just "Truncating all fields")
