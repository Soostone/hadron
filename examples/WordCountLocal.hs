{-# LANGUAGE BangPatterns              #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE TupleSections             #-}

module Main where

-------------------------------------------------------------------------------

import           Control.Category
import           Control.Lens
import           Control.Monad
import qualified Data.ByteString.Char8 as B
import           Data.CSV.Conduit
import           Data.Default
import           Hadron.Streams
import           Prelude               hiding (id, (.))
import qualified System.IO.Streams     as S
-------------------------------------------------------------------------------
import           Hadron.Controller
-------------------------------------------------------------------------------


main :: IO ()
main = hadoopMain app (LocalRun def) RSReRun


-- notice how path is a file
source = do
    t <- binaryDirTap "data" (== "data/sample.csv")
    return $ t & proto %~ (csvProtocol def . )


-- notice how path is a folder
target = tap "data/wordFrequency" (csvProtocol def)

truncated = tap "data/truncated.csv" (csvProtocol def)

-- notice how output is a file
wordCountTarget = tap "data/wordCount.csv" (csvProtocol def)


mr1 :: MapReduce (Row B.ByteString) (Row B.ByteString)
mr1 = MapReduce def pSerialize mapper' Nothing (Left reducer')


-------------------------------------------------------------------------------
mapper':: Mapper (Row B.ByteString) B.ByteString Int
mapper' is os = bindStream is go >>= S.connectTo os
  where
    go ln = S.fromList $
            concatMap (map (\w -> (w, 1 :: Int)) . B.words) ln


reducer' :: Reducer B.ByteString Int (Row B.ByteString)
reducer' is os = do
  (!w, !cnt) <- S.fold (\ (_, !cnt) (k, !x) -> (k, cnt + x)) ("", 0) is
  S.write (Just [w, B.pack . show $ cnt]) os


-------------------------------------------------------------------------------
-- | Count the number of words in mr1 output
mr2 :: MapReduce (Row B.ByteString) (Row B.ByteString)
mr2 = MapReduce def pSerialize m Nothing (Left r)
    where
      m :: Mapper (Row B.ByteString) String Int
      m is os = do
        is' <- S.map (const $ ("count", 1)) is
        S.connect is' os

      r :: Reducer (String) Int (Row B.ByteString)
      r is os = do
          cnt <- S.fold (\ !m (_, !i) -> m + i) 0 is
          S.write (Just ["Total Count", (B.pack . show) cnt]) os


mr3 :: MapReduce (Row B.ByteString) (Row B.ByteString)
mr3 = MapReduce opts pSerialize m Nothing r
  where
    opts = def & mroNumReduce .~ Just 0

    m is os = do
      is' <- S.map (\ v -> ((), map (B.take 5) v) ) is
      S.connect is' os

    r = Right (S.contramap id)



app :: Controller ()
app = do
    src <- source
    connect mr1 [src] target (Just "Counting word frequency")
    connect mr2 [target] wordCountTarget (Just "Counting words")
    connect mr3 [target] truncated (Just "Truncating all fields")


