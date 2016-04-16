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
import           Hadron.Basic
-------------------------------------------------------------------------------


main :: IO ()
main = mapReduceMain def pSerialize mapper' reducer'

mapper':: Mapper B.ByteString CompositeKey Int
mapper' = linesConduit =$= C.concatMap f
    where
      f ln = map (\w -> ([w], 1 :: Int)) $ B.words ln

reducer':: Reducer CompositeKey Int B.ByteString
reducer' = do
  (w, cnt) <- C.fold (\ (_, cnt) ([k], x) -> (k, cnt + x)) ("", 0)
  yield $ B.concat [rowToStr def [w, B.pack . show $ cnt], "\n"]
