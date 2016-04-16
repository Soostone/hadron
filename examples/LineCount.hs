{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-|

Use the Basic interface to create a simple mapreduce program.

-}

module Main where

-------------------------------------------------------------------------------
import qualified Data.ByteString.Char8 as B
import           Data.Conduit
import qualified Data.Conduit.List     as C
import           Data.Default
-------------------------------------------------------------------------------
import           Hadron.Basic
-------------------------------------------------------------------------------



main :: IO ()
main = mapReduceMain def pSerialize mapper' reducer'

mapper' = linesConduit =$= C.map (\_ -> (["cnt"], (1 :: Int)))

reducer' = do
  i <- C.fold (\ acc (_, x) -> x + acc) 0
  yield $ B.pack $ show i
