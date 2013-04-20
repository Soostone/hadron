{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE ScopedTypeVariables       #-}

module Main where

-------------------------------------------------------------------------------
import           Control.Exception
import           Control.Lens
import qualified Data.ByteString.Char8  as B
import           Data.Conduit
import qualified Data.Conduit.List      as C
import           Data.CSV.Conduit
import           Data.CSV.Conduit.Types
import           Data.Default
import           Data.List
import           Data.Map               ((!))
import           Data.Monoid
import           Hadoop.Streaming
import           Safe
import           System.Environment
import           System.IO
-------------------------------------------------------------------------------



mro = MROptions (==) def pSerialize


main :: IO ()
main = mapReduceMain mro mapper' reducer'

mapper' = linesConduit =$= C.map (\_ -> (["cnt"], (1 :: Int)))

reducer' = do
  i <- C.fold (\ acc (_, x) -> x + acc) 0
  yield $ B.pack $ show i



