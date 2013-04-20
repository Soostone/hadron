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
import           Hadoop.Streaming
-------------------------------------------------------------------------------

main :: IO ()
main = mapReduceMain defMRO mapper' reducer'

mapper' = linesConduit =$= C.concatMap f
    where
      f ln = map (\w -> ([w], 1)) $ B.words ln

reducer' :: Monad m => Reducer Int m B.ByteString
reducer' = do
  (w, cnt) <- C.fold (\ (_, cnt) ([k], x) -> (k, cnt + x)) ("", 0)
  yield $ B.concat [rowToStr def [w, B.pack . show $ cnt], "\n"]



