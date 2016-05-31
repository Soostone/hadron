{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE ScopedTypeVariables       #-}
{-|

Use the Basic interface to create a simple mapreduce program.

-}

module Main where

-------------------------------------------------------------------------------
import           Control.Monad.Catch
import qualified Data.ByteString.Char8 as B
import           Data.Conduit
import qualified Data.Conduit.List     as C
import           Data.Default
import           Data.String
-------------------------------------------------------------------------------
import           Hadron.Basic
-------------------------------------------------------------------------------



main :: IO ()
main = mapReduceMain def pSerialize mapper' reducer'


mapper'
    :: (IsString t, MonadThrow m)
    => ConduitM B.ByteString ([t], Int) m ()
mapper' = linesConduit =$= C.map (\_ -> (["cnt"], (1 :: Int)))

reducer'
    :: (Monad m, Num a, Show a)
    => ConduitM (t, a) B.ByteString m ()
reducer' = do
  i <- C.fold (\ acc (_, x) -> x + acc) 0
  yield $ B.pack $ show i
