module Main where

-------------------------------------------------------------------------------
import           Data.Conduit
import           Data.Conduit.Binary
import           Data.Conduit.List
import           System.IO
-------------------------------------------------------------------------------
import           Hadoop.Streaming
-------------------------------------------------------------------------------


main :: IO ()
main = runResourceT $
  sourceHandle stdin $=
  (protoDec linesProtocol) $=
  (protoEnc linesProtocol) $$
  sinkHandle stdout
