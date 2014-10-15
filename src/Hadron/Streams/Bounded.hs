-- | Stream utilities for working with concurrent channels.

{-# LANGUAGE BangPatterns #-}

module Hadron.Streams.Bounded
 ( -- * Channel conversions
   inputToChan
 , chanToInput
 , chanToOutput
 , makeChanPipe
 ) where

------------------------------------------------------------------------------
import           Control.Applicative            ((<$>), (<*>))
import           Control.Concurrent.BoundedChan
import           Prelude                        hiding (read)
------------------------------------------------------------------------------
import           System.IO.Streams.Internal     (InputStream, OutputStream,
                                                 makeInputStream,
                                                 makeOutputStream, read)


------------------------------------------------------------------------------
-- | Writes the contents of an input stream to a channel until the input stream
-- yields end-of-stream.
inputToChan :: InputStream a -> BoundedChan (Maybe a) -> IO ()
inputToChan is ch = go
  where
    go = do
        mb <- read is
        writeChan ch mb
        maybe (return $! ()) (const go) mb


------------------------------------------------------------------------------
-- | Turns a 'Chan' into an input stream.
--
chanToInput :: BoundedChan (Maybe a) -> IO (InputStream a)
chanToInput ch = makeInputStream $! readChan ch


------------------------------------------------------------------------------
-- | Turns a 'Chan' into an output stream.
--
chanToOutput :: BoundedChan (Maybe a) -> IO (OutputStream a)
chanToOutput = makeOutputStream . writeChan



--------------------------------------------------------------------------------
-- | Create a new pair of streams using an underlying 'Chan'. Everything written
-- to the 'OutputStream' will appear as-is on the 'InputStream'.
--
-- Since reading from the 'InputStream' and writing to the 'OutputStream' are
-- blocking calls, be sure to do so in different threads.
makeChanPipe :: Int ->  IO (InputStream a, OutputStream a)
makeChanPipe n = do
    chan <- newBoundedChan n
    (,) <$> chanToInput chan <*> chanToOutput chan
