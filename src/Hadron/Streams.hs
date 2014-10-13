{-# LANGUAGE RankNTypes #-}

module Hadron.Streams where


-------------------------------------------------------------------------------
import           Control.Applicative
import           Control.Concurrent.Async
import           Control.Concurrent.BoundedChan
import           Control.Monad
import           Control.Monad.Trans
import           Data.Attoparsec.ByteString.Char8 (Parser, endOfInput,
                                                   endOfLine, takeTill)
import qualified Data.ByteString.Char8            as B
import qualified Data.Conduit                     as C
import qualified Data.Conduit.List                as C
import           System.IO.Streams                (InputStream, OutputStream)
import qualified System.IO.Streams                as S
import qualified System.IO.Streams.Attoparsec     as S
-------------------------------------------------------------------------------

{-|

Thinking about io-streams, as things are inverted in some sense:

- An InputStream knows how to produce input when user asks for it.
User can read from it.

- An OutputStream knows how to consume the input it will be fed by the
user. User can write to it.

-}


-------------------------------------------------------------------------------
-- | Very inefficient. Use a conduit to contramap an OutputStream.
contraMapConduit :: C.Conduit b IO a -> S.OutputStream a -> IO (S.OutputStream b)
contraMapConduit c s = S.makeOutputStream $ \ i -> case i of
    Nothing -> S.write Nothing s
    Just r -> do
      xs <- C.sourceList [r] C.$= c C.$$ C.consume
      mapM_ (flip S.write s . Just) xs


-------------------------------------------------------------------------------
mapMaybeS  :: (a -> Maybe b) -> InputStream a -> IO (InputStream b)
mapMaybeS f s = S.makeInputStream g
  where
    g = S.read s >>= return . join . fmap f



-- | Parse a line of input and eat a tab character that may be at the
-- very end of the line. This tab is put by hadoop if this file is the
-- result of a previous M/R that didn't have any value in the reduce
-- step.
parseLine :: Parser (Maybe B.ByteString)
parseLine = (endOfInput >> pure Nothing) <|> (Just <$> (ln <* endOfLine))
    where
      ln = do
        x <- takeTill (== '\n')
        return $ if B.length x > 0 && B.last x == '\t'
          then B.init x
          else x


-- | Turn incoming stream into a stream of lines. This will
-- automatically eat tab characters at the end of the line.
streamLines = S.parserToInputStream parseLine


-------------------------------------------------------------------------------
-- | Apply a conduit to input stream.
conduitStream
    :: MonadIO m
    => (forall b. m b -> IO b)
    -- ^ Need a monad morphism to IO for the context.
    -> C.Conduit i m o
    -> S.InputStream i
    -> m (S.InputStream o)
conduitStream run c i = consumeSource run (inputStreamToProducer i C.$= c)


-------------------------------------------------------------------------------
consumeSource :: MonadIO m => (forall b. m b -> IO b) -> C.Source m a -> m (S.InputStream a)
consumeSource run s = do
    ref <- liftIO $ newBoundedChan 512
    liftIO . async . run $ (s C.$$ go ref)
    liftIO $ S.makeInputStream (readChan ref)
    where
      go ref = do
          r <- C.await
          liftIO $ writeChan ref r
          case r of
            Nothing -> return ()
            Just _ -> go ref



-------------------------------------------------------------------------------
inputStreamToProducer :: MonadIO m => S.InputStream a -> C.Producer m a
inputStreamToProducer s = go
    where
      go = do
          x <- liftIO $ S.read s
          case x of
            Nothing -> return ()
            Just x' -> C.yield x' >> go


-------------------------------------------------------------------------------
outputStreamToConsumer :: MonadIO m => S.OutputStream a -> C.Consumer a m ()
outputStreamToConsumer s = go
    where
      go = do
          r <- C.await
          liftIO $ S.write r s
          case r of
            Nothing -> return ()
            Just _  -> go





