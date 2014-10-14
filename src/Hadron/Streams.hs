{-# LANGUAGE BangPatterns    #-}
{-# LANGUAGE RankNTypes      #-}
{-# LANGUAGE TemplateHaskell #-}

module Hadron.Streams where

-------------------------------------------------------------------------------
import           Control.Applicative
import           Control.Concurrent.Async
import           Control.Concurrent.BoundedChan
import           Control.Lens
import           Control.Monad
import           Control.Monad.Trans
import           Data.Attoparsec.ByteString.Char8 (Parser, endOfInput,
                                                   endOfLine, takeTill)
import qualified Data.ByteString.Char8            as B
import qualified Data.Conduit                     as C
import qualified Data.Conduit.List                as C
import           Data.IORef
import           Data.Maybe
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
-- | Create a new stream for each item in the first stream and drain
-- results into a single stream.
bindStream :: InputStream a -> (a -> IO (InputStream b)) -> IO (InputStream b)
bindStream i f = do
    ref <- newIORef Nothing
    S.makeInputStream (loop ref)
  where
    loop ref = do
        !acc <- readIORef ref
        case acc of
          Just is -> do
            n <- S.read is
            case n of
              Nothing -> writeIORef ref Nothing >> loop ref
              Just _ -> return n
          Nothing -> do
            next <- S.read i
            case next of
              Nothing -> return Nothing
              Just x -> do
                !is <- f x
                writeIORef ref (Just is)
                loop ref



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
streamLines :: InputStream B.ByteString -> IO (InputStream B.ByteString)
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



-------------------------------------------------------------------------------
-- | Fold while also possibly returning elements to emit each step.
-- The IO action can be used at anytime to obtain the state of the
-- accumulator.
emitFoldM
    :: (b -> Maybe i -> IO (b, [a]))
    -> b
    -> InputStream i
    -> IO (InputStream a, IO b)
emitFoldM f a0 is = do
    ref <- newIORef ([], False, a0)
    is' <- S.makeInputStream (loop ref)
    return (is', liftM (view _3) (readIORef ref))

  where

    loop ref = do
        (!buffer, !eof, !acc) <- readIORef ref

        case buffer of

          -- previous results in buffer; stream them out
          (x:rest) -> do
              modifyIORef' ref (_1 .~ rest)
              return (Just x)

          -- buffer empty; step the input stream
          [] -> do
            case eof of
              True -> return Nothing
              False -> do
                inc <- S.read is
                (!acc', !xs) <- f acc inc
                modifyIORef' ref $
                  (_3 .~ acc') . (_1 .~ xs) .
                  if isNothing inc
                    then _2 .~ True
                    else id
                loop ref


