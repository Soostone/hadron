{-# LANGUAGE BangPatterns              #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE RankNTypes                #-}

module Hadoop.Streaming
    (
      -- * Types

      Key (..)
    , Value (..)
    , Mapper
    , Reducer
    , Finalizer

     -- * MapReduce Construction

    , mapper
    , mapperWith

    , reducer
    , reducerWith

    -- * Hadoop Utilities
    , emitCounter
    , emitStatus

    -- * Serialization of Haskell Types
    , ser
    , serialize
    , deserialize
    ) where


-------------------------------------------------------------------------------
import           Control.Applicative
import           Control.Arrow
import           Control.Lens
import           Control.Monad
import           Control.Monad.Trans
import           Data.Attoparsec.ByteString.Char8 (Parser (..), endOfLine,
                                                   takeTill)
import qualified Data.ByteString.Base64           as Base64
import qualified Data.ByteString.Char8            as B
import           Data.Conduit
import           Data.Conduit.Attoparsec
import           Data.Conduit.Binary
import qualified Data.Conduit.List                as C
import qualified Data.Map                         as M
import qualified Data.Serialize                   as Ser
import           System.IO
-------------------------------------------------------------------------------


-- | Helper for reliable serialization
serialize :: Ser.Serialize a => a -> B.ByteString
serialize = Base64.encode . Ser.encode


-- | Helper for reliable deserialization
deserialize :: Ser.Serialize c => B.ByteString -> Either String c
deserialize = Ser.decode <=< Base64.decode


ser :: Ser.Serialize a => Prism' B.ByteString a
ser = prism serialize (\x -> either (const $ Left x) Right $ deserialize x)


showBS :: Show a => a -> B.ByteString
showBS = B.pack . show


emitCounter
    :: B.ByteString
    -- ^ Group
    -> B.ByteString
    -- ^ Counter name
    -> Integer
    -- ^ Increment
    -> IO ()
emitCounter grp counter inc = B.hPutStrLn stderr txt
    where
      txt = B.concat ["reporter:counter:", grp, ":", counter, ":", showBS inc]


emitStatus = undefined


type Key = B.ByteString

type Value = B.ByteString


-- | Parse lines of (key,value) for hadoop reduce stage
lineC :: MonadThrow m => Conduit B.ByteString m (B.ByteString, B.ByteString)
lineC = conduitParser parseLine =$= C.map (ppair . snd)
    where
      ppair line = (k, v)
          where
            (k, v') = B.span (/= '\t') line
            v = B.drop 1 v'



parseLine :: Parser B.ByteString
parseLine = ln <* endOfLine
    where
      ln = takeTill (== '\n')


-- | Construct a mapper program using given serialization Prism.
mapperWith
    :: MonadIO m
    => Prism' B.ByteString t
    -> Conduit B.ByteString m (Key, t)
    -> m ()
mapperWith p f = mapper $ f =$= C.mapMaybe conv
    where
      conv x = _2 (firstOf (re p)) x


-- | Construct a mapper program using a given Conduit.
mapper :: MonadIO m => Conduit B.ByteString m (B.ByteString, B.ByteString) -> m ()
mapper f = sourceHandle stdin =$= f =$= C.map conv $$ sinkHandle stdout
    where
      conv (k,v) = B.concat [k, "\t", v, "\n"]


type Mapper m a     = Conduit B.ByteString m (Key, a)
type Reducer m a t  = (Key -> t -> a -> m t)

-- | It is up to you to call 'emitOutput' as part of this function to
-- actually emit results.
type Finalizer m t  = (Key -> t -> m ())


-- | Use this whenever you want to emit a line to the output as part
-- of the current stage, whether your mapping or reducing.
emitOutput :: MonadIO m => B.ByteString -> m ()
emitOutput bs = liftIO $ B.hPutStrLn stdout bs


-- | Like 'reducer' but automatically parses values using given Prism.
reducerWith
    :: (MonadThrow m, MonadIO m)
    => Prism' B.ByteString a
    -- ^ Serialization prism
    -> Reducer m a t
    -- ^ A step function
    -> (Key -> Key -> Bool)
    -- ^ An equivalence test for incoming keys. True means given two
    -- keys are part of the same reduce series.
    -> t
    -- ^ Accumulator
    -> Finalizer m t
    -- ^ What to do with the final accumulator
    -> m ()
reducerWith p f eqTest a0 fin = reducer f' eqTest a0 fin
    where
      f' k !a' v =
        case firstOf p v of
          Nothing -> return a'
          Just v' -> f k a' v'


-- | An easy way to construct a reducer pogram. Just supply the
-- arguments and you're done.
reducer
    :: (MonadIO m, MonadThrow m)
    => Reducer m Value t
    -- ^ A step function for the given key.
    -> (Key -> Key -> Bool)
    -- ^ An equivalence test for incoming keys. True means given two
    -- keys are part of the same reduce series.
    -> t
    -- ^ Inital state.
    -> Finalizer m t
    -- ^ What to do with the final state for a given key.
    -> m ()
reducer f eqTest a0 fin = do
    liftIO $ hSetBuffering stderr LineBuffering
    liftIO $ hSetBuffering stdout LineBuffering
    stream
    where
      go cur = do
          next <- await
          case next of
            Nothing ->
              case cur of
                Just (curKey, a) -> finalize curKey a
                Nothing -> return ()
            Just (k,v) ->
              case cur of
                Just (curKey, a) -> do
                  !a' <- case eqTest curKey k of
                    True -> lift $ f k a v
                    False -> do
                      finalize curKey a
                      lift $ f k a0 v
                  go (Just (k, a'))
                Nothing -> do
                  !a' <- lift $ f k a0 v
                  go (Just (k, a'))

      finalize k v = lift $ fin k v


      stream = sourceHandle stdin $=
               lineC $$
               go Nothing



-- | Partition the input stream so that this function recurses while
-- the input stream is for the same key and returns the newly seen key
-- when it's done.
--
-- So you get to call this function once per the key that is being
-- reduced.
mrPartition :: (Eq k, Monad m) => Maybe k -> ConduitM (k, v) v m (Maybe k)
mrPartition curKey = do
    next <- await
    case next of
      Nothing -> return Nothing
      Just x@(k,v) ->
        case (Just k == curKey) of
          True -> yield v >> mrPartition curKey
          False -> do
            leftover x
            return $! Just k
