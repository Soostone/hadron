{-# LANGUAGE BangPatterns              #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE FlexibleInstances         #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE RankNTypes                #-}
{-# LANGUAGE RecordWildCards           #-}

module Hadoop.Streaming
    (
      -- * Types

      Key (..)
    , Value (..)
    , Mapper
    , Reducer
    , Finalizer


    -- * Hadoop Utilities
    , emitCounter
    , emitStatus
    , emitOutput

     -- * MapReduce Construction
    , MROptions (..)
    , mapReduceMain
    , mapReduce

    , mapper
    , mapperWith

    , reducer
    , noopFin



    -- * Serialization of Haskell Types
    , ser
    , serialize
    , deserialize

    -- * Utils
    , linesConduit
    , lineC
    , mkKey
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
import           Data.Conduit.Binary              (sinkHandle, sourceHandle)
import qualified Data.Conduit.List                as C
import           Data.Conduit.Utils
import           Data.Default
import qualified Data.Map                         as M
import qualified Data.Serialize                   as Ser
import           Options.Applicative              hiding (Parser)
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


-- | Useful when making a key from multiple pieces of data
mkKey :: [B.ByteString] -> B.ByteString
mkKey = B.intercalate "|"


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
      txt = B.concat ["reporter:counter:", grp, ",", counter, ",", showBS inc]


-- | Emit a status line
emitStatus :: B.ByteString -> IO ()
emitStatus msg = B.hPutStrLn stderr txt
    where
      txt = B.concat ["reporter:status:", msg]


type Key = B.ByteString

type Value = B.ByteString


-- | Turn incoming stream into a stream of lines
linesConduit :: MonadThrow m => Conduit B.ByteString m B.ByteString
linesConduit = conduitParser parseLine =$= C.map snd


-- | Parse lines of (key,value) for hadoop reduce stage
lineC :: MonadThrow m => Int -> Conduit B.ByteString m ([Key], B.ByteString)
lineC n = linesConduit =$= C.map ppair
    where
      ppair line = (k, v)
          where
            k = take n spl
            v = B.intercalate "\t" $ drop n spl
            spl = B.split '\t' line



-------------------------------------------------------------------------------
mapReduce
    :: (MonadIO m, MonadThrow m)
    => MROptions a
    -> Mapper m a
    -> Reducer m a b
    -> b
    -> Finalizer m b
    -> (m (), m ())
mapReduce mro f g a0 z = (mp, rd)
    where
      mp = mapperWith (mroPrism mro) f
      rd = reducer mro g a0 z




-- | Construct a mapper program using given serialization Prism.
mapperWith
    :: MonadIO m
    => Prism' B.ByteString t
    -> Conduit B.ByteString m ([Key], t)
    -> m ()
mapperWith p f = mapper $ f =$= C.mapMaybe conv
    where
      conv x = _2 (firstOf (re p)) x


-- | Construct a mapper program using a given Conduit.
mapper :: MonadIO m => Conduit B.ByteString m ([Key], B.ByteString) -> m ()
mapper f = do
    liftIO $ hSetBuffering stderr LineBuffering
    liftIO $ hSetBuffering stdout LineBuffering
    liftIO $ hSetBuffering stdin LineBuffering
    sourceHandle stdin =$=
      f =$=
      performEvery 10 log =$=
      C.map conv $$
      sinkHandle stdout
    where
      conv (k,v) = B.concat [B.intercalate "\t" k, "\t", v, "\n"]
      log i = liftIO $ emitCounter "mapper" "rows_emitted" 10



type Mapper m a     = Conduit B.ByteString m ([Key], a)
type Reducer m a b  = [Key] -> b -> a -> m b

-- | It is up to you to call 'emitOutput' as part of this function to
-- actually emit results.
type Finalizer m b  = [Key] -> b -> m ()


-- | A no-op finalizer.
noopFin :: Monad m => Finalizer m a
noopFin _ _ = return ()


-- | Use this whenever you want to emit a line to the output as part
-- of the current stage, whether your mapping or reducing.
emitOutput :: MonadIO m => B.ByteString -> m ()
emitOutput bs = liftIO $ B.hPutStrLn stdout bs


data MROptions a = MROptions {
      mroEq      :: ([Key] -> [Key] -> Bool)
    -- ^ An equivalence test for incoming keys. True means given two
    -- keys are part of the same reduce series.
    , mroKeySegs :: Int
    -- ^ Number of segments to expect in incoming keys.
    , mroPrism   :: Prism' B.ByteString a
    -- ^ A serialization scheme for the incoming values.
    }


instance Default (MROptions B.ByteString) where
    def = MROptions (==) 1 id


-- | An easy way to construct a reducer pogram. Just supply the
-- arguments and you're done.
--
-- m : Monad
-- b : Incoming stream type
-- a : Accumulator type
reducer
    :: (MonadIO m, MonadThrow m)
    => MROptions b
    -> Reducer m b a
    -- ^ A step function for the given key.
    -> a
    -- ^ Inital state.
    -> Finalizer m a
    -- ^ What to do with the final state for a given key.
    -> m ()
reducer MROptions{..} f a0 fin = do
    liftIO $ hSetBuffering stderr LineBuffering
    liftIO $ hSetBuffering stdout LineBuffering
    liftIO $ hSetBuffering stdin LineBuffering
    stream
    where
      f' k !a' v =
        case firstOf mroPrism v of
          Nothing -> return a'
          Just v' -> f k a' v'

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
                  !a' <- case mroEq curKey k of
                    True -> lift $ f' k a v
                    False -> do
                      -- liftIO $ print $ "finalizing key: " ++ show curKey
                      finalize curKey a
                      lift $ f' k a0 v
                  go (Just (k, a'))
                Nothing -> do
                  !a' <- lift $ f' k a0 v
                  go (Just (k, a'))

      finalize k v = lift $ fin k v


      log i = liftIO $ emitCounter "reducer" "rows_processed" 10

      stream = sourceHandle stdin $=
               lineC mroKeySegs $=
               performEvery 10 log $$
               go Nothing



parseLine :: Parser B.ByteString
parseLine = ln <* endOfLine
    where
      ln = takeTill (== '\n')


                              ------------------
                              -- Main Program --
                              ------------------



-------------------------------------------------------------------------------
-- | A default main that will respond to 'map' and 'reduce' commands
-- to run the right phase appropriately.
--
-- This is the recommended approach to designing a map-reduce program.
mapReduceMain
    :: (MonadIO m, MonadThrow m)
    => MROptions a
    -> Mapper m a
    -> Reducer m a b
    -> b
    -> Finalizer m b
    -> m ()
mapReduceMain mro f g a0 z = liftIO (execParser opts) >>= run
  where
    (mp,rd) = mapReduce mro f g a0 z

    run Map = mp
    run Reduce = rd


    opts = info (helper <*> commandParse)
      ( fullDesc
      <> progDesc "This is a Hadoop Streaming Map/Reduce binary. "
      <> header "hadoop-streaming - use Haskell as your streaming Hadoop program."
      )


data Command = Map | Reduce


-------------------------------------------------------------------------------
commandParse = subparser
    ( command "map" (info (pure Map)
        ( progDesc "Run mapper." ))
   <> command "reduce" (info (pure Reduce)
        ( progDesc "Run reducer" ))
    )





