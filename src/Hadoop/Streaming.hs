{-# LANGUAGE ExistentialQuantification #-}
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
    , CompositeKey
    , Value (..)
    , Mapper
    , Reducer


    -- * Hadoop Utilities
    , emitCounter
    , emitStatus

     -- * MapReduce Construction
    , MROptions (..)
    , mapReduceMain
    , mapReduce

    -- * Low-level Utilities
    , mapper
    , mapperWith
    , reducer
    , reducerMain

    -- * Serialization Helpers
    , Protocol (..)
    , Protocol'
    , prismToProtocol

    , linesProtocol
    , serProtocol
    , showProtocol

    , pSerialize
    , pShow

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
import           Control.Category
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
import           Prelude                          hiding (id, (.))
import           Safe
import           System.IO
-------------------------------------------------------------------------------
import           Hadoop.Streaming.Hadoop
-------------------------------------------------------------------------------


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


parseLine :: Parser B.ByteString
parseLine = ln <* endOfLine
    where
      ln = takeTill (== '\n')

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
    -> Mapper B.ByteString m a
    -> Reducer a m b
    -> Conduit b m B.ByteString
    -- ^ A final output serializer
    -> (m (), m ())
mapReduce mro f g out = (mp, rd)
    where
      mp = mapperWith (mroPrism mro) f
      rd = reducerMain mro g out


-------------------------------------------------------------------------------
reducerMain
    :: (MonadIO m, MonadThrow m)
    => MROptions a
    -> Reducer a m r
    -> Conduit r m B.ByteString
    -> m ()
reducerMain mro g out =
    reducer mro g $=
    out $=
    C.mapM_ emitOutput $$
    C.sinkNull


-- | Use this whenever you want to emit a line to the output as part
-- of the current stage, whether your mapping or reducing.
emitOutput :: MonadIO m => B.ByteString -> m ()
emitOutput bs = liftIO $ B.hPutStr stdout bs


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


type CompositeKey   = [B.ByteString]


-------------------------------------------------------------------------------
-- | A 'Mapper' parses and converts the unbounded incoming stream of
-- input into a stream of (key, value) pairs.
type Mapper a m b     = Conduit a m ([Key], b)


data MROptions a = MROptions {
      mroEq    :: ([Key] -> [Key] -> Bool)
    -- ^ An equivalence test for incoming keys. True means given two
    -- keys are part of the same reduce series.
    , mroPart  :: PartitionStrategy
    -- ^ Number of segments to expect in incoming keys.
    , mroPrism :: Prism' B.ByteString a
    -- ^ A serialization scheme for the incoming values.
    }



instance Default (MROptions B.ByteString) where
    def = MROptions (==) def id


-------------------------------------------------------------------------------
-- | A reducer takes an incoming stream of (key, value) pairs and
-- emits zero or more output objects of type 'r'.
--
-- Note that this framework guarantees your reducer function (i.e. the
-- conduit you supply here) will see ONLY keys that are deemed
-- 'equivalent' based on the 'MROptions' you supply. Different keys
-- will be given to individual and isolated invocations of your
-- reducer function. This is pretty much the key abstraction provided
-- by this framework.
type Reducer a m r  = Conduit (CompositeKey, a) m r


-- | An easy way to construct a reducer pogram. Just supply the
-- arguments and you're done.
reducer
    :: (MonadIO m, MonadThrow m)
    => MROptions a
    -> Reducer a m r
    -- ^ A step function for the given key.
    -> Source m r
reducer MROptions{..} f = do
    liftIO $ hSetBuffering stderr LineBuffering
    liftIO $ hSetBuffering stdout LineBuffering
    liftIO $ hSetBuffering stdin LineBuffering
    stream
    where
      go2 = do
        next <- await
        case next of
          Nothing -> return ()
          Just x -> do
            leftover x
            block
            go2

      block = sameKey Nothing =$= f

      sameKey cur = do
          next <- await
          case next of
            Nothing -> return ()
            Just x@(k,v) ->
              case cur of
                Just curKey -> do
                  case mroEq curKey k of
                    True -> yield x >> sameKey cur
                    False -> leftover x
                Nothing -> do
                  yield x
                  sameKey (Just k)

      -- go cur = do
      --     next <- await
      --     case next of
      --       Nothing ->
      --         case cur of
      --           Just (curKey, a) -> finalize curKey a
      --           Nothing -> return ()
      --       Just (k,v) ->
      --         case cur of
      --           Just (curKey, a) -> do
      --             !a' <- case mroEq curKey k of
      --               True -> lift $ f k a v
      --               False -> do
      --                 -- liftIO $ print $ "finalizing key: " ++ show curKey
      --                 finalize curKey a
      --                 lift $ f k a0 v
      --             go (Just (k, a'))
      --           Nothing -> do
      --             !a' <- lift $ f k a0 v
      --             go (Just (k, a'))

      -- finalize k v = lift $ fin k v


      log i = liftIO $ emitCounter "reducer" "rows_processed" 10

      stream = sourceHandle stdin =$=
               lineC (numSegs mroPart) =$=
               performEvery 10 log =$=
               C.mapMaybe (_2 (firstOf mroPrism)) =$=
               go2



                              -------------------
                              -- Serialization --
                              -------------------


type Protocol' m a = Protocol m B.ByteString a


-- | A 'Protocol's is a serialization strategy when we're dealing with
-- streams of records, allowing for arbitrary data formats, delimiters
-- and other potential obstructions.
--
-- Most of the time we'll be using 'Protocol\''s.
data Protocol m b a = Protocol {
      protoEnc :: Conduit a m b
    , protoDec :: Conduit b m a
    }


instance Monad m => Category (Protocol m) where
    id = Protocol (C.map id) (C.map id)
    p1 . p2 = Protocol { protoEnc = (protoEnc p1) =$= (protoEnc p2)
                       , protoDec = (protoDec p2) =$= (protoDec p1) }



-- | Lift 'Prism' to work with a newline-separated stream of objects.
prismToProtocol :: MonadThrow m => Prism' B.ByteString a -> Protocol' m a
prismToProtocol p =
    Protocol { protoEnc = C.mapMaybe (firstOf (re p)) =$=
                          C.map (\x -> B.concat [x, "\n"])

             , protoDec = linesConduit =$=
                          C.mapMaybe (firstOf p) }


-- | A simple serialization strategy that works on lines of strings.
linesProtocol :: MonadThrow m => Protocol' m B.ByteString
linesProtocol = Protocol { protoEnc = C.map (\x -> B.concat [x, "\n"])
                         , protoDec = linesConduit }


-------------------------------------------------------------------------------
-- | Channel the 'Serialize' instance through 'Base64' encoding to
-- make it newline-safe, then turn into newline-separated stream.
serProtocol :: (MonadThrow m, Ser.Serialize a) => Protocol' m a
serProtocol = prismToProtocol pSerialize


-------------------------------------------------------------------------------
-- | Use 'Show'/'Read' instances to stream-serialize. You must be
-- careful not to have any newline characters inside, or the stream
-- will get confused.
--
-- This is meant for debugging more than anything. Do not use it in
-- serious matters. Use 'serProtocol' instead.
showProtocol :: (MonadThrow m, Read a, Show a) => Protocol' m a
showProtocol = prismToProtocol pShow


-- | Helper for reliable serialization
serialize :: Ser.Serialize a => a -> B.ByteString
serialize = Base64.encode . Ser.encode


-- | Helper for reliable deserialization
deserialize :: Ser.Serialize c => B.ByteString -> Either String c
deserialize = Ser.decode <=< Base64.decode


-- | Serialize with the 'Serialize' instance
pSerialize :: Ser.Serialize a => Prism' B.ByteString a
pSerialize = prism serialize (\x -> either (const $ Left x) Right $ deserialize x)


-- | Serialize with the Show/Read instances
pShow :: (Show a, Read a) => Prism' B.ByteString a
pShow = prism
          (B.pack . show)
          (\x -> maybe (Left x) Right . readMay . B.unpack $ x)


                              ------------------
                              -- Main Program --
                              ------------------




-- | A default main that will respond to 'map' and 'reduce' commands
-- to run the right phase appropriately.
--
-- This is the recommended approach to designing a map-reduce program.
mapReduceMain
    :: (MonadIO m, MonadThrow m)
    => MROptions a
    -> Mapper B.ByteString m a
    -> Reducer a m r
    -- ^ Reducer for a stream of values belonging to the same key.
    -> Conduit r m B.ByteString
    -- ^ A final serialization function.
    -> m ()
mapReduceMain mro f g out = liftIO (execParser opts) >>= run
  where
    (mp,rd) = mapReduce mro f g out

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





