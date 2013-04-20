{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE FlexibleInstances         #-}
{-# LANGUAGE MultiParamTypeClasses     #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE RankNTypes                #-}
{-# LANGUAGE RecordWildCards           #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Hadoop.Streaming
-- Copyright   :  Soostone Inc
-- License     :  BSD3
--
-- Maintainer  :  Ozgun Ataman
-- Stability   :  experimental
--
-- Low level building blocks for working with Hadoop streaming.
--
-- We define all the base types for MapReduce and export map/reduce
-- maker functions that know how to deal with ByteString input and
-- output.
----------------------------------------------------------------------------

module Hadoop.Streaming
    (
      -- * Types

      Key
    , CompositeKey
    , Value
    , Mapper
    , Reducer


    -- * Hadoop Utilities
    , emitCounter
    , hsEmitCounter
    , emitStatus
    , getFileName

     -- * MapReduce Construction
    , MROptions (..)
    , PartitionStrategy (..)
    , mapReduceMain
    , mapReduce

    -- * Low-level Utilities
    , mapper
    , mapperWith
    , reducer
    , reducerMain

    -- * Serialization Strategies
    , Protocol (..)
    , Protocol'
    , prismToProtocol

    , idProtocol
    , linesProtocol
    , serProtocol
    , showProtocol
    , csvProtocol

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
import           Blaze.ByteString.Builder
import           Control.Applicative
import           Control.Category
import           Control.Lens
import           Control.Monad
import           Control.Monad.Trans
import           Data.Attoparsec.ByteString.Char8 (Parser, endOfLine, takeTill)
import qualified Data.ByteString.Base64           as Base64
import qualified Data.ByteString.Char8            as B
import           Data.Conduit
import           Data.Conduit.Attoparsec
import           Data.Conduit.Binary              (sinkHandle, sourceHandle)
import           Data.Conduit.Blaze
import qualified Data.Conduit.List                as C
import           Data.Conduit.Utils
import           Data.CSV.Conduit
import           Data.List
import           Data.Monoid
import qualified Data.Serialize                   as Ser
import           Options.Applicative              hiding (Parser)
import           Prelude                          hiding (id, (.))
import           Safe
import           System.Environment
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


-- | Emit counter from this library's group
hsEmitCounter :: B.ByteString -> Integer -> IO ()
hsEmitCounter = emitCounter "hadoop-streaming"


-- | Emit a status line
emitStatus :: B.ByteString -> IO ()
emitStatus msg = B.hPutStrLn stderr txt
    where
      txt = B.concat ["reporter:status:", msg]


type Key = B.ByteString

type Value = B.ByteString


-- | Parse a line of input and eat a tab character that may be at the
-- very end of the line. This tab is put by hadoop if this file is the
-- result of a previous M/R that didn't have any value in the reduce
-- step.
parseLine :: Parser B.ByteString
parseLine = ln <* endOfLine
    where
      ln = do
        x <- takeTill (== '\n')
        return $ if B.length x > 0 && B.last x == '\t'
          then B.init x
          else x


-- | Turn incoming stream into a stream of lines. This will
-- automatically eat tab characters at the end of the line.
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
            -- ^ Re-assemble remaining segments to restore
            -- correctness.
            spl = B.split '\t' line



-- | Use this whenever you want to emit a line to the output as part
-- of the current stage, whether your mapping or reducing.
--
-- Note that we don't append a newline, you have to do that.
emitOutput :: MonadIO m => B.ByteString -> m ()
emitOutput bs = liftIO $ B.hPutStr stdout bs


-- | Get the current filename from Hadoop ENV. Useful when writing
-- 'Mapper's and you would like to know what file you're currently
-- dealing with.
getFileName :: MonadIO m => m String
getFileName = liftIO $ getEnv "map_input_file"


type CompositeKey   = [B.ByteString]


-------------------------------------------------------------------------------
-- | A 'Mapper' parses and converts the unbounded incoming stream of
-- input into a stream of (key, value) pairs.
type Mapper a m b     = Conduit a m ([Key], b)


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


-------------------------------------------------------------------------------
-- | Options for an MR job with value a emitted by 'Mapper' and
-- reduced by 'Reducer'.
data MROptions a = MROptions {
      mroEq      :: ([Key] -> [Key] -> Bool)
    -- ^ An equivalence test for incoming keys. True means given two
    -- keys are part of the same reduce series.
    , mroPart    :: PartitionStrategy
    -- ^ Number of segments to expect in incoming keys.
    , mroInPrism :: Prism' B.ByteString a
    -- ^ A serialization scheme for values between the map-reduce
    -- steps.
    }



-- | Construct a mapper program using given serialization Prism.
mapperWith
    :: (MonadIO m, MonadUnsafeIO m)
    => Prism' B.ByteString t
    -> Conduit B.ByteString m ([Key], t)
    -> m ()
mapperWith p f = mapper $ f =$= C.mapMaybe conv
    where
      conv x = _2 (firstOf (re p)) x



-- | Construct a mapper program using a given low-level conduit.
mapper
    :: (MonadIO m, MonadUnsafeIO m)
    => Conduit B.ByteString m ([Key], B.ByteString)
    -- ^ A key/value producer - don't worry about putting any newline
    -- endings yourself, we do that for you.
    -> m ()
mapper f = do
    liftIO $ hSetBuffering stderr LineBuffering
    liftIO $ hSetBuffering stdout LineBuffering
    liftIO $ hSetBuffering stdin LineBuffering
    sourceHandle stdin =$=
      performEvery every inLog =$=
      f =$=
      performEvery every outLog =$=
      C.map conv =$=
      builderToByteString $$
      sinkHandle stdout
    where
      conv (k,v) = mconcat
        [ mconcat (intersperse tab (map fromByteString k))
        , tab
        , fromByteString v
        , nl ]

      tab = fromByteString "\t"
      nl = fromByteString "\n"
      every = 1

      inLog _ = liftIO $ hsEmitCounter "Map input chunks" every
      outLog _ = do
        liftIO $ hsEmitCounter "Map emitted rows" every
        -- liftIO $ hsEmitCounter (B.concat ["Map emitted: ", fn]) every



-------------------------------------------------------------------------------
-- | Build a main function entry point for a reducer. The buck stops
-- here and we tag each bytestring line with a newline.
reducerMain
    :: (MonadIO m, MonadThrow m, MonadUnsafeIO m)
    => MROptions a
    -> Reducer a m B.ByteString
    -- ^ Important: It is assumed that each 'ByteString' here will end
    -- (your responsibility) with a newline, therefore constituting a
    -- line for the Hadoop ecosystem.
    -> m ()
reducerMain mro@MROptions{..} g =
    reducer mro g $=
    -- C.map write $=
    -- builderToByteString $=
    C.mapM_ emitOutput $$
    C.sinkNull
  -- where
    -- write x = fromByteString x `mappend` nl
    -- nl = fromByteString "\n"



-- | An easy way to construct a reducer pogram. Just supply the
-- arguments and you're done.
reducer
    :: (MonadIO m, MonadThrow m)
    => MROptions a
    -> Reducer a m B.ByteString
    -- ^ A step function for the given key.
    -> Source m B.ByteString
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
            Just x@(k,_) ->
              case cur of
                Just curKey -> do
                  case mroEq curKey k of
                    True -> yield x >> sameKey cur
                    False -> leftover x
                Nothing -> do
                  yield x
                  sameKey (Just k)


      logIn _ = liftIO $ hsEmitCounter "Reducer processed rows" every
      logConv _ = liftIO $ hsEmitCounter "Reducer deserialized objects" every
      logOut _ = liftIO $ hsEmitCounter "Reducer emitted rows" every

      every = 1

      stream = sourceHandle stdin =$=
               lineC (numSegs mroPart) =$=
               performEvery every logIn =$=
               C.mapMaybe (_2 (firstOf mroInPrism)) =$=
               performEvery every logConv =$=
               go2 =$=
               performEvery every logOut



                              -------------------
                              -- Serialization --
                              -------------------


-- | Like 'Protocol' but fixes the source format as 'ByteString'.
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
--
-- It is assumed that the prism you supply to this function do not add
-- newlines themselves. You need to make them newline-free for this to
-- work properly.
prismToProtocol
    :: (MonadUnsafeIO m, MonadThrow m)
    => Prism' B.ByteString a
    -> Protocol' m a
prismToProtocol p =
    Protocol { protoEnc = C.mapMaybe (firstOf (re p)) =$= write
             , protoDec = linesConduit =$= C.mapMaybe (firstOf p) }
  where
    write = C.map (\x -> fromByteString x `mappend` nl) =$=
            builderToByteString
    nl = fromByteString "\n"


-------------------------------------------------------------------------------
-- | Basically 'id' from Control.Category. Just pass the incoming
-- ByteString through.
idProtocol :: Monad m => Protocol' m B.ByteString
idProtocol = id


-- | A simple serialization strategy that works on lines of strings.
linesProtocol :: MonadThrow m => Protocol' m B.ByteString
linesProtocol = Protocol { protoEnc = C.map (\x -> B.concat [x, "\n"])
                         , protoDec = linesConduit }


-------------------------------------------------------------------------------
-- | Channel the 'Serialize' instance through 'Base64' encoding to
-- make it newline-safe, then turn into newline-separated stream.
serProtocol :: (MonadUnsafeIO m, MonadThrow m, Ser.Serialize a)
            => Protocol' m a
serProtocol = prismToProtocol pSerialize


-------------------------------------------------------------------------------
-- | Protocol for converting to/from any stream type 'b' and CSV type 'a'.
csvProtocol :: (MonadUnsafeIO m, MonadThrow m, CSV b a)
            => CSVSettings -> Protocol m b a
csvProtocol set = Protocol (fromCSV set) (intoCSV set)


-------------------------------------------------------------------------------
-- | Use 'Show'/'Read' instances to stream-serialize. You must be
-- careful not to have any newline characters inside, or the stream
-- will get confused.
--
-- This is meant for debugging more than anything. Do not use it in
-- serious matters. Use 'serProtocol' instead.
showProtocol :: (MonadUnsafeIO m, MonadThrow m, Read a, Show a)
             => Protocol' m a
showProtocol = prismToProtocol pShow


-- | Helper for reliable serialization
serialize :: Ser.Serialize a => a -> B.ByteString
serialize = Base64.encode . Ser.encode


-- | Helper for reliable deserialization
deserialize :: Ser.Serialize c => B.ByteString -> Either String c
deserialize = Ser.decode <=< Base64.decode


-- | Serialize with the 'Serialize' instance.
--
-- Any 'Prism' can be used as follows:
--
-- >>> import Control.Lens
--
-- To decode ByteString into target object:
--
-- >>> firstOf myPrism byteStr
-- Just a
--
-- To encode an object into ByteString:
--
-- >>> firstOf (re myPrism) myObject
-- Just byteStr
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



-------------------------------------------------------------------------------
mapReduce
    :: (MonadIO m, MonadThrow m, MonadUnsafeIO m)
    => MROptions a
    -> Mapper B.ByteString m a
    -> Reducer a m B.ByteString
    -> (m (), m ())
mapReduce mro f g = (mp, rd)
    where
      mp = mapperWith (mroInPrism mro) f
      rd = reducerMain mro g



-- | A default main that will respond to 'map' and 'reduce' commands
-- to run the right phase appropriately.
--
-- This is the recommended 'main' entry point to a map-reduce program.
-- The resulting program will respond as:
--
-- > ./myProgram map
-- > ./myProgram reduce
mapReduceMain
    :: (MonadIO m, MonadThrow m, MonadUnsafeIO m)
    => MROptions a
    -> Mapper B.ByteString m a
    -> Reducer a m B.ByteString
    -- ^ Reducer for a stream of values belonging to the same key.
    -> m ()
mapReduceMain mro f g = liftIO (execParser opts) >>= run
  where
    (mp,rd) = mapReduce mro f g

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





