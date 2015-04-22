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
-- Module      :  Hadron
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

module Hadron.Basic
    (
      -- * Types
      Key
    , CompositeKey
    , Mapper
    , Reducer


    -- * Hadoop Utilities
    , emitCounter
    , hsEmitCounter
    , emitStatus
    , getFileName

     -- * MapReduce Construction
    , mapReduceMain
    , mapReduce
    , MROptions (..)
    , PartitionStrategy (..)

    -- * Low-level Utilities
    , mapper
    , mapperWith
    , combiner
    , reducer
    , setLineBuffering

    -- * Data Serialization Utilities
    , module Hadron.Protocol

    ) where


-------------------------------------------------------------------------------
import           Blaze.ByteString.Builder
import           Control.Applicative
import           Control.Category
import           Control.Lens
import           Control.Monad
import           Control.Monad.Base
import           Control.Monad.Primitive
import           Control.Monad.Trans
import           Control.Monad.Trans.Resource
import qualified Data.ByteString.Char8        as B
import qualified Data.ByteString.Lazy.Char8   as LB
import           Data.Conduit
import           Data.Conduit.Binary          (sinkHandle, sourceHandle)
import           Data.Conduit.Blaze
import qualified Data.Conduit.List            as C
import           Data.List
import           Data.Monoid
import           Options.Applicative
import           Prelude                      hiding (id, (.))
import           System.Environment
import           System.IO
-------------------------------------------------------------------------------
import           Hadron.Protocol
import           Hadron.Run.Hadoop
import           Hadron.Types
-------------------------------------------------------------------------------



showBS :: Show a => a -> B.ByteString
showBS = B.pack . show


-- | Emit a counter to be captured, added up and reported by Hadoop.
emitCounter
    :: B.ByteString
    -- ^ Group name
    -> B.ByteString
    -- ^ Counter name
    -> Integer
    -- ^ Increment
    -> IO ()
emitCounter grp counter inc = LB.hPutStrLn stderr $ toLazyByteString txt
    where
      txt = mconcat $ map fromByteString
            ["reporter:counter:", grp, ",", counter, ",", showBS inc]


-- | Emit counter from this library's group
hsEmitCounter :: B.ByteString -> Integer -> IO ()
hsEmitCounter = emitCounter "Hadron"


-- | Emit a status line.
emitStatus :: B.ByteString -> IO ()
emitStatus msg = LB.hPutStrLn stderr $ toLazyByteString txt
    where
      txt = fromByteString "reporter:status:" <>
            fromByteString msg


-- | Get the current filename from Hadoop ENV. Useful when writing
-- 'Mapper's and you would like to know what file you're currently
-- dealing with.
getFileName :: MonadIO m => m FilePath
getFileName = liftIO $ getEnv "mapreduce_map_input_file"



-------------------------------------------------------------------------------
mapper
    :: Mapper B.ByteString CompositeKey B.ByteString
    -- ^ A key/value producer - don't worry about putting any newline
    -- endings yourself, we do that for you.
    -> IO ()
mapper f = mapperWith id f


-- | Construct a mapper program using given serialization Prism.
mapperWith
    :: Prism' B.ByteString t
    -> Mapper B.ByteString CompositeKey t
    -> IO ()
mapperWith p f = runResourceT $ do
    setLineBuffering
    sourceHandle stdin $=
      f $=
      encodeMapOutput p $$
      sinkHandle stdout


-- -------------------------------------------------------------------------------
-- -- | Drop the key and simply output the value stream.
-- mapOnly
--     :: (InputStream B.ByteString -> OutputStream B.ByteString -> IO ())
--     -> IO ()
-- mapOnly f = do
--     setLineBuffering
--     f S.stdin S.stdout


-------------------------------------------------------------------------------
combiner
    :: MROptions
    -> Prism' B.ByteString b
    -> Reducer CompositeKey b (CompositeKey, b)
    -> IO ()
combiner mro mrInPrism f  = runResourceT $ do
    setLineBuffering
    sourceHandle stdin =$=
      decodeReducerInput mro mrInPrism =$=
      f =$=
      encodeMapOutput mrInPrism $$
      sinkHandle stdout



-------------------------------------------------------------------------------
setLineBuffering :: MonadIO m => m ()
setLineBuffering = do
    liftIO $ hSetBuffering stderr LineBuffering
    liftIO $ hSetBuffering stdout LineBuffering
    liftIO $ hSetBuffering stdin LineBuffering


-------------------------------------------------------------------------------
-- | Appropriately produce lines of mapper output in a way compliant
-- with Hadoop and 'decodeReducerInput'.
encodeMapOutput
    :: (PrimMonad base, MonadBase base m)
    => Prism' B.ByteString b
    -> Conduit (CompositeKey, b) m B.ByteString
encodeMapOutput mrInPrism = C.map conv $= builderToByteString
    where

      conv (k,v) = mconcat
        [ mconcat (intersperse tab (map fromByteString k))
        , tab
        , fromByteString (review mrInPrism v)
        , nl ]

      tab = fromByteString "\t"
      nl = fromByteString "\n"


-------------------------------------------------------------------------------
-- | Chunk 'stdin' into lines and try to decode the value using given 'Prism'.
decodeReducerInput
    :: (MonadIO m, MonadThrow m)
    => MROptions
    -> Prism' B.ByteString b
    -> ConduitM a (CompositeKey, b) m ()
decodeReducerInput mro mrInPrism =
    sourceHandle stdin =$=
    lineC (numSegs (_mroPart mro)) =$=
    C.mapMaybe (_2 (firstOf mrInPrism))


-------------------------------------------------------------------------------
reducerMain
    :: MROptions
    -> Prism' B.ByteString a
    -> Reducer CompositeKey a B.ByteString
    -> IO ()
reducerMain mro p f = do
    setLineBuffering
    runResourceT $ reducer mro p f $$ sinkHandle stdout


-- | Create a reducer program.
reducer
    :: MROptions
    -> Prism' B.ByteString a
    -- ^ Input conversion function
    -> Reducer CompositeKey a b
    -- ^ A step function for any given key. Will be rerun from scratch
    -- for each unique key based on MROptions.
    -> Producer (ResourceT IO) b
reducer mro@MROptions{..} mrInPrism f = do
    sourceHandle stdin =$=
      decodeReducerInput mro mrInPrism =$=
      go2
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
                  let n = eqSegs _mroPart
                  case take n curKey == take n k of
                    True -> yield x >> sameKey cur
                    False -> leftover x
                Nothing -> do
                  yield x
                  sameKey (Just k)




                              ------------------
                              -- Main Program --
                              ------------------



-------------------------------------------------------------------------------
mapReduce
    :: MROptions
    -> Prism' B.ByteString a
    -- ^ Serialization for data between map and reduce stages
    -> Mapper B.ByteString CompositeKey a
    -> Reducer CompositeKey a B.ByteString
    -> (IO (), IO ())
mapReduce mro mrInPrism f g = (mp, rd)
    where
      mp = mapperWith mrInPrism f
      rd = reducerMain mro mrInPrism g



-- | A default main that will respond to 'map' and 'reduce' commands
-- to run the right phase appropriately.
--
-- This is the recommended 'main' entry point to a map-reduce program.
-- The resulting program will respond as:
--
-- > ./myProgram map
-- > ./myProgram reduce
mapReduceMain
    :: MROptions
    -> Prism' B.ByteString a
    -- ^ Serialization function for the in-between data 'a'.
    -> Mapper B.ByteString CompositeKey a
    -> Reducer CompositeKey a B.ByteString
    -- ^ Reducer for a stream of values belonging to the same key.
    -> IO ()
mapReduceMain mro mrInPrism f g = liftIO (execParser opts) >>= run
  where
    (mp,rd) = mapReduce mro mrInPrism f g

    run Map = mp
    run Reduce = rd


    opts = info (helper <*> commandParse)
      ( fullDesc
      <> progDesc "This is a Hadron Map/Reduce binary. "
      <> header "hadron - use Haskell for Hadron."
      )


data Command = Map | Reduce


-------------------------------------------------------------------------------
commandParse :: Parser Command
commandParse = subparser
    ( command "map" (info (pure Map)
        ( progDesc "Run mapper." ))
   <> command "reduce" (info (pure Reduce)
        ( progDesc "Run reducer" ))
    )





