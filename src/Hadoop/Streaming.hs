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
    , reducer
    , reducerMain

    -- * Data Serialization Utilities
    , module Hadoop.Streaming.Protocol

    ) where


-------------------------------------------------------------------------------
import           Blaze.ByteString.Builder
import           Control.Applicative
import           Control.Category
import           Control.Lens
import           Control.Monad
import           Control.Monad.Trans
import qualified Data.ByteString.Char8      as B
import qualified Data.ByteString.Lazy.Char8 as LB
import           Data.Conduit
import           Data.Conduit.Binary        (sinkHandle, sourceHandle)
import           Data.Conduit.Blaze
import qualified Data.Conduit.List          as C
import           Data.List
import           Data.Monoid
import           Options.Applicative        hiding (Parser)
import           Prelude                    hiding (id, (.))
import           System.Environment
import           System.IO
-------------------------------------------------------------------------------
import           Hadoop.Streaming.Hadoop
import           Hadoop.Streaming.Protocol
import           Hadoop.Streaming.Types
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
hsEmitCounter = emitCounter "hadron"


-- | Emit a status line
emitStatus :: B.ByteString -> IO ()
emitStatus msg = LB.hPutStrLn stderr $ toLazyByteString txt
    where
      txt = fromByteString "reporter:status:" <>
            fromByteString msg


-- | Use this whenever you want to emit a line to the output as part
-- of the current stage, whether your mapping or reducing.
--
-- Note that we don't append a newline, you have to do that.
emitOutput :: MonadIO m => B.ByteString -> m ()
emitOutput bs = liftIO $ B.hPutStr stdout bs


-- | Get the current filename from Hadoop ENV. Useful when writing
-- 'Mapper's and you would like to know what file you're currently
-- dealing with.
getFileName :: MonadIO m => m FilePath
getFileName = liftIO $ getEnv "map_input_file"


-- | Construct a mapper program using given serialization Prism.
mapperWith
    :: (MonadIO m, MonadUnsafeIO m)
    => Prism' B.ByteString t
    -> Conduit B.ByteString m (CompositeKey, t)
    -> m ()
mapperWith p f = mapper $ f =$= C.map conv
    where
      -- conv x = _2 (firstOf (re p)) x
      conv (k,v) = (k, review p v)



-- | Construct a mapper program using a given low-level conduit.
mapper
    :: (MonadIO m, MonadUnsafeIO m)
    => Conduit B.ByteString m (CompositeKey, B.ByteString)
    -- ^ A key/value producer - don't worry about putting any newline
    -- endings yourself, we do that for you.
    -> m ()
mapper f = do
    liftIO $ hSetBuffering stderr LineBuffering
    liftIO $ hSetBuffering stdout LineBuffering
    liftIO $ hSetBuffering stdin LineBuffering
    sourceHandle stdin $=
      -- performEvery every inLog $=
      f $=
      C.map conv $=
      -- performEvery every outLog $=
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
    => MROptions
    -> Prism' B.ByteString a
    -> Reducer CompositeKey a m B.ByteString
    -- ^ Important: It is assumed that each 'ByteString' here will end
    -- (your responsibility) with a newline, therefore constituting a
    -- line for the Hadoop ecosystem.
    -> m ()
reducerMain mro@MROptions{..} mrInPrism g =
    reducer mro mrInPrism g $=
    C.mapM_ emitOutput $$
    C.sinkNull



-- | An easy way to construct a reducer pogram. Just supply the
-- arguments and you're done.
reducer
    :: (MonadIO m, MonadThrow m)
    => MROptions
    -> Prism' B.ByteString a
    -- ^ Input conversion function
    -> Reducer CompositeKey a m B.ByteString
    -- ^ A step function for the given key.
    -> Source m B.ByteString
reducer MROptions{..} mrInPrism f = do
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
                  case _mroEq curKey k of
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
               lineC (numSegs _mroPart) =$=
               -- performEvery every logIn =$=
               C.mapMaybe (_2 (firstOf mrInPrism)) =$=
               -- performEvery every logConv =$=
               go2
               -- performEvery every logOut




                              ------------------
                              -- Main Program --
                              ------------------



-------------------------------------------------------------------------------
mapReduce
    :: (MonadIO m, MonadThrow m, MonadUnsafeIO m)
    => MROptions
    -> Prism' B.ByteString a
    -- ^ Serialization for data between map and reduce stages
    -> Mapper B.ByteString m CompositeKey a
    -> Reducer CompositeKey a m B.ByteString
    -> (m (), m ())
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
    :: (MonadIO m, MonadThrow m, MonadUnsafeIO m)
    => MROptions
    -> Prism' B.ByteString a
    -- ^ Serialization function for the in-between data 'a'.
    -> Mapper B.ByteString m CompositeKey a
    -> Reducer CompositeKey a m B.ByteString
    -- ^ Reducer for a stream of values belonging to the same key.
    -> m ()
mapReduceMain mro mrInPrism f g = liftIO (execParser opts) >>= run
  where
    (mp,rd) = mapReduce mro mrInPrism f g

    run Map = mp
    run Reduce = rd


    opts = info (helper <*> commandParse)
      ( fullDesc
      <> progDesc "This is a Hadoop Streaming Map/Reduce binary. "
      <> header "hadron - use Haskell for Hadoop Streaming."
      )


data Command = Map | Reduce


-------------------------------------------------------------------------------
commandParse = subparser
    ( command "map" (info (pure Map)
        ( progDesc "Run mapper." ))
   <> command "reduce" (info (pure Reduce)
        ( progDesc "Run reducer" ))
    )





