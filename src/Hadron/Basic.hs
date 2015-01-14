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
    , mapOnly
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
import           Control.Monad.Trans
import qualified Data.ByteString.Char8      as B
import qualified Data.ByteString.Lazy.Char8 as LB
import           Data.IORef
import           Data.List
import           Data.Monoid
import           Options.Applicative
import           Prelude                    hiding (id, (.))
import           System.Environment
import           System.IO
import           System.IO.Streams          (InputStream, OutputStream)
import qualified System.IO.Streams          as S
-------------------------------------------------------------------------------
import           Hadron.Protocol
import           Hadron.Run.Hadoop
import           Hadron.Streams
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
mapperWith p f = do
    setLineBuffering
    out <- S.contramap (encodeMapOutput p) S.stdout
    f S.stdin out


-------------------------------------------------------------------------------
-- | Drop the key and simply output the value stream.
mapOnly
    :: (InputStream B.ByteString -> OutputStream B.ByteString -> IO ())
    -> IO ()
mapOnly f = do
    setLineBuffering
    f S.stdin S.stdout


-------------------------------------------------------------------------------
combiner
    :: MROptions
    -> Prism' B.ByteString b
    -> Reducer CompositeKey b (CompositeKey, b)
    -> IO ()
combiner mro mrInPrism f  = do
    setLineBuffering

    inp <- decodeReducerInput mro mrInPrism
    out <- S.contramap (encodeMapOutput mrInPrism) S.stdout
    f inp out



setLineBuffering :: MonadIO m => m ()
setLineBuffering = do
    liftIO $ hSetBuffering stderr LineBuffering
    liftIO $ hSetBuffering stdout LineBuffering
    liftIO $ hSetBuffering stdin LineBuffering


-------------------------------------------------------------------------------
-- | Appropriately produce lines of mapper output in a way compliant
-- with Hadoop and 'decodeReducerInput'.
encodeMapOutput
    :: Prism' B.ByteString b
    -> (CompositeKey, b)
    -> B.ByteString
encodeMapOutput mrInPrism (k, v) = toByteString conv
    where

      conv = mconcat
        [ mconcat (intersperse tab (map fromByteString k))
        , tab
        , fromByteString (review mrInPrism v)
        , nl ]

      tab = fromByteString "\t"
      nl = fromByteString "\n"


-------------------------------------------------------------------------------
-- | Chunk 'stdin' into lines and try to decode the value using given 'Prism'.
decodeReducerInput
    :: MROptions
    -> Prism' B.ByteString b
    -> IO (InputStream (CompositeKey, b))
decodeReducerInput mro mrInPrism =
    lineC (numSegs (_mroPart mro)) S.stdin >>=
    mapMaybeS (_2 (firstOf mrInPrism))


-------------------------------------------------------------------------------
-- | An easy way to construct a reducer pogram. Just supply the
-- arguments and you're done.
reducer
    :: MROptions
    -> Prism' B.ByteString a
    -- ^ Input conversion function
    -> Reducer CompositeKey a B.ByteString
    -- ^ A step function for the given key.
    -> IO ()
reducer mro@MROptions{..} mrInPrism f = do
    setLineBuffering

    is <- decodeReducerInput mro mrInPrism

    go2 is

  where

    mkOut = S.makeOutputStream $ \ i ->
      case i of
        Just _ -> S.write i S.stdout
        Nothing -> return ()

    go2 is = do
        next <- S.peek is
        case next of
          Nothing -> S.write Nothing S.stdout
          Just _ -> do
           os <- mkOut
           is' <- isolateSameKey is
           f is' os
           go2 is

    isolateSameKey is = do
        ref <- newIORef Nothing
        S.makeInputStream (block ref is)


    n = eqSegs _mroPart

    block ref is = do
        cur <- readIORef ref
        next <- S.read is
        case next of
          Nothing -> return Nothing
          Just x@(k,_) ->
            case cur of
              Just curKey -> do
                case curKey == take n k of
                  True -> return $ Just x
                  False -> S.unRead x is >> return Nothing
              Nothing -> do
                writeIORef ref (Just (take n k))
                return $ Just x



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
      rd = reducer mro mrInPrism g



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





