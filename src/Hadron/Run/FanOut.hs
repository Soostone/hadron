{-# LANGUAGE BangPatterns    #-}
{-# LANGUAGE RankNTypes      #-}
{-# LANGUAGE TemplateHaskell #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Hadron.Run.FanOut
-- Copyright   :  Soostone Inc, 2015
-- License     :  BSD3
--
-- Maintainer  :  Ozgun Ataman
-- Stability   :  experimental
--
-- Haskell-native ability to stream output to multiple files in Hadoop
-- Streaming.
----------------------------------------------------------------------------


module Hadron.Run.FanOut
    ( FanOut
    , mkFanOut
    , fanWrite
    , fanClose
    , fanCloseAll
    , sinkFanOut
    , fanStats
    ) where

-------------------------------------------------------------------------------
import           Control.Applicative
import           Control.Concurrent.MVar
import           Control.Lens
import           Control.Monad
import           Control.Monad.Trans
import qualified Data.ByteString.Char8   as B
import           Data.Conduit
import qualified Data.Conduit.List       as C
import qualified Data.Map.Strict         as M
import           System.IO
-------------------------------------------------------------------------------

data FileHandle = FileHandle {
      _fhHandle :: Handle
    , _fhCount  :: !Int
    }
makeLenses ''FileHandle

data FanOut = FanOut {
      _fanFiles  :: MVar (M.Map FilePath FileHandle)
    , _fanCreate :: FilePath -> IO Handle
    }
makeLenses ''FanOut


-------------------------------------------------------------------------------
-- | Make a new fanout manager that will use given process creator.
-- Process is expected to pipe its stdin into the desired location.
mkFanOut :: (FilePath -> IO Handle) -> IO FanOut
mkFanOut f = FanOut <$> newMVar M.empty <*> pure f


-------------------------------------------------------------------------------
-- | Write into a file. A new process will be spawned if this is the
-- first time writing into this file.
fanWrite :: FanOut -> FilePath -> B.ByteString -> IO ()
fanWrite fo fp bs = modifyMVar_ (fo ^. fanFiles) $ \ m -> go m
  where
    go !m | Just fh <- M.lookup fp m = do
      B.hPut (fh ^. fhHandle) bs
      return $! m & at fp . _Just . fhCount %~ (+1)
    go !m = do
      r <- (fo ^. fanCreate) fp
      go $! M.insert fp (FileHandle r 0) m


-------------------------------------------------------------------------------
-- | Close a specific file.
fanClose :: FanOut -> FilePath -> IO ()
fanClose fo fp = modifyMVar_ (fo ^. fanFiles) $ \ m -> case m ^. at fp of
  Nothing -> return m
  Just fh -> do
      hClose (fh ^. fhHandle)
      return $! m & at fp .~ Nothing


-------------------------------------------------------------------------------
-- | Close all files. The same FanOut can be used after this, which
-- would spawn new processes to write into files.
fanCloseAll :: FanOut -> IO ()
fanCloseAll fo = modifyMVar_ (fo ^. fanFiles) $ \m -> do
  forM_ (M.toList m) $ \ (_fp, fh) -> hClose (fh ^. fhHandle)
  return M.empty


-------------------------------------------------------------------------------
-- | Grab # of writes into each file so far.
fanStats :: FanOut -> IO (M.Map FilePath Int)
fanStats fo = do
    m <- modifyMVar (fo ^. fanFiles) $ \ m -> return (m,m)
    return $ M.map (^. fhCount) m


-------------------------------------------------------------------------------
-- | Sink a stream into 'FanOut'.
sinkFanOut
    :: MonadIO m
    => (a -> FilePath)
    -> (a -> m B.ByteString)
    -> FanOut
    -> Consumer a m Int
sinkFanOut dispatch conv fo = C.foldM go 0
    where
      go !i a = do
          bs <- conv a
          liftIO (fanWrite fo (dispatch a) bs)
          return $! i + 1


