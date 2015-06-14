{-# LANGUAGE BangPatterns              #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE RankNTypes                #-}
{-# LANGUAGE RecordWildCards           #-}
{-# LANGUAGE TemplateHaskell           #-}

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

    , FanOutSink
    , sinkFanOut
    , sequentialSinkFanout
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
import           Data.Monoid
import           System.FilePath.Lens
import           System.IO
-------------------------------------------------------------------------------
import           Hadron.Utils
-------------------------------------------------------------------------------


-- | An open file handle
data FileHandle = FileHandle {
      _fhHandle       :: !Handle
    , _fhFin          :: IO ()
    , _fhPath         :: !FilePath
    , _fhCount        :: !Int
    , _fhPendingCount :: !Int
    }
makeLenses ''FileHandle


-- | Concurrent multi-output manager.
data FanOut = FanOut {
      _fanFiles  :: MVar (M.Map FilePath FileHandle)
    , _fanCreate :: FilePath -> IO (Handle, IO ())
    }
makeLenses ''FanOut


data FileChunk = FileChunk {
      _chunkOrig   :: !FilePath
    , _chunkTarget :: !FilePath
    , _chunkCnt    :: !Int
    }

makeLenses ''FileChunk


-------------------------------------------------------------------------------
-- | Make a new fanout manager that will use given process creator.
-- Process is expected to pipe its stdin into the desired location.
mkFanOut
    :: (FilePath -> IO (Handle, IO ()))
    -- ^ Open a handle for a given target path
    -> IO FanOut
mkFanOut f = FanOut <$> newMVar M.empty <*> pure f


-------------------------------------------------------------------------------
-- | Write into a file. A new process will be spawned if this is the
-- first time writing into this file.
fanWrite :: FanOut -> FilePath -> B.ByteString -> IO ()
fanWrite fo fp bs = modifyMVar_ (fo ^. fanFiles) go
  where

    go !m | Just fh <- M.lookup fp m = do
      B.hPut (fh ^. fhHandle) bs
      let newCount = fh ^. fhPendingCount + B.length bs
      upFun <- case (newCount >= chunk) of
        True -> do
          hFlush (fh ^. fhHandle)
          return $ fhPendingCount .~ 0
        False -> return $ fhPendingCount .~ newCount
      return $! M.insert fp (fh & upFun . (fhCount %~ (+1))) m

    go !m = do
      (r, p) <- (fo ^. fanCreate) fp
      go $! M.insert fp (FileHandle r p fp 0 0) m


    chunk = 1024 * 4

-------------------------------------------------------------------------------
closeHandle :: FileHandle -> IO ()
closeHandle fh = do
    hFlush $ fh ^. fhHandle
    hClose $ fh ^. fhHandle
    fh ^. fhFin


-------------------------------------------------------------------------------
-- | Close a specific file.
fanClose :: FanOut -> FilePath -> IO ()
fanClose fo fp = modifyMVar_ (fo ^. fanFiles) $ \ m -> case m ^. at fp of
  Nothing -> return m
  Just fh -> do
      closeHandle fh
      return $! m & at fp .~ Nothing


-------------------------------------------------------------------------------
-- | Close all files. The same FanOut can be used after this, which
-- would spawn new processes to write into files.
fanCloseAll :: FanOut -> IO ()
fanCloseAll fo = modifyMVar_ (fo ^. fanFiles) $ \m -> do
  forM_ (M.toList m) $ \ (_fp, fh) -> closeHandle fh
  return M.empty


-------------------------------------------------------------------------------
-- | Grab # of writes into each file so far.
fanStats :: FanOut -> IO (M.Map FilePath Int)
fanStats fo = do
    m <- modifyMVar (fo ^. fanFiles) $ \ m -> return (m,m)
    return $ M.map (^. fhCount) m


-------------------------------------------------------------------------------
-- | Sink a stream into 'FanOut'.
sinkFanOut :: FanOutSink
sinkFanOut dispatch conv fo = C.foldM go 0
    where
      go !i a = do
          bs <- conv a
          liftIO (fanWrite fo (dispatch a) bs)
          return $! i + 1


-------------------------------------------------------------------------------
-- | A fanout that keeps only a single file open at a time. Each time
-- the target filename changes, this will close/finalize the file and
-- start the new file.
sequentialSinkFanout :: FanOutSink
sequentialSinkFanout dispatch conv fo =
    liftM fst $ C.foldM go (0, Nothing)
    where
      go (!i, !chunk0) a = do
          bs <- conv a
          let fp = dispatch a

          let goNew = do
                tk <- liftIO (randomToken 16)
                let fp' = fp & basename %~ (<> "_" <> tk)
                liftIO $ fanWrite fo fp' bs
                return $! (i+1, Just (FileChunk fp fp' (B.length bs)))

          case chunk0 of
            Nothing -> goNew
            Just c@FileChunk{..} -> case fp == _chunkOrig of
              False -> do
                liftIO $ fanClose fo _chunkTarget
                goNew
              True -> do
                liftIO $ fanWrite fo _chunkTarget bs
                return $! (i+1, Just $! c & chunkCnt %~ (+ (B.length bs)))



type FanOutSink = MonadIO m => (a -> FilePath) -> (a -> m B.ByteString) -> FanOut -> Consumer a m Int


-------------------------------------------------------------------------------
test :: IO ()
test = do
    fo <- mkFanOut
      (\ fp -> (,) <$> openFile fp AppendMode <*> pure (return ()))
    fanWrite fo "test1" "foo"
    fanWrite fo "test1" "bar"
    fanWrite fo "test1" "tak"
    print =<< fanStats fo
    fanCloseAll fo
    fanWrite fo "test1" "tak"



