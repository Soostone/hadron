{-# LANGUAGE BangPatterns              #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE RankNTypes                #-}
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
    , Writer (..), noopWriter
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


-------------------------------------------------------------------------------
-- | External data writer packed with a finalization action.
data Writer = forall p. Writer {
      writerObject :: p
    , writerClose  :: p -> IO ()
    }

-------------------------------------------------------------------------------
-- | Use when there's no external state associated with the Handle,
-- such as when using 'openFile'.
noopWriter :: Writer
noopWriter = Writer () (const $ return ())


-- | An open file handle
data FileHandle = FileHandle {
      _fhHandle       :: Handle
    , _fhWriter       :: Writer
    , _fhPath         :: FilePath
    , _fhCount        :: !Int
    , _fhPendingCount :: !Int
    }
makeLenses ''FileHandle


-- | Concurrent multi-output manager.
data FanOut = FanOut {
      _fanFiles    :: MVar (M.Map FilePath FileHandle)
    , _fanCreate   :: FilePath -> IO (Handle, Writer)
    , _fanFinalize :: FilePath -> IO ()
    }
makeLenses ''FanOut


-------------------------------------------------------------------------------
-- | Make a new fanout manager that will use given process creator.
-- Process is expected to pipe its stdin into the desired location.
mkFanOut
    :: (FilePath -> IO (Handle, Writer))
    -- ^ Open a handle for a given target path
    -> (FilePath -> IO ())
    -- ^ Finalize hook after handle for target path has been closed.
    -> IO FanOut
mkFanOut f fin = FanOut <$> newMVar M.empty <*> pure f <*> pure fin


-------------------------------------------------------------------------------
-- | Write into a file. A new process will be spawned if this is the
-- first time writing into this file.
fanWrite :: FanOut -> FilePath -> B.ByteString -> IO ()
fanWrite fo fp bs = modifyMVar_ (fo ^. fanFiles) go
  where

    go !m | Just fh <- M.lookup fp m = do
      B.hPut (fh ^. fhHandle) bs
      let newCount = fh ^. fhPendingCount + B.length bs
      upFun <- case (newCount >= 4096) of
        True -> do
          hFlush (fh ^. fhHandle)
          return $ fhPendingCount .~ 0
        False -> return $ fhPendingCount .~ newCount
      return $! M.insert fp (fh & upFun . (fhCount %~ (+1))) m

    go !m = do
      (r, p) <- (fo ^. fanCreate) fp
      go $! M.insert fp (FileHandle r p fp 0 0) m


-------------------------------------------------------------------------------
closeHandle :: FanOut -> FileHandle -> IO ()
closeHandle fo fh = do
    hFlush $ fh ^. fhHandle
    hClose $ fh ^. fhHandle
    case fh ^. fhWriter of
      Writer h fin -> fin h
    (fo ^. fanFinalize) (fh ^. fhPath)


-------------------------------------------------------------------------------
-- | Close a specific file.
fanClose :: FanOut -> FilePath -> IO ()
fanClose fo fp = modifyMVar_ (fo ^. fanFiles) $ \ m -> case m ^. at fp of
  Nothing -> return m
  Just fh -> do
      closeHandle fo fh
      return $! m & at fp .~ Nothing


-------------------------------------------------------------------------------
-- | Close all files. The same FanOut can be used after this, which
-- would spawn new processes to write into files.
fanCloseAll :: FanOut -> IO ()
fanCloseAll fo = modifyMVar_ (fo ^. fanFiles) $ \m -> do
  forM_ (M.toList m) $ \ (_fp, fh) -> closeHandle fo fh
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


-------------------------------------------------------------------------------
test :: IO ()
test = do
    fo <- mkFanOut
      (\ fp -> (,) <$> openFile fp AppendMode <*> pure noopWriter)
      (const (return ()))
    fanWrite fo "test1" "foo"
    fanWrite fo "test1" "bar"
    fanWrite fo "test1" "tak"
    print =<< fanStats fo
    fanCloseAll fo
    fanWrite fo "test1" "tak"



