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
    ) where

-------------------------------------------------------------------------------
import           Control.Applicative
import           Control.Concurrent.MVar
import           Control.Lens
import           Control.Monad
import qualified Data.ByteString.Char8   as B
import qualified Data.Map.Strict         as M
import           System.IO
-------------------------------------------------------------------------------


data FanOut = FanOut {
      _fanFiles  :: MVar (M.Map FilePath Handle)
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
    go m | Just h <- M.lookup fp m = B.hPut h bs >> return m
    go m = do
      r <- (fo ^. fanCreate) fp
      go $! M.insert fp r m


-------------------------------------------------------------------------------
-- | Close a specific file.
fanClose :: FanOut -> FilePath -> IO ()
fanClose fo fp = modifyMVar_ (fo ^. fanFiles) $ \ m -> case m ^. at fp of
  Nothing -> return m
  Just h -> hClose h >> (return $! m & at fp .~ Nothing)


-------------------------------------------------------------------------------
-- | Close all files. The same FanOut can be used after this, which
-- would spawn new processes to write into files.
fanCloseAll :: FanOut -> IO ()
fanCloseAll fo = modifyMVar_ (fo ^. fanFiles) $ \m -> do
  forM_ (M.toList m) $ \ (_fp, h) -> hClose h
  return M.empty


