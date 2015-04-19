{-# LANGUAGE RankNTypes #-}
module Hadron.Conduit where

-------------------------------------------------------------------------------
import           Control.Concurrent.Chan
import           Control.Monad.Trans
import           Data.Conduit
-------------------------------------------------------------------------------


-------------------------------------------------------------------------------
sourceChan :: MonadIO m => Chan (Maybe a) -> Producer m a
sourceChan ch = go
    where
      go = do
          res <- liftIO $ readChan ch
          case res of
            Nothing -> return ()
            Just a -> yield a >> go



-------------------------------------------------------------------------------
peek :: Monad m => Consumer i m (Maybe i)
peek = do
    res <- await
    case res of
      Nothing -> return Nothing
      Just a -> leftover a >> return (Just a)


