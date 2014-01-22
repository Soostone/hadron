{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Hadron.Logger
    ( module Hadron.Logger
    , module Control.Monad.Logger
    ) where

-------------------------------------------------------------------------------
import           Control.Concurrent
import           Control.Monad.Logger
import           Control.Monad.Trans
import           Data.List
import           Data.Maybe
import qualified Data.Text                  as T
import           Language.Haskell.TH.Syntax (Loc (..))
import           System.Environment
import           System.IO
import           System.Log.FastLogger
import           System.Posix.Process
-------------------------------------------------------------------------------


instance ToLogStr LogStr where
    toLogStr = id


logTo :: MonadIO m => Handle -> (LoggingT m a) -> m a
logTo h m = do
    le <- mkLogEnv h
    runLoggingT m (\ loc ls ll str -> withLogger le loc ls ll str)


mkLogEnv :: MonadIO m => Handle -> m LogEnv
mkLogEnv lh = do
    logger <- liftIO $ mkLogger True lh
    pid <- liftIO getProcessID
    prg <- liftIO getProgName
    return $ LogEnv (T.pack $ show pid) logger (T.pack prg)


data LogEnv = LogEnv {
      leProcessId :: T.Text
    , leLogger    :: Logger
    , leProgName  :: T.Text
    }


-- | Use this function to define 'MonadLogger' instances for various
-- monads.
withLogger
    :: (Show a, MonadIO m, ToLogStr msg)
    => LogEnv
    -> Loc
    -> T.Text
    -> a
    -> msg
    -> m ()
withLogger LogEnv{..} loc ls ll msg  = do
    tm <- liftIO $ loggerDate leLogger
    tid <- liftIO myThreadId
    liftIO $ loggerPutStr leLogger $ msg' tm tid
  where
    msg' tm tid =
        [ toLogStr tm
        , toLogStr $ T.pack " - "
        , toLogStr $ T.concat [leProgName, brackets leProcessId, showTid tid]
        , toLogStr $ T.pack " - "
        , toLogStr (drop 5 $ show ll)
        , toLogStr $ T.pack " - "
        , toLogStr fileLocStr
        , toLogStr $ T.pack " - "
        , toLogStr $ if T.length ls > 0 then T.concat [ls, ": "] else ""
        , toLogStr msg
        , toLogStr $ T.pack "\n"]

    -- taken from file-location package
    -- turn the TH Loc loaction information into a human readable string
    -- leaving out the loc_end parameter
    fileLocStr = T.pack $
        "(" ++ (loc_filename loc) ++ ':' : (line loc) ++ ':' : (char loc) ++
        ")"
      where
        line = show . fst . loc_start
        char = show . snd . loc_start


    chop x = let y = fromMaybe x $ stripPrefix "ThreadId " x
             in y

    showTid tid = T.pack $ "[" ++ chop (show tid) ++ "]"
    brackets x = T.concat ["[", x, "]"]
