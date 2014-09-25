{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Hadron.Logger
    ( module Hadron.Logger
    , module System.Log.Logger
    ) where


-------------------------------------------------------------------------------
import           Control.Concurrent
import           Control.Monad.Trans
import           Data.List
import           Data.Maybe
import qualified Data.Text                  as T
import           Language.Haskell.TH.Syntax (Loc (..))
import           System.IO
import           System.Log.Formatter
import           System.Log.Handler         (setFormatter)
import           System.Log.Handler.Simple
import           System.Log.Logger
-------------------------------------------------------------------------------



-------------------------------------------------------------------------------
-- | Setup given handler for logging under given logger name.
setLogHandle :: String -> Handle -> Priority -> IO ()
setLogHandle logNm h lvl = do
    lh <- streamHandler h lvl >>= \ lh ->
         return (setFormatter lh (simpleLogFormatter "[$time : $loggername : $prio] $msg"))
    updateGlobalLogger logNm (setHandlers [lh])
    -- updateGlobalLogger rootLoggerName (addHandler lh)


-------------------------------------------------------------------------------
enableDebugLog :: IO ()
enableDebugLog = updateGlobalLogger rootLoggerName (setLevel DEBUG)



-- | Use this function to define 'MonadLogger' instances for various
-- monads in Dyna.
withLogger
    :: (Show a, MonadIO m)
    => String
    -- ^ Logger name
    -> Loc
    -- ^ Location in file
    -> T.Text
    -> a
    -- ^
    -> String
    -- ^ A custom message
    -> m ()
withLogger nm loc ls ll msg = do
    tid <- liftIO myThreadId
    liftIO $ infoM nm (msg' tid)
  where
    msg' tid = concat
        [ showTid tid
        , " - "
        , (drop 5 $ show ll)
        , " - "
        , fileLocStr
        , " - "
        , if T.length ls > 0 then concat [T.unpack ls, ": "] else ""
        , msg
        , "\n"]

    -- taken from file-location package
    -- turn the TH Loc loaction information into a human readable string
    -- leaving out the loc_end parameter
    fileLocStr =
        "(" ++ (loc_filename loc) ++ ':' : (line loc) ++ ':' : (char loc) ++
        ")"
      where
        line = show . fst . loc_start
        char = show . snd . loc_start


    chop x = let y = fromMaybe x $ stripPrefix "ThreadId " x
             in y

    showTid tid = "[" ++ chop (show tid) ++ "]"
