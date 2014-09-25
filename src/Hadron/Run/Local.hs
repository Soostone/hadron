{-# LANGUAGE FlexibleContexts          #-}
{-# LANGUAGE MultiParamTypeClasses     #-}
{-# LANGUAGE NoMonomorphismRestriction #-}
{-# LANGUAGE OverloadedStrings         #-}
{-# LANGUAGE RankNTypes                #-}
{-# LANGUAGE RecordWildCards           #-}
{-# LANGUAGE TemplateHaskell           #-}

-----------------------------------------------------------------------------
-- |
-- Module      :
-- Copyright   :
-- License     :
--
-- Maintainer  :
-- Stability   :  experimental
--
-- Emulate all hadoop operations locally
----------------------------------------------------------------------------

module Hadron.Run.Local where

-------------------------------------------------------------------------------
import           Control.Error
import           Control.Lens
import           Control.Monad.Reader
import           Control.Monad.Trans.Resource
import qualified Data.ByteString.Char8        as B
import           Data.Conduit
import           Data.Conduit.Binary
import           Data.Default
import           Data.List
import           Data.Monoid
import           Data.RNG
import           System.Directory
import           System.Environment
import           System.Exit
import           System.Process
-------------------------------------------------------------------------------
import           Hadron.Logger
import qualified Hadron.Run.Hadoop            as H
-------------------------------------------------------------------------------


data LocalRunSettings = LocalRunSettings {
      _lrsTempPath :: FilePath
    -- ^ Root of the "file system" during a localrun
    }
makeLenses ''LocalRunSettings


instance Default LocalRunSettings where
    def = LocalRunSettings "tmp"


type Local = ReaderT LocalRunSettings IO

runLocal :: r -> ReaderT r m a -> m a
runLocal env f = runReaderT f env

-------------------------------------------------------------------------------
path :: (MonadIO m, MonadReader LocalRunSettings m) => FilePath -> m FilePath
path fp = do
    root <- view lrsTempPath
    liftIO $ createDirectoryIfMissing True root
    return $ root ++ fp


-------------------------------------------------------------------------------
localMapReduce
    :: MonadIO m
    => String                   -- ^ MapReduceKey
    -> String                   -- ^ RunToken
    -> H.HadoopRunOpts
    -> EitherT String m ()
localMapReduce mrKey token H.HadoopRunOpts{..} = do
    exec <- scriptIO getExecutablePath
    prog <- scriptIO getProgName
    liftIO $ infoM "Hadron.Run.Local" $
      "Launching Hadoop job for MR key: " <> mrKey

    let infiles = intercalate " " mrsInput

        command = "cat " <> infiles <> " | " <>
          "./" <> prog <> " " <> token <> " " <> "mapper_" <> mrKey <> " | " <>
          "sort" <> " " <>
          "./" <> prog <> " " <> token <> " " <> "reducer_" <> mrKey <> " | " <>
          "> " <> mrsOutput

    liftIO $ infoM "Hadron.Run.Local" $
      "Executing local command: " ++ show command

    res <- scriptIO $ system command
    case res of
      ExitSuccess -> liftIO $ infoM "Hadron.Run.Local" "Stage complete."
      e -> do
        liftIO . errorM "Hadron.Run.Local" $ "Stage failed: " ++ show e
        hoistEither $ Left $ "Stage failed with: " ++ show e







-------------------------------------------------------------------------------
-- | Check if the target file is present.
hdfsFileExists
  :: (MonadIO m, MonadReader LocalRunSettings m) =>
     FilePath -> m Bool
hdfsFileExists p = liftIO . doesFileExist =<< path p


-------------------------------------------------------------------------------
hdfsDeletePath
    :: (MonadIO m, MonadReader LocalRunSettings m)
    => FilePath -> m ()
hdfsDeletePath p = liftIO . removeDirectoryRecursive =<< path p


-------------------------------------------------------------------------------
hdfsLs
    :: (MonadIO m, MonadReader LocalRunSettings m)
    => FilePath -> m [FilePath]
hdfsLs p = liftIO . getDirectoryContents =<< path p


-------------------------------------------------------------------------------
hdfsPut
    :: (MonadIO m, MonadReader LocalRunSettings m)
    => FilePath -> FilePath -> m ()
hdfsPut fr to = do
    fr' <- path fr
    to' <- path to
    liftIO $ copyFile fr' to'


-------------------------------------------------------------------------------
hdfsMkdir
    :: (MonadIO m, MonadReader LocalRunSettings m)
    => FilePath
    -> m ()
hdfsMkdir p = liftIO . createDirectoryIfMissing True =<< path p


-------------------------------------------------------------------------------
hdfsCat :: FilePath -> Producer (ResourceT Local) B.ByteString
hdfsCat p = sourceFile =<< (lift . lift) (path p)


-------------------------------------------------------------------------------
hdfsGet
    :: (MonadIO m, MonadReader LocalRunSettings m)
    => FilePath
    -> m FilePath
hdfsGet fp = do
    target <- randomFilename
    hdfsPut fp target
    return target



hdfsLocalStream :: FilePath -> Producer (ResourceT Local) B.ByteString
hdfsLocalStream = hdfsCat


------------------------------------------------------------------------------
-- | Generates a random filename in the /tmp/hadron directory.
randomFilename :: (MonadIO m, MonadReader LocalRunSettings m) => m FilePath
randomFilename = do
    tk <- liftIO $ mkRNG >>= randomToken 64
    path ("/tmp/" ++ B.unpack tk)





