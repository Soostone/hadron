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
import           System.FilePath.Lens
import           System.FilePath.Posix
import           System.Posix.Env
import           System.Process
-------------------------------------------------------------------------------
import           Hadron.Logger
import qualified Hadron.Run.Hadoop            as H
-------------------------------------------------------------------------------


newtype LocalFile = LocalFile { _unLocalFile :: FilePath }
    deriving (Eq,Show,Read,Ord)
makeLenses ''LocalFile


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
path :: (MonadIO m, MonadReader LocalRunSettings m) => LocalFile -> m FilePath
path (LocalFile fp) = do
    root <- view lrsTempPath
    let p = root </> fp
        dir = p ^. directory
    liftIO $ createDirectoryIfMissing True dir
    return p


-------------------------------------------------------------------------------
localMapReduce
    :: MonadIO m
    => String                   -- ^ MapReduceKey
    -> String                   -- ^ RunToken
    -> H.HadoopRunOpts
    -> EitherT String m ()
localMapReduce mrKey token H.HadoopRunOpts{..} = do
    exPath <- scriptIO getExecutablePath
    liftIO $ infoM "Hadron.Run.Local" $
      "Launching Hadoop job for MR key: " <> mrKey


    expandedInput <- liftIO $ forM mrsInput $ \ inp -> do
      chk <- doesDirectoryExist inp
      case chk of
        False -> return [inp]
        True -> do
          fs <- getDirectoryContents inp
          return $ map (inp </>)
                 $ filter (not . flip elem [".", ".."]) fs


    outFile <- liftIO $ case mrsOutput ^. extension . to null of
      False -> return mrsOutput
      True -> do
          createDirectoryIfMissing True mrsOutput
          return $ mrsOutput </> "0000.out"


    let infiles = intercalate " " $ concat expandedInput

        command = "cat " <> infiles <> " | " <>
          exPath <> " " <> token <> " " <> "mapper_" <> mrKey <> " | " <>
          "sort" <> " | " <>
          exPath <> " " <> token <> " " <> "reducer_" <> mrKey <>
          " > " <> outFile

    liftIO $ infoM "Hadron.Run.Local" $
      "Executing local command: " ++ show command

    -- TODO: We must actually map over each file individually set this
    -- env variable to each file's name at each step. This may break
    -- some MR programs that rely on accurately knowing the name of
    -- the file on which they are operating.
    scriptIO $ setEnv "map_input_file" infiles True

    res <- scriptIO $ system command
    case res of
      ExitSuccess -> liftIO $ infoM "Hadron.Run.Local" "Stage complete."
      e -> do
        liftIO . errorM "Hadron.Run.Local" $ "Stage failed: " ++ show e
        hoistEither $ Left $ "Stage failed with: " ++ show e


-------------------------------------------------------------------------------
-- | Check if the target file is present.
hdfsFileExists
    :: (MonadIO m, MonadReader LocalRunSettings m)
    => LocalFile
    -> m Bool
hdfsFileExists p = liftIO . doesFileExist =<< path p


-------------------------------------------------------------------------------
hdfsDeletePath
    :: (MonadIO m, MonadReader LocalRunSettings m)
    => LocalFile
    -> m ()
hdfsDeletePath p = liftIO . removeDirectoryRecursive =<< path p


-------------------------------------------------------------------------------
hdfsLs
    :: (MonadIO m, MonadReader LocalRunSettings m)
    => LocalFile -> m [FilePath]
hdfsLs p = liftIO . getDirectoryContents =<< path p


-------------------------------------------------------------------------------
hdfsPut
    :: (MonadIO m, MonadReader LocalRunSettings m)
    => LocalFile
    -> LocalFile
    -> m ()
hdfsPut src dest = do
    src' <- path src
    dest' <- path dest
    liftIO $ copyFile src' dest'


-------------------------------------------------------------------------------
hdfsMkdir
    :: (MonadIO m, MonadReader LocalRunSettings m)
    => LocalFile
    -> m ()
hdfsMkdir p = liftIO . createDirectoryIfMissing True =<< path p


-------------------------------------------------------------------------------
hdfsCat :: LocalFile -> Producer (ResourceT Local) B.ByteString
hdfsCat p = sourceFile =<< (lift . lift) (path p)


-------------------------------------------------------------------------------
hdfsGet
    :: (MonadIO m, MonadReader LocalRunSettings m)
    => LocalFile
    -> m LocalFile
hdfsGet fp = do
    target <- randomFileName
    hdfsPut fp target
    return target



hdfsLocalStream :: LocalFile -> Producer (ResourceT Local) B.ByteString
hdfsLocalStream = hdfsCat


randomFileName :: MonadIO m => m LocalFile
randomFileName = (LocalFile . B.unpack) `liftM` liftIO (mkRNG >>= randomToken 64)






