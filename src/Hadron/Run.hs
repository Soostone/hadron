{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE TemplateHaskell       #-}

-----------------------------------------------------------------------------
-- |
-- Module      :
-- Copyright   :
-- License     :
--
-- Maintainer  :
-- Stability   :  experimental
--
-- An operational run layer that either passes commands on to hadoop
-- or runs things locally.
----------------------------------------------------------------------------


module Hadron.Run
    ( RunContext (..)
    , L.LocalRunSettings (..)
    , H.HadoopEnv (..)
    , H.clouderaDemo
    , H.amazonEMR

    , H.PartitionStrategy (..)
    , H.numSegs
    , H.eqSegs
    , H.Comparator (..)

    , H.HadoopRunOpts (..)
    , H.mrSettings

    , H.Codec
    , H.gzipCodec
    , H.snappyCodec

    , launchMapReduce

    , hdfsRandomFilename
    , mkTempFilePath
    , hdfsFileExists
    , hdfsDeletePath
    , hdfsLs
    , hdfsPut
    , hdfsMkdir
    , hdfsCat
    , hdfsGet
    , hdfsLocalStream
    ) where


-------------------------------------------------------------------------------
import           Control.Error
import           Control.Lens
import           Control.Monad.Morph
import           Control.Monad.Trans
import           Control.Monad.Trans.Resource
import qualified Data.ByteString.Char8        as B
import           Data.Conduit
import           System.FilePath.Posix
-------------------------------------------------------------------------------
import qualified Hadron.Run.Hadoop            as H
import qualified Hadron.Run.Local             as L
-------------------------------------------------------------------------------


-------------------------------------------------------------------------------
-- | Dispatch on the type of run
data RunContext
    = LocalRun L.LocalRunSettings
    -- ^ Development mode: Emulate a hadoop run locally on this
    -- machine
    | HadoopRun H.HadoopEnv
    -- ^ Production mode: Actually run on hadoop.
makePrisms ''RunContext



-------------------------------------------------------------------------------
launchMapReduce
    :: MonadIO m
    => RunContext
    -> String
    -> String
    -> H.HadoopRunOpts
    -> EitherT String m ()
launchMapReduce (LocalRun env) mrKey token opts =
    EitherT . L.runLocal env . runEitherT $ (L.localMapReduce mrKey token opts)
launchMapReduce (HadoopRun env) mrKey token opts =
    H.hadoopMapReduce env mrKey token opts


-------------------------------------------------------------------------------
hdfsFileExists (LocalRun env) fp = L.runLocal env (L.hdfsFileExists fp)
hdfsFileExists (HadoopRun env) fp = H.hdfsFileExists env fp


-------------------------------------------------------------------------------
hdfsDeletePath :: RunContext -> FilePath -> IO ()
hdfsDeletePath env fp = case env of
    LocalRun env -> L.runLocal env (L.hdfsDeletePath fp)
    HadoopRun env -> H.hdfsDeletePath env fp

-------------------------------------------------------------------------------
hdfsLs :: RunContext -> FilePath -> IO [FilePath]
hdfsLs env fp = case env of
    LocalRun env -> L.runLocal env (L.hdfsLs fp)
    HadoopRun env -> H.hdfsLs env fp


hdfsPut :: RunContext -> FilePath -> FilePath -> IO ()
hdfsPut env f1 f2 = case env of
    LocalRun env -> L.runLocal env (L.hdfsPut f1 f2)
    HadoopRun env -> H.hdfsPut env f1 f2


-------------------------------------------------------------------------------
hdfsMkdir env fp = case env of
    LocalRun env -> L.runLocal env (L.hdfsMkdir fp)
    HadoopRun env -> H.hdfsMkdir env fp


-------------------------------------------------------------------------------
hdfsCat
    :: RunContext
    -> FilePath
    -> Producer (ResourceT IO) B.ByteString
hdfsCat env fp = case env of
    LocalRun env -> hoist (hoist (L.runLocal env)) (L.hdfsCat fp)
    HadoopRun env -> H.hdfsCat env fp


-------------------------------------------------------------------------------
hdfsGet env fp = case env of
    LocalRun env -> L.runLocal env (L.hdfsGet fp)
    HadoopRun env -> H.hdfsGet env fp


-------------------------------------------------------------------------------
hdfsLocalStream
    :: RunContext
    -> FilePath
    -> ConduitM i B.ByteString (ResourceT IO) ()
hdfsLocalStream env fp = case env of
    LocalRun{} -> hdfsCat env fp
    HadoopRun env -> H.hdfsLocalStream env fp


-------------------------------------------------------------------------------
hdfsRandomFilename :: RunContext -> IO FilePath
hdfsRandomFilename env = case env of
    LocalRun env -> L.runLocal env L.randomFilename
    HadoopRun _ -> H.randomFilename


-------------------------------------------------------------------------------
mkTempFilePath :: MonadIO m => RunContext -> FilePath -> m FilePath
mkTempFilePath env fp = case env of
    LocalRun env -> L.runLocal env (L.path fp)
    HadoopRun{} -> return $ H.tmpRoot </> fp


