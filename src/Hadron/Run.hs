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

    , hdfsTempFilePath
    , hdfsFileExists
    , hdfsDeletePath
    , hdfsLs
    , hdfsPut
    , hdfsMkdir
    , hdfsCat
    , hdfsGet
    , hdfsLocalStream

    , randomRemoteFile
    , randomLocalFile

    , L.LocalFile (..)
    , L.randomFileName
    , withLocalFile
    , withRandomLocalFile

    , module Hadron.Run.FanOut
    , hdfsFanOut

    ) where


-------------------------------------------------------------------------------
import           Control.Error
import           Control.Lens
import           Control.Monad
import           Control.Monad.Morph
import           Control.Monad.Trans
import           Control.Monad.Trans.Resource
import qualified Data.ByteString.Char8        as B
import           Data.Conduit
import           Data.Conduit.Binary          (sourceFile)
import           Data.Text                    (Text)
import           System.Directory
import           System.FilePath.Posix
-------------------------------------------------------------------------------
import           Hadron.Run.FanOut
import qualified Hadron.Run.Hadoop            as H
import           Hadron.Run.Local             (LocalFile (..))
import qualified Hadron.Run.Local             as L
import           Hadron.Utils
-------------------------------------------------------------------------------


-------------------------------------------------------------------------------
-- | Dispatch on the type of run
data RunContext
    = LocalRun L.LocalRunSettings
    -- ^ Development mode: Emulate a hadoop run locally on this
    -- machine
    | HadoopRun H.HadoopEnv L.LocalRunSettings
    -- ^ Production mode: Actually run on hadoop. However, some
    -- utilites use local facilities so we still force you to have a
    -- local policy.
makePrisms ''RunContext


lset :: RunContext -> L.LocalRunSettings
lset (LocalRun s) = s
lset (HadoopRun _ s) = s


-------------------------------------------------------------------------------
launchMapReduce
    :: (Functor m, MonadIO m)
    => RunContext
    -> String
    -> String
    -> H.HadoopRunOpts
    -> ExceptT Text m ()
launchMapReduce (LocalRun env) mrKey token opts =
    ExceptT . L.runLocal env . runExceptT $ (L.localMapReduce env mrKey token opts)
launchMapReduce (HadoopRun env _) mrKey token opts =
    H.hadoopMapReduce env mrKey token opts


-------------------------------------------------------------------------------
hdfsFileExists :: RunContext -> FilePath -> IO Bool
hdfsFileExists (LocalRun env) fp = L.runLocal env (L.hdfsFileExists (LocalFile fp))
hdfsFileExists (HadoopRun env _) fp = H.hdfsFileExists env fp


-------------------------------------------------------------------------------
hdfsDeletePath :: RunContext -> FilePath -> IO ()
hdfsDeletePath rc fp = case rc of
    LocalRun lrs -> L.runLocal lrs (L.hdfsDeletePath (LocalFile fp))
    HadoopRun he _ -> H.hdfsDeletePath he fp


-------------------------------------------------------------------------------
hdfsLs :: RunContext -> FilePath -> IO [File]
hdfsLs rc fp = case rc of
    LocalRun lrs -> L.runLocal lrs (L.hdfsLs (LocalFile fp))
    HadoopRun he _ -> H.hdfsLs he fp


-------------------------------------------------------------------------------
hdfsPut :: RunContext -> L.LocalFile -> FilePath -> IO ()
hdfsPut rc f1 f2 = case rc of
    LocalRun lrs -> L.runLocal lrs (L.hdfsPut f1 (LocalFile f2))
    HadoopRun e _ -> withLocalFile rc f1 $ \ lf -> H.hdfsPut e lf f2


-------------------------------------------------------------------------------
hdfsFanOut
    :: RunContext
    -> FilePath
    -- ^ A temporary folder where in-progress files can be placed
    -> IO FanOut
hdfsFanOut rc tmp = case rc of
    LocalRun lcs -> L.runLocal lcs $ L.hdfsFanOut tmp
    HadoopRun e _ -> H.hdfsFanOut e tmp


-------------------------------------------------------------------------------
hdfsMkdir :: RunContext -> FilePath -> IO ()
hdfsMkdir rc fp = case rc of
    LocalRun lcs -> L.runLocal lcs (L.hdfsMkdir (LocalFile fp))
    HadoopRun he _ -> H.hdfsMkdir he fp


-------------------------------------------------------------------------------
hdfsCat
    :: RunContext
    -> FilePath
    -> Producer (ResourceT IO) B.ByteString
hdfsCat rc fp = case rc of
    LocalRun lcs -> hoist (hoist (L.runLocal lcs)) $ L.hdfsCat (LocalFile fp)
    HadoopRun{} -> hdfsLocalStream rc fp


-------------------------------------------------------------------------------
-- | Copy a file from HDFS into local.
hdfsGet :: RunContext -> FilePath -> IO LocalFile
hdfsGet rc fp = do
    local <- L.randomFileName
    case rc of
      LocalRun _ -> return (LocalFile fp)
      HadoopRun h _ -> do
        withLocalFile rc local $ \ lf -> H.hdfsGet h fp lf
        return local


-------------------------------------------------------------------------------
-- | Copy a file down to local FS, then stream its content.
hdfsLocalStream
    :: RunContext
    -> FilePath
    -> Producer (ResourceT IO) B.ByteString
hdfsLocalStream env fp = case env of
    LocalRun{} -> hdfsCat env fp
    HadoopRun _ _ -> do
      random <- liftIO $ hdfsGet env fp
      withLocalFile env random $ \ local -> do
        register $ removeFile local
        sourceFile local



-- -------------------------------------------------------------------------------
-- -- | Stream contents of a folder one by one from HDFS.
-- hdfsLocalStreamMulti
--     :: (MonadIO m, MonadThrow m, MonadBase base m, PrimMonad base)
--     => HadoopEnv
--     -> FilePath
--     -- ^ Location / glob pattern
--     -> (FilePath -> Bool)
--     -- ^ File filter based on name
--     -> Source m ByteString
-- hdfsLocalStreamMulti hs loc chk = do
--     fs <- liftIO $ hdfsLs hs loc <&> filter chk
--     lfs <- liftIO $ mapConcurrently (hdfsGet hs) fs
--     forM_ (zip lfs fs) $ \ (local, fp) -> do
--         h <- liftIO $ catching _IOException
--              (openFile local ReadMode)
--              (\e ->  error $ "hdfsLocalStream failed with open file: " <> show e)
--         let getFile = sourceHandle h
--         if isSuffixOf "gz" fp
--           then getFile =$= ungzip
--           else getFile
--         liftIO $ do
--           hClose h
--           removeFile local


randomLocalFile :: MonadIO m => m LocalFile
randomLocalFile = L.randomFileName


randomRemoteFile :: RunContext -> IO FilePath
randomRemoteFile env = case env of
    LocalRun{} -> _unLocalFile `liftM` L.randomFileName
    HadoopRun e _ -> H.randomFilename e


-------------------------------------------------------------------------------
-- | Given a filename, produce an HDFS path for me in our temporary folder.
hdfsTempFilePath :: MonadIO m => RunContext -> FilePath -> m FilePath
hdfsTempFilePath env fp = case env of
    LocalRun{} -> return fp
    HadoopRun{} -> return $ H.tmpRoot </> fp


-------------------------------------------------------------------------------
-- | Helper to work with relative paths using Haskell functions like
-- 'readFile' and 'writeFile'.
withLocalFile
  :: MonadIO m => RunContext -> LocalFile -> (FilePath -> m b) -> m b
withLocalFile rs fp f = L.withLocalFile (lset rs) fp f


-------------------------------------------------------------------------------
withRandomLocalFile :: MonadIO m => RunContext -> (FilePath -> m b) -> m LocalFile
withRandomLocalFile rc f = do
    fp <- randomLocalFile
    withLocalFile rc fp f
    return fp
