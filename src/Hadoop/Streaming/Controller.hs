{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE EmptyDataDecls             #-}
{-# LANGUAGE ExistentialQuantification  #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE NoMonomorphismRestriction  #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TemplateHaskell            #-}
{-# LANGUAGE UndecidableInstances       #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Hadoop.Streaming.Controller
-- Copyright   :  Soostone Inc
-- License     :  BSD3
--
-- Maintainer  :  Ozgun Ataman
-- Stability   :  experimental
--
-- High level flow-control of Hadoop programs with ability to define a
-- sequence of Map-Reduce operations in a Monad, have strongly typed
-- data locations.
----------------------------------------------------------------------------

module Hadoop.Streaming.Controller
    (

    -- * Command Line Entry Point
      hadoopMain
    , HadoopSettings (..)
    , MRSettings (..)
    , RerunStrategy (..)
    , clouderaDemo
    , amazonEMR

    -- * Logging Related
    , logTo

    -- * Hadoop Program Construction
    , Controller
    , MapReduce (..)
    , Tap (..)
    , Tap'
    , tap

    -- * Joining Multiple Datasets
    , joinStep
    , DataDefs
    , DataSet
    , JoinType (..)
    , JoinKey

    -- * Control flow operations
    , connect
    , connect'
    , io

    ) where

-------------------------------------------------------------------------------
import           Control.Applicative
import           Control.Error
import           Control.Lens
import           Control.Monad.Operational hiding (view)
import qualified Control.Monad.Operational as O
import           Control.Monad.State
import qualified Data.ByteString.Char8     as B
import           Data.Conduit
import           Data.Conduit.Utils
import           Data.Default
import           Data.Hashable
import           Data.List
import qualified Data.Map                  as M
import           Data.Monoid
import           Data.RNG
import           Data.Serialize
import qualified Data.Text                 as T
import           System.Environment
-------------------------------------------------------------------------------
import           Hadoop.Streaming
import           Hadoop.Streaming.Hadoop
import           Hadoop.Streaming.Join
import           Hadoop.Streaming.Logger
-------------------------------------------------------------------------------



-------------------------------------------------------------------------------
-- | A packaged MapReduce step
data MapReduce a m b = forall v. MapReduce {
      mrOptions :: MROptions v
    , mrMapper  :: Mapper a m v
    , mrReducer :: Reducer v m b
    }


-- | The hadoop-understandable location of a datasource
type Location = String


-- | Tap is a data source definition that *knows* how to serve records
-- of tupe 'a'.
--
-- It comes with knowledge on how to serialize ByteString
-- to that type and can be used both as a sink (to save data form MR
-- output) or source (to feed MR programs).
data Tap m a = Tap
    { location :: Location
    , proto    :: Protocol' m a
    }


-- | If two loacitons are the same, we consider two Taps equal.
instance Eq (Tap m a) where
    a == b = location a == location b


-- | It is often just fine to use IO as the base monad for MapReduce ops.
type Tap' a = Tap IO a


-- | Construct a 'DataDef'
tap :: Location -> Protocol' m a -> Tap m a
tap = Tap


data ContState = ContState {
      _csMRCount :: Int
    }

instance Default ContState where
    def = ContState 0


makeLenses ''ContState



-------------------------------------------------------------------------------
data ConI a where
    Connect :: forall i o. MapReduce i IO o
            -> [Tap IO i] -> Tap IO o
            -> ConI ()

    MakeTap :: Serialize a => ConI (Tap IO a)

    ConIO :: IO a -> ConI a



-- | All MapReduce steps are integrated in the 'Controller' monad.
newtype Controller a = Controller { unController :: Program ConI a }
    deriving (Functor, Applicative, Monad)



-------------------------------------------------------------------------------
-- | Connect a MapReduce program to a set of inputs, returning the
-- output tap that was implicity generated.
connect' :: Serialize b => MapReduce a IO b -> [Tap IO a] -> Controller (Tap IO b)
connect' mr inp = do
    out <- makeTap
    connect mr inp out
    return out


-------------------------------------------------------------------------------
-- | Connect a typed MapReduce application you will supply with a list
-- of sources and a destination.
connect :: MapReduce a IO b -> [Tap IO a] -> Tap IO b -> Controller ()
connect mr inp outp = Controller $ singleton $ Connect mr inp outp


-------------------------------------------------------------------------------
makeTap :: Serialize a => Controller (Tap IO a)
makeTap = Controller $ singleton MakeTap


-- | LIft IO into 'Controller'. Note that this is a NOOP for when the
-- Mappers/Reducers are running; it only executes in the main
-- controller application during job-flow orchestration.
io :: IO a -> Controller a
io f = Controller $ singleton $ ConIO f


-------------------------------------------------------------------------------
newMRKey :: MonadState ContState m => m String
newMRKey = do
    i <- gets _csMRCount
    csMRCount %= (+1)
    return $! show i



-------------------------------------------------------------------------------
-- | Interpreter for the central job control process
orchestrate
    :: (MonadIO m, MonadLogger m)
    => Controller a
    -> HadoopSettings
    -> MRSettings
    -> RerunStrategy
    -> ContState
    -> m (Either String a)
orchestrate (Controller p) set mrs rr s = evalStateT (runEitherT (go p)) s
    where
      go = eval . O.view

      eval (Return a) = return a
      eval (i :>>= f) = eval' i >>= go . f

      eval' :: (MonadLogger m, MonadIO m) => ConI a -> EitherT String (StateT ContState m) a

      eval' (ConIO f) = liftIO f

      eval' MakeTap = do
          tk <- liftIO $ mkRNG >>= randomToken 64
          let loc = B.unpack $ B.concat ["/tmp/hadoop-streaming/", tk]
          return $ Tap loc serProtocol

      eval' (Connect mr@(MapReduce mro _ _) inp outp) = go'
          where
            go' = do
                chk <- liftIO $ hdfsFileExists set (location outp)
                case chk of
                  False -> go''
                  True ->
                    case rr of
                      RSFail -> lift $ $(logError) $ T.concat
                        ["Destination file exists: ", T.pack (location outp)]
                      RSSkip -> go''
                      RSReRun -> do
                        lift $ $(logInfo) $ T.pack $
                          "Destination file exists, will delete and rerun: " ++
                          location outp
                        liftIO $ hdfsDeletePath set (location outp)
                        go''
            go'' = do
              mrKey <- newMRKey
              launchMapReduce set mrKey
                mrs { mrsInput = map location inp
                    , mrsOutput = location outp
                    , mrsPart = mroPart mro }



data Phase = Map | Reduce


-------------------------------------------------------------------------------
-- | What to do when we notice that a destination file already exists.
data RerunStrategy
    = RSFail
    -- ^ Fail and log the problem.
    | RSReRun
    -- ^ Delete the file and rerun the analysis
    | RSSkip
    -- ^ Consider the analaysis already done and skip.
    deriving (Eq,Show,Read,Ord)

instance Default RerunStrategy where
    def = RSFail


-------------------------------------------------------------------------------
-- | The main entry point. Use this function to produce a command line
-- program that encapsulates everything.
hadoopMain
    :: forall m a. (MonadThrow m, MonadIO m, MonadLogger m)
    => Controller a
    -> HadoopSettings
    -- ^ Base hadoop configuration
    -> MRSettings
    -- ^ Base 'MRSettings' - jobs will build on this.
    -> RerunStrategy
    -> m ()
hadoopMain c@(Controller p) hs mrs rr = do
    args <- liftIO getArgs
    case args of
      [] -> do
        res <- orchestrate c hs mrs rr def
        liftIO $ either print (const $ putStrLn "Success.") res
      [arg] -> do
        evalStateT (interpretWithMonad (go arg) p) def
        return ()
      _ -> error "Usage: No arguments for job control or a phase name."
    where

      mkArgs mrKey = [ (Map, "map_" ++ mrKey)
                     , (Reduce, "reduce_" ++ mrKey) ]


      go :: String -> ConI b -> StateT ContState m b

      go _ (ConIO _) = return $ error "You tried to use the result of an IO action during Map-Reduce operation. That's illegal."

      go _ MakeTap = do
          tk <- liftIO $ mkRNG >>= randomToken 64
          let loc = B.unpack $ B.concat ["/tmp/hadoop-streaming/", tk]
          return $ Tap loc serProtocol

      go arg (Connect (MapReduce mro mp rd) inp outp) = do
          mrKey <- newMRKey
          case find ((== arg) . snd) $ mkArgs mrKey of
            Just (Map, _) -> do
              let inSer = proto $ head inp
                  logIn _ = liftIO $ hsEmitCounter "Map rows decoded" 1
              liftIO $ (mapperWith (mroInPrism mro) $
                protoDec inSer =$= performEvery 1 logIn =$= mp)
            Just (Reduce, _) -> do
              let outSer = proto outp
                  rd' = rd =$= protoEnc outSer
              liftIO $ (reducerMain mro rd')
            Nothing -> return ()



-- | TODO: See if this works. Objective is to increase type safety of
-- join inputs.
--
-- A join definition that ultimately produces objects of type b.
data JoinDef m b = forall a. JoinDef {
      joinTap  :: Tap m a
    , joinType :: JoinType
    , joinMap  :: Conduit a m (JoinKey, b)
    }


-------------------------------------------------------------------------------
-- | A convenient way to express multi-way join operations into a
-- single data type.
joinStep
    :: forall m b a. (Show b, MonadThrow m, Monoid b, MonadIO m,
                      Serialize b)
    => [(Tap m a, JoinType, Conduit a m (JoinKey, b))]
    -- ^ Dataset definitions and how to map each dataset.
    -> MapReduce a m b
joinStep fs = MapReduce joinOpts mp rd
    where
      salt = 0
      showBS = B.pack . show

      names :: [(Location, DataSet)]
      names = map (\ (i, loc) -> (loc, DataSet $ B.concat [showBS i, ":",  showBS $ hashWithSalt salt loc])) $
              zip [0..] locations

      nameIx :: M.Map Location DataSet
      nameIx = M.fromList names

      tapIx :: M.Map DataSet (Tap m a)
      tapIx = M.fromList $ zip (map snd names) (map (view _1) fs)


      locations :: [Location]
      locations = map (location . view _1) fs


      getTapDS :: Tap m a -> DataSet
      getTapDS t =
          fromMaybe (error "Can't identify dataset name for given location") $
          M.lookup (location t) nameIx


      fs' :: [(DataSet, JoinType)]
      fs' = map (\ (t, jt, _) -> (getTapDS t, jt)) fs


      -- | get dataset name from a given input filename
      getDS nm = fromMaybe (error "Can't identify current tap from filename.") $ do
        loc <- find (flip isInfixOf nm) locations
        name <- M.lookup loc nameIx
        return name


      -- | get the conduit for given dataset name
      mkMap' ds = fromMaybe (error "Can't identify current tap in IX.") $ do
                      tap <- M.lookup ds tapIx
                      cond <- find ((== tap) . view _1) fs
                      return $ view _3 cond

      mp = joinMapper getDS mkMap'
      rd = joinReducer fs'


