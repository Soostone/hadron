{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE EmptyDataDecls             #-}
{-# LANGUAGE ExistentialQuantification  #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE NoMonomorphismRestriction  #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TemplateHaskell            #-}
{-# LANGUAGE UndecidableInstances       #-}

module Hadoop.Streaming.Controller
    (
    -- * Command Line Entry Point
      hadoopMain

    -- * Hadoop Program Construction
    , Controller
    , MapReduce (..)
    , connect


    ) where

-------------------------------------------------------------------------------
import           Control.Applicative
import           Control.Concurrent
import           Control.Error
import           Control.Lens
import           Control.Monad.Operational as O
import           Control.Monad.State
import           Control.Monad.Trans
import qualified Data.ByteString           as B
import           Data.Conduit
import           Data.Default
import qualified Data.HashMap.Strict       as HM
import           Data.List
import qualified Data.Map                  as M
import           System.Environment
-------------------------------------------------------------------------------
import           Hadoop.Streaming
import           Hadoop.Streaming.Hadoop
-------------------------------------------------------------------------------



-------------------------------------------------------------------------------
-- | A packaged MapReduce step
data MapReduce a m b = forall v. MapReduce {
      mrMapper  :: Mapper a m v
    , mrReducer :: Reducer v m b
    , mrOptions :: MROptions v
    }


-- | The hadoop-understandable location of a datasource
type Location = String

-- | A data source definition: includes location and protocol used to
-- serialize/deserialize.
data DataDef m a = DataDef
    { location :: Location
    , proto    :: Protocol m a
    }


-- | Construct a 'DataDef'
ddef :: Location -> Protocol m a -> DataDef m a
ddef = DataDef


data ContState = ContState {
      _csMRCount :: Int
    }

instance Default ContState where
    def = ContState 0


makeLenses ''ContState



data ConI a where
    Connect :: forall i o. MapReduce i IO o
            -> [DataDef IO i] -> DataDef IO o
            -> ConI ()


-- | All MapReduce steps are integrated in the 'Controller' monad.
newtype Controller a = Controller { unController :: Program ConI a }
    deriving (Functor, Applicative, Monad)



-------------------------------------------------------------------------------
-- | Connect a typed MapReduce application you will supply with a list
-- of sources and a destination.
connect :: MapReduce a IO b -> [DataDef IO a] -> DataDef IO b -> Controller ()
connect mr inp outp = Controller $ singleton $ Connect mr inp outp


newMRKey :: MonadState ContState m => m String
newMRKey = do
    i <- gets _csMRCount
    csMRCount %= (+1)
    return $! show i



-------------------------------------------------------------------------------
-- | Interpreter for the central job control process
orchestrate
    :: MonadIO m
    => Controller a
    -> HadoopSettings
    -> ContState
    -> m (Either String a)
orchestrate (Controller p) set s = evalStateT (runEitherT (go p)) s
    where
      go = eval . O.view

      eval (Return a) = return a
      eval (i :>>= f) = eval' i >>= go . f

      eval' :: MonadIO m => ConI a -> EitherT String (StateT ContState m) a
      eval' (Connect mr inp outp) = go'
          where
            go' = do
                mrKey <- newMRKey
                launchMapReduce set mrKey
                  (mrSettings (map location inp) (location outp))



data Phase = Map | Reduce


-------------------------------------------------------------------------------
-- | The main entry point. Use this function to produce a command line
-- program that encapsulates everything.
hadoopMain
    :: forall m a. (MonadThrow m, MonadIO m)
    => Controller a
    -> HadoopSettings
    -> m ()
hadoopMain c@(Controller p) hs = do
    args <- liftIO getArgs
    case args of
      [] -> do
        res <- orchestrate c hs def
        liftIO $ either print (const $ putStrLn "Success.") res
      [arg] -> do
        evalStateT (interpretWithMonad (go arg) p) def
        return ()
      _ -> error "Usage: No arguments for job control or a phase name."
    where

      mkArgs mrKey = [ (Map, "map_" ++ mrKey)
                     , (Reduce, "reduce_" ++ mrKey) ]


      go :: String -> ConI b -> StateT ContState m b
      go arg (Connect (MapReduce mp rd mro@MROptions{..}) inp outp) = do
          mrKey <- newMRKey
          case find ((== arg) . snd) $ mkArgs mrKey of
            Just (Map, _) -> do
              let inSer = proto $ head inp
              liftIO $ (mapperWith mroPrism $ protoDec inSer =$= mp)
            Just (Reduce, _) -> do
              liftIO $ (reducerMain mro rd (protoEnc $ proto outp))
            Nothing -> return ()


