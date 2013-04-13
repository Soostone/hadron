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
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TemplateHaskell            #-}
{-# LANGUAGE UndecidableInstances       #-}

module Hadoop.Streaming.EDSL where

-------------------------------------------------------------------------------
import           Control.Applicative
import           Control.Concurrent
import           Control.Error
import           Control.Lens
import           Control.Monad.Operational as O
import           Control.Monad.State
import           Data.ByteString           (ByteString)
import           Data.Conduit
import           Data.Default
import qualified Data.HashMap.Strict       as HM
import           Data.List
import qualified Data.Map                  as M
import           Data.Vault                as V
import           System.Environment
-------------------------------------------------------------------------------
import           Hadoop.Streaming.Hadoop
-------------------------------------------------------------------------------


data JobId

data JobStatus
    = JobFinished
    | JobStarted
    | JobPending
    | JobFailed
    deriving (Eq,Show,Read,Ord)


data DataStatus
    = Ready
    | InProgress JobId




type Mapper a m v = Conduit a m (CompositeKey, v)
type Reducer v m b = Sink (CompositeKey, v) m (Maybe b)


-------------------------------------------------------------------------------
-- | A packaged MapReduce step
data MapReduce a m b = forall v. MapReduce {
      mrMapper  :: Mapper a m v
    , mrReducer :: Reducer v m b
    , mrEq      :: CompositeKey -> CompositeKey -> Bool
    , mrPrism   :: Prism' B.ByteString v
    -- ^ A prism for the intermediate value b
    , mrPart    :: PartitionStrategy
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
ddef = DataDef


data ContState = ContState {
      _csMRCount :: Int
    }

instance Default ContState where
    def = ContState 0


makeLenses ''ContState




data ConI a where
    Connect :: forall m i o. MapReduce i m o -> [DataDef m i] -> DataDef m o -> ConI ()


-- | All MapReduce steps are integrated in the 'Controller' monad.
newtype Controller a = Controller { unController :: Program ConI a }
    deriving (Functor, Applicative, Monad)



-------------------------------------------------------------------------------
-- | Connect a typed MapReduce application you will supply with a list
-- of sources and a destination.
connect :: MapReduce a m b -> [DataDef m a] -> DataDef m b -> Controller ()
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
                launchMapReduce set mrKey (mrSettings (map location inp) (location outp))



data Phase = Map | Reduce


-------------------------------------------------------------------------------
-- | Interpreter for the command line program to actually perform the
-- data processing tasks.
cli c@(Controller p) hs = do
    args <- getArgs
    case args of
      [] -> do
        res <- orchestrate c hs def
        either print (const $ putStrLn "Success.") res
      [arg] -> evalStateT (interpretWithMonad (go arg) p) def
      _ -> error "Usage: No arguments for job control or a phase name."
    where
      mkArgs mrKey = [ (Map, "map_" ++ mrKey)
                     , (Reduce, "reduce_" ++ mrKey) ]

      go :: String -> ConI a -> StateT ContState IO a
      go arg (Connect mr inp outp) = do
          mrKey <- newMRKey
          case find ((== arg) . snd) $ mkArgs mrKey of
            Just (Map, _) -> do
              let inSer = proto $ head inp
                  outSer = proto outp
              let m' = protoDec inSer =$= (mrMapper mr) =$= protoEnc outSer
              mapper m'
            Just (Reduce, _) -> do
              let mro = MROptions mrEq (keySegs mrPart) mrPrism
              reducer mro (mrReducer mr)
            Nothing -> return ()



-- -------------------------------------------------------------------------------
-- connect mr@(MapReduce m r) inp@DataDef{..} outp = do
--     execDataDef inp
--     mrKey <- liftIO newKey
--     let m' = deser =$= m
--     csJobs %= V.insert mrKey (MapReduce m' r)


-- newtype Controller m a = Controller { runController :: StateT CState m a }


-- ($$) :: MapReduce a m b -> (DataDef a) -> Controller m (DataDef b)



execDataDef = undefined


mr1 = undefined
mr2 = undefined
mr3 = undefined


-- mySequence = do
--     res1 <- connect mr1 a
--     _ <- connect mr2 a
--     connect mr3 res1


