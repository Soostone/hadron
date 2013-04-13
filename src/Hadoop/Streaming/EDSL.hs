{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE EmptyDataDecls             #-}
{-# LANGUAGE ExistentialQuantification  #-}
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
import           Control.Lens
import           Control.Monad.Operational as O
import           Control.Monad.State
import           Data.ByteString           (ByteString)
import           Data.Conduit
import qualified Data.HashMap.Strict       as HM
import qualified Data.Map                  as M
import           Data.Vault                as V
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


existing l from to = DataDef l from to


a = existing undefined undefined



type CompositeKey = [ByteString]

type Mapper a m v = Conduit a m (CompositeKey, v)
type Reducer v m b = Sink v m (Maybe b)


data PartitionStrategy
    = NoPartition
    | Partition { keySegs :: Int, partSegs :: Int }


data MapReduce a m b = forall v. MapReduce {
      mrMapper  :: Mapper a m v
    , mrReducer :: Reducer v m b
    , mrPart    :: PartitionStrategy
    }



type Location = ByteString

data DataDef m a = DataDef
    { location :: Location
    , deser    :: Conduit ByteString m a
    , ser      :: Conduit a m ByteString
    }


data ContState = ContState {
      _csDataStat :: HM.HashMap Location DataStatus
    , _csMRCount  :: Int
    }


makeLenses ''ContState




data ConI a where
    -- Connect :: forall m i o. MapReduce i m o -> DataDef m i -> DataDef m o -> ConI ()
    Dummy :: ConI String
    Blah :: ConI Int


newtype Controller a = Controller { unController :: Program ConI a }
    deriving (Functor, Applicative, Monad)



-- -------------------------------------------------------------------------------
-- connect :: MapReduce a m b -> DataDef m a -> DataDef m b -> Controller ()
-- connect mr inp outp = Controller $ singleton $ Connect mr inp outp




checkJobStatus :: MonadIO m => JobId -> m JobStatus
checkJobStatus = undefined


type MapReduceKey = String

data MapReduceOptions

launchMapReduce
    :: MonadIO m
    => MapReduceKey
    -> MapReduce a n b
    -> Location
    -> Location
    -> m ()
launchMapReduce  = undefined


newMRKey = do
    i <- gets _csMRCount
    csMRCount %= (+1)
    return $! show i


data Hole = Hole

hole = Hole

-------------------------------------------------------------------------------
orchestrate :: MonadIO m => Controller a -> ContState -> m (a, ContState)
orchestrate (Controller p) s = runStateT (go p) s
    where
      go = eval . O.view

      -- eval :: ProgramView ConI a -> StateT ContState m a
      eval (Return a) = return a
      eval (i :>>= f) = eval' i >>= go . f

      eval' Dummy = return "dummy"
      eval' Blah = return 3

      -- eval' (Connect mr inp outp) = go'
      --     where
      --       retry = liftIO (threadDelay 10000000) >> go'

      --       -- go' :: MonadIO m => StateT ContState m ()
      --       go' = do
      --           stat <- gets (HM.lookup (location inp) . _csDataStat)
      --           case stat of
      --             Nothing -> return ()
      --             Just (InProgress jid) -> do
      --               chk <- checkJobStatus jid
      --               case chk of
      --                 JobFinished -> do
      --                   csDataStat %= HM.insert (location inp) Ready
      --                   mrKey <- newMRKey
      --                   launchMapReduce mrKey mr (location inp) (location outp)
      --                 JobPending -> retry
      --                 JobFailed -> return ()
      --                 JobStarted -> retry







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


