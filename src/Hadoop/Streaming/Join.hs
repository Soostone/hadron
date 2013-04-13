{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE RecordWildCards   #-}

module Hadoop.Streaming.Join
    (

      DataDefs
    , DataSet
    , JoinType (..)
    , JoinKey


    , joinMain
    , joinMapper
    , joinReducer
    , joinOpts

    ) where

-------------------------------------------------------------------------------
import           Control.Lens
import           Control.Monad.Trans
import qualified Data.ByteString.Char8   as B
import           Data.Conduit
import qualified Data.Conduit.List       as C
import           Data.Default
import qualified Data.HashMap.Strict     as HM
import           Data.List
import           Data.Monoid
import           Data.Ord
import           Data.Serialize
import qualified Data.Vector             as V
import           Debug.Trace
import           System.Environment
-------------------------------------------------------------------------------
import           Hadoop.Streaming
import           Hadoop.Streaming.Hadoop
-------------------------------------------------------------------------------




type DataDefs = [(DataSet, JoinType)]

data JoinType = JRequired | JOptional


type DataSet = B.ByteString

type JoinKey = B.ByteString


data JoinAcc a =
    Buffering {
      bufData  :: ! (HM.HashMap DataSet [a])
    -- ^ Buffer of in-memory retained data. We have to retain (n-1) of
    -- the input datasets and we can then start emitting rows in
    -- constant-space for the nth dataset.
    , bufCurDS :: Maybe DataSet
    -- ^ DataSet we are currently streaming for the current JoinKey
    -- , bufDoneDS :: V.Vector DataSet
    -- ^ List of datasets done streaming for the current JoinKey
    }
    | Streaming { strStems :: V.Vector a }

    deriving (Eq,Show)


instance Default (JoinAcc a) where
    def = Buffering mempty Nothing -- mempty



-------------------------------------------------------------------------------
-- | Convert a buffering state to a ready-to-stream state. Once this
-- conversion is done, we'll start emitting output rows immediately
-- and in constant space.
bufToStr
    :: Monoid a
    => DataDefs
    -- ^ Table definitions for the current join
    -> JoinAcc a
    -- ^ Buffering
    -> JoinAcc a
    -- ^ Streaming
bufToStr defs Buffering{..} = Streaming rs
    where
      rs = V.fromList $ maybe [] (map mconcat . sequence) groups

      -- | Maybe will reduce to Nothing if any of the Inner joins is
      -- missing.
      groups = mapM (flip HM.lookup data' . fst) defs

      data' = foldl' step bufData defs

      step m (ds, JRequired) = m
      step m (ds, JOptional) = HM.insertWith insMissing ds [mempty] m

      insMissing new [] = new
      insMissing _ old = old
bufToStr _ _ = error "bufToStr can only convert a Buffering to a Streaming"




-- | Given a new row in the final dataset of the joinset, emit all the
-- joined rows immediately.
emitStream :: (Monad m, Monoid b) => JoinAcc b -> b -> ConduitM i b m ()
emitStream Streaming{..} a = V.mapM_ (yield . mappend a) strStems


-------------------------------------------------------------------------------
joinOpts :: Serialize a => MROptions a
joinOpts = MROptions eq (Partition 2 1) pSerialize
    where
      eq [a1,a2] [b1,b2] = a1 == b1


-------------------------------------------------------------------------------
-- | Make join reducer from given table definitions
joinReducer
    :: (Show r, Monoid r, MonadIO m, MonadThrow m, Serialize r)
    => [(DataSet, JoinType)]
    -- ^ Table definitions
    -> Reducer r m r
joinReducer fs = red def
    where
      red ja = do
          next <- await
          case next of
            Nothing -> joinFinalize fs ja
            Just x -> do
              ja' <- joinReduceStep fs ja x
              red $! ja'


-------------------------------------------------------------------------------
joinFinalize
    :: (Monad m, Monoid b)
    => [(DataSet, JoinType)]
    -> JoinAcc b
    -> ConduitM i b m ()


-- we're still in buffering, so nothing has been emitted yet. one of
-- the tables (and definitely the last table) did not have any input
-- at all. we'll try to emit in case the last table is not a required
-- table.
--
-- notice that unlike other calls to bufToStr, we include ALL the
-- tables here so that if the last table was required, it'll all
-- collapse to an empty list and nothing will be emitted.
joinFinalize fs buf@Buffering{} =
  let str = bufToStr fs buf
  in  emitStream str mempty

-- we're already in streaming, so we've been emitting output in
-- real-time. nothing left to do at this point.
joinFinalize _ Streaming{} = return ()


-------------------------------------------------------------------------------
-- | Make a step function for a join operation
joinReduceStep
    :: (Monad m, Monoid b)
    => DataDefs
    -> JoinAcc b
    -> (CompositeKey, b)
    -> ConduitM i b m (JoinAcc b)
joinReduceStep fs buf@Buffering{..} (k, x) =

    -- | Accumulate until you start seeing the last table. We'll start
    -- emitting immediately after that.
    case ds == lastDataSet of
      False -> -- traceShow accumulate $
               return $! accumulate
      True ->
        let fs' = filter ((/= ds) . fst) fs
        in joinReduceStep fs (bufToStr fs' buf) (k,x)

    where

      fs' = sortBy (comparing fst) fs

      lastDataSet = fst $ last fs'

      accumulate =
          Buffering { bufData = HM.insertWith add ds [x] bufData
                    , bufCurDS = Just ds
                    }

      add new old = new ++ old
      [jk, ds] = k

joinReduceStep _ str@Streaming{} (k,x) = emitStream str x >> return str


-- | Helper for easy construction of specialized join mapper.
--
-- This mapper identifies the active dataset from the currently
-- streaming filename and uses filename to determine how the mapping
-- shoudl be done.
joinMapper
    :: MonadIO m
    => (String -> DataSet)
    -> (DataSet -> Conduit a m (JoinKey, r))
    -> Mapper a m r
joinMapper getDS mkMap = do
    fi <- liftIO $ getEnv "map_input_file"
    let ds = getDS fi
    mkMap ds =$= C.map (go ds)
  where
    go ds (jk, a) = ([jk, ds], a)



                           ------------------------
                           -- A Main Application --
                           ------------------------


-------------------------------------------------------------------------------
joinMain
    :: (MonadIO m, Serialize r, Monoid r, MonadThrow m, Show r)
    => DataDefs
    -- ^ Define your tables
    -> (String -> DataSet)
    -- ^ Infer dataset from input filename
    -> (DataSet -> Conduit B.ByteString m (JoinKey, r))
    -- ^ Map input stream to a join key and the common-denominator
    -- uniform data type we know how to 'mconcat'.
    -> Conduit r m B.ByteString
    -- ^ Choose serialization method for final output.
    -> m ()
joinMain fs getDS mkMap out = mapReduceMain joinOpts mp rd out
    where

      mp = joinMapper getDS mkMap

      rd = joinReducer fs
