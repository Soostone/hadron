{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE RecordWildCards   #-}

module Hadoop.Streaming.Join where

-------------------------------------------------------------------------------
import           Control.Lens
import           Control.Monad.Trans
import qualified Data.ByteString.Char8 as B
import           Data.Conduit
import qualified Data.Conduit.List     as C
import           Data.Default
import qualified Data.HashMap.Strict   as HM
import           Data.List
import           Data.Monoid
import           Data.Ord
import           Data.Serialize
import qualified Data.Vector           as V
import           Debug.Trace
import           System.Environment
-------------------------------------------------------------------------------
import           Hadoop.Streaming
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
-- joined rows immediately. Given function is responsible for calling
-- 'emitOutput'.
emitStream :: (Monad m, Monoid a) => (a -> m ()) -> JoinAcc a -> a -> m ()
emitStream f Streaming{..} a = V.mapM_ (f . mappend a) strStems


-------------------------------------------------------------------------------
joinOpts :: Serialize a => MROptions a
joinOpts = MROptions eq 2 ser
    where
      eq [a1,a2] [b1,b2] = a1 == b1



joinReducer
    :: (Show b, Monoid b, MonadIO m, MonadThrow m, Serialize b)
    => [(DataSet, JoinType)]
    -- ^ Table definitions
    -> (b -> B.ByteString)
    -- ^ Convert resulting rows to bytestring
    -> m ()
joinReducer fs f = reducer joinOpts step def fin
    where
      step = joinReduceStep fs f'
      fin = joinFinalize fs f'
      f' = emitOutput . f


-- | Helper to construct a finalizer for join operations.
joinFinalize
    :: (Monad m, Monoid a)
    => [(DataSet, JoinType)]
    -> (a -> m ())
    -> t
    -> JoinAcc a
    -> m ()

-- we're still in buffering, so nothing has been emitted yet. one of
-- the tables (and definitely the last table) did not have any input
-- at all. we'll try to emit in case the last table is not a required
-- table.
joinFinalize fs f _ buf@Buffering{..} =
  let str = bufToStr fs buf
  in  emitStream f str mempty

-- we're already in streaming, so we've been emitting output in
-- real-time. nothing left to do at this point.
joinFinalize _ _ _ Streaming{..} = return ()


-------------------------------------------------------------------------------
-- | Make a step function for a join operation
joinReduceStep
    :: (Monad m, Monoid a, Show a)
    => DataDefs
    -> (a -> m ())
    -> Reducer m a (JoinAcc a)
joinReduceStep fs f k buf@Buffering{..} x =

    -- | Accumulate until you start seeing the last table. We'll start
    -- emitting immediately after that.
    case ds == lastDataSet of
      False -> -- traceShow accumulate $
               return $! accumulate
      True ->
        let fs' = filter ((/= ds) . fst) fs
        in joinReduceStep fs f k (bufToStr fs' buf) x

    where

      fs' = sortBy (comparing fst) fs

      lastDataSet = fst $ last fs'

      accumulate =
          Buffering { bufData = HM.insertWith add ds [x] bufData
                    , bufCurDS = Just ds
                    -- , bufDoneDS = done
                    }

      -- -- | add to done if the prev dataset is finished
      -- done =
      --   case bufCurDS of
      --     Nothing -> bufDoneDS
      --     Just ds' ->
      --       case ds' == ds of
      --         True -> bufDoneDS
      --         False -> V.cons ds' bufDoneDS

      add new old = new ++ old
      [jk, ds] = k

joinReduceStep _ f k str@Streaming{} x = emitStream f str x >> return str



-------------------------------------------------------------------------------
-- | Execute a mapper program tailored to emit file tags with keys.
mapJoin
    :: MonadIO m
    => (String -> DataSet)
    -- ^ Figure out the current dataset given map input file
    -> Prism' B.ByteString a
    -- ^ Prism for serialization
    -> (DataSet -> Conduit (B.ByteString) m (JoinKey, a))
    -- ^ A conduit to convert incoming stream into a join-key and a
    -- target value.
    -> m ()
mapJoin getDS prism f = do
    fi <- liftIO $ getEnv "map_input_file"
    let ds = getDS fi
    mapperWith prism $ f ds =$= C.map (go ds)
  where
    go ds (jk, a) = ([jk, ds], a)




                           ------------------------
                           -- A Main Application --
                           ------------------------


-------------------------------------------------------------------------------
joinMain :: (MonadIO m, Serialize a, Monoid a, MonadThrow m, Show a)
         => DataDefs
         -> (String -> DataSet)
         -> (DataSet -> Conduit B.ByteString m (JoinKey, a))
         -> (a -> m ())
         -> m ()
joinMain fs getDS mkMap emit = mapReduceMain joinOpts mp rd def fin
    where

      conv = do
          fi <- liftIO $ getEnv "map_input_file"
          let ds = getDS fi
          C.map (go ds)

      mp' = do
          fi <- liftIO $ getEnv "map_input_file"
          let ds = getDS fi
          mkMap ds

      mp = mp' =$= conv

      go ds (jk, a) = ([jk, ds], a)

      rd = joinReduceStep fs emit
      fin = joinFinalize fs emit
