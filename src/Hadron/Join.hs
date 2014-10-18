{-# LANGUAGE BangPatterns               #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE RecordWildCards            #-}

module Hadron.Join
    (

      DataDefs
    , DataSet (..)
    , JoinType (..)
    , JoinKey

    , joinMain
    , joinMapper
    , joinReducer
    , joinOpts

    -- * TODO: To be put into an Internal module
    , JoinAcc (..)
    , bufToStr

    ) where

-------------------------------------------------------------------------------
import           Control.Lens
import           Control.Monad
import qualified Data.ByteString.Char8 as B
import           Data.Default
import           Data.Hashable
import qualified Data.HashMap.Strict   as HM

import           Data.List
import           Data.Monoid
import           Data.Ord
import           Data.Serialize
import           Data.String
import qualified Data.Vector           as V
import           GHC.Generics
import qualified System.IO.Streams     as S
-------------------------------------------------------------------------------
import           Hadron.Basic
import           Hadron.Streams
-------------------------------------------------------------------------------


type DataDefs = [(DataSet, JoinType)]

data JoinType = JRequired | JOptional
  deriving (Eq,Show,Read,Ord)


newtype DataSet = DataSet { getDataSet :: B.ByteString }
  deriving (Eq,Show,Read,Ord,Serialize,Generic,Hashable,IsString)

type JoinKey = B.ByteString


-- | We are either buffering input rows or have started streaming, as
-- we think we're now receiving the last table we were expecting.
data JoinAcc a =
    Buffering {
      bufData :: ! (HM.HashMap DataSet [a])
    -- ^ Buffer of in-memory retained data. We have to retain (n-1) of
    -- the input datasets and we can then start emitting rows in
    -- constant-space for the nth dataset.
    }
    | Streaming { strStems :: V.Vector a }

    deriving (Eq,Show)


instance Default (JoinAcc a) where
    def = Buffering mempty



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

      step m (_, JRequired) = m
      step m (ds, JOptional) = HM.insertWith insMissing ds [mempty] m

      insMissing new [] = new
      insMissing _ old = old
bufToStr _ _ = error "bufToStr can only convert a Buffering to a Streaming"


-- | Given a new row in the final dataset of the joinset, emit all the
-- joined rows immediately.
emitStream :: Monoid b => JoinAcc b -> b -> [b]
emitStream Streaming{..} a = V.toList $ V.map (mappend a) strStems
emitStream _ _ = error "emitStream can't be called unless it's in Streaming mode."

-------------------------------------------------------------------------------
joinOpts :: MROptions
joinOpts = def { _mroPart = (Partition 2 1) }


-------------------------------------------------------------------------------
-- | Make join reducer from given table definitions
joinReducer
    :: (Show r, Monoid r)
    => [(DataSet, JoinType)]
    -- ^ Table definitions
    -> Reducer CompositeKey r r
joinReducer fs is out = go (def, [])
  where
    go (!ja, !buf) =
        case buf of
          (x:rest) -> do
            S.write (Just x) out
            go (ja, rest)
          [] -> do
            next <- S.read is
            case next of
              Nothing -> forM_ (joinFinalize fs ja) $ \ x -> S.write (Just x) out
              Just x -> do
                let (!ja', !xs) = joinReduceStep fs ja x
                go (ja', xs)


-------------------------------------------------------------------------------
joinFinalize
    :: (Monoid b)
    => [(DataSet, JoinType)]
    -> JoinAcc b
    -> [b]

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
joinFinalize _ Streaming{} = []


-------------------------------------------------------------------------------
-- | Make a step function for a join operation
joinReduceStep
    :: (Monoid b)
    => DataDefs
    -> JoinAcc b
    -> (CompositeKey, b)
    -> (JoinAcc b, [b])
joinReduceStep fs buf@Buffering{..} (k, x) =

    -- Accumulate until you start seeing the last table. We'll start
    -- emitting immediately after that.
    case ds' == lastDataSet of
      False -> -- traceShow accumulate $
               (accumulate, [])
      True ->
        let xs = filter ((/= ds') . fst) fs
        in joinReduceStep fs (bufToStr xs buf) (k,x)

    where

      fs' = sortBy (comparing fst) fs

      lastDataSet = fst $ last fs'

      !accumulate =
          Buffering { bufData = HM.insertWith add ds' [x] bufData
                    }

      add new old = new ++ old
      ds = last k
      ds' = DataSet ds

joinReduceStep _ str@Streaming{} (_,x) = (str, emitStream str x)


-- | Helper for easy construction of specialized join mapper.
--
-- This mapper identifies the active dataset from the currently
-- streaming filename and uses filename to determine how the mapping
-- shoudl be done.
joinMapper
    :: (String -> DataSet)
    -- ^ Infer dataset from current filename
    -> (DataSet -> Mapper a CompositeKey r)
    -- ^ Given a dataset, map it to a common data type
    -> Mapper a CompositeKey r
joinMapper getDS mkMap is out = do
    fi <- getFileName
    let ds = getDS fi

    out' <- S.contramap (go ds) out

    (mkMap ds) is out'
  where
    go ds (jk, a) = (jk ++ [getDataSet ds], a)



                           ------------------------
                           -- A Main Application --
                           ------------------------


-------------------------------------------------------------------------------
-- | Make a stand-alone program that can act as a mapper and reducer,
-- performing the join defined here.
--
-- For proper higher level operation, see the 'Controller' module.
joinMain
    :: (Serialize r, Monoid r, Show r)
    => DataDefs
    -- ^ Define your tables
    -> (String -> DataSet)
    -- ^ Infer dataset from input filename
    -> (DataSet -> Mapper B.ByteString CompositeKey r)
    -- ^ Map input stream to a join key and the common-denominator
    -- uniform data type we know how to 'mconcat'.
    -> Prism' B.ByteString r
    -- ^ Choose serialization method for final output.
    -> IO ()
joinMain fs getDS mkMap out = mapReduceMain joinOpts pSerialize mp rd
    where

      mp is os = joinMapper getDS mkMap is os

      rd is os = contramapMaybe (firstOf (re out)) os >>= joinReducer fs is
