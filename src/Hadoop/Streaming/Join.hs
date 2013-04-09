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
import qualified Data.Vector           as V
import           System.Environment
-------------------------------------------------------------------------------
import           Hadoop.Streaming
-------------------------------------------------------------------------------




type DataDefs = [(DataSet, JoinType)]

data JoinType = Inner | Outer


type DataSet = B.ByteString

type JoinKey = B.ByteString


data JoinAcc a =
    Buffering {
      bufData   :: ! (HM.HashMap DataSet [a])
    -- ^ Buffer of in-memory retained data. We have to retain (n-1) of
    -- the input datasets and we can then start emitting rows in
    -- constant-space for the nth dataset.
    , bufCurDS  :: Maybe DataSet
    -- ^ DataSet we are currently streaming for the current JoinKey
    , bufDoneDS :: V.Vector DataSet
    -- ^ List of datasets done streaming for the current JoinKey
    }
    | Streaming { strStems :: V.Vector a }

    deriving (Eq,Show)


instance Default (JoinAcc a) where
    def = Buffering mempty Nothing mempty


-- | Built-in separatar char is | for now.
breakKey :: Key -> (JoinKey, DataSet)
breakKey k = over _2 (B.drop 1) $ B.span (/= '\t') k


-- | Consider two keys equal if they share the same join root
joinEq :: Key -> Key -> Bool
joinEq k1 k2 = jk1 == jk2
    where
      (jk1, ds1) = breakKey k1
      (jk2, ds2) = breakKey k2



-------------------------------------------------------------------------------
-- | Convert a buffering state to a ready-to-stream state. Once this
-- conversion is done, we'll start emitting output rows immediately
-- and in constant space.
bufToStr
    :: Monoid a
    => DataDefs
    -- ^ Table definitions for the current join
    -> DataSet
    -- ^ The last remaining table in the joined tables
    -> JoinAcc a
    -- ^ Buffering
    -> JoinAcc a
    -- ^ Streaming
bufToStr defs lastDS Buffering{..} = Streaming rs
    where
      rs = V.fromList $ maybe [] (map mconcat . sequence) groups

      data' = foldl' step bufData defs

      -- | Maybe will reduce to Nothing if any of the Inner joins is
      -- missing.
      groups = mapM (flip HM.lookup data' . fst) defs

      step m (ds, Outer)
          | ds == lastDS = HM.insertWith insMissing ds [mempty] m
          | otherwise    = m
      step m (ds, Inner) = HM.insertWith insMissing ds [mempty] m

      insMissing new [] = new
      insMissing _ old = old
bufToStr _ _ _ = error "bufToStr can only convert a Buffering to a Streaming"



-- | Given a new row in the final dataset of the joinset, emit all the
-- joined rows immediately. Given function is responsible for calling
-- 'emitOutput'.
emitStream :: (Monad m, Monoid a) => (a -> m ()) -> JoinAcc a -> a -> m ()
emitStream f Streaming{..} a = V.mapM_ (f . mappend a) strStems



-------------------------------------------------------------------------------
-- | Make a step function for a join operation
joinReduceStep
    :: (Monad m, Monoid a)
    => DataDefs
    -> (a -> m ())
    -> Key
    -> JoinAcc a
    -> a
    -> m (JoinAcc a)

joinReduceStep fs f k buf@Buffering{..} x =
    case V.length bufDoneDS == n of
      False -> return $! accumulate
      True -> joinReduceStep fs f k (bufToStr fs ds buf) x

    where
      -- | point when we can start emitting
      n = length fs - 1

      accumulate =
          Buffering { bufData = HM.insertWith add ds [x] bufData
                    , bufCurDS = Just ds
                    , bufDoneDS = done }

      -- | add to done if the prev dataset is finished
      done =
        case bufCurDS of
          Nothing -> bufDoneDS
          Just ds' ->
            case ds' == ds of
              True -> bufDoneDS
              False -> V.cons ds' bufDoneDS
      add new old = new ++ old
      (jk, ds) = breakKey k

joinReduceStep fs f k str@Streaming{} x = emitStream f str x >> return str



-------------------------------------------------------------------------------
-- | Execute a mapper program tailored to emit file tags with keys.
mapJoin
    :: MonadIO m
    => (String -> DataSet)
    -- ^ Figure out the current dataset given map input file
    -> Prism' B.ByteString a
    -- ^ Prism for serialization
    -> Conduit B.ByteString m (JoinKey, a)
    -- ^ A conduit to convert incoming stream into a join-key and a
    -- target value.
    -> m ()
mapJoin getDS prism f = do
    fi <- liftIO $ getEnv "map_input_file"
    let ds = getDS fi
    mapperWith prism $ f =$= C.map (go ds)
  where
    go ds (jk, a) = (B.concat [jk, "\t", ds], a)






