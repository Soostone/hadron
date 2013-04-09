{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Hadoop.Streaming.Join where

-------------------------------------------------------------------------------
import           Control.Lens
import qualified Data.ByteString.Char8 as B
import qualified Data.HashMap.Strict   as HM
import           Data.List
import           Data.Monoid
import qualified Data.Vector           as V
-------------------------------------------------------------------------------
import           Hadoop.Streaming
-------------------------------------------------------------------------------



finalizeJoin
    :: (Monad m, Monoid a)
    => [(DataSet, JoinType)]
    -> (a -> m ())
    -> Key
    -> JoinAcc a
    -> m ()
finalizeJoin defs f k JoinAcc{..} =
    case groups of
      Nothing -> return ()
      Just gs ->  mapM_ (f . mconcat) $ sequence gs
    where
      data' = foldl' step jaData defs

      -- | Maybe will reduce to Nothing if any of the Inner joins is
      -- missing.
      groups = mapM (flip HM.lookup data' . fst) defs

      step m (ds, Outer) = m
      step m (ds, Inner) = HM.insertWith insMissing ds [mempty] m

      insMissing new [] = new
      insMissing _ old = old



reduceJoin :: Key -> JoinAcc a -> a -> JoinAcc a
reduceJoin k ja@JoinAcc{..} x =
    JoinAcc { jaData = HM.insertWith add ds [x] jaData
            , jaCurDS = Just ds
            , jaDoneDS = done }

    where

      -- | add to done if the prev dataset is finished
      done =
        case jaCurDS of
          Nothing -> jaDoneDS
          Just ds' ->
            case ds' == ds of
              True -> jaDoneDS
              False -> V.cons ds' jaDoneDS
      add new old = new ++ old
      (jk, ds) = breakKey k


data JoinType = Inner | Outer


type DataSet = B.ByteString

type JoinKey = B.ByteString


data JoinAcc a = JoinAcc {
      jaData   :: ! (HM.HashMap DataSet [a])
    -- ^ Buffer of in-memory retained data
    , jaCurDS  :: Maybe DataSet
    -- ^ DataSet we are currently streaming for the current JoinKey
    , jaDoneDS :: V.Vector DataSet
    -- ^ List of datasets done streaming for the current JoinKey
    } deriving (Eq,Show)


-- | Built-in separatar char is | for now.
breakKey :: Key -> (JoinKey, DataSet)
breakKey k = over _2 (B.drop 1) $ B.span (/= '|') k


-- | Consider two keys equal if they share the same join root
joinEq :: Key -> Key -> Bool
joinEq k1 k2 = jk1 == jk2
    where
      (jk1, ds1) = breakKey k1
      (jk2, ds2) = breakKey k2



