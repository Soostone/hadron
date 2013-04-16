{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}

module Main where


-------------------------------------------------------------------------------
import           Control.Lens
import qualified Data.ByteString.Char8               as B
import qualified Data.HashMap.Strict                 as HM
import           Data.Monoid
import qualified Data.Vector                         as V
import           Test.Framework.Providers.SmallCheck
import           Test.Framework.Runners.Console
import           Test.SmallCheck                     hiding (over)
import           Test.SmallCheck.Drivers
import           Test.SmallCheck.Series
-------------------------------------------------------------------------------
import           Hadoop.Streaming
import           Hadoop.Streaming.Join
-------------------------------------------------------------------------------


main = defaultMain
       [ testProperty "bufToStr" prop_bufToStr
       ]


instance (Monad m, Serial m a) => Serial m (JoinAcc a) where
    series = cons2 (\ dataSets objects -> Buffering (HM.fromList (zip (map B.pack dataSets) objects)) Nothing)

instance (Monad m) => Serial m (JoinType) where
    series = cons0 JRequired \/ cons0 JOptional

instance Serial m a => Serial m (Sum a) where
    series = newtypeCons Sum


-------------------------------------------------------------------------------
prop_bufToStr :: [(String, JoinType, [Sum Int])] -> Property IO
prop_bufToStr ds = valid ==> prop0
  where
    ds' = over (traverse._1) B.pack ds

    valid = not (null ds)

    nms = map (view _1) ds'
    types = map (view _2) ds'
    objs = map (view _3) ds'
    defs = zip nms types

    ja = Buffering (HM.fromList (zip nms objs)) Nothing

    prop0 = all (== JRequired) types ==> stemLen == combs
    -- prop1 = all (== JOptional) types ==> stemLen > 0

    stemLen = V.length (strStems str)
    combs = let ns = map length $ HM.elems $ bufData ja
            in if null ns then 0 else product ns

    str = bufToStr defs ja

