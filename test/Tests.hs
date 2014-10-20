{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TemplateHaskell       #-}

module Main where


-------------------------------------------------------------------------------
import           Control.Arrow
import           Control.Lens
import qualified Data.ByteString.Char8                as B
import           Data.DeriveTH
import qualified Data.HashMap.Strict                  as HM
import           Data.List
import           Data.Monoid
import           Data.Ord
import           Data.Time
import qualified Data.Vector                          as V
import           Test.Framework
import           Test.Framework.Providers.HUnit
import           Test.Framework.Providers.QuickCheck2
import           Test.Framework.Runners.Console
import           Test.HUnit
import           Test.QuickCheck
import           Test.QuickCheck.Property
-------------------------------------------------------------------------------
import           Hadron.Basic
import           Hadron.Controller
import           Hadron.Join
-------------------------------------------------------------------------------


main = defaultMain
  [ testProperty "MRKey UTCTime instance obeys ordering" prop_utcMrKeySort
  ]


-------------------------------------------------------------------------------
prop_utcMrKeySort :: [UTCTime] -> Bool
prop_utcMrKeySort ds = sortBy (comparing fst) ds' == sortBy (comparing snd) ds'
    where
      ds' = map (id &&& toCompKey) ds


-------------------------------------------------------------------------------
instance Arbitrary DiffTime where
  arbitrary = secondsToDiffTime `fmap` arbitrary


$(derives [makeArbitrary] [''UTCTime, ''Day])

