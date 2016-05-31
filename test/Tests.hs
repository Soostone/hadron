{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TemplateHaskell       #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Main where


-------------------------------------------------------------------------------
import           Control.Arrow
import           Data.DeriveTH
import           Data.List
import           Data.Ord
import           Data.Time
import           Test.Framework
import           Test.Framework.Providers.QuickCheck2
import           Test.QuickCheck
-------------------------------------------------------------------------------
import           Hadron.Controller
-------------------------------------------------------------------------------


main :: IO ()
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

