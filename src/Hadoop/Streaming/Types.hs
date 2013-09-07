{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}

module Hadoop.Streaming.Types where

-------------------------------------------------------------------------------
import           Control.Lens
import qualified Data.ByteString.Char8   as B
import           Data.Conduit
-------------------------------------------------------------------------------
import           Hadoop.Streaming.Hadoop
-------------------------------------------------------------------------------

type Key = B.ByteString

type Value = B.ByteString

type CompositeKey = [Key]



-- | Useful when making a key from multiple pieces of data
mkKey :: [B.ByteString] -> B.ByteString
mkKey = B.intercalate "|"


-------------------------------------------------------------------------------
-- | A 'Mapper' parses and converts the unbounded incoming stream of
-- input into a stream of (key, value) pairs.
type Mapper a m b     = Conduit a m (CompositeKey, b)


-------------------------------------------------------------------------------
-- | A reducer takes an incoming stream of (key, value) pairs and
-- emits zero or more output objects of type 'r'.
--
-- Note that this framework guarantees your reducer function (i.e. the
-- conduit you supply here) will see ONLY keys that are deemed
-- 'equivalent' based on the 'MROptions' you supply. Different keys
-- will be given to individual and isolated invocations of your
-- reducer function. This is pretty much the key abstraction provided
-- by this framework.
type Reducer a m r  = Conduit (CompositeKey, a) m r


-------------------------------------------------------------------------------
-- | Options for an MR job with value a emitted by 'Mapper' and
-- reduced by 'Reducer'.
data MROptions a = MROptions {
      mroEq      :: (CompositeKey -> CompositeKey -> Bool)
    -- ^ An equivalence test for incoming keys. True means given two
    -- keys are part of the same reduce series.
    , mroPart    :: PartitionStrategy
    -- ^ Number of segments to expect in incoming keys.
    , mroInPrism :: Prism' B.ByteString a
    -- ^ A serialization scheme for values between the map-reduce
    -- steps.
    }

