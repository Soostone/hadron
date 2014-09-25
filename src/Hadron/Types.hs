{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE TemplateHaskell   #-}

module Hadron.Types where

-------------------------------------------------------------------------------
import           Control.Lens
import           Control.Monad.Trans.Resource
import qualified Data.ByteString.Char8        as B
import           Data.Conduit
import           Data.Default
-------------------------------------------------------------------------------
import           Hadron.Run.Hadoop
-------------------------------------------------------------------------------

type Key = B.ByteString

type CompositeKey = [Key]



-- | Useful when making a key from multiple pieces of data
mkKey :: [B.ByteString] -> B.ByteString
mkKey = B.intercalate "|"


-------------------------------------------------------------------------------
-- | A 'Mapper' parses and converts the unbounded incoming stream of
-- input into a stream of (key, value) pairs.
type Mapper a k b     = Conduit a (ResourceT IO) (k, b)


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
type Reducer k a r  = Conduit (k, a) (ResourceT IO) r


-------------------------------------------------------------------------------
-- | Options for a single-step MR job.
data MROptions = MROptions {
      _mroPart       :: PartitionStrategy
    -- ^ Number of segments to expect in incoming keys. Affects both
    -- hadron program's understanding of key AND Hadoop's distribution
    -- of map output to reducers.
    , _mroComparator :: Comparator
    , _mroNumMap     :: Maybe Int
    -- ^ Number of map tasks; 'Nothing' leaves it to Hadoop to decide.
    , _mroNumReduce  :: Maybe Int
    -- ^ Number of reduce tasks; 'Nothing' leaves it to Hadoop to decide.
    , _mroCompress   :: Maybe String
    -- ^ Whether to use compression on reduce output.
    , _mroOutSep     :: Maybe Char
    -- ^ Separator to be communicated to Hadoop for the reduce output.
    -- Sets both the 'stream.reduce.output.field.separator' and
    -- 'mapred.textoutputformat.separator' parameters. Sometimes
    -- useful to trick Hadoop into agreeing that the reduce output has
    -- both a key and a value, therefore avoiding the trailing
    -- separator forcefully inserted by Hadoop.
    --
    -- If you're outputting CSV for example, you may want to specify
    -- 'Just ,' here so that with 2 fields Hadoop will think you
    -- already have the key-value pair.
    }


makeLenses ''MROptions

instance Default MROptions where
    def = MROptions NoPartition RegularComp Nothing Nothing Nothing Nothing


-------------------------------------------------------------------------------
-- | Obtain baseline Hadoop run-time options from provided step options
mrOptsToRunOpts :: MROptions -> HadoopRunOpts
mrOptsToRunOpts MROptions{..} = def { mrsPart = _mroPart
                                    , mrsNumMap = _mroNumMap
                                    , mrsNumReduce = _mroNumReduce
                                    , mrsCompress = _mroCompress
                                    , mrsOutSep = _mroOutSep
                                    , mrsComparator = _mroComparator }

