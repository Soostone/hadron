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
-- input data into a stream of (key, value) pairs.
--
-- A mapper is responsible for signaling its own "Nothing" to signify
-- it is done writing. Hadron will not automatically mark EOF on the
-- OutputStream, which is basically 'stdout'.
type Mapper a k b = Conduit a (ResourceT IO) (k, b)


-------------------------------------------------------------------------------
-- | A reducer takes an incoming stream of (key, value) pairs and
-- emits zero or more output objects of type 'r'.
--
-- Note that this framework guarantees your reducer function (i.e. the
-- conduit you supply here) will see ONLY keys that are deemed
-- 'equivalent' based on the 'MROptions' you supply. Different keys
-- will be given to individual and isolated invocations of your
-- reducer function. This is a very central abstraction (and one of
-- the few major ones) provided by this framework.
--
-- It does not matter if you supply your own EOF signals via Nothing
-- as Hadron will simply discard them before relaying over to 'stdout'
-- and supply its own EOF based on when its input is finished.
type Reducer k a r  = Conduit (k, a) (ResourceT IO) r


data ReduceErrorStrategy
    = ReduceErrorReThrow
    | ReduceErrorSkipKey
    | ReduceErrorRetry
    deriving (Eq,Show,Read,Ord)


-------------------------------------------------------------------------------
-- | Options for a single-step MR job.
data MROptions = MROptions {
      _mroPart        :: PartitionStrategy
    -- ^ Number of segments to expect in incoming keys. Affects both
    -- hadron program's understanding of key AND Hadoop's distribution
    -- of map output to reducers.
    , _mroComparator  :: Comparator
    , _mroNumMap      :: Maybe Int
    -- ^ Number of map tasks; 'Nothing' leaves it to Hadoop to decide.
    , _mroNumReduce   :: Maybe Int
    -- ^ Number of reduce tasks; 'Nothing' leaves it to Hadoop to decide.
    , _mroCompress    :: Maybe String
    -- ^ Whether to use compression on reduce output.
    , _mroOutSep      :: Maybe Char
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
    , _mroReduceError :: ReduceErrorStrategy
    -- ^ What to do on reducer error.
    }

makeLenses ''MROptions


instance Default MROptions where
    def = MROptions NoPartition RegularComp Nothing Nothing Nothing Nothing
          ReduceErrorReThrow


-------------------------------------------------------------------------------
-- | Obtain baseline Hadoop run-time options from provided step options
mrOptsToRunOpts :: MROptions -> HadoopRunOpts
mrOptsToRunOpts MROptions{..} = def { _mrsPart = _mroPart
                                    , _mrsNumMap = _mroNumMap
                                    , _mrsNumReduce = _mroNumReduce
                                    , _mrsCompress = _mroCompress
                                    , _mrsOutSep = _mroOutSep
                                    , _mrsComparator = _mroComparator }

