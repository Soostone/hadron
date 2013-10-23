{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RankNTypes            #-}

module Hadoop.Streaming.Protocol
    (
      Protocol (..)
    , Protocol'
    , prismToProtocol

    , base64SerProtocol
    , idProtocol
    , linesProtocol
    , gzipProtocol
    , showProtocol
    , csvProtocol

    -- * Serialization Prisms
    , pSerialize
    , pShow

    -- * Serialization Utils
    , serialize
    , deserialize

    -- * Utils
    , linesConduit
    , lineC
    , mkKey


    ) where

-------------------------------------------------------------------------------
import           Blaze.ByteString.Builder
import           Control.Applicative
import           Control.Category
import           Control.Lens
import           Control.Monad
import           Control.Monad.Trans
import           Data.Attoparsec.ByteString.Char8 (Parser, endOfLine, takeTill)
import qualified Data.ByteString.Base64           as Base64
import qualified Data.ByteString.Char8            as B
import qualified Data.ByteString.Lazy.Char8       as LB
import           Data.Conduit
import           Data.Conduit.Attoparsec
import           Data.Conduit.Binary              (sinkHandle, sourceHandle)
import           Data.Conduit.Blaze
import qualified Data.Conduit.List                as C
import           Data.Conduit.Utils
import           Data.Conduit.Zlib                (gzip, ungzip)
import           Data.CSV.Conduit
import           Data.Monoid
import qualified Data.Serialize                   as Ser
import           Prelude                          hiding (id, (.))
import           Safe
-------------------------------------------------------------------------------
import           Hadoop.Streaming.Types
-------------------------------------------------------------------------------


                              -------------------
                              -- Serialization --
                              -------------------


-- | Like 'Protocol' but fixes the source format as 'ByteString'.
type Protocol' m a = Protocol m B.ByteString a


-- | A 'Protocol's is a serialization strategy when we're dealing with
-- streams of records, allowing for arbitrary data formats, delimiters
-- and other potential obstructions.
--
-- Most of the time we'll be using 'Protocol\''s.
data Protocol m b a = Protocol {
      protoEnc :: Conduit a m b
    , protoDec :: Conduit b m a
    }


instance Monad m => Category (Protocol m) where
    id = Protocol (C.map id) (C.map id)
    p1 . p2 = Protocol { protoEnc = (protoEnc p1) =$= (protoEnc p2)
                       , protoDec = (protoDec p2) =$= (protoDec p1) }



-------------------------------------------------------------------------------
-- | Lift 'Prism' to work with a newline-separated stream of objects.
--
-- It is assumed that the prism you supply to this function does not
-- add newlines itself. You need to make them newline-free for this to
-- work properly.
prismToProtocol
    :: (MonadUnsafeIO m, MonadThrow m)
    => Prism' B.ByteString a
    -> Protocol' m a
prismToProtocol p =
    Protocol { protoEnc = C.mapMaybe (firstOf (re p)) =$= write
             , protoDec = linesConduit =$= C.mapMaybe (firstOf p) }
  where
    write = C.map (\x -> fromByteString x `mappend` nl) =$=
            builderToByteString
    nl = fromByteString "\n"


-------------------------------------------------------------------------------
-- | Basically 'id' from Control.Category. Just pass the incoming
-- ByteString through.
idProtocol :: Monad m => Protocol' m B.ByteString
idProtocol = id


-- | A simple serialization strategy that works on lines of strings.
linesProtocol :: MonadThrow m => Protocol' m B.ByteString
linesProtocol = Protocol { protoEnc = C.map (\x -> B.concat [x, "\n"])
                         , protoDec = linesConduit }


-------------------------------------------------------------------------------
-- | Channel the 'Serialize' instance through 'Base64' encoding to
-- make it newline-safe, then turn into newline-separated stream.
base64SerProtocol
    :: (MonadUnsafeIO m, MonadThrow m, Ser.Serialize a)
    => Protocol' m a
base64SerProtocol = prismToProtocol pSerialize


-------------------------------------------------------------------------------
-- | Encode and decode a gzip stream
gzipProtocol :: (MonadUnsafeIO m, MonadThrow m)
             => Protocol m B.ByteString B.ByteString
gzipProtocol = Protocol gzip ungzip


-------------------------------------------------------------------------------
-- | Protocol for converting to/from any stream type 'b' and CSV type 'a'.
csvProtocol :: (MonadUnsafeIO m, MonadThrow m, CSV b a)
            => CSVSettings -> Protocol m b a
csvProtocol set = Protocol (fromCSV set) (intoCSV set)


-------------------------------------------------------------------------------
-- | Use 'Show'/'Read' instances to stream-serialize. You must be
-- careful not to have any newline characters inside, or the stream
-- will get confused.
--
-- This is meant for debugging more than anything. Do not use it in
-- serious matters. Use 'serProtocol' instead.
showProtocol :: (MonadUnsafeIO m, MonadThrow m, Read a, Show a)
             => Protocol' m a
showProtocol = prismToProtocol pShow


-- | Helper for reliable serialization through Base64 encoding so it
-- is newline-free.
serialize :: Ser.Serialize a => a -> B.ByteString
serialize = Base64.encode . Ser.encode


-- | Helper for reliable deserialization
deserialize :: Ser.Serialize c => B.ByteString -> Either String c
deserialize = Ser.decode <=< Base64.decode


-- | Serialize with the 'Serialize' instance.
--
-- Any 'Prism' can be used as follows:
--
-- >>> import Control.Lens
--
-- To decode ByteString into target object:
--
-- >>> firstOf myPrism byteStr
-- Just a
--
-- To encode an object into ByteString:
--
-- >>> firstOf (re myPrism) myObject
-- Just byteStr
pSerialize :: Ser.Serialize a => Prism' B.ByteString a
pSerialize = prism serialize (\x -> either (const $ Left x) Right $ deserialize x)


-- | Serialize with the Show/Read instances
pShow :: (Show a, Read a) => Prism' B.ByteString a
pShow = prism
          (B.pack . show)
          (\x -> maybe (Left x) Right . readMay . B.unpack $ x)






-- | Parse a line of input and eat a tab character that may be at the
-- very end of the line. This tab is put by hadoop if this file is the
-- result of a previous M/R that didn't have any value in the reduce
-- step.
parseLine :: Parser B.ByteString
parseLine = ln <* endOfLine
    where
      ln = do
        x <- takeTill (== '\n')
        return $ if B.length x > 0 && B.last x == '\t'
          then B.init x
          else x


-- | Turn incoming stream into a stream of lines. This will
-- automatically eat tab characters at the end of the line.
linesConduit :: MonadThrow m => Conduit B.ByteString m B.ByteString
linesConduit = conduitParser parseLine =$= C.map snd


-- | Parse lines of (key,value) for hadoop reduce stage
lineC :: MonadThrow m
      => Int
      -- ^ Number of key segments (usually just 1), but may be higher
      -- if you're using multiple parts in your key.
      -> Conduit B.ByteString m (CompositeKey, B.ByteString)
lineC n = linesConduit =$= C.map ppair
    where
      ppair line = (k, v)
          where
            k = take n spl
            v = B.intercalate "\t" $ drop n spl
            -- ^ Re-assemble remaining segments to restore
            -- correctness.
            spl = B.split '\t' line
