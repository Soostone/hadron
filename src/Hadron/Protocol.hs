{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RankNTypes            #-}

module Hadron.Protocol
    (
      Protocol (..)
    , Protocol'
    , prismToProtocol

    , base64SerProtocol
    , base64SafeCopyProtocol
    , idProtocol
    , linesProtocol
    , gzipProtocol
    , showProtocol
    , csvProtocol

    -- * Serialization Prisms
    , pSerialize
    , pSafeCopy
    , pShow

    -- * Serialization Utils
    , serialize
    , deserialize

    -- * Utils
    , linesConduit
    , lineC
    , mkKey

    , eitherPrism
    , eitherProtocol


    ) where

-------------------------------------------------------------------------------
import           Blaze.ByteString.Builder
import           Control.Applicative
import           Control.Category
import           Control.Error
import           Control.Lens
import           Control.Monad
import           Control.Monad.Catch
import           Control.Monad.Trans.Resource
import           Data.Attoparsec.ByteString.Char8 (Parser, endOfLine, takeTill)
import qualified Data.ByteString.Base64           as Base64
import qualified Data.ByteString.Char8            as B
import           Data.Conduit
import           Data.Conduit.Attoparsec
import           Data.Conduit.Blaze
import qualified Data.Conduit.List                as C
import           Data.Conduit.Zlib                (gzip, ungzip)
import           Data.CSV.Conduit
import           Data.Monoid
import qualified Data.SafeCopy                    as SC
import qualified Data.Serialize                   as Ser
import           Prelude                          hiding (id, (.))

-------------------------------------------------------------------------------
import           Hadron.Types
-------------------------------------------------------------------------------


                              -------------------
                              -- Serialization --
                              -------------------


-- | Like 'Protocol' but fixes the source format as 'ByteString'.
type Protocol' a = Protocol B.ByteString a


-- | A 'Protocol's is a serialization strategy when we're dealing with
-- streams of records, allowing for arbitrary data formats, delimiters
-- and other potential obstructions.
--
-- Most of the time we'll be using 'Protocol\''s.
data Protocol b a = Protocol {
      protoEnc :: Conduit a (ResourceT IO ) b
    , protoDec :: Conduit b (ResourceT IO) a
    }


instance Category Protocol where
    id = Protocol (C.map id) (C.map id)
    p1 . p2 = Protocol { protoEnc = protoEnc p1 =$= protoEnc p2
                       , protoDec = protoDec p2 =$= protoDec p1 }



-------------------------------------------------------------------------------
-- | Lift 'Prism' to work with a newline-separated stream of objects.
--
-- It is assumed that the prism you supply to this function does not
-- add newlines itself. You need to make them newline-free for this to
-- work properly.
prismToProtocol :: Prism' B.ByteString a -> Protocol' a
prismToProtocol p =
    Protocol { protoEnc = C.map (review p) =$= write
             , protoDec = linesConduit =$= C.mapMaybe (preview p) }
  where
    write = C.map (\x -> fromByteString x `mappend` nl) =$=
            builderToByteString
    nl = fromByteString "\n"


-------------------------------------------------------------------------------
-- | Basically 'id' from Control.Category. Just pass the incoming
-- ByteString through.
idProtocol :: Protocol' B.ByteString
idProtocol = id


-- | A simple serialization strategy that works on lines of strings.
linesProtocol :: Protocol' B.ByteString
linesProtocol = Protocol { protoEnc = C.map (\x -> B.concat [x, "\n"])
                         , protoDec = linesConduit }


-------------------------------------------------------------------------------
-- | Channel the 'Serialize' instance through 'Base64' encoding to
-- make it newline-safe, then turn into newline-separated stream.
base64SerProtocol :: Ser.Serialize a => Protocol' a
base64SerProtocol = prismToProtocol pSerialize


-------------------------------------------------------------------------------
-- | Channel the 'Serialize' instance through 'Base64' encoding to
-- make it newline-safe, then turn into newline-separated stream.
base64SafeCopyProtocol :: SC.SafeCopy a => Protocol' a
base64SafeCopyProtocol = prismToProtocol pSafeCopy


-------------------------------------------------------------------------------
-- | Encode and decode a gzip stream
gzipProtocol :: Protocol B.ByteString B.ByteString
gzipProtocol = Protocol gzip ungzip


-------------------------------------------------------------------------------
-- | Protocol for converting to/from any stream type 'b' and CSV type 'a'.
csvProtocol :: (CSV b a) => CSVSettings -> Protocol b a
csvProtocol cset = Protocol (fromCSV cset) (intoCSV cset)


-------------------------------------------------------------------------------
-- | Use 'Show'/'Read' instances to stream-serialize. You must be
-- careful not to have any newline characters inside, or the stream
-- will get confused.
--
-- This is meant for debugging more than anything. Do not use it in
-- serious matters. Use 'serProtocol' instead.
showProtocol :: (Read a, Show a) => Protocol' a
showProtocol = prismToProtocol pShow


-- | Helper for reliable serialization through Base64 encoding so it
-- is newline-free.
serialize :: Ser.Serialize a => a -> B.ByteString
serialize = Base64.encode . Ser.encode


-- | Helper for reliable deserialization
deserialize :: Ser.Serialize c => B.ByteString -> Either String c
deserialize = Ser.decode <=< Base64.decode


-- | Serialize with the 'Serialize' instance coupled with Base64
-- encoding, so it's free of restrictied characters.
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


-------------------------------------------------------------------------------
pSafeCopy :: SC.SafeCopy a => Prism' B.ByteString a
pSafeCopy = prism'
  (Base64.encode . Ser.runPut . SC.safePut)
  (hush . (Ser.runGet SC.safeGet <=< Base64.decode))


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




-------------------------------------------------------------------------------
-- | Only works when a and b are disjoint types.
eitherPrism :: Prism' B.ByteString a -> Prism' B.ByteString b -> Prism' B.ByteString (Either a b)
eitherPrism f g = prism' enc dec
    where
      enc (Left a) = review f a
      enc (Right b) = review g b

      dec bs = (preview f bs <&> Left)
               `mplus`
               (preview g bs <&> Right)


-------------------------------------------------------------------------------
eitherProtocol
    :: Prism' B.ByteString a
    -> Prism' B.ByteString b
    -> Protocol' (Either a b)
eitherProtocol f g = prismToProtocol (eitherPrism f g)



