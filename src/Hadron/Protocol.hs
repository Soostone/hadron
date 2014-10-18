{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE TemplateHaskell       #-}

module Hadron.Protocol
    (
      Protocol (..)
    , Protocol'
    , protoEnc, protoDec
    , protoEncIS
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
    , lineC
    , mkKey

    , eitherPrism
    , eitherProtocol


    ) where

-------------------------------------------------------------------------------
import           Blaze.ByteString.Builder
import           Control.Category
import           Control.Concurrent.Async
import           Control.Error
import           Control.Lens
import           Control.Monad
import qualified Data.ByteString.Base64   as Base64
import qualified Data.ByteString.Char8    as B
import           Data.CSV.Conduit
import           Data.Monoid
import qualified Data.SafeCopy            as SC
import qualified Data.Serialize           as Ser
import           Data.String
import           Prelude                  hiding (id, (.))
import           System.IO.Streams        (InputStream, OutputStream)
import qualified System.IO.Streams        as S
-------------------------------------------------------------------------------
import           Hadron.Streams
import qualified Hadron.Streams.Bounded   as S
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
      _protoEnc :: OutputStream b -> IO (OutputStream a)
    , _protoDec :: InputStream b  -> IO (InputStream a)
    }
makeLenses ''Protocol

instance Category Protocol where
    id = Protocol return return
    p1 . p2 = Protocol { _protoEnc = \ o -> _protoEnc p2 o >>= _protoEnc p1
                       , _protoDec = \ i -> _protoDec p2 i >>= _protoDec p1 }


-------------------------------------------------------------------------------
-- | Get an InputStream based encode action out of the protocol.
protoEncIS :: Protocol b a -> InputStream a -> IO (InputStream b)
protoEncIS p is = do
    (is', os') <- S.makeChanPipe 64
    os'' <- (p ^. protoEnc) os'
    async $ S.connect is os''
    return is'


-------------------------------------------------------------------------------
-- | Lift 'Prism' to work with a newline-separated stream of objects.
--
-- It is assumed that the prism you supply to this function does not
-- add newlines itself. You need to make them newline-free for this to
-- work properly.
prismToProtocol :: Prism' B.ByteString a -> Protocol' a
prismToProtocol p =
    Protocol { _protoEnc = \ i -> S.contramap (write . review p) i
             , _protoDec = \ i -> S.map (mkErr . preview p) =<< streamLines i }
  where
    write x = toByteString $ fromByteString x `mappend` nl
    nl = fromByteString "\n"
    mkErr = fromMaybe $
            error "Unexpected: Prism could not decode incoming value."


-------------------------------------------------------------------------------
-- | Basically 'id' from Control.Category. Just pass the incoming
-- ByteString through.
idProtocol :: Protocol' B.ByteString
idProtocol = id


-- | A simple serialization strategy that works on lines of strings.
linesProtocol :: Protocol' B.ByteString
linesProtocol = Protocol { _protoEnc = S.contramap (\x -> B.concat [x, "\n"])
                         , _protoDec = streamLines }


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
gzipProtocol = Protocol (S.gzip S.defaultCompressionLevel) S.gunzip


-------------------------------------------------------------------------------
-- | Protocol for converting to/from any stream type 'b' and CSV type 'a'.
csvProtocol :: (IsString b, Monoid b, CSV b a) => CSVSettings -> Protocol b a
csvProtocol cset = Protocol
  (S.contramap (\r -> rowToStr cset r <> "\n"))
  (\ i -> conduitStream id (intoCSV cset) i)


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


-- | Parse lines of (key,value) for hadoop reduce stage
lineC
    :: Int
    -- ^ Number of key segments (usually just 1), but may be higher
    -- if you're using multiple parts in your key.
    -> InputStream B.ByteString
    -> IO (InputStream (CompositeKey, B.ByteString))
lineC n i = streamLines i >>= S.map ppair
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



