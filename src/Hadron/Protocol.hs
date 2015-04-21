{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE TemplateHaskell       #-}

module Hadron.Protocol
    (
      Protocol (..)
    , Protocol'
    , protoEnc, protoDec
    , prismToProtocol
    , filterP

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
    , linesConduit
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
import           Data.String
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
      _protoEnc :: Conduit a (ResourceT IO) b
    , _protoDec :: Conduit b (ResourceT IO) a
    }
makeLenses ''Protocol


instance Category Protocol where
    id = Protocol (C.map id) (C.map id)
    p1 . p2 = Protocol { _protoEnc = _protoEnc p1 =$= _protoEnc p2
                       , _protoDec = _protoDec p2 =$= _protoDec p1}


-- -------------------------------------------------------------------------------
-- -- | Get an InputStream based encode action out of the protocol.
-- protoEncIS :: Protocol b a -> InputStream a -> IO (InputStream b)
-- protoEncIS p is = do
--     (is', os') <- S.makeChanPipe 64
--     os'' <- (p ^. protoEnc) os'
--     async $ S.connect is os''
--     return is'


-------------------------------------------------------------------------------
-- | Filter elements in both directions of the protocol.
filterP :: (a -> Bool) -> Protocol b a -> Protocol b a
filterP f (Protocol enc dec) = Protocol enc' dec'
    where
      enc' = C.filter f =$= enc
      dec'  = dec =$= C.filter f



-------------------------------------------------------------------------------
-- | Lift 'Prism' to work with a newline-separated stream of objects.
--
-- It is assumed that the prism you supply to this function does not
-- add newlines itself. You need to make them newline-free for this to
-- work properly.
prismToProtocol :: Prism' B.ByteString a -> Protocol' a
prismToProtocol p =
    Protocol { _protoEnc = C.map (review p) =$= write
             , _protoDec = linesConduit =$= C.map (mkErr . preview p) }
  where
    write = C.map (\x -> fromByteString x `mappend` nl) =$=
            builderToByteString
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
linesProtocol = Protocol { _protoEnc = C.map (\x -> B.concat [x, "\n"])
                         , _protoDec = linesConduit }


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
-- | Protocol for converting to/from any stream type 'b' and CSV type
-- 'a'.
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



