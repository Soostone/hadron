{-# LANGUAGE BangPatterns    #-}
{-# LANGUAGE TemplateHaskell #-}

module Hadron.Utils where


-------------------------------------------------------------------------------
import           Control.Lens
import           Control.Monad
import           Control.Monad.Trans
import           Data.Conduit
import           Data.List.Split
import           Numeric
import           Safe
import           System.Random       (randomRIO)
-------------------------------------------------------------------------------



-- | Perform a given monadic action once every X elements flowing
-- through this conduit.
performEvery
    :: (Monad m)
    => Integer
    -- ^ Once every N items that flow throught he pipe.
    -> (Integer -> m ())
    -> ConduitM a a m ()
performEvery n f = go 1
    where
      go !i = do
          x <- await
          case x of
            Nothing -> return ()
            Just x' -> do
                when (i `mod` n == 0) $ lift (f i)
                yield $! x'
                go $! i + 1



-------------------------------------------------------------------------------
data File = File {
      _filePerms :: String
    , _fileSize  :: Int
    , _fileDate  :: String
    , _fileTime  :: String
    , _filePath  :: String
    } deriving (Eq,Show,Read,Ord)
makeLenses ''File


parseLs :: String -> Maybe File
parseLs str =
    let xs = split (dropDelims . condense $ oneOf "\t ") str
    in  File <$> xs !? 0
             <*> (xs !? 2 >>= readMay)
             <*> xs !? 3
             <*> xs !? 4
             <*> xs !? 5


-------------------------------------------------------------------------------
-- | Generate a random token
randomToken :: Int -> IO String
randomToken n = do
    is <- sequence . take n $ repeat mk
    return $ concat $ map (flip showHex "") is
  where
    mk :: IO Int
    mk = randomRIO (0,15)


-------------------------------------------------------------------------------
(!?) :: [a] -> Int -> Maybe a
(!?) = atMay
