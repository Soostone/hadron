{-# LANGUAGE BangPatterns #-}

module Hadoop.Streaming.Utils where


-------------------------------------------------------------------------------
import           Control.Monad
import           Control.Monad.Trans
import           Data.Conduit
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
