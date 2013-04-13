{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE EmptyDataDecls             #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE RankNTypes                 #-}

module Hadoop.Streaming.EDSL where

-------------------------------------------------------------------------------
import           Control.Applicative
import           Control.Monad.Operational
import           Control.Monad.State
import           Data.ByteString           (ByteString)
-------------------------------------------------------------------------------


data ContState = ContState


data ConI a where
    Dummy :: ConI String
    Blah :: ConI Int


newtype Controller a = Controller { unController :: Program ConI a }
    deriving (Functor, Applicative, Monad)


-------------------------------------------------------------------------------
orchestrate :: MonadIO m => Controller a -> ContState -> m (a, ContState)
orchestrate (Controller p) s = runStateT (go p) s
    where
      go = eval . view

      eval (Return a) = return a
      eval (i :>>= f) = eval' i >>= go . f

      eval' Dummy = return "dummy"
      eval' Blah = return 3
