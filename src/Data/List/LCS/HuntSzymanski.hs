{-# LANGUAGE FlexibleContexts #-}

-----------------------------------------------------------------------------
-- |
-- Module      :  Data.List.LCS.HuntSzymanski
-- Copyright   :  (c) Ian Lynagh 2005
-- License     :  BSD or GPL v2
-- 
-- Maintainer  :  igloo@earth.li
-- Stability   :  provisional
-- Portability :  non-portable (uses STUArray)
--
-- This is an implementation of the Hunt-Szymanski LCS algorithm.
-- Derived from the description in \"String searching algorithms\" by
-- Graham A Stephen, ISBN 981021829X.
-----------------------------------------------------------------------------

module Data.List.LCS.HuntSzymanski (
    -- * Algorithm
    -- $algorithm

    -- * LCS
    lcs
    ) where

import Data.Array (listArray, (!))
import Data.Array.MArray (newArray, newArray_)
import Data.Array.Base (unsafeRead, unsafeWrite)
import Data.Array.ST (STArray, STUArray)
import Control.Monad (when)
import Control.Monad.ST (ST, runST)
import Data.List (groupBy, sort)

{- $algorithm
We take two sequences, @xs@ and @ys@, of length @\#xs@ and @\#ys@.

First we make an array

> matchlist[i=0..(#xs-1)]

such that

> (matchlist[i] = js) => ((j `elem` js) <=> (xs !! i == ys !! j))
>                     && sort js == reverse js

i.e. @matchlist[i]@ is the indices of elements of @ys@ equal to the
ith element of @xs@, in descending order.

Let @\#xys@ be the minimum of @\#xs@ and @\#ys@. Trivially this is the maximum
possible length of the LCS of @xs@ and @ys@. Then we can imagine an array

> k[i=0..#xs][l=0..#xys]

such that @k[i][l] = j@ where @j@ is the smallest value such that the
LCS of @xs[0..i]@ and @ys[0..j]@ has length @l@. We use @\#ys@ to
mean there is no such @j@.

We will not need to whole array at once, though. Instead we use an array

> kk[l=0..#xys]

representing a row of @kk@ for a particular @i@. Initially it is for
@i = -1@, so @kk[0] = -1@ and @kk[l] = \#ys@ otherwise. As the algorithm
progresses we will increase @i@ by one at the outer level and compute
the replacement values for @k@'s elements.

But we want more than just the length of the LCS, we also want the LCS
itself. Another array

> revres[l=0..#xys]

stores the list of @xs@ indices an LCS of length @l@, if one is known,
at @revres[l]@.

Now, suppose @kk@ contains @k[i-1]@. We consider each @j@ in @matchlist[i]@
in turn. We find the @l@ such that @k[l-1] < j <= k[l]@. If @j < k[l]@ then
we updated @k[l]@ to be @j@ and set @revres[l]@ to be @i:revres[l-1]@.

Finding @l@ is basically binary search, but there are some tricks we can
do. First, as the @j@s are decreasing the last @l@ we had for this @i@ is
an upper bound on this @l@. Second, we use another array

> lastl[j=0..#ys-1]

to store the @l@ we got last time for this @j@, initially all @1@. As the
values in @kk[j]@ monotonically decrease this is a lower bound for @l@.
We also test to see whether this old @l@ is still @l@ before we start the
binary search.
-}

-- |The 'lcs' function takes two lists and returns a list with a longest
-- common subsequence of the two.
lcs :: Ord a => [a] -> [a] -> [a]
-- Start off by returning the common prefix
lcs [] _ = []
lcs _ [] = []
lcs (c1:c1s) (c2:c2s)
 | c1 == c2 = c1 : lcs c1s c2s
-- Then reverse everything, get the backwards LCS and reverse it
lcs s1 s2 = lcs_tail [] (reverse s1) (reverse s2)

-- To get the backwards LCS, we again start off by returning the common
-- prefix (or suffix, however you want to think of it  :-)  )
lcs_tail :: Ord a => [a] -> [a] -> [a] -> [a]
lcs_tail acc (c1:c1s) (c2:c2s)
 | c1 == c2 = lcs_tail (c1:acc) c1s c2s
lcs_tail acc [] _ = acc
lcs_tail acc _ [] = acc
-- Then we begin the real algorithm
lcs_tail acc s1 s2 = runST (lcs' acc s1 s2)

lcs' :: Ord a => [a] -> [a] -> [a] -> ST s [a]
lcs' acc xs ys =
 do let max_xs = length xs
        max_ys = length ys
        minmax = max_xs `min` max_ys
    -- Initialise all the arrays
    matchlist <- newArray_ (0, max_xs - 1)
    mk_matchlist matchlist xs ys
    kk <- newArray (0, minmax) max_ys
    unsafeWrite kk 0 (-1)
    lastl <- newArray (0, max_ys - 1) 1
    revres <- newArray_ (0, minmax)
    unsafeWrite revres 0 []
    -- Pass the buck to lcs'' to finish the job off
    is <- lcs'' matchlist lastl kk revres max_xs max_ys minmax
    -- Convert the list of i indices into the result sequence
    let axs = listArray (0, max_xs - 1) xs
    return $ map (axs !) is ++ acc

eqFst :: Eq a => (a, b) -> (a, b) -> Bool
eqFst (x, _) (y, _) = x == y

-- mk_matchlist fills the matchlist array such that if
-- xs !! i == ys !! j then (j+1) `elem` matchlist ! i
-- and matchlist ! i is decreasing for all i
mk_matchlist :: Ord a => STArray s Int [Int] -> [a] -> [a] -> ST s ()
mk_matchlist matchlist xs ys =
 do let -- xs' is a list of (string, ids with that string in xs)
        xs' = map (\sns -> (fst (head sns), map snd sns))
            $ groupBy eqFst $ sort $ zip xs [0..]
        -- ys' is similar, only the ids are reversed
        ys' = map (\sns -> (fst (head sns), reverse $ map snd sns))
            $ groupBy eqFst $ sort $ zip ys [0..]
        -- add_to_matchlist does all the hardwork
        add_to_matchlist all_xs@((sx, idsx):xs'') all_ys@((sy, idsy):ys'')
         = case compare sx sy of
               -- If we have the same string in xs'' and ys'' then all
               -- the indices in xs'' must map to the indices in ys''
               EQ -> do sequence_ [ unsafeWrite matchlist i idsy
                                  | i <- idsx ]
                        add_to_matchlist xs'' ys''
               -- If the string in xs'' is smaller then there are no
               -- corresponding indices in ys so we assign all the xs''
               -- indices the empty list
               LT -> do sequence_ [ unsafeWrite matchlist i []
                                  | i <- idsx ]
                        add_to_matchlist xs'' all_ys
               -- Otherwise the string appears in ys only, so we ignore it
               GT -> do add_to_matchlist all_xs ys''
        -- If we run out of ys'' altogether then just go through putting
        -- in [] for the list of indices of each index remaining in xs''
        add_to_matchlist ((_, idsx):xs'') []
         = do sequence_ [ unsafeWrite matchlist i [] | i <- idsx ]
              add_to_matchlist xs'' []
        -- When we run out of xs'' we are done
        add_to_matchlist [] _ = return ()
    -- Finally, actually call add_to_matchlist to populate matchlist
    add_to_matchlist xs' ys'

lcs'' :: STArray s Int [Int] -- matchlist
      -> STUArray s Int Int -- lastl
      -> STUArray s Int Int -- kk
      -> STArray s Int [Int] -- revres
      -> Int -> Int -> Int -> ST s [Int]
lcs'' matchlist lastl kk revres max_xs max_ys minmax =
 do let -- Out the outermost level we loop over the indices i of xs
        loop_i = sequence_ [ loop_j i | i <- [0..max_xs - 1] ]
        -- For each i we loop over the matching indices j of elements of ys
        loop_j i = do js <- unsafeRead matchlist i
                      with_js i js minmax
        -- Deal with this i and j
        with_js i (j:js) max_bound =
            do x0 <- unsafeRead lastl j
               l <- find_l j x0 max_bound
               unsafeWrite lastl j l
               vl <- unsafeRead kk l
               when (j < vl) $ do
                   unsafeWrite kk l j
                   rs <- unsafeRead revres (l - 1)
                   unsafeWrite revres l (i:rs)
               with_js i js l
        with_js _ [] _ = return ()
        -- find_l returns the l such that kk ! (l-1) < j <= kk ! l
        find_l j x0 z0
         = let f x z
                | x + 1 == z = return z
                | otherwise  = let y = (x + z) `div` 2
                               in do vy <- unsafeRead kk y
                                     if vy < j
                                      then f y z
                                      else f x y
           in j `seq` do q1 <- unsafeRead kk x0
                         if j <= q1
                           then return x0
                           else f x0 z0
    -- Do the hard work
    loop_i
    -- Find where the result starts
    succ_l <- find_l max_ys 1 (minmax + 1)
    -- Get the result
    unsafeRead revres (succ_l - 1)

