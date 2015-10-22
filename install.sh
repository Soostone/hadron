#!/bin/bash

# git clone https://github.com/Soostone/hadron.git
# cd hadron
# cabal sandbox init

cabal update

git clone https://github.com/Soostone/katip.git
mv katip/katip/katip.cabal katip/katip/katip.cabal.orig
cp katip-cabal/katip.cabal katip/katip/katip.cabal

cabal install katip/katip/katip.cabal --ghc-options=-XFlexibleContexts
cabal install lcs --ghc-options=-XFlexibleContexts

cabal install
