{-# LANGUAGE GADTs #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RecordWildCards #-}
module Database.Kioku.Internal.RankedIndex
  ( writeRanked
  , HasRank(..)
  , newRankHeap
  , heapInsert
  , heapToList
  , heapMin
  , heapMerge
  ) where

import            Control.Monad.Trans.State (execStateT, get, put)
import            Control.Monad.IO.Class (liftIO)
import            Data.Foldable
import qualified  Data.ByteString as BS
import            Data.List (sortOn)
import            Data.Traversable (for)
import qualified  Data.Vector.Unboxed.Mutable as V
import qualified  Data.Vector.Mutable as VM
import            Data.Word (Word8)
import            System.IO

import            Database.Kioku.Internal.Buffer
import            Database.Kioku.Memorizable
import            Database.Kioku.Internal.TrieIndex (breakCommonPrefix, buildSortedOffsetArray)

--class (Eq r, Ord r) => HasRank a r | a -> r where
--  getRank :: a -> r

class HasRank a where
  getRank :: a -> Word8

instance HasRank a => HasRank (a, Int) where
  getRank (a, _) = getRank a

data RankHeap a =
  RankHeap { rankHeapIdx :: !Int
           , rankHeapVec :: !(VM.IOVector a)
           }

newRankHeap :: HasRank a => Int -> IO (RankHeap a)
newRankHeap = fmap (RankHeap (-1)) . VM.new

heapInsert :: HasRank a => a -> RankHeap a -> IO (RankHeap a)
heapInsert a heap@RankHeap{..}
  | rankHeapIdx == VM.length rankHeapVec - 1 = do
    root <- VM.read rankHeapVec 0
    if getRank root >= getRank a
       then pure heap
       else do
         VM.write rankHeapVec 0 a
         percolateUp 0
         pure heap

  | otherwise = do
    let newIdx = rankHeapIdx + 1
    VM.write rankHeapVec newIdx a
    percolateDown newIdx
    pure $ heap{ rankHeapIdx = newIdx }
  where
    percolateDown 0 = pure ()
    percolateDown i = do
      let pIdx = (div (i + 1) 2) - 1
      child  <- VM.read rankHeapVec i
      parent <- VM.read rankHeapVec pIdx
      if getRank child < getRank parent
         then do
           VM.swap rankHeapVec i pIdx
           percolateDown pIdx
         else pure ()

    percolateUp i = do
      let c1Idx = (i + 1) * 2 - 1
          c2Idx = c1Idx + 1
      focus <- VM.read rankHeapVec i

      mbC1 <- fmap ((,) c1Idx) <$> heapSafeRead c1Idx heap
      mbC2 <- fmap ((,) c2Idx) <$> heapSafeRead c2Idx heap
      case asum $ sortOn (fmap $ getRank . snd) [mbC1, mbC2] of
        Just (childIdx, child) | getRank focus > getRank child -> do
          VM.swap rankHeapVec i childIdx
          percolateUp childIdx
        _ -> pure ()

heapSafeRead :: Int -> RankHeap a -> IO (Maybe a)
heapSafeRead i RankHeap{..} =
  if i > rankHeapIdx then pure Nothing else Just <$> VM.read rankHeapVec i

heapMerge :: HasRank a => RankHeap a -> RankHeap a -> IO (RankHeap a)
heapMerge h1 h2 = do
  let (little, big) | rankHeapIdx h1 > rankHeapIdx h2 = (h2, h1)
                    | otherwise = (h1, h2)

  (`execStateT` big) $ for [0 .. rankHeapIdx little] $ \idx -> do
    item <- liftIO $ VM.read (rankHeapVec little) idx
    heap <- get
    put =<< liftIO (heapInsert item heap)

heapToList :: HasRank a => RankHeap a -> IO [a]
heapToList RankHeap{..} =
  sortOn getRank <$> traverse (VM.read rankHeapVec) [0 .. rankHeapIdx]

heapMin :: HasRank a => RankHeap a -> IO (Maybe a)
heapMin heap = heapSafeRead 0 heap

writeRanked :: (HasRank a, Memorizable a)
           => (a -> BS.ByteString)
           -> Int
           -> Buffer
           -> ((Handle -> IO ()) -> IO b)
           -> IO b
writeRanked keyFunc itemLimit buf runWriter = do
  let rowCount = readBufAt buf (bufLength buf - 8)
  vec <- buildSortedOffsetArray keyFunc buf rowCount
  runWriter (writeRankedIndex keyFunc itemLimit vec buf)

writeRankedIndex :: (Memorizable a, HasRank a)
                 => (a -> BS.ByteString)
                 -> Int -- max items per node
                 -> V.IOVector Int
                 -> Buffer
                 -> Handle
                 -> IO ()
writeRankedIndex keyFunc itemLimit vec buf h = do
    heap <- newRankHeap itemLimit

    (rootOffset, totalBytes, _, _) <- writeTrie "" "" [] heap 0 0
    BS.hPutStr h $ memorize (totalBytes - rootOffset)
  where
    len = V.length vec

    --writeTrie :: BS.ByteString -- context (the parent's key)
    --          -> BS.ByteString -- arc
    --          -> [Int]         -- subtrie offsets
    --          -> RankHeap (a, Int) -- [Int]         -- value offsets
    --          -> Int           -- iteration index
    --          -> Int           -- current offset
    --          -> IO (Int, Int, Int, RankHeap (a, Int)) -- subtrie offset, total offset, ndx, values
    writeTrie ctx arc subtries heap ndx offset
      | ndx < len = do
        datOffset <- V.read vec ndx

        let item = readRowAt buf datOffset
            key = keyFunc $ item
            (ctxShared, ctxLeft, ctxRight) = breakCommonPrefix ctx key
            (arcShared, arcLeft, arcRight) = breakCommonPrefix arc ctxRight
            z  = BS.null
            nz = not . z
            sameCtx = z ctxLeft

        case () of
          -- keys are identical
          --   E.G. When processing ABC, ABDA, ABDA
          --   upon encountering the *second* ABDA we will have:
          --     ctx: AB
          --     arc: DA
          --     key: ABDA
          --     ctxShared: AB
          --     ctxLeft: ""
          --     ctxRight: DA
          --     arcShared: DA
          --     arcLeft: ""
          --     arcRight: ""
          --
          _ | sameCtx, z arcLeft, z arcRight -> do
            heap' <- heapInsert (item, datOffset) heap
            writeTrie ctx arc subtries heap' (ndx + 1) offset

          -- new item is a subtrie of the current arc
          --   E.G. When processing ABC, ABDA, ABDAX
          --   upon encountering ABDAX we will have:
          --     ctx: AB
          --     arc: DA
          --     key: ABDAX
          --     ctxShared: AB
          --     ctxLeft: ""
          --     ctxRight: DAX
          --     arcShared: DA
          --     arcLeft: ""
          --     arcRight: X
          _ | sameCtx, z arcLeft -> do
            subHeap <- heapInsert (item, datOffset) =<< newRankHeap itemLimit

            let subctx = ctx `BS.append` arc
            (sub, newOffset, newNdx, subHeap') <- writeTrie subctx
                                                         arcRight
                                                         []
                                                         subHeap
                                                         (ndx + 1)
                                                         offset

            heap' <- heapMerge heap subHeap'
            writeTrie ctx arc (sub:subtries) heap' newNdx newOffset

          -- new item is a separate (non-zero) arc in the same context
          --   E.G. When processing ABC, ABDA, ABDQ
          --   upon encountering ABDQ we will have:
          --     ctx: AB
          --     arc: DA
          --     key: ABDQ
          --     ctxShared: AB
          --     ctxLeft: ""
          --     ctxRight: DQ
          --     arcShared: D
          --     arcLeft: A
          --     arcRight: Q
          _ | sameCtx, nz arcShared, nz arcLeft, nz arcRight -> do
            values <- map snd <$> heapToList heap
            written <- flushTrie arcLeft subtries values

            let subL = offset
                subctx = ctxShared `BS.append` arcShared

            subHeap <- heapInsert (item, datOffset) =<< newRankHeap itemLimit
            (subR, newOffset, newNdx, subHeap') <- writeTrie subctx
                                                   arcRight
                                                   []
                                                   subHeap
                                                   (ndx + 1)
                                                   (offset + written)

            heap' <- heapMerge subHeap' heap
            writeTrie ctx arcShared [subR,subL] heap' newNdx newOffset

          -- the item examined was not part of our parent trie's context,
          -- so we can flush the current arc and return without moving
          -- on to the next index in the array. The current item will be
          -- reconsidered further up the recursion chain.
          _ -> do
            values <- map snd <$> heapToList heap
            bytesWritten <- flushTrie arc subtries values
            pure (offset, offset + bytesWritten, ndx, heap)

      | otherwise = do
        values <- map snd <$> heapToList heap
        bytesWritten <- flushTrie arc subtries values
        pure (offset, offset + bytesWritten, ndx, heap)

    flushTrie :: BS.ByteString
              -> [Int]  -- subtrie offsets
              -> [Int]  -- value offsets
              -> IO Int -- Bytes written
    flushTrie arc subtries values = do
      let arcLength = BS.length arc

          subtriesForward = reverse subtries
          subtriesCount = length subtriesForward

          valuesForward = reverse values
          valuesCount = length valuesForward

          bytes = [ [ memorize $ arcLength, arc]
                  , memorize <$> (subtriesCount:subtriesForward)
                  , memorize <$> (valuesCount:valuesForward)
                  ]

          byteCount = sum [ BS.length bs | bsl <- bytes, bs <- bsl ]

      traverse_ (traverse_ $ BS.hPutStr h) bytes

      pure $ byteCount
