module Database.Kioku
  (
  -- Construction
    Memorizable(..)
  , memorizeRows
  , indexRows

  -- Usage
  , TrieIndex
  , withTrieIndex
  , trieLookup
  , trieMatch
  , lookupSubtrie
  , trieElems
  , trieRootElems

  -- Diagnostic
  , inspectTrieIndex
  , loadAllRows
  ) where

import            Control.Applicative
import            Control.Monad
import            Data.Bits
import qualified  Data.ByteString as BS
import qualified  Data.ByteString.Char8 as CBS
import qualified  Data.ByteString.Lazy as LBS
import qualified  Data.ByteString.Unsafe as UBS
import            Data.IORef
import            Data.Foldable
import            Data.Function
import            Data.Traversable
import qualified  Data.Vector.Unboxed.Mutable as V
import qualified  Data.Vector.Algorithms.AmericanFlag as S
import            Data.Word
import            Foreign.Ptr
import            System.IO
import            System.IO.MMap

class Memorizable a where
  memorize :: a -> BS.ByteString
  recall :: BS.ByteString -> a

memorizeRows :: Memorizable a => [a] -> FilePath -> IO Int
memorizeRows as path = do
  withFile path WriteMode $ \h -> do
    count <- newIORef (0::Int)

    for_ as $ \a -> do
      let bytes = memorize a
          len = BS.length bytes
          header = memorize len

      BS.hPutStr h header
      BS.hPutStr h bytes

      modifyIORef count (+1)

    c <- readIORef count

    BS.hPutStr h (memorize c)

    pure c

newtype Buffer = Buffer BS.ByteString

bufLength :: Buffer -> Int
bufLength (Buffer bs) = BS.length bs

newBuffer :: Ptr () -> Int -> IO Buffer
newBuffer ptr size = Buffer <$> UBS.unsafePackCStringLen (castPtr ptr, size)

withBuffer :: FilePath -> (Buffer -> IO a) -> IO a
withBuffer path action = do
  mmapWithFilePtr path ReadOnly Nothing $ \(ptr, sz) -> do
    buf <- newBuffer ptr sz
    action buf

readBufAt :: Memorizable a => Buffer -> Int -> a
readBufAt (Buffer bytes) off =
  let readBytes =  {-# SCC readBufAt_drop #-} BS.drop off bytes
  in {-# SCC readBufAt_recall #-} recall readBytes

extractBufRange :: Buffer -> Int -> Int -> BS.ByteString
extractBufRange (Buffer bytes) offset len = BS.take len $ BS.drop offset bytes

loadAllRows :: Memorizable a => FilePath -> ([(Int, a)] -> IO ()) -> IO ()
loadAllRows file action = do
  withBuffer file $ \buf -> do
    let size = bufLength buf
        rowCount = readBufAt buf (size - 8)

    vec <- V.new rowCount
    collectRowPointers rowCount 0 0 vec buf

    rows <- for [0..rowCount-1] $ \n -> do
      offset <- V.read vec n
      pure $ (offset, readBufAt buf offset)

    action rows


indexRows :: Memorizable a => String -> (a -> BS.ByteString) -> FilePath -> IO ()
indexRows indexName keyFunc file = {-# SCC indexRows #-} do
  withBuffer file $ \buf -> do
    let rowCount = readBufAt buf (bufLength buf - 8)
        indexFile = file ++ "." ++ indexName

    vec <- buildSortedOffsetArray keyFunc buf rowCount
    withFile indexFile WriteMode (writeTrieIndex keyFunc vec buf)

buildSortedOffsetArray :: Memorizable a
                       => (a -> BS.ByteString)
                       -> Buffer
                       -> Int
                       -> IO (V.IOVector Int)
buildSortedOffsetArray keyFunc buf rowCount = do
  vec <- V.new rowCount
  collectRowPointers rowCount 0 0 vec buf

  let keyAt = keyFunc . readBufAt buf

  S.sortBy (compare `on` keyAt)
           (S.terminate . keyAt)
           (S.size BS.empty)
           (\n -> S.index n . keyAt)
           vec

  pure vec

collectRowPointers :: Int -> Int -> Int -> V.IOVector Int -> Buffer -> IO ()
collectRowPointers total ndx offset vec buf
    | ndx < total = {-# SCC collectRowPointers #-} do
      let rowOff = offset + 8
      V.write vec ndx rowOff
      let rowSize = readBufAt buf offset :: Int
      collectRowPointers total (ndx + 1) (rowOff + rowSize) vec buf

    | otherwise = pure ()

data TrieIndex = TI {
    tiBuf :: Buffer
  , tiArcDrop :: Int
  , tiRoot :: Int
  }

withTrieIndex :: FilePath -> (TrieIndex -> IO a) -> IO a
withTrieIndex path action = do
  withBuffer path $ \buf -> do
    let len = bufLength buf
        rootOffset = len - 8 - readBufAt buf (len - 8)

    action $ TI buf 0 rootOffset

lookupSubtrie :: BS.ByteString -> TrieIndex -> Maybe TrieIndex
lookupSubtrie key (TI buf rootDrop root) =
    go key rootDrop root
  where
    go subkey arcDrop offset =
      let arcLen = readBufAt buf offset
          arc = BS.drop arcDrop $ extractBufRange buf (offset + 8) arcLen
          (commonArc, remainingKey, remainingArc) = breakCommonPrefix subkey arc

          subtrieOffset = offset + 8 + arcLen

          subtrieCount :: Int
          subtrieCount = readBufAt buf subtrieOffset

          readSubtrieN :: Int -> Int
          readSubtrieN n = readBufAt buf (subtrieOffset + 8 + 8*n)

          subtries :: [Int]
          subtries = map readSubtrieN [0..subtrieCount - 1]

          trySubtries [] = Nothing
          trySubtries (subt:rest) = go remainingKey 0 subt
                                <|> trySubtries rest

      in case (remainingKey, remainingArc) of
         ("", _) -> Just $ TI buf (BS.length commonArc) offset
         (_, "") -> trySubtries subtries
         _ -> Nothing

trieElems :: TrieIndex -> [Int]
trieElems (TI buf _ rootOffset) =
    readValues $ valueListOffsets rootOffset []
  where
    valueListOffsets offset rest =
      let arcLen = readBufAt buf offset

          subtrieOffset = offset + 8 + arcLen

          subtrieCount :: Int
          subtrieCount = readBufAt buf subtrieOffset

          readSubtrieN :: Int -> Int
          readSubtrieN n = readBufAt buf (subtrieOffset + 8 + 8*n)

          valueOffset = subtrieOffset + 8 * (1 + subtrieCount)

          goSubtries n
            | n < subtrieCount =
              valueListOffsets (readSubtrieN n) (goSubtries $ n + 1)

            | otherwise = rest

      in valueOffset : goSubtries 0

    readValues [] = []
    readValues (offset:rest) =
      let valueCount :: Int
          valueCount = readBufAt buf offset

          readValueN :: Int -> Int
          readValueN n = readBufAt buf (offset + 8 + 8*n)

          goValues n
            | n < valueCount = readValueN n : goValues (n + 1)
            | otherwise = readValues rest

      in goValues 0

trieRootElems :: TrieIndex -> [Int]
trieRootElems (TI buf arcDrop offset) =
    let arcLen = readBufAt buf offset

        subtrieOffset = offset + 8 + arcLen

        subtrieCount :: Int
        subtrieCount = readBufAt buf subtrieOffset

        valueOffset = subtrieOffset + 8 * (1 + subtrieCount)

        valueCount :: Int
        valueCount = readBufAt buf valueOffset

        readValueN :: Int -> Int
        readValueN n = readBufAt buf (valueOffset + 8 + 8*n)

        goValues n
          | n < valueCount = readValueN n : goValues (n + 1)
          | otherwise = []

        valuesAtRoot = arcLen == arcDrop

    in if valuesAtRoot
       then goValues 0
       else []


trieLookup :: BS.ByteString -> TrieIndex -> [Int]
trieLookup key = maybe [] trieRootElems . lookupSubtrie key

trieMatch :: BS.ByteString -> TrieIndex -> [Int]
trieMatch prefix = maybe [] trieElems . lookupSubtrie prefix

inspectTrieIndex :: TrieIndex -> LBS.ByteString
inspectTrieIndex ti = do
    go "" ti
  where
    go indent (TI buf _ offset) =
      let arcLen = readBufAt buf offset
          arc = extractBufRange buf (offset + 8) arcLen

          subtrieOffset = offset + 8 + arcLen

          subtrieCount :: Int
          subtrieCount = readBufAt buf subtrieOffset

          readSubtrieN :: Int -> Int
          readSubtrieN n = readBufAt buf (subtrieOffset + 8 + 8*n)

          subtries :: [TrieIndex]
          subtries = TI buf 0 <$> map readSubtrieN [0..subtrieCount - 1]

          valueOffset = subtrieOffset + 8 * (1 + subtrieCount)

          valueCount :: Int
          valueCount = readBufAt buf valueOffset

          readValueN :: Int -> Int
          readValueN n = readBufAt buf (valueOffset + 8 + 8*n)

          values :: [Int]
          values = map readValueN [0..valueCount - 1]

      in LBS.fromChunks
           [       indent, "* Arc: \"", arc, "\" | Offset: ", CBS.pack (show offset)
           , "\n", indent, "  Values: ", CBS.pack $ show $ values
           , "\n"
           ]
         `LBS.append` LBS.concat (map (go (indent `BS.append` "  ")) subtries)

writeTrieIndex :: Memorizable a
               => (a -> BS.ByteString)
               -> V.IOVector Int
               -> Buffer
               -> Handle
               -> IO ()
writeTrieIndex keyFunc vec buf h = do
    -- need to handle empty case
    --offset <- vec `V.unsafeRead` 0
    --startNew offset (keyFunc $ readBufAt buf offset) 1
    (rootOffset, totalBytes, _) <- writeTrie "" "" [] [] 0 0
    BS.hPutStr h $ memorize (totalBytes - rootOffset)
  where
    len = V.length vec

    writeTrie :: BS.ByteString -- context (the parent's key)
              -> BS.ByteString -- arc
              -> [Int]         -- subtrie offsets
              -> [Int]         -- value offsets
              -> Int           -- iteration index
              -> Int           -- current offset
              -> IO (Int, Int, Int) -- subtrie offset, total offset, ndx
    writeTrie ctx arc subtries values ndx offset
      | ndx < len = do
        datOffset <- V.read vec ndx

        let key = keyFunc $ readBufAt buf datOffset
            (ctxShared, ctxLeft, ctxRight) = breakCommonPrefix ctx key
            (arcShared, arcLeft, arcRight) = breakCommonPrefix arc ctxRight
            z  = BS.null
            nz = not . z
            sameCtx = z ctxLeft

        case () of
          -- keys are identical
          _ | sameCtx, z arcLeft, z arcRight ->
            writeTrie ctx arc subtries (datOffset:values) (ndx + 1) offset

          -- new item is a subtrie of the current arc
          _ | sameCtx, z arcLeft -> do
            let subctx = ctx `BS.append` arc
            (sub, offset, ndx) <- writeTrie subctx
                                            arcRight
                                            []
                                            [datOffset]
                                            (ndx + 1)
                                            offset

            writeTrie ctx arc (sub:subtries) values ndx offset

          -- new item is a separate (non-zero) arc in the same context
          _ | sameCtx, nz arcShared, nz arcLeft, nz arcRight -> do
            written <- flushTrie arcLeft subtries values

            let subL = offset
                subctx = ctxShared `BS.append` arcShared

            (subR, offset, ndx) <- writeTrie subctx
                                             arcRight
                                             []
                                             [datOffset]
                                             (ndx + 1)
                                             (offset + written)

            writeTrie ctx arcShared [subR,subL] [] ndx offset

          _ -> do
            bytesWritten <- flushTrie arc subtries values
            pure (offset, offset + bytesWritten, ndx)

      | otherwise = do
        bytesWritten <- flushTrie arc subtries values
        pure (offset, offset + bytesWritten, ndx)

    flushTrie :: BS.ByteString
              -> [Int]  -- subtrie offsets
              -> [Int]  -- value offsets
              -> IO Int -- BytesTritten
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

breakCommonPrefix :: BS.ByteString
                  -> BS.ByteString
                  -> (BS.ByteString, BS.ByteString, BS.ByteString)
breakCommonPrefix b1 b2 =
  let prefixLength = length $ takeWhile id $ BS.zipWith (==) b1 b2
  in ( BS.take prefixLength b1
     , BS.drop prefixLength b1
     , BS.drop prefixLength b2
     )

instance Memorizable BS.ByteString where
  memorize bs = let len = BS.length bs
                in BS.append (memorize len) bs

  recall bs = let len = recallInt bs
              in BS.take len $ BS.drop 8 bs

instance Memorizable Int where
  memorize = memorizeInt
  recall = recallInt

memorizeInt :: Int -> BS.ByteString
memorizeInt n = {-# SCC memorizeInt #-}
    BS.pack [
      fromIntegral (n `unsafeShiftR` 54)
    , fromIntegral (n `unsafeShiftR` 48)
    , fromIntegral (n `unsafeShiftR` 40)
    , fromIntegral (n `unsafeShiftR` 32)
    , fromIntegral (n `unsafeShiftR` 24)
    , fromIntegral (n `unsafeShiftR` 16)
    , fromIntegral (n `unsafeShiftR` 8)
    , fromIntegral n
    ]

recallInt :: BS.ByteString -> Int
recallInt bs = {-# SCC recallInt #-}
  (     fromIntegral (bs `UBS.unsafeIndex` 0) `unsafeShiftL` 54
    .|. fromIntegral (bs `UBS.unsafeIndex` 1) `unsafeShiftL` 48
    .|. fromIntegral (bs `UBS.unsafeIndex` 2) `unsafeShiftL` 40
    .|. fromIntegral (bs `UBS.unsafeIndex` 3) `unsafeShiftL` 32
    .|. fromIntegral (bs `UBS.unsafeIndex` 4) `unsafeShiftL` 24
    .|. fromIntegral (bs `UBS.unsafeIndex` 5) `unsafeShiftL` 16
    .|. fromIntegral (bs `UBS.unsafeIndex` 6) `unsafeShiftL` 8
    .|. fromIntegral (bs `UBS.unsafeIndex` 7)
  )

