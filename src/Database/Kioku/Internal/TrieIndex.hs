module Database.Kioku.Internal.TrieIndex
  ( bufferTrieIndex
  , TrieIndex
  , writeIndex
  , trieLookup
  , trieMatch
  , trieLookupMany
  ) where

import            Control.Applicative
import qualified  Data.ByteString as BS
import qualified  Data.ByteString.Char8 as CBS
import            Data.Foldable
import            Data.Function
import            Data.List
import qualified  Data.Vector.Unboxed.Mutable as V
import qualified  Data.Vector.Algorithms.AmericanFlag as S
import            System.IO

import            Database.Kioku.Internal.Buffer
import            Database.Kioku.Memorizable

trieLookup :: BS.ByteString -> TrieIndex -> [Int]
trieLookup key = maybe [] trieRootElems . lookupSubtrie key

trieMatch :: BS.ByteString -> TrieIndex -> [Int]
trieMatch prefix = maybe [] trieElems . lookupSubtrie prefix

bufferTrieIndex :: Buffer -> TrieIndex
bufferTrieIndex buf =
  let len = bufLength buf
      rootOffset = len - 8 - readBufAt buf (len - 8)

  in TI buf 0 rootOffset

data TrieIndex = TI {
    _tiBuf :: Buffer
  , _tiArcDrop :: Int
  , _tiRoot :: Int
  }


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

data MultiKey = MultiKey {
    mkPrefix :: !BS.ByteString
  , _mkChildren :: ![MultiKey]
  } deriving (Eq)

instance Show MultiKey where
  show mk =
      "MultiKey\n" ++ go "  " mk
    where
      go indent (MultiKey pre kids) =
           indent ++ CBS.unpack pre ++ "\n"
        ++ concatMap (go (' ':' ':indent)) kids

mkInsert :: MultiKey -> BS.ByteString -> MultiKey
mkInsert (MultiKey "" []) key = MultiKey key []
mkInsert (MultiKey parent kids) key =
  case breakCommonPrefix parent key of
  (_, "", new) ->
    case kids of
    (first:rest) ->
      case mkInsert first new of
      (MultiKey "" newKids) -> MultiKey parent (newKids ++ rest)
      newKid -> MultiKey parent (newKid : rest)

    _ -> MultiKey parent (MultiKey new [] : kids)

  (newParent, old, "") ->
    MultiKey newParent [MultiKey old kids]

  (newParent, old, new) ->
    let oldKey = MultiKey old kids
        newKey = MultiKey new []
    in MultiKey newParent [newKey, oldKey]

mkMultiKey :: [BS.ByteString] -> MultiKey
mkMultiKey [] = error "Can't build MultiKey with no keys!"
mkMultiKey keys =
  let (first:rest) = nub (sortBy (flip compare) keys)
  in foldl' mkInsert (MultiKey first []) rest

data DecodedNode = DecodedNode {
    d_arc :: BS.ByteString
  , d_subtrieCount :: Int
  , d_readSubtrieN :: Int -> Int
  , d_valueCount :: Int
  , d_readValueN :: Int -> Int
  }

decodeNode :: Buffer -> Int -> DecodedNode
decodeNode buf offset =
    DecodedNode {
      d_arc = extractBufRange buf (offset + 8) arcLen
    , d_subtrieCount = stCount
    , d_readSubtrieN = \n -> readBufAt buf (subtrieOffset + 8 + 8*n)
    , d_valueCount = readBufAt buf valueOffset
    , d_readValueN = \n -> readBufAt buf (valueOffset + 8 + 8*n)
    }
  where
    arcLen = readBufAt buf offset
    subtrieOffset = offset + 8 + arcLen
    stCount = readBufAt buf subtrieOffset
    valueOffset = subtrieOffset + 8 * (1 + stCount)

trieLookupMany :: [BS.ByteString] -> TrieIndex -> [Int]
trieLookupMany [] _ = []
trieLookupMany keys (TI buf rootDrop root) =
    go (mkMultiKey keys) rootDrop root []
  where
    go (MultiKey subkey kids) arcDrop offset rest =
      let DecodedNode {
            d_arc = fullArc
          , d_subtrieCount = subtrieCount
          , d_readSubtrieN = readSubtrieN
          , d_valueCount = valueCount
          , d_readValueN = readValueN
          } = decodeNode buf offset

          arc = BS.drop arcDrop fullArc
          (_, remainingKey, remainingArc) = breakCommonPrefix subkey arc

          trySubtries key n cont
            | n < subtrieCount =
              go key
                 0
                 (readSubtrieN n)
                 (trySubtries key (n + 1) cont)

            | otherwise =
              cont

          goKids [] = rest
          goKids (kid:restKids) =
            case breakCommonPrefix (mkPrefix kid) remainingArc of
              (_, "", "") ->
                readValues 0 (goKids restKids)
              (_, remainingKid, "") ->
                trySubtries (kid { mkPrefix = remainingKid })
                            0
                            (goKids restKids)

              _ -> goKids restKids

          readValues n cont
            | n < valueCount = readValueN n : readValues (n + 1) cont
            | otherwise = cont

      in case (remainingKey, remainingArc) of
         ("", "") -> readValues 0 (goKids kids)
         (_, "") -> trySubtries (MultiKey remainingKey kids) 0 rest
         _ -> rest

trieElems :: TrieIndex -> [Int]
trieElems (TI buf _ rootOffset) =
    valueListOffsets rootOffset []
  where
    valueListOffsets offset rest =
      let DecodedNode {
            d_subtrieCount = subtrieCount
          , d_readSubtrieN = readSubtrieN
          , d_valueCount = valueCount
          , d_readValueN = readValueN
          } = decodeNode buf offset

          goSubtries n
            | n < subtrieCount =
              valueListOffsets (readSubtrieN n) (goSubtries $ n + 1)

            | otherwise = rest

          goValues n
            | n < valueCount = readValueN n : goValues (n + 1)
            | otherwise = goSubtries 0

      in goValues 0

trieRootElems :: TrieIndex -> [Int]
trieRootElems (TI buf arcDrop offset) =
    let DecodedNode {
          d_arc = arc
        , d_valueCount = valueCount
        , d_readValueN = readValueN
        } = decodeNode buf offset

        goValues n
          | n < valueCount = readValueN n : goValues (n + 1)
          | otherwise = []

        valuesAtRoot = BS.length arc == arcDrop

    in if valuesAtRoot
       then goValues 0
       else []


writeIndex :: Memorizable a
           => (a -> BS.ByteString)
           -> Buffer
           -> ((Handle -> IO ()) -> IO b)
           -> IO b
writeIndex keyFunc buf runWriter = do
  let rowCount = readBufAt buf (bufLength buf - 8)
  vec <- buildSortedOffsetArray keyFunc buf rowCount
  runWriter (writeTrieIndex keyFunc vec buf)

buildSortedOffsetArray :: Memorizable a
                       => (a -> BS.ByteString)
                       -> Buffer
                       -> Int
                       -> IO (V.IOVector Int)
buildSortedOffsetArray keyFunc buf rowCount = do
  vec <- V.new rowCount
  collectRowPointers rowCount 0 0 vec buf

  let keyAt = keyFunc . readRowAt buf

  S.sortBy (compare `on` keyAt)
           (S.terminate . keyAt)
           (S.size BS.empty)
           (\n -> S.index n . keyAt)
           vec

  pure vec

collectRowPointers :: Int -> Int -> Int -> V.IOVector Int -> Buffer -> IO ()
collectRowPointers total ndx offset vec buf
    | ndx < total = {-# SCC collectRowPointers #-} do
      V.write vec ndx offset
      let rowSize = readBufAt buf offset :: Int
      collectRowPointers total (ndx + 1) (offset + rowSize + 8) vec buf

    | otherwise = pure ()

writeTrieIndex :: Memorizable a
               => (a -> BS.ByteString)
               -> V.IOVector Int
               -> Buffer
               -> Handle
               -> IO ()
writeTrieIndex keyFunc vec buf h = do
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

        let key = keyFunc $ readRowAt buf datOffset
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
            (sub, newOffset, newNdx) <- writeTrie subctx
                                                  arcRight
                                                  []
                                                  [datOffset]
                                                  (ndx + 1)
                                                  offset

            writeTrie ctx arc (sub:subtries) values newNdx newOffset

          -- new item is a separate (non-zero) arc in the same context
          _ | sameCtx, nz arcShared, nz arcLeft, nz arcRight -> do
            written <- flushTrie arcLeft subtries values

            let subL = offset
                subctx = ctxShared `BS.append` arcShared

            (subR, newOffset, newNdx) <- writeTrie subctx
                                                   arcRight
                                                   []
                                                   [datOffset]
                                                   (ndx + 1)
                                                   (offset + written)

            writeTrie ctx arcShared [subR,subL] [] newNdx newOffset

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
