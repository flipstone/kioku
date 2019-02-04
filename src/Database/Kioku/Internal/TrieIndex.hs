module Database.Kioku.Internal.TrieIndex
  ( bufferTrieIndex
  , TrieIndex
  , writeIndex
  , trieLookup
  , trieMatch
  , trieLookupMany
  , trieFirstStopAlong
  ) where

import            Control.Applicative
import qualified  Data.ByteString as BS
import qualified  Data.ByteString.Char8 as CBS
import            Data.Foldable
import            Data.Function
import            Data.List
import qualified  Data.Proxy as P
import qualified  Data.Vector.Unboxed.Mutable as V
import qualified  Data.Vector.Algorithms.AmericanFlag as S
import            System.IO

import            Database.Kioku.Internal.Buffer
import            Database.Kioku.Memorizable

trieLookup :: BS.ByteString -> TrieIndex -> [Int]
trieLookup key = maybe [] trieRootElems . lookupSubtrie key

trieMatch :: BS.ByteString -> TrieIndex -> [Int]
trieMatch prefix = maybe [] trieElems . lookupSubtrie prefix

trieFirstStopAlong :: BS.ByteString -> TrieIndex -> [Int]
trieFirstStopAlong "" _ = []
trieFirstStopAlong path trie =
    case lookupSubtrie p trie of
      Nothing -> []
      Just subtrie ->
        case trieRootElems subtrie of
        [] -> trieFirstStopAlong ath subtrie
        elems -> elems
  where
    (p,ath) = BS.splitAt 1 path

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

data MultiKey = MultiKey !BS.ByteString ![MultiKey] deriving Eq

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
    (_, "", "") -> MultiKey parent kids
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
    go :: MultiKey -> Int -> Int -> [Int] -> [Int]
    go multiKey arcDrop offset rest =
      let DecodedNode
            { d_arc = fullArc
            , d_subtrieCount = subtrieCount
            , d_readSubtrieN = readSubtrieN
            , d_valueCount = valueCount
            , d_readValueN = readValueN
            } = decodeNode buf offset

          arc = BS.drop arcDrop fullArc
          (_, remainingMultiKey, remainingArc) = breakCommonPrefixMultiKey multiKey arc

          -- | Gets a list containing the results from recursing into all the subtries of this index with some multikey
          trySubtries :: MultiKey -> Int -> [Int] -> [Int]
          trySubtries key n cont
            | n < subtrieCount =
              go key
                 0
                 (readSubtrieN n)
                 (trySubtries key (n + 1) cont)

            | otherwise =
              cont

          -- | Gets a list containing the results for all the multikeys in the argument for the remainingArc
          goKids :: [MultiKey] -> [Int]
          goKids [] = rest
          goKids (kid:restKids) =
            case breakCommonPrefixMultiKey kid remainingArc of
              (_, MultiKey "" _, "") ->
                readValues 0 (goKids restKids) -- get all the values for this index, prepended to the remaining kid's results
              (_, k, "") -> -- if there is a remainder for the prefix and the arc was completely consumed, we descend into the subtries
                trySubtries k
                            0
                            (goKids restKids)

              _ -> goKids restKids

          -- | Reads out all the values from the index and prepend them to a list of other values
          readValues :: Int -> [Int] -> [Int]
          readValues n cont
            | n < valueCount = readValueN n : readValues (n + 1) cont
            | otherwise = cont

      in case (remainingMultiKey, remainingArc) of
           -- if both the subkey and the arc were consumed, we will branch on the children of the MultiKey
           (MultiKey "" ks, "") -> readValues 0 (goKids ks)
           -- if the arc was completely consumed by this subkey, we should descend into the subtries of this node
           (k, "") -> trySubtries k 0 rest
           -- if we have remainders from both the key and the arc then theres no possibility of finding a match in this branch.
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

-- The Lexicographic class exposes an extent field
-- instead of terminate. However, the sortBy method
-- still requires a parameter of (e -> Int -> Bool)
-- so we replicate the terminate function here.
terminate :: S.Lexicographic e => e -> Int -> Bool
terminate e i = i >= S.extent e

buildSortedOffsetArray :: Memorizable a
                       => (a -> BS.ByteString)
                       -> Buffer
                       -> Int
                       -> IO (V.IOVector Int)
buildSortedOffsetArray keyFunc buf rowCount = do
  vec <- V.new rowCount
  collectRowPointers rowCount 0 0 vec buf

  let keyAt = keyFunc . readRowAt buf
      proxy = P.Proxy :: P.Proxy BS.ByteString

  S.sortBy (compare `on` keyAt)
           (terminate . keyAt)
           (S.size proxy)
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
          _ | sameCtx, z arcLeft, z arcRight ->
            writeTrie ctx arc subtries (datOffset:values) (ndx + 1) offset

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
            let subctx = ctx `BS.append` arc
            (sub, newOffset, newNdx) <- writeTrie subctx
                                                  arcRight
                                                  []
                                                  [datOffset]
                                                  (ndx + 1)
                                                  offset

            writeTrie ctx arc (sub:subtries) values newNdx newOffset

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

          -- the item examined was not part of our parent trie's context,
          -- so we can flush the current arc and return without moving
          -- on to the next index in the array. The current item will be
          -- reconsidered further up the recursion chain.
          _ -> do
            bytesWritten <- flushTrie arc subtries values
            pure (offset, offset + bytesWritten, ndx)

      | otherwise = do
        bytesWritten <- flushTrie arc subtries values
        pure (offset, offset + bytesWritten, ndx)

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

breakCommonPrefix :: BS.ByteString
                  -> BS.ByteString
                  -> (BS.ByteString, BS.ByteString, BS.ByteString)
breakCommonPrefix b1 b2 =
  let prefixLength = length $ takeWhile id $ BS.zipWith (==) b1 b2
  in ( BS.take prefixLength b1
     , BS.drop prefixLength b1
     , BS.drop prefixLength b2
     )

breakCommonPrefixMultiKey :: MultiKey
                          -> BS.ByteString
                          -> (BS.ByteString, MultiKey, BS.ByteString)
breakCommonPrefixMultiKey (MultiKey prefix kids) arc =
  case breakCommonPrefix prefix arc of
    (common, "", "") -> (common, MultiKey "" kids, "")
    (common, remPrefix, "") -> (common, MultiKey remPrefix kids, "")
    (common, "", remArc) ->
      case dropWhile commonIsEmpty $ map (\k -> breakCommonPrefixMultiKey k remArc) kids of
        ((common', key, remArc'):_) -> (common <> common', key, remArc')
        _     -> (common, MultiKey "" [], remArc)
    (common, remPrefix, remArc) -> (common, MultiKey remPrefix kids, remArc)
  where
    commonIsEmpty ("", _, _) = True
    commonIsEmpty _          = False
