module Database.Kioku.Internal.Query (
    KiokuQuery,
    keyExact,
    keyExactIn,
    keyPrefix,
    keyAllHitsAlong,
    runQuery,
) where

import qualified Data.ByteString.Char8 as BS

import Database.Kioku.Internal.Buffer
import Database.Kioku.Internal.TrieIndex
import Database.Kioku.Memorizable

newtype KiokuQuery = KQ {kqFunc :: TrieIndex -> [Int]}

keyExact :: BS.ByteString -> KiokuQuery
keyExact key = KQ (trieLookup key)

keyExactIn :: [BS.ByteString] -> KiokuQuery
keyExactIn key = KQ (trieLookupMany key)

keyPrefix :: BS.ByteString -> KiokuQuery
keyPrefix key = KQ (trieMatch key)

keyAllHitsAlong :: BS.ByteString -> KiokuQuery
keyAllHitsAlong path = KQ (trieAllHitsAlong path)

runQuery ::
    Memorizable a =>
    KiokuQuery ->
    Buffer ->
    Buffer ->
    [a]
runQuery kQuery indexBuffer dataBuffer =
    map (readRowAt dataBuffer) $
        kqFunc kQuery (bufferTrieIndex indexBuffer)
