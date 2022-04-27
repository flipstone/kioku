{-# LANGUAGE BangPatterns #-}
module Database.Kioku.Memorizable
  ( Memorizable(..)

  , memorizeWord8, recallWord8
  , memorizeWord16, recallWord16
  , memorizeWord32, recallWord32
  , memorizeWord64, recallWord64
  , memorizeWord, recallWord

  , memorizeInt8, recallInt8
  , memorizeInt16, recallInt16
  , memorizeInt32, recallInt32
  , memorizeInt64, recallInt64
  , memorizeInt, recallInt

  , memorizeInteger, recallInteger

  , memorizeDouble, recallDouble
  , memorizeFloat, recallFloat

  , memorizeText, recallText
  , memorizeEnum, recallEnum

  , roll
  , unroll

  , delimit, undelimit
  , nullDelimit, unNullDelimit

  , lengthPrefix, unLengthPrefix
  , lengthPrefix255, unLengthPrefix255
  , lengthPrefix65535, unLengthPrefix65535

  , field, (&.), PrefixedFieldDecoder
  , MemorizeLength, RecallLength, LengthSize
  ) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as UBS
import           Data.Bits
import           Data.Int
import qualified Data.List.NonEmpty as NE
import           Data.ReinterpretCast
import           Data.Text (Text)
import qualified Data.Text.Encoding as E
import           Data.Word

class Memorizable a where
  memorize :: a -> BS.ByteString
  recall :: BS.ByteString -> a

{-# INLINE memorizeWord8 #-}
memorizeWord8 :: Word8 -> BS.ByteString
memorizeWord8 = BS.singleton

{-# INLINE recallWord8 #-}
recallWord8 :: BS.ByteString -> Word8
recallWord8 = BS.head

{-# INLINE memorizeText #-}
memorizeText :: Text -> BS.ByteString
memorizeText = E.encodeUtf8

{-# INLINE recallText #-}
recallText :: BS.ByteString -> Text
recallText = E.decodeUtf8

memorizeWord16 :: Word16 -> BS.ByteString
memorizeWord16 n = {-# SCC memorizeWord16 #-}
  BS.pack [
    fromIntegral (n `unsafeShiftR` 8)
  , fromIntegral n
  ]

recallWord16 :: BS.ByteString -> Word16
recallWord16 bs = {-# SCC recallWord16 #-}
  (     fromIntegral (bs `UBS.unsafeIndex` 0) `unsafeShiftL` 8
    .|. fromIntegral (bs `UBS.unsafeIndex` 1)
  )

memorizeWord32 :: Word32 -> BS.ByteString
memorizeWord32 n = {-# SCC memorizeWord32 #-}
  BS.pack [
    fromIntegral (n `unsafeShiftR` 24)
  , fromIntegral (n `unsafeShiftR` 16)
  , fromIntegral (n `unsafeShiftR` 8)
  , fromIntegral n
  ]

recallWord32 :: BS.ByteString -> Word32
recallWord32 bs = {-# SCC recallWord32 #-}
  (     fromIntegral (bs `UBS.unsafeIndex` 0) `unsafeShiftL` 24
    .|. fromIntegral (bs `UBS.unsafeIndex` 1) `unsafeShiftL` 16
    .|. fromIntegral (bs `UBS.unsafeIndex` 2) `unsafeShiftL` 8
    .|. fromIntegral (bs `UBS.unsafeIndex` 3)
  )

memorizeWord64 :: Word64 -> BS.ByteString
memorizeWord64 n = {-# SCC memorizeWord64 #-}
  BS.pack [
    fromIntegral (n `unsafeShiftR` 56)
  , fromIntegral (n `unsafeShiftR` 48)
  , fromIntegral (n `unsafeShiftR` 40)
  , fromIntegral (n `unsafeShiftR` 32)
  , fromIntegral (n `unsafeShiftR` 24)
  , fromIntegral (n `unsafeShiftR` 16)
  , fromIntegral (n `unsafeShiftR` 8)
  , fromIntegral n
  ]

recallWord64 :: BS.ByteString -> Word64
recallWord64 bs = {-# SCC recallWord64 #-}
  (     fromIntegral (bs `UBS.unsafeIndex` 0) `unsafeShiftL` 56
    .|. fromIntegral (bs `UBS.unsafeIndex` 1) `unsafeShiftL` 48
    .|. fromIntegral (bs `UBS.unsafeIndex` 2) `unsafeShiftL` 40
    .|. fromIntegral (bs `UBS.unsafeIndex` 3) `unsafeShiftL` 32
    .|. fromIntegral (bs `UBS.unsafeIndex` 4) `unsafeShiftL` 24
    .|. fromIntegral (bs `UBS.unsafeIndex` 5) `unsafeShiftL` 16
    .|. fromIntegral (bs `UBS.unsafeIndex` 6) `unsafeShiftL` 8
    .|. fromIntegral (bs `UBS.unsafeIndex` 7)
  )

{-# INLINE memorizeWord #-}
memorizeWord :: Word -> BS.ByteString
memorizeWord = memorizeWord64 . fromIntegral

{-# INLINE recallWord #-}
recallWord :: BS.ByteString -> Word
recallWord = fromIntegral . recallWord64

{-# INLINE memorizeInt8 #-}
memorizeInt8 :: Int8 -> BS.ByteString
memorizeInt8 = memorizeWord8 . fromIntegral

{-# INLINE recallInt8 #-}
recallInt8 :: BS.ByteString -> Int8
recallInt8 = fromIntegral . recallWord8

{-# INLINE memorizeInt16 #-}
memorizeInt16 :: Int16 -> BS.ByteString
memorizeInt16 = memorizeWord16 . fromIntegral

{-# INLINE recallInt16 #-}
recallInt16 :: BS.ByteString -> Int16
recallInt16 = fromIntegral . recallWord16

{-# INLINE memorizeInt32 #-}
memorizeInt32 :: Int32 -> BS.ByteString
memorizeInt32 = memorizeWord32 . fromIntegral

{-# INLINE recallInt32 #-}
recallInt32 :: BS.ByteString -> Int32
recallInt32 = fromIntegral . recallWord32

{-# INLINE memorizeInt64 #-}
memorizeInt64 :: Int64 -> BS.ByteString
memorizeInt64 = memorizeWord64 . fromIntegral

{-# INLINE recallInt64 #-}
recallInt64 :: BS.ByteString -> Int64
recallInt64 = fromIntegral . recallWord64

{-# INLINE memorizeInt #-}
memorizeInt :: Int -> BS.ByteString
memorizeInt = memorizeWord64 . fromIntegral

{-# INLINE recallInt #-}
recallInt :: BS.ByteString -> Int
recallInt = fromIntegral . recallWord64

{-# INLINE memorizeDouble #-}
memorizeDouble :: Double -> BS.ByteString
memorizeDouble = memorizeWord64 . doubleToWord

{-# INLINE recallDouble #-}
recallDouble :: BS.ByteString -> Double
recallDouble = wordToDouble . recallWord64

{-# INLINE memorizeFloat #-}
memorizeFloat :: Float -> BS.ByteString
memorizeFloat = memorizeWord32 . floatToWord

{-# INLINE recallFloat #-}
recallFloat :: BS.ByteString -> Float
recallFloat = wordToFloat . recallWord32

{-# INLINE memorizeEnum #-}
memorizeEnum :: Enum a => a -> BS.ByteString
memorizeEnum = memorizeInteger . fromIntegral . fromEnum

{-# INLINE recallEnum #-}
recallEnum :: Enum a => BS.ByteString -> a
recallEnum = toEnum . fromIntegral . recallInteger

memorizeInteger :: Integer -> BS.ByteString
memorizeInteger n = {-# SCC memorizeInteger #-}
    if len < fromIntegral maxWord8
    then BS.pack ((fromIntegral len):sign:bytes)
    else BS.concat [ BS.singleton maxWord8
                   , memorizeInt len
                   , BS.pack (sign:bytes)
                   ]
  where
    sign = fromIntegral (signum n)
    bytes = unroll (abs n)
    len = length bytes

    {-# INLINE maxWord8 #-}
    maxWord8 :: Word8
    maxWord8 = maxBound

recallInteger :: BS.ByteString -> Integer
recallInteger bs = {-# SCC recallInteger #-}
    case BS.head bs of
      255 ->
        let len = recallInt $ BS.drop 1 bs
            sign = bs `UBS.unsafeIndex` 9
            unsigned = roll $ BS.take (fromIntegral len) $ BS.drop 10 bs

        in applySign sign unsigned
      len ->
        let sign = bs `UBS.unsafeIndex` 1
            unsigned = roll $ BS.take (fromIntegral len) $ BS.drop 2 bs

        in applySign sign unsigned
  where
    {-# INLINE applySign #-}
    applySign   0 n = n
    applySign   1 n = n
    applySign 255 n = -n
    applySign s _ = error $ "Invalid sign in recallInteger: " ++ show s

unroll :: Integer -> [Word8]
unroll 0 = []
unroll n = fromIntegral n : unroll (n `unsafeShiftR` 8)

roll :: BS.ByteString -> Integer
roll =
    BS.foldr' shiftUp 0
  where
    shiftUp w r = (r `unsafeShiftL` 8) .|. fromIntegral w

{-# INLINE delimit #-}
delimit :: Word8 -> [BS.ByteString] -> BS.ByteString
delimit delim = BS.intercalate (BS.singleton delim)

{-# INLINE undelimit #-}
undelimit :: Word8 -> BS.ByteString -> [BS.ByteString]
undelimit delim = BS.split delim

{-# INLINE nullDelimit #-}
nullDelimit :: [BS.ByteString] -> BS.ByteString
nullDelimit = delimit 0

{-# INLINE unNullDelimit #-}
unNullDelimit :: BS.ByteString -> [BS.ByteString]
unNullDelimit = undelimit 0

type MemorizeLength = Int -> BS.ByteString
type RecallLength = BS.ByteString -> Int
type LengthSize = Int
type PrefixedFieldDecoder a t b r v =
     RecallLength
  -> LengthSize
  -> (a -> BS.ByteString -> b)
  -> (v -> t)
  -> BS.ByteString
  -> r

lengthPrefix :: MemorizeLength
             -> [BS.ByteString]
             -> BS.ByteString
lengthPrefix memorizeLength =
    BS.concat . addPrefixes
  where
    addPrefixes [] = []
    addPrefixes (f:rest) = memorizeLength (BS.length f)
                         : f
                         : addPrefixes rest

{-# INLINE field #-}
field :: Memorizable v => PrefixedFieldDecoder a a b b v
field recallLength lengthWidth cont f bs =
  let !len = recallLength bs
      !start = BS.drop lengthWidth bs
      !value = BS.take len start
      !rest = BS.drop len start

  in cont (f $ recall value) rest

{-# INLINE (&.) #-}
(&.) :: PrefixedFieldDecoder (w -> b) t c r v
     -> PrefixedFieldDecoder a b c c w
     -> PrefixedFieldDecoder a t c r v
bc &. ab = \recallL size -> bc recallL size . ab recallL size

{-# INLINE unLengthPrefix #-}
unLengthPrefix :: RecallLength
               -> LengthSize
               -> (v -> t)
               -> PrefixedFieldDecoder a t a r v
               -> BS.ByteString
               -> r
unLengthPrefix recallLength lengthWidth f decoder
  = decoder recallLength lengthWidth const f

lengthPrefix255 :: [BS.ByteString] -> BS.ByteString
lengthPrefix255 = lengthPrefix (memorizeWord8 . fromIntegral)

unLengthPrefix255 :: (v -> t)
                  -> PrefixedFieldDecoder a t a r v
                  -> BS.ByteString
                  -> r
unLengthPrefix255 = unLengthPrefix (fromIntegral . recallWord8)
                                   1

lengthPrefix65535 :: [BS.ByteString] -> BS.ByteString
lengthPrefix65535 = lengthPrefix (memorizeWord16 . fromIntegral)


unLengthPrefix65535 :: (v -> t)
                    -> PrefixedFieldDecoder a t a r v
                    -> BS.ByteString
                    -> r
unLengthPrefix65535 = unLengthPrefix (fromIntegral . recallWord16)
                                     2


--
-- Instances
--

instance Memorizable BS.ByteString where
  memorize = id
  recall = id

instance Memorizable Text where
  memorize = memorizeText
  recall = recallText

instance Memorizable Word8 where
  memorize = memorizeWord8
  recall = recallWord8

instance Memorizable Word16 where
  memorize = memorizeWord16
  recall = recallWord16

instance Memorizable Word32 where
  memorize = memorizeWord32
  recall = recallWord32

instance Memorizable Word64 where
  memorize = memorizeWord64
  recall = recallWord64

instance Memorizable Word where
  memorize = memorizeWord
  recall = recallWord

instance Memorizable Int8 where
  memorize = memorizeInt8
  recall = recallInt8

instance Memorizable Int16 where
  memorize = memorizeInt16
  recall = recallInt16

instance Memorizable Int32 where
  memorize = memorizeInt32
  recall = recallInt32

instance Memorizable Int64 where
  memorize = memorizeInt64
  recall = recallInt64

instance Memorizable Int where
  memorize = memorizeInt
  recall = recallInt

instance Memorizable Integer where
  memorize = memorizeInteger
  recall = recallInteger

instance Memorizable Double where
  memorize = memorizeDouble
  recall = recallDouble

instance Memorizable Float where
  memorize = memorizeFloat
  recall = recallFloat

instance Memorizable a => Memorizable (Maybe a) where
  memorize Nothing = BS.singleton 0
  memorize (Just a) = BS.cons 1 (memorize a)

  recall bs =
    case BS.head bs of
    0 -> Nothing
    1 -> Just $ recall $ BS.drop 1 bs
    n -> error $ "recall: Invalid Maybe constructor index: " ++ show n

instance (Memorizable a, Memorizable b) => Memorizable (Either a b) where
  memorize (Left a) = BS.cons 0 (memorize a)
  memorize (Right b) = BS.cons 1 (memorize b)

  recall bs =
    case BS.head bs of
    0 -> Left $ recall $ BS.drop 1 bs
    1 -> Right $ recall $ BS.drop 1 bs
    n -> error $ "recall: Invalid Either constructor index: " ++ show n

instance Memorizable a => Memorizable [a] where
  memorize = lengthPrefix255 . map memorize
  recall = recallItems []

instance Memorizable a => Memorizable (NE.NonEmpty a) where
  memorize = lengthPrefix255 . map memorize . NE.toList
  recall bs =
    case NE.nonEmpty $ recallItems [] bs of
      Just ne -> ne
      Nothing -> error "Tried to recall an empty list into a NonEmpty"

recallItems :: Memorizable a => [a] -> BS.ByteString -> [a]
recallItems items bs
  | bs == BS.empty = items
  | otherwise      = recallItems (items ++ [recall currSection]) rest
      where
        lengthOfCurrentElem = fromIntegral $ BS.head bs
        (currSection, rest) = BS.splitAt (lengthOfCurrentElem) $ BS.tail bs
