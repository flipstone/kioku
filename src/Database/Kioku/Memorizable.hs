module Database.Kioku.Memorizable
  ( Memorizable(..)
  , memorizeInt
  , recallInt
  ) where

import qualified  Data.ByteString as BS
import qualified  Data.ByteString.Unsafe as UBS
import            Data.Bits
import            Data.Word

class Memorizable a where
  memorize :: a -> BS.ByteString
  recall :: BS.ByteString -> a

instance Memorizable BS.ByteString where
  memorize = id
  recall = id

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

delimit :: Word8 -> [BS.ByteString] -> BS.ByteString
delimit delim = BS.intercalate (BS.singleton delim)

undelimit :: Word8 -> BS.ByteString -> [BS.ByteString]
undelimit delim = BS.split delim

nullDelimit :: [BS.ByteString] -> BS.ByteString
nullDelimit = delimit 0

unNullDelimit :: BS.ByteString -> [BS.ByteString]
unNullDelimit = undelimit 0

