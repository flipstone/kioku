module Database.Kioku.Internal.Buffer
  ( Buffer
  , bufLength
  , newBuffer
  , bufferFromByteString
  , readBufAt
  , readRowAt
  , extractBufRange
  ) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Unsafe as UBS
import Foreign.Ptr

import Database.Kioku.Memorizable

newtype Buffer = Buffer BS.ByteString

bufLength :: Buffer -> Int
bufLength (Buffer bs) = BS.length bs

newBuffer :: Ptr () -> Int -> IO Buffer
newBuffer ptr size = Buffer <$> UBS.unsafePackCStringLen (castPtr ptr, size)

{- | Wraps an ordinary heap ByteString as a buffer, for storage backends that
hold their content in memory rather than in memory-mapped files.
-}
bufferFromByteString :: BS.ByteString -> Buffer
bufferFromByteString = Buffer

-- This function does not using a length header to determine where
-- to stop reading, so it can only be used with Memorizable instances
-- that do not depend end of the ByteString for parsing
readBufAt :: Memorizable a => Buffer -> Int -> a
readBufAt (Buffer bytes) off =
  let
    readBytes = BS.drop off bytes
  in
    recall readBytes

readRowAt :: Memorizable a => Buffer -> Int -> a
readRowAt (Buffer bytes) off =
  let
    rowHeader = BS.drop off bytes
    rowLength = recall rowHeader
    rowBytes = BS.take rowLength $ BS.drop 8 rowHeader
  in
    recall rowBytes

extractBufRange :: Buffer -> Int -> Int -> BS.ByteString
extractBufRange (Buffer bytes) offset len = BS.take len $ BS.drop offset bytes
