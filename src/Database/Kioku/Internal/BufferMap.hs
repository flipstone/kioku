module Database.Kioku.Internal.BufferMap
  ( BufferMap
  , newBufferMap
  , openBuffer
  , closeBuffers
  ) where

import            Control.Concurrent.MVar
import            Data.Foldable
import qualified  Data.Map.Strict as M
import            Foreign.Ptr
import            System.IO.MMap

import            Database.Kioku.Internal.Buffer

newtype BufferMap = BufferMap {
    _buffers :: MVar (M.Map String (Ptr (), Int, Buffer))
  }

newBufferMap :: IO BufferMap
newBufferMap = BufferMap <$> newMVar M.empty

openBuffer :: FilePath -> BufferMap -> IO Buffer
openBuffer bufPath (BufferMap buffers) =
  modifyMVar buffers $ \bufMap ->
    case M.lookup bufPath bufMap of
      Just (_, _, buf) ->
        pure (bufMap, buf)

      Nothing -> do
        (rawPtr, rawSize, offset, size) <- mmapFilePtr bufPath
                                                       ReadOnly
                                                       Nothing

        buf <- newBuffer (rawPtr `plusPtr` offset) size

        let newMap = M.insert bufPath (rawPtr, rawSize, buf) bufMap

        pure (newMap, buf)

closeBuffers :: BufferMap -> IO ()
closeBuffers (BufferMap buffers) = do
  modifyMVar_ buffers $ \bufMap -> do
    for_ (M.elems bufMap) $ \(ptr, rawSize, _) ->
      munmapFilePtr ptr rawSize

    pure M.empty
