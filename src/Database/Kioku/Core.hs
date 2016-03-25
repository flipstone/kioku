module Database.Kioku.Core
  ( KiokuDB
  , DataSetName, IndexName
  , openKiokuDB, defaultKiokuPath
  , closeKiokuDB, withKiokuDB

  , createDataSet
  , createIndex
  , query, keyExact, keyPrefix
  ) where

import            Control.Concurrent.MVar
import            Control.Exception
import            Crypto.Hash.SHA256
import qualified  Data.ByteString.Char8 as BS
import qualified  Data.ByteString.Base16 as Base16
import qualified  Data.ByteString.Lazy as LBS
import qualified  Data.Map.Strict as M
import            Data.IORef
import            Data.Foldable
import            Foreign.Ptr
import            System.Directory
import            System.FilePath
import            System.IO
import            System.IO.MMap

import            Database.Kioku.Internal.Buffer
import            Database.Kioku.Internal.TrieIndex
import            Database.Kioku.Memorizable

data KiokuDB = KiokuDB {
    dataDir  :: FilePath
  , tmpDir  :: FilePath
  , objDir :: FilePath
  , openBuffers :: MVar (M.Map BS.ByteString (Ptr (), Int, Buffer))
  }

type DataSetName = String
type IndexName = String

defaultKiokuPath :: FilePath
defaultKiokuPath = ".kioku"

openKiokuDB :: FilePath -> IO KiokuDB
openKiokuDB path = do
  bufs <- newMVar M.empty

  let db = KiokuDB {
             dataDir = path </> "data"
           , tmpDir = path </> "tmp"
           , objDir = path </> "objects"
           , openBuffers = bufs
           }

  traverse_ (createDirectoryIfMissing True)
            [ dataDir db
            , tmpDir db
            , objDir db
            ]

  pure db

closeKiokuDB :: KiokuDB -> IO ()
closeKiokuDB db = do
  modifyMVar_ (openBuffers db) $ \bufMap -> do
    for_ (M.elems bufMap) $ \(ptr, rawSize, _) ->
      munmapFilePtr ptr rawSize

    pure M.empty

withKiokuDB :: FilePath -> (KiokuDB -> IO a) -> IO a
withKiokuDB path action = do
  db <- openKiokuDB path
  action db `finally` closeKiokuDB db

dataSetObjFile :: KiokuDB -> DataSetName -> FilePath
dataSetObjFile db name = objDir db </> "data_set" </> name

indexObjFile :: KiokuDB -> IndexName -> FilePath
indexObjFile db name = objDir db </> "index" </> name

dataFilePath :: KiokuDB -> BS.ByteString -> FilePath
dataFilePath db sha = dataDir db </> BS.unpack sha

hashFile :: FilePath -> IO BS.ByteString
hashFile = fmap (Base16.encode . hashlazy) . LBS.readFile


openBuffer :: BS.ByteString -> KiokuDB -> IO Buffer
openBuffer bufName db =
  modifyMVar (openBuffers db) $ \bufMap ->
    case M.lookup bufName bufMap of
      Just (_, _, buf) ->
        pure (bufMap, buf)

      Nothing -> do
        (rawPtr, rawSize, offset, size) <- mmapFilePtr (dataFilePath db bufName)
                                                       ReadOnly
                                                       Nothing

        buf <- newBuffer (rawPtr `plusPtr` offset) size

        let newMap = M.insert bufName (rawPtr, rawSize, buf) bufMap

        pure (newMap, buf)


writeDBFile :: FilePath -> BS.ByteString -> IO ()
writeDBFile path bytes = do
  createDirectoryIfMissing True (takeDirectory path)
  BS.writeFile path bytes

createDataSet :: Memorizable a => DataSetName -> [a] -> KiokuDB -> IO Int
createDataSet name as db = do
  (tmpFile, h) <- openTempFile (tmpDir db) name
  count <- hWriteRows as h
  hClose h

  dataHash <- hashFile tmpFile
  renameFile tmpFile (dataFilePath db dataHash)
  writeDBFile (dataSetObjFile db name) dataHash
  pure count

hWriteRows :: Memorizable a => [a] -> Handle -> IO Int
hWriteRows as h = do
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

createIndex :: Memorizable a
            => DataSetName
            -> IndexName
            -> (a -> BS.ByteString)
            -> KiokuDB
            -> IO ()
createIndex dataSetName indexName keyFunc db = do
  dataHash <- BS.readFile $ dataSetObjFile db dataSetName
  dataBuf <- openBuffer dataHash db
  tmpFile <- writeIndex keyFunc dataBuf $ \flushIndex -> do
    (tmpFile, h) <- openTempFile (tmpDir db) indexName
    flushIndex h
    hClose h
    pure tmpFile

  indexHash <- hashFile tmpFile
  renameFile tmpFile (dataFilePath db indexHash)
  writeDBFile (indexObjFile db indexName) (BS.unlines [indexHash, dataHash])

query :: Memorizable a
      => IndexName
      -> KiokuQuery
      -> KiokuDB
      -> IO [a]
query name kQuery db = do
  indexBytes <- BS.readFile (indexObjFile db name)

  case BS.lines indexBytes of
    [indexHash, dataHash] -> do
      index <- bufferTrieIndex <$> openBuffer indexHash db

      let offsets = kqFunc kQuery index

      dataBuf <- openBuffer dataHash db
      pure $ map (readRowAt dataBuf) offsets

    _ -> error $ "Index " ++ show name ++ " is corrupt!"

newtype KiokuQuery = KQ { kqFunc :: TrieIndex -> [Int] }

keyExact :: BS.ByteString -> KiokuQuery
keyExact key = KQ (trieLookup key)

keyPrefix :: BS.ByteString -> KiokuQuery
keyPrefix key = KQ (trieMatch key)

