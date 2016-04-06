module Database.Kioku.Core
  ( KiokuDB, KiokuQuery
  , DataSetName, IndexName
  , openKiokuDB, defaultKiokuPath
  , closeKiokuDB, withKiokuDB

  , createDataSet
  , createIndex
  , query, keyExact, keyExactIn, keyPrefix

  , gcKiokuDB
  , packKiokuDB, exportKiokuDB
  , unpackKiokuDB, importKiokuDB
  ) where

import            Control.Concurrent.MVar
import            Control.Exception
import qualified  Codec.Archive.Tar as Tar
import qualified  Codec.Compression.GZip as GZ
import            Crypto.Hash.SHA256
import qualified  Data.ByteString.Char8 as BS
import qualified  Data.ByteString.Base16 as Base16
import qualified  Data.ByteString.Lazy as LBS
import            Data.IORef
import            Data.Foldable
import            Data.List ((\\))
import qualified  Data.Map.Strict as M
import            Data.Traversable
import            Foreign.Ptr
import            System.Directory
import            System.FilePath
import            System.IO
import            System.IO.MMap

import            Database.Kioku.Internal.Buffer
import            Database.Kioku.Internal.TrieIndex
import            Database.Kioku.Memorizable

data KiokuDB = KiokuDB {
    rootDir :: FilePath
  , openBuffers :: MVar (M.Map BS.ByteString (Ptr (), Int, Buffer))
  }

dataPath,tmpPath,objPath,dataSetObjPath,indexObjPath :: FilePath
dataPath = "data"
tmpPath = "tmp"
objPath = "objects"
dataSetObjPath = objPath </> "data_set"
indexObjPath = objPath </> "index"

dataDir :: KiokuDB -> FilePath
dataDir db = rootDir db </> dataPath

tmpDir :: KiokuDB -> FilePath
tmpDir db = rootDir db </> tmpPath

dataSetObjDir :: KiokuDB -> FilePath
dataSetObjDir db = rootDir db </> dataSetObjPath

indexObjDir :: KiokuDB -> FilePath
indexObjDir db = rootDir db </> indexObjPath

dataSetObjFile :: KiokuDB -> DataSetName -> FilePath
dataSetObjFile db name = dataSetObjDir db </> name

indexObjFile :: KiokuDB -> IndexName -> FilePath
indexObjFile db name = indexObjDir db </> name

dataFilePath :: KiokuDB -> BS.ByteString -> FilePath
dataFilePath db sha = dataDir db </> BS.unpack sha

type DataSetName = String
type IndexName = String

defaultKiokuPath :: FilePath
defaultKiokuPath = ".kioku"

openKiokuDB :: FilePath -> IO KiokuDB
openKiokuDB path = do
  bufs <- newMVar M.empty

  let db = KiokuDB { rootDir = path, openBuffers = bufs }

  traverse_ (createDirectoryIfMissing True)
            [ dataDir db
            , tmpDir db
            , dataSetObjDir db
            , indexObjDir db
            ]

  pure db

closeKiokuDB :: KiokuDB -> IO ()
closeKiokuDB db = do
  modifyMVar_ (openBuffers db) $ \bufMap -> do
    for_ (M.elems bufMap) $ \(ptr, rawSize, _) ->
      munmapFilePtr ptr rawSize

    pure M.empty

gcKiokuDB :: KiokuDB -> IO ()
gcKiokuDB db = do
    closeKiokuDB db
    hashRefs <- readHashRefs
    dataFiles <- listDirectoryContents $ dataDir db

    let hashes = BS.pack <$> dataFiles
        unreferenced = hashes \\ hashRefs
        unusedFiles = dataFilePath db <$> unreferenced

    traverse_ removeFile unusedFiles
  where
    readHashRefs = do
      dataSets <- readDataSets
      indexes <- readIndexes
      pure (dataSets ++ concat indexes)

    readDataSets = do
      paths <- listDirectoryContents $ dataSetObjDir db

      for paths $ \name -> do
        dataSetFile <- readDataSetFile name db
        pure (dataSetHash dataSetFile)

    readIndexes = do
      paths <- listDirectoryContents $ indexObjDir db

      for paths $ \name -> do
        indexFile <- readIndexFile name db
        pure [ indexHash indexFile, dataHash indexFile ]


    listDirectoryContents dir =
      filter (not . (`elem` [".",".."])) <$> getDirectoryContents dir

packKiokuDB :: KiokuDB -> IO LBS.ByteString
packKiokuDB db = do
  entries <- Tar.pack (rootDir db) [dataPath, dataSetObjPath, indexObjPath]
  pure $ GZ.compress $ Tar.write entries

exportKiokuDB :: FilePath -> KiokuDB -> IO ()
exportKiokuDB path db = packKiokuDB db >>= LBS.writeFile path

unpackKiokuDB :: LBS.ByteString -> KiokuDB -> IO ()
unpackKiokuDB gzBytes db = do
  let tarBytes = GZ.decompress gzBytes
      entries = Tar.read tarBytes

  Tar.unpack (rootDir db) entries

importKiokuDB :: FilePath -> KiokuDB -> IO ()
importKiokuDB path db = LBS.readFile path >>= flip unpackKiokuDB db

withKiokuDB :: FilePath -> (KiokuDB -> IO a) -> IO a
withKiokuDB path action = do
  db <- openKiokuDB path
  action db `finally` closeKiokuDB db

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

data DataSetFile = DataSetFile {
    dataSetHash :: BS.ByteString
  }

readDataSetFile :: DataSetName -> KiokuDB -> IO DataSetFile
readDataSetFile name db =
  DataSetFile <$> (BS.readFile $ dataSetObjFile db name)

writeDataSetFile :: DataSetName -> DataSetFile -> KiokuDB -> IO ()
writeDataSetFile name file db = do
  writeDBFile (dataSetObjFile db name) (dataSetHash file)

data IndexFile = IndexFile {
    indexHash :: BS.ByteString
  , dataHash :: BS.ByteString
  }

readIndexFile :: IndexName -> KiokuDB -> IO IndexFile
readIndexFile name db = do
  indexBytes <- BS.readFile (indexObjFile db name)

  case BS.lines indexBytes of
    [idx, dat] -> pure $ IndexFile idx dat
    _ -> error $ "Index " ++ show name ++ " is corrupt!"

writeIndexFile :: IndexName -> IndexFile -> KiokuDB -> IO ()
writeIndexFile name file db =
  writeDBFile (indexObjFile db name)
              (BS.unlines [indexHash file, dataHash file])

createDataSet :: Memorizable a => DataSetName -> [a] -> KiokuDB -> IO Int
createDataSet name as db = do
  (tmpFile, h) <- openTempFile (tmpDir db) name
  count <- hWriteRows as h
  hClose h

  sha <- hashFile tmpFile
  renameFile tmpFile (dataFilePath db sha)
  writeDataSetFile name (DataSetFile { dataSetHash = sha }) db
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
  dataSetFile <- readDataSetFile dataSetName db
  dataBuf <- openBuffer (dataSetHash dataSetFile) db
  tmpFile <- writeIndex keyFunc dataBuf $ \flushIndex -> do
    (tmpFile, h) <- openTempFile (tmpDir db) indexName
    flushIndex h
    hClose h
    pure tmpFile

  sha <- hashFile tmpFile
  renameFile tmpFile (dataFilePath db sha)

  let indexFile = IndexFile { indexHash = sha
                            , dataHash = dataSetHash dataSetFile
                            }

  writeIndexFile indexName indexFile db

query :: Memorizable a
      => IndexName
      -> KiokuQuery
      -> KiokuDB
      -> IO [a]
query name kQuery db = do
  indexFile <- readIndexFile name db
  index <- bufferTrieIndex <$> openBuffer (indexHash indexFile) db

  let offsets = kqFunc kQuery index

  dataBuf <- openBuffer (dataHash indexFile) db
  pure $ map (readRowAt dataBuf) offsets


newtype KiokuQuery = KQ { kqFunc :: TrieIndex -> [Int] }

keyExact :: BS.ByteString -> KiokuQuery
keyExact key = KQ (trieLookup key)

keyExactIn :: [BS.ByteString] -> KiokuQuery
keyExactIn key = KQ (trieLookupMany key)

keyPrefix :: BS.ByteString -> KiokuQuery
keyPrefix key = KQ (trieMatch key)

