module Database.Kioku.Core
  ( KiokuDB
  , KiokuQuery
  , KiokuException
  , DataSetName
  , IndexName
  , SchemaName
  , KiokuNamespace (..)
  , openKiokuDB
  , defaultKiokuPath
  , closeKiokuDB
  , withKiokuDB
  , createDataSet
  , createIndex
  , createSchema
  , query
  , keyExact
  , keyExactIn
  , keyPrefix
  , keyAllHitsAlong
  , gcKiokuDB
  ) where

import Control.Exception
import Crypto.Hash.SHA256
import qualified Data.ByteString.Base16 as Base16
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy.Char8 as LBS
import Data.Foldable
import Data.IORef
import Data.List ((\\))
import Data.Traversable
import System.Directory
import System.IO

import Database.Kioku.Internal.BufferMap
import Database.Kioku.Internal.KiokuDB
import Database.Kioku.Internal.Query
import Database.Kioku.Internal.TrieIndex
import Database.Kioku.Memorizable

defaultKiokuPath :: FilePath
defaultKiokuPath = ".kioku"

openKiokuDB :: FilePath -> IO KiokuDB
openKiokuDB path = do
  bufs <- newBufferMap

  let
    db = KiokuDB {rootDir = path, bufferMap = bufs}

  traverse_
    (createDirectoryIfMissing True)
    [ dataDir db
    , tmpDir db
    , objDir db
    ]

  pure db

closeKiokuDB :: KiokuDB -> IO ()
closeKiokuDB = closeBuffers . bufferMap

gcKiokuDB :: KiokuDB -> IO ()
gcKiokuDB db = do
  closeKiokuDB db
  hashRefs <- readHashRefs
  dataFiles <- listDirectoryContents $ dataDir db

  let
    hashes = BS.pack <$> dataFiles
    unreferenced = hashes \\ hashRefs
    unusedFiles = dataFilePath db <$> unreferenced

  traverse_ removeFile unusedFiles
 where
  readHashRefs :: IO [BS.ByteString]
  readHashRefs = do
    namespaces <- listDirectoryContents $ objDir db
    hashRefs <- mapM readHashRefsFor $ fmap KiokuNamespace namespaces
    pure $ concat hashRefs

  readHashRefsFor namespace = do
    dataSets <- readDataSetsFor namespace
    indexes <- readIndexesFor namespace
    schemaRefs <- readSchemasFor namespace
    pure (dataSets ++ concat indexes ++ concat schemaRefs)

  readDataSetsFor namespace = do
    paths <- listDirectoryContents $ dataSetObjDir db namespace

    for paths $ \name -> do
      dataSetFile <- readDataSetFile namespace name db
      pure (dataSetHash dataSetFile)

  indexRefs index = [indexHash index, dataHash index]

  readIndexesFor namespace = do
    paths <- listDirectoryContents $ indexObjDir db namespace

    for paths $ \name -> do
      indexFile <- throwErrors $ readIndexFile namespace name db
      pure $ indexRefs indexFile

  readSchemasFor namespace = do
    paths <- listDirectoryContents $ schemaObjDir db namespace

    for paths $ \name -> do
      schema <- throwErrors $ readSchemaFile namespace name db
      pure $ concatMap (indexRefs . indexContent) $ schemaIndexes schema

listDirectoryContents :: FilePath -> IO [FilePath]
listDirectoryContents dir =
  filter (not . (`elem` [".", ".."])) <$> getDirectoryContents dir

withKiokuDB :: FilePath -> (KiokuDB -> IO a) -> IO a
withKiokuDB path action = do
  db <- openKiokuDB path
  action db `finally` closeKiokuDB db

hashFile :: FilePath -> IO BS.ByteString
hashFile = fmap (Base16.encode . hashlazy) . LBS.readFile

createSchema :: KiokuNamespace -> SchemaName -> [IndexName] -> KiokuDB -> IO ()
createSchema namespace name indexNames db = do
  let
    readSchemaIndex idxName = SchemaIndex idxName <$> (throwErrors $ readIndexFile namespace idxName db)
  indexes <- traverse readSchemaIndex indexNames
  writeSchemaFile namespace name (SchemaFile indexes) db

createDataSet :: Memorizable a => KiokuNamespace -> DataSetName -> [a] -> KiokuDB -> IO Int
createDataSet namespace name as db = do
  (tmpFile, h) <- openTempFile (tmpDir db) name
  count <- hWriteRows as h
  hClose h

  sha <- hashFile tmpFile
  renameFile tmpFile (dataFilePath db sha)
  writeDataSetFile namespace name (DataSetFile {dataSetHash = sha}) db
  pure count

hWriteRows :: Memorizable a => [a] -> Handle -> IO Int
hWriteRows as h = do
  count <- newIORef (0 :: Int)

  for_ as $ \a -> do
    let
      bytes = memorize a
      len = BS.length bytes
      header = memorize len

    BS.hPutStr h header
    BS.hPutStr h bytes

    modifyIORef count (+ 1)

  c <- readIORef count

  BS.hPutStr h (memorize c)

  pure c

createIndex ::
  Memorizable a =>
  KiokuNamespace ->
  DataSetName ->
  IndexName ->
  (a -> BS.ByteString) ->
  KiokuDB ->
  IO ()
createIndex namespace dataSetName idxName keyFunc db = do
  dataSetFile <- readDataSetFile namespace dataSetName db
  dataBuf <- openDataBuffer (dataSetHash dataSetFile) db
  tmpFile <- writeIndex keyFunc dataBuf $ \flushIndex -> do
    (tmpFile, h) <- openTempFile (tmpDir db) idxName
    flushIndex h
    hClose h
    pure tmpFile

  sha <- hashFile tmpFile
  renameFile tmpFile (dataFilePath db sha)

  let
    indexFile =
      IndexFile
        { indexHash = sha
        , dataHash = dataSetHash dataSetFile
        }

  writeIndexFile namespace idxName indexFile db

query ::
  Memorizable a =>
  KiokuNamespace ->
  IndexName ->
  KiokuQuery ->
  KiokuDB ->
  IO [a]
query namespace name kQuery db = do
  indexFile <- throwErrors $ readIndexFile namespace name db
  indexBuf <- openDataBuffer (indexHash indexFile) db
  dataBuf <- openDataBuffer (dataHash indexFile) db

  pure $ runQuery kQuery indexBuf dataBuf
