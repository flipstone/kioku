module Database.Kioku.Core
  ( KiokuDB
  , KiokuQuery
  , KiokuException
  , DataSetName
  , IndexName
  , SchemaName
  , KiokuNamespace (..)
  , openKiokuDB
  , newInMemoryKiokuDB
  , withInMemoryKiokuDB
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
import qualified Data.ByteString.Char8 as BS
import Data.Foldable
import Data.IORef
import Data.List ((\\))
import qualified Data.Map.Strict as M
import Data.Traversable
import System.Directory
import System.FilePath ((</>))

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
    db = KiokuDB {storage = FileStorage path, bufferMap = bufs}

  traverse_
    (createDirectoryIfMissing True . (path </>))
    [ dataPath
    , tmpPath
    , objPath
    ]

  pure db

{- | Creates a database that lives entirely in memory and never touches the
filesystem. It holds the same content-addressed representation as an
on-disk database and supports the same operations.

Note that this backend is more permissive about result lifetimes than a
file-backed database. Its buffers are ordinary heap 'BS.ByteString's that stay
valid indefinitely, whereas a file-backed database serves results from mmapped
regions that 'closeKiokuDB' (and therefore 'gcKiokuDB') unmaps. Code that must
also work against a file-backed database still has to force or copy query
results before the database is closed; an in-memory database will not catch a
failure to do so.
-}
newInMemoryKiokuDB :: IO KiokuDB
newInMemoryKiokuDB = do
  bufs <- newBufferMap
  contents <- newIORef M.empty
  pure KiokuDB {storage = MemoryStorage contents, bufferMap = bufs}

closeKiokuDB :: KiokuDB -> IO ()
closeKiokuDB = closeBuffers . bufferMap

gcKiokuDB :: KiokuDB -> IO ()
gcKiokuDB db = do
  closeKiokuDB db
  hashRefs <- readHashRefs
  dataFiles <- storageList db dataPath

  let
    hashes = BS.pack <$> dataFiles
    unreferenced = hashes \\ hashRefs
    unusedFiles = dataFilePath <$> unreferenced

  traverse_ (storageRemove db) unusedFiles
 where
  readHashRefs = do
    namespaces <- storageList db objPath
    hashRefs <- mapM readHashRefsFor $ fmap KiokuNamespace namespaces
    pure $ concat hashRefs

  readHashRefsFor namespace = do
    dataSets <- readDataSetsFor namespace
    indexes <- readIndexesFor namespace
    schemaRefs <- readSchemasFor namespace
    pure (dataSets ++ concat indexes ++ concat schemaRefs)

  readDataSetsFor namespace = do
    paths <- storageList db (dataSetObjPath namespace)

    for paths $ \name -> do
      dataSetFile <- readDataSetFile namespace name db
      pure (dataSetHash dataSetFile)

  indexRefs index = [indexHash index, dataHash index]

  readIndexesFor namespace = do
    paths <- storageList db (indexObjPath namespace)

    for paths $ \name -> do
      indexFile <- throwErrors $ readIndexFile namespace name db
      pure $ indexRefs indexFile

  readSchemasFor namespace = do
    paths <- storageList db (schemaObjPath namespace)

    for paths $ \name -> do
      schema <- throwErrors $ readSchemaFile namespace name db
      pure $ concatMap (indexRefs . indexContent) $ schemaIndexes schema

withKiokuDB :: FilePath -> (KiokuDB -> IO a) -> IO a
withKiokuDB path action = do
  db <- openKiokuDB path
  action db `finally` closeKiokuDB db

{- | Runs an action against a database that lives entirely in memory. See
'newInMemoryKiokuDB' for how this backend differs from a file-backed one.
-}
withInMemoryKiokuDB :: (KiokuDB -> IO a) -> IO a
withInMemoryKiokuDB action = do
  db <- newInMemoryKiokuDB
  action db `finally` closeKiokuDB db

createSchema :: KiokuNamespace -> SchemaName -> [IndexName] -> KiokuDB -> IO ()
createSchema namespace name indexNames db = do
  let
    readSchemaIndex idxName = SchemaIndex idxName <$> (throwErrors $ readIndexFile namespace idxName db)
  indexes <- traverse readSchemaIndex indexNames
  writeSchemaFile namespace name (SchemaFile indexes) db

createDataSet :: Memorizable a => KiokuNamespace -> DataSetName -> [a] -> KiokuDB -> IO Int
createDataSet namespace name as db = do
  (sha, count) <- createBlob db name (\sink -> writeRows sink as)
  writeDataSetFile namespace name (DataSetFile {dataSetHash = sha}) db
  pure count

writeRows :: Memorizable a => (BS.ByteString -> IO ()) -> [a] -> IO Int
writeRows sink as = do
  count <- newIORef (0 :: Int)

  for_ as $ \a -> do
    let
      bytes = memorize a
      len = BS.length bytes
      header = memorize len

    sink header
    sink bytes

    modifyIORef' count (+ 1)

  c <- readIORef count

  sink (memorize c)

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

  (sha, ()) <-
    createBlob db idxName $ \sink ->
      writeIndex keyFunc dataBuf $ \flushIndex ->
        flushIndex sink

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
