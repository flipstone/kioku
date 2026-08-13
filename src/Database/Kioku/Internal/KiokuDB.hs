{-# LANGUAGE TypeApplications #-}

module Database.Kioku.Internal.KiokuDB where

import Control.Exception
import Crypto.Hash (hashlazy)
import Crypto.Hash.Algorithms (SHA256)
import Data.ByteArray (convert)
import qualified Data.ByteString.Base16 as Base16
import qualified Data.ByteString.Builder as Builder
import qualified Data.ByteString.Char8 as BS
import qualified Data.ByteString.Lazy.Char8 as LBS
import Data.Foldable
import Data.IORef
import Data.List (stripPrefix)
import qualified Data.Map.Strict as M
import Data.Maybe (mapMaybe)
import qualified Data.Set as Set
import Data.Typeable
import System.Directory
import System.FilePath
import System.IO
import System.IO.Error (doesNotExistErrorType, mkIOError)

import Database.Kioku.Internal.Buffer
import Database.Kioku.Internal.BufferMap

data KiokuDB = KiokuDB
  { storage :: Storage
  , bufferMap :: BufferMap
  }

{- | Where a database keeps its bytes. 'FileStorage' is the traditional
on-disk representation read via memory mapping. 'MemoryStorage' holds the
same content in an in-process map keyed by the same relative paths, for
consumers (such as tests) that must not touch the filesystem.
-}
data Storage
  = FileStorage FilePath
  | MemoryStorage (IORef (M.Map FilePath BS.ByteString))

newtype KiokuNamespace = KiokuNamespace String

newtype KiokuException = KiokuException String
  deriving (Show, Typeable)

instance Exception KiokuException

dataPath :: FilePath
dataPath = "data"

tmpPath :: FilePath
tmpPath = "tmp"

objPath :: FilePath
objPath = "objects"

namespaceObjPath :: KiokuNamespace -> FilePath
namespaceObjPath (KiokuNamespace namespace) = objPath </> namespace

dataSetObjPath :: KiokuNamespace -> FilePath
dataSetObjPath namespace = namespaceObjPath namespace </> "data_set"

indexObjPath :: KiokuNamespace -> FilePath
indexObjPath namespace = namespaceObjPath namespace </> "index"

schemaObjPath :: KiokuNamespace -> FilePath
schemaObjPath namespace = namespaceObjPath namespace </> "schema"

dataSetObjFile :: KiokuNamespace -> DataSetName -> FilePath
dataSetObjFile namespace name = dataSetObjPath namespace </> name

indexObjFile :: KiokuNamespace -> IndexName -> FilePath
indexObjFile namespace name = indexObjPath namespace </> name

schemaObjFile :: KiokuNamespace -> SchemaName -> FilePath
schemaObjFile namespace name = schemaObjPath namespace </> name

dataFilePath :: BS.ByteString -> FilePath
dataFilePath sha = dataPath </> BS.unpack sha

type DataSetName = String
type IndexName = String
type SchemaName = String

{- | Reads a stored object by its relative path within the database. A missing
object raises a 'doesNotExistErrorType' 'IOError' in both backends, so callers
can handle it the same way regardless of where the bytes live.
-}
storageRead :: KiokuDB -> FilePath -> IO BS.ByteString
storageRead db path =
  case storage db of
    FileStorage root ->
      BS.readFile (root </> path)
    MemoryStorage ref -> do
      contents <- readIORef ref
      case M.lookup path contents of
        Just bytes -> pure bytes
        Nothing ->
          ioError $ mkIOError doesNotExistErrorType "storageRead" Nothing (Just path)

-- | Writes a stored object at its relative path within the database.
storageWrite :: KiokuDB -> FilePath -> BS.ByteString -> IO ()
storageWrite db path bytes =
  case storage db of
    FileStorage root -> do
      let
        fullPath = root </> path
      createDirectoryIfMissing True (takeDirectory fullPath)
      BS.writeFile fullPath bytes
    MemoryStorage ref ->
      atomicModifyIORef' ref $ \contents ->
        (M.insert path bytes contents, ())

{- | Lists the names of the immediate children of a relative directory path.

The backends diverge for a path that names nothing: memory storage has no
notion of an empty directory (see 'writeObjFile'), so it simply finds no keys
under the prefix and returns an empty list, while the file backend throws.
Nothing in the library relies on this today, since 'openKiokuDB' creates the
top level directories and 'writeObjFile' creates the per-namespace ones.
-}
storageList :: KiokuDB -> FilePath -> IO [FilePath]
storageList db path =
  case storage db of
    FileStorage root ->
      filter (not . (`elem` [".", ".."])) <$> getDirectoryContents (root </> path)
    MemoryStorage ref -> do
      contents <- readIORef ref

      let
        prefix = addTrailingPathSeparator path
        childName key = takeWhile (not . isPathSeparator) <$> stripPrefix prefix key

      pure . Set.toList . Set.fromList . mapMaybe childName . M.keys $ contents

-- | Removes a stored object by its relative path within the database.
storageRemove :: KiokuDB -> FilePath -> IO ()
storageRemove db path =
  case storage db of
    FileStorage root ->
      removeFile (root </> path)
    MemoryStorage ref ->
      atomicModifyIORef' ref $ \contents ->
        (M.delete path contents, ())

{- | Streams content-addressed blob data through the given writer, storing the
result under its SHA256 hash. Returns the hash and the writer's result. The
file backend streams to a temp file to keep memory usage flat for large
datasets; the memory backend accumulates the bytes in a builder.
-}
createBlob ::
  KiokuDB ->
  String ->
  ((BS.ByteString -> IO ()) -> IO a) ->
  IO (BS.ByteString, a)
createBlob db name writer =
  case storage db of
    FileStorage root -> do
      (tmpFile, h) <- openTempFile (root </> tmpPath) name

      let
        -- Nothing ever cleans up tmpPath, so a writer that throws must not
        -- leave the handle open or the temp file behind. removePathForcibly
        -- tolerates the file already being gone, which is the case when
        -- renameFile is what threw.
        cleanup = do
          hClose h
          removePathForcibly tmpFile

      flip onException cleanup $ do
        result <- writer (BS.hPutStr h)
        hClose h

        sha <- hashBytes <$> LBS.readFile tmpFile
        renameFile tmpFile (root </> dataFilePath sha)
        pure (sha, result)
    MemoryStorage ref -> do
      builderRef <- newIORef mempty
      result <- writer (\bs -> modifyIORef' builderRef (<> Builder.byteString bs))
      builder <- readIORef builderRef

      let
        lazyBytes = Builder.toLazyByteString builder
        sha = hashBytes lazyBytes

      atomicModifyIORef' ref $ \contents ->
        (M.insert (dataFilePath sha) (LBS.toStrict lazyBytes) contents, ())

      pure (sha, result)

hashBytes :: LBS.ByteString -> BS.ByteString
hashBytes = Base16.encode . convert . hashlazy @SHA256

data DataSetFile = DataSetFile
  { dataSetHash :: BS.ByteString
  }

readDataSetFile :: KiokuNamespace -> DataSetName -> KiokuDB -> IO DataSetFile
readDataSetFile namespace name db =
  DataSetFile <$> storageRead db (dataSetObjFile namespace name)

writeDataSetFile :: KiokuNamespace -> DataSetName -> DataSetFile -> KiokuDB -> IO ()
writeDataSetFile namespace name file db = do
  writeObjFile db namespace (dataSetObjFile namespace name) (dataSetHash file)

data IndexFile = IndexFile
  { indexHash :: BS.ByteString
  , dataHash :: BS.ByteString
  }

throwErrors :: IO (Either KiokuException a) -> IO a
throwErrors action = action >>= either throwIO pure

readIndexFile :: KiokuNamespace -> IndexName -> KiokuDB -> IO (Either KiokuException IndexFile)
readIndexFile namespace name db = do
  indexBytes <- storageRead db (indexObjFile namespace name)

  pure $
    case BS.lines indexBytes of
      [idx, dat] -> pure $ IndexFile idx dat
      _ -> Left $ KiokuException $ "Index " ++ show name ++ " is corrupt!"

writeIndexFile :: KiokuNamespace -> IndexName -> IndexFile -> KiokuDB -> IO ()
writeIndexFile namespace name file db =
  writeObjFile
    db
    namespace
    (indexObjFile namespace name)
    (BS.unlines [indexHash file, dataHash file])

data SchemaIndex = SchemaIndex
  { indexName :: IndexName
  , indexContent :: IndexFile
  }

data SchemaFile = SchemaFile
  { schemaIndexes :: [SchemaIndex]
  }

writeSchemaFile :: KiokuNamespace -> SchemaName -> SchemaFile -> KiokuDB -> IO ()
writeSchemaFile namespace name file db = do
  writeObjFile db namespace (schemaObjFile namespace name) schemaData
 where
  schemaData = BS.unlines $ map indexLine $ schemaIndexes file
  indexLine idx =
    BS.unwords
      [ BS.pack $ indexName idx
      , indexHash $ indexContent idx
      , dataHash $ indexContent idx
      ]

readSchemaFile :: KiokuNamespace -> SchemaName -> KiokuDB -> IO (Either KiokuException SchemaFile)
readSchemaFile namespace name db = do
  schemaBytes <- storageRead db (schemaObjFile namespace name)

  let
    indexLines = BS.lines schemaBytes
    parseIndex line = case BS.words line of
      [idxName, idx, dat] -> pure $ SchemaIndex (BS.unpack idxName) (IndexFile idx dat)
      _ -> Left $ KiokuException $ "Schema " ++ show name ++ " is corrupt!"

    indexes = traverse parseIndex indexLines

  pure $ fmap SchemaFile indexes

writeObjFile :: KiokuDB -> KiokuNamespace -> FilePath -> BS.ByteString -> IO ()
writeObjFile db namespace path bytes = do
  -- In order to avoid sparse namespace directories, we create the data set, index, and schema directories
  -- for each namespace at write if they don't exist. This only applies to file
  -- storage; memory storage has no notion of empty directories.
  case storage db of
    FileStorage root ->
      traverse_
        (createDirectoryIfMissing True . (root </>))
        [ dataSetObjPath namespace
        , indexObjPath namespace
        , schemaObjPath namespace
        ]
    MemoryStorage _ ->
      pure ()

  storageWrite db path bytes

openDataBuffer :: BS.ByteString -> KiokuDB -> IO Buffer
openDataBuffer bufName db =
  case storage db of
    FileStorage root ->
      openBuffer (root </> dataFilePath bufName) (bufferMap db)
    MemoryStorage _ ->
      bufferFromByteString <$> storageRead db (dataFilePath bufName)
