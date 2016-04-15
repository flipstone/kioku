module Database.Kioku.Core
  ( KiokuDB, KiokuQuery, KiokuException
  , DataSetName, IndexName, SchemaName
  , openKiokuDB, defaultKiokuPath
  , closeKiokuDB, withKiokuDB

  , createDataSet
  , createIndex
  , createSchema
  , query, keyExact, keyExactIn, keyPrefix

  , gcKiokuDB, validateDB
  , packKiokuDB, exportKiokuDB
  , unpackKiokuDB, importKiokuDB
  ) where

import            Control.Exception
import            Control.Monad
import qualified  Codec.Archive.Tar as Tar
import qualified  Codec.Compression.GZip as GZ
import            Crypto.Hash.SHA256
import qualified  Data.ByteString.Char8 as BS
import qualified  Data.ByteString.Base16 as Base16
import qualified  Data.ByteString.Lazy as LBS
import            Data.IORef
import            Data.Foldable
import            Data.List ((\\))
import            Data.Maybe
import            Data.Traversable
import            System.Directory
import            System.IO
import            System.IO.Temp

import            Database.Kioku.Internal.Buffer
import            Database.Kioku.Internal.BufferMap
import            Database.Kioku.Internal.KiokuDB
import            Database.Kioku.Internal.TrieIndex
import            Database.Kioku.Internal.Query
import            Database.Kioku.Memorizable

defaultKiokuPath :: FilePath
defaultKiokuPath = ".kioku"

openKiokuDB :: FilePath -> IO KiokuDB
openKiokuDB path = do
  bufs <- newBufferMap

  let db = KiokuDB { rootDir = path, bufferMap = bufs }

  traverse_ (createDirectoryIfMissing True)
            [ dataDir db
            , tmpDir db
            , dataSetObjDir db
            , indexObjDir db
            , schemaObjDir db
            ]

  pure db

closeKiokuDB :: KiokuDB -> IO ()
closeKiokuDB = closeBuffers . bufferMap

validateDB :: KiokuDB -> IO [KiokuException]
validateDB db = do
    dataErrs <- validateDataFiles
    indexErrs <- validateIndexFiles
    schemaErrs <- validateSchemaFiles

    pure $ catMaybes $ dataErrs ++ indexErrs ++ schemaErrs
  where
    validateDataFiles = do
      paths <- listDirectoryContents $ dataDir db

      for paths $ \name -> do
        let bsName = BS.pack name
            fileName = dataFilePath db bsName

        sha <- hashFile fileName

        pure $
          if sha == bsName
            then Nothing
            else Just $ KiokuException $ "Data file " ++ name ++ " is corrupt!"

    validateIndexFiles = do
      paths <- listDirectoryContents $ indexObjDir db

      for paths $ \name -> do
        index <- readIndexFile name db
        pure $ either Just (const Nothing) index

    validateSchemaFiles = do
      paths <- listDirectoryContents $ schemaObjDir db

      for paths $ \name -> do
        index <- readSchemaFile name db
        pure $ either Just (const Nothing) index



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
      schemaRefs <- readSchemas
      pure (dataSets ++ concat indexes ++ concat schemaRefs)

    readDataSets = do
      paths <- listDirectoryContents $ dataSetObjDir db

      for paths $ \name -> do
        dataSetFile <- readDataSetFile name db
        pure (dataSetHash dataSetFile)

    indexRefs index = [indexHash index, dataHash index]

    readIndexes = do
      paths <- listDirectoryContents $ indexObjDir db

      for paths $ \name -> do
        indexFile <- throwErrors $ readIndexFile name db
        pure $ indexRefs indexFile


    readSchemas = do
      paths <- listDirectoryContents $ schemaObjDir db

      for paths $ \name -> do
        schema <- throwErrors $ readSchemaFile name db
        pure $ concatMap (indexRefs . indexContent) $ schemaIndexes schema

listDirectoryContents :: FilePath -> IO [FilePath]
listDirectoryContents dir =
  filter (not . (`elem` [".",".."])) <$> getDirectoryContents dir

packKiokuDB :: KiokuDB -> IO LBS.ByteString
packKiokuDB db = do
  entries <- Tar.pack (rootDir db) [dataPath, dataSetObjPath, indexObjPath, schemaObjPath]
  pure $ GZ.compress $ Tar.write entries

exportKiokuDB :: FilePath -> KiokuDB -> IO ()
exportKiokuDB path db = packKiokuDB db >>= LBS.writeFile path

unpackKiokuDB :: LBS.ByteString -> KiokuDB -> IO ()
unpackKiokuDB gzBytes db = do
  let tarBytes = GZ.decompress gzBytes
      entries = Tar.read tarBytes

  Tar.unpack (rootDir db) entries

importKiokuDB :: FilePath -> KiokuDB -> IO ()
importKiokuDB path db = do
  withTempDirectory (tmpDir db) "import." $ \tmpDBPath -> do
    withKiokuDB tmpDBPath $ \tmpDB -> do
      LBS.readFile path >>= flip unpackKiokuDB tmpDB
      errors <- validateDB tmpDB

      case errors of
        [] -> do
          let eachFile dir action = listDirectoryContents dir >>= mapM_ action
              rename check pathFunc name = do let tmpName = pathFunc tmpDB name
                                                  dbName = pathFunc db name

                                              checked <- check dbName
                                              when checked $ renameFile tmpName dbName

              renameAlways = rename (const $ pure True)
              renameIfNew  = rename (fmap not . doesFileExist)

          eachFile (dataDir tmpDB) (renameIfNew $ \d p -> dataFilePath d (BS.pack p))
          eachFile (indexObjDir tmpDB) (renameAlways indexObjFile)
          eachFile (schemaObjDir tmpDB) (renameAlways schemaObjFile)

        _ -> do
          let msg = "Found the following errors while validating the db import. The import has been abandoned to avoid corrupting the database."
              errStrings = map show errors
          throwIO $ KiokuException $ unlines (msg:errStrings)


withKiokuDB :: FilePath -> (KiokuDB -> IO a) -> IO a
withKiokuDB path action = do
  db <- openKiokuDB path
  action db `finally` closeKiokuDB db

hashFile :: FilePath -> IO BS.ByteString
hashFile = fmap (Base16.encode . hashlazy) . LBS.readFile

createSchema :: SchemaName -> [IndexName] -> KiokuDB -> IO ()
createSchema name indexNames db = do
  let readSchemaIndex idxName = SchemaIndex idxName <$> (throwErrors $ readIndexFile idxName db)
  indexes <- traverse readSchemaIndex indexNames
  writeSchemaFile name (SchemaFile indexes) db

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
createIndex dataSetName idxName keyFunc db = do
  dataSetFile <- readDataSetFile dataSetName db
  dataBuf <- openDataBuffer (dataSetHash dataSetFile) db
  tmpFile <- writeIndex keyFunc dataBuf $ \flushIndex -> do
    (tmpFile, h) <- openTempFile (tmpDir db) idxName
    flushIndex h
    hClose h
    pure tmpFile

  sha <- hashFile tmpFile
  renameFile tmpFile (dataFilePath db sha)

  let indexFile = IndexFile { indexHash = sha
                            , dataHash = dataSetHash dataSetFile
                            }

  writeIndexFile idxName indexFile db

query :: Memorizable a
      => IndexName
      -> KiokuQuery
      -> KiokuDB
      -> IO [a]
query name kQuery db = do
  indexFile <- throwErrors $ readIndexFile name db
  indexBuf <-  openDataBuffer (indexHash indexFile) db
  dataBuf <- openDataBuffer (dataHash indexFile) db

  pure $ runQuery kQuery indexBuf dataBuf


