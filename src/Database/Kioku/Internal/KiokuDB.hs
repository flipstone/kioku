module Database.Kioku.Internal.KiokuDB where

import Control.Exception
import qualified Data.ByteString.Char8 as BS
import Data.Typeable
import System.Directory
import System.FilePath

import Database.Kioku.Internal.Buffer
import Database.Kioku.Internal.BufferMap

data KiokuDB = KiokuDB
  { rootDir :: FilePath
  , bufferMap :: BufferMap
  }

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

dataDir :: KiokuDB -> FilePath
dataDir db = rootDir db </> dataPath

tmpDir :: KiokuDB -> FilePath
tmpDir db = rootDir db </> tmpPath

objDir :: KiokuDB -> FilePath
objDir db = rootDir db </> objPath

dataSetObjDir :: KiokuDB -> KiokuNamespace -> FilePath
dataSetObjDir db namespace = rootDir db </> dataSetObjPath namespace

indexObjDir :: KiokuDB -> KiokuNamespace -> FilePath
indexObjDir db namespace = rootDir db </> indexObjPath namespace

schemaObjDir :: KiokuDB -> KiokuNamespace -> FilePath
schemaObjDir db namespace = rootDir db </> schemaObjPath namespace

dataSetObjFile :: KiokuDB -> KiokuNamespace -> DataSetName -> FilePath
dataSetObjFile db namespace name = dataSetObjDir db namespace </> name

indexObjFile :: KiokuDB -> KiokuNamespace -> IndexName -> FilePath
indexObjFile db namespace name = indexObjDir db namespace </> name

schemaObjFile :: KiokuDB -> KiokuNamespace -> SchemaName -> FilePath
schemaObjFile db namespace name = schemaObjDir db namespace </> name

dataFilePath :: KiokuDB -> BS.ByteString -> FilePath
dataFilePath db sha = dataDir db </> BS.unpack sha

type DataSetName = String
type IndexName = String
type SchemaName = String

writeDBFile :: FilePath -> BS.ByteString -> IO ()
writeDBFile path bytes = do
  createDirectoryIfMissing True (takeDirectory path)
  BS.writeFile path bytes

data DataSetFile = DataSetFile
  { dataSetHash :: BS.ByteString
  }

readDataSetFile :: KiokuNamespace -> DataSetName -> KiokuDB -> IO DataSetFile
readDataSetFile namespace name db =
  DataSetFile <$> (BS.readFile $ dataSetObjFile db namespace name)

writeDataSetFile :: KiokuNamespace -> DataSetName -> DataSetFile -> KiokuDB -> IO ()
writeDataSetFile namespace name file db = do
  writeDBFile (dataSetObjFile db namespace name) (dataSetHash file)

data IndexFile = IndexFile
  { indexHash :: BS.ByteString
  , dataHash :: BS.ByteString
  }

throwErrors :: IO (Either KiokuException a) -> IO a
throwErrors action = action >>= either throwIO pure

readIndexFile :: KiokuNamespace -> IndexName -> KiokuDB -> IO (Either KiokuException IndexFile)
readIndexFile namespace name db = do
  indexBytes <- BS.readFile (indexObjFile db namespace name)

  pure $
    case BS.lines indexBytes of
      [idx, dat] -> pure $ IndexFile idx dat
      _ -> Left $ KiokuException $ "Index " ++ show name ++ " is corrupt!"

writeIndexFile :: KiokuNamespace -> IndexName -> IndexFile -> KiokuDB -> IO ()
writeIndexFile namespace name file db =
  writeDBFile
    (indexObjFile db namespace name)
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
  writeDBFile (schemaObjFile db namespace name) schemaData
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
  schemaBytes <- BS.readFile $ schemaObjFile db namespace name

  let
    indexLines = BS.lines schemaBytes
    parseIndex line = case BS.words line of
      [idxName, idx, dat] -> pure $ SchemaIndex (BS.unpack idxName) (IndexFile idx dat)
      _ -> Left $ KiokuException $ "Schema " ++ show name ++ " is corrupt!"

    indexes = traverse parseIndex indexLines

  pure $ fmap SchemaFile indexes

openDataBuffer :: BS.ByteString -> KiokuDB -> IO Buffer
openDataBuffer bufName db = openBuffer (dataFilePath db bufName) (bufferMap db)
