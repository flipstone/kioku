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

newtype KiokuException = KiokuException String
  deriving (Show, Typeable)

instance Exception KiokuException

dataPath, tmpPath, objPath, dataSetObjPath, indexObjPath, schemaObjPath :: FilePath
dataPath = "data"
tmpPath = "tmp"
objPath = "objects"
dataSetObjPath = objPath </> "data_set"
indexObjPath = objPath </> "index"
schemaObjPath = objPath </> "schema"

dataDir :: KiokuDB -> FilePath
dataDir db = rootDir db </> dataPath

tmpDir :: KiokuDB -> FilePath
tmpDir db = rootDir db </> tmpPath

dataSetObjDir :: KiokuDB -> FilePath
dataSetObjDir db = rootDir db </> dataSetObjPath

indexObjDir :: KiokuDB -> FilePath
indexObjDir db = rootDir db </> indexObjPath

schemaObjDir :: KiokuDB -> FilePath
schemaObjDir db = rootDir db </> schemaObjPath

dataSetObjFile :: KiokuDB -> DataSetName -> FilePath
dataSetObjFile db name = dataSetObjDir db </> name

indexObjFile :: KiokuDB -> IndexName -> FilePath
indexObjFile db name = indexObjDir db </> name

schemaObjFile :: KiokuDB -> SchemaName -> FilePath
schemaObjFile db name = schemaObjDir db </> name

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

readDataSetFile :: DataSetName -> KiokuDB -> IO DataSetFile
readDataSetFile name db =
  DataSetFile <$> (BS.readFile $ dataSetObjFile db name)

writeDataSetFile :: DataSetName -> DataSetFile -> KiokuDB -> IO ()
writeDataSetFile name file db = do
  writeDBFile (dataSetObjFile db name) (dataSetHash file)

data IndexFile = IndexFile
  { indexHash :: BS.ByteString
  , dataHash :: BS.ByteString
  }

throwErrors :: IO (Either KiokuException a) -> IO a
throwErrors action = action >>= either throwIO pure

readIndexFile :: IndexName -> KiokuDB -> IO (Either KiokuException IndexFile)
readIndexFile name db = do
  indexBytes <- BS.readFile (indexObjFile db name)

  pure $
    case BS.lines indexBytes of
      [idx, dat] -> pure $ IndexFile idx dat
      _ -> Left $ KiokuException $ "Index " ++ show name ++ " is corrupt!"

writeIndexFile :: IndexName -> IndexFile -> KiokuDB -> IO ()
writeIndexFile name file db =
  writeDBFile
    (indexObjFile db name)
    (BS.unlines [indexHash file, dataHash file])

data SchemaIndex = SchemaIndex
  { indexName :: IndexName
  , indexContent :: IndexFile
  }

data SchemaFile = SchemaFile
  { schemaIndexes :: [SchemaIndex]
  }

writeSchemaFile :: SchemaName -> SchemaFile -> KiokuDB -> IO ()
writeSchemaFile name file db = do
  writeDBFile (schemaObjFile db name) schemaData
 where
  schemaData = BS.unlines $ map indexLine $ schemaIndexes file
  indexLine idx =
    BS.unwords
      [ BS.pack $ indexName idx
      , indexHash $ indexContent idx
      , dataHash $ indexContent idx
      ]

readSchemaFile :: SchemaName -> KiokuDB -> IO (Either KiokuException SchemaFile)
readSchemaFile name db = do
  schemaBytes <- BS.readFile $ schemaObjFile db name

  let
    indexLines = BS.lines schemaBytes
    parseIndex line = case BS.words line of
      [idxName, idx, dat] -> pure $ SchemaIndex (BS.unpack idxName) (IndexFile idx dat)
      _ -> Left $ KiokuException $ "Schema " ++ show name ++ " is corrupt!"

    indexes = traverse parseIndex indexLines

  pure $ fmap SchemaFile indexes

openDataBuffer :: BS.ByteString -> KiokuDB -> IO Buffer
openDataBuffer bufName db = openBuffer (dataFilePath db bufName) (bufferMap db)
