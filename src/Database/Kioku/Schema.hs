module Database.Kioku.Schema
  ( KiokuSchema
  , querySchema
  , openSchema
  ) where

import qualified  Data.Map.Strict as M
import            Data.Traversable

import            Database.Kioku.Internal.Buffer
import            Database.Kioku.Internal.BufferMap
import            Database.Kioku.Internal.KiokuDB
import            Database.Kioku.Internal.Query
import            Database.Kioku.Memorizable

newtype KiokuSchema = KiokuSchema {
    indexes :: M.Map IndexName (Buffer, Buffer)
  }

querySchema :: Memorizable a => IndexName -> KiokuQuery -> KiokuSchema -> [a]
querySchema indexName kQuery schema =
  case M.lookup indexName (indexes schema) of
    Nothing -> error $ "Missing index in Kioku schema: " ++ indexName
    Just (indexBuf, dataBuf) -> runQuery kQuery indexBuf dataBuf

openSchema :: SchemaName -> KiokuDB -> IO KiokuSchema
openSchema name db = do
  schemaFile <- throwErrors $ readSchemaFile name db

  buffers <- for (schemaIndexes schemaFile) $ \schemaIndex -> do
    let name = indexName schemaIndex
        idxHash = indexHash $ indexContent schemaIndex
        datHash = dataHash $ indexContent schemaIndex

    indexBuf <- openDataBuffer idxHash db
    dataBuf <- openDataBuffer datHash db

    pure (name, (indexBuf, dataBuf))

  pure $ KiokuSchema $ M.fromList buffers
