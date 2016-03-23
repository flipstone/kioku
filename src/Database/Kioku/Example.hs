module Database.Kioku.Example
  ( runExample
  , inspectIndex
  ) where

import qualified  Data.ByteString.Char8 as CBS
import qualified  Data.ByteString.Lazy.Char8 as LBS
import            Data.Foldable
import            Database.Kioku

runExample ::  FilePath -> [CBS.ByteString] -> IO ()
runExample file items = do
  putStrLn "Memorizing rows to file"
  _ <- memorizeRows items file

  loadAllRows file $ \rows -> do
    for_ rows $ \(offset, dat) -> do
      putStr   $ "  "
      putStr   $ show offset
      putStr   $ " - "
      putStrLn $ show (dat :: CBS.ByteString)

  putStrLn "Indexing rows"
  indexRows "idx" id file
  putStrLn "Done"

inspectIndex :: FilePath -> IO ()
inspectIndex file = do
  withTrieIndex (file ++ ".idx")
                (LBS.putStrLn . inspectTrieIndex)

