module Main where

import            Data.Bits
import qualified  Data.ByteString as BS
import qualified  Data.ByteString.Char8 as CBS
import            Database.Kioku
import            System.Environment
import            System.Random

main :: IO ()
main = do
  args <- getArgs

  case args of
    ("memorize":file:countStr:_) -> do
      gen <- newStdGen
      let count = read countStr
      putStrLn $ "Memorizing data to " ++ file
      rowsSaved <- memorizeRows (rows count gen) file
      putStrLn $ "Done... memorized " ++ show rowsSaved ++ " rows"

    ("index":file:_) -> do
      putStrLn $ "Indexing data in " ++ file
      indexRows "idx" id file

rows :: Int -> StdGen -> [BS.ByteString]
rows count g = [ CBS.pack $ show (n :: Int)
               | n <- take count $ randoms g
               ]

