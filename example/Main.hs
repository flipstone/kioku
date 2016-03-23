module Main where

import qualified  Codec.Compression.GZip as GZ
import            Data.Bits
import qualified  Data.ByteString as BS
import qualified  Data.ByteString.Char8 as CBS
import qualified  Data.ByteString.Lazy.Char8 as LBS
import            Data.Foldable
import            Database.Kioku
import            System.Environment
import            System.Random

main :: IO ()
main = do
  args <- getArgs

  case args of
    ("cities":"load":_) -> do
      putStrLn $ "Loading city data."

      count <- do cities <- loadCities "example/data/cities.txt.gz"
                  memorizeRows cities "cities.data"

      putStrLn $ "Done... loaded " ++ show count ++ " cities"

    ("cities":"index":_) -> do
      putStrLn $ "Indexing cities by name."

      indexRows "name" cityName "cities.data"

      putStrLn $ "Done... "

    ("cities":"query":name:_) -> do
      withKiokuData "cities.data" "name" $ \dat -> do
        let results = kiokuMatch (CBS.pack name) dat
        for_ results $ \city -> do
          CBS.putStr   $ cityName city
          CBS.putStr   $ " - ("
          CBS.putStr   $ cityLat city
          CBS.putStr   $ ","
          CBS.putStr   $ cityLng city
          CBS.putStrLn $ ")"

data City = City {
    cityName :: BS.ByteString
  , cityLat :: BS.ByteString
  , cityLng :: BS.ByteString
  }

instance Memorizable City where
  memorize c =
    CBS.intercalate "\t" [ cityName c
                         , cityLat c
                         , cityLng c
                         ]

  recall bs =
    let parts = CBS.split '\t' bs
    in City (parts !! 0) (parts !! 1) (parts !! 2)

loadCities :: FilePath -> IO [City]
loadCities = fmap (parseCities . GZ.decompress)
           . LBS.readFile

parseCities :: LBS.ByteString -> [City]
parseCities bytes | LBS.null bytes = []
parseCities bytes =
  let (line, lineRest) = LBS.break (== '\n') bytes
      parts = LBS.split '\t' line
      name = LBS.toStrict (parts !! 1)
      lat = LBS.toStrict (parts !! 4)
      lng = LBS.toStrict (parts !! 5)

  in (City name lat lng) : parseCities (LBS.drop 1 lineRest)

