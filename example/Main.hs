module Main where

import qualified Codec.Compression.GZip as GZ
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as CBS
import qualified Data.ByteString.Lazy.Char8 as LBS
import Data.Foldable
import Database.Kioku
import System.Environment
import System.Exit

main :: IO ()
main = do
    args <- getArgs

    withKiokuDB defaultKiokuPath $ \db -> do
        case args of
            ("cities" : "load" : _) -> do
                putStrLn $ "Loading city data."

                count <- do
                    cities <- loadCities "example/data/cities.txt.gz"
                    createDataSet "cities" cities db

                putStrLn $ "Done... loaded " ++ show count ++ " cities"
            ("cities" : "index" : _) -> do
                putStrLn $ "Indexing cities by name."
                createIndex "cities" "cities.name" cityName db
                putStrLn $ "Done... "
            ("cities" : "query" : name : _) -> do
                cities <- query "cities.name" (keyPrefix $ CBS.pack name) db
                printCities cities
            _ -> do
                putStrLn $ "Unknown command: " ++ unwords args
                exitWith (ExitFailure 1)

printCities :: [City] -> IO ()
printCities cities =
    for_ cities $ \city -> do
        CBS.putStr $ cityName city
        CBS.putStr $ " - ("
        CBS.putStr $ cityLat city
        CBS.putStr $ ","
        CBS.putStr $ cityLng city
        CBS.putStrLn $ ")"

data City = City
    { cityName :: BS.ByteString
    , cityLat :: BS.ByteString
    , cityLng :: BS.ByteString
    }

instance Memorizable City where
    memorize (City{..}) =
        lengthPrefix255
            [ memorize cityName
            , memorize cityLat
            , memorize cityLng
            ]

    recall =
        unLengthPrefix255
            City
            ( field
                &. field
                &. field
            )

loadCities :: FilePath -> IO [City]
loadCities =
    fmap (parseCities . GZ.decompress)
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
