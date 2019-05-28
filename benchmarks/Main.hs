{-# LANGUAGE GeneralizedNewtypeDeriving #-}
module Main where

import            Control.DeepSeq (NFData, deepseq)
import            Control.Exception
import            Control.Monad (replicateM, void)
import qualified  Data.ByteString as BS
import            Data.Time.Clock.POSIX
import            Data.Foldable (for_)
import            System.IO

import            Database.Kioku.Memorizable
import            Database.Kioku

main :: IO ()
main = do
  let item = BenchItem "A B C " "It's as easy as" "1 2 3" "As simple as" "do re mi"
      trials = (10 :: Int)  ^ (7 :: Int)

  bench trials "id" () id
  bench trials "(+1)" (1 :: Int) (+1)
  bench trials "memDelimited" item (memDelimited)
  bench trials "recallDelimited" (memDelimited item) recallDelimited
  bench trials "memPrefixed255" item (memPrefixed255)
  bench trials "recallPrefixed255" (memPrefixed255 item) recallPrefixed255

  bench trials "memPrefixed65535" item (memPrefixed65535)
  bench trials "recallPrefixed65535" (memPrefixed65535 item) recallPrefixed65535

  benchQueries 1000


bench :: Int -> String -> a -> (a -> b) -> IO ()
bench count name a f = do
    putStr $ name ++ " (" ++ show count ++ ")"
    _ <- evaluate a
    hFlush stdout
    start <- getPOSIXTime
    evaluate $ seqN count f a
    finish <- getPOSIXTime

    let time = finish - start

        millis :: Int
        millis = round (1000 * time)

        opsPerSecond :: Double
        opsPerSecond = realToFrac count / realToFrac time

    putStrLn $ " - "
            ++ show millis ++ " milliseconds, "
            ++ (formatExponential 1 opsPerSecond) ++ " ops/second"

  where
    seqN 0 _ _ = ()
    seqN n f' a' = f' a' `seq` seqN (n - 1) f' a'


benchmarkData :: [TestData]
benchmarkData = [ TestData "101"
                , TestData "1010"
                , TestData "101005"
                , TestData "101006"
                , TestData "101007"
                , TestData "1010065"
                , TestData "1010068"
                ]

benchQueries :: Int -> IO ()
benchQueries count = do
  let queries = [ ("keyExact", keyExact "1010065")
                , ("keyExactIn", keyExactIn ["1010065"])
                , ("keyPrefix", keyPrefix "101")
                , ("keyAllHitsAlong", keyAllHitsAlong "1010068")
                ]

  withKiokuDB defaultKiokuPath $ \db -> do
    void $ createDataSet "kioku_bench" benchmarkData db
    createIndex "kioku_bench" "kioku_bench.index" testDataKey db

    for_ queries $ \(queryName, q) -> do
      putStr $ queryName ++ " (" ++ show count ++ ")"
      start <- getPOSIXTime

      _ <- replicateM count $ do
        results <- query "kioku_bench.index" q db :: IO [TestData]
        results `deepseq` pure ()

      finish <- getPOSIXTime

      let time = finish - start
          millis :: Int
          millis = round (1000 * time)
          opsPerSecond :: Double
          opsPerSecond = realToFrac count / realToFrac time

      putStrLn $ " - "
              ++ show millis ++ " milliseconds, "
              ++ (formatExponential 1 opsPerSecond) ++ " ops/second"

data BenchItem = BenchItem {
    field1 :: !BS.ByteString
  , field2 :: !BS.ByteString
  , field3 :: !BS.ByteString
  , field4 :: !BS.ByteString
  , field5 :: !BS.ByteString
  }

memDelimited :: BenchItem -> BS.ByteString
memDelimited item =
  nullDelimit [ field1 item, field2 item, field3 item, field4 item, field4 item ]

recallDelimited :: BS.ByteString -> BenchItem
recallDelimited bs =
  case unNullDelimit bs of
    [ f1, f2, f3, f4, f5 ] -> BenchItem f1 f2 f3 f4 f5
    _ -> error "Corrupt BenchItem in recallDelimited!"

memPrefixed255 :: BenchItem -> BS.ByteString
memPrefixed255 item =
  lengthPrefix255 [ field1 item, field2 item, field3 item, field4 item, field5 item ]

recallPrefixed255 :: BS.ByteString -> BenchItem
recallPrefixed255 =
  unLengthPrefix255 BenchItem
                    (field &. field &. field &. field &. field)

memPrefixed65535 :: BenchItem -> BS.ByteString
memPrefixed65535 item =
  lengthPrefix65535 [ field1 item, field2 item, field3 item, field4 item, field5 item ]

recallPrefixed65535 :: BS.ByteString -> BenchItem
recallPrefixed65535 =
  unLengthPrefix65535 BenchItem
                      (field &. field &. field &. field &. field)

formatExponential :: Int -> Double -> String
formatExponential precision d =
  show (roundToPrec sig10) ++ "e" ++ show exp10Int
  where
    roundToPrec d' =
      let p = 10 ^^ precision
          n :: Integer
          n = round $ d' * p
      in (realToFrac n) / p

    sigRadix = significand d
    expRadix = exponent d
    radix = realToFrac $ floatRadix d

    exp10 = realToFrac expRadix * log radix / log 10

    exp10Int :: Int
    exp10Int = floor exp10

    exp10Frac = exp10 - realToFrac exp10Int

    sig10 = sigRadix * (10 ** exp10Frac)

newtype TestData = TestData BS.ByteString
  deriving (Eq, Ord, Show, NFData)

instance Memorizable TestData where
  memorize (TestData bytes) = bytes
  recall = TestData

testDataKey :: TestData -> BS.ByteString
testDataKey (TestData bytes) = bytes
