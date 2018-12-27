module Main where

import            Control.Exception
import qualified  Data.ByteString as BS
import            Data.Time.Clock.POSIX
import            System.IO

import            Database.Kioku.Memorizable

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
  show (roundToPrec sig10) ++ "e" ++ show (exp10Int :: Int)
  where
    roundToPrec d' =
      let p = 10 ^^ precision
          n :: Int
          n = round $ d' * p
      in (realToFrac n) / p

    sigRadix = significand d
    expRadix = exponent d
    radix = realToFrac $ floatRadix d

    exp10 = realToFrac expRadix * log radix / log 10
    exp10Int = floor exp10
    exp10Frac = exp10 - realToFrac exp10Int

    sig10 = sigRadix * (10 ** exp10Frac)

