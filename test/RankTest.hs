module RankTest where

import            Control.Monad.IO.Class (liftIO)
import            Data.List (sort)
import            Data.Word (Word8)

import qualified  Hedgehog as HH
import qualified  Hedgehog.Gen as Gen
import qualified  Hedgehog.Range as Range
import            Test.Tasty.Hedgehog
import            Test.Tasty (TestTree, testGroup)


import            Database.Kioku

test_rank :: TestTree
test_rank =
  testGroup
    "correct order"
    [ testProperty "root of heap is min" $ HH.property $ do
        cities <- HH.forAll citiesGen
        let m = minimum . take 10 . reverse $ sort cities
        heap <- liftIO $ newRankHeap 10

        let action = foldr (\x h -> h >>= heapInsert x) (pure heap) cities
        heap' <- liftIO action
        result <- liftIO $ heapMin heap'

        result HH.=== Just m

    , testProperty "picks the top 10" $ HH.property $ do
        cities <- HH.forAll citiesGen
        let topTen = take 10 . reverse $ sort cities

        heap <- liftIO $ newRankHeap 10
        let action = foldr (\x h -> h >>= heapInsert x) (pure heap) cities
        heap' <- liftIO action
        result <- liftIO $ heapToList heap'

        result HH.=== topTen

    , testProperty "merge works" $ HH.property $ do
        cities1 <- HH.forAll citiesGen
        heap1 <- liftIO $ newRankHeap 10
        heap1' <- liftIO $ foldr (\x h -> h >>= heapInsert x) (pure heap1) cities1

        cities2 <- HH.forAll citiesGen
        heap2 <- liftIO $ newRankHeap 10
        heap2' <- liftIO $ foldr (\x h -> h >>= heapInsert x) (pure heap2) cities2

        merged <- liftIO $ heapMerge heap1' heap2'
        result <- liftIO $ heapToList merged
        let top10 = take 10 . reverse . sort $ cities1 ++ cities2

        result HH.=== top10
    ]

newtype City = City { cityRank :: Word8 }
  deriving (Eq, Ord, Show)

instance HasRank City where
  getRank = cityRank

cityGen :: HH.Gen City
cityGen = City <$> Gen.word8 (Range.linear 0 255)

citiesGen :: HH.Gen [City]
citiesGen = Gen.list (Range.singleton 1000) cityGen
