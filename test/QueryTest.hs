{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module QueryTest where

import Control.DeepSeq (NFData, deepseq)
import Control.Monad (void, when)
import Control.Monad.IO.Class (liftIO)
import qualified Data.ByteString.Char8 as BS
import qualified Data.Set as Set
import qualified Hedgehog as HH
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.HUnit (Assertion, HasCallStack, assertFailure, testCase)
import Test.Tasty.Hedgehog (testProperty)

import Database.Kioku

newtype TestData = TestData BS.ByteString
    deriving (Eq, Ord, Show, NFData)

instance Memorizable TestData where
    memorize (TestData bytes) = bytes
    recall = TestData

testDataKey :: TestData -> BS.ByteString
testDataKey (TestData bytes) = bytes

copyTestData :: TestData -> TestData
copyTestData (TestData bytes) = TestData (BS.copy bytes)

test_queries :: TestTree
test_queries =
    testGroup
        "keyExactIn query"
        [ testProperty "works for random cases" $ HH.property $ do
            dataset <- HH.forAll datasetGen
            query' <- HH.forAll (queryGen dataset)

            results <- liftIO . withKiokuDB defaultKiokuPath $ \db -> do
                void $ createDataSet "kioku_tests" dataset db
                createIndex "kioku_tests" "kioku_tests.index" testDataKey db
                mmappedResults <- query "kioku_tests.index" (keyExactIn $ map testDataKey query') db

                let copiedResults = copyTestData <$> mmappedResults
                deepseq copiedResults (pure copiedResults)

            let expected = Set.intersection (Set.fromList dataset) (Set.fromList query')

            expected HH.=== Set.fromList results
        , testCase "finds a single result" $
            runQueryTest $
                QueryTest
                    { queryTestData = [TestData "USNYC", TestData "USATL", TestData "USNY"]
                    , queryTestQuery = keyExactIn $ testDataKey <$> [TestData "USATL"]
                    , queryTestExpected = [TestData "USATL"]
                    }
        , testCase "finds multiple results" $
            runQueryTest $
                QueryTest
                    { queryTestData = [TestData "USNYC", TestData "USATL", TestData "USNY"]
                    , queryTestQuery = keyExactIn $ testDataKey <$> [TestData "USATL", TestData "USNY"]
                    , queryTestExpected = [TestData "USATL", TestData "USNY"]
                    }
        , testCase "finds keys when the MultiKey contains a node that is a prefix of it, but the Trie does not" $
            runQueryTest $
                QueryTest
                    { queryTestData = [TestData "USNYC", TestData "USATL"]
                    , queryTestQuery = keyExactIn $ testDataKey <$> [TestData "USNYC", TestData "USNY"]
                    , queryTestExpected = [TestData "USNYC"]
                    }
        , testCase "finds a key when the Trie contains a node that is prefix of it" $
            runQueryTest $
                QueryTest
                    { queryTestData = [TestData "USNYC", TestData "USATL", TestData "USN"]
                    , queryTestQuery = keyExactIn $ testDataKey <$> [TestData "USNYC"]
                    , queryTestExpected = [TestData "USNYC"]
                    }
        , testCase "does not find a key that is a prefix of the keys being queried, but was not itself queried" $
            runQueryTest $
                QueryTest
                    { queryTestData = [TestData "USNY", TestData "USATL", TestData "USN"]
                    , queryTestQuery = keyExactIn $ testDataKey <$> [TestData "USNY", TestData "USNT"]
                    , queryTestExpected = [TestData "USNY"]
                    }
        , testCase "finds a key that is a prefix of other keys being queried when that key is also queried itself" $
            runQueryTest $
                QueryTest
                    { queryTestData = [TestData "USNY", TestData "USATL", TestData "USN"]
                    , queryTestQuery = keyExactIn $ testDataKey <$> [TestData "USNY", TestData "USN"]
                    , queryTestExpected = [TestData "USNY", TestData "USN"]
                    }
        , testCase "finds multiple results for keyAllHitsAlong with single-character nodes" $
            runQueryTest $
                QueryTest
                    { queryTestData = [TestData "1", TestData "15", TestData "16", TestData "17", TestData "165"]
                    , queryTestQuery = keyAllHitsAlong . testDataKey $ TestData "168"
                    , queryTestExpected = [TestData "1", TestData "16"]
                    }
        , testCase "finds multiple results for keyAllHitsAlong with multi-character nodes" $
            runQueryTest $
                QueryTest
                    { queryTestData = [TestData "101", TestData "1010", TestData "101005", TestData "101006", TestData "101007", TestData "1010065", TestData "1010068"]
                    , queryTestQuery = keyAllHitsAlong . testDataKey $ TestData "1010068"
                    , queryTestExpected = [TestData "101", TestData "1010", TestData "101006", TestData "1010068"]
                    }
        , testCase "finds result for keyPrefix with multi-character nodes" $
            runQueryTest $
                QueryTest
                    { queryTestData = [TestData "101", TestData "1015", TestData "1016", TestData "1017", TestData "10165"]
                    , queryTestQuery = keyPrefix . testDataKey $ TestData "1016"
                    , queryTestExpected = [TestData "1016", TestData "10165"]
                    }
        , testCase "finds results for keyExact with multi-character nodes" $
            runQueryTest $
                QueryTest
                    { queryTestData = [TestData "101", TestData "1015", TestData "1016", TestData "1017", TestData "10165"]
                    , queryTestQuery = keyExact . testDataKey $ TestData "1016"
                    , queryTestExpected = [TestData "1016"]
                    }
        ]

data QueryTest = QueryTest
    { queryTestData :: [TestData]
    , queryTestQuery :: KiokuQuery
    , queryTestExpected :: [TestData]
    }

runQueryTest :: QueryTest -> IO ()
runQueryTest test =
    withKiokuDB defaultKiokuPath $ \db -> do
        void $ createDataSet "kioku_tests" (queryTestData test) db
        createIndex "kioku_tests" "kioku_tests.index" testDataKey db
        results <- query "kioku_tests.index" (queryTestQuery test) db
        assertSameData (queryTestExpected test) results

assertSameData :: HasCallStack => [TestData] -> [TestData] -> Assertion
assertSameData expectedData actualData =
    when (Set.fromList expectedData /= Set.fromList actualData) $
        let msg =
                concat
                    [ "Expected result data: "
                    , show expectedData
                    , ", but got: "
                    , show actualData
                    ]
         in -- assertFailure includes its own deepseq, but for some reason we also
            -- need to have one here. Without it we get a "too many pending signals"
            -- error, which indicates a segfault occerred. We narrowed down the cause
            -- to using bytes from the actualData to generate the error message after
            -- the kioku database has been closed (because kioku goes out of its way to
            -- not copy bytes whenever necessary). deepseq here forces the message to
            -- be evaluated before assertFailure is called.
            deepseq msg (assertFailure msg)

datasetGen :: HH.Gen [TestData]
datasetGen = Gen.list (Range.linear 100 200) (testDataGen "ABCD")

testDataGen :: String -> HH.Gen TestData
testDataGen seedChars = TestData <$> Gen.utf8 (Range.linear 1 20) (Gen.element seedChars)

queryGen :: [TestData] -> HH.Gen [TestData]
queryGen dataSet = do
    contained <- Gen.list (Range.linear 50 100) (Gen.element dataSet)
    noise <- Gen.list (Range.linear 10 30) (testDataGen "AEFG")
    pure $ contained ++ noise
