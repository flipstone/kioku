{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE RankNTypes #-}

module QueryTest where

import Control.DeepSeq (NFData, deepseq)
import Control.Monad (void, when)
import Control.Monad.IO.Class (liftIO)
import qualified Data.ByteString.Char8 as BS
import Data.Foldable (for_)
import qualified Data.Set as Set
import qualified Hedgehog as HH
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range
import System.IO.Temp (withSystemTempDirectory)
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

testNamespace :: KiokuNamespace
testNamespace = KiokuNamespace "test_namespace"

data Backend = Backend
  { backendName :: String
  , withBackendDB :: forall a. (KiokuDB -> IO a) -> IO a
  }

-- Each file backend database gets its own temporary directory so that test
-- runs are isolated from each other and never touch a real Kioku database in
-- the working directory. The temp directory is removed only after
-- 'withKiokuDB' has closed the database, so its mmapped buffers are already
-- unmapped by then.
backends :: [Backend]
backends =
  [ Backend "file backend" $ \action ->
      withSystemTempDirectory "kioku-test" $ \dir ->
        withKiokuDB dir action
  , Backend "memory backend" withInMemoryKiokuDB
  ]

test_queries :: TestTree
test_queries =
  testGroup
    "queries"
    (map backendQueryTests backends ++ [backendEquivalenceTest])

-- Runs the same randomly generated dataset and query through both storage
-- backends and requires identical results, so the backends cannot drift.
backendEquivalenceTest :: TestTree
backendEquivalenceTest =
  testProperty "file and memory backends give identical results" $ HH.property $ do
    dataset <- HH.forAll datasetGen
    query' <- HH.forAll (queryGen dataset)

    let
      kQuery = keyExactIn $ map testDataKey query'
      runOn backend = liftIO $ withBackendDB backend (runBackendQuery dataset kQuery)

    resultsPerBackend <- traverse runOn backends

    case map Set.fromList resultsPerBackend of
      (firstResults : restResults) -> do
        for_ restResults (firstResults HH.===)
      [] -> pure ()

backendQueryTests :: Backend -> TestTree
backendQueryTests backend =
  testGroup
    (backendName backend)
    [ testProperty "keyExactIn works for random cases" $ HH.property $ do
        dataset <- HH.forAll datasetGen
        query' <- HH.forAll (queryGen dataset)

        results <-
          liftIO $
            withBackendDB backend $
              runBackendQuery dataset (keyExactIn $ map testDataKey query')

        let
          expected = Set.intersection (Set.fromList dataset) (Set.fromList query')

        expected HH.=== Set.fromList results
    , testCase "finds a single result" $
        runQueryTest backend $
          QueryTest
            { queryTestData = [TestData "USNYC", TestData "USATL", TestData "USNY"]
            , queryTestQuery = keyExactIn $ testDataKey <$> [TestData "USATL"]
            , queryTestExpected = [TestData "USATL"]
            }
    , testCase "finds multiple results" $
        runQueryTest backend $
          QueryTest
            { queryTestData = [TestData "USNYC", TestData "USATL", TestData "USNY"]
            , queryTestQuery = keyExactIn $ testDataKey <$> [TestData "USATL", TestData "USNY"]
            , queryTestExpected = [TestData "USATL", TestData "USNY"]
            }
    , testCase "finds keys when the MultiKey contains a node that is a prefix of it, but the Trie does not" $
        runQueryTest backend $
          QueryTest
            { queryTestData = [TestData "USNYC", TestData "USATL"]
            , queryTestQuery = keyExactIn $ testDataKey <$> [TestData "USNYC", TestData "USNY"]
            , queryTestExpected = [TestData "USNYC"]
            }
    , testCase "finds a key when the Trie contains a node that is prefix of it" $
        runQueryTest backend $
          QueryTest
            { queryTestData = [TestData "USNYC", TestData "USATL", TestData "USN"]
            , queryTestQuery = keyExactIn $ testDataKey <$> [TestData "USNYC"]
            , queryTestExpected = [TestData "USNYC"]
            }
    , testCase "does not find a key that is a prefix of the keys being queried, but was not itself queried" $
        runQueryTest backend $
          QueryTest
            { queryTestData = [TestData "USNY", TestData "USATL", TestData "USN"]
            , queryTestQuery = keyExactIn $ testDataKey <$> [TestData "USNY", TestData "USNT"]
            , queryTestExpected = [TestData "USNY"]
            }
    , testCase "finds a key that is a prefix of other keys being queried when that key is also queried itself" $
        runQueryTest backend $
          QueryTest
            { queryTestData = [TestData "USNY", TestData "USATL", TestData "USN"]
            , queryTestQuery = keyExactIn $ testDataKey <$> [TestData "USNY", TestData "USN"]
            , queryTestExpected = [TestData "USNY", TestData "USN"]
            }
    , testCase "finds multiple results for keyAllHitsAlong with single-character nodes" $
        runQueryTest backend $
          QueryTest
            { queryTestData = [TestData "1", TestData "15", TestData "16", TestData "17", TestData "165"]
            , queryTestQuery = keyAllHitsAlong . testDataKey $ TestData "168"
            , queryTestExpected = [TestData "1", TestData "16"]
            }
    , testCase "finds multiple results for keyAllHitsAlong with multi-character nodes" $
        runQueryTest backend $
          QueryTest
            { queryTestData = [TestData "101", TestData "1010", TestData "101005", TestData "101006", TestData "101007", TestData "1010065", TestData "1010068"]
            , queryTestQuery = keyAllHitsAlong . testDataKey $ TestData "1010068"
            , queryTestExpected = [TestData "101", TestData "1010", TestData "101006", TestData "1010068"]
            }
    , testCase "finds result for keyPrefix with multi-character nodes" $
        runQueryTest backend $
          QueryTest
            { queryTestData = [TestData "101", TestData "1015", TestData "1016", TestData "1017", TestData "10165"]
            , queryTestQuery = keyPrefix . testDataKey $ TestData "1016"
            , queryTestExpected = [TestData "1016", TestData "10165"]
            }
    , testCase "finds results for keyExact with multi-character nodes" $
        runQueryTest backend $
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

runQueryTest :: Backend -> QueryTest -> IO ()
runQueryTest backend test =
  withBackendDB backend $ \db -> do
    void $ createDataSet testNamespace "kioku_tests" (queryTestData test) db
    createIndex testNamespace "kioku_tests" "kioku_tests.index" testDataKey db
    results <- query testNamespace "kioku_tests.index" (queryTestQuery test) db
    assertSameData (queryTestExpected test) results

runBackendQuery :: [TestData] -> KiokuQuery -> KiokuDB -> IO [TestData]
runBackendQuery dataset kQuery db = do
  void $ createDataSet testNamespace "kioku_tests" dataset db
  createIndex testNamespace "kioku_tests" "kioku_tests.index" testDataKey db
  mmappedResults <- query testNamespace "kioku_tests.index" kQuery db

  let
    copiedResults = copyTestData <$> mmappedResults
  deepseq copiedResults (pure copiedResults)

assertSameData :: HasCallStack => [TestData] -> [TestData] -> Assertion
assertSameData expectedData actualData =
  when (Set.fromList expectedData /= Set.fromList actualData) $
    let
      msg =
        concat
          [ "Expected result data: "
          , show expectedData
          , ", but got: "
          , show actualData
          ]
    in
      -- assertFailure includes its own deepseq, but for some reason we also
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
