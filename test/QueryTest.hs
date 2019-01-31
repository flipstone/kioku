module QueryTest where

import            Control.Monad (void)
import qualified  Data.ByteString as BS
import            Test.Tasty (TestTree, testGroup)
import            Test.Tasty.HUnit (testCase, assertEqual)

import            Database.Kioku

newtype TestData = TestData BS.ByteString
  deriving (Eq, Ord, Show)

instance Memorizable TestData where
  memorize (TestData bytes) = bytes
  recall = TestData

testDataKey :: TestData -> BS.ByteString
testDataKey (TestData bytes) = bytes

copyTestData :: TestData -> TestData
copyTestData (TestData bytes) =
  TestData (seq newBytes newBytes)
    where
      newBytes = BS.copy bytes

test_queries :: TestTree
test_queries =
  testGroup
    "Queries"
    [ testCase "keyExactIn finds keys when TrieIndex breaks differently than query input" $ do
        let testData = TestCase [ TestData "USNTI", TestData "USNYC" ]
                                [ TestData "USNYC"]
                                [ TestData "USATL" , TestData "USNYC" ]

        withKiokuDB defaultKiokuPath $ \db -> do
          void $ createDataSet "kioku_tests" (dataSet testData) db
          createIndex "kioku_tests" "kioku_tests.index" testDataKey db

          -- the copyTestData and seqs involved here were an attempt to work around the
          -- "too many pending signals" errors on the thought that lazy evaluation was
          -- leading to attempts to read from the mmapped kioku database strings after
          -- they had been closed at the end of withKiokuDB
          results <- fmap copyTestData <$> query "kioku_tests.index" (keyExactIn $ testDataKey <$> input testData) db
          let eval x = seq x x
          assertEqual "Query failed" (expected testData) (fmap eval results)
    ]

data TestCase =
  TestCase { input    :: [TestData]
           , expected :: [TestData]
           , dataSet  :: [TestData]
           }
