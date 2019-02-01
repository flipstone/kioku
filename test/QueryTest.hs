module QueryTest where

import            Control.Monad (void)
import qualified  Data.ByteString as BS
import            Test.Tasty (TestTree, testGroup)
import            Test.Tasty.HUnit (testCase, assertEqual)

import            Database.Kioku

--import Debug.Trace

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
        let test1 = TestCase [ TestData "USNTI", TestData "USNYC" ]
                             [ TestData "USNYC"]

        let test2    = TestCase [ TestData "USATL" ]
                                [ TestData "USATL" ]

       --     test3 = TestCase [ TestData "USNYC", TestData "USNY" ]
       --                      [ TestData "USNYC", TestData "USNY" ]


        withKiokuDB defaultKiokuPath $ \db -> do
          void $ createDataSet "kioku_tests" testData db
          createIndex "kioku_tests" "kioku_tests.index" testDataKey db

          --trace "one" pure ()
          -- the copyTestData and seqs involved here were an attempt to work around the
          -- "too many pending signals" errors on the thought that lazy evaluation was
          -- leading to attempts to read from the mmapped kioku database strings after
          -- they had been closed at the end of withKiokuDB
          results1 <- fmap copyTestData <$> query "kioku_tests.index" (keyExactIn $ testDataKey <$> input test1) db
          --trace (show results1) pure ()
          let eval x = seq x x
          assertEqual "Query failed" (expected test1) (fmap eval results1)

          results2 <- fmap copyTestData <$> query "kioku_tests.index" (keyExactIn $ testDataKey <$> input test2) db
          assertEqual "Query failed" (expected test2) (fmap eval results2)

       --   results3 <- fmap copyTestData <$> query "kioku_tests.index" (keyExactIn $ testDataKey <$> input test3) db
       --   assertEqual "Query failed" (expected test3) (fmap eval results3)
    ]

testData :: [TestData]
testData =
  [ TestData "USNYC", TestData "USATL", TestData "USNY" ]

data TestCase =
  TestCase { input    :: [TestData]
           , expected :: [TestData]
           }
