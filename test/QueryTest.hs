module QueryTest where

import            Control.Monad (void)
import qualified  Data.ByteString as BS
import            Data.List ((\\))
import            Test.Tasty (TestTree, testGroup)
import            Test.Tasty.HUnit (testCase, assertBool)

import            Database.Kioku

newtype TestData = TestData BS.ByteString
  deriving (Eq, Ord, Show)

instance Memorizable TestData where
  memorize (TestData bytes) = bytes
  recall = TestData

testDataKey :: TestData -> BS.ByteString
testDataKey (TestData bytes) = bytes

test_queries :: TestTree
test_queries =
  testGroup
    "Queries"
    [ testCase "keyExactIn finds keys when TrieIndex breaks differently than query input" $ do
        let test1 = TestCase [ TestData "USNTI", TestData "USNYC" ]
                             [ TestData "USNYC"]

        let test2    = TestCase [ TestData "USATL" ]
                                [ TestData "USATL" ]

            test3 = TestCase [ TestData "USNYC", TestData "USNY" ]
                             [ TestData "USNYC", TestData "USNY" ]


        withKiokuDB defaultKiokuPath $ \db -> do
          void $ createDataSet "kioku_tests" testData db
          createIndex "kioku_tests" "kioku_tests.index" testDataKey db

          results1 <- query "kioku_tests.index" (keyExactIn $ testDataKey <$> input test1) db
          assertBool "Query failed" (equals (expected test1) results1)

          results2 <- query "kioku_tests.index" (keyExactIn $ testDataKey <$> input test2) db
          assertBool "Query failed" (equals (expected test2) results2)

          results3 <- query "kioku_tests.index" (keyExactIn $ testDataKey <$> input test3) db
          assertBool "Query failed" (equals (expected test3) results3)
    ]

equals :: Eq a => [a] -> [a] -> Bool
equals x y = null (x \\ y) && null (y \\ x)

testData :: [TestData]
testData =
  [ TestData "USNYC", TestData "USATL", TestData "USNY" ]

data TestCase =
  TestCase { input    :: [TestData]
           , expected :: [TestData]
           }
