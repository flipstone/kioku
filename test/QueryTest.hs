module QueryTest where

import            Control.Monad (void, when)
import            Control.DeepSeq (deepseq)
import qualified  Data.ByteString.Char8 as BS
import qualified  Data.Set as Set
import            Test.Tasty (TestTree, testGroup)
import            Test.Tasty.HUnit (Assertion, HasCallStack, testCase, assertFailure)

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
    [ testCase "keyExactIn can find a single result" $
        runQueryTest $
          QueryTest
            { queryTestData     = [ TestData "USNYC", TestData "USATL", TestData "USNY" ]
            , queryTestQuery    = keyExactIn $ testDataKey <$> [ TestData "USATL" ]
            , queryTestExpected = [ TestData "USATL" ]
            }

    , testCase "keyExactIn can find multiple results" $
        runQueryTest $
          QueryTest
            { queryTestData     = [ TestData "USNYC", TestData "USATL", TestData "USNY" ]
            , queryTestQuery    = keyExactIn [ testDataKey $ TestData "USATL" ]
            , queryTestExpected = [ TestData "USATL" ]
            }

    , testCase "keyExactIn finds keys when TrieIndex breaks differently than query input" $
        runQueryTest $
          QueryTest
            { queryTestData     = [ TestData "USNYC", TestData "USATL", TestData "USNY" ]
            , queryTestQuery    = keyExactIn $ testDataKey <$> [ TestData "USNYC", TestData "USNY" ]
            , queryTestExpected = [ TestData "USNYC", TestData "USNY" ]
            }
    ]

data QueryTest =
  QueryTest
    { queryTestData      :: [TestData]
    , queryTestQuery     :: KiokuQuery
    , queryTestExpected  :: [TestData]
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
    let msg = concat [ "Expected result data: "
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

