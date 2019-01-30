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
    [ testCase "keyExactIn finds everything" $ do
        -- What we've found so far:

        -- During trieLookupMany, MultiKey extracts the common prefix of the
        -- keys being queries, this is called the "subKey".
        --
        -- USNYC is the only US port that starts with 'USN'. This means the Trie
        -- node structure is US -> NYC (Just 2 nodes, with arcs "US" and "NYC")
        --
        -- When we are examining the node with arc NYC, breakCommonPrefix is used
        -- to extract "N" as a common prefix of the subkey "N" and the arc "NCY",
        -- leaving a remainingKey of "" and remainingArc of "YC". The case statement
        -- at the end of trieLookupMany then chooses to ignore this node rather than
        -- continue looking down, which is not the correct search path to take.

        -- Below are several test cases we explored to help find the issue. Some
        -- resulted in a myserious "too many pending signals" error that we suspect
        -- might be related to segfaults, but are not at all sure.

        --let dataSet = [ TestData "USMSX", TestData "USMSY" ] -- too many pending signals
       -- let dataSet = [ TestData "USMSP", TestData "USMSY" ] -- PASSES
        --let dataSet = [ TestData "USMSX", TestData "USNYC" ] -- too many pending signals
        let dataSet = [ TestData "USNTI", TestData "USNYC" ] -- FAILS
        --let dataSet = [ TestData "USM", TestData "USNYC" ] -- too many pending signals
        --let dataSet = [ TestData "USN", TestData "USNYC" ] -- FAILS
        --let dataSet = [ TestData "USBA", TestData "USBAL" ] -- FAILS

        withKiokuDB defaultKiokuPath $ \db -> do
          void $ createDataSet "kioku_tests" allTestData db
          createIndex "kioku_tests" "kioku_tests.index" testDataKey db

          -- the copyTestData and seqs involved here were an attempt to work around the
          -- "too many pending signals" errors on the thought that lazy evaluation was
          -- leading to attempts to read from the mmapped kioku database strings after
          -- they had been closed at the end of withKiokuDB
          results <- fmap copyTestData <$> query "kioku_tests.index" (keyExactIn $ testDataKey <$> dataSet) db
          let eval x = seq x x
          assertEqual "Query failed"  dataSet (fmap eval results)
    ]

allTestData :: [TestData]
allTestData =
  [ TestData "CACAL"
  , TestData "CAEDM"
  , TestData "CAHAL"
  , TestData "CAMTR"
  , TestData "CAREG"
  , TestData "CASAK"
  , TestData "CATOR"
  , TestData "CAVAN"
  , TestData "CAWNP"
  , TestData "USATL"
  , TestData "USBAL"
  , TestData "USBHM"
  , TestData "USBNA"
  , TestData "USBOS"
  , TestData "USBUF"
  , TestData "USCBF"
  , TestData "USCHI"
  , TestData "USCHS"
  , TestData "USCLE"
  , TestData "USCLT"
  , TestData "USCMH"
  , TestData "USCVG"
  , TestData "USDAL"
  , TestData "USDEN"
  , TestData "USDET"
  , TestData "USELP"
  , TestData "USEWR"
  , TestData "USEZA"
  , TestData "USGBO"
  , TestData "USHOU"
  , TestData "USHSV"
  , TestData "USILM"
  , TestData "USIND"
  , TestData "USJAX"
  , TestData "USKCK"
  , TestData "USLAX"
  , TestData "USLGB"
  , TestData "USLRD"
  , TestData "USLUI"
  , TestData "USMEM"
  , TestData "USMES"
  , TestData "USMIA"
  , TestData "USMKC"
  , TestData "USMOB"
  , TestData "USMSP"
  , TestData "USMSY"
  , TestData "USNYC"
  , TestData "USOAK"
  , TestData "USOMA"
  , TestData "USORF"
  , TestData "USPDX"
  , TestData "USPEF"
  , TestData "USPHL"
  , TestData "USPHX"
  , TestData "USPIT"
  , TestData "USPPS"
  , TestData "USPTM"
  , TestData "USRCX"
  , TestData "USSAV"
  , TestData "USSEA"
  , TestData "USSLC"
  , TestData "USSTL"
  , TestData "USTIW"
  , TestData "USTPA"
  , TestData "USXHE"
  ]
