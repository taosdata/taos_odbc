-- MIT License
-- 
-- Copyright (c) 2022-2023 freemine <freemine@yeah.net>
-- 
-- Permission is hereby granted, free of charge, to any person obtaining a copy
-- of this software and associated documentation files (the "Software"), to deal
-- in the Software without restriction, including without limitation the rights
-- to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
-- copies of the Software, and to permit persons to whom the Software is
-- furnished to do so, subject to the following conditions:
-- 
-- The above copyright notice and this permission notice shall be included in
-- all copies or substantial portions of the Software.
-- 
-- THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
-- IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
-- FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
-- AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
-- LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
-- OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
-- SOFTWARE.


module Main where

import Database.HDBC
import Database.HDBC.ODBC
import Data.Time
import Text.Printf

myAssert :: Monad m => Bool -> String -> m ()
myAssert p s
  | p == True = return ()
  | otherwise = error s

getNextTick :: Maybe String -> IO String
getNextTick Nothing = fmap (formatTime defaultTimeLocale "%Y-%m-%d %H:%M:%S.%3q") $ getZonedTime
getNextTick (Just prev) = do
  t <- fmap (formatTime defaultTimeLocale "%Y-%m-%d %H:%M:%S.%3q") $ getZonedTime
  if t > prev then
    return t
  else
    getNextTick (Just prev)

main :: IO ()
main = do
  putStrLn "Hello, Haskell!"
  conn <- connectODBC "DSN=TAOS_ODBC_DSN;Database=bar"

  v1 <- run conn "drop table if exists haskell" []
  printf "affected rows:%d\n" v1
  myAssert (v1==0) $ ("affected rows is expected to be 0, but got ==" ++ (show v1) ++ "==")

  v2 <- run conn "create table if not exists haskell (ts timestamp, name varchar(20))" []
  printf "affected rows:%d\n" v2
  myAssert (v2==0) $ ("affected rows is expected to be 0, but got ==" ++ (show v2) ++ "==")

  stmt <- prepare conn "insert into haskell (ts, name) values (?, ?)"

  t3 <- getNextTick Nothing
  v3 <- execute stmt [toSql t3, toSql "hello"]
  printf "affected rows:%d\n" v3
  myAssert (v3==1) $ ("affected rows is expected to be 1, but got ==" ++ (show v3) ++ "==")

  t4 <- getNextTick (Just t3)
  v4 <- execute stmt [toSql t4, toSql "中国"]
  printf "affected rows:%d\n" v4
  myAssert (v4==1) $ ("affected rows is expected to be 1, but got ==" ++ (show v4) ++ "==")

  rs <- quickQuery conn "select * from haskell" []
  print $ show rs
  myAssert (rs == [[toSql t3, toSql "hello"],[toSql t4, toSql "中国"]]) "data queried is not as expected"

