{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Exception (bracket)
import qualified Data.Text as T
import Database.Redis
  ( ConnectInfo (connectHost)
  , connect
  , defaultConnectInfo
  , disconnect
  )
import Dilock.Redis
  ( ReleaseOutcome (..)
  , defaultAcquireOptions
  , lock
  , provider
  , withLease
  )
import System.Environment (lookupEnv)

main :: IO ()
main = do
  redisHost <- env "DILOCK_REDIS_HOST" "127.0.0.1"
  namespace <- T.pack <$> env "DILOCK_NAMESPACE" "reports"
  epoch <- T.pack <$> env "DILOCK_COORDINATION_EPOCH" "local-redis-epoch"
  bracket (connect defaultConnectInfo {connectHost = redisHost}) disconnect $ \connection -> do
    redisProvider <- provider connection namespace epoch
    handle <- lock redisProvider "daily"
    (_value, outcome) <- withLease handle defaultAcquireOptions observeCleanup $ \_lease -> do
      putStrLn "Redis lease acquired; running short protected work"
    print outcome
    case outcome of
      Released -> pure ()
      AlreadyNotOwned -> fail "Redis lease was no longer owned at release"
      ReleaseUnknown -> fail "Redis release outcome is unknown"

observeCleanup :: ReleaseOutcome -> IO ()
observeCleanup outcome = putStrLn ("Redis cleanup after work failure: " <> show outcome)

env :: String -> String -> IO String
env name fallback = maybe fallback id <$> lookupEnv name
