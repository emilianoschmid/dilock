{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Monad (unless)
import qualified Data.Text as T
import Dilock.Etcd.CABI
  ( EtcdOptions (..)
  , EtcdReleaseOutcome (..)
  , defaultEtcdAcquireOptions
  , defaultEtcdOptions
  , etcdLeaseFence
  , withEtcdLease
  , withEtcdLock
  , withEtcdProvider
  )
import System.Environment (lookupEnv)

main :: IO ()
main = do
  endpointValue <- env "DILOCK_ETCD_ENDPOINT" "127.0.0.1:2379"
  namespace <- T.pack <$> env "DILOCK_NAMESPACE" "reports"
  epoch <- T.pack <$> env "DILOCK_COORDINATION_EPOCH" "local-etcd-epoch"
  let options = (defaultEtcdOptions epoch)
        { endpoint = T.pack endpointValue
        , nameSpace = namespace
        , autoRenew = True
        }
      policy = defaultEtcdAcquireOptions options
  withEtcdProvider options $ \etcdProvider ->
    withEtcdLock etcdProvider "daily" $ \handle -> do
      (fence, outcome) <- withEtcdLease handle policy observeCleanup $ \lease -> do
        (fenceEpoch, revision) <- etcdLeaseFence lease
        unless (fenceEpoch == epoch && revision > 0) $ fail "invalid etcd fence"
        putStrLn "etcd lease acquired; running short protected work"
        pure (fenceEpoch, revision)
      print (fence, outcome)
      case outcome of
        Released -> pure ()
        AlreadyNotOwned -> fail "etcd lease was no longer owned at release"
        ReleaseUnknown -> fail "etcd release outcome is unknown"

observeCleanup :: EtcdReleaseOutcome -> IO ()
observeCleanup outcome = putStrLn ("etcd cleanup after work failure: " <> show outcome)

env :: String -> String -> IO String
env name fallback = maybe fallback id <$> lookupEnv name
