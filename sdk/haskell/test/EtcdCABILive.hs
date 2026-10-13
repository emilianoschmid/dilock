{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Concurrent (forkIO, newEmptyMVar, putMVar, takeMVar, threadDelay, throwTo)
import Control.Exception
  ( AsyncException (ThreadKilled)
  , SomeException
  , fromException
  , try
  )
import Control.Monad (unless)
import qualified Data.Text as T
import Dilock.Etcd.CABI
import System.Environment (lookupEnv)

main :: IO ()
main = do
  endpointValue <- required "DILOCK_ETCD_ENDPOINT"
  namespace <- T.pack <$> required "DILOCK_ETCD_NAMESPACE"
  epoch <- T.pack <$> required "DILOCK_ETCD_EPOCH"
  let options = (defaultEtcdOptions epoch)
        { endpoint = T.pack endpointValue
        , nameSpace = namespace
        , defaultWaitMilliseconds = 1500
        , defaultLeaseMilliseconds = 3000
        , rpcTimeoutMilliseconds = 800
        , cleanupTimeoutMilliseconds = 1500
        , autoRenew = True
        }
      resource = "haskell-c-abi-live/cancel-lease-fence"
      acquirePolicy = AcquireOptions 5000 3000
  withEtcdProvider options $ \provider -> do
    withEtcdLock provider resource $ \ownerLock ->
      withEtcdLock provider resource $ \waiterLock -> do
        (ownerFence, ownerOutcome) <- withEtcdLease ownerLock acquirePolicy recordCleanup $ \owner -> do
          ownerIsValid <- etcdLeaseValid owner
          requireIO ownerIsValid "new owner is not valid"
          ttl <- etcdLeaseEffectiveTtlMilliseconds owner
          requireIO (ttl >= 3000) "etcd server-granted TTL is below request"
          fence <- etcdLeaseFence owner
          requireIO (fst fence == epoch && snd fence > 0) "owner fence is missing or has the wrong epoch"

          timeoutResult <- try (acquireEtcd waiterLock (AcquireOptions 150 3000))
            :: IO (Either EtcdError EtcdLease)
          case timeoutResult of
            Left failure -> requireIO (nativeStatus failure == 4) "wait timeout was not preserved as status 4"
            Right lease -> do
              _ <- releaseEtcdLease lease
              fail "a second owner acquired while the etcd lock was held"

          started <- newEmptyMVar
          completed <- newEmptyMVar
          waiter <- forkIO $ do
            putMVar started ()
            result <- try (acquireEtcd waiterLock acquirePolicy)
            putMVar completed (result :: Either SomeException EtcdLease)
          takeMVar started
          threadDelay 400000
          throwTo waiter ThreadKilled
          cancelled <- takeMVar completed
          case cancelled of
            Left failure -> requireIO (fromException failure == Just ThreadKilled)
              "Haskell async cancellation did not propagate after native cancellation cleanup"
            Right lease -> do
              _ <- releaseEtcdLease lease
              fail "cancelled queued acquisition unexpectedly returned a lease"

          ownerStillValid <- etcdLeaseValid owner
          requireIO ownerStillValid "owner lost validity during queue/cancellation test"
          pure fence
        require (ownerOutcome == Released) "scope did not report acknowledged owner release"

        (successorFence, successorOutcome) <- withEtcdLease waiterLock acquirePolicy recordCleanup $ \successor -> do
          successorIsValid <- etcdLeaseValid successor
          requireIO successorIsValid "successor is not valid after cancellation cleanup"
          etcdLeaseFence successor
        require (successorOutcome == Released) "successor release was not acknowledged"
        require (fst successorFence == epoch && snd successorFence > snd ownerFence)
          "successor create_revision did not advance in the same epoch"

        let exceptionResource = "haskell-c-abi-live/exception-cleanup"
        withEtcdLock provider exceptionResource $ \firstLock ->
          withEtcdLock provider exceptionResource $ \secondLock -> do
            cleaned <- newEmptyMVar
            let intentional = userError "intentional live work failure"
            workResult <- try $ withEtcdLease firstLock acquirePolicy
              (\outcome -> putMVar cleaned outcome)
              (\_ -> ioError intentional)
            case workResult of
              Left (_ :: IOError) -> pure ()
              Right (_ :: (), _) -> fail "work exception was swallowed by withEtcdLease"
            observed <- takeMVar cleaned
            require (observed == Released) "exception path did not report acknowledged cleanup"
            (_, nextOutcome) <- withEtcdLease secondLock acquirePolicy recordCleanup (\_ -> pure ())
            require (nextOutcome == Released) "successor could not acquire after exception cleanup"
  putStrLn "PASS: Haskell C ABI etcd live lease, auto-renew, queued cancellation, fence, release and exception cleanup"

recordCleanup :: EtcdReleaseOutcome -> IO ()
recordCleanup _ = pure ()

required :: String -> IO String
required key = do
  value <- lookupEnv key
  maybe (fail (key <> " must be set to an explicitly disposable local etcd test cluster")) pure value

require :: Bool -> String -> IO ()
require condition message = unless condition (fail message)

requireIO :: Bool -> String -> IO ()
requireIO = require
