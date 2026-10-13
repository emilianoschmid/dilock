{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Concurrent (forkIO, myThreadId, newEmptyMVar, putMVar, readMVar, takeMVar, throwTo)
import Control.Exception (AsyncException (ThreadKilled), SomeException, fromException, mask, try)
import Control.Monad (unless)
import qualified Data.Text as T
import Dilock.Etcd.CABI
import Dilock.Internal.CancellableResult (awaitPublishedResult)

main :: IO ()
main = do
  require (defaultWaitMilliseconds (defaultEtcdOptions "epoch") == 5000)
    "ABI defaults should preserve the 5-second wait budget"
  require (defaultLeaseMilliseconds (defaultEtcdOptions "epoch") == 30000)
    "ABI defaults should preserve the 30-second lease budget"
  require (defaultEtcdAcquireOptions (defaultEtcdOptions "epoch") == AcquireOptions 5000 30000)
    "default acquire policy should derive from provider defaults"
  let customProvider = (defaultEtcdOptions "epoch")
        { defaultWaitMilliseconds = 12000, defaultLeaseMilliseconds = 60000 }
  require (defaultEtcdAcquireOptions customProvider == AcquireOptions 12000 60000)
    "custom acquire policy should follow provider defaults"
  require (nativeStatus (EtcdError 4 "timeout") == 4)
    "native status codes must remain observable"
  require (ReleaseUnknown /= Released)
    "unknown release must not equal a confirmed release"
  testRejectsZeroDefaultWait
  testRejectsOutOfRangeDurations
  testLeaseDurationMinimum
  testTryAcquireUnsupportedWithoutBackend
  testCancellationAfterPublication
  putStrLn "PASS: Haskell etcd C ABI value/status contract"

testRejectsZeroDefaultWait :: IO ()
testRejectsZeroDefaultWait = do
  outcome <- try $ withEtcdProvider
    ((defaultEtcdOptions "epoch") { defaultWaitMilliseconds = 0 })
    (\_ -> pure ())
  case (outcome :: Either EtcdError ()) of
    Left (EtcdError 2 _) -> pure ()
    Left _ -> fail "zero default acquisition wait returned the wrong error"
    Right () -> fail "zero default acquisition wait was accepted"

testRejectsOutOfRangeDurations :: IO ()
testRejectsOutOfRangeDurations = do
  let overLimit = 86400001
      invalidPolicies =
        [ AcquireOptions overLimit 30000
        , AcquireOptions 5000 overLimit
        ]
  mapM_ rejectPolicy invalidPolicies
  rejectProviderOptions ((defaultEtcdOptions "epoch") { rpcTimeoutMilliseconds = overLimit })
  rejectProviderOptions ((defaultEtcdOptions "epoch") { cleanupTimeoutMilliseconds = overLimit })
  rejectProviderOptions ((defaultEtcdOptions "epoch") { safetyMarginMilliseconds = overLimit })
  rejectProviderOptions ((defaultEtcdOptions "epoch") { defaultLeaseMilliseconds = overLimit })
  where
    rejectPolicy policy = do
      result <- try (tryAcquireEtcd (error "duration validation inspected a lock") policy)
      case (result :: Either EtcdError (Maybe EtcdLease)) of
        Left (EtcdError 2 _) -> pure ()
        Left _ -> fail "out-of-range acquisition duration returned the wrong error"
        Right _ -> fail "out-of-range acquisition duration was accepted"

    rejectProviderOptions options = do
      result <- try (withEtcdProvider options (\_ -> pure ()))
      case (result :: Either EtcdError ()) of
        Left (EtcdError 2 _) -> pure ()
        Left _ -> fail "out-of-range provider duration returned the wrong error"
        Right () -> fail "out-of-range provider duration was accepted"

testTryAcquireUnsupportedWithoutBackend :: IO ()
testTryAcquireUnsupportedWithoutBackend = do
  outcome <- try $ tryAcquireEtcd (error "unsupported try inspected its lock") (AcquireOptions 1000 30000)
  case (outcome :: Either EtcdError (Maybe EtcdLease)) of
    Left (EtcdError 9 message)
      | "nonblocking try-acquire" `T.isInfixOf` message -> pure ()
    Left _ -> fail "etcd try-acquire returned the wrong capability error"
    Right _ -> fail "etcd native Lock try-acquire did not fail explicitly"

testLeaseDurationMinimum :: IO ()
testLeaseDurationMinimum = do
  let tooShort = AcquireOptions 1000 99
      minimumLease = AcquireOptions 1000 100
  rejected <- try (tryAcquireEtcd (error "lease validation inspected a lock") tooShort)
  case (rejected :: Either EtcdError (Maybe EtcdLease)) of
    Left (EtcdError 2 _) -> pure ()
    Left _ -> fail "sub-100 ms etcd lease returned the wrong error"
    Right _ -> fail "sub-100 ms etcd lease was accepted"
  acceptedByLocalValidation <- try (tryAcquireEtcd (error "unsupported check inspected a lock") minimumLease)
  case (acceptedByLocalValidation :: Either EtcdError (Maybe EtcdLease)) of
    Left (EtcdError 9 _) -> pure ()
    Left _ -> fail "100 ms etcd lease was rejected by local validation"
    Right _ -> fail "native etcd try-acquire unexpectedly acquired a lease"

  defaultRejected <- try $ withEtcdProvider
    ((defaultEtcdOptions "epoch") { defaultLeaseMilliseconds = 99 })
    (\_ -> pure ())
  case (defaultRejected :: Either EtcdError ()) of
    Left (EtcdError 2 _) -> pure ()
    Left _ -> fail "sub-100 ms default etcd lease returned the wrong error"
    Right () -> fail "sub-100 ms default etcd lease was accepted"

testCancellationAfterPublication :: IO ()
testCancellationAfterPublication = do
  result <- newEmptyMVar
  putMVar result ("native-result" :: String)
  enteredWindow <- newEmptyMVar
  window <- newEmptyMVar
  cleanupObserved <- newEmptyMVar
  caller <- myThreadId
  _ <- forkIO $ do
    takeMVar enteredWindow
    throwTo caller ThreadKilled
  outcome <- try $ mask $ \restore -> do
    let pauseAfterRead action = restore (action >>= \value -> do
          putMVar enteredWindow ()
          _ <- takeMVar window
          pure value)
        cancelAndJoin = readMVar result >>= putMVar cleanupObserved
    awaitPublishedResult pauseAfterRead cancelAndJoin result
  case (outcome :: Either SomeException String) of
    Left failure -> case fromException failure of
      Just ThreadKilled -> pure ()
      _ -> fail "published-result cancellation changed the async exception"
    Right _ -> fail "test did not deliver cancellation in the published-result window"
  joined <- takeMVar cleanupObserved
  require (joined == "native-result") "cancellation cleanup lost an already-published worker result"
  -- The cancelling thread has completed its synchronous throwTo by the time
  -- the target catches ThreadKilled.
  pure ()

require :: Bool -> String -> IO ()
require condition message = unless condition (fail message)
