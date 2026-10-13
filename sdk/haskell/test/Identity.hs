{-# LANGUAGE OverloadedStrings #-}
import qualified Data.ByteString.Char8 as B
import Control.Concurrent (forkIO, newEmptyMVar, putMVar, takeMVar, throwTo, threadDelay)
import Control.Exception (AsyncException(ThreadKilled), IOException, SomeException, bracket, finally, fromException, throwIO, try)
import Control.Concurrent.MVar (newMVar)
import Data.IORef (modifyIORef', newIORef, readIORef)
import Dilock.Internal.Exception (onExceptionPreserving, trySynchronous, withObservedBracket)
import Dilock.Internal.Backoff (pollDelayMicroseconds)
import Dilock.Internal.Duration (validLeaseDuration, validWaitDuration)
import Dilock.Internal.Metadata (validRedisNamespaceMetadata)
import Dilock.Internal.Release (releaseOnce, runReleaseAttempt)
import Dilock.Internal.Renewal (RenewalReply(..), classifyRenewalReply)
import Database.Redis (Redis, Reply(..), ConnectInfo(..), Connection, connect, defaultConnectInfo, disconnect, runRedis, sendRequest)
import Dilock.Redis (AcquireOptions(..), DilockError(..), Lease, Provider, ReleaseOutcome(..), acquire, defaultAcquireOptions, lock, provider, release, resourceId, tryAcquire, withLease)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BC
import qualified Data.Text.Encoding as TE
import Data.Maybe (isJust, isNothing)
import GHC.Clock (getMonotonicTimeNSec)
import System.Environment (lookupEnv)

main :: IO ()
main = do
  assert (resourceId "default" "camera/123/reindex" == Right (B.pack "e0cdd484a99b6aee4162335939d2a2d128a490c097069fd85b2fc1e540a940b6")) "draft 1 identity vector"
  assert (defaultAcquireOptions == AcquireOptions 5000 30000) "default acquisition policy"
  assert (validWaitDuration 1 && validWaitDuration 86400000 && not (validWaitDuration 0) && not (validWaitDuration 86400001))
    "acquisition wait duration is bounded to 1..86400000 ms"
  assert (validLeaseDuration 100 && validLeaseDuration 86400000 && not (validLeaseDuration 99) && not (validLeaseDuration 86400001))
    "lease duration is bounded to 100..86400000 ms for both acquire and tryAcquire"
  let metadata major = "{\"protocol_major\":" <> major <> ",\"identity_scheme\":\"sha256-framed-utf8-v1\",\"profile\":\"redis-single-authority-v1\",\"coordination_epoch\":\"epoch-a\",\"fixed_lease_ms\":null}"
  assert (validRedisNamespaceMetadata "epoch-a" (metadata "1")) "accept integral protocol major"
  assert (not (validRedisNamespaceMetadata "epoch-a" (metadata "1.0"))) "reject decimal protocol major with integral value"
  assert (not (validRedisNamespaceMetadata "epoch-a" (metadata "1.5"))) "reject fractional protocol major"
  assert (not (validRedisNamespaceMetadata "epoch-a" (metadata "\"1\""))) "reject textual protocol major"
  assert (not (validRedisNamespaceMetadata "epoch-a" (metadata "true"))) "reject boolean protocol major"
  assert (not (validRedisNamespaceMetadata "epoch-a" (metadata "4294967297"))) "reject out-of-range protocol major"
  assert (pollDelayMicroseconds 0 maxBound == 10000 && pollDelayMicroseconds 25000 maxBound == 35000)
    "contention poll jitter stays within 10-35 ms"
  assert (pollDelayMicroseconds 0 maxBound /= pollDelayMicroseconds 1 maxBound)
    "different random samples produce different contention delays"
  assert (pollDelayMicroseconds 0 5000 == 5000 && pollDelayMicroseconds 25000 1 == 1 && pollDelayMicroseconds 0 0 == 0)
    "contention poll never sleeps beyond its remaining acquisition budget"
  assert (either (const True) (const False) (resourceId "Bad" "x")) "reject invalid namespace"
  assert (either (const True) (const False) (resourceId "default" "")) "reject empty resource"
  synchronous <- trySynchronous (throwIO (userError "backend failure"))
  assert (either (const True) (const False) (synchronous :: Either SomeException ())) "capture synchronous backend failure"
  async <- try (trySynchronous (throwIO ThreadKilled))
  assert (either (const True) (const False) (async :: Either SomeException (Either SomeException ()))) "propagate asynchronous exception"
  failedCleanup <- runReleaseAttempt (throwIO (userError "script load failed"))
  assert (failedCleanup == ReleaseUnknown) "synchronous failure anywhere in cleanup is an unknown outcome"
  asyncCleanupAttempt <- try (runReleaseAttempt (throwIO ThreadKilled))
    :: IO (Either SomeException ReleaseOutcome)
  assert (case asyncCleanupAttempt of Left e -> fromException e == Just ThreadKilled; Right _ -> False)
    "cleanup classification must not convert async cancellation"
  preservedAsync <- try (onExceptionPreserving (throwIO ThreadKilled) (throwIO (userError "cleanup failed")))
    :: IO (Either SomeException ())
  assert (case preservedAsync of Left e -> fromException e == Just ThreadKilled; Right _ -> False)
    "best-effort owner cleanup cannot replace the original async exception"
  assert (classifyRenewalReply (Integer 1) == RenewalConfirmed) "accept confirmed renewal"
  assert (classifyRenewalReply (Integer 0) == RenewalNotOwned) "distinguish owner loss"
  assert (classifyRenewalReply (SingleLine "OK") == RenewalInvalidReply) "reject malformed renewal reply"
  assert (classifyRenewalReply (Bulk Nothing) == RenewalInvalidReply) "reject null renewal reply"
  releaseGate <- newMVar ()
  releaseState <- newIORef "held"
  releaseCalls <- newIORef (0 :: Int)
  let loseReleaseAck = modifyIORef' releaseCalls (+ 1) >> pure ReleaseUnknown
  firstUnknown <- releaseOnce releaseGate releaseState loseReleaseAck
  secondUnknown <- releaseOnce releaseGate releaseState loseReleaseAck
  observedReleaseCalls <- readIORef releaseCalls
  assert (firstUnknown == ReleaseUnknown && secondUnknown == ReleaseUnknown && observedReleaseCalls == 1)
    "ambiguous release remains terminal and is not retried"
  cancelledReleaseGate <- newMVar ()
  cancelledReleaseState <- newIORef "held"
  cancelledReleaseCalls <- newIORef (0 :: Int)
  let cancelRelease = modifyIORef' cancelledReleaseCalls (+ 1) >> throwIO ThreadKilled
      forbiddenRetry = modifyIORef' cancelledReleaseCalls (+ 1) >> pure Released
  cancelledResult <- try (releaseOnce cancelledReleaseGate cancelledReleaseState cancelRelease)
    :: IO (Either SomeException ReleaseOutcome)
  afterCancellation <- releaseOnce cancelledReleaseGate cancelledReleaseState forbiddenRetry
  cancelledCalls <- readIORef cancelledReleaseCalls
  assert (either (\e -> fromException e == Just ThreadKilled) (const False) cancelledResult &&
          afterCancellation == ReleaseUnknown && cancelledCalls == 1)
    "asynchronous cancellation is terminal and does not retry release"
  (scopedValue, scopedCleanup) <- withObservedBracket (pure ()) (\_ -> pure Released) (\_ -> fail "observer called for successful work") (\_ -> pure ("work" :: String))
  assert (scopedValue == "work" && scopedCleanup == Released) "return cleanup outcome on success"
  observed <- newEmptyMVar
  bodyFailure <- try (withObservedBracket (pure ()) (\_ -> pure ReleaseUnknown) (\outcome -> putMVar observed outcome >> throwIO (userError "observer failed")) (\_ -> throwIO (userError "work failed")))
    :: IO (Either IOException (String, ReleaseOutcome))
  assert (case bodyFailure of Left e -> show e == "user error (work failed)"; Right _ -> False) "preserve work exception"
  observedFailureCleanup <- takeMVar observed
  assert (case observedFailureCleanup of Right ReleaseUnknown -> True; _ -> False) "observe unknown cleanup after work exception"
  asyncObserved <- newEmptyMVar
  asyncStarted <- newEmptyMVar
  asyncGate <- newEmptyMVar
  asyncObservedResult <- newEmptyMVar
  thread <- forkIO $ do
    result <- try (withObservedBracket (pure ()) (\_ -> pure ReleaseUnknown) (\outcome -> putMVar asyncObserved outcome >> throwIO (userError "observer failed")) (\_ -> putMVar asyncStarted () >> takeMVar asyncGate))
      :: IO (Either SomeException ((), ReleaseOutcome))
    putMVar asyncObservedResult result
  takeMVar asyncStarted
  throwTo thread ThreadKilled
  asyncResult <- takeMVar asyncObservedResult
  assert (case asyncResult of Left e -> fromException e == Just ThreadKilled; Right _ -> False) "preserve asynchronous cancellation"
  asyncCleanup <- takeMVar asyncObserved
  assert (case asyncCleanup of Right ReleaseUnknown -> True; _ -> False) "observe unknown cleanup after cancellation"
  runLiveRedisIfConfigured
  putStrLn "identity checks passed"
  where assert True _ = pure (); assert False message = fail message

runLiveRedisIfConfigured :: IO ()
runLiveRedisIfConfigured = do
  maybeHost <- lookupEnv "DILOCK_TEST_REDIS_HOST"
  case maybeHost of
    Nothing -> putStrLn "live Redis integration skipped: DILOCK_TEST_REDIS_HOST is unset"
    Just host -> do
      let info = defaultConnectInfo { connectHost = host }
      bracket (connect info) disconnect $ \admin -> do
        firstConnection <- connect info
        secondConnection <- connect info
        (runOwnershipScenario admin firstConnection secondConnection)
          `finally` (disconnect firstConnection >> disconnect secondConnection)

runOwnershipScenario :: Connection -> Connection -> Connection -> IO ()
runOwnershipScenario admin firstConnection secondConnection = do
  started <- getMonotonicTimeNSec
  let namespace = "it" <> BC.pack (show started)
      epoch = "integration-" <> BC.pack (show started)
      metadataKey = "dilock:namespace:" <> namespace <> ":meta"
      metadata = "{\"protocol_major\":1,\"identity_scheme\":\"sha256-framed-utf8-v1\",\"profile\":\"redis-single-authority-v1\",\"coordination_epoch\":\"" <> epoch <> "\",\"fixed_lease_ms\":null}"
      resource = "haskell-live-redis-resource"
  ident <- either (fail . show) pure (resourceId (TE.decodeUtf8 namespace) (TE.decodeUtf8 resource))
  let lockKey = "dilock:{" <> ident <> "}:lease"
      deleteKey key = do
        _ <- runRedis admin (sendRequest ["DEL", key] :: Redis (Either Reply Integer))
        pure ()
      cleanupKeys = deleteKey lockKey >> deleteKey metadataKey
  (do
      wrongTypeReply <- runRedis admin (sendRequest ["LPUSH", metadataKey, "preserve-this-list"] :: Redis (Either Reply Integer))
      assertLive (case wrongTypeReply of Right 1 -> True; _ -> False) "provision wrong-type namespace metadata fixture"
      wrongTypeProvider <- try (provider firstConnection (TE.decodeUtf8 namespace) (TE.decodeUtf8 epoch))
        :: IO (Either DilockError Provider)
      assertLive (case wrongTypeProvider of Left (ProtocolMismatch _) -> True; _ -> False)
        "wrong-type namespace metadata is a protocol mismatch"
      preservedMetadata <- runRedis admin (sendRequest ["LRANGE", metadataKey, "0", "-1"])
      assertLive
        (case preservedMetadata of
          Right (MultiBulk (Just [Bulk (Just value)])) -> value == "preserve-this-list"
          _ -> False)
        "wrong-type namespace metadata remains untouched"
      writeMetadata admin metadataKey metadata
      firstProvider <- provider firstConnection (TE.decodeUtf8 namespace) (TE.decodeUtf8 epoch)
      secondProvider <- provider secondConnection (TE.decodeUtf8 namespace) (TE.decodeUtf8 epoch)
      firstHandle <- lock firstProvider (TE.decodeUtf8 resource)
      secondHandle <- lock secondProvider (TE.decodeUtf8 resource)
      firstLease <- acquire firstHandle (AcquireOptions 2000 2000)
      busy <- tryAcquire secondHandle 2000
      assertLive (isNothing busy) "real Redis second connection observes contention"
      firstRelease <- release firstLease
      assertLive (firstRelease == Released) "real Redis first owner releases"
      handoff <- acquire secondHandle (AcquireOptions 2000 2000)
      handoffRelease <- release handoff
      assertLive (handoffRelease == Released) "real Redis ownership hands off"
      stale <- acquire firstHandle (AcquireOptions 2000 100)
      threadDelay 150000
      successor <- acquire secondHandle (AcquireOptions 2000 2000)
      staleRelease <- release stale
      assertLive (staleRelease == AlreadyNotOwned) "real Redis stale owner cannot release successor"
      stillBusy <- tryAcquire firstHandle 2000
      assertLive (isNothing stillBusy) "real Redis successor remains held after stale release"
      successorRelease <- release successor
      assertLive (successorRelease == Released) "real Redis successor releases"
      (_, scopedRelease) <- withLease firstHandle (AcquireOptions 2000 2000) (const (pure ())) (const (pure ()))
      assertLive (scopedRelease == Released) "real Redis scoped lease reports cleanup outcome"
      pauseReply <- runRedis admin (sendRequest ["CLIENT", "PAUSE", "200"] :: Redis (Either Reply B.ByteString))
      assertLive (pauseReply == Right "OK")
        "pause disposable Redis briefly for a delayed acquisition acknowledgement"
      lateAcquire <- try (acquire firstHandle (AcquireOptions 50 2000))
        :: IO (Either DilockError Lease)
      assertLive (case lateAcquire of Left WaitTimeout -> True; _ -> False)
        "do not return a lease acknowledged after the acquire wait budget"
      lateSuccessor <- tryAcquire secondHandle 2000
      assertLive (isJust lateSuccessor) "successor acquires after late owner cleanup"
      lateSuccessorRelease <- maybe (fail "late successor lease disappeared") release lateSuccessor
      assertLive (lateSuccessorRelease == Released) "late successor releases after timeout cleanup"
      putStrLn "late acknowledgement wait-budget check passed"
      putStrLn "live Redis ownership checks passed against two independent connections"
    ) `finally` cleanupKeys

assertLive :: Bool -> String -> IO ()
assertLive True _ = pure ()
assertLive False message = fail message

writeMetadata :: Connection -> BS.ByteString -> BS.ByteString -> IO ()
writeMetadata conn key value = do
  reply <- runRedis conn (sendRequest ["SET", key, value])
  case reply of
    Right (SingleLine "OK") -> pure ()
    other -> fail ("failed to provision disposable Redis test metadata: " <> show other)
