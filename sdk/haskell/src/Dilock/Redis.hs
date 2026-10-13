{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Dilock.Redis
  ( Provider, Handle, Lease, AcquireOptions(..), defaultAcquireOptions, ReleaseOutcome(..), CleanupObserver
  , DilockError(..)
  , provider, lock, tryAcquire, acquire, release, renew, withLease, resourceId
  ) where

import Control.Concurrent (threadDelay)
import Control.Concurrent.MVar
import Control.Exception
import Control.Monad (unless, when)
import Crypto.Hash (Digest, SHA256, hash)
import Crypto.Random (getRandomBytes)
import Data.Bits
import qualified Data.ByteArray as BA
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC
import Data.IORef
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Data.Word
import Database.Redis (Connection, Redis, Reply(..), runRedis, sendRequest)
import Dilock.Internal.Backoff (pollDelayMicroseconds)
import Dilock.Internal.Exception (onExceptionPreserving, trySynchronous, withObservedBracket)
import Dilock.Internal.Duration (validLeaseDuration, validWaitDuration)
import Dilock.Internal.Metadata (validRedisNamespaceMetadata)
import Dilock.Internal.Release (ReleaseOutcome(..), releaseOnce, runReleaseAttempt)
import Dilock.Internal.Renewal (RenewalReply(..), classifyRenewalReply)
import GHC.Clock (getMonotonicTimeNSec)
import Paths_dilock (getDataFileName)

data DilockError = InvalidArgument String | BackendUnavailable String
                 | AcquisitionUnknown String | ProtocolMismatch String | WaitTimeout
  deriving (Eq, Show)
instance Exception DilockError
data AcquireOptions = AcquireOptions { waitMilliseconds :: !Int, leaseMilliseconds :: !Int }
  deriving (Eq, Show)

-- | Common bounded policy: wait up to 5 seconds for a 30-second lease.
-- Override either field when the operation has a different latency or TTL
-- budget. This does not change provider connection deadlines.
defaultAcquireOptions :: AcquireOptions
defaultAcquireOptions = AcquireOptions { waitMilliseconds = 5000, leaseMilliseconds = 30000 }
-- | Receives best-effort cleanup outcomes when scoped user work exits by
-- exception or cancellation. Observer exceptions are suppressed in that path.
type CleanupObserver = ReleaseOutcome -> IO ()
data Provider = Provider !Connection !Text !Text
data Handle = Handle !Provider !B.ByteString
data Lease = Lease !Provider !B.ByteString !B.ByteString !Int !(IORef Word64) !(MVar ()) !(IORef String)

u32 :: Int -> B.ByteString
u32 n = B.pack [fromIntegral (n `shiftR` 24), fromIntegral (n `shiftR` 16), fromIntegral (n `shiftR` 8), fromIntegral n]

resourceId :: Text -> Text -> Either DilockError B.ByteString
resourceId ns resource = do
  let nsBytes = TE.encodeUtf8 ns; resourceBytes = TE.encodeUtf8 resource
      firstOk = maybe False (\w -> (w >= 97 && w <= 122) || (w >= 48 && w <= 57)) (fmap fst (B.uncons nsBytes))
      restOk = B.all (\w -> firstOk && ((w >= 97 && w <= 122) || (w >= 48 && w <= 57) || w `elem` [45,46,95])) nsBytes
  unless (not (B.null nsBytes) && B.length nsBytes <= 64 && firstOk && restOk) (Left (InvalidArgument "invalid namespace"))
  unless (not (B.null resourceBytes) && B.length resourceBytes <= 1024 && not (B.elem 0 resourceBytes))
    (Left (InvalidArgument "invalid UTF-8 resource name"))
  let framed = "dilock.lock.identity\0" <> u32 (B.length nsBytes) <> nsBytes <> u32 (B.length resourceBytes) <> resourceBytes
      digest = hash framed :: Digest SHA256
      hexByte w = let digits = "0123456789abcdef" in [digits !! fromIntegral (w `shiftR` 4), digits !! fromIntegral (w .&. 15)]
  pure (BC.pack (concatMap hexByte (BA.unpack digest)))

-- | Borrow an existing hedis connection and verify provisioned namespace metadata.
provider :: Connection -> Text -> Text -> IO Provider
provider conn ns epoch = do
  either throwIO (const (pure ())) (resourceId ns "validation")
  when (T.null epoch || B.length (TE.encodeUtf8 epoch) > 1024) (throwIO (InvalidArgument "invalid epoch"))
  let metaKey = "dilock:namespace:" <> TE.encodeUtf8 ns <> ":meta"
  typeResult <- trySynchronous (runRedis conn (sendRequest ["TYPE", metaKey]))
  case typeResult of
    Left (e :: SomeException) -> throwIO (BackendUnavailable (displayException e))
    Right (Left _) -> throwIO (BackendUnavailable "metadata type check rejected")
    Right (Right (SingleLine "string")) -> do
      result <- trySynchronous (runRedis conn (sendRequest ["GET", metaKey]))
      case result of
        Left (e :: SomeException) -> throwIO (BackendUnavailable (displayException e))
        Right (Left _) -> throwIO (BackendUnavailable "metadata read rejected")
        Right (Right (Bulk (Just raw))) | validRedisNamespaceMetadata epoch raw -> pure (Provider conn ns epoch)
        _ -> throwIO (ProtocolMismatch "missing or incompatible namespace metadata")
    Right (Right _) -> throwIO (ProtocolMismatch "namespace metadata must be a Redis string")

lock :: Provider -> Text -> IO Handle
lock p@(Provider _ ns _) resource = do
  ident <- either throwIO pure (resourceId ns resource)
  pure (Handle p ("dilock:{" <> ident <> "}:lease"))

tryAcquire :: Handle -> Int -> IO (Maybe Lease)
tryAcquire (Handle p@(Provider conn _ _) key) ttl = do
  unless (validLeaseDuration ttl) (throwIO (InvalidArgument "lease must be 100..86400000 ms"))
  random <- getRandomBytes 32
  let owner = "dilock/1/" <> B.concatMap hexByte random
  sent <- getMonotonicTimeNSec
  result <- onExceptionPreserving
    (trySynchronous (runRedis conn (sendRequest ["SET", key, owner, "PX", BC.pack (show ttl), "NX"])))
    (cleanup conn key owner >> pure ())
  case result of
    Left (e :: SomeException) -> cleanup conn key owner >> throwIO (AcquisitionUnknown (displayException e))
    Right (Left _) -> throwIO (BackendUnavailable "Redis rejected SET")
    Right (Right (Bulk Nothing)) -> classifyContention
    Right (Right (SingleLine "OK")) -> do
      now <- getMonotonicTimeNSec
      let margin = max 10 (ttl `div` 100); deadline = sent + fromIntegral ((ttl - margin) * 1000000)
      when (now >= deadline) (cleanup conn key owner >> throwIO (AcquisitionUnknown "late SET acknowledgement"))
      gate <- newMVar (); state <- newIORef "held"
      deadlineRef <- newIORef deadline
      pure (Just (Lease p key owner ttl deadlineRef gate state))
    _ -> cleanup conn key owner >> throwIO (AcquisitionUnknown "unexpected SET reply")
  where
    classifyContention = do
      typeResult <- trySynchronous (runRedis conn (sendRequest ["TYPE", key]))
      case typeResult of
        Left (e :: SomeException) -> throwIO (BackendUnavailable (displayException e))
        Right (Left _) -> throwIO (BackendUnavailable "Redis TYPE check failed after contention")
        Right (Right (SingleLine "string")) -> pure Nothing
        Right (Right (SingleLine "none")) -> pure Nothing
        Right (Right (SingleLine _)) -> throwIO (ProtocolMismatch "lock key has an incompatible Redis type")
        Right (Right _) -> throwIO (ProtocolMismatch "Redis TYPE returned an invalid reply")
    hexByte w = let digits = "0123456789abcdef" in B.pack [digits `B.index` fromIntegral (w `shiftR` 4), digits `B.index` fromIntegral (w .&. 15)]

acquire :: Handle -> AcquireOptions -> IO Lease
acquire h o = do
  unless (validWaitDuration (waitMilliseconds o) && validLeaseDuration (leaseMilliseconds o))
    (throwIO (InvalidArgument "wait and lease must each be 1..86400000 ms, with a 100 ms minimum lease"))
  started <- getMonotonicTimeNSec
  let loop = do
        now <- getMonotonicTimeNSec
        let waitBudgetNs = fromIntegral (waitMilliseconds o) * 1000000
            elapsedNs = now - started
        when (elapsedNs >= waitBudgetNs) (throwIO WaitTimeout)
        attempted <- tryAcquire h (leaseMilliseconds o)
        case attempted of
          Nothing -> jitteredPoll started waitBudgetNs >> loop
          Just lease -> do
            completed <- getMonotonicTimeNSec
            if completed - started < waitBudgetNs
              then pure lease
              else do
                -- The wait budget includes backend round trips. Never hand
                -- user work a lease whose positive acknowledgement arrived
                -- after that budget; clean up only this generated owner.
                cleanupOutcome <- release lease
                case cleanupOutcome of
                  ReleaseUnknown -> throwIO (AcquisitionUnknown "acquire succeeded after wait budget; owner cleanup is unconfirmed")
                  Released -> throwIO WaitTimeout
                  AlreadyNotOwned -> throwIO WaitTimeout
  loop
  where
    jitteredPoll started waitBudgetNs = do
      sample <- getRandomBytes 4
      now <- getMonotonicTimeNSec
      let elapsedNs = now - started
          remainingMicroseconds = if elapsedNs >= waitBudgetNs
            then 0
            else fromIntegral ((waitBudgetNs - elapsedNs) `div` 1000)
      let value = B.foldl' (\acc byte -> (acc `shiftL` 8) .|. fromIntegral byte) (0 :: Word32) sample
          delayMicroseconds = pollDelayMicroseconds value remainingMicroseconds
      when (delayMicroseconds > 0) (threadDelay delayMicroseconds)

script :: String -> IO B.ByteString
script name = getDataFileName ("data/" <> name <> ".lua") >>= B.readFile

cleanup :: Connection -> B.ByteString -> B.ByteString -> IO ReleaseOutcome
cleanup conn key owner = runReleaseAttempt $ do
  source <- script "redis-release"
  result <- runRedis conn (sendRequest ["EVAL", source, "1", key, owner] :: Redis (Either Reply Reply))
  pure $ case result of
    Right (Integer 1) -> Released
    Right (Integer 0) -> AlreadyNotOwned
    _ -> ReleaseUnknown

release :: Lease -> IO ReleaseOutcome
release (Lease (Provider conn _ _) key owner _ _ gate state) =
  releaseOnce gate state (cleanup conn key owner)

renew :: Lease -> IO Bool
renew (Lease (Provider conn _ _) key owner ttl deadlineRef gate state) = withMVar gate $ \_ -> do
  now <- getMonotonicTimeNSec
  deadline <- readIORef deadlineRef
  st <- readIORef state
  if st /= "held" || now >= deadline then writeIORef state "lost" >> pure False else do
    source <- script "redis-renew"
    sent <- getMonotonicTimeNSec
    result <- trySynchronous (runRedis conn (sendRequest ["EVAL", source, "1", key, owner, BC.pack (show ttl)]))
      `onException` writeIORef state "lost"
    received <- getMonotonicTimeNSec
    case result of
      Right (Right (Integer 1)) | received < deadline && received < sent + fromIntegral (ttl * 990000) -> do
        writeIORef deadlineRef (sent + fromIntegral ((ttl - max 10 (ttl `div` 100)) * 1000000))
        pure True
      Left e -> writeIORef state "lost" >> throwIO (BackendUnavailable (displayException e))
      Right (Left _) -> writeIORef state "lost" >> throwIO (BackendUnavailable "Redis rejected renewal")
      Right (Right reply) -> do
        writeIORef state "lost"
        case classifyRenewalReply reply of
          RenewalNotOwned -> pure False
          RenewalConfirmed -> pure False -- The confirmation arrived after this lease's safe local deadline.
          RenewalInvalidReply -> throwIO (ProtocolMismatch "Redis renewal returned an invalid reply")

-- | Acquire a lease, run user work, and release it. Successful work returns
-- both its value and the release outcome. If user work throws (including an
-- asynchronous exception), the observer receives the cleanup outcome and the
-- original exception is rethrown unchanged. Observer exceptions are ignored
-- on exceptional cleanup so they cannot replace the user exception. The
-- observer should return promptly. This only bounds scope structure: the
-- lease expiry does not stop work after the process loses the lease.
withLease :: Handle -> AcquireOptions -> CleanupObserver -> (Lease -> IO a) -> IO (a, ReleaseOutcome)
withLease lockHandle options observer action =
  withObservedBracket (acquire lockHandle options) release (\cleanupResult ->
    let outcome = either (const ReleaseUnknown) id cleanupResult
    in observer outcome) action
