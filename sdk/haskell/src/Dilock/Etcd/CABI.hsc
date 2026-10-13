{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

#include <dilock/dilock.h>

-- | Experimental Haskell binding over the optional Dilock C ABI v1.
--
-- This is not an independently implemented Haskell gRPC client. The consuming
-- process must install and dynamically link the Dilock C++/gRPC C ABI v1.
module Dilock.Etcd.CABI
  ( EtcdOptions (..)
  , AcquireOptions (..)
  , EtcdProvider
  , EtcdLock
  , EtcdLease
  , EtcdError (..)
  , EtcdReleaseOutcome (..)
  , defaultEtcdOptions
  , defaultEtcdAcquireOptions
  , withEtcdProvider
  , withEtcdLock
  , acquireEtcd
  , tryAcquireEtcd
  , withEtcdLease
  , etcdLeaseValid
  , etcdLeaseEffectiveTtlMilliseconds
  , etcdLeaseFence
  , releaseEtcdLease
  ) where

import Control.Concurrent (forkIO, newEmptyMVar, putMVar, readMVar)
import Control.Exception
  ( Exception
  , SomeException
  , bracket
  , catch
  , finally
  , mask
  , onException
  , throwIO
  , try
  )
import Control.Monad (unless, when)
import qualified Data.ByteString as BS
import Data.Int (Int64)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Foreign
  ( ForeignPtr
  , Ptr
  , alloca
  , allocaBytes
  , castPtr
  , nullPtr
  , peek
  , poke
  , withForeignPtr
  )
import Foreign.C (CChar, CInt (..), CSize (..), CString, peekCString)
import qualified Foreign.Concurrent as FC
import Dilock.Internal.CancellableResult (awaitPublishedResult)
import Foreign.ForeignPtr (finalizeForeignPtr)

data EtcdOptions = EtcdOptions
  { endpoint :: !Text
  , nameSpace :: !Text
  , coordinationEpoch :: !Text
  , tlsRootCertificates :: !(Maybe Text)
  , tlsClientCertificate :: !(Maybe Text)
  , tlsClientPrivateKey :: !(Maybe Text)
  , rpcTimeoutMilliseconds :: !Int64
  , cleanupTimeoutMilliseconds :: !Int64
  , safetyMarginMilliseconds :: !Int64
  , defaultWaitMilliseconds :: !Int64
  , defaultLeaseMilliseconds :: !Int64
  , autoRenew :: !Bool
  }
  deriving (Eq, Show)

data AcquireOptions = AcquireOptions
  { waitMilliseconds :: !Int64
  , leaseMilliseconds :: !Int64
  }
  deriving (Eq, Show)

-- | Derive the common acquisition policy from the provider options, so
-- per-acquisition defaults stay aligned with the configured provider.
defaultEtcdAcquireOptions :: EtcdOptions -> AcquireOptions
defaultEtcdAcquireOptions options = AcquireOptions
  { waitMilliseconds = defaultWaitMilliseconds options
  , leaseMilliseconds = defaultLeaseMilliseconds options
  }

data EtcdProvider = EtcdProvider (ForeignPtr ()) !Text
data EtcdLock = EtcdLock (ForeignPtr ()) !Text
data EtcdLease = EtcdLease (ForeignPtr ()) !Text

data EtcdError = EtcdError
  { nativeStatus :: !Int
  , nativeMessage :: !Text
  }
  deriving (Eq, Show)

instance Exception EtcdError

data EtcdReleaseOutcome = Released | AlreadyNotOwned | ReleaseUnknown
  deriving (Eq, Show)

-- | Defaults match ABI v1. Namespace metadata and deployment epoch must
-- already be provisioned; provider creation only reads and validates them.
defaultEtcdOptions :: Text -> EtcdOptions
defaultEtcdOptions epoch = EtcdOptions
  { endpoint = "127.0.0.1:2379"
  , nameSpace = "default"
  , coordinationEpoch = epoch
  , tlsRootCertificates = Nothing
  , tlsClientCertificate = Nothing
  , tlsClientPrivateKey = Nothing
  , rpcTimeoutMilliseconds = 2000
  , cleanupTimeoutMilliseconds = 2000
  , safetyMarginMilliseconds = 25
  , defaultWaitMilliseconds = 5000
  , defaultLeaseMilliseconds = 30000
  , autoRenew = False
  }

foreign import ccall safe "hs_dilock_abi_version"
  cAbiVersion :: IO CInt
foreign import ccall safe "hs_dilock_cancellation_create"
  cCancellationCreate :: Ptr (Ptr ()) -> Ptr CChar -> IO CInt
foreign import ccall unsafe "hs_dilock_cancellation_request"
  cCancellationRequest :: Ptr () -> IO ()
foreign import ccall safe "hs_dilock_cancellation_destroy"
  cCancellationDestroy :: Ptr () -> IO ()
foreign import ccall safe "hs_dilock_etcd_provider_create"
  cProviderCreate
    :: CString -> CString -> CString
    -> CString -> CString -> CString
    -> Int64 -> Int64 -> Int64 -> Int64 -> Int64 -> CInt
    -> Ptr (Ptr ()) -> Ptr CChar -> IO CInt
foreign import ccall safe "hs_dilock_etcd_provider_destroy"
  cProviderDestroy :: Ptr () -> IO ()
foreign import ccall safe "hs_dilock_etcd_lock_create"
  cLockCreate :: Ptr () -> CString -> Ptr (Ptr ()) -> Ptr CChar -> IO CInt
foreign import ccall safe "hs_dilock_etcd_lock_destroy"
  cLockDestroy :: Ptr () -> IO ()
foreign import ccall safe "hs_dilock_etcd_acquire"
  cAcquire :: Ptr () -> Int64 -> Int64 -> Ptr () -> Ptr (Ptr ()) -> Ptr CChar -> IO CInt
foreign import ccall safe "hs_dilock_etcd_lease_valid"
  cLeaseValid :: Ptr () -> IO CInt
foreign import ccall safe "hs_dilock_etcd_lease_ttl_ms"
  cLeaseTtl :: Ptr () -> Ptr Int64 -> Ptr CChar -> IO CInt
foreign import ccall safe "hs_dilock_etcd_lease_fence"
  cLeaseFence :: Ptr () -> Ptr Int64 -> CString -> CSize -> Ptr CChar -> IO CInt
foreign import ccall safe "hs_dilock_etcd_lease_release"
  cLeaseRelease :: Ptr () -> Ptr CInt -> Ptr CChar -> IO CInt
foreign import ccall safe "hs_dilock_etcd_lease_destroy"
  cLeaseDestroy :: Ptr () -> IO ()

errorCapacity :: Int
errorCapacity = #{const DILOCK_C_ERROR_MESSAGE_CAPACITY}

unsupportedCapabilityStatus :: Int
unsupportedCapabilityStatus = #{const DILOCK_STATUS_UNSUPPORTED_CAPABILITY}

maxDurationMilliseconds :: Int64
maxDurationMilliseconds = 86400000

minLeaseMilliseconds :: Int64
minLeaseMilliseconds = 100

-- | Create one C-ABI provider. The endpoint is a gRPC target. TLS arguments
-- are PEM contents (not file paths), matching the C++ gRPC credential API.
-- The metadata must exist and match this namespace/epoch before this call.
withEtcdProvider :: EtcdOptions -> (EtcdProvider -> IO a) -> IO a
withEtcdProvider options action = bracket (createProvider options) closeProvider action

createProvider :: EtcdOptions -> IO EtcdProvider
createProvider options = do
  validateOptions options
  abi <- cAbiVersion
  unless (abi == 1) $ throwIO (EtcdError (fromIntegral abi) "Dilock C ABI v1 is required")
  withTextCString (endpoint options) $ \endpointPtr ->
    withTextCString (nameSpace options) $ \namespacePtr ->
      withTextCString (coordinationEpoch options) $ \epochPtr ->
        withOptionalCString (tlsRootCertificates options) $ \rootsPtr ->
          withOptionalCString (tlsClientCertificate options) $ \certPtr ->
            withOptionalCString (tlsClientPrivateKey options) $ \keyPtr ->
              allocaBytes errorCapacity $ \message -> do
                poke message 0
                alloca $ \out -> do
                  poke out nullPtr
                  status <- cProviderCreate endpointPtr namespacePtr epochPtr rootsPtr certPtr keyPtr
                    (rpcTimeoutMilliseconds options) (cleanupTimeoutMilliseconds options)
                    (safetyMarginMilliseconds options) (defaultWaitMilliseconds options)
                    (defaultLeaseMilliseconds options) (if autoRenew options then 1 else 0)
                    out message
                  pointer <- peek out
                  checkStatus message status
                  when (pointer == nullPtr) $ throwIO (EtcdError 10 "C ABI returned a null provider")
                  EtcdProvider <$> newOwnedForeignPtr pointer cProviderDestroy <*> pure (coordinationEpoch options)

closeProvider :: EtcdProvider -> IO ()
closeProvider (EtcdProvider pointer _) = finalizeForeignPtr pointer

-- | Create a reusable resource handle. The provider and lock may be used from
-- concurrent Haskell threads; every blocking native call uses a safe FFI call.
withEtcdLock :: EtcdProvider -> Text -> (EtcdLock -> IO a) -> IO a
withEtcdLock (EtcdProvider provider epoch) resource action = bracket create close action
  where
    create = do
      validateText "resource" resource
      withForeignPtr provider $ \providerPtr -> withTextCString resource $ \resourcePtr ->
        allocaBytes errorCapacity $ \message -> alloca $ \out -> do
          poke message 0
          poke out nullPtr
          status <- cLockCreate providerPtr resourcePtr out message
          pointer <- peek out
          checkStatus message status
          when (pointer == nullPtr) $ throwIO (EtcdError 10 "C ABI returned a null lock")
          EtcdLock <$> newOwnedForeignPtr pointer cLockDestroy <*> pure epoch
    close (EtcdLock pointer _) = finalizeForeignPtr pointer

-- | Acquire with explicit budgets. The caller's async cancellation requests
-- the ABI's native cancellation token, waits for the bounded C++ cancellation
-- and cleanup path to finish, then rethrows the original Haskell exception.
-- Backend statuses are raised as 'EtcdError' and are never mapped to busy.
acquireEtcd :: EtcdLock -> AcquireOptions -> IO EtcdLease
acquireEtcd (EtcdLock lock epoch) policy = do
  validateAcquireOptions policy
  mask $ \restore -> do
    cancellation <- createCancellation
    let cleanupAcquiredLease lease = do
          _ <- releaseEtcdLease lease `catch` (\(_ :: SomeException) -> pure ReleaseUnknown)
          finalizeLease lease
        cancelAndJoin result = withForeignPtr cancellation $ \cancelPtr -> do
          cCancellationRequest cancelPtr
          workerResult <- readMVar result
          case workerResult of
            Right lease -> cleanupAcquiredLease lease
            Left (_ :: SomeException) -> pure ()
    (withForeignPtr lock $ \lockPtr -> withForeignPtr cancellation $ \cancelPtr -> do
      result <- newEmptyMVar
      _ <- forkIO $ do
        workerResult <- try $ mask $ \_ -> do
          allocaBytes errorCapacity $ \message -> alloca $ \out -> do
            poke message 0
            poke out nullPtr
            status <- cAcquire lockPtr (waitMilliseconds policy) (leaseMilliseconds policy)
              cancelPtr out message
            pointer <- peek out
            checkStatus message status
            when (pointer == nullPtr) $ throwIO (EtcdError 10 "C ABI returned a null lease")
            EtcdLease <$> newOwnedForeignPtr pointer cLeaseDestroy <*> pure epoch
        putMVar result workerResult
      workerResult <- awaitPublishedResult restore (cancelAndJoin result) result
      either throwIO pure workerResult)
      `finally` finalizeForeignPtr cancellation

-- | etcd's native Lock RPC queues rather than providing a safe one-shot
-- contention result. Fail explicitly before inspecting the handle or making
-- any backend call; callers must use 'acquireEtcd' with a finite wait budget.
tryAcquireEtcd :: EtcdLock -> AcquireOptions -> IO (Maybe EtcdLease)
tryAcquireEtcd _ policy = do
  validateAcquireOptions policy
  throwIO (EtcdError unsupportedCapabilityStatus
    "etcd native Lock does not support nonblocking try-acquire")

createCancellation :: IO (ForeignPtr ())
createCancellation = allocaBytes errorCapacity $ \message -> alloca $ \out -> do
  poke message 0
  poke out nullPtr
  status <- cCancellationCreate out message
  pointer <- peek out
  checkStatus message status
  when (pointer == nullPtr) $ throwIO (EtcdError 10 "C ABI returned a null cancellation handle")
  newOwnedForeignPtr pointer cCancellationDestroy

-- | Acquire, run work, release deterministically and destroy the native
-- handle. On work exceptions/cancellation, cleanup is attempted, the observer
-- receives its release outcome (or 'ReleaseUnknown' if cleanup throws), and
-- the original exception is rethrown. Observer exceptions are suppressed only
-- on that exceptional work path.
withEtcdLease
  :: EtcdLock
  -> AcquireOptions
  -> (EtcdReleaseOutcome -> IO ())
  -> (EtcdLease -> IO a)
  -> IO (a, EtcdReleaseOutcome)
withEtcdLease lock policy observeCleanup action = mask $ \restore -> do
  lease <- acquireEtcd lock policy
  outcome <- try (restore (action lease))
  case outcome of
    Right value -> do
      releaseOutcome <- releaseEtcdLease lease `finally` finalizeLease lease
      pure (value, releaseOutcome)
    Left (failure :: SomeException) -> do
      releaseOutcome <- (releaseEtcdLease lease `catch` (\(_ :: SomeException) -> pure ReleaseUnknown))
        `finally` finalizeLease lease
      observeCleanup releaseOutcome `catch` (\(_ :: SomeException) -> pure ())
      throwIO failure

etcdLeaseValid :: EtcdLease -> IO Bool
etcdLeaseValid (EtcdLease pointer _) = withForeignPtr pointer $ \leasePtr -> (/= 0) <$> cLeaseValid leasePtr

etcdLeaseEffectiveTtlMilliseconds :: EtcdLease -> IO Int64
etcdLeaseEffectiveTtlMilliseconds (EtcdLease pointer _) = withForeignPtr pointer $ \leasePtr ->
  allocaBytes errorCapacity $ \message -> alloca $ \ttl -> do
    poke message 0
    status <- cLeaseTtl leasePtr ttl message
    checkStatus message status
    peek ttl

-- | Return the epoch and verified etcd create_revision. A protected resource
-- must atomically register this fence and reject stale writes.
etcdLeaseFence :: EtcdLease -> IO (Text, Int64)
etcdLeaseFence (EtcdLease pointer epoch) = withForeignPtr pointer $ \leasePtr -> do
  let capacity = BS.length (TE.encodeUtf8 epoch) + 1
  allocaBytes capacity $ \epochBuffer -> alloca $ \revision ->
    allocaBytes errorCapacity $ \message -> do
      poke message 0
      status <- cLeaseFence leasePtr revision epochBuffer (fromIntegral capacity) message
      checkStatus message status
      epochBytes <- BS.packCStringLen (castPtr epochBuffer, capacity - 1)
      actualEpoch <- case TE.decodeUtf8' epochBytes of
        Left _ -> throwIO (EtcdError 7 "C ABI returned a non-UTF-8 coordination epoch")
        Right value -> pure value
      unless (actualEpoch == epoch) $ throwIO (EtcdError 7 "C ABI fence epoch differs from provider configuration")
      actualRevision <- peek revision
      pure (actualEpoch, actualRevision)

releaseEtcdLease :: EtcdLease -> IO EtcdReleaseOutcome
releaseEtcdLease (EtcdLease pointer _) = withForeignPtr pointer $ \leasePtr ->
  allocaBytes errorCapacity $ \message -> alloca $ \outcome -> do
    poke message 0
    poke outcome 2
    status <- cLeaseRelease leasePtr outcome message
    checkStatus message status
    value <- peek outcome
    case value of
      0 -> pure Released
      1 -> pure AlreadyNotOwned
      2 -> pure ReleaseUnknown
      _ -> throwIO (EtcdError 7 "C ABI returned an unknown release outcome")

finalizeLease :: EtcdLease -> IO ()
finalizeLease (EtcdLease pointer _) = finalizeForeignPtr pointer

newOwnedForeignPtr :: Ptr () -> (Ptr () -> IO ()) -> IO (ForeignPtr ())
newOwnedForeignPtr pointer destroy = FC.newForeignPtr pointer (destroy pointer)
  `onException` destroy pointer

checkStatus :: Ptr CChar -> CInt -> IO ()
checkStatus _ 0 = pure ()
checkStatus message status = do
  text <- T.pack <$> peekCString message
  throwIO (EtcdError (fromIntegral status) text)

validateOptions :: EtcdOptions -> IO ()
validateOptions options = do
  mapM_ (uncurry validateText)
    [("endpoint", endpoint options), ("namespace", nameSpace options), ("coordination epoch", coordinationEpoch options)]
  unless (rpcTimeoutMilliseconds options > 0 && cleanupTimeoutMilliseconds options > 0
       && rpcTimeoutMilliseconds options <= maxDurationMilliseconds
       && cleanupTimeoutMilliseconds options <= maxDurationMilliseconds
       && safetyMarginMilliseconds options >= 0 && safetyMarginMilliseconds options <= maxDurationMilliseconds
       && validBoundedDuration (defaultWaitMilliseconds options)
       && validLeaseDuration (defaultLeaseMilliseconds options)) $
    throwIO (EtcdError 2 "etcd timeout and default lease values are invalid")

validateAcquireOptions :: AcquireOptions -> IO ()
validateAcquireOptions options = unless
  (validBoundedDuration (waitMilliseconds options) && validLeaseDuration (leaseMilliseconds options)) $
    throwIO (EtcdError 2 "acquisition wait must be within 1..86400000 ms and lease duration within 100..86400000 ms")

validBoundedDuration :: Int64 -> Bool
validBoundedDuration value = value > 0 && value <= maxDurationMilliseconds

validLeaseDuration :: Int64 -> Bool
validLeaseDuration value = value >= minLeaseMilliseconds && value <= maxDurationMilliseconds

validateText :: String -> Text -> IO ()
validateText field value = when (T.null value || T.any (== '\0') value) $
  throwIO (EtcdError 2 (T.pack (field <> " must be nonempty and contain no NUL")))

withTextCString :: Text -> (CString -> IO a) -> IO a
withTextCString value action = BS.useAsCString (TE.encodeUtf8 value) action

withOptionalCString :: Maybe Text -> (CString -> IO a) -> IO a
withOptionalCString Nothing action = action nullPtr
withOptionalCString (Just value) action = withTextCString value action
