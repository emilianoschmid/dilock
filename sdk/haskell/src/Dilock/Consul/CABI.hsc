{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

#include <dilock/dilock.h>

-- | Experimental Haskell facade over the shared Dilock C ABI v1 Consul engine.
module Dilock.Consul.CABI
  ( ConsulOptions (..), AcquireOptions (..), ConsulProvider, ConsulLock, ConsulLease
  , ConsulError (..), ConsulReleaseOutcome (..), defaultConsulOptions
  , defaultConsulAcquireOptions, provisionConsulNamespace, withConsulProvider
  , withConsulLock, acquireConsul, tryAcquireConsul, withConsulLease
  , consulLeaseValid, consulLeaseEffectiveTtlMilliseconds, consulLeaseFence
  , renewConsulLease, releaseConsulLease, destroyConsulLease
  ) where

import Control.Concurrent (forkIO, newEmptyMVar, putMVar, readMVar)
import Control.Exception (Exception, SomeException, bracket, catch, finally, mask, onException, throwIO, try)
import Control.Monad (unless, when)
import Data.Char (ord)
import qualified Data.ByteString as BS
import Data.Int (Int64)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Data.Word (Word64)
import Foreign (ForeignPtr, Ptr, alloca, allocaBytes, castPtr, nullPtr, peek, poke, withForeignPtr)
import Foreign.C (CChar, CInt (..), CSize (..), CString, peekCString)
import qualified Foreign.Concurrent as FC
import Foreign.ForeignPtr (finalizeForeignPtr)
import Dilock.Internal.CancellableResult (awaitPublishedResult)

data ConsulOptions = ConsulOptions
  { endpoint :: !Text
  , nameSpace :: !Text
  , coordinationEpoch :: !Text
  , aclToken :: !(Maybe Text)
  , rpcTimeoutMilliseconds :: !Int64
  , cleanupTimeoutMilliseconds :: !Int64
  , pollIntervalMilliseconds :: !Int64
  , defaultWaitMilliseconds :: !Int64
  , defaultLeaseMilliseconds :: !Int64
  } deriving (Eq, Show)

data AcquireOptions = AcquireOptions
  { waitMilliseconds :: !Int64
  , leaseMilliseconds :: !Int64
  } deriving (Eq, Show)

data ConsulProvider = ConsulProvider (ForeignPtr ()) !Text
data ConsulLock = ConsulLock (ForeignPtr ()) !Text
data ConsulLease = ConsulLease (ForeignPtr ()) !Text

data ConsulError = ConsulError { nativeStatus :: !Int, nativeMessage :: !Text }
  deriving (Eq, Show)
instance Exception ConsulError

data ConsulReleaseOutcome = Released | AlreadyNotOwned | ReleaseUnknown deriving (Eq, Show)

defaultConsulOptions :: Text -> ConsulOptions
defaultConsulOptions epoch = ConsulOptions
  { endpoint = "http://127.0.0.1:8500"
  , nameSpace = "default"
  , coordinationEpoch = epoch
  , aclToken = Nothing
  , rpcTimeoutMilliseconds = 2000
  , cleanupTimeoutMilliseconds = 2000
  , pollIntervalMilliseconds = 50
  , defaultWaitMilliseconds = 5000
  , defaultLeaseMilliseconds = 30000
  }

defaultConsulAcquireOptions :: ConsulOptions -> AcquireOptions
defaultConsulAcquireOptions options = AcquireOptions
  { waitMilliseconds = defaultWaitMilliseconds options
  , leaseMilliseconds = defaultLeaseMilliseconds options
  }

foreign import ccall safe "hs_dilock_abi_version" cAbiVersion :: IO CInt
foreign import ccall safe "hs_dilock_cancellation_create" cCancellationCreate :: Ptr (Ptr ()) -> Ptr CChar -> IO CInt
foreign import ccall unsafe "hs_dilock_cancellation_request" cCancellationRequest :: Ptr () -> IO ()
foreign import ccall safe "hs_dilock_cancellation_destroy" cCancellationDestroy :: Ptr () -> IO ()
foreign import ccall safe "hs_dilock_consul_provider_create"
  cProviderCreate :: CString -> CString -> CString -> CString -> Int64 -> Int64 -> Int64 -> Int64 -> Int64 -> Ptr (Ptr ()) -> Ptr CChar -> IO CInt
foreign import ccall safe "hs_dilock_consul_namespace_provision"
  cProvision :: CString -> CString -> CString -> CString -> Int64 -> Int64 -> Int64 -> Int64 -> Int64 -> Ptr CChar -> IO CInt
foreign import ccall safe "hs_dilock_consul_provider_destroy" cProviderDestroy :: Ptr () -> IO ()
foreign import ccall safe "hs_dilock_consul_lock_create" cLockCreate :: Ptr () -> CString -> Ptr (Ptr ()) -> Ptr CChar -> IO CInt
foreign import ccall safe "hs_dilock_consul_lock_destroy" cLockDestroy :: Ptr () -> IO ()
foreign import ccall safe "hs_dilock_consul_acquire" cAcquire :: Ptr () -> Int64 -> Int64 -> Ptr () -> Ptr (Ptr ()) -> Ptr CChar -> IO CInt
foreign import ccall safe "hs_dilock_consul_try_acquire" cTryAcquire :: Ptr () -> Int64 -> Ptr (Ptr ()) -> Ptr CChar -> IO CInt
foreign import ccall safe "hs_dilock_consul_lease_valid" cLeaseValid :: Ptr () -> IO CInt
foreign import ccall safe "hs_dilock_consul_lease_ttl_ms" cLeaseTtl :: Ptr () -> Ptr Int64 -> Ptr CChar -> IO CInt
foreign import ccall safe "hs_dilock_consul_lease_fence" cLeaseFence :: Ptr () -> Ptr Word64 -> CString -> CSize -> Ptr CChar -> IO CInt
foreign import ccall safe "hs_dilock_consul_lease_renew" cLeaseRenew :: Ptr () -> Ptr CInt -> Ptr CChar -> IO CInt
foreign import ccall safe "hs_dilock_consul_lease_release" cLeaseRelease :: Ptr () -> Ptr CInt -> Ptr CChar -> IO CInt
foreign import ccall safe "hs_dilock_consul_lease_destroy" cLeaseDestroy :: Ptr () -> IO ()

errorCapacity :: Int
errorCapacity = #{const DILOCK_C_ERROR_MESSAGE_CAPACITY}
maxDurationMilliseconds :: Int64
maxDurationMilliseconds = 86400000
busyStatus :: Int
busyStatus = #{const DILOCK_STATUS_BUSY}

-- | Provision exact namespace metadata explicitly; this never creates a provider.
provisionConsulNamespace :: ConsulOptions -> IO ()
provisionConsulNamespace options = do
  validateOptions options
  requireAbi
  withOptions options $ \endpointPtr namespacePtr epochPtr tokenPtr ->
    allocaBytes errorCapacity $ \message -> do
      poke message 0
      status <- cProvision endpointPtr namespacePtr epochPtr tokenPtr
        (rpcTimeoutMilliseconds options) (cleanupTimeoutMilliseconds options)
        (pollIntervalMilliseconds options) (defaultWaitMilliseconds options)
        (defaultLeaseMilliseconds options) message
      checkStatus message status

withConsulProvider :: ConsulOptions -> (ConsulProvider -> IO a) -> IO a
withConsulProvider options action = bracket (createProvider options) closeProvider action

createProvider :: ConsulOptions -> IO ConsulProvider
createProvider options = do
  validateOptions options
  requireAbi
  withOptions options $ \endpointPtr namespacePtr epochPtr tokenPtr ->
    allocaBytes errorCapacity $ \message -> alloca $ \out -> do
      poke message 0; poke out nullPtr
      status <- cProviderCreate endpointPtr namespacePtr epochPtr tokenPtr
        (rpcTimeoutMilliseconds options) (cleanupTimeoutMilliseconds options)
        (pollIntervalMilliseconds options) (defaultWaitMilliseconds options)
        (defaultLeaseMilliseconds options) out message
      pointer <- peek out
      checkStatus message status
      when (pointer == nullPtr) $ throwIO (ConsulError 10 "C ABI returned a null Consul provider")
      ConsulProvider <$> newOwnedForeignPtr pointer cProviderDestroy <*> pure (coordinationEpoch options)

requireAbi :: IO ()
requireAbi = do
  abi <- cAbiVersion
  unless (abi == 1) $ throwIO (ConsulError (fromIntegral abi) "Dilock C ABI v1 is required")

closeProvider :: ConsulProvider -> IO ()
closeProvider (ConsulProvider pointer _) = finalizeForeignPtr pointer

withConsulLock :: ConsulProvider -> Text -> (ConsulLock -> IO a) -> IO a
withConsulLock (ConsulProvider provider epoch) resource action = bracket create close action
  where
    create = do
      validateText "resource" resource
      withForeignPtr provider $ \providerPtr -> withTextCString resource $ \resourcePtr ->
        allocaBytes errorCapacity $ \message -> alloca $ \out -> do
          poke message 0; poke out nullPtr
          status <- cLockCreate providerPtr resourcePtr out message
          pointer <- peek out
          checkStatus message status
          when (pointer == nullPtr) $ throwIO (ConsulError 10 "C ABI returned a null Consul lock")
          ConsulLock <$> newOwnedForeignPtr pointer cLockDestroy <*> pure epoch
    close (ConsulLock pointer _) = finalizeForeignPtr pointer

-- | Acquire with bounded native cancellation. The owner lease is destroyed if
-- cancellation races with a completed native acquisition.
acquireConsul :: ConsulLock -> AcquireOptions -> IO ConsulLease
acquireConsul (ConsulLock lock epoch) policy = do
  validateAcquireOptions policy
  mask $ \restore -> do
    cancellation <- createCancellation
    let cleanupLease lease = do
          _ <- releaseConsulLease lease `catch` (\(_ :: SomeException) -> pure ReleaseUnknown)
          finalizeLease lease
        cancelAndJoin result = withForeignPtr cancellation $ \cancelPtr -> do
          cCancellationRequest cancelPtr
          workerResult <- readMVar result
          case workerResult of
            Right lease -> cleanupLease lease
            Left (_ :: SomeException) -> pure ()
    (withForeignPtr lock $ \lockPtr -> withForeignPtr cancellation $ \cancelPtr -> do
      result <- newEmptyMVar
      _ <- forkIO $ do
        workerResult <- try $ mask $ \_ -> allocaBytes errorCapacity $ \message -> alloca $ \out -> do
          poke message 0; poke out nullPtr
          status <- cAcquire lockPtr (waitMilliseconds policy) (leaseMilliseconds policy) cancelPtr out message
          pointer <- peek out
          checkStatus message status
          when (pointer == nullPtr) $ throwIO (ConsulError 10 "C ABI returned a null Consul lease")
          ConsulLease <$> newOwnedForeignPtr pointer cLeaseDestroy <*> pure epoch
        putMVar result workerResult
      workerResult <- awaitPublishedResult restore (cancelAndJoin result) result
      either throwIO pure workerResult)
      `finally` finalizeForeignPtr cancellation

tryAcquireConsul :: ConsulLock -> Int64 -> IO (Maybe ConsulLease)
tryAcquireConsul lockValue requestedLease = do
  validateAcquireOptions (AcquireOptions 1 requestedLease)
  case lockValue of
    ConsulLock lock epoch -> withForeignPtr lock $ \lockPtr ->
      allocaBytes errorCapacity $ \message -> alloca $ \out -> do
        poke message 0; poke out nullPtr
        status <- cTryAcquire lockPtr requestedLease out message
        pointer <- peek out
        if fromIntegral status == busyStatus then pure Nothing else do
          checkStatus message status
          when (pointer == nullPtr) $ throwIO (ConsulError 10 "C ABI returned a null Consul lease")
          Just <$> (ConsulLease <$> newOwnedForeignPtr pointer cLeaseDestroy <*> pure epoch)

-- | Await release on both normal and exceptional exits; a work exception remains primary.
withConsulLease :: ConsulLock -> AcquireOptions -> (ConsulReleaseOutcome -> IO ())
  -> (ConsulLease -> IO a) -> IO (a, ConsulReleaseOutcome)
withConsulLease lock policy observeCleanup action = mask $ \restore -> do
  lease <- acquireConsul lock policy
  outcome <- try (restore (action lease))
  case outcome of
    Right value -> do
      released <- releaseConsulLease lease `finally` finalizeLease lease
      pure (value, released)
    Left (failure :: SomeException) -> do
      released <- (releaseConsulLease lease `catch` (\(_ :: SomeException) -> pure ReleaseUnknown))
        `finally` finalizeLease lease
      observeCleanup released `catch` (\(_ :: SomeException) -> pure ())
      throwIO failure

consulLeaseValid :: ConsulLease -> IO Bool
consulLeaseValid (ConsulLease pointer _) = withForeignPtr pointer $ \leasePtr -> (/= 0) <$> cLeaseValid leasePtr

consulLeaseEffectiveTtlMilliseconds :: ConsulLease -> IO Int64
consulLeaseEffectiveTtlMilliseconds (ConsulLease pointer _) = withForeignPtr pointer $ \leasePtr ->
  allocaBytes errorCapacity $ \message -> alloca $ \ttl -> do
    poke message 0
    status <- cLeaseTtl leasePtr ttl message
    checkStatus message status
    peek ttl

consulLeaseFence :: ConsulLease -> IO (Text, Word64)
consulLeaseFence (ConsulLease pointer epoch) = withForeignPtr pointer $ \leasePtr -> do
  let capacity = BS.length (TE.encodeUtf8 epoch) + 1
  allocaBytes capacity $ \epochBuffer -> alloca $ \lockIndex -> allocaBytes errorCapacity $ \message -> do
    poke message 0
    status <- cLeaseFence leasePtr lockIndex epochBuffer (fromIntegral capacity) message
    checkStatus message status
    epochBytes <- BS.packCStringLen (castPtr epochBuffer, capacity - 1)
    actualEpoch <- case TE.decodeUtf8' epochBytes of
      Left _ -> throwIO (ConsulError 7 "C ABI returned a non-UTF-8 coordination epoch")
      Right value -> pure value
    unless (actualEpoch == epoch) $ throwIO (ConsulError 7 "fence epoch differs from provider configuration")
    index <- peek lockIndex
    pure (actualEpoch, index)

renewConsulLease :: ConsulLease -> IO Bool
renewConsulLease (ConsulLease pointer _) = withForeignPtr pointer $ \leasePtr ->
  allocaBytes errorCapacity $ \message -> alloca $ \renewed -> do
    poke message 0; poke renewed 0
    status <- cLeaseRenew leasePtr renewed message
    checkStatus message status
    (/= 0) <$> peek renewed

releaseConsulLease :: ConsulLease -> IO ConsulReleaseOutcome
releaseConsulLease (ConsulLease pointer _) = withForeignPtr pointer $ \leasePtr ->
  allocaBytes errorCapacity $ \message -> alloca $ \outcome -> do
    poke message 0; poke outcome 2
    status <- cLeaseRelease leasePtr outcome message
    checkStatus message status
    value <- peek outcome
    case value of
      0 -> pure Released
      1 -> pure AlreadyNotOwned
      2 -> pure ReleaseUnknown
      _ -> throwIO (ConsulError 7 "C ABI returned an unknown release outcome")

finalizeLease :: ConsulLease -> IO ()
finalizeLease (ConsulLease pointer _) = finalizeForeignPtr pointer

-- | Destroy an acquired lease handle after its release outcome is observed.
destroyConsulLease :: ConsulLease -> IO ()
destroyConsulLease = finalizeLease

createCancellation :: IO (ForeignPtr ())
createCancellation = allocaBytes errorCapacity $ \message -> alloca $ \out -> do
  poke message 0; poke out nullPtr
  status <- cCancellationCreate out message
  pointer <- peek out
  checkStatus message status
  when (pointer == nullPtr) $ throwIO (ConsulError 10 "C ABI returned a null cancellation handle")
  newOwnedForeignPtr pointer cCancellationDestroy

newOwnedForeignPtr :: Ptr () -> (Ptr () -> IO ()) -> IO (ForeignPtr ())
newOwnedForeignPtr pointer destroy = FC.newForeignPtr pointer (destroy pointer)
  `onException` destroy pointer

checkStatus :: Ptr CChar -> CInt -> IO ()
checkStatus _ 0 = pure ()
checkStatus message status = do
  detail <- T.pack <$> peekCString message
  throwIO (ConsulError (fromIntegral status) detail)

validateOptions :: ConsulOptions -> IO ()
validateOptions options = do
  mapM_ (uncurry validateText)
    [("endpoint", endpoint options), ("namespace", nameSpace options), ("coordination epoch", coordinationEpoch options)]
  unless ("http://" `T.isPrefixOf` endpoint options || "https://" `T.isPrefixOf` endpoint options) $
    throwIO (ConsulError 2 "Consul endpoint must use HTTP or HTTPS")
  case aclToken options of
    Nothing -> pure ()
    Just token -> do
      validateText "ACL token" token
      unless (T.all (\character -> ord character >= 33 && ord character <= 126) token) $
        throwIO (ConsulError 2 "Consul ACL token must be printable non-space ASCII")
  unless (validBoundedDuration (rpcTimeoutMilliseconds options)
       && validBoundedDuration (cleanupTimeoutMilliseconds options)
       && validBoundedDuration (pollIntervalMilliseconds options)
       && validBoundedDuration (defaultWaitMilliseconds options)
       && validLeaseDuration (defaultLeaseMilliseconds options)) $
    throwIO (ConsulError 2 "Consul timeout and default lease values are invalid")

validateAcquireOptions :: AcquireOptions -> IO ()
validateAcquireOptions options = unless
  (validBoundedDuration (waitMilliseconds options) && validLeaseDuration (leaseMilliseconds options)) $
  throwIO (ConsulError 2 "Consul wait must be 1..86400000 ms and lease must be 10000..86400000 ms")

validBoundedDuration :: Int64 -> Bool
validBoundedDuration value = value > 0 && value <= maxDurationMilliseconds

validLeaseDuration :: Int64 -> Bool
validLeaseDuration value = value >= 10000 && value <= maxDurationMilliseconds

validateText :: String -> Text -> IO ()
validateText field value = when (T.null value || T.any (== '\0') value) $
  throwIO (ConsulError 2 (T.pack (field <> " must be nonempty and contain no NUL")))

withTextCString :: Text -> (CString -> IO a) -> IO a
withTextCString value action = BS.useAsCString (TE.encodeUtf8 value) action

withOptionalCString :: Maybe Text -> (CString -> IO a) -> IO a
withOptionalCString Nothing action = action nullPtr
withOptionalCString (Just value) action = withTextCString value action

withOptions :: ConsulOptions -> (CString -> CString -> CString -> CString -> IO a) -> IO a
withOptions options action = withTextCString (endpoint options) $ \endpointPtr ->
  withTextCString (nameSpace options) $ \namespacePtr ->
    withTextCString (coordinationEpoch options) $ \epochPtr ->
      withOptionalCString (aclToken options) $ \tokenPtr ->
        action endpointPtr namespacePtr epochPtr tokenPtr
