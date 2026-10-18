{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Experimental Haskell facade over the shared Dilock C ABI v1 PostgreSQL
-- session-advisory provider.
module Dilock.Postgres.CABI
  ( PostgresOptions (..), AcquireOptions (..), PostgresProvider, PostgresLock, PostgresLease
  , PostgresError (..), PostgresReleaseOutcome (..), defaultPostgresOptions
  , defaultPostgresAcquireOptions, provisionPostgresNamespace, withPostgresProvider
  , withPostgresLock, acquirePostgres, tryAcquirePostgres, withPostgresLease
  , postgresLeaseValid, renewPostgresLease, releasePostgresLease, destroyPostgresLease
  ) where

#include <dilock/dilock.h>

import Control.Exception (Exception, SomeException, bracket, catch, finally, throwIO, try)
import Control.Monad (unless, when)
import qualified Data.ByteString as BS
import Data.Int (Int64)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import Foreign (ForeignPtr, Ptr, alloca, allocaBytes, nullPtr, peek, poke, withForeignPtr)
import Foreign.C (CChar, CInt (..), CString, peekCString)
import qualified Foreign.Concurrent as FC
import Foreign.ForeignPtr (finalizeForeignPtr)

data PostgresOptions = PostgresOptions
  { connectionString :: !Text, nameSpace :: !Text, coordinationEpoch :: !Text
  , rpcTimeoutMilliseconds :: !Int64, cleanupTimeoutMilliseconds :: !Int64
  , pollIntervalMilliseconds :: !Int64, defaultWaitMilliseconds :: !Int64
  , defaultLeaseMilliseconds :: !Int64
  } deriving (Eq, Show)

data AcquireOptions = AcquireOptions { waitMilliseconds :: !Int64, leaseMilliseconds :: !Int64 }
  deriving (Eq, Show)
data PostgresProvider = PostgresProvider (ForeignPtr ()) !Text
data PostgresLock = PostgresLock (ForeignPtr ()) !Text
data PostgresLease = PostgresLease (ForeignPtr ()) !Text
data PostgresError = PostgresError { nativeStatus :: !Int, nativeMessage :: !Text }
  deriving (Eq, Show)
instance Exception PostgresError
data PostgresReleaseOutcome = Released | AlreadyNotOwned | ReleaseUnknown deriving (Eq, Show)

defaultPostgresOptions :: Text -> PostgresOptions
defaultPostgresOptions epoch = PostgresOptions
  { connectionString = "postgresql://127.0.0.1:5432/dilock", nameSpace = "default"
  , coordinationEpoch = epoch, rpcTimeoutMilliseconds = 2000
  , cleanupTimeoutMilliseconds = 2000, pollIntervalMilliseconds = 25
  , defaultWaitMilliseconds = 5000, defaultLeaseMilliseconds = 30000 }

defaultPostgresAcquireOptions :: PostgresOptions -> AcquireOptions
defaultPostgresAcquireOptions options = AcquireOptions
  { waitMilliseconds = defaultWaitMilliseconds options, leaseMilliseconds = defaultLeaseMilliseconds options }

foreign import ccall safe "hs_dilock_abi_version" cAbiVersion :: IO CInt
foreign import ccall safe "hs_dilock_postgres_provider_create"
  cProviderCreate :: CString -> CString -> CString -> Int64 -> Int64 -> Int64 -> Int64 -> Int64 -> Ptr (Ptr ()) -> Ptr CChar -> IO CInt
foreign import ccall safe "hs_dilock_postgres_namespace_provision"
  cProvision :: CString -> CString -> CString -> Int64 -> Int64 -> Int64 -> Int64 -> Int64 -> Ptr CChar -> IO CInt
foreign import ccall safe "hs_dilock_postgres_provider_destroy" cProviderDestroy :: Ptr () -> IO ()
foreign import ccall safe "hs_dilock_postgres_lock_create"
  cLockCreate :: Ptr () -> CString -> Ptr (Ptr ()) -> Ptr CChar -> IO CInt
foreign import ccall safe "hs_dilock_postgres_lock_destroy" cLockDestroy :: Ptr () -> IO ()
foreign import ccall safe "hs_dilock_postgres_acquire"
  cAcquire :: Ptr () -> Int64 -> Int64 -> Ptr (Ptr ()) -> Ptr CChar -> IO CInt
foreign import ccall safe "hs_dilock_postgres_try_acquire"
  cTryAcquire :: Ptr () -> Ptr (Ptr ()) -> Ptr CChar -> IO CInt
foreign import ccall safe "hs_dilock_postgres_lease_valid" cLeaseValid :: Ptr () -> IO CInt
foreign import ccall safe "hs_dilock_postgres_lease_renew"
  cLeaseRenew :: Ptr () -> Ptr CInt -> Ptr CChar -> IO CInt
foreign import ccall safe "hs_dilock_postgres_lease_release"
  cLeaseRelease :: Ptr () -> Ptr CInt -> Ptr CChar -> IO CInt
foreign import ccall safe "hs_dilock_postgres_lease_destroy" cLeaseDestroy :: Ptr () -> IO ()

errorCapacity :: Int
errorCapacity = #{const DILOCK_C_ERROR_MESSAGE_CAPACITY}
maxDurationMilliseconds :: Int64
maxDurationMilliseconds = 86400000
busyStatus :: Int
busyStatus = #{const DILOCK_STATUS_BUSY}

provisionPostgresNamespace :: PostgresOptions -> IO ()
provisionPostgresNamespace options = do
  validateOptions options; requireAbi
  withOptions options $ \connectionPtr namespacePtr epochPtr ->
    allocaBytes errorCapacity $ \message -> do
      poke message 0
      status <- cProvision connectionPtr namespacePtr epochPtr
        (rpcTimeoutMilliseconds options) (cleanupTimeoutMilliseconds options)
        (pollIntervalMilliseconds options) (defaultWaitMilliseconds options)
        (defaultLeaseMilliseconds options) message
      checkStatus message status

withPostgresProvider :: PostgresOptions -> (PostgresProvider -> IO a) -> IO a
withPostgresProvider options action = bracket (createProvider options) closeProvider action

createProvider :: PostgresOptions -> IO PostgresProvider
createProvider options = do
  validateOptions options; requireAbi
  withOptions options $ \connectionPtr namespacePtr epochPtr ->
    allocaBytes errorCapacity $ \message -> alloca $ \out -> do
      poke message 0; poke out nullPtr
      status <- cProviderCreate connectionPtr namespacePtr epochPtr
        (rpcTimeoutMilliseconds options) (cleanupTimeoutMilliseconds options)
        (pollIntervalMilliseconds options) (defaultWaitMilliseconds options)
        (defaultLeaseMilliseconds options) out message
      pointer <- peek out; checkStatus message status
      when (pointer == nullPtr) $ throwIO (PostgresError 10 "C ABI returned a null PostgreSQL provider")
      PostgresProvider <$> newOwnedForeignPtr pointer cProviderDestroy <*> pure (coordinationEpoch options)

requireAbi :: IO ()
requireAbi = do
  abi <- cAbiVersion
  unless (abi == 1) $ throwIO (PostgresError (fromIntegral abi) "Dilock C ABI v1 is required")

closeProvider :: PostgresProvider -> IO ()
closeProvider (PostgresProvider pointer _) = finalizeForeignPtr pointer

withPostgresLock :: PostgresProvider -> Text -> (PostgresLock -> IO a) -> IO a
withPostgresLock (PostgresProvider provider epoch) resource action = bracket create close action
  where
    create = do
      validateText "resource" resource
      withForeignPtr provider $ \providerPtr -> withTextCString resource $ \resourcePtr ->
        allocaBytes errorCapacity $ \message -> alloca $ \out -> do
          poke message 0; poke out nullPtr
          status <- cLockCreate providerPtr resourcePtr out message
          pointer <- peek out; checkStatus message status
          when (pointer == nullPtr) $ throwIO (PostgresError 10 "C ABI returned a null PostgreSQL lock")
          PostgresLock <$> newOwnedForeignPtr pointer cLockDestroy <*> pure epoch
    close (PostgresLock pointer _) = finalizeForeignPtr pointer

acquirePostgres :: PostgresLock -> AcquireOptions -> IO PostgresLease
acquirePostgres (PostgresLock lock epoch) policy = do
  validateAcquireOptions policy
  withForeignPtr lock $ \lockPtr ->
    allocaBytes errorCapacity $ \message -> alloca $ \out -> do
      poke message 0; poke out nullPtr
      status <- cAcquire lockPtr (waitMilliseconds policy) (leaseMilliseconds policy) out message
      pointer <- peek out; checkStatus message status
      when (pointer == nullPtr) $ throwIO (PostgresError 10 "C ABI returned a null PostgreSQL lease")
      PostgresLease <$> newOwnedForeignPtr pointer cLeaseDestroy <*> pure epoch

tryAcquirePostgres :: PostgresLock -> IO (Maybe PostgresLease)
tryAcquirePostgres (PostgresLock lock epoch) = withForeignPtr lock $ \lockPtr ->
  allocaBytes errorCapacity $ \message -> alloca $ \out -> do
    poke message 0; poke out nullPtr
    status <- cTryAcquire lockPtr out message; pointer <- peek out
    if fromIntegral status == busyStatus then pure Nothing else do
      checkStatus message status
      when (pointer == nullPtr) $ throwIO (PostgresError 10 "C ABI returned a null PostgreSQL lease")
      Just <$> (PostgresLease <$> newOwnedForeignPtr pointer cLeaseDestroy <*> pure epoch)

withPostgresLease :: PostgresLock -> AcquireOptions -> (PostgresReleaseOutcome -> IO ())
  -> (PostgresLease -> IO a) -> IO (a, PostgresReleaseOutcome)
withPostgresLease lock policy observeCleanup action = do
  lease <- acquirePostgres lock policy
  outcome <- try (action lease)
  case outcome of
    Right value -> do
      released <- releasePostgresLease lease `finally` finalizePostgresLease lease
      pure (value, released)
    Left (failure :: SomeException) -> do
      released <- releasePostgresLease lease `catch` (\(_ :: SomeException) -> pure ReleaseUnknown)
        `finally` finalizePostgresLease lease
      observeCleanup released; throwIO failure

postgresLeaseValid :: PostgresLease -> IO Bool
postgresLeaseValid (PostgresLease pointer _) = withForeignPtr pointer $ \leasePtr -> (/= 0) <$> cLeaseValid leasePtr

renewPostgresLease :: PostgresLease -> IO Bool
renewPostgresLease (PostgresLease pointer _) = withForeignPtr pointer $ \leasePtr ->
  allocaBytes errorCapacity $ \message -> alloca $ \renewed -> do
    poke message 0; poke renewed 0; status <- cLeaseRenew leasePtr renewed message
    checkStatus message status; (/= 0) <$> peek renewed

releasePostgresLease :: PostgresLease -> IO PostgresReleaseOutcome
releasePostgresLease (PostgresLease pointer _) = withForeignPtr pointer $ \leasePtr ->
  allocaBytes errorCapacity $ \message -> alloca $ \outcome -> do
    poke message 0; poke outcome 2; status <- cLeaseRelease leasePtr outcome message
    checkStatus message status
    value <- peek outcome
    case value of
      0 -> pure Released
      1 -> pure AlreadyNotOwned
      2 -> pure ReleaseUnknown
      _ -> throwIO (PostgresError 7 "C ABI returned an unknown release outcome")

finalizePostgresLease :: PostgresLease -> IO ()
finalizePostgresLease (PostgresLease pointer _) = finalizeForeignPtr pointer

destroyPostgresLease :: PostgresLease -> IO ()
destroyPostgresLease = finalizePostgresLease

newOwnedForeignPtr :: Ptr () -> (Ptr () -> IO ()) -> IO (ForeignPtr ())
newOwnedForeignPtr pointer destroy = FC.newForeignPtr pointer (destroy pointer)

checkStatus :: Ptr CChar -> CInt -> IO ()
checkStatus _ 0 = pure ()
checkStatus message status = do
  detail <- T.pack <$> peekCString message
  throwIO (PostgresError (fromIntegral status) detail)

validateOptions :: PostgresOptions -> IO ()
validateOptions options = do
  mapM_ (uncurry validateText)
    [("connection string", connectionString options), ("namespace", nameSpace options), ("coordination epoch", coordinationEpoch options)]
  unless (all validBoundedDuration [rpcTimeoutMilliseconds options, cleanupTimeoutMilliseconds options,
      pollIntervalMilliseconds options, defaultWaitMilliseconds options, defaultLeaseMilliseconds options]) $
    throwIO (PostgresError 2 "PostgreSQL timeout and acquisition values are invalid")

validateAcquireOptions :: AcquireOptions -> IO ()
validateAcquireOptions options = unless
  (all validBoundedDuration [waitMilliseconds options, leaseMilliseconds options]) $
  throwIO (PostgresError 2 "PostgreSQL wait and lease values must be 1..86400000 ms")

validBoundedDuration :: Int64 -> Bool
validBoundedDuration value = value > 0 && value <= maxDurationMilliseconds

validateText :: String -> Text -> IO ()
validateText field value = when (T.null value || T.any (== '\0') value) $
  throwIO (PostgresError 2 (T.pack (field <> " must be nonempty and contain no NUL")))

withTextCString :: Text -> (CString -> IO a) -> IO a
withTextCString value action = BS.useAsCString (TE.encodeUtf8 value) action

withOptions :: PostgresOptions -> (CString -> CString -> CString -> IO a) -> IO a
withOptions options action = withTextCString (connectionString options) $ \connectionPtr ->
  withTextCString (nameSpace options) $ \namespacePtr ->
    withTextCString (coordinationEpoch options) $ \epochPtr -> action connectionPtr namespacePtr epochPtr
