{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Monad (unless)
import qualified Data.Text as T
import Dilock.Consul.CABI
import System.Environment (lookupEnv)

main :: IO ()
main = do
  endpointValue <- required "DILOCK_TEST_CONSUL_ENDPOINT"
  namespace <- T.pack <$> required "DILOCK_TEST_CONSUL_NAMESPACE"
  epoch <- T.pack <$> required "DILOCK_TEST_CONSUL_EPOCH"
  tokenValue <- lookupEnv "DILOCK_TEST_CONSUL_TOKEN"
  let options = (defaultConsulOptions epoch)
        { endpoint = T.pack endpointValue
        , nameSpace = namespace
        , aclToken = T.pack <$> tokenValue
        , defaultWaitMilliseconds = 5000
        , defaultLeaseMilliseconds = 12000
        }
      policy = AcquireOptions 5000 12000
      resource = "haskell-consul-cabi-live/lock-index"
  withConsulProvider options $ \firstProvider ->
    withConsulProvider options $ \secondProvider ->
      withConsulLock firstProvider resource $ \ownerLock ->
        withConsulLock secondProvider resource $ \waiterLock -> do
          (ownerFence, ownerResult) <- withConsulLease ownerLock policy (\_ -> pure ()) $ \owner -> do
            fence <- consulLeaseFence owner
            require (fst fence == epoch && snd fence > 0) "owner fence is invalid"
            ttl <- consulLeaseEffectiveTtlMilliseconds owner
            require (ttl >= 12000) "effective session TTL is below the requested duration"
            renewed <- renewConsulLease owner
            require renewed "owner-checked Consul renewal was not confirmed"
            busy <- tryAcquireConsul waiterLock 12000
            case busy of
              Nothing -> pure ()
              Just incorrectLease -> do
                _ <- releaseConsulLease incorrectLease
                destroyConsulLease incorrectLease
                fail "try-acquire succeeded while the first owner held the key"
            pure fence
          require (ownerResult == Released) "owner release was not acknowledged"
          (_, successorResult) <- withConsulLease waiterLock policy (\_ -> pure ()) $ \successor -> do
            fence <- consulLeaseFence successor
            require (fst fence == epoch && snd fence > snd ownerFence)
              "retained-key LockIndex did not advance"
            pure fence
          require (successorResult == Released) "successor release was not acknowledged"
  putStrLn "PASS: Haskell Consul live acquire, contention, fencing and release"

required :: String -> IO String
required key = do
  value <- lookupEnv key
  maybe (fail (key <> " must identify an explicitly provisioned disposable Consul namespace")) pure value

require :: Bool -> String -> IO ()
require condition message = unless condition (fail message)
