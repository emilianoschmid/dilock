{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Monad (unless)
import Dilock.Consul.CABI

main :: IO ()
main = do
  let options = (defaultConsulOptions "local-consul-epoch")
        { nameSpace = "reports"
        , aclToken = Nothing
        }
  -- Run once as an explicit setup action, or have deployment tooling provision it.
  provisionConsulNamespace options
  withConsulProvider options $ \provider ->
    withConsulLock provider "daily" $ \handle -> do
      (result, releaseOutcome) <- withConsulLease handle
        (defaultConsulAcquireOptions options)
        (\outcome -> putStrLn ("work failed; cleanup outcome: " <> show outcome))
        (\lease -> do
          (epoch, lockIndex) <- consulLeaseFence lease
          unless (epoch == coordinationEpoch options && lockIndex > 0) $
            fail "Consul fence is missing or belongs to another epoch"
          -- Atomically enforce (epoch, LockIndex) at the protected resource.
          pure "protected result")
      print (result, releaseOutcome)
