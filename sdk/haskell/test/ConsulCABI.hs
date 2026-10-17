{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Exception (try)
import Control.Monad (unless)
import Dilock.Consul.CABI

main :: IO ()
main = do
  let options = defaultConsulOptions "epoch"
  require (defaultWaitMilliseconds options == 5000) "default wait should be five seconds"
  require (defaultLeaseMilliseconds options == 30000) "default lease should be thirty seconds"
  require (defaultConsulAcquireOptions options == AcquireOptions 5000 30000)
    "default acquisition policy should follow provider defaults"
  rejectProvider ((options) { rpcTimeoutMilliseconds = 0 })
  rejectProvider ((options) { defaultLeaseMilliseconds = 9999 })
  rejectProvider ((options) { endpoint = "ftp://localhost:8500" })
  rejectProvider ((options) { aclToken = Just "invalid token" })
  invalidTry <- try (tryAcquireConsul (error "invalid lease validation inspected a lock") 9999)
  case (invalidTry :: Either ConsulError (Maybe ConsulLease)) of
    Left failure -> require (nativeStatus failure == 2) "invalid try-acquire lease returned the wrong status"
    Right _ -> fail "sub-10 second try-acquire lease was accepted"
  require (nativeStatus (ConsulError 4 "timeout") == 4) "native status must remain observable"
  require (ReleaseUnknown /= Released) "unknown release must not be reported as released"
  putStrLn "PASS: Haskell Consul C ABI value and local validation contract"

rejectProvider :: ConsulOptions -> IO ()
rejectProvider options = do
  outcome <- try (withConsulProvider options (\_ -> pure ()))
  case (outcome :: Either ConsulError ()) of
    Left failure -> require (nativeStatus failure == 2) "invalid provider options returned the wrong status"
    Right () -> fail "invalid provider options were accepted"

require :: Bool -> String -> IO ()
require condition message = unless condition (fail message)
