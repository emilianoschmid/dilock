{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Monad (unless)
import Dilock.Postgres.CABI

main :: IO ()
main = do
  let options = defaultPostgresOptions "local-postgres-epoch"
  require (defaultWaitMilliseconds options == 5000) "default wait should be five seconds"
  require (defaultLeaseMilliseconds options == 30000) "default lease should be thirty seconds"
  require (defaultPostgresAcquireOptions options == AcquireOptions 5000 30000)
    "default acquisition policy should follow provider defaults"
  require (nativeStatus (PostgresError 4 "timeout") == 4) "native status must remain observable"
  require (ReleaseUnknown /= Released) "unknown release must not be reported as released"
  putStrLn "PASS: Haskell PostgreSQL C ABI value and local validation contract"

require :: Bool -> String -> IO ()
require condition message = unless condition (fail message)
