{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Dilock.Postgres.CABI

main :: IO ()
main = do
  let options = (defaultPostgresOptions "local-postgres-epoch") { nameSpace = "reports" }
  provisionPostgresNamespace options
  withPostgresProvider options $ \provider ->
    withPostgresLock provider "daily" $ \handle -> do
      (result, outcome) <- withPostgresLease handle (defaultPostgresAcquireOptions options)
        (\cleanup -> putStrLn ("work failed; cleanup outcome: " <> show cleanup))
        (\lease -> do
          _ <- postgresLeaseValid lease
          _ <- renewPostgresLease lease
          pure "protected result")
      print (result, outcome)
