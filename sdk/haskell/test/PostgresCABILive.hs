{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Monad (unless)
import Data.Text (pack)
import Dilock.Postgres.CABI
import System.Environment (lookupEnv)

main :: IO ()
main = do
  configured <- lookupEnv "DILOCK_POSTGRES_DSN"
  case configured of
    Nothing -> putStrLn "SKIP Haskell PostgreSQL live consumer: DILOCK_POSTGRES_DSN is unset"
    Just dsn -> do
      let options = (defaultPostgresOptions "haskell-postgres-live-epoch")
            { connectionString = pack dsn, nameSpace = "haskell-postgres-live" }
      provisionPostgresNamespace options
      withPostgresProvider options $ \ownerProvider ->
        withPostgresProvider options $ \successorProvider ->
          withPostgresLock ownerProvider "haskell/postgres-live" $ \ownerLock ->
            withPostgresLock successorProvider "haskell/postgres-live" $ \successorLock -> do
              owner <- requireLease =<< tryAcquirePostgres ownerLock
              contender <- tryAcquirePostgres successorLock
              case contender of
                Nothing -> pure ()
                Just lease -> do
                  destroyPostgresLease lease
                  fail "Haskell contender acquired a held PostgreSQL key"
              live <- renewPostgresLease owner
              unless live (fail "Haskell PostgreSQL liveness check failed")
              outcome <- releasePostgresLease owner
              unless (outcome == Released) (fail "Haskell owner release was not acknowledged")
              destroyPostgresLease owner
              successor <- requireLease =<< tryAcquirePostgres successorLock
              successorOutcome <- releasePostgresLease successor
              unless (successorOutcome == Released) (fail "Haskell successor release was not acknowledged")
              destroyPostgresLease successor
      putStrLn "PASS Haskell PostgreSQL: provision, contention, liveness, release, and handoff"

requireLease :: Maybe PostgresLease -> IO PostgresLease
requireLease (Just lease) = pure lease
requireLease Nothing = fail "Haskell PostgreSQL acquisition returned no lease"
