module Dilock.Internal.Release (ReleaseOutcome(..), releaseOnce, runReleaseAttempt) where

import Control.Concurrent.MVar (MVar, withMVar)
import Control.Exception (onException)
import Data.IORef (IORef, readIORef, writeIORef)
import Dilock.Internal.Exception (trySynchronous)

data ReleaseOutcome = Released | AlreadyNotOwned | ReleaseUnknown deriving (Eq, Show)

-- | Convert a synchronous failure anywhere in a best-effort cleanup attempt
-- (including loading a bundled script) into an unknown acknowledgement. Async
-- cancellation still propagates so it cannot be mistaken for a release result.
runReleaseAttempt :: IO ReleaseOutcome -> IO ReleaseOutcome
runReleaseAttempt attempt = do
  result <- trySynchronous attempt
  pure (either (const ReleaseUnknown) id result)

-- A confirmed outcome may be reported as already-not-owned on a later call.
-- An ambiguous first result is terminal: retrying could erase that evidence.
releaseOnce :: MVar () -> IORef String -> IO ReleaseOutcome -> IO ReleaseOutcome
releaseOnce gate state attempt = withMVar gate $ \_ -> do
  current <- readIORef state
  case current of
    "released" -> pure AlreadyNotOwned
    "released_unknown" -> pure ReleaseUnknown
    _ -> do
      writeIORef state "releasing"
      outcome <- attempt `onException` writeIORef state "released_unknown"
      writeIORef state (if outcome == ReleaseUnknown then "released_unknown" else "released")
      pure outcome
