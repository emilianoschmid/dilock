module Dilock.Internal.CancellableResult (awaitPublishedResult) where

import Control.Exception (onException)
import Control.Concurrent.MVar (MVar, readMVar)

-- | Await a worker result while allowing the caller's async exceptions. The
-- result remains observable if cancellation lands after it is published but
-- before this function returns, so the cleanup action can safely join it.
awaitPublishedResult :: (IO a -> IO a) -> IO () -> MVar a -> IO a
awaitPublishedResult restore cancel result = restore (readMVar result) `onException` cancel
