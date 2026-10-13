module Dilock.Internal.Exception (trySynchronous, onExceptionPreserving, withObservedBracket) where

import Control.Exception (SomeAsyncException, SomeException, catch, fromException, mask, onException, throwIO, try, tryJust)

-- | Capture backend failures without converting cancellation or other
-- asynchronous exceptions into ordinary SDK errors.
trySynchronous :: IO a -> IO (Either SomeException a)
trySynchronous = tryJust selectSynchronous
  where
    selectSynchronous :: SomeException -> Maybe SomeException
    selectSynchronous exception = case fromException exception :: Maybe SomeAsyncException of
      Just _ -> Nothing
      Nothing -> Just exception

-- | Run best-effort cleanup after failure without allowing a cleanup failure
-- (including a second async exception at an interruptible operation) to
-- replace the exception that triggered cleanup.
onExceptionPreserving :: IO a -> IO () -> IO a
onExceptionPreserving action cleanup = action `onException` (cleanup `catch` ignore)
  where
    ignore :: SomeException -> IO ()
    ignore _ = pure ()

-- | Acquire a resource masked, run its action unmasked, then clean up masked.
-- On an action exception, cleanup and the observer are attempted without
-- replacing that exception. On success, return both value and cleanup result.
withObservedBracket :: IO r -> (r -> IO c) -> (Either SomeException c -> IO ()) -> (r -> IO a) -> IO (a, c)
withObservedBracket acquire cleanup observer action = mask $ \restore -> do
  resource <- acquire
  actionResult <- try (restore (action resource))
  case actionResult of
    Right value -> do
      cleanupResult <- try (cleanup resource)
      case cleanupResult of
        Right cleanupValue -> pure (value, cleanupValue)
        Left cleanupException -> do
          notify cleanupResult
          throwIO (cleanupException :: SomeException)
    Left actionException -> do
      cleanupResult <- try (cleanup resource)
      notify cleanupResult
      throwIO (actionException :: SomeException)
  where
    notify result = do
      _ <- try (observer result) :: IO (Either SomeException ())
      pure ()
