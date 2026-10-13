module Dilock.Internal.Duration (validWaitDuration, validLeaseDuration) where

-- Shared bounded duration validation for the Haskell Redis API. Keeping the
-- predicates pure lets tests exercise the exact policy without a Redis connection.
validWaitDuration :: Int -> Bool
validWaitDuration value = value >= 1 && value <= 86400000

validLeaseDuration :: Int -> Bool
validLeaseDuration value = value >= 100 && value <= 86400000
