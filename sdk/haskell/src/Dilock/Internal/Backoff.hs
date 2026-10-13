module Dilock.Internal.Backoff (pollDelayMicroseconds) where

import Data.Word (Word32)

-- | Map a random 32-bit sample to a bounded 10-35 ms contention poll window,
-- capped by the caller's remaining wait budget. A zero budget never sleeps.
pollDelayMicroseconds :: Word32 -> Int -> Int
pollDelayMicroseconds sample remainingMicroseconds =
  max 0 (min remainingMicroseconds (10000 + fromIntegral (sample `mod` 25001)))
