module Dilock.Internal.Renewal
  ( RenewalReply(..)
  , classifyRenewalReply
  ) where

import Database.Redis (Reply(..))

-- | The Redis renewal contract is an integer status: 1 renews this owner and
-- 0 confirms that it no longer owns the key. Every other reply is malformed.
data RenewalReply = RenewalConfirmed | RenewalNotOwned | RenewalInvalidReply
  deriving (Eq, Show)

classifyRenewalReply :: Reply -> RenewalReply
classifyRenewalReply (Integer 1) = RenewalConfirmed
classifyRenewalReply (Integer 0) = RenewalNotOwned
classifyRenewalReply _ = RenewalInvalidReply
