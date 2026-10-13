{-# LANGUAGE OverloadedStrings #-}
module Dilock.Internal.Metadata (validRedisNamespaceMetadata) where

import Data.Aeson (Value(..), eitherDecodeStrict')
import qualified Data.Aeson.KeyMap as KM
import qualified Data.ByteString as B
import Data.Scientific (base10Exponent, coefficient)
import Data.Text (Text)

-- | Validate Redis namespace metadata without accepting decimal JSON number
-- representations such as 1.0 for the integral protocol-major field.
validRedisNamespaceMetadata :: Text -> B.ByteString -> Bool
validRedisNamespaceMetadata expected raw = case eitherDecodeStrict' raw of
  Right (Object o) -> validMajor (KM.lookup "protocol_major" o)
    && KM.lookup "identity_scheme" o == Just (String "sha256-framed-utf8-v1")
    && KM.lookup "profile" o == Just (String "redis-single-authority-v1")
    && KM.lookup "coordination_epoch" o == Just (String expected)
    && KM.lookup "fixed_lease_ms" o == Just Null
  _ -> False
  where
    -- Aeson stores JSON numbers as Scientific, whose numeric equality treats
    -- 1.0 and 1 as equal. Require the decoded integer representation itself.
    validMajor (Just (Number value)) = coefficient value == 1 && base10Exponent value == 0
    validMajor _ = False
