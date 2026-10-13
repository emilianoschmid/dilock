<?php

declare(strict_types=1);

namespace Dilock;

abstract class DilockException extends \RuntimeException
{
}
final class BackendUnavailable extends DilockException
{
}
final class ProtocolMismatch extends DilockException
{
}
final class AcquisitionUnknown extends DilockException
{
}
final class WaitTimeout extends DilockException
{
}
final class LeaseLost extends DilockException
{
}
final class UnsupportedCapability extends DilockException
{
}
/** Raised when scoped work succeeds but its lease release was not acknowledged. */
final class CleanupError extends DilockException
{
}
