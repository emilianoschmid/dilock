<?php

declare(strict_types=1);

namespace Dilock;

/** Canonical draft 1 identity framing shared by every SDK. */
final class Identity
{
    public const SCHEME = 'sha256-framed-utf8-v1';

    public static function resourceId(mixed $namespace, mixed $resource): string
    {
        if (!is_string($namespace) || !is_string($resource)) {
            throw new \InvalidArgumentException('namespace and resource must be strings');
        }
        if (preg_match('/\A[a-z0-9][a-z0-9_.-]{0,63}\z/D', $namespace) !== 1) {
            throw new \InvalidArgumentException('invalid namespace');
        }
        if ($resource === '' || strlen($resource) > 1024 || str_contains($resource, "\0") || preg_match('//u', $resource) !== 1) {
            throw new \InvalidArgumentException('resource must be 1..1024 bytes of UTF-8 Unicode scalars without NUL');
        }
        $namespaceBytes = $namespace;
        $identity = "dilock.lock.identity\0"
            . pack('N', strlen($namespaceBytes)) . $namespaceBytes
            . pack('N', strlen($resource)) . $resource;
        return hash('sha256', $identity);
    }
}
