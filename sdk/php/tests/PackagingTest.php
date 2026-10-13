<?php

declare(strict_types=1);

namespace Dilock\Tests;

use PHPUnit\Framework\TestCase;

final class PackagingTest extends TestCase
{
    public function testComposerArchiveExcludesInternalValidationEvidence(): void
    {
        $raw = file_get_contents(__DIR__ . '/../composer.json');
        self::assertIsString($raw);
        $manifest = json_decode($raw, true, 512, JSON_THROW_ON_ERROR);
        $exclusions = $manifest['archive']['exclude'] ?? null;

        self::assertIsArray($exclusions);
        self::assertContains('/validation/', $exclusions);
    }
}
