<?php

declare(strict_types=1);

namespace Dilock\Tests;

use Dilock\Identity;
use PHPUnit\Framework\TestCase;

final class IdentityTest extends TestCase
{
    public function testSharedGoldenVectors(): void
    {
        $raw = file_get_contents(__DIR__ . '/../../../spec/vectors/identity.json');
        self::assertIsString($raw);
        self::assertSame(1, preg_match('/"valid"\s*:\s*(\[.*?\])\s*,\s*"invalid"/s', $raw, $matches));
        $valid = json_decode($matches[1], true, 512, JSON_THROW_ON_ERROR);
        foreach ($valid as $vector) {
            self::assertSame($vector['sha256'], Identity::resourceId($vector['namespace'], $vector['resource']), $vector['id']);
        }
    }

    public function testInvalidInputsAreRejected(): void
    {
        foreach ([['', 'r'], ['Prod', 'r'], ['a/b', 'r'], ['default', ''], ['default', "a\0b"],
                  ['default', str_repeat('x', 1025)], ['default', "\xED\xA0\x80"],
                  ['default', "\xF4\x90\x80\x80"], ['default', "\xF0\x8F\xBF\xBF"],
                  ['default', "\xF1\x80\x80"],
                  [str_repeat('n', 65), 'r'], [42, 'r'], ['default', 42]] as [$namespace, $resource]) {
            try {
                Identity::resourceId($namespace, $resource);
                self::fail('invalid identity input was accepted');
            } catch (\InvalidArgumentException) {
                self::assertTrue(true);
            }
        }
    }

    public function testAcceptsMaximumUnicodeScalarInFourByteUtf8(): void
    {
        self::assertSame(64, strlen(Identity::resourceId('default', "\xF4\x8F\xBF\xBF")));
    }
}
