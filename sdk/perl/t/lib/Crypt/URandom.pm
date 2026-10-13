package Crypt::URandom;

use strict;
use warnings;

# Deterministic test-only stand-in. Production still requires Crypt::URandom.
sub urandom {
    my ($length) = @_;
    die "invalid random byte count" unless defined($length) && $length =~ /\A[0-9]+\z/ && $length > 0;
    return "\xA5" x $length;
}

1;
