use strict;
use warnings;
use Dilock;

my $provider = Dilock->new(
    server => $ENV{DILOCK_REDIS_SERVER} // '127.0.0.1:6379',
    namespace => $ENV{DILOCK_NAMESPACE} // 'default',
    epoch => $ENV{DILOCK_COORDINATION_EPOCH} // 'local-redis-epoch',
);
my $handle = $provider->lock('reports/daily');
$handle->with_lease(sub {
    my ($lease) = @_;
    print "Default: run one short, idempotent report operation here\n";
});
my $configured = $provider->lock('reports/weekly');
my ($value, $release_outcome) = $configured->with_lease(
    sub { print "Configured: run one short, idempotent report operation here\n"; return 1 },
    wait_seconds => 3, ttl_ms => 20_000,
);
die "release outcome unknown" if $release_outcome eq 'unknown';
$provider->close;
