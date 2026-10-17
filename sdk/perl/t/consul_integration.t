use strict;
use warnings;
use Test::More;
use lib qw(lib);
use Dilock::Consul;

my $endpoint = $ENV{DILOCK_TEST_CONSUL_ENDPOINT};
plan skip_all => 'set DILOCK_TEST_CONSUL_ENDPOINT to run against a Consul agent' unless $endpoint;

my $provider = Dilock::Consul->new(
    endpoint => $endpoint,
    namespace => $ENV{DILOCK_TEST_CONSUL_NAMESPACE} // 'dilock-perl-live',
    coordination_epoch => $ENV{DILOCK_TEST_CONSUL_EPOCH} // 'perl-live-test',
    acl_token => $ENV{DILOCK_TEST_CONSUL_TOKEN},
);
$provider->provision_namespace;
$provider->connect;
my $handle = $provider->lock('lifecycle');
my $lease = $handle->acquire(wait_ms => 10_000, lease_ms => 30_000);
is($lease->fence->{epoch}, $provider->{coordination_epoch}, 'lease fence carries the configured epoch');
ok($lease->renew, 'lease renewal verifies the same session and LockIndex');
is($lease->release, 'released', 'lease release is acknowledged');
$provider->close;
done_testing();
