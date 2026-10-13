use strict;
use warnings;
use Test::More;
use lib qw(lib);

my $endpoint = $ENV{DILOCK_ETCD_ENDPOINT};
plan skip_all => 'set DILOCK_ETCD_ENDPOINT and provision test metadata to run live etcd checks' unless $endpoint;

use Dilock::Etcd;
use EV ();

my $namespace = $ENV{DILOCK_ETCD_NAMESPACE} // 'perl-integration';
my $epoch = $ENV{DILOCK_ETCD_EPOCH} // 'perl-integration-epoch';
my $provider1 = Dilock::Etcd->new(endpoint => $endpoint, namespace => $namespace, epoch => $epoch);
my $provider2 = Dilock::Etcd->new(endpoint => $endpoint, namespace => $namespace, epoch => $epoch);
my $resource = 'live-' . $$ . '-' . time;
my $handle1 = $provider1->lock($resource);
my $handle2 = $provider2->lock($resource);
my ($first, $second, $first_error, $second_error);
my $first_fence;
my @timers;
my $guard = EV::timer(15, 0, sub { fail('live etcd operation exceeded 15 seconds'); EV::break });

$handle1->acquire({ ttl_seconds => 10, wait_seconds => 10 }, sub {
    ($first, $first_error) = @_;
    if ($first_error) { fail("first acquisition failed: $first_error"); EV::break; return }
    $first_fence = $first->fence;
    $handle2->acquire({ ttl_seconds => 1, wait_seconds => 8 }, sub {
        ($second, $second_error) = @_;
        if ($second_error) { fail("queued acquisition failed: $second_error"); EV::break; return }
        my $second_fence = $second->fence;
        cmp_ok(0 + $second_fence->{create_revision}, '>', 0 + $first_fence->{create_revision}, 'queued owner receives a later create_revision');
        $second->release(sub {
            my ($result, $error) = @_;
            is($result, 'released', 'second owner releases');
            is($error, undef, 'second release is acknowledged');
            EV::break;
        });
    });
    # Keep the first lock beyond the second attempt's initial one-second TTL.
    push @timers, EV::timer(2.5, 0, sub {
        $first->release(sub {
            my ($result, $error) = @_;
            is($result, 'released', 'first owner releases and advances native queue');
            is($error, undef, 'first release is acknowledged');
        });
    });
});
EV::run;
$guard->stop;
$_->stop for @timers;
ok($first, 'first native etcd acquisition completed');
ok($second, 'queued native etcd acquisition completed');
$provider1->close;
$provider2->close;
done_testing;
