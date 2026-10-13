use strict;
use warnings;
use EV;
use Dilock::Etcd;

my $provider = Dilock::Etcd->new(
    endpoint => $ENV{DILOCK_ETCD_ENDPOINT} // '127.0.0.1:2379',
    namespace => $ENV{DILOCK_NAMESPACE} // 'default',
    epoch => $ENV{DILOCK_COORDINATION_EPOCH} // 'local-etcd-epoch',
);
my $daily = $provider->lock('reports/daily');
$daily->acquire({ ttl_seconds => 30, wait_seconds => 5 }, sub {
    my ($lease, $error) = @_;
    die "$error" if $error;
    my $fence = $lease->fence;
    print "Default policy fence $fence->{epoch}/$fence->{create_revision}\n";
    $lease->release(sub {
        my ($outcome, $release_error) = @_;
        die "$release_error" if $release_error;
        die "release outcome unknown" if $outcome eq 'unknown';

        # The second resource handle reuses the provider and EV loop.
        my $weekly = $provider->lock('reports/weekly');
        $weekly->acquire({ ttl_seconds => 20, wait_seconds => 3 }, sub {
            my ($configured_lease, $acquire_error) = @_;
            die "$acquire_error" if $acquire_error;
            my $configured_fence = $configured_lease->fence;
            print "Configured policy fence $configured_fence->{epoch}/$configured_fence->{create_revision}\n";
            $configured_lease->release(sub {
                my ($configured_outcome, $configured_error) = @_;
                die "$configured_error" if $configured_error;
                die "release outcome unknown" if $configured_outcome eq 'unknown';
                EV::break;
            });
        });
    });
});
EV::run;
$provider->close;
