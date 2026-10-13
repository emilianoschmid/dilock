use strict;
use warnings;
use Test::More;
use Time::HiRes qw(sleep);

my $server = $ENV{DILOCK_REDIS_SERVER};
plan skip_all => 'set DILOCK_REDIS_SERVER to run Redis integration tests' unless $server;

require Redis::Fast;
require Dilock;
require JSON::PP;

my $namespace = 'perltest-' . $$;
my $epoch = 'perl-integration-' . $$;
my $metadata_key = "dilock:namespace:$namespace:meta";
my $control = Redis::Fast->new(
    server => $server,
    reconnect => 0,
    conservative_reconnect => 1,
    cnx_timeout => 2,
    read_timeout => 2,
    write_timeout => 2,
    encoding => undef,
);
$control->set($metadata_key, JSON::PP->new->canonical->encode({
    protocol_major => 1,
    identity_scheme => 'sha256-framed-utf8-v1',
    profile => 'redis-single-authority-v1',
    coordination_epoch => $epoch,
    fixed_lease_ms => undef,
}));

my $provider_a = Dilock->new(server => $server, namespace => $namespace, epoch => $epoch);
my $provider_b = Dilock->new(server => $server, namespace => $namespace, epoch => $epoch);
my $resource = 'resource/' . $$;
my $handle_a = $provider_a->lock($resource);
my $handle_b = $provider_b->lock($resource);

my $lease_a = $handle_a->try_acquire(ttl_ms => 2000);
ok($lease_a, 'first provider acquires against Redis');
is($handle_b->try_acquire(ttl_ms => 2000), undef, 'second provider observes Redis contention');
is($lease_a->release, 'released', 'owner-checked release is acknowledged');

my $lease_b = $handle_b->try_acquire(ttl_ms => 2000);
ok($lease_b, 'second provider acquires after acknowledged release');
is($lease_b->release, 'released', 'successor releases its own key');

my $expiring_lease = $handle_a->try_acquire(ttl_ms => 150);
ok($expiring_lease, 'short lease acquired before expiry test');
sleep 0.25;
my $successor = $handle_b->try_acquire(ttl_ms => 2000);
ok($successor, 'successor acquires after Redis expires the old lease');
is($expiring_lease->release, 'already_not_owned', 'late release acknowledges that ownership changed');
is($control->get($successor->{key}), $successor->{owner}, 'late release preserves the successor owner');
is($successor->release, 'released', 'successor can release normally');
is($control->get($successor->{key}), undef, 'released successor key is absent');

my $uncertain_client = Redis::Fast->new(
    server => $server,
    reconnect => 0,
    conservative_reconnect => 1,
    cnx_timeout => 2,
    read_timeout => 2,
    write_timeout => 2,
    encoding => undef,
);
my $lost_reply_redis = Local::LostReleaseReplyRedis->new($uncertain_client);
my $uncertain_provider = Dilock->new(
    redis => $lost_reply_redis, namespace => $namespace, epoch => $epoch,
);
my $uncertain_handle = $uncertain_provider->lock('lost-release-reply/' . $$);
my $uncertain_lease = $uncertain_handle->try_acquire(ttl_ms => 2000);
ok($uncertain_lease, 'owner acquires before the live ambiguous-release test');
my $uncertain_key = $uncertain_lease->{key};
is($uncertain_lease->release, 'unknown', 'live Redis delete with lost reply is reported as unknown');
is($uncertain_client->get($uncertain_key), undef, 'Redis applied the delete before the reply was lost');
is($uncertain_lease->release, 'unknown', 'repeated live release preserves the first unknown outcome');
is($lost_reply_redis->{release_calls}, 1, 'repeated live release does not send another EVAL');

$uncertain_provider->close;
$provider_a->close;
$provider_b->close;
$control->del($metadata_key);
$control->quit;

done_testing;

package Local::LostReleaseReplyRedis;
sub new { bless { redis => $_[1], release_calls => 0 }, $_[0] }
sub get { my $self = shift; $self->{redis}->get(@_) }
sub type { my $self = shift; $self->{redis}->type(@_) }
sub set { my $self = shift; $self->{redis}->set(@_) }
sub eval {
    my ($self, @arguments) = @_;
    $self->{release_calls}++;
    my $reply = $self->{redis}->eval(@arguments);
    if ($self->{release_calls} == 1 && defined($reply) && $reply == 1) {
        die "simulated lost Redis release response after server-side delete";
    }
    return $reply;
}
sub quit { my $self = shift; $self->{redis}->quit(@_) }
