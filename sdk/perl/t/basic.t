use strict;
use warnings;
use Test::More;
use lib qw(t/lib lib);
use Dilock;

my $contention_min = Dilock::Handle::_contention_delay(1, 0);
my $contention_max = Dilock::Handle::_contention_delay(1, 0.999999);
cmp_ok($contention_min, '>=', 0.020, 'Redis contention delay starts at 20 ms');
cmp_ok($contention_max, '<', 0.050, 'Redis contention delay stays below 50 ms');
is(Dilock::Handle::_contention_delay(0.015, 0.999999), 0.015,
   'Redis contention delay is capped at remaining wait time');
is(Dilock::Handle::_contention_delay(0.001, 0), 0.001,
   'Redis contention delay cannot sleep beyond an imminent deadline');

is(Dilock::identity('default', 'camera/123/reindex'),
   'e0cdd484a99b6aee4162335939d2a2d128a490c097069fd85b2fc1e540a940b6',
   'shared identity vector');
isnt(Dilock::identity('default', "caf\x{e9}"), Dilock::identity('default', "cafe\x{301}"),
   'composed and decomposed Unicode stay distinct');
eval { Dilock::identity('Bad', 'resource') };
like($@, qr/invalid namespace/, 'invalid namespace rejected');

my $boundary_epoch = "\x{e9}" x 512;
my $boundary_provider = Dilock->new(redis => Local::Redis->new, epoch => $boundary_epoch);
is($boundary_provider->{epoch}, $boundary_epoch, 'epoch of exactly 1024 UTF-8 bytes is accepted');
my $oversized_epoch = "\x{e9}" x 513;
eval { Dilock->new(redis => Local::Redis->new, epoch => $oversized_epoch) };
like($@, qr/1024 UTF-8 bytes/, 'epoch exceeding 1024 UTF-8 bytes is rejected');
$boundary_provider->close;

my $redis = Local::Redis->new;
$redis->{store}{'dilock:namespace:default:meta'} =
    '{"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1","profile":"redis-single-authority-v1","coordination_epoch":"test-epoch","fixed_lease_ms":null}';
my $provider = Dilock->new(redis => $redis, namespace => 'default', epoch => 'test-epoch');
my $handle = $provider->lock('resource');
my $metadata_key = 'dilock:namespace:default:meta';
$redis->{metadata_type_override} = 'hash';
eval { $handle->try_acquire(ttl_ms => 1000) };
isa_ok($@, 'Dilock::ProtocolMismatch', 'wrong-type namespace metadata is a protocol mismatch');
like($redis->{store}{$metadata_key}, qr/protocol_major/, 'metadata type validation does not mutate the value');
delete $redis->{metadata_type_override};
$redis->{metadata_type_error} = 1;
eval { $handle->try_acquire(ttl_ms => 1000) };
isa_ok($@, 'Dilock::BackendUnavailable', 'metadata TYPE transport errors remain backend failures');
delete $redis->{metadata_type_error};
my @invalid_metadata = (
    ['protocol_major string', 'protocol_major', '"1"'],
    ['protocol_major float', 'protocol_major', '1.0'],
    ['protocol_major boolean', 'protocol_major', 'true'],
    ['identity_scheme number', 'identity_scheme', '1'],
    ['profile boolean', 'profile', 'false'],
    ['coordination_epoch number', 'coordination_epoch', '123'],
    ['fixed_lease_ms integer', 'fixed_lease_ms', '0'],
);
for my $case (@invalid_metadata) {
    my ($name, $field, $replacement) = @$case;
    my $metadata = '{"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1",'
        . '"profile":"redis-single-authority-v1","coordination_epoch":"test-epoch",'
        . '"fixed_lease_ms":null}';
    $metadata =~ s/("\Q$field\E"\s*:)\s*(?:"[^"]*"|[0-9.]+|true|false|null)/$1$replacement/;
    $redis->{store}{$metadata_key} = $metadata;
    eval { $handle->try_acquire(ttl_ms => 1000) };
    isa_ok($@, 'Dilock::ProtocolMismatch', "metadata JSON type is strict: $name");
    ok(!exists($redis->{store}{$handle->{key}}), "metadata mismatch does not acquire a lock: $name");
}
$redis->{store}{$metadata_key} =
    '{"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1","profile":"redis-single-authority-v1","coordination_epoch":"test-epoch","fixed_lease_ms":null}';
my $lease = $handle->try_acquire(ttl_ms => 1000);
ok($lease, 'first lease acquired');
is($lease->state, 'held', 'new lease has a confirmed local validity budget');
ok($lease->renew, 'Redis renewal extends only the current owner generation');
is($lease->state, 'held', 'acknowledged renewal retains held state');
is($handle->try_acquire(ttl_ms => 1000), undef, 'contention is undef');
$redis->{type_override} = 'none';
$redis->{force_contention} = 1;
is($handle->try_acquire(ttl_ms => 1000), undef, 'expired key observed by TYPE remains ordinary contention');
delete $redis->{type_override};
delete $redis->{force_contention};
$redis->{type_override} = 'hash';
eval { $handle->try_acquire(ttl_ms => 1000) };
isa_ok($@, 'Dilock::ProtocolMismatch', 'unexpected key type is a protocol mismatch');
is($redis->{store}{$lease->{key}}, $lease->{owner}, 'wrong-type detection leaves key state untouched');
delete $redis->{type_override};
$redis->{type_error} = 1;
eval { $handle->try_acquire(ttl_ms => 1000) };
isa_ok($@, 'Dilock::BackendUnavailable', 'TYPE failure is not reported as contention');
delete $redis->{type_error};
$redis->{store}{$lease->{key}} = 'dilock/1/' . ('f' x 64); # successor owner
ok(!$lease->renew, 'stale owner renewal is rejected');
is($lease->state, 'lost', 'rejected renewal makes the old lease terminal');
is($redis->{store}{$lease->{key}}, 'dilock/1/' . ('f' x 64), 'stale renewal leaves successor value untouched');
is($lease->release, 'already_not_owned', 'stale release cannot remove successor');
is($redis->{store}{$lease->{key}}, 'dilock/1/' . ('f' x 64), 'successor value preserved');
delete $redis->{store}{$lease->{key}};
my $expired = $handle->try_acquire(ttl_ms => 1000);
ok($expired, 'lease acquired for local-expiry renewal test');
$expired->{valid_until} = Time::HiRes::clock_gettime(Time::HiRes::CLOCK_MONOTONIC()) - 0.001;
eval { $expired->renew };
isa_ok($@, 'Dilock::LeaseLost', 'renewal is rejected after the confirmed local deadline');
is($expired->state, 'lost', 'expired lease remains terminal');
$expired->release;
my $uncertain_renewal = $handle->try_acquire(ttl_ms => 1000);
ok($uncertain_renewal, 'lease acquired for ambiguous-renewal test');
$redis->{unknown_after_renew} = 1;
eval { $uncertain_renewal->renew };
isa_ok($@, 'Dilock::BackendUnavailable', 'lost renewal acknowledgement is not reported as success');
is($uncertain_renewal->state, 'lost', 'ambiguous renewal ends local permission to work');
$uncertain_renewal->release;
my $uncertain = $handle->try_acquire(ttl_ms => 1000);
ok($uncertain, 'lease acquired for ambiguous release test');
$redis->{unknown_after_release} = 1;
is($uncertain->release, 'unknown', 'lost release response is reported as unknown');
my $release_calls = $redis->{release_calls};
is($uncertain->release, 'unknown', 'repeated release preserves unknown outcome');
is($redis->{release_calls}, $release_calls, 'repeated release does not retry backend command');
my @scoped_values = $handle->with_lease(
    sub { return ('invoice-42', 17) }, ttl_ms => 1000,
);
is_deeply(\@scoped_values, ['invoice-42', 17, 'released'],
    'with_lease preserves list-context callback values and appends the release outcome');
my $scalar_scoped_value = $handle->with_lease(sub { return ('first', 'discarded') }, ttl_ms => 1000);
is($scalar_scoped_value, 'discarded',
    'with_lease preserves Perl scalar-context callback semantics');
my $callback_context;
$handle->with_lease(sub {
    $callback_context = defined(wantarray) ? (wantarray ? 'list' : 'scalar') : 'void';
    return 'ignored';
}, ttl_ms => 1000);
is($callback_context, 'void', 'with_lease preserves void-context callback semantics');
$redis->{unknown_after_release} = 1;
eval { $handle->with_lease(sub { return 'work completed' }, ttl_ms => 1000) };
isa_ok($@, 'Dilock::ReleaseUnknown',
    'scalar-context scope does not hide an unknown release acknowledgement');
is($@->{outcome}, 'unknown', 'scalar-context release error preserves its outcome');

$redis->{unknown_after_release} = 1;
eval {
    $handle->with_lease(sub { return 'work completed' }, ttl_ms => 1000);
};
isa_ok($@, 'Dilock::ReleaseUnknown',
    'void-context scope does not hide an unknown release acknowledgement');

$redis->{force_release_not_owned} = 1;
eval { $handle->with_lease(sub { return 'work completed' }, ttl_ms => 1000) };
isa_ok($@, 'Dilock::LeaseLost',
    'scalar-context scope reports that ownership was already lost at cleanup');
is($@->{outcome}, 'already_not_owned', 'ownership-loss exception preserves release outcome');
my $body_failure;
eval {
    $handle->with_lease(sub { die "primary operation failed\n" }, ttl_ms => 1000);
};
$body_failure = $@;
is($body_failure, "primary operation failed\n",
    'a body exception remains unchanged when lease release is acknowledged');

$redis->{unknown_after_release} = 1;
eval {
    $handle->with_lease(sub { die "primary operation failed\n" }, ttl_ms => 1000);
};
isa_ok($@, 'Dilock::ScopeError',
    'simultaneous body and cleanup failures are reported together');
is($@->cause, "primary operation failed\n", 'scope error retains the body exception as its cause');
is($@->release_outcome, 'unknown', 'scope error retains the unconfirmed cleanup outcome');
$redis->{delay_set_seconds} = 0.04;
my $late_wait_handle = $provider->lock('late-wait-acquisition');
eval { $late_wait_handle->acquire(wait_seconds => 0.01, ttl_ms => 1000) };
isa_ok($@, 'Dilock::WaitTimeout', 'late successful Redis SET is cleaned up and reported as wait timeout');
ok(!exists($redis->{store}{$late_wait_handle->{key}}), 'acknowledged owner cleanup removes the late acquisition');
$redis->{unknown_after_release} = 1;
eval { $late_wait_handle->acquire(wait_seconds => 0.01, ttl_ms => 1000) };
isa_ok($@, 'Dilock::AcquisitionUnknown', 'late acquisition with unacknowledged release remains unknown');
ok(!exists($redis->{store}{$late_wait_handle->{key}}), 'lost release reply may still have removed the owner key');
$redis->{delay_set_seconds} = 0;
$provider->close;

done_testing;

package Local::Redis;
sub new { bless { store => {} }, shift }
sub get { $_[0]{store}{$_[1]} }
sub type {
    my ($self, $key) = @_;
    if ($key =~ /:meta\z/) {
        die "metadata TYPE failed" if $self->{metadata_type_error};
        return $self->{metadata_type_override} if defined $self->{metadata_type_override};
        return exists($self->{store}{$key}) ? 'string' : 'none';
    }
    die "TYPE failed" if $self->{type_error};
    return $self->{type_override} if defined $self->{type_override};
    return exists($self->{store}{$key}) ? 'string' : 'none';
}
sub set {
    my ($self, $key, $value, @args) = @_;
    return undef if $self->{force_contention};
    return undef if exists $self->{store}{$key};
    $self->{store}{$key} = $value;
    select undef, undef, undef, $self->{delay_set_seconds} if $self->{delay_set_seconds};
    return 'OK';
}
sub eval {
    my ($self, $script, $keys, $key, $owner) = @_;
    if ($script =~ /PEXPIRE/) {
        $self->{renew_calls}++;
        return 0 unless defined($self->{store}{$key}) && $self->{store}{$key} eq $owner;
        die "renew response lost" if delete $self->{unknown_after_renew};
        return 1;
    }
    if (delete $self->{force_release_not_owned}) {
        delete $self->{store}{$key};
        return 0;
    }
    $self->{release_calls}++;
    return 0 unless defined($self->{store}{$key}) && $self->{store}{$key} eq $owner;
    delete $self->{store}{$key};
    die "release response lost" if delete $self->{unknown_after_release};
    return 1;
}
sub quit { 1 }
