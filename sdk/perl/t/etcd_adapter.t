use strict;
use warnings;
use Test::More;
use lib qw(lib);
use Dilock::Etcd;

my $client = Local::Etcd->new;
my $provider = Dilock::Etcd->new(
    client => $client,
    namespace => 'test',
    epoch => 'etcd-test-epoch',
    cleanup_seconds => 0.1,
);
my $handle = $provider->lock('resource');
my ($lease, $error);
my $acquisition = $handle->acquire({ ttl_seconds => 10, wait_seconds => 2 }, sub {
    ($lease, $error) = @_;
});
ok($acquisition, 'acquire returns a cancellable acquisition handle');
is($error, undef, 'acquisition succeeds');
ok($lease, 'lease returned');
is_deeply($lease->fence, { epoch => 'etcd-test-epoch', create_revision => '41' }, 'fence uses exact key create_revision and configured epoch');
is_deeply($client->{leases}, [1], 'one lease is granted for this acquisition');
is($client->{lock_name}, '/dilock/locks/' . Dilock::identity('test', 'resource'), 'uses canonical lock name');

my ($release_result, $release_error);
$lease->release(sub { ($release_result, $release_error) = @_ });
is($release_result, 'released', 'release unlocks and revokes');
is($release_error, undef, 'release acknowledged');
is_deeply($client->{calls}, [qw(metadata grant keepalive lock owner release-range unlock revoke)], 'metadata, lease, native lock, exact release Range and cleanup order');
my ($repeated_release_result, $repeated_release_error);
$lease->release(sub { ($repeated_release_result, $repeated_release_error) = @_ });
is($repeated_release_result, 'released', 'repeated acknowledged release returns its cached outcome');
is($repeated_release_error, undef, 'cached release outcome has no error');
is_deeply($client->{calls}, [qw(metadata grant keepalive lock owner release-range unlock revoke)],
    'repeated release does not send Unlock or LeaseRevoke again');

for my $case (
    ['owned acknowledged Unlock and Revoke', 'owned', 'ack', 'ack', 'released', 1],
    ['owned Unlock acknowledgement with Revoke uncertainty', 'owned', 'ack', 'error', 'unknown', 1],
    ['owned Revoke-only acknowledgement', 'owned', 'error', 'ack', 'already_not_owned', 1],
    ['absent exact key', 'absent', 'ack', 'ack', 'already_not_owned', 0],
    ['mismatched exact key lease', 'mismatch', 'ack', 'ack', 'already_not_owned', 0],
    ['unknown Range with Revoke-only acknowledgement', 'error', 'ack', 'ack', 'already_not_owned', 0],
    ['unknown Range and unconfirmed Revoke', 'error', 'ack', 'error', 'unknown', 0],
    ['expiry after Range and no-op Unlock ACK with Revoke NOT_FOUND', 'owned', 'ack', 'not_found', 'already_not_owned', 1],
) {
    my ($label, $range, $unlock, $revoke, $expected, $unlock_calls) = @$case;
    my $release_client = bless {
        range => $range, unlock => $unlock, revoke => $revoke, calls => [], lease_id => 91,
    }, 'Local::ReleaseEtcd';
    my $release_lease = bless {
        client => $release_client, lease_id => 91, lock_key => '/locks/release-exact',
        state => 'held', cleanup_seconds => 1,
    }, 'Dilock::Etcd::Lease';
    my ($outcome, $error);
    $release_lease->release(sub { ($outcome, $error) = @_ });
    is($outcome, $expected, $label);
    is(scalar(grep { $_ eq 'unlock' } @{$release_client->{calls}}), $unlock_calls,
        "$label only dispatches Unlock after positive exact ownership");
    is($release_client->{range_key}, '/locks/release-exact', "$label ranges the exact opaque lock key");
}

my $ambiguous_client = bless { lease_id => 'lease-ambiguous' }, 'Local::AmbiguousReleaseEtcd';
my $ambiguous_lease = bless {
    client => $ambiguous_client, lease_id => 'lease-ambiguous', lock_key => '/locks/opaque',
    state => 'held', cleanup_seconds => 1,
}, 'Dilock::Etcd::Lease';
my @release_outcomes;
$ambiguous_lease->release(sub { push @release_outcomes, [@_] });
$ambiguous_lease->release(sub { push @release_outcomes, [@_] });
is($ambiguous_client->{unlock_calls}, 1, 'concurrent release requests share one Unlock');
$ambiguous_client->{unlock_callback}->(undef, { code => 14, message => 'reply unavailable' });
is_deeply([map { $_->[0] } @release_outcomes], [qw(already_not_owned already_not_owned)],
    'acknowledged lease revoke confirms no acquisition-owned lock remains after ambiguous Unlock');
is($ambiguous_lease->{state}, 'released', 'confirmed revoke makes the lease terminal');
my ($retry_outcome, $retry_error);
$ambiguous_lease->release(sub { ($retry_outcome, $retry_error) = @_ });
is($retry_outcome, 'already_not_owned', 'repeated release returns the cached terminal outcome');
is($retry_error, undef, 'confirmed revoke has no cleanup error');
is($ambiguous_client->{unlock_calls}, 1, 'confirmed cleanup does not retry Unlock');
is($ambiguous_client->{revoke_calls}, 1, 'confirmed cleanup does not retry Revoke');

my $unknown_client = bless { revoke_error => 1, lease_id => 'lease-unknown' }, 'Local::AmbiguousReleaseEtcd';
my $unknown_lease = bless {
    client => $unknown_client, lease_id => 'lease-unknown', lock_key => '/locks/unknown',
    state => 'held', cleanup_seconds => 1,
}, 'Dilock::Etcd::Lease';
my ($unknown_result, $unknown_error);
$unknown_lease->release(sub { ($unknown_result, $unknown_error) = @_ });
$unknown_client->{unlock_callback}->(undef, { code => 14, message => 'reply unavailable' });
is($unknown_result, 'unknown', 'release stays unknown when lease revoke acknowledgement is lost');
isa_ok($unknown_error, 'Dilock::Etcd::BackendUnavailable');
my ($unknown_retry, $unknown_retry_error);
$unknown_lease->release(sub { ($unknown_retry, $unknown_retry_error) = @_ });
is($unknown_retry, 'unknown', 'unknown terminal release result is cached');
is($unknown_client->{unlock_calls}, 1, 'unknown release does not retry Unlock');
is($unknown_client->{revoke_calls}, 1, 'unknown release does not retry Revoke');

my $unlock_ack_client = bless {}, 'Local::UnlockAckRevokeFailEtcd';
my $unlock_ack_lease = bless {
    client => $unlock_ack_client, lease_id => 'lease-unlock-ack', lock_key => '/locks/unlock-ack',
    state => 'held', cleanup_seconds => 1,
}, 'Dilock::Etcd::Lease';
my ($unlock_ack_result, $unlock_ack_error);
$unlock_ack_lease->release(sub { ($unlock_ack_result, $unlock_ack_error) = @_ });
is($unlock_ack_result, 'unknown', 'Unlock acknowledgement without positive Revoke is not Released');
isa_ok($unlock_ack_error, 'Dilock::Etcd::BackendUnavailable');

my $late_unlock_client = bless { lease_id => 'lease-late-ack' }, 'Local::AmbiguousReleaseEtcd';
my $late_unlock_lease = bless {
    client => $late_unlock_client, lease_id => 'lease-late-ack', lock_key => '/locks/late-ack',
    state => 'held', cleanup_seconds => 1,
}, 'Dilock::Etcd::Lease';
my @manual_timers;
my ($late_release_result, $late_release_error);
{
    no warnings 'redefine';
    local *EV::timer = sub {
        my ($after, $repeat, $callback) = @_;
        my $timer = bless { callback => $callback, stopped => 0 }, 'Local::ManualTimer';
        push @manual_timers, $timer;
        return $timer;
    };
    $late_unlock_lease->release(sub { ($late_release_result, $late_release_error) = @_ });
    is($late_unlock_client->{unlock_calls}, 1, 'release dispatches Unlock before its cleanup deadline');
    $manual_timers[-1]->fire;
    is($late_release_result, 'unknown', 'cleanup deadline makes a pending release terminal unknown');
    $late_unlock_client->{unlock_callback}->(undef, { code => 14, message => 'late response' });
    is($late_unlock_client->{revoke_calls} // 0, 0,
        'a late Unlock callback does not start Revoke after cleanup is terminal');
    is($late_unlock_lease->{state}, 'unknown', 'late response cannot change terminal lease state');
}

my $delayed_release_range_client = bless { lease_id => 93 }, 'Local::DelayedReleaseRangeEtcd';
my $delayed_release_range_lease = bless {
    client => $delayed_release_range_client, lease_id => 93, lock_key => '/locks/delayed-release-range',
    state => 'held', cleanup_seconds => 1,
}, 'Dilock::Etcd::Lease';
my @release_range_timers;
my ($delayed_range_result, $delayed_range_error);
{
    no warnings 'redefine';
    local *EV::timer = sub {
        my ($after, $repeat, $callback) = @_;
        my $timer = bless { callback => $callback, stopped => 0 }, 'Local::ManualTimer';
        push @release_range_timers, $timer;
        return $timer;
    };
    $delayed_release_range_lease->release(sub { ($delayed_range_result, $delayed_range_error) = @_ });
    is($delayed_release_range_client->{range_calls}, 1, 'release starts exact-key Range within its cleanup budget');
    is($delayed_release_range_client->{unlock_calls} // 0, 0, 'release waits for the ownership Range before Unlock');
    $release_range_timers[-1]->fire;
    is($delayed_range_result, 'unknown', 'cleanup deadline bounds a pending release Range');
    $delayed_release_range_client->{range_callback}->(
        { kvs => [{ key => '/locks/delayed-release-range', lease => 93 }] }, undef,
    );
    is($delayed_release_range_client->{unlock_calls} // 0, 0, 'late Range cannot start Unlock after cleanup ends');
    is($delayed_release_range_client->{revoke_calls} // 0, 0, 'late Range cannot start Revoke after cleanup ends');
}

my ($unsupported, $unsupported_error);
$handle->try_acquire(sub { ($unsupported, $unsupported_error) = @_ });
is($unsupported, undef, 'try acquire has no lease');
isa_ok($unsupported_error, 'Dilock::Etcd::UnsupportedCapability');

my $pending_provider = Dilock::Etcd->new(
    client => bless({}, 'Local::PendingEtcd'), namespace => 'test', epoch => 'etcd-test-epoch',
);
my ($pending_error);
$pending_provider->lock('pending')->acquire({}, sub { (undef, $pending_error) = @_ });
$pending_provider->close;
isa_ok($pending_error, 'Dilock::Etcd::Cancelled', 'closing a provider cancels pending acquisition');

my $late_client = bless({}, 'Local::LateGrantEtcd');
my $late_provider = Dilock::Etcd->new(client => $late_client, namespace => 'test', epoch => 'etcd-test-epoch');
my ($late_error, $late_callback_count);
my $late_attempt = $late_provider->lock('late-grant')->acquire({}, sub {
    my ($value, $failure) = @_;
    $late_error = $failure;
    $late_callback_count++;
});
$late_attempt->cancel;
isa_ok($late_error, 'Dilock::Etcd::AcquisitionUnknown',
    'cancelling after LeaseGrant dispatch reports the uncertain acquisition outcome');
like("$late_error", qr/unknown after cancelled/, 'ambiguous cancellation retains its cause');
$late_client->{grant_callback}->({ id => 81, ttl => 30 }, undef);
is($late_client->{revoked}, 81, 'a lease granted after cancellation is revoked in the background');
is($late_callback_count, 1, 'late cleanup does not invoke the acquisition callback twice');

my $grant_error_provider = Dilock::Etcd->new(
    client => bless({}, 'Local::FailedGrantEtcd'), namespace => 'test', epoch => 'etcd-test-epoch',
);
my ($grant_error);
$grant_error_provider->lock('grant-error')->acquire({}, sub { (undef, $grant_error) = @_ });
isa_ok($grant_error, 'Dilock::Etcd::AcquisitionUnknown',
    'an RPC error after LeaseGrant dispatch does not claim acquisition failed');

my $grant_timeout_client = bless({}, 'Local::LateGrantEtcd');
my $grant_timeout_provider = Dilock::Etcd->new(
    client => $grant_timeout_client, namespace => 'test', epoch => 'etcd-test-epoch',
);
my (@grant_timers, $grant_timeout_error);
{
    no warnings 'redefine';
    local *EV::timer = sub {
        my ($after, $repeat, $callback) = @_;
        my $timer = bless { callback => $callback, stopped => 0 }, 'Local::ManualTimer';
        push @grant_timers, $timer;
        return $timer;
    };
    my $timed_out_attempt = $grant_timeout_provider->lock('grant-timeout')->acquire(
        { ttl_seconds => 10, wait_seconds => 2 },
        sub { (undef, $grant_timeout_error) = @_ },
    );
    ok($timed_out_attempt, 'grant-pending acquisition remains cancellable');
    $grant_timers[0]->fire;
    isa_ok($grant_timeout_error, 'Dilock::Etcd::AcquisitionUnknown',
        'wait deadline during LeaseGrant reports an ambiguous outcome');
    $grant_timeout_client->{grant_callback}->({ id => 82, ttl => 30 }, undef);
    is($grant_timeout_client->{revoked}, 82,
        'late grant after timeout is revoked even though the caller received unknown');
}

my $late_range_client = bless {}, 'Local::LateRangeEtcd';
my $late_range_provider = Dilock::Etcd->new(
    client => $late_range_client, namespace => 'test', epoch => 'etcd-test-epoch',
    cleanup_seconds => 0.1,
);
my (@range_timers, @range_results);
{
    no warnings 'redefine';
    local *EV::timer = sub {
        my ($after, $repeat, $callback) = @_;
        my $timer = bless { callback => $callback, stopped => 0 }, 'Local::ManualTimer';
        push @range_timers, $timer;
        return $timer;
    };
    $late_range_provider->lock('late-range')->acquire(
        { ttl_seconds => 10, wait_seconds => 2 },
        sub { push @range_results, [@_] },
    );
    is(scalar @range_results, 0, 'acquisition remains pending while owner verification is delayed');
    $range_timers[0]->fire;
    is(scalar @range_results, 1, 'wait deadline completes the pending acquisition once');
    isa_ok($range_results[0][1], 'Dilock::Etcd::WaitTimeout');
    $late_range_client->{range_callback}->(
        { kvs => [{ key => '/locks/late-range', lease => 83, create_revision => '99', value => '' }] }, undef,
    );
    is(scalar @range_results, 1, 'late owner verification cannot complete the acquisition a second time');
    is($range_results[0][0], undef, 'late owner verification cannot publish a lease after timeout');
is($late_range_client->{revoked}, 83, 'timed-out attempt revokes its lease before late verification');
}

my $metadata_error_provider = Dilock::Etcd->new(
    client => bless({}, 'Local::MetadataErrorEtcd'), namespace => 'test', epoch => 'etcd-test-epoch',
);
my $metadata_error;
$metadata_error_provider->lock('metadata-error')->acquire({}, sub { (undef, $metadata_error) = @_ });
isa_ok($metadata_error, 'Dilock::Etcd::BackendUnavailable', 'metadata transport failures keep backend error classification');
like("$metadata_error", qr/code=14/, 'metadata error retains safe transport diagnostics');

my $invalid_grant_client = bless {}, 'Local::InvalidGrantEtcd';
my $invalid_grant_provider = Dilock::Etcd->new(
    client => $invalid_grant_client, namespace => 'test', epoch => 'etcd-test-epoch',
);
my $invalid_grant_error;
$invalid_grant_provider->lock('invalid-grant')->acquire(
    { ttl_seconds => 10, wait_seconds => 2 },
    sub { (undef, $invalid_grant_error) = @_ },
);
isa_ok($invalid_grant_error, 'Dilock::Etcd::ProtocolMismatch',
    'zero effective LeaseGrant TTL is not replaced with the requested TTL');
is($invalid_grant_client->{revoked}, 84,
    'a malformed grant with a usable lease ID is revoked before returning');

my $wrong_key_client = bless {}, 'Local::WrongOpaqueKeyEtcd';
my $wrong_key_provider = Dilock::Etcd->new(
    client => $wrong_key_client, namespace => 'test', epoch => 'etcd-test-epoch',
);
my $wrong_key_error;
$wrong_key_provider->lock('wrong-key')->acquire(
    { ttl_seconds => 10, wait_seconds => 2 },
    sub { (undef, $wrong_key_error) = @_ },
);
isa_ok($wrong_key_error, 'Dilock::Etcd::ProtocolMismatch',
    'Range ownership must match the exact opaque key returned by Lock');
is($wrong_key_client->{revoked}, 85,
    'wrong-key verification revokes its own attempt lease');

my $expired_keepalive_calls = 0;
my $expired_keepalive_lease = bless {
    state => 'held', valid_until => Time::HiRes::clock_gettime(Time::HiRes::CLOCK_MONOTONIC()) - 0.01,
    on_loss => sub { $expired_keepalive_calls++ },
}, 'Dilock::Etcd::Lease';
my $old_keepalive_deadline = $expired_keepalive_lease->{valid_until};
$expired_keepalive_lease->_refresh_deadline(30);
is($expired_keepalive_lease->{state}, 'lost', 'a late keepalive cannot revive a locally expired lease');
is($expired_keepalive_lease->{valid_until}, $old_keepalive_deadline,
    'a late keepalive cannot extend the old local deadline');
is($expired_keepalive_calls, 1, 'late keepalive expiry notifies loss once');

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
        . '"profile":"etcd-native-lock-v1","coordination_epoch":"etcd-test-epoch",'
        . '"fixed_lease_ms":null}';
    $metadata =~ s/("\Q$field\E"\s*:)\s*(?:"[^"]*"|[0-9.]+|true|false|null)/$1$replacement/;
    my $case_client = bless { metadata => $metadata }, 'Local::MetadataCaseEtcd';
    my $case_provider = Dilock::Etcd->new(
        client => $case_client, namespace => 'test', epoch => 'etcd-test-epoch',
    );
    my ($case_error);
    $case_provider->lock('metadata-type')->acquire({}, sub { (undef, $case_error) = @_ });
    isa_ok($case_error, 'Dilock::Etcd::ProtocolMismatch', "etcd metadata JSON type is strict: $name");
    is($case_client->{grant_calls} // 0, 0, "etcd rejects metadata before granting a lease: $name");
}

my $unconfirmed_cleanup_client = bless {}, 'Local::InvalidOwnerAndUnconfirmedCleanupEtcd';
my $unconfirmed_cleanup_provider = Dilock::Etcd->new(
    client => $unconfirmed_cleanup_client, namespace => 'test', epoch => 'etcd-test-epoch',
);
my $unconfirmed_cleanup_error;
$unconfirmed_cleanup_provider->lock('invalid-owner')->acquire(
    { ttl_seconds => 10, wait_seconds => 2 },
    sub { (undef, $unconfirmed_cleanup_error) = @_ },
);
isa_ok($unconfirmed_cleanup_error, 'Dilock::Etcd::AcquisitionUnknown',
    'invalid ownership verification with unacknowledged revoke remains an ambiguous acquisition');

my $timed_out_cleanup_client = bless {}, 'Local::FailedAttemptCleanupEtcd';
my $timed_out_cleanup_provider = Dilock::Etcd->new(
    client => $timed_out_cleanup_client, namespace => 'test', epoch => 'etcd-test-epoch',
    cleanup_seconds => 0.1,
);
my (@cleanup_timers, $timed_out_cleanup_error);
{
    no warnings 'redefine';
    local *EV::timer = sub {
        my ($after, $repeat, $callback) = @_;
        my $timer = bless { callback => $callback, stopped => 0 }, 'Local::ManualTimer';
        push @cleanup_timers, $timer;
        return $timer;
    };
    $timed_out_cleanup_provider->lock('cleanup-timeout')->acquire(
        { ttl_seconds => 10, wait_seconds => 2 },
        sub { (undef, $timed_out_cleanup_error) = @_ },
    );
    $cleanup_timers[-1]->fire;
}
isa_ok($timed_out_cleanup_error, 'Dilock::Etcd::AcquisitionUnknown',
    'attempt cleanup transport failure is not downgraded to ordinary backend failure');

$provider->close;
done_testing;

package Local::Etcd;
sub new { bless { leases => [], calls => [] }, shift }
sub get {
    my ($self, $key, $callback) = @_;
    if ($key =~ m{/meta\z}) {
        push @{$self->{calls}}, 'metadata';
        return $callback->({ kvs => [{ value => '{"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1","profile":"etcd-native-lock-v1","coordination_epoch":"etcd-test-epoch","fixed_lease_ms":null}' }] }, undef);
    }
    my $phase = $self->{owner_reads}++ ? 'release-range' : 'owner';
    push @{$self->{calls}}, $phase;
    $self->{range_key} = $key if $phase eq 'release-range';
    return $callback->({ kvs => [{ key => '/locks/opaque-1', lease => 1, create_revision => '41', value => '' }] }, undef);
}
sub lease_grant {
    my ($self, $ttl, $callback) = @_;
    push @{$self->{calls}}, 'grant';
    my $id = 1 + @{$self->{leases}};
    push @{$self->{leases}}, $id;
    return $callback->({ id => $id, ttl => $ttl }, undef);
}
sub lease_keepalive {
    my ($self, $id, $options, $callback) = @_;
    push @{$self->{calls}}, 'keepalive';
    return bless {}, 'Local::Etcd::Stream';
}
sub lock {
    my ($self, $name, $id, $callback) = @_;
    push @{$self->{calls}}, 'lock';
    $self->{lock_name} = $name;
    $self->{lock_key} = '/locks/opaque-1';
    return $callback->({ key => $self->{lock_key} }, undef);
}
sub unlock {
    my ($self, $key, $callback) = @_;
    push @{$self->{calls}}, 'unlock';
    return $callback->({}, undef);
}
sub lease_revoke {
    my ($self, $id, $callback) = @_;
    push @{$self->{calls}}, 'revoke';
    return $callback->({}, undef);
}
package Local::Etcd::Stream;
sub cancel { $_[1]->({}, undef) }
package Local::ReleaseEtcd;
sub get {
    my ($self, $key, $callback) = @_;
    push @{$self->{calls}}, 'range';
    $self->{range_key} = $key;
    return $callback->(undef, { code => 14, message => 'range unavailable' }) if $self->{range} eq 'error';
    return $callback->({ kvs => [] }, undef) if $self->{range} eq 'absent';
    my $lease = $self->{range} eq 'mismatch' ? 92 : $self->{lease_id};
    return $callback->({ kvs => [{ key => $key, lease => $lease }] }, undef);
}
sub unlock {
    my ($self, $key, $callback) = @_;
    push @{$self->{calls}}, 'unlock';
    return $callback->({}, undef) if $self->{unlock} eq 'ack';
    return $callback->(undef, { code => 14, message => 'unlock unavailable' });
}
sub lease_revoke {
    my ($self, $id, $callback) = @_;
    push @{$self->{calls}}, 'revoke';
    return $callback->({}, undef) if $self->{revoke} eq 'ack';
    return $callback->(undef, { code => 5, message => 'lease not found' }) if $self->{revoke} eq 'not_found';
    return $callback->(undef, { code => 14, message => 'revoke unavailable' });
}
package Local::PendingEtcd;
sub get { $_[0]{pending_get} = $_[2] }
package Local::LateGrantEtcd;
sub get {
    $_[2]->({ kvs => [{ value => '{"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1","profile":"etcd-native-lock-v1","coordination_epoch":"etcd-test-epoch","fixed_lease_ms":null}' }] }, undef);
}
sub lease_grant { $_[0]{grant_callback} = $_[2] }
sub lease_revoke { $_[0]{revoked} = $_[1]; $_[2]->({}, undef) }
package Local::FailedGrantEtcd;
sub get {
    $_[2]->({ kvs => [{ value => '{"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1","profile":"etcd-native-lock-v1","coordination_epoch":"etcd-test-epoch","fixed_lease_ms":null}' }] }, undef);
}
sub lease_grant { $_[2]->(undef, { code => 14, message => 'grant response unavailable' }) }
package Local::LateRangeEtcd;
sub get {
    my ($self, $key, $callback) = @_;
    if ($key =~ m{/meta\z}) {
        return $callback->({ kvs => [{ value => '{"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1","profile":"etcd-native-lock-v1","coordination_epoch":"etcd-test-epoch","fixed_lease_ms":null}' }] }, undef);
    }
    $self->{range_callback} = $callback;
}
sub lease_grant { $_[2]->({ id => 83, ttl => $_[1] }, undef) }
sub lease_keepalive { bless {}, 'Local::Etcd::Stream' }
sub lock { $_[3]->({ key => '/locks/late-range' }, undef) }
sub lease_revoke { $_[0]{revoked} = $_[1]; $_[2]->({}, undef) }
package Local::MetadataErrorEtcd;
sub get { $_[2]->(undef, { code => 14, message => 'unavailable' }) }
package Local::MetadataCaseEtcd;
sub get { $_[2]->({ kvs => [{ value => $_[0]{metadata} }] }, undef) }
sub lease_grant { $_[0]{grant_calls}++; $_[2]->({ id => 'should-not-grant', ttl => $_[1] }, undef) }
package Local::InvalidGrantEtcd;
sub get {
    my ($self, $key, $callback) = @_;
    return $callback->({ kvs => [{ value => '{"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1","profile":"etcd-native-lock-v1","coordination_epoch":"etcd-test-epoch","fixed_lease_ms":null}' }] }, undef)
        if $key =~ m{/meta\z};
    die 'ownership Range must not run after an invalid lease grant';
}
sub lease_grant { $_[2]->({ id => 84, ttl => 0 }, undef) }
sub lease_revoke { $_[0]{revoked} = $_[1]; $_[2]->({}, undef) }
package Local::WrongOpaqueKeyEtcd;
sub get {
    my ($self, $key, $callback) = @_;
    return $callback->({ kvs => [{ value => '{"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1","profile":"etcd-native-lock-v1","coordination_epoch":"etcd-test-epoch","fixed_lease_ms":null}' }] }, undef)
        if $key =~ m{/meta\z};
    return $callback->({ kvs => [{ key => '/locks/not-the-returned-key', lease => 86, create_revision => '41' }] }, undef);
}
sub lease_grant { $_[2]->({ id => 85, ttl => $_[1] }, undef) }
sub lease_keepalive { bless {}, 'Local::Etcd::Stream' }
sub lock { $_[3]->({ key => '/locks/expected-key' }, undef) }
sub lease_revoke { $_[0]{revoked} = $_[1]; $_[2]->({}, undef) }
package Local::InvalidOwnerAndUnconfirmedCleanupEtcd;
sub get {
    my ($self, $key, $callback) = @_;
    return $callback->({ kvs => [{ value => '{"protocol_major":1,"identity_scheme":"sha256-framed-utf8-v1","profile":"etcd-native-lock-v1","coordination_epoch":"etcd-test-epoch","fixed_lease_ms":null}' }] }, undef)
        if $key =~ m{/meta\z};
    return $callback->({ kvs => [{ lease => 88, create_revision => '42' }] }, undef);
}
sub lease_grant { $_[2]->({ id => 87, ttl => $_[1] }, undef) }
sub lease_keepalive { bless {}, 'Local::Etcd::Stream' }
sub lock { $_[3]->({ key => '/locks/opaque' }, undef) }
sub lease_revoke { $_[2]->(undef, { code => 14, message => 'revoke acknowledgement unavailable' }) }
package Local::FailedAttemptCleanupEtcd;
use parent -norequire, 'Local::InvalidOwnerAndUnconfirmedCleanupEtcd';
sub lease_revoke { $_[0]{revoke_callback} = $_[2] }
package Local::AmbiguousReleaseEtcd;
sub get {
    my ($self, $key, $callback) = @_;
    $self->{range_calls}++;
    $callback->({ kvs => [{ key => $key, lease => $_[0]{lease_id} // 'lease-ambiguous' }] }, undef);
}
sub unlock { $_[0]{unlock_calls}++; $_[0]{unlock_callback} = $_[2] }
sub lease_revoke {
    $_[0]{revoke_calls}++;
    return $_[2]->(undef, { code => 14, message => 'revoke reply unavailable' }) if $_[0]{revoke_error};
    $_[2]->({}, undef);
}
package Local::UnlockAckRevokeFailEtcd;
sub get { $_[2]->({ kvs => [{ key => $_[1], lease => 'lease-unlock-ack' }] }, undef) }
sub unlock { $_[2]->({}, undef) }
sub lease_revoke { $_[2]->(undef, { code => 14, message => 'revoke reply unavailable' }) }
package Local::DelayedReleaseRangeEtcd;
sub get { $_[0]{range_calls}++; $_[0]{range_callback} = $_[2] }
sub unlock { $_[0]{unlock_calls}++; $_[2]->({}, undef) }
sub lease_revoke { $_[0]{revoke_calls}++; $_[2]->({}, undef) }
package Local::ManualTimer;
sub fire {
    my ($self) = @_;
    return if $self->{stopped};
    $self->{stopped} = 1;
    $self->{callback}->();
}
sub stop { $_[0]{stopped} = 1 }
