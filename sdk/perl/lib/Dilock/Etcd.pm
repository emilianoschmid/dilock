package Dilock::Etcd;

use strict;
use warnings;
use 5.020;
use Encode qw(encode FB_CROAK);
use JSON::PP qw(decode_json);
use Time::HiRes qw(clock_gettime CLOCK_MONOTONIC);
use Dilock ();

our $VERSION = $Dilock::VERSION;

sub new {
    my ($class, %args) = @_;
    my $namespace = defined $args{namespace} ? $args{namespace} : 'default';
    Dilock::identity($namespace, 'validation');
    die "epoch must be non-empty and <= 1024 UTF-8 bytes" unless defined($args{epoch});
    my $epoch_bytes;
    eval { $epoch_bytes = encode('UTF-8', "$args{epoch}", FB_CROAK); 1 }
        or die "epoch must contain valid Unicode scalar values";
    die "epoch must be non-empty and <= 1024 UTF-8 bytes"
        unless length($epoch_bytes) >= 1 && length($epoch_bytes) <= 1024;
    die "endpoint or injected etcd client required" unless $args{client} || $args{endpoint};
    my $cleanup_seconds = $args{cleanup_seconds} // 2;
    die "cleanup_seconds must be positive" unless $cleanup_seconds =~ /\A(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)\z/ && $cleanup_seconds > 0;

    my $client = $args{client};
    if (!$client) {
        require EV::Etcd;
        $client = EV::Etcd->new(endpoints => [$args{endpoint}], max_retries => 0);
    }
    return bless {
        client => $client,
        namespace => $namespace,
        epoch => "$args{epoch}",
        cleanup_seconds => $cleanup_seconds,
        acquisitions => {},
        next_attempt => 0,
        closed => 0,
    }, $class;
}

sub lock {
    my ($self, $resource) = @_;
    die "provider is closed" if $self->{closed};
    my $id = Dilock::identity($self->{namespace}, $resource);
    return bless { provider => $self, name => "/dilock/locks/$id" }, 'Dilock::Etcd::Handle';
}

sub close {
    my ($self) = @_;
    return if $self->{closed};
    $self->{closed} = 1;
    for my $attempt (values %{ $self->{acquisitions} }) {
        Dilock::Etcd::Handle::_abandon($attempt, 'Cancelled', 'provider was closed');
    }
    return;
}

sub _metadata {
    my ($self, $callback) = @_;
    my $key = "/dilock/namespaces/$self->{namespace}/meta";
    $self->{client}->get($key, sub {
        my ($response, $error) = @_;
        return $callback->(undef, _error('BackendUnavailable', 'etcd namespace metadata read failed: ' . _error_detail($error))) if $error;
        my $kvs = ref($response) eq 'HASH' ? $response->{kvs} : undef;
        return $callback->(undef, _error('ProtocolMismatch', 'etcd namespace metadata is missing'))
            unless ref($kvs) eq 'ARRAY' && @$kvs == 1 && defined($kvs->[0]{value});
        my $metadata = eval { decode_json($kvs->[0]{value}) };
        return $callback->(undef, _error('ProtocolMismatch', 'etcd namespace metadata is invalid'))
            unless ref($metadata) eq 'HASH';
        my $protocol_major = $metadata->{protocol_major};
        return $callback->(undef, _error('ProtocolMismatch', 'incompatible etcd namespace metadata'))
            unless Dilock::_is_json_integer($protocol_major) && $protocol_major == 1;
        for my $field (qw(identity_scheme profile coordination_epoch)) {
            my $expected = $field eq 'identity_scheme' ? 'sha256-framed-utf8-v1'
                : $field eq 'profile' ? 'etcd-native-lock-v1' : $self->{epoch};
            my $actual = $metadata->{$field};
            return $callback->(undef, _error('ProtocolMismatch', 'incompatible etcd namespace metadata'))
                unless Dilock::_is_json_string($actual) && $actual eq $expected;
        }
        return $callback->(undef, _error('ProtocolMismatch', 'incompatible etcd namespace metadata'))
            unless exists($metadata->{fixed_lease_ms}) && !defined($metadata->{fixed_lease_ms});
        return $callback->(1, undef);
    });
}

sub _error {
    my ($class, $message) = @_;
    return bless { message => $message }, "Dilock::Etcd::$class";
}

sub _error_detail {
    my ($error) = @_;
    return 'unknown transport error' unless defined $error;
    return "$error" unless ref($error) eq 'HASH';
    my @parts;
    push @parts, 'code=' . $error->{code} if defined $error->{code};
    push @parts, 'message=' . $error->{message} if defined $error->{message};
    push @parts, 'status=' . $error->{status} if defined $error->{status};
    push @parts, 'source=' . $error->{source} if defined $error->{source};
    return @parts ? join(', ', @parts) : 'etcd RPC returned a structured error';
}

package Dilock::Etcd::Handle;
use strict;
use warnings;
use EV ();

sub try_acquire {
    my ($self, $callback) = @_;
    die "try_acquire requires a callback" unless ref($callback) eq 'CODE';
    $callback->(undef, Dilock::Etcd::_error('UnsupportedCapability', 'etcd native Lock has no non-blocking try operation'));
    return;
}

sub acquire {
    my ($self, $options, $callback) = @_;
    $options ||= {};
    die "acquire options must be a hash reference" unless ref($options) eq 'HASH';
    die "acquire requires a callback" unless ref($callback) eq 'CODE';
    my $provider = $self->{provider};
    return $callback->(undef, Dilock::Etcd::_error('BackendUnavailable', 'provider is closed')) if $provider->{closed};

    my $ttl = $options->{ttl_seconds} // 30;
    my $wait = $options->{wait_seconds} // 5;
    die "ttl_seconds must be an integer in 1..86400" unless $ttl =~ /\A[0-9]+\z/ && $ttl >= 1 && $ttl <= 86_400;
    die "wait_seconds must be a positive finite number" unless $wait =~ /\A(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)\z/ && $wait > 0;
    return $callback->(undef, Dilock::Etcd::_error('UnsupportedCapability', 'automatic renewal after acquisition is not supported'))
        if $options->{auto_renew};

    my $attempt_id = ++$provider->{next_attempt};
    my $attempt = {
        id => $attempt_id, provider => $provider, done => 0, callback => $callback,
        client => $provider->{client}, cleanup_seconds => $provider->{cleanup_seconds},
    };
    $provider->{acquisitions}{$attempt_id} = $attempt;
    $attempt->{wait_timer} = EV::timer($wait, 0, sub { _abandon($attempt, 'WaitTimeout', 'etcd lock wait expired') });
    $provider->_metadata(sub {
        my ($ok, $error) = @_;
        return _finish($attempt, undef, $error) if $attempt->{done};
        if ($error) {
            my $error_class = eval { $error->isa('Dilock::Etcd::ProtocolMismatch') }
                ? 'ProtocolMismatch' : 'BackendUnavailable';
            return _abandon($attempt, $error_class, "$error");
        }

        my $sent = Time::HiRes::clock_gettime(Time::HiRes::CLOCK_MONOTONIC());
        # Once LeaseGrant is dispatched, a missing/error response cannot prove
        # that etcd did not create a lease. Keep this phase explicit so timeout
        # and cancellation never misreport an ambiguous grant as ordinary
        # cancellation or wait expiry.
        $attempt->{grant_dispatched} = 1;
        $attempt->{client}->lease_grant($ttl, sub {
            my ($grant, $grant_error) = @_;
            if ($attempt->{done}) {
                _best_effort_revoke($attempt, $grant->{id})
                    if !$grant_error && ref($grant) eq 'HASH' && $grant->{id};
                return;
            }
            return _finish($attempt, undef, Dilock::Etcd::_error(
                'AcquisitionUnknown', 'etcd lease grant acknowledgement is unknown'))
                if $grant_error || ref($grant) ne 'HASH'
                    || !defined($grant->{id}) || "$grant->{id}" !~ /\A[1-9][0-9]*\z/;
            $attempt->{lease_id} = $grant->{id};
            return _abandon($attempt, 'ProtocolMismatch', 'etcd returned an invalid lease grant TTL')
                unless defined($grant->{ttl}) && "$grant->{ttl}" =~ /\A[1-9][0-9]*\z/ && $grant->{ttl} <= 86_400;
            my $effective_ttl = $grant->{ttl};
            my $margin = $effective_ttl < 4 ? $effective_ttl / 4 : 1;
            $attempt->{valid_until} = $sent + $effective_ttl - $margin;
            $attempt->{keepalive} = $attempt->{client}->lease_keepalive($attempt->{lease_id}, { auto_reconnect => 0 }, sub {
                my ($tick, $tick_error) = @_;
                my $now = Time::HiRes::clock_gettime(Time::HiRes::CLOCK_MONOTONIC());
                if ($now >= $attempt->{valid_until}) {
                    return _mark_lost($attempt->{lease}, 'etcd keepalive acknowledgement arrived after lease expiry')
                        if $attempt->{lease};
                    return _abandon($attempt, 'LeaseLost', 'etcd keepalive acknowledgement arrived after lease expiry');
                }
                if ($tick_error || ref($tick) ne 'HASH'
                    || !defined($tick->{id}) || "$tick->{id}" ne "$attempt->{lease_id}"
                    || !defined($tick->{ttl}) || "$tick->{ttl}" !~ /\A[1-9][0-9]*\z/ || $tick->{ttl} > 86400) {
                    return _mark_lost($attempt->{lease}, 'etcd lease keepalive was lost') if $attempt->{lease};
                    return _abandon($attempt, 'LeaseLost', 'etcd lease keepalive was lost');
                }
                # The stream callback does not expose its send timestamp. Subtract a
                # bounded transport margin and only extend the active attempt after an
                # acknowledged server keepalive tick.
                my $margin = $tick->{ttl} < 4 ? $tick->{ttl} / 4 : 1;
                $attempt->{valid_until} = Time::HiRes::clock_gettime(Time::HiRes::CLOCK_MONOTONIC()) + $tick->{ttl} - $margin;
                $attempt->{lease}->_refresh_deadline($tick->{ttl}) if $attempt->{lease};
            });
            $attempt->{client}->lock($self->{name}, $attempt->{lease_id}, sub {
                my ($locked, $lock_error) = @_;
                return if $attempt->{done};
                return _abandon($attempt, 'BackendUnavailable', 'etcd native Lock failed')
                    if $lock_error || ref($locked) ne 'HASH' || !defined($locked->{key});
                $attempt->{lock_key} = $locked->{key};
                $attempt->{client}->get($attempt->{lock_key}, sub {
                    # The wait timer or cancellation may have completed cleanup
                    # while this owner-verification RPC was still in flight.
                    # A late Range response must never publish a lease after
                    # the attempt has already reported a terminal result.
                    return if $attempt->{done};
                    my ($range, $range_error) = @_;
                    return _abandon($attempt, 'AcquisitionUnknown', 'etcd lock ownership verification failed')
                        if $range_error || ref($range) ne 'HASH' || ref($range->{kvs}) ne 'ARRAY' || @{$range->{kvs}} != 1;
                    my $kv = $range->{kvs}[0];
                    return _abandon($attempt, 'ProtocolMismatch', 'etcd Range did not return the exact opaque Lock key')
                        unless defined($kv->{key}) && $kv->{key} eq $attempt->{lock_key};
                    return _abandon($attempt, 'ProtocolMismatch', 'etcd returned a lock key for another lease')
                        unless defined($kv->{lease}) && "$kv->{lease}" eq "$attempt->{lease_id}";
                    my $revision = $kv->{create_revision};
                    return _abandon($attempt, 'ProtocolMismatch', 'etcd lock key has no valid create_revision')
                        unless defined($revision) && "$revision" =~ /\A[1-9][0-9]*\z/;
                    return _abandon($attempt, 'AcquisitionUnknown', 'etcd lease expired before ownership verification')
                        if Time::HiRes::clock_gettime(Time::HiRes::CLOCK_MONOTONIC()) >= $attempt->{valid_until};

                    $attempt->{done} = 1;
                    delete $provider->{acquisitions}{$attempt->{id}};
                    $attempt->{wait_timer}->stop;
                    $attempt->{keepalive}->cancel(sub { }) if $attempt->{keepalive};
                    my $lease = bless {
                        client => $attempt->{client}, lease_id => $attempt->{lease_id},
                        lock_key => $attempt->{lock_key}, keepalive => $attempt->{keepalive},
                        valid_until => $attempt->{valid_until}, revision => "$revision",
                        epoch => $provider->{epoch}, state => 'held', on_loss => $options->{on_loss},
                        cleanup_seconds => $provider->{cleanup_seconds},
                    }, 'Dilock::Etcd::Lease';
                    $attempt->{lease} = $lease;
                    $lease->_arm_expiry;
                    $attempt->{callback}->($lease, undef);
                });
            });
        });
    });

    return bless { attempt => $attempt }, 'Dilock::Etcd::Acquisition';
}

sub _finish {
    my ($attempt, $value, $error) = @_;
    return if $attempt->{done};
    $attempt->{done} = 1;
    delete $attempt->{provider}{acquisitions}{$attempt->{id}};
    $attempt->{wait_timer}->stop if $attempt->{wait_timer};
    $attempt->{callback}->($value, $error);
}

sub _best_effort_revoke {
    my ($attempt, $lease_id) = @_;
    my $finished = 0;
    my $timer = EV::timer($attempt->{cleanup_seconds}, 0, sub { $finished = 1 });
    $attempt->{client}->lease_revoke($lease_id, sub {
        return if $finished;
        $finished = 1;
        $timer->stop;
    });
}

sub _abandon {
    my ($attempt, $error_name, $message) = @_;
    return if $attempt->{done};
    if ($attempt->{grant_dispatched} && !$attempt->{lease_id}) {
        # No lease ID is available to revoke. A timeout/cancel during the RPC
        # therefore leaves the server-side grant outcome uncertain.
        my $reason = $error_name eq 'WaitTimeout' ? 'wait timeout' : lc($error_name);
        $error_name = 'AcquisitionUnknown';
        $message = "etcd lease grant acknowledgement is unknown after $reason";
    }
    return _finish($attempt, undef, Dilock::Etcd::_error($error_name, $message)) unless $attempt->{lease_id};
    $attempt->{done} = 1;
    $attempt->{wait_timer}->stop if $attempt->{wait_timer};
    my $finished = 0;
    my $cleanup_timer = EV::timer($attempt->{cleanup_seconds}, 0, sub {
        return if $finished;
        $finished = 1;
        delete $attempt->{provider}{acquisitions}{$attempt->{id}};
        $attempt->{callback}->(undef, Dilock::Etcd::_error('AcquisitionUnknown', 'etcd acquisition cleanup acknowledgement is unknown'));
    });
    my $finish = sub {
        my ($response, $error) = @_;
        return if $finished;
        $finished = 1;
        $cleanup_timer->stop;
        delete $attempt->{provider}{acquisitions}{$attempt->{id}};
        my $error_class = $error ? 'AcquisitionUnknown' : $error_name;
        $attempt->{callback}->(undef, Dilock::Etcd::_error($error_class, $error ? 'etcd lease cleanup acknowledgement is unknown' : $message));
    };
    my $revoke = sub { $attempt->{client}->lease_revoke($attempt->{lease_id}, $finish) };
    if ($attempt->{keepalive}) { $attempt->{keepalive}->cancel(sub { $revoke->() }) }
    else { $revoke->() }
}

package Dilock::Etcd::Acquisition;
use strict;
use warnings;
sub cancel { Dilock::Etcd::Handle::_abandon($_[0]->{attempt}, 'Cancelled', 'etcd acquisition was cancelled') }

package Dilock::Etcd::Lease;
use strict;
use warnings;
use EV ();
use Time::HiRes qw(clock_gettime CLOCK_MONOTONIC);

sub fence { return { epoch => $_[0]->{epoch}, create_revision => $_[0]->{revision} } }
sub state {
    my ($self) = @_;
    $self->_expire if $self->{state} eq 'held' && clock_gettime(CLOCK_MONOTONIC()) >= $self->{valid_until};
    return $self->{state};
}

sub _arm_expiry {
    my ($self) = @_;
    my $delay = $self->{valid_until} - clock_gettime(CLOCK_MONOTONIC());
    return $self->_expire if $delay <= 0;
    $self->{expiry_timer}->stop if $self->{expiry_timer};
    $self->{expiry_timer} = EV::timer($delay, 0, sub { $self->_expire });
}

sub _expire {
    my ($self) = @_;
    return unless $self->{state} eq 'held';
    $self->{state} = 'lost';
    $self->{expiry_timer}->stop if $self->{expiry_timer};
    $self->{on_loss}->($self, 'lease_expired') if ref($self->{on_loss}) eq 'CODE';
}

sub _mark_lost { $_[0]->_expire }

sub _refresh_deadline {
    my ($self, $ttl) = @_;
    return unless $self->{state} eq 'held';
    if (clock_gettime(CLOCK_MONOTONIC()) >= $self->{valid_until}) {
        $self->_expire;
        return;
    }
    # Fail closed for a delayed keepalive response; the transport does not expose send time.
    my $margin = $ttl < 4 ? $ttl / 4 : 1;
    $self->{valid_until} = clock_gettime(CLOCK_MONOTONIC()) + $ttl - $margin;
    $self->_expire if $self->{valid_until} <= clock_gettime(CLOCK_MONOTONIC());
    $self->_arm_expiry if $self->{state} eq 'held';
}

sub release {
    my ($self, $callback) = @_;
    die "release requires a callback" unless ref($callback) eq 'CODE';
    return $callback->($self->{release_outcome} // 'already_not_owned', undef) if $self->{state} eq 'released';
    return $callback->('unknown', Dilock::Etcd::_error('BackendUnavailable', 'etcd release acknowledgement is unknown'))
        if $self->{state} eq 'unknown';
    if ($self->{state} eq 'releasing') {
        push @{ $self->{release_waiters} }, $callback;
        return;
    }
    $self->{state} = 'releasing';
    $self->{release_waiters} = [$callback];
    $self->{expiry_timer}->stop if $self->{expiry_timer};
    my $deadline = Time::HiRes::clock_gettime(Time::HiRes::CLOCK_MONOTONIC()) + $self->{cleanup_seconds};
    my $ownership = 'unknown';
    my $unlock_acknowledged = 0;
    my $revoke_result = 'unknown';
    my $finished = 0;
    my $cleanup_timer;
    my $classify = sub {
        return 'already_not_owned' if $ownership eq 'absent' || $ownership eq 'mismatch'
            || $revoke_result eq 'not_found';
        return 'released' if $ownership eq 'owned' && $unlock_acknowledged
            && $revoke_result eq 'acknowledged';
        return 'already_not_owned' if $revoke_result eq 'acknowledged';
        return 'unknown';
    };
    my $complete = sub {
        my ($error) = @_;
        return if $finished;
        $finished = 1;
        $cleanup_timer->stop if $cleanup_timer;
        my $result = $classify->();
        $error ||= Dilock::Etcd::_error('BackendUnavailable', 'etcd release acknowledgement is unknown')
            if $result eq 'unknown';
        $self->{state} = $result eq 'unknown' ? 'unknown' : 'released';
        $self->{release_outcome} = $result;
        my $waiters = delete $self->{release_waiters} || [];
        $_->($result, $error) for @$waiters;
    };
    my $revoke = sub {
        return if $finished;
        $complete->() if Time::HiRes::clock_gettime(Time::HiRes::CLOCK_MONOTONIC()) >= $deadline;
        return if $finished;
        my $sent = eval {
            $self->{client}->lease_revoke($self->{lease_id}, sub {
                return if $finished;
                my ($response, $error) = @_;
                $revoke_result = !$error && defined($response) ? 'acknowledged'
                    : _is_not_found_error($error) ? 'not_found' : 'unknown';
                $complete->();
            });
            1;
        };
        if (!$sent) {
            $revoke_result = 'unknown';
            $complete->();
        }
    };
    my $unlock = sub {
        my $sent = eval {
            $self->{client}->unlock($self->{lock_key}, sub {
                return if $finished;
                my ($response, $error) = @_;
                $unlock_acknowledged = 1 if !$error && defined($response);
                $revoke->();
            });
            1;
        };
        $revoke->() unless $sent;
    };
    $cleanup_timer = EV::timer($self->{cleanup_seconds}, 0, sub {
        return if $finished;
        $complete->();
    });
    my $start_release = sub {
        return if $finished;
        $complete->() if Time::HiRes::clock_gettime(Time::HiRes::CLOCK_MONOTONIC()) >= $deadline;
        return if $finished;
        my $sent = eval {
            $self->{client}->get($self->{lock_key}, sub {
                return if $finished;
                my ($response, $error) = @_;
                my $kvs = ref($response) eq 'HASH' ? $response->{kvs} : undef;
                if (!$error && ref($kvs) eq 'ARRAY') {
                    if (!@$kvs) {
                        $ownership = 'absent';
                    } elsif (@$kvs == 1 && defined($kvs->[0]{key})
                        && $kvs->[0]{key} eq $self->{lock_key}) {
                        $ownership = defined($kvs->[0]{lease})
                            && "$kvs->[0]{lease}" eq "$self->{lease_id}" ? 'owned' : 'mismatch';
                    }
                }
                return $revoke->() unless $ownership eq 'owned';
                return $revoke->() if Time::HiRes::clock_gettime(Time::HiRes::CLOCK_MONOTONIC()) >= $deadline;
                $unlock->();
            });
            1;
        };
        $revoke->() unless $sent;
    };
    if ($self->{keepalive}) { $self->{keepalive}->cancel(sub { $start_release->() }) }
    else { $start_release->() }
}

sub _is_not_found_error {
    my ($error) = @_;
    return 0 unless ref($error) eq 'HASH';
    return defined($error->{code}) && "$error->{code}" eq '5'
        || defined($error->{status}) && $error->{status} =~ /\ANOT_FOUND\z/i;
}

package Dilock::Etcd::Error;
use overload '""' => sub { $_[0]->{message} }, fallback => 1;
package Dilock::Etcd::BackendUnavailable; our @ISA = ('Dilock::Etcd::Error');
package Dilock::Etcd::ProtocolMismatch; our @ISA = ('Dilock::Etcd::Error');
package Dilock::Etcd::AcquisitionUnknown; our @ISA = ('Dilock::Etcd::Error');
package Dilock::Etcd::UnsupportedCapability; our @ISA = ('Dilock::Etcd::Error');
package Dilock::Etcd::WaitTimeout; our @ISA = ('Dilock::Etcd::Error');
package Dilock::Etcd::Cancelled; our @ISA = ('Dilock::Etcd::Error');
package Dilock::Etcd::LeaseLost; our @ISA = ('Dilock::Etcd::Error');

1;
