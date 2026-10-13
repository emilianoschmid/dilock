package Dilock;

use strict;
use warnings;
use 5.020;
use B ();
use Digest::SHA qw(sha256_hex);
use Encode qw(encode FB_CROAK);
use JSON::PP qw(decode_json);
use POSIX qw(isfinite);
use Scalar::Util qw(looks_like_number);
use Time::HiRes qw(clock_gettime CLOCK_MONOTONIC);

our $VERSION = '0.1.0';

our $RELEASE_SCRIPT = <<'LUA';
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
LUA

our $RENEW_SCRIPT = <<'LUA';
local ttl = tonumber(ARGV[2])
if not ttl or ttl ~= math.floor(ttl) or ttl < 1 or ttl > 86400000 then
    return redis.error_reply('DILOCK_INVALID_TTL')
end
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('PEXPIRE', KEYS[1], ttl)
end
return 0
LUA

sub identity {
    my ($namespace, $resource) = @_;
    die "invalid namespace" unless defined($namespace) && $namespace =~ /\A[a-z0-9][a-z0-9_.-]{0,63}\z/;
    die "invalid resource" unless defined($resource) && length($resource) && index($resource, "\0") < 0;
    my ($ns_bytes, $resource_bytes);
    eval {
        $ns_bytes = encode('UTF-8', "$namespace", FB_CROAK);
        $resource_bytes = encode('UTF-8', "$resource", FB_CROAK);
        1;
    } or die "namespace and resource must contain valid Unicode scalar values";
    die "resource exceeds 1024 UTF-8 bytes" if length($resource_bytes) > 1024;
    return sha256_hex("dilock.lock.identity\0" . pack('N', length($ns_bytes)) . $ns_bytes
        . pack('N', length($resource_bytes)) . $resource_bytes);
}

sub new {
    my ($class, %args) = @_;
    my $namespace = defined $args{namespace} ? $args{namespace} : 'default';
    identity($namespace, 'validation');
    die "epoch must be non-empty and <= 1024 UTF-8 bytes" unless defined($args{epoch});
    my $epoch_bytes;
    eval { $epoch_bytes = encode('UTF-8', "$args{epoch}", FB_CROAK); 1 }
        or die "epoch must contain valid Unicode scalar values";
    die "epoch must be non-empty and <= 1024 UTF-8 bytes"
        unless length($epoch_bytes) >= 1 && length($epoch_bytes) <= 1024;
    die "Redis endpoint or injected client required" unless $args{redis} || $args{server};
    my $redis = $args{redis};
    if (!$redis) {
        require Redis::Fast;
        $redis = Redis::Fast->new(
            server => $args{server},
            reconnect => 0,
            conservative_reconnect => 1,
            cnx_timeout => $args{connect_timeout} // 2,
            read_timeout => $args{read_timeout} // 2,
            write_timeout => $args{write_timeout} // 2,
            encoding => undef,
        );
    }
    return bless {
        namespace => $namespace,
        epoch => $args{epoch},
        redis => $redis,
        leases => {},
        closed => 0,
    }, $class;
}

sub lock {
    my ($self, $resource) = @_;
    die "provider is closed" if $self->{closed};
    my $id = identity($self->{namespace}, $resource);
    return bless { provider => $self, key => "dilock:{${id}}:lease" }, 'Dilock::Handle';
}

sub close {
    my ($self) = @_;
    return if $self->{closed};
    $self->{closed} = 1;
    for my $lease (values %{ $self->{leases} }) { $lease->release }
    eval { $self->{redis}->quit if $self->{redis}->can('quit') };
}

sub _metadata {
    my ($self) = @_;
    my $key = "dilock:namespace:$self->{namespace}:meta";
    my $type;
    eval { $type = $self->{redis}->type($key); 1 }
        or die bless({ message => 'namespace type check failed' }, 'Dilock::BackendUnavailable');
    die bless({ message => 'missing or incompatible namespace metadata type' }, 'Dilock::ProtocolMismatch')
        unless defined($type) && $type eq 'string';
    my $raw;
    eval { $raw = $self->{redis}->get($key); 1 }
        or die bless({ message => 'namespace read failed' }, 'Dilock::BackendUnavailable');
    die bless({ message => 'missing namespace metadata' }, 'Dilock::ProtocolMismatch') unless defined $raw;
    my $meta = eval { decode_json($raw) };
    die bless({ message => 'invalid namespace metadata' }, 'Dilock::ProtocolMismatch') unless ref($meta) eq 'HASH';
    my $protocol_major = $meta->{protocol_major};
    die bless({ message => 'incompatible namespace metadata' }, 'Dilock::ProtocolMismatch')
        unless _is_json_integer($protocol_major) && $protocol_major == 1;
    for my $field (qw(identity_scheme profile coordination_epoch)) {
        my $expected = $field eq 'identity_scheme' ? 'sha256-framed-utf8-v1'
            : $field eq 'profile' ? 'redis-single-authority-v1' : $self->{epoch};
        my $actual = $meta->{$field};
        die bless({ message => 'incompatible namespace metadata' }, 'Dilock::ProtocolMismatch')
            unless _is_json_string($actual) && $actual eq $expected;
    }
    die bless({ message => 'incompatible namespace metadata' }, 'Dilock::ProtocolMismatch')
        unless exists($meta->{fixed_lease_ms}) && !defined($meta->{fixed_lease_ms});
}

# JSON::PP preserves JSON scalar kinds in Perl's internal SV flags. Check those
# flags before string comparison so values such as "1", 1.0, and true cannot
# masquerade as the integer protocol version or a string metadata field.
sub _is_json_integer {
    my ($value) = @_;
    return 0 if !defined($value) || ref($value);
    my $flags = B::svref_2object(\$value)->FLAGS;
    return ($flags & B::SVf_IOK()) && !($flags & (B::SVf_NOK() | B::SVf_POK()));
}

sub _is_json_string {
    my ($value) = @_;
    return 0 if !defined($value) || ref($value);
    my $flags = B::svref_2object(\$value)->FLAGS;
    return ($flags & B::SVf_POK()) && !($flags & (B::SVf_NOK() | B::SVf_IOK()));
}

sub _remove_lease { delete $_[0]->{leases}{$_[1]} }

package Dilock::Handle;
use strict;
use warnings;

sub try_acquire {
    my ($self, %options) = @_;
    my $ttl_ms = $options{ttl_ms} // 30_000;
    die "ttl_ms must be an integer in 1..86400000" unless $ttl_ms =~ /\A[0-9]+\z/ && $ttl_ms >= 1 && $ttl_ms <= 86_400_000;
    die bless({ message => 'Redis does not provide strong fencing' }, 'Dilock::UnsupportedCapability')
        if $options{require_fencing};
    my $provider = $self->{provider};
    die "provider is closed" if $provider->{closed};
    $provider->_metadata;
    require Crypt::URandom;
    my $owner = 'dilock/1/' . unpack('H*', Crypt::URandom::urandom(32));
    my $reply;
    my $sent_at = Time::HiRes::clock_gettime(Time::HiRes::CLOCK_MONOTONIC());
    eval { $reply = $provider->{redis}->set($self->{key}, $owner, 'NX', 'PX', $ttl_ms); 1 }
        or do {
            eval { $provider->{redis}->eval($Dilock::RELEASE_SCRIPT, 1, $self->{key}, $owner) };
            die bless({ message => 'Redis acquisition acknowledgement is unknown' }, 'Dilock::AcquisitionUnknown');
        };
    if (!defined($reply)) { # Redis acknowledged SET NX contention.
        my $type;
        eval { $type = $provider->{redis}->type($self->{key}); 1 }
            or die bless({ message => 'Redis TYPE check failed after contention' }, 'Dilock::BackendUnavailable');
        return undef if defined($type) && ($type eq 'string' || $type eq 'none');
        die bless({ message => 'lock key has an incompatible Redis type' }, 'Dilock::ProtocolMismatch');
    }
    unless ($reply eq 'OK') {
        eval { $provider->{redis}->eval($Dilock::RELEASE_SCRIPT, 1, $self->{key}, $owner) };
        die bless({ message => 'invalid Redis acquisition acknowledgement' }, 'Dilock::AcquisitionUnknown');
    }
    if ((Time::HiRes::clock_gettime(Time::HiRes::CLOCK_MONOTONIC()) - $sent_at) * 1000 >= $ttl_ms) {
        eval { $provider->{redis}->eval($Dilock::RELEASE_SCRIPT, 1, $self->{key}, $owner) };
        die bless({ message => 'acquisition acknowledgement arrived after lease expiry' }, 'Dilock::AcquisitionUnknown');
    }
    my $valid_until = $sent_at + ($ttl_ms - _safety_margin_ms($ttl_ms)) / 1000;
    if (Time::HiRes::clock_gettime(Time::HiRes::CLOCK_MONOTONIC()) >= $valid_until) {
        my $cleanup = eval {
            $provider->{redis}->eval($Dilock::RELEASE_SCRIPT, 1, $self->{key}, $owner);
        };
        die bless({ message => 'Redis acquisition has no conservative validity budget' },
            'Dilock::AcquisitionUnknown')
            unless defined($cleanup) && ($cleanup == 0 || $cleanup == 1);
        die bless({ message => 'Redis lease is too short for its safety margin' }, 'Dilock::LeaseLost');
    }
    my $lease = bless {
        provider => $provider, key => $self->{key}, owner => $owner,
        ttl_ms => $ttl_ms, sent_at => $sent_at,
        valid_until => $valid_until,
        released => 0, state => 'held',
    }, 'Dilock::Lease';
    $provider->{leases}{"$lease"} = $lease;
    return $lease;
}

sub acquire {
    my ($self, %options) = @_;
    my $wait = $options{wait_seconds} // 1;
    die "wait_seconds must be finite and positive"
        unless Scalar::Util::looks_like_number($wait)
            && POSIX::isfinite($wait)
            && $wait > 0;
    my $deadline = Time::HiRes::clock_gettime(Time::HiRes::CLOCK_MONOTONIC()) + $wait;
    while (1) {
        my $lease = $self->try_acquire(%options);
        if ($lease) {
            if (Time::HiRes::clock_gettime(Time::HiRes::CLOCK_MONOTONIC()) >= $deadline) {
                my $outcome = $lease->release;
                die bless({ message => 'late Redis acquisition cleanup is unacknowledged' }, 'Dilock::AcquisitionUnknown')
                    if $outcome eq 'unknown';
                die bless({ message => 'Redis acquisition completed after its wait deadline' }, 'Dilock::WaitTimeout');
            }
            return $lease;
        }
        my $remaining = $deadline - Time::HiRes::clock_gettime(Time::HiRes::CLOCK_MONOTONIC());
        die bless({ message => 'acquisition wait deadline elapsed' }, 'Dilock::WaitTimeout') if $remaining <= 0;
        select undef, undef, undef, _contention_delay($remaining, rand());
    }
}

# Match other Redis SDKs: 20-50 ms jitter, never sleeping past the wait deadline.
sub _contention_delay {
    my ($remaining, $sample) = @_;
    my $delay = 0.020 + 0.030 * $sample;
    return $delay < $remaining ? $delay : $remaining;
}

sub _safety_margin_ms {
    my ($ttl_ms) = @_;
    my $percentage = int(($ttl_ms + 99) / 100);
    return $percentage > 10 ? $percentage : 10;
}

sub with_lease {
    my ($self, $code, %options) = @_;
    die "with_lease requires a code reference" unless ref($code) eq 'CODE';
    my $lease = $self->acquire(%options);
    my $wantarray = wantarray;
    my (@result, $result, $ok, $error);
    $ok = eval {
        if (!defined $wantarray) { $code->($lease) }
        elsif ($wantarray) { @result = $code->($lease) }
        else { $result = $code->($lease) }
        1;
    };
    $error = $@ unless $ok;
    my $release = $lease->release;
    unless ($ok) {
        die $error if defined($release) && $release eq 'released';
        die bless({
            cause => $error,
            release_outcome => $release,
            message => 'Redis lease scope body failed and cleanup was not confirmed',
        }, 'Dilock::ScopeError');
    }
    if (!defined($wantarray)) {
        _throw_scope_release_error($release);
        return;
    }
    return (@result, $release) if $wantarray;
    _throw_scope_release_error($release);
    return $result;
}

sub _throw_scope_release_error {
    my ($outcome) = @_;
    return if defined($outcome) && $outcome eq 'released';
    my $class = defined($outcome) && $outcome eq 'already_not_owned'
        ? 'Dilock::LeaseLost' : 'Dilock::ReleaseUnknown';
    my $message = $class eq 'Dilock::LeaseLost'
        ? 'Redis lease was no longer owned when its scope ended'
        : 'Redis lease release acknowledgement is unknown at scope exit';
    die bless({ message => $message, outcome => $outcome }, $class);
}

package Dilock::Lease;
use strict;
use warnings;
use Time::HiRes qw(clock_gettime CLOCK_MONOTONIC);

sub state {
    my ($self) = @_;
    if ($self->{state} eq 'held'
        && clock_gettime(CLOCK_MONOTONIC()) >= $self->{valid_until}) {
        $self->{state} = 'lost';
    }
    return $self->{state};
}

sub renew {
    my ($self, %options) = @_;
    my $ttl_ms = $options{ttl_ms} // $self->{ttl_ms};
    die "ttl_ms must be an integer in 1..86400000"
        unless defined($ttl_ms) && $ttl_ms =~ /\A[0-9]+\z/ && $ttl_ms >= 1 && $ttl_ms <= 86_400_000;
    die bless({ message => 'lease is no longer within its confirmed validity budget' }, 'Dilock::LeaseLost')
        unless $self->state eq 'held';

    my $sent_at = clock_gettime(CLOCK_MONOTONIC());
    my $reply;
    my $ok = eval {
        $reply = $self->{provider}{redis}->eval(
            $Dilock::RENEW_SCRIPT, 1, $self->{key}, $self->{owner}, $ttl_ms,
        );
        1;
    };
    unless ($ok) {
        $self->{state} = 'lost';
        die bless({ message => 'Redis renewal acknowledgement is unknown' }, 'Dilock::BackendUnavailable');
    }
    unless (defined($reply) && ($reply == 0 || $reply == 1)) {
        $self->{state} = 'lost';
        die bless({ message => 'Redis renewal returned an invalid acknowledgement' }, 'Dilock::BackendUnavailable');
    }
    if ($reply != 1
        || clock_gettime(CLOCK_MONOTONIC()) >= $self->{valid_until}) {
        $self->{state} = 'lost';
        return 0;
    }

    my $valid_until = $sent_at + ($ttl_ms - Dilock::Handle::_safety_margin_ms($ttl_ms)) / 1000;
    if (clock_gettime(CLOCK_MONOTONIC()) >= $valid_until) {
        $self->{state} = 'lost';
        return 0;
    }
    $self->{ttl_ms} = $ttl_ms;
    $self->{sent_at} = $sent_at;
    $self->{valid_until} = $valid_until;
    return 1;
}

sub release {
    my ($self) = @_;
    return $self->{release_outcome} if $self->{released};
    $self->{released} = 1;
    $self->{state} = 'releasing';
    my $reply;
    my $ok = eval { $reply = $self->{provider}{redis}->eval($Dilock::RELEASE_SCRIPT, 1, $self->{key}, $self->{owner}); 1 };
    $self->{provider}->_remove_lease("$self");
    $self->{release_outcome} = 'unknown'
        unless $ok && defined($reply) && ($reply == 0 || $reply == 1);
    $self->{release_outcome} = $reply == 1 ? 'released' : 'already_not_owned'
        if $ok && defined($reply) && ($reply == 0 || $reply == 1);
    $self->{state} = $self->{release_outcome} eq 'unknown' ? 'unknown' : 'released';
    return $self->{release_outcome};
}

package Dilock::BackendUnavailable;
use overload '""' => sub { $_[0]->{message} }, fallback => 1;
package Dilock::ProtocolMismatch;
use overload '""' => sub { $_[0]->{message} }, fallback => 1;
package Dilock::AcquisitionUnknown;
use overload '""' => sub { $_[0]->{message} }, fallback => 1;
package Dilock::UnsupportedCapability;
use overload '""' => sub { $_[0]->{message} }, fallback => 1;
package Dilock::WaitTimeout;
use overload '""' => sub { $_[0]->{message} }, fallback => 1;
package Dilock::LeaseLost;
use overload '""' => sub { $_[0]->{message} }, fallback => 1;
package Dilock::ReleaseUnknown;
use overload '""' => sub { $_[0]->{message} }, fallback => 1;
package Dilock::ScopeError;
use overload '""' => sub { $_[0]->{message} }, fallback => 1;
sub cause { $_[0]->{cause} }
sub release_outcome { $_[0]->{release_outcome} }

1;
