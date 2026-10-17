package Dilock::Consul;

use strict;
use warnings;
use 5.020;
use Digest::SHA qw(sha256_hex);
use Encode qw(encode FB_CROAK);
use HTTP::Tiny;
use JSON::PP qw(decode_json encode_json);
use MIME::Base64 qw(decode_base64);
use Time::HiRes qw(clock_gettime sleep CLOCK_MONOTONIC);
use Dilock ();

our $VERSION = '0.1.0';
my $MAX_MS = 86_400_000;
my $MIN_TTL_MS = 10_000;
my $JSON = JSON::PP->new->allow_bignum;

sub _error {
    my ($class, $message) = @_;
    die bless { message => $message }, $class;
}

sub _decode_json { $JSON->decode($_[0]) }

sub _identity {
    my ($namespace, $resource) = @_;
    die "invalid namespace" unless defined($namespace) && $namespace =~ /\A[a-z0-9][a-z0-9_.-]{0,63}\z/;
    my ($ns, $name);
    eval { $ns = encode('UTF-8', $namespace, FB_CROAK); $name = encode('UTF-8', $resource, FB_CROAK); 1 }
        or die "namespace and resource must be valid UTF-8";
    die "resource must be 1..1024 UTF-8 bytes without NUL" if !length($name) || length($name) > 1024 || index($name, "\0") >= 0;
    return sha256_hex("dilock.lock.identity\0" . pack('N', length($ns)) . $ns . pack('N', length($name)) . $name);
}

sub defaults {
    my ($class, $epoch) = @_;
    return {
        endpoint => 'http://127.0.0.1:8500', namespace => 'default', coordination_epoch => $epoch,
        rpc_timeout => 2, cleanup_timeout => 2, poll_interval_ms => 50,
        wait_ms => 5_000, lease_ms => 30_000,
    };
}

sub new {
    my ($class, %args) = @_;
    my $defaults = $class->defaults($args{coordination_epoch});
    my $self = { %$defaults, %args, leases => {}, connected => 0, closed => 0 };
    die "Consul endpoint must be an HTTP(S) authority without userinfo or path" unless
        defined($self->{endpoint}) && $self->{endpoint} =~ m{\Ahttps?://(?:\[[0-9A-Fa-f:]+\]|[A-Za-z0-9.-]+)(?::[0-9]{1,5})?/?\z};
    _identity($self->{namespace}, 'validation');
    my $epoch_value = $self->{coordination_epoch};
    my $epoch = eval { encode('UTF-8', $epoch_value, FB_CROAK) };
    die "coordination_epoch must be 1..1024 UTF-8 bytes" unless defined($epoch) && length($epoch) >= 1 && length($epoch) <= 1024 && index($epoch, "\0") < 0;
    die "ACL token must be printable non-space ASCII" if defined($self->{acl_token}) && $self->{acl_token} !~ /\A[!-~]+\z/;
    for my $key (qw(rpc_timeout cleanup_timeout)) {
        die "$key must be finite and in (0,86400]" unless defined($self->{$key}) && $self->{$key} > 0 && $self->{$key} <= 86_400;
    }
    die "poll_interval_ms must be in 1..86400000" unless $self->{poll_interval_ms} =~ /\A[0-9]+\z/ && $self->{poll_interval_ms} > 0 && $self->{poll_interval_ms} <= $MAX_MS;
    return bless $self, $class;
}

sub _http {
    my ($self, $method, $path, $content, $timeout, $mutation) = @_;
    my %headers = (Accept => 'application/json');
    $headers{'Content-Type'} = 'application/json' if defined $content;
    $headers{'X-Consul-Token'} = $self->{acl_token} if defined $self->{acl_token};
    my $url = $self->{endpoint}; $url =~ s{/\z}{};
    my $http = HTTP::Tiny->new(timeout => $timeout, max_redirect => 0, verify_SSL => 1, keep_alive => 0);
    my $response = eval { $http->request($method, $url . $path, { headers => \%headers, (defined($content) ? (content => $content) : ()) }) };
    return undef if $response && $response->{status} == 404 && $method eq 'GET';
    if (!$response || !$response->{success}) {
        my $status = $response ? $response->{status} : 'transport failure';
        my $unknown = $mutation && (!$response || $status !~ /\A4[0-9][0-9]\z/);
        _error($unknown ? 'Dilock::AcquisitionUnknown' : 'Dilock::BackendUnavailable', "Consul $method $path failed ($status)");
    }
    return $response->{content} // '';
}

sub _metadata_key { 'dilock/namespaces/' . $_[0]->{namespace} . '/meta' }
sub _expected_metadata {
    my ($self) = @_;
    return { protocol_major => 1, identity_scheme => 'sha256-framed-utf8-v1', profile => 'consul-kv-session-v1', coordination_epoch => $self->{coordination_epoch}, fixed_lease_ms => undef };
}

sub _read_kv {
    my ($self, $key, $timeout) = @_;
    my $raw = eval { $self->_http('GET', "/v1/kv/$key?consistent", undef, $timeout, 0) };
    die $@ if $@;
    return undef unless defined $raw;
    my $rows = eval { _decode_json($raw) };
    _error('Dilock::ProtocolMismatch', 'Consul returned malformed KV JSON') if $@;
    _error('Dilock::ProtocolMismatch', 'Consul returned an unexpected KV row set') unless ref($rows) eq 'ARRAY' && @$rows == 1 && ref($rows->[0]) eq 'HASH';
    my $row = $rows->[0];
    _error('Dilock::ProtocolMismatch', 'Consul KV row omitted its key or LockIndex') unless defined($row->{Key}) && $row->{Key} eq $key && defined($row->{LockIndex}) && "$row->{LockIndex}" =~ /\A[0-9]+\z/;
    return $row;
}

sub _check_metadata {
    my ($self) = @_;
    my $row = $self->_read_kv($self->_metadata_key, $self->{rpc_timeout});
    Dilock::Consul::_error('Dilock::ProtocolMismatch', 'Consul namespace metadata is missing or malformed') unless $row && defined($row->{Value});
    _error('Dilock::ProtocolMismatch', 'Consul metadata Value is malformed base64')
        unless $row->{Value} =~ /\A(?:[A-Za-z0-9+\/]{4})*(?:[A-Za-z0-9+\/]{2}==|[A-Za-z0-9+\/]{3}=)?\z/;
    my $decoded = decode_base64($row->{Value});
    my $actual = eval { _decode_json($decoded) };
    my $expected = $self->_expected_metadata;
    Dilock::Consul::_error('Dilock::ProtocolMismatch', 'Consul namespace metadata is incompatible') unless ref($actual) eq 'HASH'
        && join(',', sort keys %$actual) eq join(',', sort keys %$expected)
        && Dilock::_is_json_integer($actual->{protocol_major}) && $actual->{protocol_major} == 1
        && Dilock::_is_json_string($actual->{identity_scheme})
        && $actual->{identity_scheme} eq $expected->{identity_scheme}
        && Dilock::_is_json_string($actual->{profile})
        && $actual->{profile} eq $expected->{profile}
        && Dilock::_is_json_string($actual->{coordination_epoch})
        && $actual->{coordination_epoch} eq $expected->{coordination_epoch}
        && exists($actual->{fixed_lease_ms}) && !defined($actual->{fixed_lease_ms});
    return 1;
}

sub provision_namespace {
    my ($self) = @_;
    my $body = JSON::PP::encode_json($self->_expected_metadata);
    my $result = $self->_http('PUT', '/v1/kv/' . $self->_metadata_key . '?cas=0', $body, $self->{rpc_timeout}, 1);
    return 1 if $result =~ /\A\s*true\s*\z/;
    return 1 if $self->_check_metadata;
    _error('Dilock::AcquisitionUnknown', 'metadata CAS was not acknowledged and existing metadata is incompatible');
}

sub connect {
    my ($self) = @_;
    die "provider is closed" if $self->{closed};
    $self->_check_metadata;
    $self->{connected} = 1;
    return $self;
}

sub lock {
    my ($self, $resource) = @_;
    die "provider is closed" if $self->{closed};
    die "provider must connect after metadata provisioning" unless $self->{connected};
    return bless { provider => $self, key => 'dilock/locks/' . Dilock::Consul::_identity($self->{namespace}, $resource) }, 'Dilock::Consul::Handle';
}

sub close {
    my ($self) = @_;
    $self->{closed} = 1;
    $_->release for values %{ $self->{leases} };
    return 1;
}

package Dilock::Consul::Handle;
use strict;
use warnings;
use Time::HiRes qw(clock_gettime sleep CLOCK_MONOTONIC);

sub acquire {
    my ($self, %args) = @_;
    my $provider = $self->{provider};
    my $wait_ms = $args{wait_ms} // $provider->{wait_ms};
    my $requested = $args{lease_ms} // $provider->{lease_ms};
    die "wait_ms must be in 1..86400000" unless $wait_ms =~ /\A[0-9]+\z/ && $wait_ms >= 1 && $wait_ms <= $MAX_MS;
    die "lease_ms must be in 10000..86400000" unless $requested =~ /\A[0-9]+\z/ && $requested >= 10_000 && $requested <= $MAX_MS;
    my $ttl = int(($requested + 999) / 1000) * 1000;
    my $margin = $ttl / 100; $margin = 100 if $margin < 100;
    die "RPC timeout plus safety margin must be less than the effective Consul TTL"
        if $provider->{rpc_timeout} * 1000 + $margin >= $ttl;
    my $deadline = clock_gettime(CLOCK_MONOTONIC) + $wait_ms / 1000;
    my $sent = clock_gettime(CLOCK_MONOTONIC);
    my $session_body = JSON::PP::encode_json({ Name => 'dilock-attempt', TTL => int($ttl / 1000) . 's', Behavior => 'release', LockDelay => '0s', Checks => [] });
    my $created = eval { $provider->_http('PUT', '/v1/session/create', $session_body, $provider->{rpc_timeout}, 1) };
    die $@ if $@;
    my $session = eval { Dilock::Consul::_decode_json($created)->{ID} };
    Dilock::Consul::_error('Dilock::Consul::AcquisitionUnknown', 'Consul session creation returned no ID') unless defined($session) && $session =~ /\A[A-Za-z0-9-]+\z/;
    my $lease_deadline = $sent + ($ttl - $margin) / 1000;
    my $next_renew = $sent + $ttl / 3000;
    my $cleanup = sub {
        my $raw = eval { $provider->_http('PUT', "/v1/session/destroy/$session", undef, $provider->{cleanup_timeout}, 1) };
        return 0 if $@;
        return $raw =~ /\A\s*true\s*\z/ ? 1 : 0;
    };
    my $lease;
    eval {
        while (clock_gettime(CLOCK_MONOTONIC) < $deadline) {
            my $remaining_ms = int(($deadline - clock_gettime(CLOCK_MONOTONIC)) * 1000);
            my $request_timeout = $remaining_ms / 1000;
            $request_timeout = 0.001 if $request_timeout < 0.001;
            $request_timeout = $provider->{rpc_timeout} if $request_timeout > $provider->{rpc_timeout};
            my $reply = $provider->_http('PUT', "/v1/kv/$self->{key}?acquire=$session", '', $request_timeout, 1);
            if ($reply =~ /\A\s*true\s*\z/) {
                my $row = $provider->_read_kv($self->{key}, $provider->{rpc_timeout});
                die bless({ message => 'Consul acquire acknowledgement ownership could not be confirmed' }, 'Dilock::Consul::AcquisitionUnknown')
                    unless $row && defined($row->{Session}) && $row->{Session} eq $session && $row->{LockIndex} =~ /\A[1-9][0-9]*\z/;
                die bless({ message => 'Consul acquire completed after its wait or lease deadline' }, 'Dilock::Consul::AcquisitionUnknown')
                    if clock_gettime(CLOCK_MONOTONIC) >= $deadline || clock_gettime(CLOCK_MONOTONIC) >= $lease_deadline;
                $lease = bless { provider => $provider, key => $self->{key}, session => $session, epoch => $provider->{coordination_epoch},
                    lock_index => "$row->{LockIndex}", ttl_ms => $ttl, deadline => $lease_deadline, state => 'held' }, 'Dilock::Consul::Lease';
                $provider->{leases}{"$lease"} = $lease;
                last;
            }
            die bless({ message => 'Consul acquire returned a malformed acknowledgement' }, 'Dilock::Consul::AcquisitionUnknown')
                unless $reply =~ /\A\s*false\s*\z/;
            if (clock_gettime(CLOCK_MONOTONIC) >= $next_renew) {
                my $renew_sent = clock_gettime(CLOCK_MONOTONIC);
                my $renewed = $provider->_http('PUT', "/v1/session/renew/$session", undef, $provider->{rpc_timeout}, 1);
                my $rows = eval { Dilock::Consul::_decode_json($renewed) };
                die bless({ message => 'Consul waiter session renewal failed' }, 'Dilock::Consul::LeaseLost')
                    unless ref($rows) eq 'ARRAY' && @$rows && $rows->[0]{ID} eq $session;
                $lease_deadline = $renew_sent + ($ttl - $margin) / 1000;
                $next_renew = clock_gettime(CLOCK_MONOTONIC) + $ttl / 3000;
            }
            my $sleep_seconds = $deadline - clock_gettime(CLOCK_MONOTONIC);
            sleep(($provider->{poll_interval_ms} / 1000) < $sleep_seconds
                ? $provider->{poll_interval_ms} / 1000 : $sleep_seconds) if $sleep_seconds > 0;
        }
        die bless({ message => 'Consul acquisition wait expired' }, 'Dilock::Consul::WaitTimeout') unless $lease;
    };
    my $error = $@;
    return $lease if $lease && !$error;
    my $cleaned = $cleanup->();
    if (ref($error) eq 'Dilock::Consul::WaitTimeout' && !$cleaned) {
        die bless({ message => 'Consul wait expired and attempt-session cleanup is unconfirmed' }, 'Dilock::Consul::AcquisitionUnknown');
    }
    die $error if $error;
    return;
}

sub try_acquire {
    my ($self, %args) = @_;
    my $lease = eval { $self->acquire(%args, wait_ms => 1) };
    return $lease unless $@;
    return undef if ref($@) eq 'Dilock::Consul::WaitTimeout';
    die $@;
}

sub with_lease {
    my ($self, $callback, %options) = @_;
    die "with_lease requires a callback" unless ref($callback) eq 'CODE';
    my $context = wantarray;
    my ($scalar_result, @list_result);
    my $lease = $self->acquire(%options);
    my $worked = eval {
        if (!defined $context) { $callback->($lease); }
        elsif ($context) { @list_result = $callback->($lease); }
        else { $scalar_result = $callback->($lease); }
        1;
    };
    my $work_error = $@;
    my $released = eval { $lease->release };
    my $release_error = $@;
    die $work_error unless $worked;
    Dilock::Consul::_error('Dilock::ReleaseUnknown', 'Consul lease release could not be confirmed')
        if $release_error || !defined($released) || $released eq 'unknown';
    Dilock::Consul::_error('Dilock::LeaseLost', 'Consul lease was no longer owned at scope exit')
        if $released ne 'released';
    return $context ? @list_result : $scalar_result;
}

package Dilock::Consul::Lease;
use strict;
use warnings;
use Time::HiRes qw(clock_gettime CLOCK_MONOTONIC);

sub valid { $_[0]->{state} eq 'held' && clock_gettime(CLOCK_MONOTONIC) < $_[0]->{deadline} }
sub effective_ttl_ms { $_[0]->{ttl_ms} }
sub fence { return { epoch => $_[0]->{epoch}, lock_index => $_[0]->{lock_index} } }
sub state { $_[0]->{state} }

sub renew {
    my ($self) = @_;
    my $provider = $self->{provider};
    Dilock::Consul::_error('Dilock::Consul::LeaseLost', 'lease is no longer locally valid') unless $self->valid;
    my $renewed = eval {
        my $sent = clock_gettime(CLOCK_MONOTONIC);
        my $raw = $provider->_http('PUT', "/v1/session/renew/$self->{session}", undef, $provider->{rpc_timeout}, 1);
        my $rows = Dilock::Consul::_decode_json($raw);
        Dilock::Consul::_error('Dilock::Consul::LeaseLost', 'Consul session renewal failed') unless ref($rows) eq 'ARRAY' && @$rows && $rows->[0]{ID} eq $self->{session};
        my $row = $provider->_read_kv($self->{key}, $provider->{rpc_timeout});
        Dilock::Consul::_error('Dilock::Consul::LeaseLost', 'Consul lease ownership changed') unless $row && $row->{Session} eq $self->{session} && "$row->{LockIndex}" eq $self->{lock_index};
        my $margin = $self->{ttl_ms} / 100; $margin = 100 if $margin < 100;
        $self->{deadline} = $sent + ($self->{ttl_ms} - $margin) / 1000;
        1;
    };
    if (!$renewed) { $self->{state} = 'lost'; die $@; }
    return 1;
}

sub release {
    my ($self) = @_;
    return $self->{outcome} if defined $self->{outcome};
    my $provider = $self->{provider};
    my $outcome = 'unknown';
    eval {
        my $raw = $provider->_http('PUT', "/v1/kv/$self->{key}?release=$self->{session}", '', $provider->{cleanup_timeout}, 1);
        if ($raw =~ /\A\s*true\s*\z/) { $outcome = 'released'; }
        elsif ($raw =~ /\A\s*false\s*\z/) {
            my $row = $provider->_read_kv($self->{key}, $provider->{cleanup_timeout});
            $outcome = $row && $row->{Session} eq $self->{session} ? 'unknown' : 'already_not_owned';
        } else { die "malformed Consul release acknowledgement"; }
        1;
    } or $outcome = 'unknown';
    my $destroyed = 0;
    eval {
        my $raw = $provider->_http('PUT', "/v1/session/destroy/$self->{session}", undef, $provider->{cleanup_timeout}, 1);
        $destroyed = $raw =~ /\A\s*true\s*\z/ ? 1 : 0;
        1;
    };
    $outcome = 'unknown' unless $destroyed;
    $self->{state} = 'released'; $self->{outcome} = $outcome;
    delete $provider->{leases}{"$self"};
    return $outcome;
}

1;
