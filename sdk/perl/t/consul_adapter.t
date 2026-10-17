use strict;
use warnings;
use Test::More;
use lib qw(lib);
use Dilock;
use Dilock::Consul;
use JSON::PP qw(encode_json);
use MIME::Base64 qw(encode_base64);

package Local::Consul;
our @ISA = ('Dilock::Consul');
sub _http {
    my ($self, $method, $path) = @_;
    if ($path eq '/v1/session/create') { push @{$self->{calls}}, 'create'; return '{"ID":"session-1"}'; }
    if ($path =~ /\?acquire=/) { push @{$self->{calls}}, 'acquire'; return 'true'; }
    if ($path =~ m{/v1/session/renew/}) { push @{$self->{calls}}, 'renew'; return '[{"ID":"session-1"}]'; }
    if ($path =~ /\?release=/) { push @{$self->{calls}}, 'release'; return 'true'; }
    if ($path =~ m{/v1/session/destroy/}) { push @{$self->{calls}}, 'destroy'; return $self->{fail_destroy} ? 'false' : 'true'; }
    die "unexpected Consul mock request: $path";
}
sub _read_kv {
    my ($self, $key) = @_;
    return { Key => $key, Session => 'session-1', LockIndex => 7 } if $key =~ m{^dilock/locks/};
    return { Key => $key, LockIndex => 0, Value => MIME::Base64::encode_base64(JSON::PP::encode_json($self->_expected_metadata), '') };
}

package main;

my $defaults = Dilock::Consul->defaults('epoch-test');
is($defaults->{endpoint}, 'http://127.0.0.1:8500', 'Consul endpoint default');
is($defaults->{lease_ms}, 30_000, 'default session TTL');

for my $bad_endpoint ('ftp://localhost:8500', 'http://user:pass@localhost:8500', 'http://localhost:8500/v1') {
    my $ok = eval { Dilock::Consul->new(namespace => 'test', coordination_epoch => 'epoch', endpoint => $bad_endpoint); 1 };
    ok(!$ok, "reject unsafe endpoint $bad_endpoint");
}

is(Dilock::Consul::_identity('test', 'resource'), Dilock::identity('test', 'resource'), 'Consul uses shared framed resource identity');
my $meta = Dilock::Consul->new(namespace => 'test', coordination_epoch => 'epoch')->_expected_metadata;
is_deeply($meta, {
    protocol_major => 1,
    identity_scheme => 'sha256-framed-utf8-v1',
    profile => 'consul-kv-session-v1',
    coordination_epoch => 'epoch',
    fixed_lease_ms => undef,
}, 'Consul namespace metadata matches the backend profile');

my $mock = Local::Consul->new(namespace => 'test', coordination_epoch => 'epoch');
$mock->{connected} = 1;
my $handle = $mock->lock('resource');
my $lease = $handle->acquire(wait_ms => 1_000, lease_ms => 10_001);
isa_ok($lease, 'Dilock::Consul::Lease');
is($lease->effective_ttl_ms, 11_000, 'Consul TTL rounds up to whole seconds');
is_deeply($lease->fence, { epoch => 'epoch', lock_index => '7' }, 'fence retains the epoch and per-key LockIndex');
ok($lease->valid, 'acknowledged ownership is locally valid');
ok($lease->renew, 'session renewal confirms same owner and LockIndex');
is($lease->release, 'released', 'acknowledged owner release is reported');
is($lease->release, 'released', 'release outcome is idempotently cached');
is($handle->with_lease(sub { return 42 }, wait_ms => 1_000, lease_ms => 10_000), 42, 'scoped work preserves its scalar result after confirmed release');
is_deeply($mock->{calls}, [qw(create acquire renew release destroy create acquire release destroy)], 'session lifecycle has no key deletion or acquisition retry');

my $invalid_handle = bless { provider => $mock, key => 'dilock/locks/test' }, 'Dilock::Consul::Handle';
my $bad_ttl = eval { $invalid_handle->acquire(lease_ms => 9_999); 1 };
ok(!$bad_ttl, 'reject lease below Consul minimum before backend calls');

my $cleanup_mock = Local::Consul->new(namespace => 'test', coordination_epoch => 'epoch');
$cleanup_mock->{connected} = 1;
$cleanup_mock->{fail_destroy} = 1;
my $cleanup_lease = $cleanup_mock->lock('destroy-unconfirmed')->acquire(wait_ms => 1_000, lease_ms => 10_000);
is($cleanup_lease->release, 'unknown', 'unconfirmed session destruction makes release unknown');

done_testing();
