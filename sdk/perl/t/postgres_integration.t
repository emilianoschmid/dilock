use strict;
use warnings;
use Test::More;

my $dsn = $ENV{DILOCK_POSTGRES_DSN};
plan skip_all => 'set DILOCK_POSTGRES_DSN to run PostgreSQL integration tests' unless $dsn;

eval { require DBI; require Dilock::Postgres; 1 }
    or plan skip_all => "DBI/DBD::Pg or Dilock::Postgres unavailable: $@";

my $namespace = 'perl' . unpack('H*', pack('N', $$)) . int(rand(1_000_000));
my $epoch = 'perl-postgres-' . $$ . '-' . int(rand(1_000_000));
my $resource = 'perl-postgres-live-' . $$ . '-' . int(rand(1_000_000));
my %connection = (
    dsn => $dsn,
    username => $ENV{DILOCK_POSTGRES_USER},
    password => $ENV{DILOCK_POSTGRES_PASSWORD},
    namespace => $namespace,
    epoch => $epoch,
);

is(Dilock::Postgres->provision_namespace(%connection), 'created', 'provisions namespace metadata');
my $first = Dilock::Postgres->new(%connection);
my $second = Dilock::Postgres->new(%connection);
my ($first_lease, $successor);
eval {
    my $first_handle = $first->lock($resource);
    my $second_handle = $second->lock($resource);
    $first_lease = $first_handle->try_acquire;
    ok($first_lease, 'first provider acquires a PostgreSQL advisory session');
    ok(!defined($second_handle->try_acquire), 'second provider observes contention');
    ok($first_lease->renew, 'renew is a liveness check on the held session');
    is($first_lease->release, 'released', 'owner releases on the same session');

    $successor = $second_handle->try_acquire;
    ok($successor, 'successor acquires after acknowledged release');
    is($successor->release, 'released', 'successor releases its own session');
    1;
} or do {
    my $error = $@ || 'unknown PostgreSQL integration failure';
    $first_lease->release if $first_lease;
    $successor->release if $successor;
    $first->close;
    $second->close;
    die $error;
};
$first->close;
$second->close;

done_testing;
