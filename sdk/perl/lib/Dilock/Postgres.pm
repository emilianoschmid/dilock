package Dilock::Postgres;

use strict;
use warnings;
use 5.020;
use DBI;
use Digest::SHA qw(sha256_hex);
use Encode qw(encode FB_CROAK);
use Time::HiRes qw(clock_gettime CLOCK_MONOTONIC);

our $PROFILE = 'postgres-advisory-session-v1';
our $IDENTITY_SCHEME = 'sha256-framed-utf8-v1';

sub identity {
    my ($namespace, $resource) = @_;
    die 'invalid namespace' unless defined($namespace) && $namespace =~ /\A[a-z0-9][a-z0-9_.-]{0,63}\z/;
    my ($ns, $value);
    eval { $ns = encode('UTF-8', $namespace, FB_CROAK); $value = encode('UTF-8', $resource, FB_CROAK); 1 }
        or die 'namespace and resource must be valid UTF-8';
    die 'invalid resource' unless length($value) >= 1 && length($value) <= 1024 && index($value, "\0") < 0;
    return sha256_hex("dilock.lock.identity\0" . pack('N', length($ns)) . $ns . pack('N', length($value)) . $value);
}

sub advisory_key {
    my ($resource_id) = @_;
    die 'invalid resource identity' unless defined($resource_id) && $resource_id =~ /\A[0-9a-f]{64}\z/;
    my ($high, $low) = unpack('N2', pack('H*', substr($resource_id, 0, 16)));
    return $high >= 0x80000000 ? $high * 4294967296 + $low - 18446744073709551616 : $high * 4294967296 + $low;
}

sub new {
    my ($class, %args) = @_;
    my $namespace = $args{namespace} // 'default';
    identity($namespace, 'validation');
    die 'epoch must be non-empty' unless defined($args{epoch}) && length($args{epoch});
    my $dbh = DBI->connect($args{dsn}, $args{username}, $args{password}, { RaiseError => 1, PrintError => 0, AutoCommit => 1 });
    my $self = bless { dbi => $args{dsn}, username => $args{username}, password => $args{password}, namespace => $namespace, epoch => $args{epoch}, leases => {}, closed => 0 }, $class;
    eval { $self->_validate_metadata($dbh); 1 } or do { my $error = $@; eval { $dbh->disconnect }; die $error; };
    $dbh->disconnect;
    return $self;
}

sub provision_namespace {
    my ($class, %args) = @_;
    my $namespace = $args{namespace} // 'default';
    identity($namespace, 'validation');
    my $dbh = DBI->connect($args{dsn}, $args{username}, $args{password}, { RaiseError => 1, PrintError => 0, AutoCommit => 0 });
    my $created = 0;
    eval {
        $dbh->do("SELECT pg_advisory_xact_lock(hashtextextended('dilock:namespace-meta:v1', 0))");
        $dbh->do('CREATE TABLE IF NOT EXISTS dilock_namespace_meta (namespace text PRIMARY KEY, protocol_major integer NOT NULL, identity_scheme text NOT NULL, profile text NOT NULL, coordination_epoch text NOT NULL, fixed_lease_ms bigint NULL)');
        my $sth = $dbh->prepare('INSERT INTO dilock_namespace_meta (namespace, protocol_major, identity_scheme, profile, coordination_epoch, fixed_lease_ms) VALUES (?, 1, ?, ?, ?, NULL) ON CONFLICT (namespace) DO NOTHING');
        $sth->execute($namespace, $IDENTITY_SCHEME, $PROFILE, $args{epoch});
        $created = $sth->rows == 1;
        $sth->finish;
        $dbh->commit;
        1;
    } or do { my $error = $@; eval { $dbh->rollback }; eval { $dbh->disconnect }; die $error; };
    $dbh->disconnect;
    __PACKAGE__->new(%args);
    return $created ? 'created' : 'compatible-existing';
}

sub lock {
    my ($self, $resource) = @_;
    die 'provider is closed' if $self->{closed};
    return bless { provider => $self, resource_id => identity($self->{namespace}, $resource) }, 'Dilock::Postgres::Handle';
}

sub close {
    my ($self) = @_;
    return if $self->{closed};
    $self->{closed} = 1;
    $_->release for values %{ $self->{leases} };
}

sub _validate_metadata {
    my ($self, $dbh) = @_;
    my $sth = $dbh->prepare('SELECT protocol_major, identity_scheme, profile, coordination_epoch, fixed_lease_ms FROM dilock_namespace_meta WHERE namespace = ?');
    $sth->execute($self->{namespace});
    my $row = $sth->fetchrow_hashref;
    my $extra = $sth->fetchrow_hashref;
    my $compatible = $row
        && !$extra
        && $row->{protocol_major} == 1
        && $row->{identity_scheme} eq $IDENTITY_SCHEME
        && $row->{profile} eq $PROFILE
        && $row->{coordination_epoch} eq $self->{epoch}
        && !defined($row->{fixed_lease_ms});
    $sth->finish;
    die 'PostgreSQL namespace metadata is missing' unless $row && !$extra;
    die 'PostgreSQL namespace metadata is incompatible' unless $compatible;
}

package Dilock::Postgres::Handle;
use strict;
use warnings;
use Time::HiRes qw(clock_gettime CLOCK_MONOTONIC);

sub try_acquire {
    my ($self, %args) = @_;
    die 'fencing is unsupported by PostgreSQL advisory sessions' if $args{require_fencing};
    my $provider = $self->{provider};
    my $dbh = DBI->connect($provider->{dbi}, $provider->{username}, $provider->{password}, { RaiseError => 1, PrintError => 0, AutoCommit => 1 });
    my $key = Dilock::Postgres::advisory_key($self->{resource_id});
    my $sth = $dbh->prepare('SELECT pg_try_advisory_lock(?::bigint) AS acquired');
    eval { $sth->execute($key); 1 } or do { my $error = $@; eval { $dbh->disconnect }; die $error; };
    my ($acquired) = $sth->fetchrow_array;
    $sth->finish;
    unless ($acquired) { $dbh->disconnect; return undef; }
    my $lease = bless { provider => $provider, dbh => $dbh, key => $key, state => 'held', outcome => undef }, 'Dilock::Postgres::Lease';
    $provider->{leases}{"$lease"} = $lease;
    return $lease;
}

sub acquire {
    my ($self, %args) = @_;
    my $wait = $args{wait_seconds} // 5;
    my $deadline = clock_gettime(CLOCK_MONOTONIC()) + $wait;
    while (clock_gettime(CLOCK_MONOTONIC()) < $deadline) {
        my $lease = $self->try_acquire(%args);
        return $lease if $lease;
        select undef, undef, undef, 0.025;
    }
    die 'PostgreSQL acquisition wait budget exhausted';
}

sub with_lease {
    my ($self, $code, %args) = @_;
    die 'with_lease requires a code reference' unless ref($code) eq 'CODE';
    my $lease = $self->acquire(%args);
    my ($value, $error);
    eval { $value = $code->($lease); 1 } or $error = $@;
    my $outcome = $lease->release;
    die $error if $error;
    die 'PostgreSQL release acknowledgement is unknown' unless $outcome eq 'released';
    return $value;
}

package Dilock::Postgres::Lease;
use strict;
use warnings;

sub state { $_[0]->{state} }
sub ensure_held { die 'PostgreSQL session is no longer available' unless $_[0]->{state} eq 'held' }
sub renew {
    my ($self) = @_;
    $self->ensure_held;
    eval { $self->{dbh}->do('SELECT 1'); 1 } or do { $self->{state} = 'lost'; die $@; };
    return 1;
}
sub release {
    my ($self) = @_;
    return $self->{outcome} if defined $self->{outcome};
    $self->{state} = 'releasing';
    my $outcome = 'unknown';
    eval {
        my $sth = $self->{dbh}->prepare('SELECT pg_advisory_unlock(?::bigint) AS released');
        $sth->execute($self->{key});
        my ($released) = $sth->fetchrow_array;
        $sth->finish;
        $outcome = $released ? 'released' : 'already_not_owned';
        1;
    };
    eval { $self->{dbh}->disconnect };
    $self->{state} = $outcome eq 'unknown' ? 'released_unknown' : 'released';
    $self->{outcome} = $outcome;
    delete $self->{provider}{leases}{"$self"};
    return $outcome;
}

1;
