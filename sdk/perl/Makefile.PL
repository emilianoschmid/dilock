use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME             => 'Dilock',
    VERSION_FROM     => 'lib/Dilock.pm',
    ABSTRACT         => 'Distributed lock client for Redis and etcd',
    LICENSE          => 'apache_2_0',
    MIN_PERL_VERSION => '5.020',
    PREREQ_PM        => {
        'Digest::SHA' => 0,
        'Encode'     => 0,
        'JSON::PP'   => 0,
        'HTTP::Tiny' => 0,
        'MIME::Base64' => 0,
    },
    TEST_REQUIRES => {
        'Test::More' => 0,
        'EV'         => 0,
    },
    META_MERGE => {
        'meta-spec' => { version => 2 },
        prereqs => {
            runtime => {
                recommends => {
                    'Crypt::URandom' => '0.55',
                    'Redis::Fast'    => '0.39',
                    'EV'             => 0,
                    'EV::Etcd'       => '0.10',
                },
            },
        },
    },
);
