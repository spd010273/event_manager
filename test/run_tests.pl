#!/usr/bin/perl
=pod
/*------------------------------------------------------------------------
 *
 * run_tests.pl
 *     Test suite for synchronous event triggering and basic async events
 *
 * Copyright (c) 2018-2021, MerchLogix Inc.
 *
 * IDENTIFICATION
 *        run_tests.pl
 *
 *------------------------------------------------------------------------
 */
=cut

use strict;
use warnings;
use utf8;

use Params::Validate qw( :all );
use Carp;
use Readonly;
use Perl6::Export::Attrs;
use English qw( -no_match_vars );

use JSON;
use DBI;
use Getopt::Std;
use Time::HiRes qw( gettimeofday tv_interval );
use Cwd qw( abs_path );
use File::Temp;
use File::Glob;

my $version = &_get_version_from_file();

# make check parameters
Readonly my $MC_PORT             => '5432';
Readonly my $MC_USER             => 'postgres';
Readonly my $MC_HOST             => 'localhost';
Readonly my $VERSION             => "$version";
Readonly my $TESTDIR             => './test/sql/';
Readonly my $SETUP_SQL           => './tools/setup.sql';
Readonly my $EXT_SQL             => './sql/';
Readonly my $TEST_DATABASE       => '__em_test__';
Readonly my $MC_POSTGRES_CS      => "dbi:Pg:dbname=postgres;host=$MC_HOST;port=$MC_PORT";
Readonly my $MC_TEST_CS          => "dbi:Pg:dbname=$TEST_DATABASE;host=$MC_HOST;port=$MC_PORT";
Readonly my $VALGRIND_PREFIX     => <<VALGRIND;
valgrind --track-origins=yes --read-inline-info=yes --read-var-info=yes --leak-check=full --show-leak-kinds=all --read-var-info=yes --trace-children=yes -v
VALGRIND
Readonly my $USAGE_MESSAGE       => <<"USAGE";
Usage:
    $0 -U <username> -d <dbname> -h <hostname> -p <port>
        -U db user name (default: postgres)
        -d db name (default: -U value )
        -p db port (default: 5432 )
        -h host name (default: localhost)
      [ -v  Prints version ]
      [ -l  list tests ]
      [ -D  Debug flag (show error detail) ]
      [ -V  With Valgrind ]
USAGE
Readonly my $CHECK_WORKER_RUNNING => <<SQL;
    SELECT l.objid AS worker_pid
      FROM pg_locks l
INNER JOIN pg_stat_activity a
        ON a.pid = l.pid
INNER JOIN pg_namespace n
        ON n.nspname = 'event_manager'
INNER JOIN pg_class c
        ON c.relkind = 'r'
       AND c.relname = ?
       AND c.relnamespace = n.oid
       AND c.oid = l.classid
     WHERE l.locktype = 'advisory'
       AND a.datname = ?
SQL
Readonly my $CHECK_VERSION => <<SQL;
    SELECT extversion
      FROM pg_extension
     WHERE extname = 'event_manager'
SQL
Readonly my $WARNING_MESSAGE => <<TEXT;
WARNING: This script will have side effects on the specified database!
This includes dropping the extension and recreating it, generating
test tables and test data. Author(s) are not responsible for misuse of
this test harness! To acknowledge, press [Y] or [N] and hit ENTER.
TEXT
$Getopt::Std::OUTPUT_HELP_VERSION   = $VERSION;
$Getopt::Std::STANDARD_HELP_VERSION = 1;

my  $username;
my  $dbname;
my  $port;
my  $hostname;
my  $connection_string;
my  @children;
my  $debug                     = 0;
my  $use_valgrind              = 0;
my  $manager_running           = 0;
my  $manager_should_be_running = 0;
my  $version_updated           = 0;
our $make_check                = 0;

sub _get_version_from_file()
{
    my $fh;
    my $cwd = abs_path();
    if( $cwd =~ m/^.*\/event_manager$/ && -e 'CURRENT_VERSION' )
    {
        unless( open( $fh, '<', 'CURRENT_VERSION' ) )
        {
            croak 'Failed to open file CURRENT_VERSION in event_manager root directory';
        }
    }
    elsif( $cwd =~ m/event_manager\/test$/ &&-e '../CURRENT_VERSION' )
    {
        unless( open( $fh, '<', '../CURRENT_VERSION' ) )
        {
            croak "Failed to open file '../CURRENT_VERSION' in event_manager root directory";
        }
    }
    else
    {
        croak 'Unable to locate a valid version file';
    }

    my $line = <$fh>;
    my $version = $line;
    $version =~ s/\n//;

    close( $fh );
    return $version;
}

sub HELP_MESSAGE()
{
    usage();
}

sub VERSION_MESSAGE()
{
    print "Event Manager Version $VERSION - Test Suite\n";
}

sub usage(;$)
{
    my( $message ) = validate_pos(
        @_,
        {
            type => SCALAR,
            optional => 1,
        },
    );

    if( $message )
    {
        carp "$message\n";
    }

    carp "$USAGE_MESSAGE\n";

    exit 1;
}

sub check_and_upgrade_extension()
{
    return if( length( $connection_string ) == 0 );
    return if( $version_updated );
    my $major_version = $VERSION;
    $major_version =~ s/\.\d+$//;
    my $minor_version = $VERSION;
    $minor_version =~ s/^\d+\.//;

    my $check_versions = [];
    for( my $ma = 0; $ma <= $major_version; $ma++ )
    {
        for( my $mi = 1; $mi <= $minor_version; $mi++ )
        {
            push( @$check_versions, "${ma}.${mi}" );
        }
    }

    my $dir;

    unless( opendir( $dir, $EXT_SQL ) )
    {
        croak( "Failed to open extension SQL directory '$EXT_SQL': $OS_ERROR\n" );
    }

    my $upgradable_versions = [];

    while( my $entry = readdir( $dir ) )
    {
        next if( $entry =~ m/^\./ );

        foreach my $version( @$check_versions )
        {
            if( $entry =~ m/event_manager--\d+\.\d+--${version}.sql$/ )
            {
                push( @$upgradable_versions, $version );
            }
        }
    }

    closedir( $dir );

    my $conn = DBI->connect( $connection_string, 'postgres', undef );

    unless( $conn )
    {
        croak( 'Failed to connect to database' );
    }

    my $sth = $conn->prepare( $CHECK_VERSION );
    unless( $sth->execute() )
    {
        croak( 'Failed to check extension version' );
    }

    my $row = $sth->fetchrow_hashref();
    my $installed_version = $row->{extversion};

    $sth->finish();

    foreach my $version( @$upgradable_versions )
    {
        return if( $version eq $installed_version );
        unless( $conn->do( "ALTER EXTENSION event_manager UPDATE TO '$version'" ) )
        {
            croak( "Failed while upgrading extension to $version\n" );
        }
    }

    $version_updated = 1;
    return;
}

sub run_test($) :Export(:DEFAULT)
{
    my( $filename ) = validate_pos(
        @_,
        {
            type => SCALAR,
        },
    );

    unless( -e $filename )
    {
        croak "File '$filename' does not exist!";
    }

    my $file_handle;

    unless( open( $file_handle, "<$filename" ) )
    {
        croak "Failed to open '$filename' for read";
    }

    my $sql = '';

    while( my $line = <$file_handle> )
    {
        if( $line =~ /^--require:\s?/ )
        {
            my $requirement = $line;
            $requirement =~ s/^--require:\s?//;

            if( $requirement =~ /event_manager/ )
            {
                if( $manager_should_be_running and not check_event_manager_running() )
                {
                    return { result => 0, error_text => 'Event and/or Work processors was running but died' };
                }

                if( not $manager_should_be_running  and not check_event_manager_running( 1 ) )
                {
                    return { result => 0, error_text => 'Event and/or Work processors not running' };
                }
            }
        }

        $sql .= $line;
    }

    unless( close( $file_handle ) )
    {
        croak "Failed to close '$filename'";
    }

    my $temp_file = File::Temp->new(
        UNLINK => 1,
        SUFFIX => '.sql'
    );

    unless( $temp_file )
    {
        croak 'Failed to create temp file for test';
    }

    print $temp_file $sql;
    my $output_file = "/tmp/eventmanagertest.txt";
    my $command = '';

    if( $make_check )
    {
        $command = "psql -U $MC_USER -d $TEST_DATABASE -p $MC_PORT -h $MC_HOST -f $filename &> $output_file 2>&1";
    }
    else
    {
        $command = "psql -U $username -d $dbname -p $port -h $hostname -f $filename &> $output_file 2>&1";
    }

    my $signal = system( $command );

    if( -e $output_file )
    {
        open( FILE, "<$output_file" );
    }
    else
    {
        unless( $signal && 127 == 2 )
        {
            # Test had no output?
        }

        carp 'No output from test!';
        return;
    }

    my $result_text = '';
    my $result;
    while( my $line = <FILE> )
    {
        $result_text .= $line;

        if( $line =~ /FAILED:/ )
        {
            $result = 0;
        }

        if( $line =~ /PASSED:/ and not defined( $result ) )
        {
            $result = 1;
        }
    }

    return { result => $result, error_text => $result_text };
}

sub get_tests() :Export(:DEFAULT)
{
    my $dir;
    my $result = {}; #  {version => [ file_name => path ] }

    unless( opendir( $dir, $TESTDIR ) )
    {
        croak( "Failed to find tests in '$TESTDIR': $OS_ERROR" );
    }

    print "Gathering tests for version $VERSION\n";

    my $versions = [];
    my $run_versions = [];

    while( my $version_entry = readdir $dir )
    {
        next if( $version_entry =~ /^\./ or not $version_entry =~ /^v/ );
        push( @$versions, $version_entry );
    }

    closedir( $dir );

    if( scalar( @$versions ) == 0 )
    {
        croak( "Could not find expected versioned directories for tests\n" );
    }

    foreach my $version( sort @$versions )
    {
        my $numeric_version = $version;
        $numeric_version =~ s/^v//;

        if( $numeric_version <= $VERSION )
        {
            push( @$run_versions, $version );
        }
    }

    foreach my $run_version( @$run_versions )
    {
        my @test_files;

        unless( opendir( $dir, "${TESTDIR}/${run_version}" ) )
        {
            corak( "Failed to find tests in '$TESTDIR': $OS_ERROR" );
        }

        while( my $entry = readdir $dir )
        {
            if( $entry =~ /^\./ or not $entry =~ /\.sql$/ )
            {
                next;
            }

            push( @test_files, $entry );
        }

        closedir( $dir );

        @test_files = sort( @test_files );

        foreach my $test_file( @test_files )
        {
            my $hash = { $test_file => "${TESTDIR}/${run_version}/${test_file}" };
            push( @{$result->{$run_version}}, $hash );
        }
    }

    return $result;
}

sub check_event_manager_running(;$)
{
    my ( $start_process ) = validate_pos(
        @_,
        {
            type => SCALAR,
            optional => 1,
        },
    );

    my $event_processor_running = 0;
    my $work_processor_running  = 0;

    if( $manager_should_be_running )
    {
        if( $use_valgrind )
        {
            # Process may have been SIGHUP'd so we'll give it a sec to reconnect
            sleep( 10 );
        }

        my $dbh;
        if( $make_check )
        {
            $dbh = DBI->connect( $MC_TEST_CS, 'postgres', undef );
        }
        else
        {
            $dbh = DBI->connect( $connection_string, $username, undef );
        }

        unless( $dbh )
        {
            carp "Could not connect to test database \"$TEST_DATABASE\"";
            return 0;
        }

        my $sth = $dbh->prepare( $CHECK_WORKER_RUNNING );

        unless( $sth )
        {
            carp 'Failed to prepare check transaction';
            return 0;
        }

        $sth->bind_param( 1, 'tb_event_queue' );

        if( $make_check )
        {
            $sth->bind_param( 2, $TEST_DATABASE );
        }
        else
        {
            $sth->bind_param( 2, $dbname );
        }

        unless( $sth->execute() )
        {
            carp 'Failed to check event queue worker';
            return 0;
        }

        $event_processor_running = $sth->rows();
        $sth->bind_param( 1, 'tb_work_queue' );

        if( $make_check )
        {
            $sth->bind_param( 2, $TEST_DATABASE );
        }
        else
        {
            $sth->bind_param( 2, $dbname );
        }

        unless( $sth->execute() )
        {
            carp 'Failed to check work queue worker';
            return 0;
        }

        $work_processor_running = $sth->rows();
        $sth->finish();
        $dbh->disconnect();

        if( $work_processor_running and $event_processor_running )
        {
            return 1;
        }

        return 0;
    }

    if( $start_process and not $manager_should_be_running )
    {
        check_and_upgrade_extension();

        $manager_should_be_running = 1;
        # Do the thing
        my $startflags = [];

        push( @$startflags, '-W 1' );
        push( @$startflags, '-E 1' );

        my $command = '';
        my $flag    = join( ' ', @$startflags );
        my $log     = $flag;
        $log        =~ s/^-//g;
        $log        =~ s/\s//g;

        if( $make_check )
        {
            $command = "./event_manager -U $MC_USER -d \"$TEST_DATABASE\" -h $MC_HOST -p $MC_PORT $flag \&> /tmp/event_manager_${log}.log";
        }
        else
        {
            $command = "./event_manager -U $username -d $dbname -h $hostname -p $port $flag \&> /tmp/event_manager_${log}.log";
        }

        my $startup_wait = 5; # seconds

        if( $use_valgrind )
        {
            $command = "${VALGRIND_PREFIX} ${command}";
            $command =~ s/\n//;
            $startup_wait += 15; # valgrind takes a bit of time
        }

        my $pid = fork();

        if( not defined( $pid ) )
        {
            croak 'Fork failed.';
        }
        elsif( $pid == 0 )
        {
            # child
            start_process( $command );
            carp 'EM started';
            exit 0;
        }
        else
        {
            push( @children, $pid );
        }

        sleep( $startup_wait );

        if( &check_event_manager_running() )
        {
            return 1;
        }

        carp 'Failed to verify the processors are running after start attempt';
    }

    return 0;
}

sub start_process($)
{
    my( $command ) = validate_pos(
        @_,
        { type => SCALAR },
    );

    exec( $command );
    exit 0;
}

sub _wait_for_user_input()
{
    return 1 if( $make_check );
    my $count = 0;
    WAIT:
    my $result = <STDIN>;
    if( $result =~ m/^y$/i )
    {
        return 1;
    }
    elsif( $result =~ m/^n$/i )
    {
        return 0;
    }

    $count++;
    print 'Please ' if( $count < 3 );
    print 'Pretty please ' if( $count >= 3 and $count < 6 );
    print 'Pretty please with a cherry on top, ' if( $count >= 6 );
    print "respond [y] or [n] and hit ENTER\n";
    goto WAIT;
}

## MAIN PROGRAM
our( $opt_d, $opt_h, $opt_U, $opt_p, $opt_l, $opt_v, $opt_D, $opt_V, $opt_M );
usage( 'invalid arguments' ) unless( getopts( 'd:U:p:h:lvDVM' ) );

$username   = $opt_U;
$port       = $opt_p;
$hostname   = $opt_h;
$dbname     = $opt_d;
$debug      = $opt_D;
$make_check = $opt_M;

if( $opt_v )
{
    print "Version $VERSION\n";
    exit 0;
}

if( $opt_l )
{
    # Only list tests
    my $tests = get_tests();
    print "Tests that will be run:\n";
    foreach my $version( sort keys %$tests )
    {
        print "Tests for $version:\n";

        foreach my $test_entry( @{$tests->{$version}} )
        {
            foreach my $test( keys %$test_entry )
            { # This will only have 1 entry, I'm just being lazy
                print "  $test\n";
            }
        }
    }
    exit 0;
}

if( $opt_V )
{
    $use_valgrind = 1;
}

if( $make_check )
{
    my $dbh = DBI->connect(
        $MC_POSTGRES_CS,
        'postgres',
        undef
    );

    unless( $dbh )
    {
        croak 'There is no database running locally or make check could not connect';
    }

    unless( $dbh->do( "DROP DATABASE IF EXISTS \"$TEST_DATABASE\"" ) )
    {
        croak "Failed to remove existing test database \"$TEST_DATABASE\"";
    }

    unless( $dbh->do( "CREATE DATABASE \"$TEST_DATABASE\"" ) )
    {
        croak "Failed to create test database \"$TEST_DATABASE\"";
    }

    $connection_string = $MC_TEST_CS;
    $dbh->disconnect();
}
else
{
    unless( defined $username )
    {
        $username = 'postgres';
    }

    unless( defined $port )
    {
        $port = 5432;
    }

    unless( defined $dbname )
    {
        $dbname = $username;
    }

    unless( defined $hostname )
    {
        $hostname = 'localhost';
    }

    if( defined $port and ($port !~ /^\d+$/ or $port < 1 or $port > 65536 ) )
    {
        usage( 'Invalid port' );
    }

    unless( defined $dbname and length $dbname > 0 )
    {
        usage( 'Invalid dbname' );
    }

    unless( defined $username and length $username > 0 )
    {
        usage( 'Invalid username' );
    }

    unless( defined $hostname and length $hostname > 0 )
    {
        usage( 'Invalid hostname' );
    }

    $connection_string = "dbi:Pg:dbname=$dbname;host=$hostname;port=$port";
    print "Using connection string '$connection_string'\n";
    print $WARNING_MESSAGE;
    unless( _wait_for_user_input() )
    {
        exit 1;
    }
}

my $tests = get_tests();
if( check_event_manager_running() and not $manager_should_be_running )
{
    croak 'Please stop the event_manager process(es) for testing';
}

foreach my $version( sort keys %$tests )
{
    print "RUNNING $version TESTS:\n";

    my $version_failure = 0;
    foreach my $test( @{$tests->{$version}} )
    {
        foreach my $test_name( keys %$test )
        {
            print "  RUNNING $test_name... ";
            my $test_file = $test->{$test_name};
            my $result = run_test( $test_file );

            if( not defined( $result->{result} ) or $result->{result} == 0 )
            {
                print "FAILED\n";
                print "    $result->{error_text}\n" if( $debug and defined( $result->{error_text} ) );
                $version_failure++;
            }
            elsif( defined( $result->{result} ) and $result->{result} == 1 )
            {
                print "PASSED\n";
            }
        }

        last if( $version_failure );
    }

    if( $version_failure )
    {
        print "Version $version has failed test(s).\n";
        last;
    }
    else
    {
        print "Version $version passed tests\n";
    }
}

if( $make_check )
{
    my $dbh = DBI->connect(
        $MC_POSTGRES_CS,
        'postgres',
        undef
    );

    unless( $dbh )
    {
        croak 'There is no database running locally or make check could not connect';
    }

    unless( $debug )
    {
        unless( $dbh->do( "DROP DATABASE \"$TEST_DATABASE\"") )
        {
            croak "Failed to remove test database \"$TEST_DATABASE\"";
        }
    }

    $dbh->disconnect();
}

if( scalar( @children ) > 0 )
{
    kill 'INT', @children;
}

## Do it twice, handles valgrind, naming variants.
my $result = `ps aux | grep "event_manager" | grep -v grep | awk '{ print \$2 }'`;

foreach my $pid( split( "\n", $result ) )
{
    chomp( $pid );
    next unless( $pid =~ /^\d+$/ );
    kill 'INT', $pid;
}

$result = `ps aux | grep "Event Manager" | grep -v grep | awk '{ print \$2 }'`;

foreach my $pid( split( "\n", $result ) )
{
    chomp( $pid );
    next unless( $pid =~ /^\d+$/ );
    kill 'INT', $pid;
}

sleep( 1 );

if( check_event_manager_running() )
{
    croak 'Could not reap children';
}

exit 0;
