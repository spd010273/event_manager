#!/usr/bin/perl
=pod
/*------------------------------------------------------------------------
 *
 * run_tests.pl
 *     Test suite for synchronous event triggering and basic async events
 *
 * Copyright (c) 2018, Nead Werx, Inc.
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

Readonly my $VERSION         => '0.1';
Readonly my $TESTDIR         => './test/sql/';
Readonly my $TEST_DATABASE   => '__em_test__';
Readonly my $MC_POSTGRES_CS  => 'dbi:Pg:dbname=postgres;host=localhost;port=5432';
Readonly my $MC_TEST_CS      => "dbi:Pg:dbname=$TEST_DATABASE;host=localhost;port=5432";
Readonly my $VALGRIND_PREFIX => <<VALGRIND;
valgrind --track-origins=yes --read-inline-info=yes --read-var-info=yes --leak-check=full --show-leak-kinds=all
VALGRIND
Readonly my $USAGE_MESSAGE   => <<"USAGE";
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

$Getopt::Std::OUTPUT_HELP_VERSION = $VERSION;
$Getopt::Std::STANDARD_HELP_VERSION = 1;

my $username;
my $dbname;
my $port;
my $hostname;
my $debug = 0;
my @children;
my $use_valgrind = 0;
my $manager_running = 0;
my $manager_should_be_running = 0;
our $make_check = 0;

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
                $manager_should_be_running = 1;
                unless( check_event_manager_running( 1 ) )
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
        $command = "psql -U postgres -d $TEST_DATABASE -p 5432 -h localhost -f $filename &> $output_file 2>&1";
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

    unless( opendir( $dir, $TESTDIR ) )
    {
        croak( "Failed to find tests in '$TESTDIR': $OS_ERROR" );
    }

    my @test_files;

    while( my $entry = readdir $dir )
    {
        if( $entry =~ /^\./ or not $entry =~ /\.sql$/ )
        {
            next;
        }

        push( @test_files, $entry );
    }

    closedir( $dir );

    @test_files = sort @test_files;

    return \@test_files;
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

    system( 'ps aux | grep Event &> /tmp/ps_result.txt' );

    my $file;
    my $file_contents = '';

    open( $file, "</tmp/ps_result.txt" );

    my $event_processor_running = 0;
    my $work_processor_running = 0;

    while( my $line = <$file> )
    {
        if( $line =~ /\sgrep\s/ )
        {
            next;
        }

        if( $line =~ /\s-E\s/ and $line =~ /\s-d\s$dbname\s/ )
        {
            $event_processor_running = 1;
        }

        if( $line =~ /\s-W\s/ and $line =~ /\s-d\s$dbname\s/ )
        {
            $work_processor_running = 1;
        }
    }

    if( $work_processor_running and $event_processor_running )
    {
        return 1;
    }

    if( $start_process )
    {
        # Do the thing
        my $startflags = [];
        unless( $work_processor_running )
        {
            push( @$startflags, '-W 1' );
        }

        unless( $event_processor_running )
        {
            push( @$startflags, '-E 1' );
        }

        foreach my $flag( @$startflags )
        {
            my $log = $flag;
            $log =~ s/^-//;
            my $command = '';
            if( $make_check )
            {
                $command = "./event_manager -U postgres -d \"$TEST_DATABASE\" -h localhost -p 5432 $flag \&> /tmp/event_manager_${log}.log";
            }
            else
            {
                $command = "./event_manager -U $username -d $dbname -h $hostname -p $port $flag \&> /tmp/event_manager_${log}.log";
            }

            if( $use_valgrind )
            {
                $command = "${VALGRIND_PREFIX}${command}";
                print "Executing async processor with command:\n $command\n";
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
                exit 0;
            }
            else
            {
                push( @children, $pid );
            }
        }

        if( &check_event_manager_running() )
        {
            sleep( 5 );
            return 1;
        }

        carp "Failed to startup event_manager";
    }

    return 0;
}

sub start_process($)
{
    my( $command ) = validate_pos(
        @_,
        { type => SCALAR },
    );

    system( $command );
    exit 0;
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
    foreach my $test( @$tests )
    {
        print "$test\n";
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
}

my $tests = get_tests();
if( check_event_manager_running() and not $manager_should_be_running )
{
    croak 'Please stop the event_manager process(es) for testing';
}

foreach my $test( @$tests )
{
    print "RUNNING: $test... ";
    my $result = run_test( "$TESTDIR/$test" );

    if( not defined( $result->{result} ) or $result->{result} == 0 )
    {
        print "FAILED\n";
        print "    $result->{error_text}\n" if( $debug and defined( $result->{error_text} ) );
        last;
    }
    elsif( defined( $result->{result} ) and $result->{result} == 1 )
    {
        print "PASSED\n";
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
    kill 'TERM', @children;
#    sleep( 1 );
#    kill 'KILL', @children;
}

my $result = `ps aux | grep "Event Manager " | awk '{ print \$2 }'`;

#foreach my $pid( split( "\n", $result ) )
#{
#    chomp( $pid );
#    next unless( $pid =~ /^\d+$/ );
#    kill 'TERM', $pid;
#    sleep( 1 );
#    kill 'KILL', $pid;
#}

if( check_event_manager_running() )
{
    croak 'Could not reap children';
}

exit 0;
