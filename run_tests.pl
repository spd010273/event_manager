#!/usr/bin/perl

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

Readonly my $VERSION => '0.1';
Readonly my $TESTDIR => './test/';
my $username;
my $dbname;
my $port;
my $hostname;
my $debug = 0;

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
        warn "$message\n";
    }

    my $usage = <<"USAGE";
    Usage:
        $0 -U <username> -d <dbname> -h <hostname> -p <port>
            -U db user name (default: postgres)
            -d db name (default: -U value )
            -p db port (default: 5432 )
            -h host name (default: localhost)
          [ -v  Prints version ]
          [ -l  list tests ]
          [ -D  Debug flag (show error detail) ]
USAGE
    warn "$usage\n";

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
    my $command = "psql -U $username -d $dbname -p $port -h $hostname -f $filename &> $output_file 2>&1";

    my $signal = system( $command );

    if( -e $output_file )
    {
        open( FILE, "<$output_file" );
    }
    else
    {
        unless( $signal & 127 == 2 )
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
        croak( "$1" );
    }

    my @test_files;

    while( my $entry = readdir $dir )
    {
        if( $entry =~ /^\./ )
        {
            next;
        }

        push( @test_files, $entry );
    }

    closedir( $dir );

    @test_files = sort @test_files;

    return \@test_files;
}

## MAIN PROGRAM
our( $opt_d, $opt_h, $opt_U, $opt_p, $opt_l, $opt_v, $opt_D );
usage( 'invalid arguments' ) unless( getopts( 'd:U:p:h:lvD' ) );

$username = $opt_U;
$port     = $opt_p;
$hostname = $opt_h;
$dbname   = $opt_d;
$debug    = $opt_D;

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

my $tests = get_tests();

foreach my $test( @$tests )
{
    my $result = run_test( "$TESTDIR/$test" );

    if( not defined( $result->{result} ) or $result->{result} == 0 )
    {
        print "FAILED: $test\n";
        print "    $result->{error_text}\n" if( $debug and defined( $result->{error_text} ) );
        exit 1;
    }
    elsif( defined( $result->{result} ) and $result->{result} == 1 )
    {
        print "PASSED: $test\n";
    }
}

exit 0;
