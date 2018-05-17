#!/usr/bin/perl

use strict;
use warnings;

use Readonly;
use Carp;
use Fatal qw( open );
use Cwd;
use DBI;
use English qw( -no_match_vars );
use Params::Validate qw( :all );
use Getopt::Std;

Readonly my $start_sh => <<BASH;
#!/bin/bash
#    This script will start the Event Manager daemons

__INSTALL_DIR__/event_manager -U __USERNAME__ -d __DBNAME__ -h __HOSTNAME__ -p __PORT__ -W &
__INSTALL_DIR__/event_manager -U __USERNAME__ -d __DBNAME__ -h __HOSTNAME__ -p __PORT__ -E &
BASH

Readonly my $stop_sh => <<BASH;
#!/bin/bash
#    This script will stop the event_manager daemons

killall -w event_manager
BASH

Readonly my $start_file_name => 'event_manager-startup.sh';
Readonly my $stop_file_name => 'event_manager-shutdown.sh';
Readonly my $sh_target_dir => '/usr/bin/';
Readonly my $systemd_service_dir => '/usr/lib/systemd/system/';

my $hostname;
my $username;
my $port;
my $dbname;

sub get_user_input($)
{
    my( $message ) = validate_pos(
        @_,
        { type => SCALAR }
    );

    print "$message\n";
    my $response = <STDIN>;
    chomp( $response );

    return $response;
}

sub test_connection()
{
    my $connection_string = "dbi:Pg:dbname=${dbname};host=${hostname};port=${port}";
    my $handle = DBI->connect(
        $connection_string,
        $username,
        undef
    );

    if( $handle->ping() > 0 )
    {
        $handle->disconnect();
        return 1;
    }

    return 0;
}

sub get_username()
{
    $username = get_user_input( 'Please enter a username:' );
    return;
}

sub get_hostname()
{
    $hostname = get_user_input( 'Please enter a hostname:' );
    return;
}

sub get_port()
{
    $port = get_user_input( 'Please enter a port:' );
    return;
}

sub get_dbname()
{
    $dbname = get_user_input( 'Please enter a database name:' );
    return;
}

## Main Program
if( $EFFECTIVE_USER_ID != 0 )
{
    croak( 'Must be root' );
}

my $dir = getcwd();

unless( -e "$dir/../event_manager" )
{
    croak "Could not locate event_manager daemon - is it built?";
}

chdir( '../' );
my $install_dir = getcwd();
chdir( $dir );

our( $opt_d, $opt_U, $opt_p, $opt_h );
if( getopt( 'd:U:p:h' ) )
{
    if( defined $opt_d and length( $opt_d ) > 0 )
    {
        $dbname = $opt_d;
    }
    else
    {
        get_dbname();
    }

    if( defined $opt_U and length( $opt_U ) > 0 )
    {
        $username = $opt_U;
    }
    else
    {
        get_username();
    }

    if( defined $opt_h and length( $opt_h ) > 0 )
    {
        $hostname = $opt_h;
    }
    else
    {
        get_hostname();
    }

    if( defined $opt_p and $opt_p =~ /^\d+$/ )
    {
        $port = $opt_p;
    }
    else
    {
        get_port();
    }
}
else
{
    GET_ARGS:
    get_dbname();
    get_username();
    get_hostname();
    get_port();
}


unless( test_connection() )
{
    print "Connection parameters do not work\n";
    goto GET_ARGS;
}

my $start_shell_script = $start_sh;
$start_shell_script =~ s/__INSTALL_DIR__/$install_dir/g;
$start_shell_script =~ s/__USERNAME__/$username/g;
$start_shell_script =~ s/__DBNAME__/$dbname/g;
$start_shell_script =~ s/__HOSTNAME__/$hostname/g;
$start_shell_script =~ s/__PORT__/$port/g;

unless( open( STARTFILE, ">${sh_target_dir}${start_file_name}" ) )
{
    croak "Failed to write to ${sh_target_dir} for systemd startup script";
}

print STARTFILE $start_shell_script;

close STARTFILE;

unless( open( STOPFILE, ">${sh_target_dir}${stop_file_name}" ) )
{
    croak "Failed to write to ${sh_target_dir} for systemd shutdown script";
}

print STOPFILE $stop_sh;

close STOPFILE;

chmod "0755", "${sh_target_dir}${start_file_name}";
chmod "0755", "${sh_target_dir}${stop_file_name}";

unless( -e $systemd_service_dir )
{
    croak 'Is systemd installed??';
}

copy( "${install_dir}/service/event_manager.service", $systemd_service_dir );
my $result = system( "systemd-analyze verify ${systemd_service_dir}/event_manager.service" );

if( $result )
{
    croak "Service installation failed";
}

exit 0;
