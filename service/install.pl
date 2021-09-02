#!/usr/bin/perl

=pod
/*------------------------------------------------------------------------
 *
 * install.pl
 *     Basic systemctl installer for Event Manager
 *
 * Copyright (c) 2018, MerchLogix Inc.
 *
 * IDENTIFICATION
 *        install.pl
 *
 *------------------------------------------------------------------------
 */
=cut

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
use File::Copy;
use File::Path qw( make_path );
use IO::Interactive qw( is_interactive );

Readonly my $LOG_DIR             => '/var/log/event_manager/';
Readonly my $LOG_FILE            => 'event_manager.log';
Readonly my $START_FILE_NAME     => 'event_manager-startup.sh';
Readonly my $STOP_FILE_NAME      => 'event_manager-shutdown.sh';
Readonly my $RELOAD_FILE_NAME    => 'event_manager-reload.sh';
Readonly my $SH_TARGET_DIR       => '/usr/bin/';
Readonly my $SYSTEMD_SERVICE_DIR => '/usr/lib/systemd/system/';
Readonly my $LOGROTATE_DEST      => '/etc/logrotate.d/event_manager';
Readonly my $GIT_INIT_COMMAND    => 'git submodule update --init --recursive';

Readonly my $START_SH => <<BASH;
#!/bin/bash
#    This script will start the Event Manager daemons

__INSTALL_DIR__/event_manager -U __USERNAME__ -d __DBNAME__ -h __HOSTNAME__ -p __PORT__ -W __WORKER_COUNT__ -E __EVENT_COUNT__ -D
BASH

Readonly my $STOP_SH => <<BASH;
#!/bin/bash
#    This script will stop the event_manager daemons
if [ -f /var/run/event_manager.pid ]; then
    kill \$(cat /var/run/event_manager.pid)
    rm /var/run/event_manager.pid
elif [ -f ~/event_manager.pid ]; then
    kill \$(cat ~/event_manager.pid)
    rm ~/event_manager.pid
elif [ -f ./event_manager.pid ]; then
    kill \$(cat ./event_manager.pid)
    rm ./event_manager.pid
else
    echo "Could not locate PID file!"
fi

BASH

Readonly my $RELOAD_SH => <<BASH;
#!/bin/bash
#   This script will issue a SIGHUP to event_manager
if [ -f /var/run/event_manager.pid ]; then
    kill -1 \$(cat /var/run/event_manager.pid)
elif [ -f ~/event_manager.pid ]; then
    kill -1 \$(cat ~/event_manager.pid)
elif [ -f ./event_manager.pid ]; then
    kill -1 \$(cat ./event_manager.pid)
else
    echo "Could not locate PID file!"
fi
BASH

my $hostname;
my $username;
my $port;
my $dbname;
my $worker_count;
my $event_count;

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

sub build_repo()
{
    # It's expected we're in the event_manager root
    system( $GIT_INIT_COMMAND );
    #TODO:
    #  - Check for lib prerequisites

    system( 'make clean' );
    system( 'make' );
    system( 'make install' );

    return;
}

sub test_connection()
{
    my $connection_string = "dbi:Pg:dbname=${dbname};host=${hostname};port=${port}";
    my $handle = DBI->connect(
        $connection_string,
        $username,
        undef
    );

    unless( defined $handle )
    {
        return 0;
    }

    if( $handle->ping() > 0 )
    {
        $handle->disconnect();
        return 1;
    }

    return 0;
}

sub get_username()
{
    if( is_interactive() )
    {
        $username = get_user_input( 'Please enter a username:' );
    }
    else
    {
        $username = 'postgres';
    }

    return;
}

sub get_hostname()
{
    if( is_interactive() )
    {
        $hostname = get_user_input( 'Please enter a hostname:' );
    }
    else
    {
        $hostname = 'localhost';
    }

    return;
}

sub get_port()
{
    if( is_interactive() )
    {
        $port = get_user_input( 'Please enter a port:' );
    }
    else
    {
        $port = 5432;
    }

    return;
}

sub get_dbname()
{
    if( is_interactive() )
    {
        $dbname = get_user_input( 'Please enter a database name:' );
    }
    else
    {
        carp 'No database name provided :|';
        $dbname = 'postgres';
    }

    return;
}

sub get_worker_count()
{
    if( is_interactive() )
    {
        $worker_count = get_user_input( 'Please enter the number of work queue processes:' );
    }
    else
    {
        $worker_count = 1;
    }

    if(
            defined $worker_count
        and length( $worker_count ) > 0
        and $worker_count =~ m/^\d+$/
        and $worker_count > 0
        and $worker_count <= 16
      )
    {
        return;
    }

    $worker_count = 1;
    return;
}

sub get_event_count()
{
    if( is_interactive() )
    {
        $event_count = get_user_input( 'Please enter the number of event queue processes:' );
    }
    else
    {
        $event_count = 1;
    }

    if(
            defined $event_count
        and length( $event_count ) > 0
        and $event_count =~ m/^\d+$/
        and $event_count > 0
        and $event_count <= 16
      )
    {
        return;
    }

    $event_count = 1;
    return;
}

## Main Program
if( $EFFECTIVE_USER_ID != 0 )
{
    croak( 'Must be root' );
}

unless( -e $LOG_DIR )
{
    unless( make_path( $LOG_DIR ) )
    {
        croak "Failed to create logging directory $LOG_DIR";
    }
}

my $dir = getcwd();

unless( -e "$dir/../event_manager" )
{
    croak 'Could not locate event_manager daemon - is it built?';
}

if( $dir =~ /\/service/ )
{
    chdir( '../' );
}

build_repo();

my $install_dir = getcwd();
chdir( $dir );

our( $opt_d, $opt_U, $opt_p, $opt_h, $opt_E, $opt_W );

unless( getopts( 'd:U:p:h:E:W:' ) )
{
    get_dbname();
    get_username();
    get_hostname();
    get_port();
    get_worker_count();
    get_event_count();
}

GET_ARGS:
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

if( defined $opt_E and $opt_E =~ /^\d+$/ and ( $opt_E > 0 and $opt_E <= 16 ) )
{
    $event_count = $opt_E;
}
else
{
    get_event_count();
}

if( defined $opt_W and $opt_W =~ /^\d+$/ and ( $opt_W > 0 and $opt_W <= 16 ) )
{
    $worker_count = $opt_W;
}
else
{
    get_worker_count();
}

unless( test_connection() )
{
    carp 'Connection parameters do not work';
    goto GET_ARGS;
}

my $start_shell_script = $START_SH;
$start_shell_script    =~ s/__INSTALL_DIR__/$install_dir/g;
$start_shell_script    =~ s/__USERNAME__/$username/g;
$start_shell_script    =~ s/__DBNAME__/$dbname/g;
$start_shell_script    =~ s/__HOSTNAME__/$hostname/g;
$start_shell_script    =~ s/__PORT__/$port/g;
$start_shell_script    =~ s/__WORKER_COUNT__/$worker_count/g;
$start_shell_script    =~ s/__EVENT_COUNT__/$event_count/g;

my $stop_shell_script  = $STOP_SH;
$stop_shell_script     =~ s/__INSTALL_DIR__/$install_dir/g;
$stop_shell_script     =~ s/__USERNAME__/$username/g;
$stop_shell_script     =~ s/__DBNAME__/$dbname/g;
$stop_shell_script     =~ s/__HOSTNAME__/$hostname/g;
$stop_shell_script     =~ s/__PORT__/$port/g;
$stop_shell_script     =~ s/__WORKER_COUNT__/$worker_count/g;
$stop_shell_script     =~ s/__EVENT_COUNT__/$event_count/g;

my $reload_shell_script = $RELOAD_SH;
$reload_shell_script    =~ s/__INSTALL_DIR__/$install_dir/g;
$reload_shell_script    =~ s/__USERNAME__/$username/g;
$reload_shell_script    =~ s/__DBNAME__/$dbname/g;
$reload_shell_script    =~ s/__HOSTNAME__/$hostname/g;
$reload_shell_script    =~ s/__PORT__/$port/g;
$reload_shell_script    =~ s/__WORKER_COUNT__/$worker_count/g;
$reload_shell_script    =~ s/__EVENT_COUNT__/$event_count/g;

unless( open( STARTFILE, ">${SH_TARGET_DIR}${START_FILE_NAME}" ) )
{
    croak "Failed to write to ${SH_TARGET_DIR} for systemd startup script";
}

print STARTFILE $start_shell_script;
close STARTFILE;
chmod "0755", "${SH_TARGET_DIR}${START_FILE_NAME}";

unless( open( STOPFILE, ">${SH_TARGET_DIR}${STOP_FILE_NAME}" ) )
{
    croak "Failed to write to ${SH_TARGET_DIR} for systemd shutdown script";
}

print STOPFILE $stop_shell_script;
close STOPFILE;
chmod "0755", "${SH_TARGET_DIR}${STOP_FILE_NAME}";

unless( open( RELOADFILE, ">${SH_TARGET_DIR}${RELOAD_FILE_NAME}" ) )
{
    croak "Failed to write to ${SH_TARGET_DIR} for systemd reload script";
}

print RELOADFILE $reload_shell_script;
close RELOADFILE;
chmod "0755", "${SH_TARGET_DIR}${RELOAD_FILE_NAME}";

if(
        -e "${SH_TARGET_DIR}${START_FILE_NAME}"
    and -e "${SH_TARGET_DIR}${STOP_FILE_NAME}"
    and -e "${SH_TARGET_DIR}${RELOAD_FILE_NAME}"
  )
{
    print "Copied start/stop scripts to ${SH_TARGET_DIR}\n";
}
else
{
    croak "Failed to setup start/stop scripts in ${SH_TARGET_DIR}\n";
}

unless( -e $SYSTEMD_SERVICE_DIR )
{
    croak 'Is systemd installed??';
}

unless( copy( "${install_dir}/service/templates/event_manager.service", $SYSTEMD_SERVICE_DIR ) )
{
    print "source dir is ${install_dir}/service/templates/event_manager.service\n";
    croak "Failed to copy file to $SYSTEMD_SERVICE_DIR: $OS_ERROR";
}

my $result = system( "systemd-analyze verify ${SYSTEMD_SERVICE_DIR}/event_manager.service" );

if( $result )
{
    croak "Service installation failed";
}

unless( copy( "${install_dir}/service/templates/event_manager.logrotate", $LOGROTATE_DEST ) )
{
    croak "Failed to setup logrotation: $OS_ERROR";
}

system( 'systemctl daemon-reload' );
system( 'systemctl enable event_manager.service' );
exit 0;
