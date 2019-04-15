#!/usr/bin/perl
=pod
/*------------------------------------------------------------------------
 *
 * uninstall.pl
 *     Basic systemctl uninstaller for Event Manager
 *
 * Copyright (c) 2018, Nead Werx, Inc.
 *
 * IDENTIFICATION
 *        uninstall.pl
 *
 *------------------------------------------------------------------------
 */
=cut

use strict;
use warnings;

use Readonly;
use Carp;
use Cwd;
use English qw( -no_match_vars );
use File::Path qw( make_path );
use Params::Validate qw( :all );

Readonly my $SH_TARGET_DIR       => '/usr/bin/';
Readonly my $START_FILE_NAME     => 'event_manager-startup.sh';
Readonly my $STOP_FILE_NAME      => 'event_manager-shutdown.sh';
Readonly my $RELOAD_FILE_NAME    => 'event_manager-reload.sh';
Readonly my $SYSTEMD_SERVICE_DIR => '/usr/lib/systemd/system/';
Readonly my $LOGROTATE_SCRIPT    => '/etc/logrotate.d/event_manager';

if( $EFFECTIVE_USER_ID != 0 )
{
    croak( 'Must be root' );
}

if( -e "${SYSTEMD_SERVICE_DIR}event_manager.service" )
{
    system( 'systemctl disable event_manager.service' );

    unless( unlink( "${SYSTEMD_SERVICE_DIR}event_manager.service" ) )
    {
        croak "Failed to remove service file from '${SYSTEMD_SERVICE_DIR}'";
    }

    unless( unlink( "${SH_TARGET_DIR}${START_FILE_NAME}" ) )
    {
        croak "Failed to remove startup script";
    }

    unless( unlink( "${SH_TARGET_DIR}${STOP_FILE_NAME}" ) )
    {
        croak "Failed to remove shutdown script";
    }

    unless( unlink( "${SH_TARGET_DIR}${RELOAD_FILE_NAME}" ) )
    {
        croak "Failed to remove reload script";
    }

    if( -e "${LOGROTATE_SCRIPT}" && !unlink( "${LOGROTATE_SCRIPT}" ) )
    {
        croak "Failed to remove logrotate script";
    }

    system( 'systemctl daemon-reload' );
    print( "Event manager has been uninstalled\n" );
}
else
{
    print( "Event manager is not installed\n" );
}

exit 0;
