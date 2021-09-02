#!/usr/bin/perl

use strict;
use warnings;

use Readonly;
use Cwd qw( abs_path );
use Carp;

# Simply verify the version string in CURRENT_VERSION is valid form and acceptable range
Readonly my $VALID_VERSION => [ '0.1', '0.2', '0.3' ];
Readonly my $FILE          => 'CURRENT_VERSION';
Readonly my $ALT_FILE      => '../CURRENT_VERSION';

my $fh;
my $cwd = abs_path();

if( $cwd =~ m/event_manager$/ and -e $FILE )
{
    unless( open( $fh, '<', $FILE ) )
    {
        carp( "Cannot open version file '$FILE'" );
        exit 1;
    }
}
elsif( $cwd =~ m/event_manager\/tools\/$/ and -e $ALT_FILE )
{
    unless( open( $fh, '<', $ALT_FILE ) )
    {
        carp( "Cannot open version file '$ALT_FILE'" );
        exit 1;
    }
}
else
{
    carp( 'Version file does not exist or this script is being run outside of build root' );
    exit 1;
}

my $line = <$fh>;
close( $fh );

$line =~ s/\n//;

unless( $line =~ m/^\d+\.\d+$/ )
{
    carp( "Version line in $FILE is expected to be in the form <number>.<number>" );
    exit 1;
}

my %versions = map { $_ => 1 } @$VALID_VERSION;

unless( exists( $versions{$line} ) )
{
    carp( "Version '$line' is not an acceptable version." );
    exit 1;
}

exit 0;
