Event Manager Setup
===================

# Installation

## CentOS / RHEL

Package version numbers are (mostly) expressed in the following format:
postgresXY, where X is the major version number and Y is the minor version number. For instance:
PostgreSQL 9.6 = postgresql96-server
PostgreSQL 10.2 = postgresql10-server

```bash
yum update
yum install postgresql<version>-devel postgresql<version>-libs gcc git libtool make libcurl-devel

which pg_config | grep 'no\spg_config' # Make sure it is in the users PATH

# Make sure libpq* is listed
sudo ldconfig -v | grep libpq
ldconfig -p | grep libpq

git clone https://bitbucket.org/neadwerx/event_manager.git
cd event_manager
git submodule init
git submodule update
make
sudo env "PATH=$PATH" make install
```
Additionally, you can run the install script in the event_manager repo. This will initialize the submodule as well as install event_manager under systemd. Event manager logs to /var/log/event_manager/event_manager.log when in daemon mode (-D argument)
## Debian / Ubuntu

apt-get update
apt-get install libcurl4-openssl-dev git gcc make

## Windows
No

## MacOS
No

# Extension Creation

The extension will, by default, be created in a schema called event_manager after running
```sql
CREATE EXTENSION event_manager;
```

This will generate the necessary tables, functions, and triggers for the Event Manager extension to function.

# Specifying a build version

The file in this repository root, CURRENT_VERSION, allows you to specify the version you with to compile, install, and test. This file is expected to have a single line in the form of <major_version>.<minor_version>. By default, the event_manager.control file is set to install 0.1.


# Upgrade

## v0.2
If you would like to update the version which event_manager runs as, you will need to modify CURRENT_VERSION in the root directory. This file should contain only the version number with no linebreaks or empty lines

```sql
ALTER EXTENSION event_manager UPDATE TO '0.2';
```
