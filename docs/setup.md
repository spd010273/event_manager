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
yum install postgresql<version>-devel postgresql<version>-libs gcc git libtool make

which pg_config | grep 'no\spg_config' # Make sure it is in the users PATH

# Make sure libpq* is listed
sudo ldconfig -v | grep libpq
ldconfig -p | grep libpq

git clone https://spd010273@bitbucket.org/neadwerx/event_manager.git
cd event_manager
make
sudo make install
```

## Debian / Ubuntu


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
