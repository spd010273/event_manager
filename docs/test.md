Event Manager Tests
===================

# Preparation

The tests are implemented using both Perl and SQL. To run testing, the following is required:

* Perl
* Params::Validate
* Carp
* Readonly
* Perl6::Export::Attrs
* English
* JSON
* DBI::pg
* Getopt::Std
* Time::HiRes
* Cwd
* File::Temp
* File::Glob

These can be installed via cpan minus (cpanm) or using the command line:

```bash
perl -MCPAN -e shell
install <library>
```

These tests assume this is a development system, and will terminate running instances of event_manager daemons in order to test asynchronous mode.

It goes without saying that this should not be run against a production environment

# Running

Tests can be ran using the run_tests.pl script:

```bash
./run_tests.pl -U <username> -p <port> -h <hostname> -d <dbname>
```

It will return an itemized list of passed tests, stopping on the first failure

# Development Testing

Regression tests can be performed by using run_tests.pl, and debugging can be done using Valgrind.

To perform testing using Valgrind, pass the -V flag to run_tests.pl - It will start the queue processors under valgrind
