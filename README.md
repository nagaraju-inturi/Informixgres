# ![Informixgress](informixgres.png)
## PostgreSQL protocol gateway for Informix

**Informixgres** is a gateway server that allows clients to use PostgreSQL protocol to run queries on Informix.

You can use any PostgreSQL clients (see also *[Limitation](#limitation)* section):

* `psql` command
* [PostgreSQL ODBC driver](http://psqlodbc.projects.pgfoundry.org/)
* [PostgreSQL JDBC driver](http://jdbc.postgresql.org/)

Informixgres also offers password-based authentication and SSL.

Architecture Diagram:

![alt text](informixgres.png "Architecture Diagram")

## Documents

* [How it works?](#how-it-works)
* [Limitation](#limitation)
* [Installation](#installation)
  * [1. Install PostgreSQL >= 9.3](#1-install-postgresql--93)
  * [2. Install Informixgres](#2-install-prestogres)
* [Running servers](#running-servers)
  * [Setting shmem max parameter](#setting-shmem-max-parameter)
* [Configuration](#configuration)
* [Authentication](#authentication)
  * [md5 method](#md5-method)
  * [external method](#external-method)
* [FAQ](#faq)
  * [I can connect from localhost but cannot from remote host](#i-can-connect-from-localhost-but-cannot-from-remote-host)
  * [I can connect to Informixgres but cannot run any queries](#i-can-connect-to-prestogres-but-cannot-run-any-queries)
  * [All queries by JDBC or ODBC clients fail with "Informixgres doesn't support extended query"](#all-queries-by-jdbc-or-odbc-clients-fail-with-prestogres-doesnt-support-extended-query)
  * [Time zone of timestamp type is wrong](#time-zone-of-timestamp-type-is-wrong")

---

## How it works?

Informixgres uses modified version of **[pgpool-II](http://www.pgpool.net/)** to rewrite queries before sending them to PostgreSQL.
pgpool-II is originally a middleware to provide connection pool and load balancing to PostgreSQL. Informixgres hacked it as following:

* When a client connects to pgpool-II, the modified pgpool-II runs **SELECT setup\_system\_catalog(...)** statement on PostgreSQL.
  * This function is implemented on PostgreSQL using PL/Python.
  * It gets list of tables from Presto, and runs CREATE TABLE for each tables.
  * Those created tables are empty, but clients can get the table schemas.
* When the client runs a regular SELECT statement, the modified pgpool-II rewrites the query to run **SELECT * FROM fetch\_presto\_query\_results(...)** statement.
  * This function runs the original query on Presto and returns the results.
* If the statement is not regular SELECT (such as SET, SELECT from system catalogs, etc.), pgpool-II simply forwards the statement to PostgreSQL without rewriting.

In fact, there're some more tricks. See [prestogres/pgsql/prestogres.py](prestogres/pgsql/prestogres.py) for the real behavior.

---

## Limitation

* Extended query is not supported
  * ODBC driver needs to set:
     * **Server side prepare = no** property (UseServerSidePrepare=0 at .ini file)
     * **Level of rollback on errors = Transaction** property (Protocol=7.4-0 or Protocol=6.4 at .ini file)
     * **Unicode** mode
  * JDBC driver needs to set:
     * **protocolVersion=2** property
* Temporary table is not supported
* Some SQL commands of Presto don't work
  * Supported:
    * SELECT
    * EXPLAIN
    * INSERT INTO
    * CREATE TABLE
    * CREATE VIEW
  * Not supported:
    * DROP TABLE

---

## Installation

### 1. Install PostgreSQL >= 9.3

You need to install PostgreSQL separately. Following commands install PostgreSQL 9.3 from postgresql.org:

**Ubuntu/Debian:**

```sh
# add apt source
$ sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
$ wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
$ sudo apt-get update
# install PostgreSQL
$ sudo apt-get install postgresql-9.3 postgresql-contrib-9.3 postgresql-server-dev-9.3 postgresql-plpython-9.3
# install other dependencies
$ sudo apt-get install gcc make libssl-dev libpcre3-dev
```

**RedHat/CentOS:**

```sh
# add yum source
$ sudo yum install http://yum.postgresql.org/9.3/redhat/rhel-6-x86_64/pgdg-redhat93-9.3-1.noarch.rpm
# install PostgreSQL
$ sudo yum install postgresql93-server postgresql93-contrib postgresql93-devel postgresql93-plpython
# install other dependencies
$ sudo yum install gcc make openssl-devel pcre-devel
```

**Mac OS X:**

You can install PostgreSQL using [Homebrew](http://brew.sh/).

```sh
brew install postgresql
```

### 2. Install Informixgres

Download the latest release from [releases](https://github.com/treasure-data/prestogres/releases) or clone the [git repository](https://github.com/treasure-data/prestogres). You can install the binary as following:

```
$ ./configure --program-prefix=prestogres- # if error occurs, add pg_config command $PATH (e.g. $ export PATH=/usr/pgsql-9.3/bin:$PATH)
$ make
$ sudo make install
```

You can find **prestogres-ctl** command:

```
$ prestogres-ctl --help
```

---

## Running servers

You need to run 2 server programs: pgpool-II and PostgreSQL.
You can use `prestogres-ctl` command to setup & run them as following:

```sh
# 1. Configure configuration file (at least presto_server parameter):
$ vi /usr/local/etc/informix.conf

# 2. Create a data directory:
$ prestogres-ctl create pgdata
# vi pgdata/postgresql.conf  # edit configuration if necessary

# 3. Start PostgreSQL
$ prestogres-ctl postgres -D pgdata

# 4. Open another shell, and initialize the database to install PL/Python functions
$ prestogres-ctl migrate

# 5. Start pgpool-II:
$ prestogres-ctl pgpool

# 6. Finally, you can connect to pgpool-II using psql command.
#    Database name ('hive') is name of a Presto catalog:
$ psql -h 127.0.0.1 -p 5439 -U presto hive
```

If configuration is correct, you can run `SELECT * FROM sys.node;` query. Otherwise, see log messages.

#### Setting shmem max parameter

Above command fails first time on most of environments! Error message is:

```
FATAL:  could not create shared memory segment: Cannot allocate memory
DETAIL:  Failed system call was shmget(key=6432001, size=3809280, 03600).
HINT:  This error usually means that PostgreSQL's request for a shared memory segment exceeded
available memory or swap space, or exceeded your kernel's SHMALL parameter.  You can either
reduce the request size or reconfigure the kernel with larger SHMALL.  To reduce the request
size (currently 3809280 bytes), reduce PostgreSQL's shared memory usage, perhaps by reducing
shared_buffers or max_connections.
```

You need to set 2 kernel parameters to run PostgreSQL.

**Linux:**

```
sudo bash -c "echo kernel.shmmax = 17179869184 >> /etc/sysctl.conf"
sudo bash -c "echo kernel.shmall = 4194304 >> /etc/sysctl.conf"
sudo sysctl -p /etc/sysctl.conf
```

**Mac OS X:**

```
$ sudo sysctl -w kern.sysv.shmmax=1073741824
$ sudo sysctl -w kern.sysv.shmall=1073741824
```

---

## Configuration

Please read [pgpool-II documentation](http://www.pgpool.net/docs/latest/pgpool-en.html) for most of parameters used in informix.conf file.
Following parameters are unique to Informixgres:

* **informix_server**: Python/ODBC URL for Informix Server.
* **presto_catalog**: (optional) catalog name of Presto (such as `hive`, etc.). By default, login database name is used as the catalog name
* **presto_schema**: (optional) schema name of Presto (such as `hive`, etc.). By default, login database name is used as the schema name
* **presto_external_auth_prog**: (optional) path to an external authentication program used by `external` authentication moethd. See following *Authentication* section for details.

You can overwrite these parameters for each connecting users (and databases) using informix\_hba.conf file. See also following *Authentication* section.

## Authentication

By default, Informixgres accepts all connections from 127.0.0.1 without password and rejects any other connections. You can change this behavior by updating **$prefix/etc/informix\_hba.conf** file.

See [sample informix_hba.conf file](prestogres/config/informix_hba.conf) for details. Basic syntax is:

```conf
# TYPE  DATABASE  USER    CIDR-ADDRESS             METHOD        OPTIONS

# trust from 192.168.x.x without password
host    all       all     127.0.0.1/32             trust

# trust from 192.168.x.x without password
host    all       all     192.168.0.0/16           trust

# trust from 10.{1,2}.x.x without password
host    all       all     10.0.0.0/16,10.1.0.0/16  trust

# require password authentication from 10.3.x.x
host    all       all     10.3.0.0/16              md5

# overwrite presto_server address and catalog name if the login database name is altdb
host    altdb     all     0.0.0.0/0                md5           presto_server:alt.presto.example.com:8190,presto_catalog:hive

# run external command to authenticate if login user name is myuser
host    all       myuser  0.0.0.0/0                external      auth_prog:/opt/prestogres/auth.py
```

### md5 method

This authentication method uses a password file (**$prefix/etc/prestogres\_passwd**) to authenticate an user. You can use `prestogres passwd` command to add an user to this file:

```sh
$ prestogres-pg_md5 -pm -u myuser
password: (enter password here)
```

In informix\_hba.conf file, you can set following options to the OPTIONS field:

* **informix_server**: Python and ODBC URL for Informix database server. URL of Informix coordinator, which overwrites `informix_servers` parameter in informix.conf.
* **presto_catalog**: catalog name of Presto, which overwrites `presto_catalog` parameter in informix.conf.
* **presto_schema**: schema name of Presto, which overwrites `presto_schema` parameter in informix.conf.
* **presto_user**: user name to run queries on Presto (X-Presto-User). By default, login user name is used. Following `pg_user` parameter doesn't affect this parameter.
* **pg_database**: (advanced) Overwrite database name on PostgreSQL. By default, login database name is used as-is. If this database does not exist on PostgreSQL, Informixgres automatically creates it.
* **pg_user**: (advanced) Overwrite user name connecting to PostgreSQL. This value should be `prestogres` in most of cases. If you create another superuser on PostgreSQL manually, you may use this parameter.


### external method

This authentication method runs an external command to authentication an user.

- Note: This method is still experimental. Interface could be changed.
- Note: This method requires clients to send password in clear text. It's recommended to enable SSL in informix.conf.

You need to set `presto_external_auth_prog` parameter in informix.conf or `auth_prog` option in informix\_hba.conf. Informixgres runs the program every time when an user connects. The program receives following data through STDIN:

```
user:USER_NAME
password:PASSWORD
database:DATABASE
address:IPADDR

```

If you want to allow this connection, the program optionally prints parameters as following to STDOUT, and exists with status code 0:

```
presto_server:PRESTO_SERVER_ADDRESS
presto_catalog:PRESTO_CATALOG_NAME
presto_schema:PRESTO_SCHEMA_NAME
presto_user:USER_NAME
pg_database:DATABASE
pg_user:USER_NAME

```

If you want to reject this connection, the program exists with non-0 status code.

---

## FAQ

### I can connect from localhost but cannot from remote host

Informixgres trusts connections from localhost and rejects any other connections by default.
To connect Informixgres from a remote host, you need to edit informix\_hba.conf file.

See [Authentication](#authentication) section for details.


### I can connect to Informixgres but cannot run any queries

Informixgres gets all table information from Presto when you run the first query for each connection. If this initialization fails, all queries fail.

Informixgres runs following SQL on Presto to get table information. If this query fails on your Presto, Informixgres doesn't work.

```sql
select table_schema, table_name, column_name, is_nullable, data_type
from information_schema.columns
```


### All queries by JDBC or ODBC clients fail with "Informixgres doesn't support extended query"

PostgreSQL has 2 protocols to run a query: simple query and extended query.

Extended query is a new protocol to support prepared statements (server-side prepared statements). Informixgres supports only simple query.

Fortunately, JDBC and ODBC clients implement prepared statements at client-side to be complatible with old PostgreSQL. You need to disable server-side prepared statements and enable the client-side implementation.

See [Limitation](#limitation) section for the parameters.

If you have interest in the detailed protocol specification: [PostgreSQL Frontend/Backend Protocol](http://www.postgresql.org/docs/9.3/static/protocol.html).


### Time zone of timestamp type is wrong

Informixgres checks `timezone` session variable and passes it to Presto (X-Presto-Time-Zone).

* To check timezone session variable: `SHOW timezone`
* To change the timezone on a session: `SET timezone TO UTC`
* To change the default timezone, edit `timezone` parameter at postgresql.conf file located in PostgreSQL's data directory you created using `prestogres-ctl create` command.


___

    Informixgres is licensed under Apache License, Version 2.0. Its based on Prestogres project. https://github.com/treasure-data/prestogres

