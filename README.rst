rdbms-subsetter
===============

.. image:: https://travis-ci.org/18F/rdbms-subsetter.svg?branch=master
    :target: https://travis-ci.org/18F/rdbms-subsetter
Generate a random sample of rows from a relational database that preserves
referential integrity - so long as constraints are defined, all parent rows
will exist for child rows.

Good for creating test/development databases from production.  It's slow,
but how often do you need to generate a test/development database?

Usage::

    rdbms-subsetter <source SQLAlchemy connection string> <destination connection string> <fraction of rows to use>

Example::

    rdbms-subsetter postgresql://:@/bigdb postgresql://:@/littledb 0.05

Valid SQLAlchemy connection strings are described
`here <docs.sqlalchemy.org/en/latest/core/engines.html#database-urls#database-urls>`_.

The destination database will have approximately <fraction> of the source's
rows for parent tables.  Actual numbers may vary wildly, as related rows are
loaded to satisfy foreign key relationships.

When row numbers in your tables vary wildly (tens to billions, for example),
consider using the ``-l`` flag reduces row counts by a logarithmic formula.
If ``f`` is the fraction specified, and ``-l`` is set, and the original table
has ``n`` rows, then each new table's rowcount will be::

    math.pow(10, math.log10(n)*f)

A fraction of ``0.5`` seems to produce good results, converting 10 rows to 3,
1,000,000 to 1,000,000, and 1,000,000,000 to 31,622.

rdbms-subsetter guarantees that your child rows have the necessary parent rows
to satisfy the foreign keys.  Sometimes it's also important to ensure that
parent records have child records, when a parent record without children would
be nonsensical or useless for testing.  For those parent tables, you can demand
child records for each parent record by specifying::

    --require-children=<tablename>

You can require children for multiple tables with multiple uses of
``--require-children``.  However, requiring children for too many parent
tables, when you have a complex web of foreign key relationships, could
lead to your entire source database being drawn into the destination, as
parent rows require child rows require parent rows require child rows...

Rows are selected randomly, but for tables with a single primary key column, you
can force rdbms-subsetter to include specific rows (and their dependencies) with
``force=<tablename>:<primary key value>``.

rdbms-subsetter only performs the INSERTS; it's your responsibility to set
up the target database first, with its foreign key constraints.  The easiest
way to do this is with your RDBMS's dump utility.  For example, for PostgreSQL,

::

    pg_dump --schema-only -f schemadump.sql source_database
    createdb partial_database
    psql -f schemadump.sql partial_database

Currently rdbms-subsetter takes no account of schema names and simply assumes all
tables live in the same schema.  This will probably cause horrible errors if used
against databases where foreign keys span schemas.

Installing
----------

::

    pip install rdbms-subsetter

Then the DB-API2 module for your RDBMS; for example, for PostgreSQL,

::

    pip install psycopg2

See also
--------

* `Jailer <http://jailer.sourceforge.net/home.htm>`_
