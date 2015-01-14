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

``rdbms-subsetter`` promises that each child row will have whatever parent rows are
required by its foreign keys.  It will also *try* to include most child rows belonging
to each parent row (up to the supplied ``--children`` parameter, default 3 each), but it
can't make any promises.  (Demanding all children can lead to infinite propagation in
thoroughly interlinked databases, as every child record demands new parent records,
which demand new child records, which demand new parent records...
so increase ``--children`` with caution.)

When row numbers in your tables vary wildly (tens to billions, for example),
consider using the ``-l`` flag, which sets row number targets
by a logarithmic formula.  
When ``-l`` is set, if ``f`` is the fraction specified, 
and the original table has ``n`` rows,
then each new table's row target will be::

    math.pow(10, math.log10(n)*f)

A fraction of ``0.5`` seems to produce good results, converting 10 rows to 3,
1,000,000 to 1,000, and 1,000,000,000 to 31,622.

Rows are selected randomly, but for tables with a single primary key column, you
can force rdbms-subsetter to include specific rows (and their dependencies) with
``force=<tablename>:<primary key value>``.  The children, grandchildren, etc. of 
these rows
are exempted from the ``--children`` limit.

``rdbms-subsetter`` only performs the INSERTS; it's your responsibility to set
up the target database first, with its foreign key constraints.  The easiest
way to do this is with your RDBMS's dump utility.  For example, for PostgreSQL,

::

    pg_dump --schema-only -f schemadump.sql bigdb
    createdb littledb
    psql -f schemadump.sql littledb

Rows are taken from the schema visible by default to your
database connection.  You can also include rows from non-default schemas
with the ``--schema=<name>`` parameter (which can be used multiple times).
Currently the target database must contain the corresponding tables in its own
schema of the same name (moving between schemas of different names is not yet
supported).

Installing
----------

::

    pip install rdbms-subsetter

Then the DB-API2 module for your RDBMS; for example, for PostgreSQL,

::

    pip install psycopg2

Memory
------

Will consume memory roughly equal to the size of the *extracted* database.
(Not the size of the *source* database!)

Development
-----------

https://github.com/18F/rdbms-subsetter

See also
--------

* `Jailer <http://jailer.sourceforge.net/home.htm>`_
