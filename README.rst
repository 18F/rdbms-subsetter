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
which demand new child records, which demand new parent records...)

When row numbers in your tables vary wildly (tens to billions, for example),
consider using the ``-l`` flag, which reduces row counts by a logarithmic formula.  If ``f`` is
the fraction specified, and ``-l`` is set, and the original table has ``n`` rows,
then each new table's row target will be::

    math.pow(10, math.log10(n)*f)

A fraction of ``0.5`` seems to produce good results, converting 10 rows to 3,
1,000,000 to 1,000,000, and 1,000,000,000 to 31,622.

rdbms-subsetter guarantees that your child rows have the necessary parent rows to
satisfy the foreign keys.  It also *tries* to ensure that your parent rows have
child keys, but that becomes tricky when you have a complex web of foreign keys.
Creating children for a parent may require creating more parent rows in multiple
tables, each of which may call for their own children... that process can propagate
endlessly.  rdbms-subsetter cuts the propagation off eventually, but you can
guarantee that specific tables will always have children by naming those tables
with ``require-children=<tablename>``.

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
