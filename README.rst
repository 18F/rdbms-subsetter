rdbms-subsetter
===============

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

The destination database will have <fraction> of the source's rows for child
tables.  Parent tables will have more rows - enough to meet all the child 
tables' referential integrity constraints.

When row numbers in your tables vary wildly (tens to billions, for example),
consider using 
the ``-l`` flag reduces row counts by a logarithmic formula.  If ``f`` is
the fraction specified, and ``-l`` is set, and the original table has ``n`` rows,
then each new table's rowcount will be::

    math.pow(10, math.log10(n)*f)
    
A fraction of ``0.5`` seems to produce good results, converting 10 rows to 3,
1,000,000 to 1,000,000, and 1,000,000,000 to 31,622.

rdbms-subsetter only performs the INSERTS; it's your responsibility to set
up the target database first, with its foreign key constraints.  The easiest
way to do this is with your RDBMS's dump utility.  For example, for PostgreSQL,

::

    pg_dump --schema-only -f schemadump.sql source_database
    createdb partial_database
    psql -f schemadump.sql partial_database

Installing
----------

::

    pip install rdbms-subsetter
    
Then the DB-API2 module for your RDBMS; for example, for PostgreSQL,

::

    pip install psycopg2
