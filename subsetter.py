"""
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

"""
import argparse
import logging
from collections import OrderedDict
import math
import random
import types
import sqlalchemy as sa
from sqlalchemy.engine.reflection import Inspector


def _find_n_rows(self):
    self.n_rows = self.db.conn.execute(self.count()).fetchone()[0]

def _random_rows(self, n):
    """
    Random sample of *approximate* size n
    """
    if self.n_rows:
        if self.n_rows > 1000:
            fraction = n / float(self.n_rows)
            qry = sa.sql.select([self,]).where(sa.sql.functions.random() < fraction)
        else:
            qry = sa.sql.select([self,]).order_by(sa.sql.functions.random()).limit(n)
        for row in self.db.conn.execute(qry):
            yield row
    
class Db(object):
    
    def __init__(self, sqla_conn):
        self.sqla_conn = sqla_conn
        self.engine = sa.create_engine(sqla_conn)
        self.meta = sa.MetaData(bind=self.engine)
        self.meta.reflect()
        self.inspector = Inspector(bind=self.engine)
        self.conn = self.engine.connect()
        self.tables = OrderedDict()
        for tbl in reversed(self.meta.sorted_tables):
            tbl.db = self
            tbl.fks = self.inspector.get_foreign_keys(tbl.name)
            tbl.find_n_rows = types.MethodType(_find_n_rows, tbl)
            tbl.random_rows = types.MethodType(_random_rows, tbl)
            self.tables[tbl.name] = tbl
        logging.debug('Tables will be populated in this order: ' +
                      ', '.join(self.tables.keys()))
        
    def __repr__(self):
        return "Db('%s')" % self.sqla_conn
 
    def create_row_in(self, source_child_row, target_db, target_child):
        # make sure that all required rows are in parent table(s)
        for fk in target_child.fks:
            target_parent = target_db.tables[fk['referred_table']]
            slct = sa.sql.select([target_parent,])
            any_non_null_key_columns = False
            for (parent_col, child_col) in zip(fk['referred_columns'], 
                                               fk['constrained_columns']):
                slct = slct.where(target_parent.c[parent_col] == 
                                  source_child_row[child_col])
                if source_child_row[child_col] is not None:
                    any_non_null_key_columns = True
            if any_non_null_key_columns:
                target_parent_row = target_db.conn.execute(slct).first()
                if not target_parent_row:
                    source_parent_row = self.conn.execute(slct).first()
                    self.create_row_in(source_parent_row, target_db, target_parent)
        ins = target_child.insert().values(**source_child_row)
        target_db.conn.execute(ins)
        
    def create_subset_in(self, target_db, fraction, logarithmic=False):
        for (source_child_name, source_child) in self.tables.items():
            source_child.find_n_rows()
            if not source_child.n_rows:
                logging.warn("No source rows for %s, skipping" % source_child_name)
                continue
            target_child = target_db.tables[source_child_name] 
            target_child.find_n_rows()
            if logarithmic:
                n_rows_desired = int(math.pow(10, math.log10(source_child.n_rows)
                                                  * fraction)) or 1
            else:
                n_rows_desired = int(source_child.n_rows * fraction) or 1
            n_to_create = n_rows_desired - target_child.n_rows
            logging.info("%s in source has %d rows" % 
                         (source_child_name, source_child.n_rows))
            logging.info("%s in target has %d rows" % 
                         (source_child_name, target_child.n_rows))
            if n_to_create > 0:
                logging.info("adding %d rows to target (desired total: %d)" % 
                             (n_to_create, n_rows_desired))
                for (n, source_child_row) in enumerate(source_child.random_rows(n_to_create)):
                    self.create_row_in(source_child_row, target_db, target_child)
                    if n and (not n % 1000):
                        logging.info("%s rows created so far in %s" % (n, source_child_name))
                logging.info("%s rows created in %s" % (n+1, source_child_name))
                    
                    
                    
def fraction(n):
    n = float(n)        
    if 0 < n <= 1:
        return n
    raise argparse.ArgumentError('Fraction must be greater than 0 and no greater than 1')

all_loglevels = "CRITICAL, FATAL, ERROR, DEBUG, INFO, WARN, WARNING"
def loglevel(raw):
    try:
        return int(raw)
    except ValueError:
        upper = raw.upper()
        if upper in all_loglevels:
            return getattr(logging, upper)
        raise NotImplementedError('log level "%s" not one of %s' % (raw, all_levels))
   
argparser = argparse.ArgumentParser(description='Generate consistent subset of a database')
argparser.add_argument('source', help='SQLAlchemy connection string for data origin',
                       type=str)
argparser.add_argument('dest', help='SQLAlchemy connection string for data destination',
                       type=str)
argparser.add_argument('fraction', help='Proportion of rows to create in dest (0.0 to 1.0)',
                       type=fraction)
argparser.add_argument('-l', '--logarithmic', help='Cut row numbers logarithmically; use 0.5 for fraction', 
                       action='store_true')
argparser.add_argument('--loglevel', type=loglevel, help='log level (%s)' % all_loglevels,
                       default='INFO')


def generate():
    args = argparser.parse_args()
    logging.getLogger().setLevel(args.loglevel)
    source = Db(args.source)
    target = Db(args.dest)
    source.create_subset_in(target, args.fraction, args.logarithmic)
   
