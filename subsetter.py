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

The destination database will have approximately <fraction> of the source's rows for parent
tables.  Child table numbers are harder to predict - the program tries to create the full complement of
child records belonging to each parent, when that number is reasonable.

When row numbers in your tables vary wildly (tens to billions, for example), consider using
the ``-l`` flag reduces row counts by a logarithmic formula.  If ``f`` is
the fraction specified, and ``-l`` is set, and the original table has ``n`` rows,
then each new table's rowcount will be::

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
    self.n_rows = 0
    try:
        if self.db.engine.driver in ('psycopg2', 'pg8000',):
            qry = """SELECT reltuples FROM pg_class 
                     WHERE oid = '%s'::regclass""" % self.name
        elif 'oracle' in self.db.engine.driver:
            qry = """SELECT num_rows FROM all_tables
                     WHERE table_name='%s'""" % self.name
        else:
            raise NotImplementedError("No approximation known for driver %s"
                                      % self.db.engine.driver)
        self.n_rows = self.db.conn.execute(qry).fetchone()[0]
    except Exception as e:
        logging.debug("failed to get approximate rowcount for %s\n%s" %
                      (self.name, str(e)))
    if not self.n_rows:
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
   
def _filtered_by(self, **kw):
    slct = sa.sql.select([self,])
    slct = slct.where(sa.sql.and_((self.c[k] == v) for (k, v) in kw.items()))
    return slct

def _pk_val(self, row):
    if self.pk:
        return row[self.pk[0]]
    else:
        return None

def _by_pk(self, pk):
    pk_name = self.db.inspector.get_primary_keys(self.name)[0]
    slct = self.filtered_by(**({pk_name:pk}))
    return self.db.conn.execute(slct).fetchone()

def _exists(self, **kw):
    return bool(self.db.conn.execute(self.filtered_by(**kw)).first())
    
class Db(object):
    
    def __init__(self, sqla_conn):
        self.sqla_conn = sqla_conn
        self.engine = sa.create_engine(sqla_conn)
        self.meta = sa.MetaData(bind=self.engine)
        self.meta.reflect()
        self.inspector = Inspector(bind=self.engine)
        self.conn = self.engine.connect()
        self.tables = OrderedDict()
        for tbl in self.meta.sorted_tables:
            tbl.db = self
            tbl.fks = self.inspector.get_foreign_keys(tbl.name)
            tbl.pk = self.inspector.get_primary_keys(tbl.name)
            tbl.find_n_rows = types.MethodType(_find_n_rows, tbl)
            tbl.random_rows = types.MethodType(_random_rows, tbl)
            tbl.filtered_by = types.MethodType(_filtered_by, tbl)
            tbl.by_pk = types.MethodType(_by_pk, tbl)
            tbl.pk_val = types.MethodType(_pk_val, tbl)
            tbl.exists = types.MethodType(_exists, tbl)
            self.tables[tbl.name] = tbl
            tbl.child_fks = []
            tbl.rows_already_created = False
        for (tbl_name, tbl) in self.tables.items():
            logging.debug("Counting rows in %s" % tbl_name)
            tbl.find_n_rows()
            logging.debug("Counted %d rows in %s" % (tbl.n_rows, tbl_name))
            for fk in tbl.fks:
                fk['constrained_table'] = tbl_name
                self.tables[fk['referred_table']].child_fks.append(fk)
        logging.debug('Tables will be populated in this order: ' +
                      ', '.join(self.tables.keys()))
        
    def __repr__(self):
        return "Db('%s')" % self.sqla_conn

    GUARANTEED_CHILDREN = 8
                              
    def create_row_in(self, source_row, target_db, target, depth, prompted_by=None):
        logging.debug('create_row_in %s:%s depth %d' % 
                      (target.name, target.pk_val(source_row), depth))
        
        if target.exists(**(dict(source_row))):
            logging.debug("Row already exists; not creating")
            return
        
        # make sure that all required rows are in parent table(s)
        for fk in target.fks: 
            target_parent = target_db.tables[fk['referred_table']]
            slct = sa.sql.select([target_parent,])
            any_non_null_key_columns = False
            for (parent_col, child_col) in zip(fk['referred_columns'], 
                                               fk['constrained_columns']):
                slct = slct.where(target_parent.c[parent_col] == 
                                  source_row[child_col])
                if source_row[child_col] is not None:
                    any_non_null_key_columns = True
            if any_non_null_key_columns:
                target_parent_row = target_db.conn.execute(slct).first()
                if not target_parent_row:
                    source_parent_row = self.conn.execute(slct).first()
                    logging.debug("%s:%s needs a parent in %s; creating" % 
                                  (target.name, target.pk_val(source_row), 
                                   target_parent.name))
                    self.create_row_in(source_parent_row, target_db, target_parent, 
                                       depth=depth+1, prompted_by=target)
                    
        logging.debug('finished creating parents for %s:%s' % 
                      (target.name, target.pk_val(source_row)))
 
        # need to check for existence again, b/c creation of parent rows
        # could have touched off cascade to create it
        if target.exists(**(dict(source_row))):
            logging.debug("%s:%s apparently created recursively; not creating" %
                          (target.name, target.pk_val(source_row)))
            return
 
        ins = target.insert().values(**source_row)
        target_db.conn.execute(ins) 
        target.rows_already_created = True
   
        # child records may be required
        if target.name in self.args.require_children:
            for child_fk in target.child_fks:
                source_child_tbl = self.tables[child_fk['constrained_table']]
                logging.debug("Creating rows in %s as children of %s:%s" % 
                              (source_child_tbl.name, target.name, 
                               target.pk_val(source_row)))
                slct = sa.sql.select([source_child_tbl,])
                for (parent_col, child_col) in zip(child_fk['referred_columns'], 
                                                   child_fk['constrained_columns']):
                    slct = slct.where(source_child_tbl.c[child_col] == 
                                      source_row[parent_col])        
                slct = slct.limit(self.GUARANTEED_CHILDREN)
                target_child_tbl = target_db.tables[source_child_tbl.name]
                for child_row in self.conn.execute(slct):
                    self.create_row_in(child_row, target_db, target_child_tbl, 
                                       depth=depth+1, prompted_by=target)
                logging.debug("Finished creating rows in %s as children of %s:%s" % 
                              (source_child_tbl.name, target.name, 
                               target.pk_val(source_row)))
        
        logging.debug('finished creating all children for %s:%s' % 
                      (target.name, target.pk_val(source_row)))
        
    def create_subset_in(self, target_db, args):
        self.args = args
        for (source_name, source) in self.tables.items():
            if not source.n_rows:
                logging.warn("No source rows for %s, skipping" % source_name)
                continue
            target = target_db.tables[source_name] 
            rows_already_created = target.rows_already_created
            if source_name in args.force_rows:
                for pk in args.force_rows[source_name]:
                    source_row = source.by_pk(pk)  
                    if source_row:
                        self.create_row_in(source_row, target_db, target, 0)
                    else:
                        logging.warn("%s:%s not found in source db, could not create" %
                                     (source_name, pk))
            if rows_already_created:
                logging.info("table %s already populated via foreign key relations"
                             % source_name)
                continue
            if args.logarithmic:
                n_rows_desired = int(math.pow(10, math.log10(source.n_rows)
                                                  * args.fraction)) or 1
            else:
                n_rows_desired = int(source.n_rows * args.fraction) or 1
            n_to_create = n_rows_desired - target.n_rows
            logging.info("%s in source has %d rows" % 
                         (source_name, source.n_rows))
            logging.info("%s in target has %d rows" % 
                         (source_name, target.n_rows))
            if n_to_create > 0:
                logging.info("adding %d rows to target (desired total: %d)" % 
                             (n_to_create, n_rows_desired))
                for (n, source_row) in enumerate(source.random_rows(n_to_create)):
                    self.create_row_in(source_row, target_db, target, 0)
                    
                    
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
argparser.add_argument('-f', '--force', help='<table name>:<primary_key_val> to force into dest',
                       type=str.lower, action='append')
argparser.add_argument('-c', '--require-children', help='name of table that must have children',
                       type=str.lower, action='append')

def generate():
    args = argparser.parse_args()
    args.force_rows = {}
    for force_row in args.force:
        (table_name, pk) = force_row.split(':')
        if table_name not in args.force_rows:
            args.force_rows[table_name] = []
        args.force_rows[table_name].append(pk)
    logging.getLogger().setLevel(args.loglevel)
    logging.debug("Initializing source DB info")
    source = Db(args.source)
    logging.debug("Initializing target DB info")
    target = Db(args.dest)
    logging.debug("Begin copying rows from source to target")
    source.create_subset_in(target, args)
   
