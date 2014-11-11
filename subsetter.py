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


def _find_n_rows(self, estimate=False):
    self.n_rows = 0
    if estimate:
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


def _random_row_gen_fn(self):
    """
    Random sample of *approximate* size n
    """
    if self.n_rows:
        while True:
            n = self.target.n_rows_desired
            if self.n_rows > 1000:
                fraction = n / float(self.n_rows)
                qry = sa.sql.select([self,]).where(sa.sql.functions.random() < fraction)
                results = self.db.conn.execute(qry).fetchall()
                # we may stop wanting rows at any point, so shuffle them so as not to 
                # skew the sample toward those near the beginning 
                random.shuffle(results)
                for row in results:
                    yield row
            else:
                qry = sa.sql.select([self,]).order_by(sa.sql.functions.random()).limit(n)
                for row in self.db.conn.execute(qry):
                    yield row

def _next_row(self):
    try:
        return self.target.requested.pop(0)
    except IndexError:
        try:
            return next(self.random_rows)
        except StopIteration:
            return None

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

def _incompleteness_score(self):
    return ( (self.n_rows_desired - self.n_rows + len(self.requested))
             / float(self.source.n_rows or 1) )

class Db(object):

    def __init__(self, sqla_conn, args):
        self.sqla_conn = sqla_conn
        self.engine = sa.create_engine(sqla_conn)
        self.meta = sa.MetaData(bind=self.engine)
        self.meta.reflect()
        self.inspector = Inspector(bind=self.engine)
        self.conn = self.engine.connect()
        self.tables = OrderedDict()
        for tbl in reversed(self.meta.sorted_tables):
            tbl.db = self
            tbl.find_n_rows = types.MethodType(_find_n_rows, tbl)
            tbl.fks = self.inspector.get_foreign_keys(tbl.name)
            tbl.pk = self.inspector.get_primary_keys(tbl.name)
            tbl.filtered_by = types.MethodType(_filtered_by, tbl)
            tbl.by_pk = types.MethodType(_by_pk, tbl)
            tbl.pk_val = types.MethodType(_pk_val, tbl)
            tbl.exists = types.MethodType(_exists, tbl)
            tbl.child_fks = []
            tbl.find_n_rows(estimate=True)
            self.tables[tbl.name] = tbl
        for (tbl_name, tbl) in self.tables.items():
            for fk in tbl.fks:
                fk['constrained_table'] = tbl_name
                self.tables[fk['referred_table']].child_fks.append(fk)

    def __repr__(self):
        return "Db('%s')" % self.sqla_conn

    def assign_target(self, target_db, args):
        for (tbl_name, tbl) in self.tables.items():
            tbl._random_row_gen_fn = types.MethodType(_random_row_gen_fn, tbl)
            tbl.random_rows = tbl._random_row_gen_fn()
            tbl.next_row = types.MethodType(_next_row, tbl)
            target = target_db.tables[tbl_name]
            target.requested = []
            if args.logarithmic:
                target.n_rows_desired = int(math.pow(10, math.log10(tbl.n_rows)
                                            * args.fraction)) or 1
            else:
                target.n_rows_desired = int(tbl.n_rows * args.fraction) or 1
            target.source = tbl
            tbl.target = target
            target.incompleteness_score = types.MethodType(_incompleteness_score, target)
            
    def create_requested(self, source, target_db, rows_desired):
        target = target_db.tables[source.name]
        while target.n_rows < rows_desired:  
            try:
                row = source.requested.pop(0)
            except IndexError:
                return
            self.create_row_in(row, target_db, target)


    def create_row_in(self, source_row, target_db, target):
        logging.debug('create_row_in %s:%s ' % 
                      (target.name, target.pk_val(source_row)))

        if target.exists(**(dict(source_row))):
            logging.debug("Row already exists; not creating")
            return

            # make sure that all required rows1kkk are in parent table(s)
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
                    self.create_row_in(source_parent_row, target_db, target_parent)
        ins = target.insert().values(**source_row)
        target_db.conn.execute(ins)
        target.n_rows += 1

        for child_fk in target.child_fks:
            child = self.tables[child_fk['constrained_table']]
            slct = sa.sql.select([child,])
            for (child_col, this_col) in zip(child_fk['constrained_columns'], 
                                             child_fk['referred_columns']):
                slct = slct.where(child.c[child_col] == source_row[this_col])
            desired_row = self.conn.execute(slct).first()
            if desired_row:
                child.target.requested.append(desired_row) 

    def create_subset_in(self, target_db, args):
        self.args = args
        
        for (tbl_name, pks) in args.force_rows.items():
            for pk in pks:
                source_row = source.by_pk(pk)  
                if source_row:
                    self.create_row_in(source_row, target_db, target)
                else:
                    logging.warn("requested %s:%s not found in source db,"
                                 "could not create" % (source_name, pk))
      
        while True: 
            targets = sorted(target_db.tables.values(), 
                             key=lambda t: t.incompleteness_score())
            try:
                target = targets.pop()
                while not target.source.n_rows:
                    target = targets.pop()
            except IndexError: # pop failure, no more tables
                return
            if target.incompleteness_score() < 0.05:
                return
            source_row = target.source.next_row()
            self.create_row_in(source_row, target_db, target)
        

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


def generate():
    args = argparser.parse_args()
    args.force_rows = {}
    for force_row in (args.force or []):
        (table_name, pk) = force_row.split(':')
        if table_name not in args.force_rows:
            args.force_rows[table_name] = []
        args.force_rows[table_name].append(pk)
    logging.getLogger().setLevel(args.loglevel)
    source = Db(args.source, args)
    target = Db(args.dest, args)
    source.assign_target(target, args)
    source.create_subset_in(target, args)
