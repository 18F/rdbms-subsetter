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

rdbms-subsetter only performs the INSERTS; it's your responsibility to set
up the target database first, with its foreign key constraints.  The easiest
way to do this is with your RDBMS's dump utility.  For example, for PostgreSQL,

::

    pg_dump --schema-only -f schemadump.sql source_database
    createdb partial_database
    psql -f schemadump.sql partial_database

"""
import argparse
from collections import OrderedDict
import random
import types
import sqlalchemy as sa
from sqlalchemy.engine.reflection import Inspector


def _n_rows(self):
    result = self.db.conn.execute(self.count()).fetchone()[0]
    return result

def _random_rows(self, n):
    existing_rows = self.n_rows() 
    row_indexes = random.sample(range(self.n_rows()), n)     
    for offset in row_indexes:
        results = self.db.conn.execute(sa.sql.select([self,]).offset(offset).limit(1))
        yield results.fetchone()                                          
 
    
class Db(object):
    
    def __init__(self, sqla_conn):
        self.engine = sa.create_engine(sqla_conn)
        self.meta = sa.MetaData(bind=self.engine)
        self.meta.reflect()
        self.inspector = Inspector(bind=self.engine)
        self.conn = self.engine.connect()
        self.tables = OrderedDict()
        for tbl in reversed(self.meta.sorted_tables):
            tbl.db = self
            tbl.fks = self.inspector.get_foreign_keys(tbl.name)
            tbl.n_rows = types.MethodType(_n_rows, tbl)
            tbl.random_rows = types.MethodType(_random_rows, tbl)
            self.tables[tbl.name] = tbl
  
    def create_subset_in(self, target_db, fraction):
        for (source_child_name, source_child) in self.tables.items():
            target_child = target_db.tables[source_child_name] 
            n_rows_desired = int(source_child.n_rows() * fraction)
            n_rows_already = target_child.n_rows()
            n_to_create = n_rows_desired - n_rows_already
            if n_to_create > 0:
                for source_child_row in source_child.random_rows(n_to_create):
                    child_slct = sa.sql.select([target_child,])
                    for (key, val) in dict(source_child_row).items():
                        child_slct = child_slct.where(target_child.c[key] == val)
                    existing_child_row = target_db.conn.execute(child_slct).first()
                    if existing_child_row:
                        continue
                    # make sure that all required rows are in parent table(s)
                    for fk in source_child.fks:
                        target_parent = target_db.tables[fk['referred_table']]
                        slct = sa.sql.select([target_parent,])
                        for (parent_col, child_col) in zip(fk['referred_columns'], 
                                                           fk['constrained_columns']):
                            slct = slct.where(target_parent.c[parent_col] == 
                                              source_child_row[child_col])
                        target_parent_row = target_db.conn.execute(slct).first()
                        if not target_parent_row:
                            source_parent_row = self.conn.execute(slct).first()
                            ins = target_parent.insert().values(**dict(source_parent_row))
                            target_db.conn.execute(ins)
                    ins = target_child.insert().values(**source_child_row)
                    target_db.conn.execute(ins)
                   
                    
def fraction(n):
    n = float(n)        
    if 0 < n <= 1:
        return n
    raise argparse.ArgumentError('Fraction must be greater than 0 and no greater than 1')

argparser = argparse.ArgumentParser(description='Generate consistent subset of a database')
argparser.add_argument('source', help='SQLAlchemy connection string for data origin',
                       type=str)
argparser.add_argument('dest', help='SQLAlchemy connection string for data destination',
                       type=str)
argparser.add_argument('fraction', help='Proportion of rows to create in dest (0.0 to 1.0)',
                       type=fraction)


def generate():
    args = argparser.parse_args()
    source = Db(args.source)
    target = Db(args.dest)
    source.create_subset_in(target, args.fraction)
   
