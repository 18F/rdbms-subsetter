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

``rdbms-subsetter`` promises that each child row will have whatever parent rows are
required by its foreign keys.  It will also *try* to include most child rows belonging
to each parent row (up to the supplied ``--children`` parameter, default 3 each), but it
can't make any promises.  (Demanding all children can lead to infinite propagation in
thoroughly interlinked databases, as every child record demands new parent records,
which demand new child records, which demand new parent records...
so increase ``--children`` with caution.)

When row numbers in your tables vary wildly (tens to billions, for example),
consider using the ``-l`` flag, which reduces row counts by a logarithmic formula.  If ``f`` is
the fraction specified, and ``-l`` is set, and the original table has ``n`` rows,
then each new table's row target will be::

    math.pow(10, math.log10(n)*f)

A fraction of ``0.5`` seems to produce good results, converting 10 rows to 3,
1,000,000 to 1,000, and 1,000,000,000 to 31,622.

Rows are selected randomly, but for tables with a single primary key column, you
can force rdbms-subsetter to include specific rows (and their dependencies) with
``force=<tablename>:<primary key value>``.  The immediate children of these rows
are also exempted from the ``--children`` limit.

rdbms-subsetter only performs the inserts; it's your responsibility to set
up the target database first, with its foreign key constraints.  The easiest
way to do this is with your RDBMS's dump utility.  For example, for PostgreSQL,

::

    pg_dump --schema-only -f schemadump.sql bigdb
    createdb littledb
    psql -f schemadump.sql littledb

You can pull rows from a non-default schema by passing ``--schema=<name>``.
Currently the target database must contain the corresponding tables in its own
schema of the same name (moving between schemas of different names is not yet
supported).

Case-specific table names will probably create bad results in rdbms-subsetter,
and in the rest of your life, for that matter.  Don't do it.
"""
import argparse
import fnmatch
import functools
import json
import logging
import math
import random
import types
from collections import OrderedDict, deque

import sqlalchemy as sa
from blinker import signal
from sqlalchemy.engine.reflection import Inspector

# Python2 has a totally different definition for ``input``; overriding it here
try:
    input = raw_input
except NameError:
    pass

try:
    from psycopg2 import IntegrityError
except ImportError:
    class IntegrityError(Exception):
        "Dummy exception type, placeholder for missing PostgreSQL-specific error"
    
__version__ = '0.2.5'

SIGNAL_ROW_ADDED = 'row_added'


class TableHelper(object):
    "Provides self-evaulation functions for SQLAlchemy tables"

    def __init__(self, table):
        self.t = table

    def find_n_rows(self, estimate=False):
        n_rows = 0
        if estimate:
            try:
                if self.t.db.engine.driver in ('psycopg2', 'pg8000', ):
                    schema = (self.t.schema + '.') if self.t.schema else ''
                    qry = """SELECT reltuples FROM pg_class
                             WHERE oid = lower('%s%s')::regclass""" % (
                        schema, self.t.name.lower())
                elif 'oracle' in self.t.db.engine.driver:
                    qry = """SELECT num_rows FROM all_tables
                             WHERE LOWER(table_name)='%s'""" % self.name.lower(
                    )
                else:
                    raise NotImplementedError(
                        "No approximation known for driver %s" %
                        self.t.db.engine.driver)
                n_rows = self.t.db.conn.execute(qry).fetchone()[0]
            except Exception as e:
                logging.debug("failed to get approximate rowcount for %s\n%s" %
                              (self.t.name, str(e)))
        if not n_rows:
            n_rows = self.t.db.conn.execute(self.t.count()).fetchone()[0]
        return n_rows

    def random_row_func(self):
        dialect = self.t.bind.engine.dialect.name
        if 'mysql' in dialect:
            return sa.sql.func.rand()
        elif 'oracle' in dialect:
            return sa.sql.func.dbms_random.value()
        else:
            return sa.sql.func.random()

    def random_rows(self):
        """
        Random sample from ``.t`` of *approximate* size ``t.n_rows_desired``
        """
        if self.t.n_rows:
            while True:
                n = self.t.target.n_rows_desired
                if self.t.n_rows > 1000:
                    fraction = n / float(self.t.n_rows)
                    qry = sa.sql.select([self.t, ]).where(
                        self.random_row_func() < fraction)
                    results = self.t.db.conn.execute(qry).fetchall()
                    # we may stop wanting rows at any point, so shuffle them so as not to
                    # skew the sample toward those near the beginning
                    random.shuffle(results)
                    for row in results:
                        yield row
                else:
                    qry = sa.sql.select([self.t, ]).order_by(
                        self.random_row_func()).limit(n)
                    for row in self.t.db.conn.execute(qry):
                        yield row

    def next_row(self):
        """
        Returns (`row`, `prioritized`)

        where `row` from this table, beginning with those required,
        then requested, then randomly requested.
        """
        if self.t.target.required:
            return self.t.target.required.popleft()
        elif self.t.target.requested:
            return self.t.target.requested.popleft()
        else:
            try:
                return (next(self.random_rows()), False)  # not prioritized
            except StopIteration:
                return (None, None)

    def filtered_by(self, **kw):
        slct = sa.sql.select([self.t, ])
        slct = slct.where(sa.sql.and_((self.t.c[k] == v) for (k, v) in
                                      kw.items()))
        return slct

    def by_pk(self, pk):
        pk_name = self.t.db.inspector.get_primary_keys(self.t.name,
                                                       self.t.schema)[0]
        slct = self.filtered_by(**{pk_name: pk})
        return self.t.db.conn.execute(slct).fetchone()

    def pk_val(self, row):
        if self.t.pk:
            return row[self.t.pk[0]]
        else:
            return None

    def completeness_score(self):
        """Scores how close a target table is to being filled enough to quit"""
        table = (self.t.schema if self.t.schema else "") + self.t.name
        fetch_all = self.t.fetch_all
        requested = len(self.t.requested)
        required = len(self.t.required)
        n_rows = float(self.t.n_rows)
        n_rows_desired = float(self.t.n_rows_desired)
        logging.debug("%s.fetch_all      = %s", table, fetch_all)
        logging.debug("%s.requested      = %d", table, requested)
        logging.debug("%s.required       = %d", table, required)
        logging.debug("%s.n_rows         = %d", table, n_rows)
        logging.debug("%s.n_rows_desired = %d", table, n_rows_desired)
        if fetch_all:
            if n_rows < n_rows_desired:
                return 1 + (n_rows or 1) - (n_rows_desired or 1)
        result = 0 - (requested / (n_rows or 1)) - required
        if not self.t.required:  # anything in `required` queue disqualifies
            result += (n_rows / (n_rows_desired or 1))**0.33
        return result

    def assign_as_target_of(self, source_tbl):

        for ((tbl_schema, tbl_name), tbl) in self.tables.items():
            target = target_db.tables[(tbl_schema, tbl_name)]

            self.t.requested = deque()
            self.t.required = deque()
            self.t.pending = dict()
            self.t.done = set()
            self.t.fetch_all = False
            self.t.source = source_tbl
            source_tbl.target = self.t
            self.t.n_rows_desired = self.determine_rows_desired(source_tbl,
                                                                self.t.args)
            logging.debug("assigned methods to %s" % self.t.name)

            if _table_matches_any_pattern(tbl.schema, tbl.name,
                                          self.args.full_tables):
                target.n_rows_desired = tbl.n_rows
                target.fetch_all = True
            else:
                if tbl.n_rows:
                    if self.args.logarithmic:
                        target.n_rows_desired = int(math.pow(10, math.log10(
                            tbl.n_rows) * self.args.fraction)) or 1
                    else:
                        target.n_rows_desired = int(tbl.n_rows *
                                                    self.args.fraction) or 1
                else:
                    target.n_rows_desired = 0
            logging.debug("assigned methods to %s" % target.name)


def _table_matches_any_pattern(schema, table, patterns):
    """Test if the table `<schema>.<table>` matches any of the provided patterns.

    Will attempt to match both `schema.table` and just `table` against each pattern.

    Params:
        - schema.      Name of the schema the table belongs to.
        - table.       Name of the table.
        - patterns.    The patterns to try.
    """
    qual_name = '{}.{}'.format(schema, table)
    return any(fnmatch.fnmatch(qual_name, each) or fnmatch.fnmatch(table, each)
               for each in patterns)


def _import_modules(import_list):
    for module_name in import_list:
        __import__(module_name)


class Db(object):
    def __init__(self, sqla_conn, args, schemas=[None]):
        self.args = args
        self.sqla_conn = sqla_conn
        self.schemas = schemas
        self.engine = sa.create_engine(sqla_conn)
        self.inspector = Inspector(bind=self.engine)
        self.conn = self.engine.connect()
        self.tables = OrderedDict()

        for schema in self.schemas:
            meta = sa.MetaData(
                bind=self.engine)  # excised schema=schema to prevent errors
            meta.reflect(schema=schema)
            for tbl in meta.sorted_tables:
                if args.tables and not _table_matches_any_pattern(
                        tbl.schema, tbl.name, self.args.tables):
                    continue
                if _table_matches_any_pattern(tbl.schema, tbl.name,
                                              self.args.exclude_tables):
                    continue
                tbl.db = self
                tbl.h = TableHelper(tbl)
                tbl.fks = self.inspector.get_foreign_keys(tbl.name,
                                                          schema=tbl.schema)
                tbl.pk = self.inspector.get_primary_keys(tbl.name,
                                                         schema=tbl.schema)
                if not tbl.pk:
                    tbl.pk = [d['name']
                              for d in self.inspector.get_columns(tbl.name,
                                                                  schema=tbl.schema)]
                tbl.child_fks = []
                estimate_rows = not _table_matches_any_pattern(
                    tbl.schema, tbl.name, self.args.full_tables)
                tbl.n_rows = tbl.h.find_n_rows(estimate=estimate_rows)
                self.tables[(tbl.schema, tbl.name)] = tbl
        all_constraints = args.config.get('constraints', {})
        for ((tbl_schema, tbl_name), tbl) in self.tables.items():
            qualified = "{}.{}".format(tbl_schema, tbl_name)
            if qualified in all_constraints:
                constraints = all_constraints[qualified]
            else:
                constraints = all_constraints.get(tbl_name, [])
            tbl.constraints = constraints
            for fk in (tbl.fks + constraints):
                fk['constrained_schema'] = tbl_schema
                fk['constrained_table'] = tbl_name  # TODO: check against constrained_table
                self.tables[(fk['referred_schema'], fk['referred_table']
                             )].child_fks.append(fk)

    def __repr__(self):
        return "Db('%s')" % self.sqla_conn

    def assign_target(self, target_db):

        for ((tbl_schema, tbl_name), tbl) in self.tables.items():
            tbl.random_rows = tbl.h.random_rows()
            target = target_db.tables[(tbl_schema, tbl_name)]
            target.requested = deque()
            target.required = deque()
            target.pending = dict()
            target.done = set()
            target.fetch_all = False
            if _table_matches_any_pattern(tbl.schema, tbl.name,
                                          self.args.full_tables):
                target.n_rows_desired = tbl.n_rows
                target.fetch_all = True
            else:
                if tbl.n_rows:
                    if self.args.logarithmic:
                        target.n_rows_desired = int(math.pow(10, math.log10(
                            tbl.n_rows) * self.args.fraction)) or 1
                    else:
                        target.n_rows_desired = int(tbl.n_rows *
                                                    self.args.fraction) or 1
                else:
                    target.n_rows_desired = 0
            target.source = tbl
            tbl.target = target
            logging.debug("assigned methods to %s" % target.name)

    def confirm(self):
        message = []
        for (tbl_schema, tbl_name) in sorted(self.tables, key=lambda t: t[1]):
            tbl = self.tables[(tbl_schema, tbl_name)]
            message.append("Create %d rows from %d in %s.%s" %
                           (tbl.target.n_rows_desired, tbl.n_rows,
                            tbl_schema or '', tbl_name))
        print("\n".join(sorted(message)))
        if self.args.yes:
            return True
        response = input("Proceed? (Y/n) ").strip().lower()
        return (not response) or (response[0] == 'y')

    def create_row_in(self, source_row, target_db, target, prioritized=False):
        logging.debug('create_row_in %s:%s ' %
                      (target.name, target.h.pk_val(source_row)))

        pks = tuple((source_row[key] for key in target.pk))
        row_exists = pks in target.pending or pks in target.done
        logging.debug("Row exists? %s" % str(row_exists))
        if row_exists and not prioritized:
            return

        if not row_exists:
            # make sure that all required rows are in parent table(s)
            for fk in target.fks:
                target_parent = target_db.tables[(fk['referred_schema'], fk[
                    'referred_table'])]
                slct = sa.sql.select([target_parent, ])
                any_non_null_key_columns = False
                for (parent_col, child_col) in zip(fk['referred_columns'],
                                                   fk['constrained_columns']):
                    slct = slct.where(
                        target_parent.c[parent_col] == source_row[child_col])
                    if source_row[child_col] is not None:
                        any_non_null_key_columns = True
                        break
                if any_non_null_key_columns:
                    target_parent_row = target_db.conn.execute(slct).first()
                    if not target_parent_row:
                        source_parent_row = self.conn.execute(slct).first()
                        self.create_row_in(source_parent_row, target_db,
                                           target_parent)

            # make sure that all referenced rows are in referenced table(s)
            for constraint in target.constraints:
                target_referred = target_db.tables[(constraint[
                    'referred_schema'], constraint['referred_table'])]
                slct = sa.sql.select([target_referred, ])
                any_non_null_key_columns = False
                for (referred_col, constrained_col) in zip(
                        constraint['referred_columns'],
                        constraint['constrained_columns']):
                    slct = slct.where(target_referred.c[referred_col] ==
                                      source_row[constrained_col])
                    if source_row[constrained_col] is not None:
                        any_non_null_key_columns = True
                        break
                if any_non_null_key_columns:
                    target_referred_row = target_db.conn.execute(slct).first()
                    if not target_referred_row:
                        source_referred_row = self.conn.execute(slct).first()
                        # because constraints aren't enforced like real FKs, the referred row isn't guaranteed to exist
                        if source_referred_row:
                            self.create_row_in(source_referred_row, target_db,
                                               target_referred)

            pks = tuple((source_row[key] for key in target.pk))
            target.n_rows += 1

            if self.args.buffer == 0:
                target_db.insert_one(target, pks, source_row)
            else:
                target.pending[pks] = source_row
            signal(SIGNAL_ROW_ADDED).send(self,
                                          source_row=source_row,
                                          target_db=target_db,
                                          target_table=target,
                                          prioritized=prioritized)

        for child_fk in target.child_fks:
            child = self.tables[(child_fk['constrained_schema'], child_fk[
                'constrained_table'])]
            slct = sa.sql.select([child])
            for (child_col, this_col) in zip(child_fk['constrained_columns'],
                                             child_fk['referred_columns']):
                slct = slct.where(child.c[child_col] == source_row[this_col])
            if not prioritized:
                slct = slct.limit(self.args.children)
            for (n, desired_row) in enumerate(self.conn.execute(slct)):
                if prioritized:
                    child.target.required.append((desired_row, prioritized))
                elif (n == 0):
                    child.target.requested.appendleft((desired_row, prioritized
                                                       ))
                else:
                    child.target.requested.append((desired_row, prioritized))

    @property
    def pending(self):
        return functools.reduce(
            lambda count, table: count + len(table.pending),
            self.tables.values(), 0)

    def insert_one(self, table, pk, values):
        self.conn.execute(table.insert(), values)
        table.done.add(pk)

    def flush(self):
        for table in self.tables.values():
            if not table.pending:
                continue
            try:
                self.conn.execute(table.insert(), list(table.pending.values()))
            except Exception as e:
                # We are supposed to avoid trying to create existing rows,
                # but an undiagnosed bug lets one through occasionally.
                # As a stopgap, ignoring the existing row is correct.
                # TODO: this must presumably be patched for non-pg RDBMS, too
                # rollback?
                for vals in table.pending.values():
                    try:
                        self.conn.execute(table.insert(), vals)
                    except Exception as e2:
                        logging.warning('Problem inserting row: {}'.format(str(e2)))
                        logging.warning('For row {}'.format(str(vals))) 
                        logging.warning('Skipping insert')
            table.done = table.done.union(table.pending.keys())
            table.pending = dict()

    def create_subset_in(self, target_db):

        for (tbl_name, pks) in self.args.force_rows.items():
            if '.' in tbl_name:
                (tbl_schema, tbl_name) = tbl_name.split('.', 1)
            else:
                tbl_schema = None
            source = self.tables[(tbl_schema, tbl_name)]
            for pk in pks:
                source_row = source.h.by_pk(pk)
                if source_row:
                    self.create_row_in(source_row,
                                       target_db,
                                       source.target,
                                       prioritized=True)
                else:
                    logging.warn("requested %s:%s not found in source db,"
                                 "could not create" % (source.name, pk))

        while True:
            targets = sorted(target_db.tables.values(),
                             key=lambda t: t.h.completeness_score())
            try:
                target = targets.pop(0)
                while not target.source.n_rows:
                    target = targets.pop(0)
            except IndexError:  # pop failure, no more tables
                break
            logging.debug("total n_rows in target: %d" %
                          sum((t.n_rows for t in target_db.tables.values())))
            logging.debug("target tables with 0 n_rows: %s" %
                          ", ".join(t.name for t in target_db.tables.values()
                                    if not t.n_rows))
            logging.info("lowest completeness score (in %s) at %f" %
                         (target.name, target.h.completeness_score()))
            if target.h.completeness_score() > 0.97:
                break
            (source_row, prioritized) = target.source.h.next_row()
            self.create_row_in(source_row,
                               target_db,
                               target,
                               prioritized=prioritized)

            if target_db.pending > self.args.buffer > 0:
                target_db.flush()

        if self.args.buffer > 0:
            target_db.flush()


def update_sequences(source, target, tables, exclude_tables):
    """Set database sequence values to match the source db

       Needed to avoid subsequent unique key violations after DB build.
       Currently only implemented for postgresql -> postgresql."""

    if source.engine.name != 'postgresql' or target.engine.name != 'postgresql':
        return
    qry = """SELECT 'SELECT last_value FROM ' || n.nspname ||
                     '.' || s.relname || ';' AS qry,
                    n.nspname || '.' || s.relname AS qual_name,
                    n.nspname AS schema, t.relname AS table
             FROM pg_class s
             JOIN pg_depend d ON (d.objid=s.oid AND d.classid='pg_class'::regclass AND d.refclassid='pg_class'::regclass)
             JOIN pg_class t ON (t.oid=d.refobjid)
             JOIN pg_namespace n ON (n.oid=t.relnamespace)
             WHERE s.relkind='S' AND d.deptype='a'"""
    for (qry, qual_name, schema, table) in list(source.conn.execute(qry)):
        if tables and not _table_matches_any_pattern(schema, table, tables):
            continue
        if _table_matches_any_pattern(schema, table, exclude_tables):
            continue
        (lastval, ) = source.conn.execute(qry).first()
        nextval = int(lastval) + 1
        updater = "ALTER SEQUENCE %s RESTART WITH %s;" % (qual_name, nextval)
        target.conn.execute(updater)
    target.conn.execute('commit')


def fraction(n):
    n = float(n)
    if 0 <= n <= 1:
        return n
    raise argparse.ArgumentError(
        'Fraction must be greater than 0 and no greater than 1')


all_loglevels = "CRITICAL, FATAL, ERROR, DEBUG, INFO, WARN, WARNING"


def loglevel(raw):
    try:
        return int(raw)
    except ValueError:
        upper = raw.upper()
        if upper in all_loglevels:
            return getattr(logging, upper)
        raise NotImplementedError('log level "%s" not one of %s' %
                                  (raw, all_loglevels))


argparser = argparse.ArgumentParser(
    description='Generate consistent subset of a database')
argparser.add_argument('source',
                       help='SQLAlchemy connection string for data origin',
                       type=str)
argparser.add_argument(
    'dest',
    help='SQLAlchemy connection string for data destination',
    type=str)
argparser.add_argument(
    'fraction',
    help='Proportion of rows to create in dest (0.0 to 1.0)',
    type=fraction)
argparser.add_argument(
    '-l',
    '--logarithmic',
    help='Cut row numbers logarithmically; try 0.5 for fraction',
    action='store_true')
argparser.add_argument(
    '-b',
    '--buffer',
    help='Number of records to store in buffer before flush; use 0 for no buffer',
    type=int,
    default=1000)
argparser.add_argument('--loglevel',
                       type=loglevel,
                       help='log level (%s)' % all_loglevels,
                       default='INFO')
argparser.add_argument(
    '-f',
    '--force',
    help='<table name>:<primary_key_val> to force into dest',
    type=str.lower,
    action='append')
argparser.add_argument(
    '-c',
    '--children',
    help='Max number of child rows to attempt to pull for each parent row',
    type=int,
    default=3)
argparser.add_argument('--schema',
                       help='Non-default schema to include',
                       type=str,
                       action='append',
                       default=[])
argparser.add_argument('--config',
                       help='Path to configuration .json file',
                       type=argparse.FileType('r'))
argparser.add_argument('--table',
                       '-t',
                       dest='tables',
                       help='Include the named table(s) only',
                       type=str,
                       action='append',
                       default=[])
argparser.add_argument(
    '--exclude-table',
    '-T',
    dest='exclude_tables',
    help='Tables to exclude. When both -t and -T are given, the behavior is to include just the tables that match at least one -t switch but no -T switches.',
    type=str,
    action='append',
    default=[])
argparser.add_argument('--full-table',
                       '-F',
                       dest='full_tables',
                       help='Tables to include every row of',
                       type=str,
                       action='append',
                       default=[])
argparser.add_argument(
    '--import',
    '-i',
    dest='import_list',
    help='Dotted module name to import; e.g. custom.signalhandler',
    type=str,
    action='append',
    default=[])
argparser.add_argument('-y',
                       '--yes',
                       help='Proceed without stopping for confirmation',
                       action='store_true')

log_format = "%(asctime)s %(levelname)-5s %(message)s"


def _parse_force(args):
    "Collects ``args.force`` into {table name: [pk values to force]}"
    force_rows = {}
    for force_row in (args.force or []):
        (table_name, pk) = force_row.split(':')
        if table_name not in force_rows:
            force_rows[table_name] = []
        force_rows[table_name].append(pk)
    return force_rows


def generate():
    args = argparser.parse_args()
    _import_modules(args.import_list)
    args.force_rows = _parse_force(args)
    logging.getLogger().setLevel(args.loglevel)
    logging.basicConfig(format=log_format)
    schemas = args.schema + [None, ]
    args.config = json.load(args.config) if args.config else {}
    source = Db(args.source, args, schemas)
    target = Db(args.dest, args, schemas)
    if set(source.tables.keys()) != set(target.tables.keys()):
        raise Exception('Source and target databases have different tables')
    source.assign_target(target)
    if source.confirm():
        source.create_subset_in(target)
    update_sequences(source, target, args.tables, args.exclude_tables)


if __name__ == '__main__':
    generate()
