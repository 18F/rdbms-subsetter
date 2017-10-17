import os
import unittest
import tempfile
import sqlite3
from subsetter import Db, merge_config_args

class DummyArgs(object):
    def __init__(self):
        self.logarithmic = False
        self.fraction = 0.25
        self.force_rows = {}
        self.children = 25
        self.config = {}
        self.tables = []
        self.schema = []
        self.exclude_tables = []
        self.full_tables = []
        self.buffer = 1000

dummy_args = DummyArgs()

class OverallTest(unittest.TestCase):

    def setUp(self):
        schema = ["CREATE TABLE state (abbrev, name)",
                  "CREATE TABLE zeppos (name, home_city)",
                  """CREATE TABLE city (name, state_abbrev,
                                        FOREIGN KEY (state_abbrev)
                                        REFERENCES state(abbrev))""",
                  """CREATE TABLE landmark (name, city,
                                            FOREIGN KEY (city)
                                            REFERENCES city(name))""",
                  """CREATE TABLE zeppelins (name, home_city,
                                             FOREIGN KEY (home_city)
                                             REFERENCES city(name))""", # NULL FKs
                  """CREATE TABLE languages_better_than_python (name)""", # empty table
                  ]
        self.source_db_filename = tempfile.mktemp()
        self.source_db = sqlite3.connect(self.source_db_filename)
        self.source_sqla = "sqlite:///%s" % self.source_db_filename
        self.dest_db_filename = tempfile.mktemp()
        self.dest_db = sqlite3.connect(self.dest_db_filename)
        self.dest_sqla = "sqlite:///%s" % self.dest_db_filename
        for statement in schema:
            self.source_db.execute(statement)
            self.dest_db.execute(statement)
        for params in (('MN', 'Minnesota'), ('OH', 'Ohio'),
                       ('MA', 'Massachussetts'), ('MI', 'Michigan')):
            self.source_db.execute("INSERT INTO state VALUES (?, ?)", params)
        for params in (('Duluth', 'MN'), ('Dayton', 'OH'),
                       ('Boston', 'MA'), ('Houghton', 'MI')):
            self.source_db.execute("INSERT INTO city VALUES (?, ?)", params)
        for params in (('Lift Bridge', 'Duluth'), ("Mendelson's", 'Dayton'),
                       ('Trinity Church', 'Boston'), ('Michigan Tech', 'Houghton')):
            self.source_db.execute("INSERT INTO landmark VALUES (?, ?)", params)
        for params in (('Graf Zeppelin', None), ('USS Los Angeles', None),
                       ('Nordstern', None), ('Bodensee', None)):
            self.source_db.execute("INSERT INTO zeppelins VALUES (?, ?)", params)
        for params in (('Zeppo Marx', 'New York City'), ):
            self.source_db.execute("INSERT INTO zeppos VALUES (?, ?)", params)
        self.source_db.commit()
        self.dest_db.commit()

    def tearDown(self):
        self.source_db.close()
        os.unlink(self.source_db_filename)
        self.dest_db.close()
        os.unlink(self.dest_db_filename)

    def test_parents_kept(self):
        src = Db(self.source_sqla, dummy_args)
        dest = Db(self.dest_sqla, dummy_args)
        src.assign_target(dest)
        src.create_subset_in(dest)
        cities = self.dest_db.execute("SELECT * FROM city").fetchall()
        self.assertEqual(len(cities), 1)
        joined = self.dest_db.execute("""SELECT c.name, s.name
                                         FROM city c JOIN state s
                                                     ON (c.state_abbrev = s.abbrev)""")
        joined = joined.fetchall()
        self.assertEqual(len(joined), 1)

    def test_null_foreign_keys(self):
        src = Db(self.source_sqla, dummy_args)
        dest = Db(self.dest_sqla, dummy_args)
        src.assign_target(dest)
        src.create_subset_in(dest)
        zeppelins = self.dest_db.execute("SELECT * FROM zeppelins").fetchall()
        self.assertEqual(len(zeppelins), 1)

    def test_tables(self):
        args_with_tables = DummyArgs()
        args_with_tables.tables = ['state', 'city']
        src = Db(self.source_sqla, args_with_tables)
        dest = Db(self.dest_sqla, args_with_tables)
        src.assign_target(dest)
        src.create_subset_in(dest)
        states = self.dest_db.execute("SELECT * FROM state").fetchall()
        self.assertEqual(len(states), 1)
        cities = self.dest_db.execute("SELECT * FROM city").fetchall()
        self.assertEqual(len(cities), 1)
        excluded_tables = ('landmark', 'languages_better_than_python', 'zeppos', 'zeppelins')
        for table in excluded_tables:
            rows = self.dest_db.execute("SELECT * FROM {}".format(table)).fetchall()
            self.assertEqual(len(rows), 0)

    def test_exclude_tables(self):
        args_with_exclude = DummyArgs()
        args_with_exclude.exclude_tables = ['zeppelins',]
        src = Db(self.source_sqla, args_with_exclude)
        dest = Db(self.dest_sqla, args_with_exclude)
        src.assign_target(dest)
        src.create_subset_in(dest)
        zeppelins = self.dest_db.execute("SELECT * FROM zeppelins").fetchall()
        self.assertEqual(len(zeppelins), 0)

    def test_tables_and_exclude_tables(self):
        args_with_tables_and_exclude_tables = DummyArgs()
        args_with_tables_and_exclude_tables.tables = ['state', 'city']
        args_with_tables_and_exclude_tables.exclude_tables = ['city']
        src = Db(self.source_sqla, args_with_tables_and_exclude_tables)
        dest = Db(self.dest_sqla, args_with_tables_and_exclude_tables)
        src.assign_target(dest)
        src.create_subset_in(dest)
        states = self.dest_db.execute("SELECT * FROM state").fetchall()
        self.assertEqual(len(states), 1)
        excluded_tables = ('city', 'landmark', 'languages_better_than_python', 'zeppos', 'zeppelins')
        for table in excluded_tables:
            rows = self.dest_db.execute("SELECT * FROM {}".format(table)).fetchall()
            self.assertEqual(len(rows), 0)

    def test_full_tables(self):
        args_with_full = DummyArgs()
        args_with_full.full_tables = ['city',]
        src = Db(self.source_sqla, args_with_full)
        dest = Db(self.dest_sqla, args_with_full)
        src.assign_target(dest)
        src.create_subset_in(dest)
        cities = self.dest_db.execute("SELECT * FROM city").fetchall()
        self.assertEqual(len(cities), 4)

    def test_exclude_tables_wildcard(self):
        args_with_exclude = DummyArgs()
        args_with_exclude.exclude_tables = ['zep*',]
        src = Db(self.source_sqla, args_with_exclude)
        dest = Db(self.dest_sqla, args_with_exclude)
        src.assign_target(dest)
        src.create_subset_in(dest)
        zeppelins = self.dest_db.execute("SELECT * FROM zeppelins").fetchall()
        self.assertEqual(len(zeppelins), 0)
        zeppos = self.dest_db.execute("SELECT * FROM zeppos").fetchall()
        self.assertEqual(len(zeppos), 0)

class MergeConfigArgsTest(unittest.TestCase):
    def test_merges_tables_from_config_file(self):
        args = DummyArgs()
        args.tables = ["zeppelins"]
        args.config["tables"] = [ "zeppos" ]

        merge_config_args(args)

        self.assertEqual(args.tables, ["zeppelins", "zeppos"])

    def test_merges_schemas_from_config_file(self):
        args = DummyArgs()
        args.schema = ["schema1"]
        args.config["schemas"] = [ "schema2" ]

        merge_config_args(args)
        self.assertEqual(args.schema, ["schema1", "schema2"])

    def test_merges_full_tables_from_config_file(self):
        args = DummyArgs()
        args.full_tables = ["table1"]
        args.config["full_tables"] = [ "table2" ]

        merge_config_args(args)
        self.assertEqual(args.full_tables, ["table1", "table2"])
