#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Tests for `sql_insert_writer` package."""

import os
import sqlite3
import tempfile

import pytest

from subsetter import Db

TABLE_DEFINITIONS = [
    "CREATE TABLE state (abbrev, name)",
    "CREATE TABLE zeppos (name, home_city)",
    """CREATE TABLE city (name, state_abbrev,
       FOREIGN KEY (state_abbrev)
       REFERENCES state(abbrev))""",
    """CREATE TABLE landmark (name, city,
       FOREIGN KEY (city)
       REFERENCES city(name))""",
    """CREATE TABLE zeppelins (name, home_city,
       FOREIGN KEY (home_city)
       REFERENCES city(name))""",  # NULL FKs
    """CREATE TABLE languages_better_than_python (name)""",  # empty table
]


class DummyArgs(object):
    logarithmic = False
    fraction = 0.25
    force_rows = {}
    children = 25
    config = {}
    tables = []
    schema = []
    exclude_tables = []
    full_tables = []
    buffer = 1000


dummy_args = DummyArgs()


def temp_sqlite_db():
    """Creates a temporary sqlite database file

    Return (filename, connection)"""

    filename = tempfile.mktemp()
    db = sqlite3.connect(filename)
    return (filename, db)


def sqla_url(filename):
    return "sqlite:///%s" % filename


def insert_data(curs):
    for params in (('MN', 'Minnesota'), ('OH', 'Ohio'),
                   ('MA', 'Massachussetts'), ('MI', 'Michigan')):
        curs.execute("INSERT INTO state VALUES (?, ?)", params)
    for params in (('Duluth', 'MN'), ('Dayton', 'OH'), ('Boston', 'MA'),
                   ('Houghton', 'MI')):
        curs.execute("INSERT INTO city VALUES (?, ?)", params)
    for params in (('Lift Bridge', 'Duluth'), ("Mendelson's", 'Dayton'),
                   ('Trinity Church', 'Boston'),
                   ('Michigan Tech', 'Houghton')):
        curs.execute("INSERT INTO landmark VALUES (?, ?)", params)
    for params in (('Graf Zeppelin', None), ('USS Los Angeles', None),
                   ('Nordstern', None), ('Bodensee', None)):
        curs.execute("INSERT INTO zeppelins VALUES (?, ?)", params)
    for params in (('Zeppo Marx', 'New York City'), ):
        curs.execute("INSERT INTO zeppos VALUES (?, ?)", params)


@pytest.fixture
def sqlite_data(request):
    (source_filename, source_db) = temp_sqlite_db()
    (dest_filename, dest_db) = temp_sqlite_db()

    source_curs = source_db.cursor()
    dest_curs = dest_db.cursor()
    for table_def in TABLE_DEFINITIONS:
        source_curs.execute(table_def)
        dest_curs.execute(table_def)
    insert_data(source_curs)
    source_db.commit()
    dest_db.commit()

    yield (sqla_url(source_filename), sqla_url(dest_filename))

    source_db.close()
    os.unlink(source_filename)
    dest_db.close()
    os.unlink(dest_filename)


def results(src_url, dest_url, arguments):
    src = Db(src_url, arguments)
    dest = Db(dest_url, arguments)
    src.assign_target(dest)
    src.create_subset_in(dest)
    return (src, dest)


def test_parents_kept(sqlite_data):
    (src, dest) = results(*sqlite_data, dummy_args)
    dest_curs = dest.conn.connection.cursor()
    cities = dest_curs.execute("SELECT * FROM city").fetchall()
    assert len(cities) == 1
    joined = dest_curs.execute("""SELECT c.name, s.name
                                    FROM city c JOIN state s
                                    ON (c.state_abbrev = s.abbrev)""").fetchall(
    )
    assert len(joined) == 1


def test_null_foreign_keys(sqlite_data):
    (src, dest) = results(*sqlite_data, dummy_args)
    dest_curs = dest.conn.connection.cursor()
    zeppelins = dest_curs.execute("SELECT * FROM zeppelins").fetchall()
    assert len(zeppelins) == 1


def test_tables_arg(sqlite_data):
    args_with_tables = DummyArgs()
    args_with_tables.tables = ['state', 'city']
    (src, dest) = results(*sqlite_data, args_with_tables)
    dest_curs = dest.conn.connection.cursor()
    states = dest_curs.execute("SELECT * FROM state").fetchall()
    assert len(states) == 1
    cities = dest_curs.execute("SELECT * FROM city").fetchall()
    assert len(cities) == 1
    excluded_tables = ('landmark', 'languages_better_than_python', 'zeppos',
                       'zeppelins')
    for table in excluded_tables:
        rows = dest_curs.execute("SELECT * FROM {}".format(table)).fetchall()
        assert not rows


def test_exclude_tables(sqlite_data):
    args_with_exclude = DummyArgs()
    args_with_exclude.exclude_tables = ['zeppelins', ]
    (src, dest) = results(*sqlite_data, args_with_exclude)
    dest_curs = dest.conn.connection.cursor()
    zeppelins = dest_curs.execute("SELECT * FROM zeppelins").fetchall()
    assert not zeppelins


def test_tables_and_exclude_tables(sqlite_data):
    args_with_tables_and_exclude_tables = DummyArgs()
    args_with_tables_and_exclude_tables.tables = ['state', 'city']
    args_with_tables_and_exclude_tables.exclude_tables = ['city']
    (src, dest) = results(*sqlite_data, args_with_tables_and_exclude_tables)
    dest_curs = dest.conn.connection.cursor()
    states = dest_curs.execute("SELECT * FROM state").fetchall()
    assert len(states) == 1
    excluded_tables = ('city', 'landmark', 'languages_better_than_python',
                       'zeppos', 'zeppelins')
    for table in excluded_tables:
        rows = dest_curs.execute("SELECT * FROM {}".format(table)).fetchall()
        assert not rows


def test_full_tables(sqlite_data):
    args_with_full = DummyArgs()
    args_with_full.full_tables = ['city', ]
    (src, dest) = results(*sqlite_data, args_with_full)
    dest_curs = dest.conn.connection.cursor()
    cities = dest_curs.execute("SELECT * FROM city").fetchall()
    assert len(cities) == 4


def test_exclude_tables_wildcard(sqlite_data):
    args_with_exclude = DummyArgs()
    args_with_exclude.exclude_tables = ['zep*', ]
    (src, dest) = results(*sqlite_data, args_with_exclude)
    dest_curs = dest.conn.connection.cursor()
    zeppelins = dest_curs.execute("SELECT * FROM zeppelins").fetchall()
    assert not zeppelins
    zeppos = dest_curs.execute("SELECT * FROM zeppos").fetchall()
    assert not zeppos
