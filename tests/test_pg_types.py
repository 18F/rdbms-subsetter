#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Tests for postgres-specific data types"""

import subprocess

import pytest
import pytest_postgresql
from pytest_postgresql import factories

from subsetter import Db

try:
    subprocess.check_output('command -v pg_ctl', shell=True)
    PG_CTL_MISSING = False  # sorry for the double-negative, but it's convenient later
except subprocess.CalledProcessError:
    PG_CTL_MISSING = True

postgresql_dest_proc = factories.postgresql_proc()
postgresql_dest = factories.postgresql('postgresql_dest_proc')


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


def dsn_to_url(engine, dsn):
    """
    Converts a DSN to a SQLAlchemy-style database URL

    pytest_postgresql connection only available in DSN form, like
    'dbname=tests user=postgres host=127.0.0.1 port=41663'
    """
    params = dict(s.split('=') for s in dsn.split())
    return '{engine}://{user}@{host}:{port}/{dbname}'.format(engine=engine,
                                                             **params)


def sqla_url(filename):
    return "sqlite:///%s" % filename


TABLE_DEFINITIONS = [
    "CREATE TYPE mood AS ENUM ('grumpy', 'hungry', 'happy')",
    "CREATE TABLE cat (name text, current_mood mood)",
    """CREATE TABLE moody_cat (name text,
        current_mood mood,
        possible_moods mood[])""",
    "CREATE TABLE nameful_cat (name text, more_names text[])",
]


def insert_data(curs):
    for params in (('Buzz', 'hungry'),
                   ('Fuzz', 'happy'),
                   ('Suzz', 'happy'),
                   ('Agamemnon', 'grumpy'), ):
        curs.execute("INSERT INTO cat VALUES (%s, %s)", params)
    for params in (('Buzz', 'hungry', 'hungry', 'grumpy'),
                   ('Fuzz', 'happy', 'happy', 'hungry'),
                   ('Suzz', 'happy', 'happy', 'grumpy'),
                   ('Agamemnon', 'grumpy', 'hungry', 'grumpy'), ):
        # param substitution syntax not working  -
        # fallback to string templating
        template = """INSERT INTO moody_cat VALUES ('%s', '%s',
            ARRAY['%s'::mood, '%s'::mood])"""
        query = template % params
        curs.execute(query)
    for params in (('Buzz', 'Space-kitty', 'Pitons'),
                   ('Fuzz', 'The Paw', 'Nosy'),
                   ('Suzz', 'Laptop', 'Cowcat'),
                   ('Agamemnon', 'Your Majesty', ''), ):
        template = """INSERT INTO nameful_cat VALUES ('%s',
            ARRAY['%s', '%s'])"""
        query = template % params
        curs.execute(query)


@pytest.mark.skipif(PG_CTL_MISSING, reason='PostgreSQL not installed locally')
@pytest.fixture
def pg_data(postgresql, postgresql_dest):
    curs = postgresql.cursor()
    dest_curs = postgresql_dest.cursor()
    for table_definition in TABLE_DEFINITIONS:
        curs.execute(table_definition)
        dest_curs.execute(table_definition)
    insert_data(curs)
    postgresql.commit()
    postgresql_dest.commit()
    url = dsn_to_url('postgresql', postgresql.dsn)
    dest_url = dsn_to_url('postgresql', postgresql_dest.dsn)
    return (url, dest_url)


def results(src_url, dest_url, arguments):
    src = Db(src_url, arguments)
    dest = Db(dest_url, arguments)
    src.assign_target(dest)
    src.create_subset_in(dest)
    return (src, dest)


@pytest.mark.skipif(PG_CTL_MISSING, reason='PostgreSQL not installed locally')
def test_enum_type(pg_data):
    args_with_scalar_enum = DummyArgs()
    args_with_scalar_enum.tables = ['cat', ]
    (src, dest) = results(*pg_data, args_with_scalar_enum)
    dest_curs = dest.conn.connection.cursor()
    dest_curs.execute("SELECT * FROM cat")
    cats = dest_curs.fetchall()
    assert len(cats) == 1

@pytest.mark.skipif(PG_CTL_MISSING, reason='PostgreSQL not installed locally')
def test_array_of_text(pg_data):
    args_with_array_text = DummyArgs()
    args_with_array_text.tables = ['nameful_cat', ]
    (src, dest) = results(*pg_data, args_with_array_text)
    dest_curs = dest.conn.connection.cursor()
    dest_curs.execute("SELECT * FROM nameful_cat")
    cats = dest_curs.fetchall()
    assert len(cats) == 1

@pytest.mark.skipif(PG_CTL_MISSING, reason='PostgreSQL not installed locally')
def test_array_of_enums(pg_data):
    args_with_array_enum = DummyArgs()
    args_with_array_enum.tables = ['moody_cat', ]
    (src, dest) = results(*pg_data, args_with_array_enum)
    dest_curs = dest.conn.connection.cursor()
    dest_curs.execute("SELECT * FROM moody_cat")
    cats = dest_curs.fetchall()
    assert len(cats) == 1
