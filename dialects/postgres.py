import re

import sqlalchemy as sa
from sqlalchemy import cast
from sqlalchemy.dialects.postgresql import ARRAY, ENUM


class ArrayOfEnum(ARRAY):
    def bind_expression(self, bindvalue):
        return cast(bindvalue, self)

    def result_processor(self, dialect, coltype):
        super_rp = super(ArrayOfEnum, self).result_processor(dialect, coltype)

        def handle_raw_string(value):
            # Interprets PostgreSQL's array syntax in terms of a list
            if value is None:
                return []
            inner = re.match(r"^{(.*)}$", value).group(1)
            return inner.split(",")

        def process(value):
            # Convert array to Python objects
            return super_rp(handle_raw_string(value))

        return process


def fix_postgres_array_of_enum(connection, tbl):
    "Change type of ENUM[] columns to a custom type"

    for col in tbl.c:
        col_str = str(col.type)
        if col_str.endswith('[]'):  # this is an array
            enum_name = col_str[:-2]
            try:  # test if 'enum_name' is an enum
                connection.execute('''
                        SELECT enum_range(NULL::%s);
                    ''' % enum_name).fetchone()
                # create type caster
                tbl.c[col.name].type = ArrayOfEnum(ENUM(name=enum_name))
            except sa.exc.ProgrammingError as enum_excep:
                if 'does not exist' in str(enum_excep):
                    pass  # Must not have been an enum
                else:
                    raise
