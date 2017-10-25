import re

import sqlalchemy as sa
from sqlalchemy import cast
from sqlalchemy.dialects.postgresql import ARRAY, ENUM


def sql_enum_to_list(value):
    """
    Interprets PostgreSQL's array syntax in terms of a list

    Enums come back from SQL as '{val1,val2,val3}'
    """

    if value is None:
        return []
    inner = re.match(r"^{(.*)}$", value).group(1)
    return inner.split(",")


class ArrayOfEnum(ARRAY):
    """
    Workaround for array-of-enum problem

    See http://docs.sqlalchemy.org/en/latest/dialects/postgresql.html#postgresql-array-of-enum
    """

    def bind_expression(self, bindvalue):
        return cast(bindvalue, self)

    def result_processor(self, dialect, coltype):
        super_rp = super(ArrayOfEnum, self).result_processor(dialect, coltype)

        def process(value):
            # Convert array to Python objects
            return super_rp(sql_enum_to_list(value))

        return process


def fix_postgres_array_of_enum(connection, tbl):
    "Change type of ENUM[] columns to a custom type"

    for col in tbl.c:
        col_str = str(col.type)
        if col_str.endswith('[]'):  # this is an array
            enum_name = col_str[:-2]
            try:  # test if 'enum_name' is an enum
                enum_ranges = connection.execute('''
                        SELECT enum_range(NULL::%s);
                    ''' % enum_name).fetchone()
                enum_values = sql_enum_to_list(enum_ranges[0])
                enum = ENUM(*enum_values, name=enum_name)
                tbl.c[col.name].type = ArrayOfEnum(enum)
            except sa.exc.ProgrammingError as enum_excep:
                if 'does not exist' in str(enum_excep):
                    pass  # Must not have been an enum
                else:
                    raise
