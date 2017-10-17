from subsetter import merge_config_args

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
    buffer = 100


def test_merges_tables_from_config_file():
    args = DummyArgs()
    args.tables = ["zeppelins"]
    args.config["tables"] = [ "zeppos" ]

    merge_config_args(args)

    assert args.tables == ["zeppelins", "zeppos"]

def test_merges_schemas_from_config_file():
    args = DummyArgs()
    args.schema = ["schema1"]
    args.config["schemas"] = [ "schema2" ]

    merge_config_args(args)
    assert args.schema == ["schema1", "schema2"]

def test_merges_full_tables_from_config_file():
    args = DummyArgs()
    args.full_tables = ["table1"]
    args.config["full_tables"] = [ "table2" ]

    merge_config_args(args)
    assert args.full_tables == ["table1", "table2"]
