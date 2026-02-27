"""Unit tests for SQL parser."""

from databricks_advanced_mcp.parsers.sql_parser import (
    ColumnReference,
    SQLParseResult,
    TableReference,
    extract_table_names,
    parse_sql,
)


class TestParseSql:
    """Tests for the parse_sql function."""

    def test_simple_select(self):
        result = parse_sql("SELECT id, name FROM users")
        assert len(result.tables) == 1
        assert result.tables[0].table == "users"
        assert result.tables[0].reference_type == "reads_from"

    def test_fully_qualified_table(self):
        result = parse_sql("SELECT * FROM catalog.schema.my_table")
        assert len(result.tables) == 1
        ref = result.tables[0]
        assert ref.catalog == "catalog"
        assert ref.schema == "schema"
        assert ref.table == "my_table"
        assert ref.fqn == "catalog.schema.my_table"

    def test_insert_into(self):
        result = parse_sql("INSERT INTO main.silver.output SELECT * FROM main.bronze.input")
        assert len(result.tables) >= 2
        
        write_refs = [r for r in result.tables if r.reference_type == "writes_to"]
        read_refs = [r for r in result.tables if r.reference_type == "reads_from"]
        assert len(write_refs) >= 1
        assert write_refs[0].fqn == "main.silver.output"
        assert len(read_refs) >= 1

    def test_join_query(self):
        sql = """
        SELECT o.id, c.name
        FROM orders o
        JOIN customers c ON o.customer_id = c.id
        """
        result = parse_sql(sql)
        table_names = {r.table for r in result.tables}
        assert "orders" in table_names
        assert "customers" in table_names

    def test_create_table_as_select(self):
        result = parse_sql("CREATE TABLE main.gold.summary AS SELECT * FROM main.silver.events")
        write_refs = [r for r in result.tables if r.reference_type == "writes_to"]
        assert any(r.fqn == "main.gold.summary" for r in write_refs)

    def test_merge_statement(self):
        sql = """
        MERGE INTO main.gold.target AS t
        USING main.silver.source AS s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET t.value = s.value
        WHEN NOT MATCHED THEN INSERT *
        """
        result = parse_sql(sql)
        write_refs = [r for r in result.tables if r.reference_type == "writes_to"]
        assert any("target" in r.fqn for r in write_refs)

    def test_default_catalog_schema(self):
        result = parse_sql("SELECT * FROM my_table", default_catalog="main", default_schema="default")
        assert len(result.tables) == 1
        ref = result.tables[0]
        assert ref.catalog == "main"
        assert ref.schema == "default"

    def test_column_extraction(self):
        result = parse_sql("SELECT id, name FROM users WHERE age > 18")
        col_names = {c.column for c in result.columns}
        assert "id" in col_names
        assert "name" in col_names
        assert "age" in col_names

    def test_invalid_sql(self):
        result = parse_sql("THIS IS NOT SQL AT ALL $$$$")
        # Should return without crashing; may have errors
        assert isinstance(result, SQLParseResult)

    def test_multiple_statements(self):
        sql = "SELECT * FROM table1; SELECT * FROM table2"
        result = parse_sql(sql)
        table_names = {r.table for r in result.tables}
        assert "table1" in table_names
        assert "table2" in table_names


class TestExtractTableNames:
    """Tests for the quick helper function."""

    def test_basic(self):
        names = extract_table_names("SELECT * FROM main.schema.my_table")
        assert "main.schema.my_table" in names

    def test_deduplication(self):
        sql = "SELECT * FROM t1 JOIN t1 ON t1.id = t1.parent_id"
        names = extract_table_names(sql)
        assert len([n for n in names if "t1" in n]) == 1
