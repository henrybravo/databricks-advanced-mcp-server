"""Unit tests for notebook parser."""

from databricks_advanced_mcp.parsers.notebook_parser import (
    CellType,
    NotebookCell,
    _extract_pyspark_references,
    extract_table_references,
    parse_notebook,
)


class TestParseNotebook:
    """Tests for notebook parsing."""

    def test_python_notebook_cell_splitting(self):
        source = """# Cell 1
x = 1

# COMMAND ----------

# Cell 2
y = 2

# COMMAND ----------

# Cell 3
z = 3
"""
        result = parse_notebook(source, default_language="python")
        assert len(result.cells) == 3
        assert result.cells[0].cell_type == CellType.PYTHON

    def test_sql_notebook_cell_splitting(self):
        source = """SELECT 1

-- COMMAND ----------

SELECT 2
"""
        result = parse_notebook(source, default_language="sql")
        assert len(result.cells) == 2
        assert result.cells[0].cell_type == CellType.SQL

    def test_magic_command_classification(self):
        source = """%sql
SELECT * FROM my_table

# COMMAND ----------

%python
x = spark.table("other_table")
"""
        result = parse_notebook(source, default_language="python")
        assert any(c.cell_type == CellType.SQL for c in result.cells)
        assert any(c.cell_type == CellType.PYTHON for c in result.cells)

    def test_empty_notebook(self):
        result = parse_notebook("", default_language="python")
        assert result.cells == []
        assert result.table_references == []


class TestPySparkExtraction:
    """Tests for PySpark regex table extraction."""

    def test_spark_table(self):
        code = 'df = spark.table("main.schema.my_table")'
        refs = _extract_pyspark_references(code)
        assert len(refs) >= 1
        assert any(r.fqn == "main.schema.my_table" for r in refs)

    def test_spark_read_table(self):
        code = 'df = spark.read.table("catalog.schema.table_name")'
        refs = _extract_pyspark_references(code)
        assert any(r.fqn == "catalog.schema.table_name" for r in refs)

    def test_delta_table_for_name(self):
        code = 'dt = DeltaTable.forName(spark, "main.gold.events")'
        refs = _extract_pyspark_references(code)
        assert any(r.fqn == "main.gold.events" for r in refs)

    def test_write_save_as_table(self):
        code = 'df.write.mode("overwrite").saveAsTable("main.silver.output")'
        refs = _extract_pyspark_references(code)
        write_refs = [r for r in refs if r.reference_type == "writes_to"]
        assert any(r.fqn == "main.silver.output" for r in write_refs)

    def test_write_insert_into(self):
        code = 'df.write.insertInto("main.silver.output")'
        refs = _extract_pyspark_references(code)
        write_refs = [r for r in refs if r.reference_type == "writes_to"]
        assert any(r.fqn == "main.silver.output" for r in write_refs)

    def test_dlt_read(self):
        code = 'df = dlt.read("bronze_events")'
        refs = _extract_pyspark_references(code)
        assert any(r.table == "bronze_events" for r in refs)

    def test_dlt_read_stream(self):
        code = 'df = dlt.read_stream("silver_events")'
        refs = _extract_pyspark_references(code)
        assert any(r.table == "silver_events" for r in refs)

    def test_no_references(self):
        code = "x = 1 + 2\nprint(x)"
        refs = _extract_pyspark_references(code)
        assert refs == []

    def test_single_part_table_name(self):
        code = 'df = spark.table("my_table")'
        refs = _extract_pyspark_references(code)
        assert any(r.table == "my_table" for r in refs)

    def test_two_part_table_name(self):
        code = 'df = spark.table("schema.my_table")'
        refs = _extract_pyspark_references(code)
        assert any(r.schema == "schema" and r.table == "my_table" for r in refs)


class TestExtractTableReferences:
    """Integration tests for extract_table_references."""

    def test_mixed_sql_and_python_cells(self):
        cells = [
            NotebookCell(0, "SELECT * FROM main.bronze.events", CellType.SQL, "sql"),
            NotebookCell(1, 'df = spark.table("main.silver.users")', CellType.PYTHON, "python"),
        ]
        refs = extract_table_references(cells)
        table_fqns = {r.fqn for r in refs}
        assert "main.bronze.events" in table_fqns
        assert "main.silver.users" in table_fqns

    def test_deduplication(self):
        cells = [
            NotebookCell(0, "SELECT * FROM my_table", CellType.SQL, "sql"),
            NotebookCell(1, "SELECT * FROM my_table", CellType.SQL, "sql"),
        ]
        refs = extract_table_references(cells)
        assert len([r for r in refs if r.table == "my_table"]) == 1
