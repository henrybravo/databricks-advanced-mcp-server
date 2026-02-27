"""Unit tests for reviewer rule engines."""

from databricks_advanced_mcp.reviewers.performance import check_performance
from databricks_advanced_mcp.reviewers.standards import check_standards
from databricks_advanced_mcp.reviewers.suggestions import check_suggestions


class TestPerformanceRules:
    """Tests for performance review rules."""

    def test_collect_without_limit(self):
        code = "results = df.collect()"
        findings = check_performance(code, 0, "python")
        assert any(f.rule_id == "PERF002" for f in findings)

    def test_topandas_without_limit(self):
        code = "pdf = df.toPandas()"
        findings = check_performance(code, 0, "python")
        assert any(f.rule_id == "PERF003" for f in findings)

    def test_select_star_without_where_sql(self):
        code = "SELECT * FROM my_table"
        findings = check_performance(code, 0, "sql")
        assert any(f.rule_id == "PERF001" for f in findings)

    def test_select_with_where_no_flag(self):
        code = "SELECT * FROM my_table WHERE id > 10"
        findings = check_performance(code, 0, "sql")
        # PERF001 should not fire because there's a WHERE
        perf001 = [f for f in findings if f.rule_id == "PERF001"]
        assert len(perf001) == 0

    def test_cross_join_detection(self):
        code = "SELECT * FROM t1 CROSS JOIN t2"
        findings = check_performance(code, 0, "sql")
        assert any(f.rule_id == "PERF007" for f in findings)

    def test_udf_detection(self):
        code = "@udf\ndef my_func(x):\n    return x + 1"
        findings = check_performance(code, 0, "python")
        assert any(f.rule_id == "PERF008" for f in findings)

    def test_clean_code_no_findings(self):
        code = "df = spark.table('t').filter('id > 0')"
        findings = check_performance(code, 0, "python")
        # No critical issues in this clean code
        critical = [f for f in findings if f.severity == "critical"]
        assert len(critical) == 0


class TestStandardsRules:
    """Tests for coding standards rules."""

    def test_hardcoded_password(self):
        code = 'password = "secret123"'
        findings = check_standards(code, 0, "python")
        assert any(f.rule_id == "STD020" for f in findings)

    def test_hardcoded_token(self):
        code = 'api_key = "my-secret-key"'
        findings = check_standards(code, 0, "python")
        assert any(f.rule_id == "STD020" for f in findings)

    def test_wildcard_import(self):
        code = "from pyspark.sql.functions import *"
        findings = check_standards(code, 0, "python")
        assert any(f.rule_id == "STD002" for f in findings)

    def test_hardcoded_path(self):
        code = 'df = spark.read.parquet("dbfs:/mnt/data/table")'
        findings = check_standards(code, 0, "python")
        assert any(f.rule_id == "STD003" for f in findings)

    def test_select_star_sql(self):
        code = "SELECT * FROM my_table WHERE id = 1"
        findings = check_standards(code, 0, "sql")
        assert any(f.rule_id == "STD010" for f in findings)

    def test_clean_code(self):
        code = "import pyspark.sql.functions as F\ndf = spark.table('t')"
        findings = check_standards(code, 0, "python")
        # Should not have critical findings
        critical = [f for f in findings if f.severity == "critical"]
        assert len(critical) == 0


class TestSuggestionRules:
    """Tests for optimization suggestions."""

    def test_withcolumn_in_loop(self):
        code = "for col_name in columns:\n    df = df.withColumn(col_name, F.lit(0))"
        findings = check_suggestions(code, 0, "python")
        assert any(f.rule_id == "OPT011" for f in findings)

    def test_repartition_one(self):
        code = "df.repartition(1).write.parquet('output')"
        findings = check_suggestions(code, 0, "python")
        assert any(f.rule_id == "OPT012" for f in findings)

    def test_cache_without_unpersist(self):
        code = "df.cache()\ndf.count()"
        findings = check_suggestions(code, 0, "python")
        assert any(f.rule_id == "OPT013" for f in findings)

    def test_nested_subqueries(self):
        code = "SELECT * FROM (SELECT * FROM (SELECT * FROM (SELECT 1)))"
        findings = check_suggestions(code, 0, "sql")
        assert any(f.rule_id == "OPT004" for f in findings)

    def test_merge_without_optimize(self):
        code = "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN UPDATE SET *"
        findings = check_suggestions(code, 0, "sql")
        assert any(f.rule_id == "OPT001" for f in findings)
