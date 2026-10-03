"""
Contract Enforcer for Databricks & Unity Catalog
------------------------------------------------
Este módulo processa especificações de contrato semântico (YAML) e:
1. Valida conformidade estrutural e semântica de DataFrames PySpark/Pandas.
2. Gera comandos SQL para Unity Catalog (comentários, tags de colunas/tabelas, constraints).
3. Converte regras de qualidade em expectativas Delta Live Tables (DLT).
"""

from typing import Dict, Any, List
import yaml
from pathlib import Path


class SemanticContract:
    def __init__(self, contract_path: str):
        self.contract_path = Path(contract_path)
        with open(self.contract_path, "r", encoding="utf-8") as f:
            self.spec: Dict[str, Any] = yaml.safe_load(f)

    @property
    def full_table_name(self) -> str:
        tgt = self.spec.get("target", {})
        catalog = tgt.get("catalog", "main")
        schema = tgt.get("schema", "default")
        table = tgt.get("table", self.spec.get("dataset", "unknown"))
        return f"{catalog}.{schema}.{table}"

    @property
    def fields(self) -> List[Dict[str, Any]]:
        return self.spec.get("fields", [])

    @property
    def quality_checks(self) -> List[Dict[str, Any]]:
        return self.spec.get("quality_checks", [])

    def generate_unity_catalog_ddl(self) -> str:
        """
        Gera o script DDL SQL com comentários, chaves primárias e CHECK constraints para o Unity Catalog.
        """
        sql_lines = []
        full_table = self.full_table_name
        
        # 1. Colunas e Tipos
        cols_sql = []
        pk_cols = []
        
        for field in self.fields:
            name = field["name"]
            data_type = field["type"].upper()
            nullable = "" if field.get("nullable", True) else " NOT NULL"
            comment = f" COMMENT '{field.get('description', '')}'" if field.get("description") else ""
            cols_sql.append(f"  {name} {data_type}{nullable}{comment}")
            
            if field.get("primary_key", False):
                pk_cols.append(name)
        
        if pk_cols:
            cols_sql.append(f"  PRIMARY KEY ({', '.join(pk_cols)}) NOT ENFORCED")

        cols_body = ",\n".join(cols_sql)
        table_comment = self.spec.get("description", "")
        
        ddl = f"CREATE TABLE IF NOT EXISTS {full_table} (\n{cols_body}\n)\n"
        ddl += f"USING DELTA\n"
        if table_comment:
            ddl += f"COMMENT '{table_comment}';\n\n"

        sql_lines.append(ddl)

        # 2. CHECK Constraints
        sql_lines.append(f"-- ==========================================")
        sql_lines.append(f"-- 1. Unity Catalog CHECK Constraints")
        sql_lines.append(f"-- ==========================================")
        for field in self.fields:
            for check in field.get("checks", []):
                rule = check["rule"]
                constraint_name = f"chk_{field['name']}_{abs(hash(rule)) % 10000}"
                sql_lines.append(
                    f"ALTER TABLE {full_table} ADD CONSTRAINT {constraint_name} CHECK ({rule});"
                )
        sql_lines.append("")

        # 3. Tags de Governança Semântica no Unity Catalog
        sql_lines.append(f"-- ==========================================")
        sql_lines.append(f"-- 2. Tags Semânticas & Governança (Unity Catalog)")
        sql_lines.append(f"-- ==========================================")
        # Tag de nível de tabela
        domain = self.spec.get("domain", "general")
        owner_team = self.spec.get("owner", {}).get("team", "unassigned")
        sql_lines.append(
            f"ALTER TABLE {full_table} SET TAGS ('domain' = '{domain}', 'owner' = '{owner_team}');"
        )

        # Tags de nível de coluna
        for field in self.fields:
            col_name = field["name"]
            tags = field.get("tags", {})
            semantic_type = field.get("semantic_type")
            if semantic_type:
                tags["semantic_type"] = semantic_type
            
            if tags:
                tag_pairs = ", ".join([f"'{k}' = '{v}'" for k, v in tags.items()])
                sql_lines.append(
                    f"ALTER TABLE {full_table} ALTER COLUMN {col_name} SET TAGS ({tag_pairs});"
                )

        return "\n".join(sql_lines)

    def get_dlt_expectations(self) -> Dict[str, str]:
        """
        Retorna as regras no formato aceito pelo Delta Live Tables (DLT):
        @dlt.expect, @dlt.expect_or_fail, @dlt.expect_or_drop
        """
        expectations = {}
        for check in self.quality_checks:
            name = check["name"]
            expression = check["expression"]
            expectations[name] = expression
        return expectations

    def validate_schema(self, df_columns_and_types: Dict[str, str]) -> Dict[str, Any]:
        """
        Valida se as colunas e tipos de um DataFrame batem com o contrato.
        """
        contract_cols = {f["name"]: f["type"].lower() for f in self.fields}
        missing_cols = [c for c in contract_cols if c not in df_columns_and_types]
        extra_cols = [c for c in df_columns_and_types if c not in contract_cols]
        type_mismatches = []

        for col, expected_type in contract_cols.items():
            if col in df_columns_and_types:
                actual_type = df_columns_and_types[col].lower()
                # Simplificação de validação de tipos
                if expected_type not in actual_type and actual_type not in expected_type:
                    type_mismatches.append({
                        "column": col,
                        "expected": expected_type,
                        "actual": actual_type
                    })

        passed = len(missing_cols) == 0 and len(type_mismatches) == 0
        return {
            "passed": passed,
            "missing_columns": missing_cols,
            "extra_columns": extra_cols,
            "type_mismatches": type_mismatches
        }
