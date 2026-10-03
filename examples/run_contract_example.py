"""
Demonstração de Uso: Data & Semantic Contract no Databricks
------------------------------------------------------------
Este script executa a validação de um contrato e exibe:
1. Validação de conformidade estrutural.
2. DDL gerado para o Unity Catalog (Tabelas, Comentários, Constraints e Tags de Governança).
3. Configuração de regras prontas para uso com Delta Live Tables (DLT).
"""

import sys
from pathlib import Path

# Adiciona o diretório raiz ao PYTHONPATH
sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.contract_enforcer import SemanticContract


def main():
    contract_file = Path(__file__).resolve().parent.parent / "contracts" / "orders_v1.contract.yaml"
    print(f"📖 Carregando Contrato Semântico: {contract_file.name}")
    contract = SemanticContract(str(contract_file))

    print("\n" + "=" * 60)
    print("1. VALIDAÇÃO DE SCHEMA (Exemplo)")
    print("=" * 60)
    
    # Exemplo de schema recebido em um DataFrame
    mock_df_schema = {
        "order_id": "string",
        "customer_id": "string",
        "order_status": "string",
        "total_amount": "decimal(18,2)",
        "customer_email": "string",
        "created_at": "timestamp"
    }
    
    validation_res = contract.validate_schema(mock_df_schema)
    print(f"Status da Validação: {'✅ APROVADO' if validation_res['passed'] else '❌ REPROVADO'}")
    if not validation_res['passed']:
        print(f"Detalhes: {validation_res}")

    print("\n" + "=" * 60)
    print("2. DDL SQL PARA O UNITY CATALOG (Tags, Comments & Constraints)")
    print("=" * 60)
    ddl_sql = contract.generate_unity_catalog_ddl()
    print(ddl_sql)

    print("\n" + "=" * 60)
    print("3. REGRAS DE QUALIDADE PARA DELTA LIVE TABLES (DLT)")
    print("=" * 60)
    dlt_rules = contract.get_dlt_expectations()
    for name, expr in dlt_rules.items():
        print(f" - Rule '{name}': {expr}")


if __name__ == "__main__":
    main()
