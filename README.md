# Databricks Semantic & Data Contract Experiment

Projeto de referência para implementação de **Contratos de Dados e Semânticos (Data & Semantic Contracts)** no ecossistema **Databricks / Lakehouse**, com suporte nativo a **Unity Catalog** e **Delta Live Tables (DLT)**.

---

## 📌 O que é um Semantic Contract no Databricks?

Um **Data Contract (Contrato de Dados)** estabelece um acordo formal entre quem produz e quem consome os dados. Quando adicionamos a camada **Semântica**, o contrato define não apenas tipos e colunas, mas:

1. **Significado de Negócio**: Termos de glossário, unidade de medida, taxonomia.
2. **Governança e Sensibilidade**: Classificação de dados (PII, GDPR, LGPD, restrito/público) que se traduzem em **Tags no Unity Catalog**.
3. **Qualidade e Integridade**: Regras de validação (ex: `total_amount >= 0`) mapeadas para **CHECK Constraints** e **Expectativas DLT** (`@dlt.expect_or_fail`).
4. **Acordos de Serviço (SLA)**: Freshness e formato esperado de ingestão.

---

## 📂 Estrutura do Projeto

```text
databricks_semantic_contract_experiment/
├── contracts/
│   └── orders_v1.contract.yaml       # Especificação do contrato (Schema, Semântica, Regras)
├── notebooks/
│   └── databricks_medallion_semantic_contract.py # Notebook Databricks ponta a ponta (Medallion + UC)
├── src/
│   └── contract_enforcer.py          # Parser e gerador de DDL/Tags para Unity Catalog & DLT
├── examples/
│   └── run_contract_example.py       # Exemplo prático de execução e validação
└── README.md                         # Documentação do projeto
```

---

## 🎯 Cobertura de Competências Essenciais da Plataforma

Este projeto e o notebook cobrem diretamente os pilares exigidos no ecossistema Databricks:

| Pilar | Como é demonstrado no projeto |
|---|---|
| **Databricks Platform & Unity Catalog** | Gestão de `CATALOG`, `SCHEMA`, `TABLE`, Primary Keys e Tags de governança/PII. |
| **Apache Spark & Python/SQL** | Transformações com PySpark DataFrame API, funções analíticas SQL e casting de tipos. |
| **Delta Lake Features** | Transações ACID, `MERGE INTO` (Upsert), `CHECK Constraints` e `DESCRIBE HISTORY` (Time Travel). |
| **Arquitetura Medallion** | Fluxo de dados claro entre camadas **Bronze** (Raw), **Silver** (Curated/Clean) e **Gold** (Semantic/Business). |
| **Ingestão & ETL** | Ingestão incremental, metadados de ingestão (`_ingested_at`) e processamento multiformato (JSON/Parquet). |
| **Contratos Semânticos** | Especificação em YAML e validação automatizada de integridade e metadados de negócio. |

---

## 📄 Exemplo do Contrato (`contracts/orders_v1.contract.yaml`)

```yaml
version: "1.0.0"
dataset: "gold_orders"
domain: "sales"
owner:
  team: "Sales Analytics Data Product Team"
  email: "sales-data@example.com"

target:
  catalog: "main"
  schema: "sales"
  table: "gold_orders"
  format: "delta"

fields:
  - name: "order_id"
    type: "string"
    nullable: false
    semantic_type: "identifier"
    primary_key: true

  - name: "total_amount"
    type: "decimal(18,2)"
    nullable: false
    semantic_type: "currency"
    unit: "BRL"
    checks:
      - rule: "total_amount >= 0.0"

  - name: "customer_email"
    type: "string"
    nullable: true
    semantic_type: "email"
    tags:
      classification: "PII"
      gdpr_sensitive: "true"

quality_checks:
  - name: "valid_amount_range"
    expression: "total_amount >= 0 AND total_amount <= 1000000"
    action: "FAIL"
```

---

## 🚀 Como o Contrato é Aplicado no Databricks

### 1. Governança Automática no Unity Catalog
O enforcer gera automaticamente o SQL para criar e taguear a tabela:

```sql
-- DDL com Comentários e Primary Key
CREATE TABLE IF NOT EXISTS main.sales.gold_orders (
  order_id STRING NOT NULL COMMENT 'Identificador único universal do pedido (UUIDv4)',
  total_amount DECIMAL(18,2) NOT NULL COMMENT 'Valor total bruto do pedido na moeda local',
  customer_email STRING COMMENT 'Email de contato do cliente no checkout',
  PRIMARY KEY (order_id) NOT ENFORCED
) USING DELTA;

-- CHECK Constraint nativo do Delta Lake
ALTER TABLE main.sales.gold_orders 
ADD CONSTRAINT chk_total_amount_positive CHECK (total_amount >= 0.0);

-- Tags de Governança Semântica
ALTER TABLE main.sales.gold_orders SET TAGS ('domain' = 'sales', 'owner' = 'Sales Analytics Team');
ALTER TABLE main.sales.gold_orders ALTER COLUMN customer_email SET TAGS ('classification' = 'PII', 'gdpr_sensitive' = 'true');
```

### 2. Uso com Delta Live Tables (DLT)
Em pipelines DLT, as expectativas do contrato podem ser aplicadas dinamicamente:

```python
import dlt
from src.contract_enforcer import SemanticContract

contract = SemanticContract("contracts/orders_v1.contract.yaml")

@dlt.table(name="gold_orders")
@dlt.expect_all_or_fail(contract.get_dlt_expectations())
def gold_orders():
    return spark.readStream.table("silver_orders")
```
