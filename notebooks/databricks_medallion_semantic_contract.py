# Databricks notebook source
# MAGIC %md
# MAGIC # 🔥 Databricks Data Intelligence Platform: Medallion Architecture & Semantic Contracts
# MAGIC 
# MAGIC Este notebook está configurado sob medida para a estrutura do catálogo **`dados_em_chamas`** no Unity Catalog:
# MAGIC 
# MAGIC 1. **Unity Catalog**: Governança unificada no catálogo `dados_em_chamas`.
# MAGIC 2. **Camada Bronze (`dados_em_chamas.bronze`)**: Ingestão de dados brutos (JSON/Streaming/Batch) com metadados de auditoria.
# MAGIC 3. **Camada Silver (`dados_em_chamas.silver`)**: Saneamento, deduplicação, tipagem e upsert idempotente via `MERGE INTO` (ACID).
# MAGIC 4. **Quarentena (`dados_em_chamas.quarentena`)**: Isolamento automático de registros que violam o contrato semântico ou regras de qualidade.
# MAGIC 5. **Camada Gold (`dados_em_chamas.gold`)**: Data Product final protegido por **Contrato Semântico**, `CHECK constraints` e **Tags do Unity Catalog** (PII).
# MAGIC 6. **Auditoria & Qualidade (`dados_em_chamas.gov_quality`)**: Registro histórico das validações do contrato.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🛠️ 1. Configuração do Ambiente e Schemas no Unity Catalog

# COMMAND ----------

# Catálogo e esquemas identificados no ambiente
CATALOG_NAME = "dados_em_chamas"

spark.sql(f"USE CATALOG {CATALOG_NAME}")

# Garantindo que os esquemas estejam prontos para o laboratório
for schema in ["bronze", "silver", "gold", "quarentena", "gov_quality"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{schema}")

current_env = spark.sql("SELECT current_catalog(), current_schema()").first()
print(f"✅ Conectado ao Catálogo: {current_env[0]} (Schema padrão: {current_env[1]})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📥 2. Camada Bronze: Ingestão de Dados Brutos (Raw Ingestion)
# MAGIC 
# MAGIC Ingestão simulando a chegada de arquivos JSON na *Landing Zone*.
# MAGIC Adicionamos colunas de rastreabilidade e gravamos em tabela **Delta Lake**.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# 1. Simulação de payload de eventos de pedidos (incluindo anomalias para teste de contrato)
mock_raw_orders = [
    # Pedido válido normal
    '{"order_id": "ord-1001", "customer_id": "cust-01", "order_status": "COMPLETED", "total_amount": 150.50, "customer_email": "ana@empresa.com", "event_time": "2026-10-01 10:00:00"}',
    # Pedido válido pendente
    '{"order_id": "ord-1002", "customer_id": "cust-02", "order_status": "PENDING", "total_amount": 89.90, "customer_email": "carlos@gmail.com", "event_time": "2026-10-01 10:05:00"}',
    # Pedido com valor negativo (viola contrato semântico)
    '{"order_id": "ord-1003", "customer_id": "cust-03", "order_status": "COMPLETED", "total_amount": -50.00, "customer_email": "fake@teste.com", "event_time": "2026-10-01 10:10:00"}',
    # Pedido de alto valor válido
    '{"order_id": "ord-1004", "customer_id": "cust-04", "order_status": "COMPLETED", "total_amount": 3499.00, "customer_email": "marina@tech.io", "event_time": "2026-10-01 10:15:00"}'
]

raw_df = spark.read.json(spark.sparkContext.parallelize(mock_raw_orders))

# 2. Metadados de auditoria técnica
bronze_df = (
    raw_df
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.lit("landing_zone/orders_events.json"))
)

# 3. Gravação na tabela Bronze Delta
bronze_table = f"{CATALOG_NAME}.bronze.orders"
bronze_df.write.format("delta").mode("append").saveAsTable(bronze_table)

display(spark.table(bronze_table))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 3. Camada Silver: Saneamento, Tipagem e `MERGE` (Upsert Idempotente)
# MAGIC 
# MAGIC Na camada **Silver**:
# MAGIC - Tipamos campos rigorosamente (`DECIMAL`, `TIMESTAMP`).
# MAGIC - Deduplicamos pela chave de negócio (`order_id`).
# MAGIC - Aplicamos transações ACID com `MERGE INTO`.

# COMMAND ----------

from delta.tables import DeltaTable

silver_clean_df = (
    spark.table(bronze_table)
    .filter(F.col("order_id").isNotNull())
    .withColumn("total_amount", F.col("total_amount").cast("decimal(18,2)"))
    .withColumn("created_at", F.to_timestamp(F.col("event_time"), "yyyy-MM-dd HH:mm:ss"))
    .drop("event_time", "_source_file")
    .dropDuplicates(["order_id"])
)

silver_table = f"{CATALOG_NAME}.silver.orders"

if not spark.catalog.tableExists(silver_table):
    silver_clean_df.write.format("delta").saveAsTable(silver_table)
else:
    target_silver = DeltaTable.forName(spark, silver_table)
    (
        target_silver.alias("target")
        .merge(
            silver_clean_df.alias("source"),
            "target.order_id = source.order_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

display(spark.table(silver_table))

# COMMAND ----------

# MAGIC %md
# MAGIC ### ⏱️ Delta Lake Time Travel & Histórico de Commits

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY dados_em_chamas.silver.orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🛡️ 4. Validação do Contrato Semântico & Roteamento para Quarentena
# MAGIC 
# MAGIC O **Contrato Semântico** estipula:
# MAGIC 1. `total_amount >= 0.0` (Regra de integridade semântica)
# MAGIC 2. `order_id` não nulo e formato válido.
# MAGIC 
# MAGIC Registros aprovados avançam para a camada **Gold**; registros inválidos são roteados para **`quarentena`**.

# COMMAND ----------

silver_df = spark.table(silver_table)

# Regra do Contrato Semântico: Valor não pode ser negativo
valid_orders_df = silver_df.filter("total_amount >= 0.0 AND order_status = 'COMPLETED'")
quarantine_orders_df = silver_df.filter("total_amount < 0.0").withColumn(
    "_quarantine_reason", F.lit("VIOLATION: total_amount must be >= 0.0 (Semantic Contract v1.0.0)")
).withColumn("_quarantined_at", F.current_timestamp())

# Gravação na Quarentena
quarantine_table = f"{CATALOG_NAME}.quarentena.orders"
quarantine_orders_df.write.format("delta").mode("append").saveAsTable(quarantine_table)

print(f"🚨 Registros em Quarentena:")
display(spark.table(quarantine_table))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏆 5. Camada Gold & Governança no Unity Catalog
# MAGIC 
# MAGIC Criamos a tabela Gold aplicando:
# MAGIC - **Comentários de Negócio** em tabela e colunas.
# MAGIC - **CHECK Constraint** nativa do Delta Lake.
# MAGIC - **Tags Semânticas** de sensibilidade e privacidade (PII/LGPD) no Unity Catalog.

# COMMAND ----------

gold_table = f"{CATALOG_NAME}.gold.orders"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {gold_table} (
  order_id STRING NOT NULL COMMENT 'Identificador único universal do pedido',
  customer_id STRING NOT NULL COMMENT 'Identificador do cliente no CRM',
  order_status STRING NOT NULL COMMENT 'Status final do pedido',
  total_amount DECIMAL(18,2) NOT NULL COMMENT 'Valor financeiro total em BRL',
  customer_email STRING COMMENT 'Email de contato do comprador',
  created_at TIMESTAMP NOT NULL COMMENT 'Data e hora do pedido (UTC)',
  _ingested_at TIMESTAMP,
  PRIMARY KEY (order_id) NOT ENFORCED
)
USING DELTA
COMMENT 'Data Product Gold: Pedidos confirmados e auditados por contrato semântico'
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
""")

# 1. CHECK Constraint no Delta Lake
try:
    spark.sql(f"ALTER TABLE {gold_table} ADD CONSTRAINT chk_total_amount_positive CHECK (total_amount >= 0.0)")
    print("✅ CHECK constraint aplicada com sucesso na Gold.")
except Exception as e:
    print(f"ℹ️ {e}")

# 2. Governança e Tags Semânticas no Unity Catalog
spark.sql(f"ALTER TABLE {gold_table} SET TAGS ('domain' = 'sales', 'data_product' = 'orders_gold', 'compliance' = 'audited')")
spark.sql(f"ALTER TABLE {gold_table} ALTER COLUMN customer_email SET TAGS ('classification' = 'PII', 'privacy_regulation' = 'LGPD')")

# 3. Carga final da Gold
valid_orders_df.write.format("delta").mode("overwrite").saveAsTable(gold_table)

display(spark.table(gold_table))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📈 6. Registro de Auditoria de Qualidade (`gov_quality`)

# COMMAND ----------

# Registrando métricas da execução do contrato semântico
total_valid = valid_orders_df.count()
total_quarantined = quarantine_orders_df.count()

audit_log_df = spark.createDataFrame([
    ("orders_v1.contract.yaml", "gold.orders", total_valid, total_quarantined, "SUCCESS")
], ["contract_name", "target_table", "records_passed", "records_quarantined", "status"]).withColumn(
    "executed_at", F.current_timestamp()
)

gov_quality_table = f"{CATALOG_NAME}.gov_quality.contract_audit_log"
audit_log_df.write.format("delta").mode("append").saveAsTable(gov_quality_table)

display(spark.table(gov_quality_table))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 7. Consumo Analítico (SQL)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG dados_em_chamas;
# MAGIC USE SCHEMA gold;
# MAGIC 
# MAGIC SELECT 
# MAGIC   customer_id,
# MAGIC   COUNT(order_id) AS total_pedidos,
# MAGIC   SUM(total_amount) AS total_gasto,
# MAGIC   ROUND(AVG(total_amount), 2) AS ticket_medio
# MAGIC FROM 
# MAGIC   orders
# MAGIC GROUP BY 
# MAGIC   customer_id
# MAGIC ORDER BY 
# MAGIC   total_gasto DESC;
