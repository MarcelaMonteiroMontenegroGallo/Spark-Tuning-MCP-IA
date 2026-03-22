"""
✅ SOLUÇÃO 2: AQE + SALTING para Data Skew
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Duas abordagens:

OPÇÃO A — AQE (Adaptive Query Execution):
  Spark 3.0+. Liga e esquece. O Spark detecta partições
  grandes automaticamente e as divide em pedaços menores.
  Configuração: spark.sql.adaptive.skewJoin.enabled=true

OPÇÃO B — Salting Manual:
  Adiciona um número aleatório (0-9) na chave do join.
  "PROMO_ESPECIAL" vira "PROMO_ESPECIAL_0", "PROMO_ESPECIAL_1"...
  Distribui os dados em 10 partições ao invés de 1.
  Use quando AQE não resolver ou para Spark < 3.0.

Resultado esperado:
  Tempo da task mais lenta: 40min → 4min
  Distribuição: 78% numa partição → ~10% em cada
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count,
    concat, lit, floor, rand, expr
)

spark = SparkSession.builder \
    .appName("Solucao2-AQE-Salting") \
    .master("local[*]") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5") \
    .config("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "50") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "/tmp/spark-events") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("✅ SOLUÇÃO 2A: AQE (Adaptive Query Execution)")
print("   O Spark vai dividir automaticamente as partições grandes\n")

df_vendas = spark.read.parquet("/tmp/spark_aula/vendas/")

# ✅ OPÇÃO A: AQE — só ligar e o Spark cuida do resto
df_resultado_aqe = df_vendas \
    .groupBy("categoria", "produto_id") \
    .agg(
        spark_sum("valor").alias("total_valor"),
        count("venda_id").alias("qtd_vendas")
    )

df_resultado_aqe.show(5)
print(f"Total AQE: {df_resultado_aqe.count()}")

print("\n" + "─"*50)
print("✅ SOLUÇÃO 2B: Salting Manual (para casos extremos)")
print("   Adiciona sufixo aleatório para distribuir a chave skewed\n")

SALT_FACTOR = 10  # divide em 10 partições

# Adiciona salt na tabela grande
df_vendas_salted = df_vendas.withColumn(
    "categoria_salt",
    concat(col("categoria"), lit("_"), (floor(rand() * SALT_FACTOR)).cast("string"))
)

# Agrega com a chave saltada
df_agg_salt = df_vendas_salted \
    .groupBy("categoria_salt", "produto_id") \
    .agg(
        spark_sum("valor").alias("total_valor"),
        count("venda_id").alias("qtd_vendas")
    )

# Remove o salt e agrega novamente (dois estágios)
df_resultado_salt = df_agg_salt \
    .withColumn("categoria", expr("split(categoria_salt, '_')[0]")) \
    .groupBy("categoria", "produto_id") \
    .agg(
        spark_sum("total_valor").alias("total_valor"),
        spark_sum("qtd_vendas").alias("qtd_vendas")
    )

df_resultado_salt.show(5)
print(f"Total Salting: {df_resultado_salt.count()}")

spark.stop()
print("\n🎯 Pergunta para o MCP: 'As tasks estão mais balanceadas agora?'")
