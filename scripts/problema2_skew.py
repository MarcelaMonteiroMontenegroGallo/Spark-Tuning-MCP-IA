"""
❌ PROBLEMA 2: DATA SKEW (Dados Desbalanceados)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
O que é skew?
  Quando os dados não estão distribuídos igualmente entre
  as partições. Uma partição tem 80% dos dados e as outras
  ficam ociosas esperando a "partição gorda" terminar.

Por que acontece aqui?
  - categoria "PROMO_ESPECIAL" tem 80% das vendas
  - AQE desligado — Spark não consegue detectar e corrigir
  - 1 executor trabalha por 40min, os outros ficam parados

Como o MCP detecta?
  Pergunta: "Tem algum executor sobrecarregado?"
  Resposta: "Executor 3 processou 78% dos dados no Stage 4.
             Partição 0 tem 3.8M linhas vs média de 50k"
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count

spark = SparkSession.builder \
    .appName("Problema2-DataSkew") \
    .master("local[*]") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.sql.adaptive.skewJoin.enabled", "false") \
    .config("spark.sql.shuffle.partitions", "10") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "/tmp/spark-events") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("❌ PROBLEMA 2: Data Skew")
print("   Observe no Spark UI → Stages → Tasks")
print("   Uma task vai demorar MUITO mais que as outras")
print("   URL: http://localhost:4040\n")

df_vendas = spark.read.parquet("/tmp/spark_aula/vendas/")

# Mostrar o skew nos dados
print("📊 Distribuição dos dados por categoria:")
df_vendas.groupBy("categoria").count().orderBy(col("count").desc()).show()

# ❌ RUIM: groupBy sem tratar skew — PROMO_ESPECIAL vai sobrecarregar 1 partição
df_resultado = df_vendas \
    .groupBy("categoria", "produto_id") \
    .agg(
        spark_sum("valor").alias("total_valor"),
        count("venda_id").alias("qtd_vendas")
    )

df_resultado.show(10)
print(f"\nTotal de grupos: {df_resultado.count()}")

spark.stop()
print("\n✅ Rode agora: solucao2_aqe_salting.py para ver a diferença!")
