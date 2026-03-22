"""
❌ PROBLEMA 1: SHUFFLE EXCESSIVO
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
O que é shuffle?
  Quando o Spark precisa redistribuir dados entre executores
  pela rede. É a operação mais cara do Spark.

Por que acontece aqui?
  - autoBroadcastJoinThreshold = -1 (broadcast desligado)
  - df_produtos tem só 1000 linhas mas vai pelo shuffle
  - Resultado: 8GB trafegando pela rede desnecessariamente

Como o MCP detecta?
  Pergunta: "Qual stage tem mais shuffle write?"
  Resposta: "Stage 2 — 8.2GB de shuffle write no join de produtos"
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count

spark = SparkSession.builder \
    .appName("Problema1-ShuffleExcessivo") \
    .master("local[*]") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "/tmp/spark-events") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("❌ PROBLEMA 1: Shuffle Excessivo")
print("   Observe no Spark UI: Stage com alto 'Shuffle Write'")
print("   URL: http://localhost:4040\n")

df_vendas   = spark.read.parquet("/tmp/spark_aula/vendas/")
df_produtos = spark.read.parquet("/tmp/spark_aula/produtos/")

# ❌ RUIM: join sem broadcast — df_produtos (1000 linhas) vai pelo shuffle
df_resultado = df_vendas.join(df_produtos, "produto_id") \
    .groupBy("categoria") \
    .agg(spark_sum("valor").alias("total"), count("venda_id").alias("qtd"))

df_resultado.show(10)
print(f"\nTotal de categorias: {df_resultado.count()}")

spark.stop()
print("\n✅ Rode agora: solucao1_broadcast.py para ver a diferença!")
