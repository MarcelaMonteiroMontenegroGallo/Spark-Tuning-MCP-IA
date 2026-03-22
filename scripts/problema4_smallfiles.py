"""
❌ PROBLEMA 4: SMALL FILES (Arquivos Pequenos)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
O que é o problema de small files?
  O Spark escreve 1 arquivo por partição. Com 200 partições
  padrão para 100MB de dados, você gera 200 arquivos de 500KB.
  Cada arquivo tem overhead de metadados no S3/HDFS.
  Leituras futuras ficam lentas por causa do overhead.

Por que acontece aqui?
  - shuffle.partitions = 200 (padrão) para dados pequenos
  - Sem coalesce() antes de escrever
  - Resultado: 200 arquivos de ~500KB cada

Como o MCP detecta?
  Pergunta: "Quantas tasks de escrita teve no último stage?"
  Resposta: "Stage 5 (write): 200 tasks, média de 512KB por task.
             Recomendo coalesce() para reduzir para 10-20 arquivos"
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count
import os

spark = SparkSession.builder \
    .appName("Problema4-SmallFiles") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "/tmp/spark-events") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("❌ PROBLEMA 4: Small Files")
print("   Observe no Spark UI → Stages → número de tasks na escrita")
print("   URL: http://localhost:4040\n")

df_vendas = spark.read.parquet("/tmp/spark_aula/vendas/")

df_resultado = df_vendas \
    .groupBy("categoria", "data_venda") \
    .agg(spark_sum("valor").alias("total"), count("venda_id").alias("qtd"))

print(f"❌ Número de partições antes de escrever: {df_resultado.rdd.getNumPartitions()}")
print("❌ Isso vai gerar 200 arquivos pequenos no S3!\n")

# ❌ RUIM: escreve 200 arquivos pequenos
df_resultado.write.mode("overwrite").parquet("/tmp/spark_aula/resultado_smallfiles/")

# Conta os arquivos gerados
arquivos = [f for f in os.listdir("/tmp/spark_aula/resultado_smallfiles/") if f.endswith(".parquet")]
print(f"❌ Arquivos gerados: {len(arquivos)} arquivos parquet")
print("   Cada um com ~500KB — overhead enorme para leituras futuras!")

spark.stop()
print("\n✅ Rode agora: solucao4_coalesce.py para ver a diferença!")
