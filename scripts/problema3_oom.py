"""
❌ PROBLEMA 3: OUT OF MEMORY (OOM)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
O que é OOM?
  O executor fica sem memória e o job falha ou fica
  extremamente lento por causa de spill para disco.

Por que acontece aqui?
  - cache() sem unpersist() — dados ficam na memória para sempre
  - Múltiplos DataFrames cacheados ao mesmo tempo
  - memoryFraction muito baixo (pouca memória para execução)

Como o MCP detecta?
  Pergunta: "Tem spill para disco nos stages?"
  Resposta: "Stage 3: Spill (memory) = 4.2GB, Spill (disk) = 1.8GB.
             Executor 2 falhou com OutOfMemoryError às 14:32"
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, avg

spark = SparkSession.builder \
    .appName("Problema3-OOM") \
    .master("local[*]") \
    .config("spark.executor.memory", "1g") \
    .config("spark.memory.fraction", "0.3") \
    .config("spark.memory.storageFraction", "0.8") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "/tmp/spark-events") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("❌ PROBLEMA 3: Out of Memory / Spill para disco")
print("   Observe no Spark UI → Stages → 'Spill (memory)' e 'Spill (disk)'")
print("   URL: http://localhost:4040\n")

df_vendas   = spark.read.parquet("/tmp/spark_aula/vendas/")
df_produtos = spark.read.parquet("/tmp/spark_aula/produtos/")
df_clientes = spark.read.parquet("/tmp/spark_aula/clientes/")

# ❌ RUIM: cache() em tudo sem necessidade e sem unpersist()
print("❌ Cacheando tudo sem controle...")
df_vendas.cache()    # 5M linhas na memória
df_produtos.cache()  # 1k linhas na memória (ok, mas desnecessário)
df_clientes.cache()  # 100k linhas na memória

# Força o cache
df_vendas.count()
df_produtos.count()
df_clientes.count()

# ❌ Cria mais DataFrames derivados e cacheia também
df_join1 = df_vendas.join(df_produtos, "produto_id").cache()
df_join1.count()  # Força cache — agora temos 5M linhas duplicadas na memória!

df_agg = df_join1.groupBy("categoria").agg(spark_sum("valor").alias("total"))
df_agg.cache()
df_agg.count()  # Mais dados na memória

# Nenhum unpersist() — memória nunca é liberada
print("❌ Memória nunca liberada — próximas operações vão fazer spill para disco")

df_final = df_agg.orderBy(col("total").desc())
df_final.show(10)

spark.stop()
print("\n✅ Rode agora: solucao3_memory.py para ver a diferença!")
