"""
✅ SOLUÇÃO 3: GESTÃO CORRETA DE MEMÓRIA
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Regras de ouro para cache:
  1. Só cache DataFrames usados MAIS DE UMA VEZ
  2. Sempre chame unpersist() quando não precisar mais
  3. Use StorageLevel correto para o seu caso
  4. Ajuste memory.fraction para dar mais espaço à execução

StorageLevels:
  MEMORY_ONLY       → mais rápido, pode falhar se não couber
  MEMORY_AND_DISK   → mais seguro, usa disco se necessário
  DISK_ONLY         → lento mas não causa OOM
  OFF_HEAP          → fora do heap Java, evita GC pressure

Resultado esperado:
  Spill para disco: 4.2GB → 0
  GC time: 45% → 3%
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, broadcast
from pyspark.storagelevel import StorageLevel

spark = SparkSession.builder \
    .appName("Solucao3-MemoriaCorreta") \
    .master("local[*]") \
    .config("spark.executor.memory", "4g") \
    .config("spark.memory.fraction", "0.8") \
    .config("spark.memory.storageFraction", "0.3") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "/tmp/spark-events") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("✅ SOLUÇÃO 3: Gestão Correta de Memória\n")

df_vendas   = spark.read.parquet("/tmp/spark_aula/vendas/")
df_produtos = spark.read.parquet("/tmp/spark_aula/produtos/")
df_clientes = spark.read.parquet("/tmp/spark_aula/clientes/")

# ✅ BOM: só cacheia o que vai ser reutilizado múltiplas vezes
# df_vendas é usado em 3 análises diferentes — vale cachear
df_vendas.persist(StorageLevel.MEMORY_AND_DISK)
df_vendas.count()  # materializa o cache
print("✅ df_vendas cacheado com MEMORY_AND_DISK (seguro)")

# ✅ Análise 1 — usa o cache
df_por_categoria = df_vendas \
    .join(broadcast(df_produtos), "produto_id") \
    .groupBy("categoria") \
    .agg(spark_sum("valor").alias("total"))
df_por_categoria.show(5)

# ✅ Análise 2 — reutiliza o cache (aqui vale o cache!)
df_por_data = df_vendas \
    .groupBy("data_venda") \
    .agg(count("venda_id").alias("qtd"))
df_por_data.show(5)

# ✅ Análise 3 — reutiliza o cache novamente
df_top_produtos = df_vendas \
    .join(broadcast(df_produtos), "produto_id") \
    .groupBy("produto_id", "nome_produto") \
    .agg(spark_sum("valor").alias("total")) \
    .orderBy(col("total").desc())
df_top_produtos.show(5)

# ✅ IMPORTANTE: libera a memória quando não precisa mais
df_vendas.unpersist()
print("\n✅ Cache liberado com unpersist() — memória disponível para próximas operações")

spark.stop()
print("\n🎯 Pergunta para o MCP: 'Quanto de spill para disco teve nesse job?'")
