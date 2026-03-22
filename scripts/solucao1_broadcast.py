"""
✅ SOLUÇÃO 1: BROADCAST JOIN
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
O que muda?
  broadcast() envia a tabela pequena para TODOS os executores
  uma vez só. Cada executor faz o join localmente, sem
  precisar trocar dados pela rede.

Regra de ouro:
  Tabela < 10MB  → sempre use broadcast()
  Tabela < 200MB → considere broadcast()
  Tabela > 200MB → deixa o Spark decidir (AQE)

Resultado esperado:
  Shuffle Write: 8.2GB → 0 bytes
  Tempo: ~45min → ~8min
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, broadcast

spark = SparkSession.builder \
    .appName("Solucao1-BroadcastJoin") \
    .master("local[*]") \
    .config("spark.sql.autoBroadcastJoinThreshold", "10485760") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "/tmp/spark-events") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("✅ SOLUÇÃO 1: Broadcast Join")
print("   Observe no Spark UI: Stage sem 'Shuffle Write'")
print("   URL: http://localhost:4040\n")

df_vendas   = spark.read.parquet("/tmp/spark_aula/vendas/")
df_produtos = spark.read.parquet("/tmp/spark_aula/produtos/")

# ✅ BOM: broadcast() — df_produtos vai para cada executor uma vez só
df_resultado = df_vendas.join(broadcast(df_produtos), "produto_id") \
    .groupBy("categoria") \
    .agg(spark_sum("valor").alias("total"), count("venda_id").alias("qtd"))

df_resultado.show(10)
print(f"\nTotal de categorias: {df_resultado.count()}")

# Mostrar o plano de execução para confirmar o BroadcastHashJoin
print("\n📋 Plano de execução (procure por 'BroadcastHashJoin'):")
df_resultado.explain()

spark.stop()
print("\n🎯 Pergunta para o MCP: 'O Stage 2 ainda tem shuffle write?'")
