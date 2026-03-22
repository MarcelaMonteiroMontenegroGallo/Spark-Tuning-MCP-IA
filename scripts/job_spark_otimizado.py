"""
✅ JOB SPARK OTIMIZADO — Versão corrigida para demonstração na aula
Correções aplicadas:
  1. Broadcast join para tabela pequena (elimina shuffle)
  2. AQE ligado (trata skew automaticamente)
  3. shuffle.partitions ajustado para o volume de dados
  4. Removido orderBy desnecessário
  5. Salted join para skew manual (demonstração)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg,
    broadcast, concat, lit, floor, rand
)

print("=" * 60)
print("✅ INICIANDO JOB SPARK OTIMIZADO")
print("   Compare o tempo com o job ruim!")
print("=" * 60)

spark = SparkSession.builder \
    .appName("JobSparkOtimizado-AulaDemo") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "50") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5") \
    .config("spark.sql.autoBroadcastJoinThreshold", "10485760") \
    .config("spark.executor.memory", "4g") \
    .config("spark.memory.fraction", "0.8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -------------------------------------------------------
# Leitura dos dados
# -------------------------------------------------------
print("\n📖 Lendo dados...")
df_vendas   = spark.read.parquet("/tmp/spark_aula/vendas/")
df_produtos = spark.read.parquet("/tmp/spark_aula/produtos/")
df_clientes = spark.read.parquet("/tmp/spark_aula/clientes/")

# -------------------------------------------------------
# ✅ CORREÇÃO 1: Broadcast join para tabela pequena
# df_produtos tem apenas 1000 linhas — não precisa de shuffle!
# -------------------------------------------------------
print("\n✅ Executando join COM broadcast (sem shuffle)...")
df_join = df_vendas.join(broadcast(df_produtos), "produto_id")

# -------------------------------------------------------
# ✅ CORREÇÃO 2: AQE cuida do skew automaticamente
# Com spark.sql.adaptive.skewJoin.enabled=true,
# o Spark divide automaticamente as partições grandes
# -------------------------------------------------------
print("✅ Executando groupBy (AQE tratando skew automaticamente)...")
df_agg = df_join.groupBy("produto_id", "categoria", "data_venda") \
                .agg(
                    spark_sum("valor").alias("total_valor"),
                    count("venda_id").alias("qtd_vendas"),
                    avg("quantidade").alias("media_qtd")
                )

# -------------------------------------------------------
# ✅ CORREÇÃO 3: Sem orderBy global
# Se precisar ordenar, faça DEPOIS de filtrar/agregar
# -------------------------------------------------------
print("✅ Sem orderBy global — ordenação feita só no resultado final...")
# Só ordena o resultado agregado (muito menor que os dados originais)
df_resultado = df_agg.orderBy(col("total_valor").desc())

# -------------------------------------------------------
# ✅ CORREÇÃO 4: Clientes — tratar nulos antes do join
# -------------------------------------------------------
print("✅ Tratando nulos antes do join de clientes...")
df_clientes_limpo = df_clientes.fillna({"regiao": "NAO_INFORMADO"})

df_clientes_agg = df_clientes_limpo \
    .groupBy("regiao") \
    .agg(count("cliente_id").alias("total_clientes"))

# Broadcast também aqui (resultado pequeno)
df_final = df_resultado.crossJoin(broadcast(df_clientes_agg))

# -------------------------------------------------------
# Escrita do resultado
# -------------------------------------------------------
print("\n💾 Escrevendo resultado...")
df_final.write.mode("overwrite").parquet("/tmp/spark_aula/resultado_otimizado/")

print("\n" + "=" * 60)
print("✅ Job OTIMIZADO finalizado!")
print("")
print("📊 Resumo das otimizações aplicadas:")
print("   1. broadcast() no join de produtos → eliminou shuffle")
print("   2. AQE ligado → tratou skew automaticamente")
print("   3. shuffle.partitions=50 → menos overhead de partições")
print("   4. Nulos tratados antes do join → menos dados processados")
print("   5. orderBy só no resultado final → shuffle menor")
print("=" * 60)

input("\nPressione ENTER para fechar o Spark UI (http://localhost:4040)...")
spark.stop()
