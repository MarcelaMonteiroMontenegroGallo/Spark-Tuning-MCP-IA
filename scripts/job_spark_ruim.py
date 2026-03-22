"""
❌ JOB SPARK RUIM — Para demonstração na aula
Contém propositalmente:
  1. Shuffle excessivo (200 partições desnecessárias)
  2. Data Skew (join sem tratamento de skew)
  3. Configurações ruins (AQE desligado)
  4. orderBy() desnecessário causando shuffle global
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, avg

print("=" * 60)
print("❌ INICIANDO JOB SPARK RUIM (para demonstração)")
print("   Observe os problemas no Spark UI: http://localhost:4040")
print("=" * 60)

spark = SparkSession.builder \
    .appName("JobSparkRuim-AulaDemo") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.sql.adaptive.skewJoin.enabled", "false") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -------------------------------------------------------
# Leitura dos dados
# -------------------------------------------------------
print("\n📖 Lendo dados...")
df_vendas   = spark.read.parquet("/tmp/spark_aula/vendas/")
df_produtos = spark.read.parquet("/tmp/spark_aula/produtos/")
df_clientes = spark.read.parquet("/tmp/spark_aula/clientes/")

print(f"   Vendas:   {df_vendas.count():,} linhas")
print(f"   Produtos: {df_produtos.count():,} linhas")
print(f"   Clientes: {df_clientes.count():,} linhas")

# -------------------------------------------------------
# ❌ PROBLEMA 1: Join sem broadcast (shuffle desnecessário)
# df_produtos é pequeno, deveria usar broadcast
# -------------------------------------------------------
print("\n❌ Executando join SEM broadcast (problema de shuffle)...")
df_join = df_vendas.join(df_produtos, "produto_id")  # Shuffle desnecessário!

# -------------------------------------------------------
# ❌ PROBLEMA 2: groupBy com skew (PROMO_ESPECIAL tem 80% dos dados)
# Uma partição vai ter 4 milhões de linhas, as outras vão ter poucas
# -------------------------------------------------------
print("❌ Executando groupBy com skew...")
df_agg = df_join.groupBy("produto_id", "categoria", "data_venda") \
                .agg(
                    spark_sum("valor").alias("total_valor"),
                    count("venda_id").alias("qtd_vendas"),
                    avg("quantidade").alias("media_qtd")
                )

# -------------------------------------------------------
# ❌ PROBLEMA 3: orderBy global (força shuffle em TODAS as partições)
# -------------------------------------------------------
print("❌ Executando orderBy global (shuffle desnecessário)...")
df_ordenado = df_agg.orderBy(col("total_valor").desc())

# -------------------------------------------------------
# ❌ PROBLEMA 4: Segundo join sem otimização (mais shuffle)
# -------------------------------------------------------
print("❌ Executando segundo join sem otimização...")
df_final = df_ordenado.join(
    df_clientes.groupBy("regiao").agg(count("cliente_id").alias("total_clientes")),
    df_ordenado["data_venda"] == df_clientes.groupBy("regiao")
        .agg(count("cliente_id").alias("total_clientes"))["regiao"],
    "left"
)

# -------------------------------------------------------
# Escrita do resultado
# -------------------------------------------------------
print("\n💾 Escrevendo resultado...")
df_final.write.mode("overwrite").parquet("/tmp/spark_aula/resultado_ruim/")

print("\n" + "=" * 60)
print("✅ Job RUIM finalizado!")
print("   Verifique o Spark History Server para ver os problemas")
print("   Agora pergunte ao Kiro: 'Por que esse job está lento?'")
print("=" * 60)

input("\nPressione ENTER para fechar o Spark UI (http://localhost:4040)...")
spark.stop()
