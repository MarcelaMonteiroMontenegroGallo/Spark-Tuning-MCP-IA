"""
Script para gerar massa de dados com problemas propositais de Spark.
Roda localmente com PySpark.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rand, when, lit
import random

spark = SparkSession.builder \
    .appName("GerarDados-AulaSparkTuning") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("🔧 Gerando massa de dados para a aula...")

# -------------------------------------------------------
# Tabela de VENDAS — 5 milhões de linhas
# Com skew proposital: produto_id = 'PROMO_ESPECIAL' tem 80% dos dados
# -------------------------------------------------------
print("📦 Gerando tabela de vendas (5M linhas com skew)...")

n_linhas = 5_000_000

df_vendas = spark.range(n_linhas).select(
    col("id").alias("venda_id"),
    # 80% das vendas são do produto PROMO_ESPECIAL (skew proposital)
    when(rand() < 0.80, lit("PROMO_ESPECIAL"))
    .when(rand() < 0.90, lit("PRODUTO_A"))
    .when(rand() < 0.95, lit("PRODUTO_B"))
    .otherwise(lit("PRODUTO_C")).alias("produto_id"),
    (rand() * 1000).alias("valor"),
    (rand() * 100).cast("int").alias("quantidade"),
    # Datas concentradas em 2024-01-01 (mais skew)
    when(rand() < 0.70, lit("2024-01-01"))
    .otherwise(lit("2024-01-02")).alias("data_venda")
)

df_vendas.write.mode("overwrite").parquet("/tmp/spark_aula/vendas/")
print(f"   ✅ {n_linhas:,} linhas geradas")

# -------------------------------------------------------
# Tabela de PRODUTOS — 1000 linhas (pequena)
# -------------------------------------------------------
print("📦 Gerando tabela de produtos (1000 linhas)...")

produtos = [
    ("PROMO_ESPECIAL", "Promoção Especial", "Eletrônicos"),
    ("PRODUTO_A", "Produto A", "Roupas"),
    ("PRODUTO_B", "Produto B", "Alimentos"),
    ("PRODUTO_C", "Produto C", "Casa"),
]
# Adiciona mais produtos para simular tabela maior
for i in range(996):
    produtos.append((f"PROD_{i:04d}", f"Produto {i}", "Outros"))

df_produtos = spark.createDataFrame(
    produtos, ["produto_id", "nome_produto", "categoria"]
)

df_produtos.write.mode("overwrite").parquet("/tmp/spark_aula/produtos/")
print(f"   ✅ {len(produtos):,} produtos gerados")

# -------------------------------------------------------
# Tabela de CLIENTES — 100k linhas
# Com muitos nulos na chave de join (mais skew)
# -------------------------------------------------------
print("📦 Gerando tabela de clientes (100k linhas com nulos)...")

df_clientes = spark.range(100_000).select(
    col("id").alias("cliente_id"),
    # 30% dos clientes sem região (nulo — causa skew no join)
    when(rand() < 0.30, lit(None))
    .when(rand() < 0.50, lit("SP"))
    .when(rand() < 0.70, lit("RJ"))
    .otherwise(lit("MG")).alias("regiao"),
    (rand() * 50000 + 1000).alias("renda_mensal")
)

df_clientes.write.mode("overwrite").parquet("/tmp/spark_aula/clientes/")
print("   ✅ 100.000 clientes gerados")

print("\n✅ Dados gerados com sucesso em /tmp/spark_aula/")
print("   Agora rode: python job_spark_ruim.py")

spark.stop()
