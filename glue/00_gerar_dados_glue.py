"""
SCRIPT 0 - GERADOR DE DADOS (AWS Glue 4.0)
============================================================
Roda PRIMEIRO para criar a massa de dados no S3.

Bucket hardcoded: youtubespark
Prefixo:          spark-aula/dados
============================================================
"""
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, rand, when, lit

# -- Inicializacao padrao Glue --
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

S3_BUCKET = "youtubespark"
S3_PREFIX = "spark-aula/dados"
S3_BASE   = f"s3://{S3_BUCKET}/{S3_PREFIX}"
print(f"Gravando dados em: {S3_BASE}")

# -- Tabela VENDAS - 5 milhoes de linhas com skew --
print("Gerando vendas (5M linhas, skew proposital)...")

df_vendas = spark.range(5_000_000).select(
    col("id").alias("venda_id"),
    # 80% das vendas sao PROMO_ESPECIAL -> skew proposital
    when(rand() < 0.80, lit("PROMO_ESPECIAL"))
    .when(rand() < 0.90, lit("PRODUTO_A"))
    .when(rand() < 0.95, lit("PRODUTO_B"))
    .otherwise(lit("PRODUTO_C")).alias("produto_id"),
    (rand() * 1000).alias("valor"),
    (rand() * 100).cast("int").alias("quantidade"),
    when(rand() < 0.70, lit("2024-01-01"))
    .otherwise(lit("2024-01-02")).alias("data_venda")
)

df_vendas.write.mode("overwrite").parquet(f"{S3_BASE}/vendas/")
print("   Vendas gravadas")

# -- Tabela PRODUTOS - 1000 linhas (pequena) --
print("Gerando produtos (1000 linhas)...")

produtos = [
    ("PROMO_ESPECIAL", "Promocao Especial", "Eletronicos"),
    ("PRODUTO_A",      "Produto A",         "Roupas"),
    ("PRODUTO_B",      "Produto B",         "Alimentos"),
    ("PRODUTO_C",      "Produto C",         "Casa"),
]
for i in range(996):
    produtos.append((f"PROD_{i:04d}", f"Produto {i}", "Outros"))

df_produtos = spark.createDataFrame(produtos, ["produto_id", "nome_produto", "categoria"])
df_produtos.write.mode("overwrite").parquet(f"{S3_BASE}/produtos/")
print("   Produtos gravados")

# -- Tabela CLIENTES - 100k linhas com nulos --
print("Gerando clientes (100k linhas, 30% sem regiao)...")

df_clientes = spark.range(100_000).select(
    col("id").alias("cliente_id"),
    when(rand() < 0.30, lit(None))
    .when(rand() < 0.50, lit("SP"))
    .when(rand() < 0.70, lit("RJ"))
    .otherwise(lit("MG")).alias("regiao"),
    (rand() * 50000 + 1000).alias("renda_mensal")
)

df_clientes.write.mode("overwrite").parquet(f"{S3_BASE}/clientes/")
print("   Clientes gravados")

print(f"\nDados prontos em {S3_BASE}")
print("   Proximo passo: rode o job 01_job_ruim_glue.py")

job.commit()
