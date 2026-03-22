"""
SOLUCAO 3: GESTAO CORRETA DE MEMORIA - AWS Glue 4.0
Bucket hardcoded: youtubespark
"""
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, sum as spark_sum, count, broadcast
from pyspark.storagelevel import StorageLevel

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext(); glueContext = GlueContext(sc)
spark = glueContext.spark_session; job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.memory.fraction",        "0.8")
spark.conf.set("spark.memory.storageFraction", "0.3")

S3_BUCKET = "youtubespark"
S3_PREFIX = "spark-aula/dados"
S3_BASE   = f"s3://{S3_BUCKET}/{S3_PREFIX}"

print("SOLUCAO 3: Gestao Correta de Memoria")

df_vendas   = spark.read.parquet(f"{S3_BASE}/vendas/")
df_produtos = spark.read.parquet(f"{S3_BASE}/produtos/")
df_clientes = spark.read.parquet(f"{S3_BASE}/clientes/")

# Cacheia APENAS o que sera reutilizado multiplas vezes
# df_vendas e usado em 3 analises -> vale cachear
# MEMORY_AND_DISK: usa disco se nao couber na memoria (seguro no Glue)
df_vendas.persist(StorageLevel.MEMORY_AND_DISK)
df_vendas.count()  # materializa
print("df_vendas cacheado com MEMORY_AND_DISK")

# Analise 1 - usa o cache
df_por_categoria = df_vendas \
    .join(broadcast(df_produtos), "produto_id") \
    .groupBy("categoria") \
    .agg(spark_sum("valor").alias("total"))
df_por_categoria.write.mode("overwrite").parquet(
    f"s3://youtubespark/spark-aula/resultado-por-categoria/"
)
print("Analise 1 concluida")

# Analise 2 - reutiliza o cache (aqui vale o cache!)
df_por_data = df_vendas \
    .groupBy("data_venda") \
    .agg(count("venda_id").alias("qtd"))
df_por_data.write.mode("overwrite").parquet(
    f"s3://youtubespark/spark-aula/resultado-por-data/"
)
print("Analise 2 concluida")

# Analise 3 - reutiliza o cache novamente
df_top = df_vendas \
    .join(broadcast(df_produtos), "produto_id") \
    .groupBy("produto_id") \
    .agg(spark_sum("valor").alias("total")) \
    .orderBy(col("total").desc())
df_top.write.mode("overwrite").parquet(
    f"s3://youtubespark/spark-aula/resultado-top-produtos/"
)
print("Analise 3 concluida")

# IMPORTANTE: libera memoria quando nao precisa mais
df_vendas.unpersist()
print("Cache liberado com unpersist()")

print("Pergunta para o MCP: 'Quanto de spill para disco teve nesse job?'")
job.commit()
