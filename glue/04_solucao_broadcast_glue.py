"""
SOLUCAO 1: BROADCAST JOIN - AWS Glue 4.0
Bucket hardcoded: youtubespark
"""
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, sum as spark_sum, count, broadcast

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext(); glueContext = GlueContext(sc)
spark = glueContext.spark_session; job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
spark.conf.set("spark.sql.adaptive.enabled",           "true")

S3_BUCKET = "youtubespark"
S3_PREFIX = "spark-aula/dados"
S3_BASE   = f"s3://{S3_BUCKET}/{S3_PREFIX}"

print("SOLUCAO 1: Broadcast Join")
print("   Observe no Spark UI: Stage SEM 'Shuffle Write'")

df_vendas   = spark.read.parquet(f"{S3_BASE}/vendas/")
df_produtos = spark.read.parquet(f"{S3_BASE}/produtos/")

# BOM: broadcast() - df_produtos vai para cada executor uma vez
# Shuffle: 8GB -> 0 bytes
df_resultado = df_vendas.join(broadcast(df_produtos), "produto_id") \
    .groupBy("categoria") \
    .agg(spark_sum("valor").alias("total"), count("venda_id").alias("qtd"))

df_resultado.write.mode("overwrite").parquet(
    f"s3://youtubespark/spark-aula/resultado-broadcast/"
)

# Mostra o plano - procure por BroadcastHashJoin nos logs do Glue
df_resultado.explain()

print(f"Total categorias: {df_resultado.count()}")
print("Pergunta para o MCP: 'O Stage 2 ainda tem shuffle write?'")
job.commit()
