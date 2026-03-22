"""
PROBLEMA 1: SHUFFLE EXCESSIVO - AWS Glue 4.0
Bucket hardcoded: youtubespark
"""
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, sum as spark_sum, count

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext(); glueContext = GlueContext(sc)
spark = glueContext.spark_session; job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.adaptive.enabled",           "false")

S3_BUCKET = "youtubespark"
S3_PREFIX = "spark-aula/dados"
S3_BASE   = f"s3://{S3_BUCKET}/{S3_PREFIX}"

print("PROBLEMA 1: Shuffle Excessivo")
print("   Observe no Spark UI: Stage com alto 'Shuffle Write'")

df_vendas   = spark.read.parquet(f"{S3_BASE}/vendas/")
df_produtos = spark.read.parquet(f"{S3_BASE}/produtos/")

# RUIM: df_produtos (1000 linhas) vai pelo shuffle - 8GB na rede
df_resultado = df_vendas.join(df_produtos, "produto_id") \
    .groupBy("categoria") \
    .agg(spark_sum("valor").alias("total"), count("venda_id").alias("qtd"))

df_resultado.write.mode("overwrite").parquet(
    f"s3://youtubespark/spark-aula/resultado-shuffle-ruim/"
)

print(f"Total categorias: {df_resultado.count()}")
print("Rode agora: 04_solucao_broadcast_glue.py para comparar!")
job.commit()
