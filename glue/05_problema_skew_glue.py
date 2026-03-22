"""
PROBLEMA 2: DATA SKEW - AWS Glue 4.0
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

spark.conf.set("spark.sql.adaptive.enabled",          "false")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions",        "10")

S3_BUCKET = "youtubespark"
S3_PREFIX = "spark-aula/dados"
S3_BASE   = f"s3://{S3_BUCKET}/{S3_PREFIX}"

print("PROBLEMA 2: Data Skew")
print("   Observe no Spark UI -> Stages -> Tasks")
print("   Uma task vai demorar MUITO mais que as outras")

df_vendas = spark.read.parquet(f"{S3_BASE}/vendas/")

# Mostra o skew nos logs do Glue
print("Distribuicao por categoria:")
df_vendas.groupBy("categoria").count().orderBy(col("count").desc()).show()

# RUIM: PROMO_ESPECIAL (80% dos dados) vai para 1 particao
df_resultado = df_vendas \
    .groupBy("categoria", "produto_id") \
    .agg(spark_sum("valor").alias("total_valor"), count("venda_id").alias("qtd_vendas"))

df_resultado.write.mode("overwrite").parquet(
    f"s3://youtubespark/spark-aula/resultado-skew-ruim/"
)

print(f"Total grupos: {df_resultado.count()}")
print("Rode agora: 06_solucao_aqe_glue.py para comparar!")
job.commit()
