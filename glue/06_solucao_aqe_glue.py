"""
SOLUCAO 2: AQE + SALTING - AWS Glue 4.0
Bucket hardcoded: youtubespark
"""
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, sum as spark_sum, count, concat, lit, floor, rand, expr

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext(); glueContext = GlueContext(sc)
spark = glueContext.spark_session; job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.adaptive.enabled",                          "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled",                 "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor",   "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "268435456")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled",       "true")
spark.conf.set("spark.sql.shuffle.partitions",                        "50")

S3_BUCKET = "youtubespark"
S3_PREFIX = "spark-aula/dados"
S3_BASE   = f"s3://{S3_BUCKET}/{S3_PREFIX}"

print("SOLUCAO 2A: AQE (Adaptive Query Execution)")
df_vendas = spark.read.parquet(f"{S3_BASE}/vendas/")

# OPCAO A: AQE - so ligar, Spark cuida do resto
df_aqe = df_vendas \
    .groupBy("categoria", "produto_id") \
    .agg(spark_sum("valor").alias("total_valor"), count("venda_id").alias("qtd_vendas"))

df_aqe.write.mode("overwrite").parquet(
    f"s3://youtubespark/spark-aula/resultado-aqe/"
)
print(f"AQE - total grupos: {df_aqe.count()}")

print("\nSOLUCAO 2B: Salting Manual (para casos extremos)")
SALT = 10  # divide em 10 particoes

# Adiciona sufixo aleatorio na chave skewed
df_salted = df_vendas.withColumn(
    "categoria_salt",
    concat(col("categoria"), lit("_"), (floor(rand() * SALT)).cast("string"))
)

# Agrega com chave saltada
df_agg_salt = df_salted \
    .groupBy("categoria_salt", "produto_id") \
    .agg(spark_sum("valor").alias("total_valor"), count("venda_id").alias("qtd_vendas"))

# Remove o salt e agrega novamente
df_salt_final = df_agg_salt \
    .withColumn("categoria", expr("split(categoria_salt, '_')[0]")) \
    .groupBy("categoria", "produto_id") \
    .agg(spark_sum("total_valor").alias("total_valor"), spark_sum("qtd_vendas").alias("qtd_vendas"))

df_salt_final.write.mode("overwrite").parquet(
    f"s3://youtubespark/spark-aula/resultado-salting/"
)
print(f"Salting - total grupos: {df_salt_final.count()}")
print("Pergunta para o MCP: 'As tasks estao mais balanceadas agora?'")
job.commit()
