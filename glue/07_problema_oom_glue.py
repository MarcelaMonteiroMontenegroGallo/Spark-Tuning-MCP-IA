"""
PROBLEMA 3: OUT OF MEMORY / SPILL - AWS Glue 4.0
Bucket hardcoded: youtubespark

ATENCAO: No Glue voce nao controla spark.executor.memory diretamente.
A memoria e definida pelo Worker Type:
  G.1X  = 1 DPU  = 16GB RAM por worker
  G.2X  = 2 DPUs = 32GB RAM por worker
  G.4X  = 4 DPUs = 64GB RAM por worker

Para forcar o problema de OOM/spill neste script,
use Worker Type G.1X com 2 workers no Glue Studio.
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

spark.conf.set("spark.memory.fraction",        "0.3")
spark.conf.set("spark.memory.storageFraction", "0.8")

S3_BUCKET = "youtubespark"
S3_PREFIX = "spark-aula/dados"
S3_BASE   = f"s3://{S3_BUCKET}/{S3_PREFIX}"

print("PROBLEMA 3: OOM / Spill para disco")
print("   Observe no Spark UI -> Stages -> 'Spill (memory)' e 'Spill (disk)'")

df_vendas   = spark.read.parquet(f"{S3_BASE}/vendas/")
df_produtos = spark.read.parquet(f"{S3_BASE}/produtos/")
df_clientes = spark.read.parquet(f"{S3_BASE}/clientes/")

# RUIM: cache() em tudo sem necessidade e sem unpersist()
print("Cacheando tudo sem controle...")
df_vendas.cache()
df_produtos.cache()
df_clientes.cache()
df_vendas.count()    # forca materializacao
df_produtos.count()
df_clientes.count()

# Cria DataFrame derivado e cacheia tambem - memoria duplicada
df_join1 = df_vendas.join(df_produtos, "produto_id").cache()
df_join1.count()  # 5M linhas duplicadas na memoria!

df_agg = df_join1.groupBy("categoria").agg(spark_sum("valor").alias("total"))
df_agg.cache()
df_agg.count()

# Nenhum unpersist() - memoria nunca liberada
print("Memoria nunca liberada - proximas operacoes vao fazer spill")

df_final = df_agg.orderBy(col("total").desc())
df_final.write.mode("overwrite").parquet(
    f"s3://youtubespark/spark-aula/resultado-oom-ruim/"
)

print("Rode agora: 08_solucao_memory_glue.py para comparar!")
job.commit()
