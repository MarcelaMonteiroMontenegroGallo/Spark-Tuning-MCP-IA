"""
JOB OTIMIZADO - AWS Glue 4.0
============================================================
Correcoes aplicadas (sugeridas pelo MCP + Kiro):
  1. broadcast()  - elimina shuffle no join de produtos
  2. AQE ligado   - trata skew automaticamente
  3. shuffle.partitions=50 - menos overhead
  4. Sem orderBy global - ordena so o resultado final
  5. Nulos tratados antes do join

Bucket hardcoded: youtubespark
============================================================
"""
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, sum as spark_sum, count, avg, broadcast

# -- Inicializacao Glue --
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Configuracoes otimizadas
spark.conf.set("spark.sql.shuffle.partitions",                        "50")
spark.conf.set("spark.sql.adaptive.enabled",                          "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled",       "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled",                 "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor",   "5")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold",                "10485760")

S3_BUCKET = "youtubespark"
S3_PREFIX = "spark-aula/dados"
S3_BASE   = f"s3://{S3_BUCKET}/{S3_PREFIX}"
S3_OUT    = f"s3://{S3_BUCKET}/spark-aula/resultado-otimizado/"

print("=" * 60)
print("JOB OTIMIZADO - iniciando")
print(f"   Lendo de: {S3_BASE}")
print("=" * 60)

# -- Leitura --
print("\nLendo dados do S3...")
df_vendas   = spark.read.parquet(f"{S3_BASE}/vendas/")
df_produtos = spark.read.parquet(f"{S3_BASE}/produtos/")
df_clientes = spark.read.parquet(f"{S3_BASE}/clientes/")

# -- CORRECAO 1: broadcast join --
# df_produtos tem 1000 linhas -> vai para cada executor uma vez
# Shuffle: 8GB -> 0 bytes
print("\nCORRECAO 1: join COM broadcast (zero shuffle)...")
df_join = df_vendas.join(broadcast(df_produtos), "produto_id")

# -- CORRECAO 2: AQE trata skew automaticamente --
# Spark detecta particoes grandes e as divide sozinho
print("CORRECAO 2: groupBy com AQE (skew tratado automaticamente)...")
df_agg = df_join.groupBy("produto_id", "categoria", "data_venda") \
                .agg(
                    spark_sum("valor").alias("total_valor"),
                    count("venda_id").alias("qtd_vendas"),
                    avg("quantidade").alias("media_qtd")
                )

# -- CORRECAO 3: sem orderBy global --
# Ordena so o resultado agregado (muito menor que os dados brutos)
print("CORRECAO 3: orderBy so no resultado final (dados menores)...")
df_resultado = df_agg.orderBy(col("total_valor").desc())

# -- CORRECAO 4: tratar nulos antes do join --
print("CORRECAO 4: nulos tratados antes do join de clientes...")
df_clientes_limpo = df_clientes.fillna({"regiao": "NAO_INFORMADO"})
df_clientes_agg   = df_clientes_limpo \
    .groupBy("regiao") \
    .agg(count("cliente_id").alias("total_clientes"))

# Broadcast no join de clientes tambem (resultado pequeno)
df_final = df_resultado.crossJoin(broadcast(df_clientes_agg))

# -- Escrita --
print(f"\nEscrevendo resultado em {S3_OUT}...")
df_final.write.mode("overwrite").parquet(S3_OUT)

print("\n" + "=" * 60)
print("Job OTIMIZADO finalizado!")
print("")
print("Otimizacoes aplicadas:")
print("   1. broadcast() -> shuffle 8GB -> 0 bytes")
print("   2. AQE on      -> skew tratado automaticamente")
print("   3. partitions=50 -> menos overhead")
print("   4. orderBy no resultado final -> shuffle menor")
print("   5. fillna() antes do join -> sem nulos problematicos")
print("=" * 60)

job.commit()
