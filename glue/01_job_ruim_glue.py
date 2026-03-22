"""
❌ JOB RUIM — AWS Glue 4.0
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Contém propositalmente 4 problemas de performance:
  1. Shuffle excessivo  — join sem broadcast
  2. Data Skew          — AQE desligado
  3. orderBy global     — shuffle desnecessário
  4. Configurações ruins — shuffle.partitions=200

Parâmetros do job:
  --S3_BUCKET  → nome do bucket
  --S3_PREFIX  → prefixo dos dados (ex: spark-aula/dados)

Configurações extras no Glue Studio (Job parameters):
  --conf  spark.sql.shuffle.partitions=200
  --conf  spark.sql.adaptive.enabled=false
  --conf  spark.sql.autoBroadcastJoinThreshold=-1

Após rodar: abra o Spark UI no Glue Studio → Runs
e pergunte ao Kiro: "Por que esse job está lento?"
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, sum as spark_sum, count, avg

# ── Inicialização Glue ─────────────────────────────────
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc          = SparkContext()
glueContext = GlueContext(sc)
spark       = glueContext.spark_session
job         = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ❌ Configurações ruins aplicadas via Job parameters no Glue Studio:
#    spark.sql.shuffle.partitions=200
#    spark.sql.adaptive.enabled=false
#    spark.sql.autoBroadcastJoinThreshold=-1
# Aqui forçamos via código também para garantir:
spark.conf.set("spark.sql.shuffle.partitions",        "200")
spark.conf.set("spark.sql.adaptive.enabled",          "false")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold","-1")

S3_BUCKET = "youtubespark"
S3_PREFIX = "spark-aula/dados"
S3_BASE   = f"s3://{S3_BUCKET}/{S3_PREFIX}"
S3_OUT    = f"s3://{S3_BUCKET}/spark-aula/resultado-ruim/"

print("=" * 60)
print("❌ JOB RUIM — iniciando (observe o Spark UI no Glue)")
print(f"   Lendo de: {S3_BASE}")
print("=" * 60)

# ── Leitura ────────────────────────────────────────────
print("\n📖 Lendo dados do S3...")
df_vendas   = spark.read.parquet(f"{S3_BASE}/vendas/")
df_produtos = spark.read.parquet(f"{S3_BASE}/produtos/")
df_clientes = spark.read.parquet(f"{S3_BASE}/clientes/")

print(f"   Vendas:   {df_vendas.count():,} linhas")
print(f"   Produtos: {df_produtos.count():,} linhas")
print(f"   Clientes: {df_clientes.count():,} linhas")

# ── ❌ PROBLEMA 1: Join sem broadcast ──────────────────
# df_produtos tem 1000 linhas mas vai pelo shuffle (8GB na rede)
print("\n❌ PROBLEMA 1: join SEM broadcast...")
df_join = df_vendas.join(df_produtos, "produto_id")

# ── ❌ PROBLEMA 2: groupBy com skew ────────────────────
# PROMO_ESPECIAL tem 80% dos dados → 1 partição gigante
print("❌ PROBLEMA 2: groupBy com skew (AQE desligado)...")
df_agg = df_join.groupBy("produto_id", "categoria", "data_venda") \
                .agg(
                    spark_sum("valor").alias("total_valor"),
                    count("venda_id").alias("qtd_vendas"),
                    avg("quantidade").alias("media_qtd")
                )

# ── ❌ PROBLEMA 3: orderBy global ──────────────────────
# Força redistribuição de TODOS os dados entre executores
print("❌ PROBLEMA 3: orderBy global (shuffle em tudo)...")
df_ordenado = df_agg.orderBy(col("total_valor").desc())

# ── ❌ PROBLEMA 4: join de clientes sem otimização ─────
print("❌ PROBLEMA 4: segundo join sem tratar nulos...")
df_clientes_agg = df_clientes \
    .groupBy("regiao") \
    .agg(count("cliente_id").alias("total_clientes"))

df_final = df_ordenado.join(df_clientes_agg, "data_venda", "left")

# ── Escrita ────────────────────────────────────────────
print(f"\n💾 Escrevendo resultado em {S3_OUT}...")
df_final.write.mode("overwrite").parquet(S3_OUT)

print("\n" + "=" * 60)
print("✅ Job RUIM finalizado!")
print("   Agora abra o Glue Studio → Runs → Spark UI")
print("   Pergunte ao Kiro: 'Por que esse job está lento?'")
print("=" * 60)

job.commit()
