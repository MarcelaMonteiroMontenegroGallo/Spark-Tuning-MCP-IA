# Como subir os scripts no AWS Glue Studio

## Pré-requisitos

1. Conta AWS com permissões em Glue, S3 e IAM
2. Um bucket S3 criado (ex: `meu-bucket-aula-spark`)
3. Role IAM para o Glue com políticas:
   - `AWSGlueServiceRole`
   - `AmazonS3FullAccess` (ou acesso ao seu bucket)

---

## Passo 1 — Subir os scripts para o S3

Faça upload de todos os arquivos `.py` desta pasta para o S3:

```
s3://meu-bucket-aula-spark/scripts/
```

No console AWS → S3 → seu bucket → Create folder `scripts` → Upload.

---

## Passo 2 — Criar cada job no Glue Studio

Para cada script, repita:

1. AWS Console → **AWS Glue** → **ETL Jobs** → **Script editor**
2. Escolha **Spark** e **Upload script**
3. Selecione o arquivo `.py`
4. Configure:

### Configurações básicas (aba "Job details")

| Campo | Valor |
|-------|-------|
| Name | nome do job (ex: `aula-spark-ruim`) |
| IAM Role | sua role do Glue |
| Glue version | **Glue 4.0** (Spark 3.3) |
| Language | Python 3 |
| Worker type | **G.1X** (para jobs ruins) / **G.2X** (para otimizados) |
| Number of workers | 2 |
| Job timeout | 30 minutos |

### Habilitar Spark UI (OBRIGATÓRIO para o MCP)

Na aba **Job details** → **Advanced properties** → **Monitoring options**:

- ✅ Marcar **Spark UI**
- Em **Amazon S3 prefix for Spark event logs**:
  ```
  s3://meu-bucket-aula-spark/spark-logs/
  ```

### Job parameters (aba "Job details" → "Job parameters")

Clique em **Add new parameter** e adicione:

| Key | Value |
|-----|-------|
| `--S3_BUCKET` | `meu-bucket-aula-spark` |
| `--S3_PREFIX` | `spark-aula/dados` |

### Para o job RUIM — adicionar parâmetros extras de configuração

| Key | Value |
|-----|-------|
| `--conf` | `spark.sql.shuffle.partitions=200` |

---

## Passo 3 — Ordem de execução

```
1. 00_gerar_dados_glue.py     ← roda primeiro, gera os dados no S3
2. 01_job_ruim_glue.py        ← job com todos os problemas
3. 02_job_otimizado_glue.py   ← job corrigido (compare o tempo!)

Para a demo problema a problema:
4. 03_problema_shuffle_glue.py  → 04_solucao_broadcast_glue.py
5. 05_problema_skew_glue.py     → 06_solucao_aqe_glue.py
6. 07_problema_oom_glue.py      → 08_solucao_memory_glue.py
```

---

## Passo 4 — Ver o Spark UI

Após rodar um job:

1. Glue Studio → **Jobs** → clique no job
2. Aba **Runs** → clique na execução
3. Clique em **Spark UI** (abre o History Server)
4. Explore: **Stages**, **Tasks**, **SQL**, **Executors**

---

## Passo 5 — Conectar o MCP ao Spark History Server do Glue

O Glue expõe o Spark History Server via URL temporária.
Para conectar o MCP, você tem duas opções:

### Opção A — Docker local apontando para os logs do S3

```bash
docker run -p 18080:18080 \
  -e AWS_ACCESS_KEY_ID=SUA_KEY \
  -e AWS_SECRET_ACCESS_KEY=SUA_SECRET \
  -e AWS_DEFAULT_REGION=us-east-1 \
  apache/spark:3.3.0 \
  /opt/spark/bin/spark-class \
  org.apache.spark.deploy.history.HistoryServer \
  --properties-file /dev/stdin <<EOF
spark.history.fs.logDirectory=s3a://meu-bucket-aula-spark/spark-logs/
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
EOF
```

### Opção B — EC2 com o MCP Server (recomendado para a aula)

```bash
# Na EC2 (Amazon Linux 2, t3.medium)
git clone https://github.com/aws/spark-history-server-mcp
cd spark-history-server-mcp
pip install -r requirements.txt

# Configurar apontando para o S3 do Glue
export SPARK_HISTORY_SERVER_URL=http://localhost:18080
task start-spark-bg   # sobe o History Server
task start-mcp-bg     # sobe o MCP Server
```

### Configurar o Kiro para usar o MCP

Edite `~/.kiro/settings/mcp.json`:

```json
{
  "mcpServers": {
    "spark-history-server": {
      "command": "python",
      "args": ["-m", "spark_history_server_mcp.server"],
      "env": {
        "SPARK_HISTORY_SERVER_URL": "http://localhost:18080"
      },
      "disabled": false,
      "autoApprove": []
    }
  }
}
```

---

## Perguntas para fazer ao Kiro durante a aula

Após rodar o job ruim, pergunte:

```
Liste os jobs no Spark History Server
```
```
Por que o job aula-spark-ruim está lento?
```
```
Qual stage tem mais shuffle write?
```
```
Tem algum executor sobrecarregado?
```
```
Como otimizar o job aula-spark-ruim?
```

Após rodar o job otimizado:
```
Compare o job aula-spark-ruim com o aula-spark-otimizado
```
