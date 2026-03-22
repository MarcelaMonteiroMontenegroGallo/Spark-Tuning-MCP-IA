# Spark Tuning com MCP + IA

Diagnóstico e otimização de jobs Apache Spark usando IA + MCP Server, rodando no AWS Glue.

---

## O problema

Jobs Spark lentos são difíceis de diagnosticar. O Spark UI tem dezenas de abas, métricas confusas e nenhuma explicação em português. Engenheiros perdem horas tentando entender o que está errado.

Este projeto mostra como usar IA + MCP Server para diagnosticar e corrigir os 4 problemas mais comuns de performance em minutos.

---

## Os 4 problemas abordados

### 1. Shuffle excessivo — Join sem Broadcast
Quando você faz join de uma tabela grande com uma tabela pequena sem broadcast, o Spark redistribui todos os dados pela rede.

```
Sintoma:  Shuffle read alto no Spark UI (ex: 30 GB)
Causa:    autoBroadcastJoinThreshold=-1 ou tabela pequena não detectada
Solução:  broadcast(df_pequeno) ou habilitar AQE
```

### 2. Data Skew — Partições desbalanceadas
Quando uma chave concentra a maioria dos dados (ex: 80% em PROMO_ESPECIAL), uma task fica com todo o trabalho enquanto as outras ficam ociosas.

```
Sintoma:  1 task demora 10x mais que as outras no Stage
Causa:    Distribuição desigual dos dados + AQE desligado
Solução:  AQE com skewJoin habilitado ou salting manual
```

### 3. OrderBy global — Shuffle desnecessário
`orderBy()` redistribui 100% dos dados para ordenar globalmente. Na maioria dos casos não é necessário.

```
Sintoma:  Stage extra com shuffle de todos os dados
Causa:    df.orderBy() em vez de df.sortWithinPartitions()
Solução:  Usar sortWithinPartitions() ou remover se não necessário
```

### 4. CrossJoin — Produto cartesiano
Join sem condição multiplica o número de linhas: N × M. Com 50M de vendas × 4 regiões = 200M linhas e 40.000 tasks.

```
Sintoma:  Número absurdo de tasks, spill para disco, memória estourada
Causa:    crossJoin() ou join sem chave
Solução:  Definir chave de join correta ou usar agregação antes do join
```

---

## Estrutura do repositório

```
glue/
  00_gerar_dados_glue.py      # Gera massa de dados no S3 (50M linhas)
  01_job_ruim_glue.py         # Job com os 4 problemas propositais
  02_job_otimizado_glue.py    # Job corrigido com broadcast + AQE
  03_problema_shuffle_glue.py # Demonstração isolada do shuffle
  04_solucao_broadcast_glue.py
  05_problema_skew_glue.py
  06_solucao_aqe_glue.py
  07_problema_oom_glue.py
  08_solucao_memory_glue.py

scripts/
  gerar_dados.py              # Gerador local (PySpark)
  job_spark_ruim.py           # Versão local do job ruim
  job_spark_otimizado.py      # Versão local otimizada
  problema1_shuffle.py
  solucao1_broadcast.py
  problema2_skew.py
  solucao2_aqe_salting.py
  problema3_oom.py
  solucao3_memory.py
  problema4_smallfiles.py
  solucao4_coalesce.py
```

---

## Como instalar e rodar no AWS Glue

### Pré-requisitos
- Conta AWS com acesso ao Glue e S3
- Bucket S3 criado (ex: `meu-bucket`)
- Role IAM com permissão de leitura/escrita no bucket

### Passo 1 — Gerar os dados
No Glue Studio, crie um job com o script `glue/00_gerar_dados_glue.py`.

Edite a variável no topo do script:
```python
S3_BUCKET = "meu-bucket"   # substitua pelo seu bucket
```

Rode o job. Ele vai criar:
- `s3://meu-bucket/spark-aula/dados/vendas/`     — 50M linhas com skew
- `s3://meu-bucket/spark-aula/dados/produtos/`   — 1.000 linhas
- `s3://meu-bucket/spark-aula/dados/clientes/`   — 500k linhas

### Passo 2 — Rodar o job ruim
Crie um job com `glue/01_job_ruim_glue.py` e rode. Observe no Spark UI:
- Shuffle read alto
- Tasks desbalanceadas
- Spill para disco

### Passo 3 — Rodar o job otimizado
Crie um job com `glue/02_job_otimizado_glue.py` e compare os tempos.

### Passo 4 — Habilitar Spark UI logs
Nos parâmetros do job no Glue Studio, adicione:
```
--enable-spark-ui        true
--spark-event-logs-path  s3://meu-bucket/spark-logs/
```

---

## Configurar o MCP + Kiro para diagnóstico com IA

### Opção 1 — Spark History Server na EC2 (recomendado)
A AWS tem um template CloudFormation oficial. No console AWS:

1. Acesse CloudFormation → Create Stack
2. Use o template da documentação oficial: [Launching the Spark history server](https://docs.aws.amazon.com/glue/latest/dg/monitor-spark-ui-history.html)
3. Preencha:
   - Event log directory: `s3a://meu-bucket/spark-logs/`
   - Instance type: `t3.medium`
4. Após criar, copie a URL do output `SparkUiPublicUrl`

### Opção 2 — MCP Server local (requer Docker)
```bash
docker run -itd \
  -e SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=s3a://meu-bucket/spark-logs/ \
     -Dspark.hadoop.fs.s3a.access.key=SUA_KEY \
     -Dspark.hadoop.fs.s3a.secret.key=SUA_SECRET" \
  -p 18080:18080 \
  glue/sparkui:latest \
  "/opt/spark/bin/spark-class org.apache.spark.deploy.history.HistoryServer"
```

### Configurar no Kiro
Edite `~/.kiro/settings/mcp.json`:
```json
{
  "mcpServers": {
    "spark-history-mcp": {
      "type": "http",
      "url": "http://SEU-EC2-URL:18888/mcp",
      "disabled": false
    }
  }
}
```

Depois é só perguntar ao Kiro: **"Por que o job 01_job_ruim está lento?"**

---

## Ganhos observados

| Métrica            | Job Ruim     | Job Otimizado |
|--------------------|-------------|---------------|
| Shuffle read       | 30+ GB      | < 1 GB        |
| Spill para disco   | 19+ GB      | 0             |
| Tasks no stage     | 40.000      | ~50           |
| Tempo total        | 60+ min     | < 5 min       |

---

## Tecnologias

- Apache Spark 3.5 / AWS Glue 5.0
- Python 3.12
- MCP Server (mcp-apache-spark-history-server)
- Kiro IDE
- AWS S3, EC2, CloudFormation

---

## Autora

**Marcela Gallo** — Arquiteta de Dados e AI | 15+ anos | 9x AWS + 2x Databricks

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=flat&logo=linkedin&logoColor=white)](https://linkedin.com/in/marcelagallo/)
[![YouTube](https://img.shields.io/badge/YouTube-FF0000?style=flat&logo=youtube&logoColor=white)](https://youtube.com/@mammgallogallo6194/)
