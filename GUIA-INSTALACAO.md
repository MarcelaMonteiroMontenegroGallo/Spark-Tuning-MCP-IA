# 🛠️ Guia de Instalação — Spark History Server MCP
## Para iniciantes — passo a passo completo

---

## ✅ O que você vai precisar instalar

| Ferramenta | Para que serve | Já tem? |
|-----------|---------------|---------|
| Python 3.10+ | Rodar os scripts | Verificar |
| Git | Baixar o repositório | Verificar |
| Docker Desktop | Rodar o Spark localmente | Verificar |
| Java 11+ | O Spark precisa do Java | Verificar |
| Kiro ou Claude Desktop | A IA que vai analisar | Verificar |

---

## PASSO 1 — Verificar o que já está instalado

Abra o terminal (CMD no Windows, Terminal no Mac) e rode:

```bash
python --version
# Precisa aparecer: Python 3.10 ou maior

git --version
# Precisa aparecer: git version 2.x

docker --version
# Precisa aparecer: Docker version 24.x ou maior

java -version
# Precisa aparecer: openjdk version "11" ou maior
```

Se algum não aparecer, veja a seção de instalação abaixo.

---

## PASSO 2 — Instalar o que estiver faltando

### Instalar Python (se não tiver)
1. Acesse: https://www.python.org/downloads/
2. Baixe a versão 3.11 ou 3.12
3. Na instalação, MARQUE a opção "Add Python to PATH"
4. Clique em Install Now

### Instalar Git (se não tiver)
1. Acesse: https://git-scm.com/downloads
2. Baixe para seu sistema operacional
3. Instale com as opções padrão

### Instalar Docker Desktop (se não tiver)
1. Acesse: https://www.docker.com/products/docker-desktop/
2. Baixe para seu sistema operacional
3. Instale e abra o Docker Desktop
4. Aguarde o Docker iniciar (ícone na barra de tarefas)

### Instalar Java 11 (se não tiver)
1. Acesse: https://adoptium.net/
2. Baixe o "Temurin 11 (LTS)"
3. Instale com as opções padrão

---

## PASSO 3 — Clonar o repositório do MCP

```bash
# No terminal, navegue para onde quer salvar o projeto
# Exemplo: sua área de trabalho
cd Desktop

# Clone o repositório
git clone https://github.com/kubeflow/mcp-apache-spark-history-server.git

# Entre na pasta
cd mcp-apache-spark-history-server
```

---

## PASSO 4 — Instalar as dependências Python

```bash
# Criar ambiente virtual (boa prática — isola as dependências)
python -m venv venv

# Ativar o ambiente virtual
# No Windows:
venv\Scripts\activate

# No Mac/Linux:
source venv/bin/activate

# Instalar as dependências
pip install -r requirements.txt
```

Você vai ver várias linhas de instalação. Aguarde terminar.

---

## PASSO 5 — Instalar o Task (ferramenta de automação)

O projeto usa uma ferramenta chamada "Task" para facilitar os comandos.

### No Mac:
```bash
brew install go-task
```

### No Windows:
```bash
# Usando Chocolatey (se tiver):
choco install go-task

# Ou baixe direto em: https://taskfile.dev/installation/
# Baixe o .exe e coloque em C:\Windows\System32\
```

### No Linux:
```bash
sh -c "$(curl --location https://taskfile.dev/install.sh)" -- -d -b ~/.local/bin
```

---

## PASSO 6 — Iniciar o ambiente completo

```bash
# Dentro da pasta mcp-apache-spark-history-server

# 1. Instalar dependências do projeto
task install

# 2. Iniciar o Spark History Server com dados de exemplo
task start-spark-bg

# 3. Iniciar o MCP Server
task start-mcp-bg

# 4. (Opcional) Iniciar o MCP Inspector para testar
task start-inspector-bg
```

Aguarde alguns segundos. Você deve ver mensagens de sucesso.

**Verificar se está rodando:**
- Spark History Server: http://localhost:18080
- MCP Inspector: http://localhost:5173

---

## PASSO 7 — Configurar o Kiro para usar o MCP

1. Abra o Kiro
2. Vá em **Settings** → **MCP Servers**
3. Abra o arquivo `mcp.json` (ou crie em `~/.kiro/settings/mcp.json`)
4. Adicione a configuração abaixo:

```json
{
  "mcpServers": {
    "spark-history-server": {
      "command": "python",
      "args": [
        "-m", "spark_history_server_mcp.server"
      ],
      "env": {
        "SPARK_HISTORY_SERVER_URL": "http://localhost:18080"
      },
      "disabled": false,
      "autoApprove": []
    }
  }
}
```

5. Salve o arquivo
6. No Kiro, clique em **Reconnect** no painel MCP Servers

---

## PASSO 8 — Testar se está funcionando

No chat do Kiro, digite:

```
Liste todos os jobs Spark que estão no History Server
```

Se aparecer uma lista de aplicações, está funcionando! 🎉

---

## PASSO 9 — Configurar para AWS Glue (para a demo)

Para usar com jobs reais do AWS Glue, você precisa:

### 9.1 — Habilitar Spark UI no Glue

1. Acesse o **AWS Console** → **AWS Glue** → **Jobs**
2. Clique no seu job → **Edit**
3. Vá em **Job details** → **Advanced properties**
4. Em **Monitoring options**, ative:
   - ✅ **Spark UI**
   - ✅ **Continuous logging**
5. Em **Amazon S3 prefix for Spark event logs**, coloque:
   ```
   s3://SEU-BUCKET/spark-logs/
   ```
6. Salve e rode o job

### 9.2 — Subir o Spark History Server apontando para o S3 do Glue

Crie um arquivo `docker-compose-glue.yml`:

```yaml
version: '3'
services:
  spark-history-server:
    image: apache/spark:3.5.0
    ports:
      - "18080:18080"
    environment:
      - AWS_ACCESS_KEY_ID=SUA_ACCESS_KEY
      - AWS_SECRET_ACCESS_KEY=SUA_SECRET_KEY
      - AWS_DEFAULT_REGION=us-east-1
    command: >
      /opt/spark/bin/spark-class
      org.apache.spark.deploy.history.HistoryServer
      --properties-file /opt/spark/conf/spark-defaults.conf
    volumes:
      - ./spark-defaults.conf:/opt/spark/conf/spark-defaults.conf
```

Crie o arquivo `spark-defaults.conf`:

```properties
spark.history.fs.logDirectory=s3a://SEU-BUCKET/spark-logs/
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain
```

Rode:
```bash
docker-compose -f docker-compose-glue.yml up -d
```

### 9.3 — Atualizar o MCP para apontar para o Glue

No `mcp.json`, mude a URL:
```json
"SPARK_HISTORY_SERVER_URL": "http://localhost:18080"
```

Agora o MCP vai analisar os jobs reais do seu Glue!

---

## 🛑 Parar tudo quando terminar

```bash
# Dentro da pasta do projeto
task stop-all

# Ou manualmente:
docker-compose down
```

---

## ❓ Problemas comuns

### "Docker não está rodando"
→ Abra o Docker Desktop e aguarde o ícone ficar verde

### "Port 18080 already in use"
→ Algum processo está usando a porta. Rode:
```bash
# Mac/Linux:
lsof -i :18080 | kill -9 PID

# Windows:
netstat -ano | findstr :18080
taskkill /PID NUMERO_DO_PID /F
```

### "Python não encontrado"
→ Feche e abra o terminal após instalar o Python

### "task: command not found"
→ Reinstale o Task ou use os comandos diretamente sem o `task`

---

## 📞 Precisa de ajuda?

Deixa nos comentários do vídeo ou abre uma issue no GitHub do projeto:
https://github.com/kubeflow/mcp-apache-spark-history-server/issues
