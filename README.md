# Atividade 2 — Pipeline de Dados com Amazon S3 e Amazon Athena

**Disciplina:** DATA LAKE, LAKEHOUSES E DATAS MESHES
**Professor:** Yuri Menezes
**Aluna:** Adriana Ribeiro da Silva RA 10775703

## Objetivo

Construir um pipeline completo de ingestão, validação de qualidade (Data
Quality), segregação de anomalias em quarentena, transformação em camadas
analíticas (Arquitetura Medallion — Raw, Silver e Gold) e auditoria de
metadados/consistência, utilizando Amazon S3 e Amazon Athena.

## Arquitetura

```
Geração de dados (com anomalias intencionais)
        │
        ▼
   Camada Raw (S3)  ──────────────► raw/{clientes,produtos,pedidos}/ingest_date=AAAA-MM-DD/
        │
        ▼
  Data Quality  ──► registros inválidos ──► Quarentena (S3, JSON) ──► quarantine/pedidos_rejeitados/data=AAAA-MM-DD/
        │
        ▼ (registros válidos)
  JOIN + valor_total = quantidade * preço
        │
        ▼
  Camada Silver (S3, Parquet/Snappy) ────► processed/fato_vendas/ingest_date=AAAA-MM-DD/
        │
        ▼
  Agregação por uf e categoria
        │
        ▼
  Camada Gold (S3, Parquet/Snappy) ──────► gold/vendas_por_uf_categoria/ingest_date=AAAA-MM-DD/
        │
        ▼
  Tabelas externas no Amazon Athena (consultas e auditoria)
```

## Estrutura do repositório

```
.
├── ingestao_raw.py       # Etapa 1: gera dados simulados (com anomalias) e sobe para a camada Raw
├── criar_bucket.py       # Script auxiliar: cria o bucket S3 do projeto
├── data_quality_silver.py # Etapa 2: aplica Data Quality, gera quarentena e a camada Silver
├── gold.py                # Etapa 3: agrega métricas de negócio e gera a camada Gold
├── athena_setup.py        # Cria o banco e as tabelas externas no Athena
├── aws_credentials         # Credenciais AWS (NÃO versionado — ver .gitignore)
├── .gitignore
└── README.md
```

## Pré-requisitos

- Python 3.10+
- Conta AWS com um usuário IAM com permissões `AmazonS3FullAccess` e
  `AmazonAthenaFullAccess`
- Bibliotecas Python:
  ```bash
  pip install boto3 pandas pyarrow faker
  ```

## Configuração das credenciais

Crie um arquivo chamado `aws_credentials` na raiz do projeto (mesmo formato
do `~/.aws/credentials`):

```ini
[default]
aws_access_key_id = SUA_ACCESS_KEY
aws_secret_access_key = SUA_SECRET_KEY
```

Esse arquivo é lido automaticamente pelos scripts (via
`AWS_SHARED_CREDENTIALS_FILE`) e está listado no `.gitignore` — **nunca é
enviado ao GitHub**.

Em cada script (`ingestao_raw.py`, `data_quality_silver.py`, `gold.py`,
`athena_setup.py`, `criar_bucket.py`), edite as constantes no topo do
arquivo:

```python
BUCKET_NAME = "seu-bucket-aqui"
AWS_REGION = "us-east-1"
```

## Instruções de execução

Rode os scripts **nesta ordem**:

1. **Criar o bucket S3** (uma única vez):
   ```bash
   python criar_bucket.py
   ```

2. **Ingestão — Camada Raw** (gera as massas de dados com anomalias e sobe
   para o S3):
   ```bash
   python ingestao_raw.py
   ```

3. **Data Quality + Quarentena + Camada Silver**:
   ```bash
   python data_quality_silver.py
   ```

4. **Camada Gold** (agregações analíticas por UF e categoria):
   ```bash
   python gold.py
   ```

5. **Criar banco e tabelas no Athena**:
   ```bash
   python athena_setup.py
   ```

6. **No Console AWS → Athena**: selecionar o banco `atividade2_db` e rodar
   as queries de auditoria (seção abaixo).

## Regras de Data Quality aplicadas

Um pedido é descartado (e vai para a quarentena) quando:

- `quantidade <= 0`, e/ou
- `cliente_id` não existe na tabela de clientes, e/ou
- `produto_id` não existe na tabela de produtos

Cada registro rejeitado é gravado em JSON com a lista de motivos da rejeição
(`motivos_rejeicao`), permitindo mais de um motivo por pedido.

## Estrutura de pastas gerada no S3

```
s3://<bucket>/
├── raw/
│   ├── clientes/ingest_date=AAAA-MM-DD/clientes.csv
│   ├── produtos/ingest_date=AAAA-MM-DD/produtos.csv
│   └── pedidos/ingest_date=AAAA-MM-DD/pedidos.csv
├── quarantine/
│   └── pedidos_rejeitados/data=AAAA-MM-DD/rejeitados.json
├── processed/
│   └── fato_vendas/ingest_date=AAAA-MM-DD/fato_vendas.parquet
├── gold/
│   └── vendas_por_uf_categoria/ingest_date=AAAA-MM-DD/vendas_por_uf_categoria.parquet
└── athena-results/
    └── (resultados das queries do Athena)
```

## Consultas de auditoria no Athena

### Metadados físicos dos arquivos (`$path`, `$file_size`)

```sql
SELECT
  pedido_id,
  ingest_date,
  "$path"      AS arquivo_origem,
  "$file_size" AS tamanho_arquivo_bytes
FROM raw_pedidos
LIMIT 20;
```

**Print do resultado:**

`[inserir print do console do Athena aqui]`

### Conciliação de integridade — Raw = Silver + Quarentena

```sql
WITH total_raw AS (
  SELECT COUNT(*) AS qtd FROM raw_pedidos
),
total_silver AS (
  SELECT COUNT(*) AS qtd FROM silver_fato_vendas
),
total_quarentena AS (
  SELECT COUNT(*) AS qtd FROM quarentena_pedidos_rejeitados
)
SELECT
  total_raw.qtd        AS total_raw,
  total_silver.qtd     AS total_silver,
  total_quarentena.qtd AS total_quarentena,
  (total_silver.qtd + total_quarentena.qtd) AS silver_mais_quarentena,
  total_raw.qtd = (total_silver.qtd + total_quarentena.qtd) AS bate
FROM total_raw, total_silver, total_quarentena;
```

**Print do resultado (comprovando `bate = true`):**

`[inserir print do console do Athena aqui]`

## Resultado da execução (exemplo)

| Métrica | Valor |
|---|---|
| Total de pedidos gerados (Raw) | 500 |
| Pedidos rejeitados (Quarentena) | 74 |
| Pedidos válidos (Silver) | 426 |
| Linhas agregadas (Gold) | 85 |

*(ajuste esses números para os valores da sua própria execução)*

## Observações

- Os nomes de bucket S3 são globais — o nome usado neste projeto é
  específico desta conta AWS.
- O arquivo de credenciais (`aws_credentials`) nunca deve ser commitado;
  confira o `.gitignore` antes do primeiro `git push`.
