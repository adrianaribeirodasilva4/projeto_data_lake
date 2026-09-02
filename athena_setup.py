"""
Atividade 2 - Script auxiliar: Criar banco e tabelas no Athena

O que este script faz:
1. Cria o banco (database) no Athena.
2. Cria as tabelas externas: raw_pedidos, raw_clientes, raw_produtos,
   quarentena_pedidos_rejeitados, silver_fato_vendas e gold_vendas_uf_categoria.
3. Roda MSCK REPAIR TABLE em cada uma, para registrar as particoes que
   ja existem no S3 (ignora tabelas que ainda nao tem nenhuma particao).

Pre-requisitos:
- pip install boto3
- Arquivo "aws_credentials" na mesma pasta deste script, no formato:
      [default]
      aws_access_key_id = sua_access_key
      aws_secret_access_key = sua_secret_key
- Ja ter rodado ao menos o ingestao_raw.py (para o bucket/pastas existirem)
"""

import os
import time

import boto3

# ---------------------------------------------------------------------------
# CONFIGURACAO - use os MESMOS valores dos scripts anteriores
# ---------------------------------------------------------------------------
BUCKET_NAME = "SELECT
  pedido_id,
  ingest_date,
  "$path"      AS arquivo_origem,
  "$file_size" AS tamanho_arquivo_bytes
FROM raw_pedidos
LIMIT 20;"      # <-- mesmo bucket dos scripts anteriores
AWS_REGION = "us-east-1"             # <-- mesma regiao dos scripts anteriores
DATABASE_NAME = "datalake_db_atividade2"

# Pasta no S3 onde o Athena grava o resultado de cada query
ATHENA_OUTPUT_LOCATION = f"s3://{BUCKET_NAME}/athena-results/"

AWS_CREDENTIALS_FILE = os.path.join(os.path.dirname(__file__), "aws_credentials")
AWS_PROFILE = "default"

os.environ["AWS_SHARED_CREDENTIALS_FILE"] = AWS_CREDENTIALS_FILE
session = boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)


# ---------------------------------------------------------------------------
# DDL - CRIACAO DO BANCO E DAS TABELAS
# ---------------------------------------------------------------------------
DDL_DATABASE = f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME}"

DDL_TABELAS = {
    "raw_pedidos": f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS raw_pedidos (
          pedido_id     INT,
          cliente_id    INT,
          produto_id    INT,
          quantidade    INT,
          data_pedido   STRING
        )
        PARTITIONED BY (ingest_date STRING)
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        LOCATION 's3://{BUCKET_NAME}/raw/pedidos/'
        TBLPROPERTIES ('skip.header.line.count'='1')
    """,
    "raw_clientes": f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS raw_clientes (
          cliente_id INT,
          nome       STRING,
          cidade     STRING,
          estado     STRING
        )
        PARTITIONED BY (ingest_date STRING)
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        LOCATION 's3://{BUCKET_NAME}/raw/clientes/'
        TBLPROPERTIES ('skip.header.line.count'='1')
    """,
    "raw_produtos": f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS raw_produtos (
          produto_id INT,
          nome       STRING,
          categoria  STRING,
          preco      DOUBLE
        )
        PARTITIONED BY (ingest_date STRING)
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        LOCATION 's3://{BUCKET_NAME}/raw/produtos/'
        TBLPROPERTIES ('skip.header.line.count'='1')
    """,
    "quarentena_pedidos_rejeitados": f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS quarentena_pedidos_rejeitados (
          pedido_id        INT,
          cliente_id       INT,
          produto_id       INT,
          quantidade       INT,
          data_pedido      STRING,
          motivos_rejeicao ARRAY<STRING>
        )
        PARTITIONED BY (data STRING)
        ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
        LOCATION 's3://{BUCKET_NAME}/quarantine/pedidos_rejeitados/'
    """,
    "silver_fato_vendas": f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS silver_fato_vendas (
          pedido_id    INT,
          cliente_id   INT,
          produto_id   INT,
          quantidade   INT,
          data_pedido  STRING,
          nome_cliente STRING,
          cidade       STRING,
          estado       STRING,
          nome_produto STRING,
          categoria    STRING,
          preco        DOUBLE,
          valor_total  DOUBLE
        )
        PARTITIONED BY (ingest_date STRING)
        STORED AS PARQUET
        LOCATION 's3://{BUCKET_NAME}/processed/fato_vendas/'
    """,
    "gold_vendas_uf_categoria": f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS gold_vendas_uf_categoria (
          uf                  STRING,
          categoria           STRING,
          valor_total_vendas  DOUBLE,
          qtd_pedidos         INT,
          qtd_itens_vendidos  INT,
          ticket_medio        DOUBLE
        )
        PARTITIONED BY (ingest_date STRING)
        STORED AS PARQUET
        LOCATION 's3://{BUCKET_NAME}/gold/vendas_por_uf_categoria/'
    """,
}


# ---------------------------------------------------------------------------
# EXECUCAO DE QUERIES NO ATHENA
# ---------------------------------------------------------------------------
def rodar_query(athena, sql, database=None, exibir_erro_como_aviso=False):
    """Envia uma query ao Athena e espera terminar. Retorna o status final."""
    kwargs = {
        "QueryString": sql,
        "ResultConfiguration": {"OutputLocation": ATHENA_OUTPUT_LOCATION},
    }
    # A API do Athena rejeita um QueryExecutionContext vazio, entao so
    # incluimos esse parametro quando ha de fato um banco selecionado
    # (ex.: CREATE DATABASE roda sem contexto nenhum).
    if database:
        kwargs["QueryExecutionContext"] = {"Database": database}

    execucao = athena.start_query_execution(**kwargs)
    execution_id = execucao["QueryExecutionId"]

    while True:
        status_resp = athena.get_query_execution(QueryExecutionId=execution_id)
        status = status_resp["QueryExecution"]["Status"]["State"]

        if status in ("SUCCEEDED", "FAILED", "CANCELLED"):
            if status == "FAILED":
                motivo = status_resp["QueryExecution"]["Status"].get(
                    "StateChangeReason", "motivo desconhecido"
                )
                if exibir_erro_como_aviso:
                    print(f"  aviso: query falhou ({motivo})")
                else:
                    raise RuntimeError(f"Query falhou: {motivo}\nSQL:\n{sql}")
            return status

        time.sleep(1)


def criar_banco(athena):
    print(f"Criando banco '{DATABASE_NAME}'...")
    rodar_query(athena, DDL_DATABASE)
    print("Banco pronto.")


def criar_tabelas(athena):
    for nome_tabela, ddl in DDL_TABELAS.items():
        print(f"Criando tabela '{nome_tabela}'...")
        rodar_query(athena, ddl, database=DATABASE_NAME)
    print("Todas as tabelas foram criadas.")


def registrar_particoes(athena):
    for nome_tabela in DDL_TABELAS:
        print(f"Registrando particoes de '{nome_tabela}' (MSCK REPAIR)...")
        # exibir_erro_como_aviso=True porque uma camada que ainda nao rodou
        # (ex.: gold antes do gold.py) nao tem particoes, e isso e esperado.
        rodar_query(
            athena,
            f"MSCK REPAIR TABLE {nome_tabela}",
            database=DATABASE_NAME,
            exibir_erro_como_aviso=True,
        )
    print("Particoes registradas (quando existentes).")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    athena = session.client("athena")

    criar_banco(athena)
    criar_tabelas(athena)
    registrar_particoes(athena)

    print(f"\nTudo pronto. No console do Athena, selecione o banco '{DATABASE_NAME}' "
          "e rode suas queries de auditoria.")


if __name__ == "__main__":
    main()
