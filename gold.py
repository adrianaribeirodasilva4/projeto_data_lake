"""
Atividade 2 - Etapa 3: Camada Gold (agregacoes analiticas)

O que este script faz:
1. Le a camada Silver no S3 (processed/fato_vendas/), pegando
   automaticamente a particao ingest_date mais recente.
2. Agrega metricas de negocio por uf e por categoria de produto:
   - valor_total_vendas: soma de valor_total
   - qtd_pedidos: numero de pedidos
   - qtd_itens_vendidos: soma de quantidade
   - ticket_medio: valor_total_vendas / qtd_pedidos
3. Salva o resultado (Parquet/Snappy) em:
   s3://<bucket>/gold/vendas_por_uf_categoria/ingest_date=YYYY-MM-DD/vendas_por_uf_categoria.parquet

Pre-requisitos:
- pip install boto3 pandas pyarrow
- Arquivo "aws_credentials" na mesma pasta deste script, no formato:
      [default]
      aws_access_key_id = sua_access_key
      aws_secret_access_key = sua_secret_key
- Ter rodado antes: ingestao_raw.py e data_quality_silver.py
"""

import io
import os

import boto3
import pandas as pd

# ---------------------------------------------------------------------------
# CONFIGURACAO - use os MESMOS valores dos scripts anteriores
# ---------------------------------------------------------------------------
BUCKET_NAME = "adriana-datalake-atividade2"     # <-- mesmo bucket dos scripts anteriores
AWS_REGION = "us-east-1"            # <-- mesma regiao dos scripts anteriores

AWS_CREDENTIALS_FILE = os.path.join(os.path.dirname(__file__), "aws_credentials")
AWS_PROFILE = "default"

os.environ["AWS_SHARED_CREDENTIALS_FILE"] = AWS_CREDENTIALS_FILE
session = boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)

LOCAL_ROOT = "data_gold"            # pasta local para arquivos intermediarios/saida


# ---------------------------------------------------------------------------
# LOCALIZAR E LER A CAMADA SILVER NO S3
# ---------------------------------------------------------------------------
def obter_ultima_particao_silver(s3):
    prefixo = "processed/fato_vendas/"
    resposta = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefixo, Delimiter="/")

    prefixos = [cp["Prefix"] for cp in resposta.get("CommonPrefixes", [])]
    if not prefixos:
        raise FileNotFoundError(
            f"Nenhuma particao encontrada em s3://{BUCKET_NAME}/{prefixo} "
            "- rode o data_quality_silver.py primeiro."
        )

    ultima = sorted(prefixos)[-1]
    data_particao = ultima.split("ingest_date=")[1].rstrip("/")
    return ultima, data_particao


def ler_silver(s3):
    prefixo, data_particao = obter_ultima_particao_silver(s3)
    chave = f"{prefixo}fato_vendas.parquet"
    print(f"Lendo s3://{BUCKET_NAME}/{chave}")

    obj = s3.get_object(Bucket=BUCKET_NAME, Key=chave)
    fato_vendas = pd.read_parquet(io.BytesIO(obj["Body"].read()), engine="pyarrow")
    return fato_vendas, data_particao


# ---------------------------------------------------------------------------
# AGREGACAO (CAMADA GOLD)
# ---------------------------------------------------------------------------
def agregar_gold(fato_vendas):
    gold = (
        fato_vendas.groupby(["estado", "categoria"], as_index=False)
        .agg(
            valor_total_vendas=("valor_total", "sum"),
            qtd_pedidos=("pedido_id", "count"),
            qtd_itens_vendidos=("quantidade", "sum"),
        )
    )
    gold["ticket_medio"] = (gold["valor_total_vendas"] / gold["qtd_pedidos"]).round(2)
    gold["valor_total_vendas"] = gold["valor_total_vendas"].round(2)

    gold = gold.rename(columns={"estado": "uf"}).sort_values(
        ["uf", "valor_total_vendas"], ascending=[True, False]
    )
    return gold.reset_index(drop=True)


def salvar_gold(s3, gold, data_particao):
    pasta = os.path.join(LOCAL_ROOT, "vendas_por_uf_categoria", f"ingest_date={data_particao}")
    os.makedirs(pasta, exist_ok=True)
    caminho_local = os.path.join(pasta, "vendas_por_uf_categoria.parquet")

    gold.to_parquet(caminho_local, engine="pyarrow", compression="snappy", index=False)
    print(f"Camada Gold salva localmente: {caminho_local} ({len(gold)} linhas)")

    chave_s3 = (
        f"gold/vendas_por_uf_categoria/ingest_date={data_particao}/"
        "vendas_por_uf_categoria.parquet"
    )
    s3.upload_file(caminho_local, BUCKET_NAME, chave_s3)
    print(f"Camada Gold enviada para: s3://{BUCKET_NAME}/{chave_s3}")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    s3 = session.client("s3")

    print("=== Lendo a camada Silver ===")
    fato_vendas, data_particao = ler_silver(s3)
    print(f"Total de linhas na Silver: {len(fato_vendas)}")

    print("\n=== Agregando metricas de negocio (Gold) ===")
    gold = agregar_gold(fato_vendas)
    print(gold.to_string(index=False))

    print("\n=== Salvando a camada Gold ===")
    salvar_gold(s3, gold, data_particao)

    print("\nEtapa 3 (Gold) concluida com sucesso.")


if __name__ == "__main__":
    main()
