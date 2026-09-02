"""
Atividade 2 - Etapa 2: Data Quality, Quarentena e Camada Silver

O que este script faz:
1. Le os CSVs da camada Raw no S3 (clientes, produtos, pedidos), pegando
   automaticamente a particao ingest_date mais recente de cada tabela.
2. Aplica as regras de Data Quality sobre os pedidos:
   - descarta quantidade <= 0
   - descarta cliente_id ou produto_id que nao existam nas dimensoes
3. Grava os pedidos invalidos em JSON (com o motivo da rejeicao) em:
   s3://<bucket>/quarantine/pedidos_rejeitados/data=YYYY-MM-DD/rejeitados.json
4. Enriquece os pedidos validos via JOIN com clientes e produtos, calcula
   valor_total = quantidade * preco, e salva a camada Silver (Parquet/Snappy) em:
   s3://<bucket>/processed/fato_vendas/ingest_date=YYYY-MM-DD/fato_vendas.parquet

Pre-requisitos:
- pip install boto3 pandas pyarrow
- Arquivo "aws_credentials" na mesma pasta deste script, no formato:
      [default]
      aws_access_key_id = sua_access_key
      aws_secret_access_key = sua_secret_key
"""

import io
import json
import os
from datetime import date

import boto3
import pandas as pd

# ---------------------------------------------------------------------------
# CONFIGURACAO - use os MESMOS valores do ingestao_raw.py
# ---------------------------------------------------------------------------
BUCKET_NAME = "adriana-datalake-atividade2"     # <-- mesmo bucket do ingestao_raw.py
AWS_REGION = "us-east-1"            # <-- mesma regiao do ingestao_raw.py

AWS_CREDENTIALS_FILE = os.path.join(os.path.dirname(__file__), "aws_credentials")
AWS_PROFILE = "default"

os.environ["AWS_SHARED_CREDENTIALS_FILE"] = AWS_CREDENTIALS_FILE
session = boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)

LOCAL_ROOT = "data_silver"          # pasta local para arquivos intermediarios/saida


# ---------------------------------------------------------------------------
# LOCALIZAR E LER A CAMADA RAW NO S3
# ---------------------------------------------------------------------------
def obter_ultima_particao(s3, tabela):
    """Encontra a particao ingest_date=YYYY-MM-DD mais recente de uma
    tabela na camada Raw, olhando os 'diretorios' (common prefixes) do S3."""
    prefixo = f"raw/{tabela}/"
    resposta = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefixo, Delimiter="/")

    prefixos = [cp["Prefix"] for cp in resposta.get("CommonPrefixes", [])]
    if not prefixos:
        raise FileNotFoundError(
            f"Nenhuma particao encontrada em s3://{BUCKET_NAME}/{prefixo} "
            "- rode o ingestao_raw.py primeiro."
        )

    # "ingest_date=YYYY-MM-DD" ordena corretamente como string (formato ISO)
    ultima = sorted(prefixos)[-1]
    data_particao = ultima.split("ingest_date=")[1].rstrip("/")
    return ultima, data_particao


def ler_csv_do_s3(s3, chave):
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=chave)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))


def carregar_camada_raw(s3):
    """Baixa clientes, produtos e pedidos da particao Raw mais recente de
    cada tabela e retorna os DataFrames + a data da particao de pedidos
    (usada para nomear as saidas de quarentena e Silver)."""
    tabelas = {}
    data_pedidos = None

    for tabela in ["clientes", "produtos", "pedidos"]:
        prefixo, data_particao = obter_ultima_particao(s3, tabela)
        chave = f"{prefixo}{tabela}.csv"
        print(f"Lendo s3://{BUCKET_NAME}/{chave}")
        tabelas[tabela] = ler_csv_do_s3(s3, chave)

        if tabela == "pedidos":
            data_pedidos = data_particao

    return tabelas["clientes"], tabelas["produtos"], tabelas["pedidos"], data_pedidos


# ---------------------------------------------------------------------------
# DATA QUALITY
# ---------------------------------------------------------------------------
def aplicar_data_quality(pedidos, clientes, produtos):
    """Marca cada pedido invalido com o motivo da rejeicao. Um pedido pode
    ter mais de um motivo (ex.: quantidade negativa E cliente inexistente)."""
    pedidos = pedidos.copy()

    quantidade_invalida = pedidos["quantidade"] <= 0
    cliente_inexistente = ~pedidos["cliente_id"].isin(clientes["cliente_id"])
    produto_inexistente = ~pedidos["produto_id"].isin(produtos["produto_id"])

    def motivos(row_idx):
        lista = []
        if quantidade_invalida.iloc[row_idx]:
            lista.append("quantidade <= 0")
        if cliente_inexistente.iloc[row_idx]:
            lista.append("cliente_id inexistente")
        if produto_inexistente.iloc[row_idx]:
            lista.append("produto_id inexistente")
        return lista

    pedidos["motivos_rejeicao"] = [motivos(i) for i in range(len(pedidos))]
    invalido = quantidade_invalida | cliente_inexistente | produto_inexistente

    rejeitados = pedidos[invalido].copy()
    validos = pedidos[~invalido].drop(columns=["motivos_rejeicao"]).copy()

    print(f"Total de pedidos lidos: {len(pedidos)}")
    print(f"  -> quantidade invalida: {int(quantidade_invalida.sum())}")
    print(f"  -> cliente_id inexistente: {int(cliente_inexistente.sum())}")
    print(f"  -> produto_id inexistente: {int(produto_inexistente.sum())}")
    print(f"  -> total rejeitados (quarentena): {len(rejeitados)}")
    print(f"  -> total validos (seguem para Silver): {len(validos)}")

    return validos, rejeitados


# ---------------------------------------------------------------------------
# QUARENTENA
# ---------------------------------------------------------------------------
def salvar_quarentena(s3, rejeitados, data_particao):
    pasta = os.path.join(LOCAL_ROOT, "quarantine", f"data={data_particao}")
    os.makedirs(pasta, exist_ok=True)
    caminho_local = os.path.join(pasta, "rejeitados.json")

    registros = json.loads(rejeitados.to_json(orient="records"))
    with open(caminho_local, "w", encoding="utf-8") as f:
        json.dump(registros, f, ensure_ascii=False, indent=2)

    print(f"Quarentena salva localmente: {caminho_local} ({len(registros)} registros)")

    chave_s3 = f"quarantine/pedidos_rejeitados/data={data_particao}/rejeitados.json"
    s3.upload_file(caminho_local, BUCKET_NAME, chave_s3)
    print(f"Quarentena enviada para: s3://{BUCKET_NAME}/{chave_s3}")


# ---------------------------------------------------------------------------
# CAMADA SILVER (enriquecimento + valor_total)
# ---------------------------------------------------------------------------
def montar_camada_silver(validos, clientes, produtos):
    fato_vendas = (
        validos.merge(clientes, on="cliente_id", how="left")
        .merge(produtos, on="produto_id", how="left", suffixes=("_cliente", "_produto"))
    )
    fato_vendas["valor_total"] = fato_vendas["quantidade"] * fato_vendas["preco"]
    return fato_vendas


def salvar_silver(s3, fato_vendas, data_particao):
    pasta = os.path.join(LOCAL_ROOT, "fato_vendas", f"ingest_date={data_particao}")
    os.makedirs(pasta, exist_ok=True)
    caminho_local = os.path.join(pasta, "fato_vendas.parquet")

    fato_vendas.to_parquet(caminho_local, engine="pyarrow", compression="snappy", index=False)
    print(f"Camada Silver salva localmente: {caminho_local} ({len(fato_vendas)} linhas)")

    chave_s3 = f"processed/fato_vendas/ingest_date={data_particao}/fato_vendas.parquet"
    s3.upload_file(caminho_local, BUCKET_NAME, chave_s3)
    print(f"Camada Silver enviada para: s3://{BUCKET_NAME}/{chave_s3}")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    s3 = session.client("s3")

    print("=== Carregando dados da camada Raw ===")
    clientes, produtos, pedidos, data_particao = carregar_camada_raw(s3)

    print("\n=== Aplicando regras de Data Quality ===")
    validos, rejeitados = aplicar_data_quality(pedidos, clientes, produtos)

    print("\n=== Gravando quarentena ===")
    if len(rejeitados) > 0:
        salvar_quarentena(s3, rejeitados, data_particao)
    else:
        print("Nenhum registro rejeitado - quarentena nao gerada.")

    print("\n=== Montando e salvando a camada Silver ===")
    fato_vendas = montar_camada_silver(validos, clientes, produtos)
    salvar_silver(s3, fato_vendas, data_particao)

    print(f"\nValor total de vendas (validas): {fato_vendas['valor_total'].sum():.2f}")
    print("\nEtapa 2 concluida com sucesso.")


if __name__ == "__main__":
    main()
