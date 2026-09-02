"""
Atividade 2 - Etapa 1: Ingestao e Particionamento no Amazon S3 (Camada Raw)

O que este script faz:
1. Gera massas de dados simuladas para clientes, produtos e pedidos.
2. Injeta anomalias intencionais nos pedidos:
   - quantidade <= 0
   - cliente_id / produto_id que nao existem nas tabelas de dimensao
3. Salva tudo localmente em CSV, espelhando a estrutura de particoes do S3.
4. Sobe os arquivos para o S3 em formato Hive-style:
   s3://<bucket>/raw/<tabela>/ingest_date=YYYY-MM-DD/<tabela>.csv

Pre-requisitos:
- pip install boto3 faker
- Criar o arquivo "aws_credentials" na mesma pasta deste script, no formato:
      [default]
      aws_access_key_id = sua_access_key
      aws_secret_access_key = sua_secret_key
  Mantenha esse arquivo fora do controle de versao (.gitignore).
"""

import csv
import os
import random
from datetime import date

import boto3
from faker import Faker

# ---------------------------------------------------------------------------
# CONFIGURACAO - edite estes valores
# ---------------------------------------------------------------------------
BUCKET_NAME = "adriana-datalake-atividade2"     # <-- troque pelo nome do seu bucket S3
AWS_REGION = "us-east-1"            # <-- troque pela sua regiao, se necessario

# Arquivo de credenciais AWS dentro do projeto (formato igual ao
# ~/.aws/credentials, com uma secao [default]). Mantenha-o fora do Git
# (adicione ao .gitignore).
AWS_CREDENTIALS_FILE = os.path.join(os.path.dirname(__file__), "aws_credentials")
AWS_PROFILE = "default"

# Aponta o boto3 para o arquivo de credenciais do projeto antes de abrir
# qualquer sessao/cliente.
os.environ["AWS_SHARED_CREDENTIALS_FILE"] = AWS_CREDENTIALS_FILE

session = boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)

N_CLIENTES = 100
N_PRODUTOS = 50
N_PEDIDOS = 500

# Percentual de pedidos que devem nascer com cada tipo de anomalia
PCT_QUANTIDADE_INVALIDA = 0.05      # ~5% dos pedidos com quantidade <= 0
PCT_CHAVE_INEXISTENTE = 0.08        # ~8% dos pedidos com FK invalida

LOCAL_ROOT = "data_raw"             # pasta local que espelha a estrutura do S3
INGEST_DATE = date.today().isoformat()  # YYYY-MM-DD

# ---------------------------------------------------------------------------
# DADOS DE REFERENCIA (para gerar valores realistas)
# ---------------------------------------------------------------------------
ESTADOS_BR = ["SC", "SP", "DF", "RS", "CE", "MG", "PR", "BA", "PE", "RJ", "GO"]

CATEGORIAS_PRODUTOS = {
    "Eletronicos": ["Televisao", "Smartphone", "Monitor"],
    "Moveis": ["Sofa", "Mesa", "Cadeira"],
    "Acessorios": ["Relogio", "Mochila", "Smartwatch"],
    "Informatica": ["Notebook", "Teclado", "Mouse"],
    "Livros": ["Livro"],
    "Eletrodomesticos": ["Geladeira", "Fogao", "Microondas"],
    "Cozinha": ["Cafeteira", "Panela"],
    "Vestuario": ["Tenis", "Camisa"],
}

fake = Faker("pt_BR")


# ---------------------------------------------------------------------------
# GERACAO DAS TABELAS
# ---------------------------------------------------------------------------
def gerar_clientes(n):
    clientes = []
    for cliente_id in range(1, n + 1):
        clientes.append(
            {
                "cliente_id": cliente_id,
                "nome": fake.name(),
                "cidade": fake.city(),
                "estado": random.choice(ESTADOS_BR),
            }
        )
    return clientes


def gerar_produtos(n):
    produtos = []
    for produto_id in range(1, n + 1):
        categoria = random.choice(list(CATEGORIAS_PRODUTOS.keys()))
        nome = random.choice(CATEGORIAS_PRODUTOS[categoria])
        produtos.append(
            {
                "produto_id": produto_id,
                "nome": nome,
                "categoria": categoria,
                "preco": round(random.uniform(40, 9200), 2),
            }
        )
    return produtos


def gerar_pedidos(n, n_clientes, n_produtos):
    """Gera pedidos, injetando anomalias de proposito para as etapas
    seguintes (Data Quality / Quarentena) terem o que tratar."""
    pedidos = []
    contagem_qtd_invalida = 0
    contagem_fk_invalida = 0

    for pedido_id in range(1, n + 1):
        cliente_id = random.randint(1, n_clientes)
        produto_id = random.randint(1, n_produtos)
        quantidade = random.randint(1, 10)

        # Anomalia 1: quantidade invalida (zero ou negativa)
        if random.random() < PCT_QUANTIDADE_INVALIDA:
            quantidade = -random.randint(1, 10)
            contagem_qtd_invalida += 1

        # Anomalia 2: chave estrangeira inexistente
        if random.random() < PCT_CHAVE_INEXISTENTE:
            if random.random() < 0.5:
                cliente_id = random.randint(n_clientes + 1, n_clientes + 9999)
            else:
                produto_id = random.randint(n_produtos + 1, n_produtos + 9999)
            contagem_fk_invalida += 1

        pedidos.append(
            {
                "pedido_id": pedido_id,
                "cliente_id": cliente_id,
                "produto_id": produto_id,
                "quantidade": quantidade,
                "data_pedido": fake.date_between(start_date="-180d", end_date="today").isoformat(),
            }
        )

    print(f"  -> {contagem_qtd_invalida} pedidos com quantidade invalida")
    print(f"  -> {contagem_fk_invalida} pedidos com chave estrangeira inexistente")
    return pedidos


# ---------------------------------------------------------------------------
# ESCRITA LOCAL (espelhando a estrutura Hive do S3)
# ---------------------------------------------------------------------------
def escrever_csv_local(tabela, registros):
    """Salva em: data_raw/<tabela>/ingest_date=YYYY-MM-DD/<tabela>.csv"""
    pasta = os.path.join(LOCAL_ROOT, tabela, f"ingest_date={INGEST_DATE}")
    os.makedirs(pasta, exist_ok=True)
    caminho = os.path.join(pasta, f"{tabela}.csv")

    with open(caminho, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=registros[0].keys())
        writer.writeheader()
        writer.writerows(registros)

    print(f"Arquivo local gerado: {caminho} ({len(registros)} linhas)")
    return caminho


# ---------------------------------------------------------------------------
# UPLOAD PARA O S3
# ---------------------------------------------------------------------------
def subir_para_s3(caminho_local, tabela):
    """Sobe o arquivo mantendo o particionamento Hive-style:
    s3://<bucket>/raw/<tabela>/ingest_date=YYYY-MM-DD/<tabela>.csv"""
    s3 = session.client("s3")

    chave_s3 = f"raw/{tabela}/ingest_date={INGEST_DATE}/{tabela}.csv"

    s3.upload_file(caminho_local, BUCKET_NAME, chave_s3)
    print(f"Enviado para: s3://{BUCKET_NAME}/{chave_s3}")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
def main():
    print("=== Gerando clientes ===")
    clientes = gerar_clientes(N_CLIENTES)
    caminho_clientes = escrever_csv_local("clientes", clientes)

    print("\n=== Gerando produtos ===")
    produtos = gerar_produtos(N_PRODUTOS)
    caminho_produtos = escrever_csv_local("produtos", produtos)

    print("\n=== Gerando pedidos (com anomalias intencionais) ===")
    pedidos = gerar_pedidos(N_PEDIDOS, N_CLIENTES, N_PRODUTOS)
    caminho_pedidos = escrever_csv_local("pedidos", pedidos)

    print("\n=== Subindo arquivos para o S3 ===")
    subir_para_s3(caminho_clientes, "clientes")
    subir_para_s3(caminho_produtos, "produtos")
    subir_para_s3(caminho_pedidos, "pedidos")

    print("\nIngestao concluida com sucesso.")


if __name__ == "__main__":
    main()
