"""
Script auxiliar - Criar o bucket S3 do projeto (Atividade 2)

Roda uma unica vez, antes do ingestao_raw.py, para garantir que o bucket
realmente existe na sua conta AWS e na regiao configurada.

Pre-requisitos:
- pip install boto3
- Arquivo "aws_credentials" na mesma pasta deste script, no formato:
      [default]
      aws_access_key_id = sua_access_key
      aws_secret_access_key = sua_secret_key
"""

import os

import boto3
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# CONFIGURACAO - use os MESMOS valores do ingestao_raw.py
# ---------------------------------------------------------------------------
BUCKET_NAME = "adriana-datalake-atividade2"     # <-- troque pelo mesmo nome usado no ingestao_raw.py
AWS_REGION = "us-east-1"            # <-- troque pela mesma regiao usada no ingestao_raw.py

AWS_CREDENTIALS_FILE = os.path.join(os.path.dirname(__file__), "aws_credentials")
AWS_PROFILE = "default"

os.environ["AWS_SHARED_CREDENTIALS_FILE"] = AWS_CREDENTIALS_FILE
session = boto3.Session(profile_name=AWS_PROFILE, region_name=AWS_REGION)


def criar_bucket():
    s3 = session.client("s3")

    try:
        if AWS_REGION == "us-east-1":
            # us-east-1 e a regiao padrao da API S3 e NAO aceita
            # LocationConstraint no create_bucket - passar o parametro
            # nesse caso quebra a chamada.
            s3.create_bucket(Bucket=BUCKET_NAME)
        else:
            s3.create_bucket(
                Bucket=BUCKET_NAME,
                CreateBucketConfiguration={"LocationConstraint": AWS_REGION},
            )
        print(f"Bucket '{BUCKET_NAME}' criado com sucesso em '{AWS_REGION}'.")

    except ClientError as erro:
        codigo = erro.response["Error"]["Code"]

        if codigo == "BucketAlreadyOwnedByYou":
            print(f"O bucket '{BUCKET_NAME}' ja existe e ja e seu. Nada a fazer.")
        elif codigo == "BucketAlreadyExists":
            print(
                f"O nome '{BUCKET_NAME}' ja esta em uso por OUTRA conta AWS.\n"
                "Nomes de bucket S3 sao globais e unicos. Escolha outro nome, "
                "atualize BUCKET_NAME aqui e no ingestao_raw.py, e rode de novo."
            )
        elif codigo == "InvalidAccessKeyId" or codigo == "SignatureDoesNotMatch":
            print(
                "As credenciais em 'aws_credentials' parecem invalidas. "
                "Confira o access key e o secret key."
            )
        else:
            print(f"Erro ao criar o bucket ({codigo}): {erro}")
            raise


def verificar_bucket():
    """Confirma que o bucket existe e esta acessivel com as credenciais atuais."""
    s3 = session.client("s3")
    try:
        s3.head_bucket(Bucket=BUCKET_NAME)
        print(f"Confirmado: o bucket '{BUCKET_NAME}' existe e esta acessivel.")
    except ClientError as erro:
        codigo = erro.response["Error"]["Code"]
        print(f"Nao foi possivel confirmar o bucket (codigo {codigo}).")


if __name__ == "__main__":
    criar_bucket()
    verificar_bucket()