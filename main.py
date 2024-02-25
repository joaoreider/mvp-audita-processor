
from time import time
from app import app
import json
from flask import jsonify
import boto3
import os
import pandas as pd
from io import BytesIO
from utils import normalize_md_file

BUCKET_NAME = 'audita-dev'

def get_files_from_folder_from_s3(bucket_name, folder_id, s3_instance):
  # Get folder from S3
  files = []
  objects = s3_instance.list_objects(Bucket=bucket_name, Prefix=folder_id)
  for obj in objects['Contents']:
    files.append(obj['Key'])
  return files

def read_s3_file(bucket, key, s3_instance):
    obj = s3_instance.get_object(Bucket=bucket, Key=key)
    content = obj['Body'].read()
    excel_file = BytesIO(content)
    df = pd.read_excel(excel_file)
    return df

def normalize_file(file):
  # treats and suits the files (trata e adéqua)
  try: 
    return normalize_md_file(file)
  except:
    # tentar analisar o arquivo de outra em outro padrão
    return None

def analyze_file(file):
  # analyze and add data to the dataframe
  pass


def main(s3_folder_id):
  if s3_folder_id:
    start = time()
    app.logger.info("Processor started!")

    # Estrutura do  dataframe (resultado final do processamento de cada arquivo)
    resultado = {}
    registro = []
    situacao_registro = []
    laboratorio = []
    situacao_da_marca = []
    situacao_do_preco = []
    preco_da_proposta = []
    preco_da_tabela = []
    descricao_da_proposta = []
    descricao_da_tabela = []


    s3 = boto3.client('s3', region_name='us-east-1', aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
    s3_files = get_files_from_folder_from_s3(bucket_name=BUCKET_NAME, folder_id=s3_folder_id, s3_instance=s3)
    
    dataframe_to_analyze = pd.DataFrame()

    for file in s3_files:

      # lê o arquivo do S3
      proposta = read_s3_file(BUCKET_NAME, file, s3_instance=s3)

      # trata e adéqua ao modelo padrão
      proposta_normalizada_no_formato_dataframe = normalize_file(proposta)

      # concatena com o dataframe to analyze
      # dataframe_to_analyze = pd.concat([dataframe_to_analyze, proposta_normalizada_no_formato_dataframe])

    
    
    result_dataframe = analyze_file(file)
    #TODO: Converter o dataframe final para JSON no formato do mock_response e retornar
    with open('mock_response_large.json', 'r') as f:
        data = json.load(f)

    end = time()
    app.logger.info(f"Processor finished in {round(end - start)} seconds")
    return data
    
  else:
    return 'No files to process'