
from time import time
from app import app
import json
from flask import jsonify
import boto3
import os
import pandas as pd
from io import BytesIO, StringIO
import utils as helper

BUCKET_NAME = 'audita-dev'

def get_files_from_folder_from_s3(bucket_name, folder_id, s3_instance):
  # Get folder from S3
  files = []
  objects = s3_instance.list_objects(Bucket=bucket_name, Prefix=folder_id)
  for obj in objects['Contents']:
    # Não incluir o arquivo de resultado na lista de arquivos a serem processados
    if obj['Key'] != f"{folder_id}resultado.csv":
      files.append(obj['Key'])
  return files

def read_s3_excel_file(bucket, key, s3_instance):
    obj = s3_instance.get_object(Bucket=bucket, Key=key)
    content = obj['Body'].read()
    excel_file = BytesIO(content)
    df = pd.read_excel(excel_file)
    return df

def read_s3_cmed_table_file(bucket, s3_instance):
    obj = s3_instance.get_object(Bucket=bucket, Key="tabela-cmed/tabela_cmed_tratada.csv")
    content = obj['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(content), sep=';')
    return df

def normalize_file(file):
  # treats and suits the files (trata e adéqua)
  try: 
    return helper.normalize_md_file(file)
  except:
    # tentar analisar o arquivo de outra em outro padrão
    app.logger.error(f"Error trying to normalize file {file}")
    return None

def analyze_file(proposta, tabela_cmed):

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

    for row in proposta.iterrows():
      registro_proposta = row[1]['registro']
      marca_proposta = row[1]['marca']
      apresentacao_proposta= row[1]['descricao']
      preco_unitario_proposta = row[1]['preco_unitario']

      registro_completo = len(str(registro_proposta)) == 13
      # Campos da Tabela CMED
      if registro_completo:
          item_tabela_cmed = tabela_cmed[tabela_cmed['REGISTRO'] == str(registro_proposta)]
      else:
          item_tabela_cmed = tabela_cmed[tabela_cmed['REGISTRO'].apply(lambda x: str(registro_proposta) in str(x))]

      achou_registro_proposta = len(item_tabela_cmed) > 0

      registro.append(registro_proposta)
      descricao_da_proposta.append(apresentacao_proposta)
      preco_da_proposta.append(preco_unitario_proposta)
      # Comparando os campos
      if achou_registro_proposta: 
          # print('Registro encontrado!')
          marca_tabela_cmed = item_tabela_cmed['LABORATÓRIO'].values[0]
          apresentacao_tabela_cmed = item_tabela_cmed['DESCRIÇÃO'].values[0]
          preco_unitario_tabela_cmed = item_tabela_cmed['PF 18%'].values[0].replace(',', '.')
          quantidade_tabela_cmed = item_tabela_cmed['quantidade'].values[0]
          marca_valida = helper.confirmar_marca_do_registro(marca_proposta, marca_tabela_cmed)
          descricao_da_tabela.append(apresentacao_tabela_cmed)
          laboratorio.append(marca_tabela_cmed)
          preco_da_tabela.append(preco_unitario_tabela_cmed)


          if marca_valida:
              # print('Marca válida!')
              situacao_da_marca.append('OK')
              if registro_completo:
                  apresentacao_valida = helper.comparar_apresentacao_do_registro(apresentacao_proposta, apresentacao_tabela_cmed)

                  if apresentacao_valida:
                      # print('Apresentação válida!')
                      preco_valido = helper.comparar_preco_unitario_do_registro(preco_unitario_proposta, preco_unitario_tabela_cmed)

                      if preco_valido:
                          # print('Preço válido!')
                          situacao_do_preco.append('Abaixo')
                          # print('Registro Aprovado!')
                          situacao_registro.append('APROVADO')
                      elif quantidade_tabela_cmed == 1:
                          # print('Preço inválido!')
                          situacao_do_preco.append('VERIFICAR')
                          # print('Registro necessita de confirmação manual!')
                          situacao_registro.append('VERIFICAR')
                      else:
                          # print('Preço inválido!')
                          situacao_do_preco.append('Acima')
                          situacao_registro.append('APROVADO')

                  else:
                      # print('Não foi possível comparar apresentação!')
                      situacao_do_preco.append('VERIFICAR')
                      # print('Registro necessita de confirmação manual!')
                      situacao_registro.append('VERIFICAR')
              else:
                  situacao_do_preco.append('Inacessível')
                  # print('Registro com 8 dígitos!')
                  situacao_registro.append('APROVADO')
          else:
              # print('Não foi possível confirmar marca!')
              situacao_da_marca.append('Inválida')
              situacao_do_preco.append('Inacessível')
              # print('Registro Reprovado!')
              situacao_registro.append('REPROVADO')
      else:
          # print('Registro não encontrado!')
          # print('Registro Reprovado!')
          situacao_registro.append('REPROVADO')
          situacao_da_marca.append('Inacessível')
          situacao_do_preco.append('Inacessível')
          descricao_da_tabela.append('Inacessível')
          laboratorio.append('Inacessível')
          preco_da_tabela.append('Inacessível')


    # Monto o dataframe de resultados
    resultado['registro'] = registro
    resultado['situacao_registro'] = situacao_registro
    resultado['laboratorio'] = laboratorio
    resultado['situacao_da_marca'] = situacao_da_marca
    resultado['situacao_do_preco'] = situacao_do_preco
    resultado['preco_da_proposta'] = preco_da_proposta
    resultado['preco_da_tabela'] = preco_da_tabela
    resultado['descricao_da_proposta'] = descricao_da_proposta
    resultado['descricao_da_tabela'] = descricao_da_tabela


    try:

      # for chave, valor in resultado.items():
      #print(f'Chave: {chave}, tamanho: {len(valor)}')
      df_final = pd.DataFrame(resultado)
      return df_final

    except: raise Exception('N foi possível montar o dataframe')

def check_s3():
   s3 = boto3.client('s3', region_name='us-east-1', aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
   return s3.list_buckets()
   

def send_result_to_s3(bucket_name, path, s3_instance, result_dataframe):
  # Send result to S3
  csv_buffer = StringIO()
  result_dataframe.to_csv(csv_buffer, index=False, sep=';')
  path = path if path.endswith('/') else f"{path}/"
  s3_instance.put_object(Bucket=bucket_name, Key=f"{path}resultado.csv", Body=csv_buffer.getvalue())

def main(s3_folder_id):
  if s3_folder_id:
    start = time()
    app.logger.info("Processor started!")


    s3 = boto3.client('s3', region_name='us-east-1', aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'], aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
    s3_files = get_files_from_folder_from_s3(bucket_name=BUCKET_NAME, folder_id=s3_folder_id, s3_instance=s3)
    
    dataframe_to_analyze = pd.DataFrame()

    for file in s3_files:
      app.logger.debug(f"Processing file {file}")
      # lê o arquivo do S3
      proposta = read_s3_excel_file(BUCKET_NAME, file, s3_instance=s3)

      # trata e adéqua ao modelo padrão
      proposta_normalizada_no_formato_dataframe = normalize_file(proposta)

      # dataframe to analyze: grupo de propostas normalizadas para analisar de uma vez
      dataframe_to_analyze = pd.concat([dataframe_to_analyze, proposta_normalizada_no_formato_dataframe])

    CMED_TABLE = read_s3_cmed_table_file(BUCKET_NAME, s3_instance=s3)
    result_dataframe = analyze_file(dataframe_to_analyze, CMED_TABLE)

    json_result = result_dataframe.to_json(orient='records')
    # json string to json
    data = json.loads(json_result)

    send_result_to_s3(BUCKET_NAME, s3_folder_id, s3_instance=s3, result_dataframe=result_dataframe)
    end = time()
    app.logger.info(f"Processor finished in {round(end - start)} seconds")
    return data
    
  else:
    return 'No files to process'