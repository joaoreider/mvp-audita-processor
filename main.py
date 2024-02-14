
from time import time, sleep
from app import app

def get_folder_from_s3(folder_id):
  # Get folder from S3
  return []

def treat_file(file):
  # treats and suits the files (trata e adéqua)
  pass

def analyze_file(file):
  # analyze and add data to the dataframe
  pass


def main(s3_folder_id):
  if s3_folder_id:
    start = time()
    app.logger.info("Processor started!")


    #TODO: Montar a estrutura do  dataframe (resultado final do processamento de cada arquivo)

    s3_files = get_folder_from_s3(s3_folder_id)
    
    for file in s3_files:
      # trata e adéqua ao modelo padrão
      treated_files = treat_file(s3_files)
    
      dataframed_file = analyze_file(file)
      sleep(1)

      #TODO: Adicionar o dataframe do arquivo ao dataframe final
    
    #TODO: Converter o dataframe final para JSON e retornar
    mocked_json = {
    "columns": [
        "col 1",
        "col 2"
    ],
    "index": [
        "row 1",
        "row 2"
    ],
    "data": [
        [
            "a",
            "b"
        ],
        [
            "c",
            "d"
        ]
    ]
}
    
    end = time()
    app.logger.info(f"Processor finished in {round(end - start)} seconds")
    return mocked_json
    
  else:
    return 'No files to process'