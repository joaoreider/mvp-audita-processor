
from time import time
from app import app

def main(files):
  if files:
    start = time()
    app.logger.info("Processor started!")
    end = time()
    app.logger.info(f"Processor finished in {round(end - start)} seconds")
    return 'Files processed'
  else:
    return 'No files to process'