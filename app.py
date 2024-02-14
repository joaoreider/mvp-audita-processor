from flask import Flask

from flask import Flask, request, jsonify, make_response

app = Flask(__name__)
from main import main

@app.route('/health', methods=['GET'])
def health():
    return make_response(jsonify({'status': 'healthy'}), 200)

# /process?files=filename1,filename2
@app.route('/process', methods=['GET'])
def processFiles():
    files = request.args.get('files')
    if files:
      files = files.split(',')
      app.logger.info(f"Files to process: {files}")
      try:
        result = main(files)
        return make_response(result, 200)
      except Exception as e:
        app.logger.error(f"Error processing files: {e}")
        return make_response(jsonify('error processing files'), 500)
    else:
      app.logger.error('No files to process')
      return make_response(jsonify({'error': 'No files to process'}), 400)
    
if __name__ == '__main__':
    app.run(debug = True, host = '0.0.0.0')