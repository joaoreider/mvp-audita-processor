from flask import Flask

from flask import Flask, request, jsonify, make_response

app = Flask(__name__)
import main

@app.route('/health', methods=['GET'])
def health():
    return make_response(jsonify({'status': 'healthy'}), 200)

# /process?files=filename1,filename2
@app.route('/process', methods=['GET'])
def processFiles():
    folder_id = request.args.get('folder_id')
    if folder_id:
      app.logger.info(f"Folder to process: {folder_id}")
      try:
        result = main.main(folder_id)
        return make_response(jsonify(result), 200)
      except Exception as e:
        app.logger.error(f"Error processing files: {e}")
        return make_response(jsonify('error processing files'), 500)
    else:
      app.logger.error('No files to process')
      return make_response(jsonify({'error': 'No files to process'}), 400)
    
if __name__ == '__main__':
  #DEV
  # app.run(debug = True, host = '0.0.0.0')
  #PRD
  app.run(host = '0.0.0.0', port=5000)