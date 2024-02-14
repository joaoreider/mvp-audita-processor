from flask import Flask

from flask import Flask, request, jsonify, make_response

app = Flask(__name__)

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
      return make_response(jsonify({'files': files}), 200)
    else:
      return make_response(jsonify({'error': 'No files to process'}), 400)
    
if __name__ == '__main__':
    app.run(debug = True, host = '0.0.0.0')