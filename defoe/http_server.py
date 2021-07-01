import json
from defoe.extract_contents import extract

from http.server import BaseHTTPRequestHandler, HTTPServer

class DefoeExtractTextHandler(BaseHTTPRequestHandler):
    def _set_response(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        json_data = json.loads(post_data)
        modelname = json_data['model']
        filename = json_data['file']
        if 'metadata' in json_data:
            metadata = json_data['metadata']
        else:
            metadata = {}
        self._set_response()
        print(f'reading {filename} using model {modelname}')
        for obj in extract(modelname, filename):
            obj.update(metadata)
            outtext = json.dumps(obj) + '\n'
            self.wfile.write(outtext.encode('utf-8'))


def run(server_class=HTTPServer, handler_class=DefoeExtractTextHandler, port=8080):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print(f'Server listening at localhost:{port}')
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()

if __name__ == '__main__':
    from sys import argv

    if len(argv) == 2:
        run(port=int(argv[1]))
    else:
        run()
 