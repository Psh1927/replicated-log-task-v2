from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import logging

memory_list = list()
FORMAT = '%(asctime)s %(message)s'
logging.basicConfig(format=FORMAT, level=1)


def print_list():
    print(memory_list)
    if len(memory_list) == 0:
        response = "Empty list"
    else:
        response = ''
        for i, row in enumerate(memory_list):
            if (i + 1) != row['id']:
                break
            response += str(row['id']) + ' ' + row['msg'] + '\n'
    return response


class RequestHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Type', 'text/html')
        self.end_headers()
        response = print_list()
        self.wfile.write(response.encode())

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        body = self.rfile.read(content_length)
        data = json.loads(json.loads(body))
        try:
            if len(memory_list) == 0:
                memory_list.append(data)
                logging.info('Saved to ' + self.headers['Host'] + ': '
                             + 'id=' + str(memory_list[-1]['id']) + ' msg=' + memory_list[-1]['msg'])
            else:
                j = 0
                while memory_list[-1 - j]['id'] > data['id']:
                    j += 1
                if memory_list[-1 - j]['id'] == data['id']:
                    logging.info('Duplicated ID. Skip')
                else:
                    memory_list.insert(len(memory_list) - j, data)
                    logging.info('Saved to ' + self.headers['Host'] + ': '
                                 + 'id=' + str(memory_list[-1]['id']) + ' msg=' + memory_list[-1]['msg'])
            self.send_response(200)
        except:
            logging.error('Error on ' + self.headers['Host'] + ': '
                         + 'id=' + str(data['id']) + ' msg=' + data['msg'])
            self.send_response(408)
        self.end_headers()


def main():
    port = 8081
    print('Listening on localhost:%s' % port)
    server = HTTPServer(('', port), RequestHandler)
    server.serve_forever()


if __name__ == "__main__":
    main()