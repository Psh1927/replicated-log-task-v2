from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import requests
import logging
from threading import Thread
from threading import Condition


class CountDownLatch:
    def __init__(self, count, count_good_result):
        self.count = count
        self.count_good_result = count_good_result
        self.condition = Condition()

    def count_down(self, result):
        with self.condition:
            if self.count == 0:
                return
            self.count -= 1
            if result:
                self.count_good_result -= 1
            if self.count == 0:
                self.condition.notify_all()

    def wait(self):
        with self.condition:
            if self.count == 0 or self.count_good_result == 0:
                return
            self.condition.wait()


secondaries = [{'name': 'secondary-1', 'address': 'http://secondary-1:8081'},
                {'name': 'secondary-2', 'address': 'http://secondary-2:8082'}]
memory_list = list()
id_count = 1
FORMAT = '%(asctime)s %(message)s'
logging.basicConfig(format=FORMAT, level=0)


def send_to_secondary(latch, secondary, value):
    logging.info('Replication to ' + secondary['name'] + ' ' + secondary['address'] + ': '
                 + 'id=' + str(value['id']) + ' msg=' + value['msg'])
    result = requests.post(secondary['address'], json=json.dumps(value)).ok
    latch.count_down(result)
    if result:
        logging.info('Replication to ' + secondary['name'] + ' completed')
    else:
        logging.error('Error with replication to ' + secondary['name'])
    return result


def message_handler(data):
    global id_count
    new_value = {'id': id_count, 'msg': data['value']}
    memory_list.append(new_value)
    logging.info('Write in memory: id=' + str(id_count) + ' msg=' + data['value'])
    id_count += 1

    latch = CountDownLatch(len(secondaries), int(data['w']) - 1)
    print(latch.count, latch.count_good_result)
    for secondary in secondaries:
        thread = Thread(target=send_to_secondary, args=(latch, secondary, new_value))
        thread.start()
    latch.wait()
    print(latch.count, latch.count_good_result)

    if latch.count_good_result > 0:
        return False
    return True


class RequestHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-Type', 'text/html')
        self.end_headers()
        if len(memory_list) == 0:
            self.wfile.write("Empty list".encode())
        response = ""
        for row in memory_list:
            response +=  str(row['id']) + ' ' + row['msg'] + '\n'
        self.wfile.write(response.encode())

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        body = self.rfile.read(content_length)
        data = json.loads(json.loads(body))
        if message_handler(data):
            self.send_response(200)
        else:
            self.send_response(408)
        self.send_response(200)
        self.end_headers()


def main():
    port = 8080
    print('Listening on localhost:%s' % port)
    server = HTTPServer(('', port), RequestHandler)
    server.serve_forever()


if __name__ == "__main__":
    main()