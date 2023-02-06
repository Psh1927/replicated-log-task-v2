import requests

print("Send: ", requests.post('http://127.0.0.1:8080', json='{"value":"test", "w":"1"}').ok)
print("Get list from Master\n", requests.get('http://127.0.0.1:8080').text)
print("Get list from Secondary-1:\n", requests.get('http://127.0.0.1:8081').text)
print("Get list from Secondary-2:\n", requests.get('http://127.0.0.1:8082').text)
print("Send test_1: ", requests.post('http://127.0.0.1:8080', json='{"value":"test_1", "w":"1"}').ok)
print("Send test_2: ", requests.post('http://127.0.0.1:8080', json='{"value":"test_2", "w":"1"}').ok)
print("Send test_3: ", requests.post('http://127.0.0.1:8080', json='{"value":"test_3", "w":"1"}').ok)
print("Get list from Master\n", requests.get('http://127.0.0.1:8080').text)
print("Get list from Secondary-1:\n", requests.get('http://127.0.0.1:8081').text)
print("Get list from Secondary-2:\n", requests.get('http://127.0.0.1:8082').text)
print("Send test_4: ", requests.post('http://127.0.0.1:8080', json='{"value":"test_4", "w":"1"}').ok)
print("Send test_5: ", requests.post('http://127.0.0.1:8080', json='{"value":"test_5", "w":"3"}').ok)
print("Send test_6: ", requests.post('http://127.0.0.1:8080', json='{"value":"test_6", "w":"1"}').ok)
print("Send test_7: ", requests.post('http://127.0.0.1:8080', json='{"value":"test_7", "w":"3"}').ok)
print("Get list from Master:\n", requests.get('http://127.0.0.1:8080').text)
print("Get list from Secondary-1:\n", requests.get('http://127.0.0.1:8081').text)
print("Get list from Secondary-2:\n", requests.get('http://127.0.0.1:8082').text)


