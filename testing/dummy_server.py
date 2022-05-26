import os
import sys
import json
import flask
import socket
import requests
import threading
import subprocess

lock = threading.Lock()

app = flask.Flask(__name__)

clients = []

def on_client_connected(client):

	print(f"[dummy_server] Detected new client: {client}")

	with lock:
		clients.append(client)
		if len(clients) == clients_expected:
			print("[dummy_server] Shutting down everything")
			for c in clients:
				try:
					requests.get(f'http://{c}/', timeout=0.001) # tell client to exit
				except:
					pass
			print("[dummy_server] Exiting")
			os._exit(0)


@app.route('/', methods=['POST'])
def endpoint():

	json_object = flask.request.json
	client = f"{json_object['host']}:{json_object['port']}"
	
	x = threading.Thread(target=on_client_connected, args=(client,))
	x.start()

	return "", 200


if __name__ == '__main__':
	if len(sys.argv) < 3:
		print("Run: dummy_server.py port clients_expected")
		exit()
		
	host = socket.gethostbyname(socket.gethostname())
	port = sys.argv[1]
	clients_expected = int(sys.argv[2])

	app.run(host='0.0.0.0', port=port)