import os
import sys
import time
import json
import flask
import socket
import requests
import subprocess

app = flask.Flask(__name__)


@app.route('/', methods=['GET'])
def endpoint():

	print("[dummy_client] Exiting")
	os._exit(0)

	return "", 200


if __name__ == '__main__':
	if len(sys.argv) < 3:
		print("Run: dummy_client.py port server_port")
		exit()
		
	host = socket.gethostbyname(socket.gethostname())
	port = sys.argv[1]
	server_port = sys.argv[2]

	worker_job_id = os.environ['worker_job_id']
	scheduler_host = os.environ['scheduler_host']
	scheduler_port = os.environ['scheduler_port']

	# get server worker from scheduler
	workers = requests.get(f"http://{scheduler_host}:{scheduler_port}/workers").json()
	server_worker = workers["working"][worker_job_id]["server"]
	server_worker_host = server_worker.split(':')[0]

	print(f"[dummy_client] Connecting to server: http://{server_worker_host}:{server_port}")
	requests.post(f"http://{server_worker_host}:{server_port}/", json={'host': host, 'port': port})

	app.run(host='0.0.0.0', port=port)