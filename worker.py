import os
import sys
import json
import flask
import socket
import requests
import threading
import subprocess

app = flask.Flask(__name__)

host = None
port = None


def run_from_object(run_object):

	# give subprocess info so it can find other stuff executed by other workers
	process_env = os.environ.copy()
	process_env["worker_job_id"] = run_object["job_id"]
	process_env["scheduler_host"] = scheduler_host
	process_env["scheduler_port"] = scheduler_port

	print(f"Starting process for command '{' '.join(run_object['command'])}'")
	timed_out = False
	try:
		subprocess.run(run_object["command"], env=process_env, timeout=None if "timeout" not in run_object.keys() else run_object["timeout"])
	except subprocess.TimeoutExpired:
		timed_out = True
	print(f"Process for command '{' '.join(run_object['command'])}' finished.")

	# tell scheduler we completed chunk of work
	request_json = {
		'job_id': run_object["job_id"],
		'state': run_object["state"],
		'host': host,
		'port': port,
		'timed_out': timed_out
	}
	requests.put(f'http://{scheduler_host}:{scheduler_port}/jobs', json=request_json)
	print("Put request to scheduler done:")


def run_from_file(file_path):
	with open(file_path, "r") as file:
		run_object = json.loads(file.read())
	run_from_object(run_object)


@app.route("/run", methods=['POST'])
def run():
	post_object = flask.request.json
	# print("Running:")
	# print(json.dumps(post_object, indent=4))
	x = threading.Thread(target=run_from_object, args=(post_object,))
	x.start()
	return "", 200


@app.route("/")
def hello_world():
	return "Hello from worker"


if __name__ == '__main__':
	if len(sys.argv) < 4:
		print("Run: worker.py port scheduler_host scheduler_port")
		exit()
		
	host = socket.gethostbyname(socket.gethostname())
	port = sys.argv[1]
	scheduler_host = sys.argv[2]
	scheduler_port = sys.argv[3]

	# tell scheduler worker is alive
	requests.post(f'http://{scheduler_host}:{scheduler_port}/workers', json={'host': host, 'port': port})

	app.run(host='0.0.0.0', port=port)