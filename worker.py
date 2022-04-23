import os
import sys
import json
import time
import flask
import socket
import signal
import psutil
import requests
import threading
import subprocess

app = flask.Flask(__name__)

host = None
port = None

active_processes = set()
killing_processes = False

lock = threading.Lock()


def kill_process(pid):
	children = psutil.Process(pid).children(recursive=True)
	for child in children:
		child.kill()
	os.kill(pid, signal.SIGTERM)
	return len(children)


# Can't use popen timeout cause it doesn't kill children processes
def timeout_thread(process, timeout):
	time.sleep(timeout)
	if not psutil.pid_exists(process.pid):
		return

	children_killed = kill_process(process.pid)
	print(f"[worker] Process was killed because of timeout, {children_killed} children processes terminated")


def run_from_object(run_object):
	global active_processes
	global killing_processes

	# give subprocess info so it can find other stuff executed by other workers
	process_env = os.environ.copy()
	process_env["worker_job_id"] = run_object["job_id"]
	process_env["scheduler_host"] = scheduler_host
	process_env["scheduler_port"] = scheduler_port

	command_string = ' '.join(run_object['command']) if isinstance(run_object['command'], list) else run_object['command']

	print(f"[worker] Starting process for command '{command_string}'")

	try:
		process = subprocess.Popen(run_object["command"], env=process_env)

		with lock:
			active_processes.add(process)

		if "timeout" in run_object.keys():
			x = threading.Thread(target=timeout_thread, args=(process, run_object["timeout"],))
			x.start()

		process.wait()

		with lock:
			active_processes.remove(process)
			if killing_processes:
				return

	except Exception as e:
		print(f"[worker] Unexpected exception '{repr(e)}'")

	# tell scheduler we completed chunk of work
	request_json = {
		'job_id': run_object["job_id"],
		'state': run_object["state"],
		'host': host,
		'port': port
	}
	requests.put(f'http://{scheduler_host}:{scheduler_port}/jobs', json=request_json)
	print("[worker] Put request to scheduler done")


def run_from_file(file_path):
	with open(file_path, "r") as file:
		run_object = json.loads(file.read())
	run_from_object(run_object)


@app.route("/run", methods=['POST', 'DELETE'])
def run():
	global active_processes
	global killing_processes

	if flask.request.method == 'DELETE':
		with lock:
			killing_processes = True
			temp_list = list(active_processes)
		for p in temp_list:
			children_killed = kill_process(p.pid)
			print(f"[worker] Process was killed to cancel job, {children_killed} children processes terminated")
		while True:
			with lock:
				all_process_killed = len(active_processes) == 0
			if all_process_killed:
				break
		with lock:
			killing_processes = False
		return "", 200
	else:
		json_object = flask.request.json
		x = threading.Thread(target=run_from_object, args=(json_object,))
		x.start()
		return "", 200


@app.route("/")
def hello_world():
	return "Hello from worker"


if __name__ == '__main__':
	if len(sys.argv) < 4:
		print("[worker] Run: worker.py port scheduler_host scheduler_port")
		exit()
		
	host = socket.gethostbyname(socket.gethostname())
	port = sys.argv[1]
	scheduler_host = sys.argv[2]
	scheduler_port = sys.argv[3]

	# tell scheduler worker is alive
	requests.post(f'http://{scheduler_host}:{scheduler_port}/workers', json={'host': host, 'port': port})

	app.run(host='0.0.0.0', port=port)