import os
import sys
import json
import time
import flask
import signal
import psutil
import hashlib
import requests
import threading
import importlib
import subprocess

app = flask.Flask(__name__)

host = None
port = None

active_processes = set()
killing_processes = False

lock = threading.Lock()

current_job = None


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
	global current_job

	with lock:
		if current_job is None or current_job != run_object["job_id"]:
			current_job = run_object["job_id"]
			print(f"[worker] --------- SWITCHED TO JOB {current_job} ---------")

	# give subprocess info so it can find other stuff executed by other workers
	process_env = os.environ.copy()
	process_env["worker_role"] = run_object["role"]
	process_env["worker_job_id"] = run_object["job_id"]
	process_env["scheduler_host"] = scheduler_host
	process_env["scheduler_port"] = scheduler_port

	command_string = ' '.join(run_object['command']) if isinstance(run_object['command'], list) else run_object['command']

	print(f"[worker] Starting process: '{command_string}'")

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

	# these exceptions are not under our control
	except Exception as e:
		print(f"[worker] Unexpected exception: '{repr(e)}'")

	# tell scheduler we completed chunk of work
	request_json = {
		'job_id': run_object["job_id"],
		'state': run_object["state"],
		'host': host,
		'port': port
	}
	requests.put(f'http://{scheduler_host}:{scheduler_port}/jobs', json=request_json)
	print("[worker] Let scheduler know work has been completed")


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


@app.route("/suits_roles")
def suits_roles():
	roles = eval(flask.request.args.get('roles'))
	assert isinstance(roles, list)

	if not os.path.exists("suits_role.py"):
		return json.dumps({"result": [True for x in roles]}, indent=4)

	import suits_role
	importlib.reload(suits_role)
	result = [suits_role.can_work_as(x) for x in roles]
	
	return json.dumps({"result": result}, indent=4)


def update_thread():
	time.sleep(0.1) # wait for the http response to be sent
	os.execl(sys.executable, *([sys.executable] + sys.argv))

@app.route("/update", methods=['POST'])
def update():
	if len(active_processes) > 0:
		return "Cannot update while subprocesses are running", 409
	if 'file' not in flask.request.files:
		return "", 400
	print(f"[worker] Updating")
	updated_file = flask.request.files['file']
	updated_file_contents = updated_file.read()
	print(f"[worker] Received update file with length: {len(updated_file_contents)}")
	with open("worker.py", 'wb') as worker_file:
		worker_file.write(updated_file_contents)
	x = threading.Thread(target=update_thread)
	x.start()
	return "", 200


@app.route("/")
def hello_world():
	with open("worker.py", 'rb') as worker_file:
		file_md5_hash = hashlib.md5(worker_file.read()).hexdigest()
	return json.dumps({"worker_file_md5": file_md5_hash})


if __name__ == '__main__':
	if len(sys.argv) < 5:
		print("[worker] Run: worker.py host port scheduler_host scheduler_port")
		exit()

	host = sys.argv[1]
	port = sys.argv[2]
	scheduler_host = sys.argv[3]
	scheduler_port = sys.argv[4]

	# tell scheduler worker is alive
	requests.post(f'http://{scheduler_host}:{scheduler_port}/workers', json={'host': host, 'port': port})

	app.run(host='0.0.0.0', port=port)
