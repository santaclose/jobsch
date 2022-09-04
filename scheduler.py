import os
import sys
import json
import flask
import hashlib
import requests
import datetime
import threading

app = flask.Flask(__name__)

KNOWN_WORKERS_FILE_PATH = "known_workers.json"
WORKER_AVAILABILITY_TIMEOUT = 5

available_workers = set()

pending_jobs_order = []
pending_jobs = dict() # holds job id and job object
active_jobs = dict() # holds job id and job object

job_workers = dict()
job_roles = dict()
job_worker_status = dict()
job_delegated = dict()
job_completed = dict()

lock = threading.Lock()


def hash_job(job_object):
	return hashlib.md5(json.dumps(job_object, sort_keys=True).encode()).hexdigest()


def compute_states(job_object, current_state=(0,0,0)):

	if job_object["type"] == "execute":
		current_state = (current_state[0] + 1, current_state[1], current_state[2])
		job_object["state"] = current_state

	elif job_object["type"] == "sequence_group":
		current_state = (current_state[0], current_state[1] + 1, current_state[2])
		job_object["state"] = current_state
		for i in range(len(job_object["work"])):
			current_state = compute_states(job_object["work"][i], current_state)

	elif job_object["type"] == "parallel_group":
		current_state = (current_state[0], current_state[1], current_state[2] + 1)
		job_object["state"] = current_state
		for i in range(len(job_object["work"])):
			current_state = compute_states(job_object["work"][i], current_state)

	return current_state


def has_job_completed_execution(job_id, job_object=None):

	if job_object is None:
		assert job_id in active_jobs.keys()
		job_object = active_jobs[job_id]

	current_state = job_object["state"]

	if job_object["type"] == "execute":
		if current_state not in job_completed[job_id]:
			return False

	elif job_object["type"] == "sequence_group":
		for i in range(len(job_object["work"])):
			res = has_job_completed_execution(job_id, job_object["work"][i])
			if not res:
				return False

	elif job_object["type"] == "parallel_group":
		for i in range(len(job_object["work"])):
			res = has_job_completed_execution(job_id, job_object["work"][i])
			if not res:
				return False

	return True


def has_parallel_group_completed_execution(job_id, parallel_group_state, job_object=None, is_root_call=True):

	if job_object is None:
		assert job_id in active_jobs.keys()
		job_object = active_jobs[job_id]

	current_state = job_object["state"]

	if job_object["type"] == "execute":
		return None

	elif job_object["type"] == "sequence_group":
		for i in range(len(job_object["work"])):
			res = has_parallel_group_completed_execution(job_id, parallel_group_state, job_object["work"][i], False)
			if res is not None:
				return res

	elif job_object["type"] == "parallel_group":
		if current_state == parallel_group_state:
			for i in range(len(job_object["work"])):
				child_res = has_job_completed_execution(job_id, job_object["work"][i])
				if not child_res:
					return False
			return True

		else:
			for i in range(len(job_object["work"])):
				res = has_parallel_group_completed_execution(job_id, parallel_group_state, job_object["work"][i], False)
				if res is not None:
					return res

	if not is_root_call:
		return None

	assert False


def state_is_in_state(job_id, child, parent, job_object=None, entered_parent_state=False):

	if job_object is None:
		assert job_id in active_jobs.keys()
		job_object = active_jobs[job_id]

	current_state = job_object["state"]

	if job_object["type"] == "execute":
		if current_state == parent:
			entered_parent_state = True
		if current_state == child and entered_parent_state:
			return True

	else:
		if current_state == parent:
			entered_parent_state = True
		if current_state == child and entered_parent_state:
			return True
		for i in range(len(job_object["work"])):
			res = state_is_in_state(job_id, child, parent, job_object["work"][i], entered_parent_state)
			if res:
				return True

	return False


def get_required_roles_for_job(job_object, roles_set=None, current_role=None):

	if roles_set is None:
		roles_set = set()

	if "role" in job_object.keys():
		current_role = job_object["role"]
		roles_set.add(job_object["role"])

	if job_object["type"] != "execute":
		for i in range(len(job_object["work"])):
			roles_set = get_required_roles_for_job(job_object["work"][i], roles_set, current_role)

	elif current_role is None:
		roles_set.add("default_role")

	return roles_set


def try_to_delegate_for_job(job_id, last_completed_state=None, job_object=None, role="default_role"):

	# return values:
	#  None means skipping delegation
	#  True means we were able to delegate
	#  False means we were not able to delegate

	if job_object is None:
		assert job_id in active_jobs.keys()
		job_object = active_jobs[job_id]

	current_state = job_object["state"]

	if "role" in job_object.keys():
		role = job_object["role"]

	if job_object["type"] == "execute":

		if current_state not in job_delegated[job_id]:
			target_worker = job_roles[job_id][role]
			target_url = f'http://{target_worker}/run'
			print(f"[scheduler] Delegating work {current_state} to worker '{target_worker}' for job '{job_id}'")
			run_object = {
				"command": job_object["command"],
				"state": repr(current_state),
				"job_id": job_id,
				"role": role
			}
			if "timeout" in job_object.keys():
				run_object["timeout"] = job_object["timeout"]
			requests.post(target_url, json=run_object)
			job_worker_status[job_id][target_worker].add(current_state)
			job_delegated[job_id].add(current_state)
			return True # could delegate
		else:
			# print(f"[scheduler] Skipping already delegated state: {current_state}")
			return None # don't have to delegate


	if job_object["type"] == "sequence_group":

		for i in range(len(job_object["work"])):
			could_delegate = try_to_delegate_for_job(job_id, last_completed_state, job_object["work"][i], role)
			if could_delegate is not None:
				return could_delegate

		# sequence work has been completed if we reach this point
		return None

	if job_object["type"] == "parallel_group":

		if current_state not in job_delegated[job_id]:
			parallel_group_state = current_state
			delegation_results = []
			for i in range(len(job_object["work"])):
				could_delegate = try_to_delegate_for_job(job_id, last_completed_state, job_object["work"][i], role)
				delegation_results.append(could_delegate)

			assert all(x for x in delegation_results)
			job_delegated[job_id].add(parallel_group_state) # special usage of job_delegated for parallel groups
			return True

		else:
			if has_parallel_group_completed_execution(job_id, current_state): # skip all states in parallel branches
				return None
			else:
				for i in range(len(job_object["work"])):
					child_state = job_object["work"][i]["state"]
					assert last_completed_state is not None

					# find which branch we are in
					if not state_is_in_state(job_id, last_completed_state, child_state):
						# print(f"[scheduler] Skipping parallel group branch at {child_state} as it doesn't contain {last_completed_state}")
						continue
					
					could_delegate = try_to_delegate_for_job(job_id, last_completed_state, job_object["work"][i], role)
					if could_delegate is None: # whole branch has been completely delegated
						print("[scheduler] Not delegating work")
						return False

					return could_delegate


	print("[scheduler] |||||||||||||| ISSUE ||||||||||||||")
	assert False


def refresh_available_workers():
	temp_list = list(available_workers)
	for worker in temp_list:
		try:
			requests.head(f"http://{worker}", timeout=WORKER_AVAILABILITY_TIMEOUT)
		except:
			available_workers.remove(worker)
			print(f"[scheduler] Worker {worker} not available anymore")


def assign_roles_to_workers(required_roles_list, worker_can_work_as, assignment=None):
	is_root_call = False
	if assignment is None:
		is_root_call = True
		assignment = {}

	role_with_least_possible_workers = None
	workers_for_role_with_least_possible_workers = None
	for i, role in enumerate(required_roles_list):
		possible_workers_for_role = []
		for worker in worker_can_work_as.keys():
			if worker_can_work_as[worker][i]:
				possible_workers_for_role.append(worker)
		if len(possible_workers_for_role) == 0:
			return None if is_root_call else False
		if workers_for_role_with_least_possible_workers is None or len(possible_workers_for_role) < len(workers_for_role_with_least_possible_workers):
			workers_for_role_with_least_possible_workers = possible_workers_for_role
			role_with_least_possible_workers = role
	
	assignment[role_with_least_possible_workers] = workers_for_role_with_least_possible_workers[0]
	if len(required_roles_list) == 1 and required_roles_list[0] == role_with_least_possible_workers: # base case
		return assignment if is_root_call else True

	index_of_role_to_delete = required_roles_list.index(role_with_least_possible_workers)

	required_roles_sub_list = [x for x in required_roles_list if x not in assignment.keys()]
	worker_can_work_as_sub_dict = {}
	for k in worker_can_work_as.keys():
		if k in assignment.values():
			continue
		new_list = worker_can_work_as[k].copy()
		del new_list[index_of_role_to_delete]
		worker_can_work_as_sub_dict[k] = new_list


	res = assign_roles_to_workers(required_roles_sub_list, worker_can_work_as_sub_dict, assignment)
	if is_root_call:
		return assignment if res else None
	return res


def try_to_start_jobs():
	
	refresh_available_workers()
	for i in reversed(range(len(pending_jobs_order))):

		job_id = pending_jobs_order[i]
		job_object = pending_jobs[job_id]

		required_roles = get_required_roles_for_job(job_object)
		if len(available_workers) < len(required_roles):
			print(f"[scheduler] Not enough workers for {len(required_roles)} roles")
			continue

		required_roles_list = list(required_roles)
		worker_can_work_as = {}
		for worker in available_workers:
			worker_can_work_as[worker] = requests.get(f"http://{worker}/suits_roles?roles={required_roles_list}").json()["result"]

		role_assignment = assign_roles_to_workers(required_roles_list, worker_can_work_as)
		if role_assignment is None:
			print(f"[scheduler] No possible way to assign required roles to workers")
			continue

		# Start working on this job
		print(f"[scheduler] --------- STARTING JOB {job_id} ---------")
		print(f"[scheduler] Assigning {len(required_roles)} workers to job")
		active_jobs[job_id] = pending_jobs[job_id]
		compute_states(active_jobs[job_id])

		del pending_jobs[job_id]
		del pending_jobs_order[i]

		job_workers[job_id] = set(role_assignment.values())
		job_roles[job_id] = role_assignment

		job_worker_status[job_id] = dict()
		for worker in role_assignment.values():
			job_worker_status[job_id][worker] = set()
			available_workers.remove(worker)

		job_delegated[job_id] = set()
		job_completed[job_id] = set()

		try_to_delegate_for_job(job_id)


def on_worker_finished_work(job_id, worker, state):
	with lock:

		if job_id not in job_workers:
			print(f"[scheduler] No workers assigned to job '{job_id}', doing nothing, this should only happen when a job is canceled")
			return

		print(f"[scheduler] Worker '{worker}' completed work {state} for job '{job_id}'")
		
		job_completed[job_id].add(state)
		job_worker_status[job_id][worker].remove(state)

		if len(job_worker_status[job_id][worker]) == 0:
			print(f"[scheduler] Worker '{worker}' not busy anymore")

		could_delegate = try_to_delegate_for_job(job_id, state)
		if could_delegate is None and has_job_completed_execution(job_id):
			print(f"[scheduler] --------- FINISHED JOB {job_id} ---------")

			assert all(len(job_worker_status[job_id][k]) == 0 for k in job_worker_status[job_id].keys()) # none of the workers should be busy

			while len(job_workers[job_id]) > 0:
				available_workers.add(job_workers[job_id].pop())

			del job_workers[job_id]
			del job_roles[job_id]
			del job_worker_status[job_id]
			del job_delegated[job_id]
			del job_completed[job_id]

			del active_jobs[job_id]

			try_to_start_jobs()


def on_worker_connected(worker):
	with lock:
		if os.path.exists(KNOWN_WORKERS_FILE_PATH):
			with open(KNOWN_WORKERS_FILE_PATH, 'r+') as known_workers_file:
				known_workers = json.loads(known_workers_file.read())
				known_workers_file.seek(0)
				known_workers = set(known_workers)
				known_workers.add(worker)
				known_workers_file.write(json.dumps(list(known_workers)))
				known_workers_file.truncate()
		else:
			with open(KNOWN_WORKERS_FILE_PATH, 'w') as known_workers_file:
				known_workers_file.write(json.dumps([worker]))

		available_workers.add(worker)
		try_to_start_jobs()


def on_job_added(job_id, job_object):
	with lock:
		pending_jobs[job_id] = job_object
		pending_jobs_order.append(job_id)

		try_to_start_jobs()


def on_job_cancel(job_id):
	with lock:

		if job_id in pending_jobs.keys():
			del pending_jobs[job_id]
			pending_jobs_order.remove(job_id)


		elif job_id in active_jobs.keys():
			# send requests to workers
			for worker in job_workers[job_id]:
				print(f"[scheduler] Stopping worker '{worker}'")
				requests.delete(f'http://{worker}/run')

			print(f"[scheduler] --------- CANCELED JOB {job_id} ---------")

			while len(job_workers[job_id]) > 0:
				available_workers.add(job_workers[job_id].pop())

			del job_workers[job_id]
			del job_roles[job_id]
			del job_worker_status[job_id]
			del job_delegated[job_id]
			del job_completed[job_id]

			del active_jobs[job_id]

			try_to_start_jobs()


@app.route('/workers', methods=['GET', 'POST'])
def workers():
	if flask.request.method == 'POST':
		json_object = flask.request.json
		worker = f"{json_object['host']}:{json_object['port']}"
		
		x = threading.Thread(target=on_worker_connected, args=(worker,))
		x.start()

		return "", 200
	else:
		output_json = {
			"available": list(available_workers),
			"working": {}
		}
		for job_id in job_roles.keys():
			output_json["working"][job_id] = {}
			for role in job_roles[job_id]:
				output_json["working"][job_id][role] = job_roles[job_id][role]

		return json.dumps(output_json, indent=4)


@app.route('/jobs', methods=['GET', 'POST', 'PUT', 'DELETE'])
def jobs():
	if flask.request.method == 'POST':
		job_object = flask.request.json

		job_object["request_time"] = datetime.datetime.utcnow().strftime("%d-%b-%Y (%H:%M:%S.%f)")
		job_id = hash_job(job_object)

		x = threading.Thread(target=on_job_added, args=(job_id, job_object,))
		x.start()

		return json.dumps({"job_id": job_id}, indent=4)
	elif flask.request.method == 'PUT':
		json_object = flask.request.json
		worker = f"{json_object['host']}:{json_object['port']}"

		x = threading.Thread(target=on_worker_finished_work, args=(json_object["job_id"], worker, eval(json_object["state"]),))
		x.start()

		return "", 200
	elif flask.request.method == 'DELETE':
		json_object = flask.request.json
		job_id = json_object["job_id"]

		with lock:
			job_exists = job_id in active_jobs.keys() or job_id in pending_jobs.keys()
		if not job_exists:
			return "", 400

		on_job_cancel(job_id)

		return "", 200
	else:
		return json.dumps({"active": [active_jobs[k] for k in active_jobs.keys()], "pending": [pending_jobs[k] for k in pending_jobs.keys()]}, indent=4)


@app.route("/update_workers", methods=['GET', 'POST'])
def update_workers():
	if flask.request.method == 'GET':
		return '''
		<!doctype html>
		<title>Update workers</title>
		<h1>Upload worker.py file</h1>
		<form method=post enctype=multipart/form-data>
			<input type=file name=file>
			<input type=submit value=Upload>
		</form>
		'''
	else: # POST
		if len(active_jobs.keys()) > 0 or len(pending_jobs.keys()) > 0:
			return "Cannot update while jobs are being executed", 409
		if 'file' not in flask.request.files:
			return "", 400
		refresh_available_workers()
		new_worker_file_bytes = flask.request.files['file'].read()
		temp_list = list(available_workers)
		failed_workers = []
		for worker in temp_list:
			try:
				print(f"[scheduler] Updating worker: {worker}")
				requests.post(f"http://{worker}/update", files={"file": new_worker_file_bytes})
			except Exception as e:
				print(f"[scheduler] Failed to update worker {worker}: {e}")
				failed_workers.append(worker)

		if len(failed_workers) > 0:
			return "Failed to update workers " + ", ".join(failed_workers), 500
		return "", 200


@app.route("/")
def hello_world():
	return "Hello from scheduler"


if __name__ == '__main__':
	if os.path.exists(KNOWN_WORKERS_FILE_PATH):
		print("[scheduler] Loading known workers from file")
		with open(KNOWN_WORKERS_FILE_PATH, 'r+') as known_workers_file:
			known_workers = json.loads(known_workers_file.read())
		for worker in known_workers:
			available_workers.add(worker)
		refresh_available_workers()

	port = sys.argv[1]
	app.run(host='0.0.0.0', port=port)