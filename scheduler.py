import sys
import json
import flask
import hashlib
import requests
import datetime
import threading

app = flask.Flask(__name__)

available_workers = set()

pending_jobs_order = []
pending_jobs = dict() # holds job id and job object
active_jobs = dict() # holds job id and job object

job_workers = dict()
job_worker_names = dict()
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


def get_required_workers_for_job(job_object, worker_set=None, current_worker=None):

	if worker_set is None:
		worker_set = set()

	if "worker" in job_object.keys():
		current_worker = job_object["worker"]
		worker_set.add(job_object["worker"])

	if job_object["type"] != "execute":
		for i in range(len(job_object["work"])):
			worker_set = get_required_workers_for_job(job_object["work"][i], worker_set, current_worker)

	elif current_worker is None:
		worker_set.add("default_worker_name")

	return worker_set


def try_to_delegate_for_job(job_id, last_completed_state=None, job_object=None, worker_name="default_worker_name"):

	# return values:
	#  None means skipping delegation
	#  True means we were able to delegate
	#  False means we were not able to delegate

	if job_object is None:
		assert job_id in active_jobs.keys()
		job_object = active_jobs[job_id]

	current_state = job_object["state"]

	if "worker" in job_object.keys():
		worker_name = job_object["worker"]

	if job_object["type"] == "execute":

		if current_state not in job_delegated[job_id]:
			target_worker = job_worker_names[job_id][worker_name]
			target_url = f'http://{target_worker}/run'
			print(f"[scheduler] Delegating work {current_state} to worker '{target_url}'")
			run_object = {
				"command": job_object["command"],
				"state": repr(current_state),
				"job_id": job_id
			}
			if "timeout" in job_object.keys():
				run_object["timeout"] = job_object["timeout"]
			requests.post(target_url, json=run_object)
			job_worker_status[job_id][target_worker].add(current_state)
			job_delegated[job_id].add(current_state)
			return True # could delegate
		else:
			print(f"[scheduler] Skipping already delegated state: {current_state}")
			return None # don't have to delegate


	if job_object["type"] == "sequence_group":

		for i in range(len(job_object["work"])):
			could_delegate = try_to_delegate_for_job(job_id, last_completed_state, job_object["work"][i], worker_name)
			if could_delegate is not None:
				return could_delegate

		# sequence work has been completed if we reach this point
		return None

	if job_object["type"] == "parallel_group":

		if current_state not in job_delegated[job_id]:
			parallel_group_state = current_state
			delegation_results = []
			for i in range(len(job_object["work"])):
				could_delegate = try_to_delegate_for_job(job_id, last_completed_state, job_object["work"][i], worker_name)
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
						print(f"[scheduler] Skipping parallel group branch at {child_state} as it doesn't contain {last_completed_state}")
						continue
					
					could_delegate = try_to_delegate_for_job(job_id, last_completed_state, job_object["work"][i], worker_name)
					if could_delegate is None: # whole branch has been completely delegated
						print("[scheduler] Not delegating work")
						return False

					return could_delegate


	print("[scheduler] |||||||||||||| ISSUE ||||||||||||||")
	assert False


def update_available_workers():
	temp_list = list(available_workers)
	for worker in temp_list:
		try:
			requests.head(f"http://{worker}", timeout=5)
		except:
			available_workers.remove(worker)
			print(f"[scheduler] Worker {worker} not available anymore")


def try_to_start_jobs():
	
	update_available_workers()
	for i in reversed(range(len(pending_jobs_order))):

		job_id = pending_jobs_order[i]
		job_object = pending_jobs[job_id]

		required_worker_names = get_required_workers_for_job(job_object)
		number_of_required_workers = len(required_worker_names)
		if len(available_workers) < number_of_required_workers:
			continue

		# Start working on this job
		print(f"[scheduler] --------- STARTING JOB {job_id} ---------")
		print(f"[scheduler] Assigning {len(required_worker_names)} workers to job")
		active_jobs[job_id] = pending_jobs[job_id]
		compute_states(active_jobs[job_id])

		del pending_jobs[job_id]
		del pending_jobs_order[i]

		chosen_workers = [available_workers.pop() for i in range(len(required_worker_names))]
		job_workers[job_id] = set(chosen_workers)
		for i, worker_name in enumerate(required_worker_names):
			if job_id not in job_worker_names.keys():
				job_worker_names[job_id] = dict()
			job_worker_names[job_id][worker_name] = chosen_workers[i]

		job_worker_status[job_id] = dict()
		for worker in chosen_workers:
			job_worker_status[job_id][worker] = set()

		job_delegated[job_id] = set()
		job_completed[job_id] = set()

		try_to_delegate_for_job(job_id)


def on_worker_finished_work(job_id, worker, state):
	with lock:

		if job_id not in job_workers:
			print(f"[scheduler] No workers assigned to job '{job_id}', doing nothing, this should only happen when a job is canceled")
			return

		print(f"[scheduler] Worker '{worker}' completed work {state}")
		
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
			del job_worker_names[job_id]
			del job_worker_status[job_id]
			del job_delegated[job_id]
			del job_completed[job_id]

			del active_jobs[job_id]

			try_to_start_jobs()


def on_worker_connected(worker):
	with lock:
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
			del job_worker_names[job_id]
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
		for job_id in job_worker_names.keys():
			output_json["working"][job_id] = {}
			for worker_name in job_worker_names[job_id]:
				output_json["working"][job_id][worker_name] = job_worker_names[job_id][worker_name]

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


@app.route("/")
def hello_world():
	return "Hello from scheduler"


if __name__ == '__main__':
	port = sys.argv[1]
	app.run(host='0.0.0.0', port=port)