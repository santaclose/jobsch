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


def has_job_completed_execution(job_id, job_object=None, is_root_call=True, current_state=(0,0,0)):

	assert job_id in active_jobs.keys()
	if job_object is None:
		job_object = active_jobs[job_id]

	if job_object["type"] == "execute":
		current_state = (current_state[0] + 1, current_state[1], current_state[2])
		if current_state not in job_completed[job_id]:
			return False, current_state
	elif job_object["type"] == "sequence_group":
		current_state = (current_state[0], current_state[1] + 1, current_state[2])
		for i in range(len(job_object["work"])):
			res, current_state = has_job_completed_execution(job_id, job_object["work"][i], False, current_state)
			if not res:
				if is_root_call:
					return False
				else:
					return False, current_state
	elif job_object["type"] == "parallel_group":
		current_state = (current_state[0], current_state[1], current_state[2] + 1)
		for i in range(len(job_object["work"])):
			res, current_state = has_job_completed_execution(job_id, job_object["work"][i], False, current_state)
			if not res:
				if is_root_call:
					return False
				else:
					return False, current_state

	if is_root_call:
		return True
	else:
		return True, current_state


def has_parallel_group_completed_execution(job_id, parallel_group_state, job_object=None, is_root_call=True, current_state=(0,0,0)):

	assert job_id in active_jobs.keys()
	if job_object is None:
		job_object = active_jobs[job_id]

	if job_object["type"] == "execute":
		current_state = (current_state[0] + 1, current_state[1], current_state[2])
		if is_root_call:
			return None
		return None, current_state

	elif job_object["type"] == "sequence_group":
		current_state = (current_state[0], current_state[1] + 1, current_state[2])
		for i in range(len(job_object["work"])):
			res, current_state = has_parallel_group_completed_execution(job_id, parallel_group_state, job_object["work"][i], False, current_state)
			if res is not None:
				if is_root_call:
					return res
				return res, current_state

	elif job_object["type"] == "parallel_group":
		current_state = (current_state[0], current_state[1], current_state[2] + 1)
		if current_state == parallel_group_state:
			for i in range(len(job_object["work"])):
				child_res, current_state = has_job_completed_execution(job_id, job_object["work"][i], False, current_state)
				if not child_res:
					if is_root_call:
						return False
					return False, current_state
			if is_root_call:
				return True
			return True, current_state

		else:
			for i in range(len(job_object["work"])):
				res, current_state = has_parallel_group_completed_execution(job_id, parallel_group_state, job_object["work"][i], False, current_state)
				if res is not None:
					if is_root_call:
						return res
					return res, current_state

	if not is_root_call:
		return None, current_state
	assert False


def state_is_in_state(job_id, child, parent, job_object=None, is_root_call=True, current_state=(0,0,0), entered_parent_state=False):

	assert job_id in active_jobs.keys()
	if job_object is None:
		job_object = active_jobs[job_id]

	if job_object["type"] == "execute":
		current_state = (current_state[0] + 1, current_state[1], current_state[2])

		if current_state == parent:
			entered_parent_state = True

		if current_state == child and entered_parent_state:
			if is_root_call:
				return True
			return True, current_state

	else:
		current_state = (current_state[0], current_state[1] + (1 if job_object["type"] == "sequence_group" else 0), current_state[2] + (1 if job_object["type"] == "parallel_group" else 0))

		if current_state == parent:
			entered_parent_state = True

		if current_state == child and entered_parent_state:
			if is_root_call:
				return True
			return True, current_state

		for i in range(len(job_object["work"])):
			res, current_state = state_is_in_state(job_id, child, parent, job_object["work"][i], False, current_state, entered_parent_state)
			if res:
				if is_root_call:
					return True
				return True, current_state

	if is_root_call:
		return False
	return False, current_state


def get_required_workers_for_job(job_object, is_root_call=True, worker_set=set()):

	if is_root_call:
		worker_set = set()

	if "worker" in job_object.keys():
		worker_set.add(job_object["worker"])

	if job_object["type"] != "execute":
		for i in range(len(job_object["work"])):
			worker_set = get_required_workers_for_job(job_object["work"][i], False, worker_set)

	if is_root_call and len(worker_set) == 0:
		worker_set.add("default_worker_name")
	return worker_set


def jump_to_next_parallel_branch(job_id, from_location, job_object=None, is_root_call=True, current_state=(0,0,0), found_at_parallel_group=None, current_parallel_group=None, result=None):

	assert job_id in active_jobs.keys()
	if job_object is None:
		job_object = active_jobs[job_id]

	if job_object["type"] == "execute":
		current_state = (current_state[0] + 1, current_state[1], current_state[2])
		if current_state == from_location:
			found_at_parallel_group = current_parallel_group
		if is_root_call:
			return result
		return current_state, found_at_parallel_group, result

	elif job_object["type"] == "sequence_group":
		current_state = (current_state[0], current_state[1] + 1, current_state[2])
		if current_state == from_location:
			found_at_parallel_group = current_parallel_group
		for i in range(len(job_object["work"])):
			current_state, found_at_parallel_group, result = jump_to_next_parallel_branch(job_id, from_location, job_object["work"][i], False, current_state, found_at_parallel_group, current_parallel_group, result)
		if is_root_call:
			return result
		return current_state, found_at_parallel_group, result

	elif job_object["type"] == "parallel_group":
		current_state = (current_state[0], current_state[1], current_state[2] + 1)
		current_parallel_group_ = hash_job(job_object)
		if current_state == from_location:
			found_at_parallel_group = current_parallel_group_
		for i in range(len(job_object["work"])):
			current_state, found_at_parallel_group, result = jump_to_next_parallel_branch(job_id, from_location, job_object["work"][i], False, current_state, found_at_parallel_group, current_parallel_group_, result)
			if found_at_parallel_group == current_parallel_group_ and result is None:
				result = current_state
		if is_root_call:
			return result
		return current_state, found_at_parallel_group, result




def try_to_delegate_for_job(job_id, last_completed_state=None, job_object=None, current_state=(0,0,0), worker_name="default_worker_name"):

	# None means skipping delegation
	# True means we were able to delegate
	# False means we were not able to delegate

	assert job_id in active_jobs.keys()
	if job_object is None:
		job_object = active_jobs[job_id]

	if "worker" in job_object.keys():
		worker_name = job_object["worker"]


	if job_object["type"] == "execute":

		current_state = (current_state[0] + 1, current_state[1], current_state[2])

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
			return True, current_state, worker_name # could delegate
		else:
			print(f"[scheduler] Skipping already delegated state: {current_state}")
			return None, current_state, worker_name # don't have to delegate


	if job_object["type"] == "sequence_group":

		current_state = (current_state[0], current_state[1] + 1, current_state[2])

		for i in range(len(job_object["work"])):
			could_delegate, current_state, child_worker_name = try_to_delegate_for_job(job_id, last_completed_state, job_object["work"][i], current_state, worker_name)
			if could_delegate is not None:
				return could_delegate, current_state, worker_name

		# sequence work has been completed if we reach this point
		return None, current_state, worker_name

	if job_object["type"] == "parallel_group":

		current_state = (current_state[0], current_state[1], current_state[2] + 1)

		if current_state not in job_delegated[job_id]:
			parallel_group_state = current_state
			delegation_results = []
			for i in range(len(job_object["work"])):
				delegation_result = try_to_delegate_for_job(job_id, last_completed_state, job_object["work"][i], current_state, worker_name)
				delegation_results.append(delegation_result)
				current_state = jump_to_next_parallel_branch(job_id, delegation_result[1]) # skip contents of branch of parallel work

			assert all(x[0] == True for x in delegation_results)
			job_delegated[job_id].add(parallel_group_state) # special usage of job_delegated for parallel groups
			return True, current_state, worker_name

		else:
			if has_parallel_group_completed_execution(job_id, current_state):
				for i in range(len(job_object["work"])): # skip all states in parallel branches
					child_state = (current_state[0] + (1 if job_object["work"][i]["type"] == "execute" else 0), current_state[1] + (1 if job_object["work"][i]["type"] == "sequence_group" else 0), current_state[2] + (1 if job_object["work"][i]["type"] == "parallel_group" else 0))
					current_state = jump_to_next_parallel_branch(job_id, child_state) # skip contents of branch of parallel work
				return None, current_state, worker_name
			else:
				for i in range(len(job_object["work"])):
					child_state = (current_state[0] + (1 if job_object["work"][i]["type"] == "execute" else 0), current_state[1] + (1 if job_object["work"][i]["type"] == "sequence_group" else 0), current_state[2] + (1 if job_object["work"][i]["type"] == "parallel_group" else 0))
					assert last_completed_state is not None

					# find which branch we are in
					if not state_is_in_state(job_id, last_completed_state, child_state):
						current_state = jump_to_next_parallel_branch(job_id, child_state) # skip contents of branch of parallel work
						print(f"[scheduler] Skipping parallel group branch at {child_state} as it doesn't contain {last_completed_state}")
						continue
					could_delegate, current_state, child_worker_name = try_to_delegate_for_job(job_id, last_completed_state, job_object["work"][i], current_state, worker_name)
					
					if could_delegate is None: # whole branch has been completely delegated
						print("[scheduler] Not delegating work")
						return False, current_state, worker_name

					return could_delegate, current_state, worker_name


	print("[scheduler] |||||||||||||| ISSUE ||||||||||||||")
	assert False


def try_to_start_jobs():

	for i in reversed(range(len(pending_jobs_order))):

		job_id = pending_jobs_order[i]
		job_object = pending_jobs[job_id]

		required_worker_names = get_required_workers_for_job(job_object)
		number_of_required_workers = len(required_worker_names)
		if len(available_workers) < number_of_required_workers:
			continue

		# Start working on this job
		print(f"[scheduler] --------- STARTING JOB {job_id} ---------")
		active_jobs[job_id] = pending_jobs[job_id]
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
		print(f"[scheduler] Job completed updated: {list(job_completed[job_id])}")

		job_worker_status[job_id][worker].remove(state)

		if len(job_worker_status[job_id][worker]) == 0:
			print(f"[scheduler] Worker '{worker}' not busy anymore")


		could_delegate, current_state, worker_name = try_to_delegate_for_job(job_id, state)
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
	app.run(host='0.0.0.0', port=666)