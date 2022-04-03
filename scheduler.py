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


def hash_job(job_object):
	return hashlib.md5(json.dumps(job_object, sort_keys=True).encode()).hexdigest()


def has_job_completed_execution(job_id, job_object=None, is_root_call=True, current_state=(0,0,0)):

	assert job_id in active_jobs.keys()
	if job_object is None:
		job_object = active_jobs[job_id]

	if job_object["type"] == "execute":
		current_state = (current_state[0] + 1, current_state[1], current_state[2])
		if current_state not in job_completed[job_id]:
			return False, None
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




def try_to_delegate_for_job(job_id, job_object=None, current_state=(0,0,0), worker_name="default_worker_name", delegating_parallel_group=False):

	assert job_id in active_jobs.keys()
	if job_object is None:
		job_object = active_jobs[job_id]

	if "worker" in job_object.keys():
		worker_name = job_object["worker"]


	if job_object["type"] == "execute":

		current_state = (current_state[0] + 1, current_state[1], current_state[2])

		if current_state not in job_delegated[job_id]:
			target_worker = job_worker_names[job_id][worker_name]
			if delegating_parallel_group or len(job_worker_status[job_id][target_worker]) == 0: # check if the worker supposed to run this is not busy
				target_url = f'http://{target_worker}/run'
				print(f"Delegating work to {target_url}")
				requests.post(target_url, json={'command': job_object["command"], 'state': repr(current_state), 'job_id': job_id})
				job_worker_status[job_id][target_worker].add(current_state)
				job_delegated[job_id].add(current_state)
				return True, current_state, worker_name # could delegate
			else:
				print(f"No idle workers to delegate work to")
				return False, current_state, worker_name # couldn't delegate
		else:
			print(f"Skipping already delegated state: {current_state}")
			return None, current_state, worker_name # don't have to delegate


	if job_object["type"] == "sequence_group":

		current_state = (current_state[0], current_state[1] + 1, current_state[2])

		for i in range(len(job_object["work"])):
			could_delegate, current_state, child_worker_name = try_to_delegate_for_job(job_id, job_object["work"][i], current_state, worker_name, delegating_parallel_group)
			if could_delegate is not None:
				return could_delegate, current_state, worker_name

		# sequence work has been completed if we reach this point
		return None, current_state, worker_name

	if job_object["type"] == "parallel_group":

		current_state = (current_state[0], current_state[1], current_state[2] + 1)

		if current_state not in job_delegated[job_id]:
			parallel_group_state = current_state
			parallel_group_workers = [job_worker_names[job_id][x] for x in  get_required_workers_for_job(job_object)]
			if all(len(job_worker_status[job_id][x]) == 0 for x in parallel_group_workers): # check if the workers supposed to run this are not busy
				delegation_results = []
				for i in range(len(job_object["work"])):
					delegation_result = try_to_delegate_for_job(job_id, job_object["work"][i], current_state, worker_name, True)
					delegation_results.append(delegation_result)
					current_state = jump_to_next_parallel_branch(job_id, delegation_result[1]) # skip contents of branch of parallel work

				for could_delegate, child_state, child_worker_name in delegation_results:
					child_worker = job_worker_names[job_id][child_worker_name]
					job_worker_status[job_id][child_worker].add(child_state)
					job_delegated[job_id].add(child_state)

				assert all(x[0] == True for x in delegation_results)
				job_delegated[job_id].add(parallel_group_state) # special usage of job_delegated for parallel groups
				return True, current_state, worker_name
			else:
				print("Workers required to run parallel group were busy, waiting for next event")
				return False, current_state, worker_name # couldn't delegate

		else:
			for i in range(len(job_object["work"])):
				could_delegate, current_state, child_worker_name = try_to_delegate_for_job(job_id, job_object["work"][i], current_state, worker_name, True)
				if could_delegate is not None:
					return could_delegate, current_state, worker_name

			# parallel work has been completed if we reach this point
			return None, current_state, worker_name

	assert False


def on_worker_finished_work(job_id, state):
	could_delegate, current_state, worker_name = try_to_delegate_for_job(job_id)
	if could_delegate is None and has_job_completed_execution(job_id):
		print(f"Finished job: {job_id}")

		assert all(len(job_worker_status[job_id][k]) == 0 for k in job_worker_status[job_id].keys()) # none of the workers should be busy

		while len(job_workers[job_id]) > 0:
			available_workers.add(job_workers[job_id].pop())

		del job_workers[job_id]
		del job_worker_status[job_id]
		del job_delegated[job_id]
		del job_completed[job_id]

		del active_jobs[job_id]

		try_to_start_jobs()


def try_to_start_jobs():

	for i in reversed(range(len(pending_jobs_order))):

		job_id = pending_jobs_order[i]
		job_object = pending_jobs[job_id]

		required_worker_names = get_required_workers_for_job(job_object)
		if len(available_workers) < len(required_worker_names):
			continue

		# Start working on this job
		active_jobs[job_id] = pending_jobs[job_id]
		del pending_jobs[job_id]
		del pending_jobs_order[i]

		chosen_workers = [available_workers.pop() for i in range(len(required_worker_names))]
		job_workers[job_id] = set(chosen_workers)
		for i, item in enumerate(required_worker_names):
			if job_id not in job_worker_names.keys():
				job_worker_names[job_id] = dict()
			job_worker_names[job_id][item] = chosen_workers[i]

		job_worker_status[job_id] = dict()
		for worker in chosen_workers:
			job_worker_status[job_id][worker] = set()

		job_delegated[job_id] = set()
		job_completed[job_id] = set()

		try_to_delegate_for_job(job_id)


@app.route('/workers', methods=['GET', 'POST'])
def workers():
	if flask.request.method == 'POST':
		json_object = flask.request.json
		available_workers.add(f"{json_object['host']}:{json_object['port']}")

		# try_to_start_jobs()
		
		x = threading.Thread(target=try_to_start_jobs)
		x.start()

		return "", 200
	else:
		return json.dumps({"available": list(available_workers), "all": [list(job_workers[k]) for k in job_workers.keys()]}, indent=4)


@app.route('/jobs', methods=['GET', 'POST', 'PUT'])
def jobs():
	if flask.request.method == 'POST':
		json_object = flask.request.json
		json_object["request_time"] = datetime.datetime.utcnow().strftime("%d-%b-%Y (%H:%M:%S.%f)")
		job_id = hash_job(json_object)

		pending_jobs[job_id] = json_object
		pending_jobs_order.append(job_id)

		try_to_start_jobs()

		return "", 200
	elif flask.request.method == 'PUT':
		json_object = flask.request.json
		job_completed[json_object["job_id"]].add(eval(json_object["state"]))
		print(f"Job completed updated: {list(job_completed[json_object['job_id']])}")

		worker = f"{json_object['host']}:{json_object['port']}"

		job_worker_status[json_object["job_id"]][worker].remove(eval(json_object["state"]))

		if len(job_worker_status[json_object["job_id"]][worker]) == 0:
			print(f"Worker '{worker}' not busy anymore")

		x = threading.Thread(target=on_worker_finished_work, args=(json_object["job_id"], eval(json_object["state"])))
		x.start()

		return "", 200
	else:
		return json.dumps({"active": [active_jobs[k] for k in active_jobs.keys()], "pending": [pending_jobs[k] for k in pending_jobs.keys()]}, indent=4)



@app.route("/")
def hello_world():
	return "Hello from scheduler"


if __name__ == '__main__':
	app.run(host='0.0.0.0', port=666)