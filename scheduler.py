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

job_idle_workers = dict()
job_busy_workers = dict()
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

def get_number_of_required_workers_for_job(job_object, is_root_call=True):

	# resolve deeper first
	children = None
	if job_object["type"] != "execute":
		children = [get_number_of_required_workers_for_job(item, False) for item in job_object["work"]]


	if job_object["type"] == "execute":
		if "run_alone" in job_object.keys() and job_object["run_alone"]:
			return 1
		if is_root_call:
			return 1
		else:
			return None

	if job_object["type"] == "parallel_group":
		# if "use_different_workers" in job_object.keys() and job_object["use_different_workers"]:
		# 	temp = [1 if x is None else x for x in children]
		# else:
		# 	temp = [0 if x is None else x for x in children]

		temp = [0 if x is None else x for x in children]
		res = 1 if any(x == 0 for x in temp) else 0
		res += sum(temp)
		return res


	if job_object["type"] == "sequence_group":
		temp = [-1 if x is None else x for x in children]
		max_val = max(temp)
		if max_val == -1:
			if is_root_call:
				return 1
			else:
				return None
		return max_val


def try_to_delegate_for_job(job_id, job_object=None, current_state=(0,0,0)):

	if job_object is None:
		assert job_id in active_jobs.keys()
		job_object = active_jobs[job_id]

	if job_object["type"] == "execute":

		current_state = (current_state[0] + 1, current_state[1], current_state[2])

		if current_state not in job_delegated[job_id]:
			if len(job_idle_workers[job_id]) > 0:
				target_worker = job_idle_workers[job_id].pop()
				target_url = f'http://{target_worker}/run'
				print(f"Delegating work to {target_url}")
				requests.post(target_url, json={'commands': [job_object["command"]], 'state': repr(current_state), 'job_id': job_id})
				job_busy_workers[job_id].add(target_worker)
				job_delegated[job_id].add(current_state)
				return True, current_state # could delegate
			else:
				print(f"No idle workers to delegate work to")
				return False, current_state # couldn't delegate
		else:
			print(f"Skipping already delegated state: {current_state}")
			return None, current_state # don't have to delegate


	if job_object["type"] == "sequence_group":

		current_state = (current_state[0], current_state[1] + 1, current_state[2])

		for i in range(len(job_object["work"])):
			could_delegate, current_state = try_to_delegate_for_job(job_id, job_object["work"][i], current_state)
			if could_delegate is not None:
				return could_delegate, current_state

		# sequence work has been completed if we reach this point
		return None, current_state

	if job_object["type"] == "parallel_group":

		current_state = (current_state[0], current_state[1], current_state[2] + 1)

		if current_state not in job_delegated[job_id]:
			parallel_group_state = current_state
			required_workers_for_job = get_number_of_required_workers_for_job(job_object)
			if len(job_idle_workers[job_id]) >= required_workers_for_job:
				delegation_results = []
				for i in range(len(job_object["work"])):
					could_delegate, current_state = try_to_delegate_for_job(job_id, job_object["work"][i], current_state)
					delegation_results.append(could_delegate)

				assert all(x == True for x in delegation_results)
				job_delegated[job_id].add(parallel_group_state)
				return True, current_state
			else:
				print(f"Not enough idle workers to delegate work to, required {required_workers_for_job}")
				return False, current_state # couldn't delegate

		else:
			for i in range(len(job_object["work"])):
				could_delegate, current_state = try_to_delegate_for_job(job_id, job_object["work"][i], current_state)
				if could_delegate is not None:
					return could_delegate, current_state

			# parallel work has been completed if we reach this point
			return None, current_state

	assert False


def on_worker_finished_work(job_id):
	could_delegate, current_state = try_to_delegate_for_job(job_id)
	if could_delegate is None and has_job_completed_execution(job_id):
		print(f"Finished job: {job_id}")

		assert len(job_busy_workers[job_id]) == 0
		while len(job_idle_workers[job_id]) > 0:
			available_workers.add(job_idle_workers[job_id].pop())

		del job_idle_workers[job_id]
		del job_busy_workers[job_id]
		del job_delegated[job_id]
		del job_completed[job_id]

		del active_jobs[job_id]

		try_to_start_jobs()


def try_to_start_jobs():

	for i in reversed(range(len(pending_jobs_order))):

		job_id = pending_jobs_order[i]
		job_object = pending_jobs[job_id]

		required_workers = get_number_of_required_workers_for_job(job_object)
		if len(available_workers) < required_workers:
			continue

		# Start working on this job
		active_jobs[job_id] = pending_jobs[job_id]
		del pending_jobs[job_id]
		del pending_jobs_order[i]

		chosen_workers = [available_workers.pop() for i in range(required_workers)]

		job_idle_workers[job_id] = set(chosen_workers)
		job_busy_workers[job_id] = set()
		job_delegated[job_id] = set()
		job_completed[job_id] = set()

		try_to_delegate_for_job(job_id)




@app.route('/workers', methods=['GET', 'POST'])
def workers():
	if flask.request.method == 'POST':
		json_object = flask.request.json
		available_workers.add(f"{json_object['host']}:{json_object['port']}")

		try_to_start_jobs()

		return "", 200
	else:
		return json.dumps({"available": list(available_workers), "busy": [list(job_busy_workers[k]) for k in job_busy_workers.keys()], "idle": [list(job_idle_workers[k]) for k in job_idle_workers.keys()]}, indent=4)


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

		job_busy_workers[json_object["job_id"]].remove(f"{json_object['host']}:{json_object['port']}")
		job_idle_workers[json_object["job_id"]].add(f"{json_object['host']}:{json_object['port']}")

		x = threading.Thread(target=on_worker_finished_work, args=(json_object["job_id"],))
		x.start()

		return "", 200
	else:
		return json.dumps({"active": [active_jobs[k] for k in active_jobs.keys()], "pending": [pending_jobs[k] for k in pending_jobs.keys()]}, indent=4)


@app.route("/")
def hello_world():
	return "Hello from scheduler"

if __name__ == '__main__':
	app.run(host='0.0.0.0', port=666)