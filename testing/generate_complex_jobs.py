import sys
import json
import random
import requests

FADING_FACTOR = 1.3
DUMMY_WAIT_TIME = 0.5

def generate(group_prob, exec_prob, count, job_object=None):

	if job_object is None:
		job_object = {"type": "sequence_group", "work": []}

	for i in range(count):
		is_execute = random.random() < exec_prob
		if is_execute:
			job_object["work"].append({"type": "execute", "command": f"python testing/dummy.py {DUMMY_WAIT_TIME}"})
		else:
			is_sequence = random.random() < group_prob
			job_object["work"].append({"type": "sequence_group" if is_sequence else "parallel_group", "work": []})

			generate(group_prob, exec_prob * FADING_FACTOR, count, job_object["work"][-1])

	return job_object


def add_x_ids(job_object, current_execute_id=0):
	if job_object["type"] == "execute":
		job_object["x_id"] = current_execute_id
		current_execute_id += 1
	else:
		for item in job_object["work"]:
			current_execute_id = add_x_ids(item, current_execute_id)
	return current_execute_id


def add_parents(job_object, parent=None):
	job_object["parent"] = parent
	if job_object["type"] != "execute":
		for item in job_object["work"]:
			add_parents(item, job_object)


def remove_parents(job_object):
	del job_object["parent"]
	if job_object["type"] != "execute":
		for item in job_object["work"]:
			remove_parents(item)


def add_contains(job_object, parent=None):

	if job_object["type"] == "execute":
		if parent is not None:
			parent["contains"].append(job_object["x_id"])
	else:
		job_object["contains"] = []
		for item in job_object["work"]:
			add_contains(item, job_object)
			if item["type"] != "execute":
				job_object["contains"].extend(item["contains"])


def remove_contains(job_object):
	if job_object["type"] != "execute":
		del job_object["contains"]
		for item in job_object["work"]:
			remove_contains(item)


def compute_dependencies_helper(job_object, current_dependencies, is_root_call=True):
	if job_object["parent"] is None:
		return
	elif job_object["parent"]["type"] == "sequence_group":
		index_in_parent = job_object["parent"]["work"].index(job_object)
		if index_in_parent > 0: # has something executing before
			if job_object["parent"]["work"][index_in_parent - 1]["type"] == "execute":
				current_dependencies.append(job_object["parent"]["work"][index_in_parent - 1]["x_id"])
			else:
				current_dependencies.extend(job_object["parent"]["work"][index_in_parent - 1]["contains"])
			compute_dependencies_helper(job_object["parent"]["work"][index_in_parent - 1], current_dependencies, False)
		else:
			compute_dependencies_helper(job_object["parent"], current_dependencies, False)
	else: # is parallel group
		compute_dependencies_helper(job_object["parent"], current_dependencies, False)

	if is_root_call:
		job_object["dep"] = current_dependencies


def compute_dependencies(job_object):
	if job_object["type"] == "execute":
		compute_dependencies_helper(job_object, [])
	else:
		for item in job_object["work"]:
			compute_dependencies(item)


def move_x_id_and_dependencies_to_dummy_execute(job_object):
	if job_object["type"] == "execute":
		job_object["command"] += f" out=testing/{job_object['x_id']}.txt"
		if len(job_object["dep"]) > 0:
			in_part = " in=" + ",".join([f"testing/{x}.txt" for x in job_object["dep"]])
			job_object["command"] += in_part
		del job_object["dep"]
		del job_object["x_id"]
	else:
		for item in job_object["work"]:
			move_x_id_and_dependencies_to_dummy_execute(item)



job_object = generate(0.5, 0.5, 3)

add_x_ids(job_object)

add_contains(job_object)

add_parents(job_object)

compute_dependencies(job_object)

move_x_id_and_dependencies_to_dummy_execute(job_object)

remove_parents(job_object)

remove_contains(job_object)

print(json.dumps(job_object, indent=4))

if len(sys.argv) > 1:
	response = requests.post(f'http://localhost:666/jobs', json=job_object)
	print(json.dumps(response.json(), indent=4))


# dependency algorithm
# S:
# 	X					1 	()									parent is sequence, check if has something above, has nothing, move to parent, parent is root, done
# 	P:
# 		X				2	(1)									parent is parallel, don't check if has something above, move to parent, parent is sequence, check if has something above, has 1, add as dependency, move to 1, parent is sequence, check if has something above, has nothing, move to parent, parent is root, done
# 		X				3 	(1)									parent is parallel, don't check if has something above, move to parent, parent is sequence, check if has something above, has 1, add as dependency, move to 1, parent is sequence, check if has something above, has nothing, move to parent, parent is root, done
# 		S:
# 			X			4 	(1)									parent is sequence, check if has something above, has nothing, move to parent, parent is parallel, don't check if has something above, move to parent, parent is sequence, check if has something above, has 1, add as dependency, move to 1, parent is sequence, check if has something above, has nothing, move to parent, parent is root, done
# 			X			5 	(4,1)								parent is sequence, check if has something above, has 4, add as dependency, move to parent, parent is parallel, don't check if has something above, move to parent, parent is sequence, check if has something above, has 1, add as dependency, move to 1, parent is sequence, check if has something above, has nothing, move to parent, parent is root, done
# 		X				6 	(1)									parent is parallel, don't check if has something above, move to parent, parent is sequence, check if has something above, has 1, add as dependency, move to 1, parent is sequence, check if has something above, has nothing, move to parent, parent is root, done
# 		S:
# 			X			7 	(1)									parent is sequence, check if has something above, has nothing, move to parent, parent is parallel, don't check if has something above, move to parent, parent is sequence, check if has something above, has 1, add as dependency, move to 1, parent is sequence, check if has something above, has nothing, move to parent, parent is root, done
# 			P:
# 				X		8	(7,1)								parent is parallel, don't check if has something above, move to parent, parent is sequence, check if has something above, has 7, add as dependency, move to parent, parent is parallel, don't check if has something above, move to parent, parent is sequence, check if has something above, has 1, add as dependency, move to 1, parent is sequence, check if has something above, has nothing, move to parent, parent is root, done
# 				X		9	(7,1)								parent is parallel, don't check if has something above, move to parent, parent is sequence, check if has something above, has 7, add as dependency, move to parent, parent is parallel, don't check if has something above, move to parent, parent is sequence, check if has something above, has 1, add as dependency, move to 1, parent is sequence, check if has something above, has nothing, move to parent, parent is root, done
# 		X				10	(1)									parent is parallel, don't check if has something above, move to parent, parent is sequence, check if has something above, has 1, add as dependency, move to 1, parent is sequence, check if has something above, has nothing, move to parent, parent is root, done
# 	X					11	(10,9,8,7,6,5,4,3,2,1)				parent is sequence, check if has something above, has parallel group, add all Xs inside that group as dependencies, move to parallel group, parent is sequence, check if has something above, has 1, add as dependency, move to 1, parent is sequence, check if has something above, has nothing, move to parent, parent is root, done
# 	S:
# 		X				12 	(11,10,9,8,7,6,5,4,3,2,1)			parent is sequence, check if has something above, has nothing, move to parent, parent is sequence, check if has something above, has 11, add as dependency, move to 11, parent is sequence, check if has something above, has parallel group, add all Xs inside that parallel group as dependencies, move to parallel group, parent is sequence, check if has something above, has 1, add as dependency, move to 1, parent is sequence, check if has something above, has nothing, move to parent, parent is root, done
# 		X				13 	(12,11,10,9,8,7,6,5,4,3,2,1)		parent is sequence, check if has something above, has 12, add as dependency, move to 12, parent is sequence, check if has something above, has nothing, move to parent, parent is sequence, check if has something above, has 11, add as dependency, move to 11, parent is sequence, check if has something above, has parallel group, add all Xs inside that parallel group as dependencies, move to parallel group, parent is sequence, check if has something above, has 1, add as dependency, move to 1, parent is sequence, check if has something above, has nothing, move to parent, parent is root, done
# 	X					14 	(13,12,11,10,9,8,7,6,5,4,3,2,1)		parent is sequence, check if has something above, has sequence group, add all Xs inside that group as dependencies, move to sequence group, move to parent, parent is sequence, check if has something above, has 11, add as dependency, move to 11, parent is sequence, check if has something above, has parallel group, add all Xs inside that parallel group as dependencies, move to parallel group, parent is sequence, check if has something above, has 1, add as dependency, move to 1, parent is sequence, check if has something above, has nothing, move to parent, parent is root, done
# 	