import scheduler
import json

job_json = """
{
	"type": "parallel_group",
	"work":
	[
		{
			"type": "sequence_group",
			"work":
			[
				{
					"type": "execute",
					"command": ["python", "dummy.py", "1"]
				},
				{
					"type": "execute",
					"command": ["python", "dummy.py", "1"]
				},
				{
					"type": "execute",
					"command": ["python", "dummy.py", "1"]
				},
				{
					"type": "parallel_group",
					"work":
					[
						{
							"type": "sequence_group",
							"work":
							[
								{
									"type": "execute",
									"command": ["python", "dummy.py", "1"]
								},
								{
									"type": "execute",
									"command": ["python", "dummy.py", "1"]
								}
							]
						},
						{
							"type": "execute",
							"command": ["python", "dummy.py", "1"]
						}
					]
				},
				{
					"type": "execute",
					"command": ["python", "dummy.py", "1"]
				}
			]
		},
		{
			"type": "execute",
			"command": ["python", "dummy.py", "10"]
		}
	]
}

"""

another_job_json = """
{
	"type": "parallel_group",
	"work":
	[
		{
			"type": "sequence_group",
			"role": "1",
			"work":
			[
				{
					"type": "execute",
					"command": ["python", "testing/dummy.py", "1", "out=testing/a.txt"]
				},
				{
					"type": "parallel_group",
					"work":
					[
						{
							"type": "execute",
							"command": ["python", "testing/dummy.py", "3", "in=testing/a.txt", "out=testing/b.txt"]
						},
						{
							"type": "execute",
							"command": ["python", "testing/dummy.py", "10", "in=testing/a.txt", "out=testing/c.txt"]
						},
						{
							"type": "execute",
							"command": ["python", "testing/dummy.py", "1", "in=testing/a.txt", "out=testing/d.txt"]
						}
					]
				},
				{
					"type": "execute",
					"command": ["python", "testing/dummy.py", "1", "in=testing/a.txt,testing/b.txt,testing/c.txt,testing/d.txt"]
				}
			]
		},
		{
			"type": "sequence_group",
			"role": "0",
			"work":
			[
				{
					"type": "execute",
					"command": ["python", "testing/dummy.py", "2", "out=testing/0.txt"]
				},
				{
					"type": "execute",
					"command": ["python", "testing/dummy.py", "2"]
				},
				{
					"type": "execute",
					"command": ["curl", "https://raw.githubusercontent.com/santaclose/noose/master/premake5.lua", "--output", "testing/premake5.lua"]
				}
			]
		}
	]
}
"""

yet_another_job_json="""
{
	"type": "sequence_group",
	"work":
	[
		{
			"type": "parallel_group",
			"work":
			[
				{
					"type": "execute",
					"role": "server 0",
					"command": ["python", "testing/dummy.py", "2"]
				},
				{
					"type": "execute",
					"role": "client 0",
					"command": ["python", "testing/dummy.py", "4"]
				},
				{
					"type": "execute",
					"role": "client 1",
					"command": ["python", "testing/dummy.py", "3"]
				}
			]
		},
		{
			"type": "parallel_group",
			"work":
			[
				{
					"type": "execute",
					"role": "server 0",
					"command": ["python", "testing/dummy.py", "5"]
				},
				{
					"type": "execute",
					"role": "client 0",
					"command": ["python", "testing/dummy.py", "5"]
				},
				{
					"type": "execute",
					"role": "client 1",
					"command": ["python", "testing/dummy.py", "5"]
				}
			]
		},
		{
			"type": "parallel_group",
			"work":
			[
				{
					"type": "execute",
					"role": "server 0",
					"command": ["python", "testing/dummy.py", "1"]
				},
				{
					"type": "execute",
					"role": "client 0",
					"command": ["python", "testing/dummy.py", "1"]
				},
				{
					"type": "execute",
					"role": "client 1",
					"command": ["python", "testing/dummy.py", "1"]
				}
			]
		}
	]
}
"""

# def test_parallel_group_completed():
job_object = json.loads(job_json)
scheduler.compute_states(job_object)
scheduler.active_jobs = {"asdf": "asdf"}

scheduler.job_completed = {"asdf": set()}
res = scheduler.has_parallel_group_completed_execution("asdf", (3,1,2), job_object)
assert res == False
scheduler.job_completed = {"asdf": set([(4,2,2)])}
res = scheduler.has_parallel_group_completed_execution("asdf", (3,1,2), job_object)
assert res == False
scheduler.job_completed = {"asdf": set([(4,2,2), (5,2,2)])}
res = scheduler.has_parallel_group_completed_execution("asdf", (3,1,2), job_object)
assert res == False
scheduler.job_completed = {"asdf": set([(4,2,2), (5,2,2), (6,2,2)])}
res = scheduler.has_parallel_group_completed_execution("asdf", (3,1,2), job_object)
assert res == True

scheduler.job_completed = {"asdf": set()}
res = scheduler.has_parallel_group_completed_execution("asdf", (0,0,1), job_object)
assert res == False
scheduler.job_completed = {"asdf": set([(1,1,1)])}
res = scheduler.has_parallel_group_completed_execution("asdf", (0,0,1), job_object)
assert res == False
scheduler.job_completed = {"asdf": set([(1,1,1), (2,1,1)])}
res = scheduler.has_parallel_group_completed_execution("asdf", (0,0,1), job_object)
assert res == False
scheduler.job_completed = {"asdf": set([(1,1,1), (2,1,1), (3,1,1)])}
res = scheduler.has_parallel_group_completed_execution("asdf", (0,0,1), job_object)
assert res == False
scheduler.job_completed = {"asdf": set([(1,1,1), (2,1,1), (3,1,1), (4,2,2)])}
res = scheduler.has_parallel_group_completed_execution("asdf", (0,0,1), job_object)
assert res == False
scheduler.job_completed = {"asdf": set([(1,1,1), (2,1,1), (3,1,1), (4,2,2), (5,2,2)])}
res = scheduler.has_parallel_group_completed_execution("asdf", (0,0,1), job_object)
assert res == False
scheduler.job_completed = {"asdf": set([(1,1,1), (2,1,1), (3,1,1), (4,2,2), (5,2,2), (6,2,2)])}
res = scheduler.has_parallel_group_completed_execution("asdf", (0,0,1), job_object)
assert res == False
scheduler.job_completed = {"asdf": set([(1,1,1), (2,1,1), (3,1,1), (4,2,2), (5,2,2), (6,2,2), (7,2,2)])}
res = scheduler.has_parallel_group_completed_execution("asdf", (0,0,1), job_object)
assert res == False
scheduler.job_completed = {"asdf": set([(1,1,1), (2,1,1), (3,1,1), (4,2,2), (5,2,2), (6,2,2), (7,2,2), (8,2,2)])}
res = scheduler.has_parallel_group_completed_execution("asdf", (0,0,1), job_object)
assert res == True

job_object = json.loads(yet_another_job_json)
scheduler.compute_states(job_object)
scheduler.job_completed = {"asdf": set([(1,1,1), (2,1,1), (3,1,1), (6,1,2)])}
res = scheduler.has_parallel_group_completed_execution("asdf", (3,1,2), job_object)
assert res == False


# def test_state_in_state():
job_object = json.loads(job_json)
scheduler.compute_states(job_object)
scheduler.active_jobs = {"asdf": "asdf"}

res = scheduler.state_is_in_state("asdf", (8,2,2), (8,2,2), job_object)
assert res == True
res = scheduler.state_is_in_state("asdf", (4,2,2), (3,2,2), job_object)
assert res == True
res = scheduler.state_is_in_state("asdf", (5,2,2), (3,2,2), job_object)
assert res == True
res = scheduler.state_is_in_state("asdf", (6,2,2), (3,2,2), job_object)
assert res == False
res = scheduler.state_is_in_state("asdf", (3,2,2), (3,1,2), job_object)
assert res == True
res = scheduler.state_is_in_state("asdf", (1,1,1), (0,1,1), job_object)
assert res == True
res = scheduler.state_is_in_state("asdf", (1,1,1), (0,0,1), job_object)
assert res == True
res = scheduler.state_is_in_state("asdf", (0,1,1), (1,1,1), job_object)
assert res == False
res = scheduler.state_is_in_state("asdf", (0,0,1), (1,1,1), job_object)
assert res == False

job_object = json.loads(another_job_json)
scheduler.compute_states(job_object)
res = scheduler.state_is_in_state("asdf", (5,1,2), (1,1,2), job_object)
assert res == False
res = scheduler.state_is_in_state("asdf", (2,1,2), (1,1,2), job_object)
assert res == True
res = scheduler.state_is_in_state("asdf", (3,1,2), (1,1,2), job_object)
assert res == True
res = scheduler.state_is_in_state("asdf", (4,1,2), (1,1,2), job_object)
assert res == True


# def assign_roles_to_workers():
res = scheduler.assign_roles_to_workers(["client 1", "client 2", "server"], {'workerA': [False, True, False], 'workerB': [False, True, False], 'workerC': [False, True, False]})
assert res is None

res = scheduler.assign_roles_to_workers(["client 1", "client 2", "server"], {'workerA': [True, True, True], 'workerB': [True, True, True], 'workerC': [False, False, False]})
assert res is None

res = scheduler.assign_roles_to_workers(["client 1", "client 2", "server"], {'workerA': [True, True, True], 'workerB': [True, True, True], 'workerC': [True, True, True]})
assert res is not None

res = scheduler.assign_roles_to_workers(["client 1", "client 2", "server"], {'workerA': [True, False, False], 'workerB': [False, True, False], 'workerC': [False, False, True]})
assert "'client 1': 'workerA'" in repr(res)
assert "'client 2': 'workerB'" in repr(res)
assert "'server': 'workerC'" in repr(res)

res = scheduler.assign_roles_to_workers(["client 1", "client 2", "server"], {'workerA': [False, False, True], 'workerB': [False, True, False], 'workerC': [True, False, False]})
assert "'client 1': 'workerC'" in repr(res)
assert "'client 2': 'workerB'" in repr(res)
assert "'server': 'workerA'" in repr(res)

res = scheduler.assign_roles_to_workers(["client 1", "client 2", "server"], {'workerA': [False, False, True], 'workerB': [False, True, False], 'workerC': [True, True, True]})
assert "'client 1': 'workerC'" in repr(res)
assert "'client 2': 'workerB'" in repr(res)
assert "'server': 'workerA'" in repr(res)

res = scheduler.assign_roles_to_workers(["client 1", "client 2", "server"], {'workerA': [False, False, True], 'workerB': [True, True, True], 'workerC': [True, False, False]})
assert "'client 1': 'workerC'" in repr(res)
assert "'client 2': 'workerB'" in repr(res)
assert "'server': 'workerA'" in repr(res)

res = scheduler.assign_roles_to_workers(["client 1", "client 2", "server"], {'workerA': [True, True, True], 'workerB': [False, True, False], 'workerC': [True, False, False]})
assert "'client 1': 'workerC'" in repr(res)
assert "'client 2': 'workerB'" in repr(res)
assert "'server': 'workerA'" in repr(res)

res = scheduler.assign_roles_to_workers(["client 1", "client 2"], {'workerA': [False, False], 'workerB': [False, True], 'workerC': [True, False]})
assert "'client 1': 'workerC'" in repr(res)
assert "'client 2': 'workerB'" in repr(res)

res = scheduler.assign_roles_to_workers(["client 1", "client 2"], {'workerA': [True, True], 'workerB': [False, True], 'workerC': [False, False]})
assert "'client 1': 'workerA'" in repr(res)
assert "'client 2': 'workerB'" in repr(res)

res = scheduler.assign_roles_to_workers(["client 1", "client 2"], {'workerA': [True, True], 'workerB': [True, False], 'workerC': [False, False]})
assert "'client 1': 'workerB'" in repr(res)
assert "'client 2': 'workerA'" in repr(res)

res = scheduler.assign_roles_to_workers(["client 1", "client 2", "client 3", "server"], {'workerA': [True, True, True, True], 'workerB': [False, False, False, False], 'workerC': [False, True, True, False], 'workerD': [False, True, True, False], 'workerE': [False, False, False, True]})
assert "workerB" not in repr(res)
assert "'client 1': 'workerA'" in repr(res)
assert "'server': 'workerE'" in repr(res)
assert "'client 2': 'workerC'" in repr(res) or "'client 2': 'workerD'" in repr(res)
assert "'client 3': 'workerC'" in repr(res) or "'client 3': 'workerD'" in repr(res)

res = scheduler.assign_roles_to_workers(["client 1", "client 2", "client 3", "server"], {'workerA': [True, True, True, True], 'workerB': [False, False, False, False], 'workerD': [False, True, True, False], 'workerC': [False, True, True, False], 'workerE': [False, False, False, True]})
assert "workerB" not in repr(res)
assert "'client 1': 'workerA'" in repr(res)
assert "'server': 'workerE'" in repr(res)
assert "'client 2': 'workerC'" in repr(res) or "'client 2': 'workerD'" in repr(res)
assert "'client 3': 'workerC'" in repr(res) or "'client 3': 'workerD'" in repr(res)

res = scheduler.assign_roles_to_workers(["client 1"], {'workerA': [False], 'workerB': [False], 'workerC': [True]})
assert "workerB" not in repr(res)
assert "workerA" not in repr(res)
assert "'client 1': 'workerC'" in repr(res)

res = scheduler.assign_roles_to_workers(["client 1"], {'workerA': [True], 'workerB': [False], 'workerC': [False]})
assert "workerB" not in repr(res)
assert "workerC" not in repr(res)
assert "'client 1': 'workerA'" in repr(res)

res = scheduler.assign_roles_to_workers(["client 1"], {'workerA': [False], 'workerB': [True], 'workerC': [False]})
assert "workerA" not in repr(res)
assert "workerC" not in repr(res)
assert "'client 1': 'workerB'" in repr(res)

print("Tests passed")