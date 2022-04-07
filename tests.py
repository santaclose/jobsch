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
			"worker": "1",
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
			"worker": "0",
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
					"worker": "server 0",
					"command": ["python", "testing/dummy.py", "2"]
				},
				{
					"type": "execute",
					"worker": "client 0",
					"command": ["python", "testing/dummy.py", "4"]
				},
				{
					"type": "execute",
					"worker": "client 1",
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
					"worker": "server 0",
					"command": ["python", "testing/dummy.py", "5"]
				},
				{
					"type": "execute",
					"worker": "client 0",
					"command": ["python", "testing/dummy.py", "5"]
				},
				{
					"type": "execute",
					"worker": "client 1",
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
					"worker": "server 0",
					"command": ["python", "testing/dummy.py", "1"]
				},
				{
					"type": "execute",
					"worker": "client 0",
					"command": ["python", "testing/dummy.py", "1"]
				},
				{
					"type": "execute",
					"worker": "client 1",
					"command": ["python", "testing/dummy.py", "1"]
				}
			]
		}
	]
}
"""


# def test_jump_to_next_parallel_branch():
job_object = json.loads(job_json)
scheduler.active_jobs = {"asdf": "asdf"}

res = scheduler.jump_to_next_parallel_branch("asdf", (0,1,1), job_object)
assert res == (7,2,2)
res = scheduler.jump_to_next_parallel_branch("asdf", (1,1,1), job_object)
assert res == (7,2,2)
res = scheduler.jump_to_next_parallel_branch("asdf", (2,1,1), job_object)
assert res == (7,2,2)

res = scheduler.jump_to_next_parallel_branch("asdf", (3,2,2), job_object)
assert res == (5,2,2)
res = scheduler.jump_to_next_parallel_branch("asdf", (3,2,2), job_object)
assert res == (5,2,2)
res = scheduler.jump_to_next_parallel_branch("asdf", (4,2,2), job_object)
assert res == (5,2,2)
res = scheduler.jump_to_next_parallel_branch("asdf", (5,2,2), job_object)
assert res == (5,2,2)


# def test_parallel_group_completed():
job_object = json.loads(job_json)
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
scheduler.job_completed = {"asdf": set([(1,1,1), (2,1,1), (3,1,1), (6,1,2)])}
res = scheduler.has_parallel_group_completed_execution("asdf", (3,1,2), job_object)
assert res == False


# def test_state_in_state():
job_object = json.loads(job_json)
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
res = scheduler.state_is_in_state("asdf", (5,1,2), (1,1,2), job_object)
assert res == False
res = scheduler.state_is_in_state("asdf", (2,1,2), (1,1,2), job_object)
assert res == True
res = scheduler.state_is_in_state("asdf", (3,1,2), (1,1,2), job_object)
assert res == True
res = scheduler.state_is_in_state("asdf", (4,1,2), (1,1,2), job_object)
assert res == True