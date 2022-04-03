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