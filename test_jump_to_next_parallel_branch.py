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

res = scheduler.jump_to_next_parallel_branch("asdf", (0,1,1), job_object)
print(res) # expected 7 2 2
res = scheduler.jump_to_next_parallel_branch("asdf", (1,1,1), job_object)
print(res) # expected 7 2 2
res = scheduler.jump_to_next_parallel_branch("asdf", (2,1,1), job_object)
print(res) # expected 7 2 2

res = scheduler.jump_to_next_parallel_branch("asdf", (3,2,2), job_object)
print(res) # expected 5 2 2
res = scheduler.jump_to_next_parallel_branch("asdf", (3,2,2), job_object)
print(res) # expected 5 2 2
res = scheduler.jump_to_next_parallel_branch("asdf", (4,2,2), job_object)
print(res) # expected 5 2 2
res = scheduler.jump_to_next_parallel_branch("asdf", (5,2,2), job_object)
print(res) # expected 5 2 2