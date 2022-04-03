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