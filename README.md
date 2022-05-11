# jobsch

this is a job scheduler

## concepts

### job

jobs are commands organized in groups to be executed by one or more workers.

we use json to define and run jobs

simplest job:
```json
{
	"type": "execute",
	"command": ["echo", "asdf"]
}
```

#### sequence groups
they can be used to group work that needs to be executed in sequence.
example:
```json
{
	"type": "sequence_group",
	"work":
	[
		{
			"type": "execute",
			"command": "echo asdf > asdf.txt"
		},
		{
			"type": "execute",
			"command": "cat asdf.txt"
		}
	]
}
```

#### parallel groups
they can be used to group work that needs to be executed in parallel.
example:
```json
{
	"type": "parallel_group",
	"work":
	[
		{
			"type": "execute",
			"command": "echo asdf > asdf.txt"
		},
		{
			"type": "execute",
			"command": "echo zxcv > zxcv.txt"
		}
	]
}
```

parallel and sequence groups can be nested in any way to create complex jobs

#### roles

roles allow you to execute parts of the job on different workers
example:
```json
{
	"type": "parallel_group",
	"work":
	[
		{
			"type": "execute",
			"role": "machine_0",
			"command": "echo asdf > asdf.txt"
		},
		{
			"type": "execute",
			"role": "machine_1",
			"command": "echo zxcv > zxcv.txt"
		}
	]
}
```

- roles are inherited in the job json object from parent to children
- the job will require as many workers as different roles there are in the job
- the role `default_role` will be used for jobs of type "execute" if no role is provided to it or a parent
- workers are allocated before the job starts executing so only one worker plays each role

by default workers will work for any role, but this can be changed for each worker by creating a `suits_role.py` file where the `worker.py` file is and implementing a `can_work_as(role)` function in it
example `suits_role.py` file:
```py
def can_work_as(role):
	if "server" in role:
		return True
	return False
```

#### timeout

a timeout in seconds can be set to jobs of type "execute" to kill the whole process tree if it's still there when the time runs out.

- if no timeout is specified no processes will be killed

### scheduler

has all the logic to assign work to workers.

HTTP API:

- /jobs
	+ post: adds the given json as a job to the scheduler
		- expects: job json
		- returns: json with job_id, something like `{"job_id": "30681e112faddd2eb9ceeebbfeb6611b"}`
	+ delete: cancels the job with the given job id
		- expects: json with job_id, something like  `{"job_id": "30681e112faddd2eb9ceeebbfeb6611b"}`
		- returns: 200 if job was canceled, 400 if job does not exist


### workers

they execute job commands