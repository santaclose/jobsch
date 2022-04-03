import scheduler
def try_file(file_path):
	with open(file_path, 'r') as file:
			job_object = json.loads(file.read())
	return scheduler.get_number_of_required_workers_for_job(job_object)

import json
print(try_file("job.json"))
