import sys
import json
import requests

job_file = "job.json"
if len(sys.argv) > 1:
	job_file = sys.argv[1]

with open(job_file, 'r') as job_file:
	job_object = json.loads(job_file.read())

response = requests.post(f'http://localhost:666/jobs', json=job_object)
print(json.dumps(response.json(), indent=4))