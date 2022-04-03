import requests
import json

with open("job.json", 'r') as job_file:
	job_object = json.loads(job_file.read())


requests.post(f'http://localhost:666/jobs', json=job_object)