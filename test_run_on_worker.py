import requests
import json

with open("run.json", 'r') as run_file:
	run_object = json.loads(run_file.read())


requests.post(f'http://localhost:555/run', json=run_object)