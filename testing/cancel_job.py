import sys
import requests

job_id = sys.argv[1]
requests.delete("http://localhost:666/jobs", json={"job_id": job_id})