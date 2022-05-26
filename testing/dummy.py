import os
import sys
import time

job_id = os.getenv("worker_job_id")

in_files = []
out_files = []

for arg in sys.argv:
	if arg[:3] == "in=":
		in_files = arg[3:].split(",")
	elif arg[:4] == "out=":
		out_files = arg[4:].split(",")

dependencies_ok = True
for f in in_files:
	if not os.path.exists(f):
		dependencies_ok = False
		break
	with open(f, 'r') as dependency_file:
		dependencies_ok = dependencies_ok and dependency_file.read() == job_id

if len(sys.argv) > 1:
	time.sleep(float(sys.argv[1]))
else:
	time.sleep(5)


if dependencies_ok:
	for f in out_files:
		with open(f, 'w') as dependency_file:
			dependency_file.write(job_id)
	print("[dummy] ALL OK")
else:
	print("[dummy] FAILURE")