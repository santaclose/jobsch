import sys
import time

in_files = []
out_files = []

for arg in sys.argv:
	if arg[:3] == "in=":
		in_files = arg[3:].split(",")
	elif arg[:4] == "out=":
		out_files = arg[4:].split(",")

dependencies_ok = True
for f in in_files:
	with open(f, 'r') as dependency_file:
		dependencies_ok = dependencies_ok and dependency_file.read() == 'asdf'

if len(sys.argv) > 1:
	time.sleep(int(sys.argv[1]))
else:
	time.sleep(5)


if dependencies_ok:
	for f in out_files:
		with open(f, 'w') as dependency_file:
			dependency_file.write("asdf")
	print("ALL OK")
else:
	print("FAILURE")