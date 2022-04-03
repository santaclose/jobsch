import sys
import time

if len(sys.argv) > 1:
	time.sleep(int(sys.argv[1]))
else:
	time.sleep(5)