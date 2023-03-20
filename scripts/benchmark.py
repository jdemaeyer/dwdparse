#!/bin/env python

import logging
import resource
import time

from dwdparse import parse
from dwdparse.utils import StationIDConverter


logging.getLogger().setLevel(logging.ERROR)

StationIDConverter.update = lambda *a, **kw: None

start = time.time()
count = 0
for _ in parse('MOSMIX_S_LATEST_240.kmz'):
    count += 1
duration = time.time() - start
max_mem = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024)

print(f"Parsed {count:,} records in {duration:.2f} seconds")
print(f"Maximum memory usage: {max_mem:,} MiB")
