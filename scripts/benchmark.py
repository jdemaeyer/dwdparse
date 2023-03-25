#!/bin/env python

import json
import logging
import resource
import time

from dwdparse import parse
from dwdparse.stations import StationIDConverter


logging.getLogger().setLevel(logging.ERROR)


# FILENAME = 'stundenwerte_RR_01766_19950901_20211231_hist.zip'
FILENAME = 'Z__C_EDZW_20230325155702_bda01,synop_bufr_GER_999999_999999__MW_084.json.bz2'  # noqa


StationIDConverter.update = lambda *a, **kw: None

base_mem = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024)
print(f"Base memory usage: {base_mem:,} MiB")

start = time.time()
count = 0
for _ in parse(FILENAME):  # noqa
    count += 1
    print(json.dumps(_, default=str))
duration = time.time() - start
max_mem = int(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024)

print(f"Parsed {count:,} records in {duration:.2f} seconds")
print(f"Maximum memory usage: {max_mem:,} MiB")
