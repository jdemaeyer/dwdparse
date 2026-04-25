import json
import logging
import urllib.request


def configure_logging():
    log_fmt = '%(asctime)s %(name)s %(levelname)s  %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=log_fmt)
    # Disable some third-party noise
    logging.getLogger('urllib3').setLevel(logging.WARNING)


def dump_records(it):
    for record in it:
        print(json.dumps(record, default=str))


def fetch(url, timeout=10):
    with urllib.request.urlopen(url, timeout=timeout) as f:
        return f.read()
