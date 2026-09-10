import os
from pathlib import Path

import pytest


@pytest.fixture(scope='session')
def data_dir() -> Path:
    return Path(os.path.dirname(__file__)) / 'data'


def pytest_configure(config: pytest.Config) -> None:
    # Dirty mock so we don't download the station list on every test run
    from dwdparse.stations import _converter
    # Must contain all stations that we use in test data
    _converter.dwd_to_wmo = {
        'XXX': 'P0036',
        'YYY': '01049',
        '01766': '10315',
        '04911': '10788',
        '05484': 'M031',
    }
    _converter.wmo_to_dwd = {
        v: k for k, v in _converter.dwd_to_wmo.items()
    }
