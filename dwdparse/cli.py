import argparse
import logging
import os
import re

from dwdparse.api import parse, parse_url
from dwdparse.stations import StationIDConverter, load_stations
from dwdparse.units import convert_record
from dwdparse.utils import configure_logging, dump_records, fetch


logger = logging.getLogger(__name__)


EPILOG = """
examples:
  # Parse local file
  dwdparse MOSMIX_S_LATEST_240.kmz

  # Parse local file, outputting units used by DWD instead of SI
  dwdparse --units dwd MOSMIX_S_LATEST_240.kmz

  # Download and parse a file from the open data server
  dwdparse https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/precipitation/recent/stundenwerte_RR_01766_akt.zip

  # Parse a local file with conversion of DWD/WMO station IDs, and store a copy
  # of the station list in a file called `stations.html`. Subsequent similar
  # calls will use the stored file instead of downloading from the DWD server.
  dwdparse --load-stations --stations stations.html MOSMIX_S_LATEST_240.kmz

Any problems or thoughts on dwdparse? Open an issue in our GitHub repo at <https://github.com/jdemaeyer/dwdparse>!
"""  # noqa: E501


parser = argparse.ArgumentParser(
    prog='dwdparse',
    description="Parsers for DWD's open weather data.",
    epilog=EPILOG,
    formatter_class=argparse.RawDescriptionHelpFormatter,
)
parser.add_argument(
    '--units',
    choices=['dwd'],
    help='use given measurement units instead of SI',
)
parser.add_argument(
    '--load-stations',
    action='store_true',
    help='download DWD/WMO station ID mappings before parsing',
)
parser.add_argument(
    '--stations',
    help=(
        'path of station list containing DWD/WMO mappings; you can use this '
        'together with --load-stations, in which case dwdparse will download '
        'the list to the supplied path if (and only if) it does not exist'
    ),
)
parser.add_argument(
    'targets',
    help='path or URL of file(s) to be parsed',
    nargs='+',
    metavar='TARGET',
)


def main():
    configure_logging()
    args = parser.parse_args()
    if args.stations:
        if args.load_stations and not os.path.exists(args.stations):
            logger.info("Downloading station list to %s", args.stations)
            with open(args.stations, 'wb') as f:
                f.write(fetch(StationIDConverter.STATION_LIST_URL))
        load_stations(path=args.stations)
    elif args.load_stations:
        load_stations()
    for target in args.targets:
        command = parse_url if re.match(r'^https?://', target) else parse
        records = command(target)
        if args.units:
            records = (convert_record(r, args.units) for r in records)
        dump_records(records)
