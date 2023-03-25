import bz2
import csv
import datetime
import io
import json
import logging
import re
import xml.etree.ElementTree as ET
import zipfile
from contextlib import suppress

from dwdparse.stations import (
    dwd_id_to_wmo,
    wmo_id_to_dwd,
)
from dwdparse.units import (
    celsius_to_kelvin,
    current_observations_weather_code_to_condition,
    eighths_to_percent,
    hpa_to_pa,
    km_to_m,
    kmh_to_ms,
    minutes_to_seconds,
    synop_current_weather_code_to_condition,
    synop_form_of_precipitation_code_to_condition,
    synop_past_weather_code_to_condition,
)


class SkipRecord(Exception):
    pass


class Parser:

    @property
    def logger(self):
        if not hasattr(self, '_logger'):
            name = __name__ + '.' + self.__class__.__name__
            self._logger = logging.getLogger(name)
        return self._logger

    def parse(self, path):
        raise NotImplementedError

    def get_extra_urls(self, path):
        return {}


class MOSMIXSParser(Parser):

    ELEMENTS = {
        'DD': 'wind_direction',
        'FF': 'wind_speed',
        'FX1': 'wind_gust_speed',
        'N': 'cloud_cover',
        'PPPP': 'pressure_msl',
        'RR1c': 'precipitation',
        'SunD1': 'sunshine',
        'Td': 'dew_point',
        'TTT': 'temperature',
        'VV': 'visibility',
        'ww': 'condition',
    }

    def parse(self, path):
        self.logger.info("Parsing %s", path)
        with zipfile.ZipFile(path) as zf:
            infolist = zf.infolist()
            assert len(infolist) == 1, f'Unexpected zip content in {self.path}'
            with zf.open(infolist[0]) as f:
                yield from self._parse_stream(f)

    def _parse_stream(self, f):
        timestamps = None
        source = None
        ns = {}
        for event, element in ET.iterparse(f, ['end', 'start-ns']):
            if event == 'start-ns':
                ns[element[0]] = element[1]
                continue
            elif self._is_tag(element, 'dwd:ProductID', ns):
                assert source is None, "Unexpected extra product ID"
                source = element.text
            elif self._is_tag(element, 'dwd:IssueTime', ns):
                assert source is not None, "Unexpected issue time w/o ID"
                source += ':' + element.text
            elif self._is_tag(element, 'dwd:ForecastTimeSteps', ns):
                assert timestamps is None, "Unexpected extra time steps"
                timestamps = self.parse_timestamps(element, ns)
            elif self._is_tag(element, 'kml:Placemark', ns):
                assert timestamps is not None, "Placemark without time steps"
                assert source is not None, "Placemark without source"
                records = self.parse_station(element, ns, timestamps, source)
                yield from self.sanitize_records(records)
                # XXX: Reduce memory footprint from 1 GB to 30 MB
                element.clear()

    def _is_tag(self, element, tag, ns):
        prefix, tag = tag.split(':')
        return element.tag == f'{{{ns[prefix]}}}{tag}'

    def parse_timestamps(self, steps, ns):
        return [
            datetime.datetime.fromisoformat(re.sub(r'Z$', '+00:00', el.text))
            for el in steps.findall('dwd:TimeStep', ns)
        ]

    def parse_station(self, place, ns, timestamps, source):
        wmo_station_id = place.find('kml:name', ns).text
        dwd_station_id = wmo_id_to_dwd(wmo_station_id)
        station_name = place.find('kml:description', ns).text
        try:
            coords = place.find('kml:Point', ns).find('kml:coordinates', ns)
            lon, lat, height = coords.text.split(',')
        except AttributeError:
            self.logger.warning(
                "Ignoring station without coordinates, WMO ID '%s', DWD ID "
                "'%s', name '%s'",
                wmo_station_id, dwd_station_id, station_name)
            return []
        records = {'timestamp': timestamps}
        data = place.find('kml:ExtendedData', ns)
        for forecast in data.findall('dwd:Forecast', ns):
            param = forecast.attrib[f"{{{ns['dwd']}}}elementName"]
            try:
                column = self.ELEMENTS[param]
            except KeyError:
                continue
            values_str = forecast.find('dwd:value', ns).text
            converter = getattr(self, f'parse_{column}', float)
            # XXX: Roughly 50 % of our parsing time is spent here
            records[column] = [
                None if x == '-' else converter(x)
                for x in re.split(r'\s+', values_str.strip())
            ]
            assert len(records[column]) == len(timestamps)
        base_record = {
            'observation_type': 'forecast',
            'source': source,
            'lat': float(lat),
            'lon': float(lon),
            'height': float(height),
            'dwd_station_id': dwd_station_id,
            'wmo_station_id': wmo_station_id,
            'station_name': station_name,
        }
        # Turn dict of lists into list of dicts
        return (
            {**base_record, **dict(zip(records, row))}
            for row in zip(*records.values())
        )

    def _convert(self, value, converter):
        try:
            return converter(value)
        except ValueError:
            return None

    def parse_condition(self, value):
        code = int(value.split('.')[0])
        return synop_current_weather_code_to_condition(code)

    def sanitize_records(self, records):
        for r in records:
            if r['precipitation'] and r['precipitation'] < 0:
                self.logger.warning(
                    "Ignoring negative precipitation value: %s", r)
                r['precipitation'] = None
            if r['wind_direction'] and r['wind_direction'] > 360:
                self.logger.warning(
                    "Fixing out-of-bounds wind direction: %s", r)
                r['wind_direction'] -= 360
            yield r


class SYNOPParser(Parser):

    mandatory_fields = [
        'wmo_station_id', 'station_name', 'lat', 'lon', 'height', 'timestamp']

    elements = {
        'cloudCoverTotal': 'cloud_cover',
        'heightOfStationGroundAboveMeanSeaLevel': 'height',
        'latitude': 'lat',
        'longitude': 'lon',
        'meteorologicalOpticalRange': 'visibility',
        'pressureReducedToMeanSeaLevel': 'pressure_msl',
        'stationOrSiteName': 'station_name',
    }
    height_field = 'heightOfSensorAboveLocalGroundOrDeckOfMarinePlatform'
    height = 2
    height_elements = {
        'airTemperature': 'temperature',
        'dewpointTemperature': 'dew_point',
        'relativeHumidity': 'relative_humidity',
    }
    time_period_field = 'timePeriod'
    time_periods = (-10, -30, -60)
    time_period_elements = {
        'windDirection': 'wind_direction',
        'windSpeed': 'wind_speed',
        'maximumWindGustDirection': 'wind_gust_direction',
        'maximumWindGustSpeed': 'wind_gust_speed',
        'totalPrecipitationOrTotalWaterEquivalent': 'precipitation',
        'totalSunshine': 'sunshine',
    }

    def parse(self, path):
        self.logger.info("Parsing %s", path)
        with bz2.open(path) as f:
            if not f.read(1):
                return
            f.seek(0)
            for block in self._get_message_blocks(f):
                for message in block[-1]:
                    with suppress(SkipRecord):
                        record = self.parse_message(message)
                        self.sanitize_record(record)
                        yield record

    def _get_message_blocks(self, f):
        with suppress(ImportError):
            import ijson
            return ijson.items(f, 'messages.item', use_float=True)
        return json.load(f)['messages']

    def parse_message(self, message):
        record = {
            'observation_type': 'synop',
        }
        self.parse_tree(record, message)
        is_complete = all(
            record.get(field) is not None for field in self.mandatory_fields)
        if not is_complete:
            self.logger.error("Skipping incomplete record: %s", record)
            raise SkipRecord
        return record

    def parse_tree(self, record, message, base=None):
        data = {} if base is None else base.copy()
        for block in message:
            if isinstance(block, dict):
                key = block['key']
                value = block['value']
                data[key] = value
                if field := self.elements.get(key):
                    record[field] = value
                elif field := self.height_elements.get(key):
                    if data[self.height_field] == self.height:
                        record[field] = value
                elif field := self.time_period_elements.get(key):
                    time_period = data[self.time_period_field]
                    if time_period in self.time_periods:
                        if field == 'sunshine' and value:
                            value *= 60
                        record[field + f'_{-time_period}'] = value
                elif parse_method := getattr(self, f'parse_{key}', None):
                    parse_method(record, data, value)
            else:
                self.parse_tree(record, block, base=data)

    def parse_minute(self, record, data, value):
        parts = ['year', 'month', 'day', 'hour', 'minute']
        if any(data[part] is None for part in parts):
            raise SkipRecord
        record['timestamp'] = datetime.datetime(
            data['year'], data['month'], data['day'], data['hour'],
            data['minute'], tzinfo=datetime.timezone.utc)

    def parse_stationNumber(self, record, data, value):
        if data['stationNumber']:
            wmo_id = f"{data['blockNumber']}{data['stationNumber']:03d}"
        else:
            wmo_id = data['shortStationName']
        record['wmo_station_id'] = wmo_id
        record['dwd_station_id'] = wmo_id_to_dwd(wmo_id)

    def parse_presentWeather(self, record, data, value):
        if record.get('timePeriod'):
            return
        condition = synop_current_weather_code_to_condition(value)
        # Don't overwrite any condition from pastWeather1 with None, but do
        # prioritize presentWeather if it's not None
        if condition:
            record['condition'] = condition

    def parse_pastWeather1(self, record, data, value):
        if value and not record.get('condition'):
            record['condition'] = synop_past_weather_code_to_condition(value)

    def sanitize_record(self, record):
        for field in list(record):
            if field.startswith('precipitation_') and (record[field] or 0) < 0:
                record[field] = None
        if (record.get('cloud_cover') or 0) > 100:
            record['cloud_cover'] = None


class CurrentObservationsParser(Parser):

    ELEMENTS = {
        'cloud_cover_total': 'cloud_cover',
        'dew_point_temperature_at_2_meter_above_ground': 'dew_point',
        'dry_bulb_temperature_at_2_meter_above_ground': 'temperature',
        'horizontal_visibility': 'visibility',
        'maximum_wind_speed_last_hour': 'wind_gust_speed',
        'mean_wind_direction_during_last_10 min_at_10_meters_above_ground': (
            'wind_direction'),
        'mean_wind_speed_during last_10_min_at_10_meters_above_ground': (
            'wind_speed'),
        'precipitation_amount_last_hour': 'precipitation',
        'present_weather': 'condition',
        'pressure_reduced_to_mean_sea_level': 'pressure_msl',
        'relative_humidity': 'relative_humidity',
        'total_time_of_sunshine_during_last_hour': 'sunshine',
    }
    DATE_COLUMN = 'surface observations'
    HOUR_COLUMN = 'Parameter description'

    CONVERTERS = {
        'condition': current_observations_weather_code_to_condition,
        'dew_point': celsius_to_kelvin,
        'pressure_msl': hpa_to_pa,
        'sunshine': minutes_to_seconds,
        'temperature': celsius_to_kelvin,
        'visibility': km_to_m,
        'wind_speed': kmh_to_ms,
        'wind_gust_speed': kmh_to_ms,
    }

    def parse(self, path, lat=None, lon=None, height=None, station_name=None):
        self.logger.info("Parsing %s", path)
        with open(path) as f:
            reader = csv.DictReader(f, delimiter=';')
            wmo_station_id = next(reader)[self.DATE_COLUMN].rstrip('_')
            dwd_station_id = wmo_id_to_dwd(wmo_station_id)
            # Skip row with German header titles
            next(reader)
            for row in reader:
                yield {
                    'observation_type': 'current',
                    'lat': lat,
                    'lon': lon,
                    'height': height,
                    'dwd_station_id': dwd_station_id,
                    'wmo_station_id': wmo_station_id,
                    'station_name': station_name,
                    **self.parse_row(row)
                }

    def parse_row(self, row):
        record = {
            element: (
                None
                if row[column] == '---'
                else float(row[column].replace(',', '.')))
            for column, element in self.ELEMENTS.items()
        }
        record['timestamp'] = datetime.datetime.strptime(
            f'{row[self.DATE_COLUMN]} {row[self.HOUR_COLUMN]}',
            '%d.%m.%y %H:%M'
        ).replace(tzinfo=datetime.timezone.utc)
        self.convert_units(record)
        self.sanitize_record(record)
        return record

    def convert_units(self, record):
        for element, converter in self.CONVERTERS.items():
            if record[element] is not None:
                record[element] = converter(record[element])

    def sanitize_record(self, record):
        if record['cloud_cover'] and record['cloud_cover'] > 100:
            self.logger.warning(
                "Ignoring unphysical cloud cover value: %s", record)
            record['cloud_cover'] = None
        if record['relative_humidity'] and record['relative_humidity'] > 100:
            self.logger.warning(
                "Ignoring unphysical relative humidity value: %s", record)
            record['relative_humidity'] = None
        if record['sunshine'] and record['sunshine'] > 3600:
            self.logger.warning(
                "Ignoring unphysical sunshine value: %s", record)
            record['sunshine'] = None


class ObservationsParser(Parser):

    elements = {}
    converters = {}
    ignored_values = {}

    def parse(self, path, **extra):
        self.logger.info("Parsing %s", path)
        with zipfile.ZipFile(path) as zf:
            dwd_station_id = self.parse_station_id(zf, **extra)
            wmo_station_id = dwd_id_to_wmo(dwd_station_id)
            lat_lon_history = self.parse_lat_lon_history(
                zf,
                dwd_station_id,
                **extra,
            )
            for record in self.parse_records(zf, lat_lon_history, **extra):
                yield {
                    'observation_type': 'historical',
                    'dwd_station_id': dwd_station_id,
                    'wmo_station_id': wmo_station_id,
                    **record
                }

    def parse_station_id(self, zf, **extra):
        for filename in zf.namelist():
            if (m := re.match(r'Metadaten_Geographie_(\d+)\.txt', filename)):
                return m.group(1)
        raise ValueError(f"Unable to parse station ID for {self.path}")

    def parse_lat_lon_history(self, zf, dwd_station_id, **extra):
        with zf.open(f'Metadaten_Geographie_{dwd_station_id}.txt') as f:
            reader = csv.DictReader(
                io.TextIOWrapper(f, encoding='latin1'),
                delimiter=';')
            history = {}
            for row in reader:
                date_from = datetime.datetime.strptime(
                    row['von_datum'].strip(), '%Y%m%d'
                ).replace(tzinfo=datetime.timezone.utc)
                history[date_from] = (
                    float(row['Geogr.Breite']),
                    float(row['Geogr.Laenge']),
                    float(row['Stationshoehe']),
                    row['Stationsname'])
            return history

    def parse_records(self, zf, lat_lon_history, **extra):
        product_filenames = [
            fn for fn in zf.namelist() if fn.startswith('produkt_')]
        assert len(product_filenames) == 1, "Unexpected product count"
        filename = product_filenames[0]
        with zf.open(filename) as f:
            reader = csv.DictReader(
                io.TextIOWrapper(f, encoding='latin1'),
                delimiter=';')
            yield from self.parse_reader(filename, reader, lat_lon_history)

    def parse_reader(self, filename, reader, lat_lon_history):
        for row in reader:
            timestamp = datetime.datetime.strptime(
                row['MESS_DATUM'],
                '%Y%m%d%H',
            ).replace(
                tzinfo=datetime.timezone.utc,
            )
            if self.skip_timestamp(timestamp):
                continue
            lat, lon, height, station_name = self._station_params(
                timestamp, lat_lon_history)
            yield {
                'source': f'Observations:Recent:{filename}',
                'lat': lat,
                'lon': lon,
                'height': height,
                'station_name': station_name,
                'timestamp': timestamp,
                **self.parse_elements(row, lat, lon, height),
            }

    def _station_params(self, timestamp, lat_lon_history):
        info = None
        for date, lat_lon_height_name in lat_lon_history.items():
            if date > timestamp:
                break
            info = lat_lon_height_name
        return info

    def parse_elements(self, row, lat, lon, height):
        elements = {
            element: (
                float(row[element_key])
                if row[element_key].strip() != '-999'
                and row[element_key].strip() not in self.ignored_values.get(
                    element, [])
                else None)
            for element, element_key in self.elements.items()
        }
        for element, converter in self.converters.items():
            if elements[element] is not None:
                elements[element] = converter(elements[element])
        return elements

    def skip_timestamp(self, timestamp):
        return False


class CloudCoverObservationsParser(ObservationsParser):

    elements = {
        'cloud_cover': ' V_N',
    }
    ignored_values = {
        'cloud_cover': ['-1', '9'],
    }
    converters = {
        'cloud_cover': eighths_to_percent,
    }


class DewPointObservationsParser(ObservationsParser):

    elements = {
        'dew_point': '  TD',
    }
    converters = {
        'dew_point': celsius_to_kelvin,
    }


class TemperatureObservationsParser(ObservationsParser):

    elements = {
        'relative_humidity': 'RF_TU',
        'temperature': 'TT_TU',
    }
    converters = {
        'temperature': celsius_to_kelvin,
    }


class PrecipitationObservationsParser(ObservationsParser):

    elements = {
        'precipitation': '  R1',
        'condition': 'WRTR',
    }
    converters = {
        'condition': synop_form_of_precipitation_code_to_condition,
    }

    def parse_reader(self, filename, reader, lat_lon_history):
        # XXX: WRTR is missing every third hour, we fill it up from the
        #      previous or next row where sensible
        return super().parse_reader(
            filename,
            self._fill_rows(reader),
            lat_lon_history,
        )

    def _fill_rows(self, reader):
        # There's probably a smarter way to do this with itertools.tee()...
        last_row = next(reader)
        self._fill_row(last_row, None, None)
        yield last_row
        row = next(reader)
        for next_row in reader:
            self._fill_row(row, last_row, next_row)
            yield row
            last_row = row
            row = next_row
        self._fill_row(row, last_row, None)
        yield row

    def _fill_row(self, row, last_row, next_row):
        if row['WRTR'] != '-999':
            return
        elif row['RS_IND'].strip() == '0':
            row['WRTR'] = '0'
        elif last_row and last_row['RS_IND'].strip() == '1':
            row['WRTR'] = last_row['WRTR']
        elif next_row and next_row['RS_IND'].strip() == '1':
            row['WRTR'] = next_row['WRTR']
        else:
            row['WRTR'] = '9'


class VisibilityObservationsParser(ObservationsParser):

    elements = {
        'visibility': 'V_VV',
    }
    converters = {
        'visibility': int,
    }


class WindObservationsParser(ObservationsParser):

    elements = {
        'wind_speed': '   F',
        'wind_direction': '   D',
    }
    converters = {
        'wind_direction': int,
    }
    ignored_values = {
        'wind_direction': ['990'],
    }


class WindGustsObservationsParser(ObservationsParser):

    META_DATA_URL = (
        'https://opendata.dwd.de/climate_environment/CDC/observations_germany/'
        'climate/10_minutes/extreme_wind/meta_data/'
        'Meta_Daten_zehn_min_fx_{dwd_station_id}.zip')

    elements = {
        'wind_gust_direction': 'DX_10',
        'wind_gust_speed': 'FX_10',
    }

    def get_extra_urls(self, path):
        with zipfile.ZipFile(path) as zf:
            dwd_station_id = self.parse_station_id(zf)
        return {
            'meta_path': self.META_DATA_URL.format(
                dwd_station_id=dwd_station_id,
            ),
        }

    def parse_station_id(self, zf, **extra):
        for filename in zf.namelist():
            if (m := re.match(r'produkt_.*_(\d+)\.txt', filename)):
                return m.group(1)
        raise ValueError(f"Unable to parse station ID for {self.path}")

    def parse_lat_lon_history(self, zf, dwd_station_id, **extra):
        if 'meta_path' not in extra:
            raise ValueError(
                "Must supply a `meta_path` keyword argument for "
                "WindGustObservationsParser",
            )
        with zipfile.ZipFile(extra['meta_path']) as meta_zf:
            return super().parse_lat_lon_history(meta_zf, dwd_station_id)

    def parse_reader(self, filename, reader, lat_lon_history):
        hour_values = []
        for row in reader:
            timestamp = datetime.datetime.strptime(
                row['MESS_DATUM'],
                '%Y%m%d%H%M',
            ).replace(
                tzinfo=datetime.timezone.utc,
            )
            if self.skip_timestamp(timestamp + datetime.timedelta(hours=1)):
                continue
            # Should this be refactored into a base class we will need to
            # properly parse the station parameters and pass them
            values = self.parse_elements(row, None, None, None)
            if values['wind_gust_speed']:
                hour_values.append(values)
            if timestamp.minute == 0:
                yield self._make_record(
                    timestamp, hour_values, filename, lat_lon_history)
                hour_values.clear()

    def _make_record(self, timestamp, hour_values, filename, lat_lon_history):
        lat, lon, height, station_name = self._station_params(
            timestamp, lat_lon_history)
        if hour_values:
            max_value = max(hour_values, key=lambda v: v['wind_gust_speed'])
            direction = max_value['wind_gust_direction']
            speed = max_value['wind_gust_speed']
        else:
            direction = None
            speed = None
        return {
            'source': f'Observations:Recent:{filename}',
            'lat': lat,
            'lon': lon,
            'height': height,
            'station_name': station_name,
            'timestamp': timestamp,
            'wind_gust_direction': direction,
            'wind_gust_speed': speed,
        }


class SunshineObservationsParser(ObservationsParser):

    elements = {
        'sunshine': 'SD_SO',
    }
    converters = {
        'sunshine': minutes_to_seconds,
    }


class PressureObservationsParser(ObservationsParser):

    elements = {
        'pressure_msl': '   P',
        'pressure_station': '  P0',
    }
    converters = {
        'pressure_msl': hpa_to_pa,
        'pressure_station': hpa_to_pa,
    }

    def parse_elements(self, row, lat, lon, height):
        elements = super().parse_elements(row, lat, lon, height)
        if not elements['pressure_msl'] and elements['pressure_station']:
            # Some stations do not record reduced pressure, but do record
            # pressure at station height. We can approximate the pressure at
            # mean sea level through the barometric formula. The error of this
            # approximation could be reduced if we had the current temperature.
            elements['pressure_msl'] = int(round(
                elements['pressure_station']
                * (1 - .0065 * height / 288.15) ** -5.255,
                -1))
        del elements['pressure_station']
        return elements


def get_parser(filename):
    parsers = {
        r'MOSMIX_S_LATEST_240\.kmz$': MOSMIXSParser,
        r'Z__C_EDZW_\d+_.*\.json\.bz2$': SYNOPParser,
        r'\w{5}-BEOB\.csv$': CurrentObservationsParser,
        'stundenwerte_FF_': WindObservationsParser,
        'stundenwerte_N_': CloudCoverObservationsParser,
        'stundenwerte_P0_': PressureObservationsParser,
        'stundenwerte_RR_': PrecipitationObservationsParser,
        'stundenwerte_SD_': SunshineObservationsParser,
        'stundenwerte_TD_': DewPointObservationsParser,
        'stundenwerte_TU_': TemperatureObservationsParser,
        'stundenwerte_VV_': VisibilityObservationsParser,
        '10minutenwerte_extrema_wind_': WindGustsObservationsParser,
    }
    for pattern, parser in parsers.items():
        if re.match(pattern, filename):
            return parser
