import array
import bz2
import csv
import datetime
import io
import itertools
import json
import logging
import re
import sys
import tarfile
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
    j_per_cm2_to_j_per_m2,
    kj_per_m2_to_j_per_m2,
    km_to_m,
    kmh_to_ms,
    minutes_to_seconds,
    synop_current_weather_code_to_condition,
    synop_form_of_precipitation_code_to_condition,
    synop_past_weather_code_to_condition,
    w_per_m2_to_hourly_j_per_m2,
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


class MOSMIXParser(Parser):

    ELEMENTS = {
        'DD': 'wind_direction',
        'FF': 'wind_speed',
        'FX1': 'wind_gust_speed',
        'N': 'cloud_cover',
        'PPPP': 'pressure_msl',
        'R101': 'precipitation_probability',
        'R602': 'precipitation_probability_6h',
        'Rad1h': 'solar',
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
            assert len(infolist) == 1, f'Unexpected zip content in {path}'
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

    def parse_solar(self, value):
        return kj_per_m2_to_j_per_m2(float(value))

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
            if r['cloud_cover'] and r['cloud_cover'] < 0:
                self.logger.warning("Fixing negative cloud cover: %s", r)
                r['cloud_cover'] = 0
            if r['cloud_cover'] and r['cloud_cover'] > 100:
                self.logger.warning("Fixing overflown cloud cover: %s", r)
                r['cloud_cover'] = 100
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
        'globalSolarRadiationIntegratedOverPeriodSpecified': 'solar',
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
                    time_period = data.get(self.time_period_field, -10)
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
        'global_radiation_last_hour': 'solar',
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
        'solar': w_per_m2_to_hourly_j_per_m2,
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
        raise ValueError(f"Unable to parse station ID for {zf.filename}")

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


class TenMinutesObservationsParser(ObservationsParser):

    META_DATA_URL = None
    TRIGGER_MINUTE = 0

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
        raise ValueError(f"Unable to parse station ID for {zf.filename}")

    def parse_lat_lon_history(self, zf, dwd_station_id, **extra):
        if 'meta_path' not in extra:
            raise ValueError(
                f"Must supply a `meta_path` keyword argument for "
                f"{self.__class__.__name__}",
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
            if self.skip_timestamp(timestamp + datetime.timedelta(minutes=50)):
                continue
            # XXX: Station parameters are currently not supported for
            #      10-minute parsers
            hour_values.append(self.parse_elements(row, None, None, None))
            if timestamp.minute == self.TRIGGER_MINUTE:
                if self.TRIGGER_MINUTE > 30:
                    # Likely triggered at :50, round to next full hour
                    timestamp += datetime.timedelta(
                        minutes=60 - self.TRIGGER_MINUTE,
                    )
                elif self.TRIGGER_MINUTE:
                    timestamp = timestamp.replace(minute=0)
                yield self._make_record(
                    timestamp, hour_values, filename, lat_lon_history)
                hour_values.clear()

    def _make_record(self, timestamp, hour_values, filename, lat_lon_history):
        raise NotImplementedError


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
            itertools.starmap(self.fill_wrtr, self.with_neighbors(reader)),
            lat_lon_history,
        )

    def with_neighbors(self, it):
        """
        'ABCDEF' -> (None, 'A', 'B'), ('A', 'B', 'C'), ..., ('E', 'F', None)
        """
        a, b, c = itertools.tee(it, 3)
        next(c, None)
        return zip(
            itertools.chain([None], a),
            b,
            itertools.chain(c, [None]),
        )

    def fill_wrtr(self, last_row, row, next_row):
        if row['WRTR'] != '-999':
            pass
        elif row['RS_IND'].strip() == '0':
            row['WRTR'] = '0'
        elif last_row and last_row['RS_IND'].strip() == '1':
            row['WRTR'] = last_row['WRTR']
        elif next_row and next_row['RS_IND'].strip() == '1':
            row['WRTR'] = next_row['WRTR']
        else:
            row['WRTR'] = '9'
        return row


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


class WindGustsObservationsParser(TenMinutesObservationsParser):

    META_DATA_URL = (
        'https://opendata.dwd.de/climate_environment/CDC/observations_germany/'
        'climate/10_minutes/extreme_wind/meta_data/'
        'Meta_Daten_zehn_min_fx_{dwd_station_id}.zip')

    elements = {
        'wind_gust_direction': 'DX_10',
        'wind_gust_speed': 'FX_10',
    }

    def _make_record(self, timestamp, hour_values, filename, lat_lon_history):
        lat, lon, height, station_name = self._station_params(
            timestamp, lat_lon_history)
        hour_values = [x for x in hour_values if x['wind_gust_speed']]
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


class SolarRadiationObservationsParser(TenMinutesObservationsParser):

    META_DATA_URL = (
        'https://opendata.dwd.de/climate_environment/CDC/observations_germany/'
        'climate/10_minutes/solar/meta_data/'
        'Meta_Daten_zehn_min_sd_{dwd_station_id}.zip')

    # It seems that the measurement rows contain the global irradiance for the
    # NEXT ten minutes -- at least then the values align with
    # "global_radiation_last_hour" from the current observations
    TRIGGER_MINUTE = 50

    elements = {
        'solar': 'GS_10',
    }
    converters = {
        'solar': j_per_cm2_to_j_per_m2,
    }

    def _make_record(self, timestamp, hour_values, filename, lat_lon_history):
        lat, lon, height, station_name = self._station_params(
            timestamp, lat_lon_history)
        solar = None
        for values in hour_values:
            if values['solar'] is not None:
                solar = (solar or 0) + values['solar']
        return {
            'source': f'Observations:Recent:{filename}',
            'lat': lat,
            'lon': lon,
            'height': height,
            'station_name': station_name,
            'timestamp': timestamp,
            'solar': solar,
        }


class RADOLANParser(Parser):

    PRODUCT = 'RV'
    WMO_ID = '10000'  # WMO ID for composite products
    HEIGHT = 1200
    WIDTH = 1100
    INTERVAL = 5
    PRECISION = 'E-02'
    FIELD_NAME = 'precipitation_5'

    def parse(self, path):
        with tarfile.open(path, 'r:bz2') as tar:
            for filename in sorted(tar.getnames()):
                yield self.parse_single(tar.extractfile(filename))

    def parse_single(self, f):
        product, timestamp, offset = self.parse_header(f)
        data = self.parse_data(f)
        return {
            'observation_type': 'radar',
            'source': f'RADOLAN::{product}::{timestamp.isoformat()}',
            'timestamp': timestamp + offset,
            **data,
        }

    def parse_header(self, f):
        header = ''
        while (ch := f.read(1)) != b'\x03':
            header += ch.decode()
        # Product type
        product = header[:2]
        assert product == self.PRODUCT
        # WMO ID should be 10000 for composite
        assert header[8:13] == self.WMO_ID
        timestamp = datetime.datetime.strptime(
            header[2:8] + header[13:17],
            '%d%H%M%m%y',
        ).replace(
            tzinfo=datetime.timezone.utc,
        )
        # 1200 km x 1100 km grid
        assert f'GP{self.HEIGHT}x{self.WIDTH}' in header
        # 2 bytes per cell
        expected_bytes = 2 * self.HEIGHT * self.WIDTH + len(header) + 1
        assert f'BY{expected_bytes:10d}' in header
        # Integers represent 0.01 mm
        assert f'PR{self.PRECISION:>5s}' in header
        # Five minute interval
        assert f'INT{self.INTERVAL:4d}' in header
        offset_minutes = int(re.search(r'VV([ \d]{4})', header).group(1))
        offset = datetime.timedelta(minutes=offset_minutes)
        return product, timestamp, offset

    def parse_data(self, f):
        buf = f.read()
        assert len(buf) == 2 * self.HEIGHT * self.WIDTH, "Unexpected grid size"
        raw = array.array('H')
        raw.frombytes(buf)
        if sys.byteorder != 'little':
            raw.byteswap()
        return {
            self.FIELD_NAME: self.process_raw_data(raw),
        }

    def process_raw_data(self, raw):
        multiplier = float('1' + self.PRECISION)
        return [
            [
                x * multiplier if x < 4096 else None
                for x in raw[row*self.WIDTH:(row+1)*self.WIDTH]
            ]
            for row in reversed(range(self.HEIGHT))
        ]


class CAPParser(Parser):

    ns = {
        'cap': 'urn:oasis:names:tc:emergency:cap:1.2',
    }

    TAG_MAP = {
        'de': {
            'event': 'event_de',
            'headline': 'headline_de',
            'description': 'description_de',
            'instruction': 'instruction_de',
        },
        'en': {
            'event': 'event_en',
            'headline': 'headline_en',
            'description': 'description_en',
            'instruction': 'instruction_en',
            'category': 'category',
            'responseType': 'response_type',
            'urgency': 'urgency',
            'severity': 'severity',
            'certainty': 'certainty',
            'effective': 'effective',
            'onset': 'onset',
            'expires': 'expires',
        }
    }
    OPTIONAL_FIELDS = [
        'expires',
    ]
    TOKEN_FIELDS = [
        'category',
        'certainty',
        'response_type',
        'severity',
        'urgency',
    ]
    TIMESTAMP_FIELDS = [
        'effective',
        'onset',
        'expires',
    ]

    def parse(self, path):
        self.logger.info("Parsing %s", path)
        with zipfile.ZipFile(path) as zf:
            for info in zf.infolist():
                with zf.open(info) as f:
                    yield self.parse_event(f)

    def parse_event(self, f):
        event = {}
        for _, element in ET.iterparse(f):
            if self._is_tag(element, 'cap:info'):
                self._parse_info(event, element)
                element.clear()
            elif self._is_tag(element, 'cap:alert'):
                event['id'] = element.find(
                    'cap:identifier',
                    self.ns,
                ).text.rsplit('.', 1)[0]
        self.sanitize_event(event)
        return event

    def _parse_info(self, event, element):
        lang = element.find('cap:language', self.ns).text.split('-')[0]
        tag_map = self.TAG_MAP.get(lang, {})
        for tag, field in tag_map.items():
            e = element.find(f'cap:{tag}', self.ns)
            if e is not None:
                event[field] = e.text
            elif field in self.OPTIONAL_FIELDS:
                event[field] = None
            else:
                raise ValueError("Unable to find <%s>" % tag)
        if 'event_code' not in event:
            event['event_code'] = self._parse_event_code(element)
        if 'warn_cell_ids' not in event:
            event['warn_cell_ids'] = list(self._parse_warn_cell_ids(element))

    def _is_tag(self, element, tag):
        prefix, tag = tag.split(':')
        return element.tag == f'{{{self.ns[prefix]}}}{tag}'

    def _parse_event_code(self, element):
        for ec_element in element.findall('cap:eventCode', self.ns):
            if ec_element.find('cap:valueName', self.ns).text == 'II':
                return int(ec_element.find('cap:value', self.ns).text)

    def _parse_warn_cell_ids(self, element):
        for gc_element in element.findall('cap:area/cap:geocode', self.ns):
            if gc_element.find('cap:valueName', self.ns).text == 'WARNCELLID':
                yield int(gc_element.find('cap:value', self.ns).text)

    def sanitize_event(self, event):
        for field in self.TOKEN_FIELDS:
            if event[field] is None:
                continue
            event[field] = event[field].lower()
        for field in self.TIMESTAMP_FIELDS:
            if event[field] is None:
                continue
            event[field] = datetime.datetime.fromisoformat(event[field])


def get_parser(filename):
    parsers = {
        r'DE1200_RV': RADOLANParser,
        r'MOSMIX_(S|L)_LATEST(_240)?\.kmz$': MOSMIXParser,
        r'Z__C_EDZW_\d+_.*\.json\.bz2$': SYNOPParser,
        r'Z_CAP_.*\.zip': CAPParser,
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
        '10minutenwerte_SOLAR_': SolarRadiationObservationsParser,
    }
    for pattern, parser in parsers.items():
        if re.match(pattern, filename):
            return parser
