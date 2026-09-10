import datetime
import io
import tarfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import pytest

from dwdparse.parsers import (
    CAPParser,
    CloudCoverObservationsParser,
    CurrentObservationsParser,
    DewPointObservationsParser,
    MOSMIXParser,
    Parser,
    PrecipitationObservationsParser,
    PressureObservationsParser,
    RadarParser,
    RADOLANParser,
    SolarRadiationObservationsParser,
    SunshineObservationsParser,
    SYNOPParser,
    TemperatureObservationsParser,
    TenMinutesObservationsParser,
    VisibilityObservationsParser,
    WindGustsObservationsParser,
    WindObservationsParser,
    get_parser,
)

from .utils import is_subset


utc = datetime.timezone.utc


def test_mosmix_parser(data_dir: Path) -> None:
    records = list(MOSMIXParser().parse(data_dir / 'MOSMIX_L_LATEST.kmz'))
    assert len(records) == 247
    assert records[0] == {
        'observation_type': 'forecast',
        'source': 'MOSMIX:2023-04-12T09:00:00.000Z',
        'lat': 51.97,
        'lon': 7.63,
        'height': 60.,
        'dwd_station_id': 'XXX',
        'wmo_station_id': 'P0036',
        'station_name': 'MUENSTER ZENTRUM',
        'timestamp': datetime.datetime(2023, 4, 12, 10, 0, tzinfo=utc),
        'cloud_cover': 100.0,
        'dew_point': 279.05,
        'precipitation': 1.2,
        'precipitation_probability': 75.,
        'precipitation_probability_6h': None,
        'pressure_msl': 99700.0,
        'sunshine': 0.0,
        'temperature': 282.75,
        'visibility': 14900.0,
        'wind_direction': 179.0,
        'wind_speed': 5.14,
        'wind_gust_speed': 9.26,
        'condition': 'rain',
        'solar': 510000.0,
    }
    assert records[-1] == {
        'observation_type': 'forecast',
        'source': 'MOSMIX:2023-04-12T09:00:00.000Z',
        'lat': 51.97,
        'lon': 7.63,
        'height': 60.,
        'dwd_station_id': 'XXX',
        'wmo_station_id': 'P0036',
        'station_name': 'MUENSTER ZENTRUM',
        'timestamp': datetime.datetime(2023, 4, 22, 16, 0, tzinfo=utc),
        'cloud_cover': 58.,
        'dew_point': 277.85,
        'precipitation': 0.0,
        'precipitation_probability': 9.0,
        'precipitation_probability_6h': None,
        'pressure_msl': 101790.0,
        'sunshine': 2100.0,
        'temperature': 290.85,
        'visibility': 24900.0,
        'wind_direction': 35.0,
        'wind_speed': 3.6,
        'wind_gust_speed': 7.72,
        'condition': None,
        'solar': 1170000.0,
    }
    assert records[2]['precipitation_probability_6h'] == 85.0


def test_synop_parser(data_dir: Path) -> None:
    records = list(SYNOPParser().parse(data_dir / 'synop.json.bz2'))
    assert len(records) == 3
    assert records[0] == {
        'observation_type': 'synop',
        'lat': 52.1344,
        'lon': 7.69685,
        'height': 47.8,
        'wmo_station_id': '10315',
        'dwd_station_id': '01766',
        'station_name': 'Muenster/Osnabrueck',
        'timestamp': datetime.datetime(2020, 6, 17, 9, 0, tzinfo=utc),
        'cloud_cover': 88,
        'dew_point': 287.37,
        'pressure_msl': 101290,
        'relative_humidity': 66,
        'temperature': 294.05,
        'wind_direction_10': 30,
        'wind_speed_10': 2,
        'wind_gust_direction_10': None,
        'wind_gust_speed_10': None,
        'condition': 'dry',
    }
    assert records[1] == {
        'observation_type': 'synop',
        'lat': 52.1344,
        'lon': 7.69685,
        'height': 47.8,
        'wmo_station_id': '10315',
        'dwd_station_id': '01766',
        'station_name': 'Muenster/Osnabrueck',
        'timestamp': datetime.datetime(2020, 6, 17, 9, 0, tzinfo=utc),
        'visibility': None,
        'sunshine_60': 2520,
        'precipitation_60': 0,
        'wind_direction_10': None,
        'wind_speed_10': None,
        'wind_gust_direction_10': None,
        'wind_gust_speed_10': None,
        'wind_gust_direction_60': None,
        'wind_gust_speed_60': 4.6,
        'wind_gust_direction_30': 340,
        'wind_gust_speed_30': 4.6,
    }
    assert records[2]['wmo_station_id'] == 'M031'
    assert records[2]['dwd_station_id'] == '05484'


def test_current_observation_parser(data_dir: Path) -> None:
    records = list(
        CurrentObservationsParser().parse(
            data_dir / '10315-BEOB.csv',
            10.1,
            20.2,
            30.3,
            'Muenster',
        )
    )
    assert len(records) == 25
    assert records[0] == {
        'observation_type': 'current',
        'lat': 10.1,
        'lon': 20.2,
        'height': 30.3,
        'dwd_station_id': '01766',
        'wmo_station_id': '10315',
        'station_name': 'Muenster',
        'timestamp': datetime.datetime(2023, 4, 12, 11, 0, tzinfo=utc),
        'cloud_cover': 100.0,
        'dew_point': 281.45,
        'precipitation': 2.1,
        'pressure_msl': 99780,
        'relative_humidity': 94.,
        'sunshine': 0,
        'temperature': 282.35,
        'visibility': 7900.0,
        'wind_direction': 180.0,
        'wind_speed': 2.2,
        'wind_gust_speed': 4.7,
        'condition': 'rain',
        'solar': 291600.0,
    }
    assert records[15] == {
        'observation_type': 'current',
        'lat': 10.1,
        'lon': 20.2,
        'height': 30.3,
        'dwd_station_id': '01766',
        'wmo_station_id': '10315',
        'station_name': 'Muenster',
        'timestamp': datetime.datetime(2023, 4, 11, 20, 0, tzinfo=utc),
        'cloud_cover': 63.0,
        'dew_point': 273.65,
        'precipitation': 0.0,
        'pressure_msl': 101170,
        'relative_humidity': 67.0,
        'sunshine': 0,
        'temperature': 279.35,
        'visibility': 49400.0,
        'wind_direction': 140.0,
        'wind_speed': 1.9,
        'wind_gust_speed': 2.2,
        'condition': 'dry',
        'solar': 0.0,
    }


def test_observations_parser_parses_metadata(data_dir: Path) -> None:
    p = WindObservationsParser()
    metadata = {
        'observation_type': 'historical',
        'source': (
            'Observations:Recent:produkt_ff_stunde_20180915_20200317_04911.txt'
        ),
        'lat': 48.8275,
        'lon': 12.5597,
        'height': 350.5,
        'dwd_station_id': '04911',
        'wmo_station_id': '10788',
        'station_name': 'Straubing',
    }
    for record in p.parse(data_dir / 'observations_recent_FF_akt.zip'):
        assert is_subset(metadata, record)


def test_observations_parser_handles_missing_values(data_dir: Path) -> None:
    p = WindObservationsParser()
    records = list(p.parse(data_dir / 'observations_recent_FF_akt.zip'))
    assert records[5]['wind_direction'] == 90
    assert records[5]['wind_speed'] is None


def test_observations_parser_handles_ignored_values(data_dir: Path) -> None:
    p = WindObservationsParser()
    p.ignored_values = {'wind_direction': ['80']}
    records = list(p.parse(data_dir / 'observations_recent_FF_akt.zip'))
    assert records[0]['wind_direction'] is None
    assert records[0]['wind_speed'] == 1.6


def test_observations_parser_handles_location_changes(data_dir: Path) -> None:
    p = WindObservationsParser()
    path = data_dir / 'observations_recent_FF_location_change_akt.zip'
    records = list(p.parse(path))
    assert is_subset(
        {'lat': 48.8275, 'lon': 12.5597, 'height': 350.5}, records[0])
    assert is_subset(
        {'lat': 50.0, 'lon': 13.0, 'height': 345.0}, records[-1])


def test_observations_parser_skip_timestamp(data_dir: Path) -> None:
    p = WindObservationsParser()
    records = list(p.parse(data_dir / 'observations_recent_FF_akt.zip'))
    assert len(records) == 10
    p.skip_timestamp = (  # type: ignore[method-assign]
        lambda timestamp: timestamp.year != 2019
    )
    records = list(p.parse(data_dir / 'observations_recent_FF_akt.zip'))
    assert len(records) == 1
    assert records[0]['timestamp'].year == 2019


def test_ten_minutes_observations_parser_extra_urls(data_dir: Path) -> None:
    class TestParser(TenMinutesObservationsParser):
        META_DATA_URL = 'test_{dwd_station_id}.zip'

    parser = TestParser()
    path = data_dir / 'observations_recent_extrema_wind_akt.zip'
    expected_meta_url = 'test_01766.zip'
    assert parser.get_extra_urls(path) == {
        'meta_path': expected_meta_url,
    }


def _test_parser(
    cls: type[Parser],
    path: Path,
    first: dict[str, Any],
    last: dict[str, Any],
    count: int = 10,
    first_idx: int = 0,
    last_idx: int = -1,
    **kwargs: Any,
) -> None:
    p = cls()
    records = list(p.parse(path, **kwargs))
    first['timestamp'] = datetime.datetime.strptime(
        first['timestamp'], '%Y-%m-%d %H:%M').replace(tzinfo=utc)
    last['timestamp'] = datetime.datetime.strptime(
        last['timestamp'], '%Y-%m-%d %H:%M').replace(tzinfo=utc)
    assert len(records) == count
    assert is_subset(first, records[first_idx])
    assert is_subset(last, records[last_idx])


def test_cloud_cover_observations_parser(data_dir: Path) -> None:
    _test_parser(
        CloudCoverObservationsParser,
        data_dir / 'observations_recent_N_akt.zip',
        {'timestamp': '2018-12-03 07:00', 'cloud_cover': 50},
        {'timestamp': '2019-11-20 00:00', 'cloud_cover': None},
    )


def test_dew_point_observations_parser(data_dir: Path) -> None:
    _test_parser(
        DewPointObservationsParser,
        data_dir / 'observations_recent_TD_akt.zip',
        {'timestamp': '2018-12-03 00:00', 'dew_point': 284.55},
        {'timestamp': '2020-05-29 15:00', 'dew_point': 271.65},
    )


def test_temperature_observations_parser(data_dir: Path) -> None:
    _test_parser(
        TemperatureObservationsParser,
        data_dir / 'observations_recent_TU_akt.zip',
        {'timestamp': '2018-09-15 00:00',
         'temperature': 286.85, 'relative_humidity': 96},
        {'timestamp': '2020-03-17 23:00',
         'temperature': 275.75, 'relative_humidity': 100},
    )


def test_precipitation_observations_parser(data_dir: Path) -> None:
    _test_parser(
        PrecipitationObservationsParser,
        data_dir / 'observations_recent_RR_akt.zip',
        {
            'timestamp': '2018-09-22 20:00',
            'precipitation': 0.0,
            'condition': 'dry',
        },
        {
            'timestamp': '2020-02-11 02:00',
            'precipitation': 0.3,
            # This value should be filled up from the previous row
            'condition': 'rain',
        },
    )


def test_precipitation_observations_parser_with_neighbors() -> None:
    parser = PrecipitationObservationsParser()
    assert list(parser.with_neighbors('')) == []
    assert list(parser.with_neighbors('A')) == [(None, 'A', None)]
    assert list(parser.with_neighbors('AB')) == [
        (None, 'A', 'B'),
        ('A', 'B', None),
    ]
    assert list(parser.with_neighbors('ABC')) == [
        (None, 'A', 'B'),
        ('A', 'B', 'C'),
        ('B', 'C', None),
    ]
    assert list(parser.with_neighbors(iter('ABCDEF'))) == [
        (None, 'A', 'B'),
        ('A', 'B', 'C'),
        ('B', 'C', 'D'),
        ('C', 'D', 'E'),
        ('D', 'E', 'F'),
        ('E', 'F', None),
    ]


def test_visibility_observations_parser(data_dir: Path) -> None:
    _test_parser(
        VisibilityObservationsParser,
        data_dir / 'observations_recent_VV_akt.zip',
        {'timestamp': '2018-12-03 00:00', 'visibility': 15000},
        {'timestamp': '2020-06-04 23:00', 'visibility': 30000},
    )


def test_wind_observations_parser(data_dir: Path) -> None:
    _test_parser(
        WindObservationsParser,
        data_dir / 'observations_recent_FF_akt.zip',
        {'timestamp': '2018-09-15 00:00',
         'wind_speed': 1.6, 'wind_direction': 80},
        {'timestamp': '2020-03-17 23:00',
         'wind_speed': 1.5, 'wind_direction': 130},
    )


def test_wind_gusts_observations_parser(data_dir: Path) -> None:
    _test_parser(
        WindGustsObservationsParser,
        data_dir / 'observations_recent_extrema_wind_akt.zip',
        {'timestamp': '2018-12-03 00:00',
         'wind_gust_speed': 6.3, 'wind_gust_direction': 210},
        {'timestamp': '2020-06-04 23:00',
         'wind_gust_speed': 6.2, 'wind_gust_direction': 270},
        meta_path=data_dir / 'observations_recent_extrema_wind_akt_meta.zip'
    )


def test_sunshine_observations_parser(data_dir: Path) -> None:
    _test_parser(
        SunshineObservationsParser,
        data_dir / 'observations_recent_SD_akt.zip',
        {'timestamp': '2018-09-15 11:00', 'sunshine': 600.},
        {'timestamp': '2020-03-17 16:00', 'sunshine': 0.},
        first_idx=2,
    )


def test_pressure_observations_parser(data_dir: Path) -> None:
    _test_parser(
        PressureObservationsParser,
        data_dir / 'observations_recent_P0_hist.zip',
        {'timestamp': '2018-09-15 00:00', 'pressure_msl': 102120},
        {'timestamp': '2020-03-17 23:00', 'pressure_msl': 103190},
    )


def test_pressure_observations_parser_approximates_pressure_msl(
    data_dir: Path,
) -> None:
    p = PressureObservationsParser()
    records = list(p.parse(data_dir / 'observations_recent_P0_hist.zip'))
    # The actual reduced pressure deleted from the test observation file was
    # 1023.0 hPa
    assert records[4]['pressure_msl'] == 102260


def test_solar_radiation_observations_parser(data_dir: Path) -> None:
    _test_parser(
        SolarRadiationObservationsParser,
        data_dir / '10minutenwerte_SOLAR_01766_now.zip',
        {'timestamp': '2023-04-12 01:00', 'solar': 0.0},
        {'timestamp': '2023-04-12 12:00', 'solar': 674000.},
        meta_path=data_dir / 'Meta_Daten_zehn_min_sd_01766.zip',
        count=12,
    )


def test_radolan_parser(data_dir: Path) -> None:
    p = RADOLANParser()
    records = list(p.parse(data_dir / 'DE1200_RV2305081330.tar.bz2'))
    assert len(records) == 2
    assert records[0]['observation_type'] == 'radar'
    assert records[0]['source'] == 'RADOLAN::RV::2023-05-08T13:30:00+00:00'
    assert records[0]['timestamp'] == datetime.datetime(
        2023, 5, 8, 13, 30, tzinfo=utc,
    )
    assert records[1]['source'] == 'RADOLAN::RV::2023-05-08T13:30:00+00:00'
    assert records[1]['timestamp'] == datetime.datetime(
        2023, 5, 8, 14, 20, tzinfo=utc,
    )
    data = records[0]['precipitation_5']
    data_flat = [x for row in data for x in row]
    assert len(data) == 1200
    assert all(len(row) == 1100 for row in data)
    assert sum(x is None for x in data_flat) == 623059
    assert round(sum(x or 0 for x in data_flat), 2) == 5640.30
    clipped = [
        row[334:339]
        for row in data[1117:1122]
    ]
    assert clipped == [
        [ .03,  .05,  .02,  .01,  .03],  # noqa: E201
        [ .02,  .03,  .03,    0,    0],  # noqa: E201
        [ .03,  .04,  .01,    0,  .03],  # noqa: E201
        [None,  .08,    0,    0,    0],
        [None, None, None, None, None],
    ]


def test_radar_parser(data_dir: Path) -> None:
    p = RadarParser()
    records = list(p.parse(data_dir / 'composite_rv_20250923_0855.tar'))
    assert len(records) == 2
    assert records[0]['observation_type'] == 'radar'
    assert records[0]['source'] == 'RADARCOMP::RV::2025-09-23T08:55:00+00:00'
    assert records[0]['timestamp'] == datetime.datetime(
        2025, 9, 23, 8, 55, tzinfo=utc,
    )
    assert records[1]['source'] == 'RADARCOMP::RV::2025-09-23T08:55:00+00:00'
    assert records[1]['timestamp'] == datetime.datetime(
        2025, 9, 23, 9, 45, tzinfo=utc,
    )
    data = records[0]['precipitation_5']
    data_flat = [x for row in data for x in row]
    assert len(data) == 1200
    assert all(len(row) == 1100 for row in data)
    assert sum(x is None for x in data_flat) == 621995
    assert round(sum(x or 0 for x in data_flat), 3) == 2467.116
    clipped = [
        [round(x, 3) if x is not None else x for x in row[258:263]]
        for row in data[851:856]
    ]
    assert clipped == [
        [.136, .115, .086, .091, .095],
        [.124, .110, .079, .088, .094],
        [.097, .107, .087, .087, .094],
        [None, None, .025, .066, .080],
        [None, None, None, None, None],
    ]


def test_cap_parser(data_dir: Path) -> None:
    p = CAPParser()
    fn = 'Z_CAP_C_EDZW_LATEST_PVW_STATUS_PREMIUMDWD_COMMUNEUNION_MUL.zip'
    records = list(p.parse(data_dir / fn))
    expected_ids = set(
        f'2.49.0.0.276.0.DWD.PVW.{suffix}'
        for suffix in [
            '1687514160000.20999218-5d5e-4761-b271-6c243f695568',
            '1687511640000.0d1a4fb5-251a-46fc-aae3-a688d2021e71',
            '1687470000000.fe90b61b-3755-4efb-8eda-b161251da9f7',
        ]
    )
    assert len(records) == 3
    assert set(r['id'] for r in records) == expected_ids
    assert records[1] == {
        'id': '2.49.0.0.276.0.DWD.PVW.1687511640000.0d1a4fb5-251a-46fc-aae3-a688d2021e71',  # noqa
        'event_de': 'STARKREGEN',
        'headline_de': 'Amtliche WARNUNG vor STARKREGEN',
        'description_de': 'Es tritt Starkregen auf. Dabei werden Niederschlagsmengen zwischen 20 l/m² und 30 l/m² in 6 Stunden erwartet.',  # noqa
        'instruction_de': 'ACHTUNG! Hinweis auf mögliche Gefahren: Während des Platzregens sind kurzzeitig Verkehrsbehinderungen möglich.',  # noqa
        'event_code': 61,
        'warn_cell_ids': [
          812069270,
          812063189,
          812069018
        ],
        'event_en': 'heavy rain',
        'headline_en': 'Official WARNING of HEAVY RAIN',
        'description_en': 'There is a risk of heavy rain (Level 2 of 4).\nPrecipitation amounts: 20-30 l/m²/6h',  # noqa
        'instruction_en': 'NOTE: Be aware of the following possible dangers: The downpours can cause temporary traffic disruption.',  # noqa
        'category': 'met',
        'response_type': 'prepare',
        'urgency': 'immediate',
        'severity': 'moderate',
        'certainty': 'likely',
        'status': 'actual',
        'effective': datetime.datetime(2023, 6, 23, 9, 14, tzinfo=utc),
        'onset': datetime.datetime(2023, 6, 23, 9, 14, tzinfo=utc),
        'expires': datetime.datetime(2023, 6, 23, 13, 0, tzinfo=utc),
    }


def test_cap_parser_test_status(data_dir: Path) -> None:
    p = CAPParser()
    fn = 'Z_CAP_with_test.zip'
    records = list(p.parse(data_dir / fn))

    assert len(records) == 1
    assert records[0]['status'] == 'test'


def test_get_parser() -> None:
    synop_with_timestamp = (
        'Z__C_EDZW_20200617114802_bda01,synop_bufr_GER_999999_999999__MW_617'
        '.json.bz2')
    synop_latest = (
        'Z__C_EDZW_latest_bda01,synop_bufr_GER_999999_999999__MW_XXX.json.bz2')
    cap_latest = (
        'Z_CAP_C_EDZW_LATEST_PVW_STATUS_PREMIUMDWD_COMMUNEUNION_MUL.zip')
    expected = {
        '10minutenwerte_extrema_wind_00427_akt.zip': (
            WindGustsObservationsParser),
        '10minutenwerte_SOLAR_01766_now.zip': SolarRadiationObservationsParser,
        'stundenwerte_FF_00011_akt.zip': WindObservationsParser,
        'stundenwerte_FF_00090_akt.zip': WindObservationsParser,
        'stundenwerte_N_01766_akt.zip': CloudCoverObservationsParser,
        'stundenwerte_P0_00096_akt.zip': PressureObservationsParser,
        'stundenwerte_RR_00102_akt.zip': PrecipitationObservationsParser,
        'stundenwerte_SD_00125_akt.zip': SunshineObservationsParser,
        'stundenwerte_TD_01766.zip': DewPointObservationsParser,
        'stundenwerte_TU_00161_akt.zip': TemperatureObservationsParser,
        'stundenwerte_VV_00161_akt.zip': VisibilityObservationsParser,
        'MOSMIX_S_LATEST_240.kmz': MOSMIXParser,
        'MOSMIX_S_2025070602_240.kmz': MOSMIXParser,
        'MOSMIX_L_2025070703.kmz': MOSMIXParser,
        'MOSMIX_L_2025070615_01089.kmz': MOSMIXParser,
        'DE1200_RV2305081330.tar.bz2': RADOLANParser,
        'composite_rv_20250923_0855.tar': RadarParser,
        'K611_-BEOB.csv': CurrentObservationsParser,
        synop_with_timestamp: SYNOPParser,
        synop_latest: None,
        cap_latest: CAPParser,
    }
    for filename, expected_parser in expected.items():
        assert get_parser(filename) is expected_parser


def test_get_parser_unknown_filename() -> None:
    assert get_parser('totally_unknown_file.dat') is None


def test_parse_raises_for_unknown_file() -> None:
    from dwdparse.api import parse
    with pytest.raises(ValueError, match="No parser found"):
        list(parse('totally_unknown_file.dat'))


@pytest.mark.parametrize('parser_cls', [
    MOSMIXParser, SYNOPParser, CurrentObservationsParser])
def test_sanitize_passes_through_in_bounds_values(
    parser_cls: type[Parser],
) -> None:
    record = {
        'precipitation': 0.0,
        'wind_speed': 0.0,
        'wind_direction': 0.0,
        'cloud_cover': 100.0,
        'relative_humidity': 100.0,
        'sunshine': 3600,
    }
    parser_cls().sanitize_record(record)
    assert record == {
        'precipitation': 0.0,
        'wind_speed': 0.0,
        'wind_direction': 0.0,
        'cloud_cover': 100.0,
        'relative_humidity': 100.0,
        'sunshine': 3600,
    }


@pytest.mark.parametrize('parser_cls', [
    MOSMIXParser, SYNOPParser, CurrentObservationsParser])
def test_sanitize_passes_through_none(parser_cls: type[Parser]) -> None:
    record = {
        'precipitation': None,
        'wind_speed': None,
        'wind_direction': None,
        'cloud_cover': None,
        'relative_humidity': None,
        'sunshine': None,
    }
    parser_cls().sanitize_record(record)
    assert all(v is None for v in record.values())


@pytest.mark.parametrize('parser_cls', [
    MOSMIXParser, SYNOPParser, CurrentObservationsParser])
def test_sanitize_clamps_negative_values(parser_cls: type[Parser]) -> None:
    record = {
        'precipitation': -1.0,
        'wind_speed': -0.5,
        'cloud_cover': -10.0,
        'relative_humidity': -5.0,
        'sunshine': -1,
    }
    parser_cls().sanitize_record(record)
    assert record['precipitation'] == 0
    assert record['wind_speed'] == 0
    assert record['cloud_cover'] == 0
    assert record['relative_humidity'] == 0
    assert record['sunshine'] == 0


@pytest.mark.parametrize('parser_cls', [
    MOSMIXParser, SYNOPParser, CurrentObservationsParser])
def test_sanitize_clamps_overflow_values(parser_cls: type[Parser]) -> None:
    record = {
        'cloud_cover': 110.0,
        'relative_humidity': 105.0,
        'sunshine': 3700,
    }
    parser_cls().sanitize_record(record)
    assert record['cloud_cover'] == 100
    assert record['relative_humidity'] == 100
    assert record['sunshine'] == 3600


@pytest.mark.parametrize('value,expected', [
    (725.0, 5.0),    # 725 % 360
    (-10.0, 350.0),  # negative wraps via modulo
    (360.0, 360.0),  # exactly 360 left alone
])
def test_sanitize_wind_direction_modulo(value: float, expected: float) -> None:
    record = {'wind_direction': value}
    MOSMIXParser().sanitize_record(record)
    assert record['wind_direction'] == expected


def test_pressure_observations_approximation_for_missing(
    data_dir: Path,
) -> None:
    """pressure_msl == None (DWD's -999 sentinel) is approximated from the
    station-level pressure via the barometric formula."""
    p = PressureObservationsParser()
    records = list(p.parse(data_dir / 'observations_recent_P0_hist.zip'))
    # Record 4 has pressure_msl missing in the fixture
    assert records[4]['pressure_msl'] == 102260
    # Records with actual pressure_msl keep their original values
    assert records[0]['pressure_msl'] == 102120


def test_pressure_observations_approximation_for_zero() -> None:
    """pressure_msl == 0 is treated as a data error and triggers the same
    barometric approximation as a missing value."""
    p = PressureObservationsParser()
    elements = p.parse_elements(
        {'   P': '0.0', '  P0': '980.9'},
        lat=50.0, lon=9.0, height=339.5)
    assert elements['pressure_msl'] is not None
    assert elements['pressure_msl'] > 0


def test_pressure_observations_approximation_without_height() -> None:
    """Used to multiply by None. The formula cannot be evaluated without a
    height, so say so rather than emitting a record with no pressure."""
    p = PressureObservationsParser()
    with pytest.raises(ValueError, match="without a height"):
        p.parse_elements(
            {'   P': '0.0', '  P0': '980.9'},
            lat=None, lon=None, height=None)


def test_observations_parser_rejects_timestamp_before_metadata() -> None:
    """_station_params returned None here, which the caller unpacked
    into four values."""
    p = WindObservationsParser()
    history = {
        datetime.datetime(2020, 1, 1, tzinfo=utc): (50.0, 9.0, 339.5, 'X'),
    }
    with pytest.raises(ValueError, match="No station metadata"):
        p._station_params(datetime.datetime(2019, 1, 1, tzinfo=utc), history)


def test_radolan_parser_skips_non_file_tar_members(tmp_path: Path) -> None:
    """tarfile.extractfile returns None for members that are not regular
    files, such as a directory."""
    archive = tmp_path / 'only_a_directory.tar.bz2'
    with tarfile.open(archive, 'w:bz2') as out:
        info = tarfile.TarInfo('a_directory')
        info.type = tarfile.DIRTYPE
        out.addfile(info)
    assert list(RADOLANParser().parse(archive)) == []


def test_sanitize_synop_time_period_fields() -> None:
    record = {
        'precipitation_60': -1.0,
        'sunshine_30': 4000,
        'wind_direction_10': -5.0,
        'wind_speed_60': -2.0,
    }
    SYNOPParser().sanitize_record(record)
    assert record['precipitation_60'] == 0
    assert record['sunshine_30'] == 3600
    assert record['wind_direction_10'] == 355.0
    assert record['wind_speed_60'] == 0


MOSMIX_NS = {
    'kml': 'http://www.opengis.net/kml/2.2',
    'dwd': (
        'https://opendata.dwd.de/weather/lib/'
        'pointforecast_dwd_extension_V1_0.xsd'),
}


def _placemark(body: str = '') -> ET.Element:
    return ET.fromstring(
        '<kml:Placemark xmlns:kml="http://www.opengis.net/kml/2.2">'
        f'{body}</kml:Placemark>')


def test_mosmix_parser_skips_station_without_coordinates(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A station without coordinates is skipped with a warning."""
    place = _placemark(
        '<kml:name>01049</kml:name>'
        '<kml:description>Test Station</kml:description>')
    records = MOSMIXParser().parse_station(place, MOSMIX_NS, [], 'source')
    assert records == []
    assert "Ignoring station without coordinates" in caplog.text


def test_mosmix_parser_rejects_station_without_name() -> None:
    """Used to fail with "'NoneType' object has no attribute 'text'",
    without naming the element."""
    with pytest.raises(ValueError, match="Missing kml:name"):
        MOSMIXParser().parse_station(_placemark(), MOSMIX_NS, [], 'source')


def test_mosmix_parser_rejects_station_without_description() -> None:
    place = _placemark('<kml:name>01049</kml:name>')
    with pytest.raises(ValueError, match="Missing kml:description"):
        MOSMIXParser().parse_station(place, MOSMIX_NS, [], 'source')


def test_mosmix_parser_allows_empty_name_and_description() -> None:
    """Empty elements are data, not errors: only a missing one raises."""
    place = _placemark(
        '<kml:name/>'
        '<kml:description/>'
        '<kml:Point><kml:coordinates>9.0,50.0,100.0</kml:coordinates>'
        '</kml:Point>'
        '<kml:ExtendedData/>')
    records = list(
        MOSMIXParser().parse_station(
            place, MOSMIX_NS, [datetime.datetime(2024, 1, 1, tzinfo=utc)],
            'source'))
    assert len(records) == 1
    assert records[0]['station_name'] is None
    assert records[0]['wmo_station_id'] is None


def test_cap_parser_skips_event_code_with_empty_value_name() -> None:
    """An empty valueName must not abort the alert: the remaining
    eventCode elements still have to be searched for II."""
    ns = 'urn:oasis:names:tc:emergency:cap:1.2'
    info = ET.fromstring(
        f'<info xmlns="{ns}">'
        '<eventCode><valueName/><value>x</value></eventCode>'
        '<eventCode><valueName>II</valueName><value>42</value></eventCode>'
        '</info>')
    assert CAPParser()._parse_event_code(info) == 42


def test_mosmix_parser_rejects_empty_timestamp() -> None:
    """Used to fail inside re.sub, which will not take a None."""
    steps = ET.fromstring(
        '<TimeSteps xmlns:dwd="{}"><dwd:TimeStep/></TimeSteps>'.format(
            MOSMIX_NS['dwd']))
    with pytest.raises(ValueError, match="Empty dwd:TimeStep element"):
        MOSMIXParser().parse_timestamps(steps, MOSMIX_NS)


def test_cap_parser_rejects_empty_event_code_value() -> None:
    """Used to fail with int(None). An empty valueName is still skipped,
    but an empty value under the name we want is not recoverable."""
    ns = 'urn:oasis:names:tc:emergency:cap:1.2'
    info = ET.fromstring(
        f'<info xmlns="{ns}"><eventCode>'
        '<valueName>II</valueName><value/></eventCode></info>')
    with pytest.raises(ValueError, match="Empty cap:value element"):
        CAPParser()._parse_event_code(info)


def test_cap_parser_rejects_blank_event_code_value() -> None:
    """Whitespace-only text used to fail anonymously inside int()."""
    ns = 'urn:oasis:names:tc:emergency:cap:1.2'
    info = ET.fromstring(
        f'<info xmlns="{ns}"><eventCode>'
        '<valueName>II</valueName><value>   </value></eventCode></info>')
    with pytest.raises(ValueError, match="Empty cap:value element"):
        CAPParser()._parse_event_code(info)


def test_mosmix_parser_rejects_empty_product_id() -> None:
    """An empty ProductID used to leave source unset, so the next element
    reported "Unexpected issue time w/o ID" and blamed the wrong one."""
    kmz = (
        '<kml:kml xmlns:kml="{kml}" xmlns:dwd="{dwd}">'
        '<dwd:ProductID/><dwd:IssueTime>2024-01-01T00:00:00Z</dwd:IssueTime>'
        '</kml:kml>').format(**MOSMIX_NS)
    with pytest.raises(ValueError, match="Empty dwd:ProductID element"):
        list(MOSMIXParser()._parse_stream(io.BytesIO(kmz.encode())))


def test_mosmix_parser_rejects_empty_issue_time() -> None:
    """Used to fail on str concatenation with a None."""
    kmz = (
        '<kml:kml xmlns:kml="{kml}" xmlns:dwd="{dwd}">'
        '<dwd:ProductID>P</dwd:ProductID><dwd:IssueTime/>'
        '</kml:kml>').format(**MOSMIX_NS)
    with pytest.raises(ValueError, match="Empty dwd:IssueTime element"):
        list(MOSMIXParser()._parse_stream(io.BytesIO(kmz.encode())))


def test_cap_parser_rejects_empty_language() -> None:
    """Used to fail on str.split with a None."""
    ns = 'urn:oasis:names:tc:emergency:cap:1.2'
    info = ET.fromstring(f'<info xmlns="{ns}"><language/></info>')
    with pytest.raises(ValueError, match="Empty cap:language element"):
        CAPParser()._parse_info({}, info)


def test_parser_text_does_not_strip() -> None:
    """dwd:value is a padded, whitespace-separated list, so the text has to
    come back as it was."""
    el = ET.fromstring('<value>  1 2 3  </value>')
    assert Parser._text(el, 'value') == '  1 2 3  '


def test_parser_parse_accepts_the_extras_api_forwards() -> None:
    """`parse_url` forwards whatever `get_extra_urls` asked for, so the base
    signature has to admit them even though only some parsers use them."""
    with pytest.raises(NotImplementedError):
        Parser().parse('some.zip', meta_path='meta.zip')
