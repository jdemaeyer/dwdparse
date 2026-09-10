from collections.abc import Callable, Mapping
from typing import Any


def celsius_to_kelvin(temperature: float) -> float:
    return round(temperature + 273.15, 2)


def eighths_to_percent(eighths: float) -> int:
    return int(eighths / 8 * 100)


def hpa_to_pa(pressure: float) -> int:
    return int(pressure * 100)


def kelvin_to_celsius(temperature: float) -> float:
    return round(temperature - 273.15, 2)


def j_per_cm2_to_j_per_m2(solar: float) -> float:
    return solar * 10000


def j_per_m2_to_kwh_per_m2(solar: float) -> float:
    return round(solar / 3600000, 3)


def kj_per_m2_to_j_per_m2(solar: float) -> float:
    return solar * 1000


def km_to_m(distance: float) -> float:
    return distance * 1000


def kmh_to_ms(speed: float) -> float:
    return round(speed / 3.6, 1)


def minutes_to_seconds(duration: float) -> int:
    return int(duration * 60)


def ms_to_kmh(speed: float) -> float:
    return round(speed * 3.6, 1)


def pa_to_hpa(pressure: float) -> float:
    return pressure / 100


def seconds_to_minutes(duration: float) -> float:
    return duration / 60


def w_per_m2_to_hourly_j_per_m2(solar: float) -> float:
    return solar * 3600


# XXX: Subsequent codes with the same mapping are left out. I.e. all codes from
#      0 through 16 map to 'dry', etc.
SYNOP_CURRENT_CONDITION_MAP = {
    0: 'dry',
    17: 'thunderstorm',
    18: 'dry',
    40: 'fog',
    50: 'rain',
    56: 'sleet',
    58: 'rain',
    66: 'sleet',
    70: 'snow',
    80: 'rain',
    83: 'sleet',
    85: 'snow',
    87: 'sleet',
    89: 'hail',
    91: 'rain',
    93: 'snow',
    95: 'thunderstorm',
    100: 'dry',
    130: 'fog',
    140: 'rain',
    145: 'snow',
    147: 'sleet',
    150: 'rain',
    154: 'sleet',
    157: 'rain',
    164: 'sleet',
    170: 'snow',
    174: 'hail',
    180: 'rain',
    185: 'sleet',
    189: 'hail',
    190: 'thunderstorm',
    193: 'hail',
    194: 'thunderstorm',
    196: 'hail',
    197: None,
    199: 'thunderstorm',
    200: None,
}


SYNOP_PAST_CONDITION_MAP = {
    0: 'dry',
    4: 'fog',
    5: 'rain',
    7: 'snow',
    8: 'rain',
    9: 'thunderstorm',
    10: 'dry',
    11: 'fog',
    14: 'rain',
    17: 'snow',
    18: 'rain',
    19: 'thunderstorm',
    20: None,
}


SYNOP_FORM_OF_PRECIPITATION_CONDITION_MAP = {
    0: 'dry',
    6: 'rain',
    7: 'snow',
    8: 'sleet',
    9: 'rain',
    10: 'dry',
    11: 'rain',
    12: 'snow',
    13: 'sleet',
    14: 'rain',
}


CURRENT_OBSERVATIONS_CONDITION_MAP = {
    1: 'dry',
    5: 'fog',
    7: 'rain',
    10: 'sleet',
    14: 'snow',
    18: 'rain',
    20: 'sleet',
    22: 'snow',
    26: 'thunderstorm',
    29: 'hail',
    31: 'dry',
    32: None,
}


def _find(
    mapping: Mapping[int, str | None],
    code: float | None,
) -> str | None:
    """Look up a weather code in a sorted threshold mapping.

    Each mapping must end with a sentinel entry whose value is None,
    so that codes beyond the last real entry return None rather than
    silently returning the last real value.
    """
    if code is None:
        return None
    value = None
    for k, v in mapping.items():
        if k > code:
            return value
        value = v
    return None


def synop_current_weather_code_to_condition(code: float | None) -> str | None:
    return _find(SYNOP_CURRENT_CONDITION_MAP, code)


def synop_past_weather_code_to_condition(code: float | None) -> str | None:
    return _find(SYNOP_PAST_CONDITION_MAP, code)


def synop_form_of_precipitation_code_to_condition(
    code: float | None,
) -> str | None:
    return _find(SYNOP_FORM_OF_PRECIPITATION_CONDITION_MAP, code)


def current_observations_weather_code_to_condition(
    code: float | None,
) -> str | None:
    return _find(CURRENT_OBSERVATIONS_CONDITION_MAP, code)


CONVERTERS: dict[str, dict[str, Callable[[float], float]]] = {
    'dwd': {
        'dew_point': kelvin_to_celsius,
        'pressure_msl': pa_to_hpa,
        'solar': j_per_m2_to_kwh_per_m2,
        'solar_10': j_per_m2_to_kwh_per_m2,
        'solar_30': j_per_m2_to_kwh_per_m2,
        'solar_60': j_per_m2_to_kwh_per_m2,
        'sunshine': seconds_to_minutes,
        'sunshine_10': seconds_to_minutes,
        'sunshine_30': seconds_to_minutes,
        'sunshine_60': seconds_to_minutes,
        'temperature': kelvin_to_celsius,
        'wind_speed': ms_to_kmh,
        'wind_speed_10': ms_to_kmh,
        'wind_speed_30': ms_to_kmh,
        'wind_speed_60': ms_to_kmh,
        'wind_gust_speed': ms_to_kmh,
        'wind_gust_speed_10': ms_to_kmh,
        'wind_gust_speed_30': ms_to_kmh,
        'wind_gust_speed_60': ms_to_kmh,
    }
}


def convert_record(
    record: dict[str, Any],
    units: str,
) -> dict[str, Any]:
    for field, converter in CONVERTERS[units].items():
        if record.get(field) is not None:
            record[field] = converter(record[field])
    return record
