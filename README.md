# dwdparse

[![Build Status](https://img.shields.io/github/actions/workflow/status/jdemaeyer/dwdparse/main.yml)](https://github.com/jdemaeyer/dwdparse/actions)
[![PyPI Release](https://img.shields.io/pypi/v/dwdparse)](https://pypi.org/project/dwdparse/)

### Parsers for DWD's open weather data.

The DWD ([Deutscher Wetterdienst](https://www.dwd.de/)), as Germany's
meteorological service, publishes a myriad of meteorological observations and
calculations as part of their [Open Data
program](https://www.dwd.de/DE/leistungen/opendata/opendata.html).

`dwdparse` is a Python library for parsing the various formats that this data
is published in, with no dependencies outside the standard library. It serves
as the parsing backend for [Bright Sky](https://brightsky.dev/).

Our development effort focuses mainly on the data that is made available
through Bright Sky, although we are very open to requests or contributions
regarding other parsing targets. If you are looking for a more complete set of
parsers - including for data from other weather agencies - and you don't mind
the extra dependencies, take a look at the excellent
[`wetterdienst`](https://github.com/earthobservations/wetterdienst).


### Looking for something specific?

#### I just want to retrieve some weather data

You can use the free [public Bright Sky instance](https://brightsky.dev/)!

#### I want to parse DWD weather files or contribute to dwdparse's source code

Read on. :)


## Quickstart

You can use this package both as a stand-alone command-line tool or as a Python
library.


### Stand-alone DWD file parsing

1. Install the `dwdparse` package from PyPI:
   ```bash
   $ pip install dwdparse
   ```

2. Call `dwdparse`, providing your target file (or URL) as argument:
   ```bash
   $ dwdparse stundenwerte_TU_01766_akt.zip
   ```

This will output a newline-separated list of JSON records. **Note that all
numerical weather data is converted to SI units.** If you wish to use DWD
units, or if you need both DWD and WMO station IDs, check out the `--units`
option and the example section with `dwdparse --help`.


### Parsing DWD files from Python code

1. Install the `dwdparse` package from PyPI:
   ```bash
   $ pip install dwdparse
   ```

2. In Python, import one of the `dwdparse` parsers (or the `get_parser`
   utility function) from `dwdparse`, then call it's `parse()` method with the
   path of the file you would like to parse. This will return an iterable over
   weather records:
   ```python
   from brightsky import get_parser


   DWD_FILE_PATH = 'stundenwerte_TU_01766_akt.zip'

   parser_class = get_parser(DWD_FILE_PATH)
   parser = parser_class()

   for record in parser.parse(DWD_FILE_PATH):
       print(record)
   ```


## Acknowledgements

`dwdparse` is developed as the main parsing core for [Bright
Sky](https://github.com/jdemaeyer/brightsky). Bright Sky's development is
boosted by the priceless guidance and support of the [Open Knowledge
Foundation](https://www.okfn.de/)'s [Prototype Fund](https://prototypefund.de/)
program, and is generously funded by Germany's [Federal Ministry of Education
and Research](https://www.bmbf.de/). Obvious as it may be, it should be
mentioned that none of this would be possible without the painstaking,
never-ending effort of the [Deutscher Wetterdienst](https://www.dwd.de/).

<a href="https://prototypefund.de/"><img src="docs/img/pf.svg" alt="Prototype Fund" height="100"></a>&nbsp;&nbsp;&nbsp;&nbsp;
<a href="https://okfn.de/"><img src="docs/img/okfde.svg" alt="Open Knowledge Foundation Germany" height="100"></a>&nbsp;&nbsp;&nbsp;&nbsp;
<a href="https://www.bmbf.de/"><img src="docs/img/bmbf.svg" alt="Bundesministerium für Bildung und Forschung" height="100"></a>&nbsp;&nbsp;&nbsp;&nbsp;
<a href="https://www.dwd.de/"><img src="docs/img/dwd.svg" alt="Deutscher Wetterdienst" height="100"></a>
