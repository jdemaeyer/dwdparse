import re

from setuptools import setup


def get_version():
    with open('dwdparse/__init__.py') as f:
        match = re.search(r"__version__\s*=\s*['\"]([^'\"]+)['\"]", f.read())
        if not match:
            raise RuntimeError("Unable to find version string")
        return match.group(1)


with open('README.md') as f:
    long_description = f.read()

setup(
    name='dwdparse',
    version=get_version(),
    author='Jakob de Maeyer',
    author_email='jakob@naboa.de',
    description="Parsers for DWD's open weather data.",
    long_description=long_description,
    long_description_content_type='text/markdown',
    project_urls={
        'Source': 'https://github.com/jdemaeyer/dwdparse/',
        'Tracker': 'https://github.com/jdemaeyer/dwdparse/issues/',
    },
    packages=['dwdparse'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.8',
    extras_require={
        'lean': [
            'ijson',
        ],
        'radar': [
            'h5py',
        ],
        'all': [
            'h5py',
            'ijson',
        ],
    },
    entry_points={
        'console_scripts': ['dwdparse = dwdparse.cli:main'],
    },
)
