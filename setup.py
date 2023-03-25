from setuptools import setup

import dwdparse


with open('README.md') as f:
    long_description = f.read()

setup(
    name='dwdparse',
    version=dwdparse.__version__,
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
    },
    entry_points={
        'console_scripts': ['dwdparse = dwdparse.cli:main'],
    },
)
