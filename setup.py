#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-telemetrics",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["telemetrics"],
    install_requires=[
        "singer-python",
        "requests",
        "futures3==1.0.0"
    ],
    entry_points="""
    [console_scripts]
    tap-telemetrics=tap_telemetrics:main
    """,
    packages=["tap_telemetrics"],
    package_data = {
        "schemas": ["tap_telemetrics/schemas/*.json"]
    },
    include_package_data=True,
)
