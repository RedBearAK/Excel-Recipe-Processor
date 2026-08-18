"""
Setup shim for excel_recipe_processor.

setup.py

All project metadata lives in pyproject.toml (the [project] table); the
version in excel_recipe_processor/_version.py and the dependencies in
requirements.txt are read from there dynamically. This file exists only so
legacy `python setup.py ...` invocations keep working - do not add metadata
here, it would be ignored or fight the pyproject values.
"""

from setuptools import setup

setup()

# End of file #
