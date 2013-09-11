from setuptools import setup, find_packages
import sys, os

version = '0.1'

setup(
	name='ckanext-geogratis',
	version=version,
	description="CKAN client for the Geogratis API from NRCAN",
	long_description="""\
	""",
	classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
	keywords='',
	author='Ross Thompson',
	author_email='ross.thompson@statcan.gc.ca',
	url='http://data.gc.ca',
	license='MIT',
	packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
	namespace_packages=['ckanext', 'ckanext.geogratis'],
	include_package_data=True,
	zip_safe=False,
	install_requires=[
		# -*- Extra requirements: -*-
	],
    entry_points=\
    """
    [paste.paster_command]
    geogratis=ckanext.geogratis.commands:GeogratisCommand
    """,
)
