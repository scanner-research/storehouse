from setuptools import setup
import os
import shutil

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
STOREHOUSE_DIR = '.'
BUILD_DIR = os.path.join(STOREHOUSE_DIR, 'build')
PIP_DIR = os.path.join(BUILD_DIR, 'pip')
SO_PATH = os.path.join(BUILD_DIR, 'libstorehouse.so')

shutil.rmtree(PIP_DIR, ignore_errors=True)
shutil.copytree(SCRIPT_DIR, PIP_DIR)
shutil.copy(SO_PATH, os.path.join(PIP_DIR, 'storehouse'))

setup(
    name='storehouse',
    version='0.4.0',
    url='https://github.com/scanner-research/storehouse',
    author='Alex Poms and Will Crichton',
    author_email='wcrichto@cs.stanford.edu',

    package_dir={'': PIP_DIR},
    packages=['storehouse'],
    package_data={
        'storehouse': [
            '*.so',
        ]
    },

    license='Apache 2.0'
)
