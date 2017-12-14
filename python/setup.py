from setuptools import setup
import os

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
SO_PATH = os.path.abspath('{p:s}/../build/libstorehouse.so'.format(
    p=SCRIPT_DIR))
DEST_PATH = os.path.abspath('{p:s}/storehouse/'.format(p=SCRIPT_DIR))
os.system('ln -s {from_path:s} {to_path:s}'.format(from_path=SO_PATH,
                                                   to_path=DEST_PATH))

setup(
    name='storehouse',
    version='0.4.0',
    url='https://github.com/scanner-research/storehouse',
    author='Alex Poms and Will Crichton',
    author_email='wcrichto@cs.stanford.edu',

    packages=['storehouse'],
    package_data={
        'storehouse': [
            '*.so',
        ]
    },

    license='Apache 2.0'
)
