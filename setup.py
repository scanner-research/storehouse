from setuptools import setup
import os

os.system('ln -s ../build/libstorehouse.so storehousepy')
os.system('ln -s ../build/storehousepy.so storehousepy')

setup(
    name='storehouse',
    version='0.1.2',
    url='https://github.com/scanner-research/storehouse',
    author='Will Crichton',
    author_email='wcrichto@cs.stanford.edu',

    packages=['storehousepy'],
    package_data={
        'storehousepy': [
            '*.so',
        ]
    },

    license='Apache 2.0'
)
