from setuptools import setup, Extension
import os
import shutil
from sys import platform

if platform == 'linux' or platform == 'linux2':
    EXT = '.so'
else:
    EXT = '.dylib'

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
ROOT_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))
BUILD_DIR = os.path.join(ROOT_DIR, 'build')
PIP_DIR = os.path.join(BUILD_DIR, 'pip')
SO_PATH = os.path.join(BUILD_DIR, 'libstorehouse' + EXT)
DEST_PATH = os.path.abspath('{p:s}/lib/libstorehouse{e:s}'.format(p=PIP_DIR,
                                                                  e=EXT))
#shutil.copyfile(SO_PATH, DEST_PATH)

module1 = Extension(
    'storehouse._python',
    include_dirs = [ROOT_DIR, os.path.join(ROOT_DIR, 'build')],
    libraries = ['storehouse'],
    library_dirs = [os.path.join(ROOT_DIR, 'build')],
    sources = [os.path.join(ROOT_DIR, 'storehouse/storehouse_python.cpp')],
    extra_compile_args=['-std=c++11'])

setup(
    name='storehouse',
    version='0.6.1',
    url='https://github.com/scanner-research/storehouse',
    author='Alex Poms and Will Crichton',
    author_email='wcrichto@cs.stanford.edu',
    packages=['storehouse'],
    license='Apache 2.0',
    ext_modules=[module1],
)
