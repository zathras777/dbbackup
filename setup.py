from setuptools import setup, find_packages
from os import path
import io
from glob import glob

from dbbackup import __version__


here = path.abspath(path.dirname(__file__))

# Get the long description from the relevant file
with io.open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='dbbackup',
    version=__version__,
    description='Database backup utility',
    long_description=long_description,
    url='https://github.com/zathras777/dbbackup',
    author='david reid',
    author_email='zathrasorama@gmail.com',
    license='Unlicense',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
    ],
    keywords='database backup automated mysql postgresql',
    data_files=[('/usr/local/etc', glob("conf/*.conf"))],
    packages=find_packages(exclude=['tests']),
    install_requires=['psycopg2'],
    entry_points={
        'console_scripts': ['dbbackup=dbbackup.command_line:main']
    }
)
