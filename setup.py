import re
import os.path
import sys
from setuptools import setup, find_packages


install_requires = ['python-snappy==0.6.1', 'aiohttp']
NAME = 'asyncnsq'
PACKAGE = 'asyncnsq'
PY_VER = sys.version_info

if PY_VER >= (3, 6):
    pass
else:
    raise RuntimeError("asyncnsq doesn't support Python version prior 3.6")


def read(*parts):
    with open(os.path.join(*parts), 'rt') as f:
        return f.read().strip()


def read_version():
    regexp = re.compile(r"^__version__\W*=\W*'([\d.abrc]+)'")
    init_py = os.path.join(os.path.dirname(__file__),
                           'asyncnsq', '__init__.py')
    with open(init_py) as f:
        for line in f:
            match = regexp.match(line)
            if match is not None:
                return match.group(1)
        else:
            raise RuntimeError('Cannot find version in asyncnsq/__init__.py')


classifiers = [
    'License :: OSI Approved :: MIT License',
    'Development Status :: 4 - Beta',
    'Programming Language :: Python',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.7',
    'Programming Language :: Python :: 3.8',
    'Operating System :: POSIX',
    'Environment :: Web Environment',
    'Intended Audience :: Developers',
    'Topic :: Software Development',
    'Topic :: Software Development :: Libraries',
]


if os.path.exists('README.md'):
    with open('README.md', 'r') as f:
        long_description = f.read()
else:
    long_description = 'See http://pypi.python.org/pypi/%s' % (NAME,)

setup(name='asyncnsq',
      version=read_version(),
      description=("asyncio async/await nsq support"),
      long_description=long_description,
      long_description_content_type="text/markdown",
      classifiers=classifiers,
      platforms=["POSIX"],
      author="aohan237",
      author_email="aohan237@gmail.com",
      url="https://github.com/aohan237/asyncnsq",
      license="MIT",
      packages=find_packages(exclude=["tests"]),
      install_requires=install_requires,
      include_package_data=True,
      )
