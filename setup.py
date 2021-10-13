#!/usr/bin/env python
from typing import Final
from setuptools import setup

with open('VERSION.txt') as version_file:
    VERSION: Final[str] = version_file.readline().strip()

dependencies = []
with open('Pipfile') as file:
    aiobotocore_version_suffix = ''
    in_packages = False
    for line in file:
        if 'aiobotocore' in line:
            aiobotocore_version_suffix = line.split('"')[1]
        if in_packages:
            if line.strip() == '':
                in_packages = False
            else:
                package_name, version_string = line.split(' = "')
                version_string = version_string.replace('"', '').strip()
                package = f'{package_name}{version_string}'
                dependencies.append(package)
        if '[packages]' in line:
            in_packages = True

print(dependencies)

setup(name='hs-s3fs',
      version=VERSION,
      classifiers=[
          'Development Status :: 4 - Beta',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: BSD License',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 3.9',
      ],
      description='Convenient Filesystem interface over S3',
      url='http://github.com/dask/s3fs/',
      maintainer='Martin Durant',
      maintainer_email='mdurant@continuum.io',
      license='BSD',
      keywords='s3, boto',
      packages=['s3fs'],
      python_requires='>= 3.9.2',
      install_requires=dependencies,
      extras_require={
          'awscli': [
              f"aiobotocore[awscli]{aiobotocore_version_suffix}",
          ],
          'boto3': [
              f"aiobotocore[boto3]{aiobotocore_version_suffix}",
          ],
      },
      zip_safe=False)
