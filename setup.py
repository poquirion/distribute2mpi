#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup


with open('requirements.txt') as req_file:
    requirements = req_file.readlines()

setup(
    name='ponyexpress',
    version='0.0.1',
    description="",
    long_description="",
    author="",
    author_email='',
    url='https://github.com/poquirion/poor_man_scheduler/',
    packages=['ponyexpress', ],
    package_dir={'ponyexpress': 'ponyexpress'},
    include_package_data=True,
    install_requires=requirements,
    license="{{ cookiecutter.open_source_license }}",
    zip_safe=False,
    keywords='',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    test_suite='tests',
)
