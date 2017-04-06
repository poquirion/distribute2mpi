#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup


requirements = ['mpi4py', 'psutil']

setup(
    name='ponyexpress',
    version='0.1.0  ',
    description="multiprocessing for HPC system ",
    long_description="",
    author="P.-O. Quirion",
    author_email='pioliqui@gmail.com',
    url='https://github.com/poquirion/poneyexpress/',
    packages=['ponyexpress', ],
    package_dir={'ponyexpress': 'ponyexpress'},
    include_package_data=True,
    install_requires=requirements,
    license="MIT",
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
