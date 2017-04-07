#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup


requirements = ['mpi4py', 'psutil']

setup(
    name='distribute2mpi',
    version='0.2.2',
    description="Multiprocessing for HPC system using MPI",
    long_description="Expand the fonctionality of the multiprocessing module from multiple processes on a single machine to multiple processes on multiple machines.",
    author="P.-O. Quirion",
    author_email='pioliqui@gmail.com',
    url='https://github.com/poquirion/distribute2mpi/',
    py_modules = ['distribute2mpi'],
    include_package_data=True,
    install_requires=requirements,
    license="MIT",
    zip_safe=False,
    keywords='',
    classifiers=[
        'Development Status :: 4 - Beta',
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
