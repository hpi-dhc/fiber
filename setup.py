#!/usr/bin/env python
# -*- coding: utf-8 -*-

'The setup script.'

from setuptools import setup, find_packages

with open('README.md') as readme_file:
    readme = readme_file.read()

with open('HISTORY.md') as history_file:
    history = history_file.read()

requirements = []

setup_requirements = ['pytest-runner', ]

test_requirements = ['pytest', ]

setup(
    author='HPI FIBER Team',
    author_email='philipp.bode@student.hpi.de',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
    ],
    description=(
        'Fiber enables you to query the MSDW for high-level concepts, '
        'without worrying about database internals.'
    ),
    install_requires=requirements,
    license='MIT license',
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='fiber',
    name='fiber',
    packages=find_packages(include=['fiber']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://gitlab.hpi.de/fiber/fiber',
    version='0.1.0',
    zip_safe=False,
)
