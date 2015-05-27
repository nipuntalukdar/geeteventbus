#!/usr/bin/env python

import os

from setuptools import setup, find_packages


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name='geeteventbus',
    version='1.0',
    description='An eventbus for highly concurrent system',
    long_description=read('README.rst'),
    author='Nipun Talukdar',
    author_email='nipunmlist@gmail.com',
    maintainer='Nipun Talukdar',
    maintainer_email='nipunmlist@gmail.com',
    url='https://github.com/nipuntalukdar/NipunTalukdarExamples',
    license='MIT',
    packages=find_packages(),
    install_requires=[],
    extras_require={},
    entry_points={},
    platforms=['all'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
    ],
)
