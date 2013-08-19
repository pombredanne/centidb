#!/usr/bin/env python
#
# Copyright 2013, David Wilson
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""centidb distutils script.
"""

from __future__ import absolute_import
import os
import sys
import platform

from setuptools import Extension
from setuptools import setup


if hasattr(platform, 'python_implementation'):
    use_cpython = platform.python_implementation() == 'CPython'
else:
    use_cpython = True

extra_compile_args = ['-std=c99']
ext_modules = []
if use_cpython:
    ext_modules = [
        Extension("_centidb", sources=['ext/centidb.c'],
                  extra_compile_args=extra_compile_args),
        Extension("_keycoder", sources=['ext/keycoder.c', 'ext/tzinfo.c'],
                  extra_compile_args=extra_compile_args)
    ]


setup(
    name =          'centidb',
    version =       '0.11',
    description =   'Minimalist DBMS middleware for key/value stores.',
    author =        'David Wilson',
    author_email =  'dw@botanicus.net',
    license =       'Apache 2',
    url =           'http://github.com/dw/centidb/',
    zip_safe =      False,
    packages =      ['centidb'],
    ext_package =   'centidb',
    ext_modules =   ext_modules
)
