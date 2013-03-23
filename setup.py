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

from setuptools import setup
from distutils.extension import Extension


setup(
    name =          'centidb',
    version =       '0.1',
    description =   'Minimalist DBMS middleware for key/value stores.',
    author =        'David Wilson',
    author_email =  'dw@botanicus.net',
    license =       'Apache 2',
    url =           'http://github.com/dw/centidb/',
    zip_safe = False,
    packages =      ['centidb'],
    ext_modules = [
        Extension("_centidb", sources=['_centidb.c'],
                  extra_compile_args=['-std=c99', '-O2'])
    ])
