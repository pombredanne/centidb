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

"""sortedfile distutils script.
"""

from distutils.core import setup
from distutils.extension import Extension

# Install Cython's builder if available, otherwise use the pre-generated C file
# in the repository.
try:
    import Cython.Distutils
    kwargs = dict(cmdclass={
        'build_ext': Cython.Distutils.build_ext
    })
    mod_filename = '_centidb.pyx'
except ImportError:
    kwargs = {}
    mod_filename = '_centidb.c'


setup(
    name =          'centidb',
    version =       '0.1',
    description =   'Minimalist DBMS middleware for key/value stores.',
    author =        'David Wilson',
    author_email =  'dw@botanicus.net',
    license =       'Apache 2',
    url =           'http://github.com/dw/centidb/',
    packages =      ['centidb'],
    ext_modules = [
        Extension("_centidb", sources=[mod_filename])
    ],
    **kwargs
)
