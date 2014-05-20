#
# Copyright 2013, David Wilson.
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

from __future__ import absolute_import
import os
import sys

# Hack: disable speedups while testing or reading docstrings.
_use_speedups = (
    os.path.basename(sys.argv[0]) not in ('sphinx-build', 'pydoc') and \
    os.getenv('ACID_NO_SPEEDUPS') is None)

if _use_speedups:
    try:
        import acid._acid
    except ImportError:
        pass

from acid import core
from acid.core import *
from acid.keylib import Key
__all__ = core.__all__
__doc__ = core.__doc__
__version__ = '0.0.16'

# Hack to support Python v2.5 'python -macid'
if __name__ == '__main__':
    import acid.tool
    import atexit
    atexit.register(acid.tool.main)
