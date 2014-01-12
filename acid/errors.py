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

"""
All custom exceptions thrown by Acid should be defined here.
"""


class Error(Exception):
    """Base class for Acid exceptions."""
    def __init__(self, msg, inner=None):
        Exception.__init__(self, msg)
        #: The inner exception, if any.
        self.inner = inner


class AbortError(Error):
    """Used by :py:func:`acid.abort` to trigger graceful abort of the active
    transaction."""


class ConfigError(Error):
    """Attempt to use a store in a misconfigured state (e.g. missing index
    functions, or incompatible constructor options)."""


class ConstraintError(Error):
    """An acid.meta model constraint failed."""
    def __init__(self, msg, name):
        Error.__init__(self, msg, None)
        #: String name of the constraint function that failed.
        self.name = name


class EngineError(Error):
    """Unspecified error occurred with the database engine. The original
    exception may be available as the :py:attr:`inner` attribute."""


class NameInUse(Error):
    """Attempt to rename an object to a name already in use."""


class NotFound(Error):
    """Attempt to fetch an object that doesn't exist."""


class TxnError(Error):
    """An attempt to start, cancel or commit a transaction failed."""
