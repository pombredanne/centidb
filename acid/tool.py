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
Basic tools for working with Acid stores.

    info: Dump all database information.

    shell: Open interactive console with ENV set to the open environment.
"""

from __future__ import absolute_import
from __future__ import with_statement
import fcntl
import optparse
import os
import pprint
import signal
import struct
import sys
import termios

import acid


STORE = None
COLL = None


def make_parser():
    parser = optparse.OptionParser()
    parser.prog = 'python -macid'
    parser.usage = '%prog [options] <command>\n' + __doc__.rstrip()
    parser.add_option('-d', '--db', help='Database URL to open')
    parser.add_option('-c', '--coll', help='Collection to change')
    return parser


def die(fmt, *args):
    if args:
        fmt %= args
    sys.stderr.write('acid.tool: %s\n' % (fmt,))
    raise SystemExit(1)


def cmd_shell(opts, args):
    import code
    import readline
    code.InteractiveConsole(globals()).interact()


def cmd_info(opts, args):
    with STORE.begin():
        pprint.pprint(list(STORE._meta.items()))


def _get_term_width(default=80):
    try:
        s = fcntl.ioctl(sys.stdout.fileno(), termios.TIOCGWINSZ, '1234')
        height, width = struct.unpack('hh', s)
        return width, height
    except:
        return default


def _on_sigwinch(*args):
    global _TERM_WIDTH, _TERM_HEIGHT
    _TERM_WIDTH, _TERM_HEIGHT = _get_term_width()


def main():
    parser = make_parser()
    opts, args = parser.parse_args()

    if not args:
        die('Please specify a command (see --help)')
    if not opts.db:
        die('Please specify store URL (-d, --db)')

    global STORE
    STORE = acid.open(opts.db)
    signal.signal(signal.SIGWINCH, _on_sigwinch)
    _on_sigwinch()

    func = globals().get('cmd_' + args[0])
    if not func:
        die('No such command: %r' % (args[0],))

    func(opts, args[1:])


if __name__ == '__main__':
    main()
