#!/usr/bin/env python

import readline
import os
os.environ['PYTHONINSPECT'] = '1'
from models import *

store = init_store()
txn = store.begin()
txn.__enter__()

