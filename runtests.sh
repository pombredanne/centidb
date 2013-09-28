#!/bin/sh

# Attempt to trigger timezone-related bugs.
export TZ=America/Caracas

PYTHONPATH=tests python -munittest "$@" core_test engines_test iterator_test keylib_test meta_test
