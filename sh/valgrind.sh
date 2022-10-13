#!/bin/bash

_path_to_this_file=$( { pushd $(dirname "$0") >/dev/null; pwd; popd >/dev/null; } )
_path_to_valgrind=${_path_to_this_file}/../valgrind


valgrind --leak-check=full                                                \
         --show-leak-kinds=all                                            \
         --show-reachable=no                                              \
         --num-callers=100                                                \
         --exit-on-first-error=no                                         \
         --error-exitcode=1                                               \
         --suppressions=/usr/lib/x86_64-linux-gnu/valgrind/default.supp   \
         --suppressions=${_path_to_valgrind}/taos.supp                    \
         --suppressions=${_path_to_valgrind}/node.supp                    \
         --gen-suppressions=all                                           \
         --track-origins=yes                                              \
         "$@"

