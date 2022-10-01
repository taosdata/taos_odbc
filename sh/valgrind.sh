#!/bin/bash

_path_to_this_file=$( { pushd $(dirname "$0") >/dev/null; pwd; popd >/dev/null; } )
_path_to_valgrind=${_path_to_this_file}/../valgrind


valgrind --leak-check=full                                                \
         --show-leak-kinds=all                                            \
         --num-callers=100                                                \
         --exit-on-first-error=no                                         \
         --error-exitcode=1                                               \
         --suppressions=${_path_to_valgrind}/valgrind.supp                \
         --suppressions=${_path_to_valgrind}/unixodbc.supp                \
         --suppressions=${_path_to_valgrind}/rust.supp                    \
         --suppressions=${_path_to_valgrind}/node.supp                    \
         --suppressions=/usr/lib/x86_64-linux-gnu/valgrind/default.supp   \
         --gen-suppressions=all                                           \
         "$@"

