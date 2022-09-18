#!/bin/bash

_path_to_this_file=$( { pushd $(dirname "$0") >/dev/null; pwd; popd >/dev/null; } )

valgrind --leak-check=full                                                \
         --show-leak-kinds=all                                            \
         --num-callers=100                                                \
         --exit-on-first-error=yes                                        \
         --error-exitcode=1                                               \
         --suppressions=${_path_to_this_file}/valgrind.supp               \
         --gen-suppressions=all                                           \
         "$@"

