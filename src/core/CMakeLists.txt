###############################################################################
# MIT License
#
# Copyright (c) 2022 freemine <freemine@yeah.net>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
###############################################################################

list(APPEND core_SOURCES charset.c)
list(APPEND core_SOURCES conn.c)
list(APPEND core_SOURCES desc.c)
list(APPEND core_SOURCES env.c)
list(APPEND core_SOURCES errs.c)
list(APPEND core_SOURCES stmt.c)
list(APPEND core_SOURCES tls.c)

add_library(core_obj OBJECT ${core_SOURCES})
target_include_directories(core_obj PRIVATE
    ${INTERNAL_INC_PATH}
    ${ODBC_INCLUDE_DIRECTORY}
    ${ODBCINST_INCLUDE_DIRECTORY})
set_property(TARGET core_obj PROPERTY POSITION_INDEPENDENT_CODE 1)

