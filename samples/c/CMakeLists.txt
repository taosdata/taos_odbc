###############################################################################
# MIT License
#
# Copyright (c) 2022-2023 freemine <freemine@yeah.net>
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

add_executable(dump
    $<TARGET_OBJECTS:common_obj>
    $<TARGET_OBJECTS:os_port_obj>
    dump.c)
add_dependencies(dump ex_cjson)

target_include_directories(dump PRIVATE ${CJSON_INSTALL_PATH}/include)
target_link_directories(dump PRIVATE ${CJSON_INSTALL_PATH}/lib)
if(NOT TODBC_WINDOWS)
    target_link_libraries(dump odbc cjson pthread)
else()
    set_source_files_properties(dump.c PROPERTIES COMPILE_OPTIONS "/source-charset:utf-8")
    target_link_libraries(dump odbc32 cjson)
endif()

add_executable(demo_topic
    $<TARGET_OBJECTS:common_obj>
    $<TARGET_OBJECTS:os_port_obj>
    demo_topic.c)

if(NOT TODBC_WINDOWS)
    target_link_libraries(demo_topic odbc pthread)
else()
    target_link_libraries(demo_topic odbc32)
endif()

