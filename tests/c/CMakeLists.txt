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

add_executable(conformance_test
    $<TARGET_OBJECTS:common_obj>
    $<TARGET_OBJECTS:os_port_obj>
    conformance_test.c)
add_dependencies(conformance_test ex_cjson)

target_include_directories(conformance_test PRIVATE ${CJSON_INSTALL_PATH}/include)
target_link_directories(conformance_test PRIVATE ${CJSON_INSTALL_PATH}/lib)
if(NOT TODBC_WINDOWS)
    target_link_libraries(conformance_test odbc cjson pthread)
else()
    set_source_files_properties(conformance_test.c PROPERTIES COMPILE_OPTIONS "/source-charset:utf-8")
    target_link_libraries(conformance_test odbc32 cjson)
endif()

add_test(NAME conformance_test
    COMMAND conformance_test)

if(HAVE_VALGRIND)
  add_test(NAME Vconformance_test
      COMMAND ${CMAKE_SOURCE_DIR}/sh/valgrind.sh ${CMAKE_CURRENT_BINARY_DIR}/conformance_test)
endif()

if(ENABLE_MYSQL_TEST)
  add_test(NAME mysql_conformance_test
      COMMAND conformance_test --dsn MYSQL_ODBC_DSN)
  if(HAVE_VALGRIND)
    add_test(NAME Vmysql_conformance_test
        COMMAND ${CMAKE_SOURCE_DIR}/sh/valgrind.sh ${CMAKE_CURRENT_BINARY_DIR}/conformance_test --dsn MYSQL_ODBC_DSN)
  endif()
endif()

if(ENABLE_SQLITE3_TEST)
  add_test(NAME sqlite3_conformance_test
      COMMAND conformance_test --dsn SQLITE3_DSN)
  if(HAVE_VALGRIND)
    add_test(NAME Vsqlite3_conformance_test
        COMMAND ${CMAKE_SOURCE_DIR}/sh/valgrind.sh ${CMAKE_CURRENT_BINARY_DIR}/conformance_test --dsn SQLITE3_DSN)
  endif()
endif()

add_executable(api_test
    $<TARGET_OBJECTS:common_obj>
    $<TARGET_OBJECTS:os_port_obj>
    api_test.c)

if(NOT TODBC_WINDOWS)
    target_link_libraries(api_test odbc dl pthread)
else()
    set_source_files_properties(api_test.c PROPERTIES COMPILE_OPTIONS "/source-charset:utf-8")
    target_link_libraries(api_test odbc32)
endif()

add_test(NAME api_test
    COMMAND api_test)

if(HAVE_VALGRIND)
add_test(NAME Vapi_test
    COMMAND ${CMAKE_SOURCE_DIR}/sh/valgrind.sh ${CMAKE_CURRENT_BINARY_DIR}/api_test)
endif()

add_executable(edge_cases_test
    $<TARGET_OBJECTS:common_obj>
    $<TARGET_OBJECTS:os_port_obj>
    edge_cases_test.c)

if(NOT TODBC_WINDOWS)
    target_link_libraries(edge_cases_test odbc pthread)
else()
    set_source_files_properties(edge_cases_test.c PROPERTIES COMPILE_OPTIONS "/source-charset:utf-8")
    target_link_libraries(edge_cases_test odbc32)
endif()

add_test(NAME edge_cases_test
    COMMAND edge_cases_test)

if(HAVE_VALGRIND)
add_test(NAME Vedge_cases_test
    COMMAND ${CMAKE_SOURCE_DIR}/sh/valgrind.sh ${CMAKE_CURRENT_BINARY_DIR}/edge_cases_test)
endif()

add_executable(odbc_test
    $<TARGET_OBJECTS:common_obj>
    $<TARGET_OBJECTS:os_port_obj>
    odbc_test.c)
add_dependencies(odbc_test ex_cjson)

target_include_directories(odbc_test PRIVATE ${CJSON_INSTALL_PATH}/include ${CMAKE_CURRENT_BINARY_DIR}/..)
target_link_directories(odbc_test PRIVATE ${CJSON_INSTALL_PATH}/lib)
if(NOT TODBC_WINDOWS)
    target_link_libraries(odbc_test odbc cjson pthread)
else()
    target_link_libraries(odbc_test odbc32 cjson)
endif()

add_test(NAME odbc_test
    COMMAND odbc_test)

if(HAVE_VALGRIND)
  add_test(NAME Vodbc_test
      COMMAND ${CMAKE_SOURCE_DIR}/sh/valgrind.sh ${CMAKE_CURRENT_BINARY_DIR}/odbc_test)
endif()

if(ENABLE_MYSQL_TEST)
  add_test(NAME mysql_odbc_test
      COMMAND odbc_test --dsn MYSQL_ODBC_DSN)

  if(HAVE_VALGRIND)
    add_test(NAME Vmysql_odbc_test
        COMMAND ${CMAKE_SOURCE_DIR}/sh/valgrind.sh ${CMAKE_CURRENT_BINARY_DIR}/odbc_test --dsn MYSQL_ODBC_DSN)
  endif()
endif()

if(ENABLE_SQLITE3_TEST)
  add_executable(sqlite3_test
      $<TARGET_OBJECTS:common_obj>
      $<TARGET_OBJECTS:os_port_obj>
      sqlite3_test.c)

  target_link_libraries(sqlite3_test odbc)

  add_test(NAME sqlite3_test
      COMMAND sqlite3_test)

  if(HAVE_VALGRIND)
    add_test(NAME Vsqlite3_test
        COMMAND ${CMAKE_SOURCE_DIR}/sh/valgrind.sh ${CMAKE_CURRENT_BINARY_DIR}/sqlite3_test)
  endif()
endif()

