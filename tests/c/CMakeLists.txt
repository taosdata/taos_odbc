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

add_executable(api_test
    $<TARGET_OBJECTS:common_obj>
    api_test.c)

target_link_libraries(api_test odbc dl)

add_test(NAME api_test
    COMMAND api_test)

if(HAVE_VALGRIND)
  add_test(NAME Vapi_test
      COMMAND ${CMAKE_SOURCE_DIR}/sh/valgrind.sh ${CMAKE_CURRENT_BINARY_DIR}/api_test)
endif()

add_executable(edge_cases_test
    $<TARGET_OBJECTS:common_obj>
    edge_cases_test.c)

target_link_libraries(edge_cases_test odbc)

add_test(NAME edge_cases_test
    COMMAND edge_cases_test)

if(HAVE_VALGRIND)
  add_test(NAME Vedge_cases_test
      COMMAND ${CMAKE_SOURCE_DIR}/sh/valgrind.sh ${CMAKE_CURRENT_BINARY_DIR}/edge_cases_test)
endif()

add_executable(odbc_test
    $<TARGET_OBJECTS:common_obj>
    odbc_test.c)
add_dependencies(odbc_test ex_cjson)

target_include_directories(odbc_test PRIVATE ${CJSON_INSTALL_PATH}/include)
target_link_directories(odbc_test PRIVATE ${CJSON_INSTALL_PATH}/lib)
target_link_libraries(odbc_test odbc cjson)

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

