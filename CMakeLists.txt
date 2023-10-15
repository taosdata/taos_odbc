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

cmake_minimum_required(VERSION 3.0...3.21)
project(taos_odbc)

set(TODBC_VERSION 0.1)

option(USE_SAN "whether to use sanitizer or not" OFF)
option(ENABLE_MYSQL_TEST "whether to test with mysql or not" OFF)
option(ENABLE_SQLITE3_TEST "whether to test with sqlite3 or not" OFF)
option(ENABLE_SQLSERVER_TEST "whether to test with sqlserver or not" OFF)
option(FAKE_TAOS "whether to fake `taos` or not" OFF)

if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  set(TODBC_LINUX TRUE)
elseif(${CMAKE_SYSTEM_NAME} MATCHES "Darwin")
  set(TODBC_DARWIN TRUE)
elseif(${CMAKE_SYSTEM_NAME} MATCHES "Windows")
  set(TODBC_WINDOWS TRUE)
else()
  message(FATAL_ERROR "Not ported on `${CMAKE_SYSTEM_NAME}`")
endif()

set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 11)

if (TODBC_WINDOWS)
  option(DISABLE_C5105 "whether to disable compiler warning C5105 or not" OFF)
endif()

if(NOT TODBC_WINDOWS)
  # add_compile_options(-Wall -Wextra -pedantic -Werror)
  add_compile_options(-Wall -Wextra -Werror -O0 -DODBCVER=0x0351 -fvisibility=hidden)
else()
  add_compile_options(/W3 /WX /DODBCVER=0x0351)
  if(DISABLE_C5105)
    add_compile_options(/wd5105)
  endif()
endif()

include(CTest)
include(cmake/macros.cmake)

set_colorful()
check_requirements()

include_directories(inc)
if(TODBC_DARWIN)
  include_directories(/usr/local/include)
  link_directories(/usr/local/lib)
  link_libraries(iconv)
endif()
if(TODBC_WINDOWS)
  include_directories(C:/TDengine/include ${ICONV_INSTALL_PATH}/include)
  link_directories(C:/TDengine/driver ${ICONV_INSTALL_PATH}/lib)
  link_libraries(iconv)
  #better set /source-charset:utf-8 for source-file-specific compile_options
  #add_compile_options(/source-charset:utf-8)
  add_compile_options(/DICONV_CONST=)
endif()
include_directories(${CMAKE_CURRENT_BINARY_DIR})

if(TODBC_DARWIN)
  message(STATUS "USE_SAN is ${USE_SAN}")
  if(USE_SAN)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fsanitize=undefined -fsanitize-recover=all -fsanitize=float-divide-by-zero -fsanitize=float-cast-overflow -fno-sanitize=shift-base -fno-sanitize=alignment")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address -fsanitize=undefined -fsanitize-recover=all -fsanitize=float-divide-by-zero -fsanitize=float-cast-overflow -fno-sanitize=shift-base -fno-sanitize=alignment")
  endif()
endif()

if(TODBC_WINDOWS)
  if(NOT DEFINED SERVER_FOR_TEST)
    message(FATAL_ERROR " you must define SERVER_FOR_TEST on windows platform,\n"
                        " because taosd is NOT included TDengine OSS since 3.1.x.x\n"
                        " eg.: -DSERVER_FOR_TEST:STRING=127.0.0.1:6030")
  endif()
  if(NOT DEFINED WS_FOR_TEST)
    message(FATAL_ERROR " you must define WS_FOR_TEST on windows platform,\n"
                        " because taosd is NOT included TDengine OSS since 3.1.x.x\n"
                        " eg.: -DWS_FOR_TEST:STRING=127.0.0.1:6041")
  endif()
endif()

if(DEFINED SERVER_FOR_TEST)
  string(FIND "${SERVER_FOR_TEST}" ":" HP_POS)
  if(${HP_POS} STREQUAL "-1")
    set(HOST_FOR_TEST "${SERVER_FOR_TEST}")
  else()
    string(SUBSTRING ${SERVER_FOR_TEST} 0 ${HP_POS} HOST_FOR_TEST)
    string(SUBSTRING ${SERVER_FOR_TEST} ${HP_POS} -1 PORT_FOR_TEST)
    string(SUBSTRING ${PORT_FOR_TEST} 1 -1 PORT_FOR_TEST)
  endif()
endif()

if(DEFINED WS_FOR_TEST)
  string(FIND "${WS_FOR_TEST}" ":" HP_POS)
  if(${HP_POS} STREQUAL "-1")
    set(WS_HOST_FOR_TEST "${WS_FOR_TEST}")
  else()
    string(SUBSTRING ${WS_FOR_TEST} 0 ${HP_POS} WS_HOST_FOR_TEST)
    string(SUBSTRING ${WS_FOR_TEST} ${HP_POS} -1 WS_PORT_FOR_TEST)
    string(SUBSTRING ${WS_PORT_FOR_TEST} 1 -1 WS_PORT_FOR_TEST)
  endif()
endif()

configure_file(inc/taos_odbc_config.h.in ${CMAKE_CURRENT_BINARY_DIR}/taos_odbc_config.h)

add_subdirectory(common)
add_subdirectory(src)
add_subdirectory(templates)
add_subdirectory(taosws)
add_subdirectory(tests)
add_subdirectory(samples)
add_subdirectory(benchmark)

