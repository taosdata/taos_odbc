cmake_minimum_required(VERSION 3.0)
project(taos_odbc)

set(TODBC_VERSION 0.1)

# add_compile_options(-Wall -Wextra -pedantic -Werror)
add_compile_options(-Wall -Wextra -Werror -O0)
set(CMAKE_C_STANDARD 11)
set(CMAKE_CXX_STANDARD 11)

if(${CMAKE_SYSTEM_NAME} MATCHES "Linux")
  set(TODBC_LINUX TRUE)
else()
  message(FATAL_ERROR "Not ported on `${CMAKE_SYSTEM_NAME}`")
endif()

find_library(TAOS NAMES taos)
if(${TAOS} STREQUAL TAOS-NOTFOUND)
  message(FATAL_ERROR "`libtaos.so` is required but not found")
endif()

include(CheckSymbolExists)
set(CMAKE_REQUIRED_LIBRARIES taos)
check_symbol_exists(taos_init "taos.h" HAVE_TAOS)
if(NOT HAVE_TAOS)
  message(FATAL_ERROR "`taos.h` is required but not found")
endif()

include(CTest)
include(cmake/macros.cmake)

check_odbc_requirement()

add_subdirectory(tests)
add_subdirectory(src)
add_subdirectory(templates)

