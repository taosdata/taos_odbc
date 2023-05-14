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

macro(check_requirements)
  if(NOT WIN32)
    string(ASCII 27 Esc)
    set(ColorReset  "${Esc}[m")
    set(ColorBold   "${Esc}[1m")
    set(Red         "${Esc}[31m")
    set(Green       "${Esc}[32m")
    set(Yellow      "${Esc}[33m")
    set(Blue        "${Esc}[34m")
    set(Magenta     "${Esc}[35m")
    set(Cyan        "${Esc}[36m")
    set(White       "${Esc}[37m")
    set(BoldRed     "${Esc}[1;31m")
    set(BoldGreen   "${Esc}[1;32m")
    set(BoldYellow  "${Esc}[1;33m")
    set(BoldBlue    "${Esc}[1;34m")
    set(BoldMagenta "${Esc}[1;35m")
    set(BoldCyan    "${Esc}[1;36m")
    set(BoldWhite   "${Esc}[1;37m")
  endif()

  set(TAOS_ODBC_LOCAL_REPO ${CMAKE_SOURCE_DIR}/.externals)
  include(CheckSymbolExists)
  include(ExternalProject)

  ## prepare `cjson`
  set(CJSON_INSTALL_PATH ${TAOS_ODBC_LOCAL_REPO}/install)
  ExternalProject_Add(ex_cjson
      GIT_REPOSITORY https://github.com/taosdata-contrib/cJSON.git
      GIT_TAG v1.7.15
      GIT_SHALLOW TRUE
      PREFIX "${TAOS_ODBC_LOCAL_REPO}/build/cjson"
      CMAKE_ARGS "-DCMAKE_INSTALL_PREFIX:PATH=${CJSON_INSTALL_PATH}"
      CMAKE_ARGS "-DBUILD_SHARED_LIBS:BOOL=OFF"
      CMAKE_ARGS "-DENABLE_CJSON_TEST:BOOL=OFF"
      )

  ## prepare `iconv`
  if(TODBC_WINDOWS)
    set(ICONV_INSTALL_PATH ${TAOS_ODBC_LOCAL_REPO}/install)
    ExternalProject_Add(ex_iconv
        GIT_REPOSITORY https://github.com/win-iconv/win-iconv.git
        GIT_TAG v0.0.8
        GIT_SHALLOW TRUE
        PREFIX "${TAOS_ODBC_LOCAL_REPO}/build/iconv"
        CMAKE_ARGS "-DCMAKE_INSTALL_PREFIX:PATH=${ICONV_INSTALL_PATH}"
        CMAKE_ARGS "-DBUILD_STATIC:BOOL=ON"
        CMAKE_ARGS "-DBUILD_SHARED:BOOL=OFF"
        )
  endif()

  ## check `taos`
  find_library(TAOS NAMES taos PATHS C:/TDengine/driver)
  if(${TAOS} STREQUAL TAOS-NOTFOUND)
    message(FATAL_ERROR "${Red}`libtaos.so` is required but not found, you may refer to https://github.com/taosdata/TDengine${ColorReset}")
  endif()

  set(CMAKE_REQUIRED_LIBRARIES taos)
  if(TODBC_DARWIN)
    set(CMAKE_REQUIRED_INCLUDES /usr/local/include)
    set(CMAKE_REQUIRED_LINK_OPTIONS -L/usr/local/lib)
  elseif(TODBC_WINDOWS)
    set(CMAKE_REQUIRED_INCLUDES C:/TDengine/include)
    set(CMAKE_REQUIRED_LINK_OPTIONS /LIBPATH:C:/TDengine/driver)
  endif()
  check_symbol_exists(taos_query "taos.h" HAVE_TAOS)
  if(NOT HAVE_TAOS)
    message(FATAL_ERROR "${Red}`taos.h` is required but not found, you may refer to https://github.com/taosdata/TDengine${ColorReset}")
  endif()

  if(TODBC_DARWIN)
    ## check `iconv`
    find_package(Iconv)
    if(NOT Iconv_FOUND)
      message(FATAL_ERROR "${Red}you need to install `iconv` first${ColorReset}")
    endif()
  endif()

  ## check `flex`
  find_package(FLEX)
  if(NOT FLEX_FOUND)
    message(FATAL_ERROR "${Red}you need to install `flex` first${ColorReset}")
  endif()
  if(CMAKE_C_COMPILER_ID STREQUAL "GNU" AND CMAKE_C_COMPILER_VERSION VERSION_LESS 5.0.0)
    message(FATAL_ERROR "${Red}gcc 4.8.0 will complain too much about flex-generated code, we just bypass building ODBC driver in such case${ColorReset}")
  endif()

  ## check `bison`
  find_package(BISON)
  if(NOT BISON_FOUND)
    message(FATAL_ERROR "${Red}you need to install `bison` first${ColorReset}")
  endif()
  if(CMAKE_C_COMPILER_ID STREQUAL "GNU" AND CMAKE_C_COMPILER_VERSION VERSION_LESS 5.0.0)
    message(FATAL_ERROR "${Red}gcc 4.8.0 will complain too much about bison-generated code, we just bypass building ODBC driver in such case${ColorReset}")
  endif()

  if(NOT TODBC_WINDOWS)
    ## check `odbcinst`
    find_program(TAOS_ODBC_ODBCINST_INSTALLED NAMES odbcinst)
    if(NOT TAOS_ODBC_ODBCINST_INSTALLED)
      if(TAOS_ODBC_DARWIN)
        message(FATAL_ERROR "${Red}unixodbc is not installed yet, you may install it under macOS by typing: brew install unixodbc${ColorReset}")
      else()
        message(FATAL_ERROR "${Red}odbcinst is not installed yet, you may install it under Ubuntu by typing: sudo apt install odbcinst${ColorReset}")
      endif()
    endif()

    ## check `isql`
    find_program(TAOS_ODBC_ISQL_INSTALLED NAMES isql)
    if(NOT TAOS_ODBC_ISQL_INSTALLED)
      if(TAOS_ODBC_DARWIN)
        message(FATAL_ERROR "${Red}unixodbc is not installed yet, you may install it under macOS by typing: brew install unixodbc${ColorReset}")
      else()
        message(FATAL_ERROR "${Red}unixodbc is not installed yet, you may install it under Ubuntu by typing: sudo apt install unixodbc${ColorReset}")
      endif()
    endif()

    ## check `pkg-config`
    find_program(TAOS_ODBC_PKG_CONFIG_INSTALLED NAMES pkg-config)
    if(NOT TAOS_ODBC_PKG_CONFIG_INSTALLED)
      if(TAOS_ODBC_DARWIN)
        message(FATAL_ERROR "${Red}pkg-config is not installed yet, you may install it under macOS by typing: brew install pkg-config${ColorReset}")
      else()
        message(FATAL_ERROR "${Red}pkg-config is not installed yet, you may install it under Ubuntu by typing: sudo apt install pkg-config${ColorReset}")
      endif()
    endif()

    ## get `odbc/odbcinst` info via `pkg-config`
    execute_process(COMMAND pkg-config --variable=includedir odbc ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE ODBC_INCLUDE_DIRECTORY)
    execute_process(COMMAND pkg-config --variable=libdir odbc ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE ODBC_LIBRARY_DIRECTORY)
    execute_process(COMMAND pkg-config --libs-only-L odbc ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE ODBC_LINK_OPTIONS)

    execute_process(COMMAND pkg-config --variable=includedir odbcinst ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE ODBCINST_INCLUDE_DIRECTORY)
    execute_process(COMMAND pkg-config --variable=libdir odbcinst ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE ODBCINST_LIBRARY_DIRECTORY)
    execute_process(COMMAND pkg-config --libs-only-L odbcinst ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE ODBCINST_LINK_OPTIONS)
  endif()

  if(NOT TODBC_WINDOWS)
    set(CMAKE_REQUIRED_LIBRARIES odbc odbcinst)
    set(CMAKE_REQUIRED_INCLUDES ${ODBC_INCLUDE_DIRECTORY} ${ODBCINST_INCLUDE_DIRECTORY})
    set(CMAKE_REQUIRED_LINK_OPTIONS ${ODBC_LINK_OPTIONS} ${ODBCINST_LINK_OPTIONS})
  else()
    set(CMAKE_REQUIRED_LIBRARIES odbc32 odbccp32 legacy_stdio_definitions)
  endif()

  ## check `sql.h`
  if(NOT TODBC_WINDOWS)
    check_symbol_exists(SQLExecute "sql.h" HAVE_ODBC_DEV)
  else()
    check_symbol_exists(SQLExecute "windows.h;sql.h" HAVE_ODBC_DEV)
  endif()
  if(NOT HAVE_ODBC_DEV)
    message(FATAL_ERROR "${Red}odbc requirement not satisfied, please install unixodbc-dev. Check detail in ${CMAKE_BINARY_DIR}/CMakeFiles/CMakeError.log${ColorReset}")
  endif()

  ## check `odbcinst.h`
  if(NOT TODBC_WINDOWS)
    check_symbol_exists(SQLConfigDataSource "odbcinst.h" HAVE_ODBCINST_DEV)
  else()
    check_symbol_exists(SQLConfigDataSource "windows.h;odbcinst.h" HAVE_ODBCINST_DEV)
  endif()
  if(NOT HAVE_ODBCINST_DEV)
    message(FATAL_ERROR "${Red}odbc requirement not satisfied, check detail in ${CMAKE_BINARY_DIR}/CMakeFiles/CMakeError.log${ColorReset}")
  endif()

  ## check `valgrind`
  find_program(VALGRIND NAMES valgrind)
  if(NOT VALGRIND)
    message(STATUS "${Yellow}`valgrind` tool not found, thus valgrind-related-test-cases would be eliminated, you may refer to https://valgrind.org/${ColorReset}")
  else()
    set(HAVE_VALGRIND ON)
  endif()

  ## check `node`
  find_program(NODEJS NAMES node)
  if(NOT NODEJS)
    message(STATUS "${Yellow}`node` not found, thus nodejs-related-test-cases would be eliminated, you may refer to https://nodejs.org/${ColorReset}")
  else()
    set(HAVE_NODEJS ON)
    execute_process(COMMAND node --version ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE NODEJS_VERSION)
    message(STATUS "${Green}`node` found -- ${NODEJS_VERSION}, please be noted, nodejs v12 and above are expected compatible${ColorReset}")
  endif()

  ## check `python3`
  set(PYTHON3_NAME python)
  find_program(PYTHON3 NO_CACHE NAMES python)
  if (NOT PYTHON3)
    set(PYTHON3_NAME python3)
    find_program(PYTHON3 NO_CACHE NAMES python3)
  endif ()
  if (NOT PYTHON3)
    message(STATUS "${Yellow}neither `python3` nor `python` is found, thus python3-related-test-cases would be eliminated, you may refer to https://www.python.org/${ColorReset}")
  else ()
    execute_process(COMMAND ${PYTHON3_NAME} --version ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE PYTHON3_VERSION)
    string(REPLACE "Python " "" PYTHON3_VERSION_ONLY ${PYTHON3_VERSION})
    if (${PYTHON3_VERSION_ONLY} VERSION_LESS "3.10")
      message(STATUS "${Yellow}`${PYTHON3_VERSION}` found -- please be noted, python v3.10 and above are expected compatible, thus python3-related-test-cases would be eliminated, you may refer to https://www.python.org/${ColorReset}")
    else ()
      set(HAVE_PYTHON3 ON)
      message(STATUS "${Green}`python{3}` found -- ${PYTHON3_VERSION}, please be noted, python3 v3.10 and above are expected compatible${ColorReset}")
    endif ()
  endif ()

  ## check `go`
  find_program(GO NAMES go)
  if(NOT GO)
    message(STATUS "${Yellow}`go` not found, thus go-related-test-cases would be eliminated, you may refer to https://go.dev/${ColorReset}")
  else()
    set(HAVE_GO ON)
    execute_process(COMMAND go version ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE GO_VERSION)
    message(STATUS "${Green}`go` found -- ${GO_VERSION}, please be noted, go v1.20 and above are expected compatible${ColorReset}")
  endif()

  if(TRUE OR NOT TODBC_WINDOWS)
    ## NOTE: check http-proxy sort of issues
    ## check `rustc`
    find_program(RUSTC NAMES rustc)
    if(NOT RUSTC)
      message(STATUS "${Yellow}`rustc` not found, thus rustc-related-test-cases would be eliminated, you may refer to https://rust-lang.org/${ColorReset}")
    else()
      set(HAVE_RUSTC ON)
      execute_process(COMMAND rustc --version ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE RUSTC_VERSION)
      message(STATUS "${Green}`rustc` found -- ${RUSTC_VERSION}, please be noted, rustc v1.63 and above are expected compatible${ColorReset}")
    endif()

    ## check `cargo`
    find_program(CARGO NAMES cargo)
    if(NOT CARGO)
      message(STATUS "${Yellow}`cargo` not found, thus cargo-related-test-cases would be eliminated, you may refer to https://rust-lang.org/${ColorReset}")
    else()
      set(HAVE_CARGO ON)
      execute_process(COMMAND cargo --version ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE CARGO_VERSION)
      message(STATUS "${Green}`cargo` found -- ${CARGO_VERSION}, please be noted, cargo v1.63 and above are expected compatible${ColorReset}")
    endif()
  endif()

  ## check `mysql`
  if (ENABLE_MYSQL_TEST)
    find_program(HAVE_MYSQL NAMES mysql)
    if(NOT HAVE_MYSQL)
      message(FATAL_ERROR "${Yellow}`mysql-related-test-cases` is requested, but `mysql` is not found, you may refer to https://www.mysql.com/${ColorReset}")
    else()
      execute_process(COMMAND mysql --version ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE MYSQL_VERSION)
      message(STATUS "${Green}`mysql` found -- ${MYSQL_VERSION}, please be noted, mysql v8.0 and above are expected compatible${ColorReset}")
    endif()
  endif()

  ## check `sqlite3`
  if(ENABLE_SQLITE3_TEST)
    find_program(HAVE_SQLITE3 NAMES sqlite3)
    if(NOT HAVE_SQLITE3)
      message(FATAL_ERROR "${Yellow}`sqlite3-related-test-cases` is requested, but `sqlite3` is not found, you may refer to https://www.sqlite.org/${ColorReset}")
    else()
      execute_process(COMMAND sqlite3 --version ERROR_QUIET OUTPUT_STRIP_TRAILING_WHITESPACE OUTPUT_VARIABLE SQLITE3_VERSION)
      message(STATUS "${Green}`sqlite3` found -- ${SQLITE3_VERSION}, please be noted, sqlite3 v3.31 and above are expected compatible${ColorReset}")
    endif()
  endif()

endmacro()

macro(parser_gen _name)
    set(_parser          ${_name}_parser)
    set(_scanner         ${_name}_scanner)
    set(_src_path        ${CMAKE_CURRENT_SOURCE_DIR})
    set(_dst_path        ${CMAKE_CURRENT_BINARY_DIR})
    set(_src_name        ${_src_path}/${_name})
    set(_dst_name        ${_dst_path}/${_name})
    set(_y               ${_src_name}.y)
    set(_l               ${_src_name}.l)
    set(_tab_c           ${_src_name}_tab.c)
    set(_dst_tab_c       ${_dst_name}.tab.c)
    set(_dst_lex_c       ${_dst_name}.lex.c)
    set(_dst_lex_h       ${_dst_name}.lex.h)

    BISON_TARGET(${_parser} ${_y} ${_dst_tab_c}
        COMPILE_FLAGS "--warnings=error -Dapi.prefix={${_name}_yy}")
    FLEX_TARGET(${_scanner} ${_l} ${_dst_lex_c}
        COMPILE_FLAGS "--header-file=${_dst_lex_h} --prefix=${_name}_yy")
    ADD_FLEX_BISON_DEPENDENCY(${_scanner} ${_parser})

    set(_bison_outputs ${BISON_${_parser}_OUTPUTS})
    set(_flex_outputs ${FLEX_${_scanner}_OUTPUTS})
    set(_outputs ${_bison_outputs} ${_flex_outputs})

    set_source_files_properties(${_tab_c}
            PROPERTY COMPILE_FLAGS "-I${_dst_path}")
    set_source_files_properties(${_tab_c}
            PROPERTY OBJECT_DEPENDS "${_outputs}")

    unset(_bison_outputs)
    unset(_flex_outputs)
    unset(_outputs)

    unset(_parser)
    unset(_scanner)
    unset(_src_path)
    unset(_dst_path)
    unset(_src_name)
    unset(_dst_name)
    unset(_y)
    unset(_l)
    unset(_tab_c)
    unset(_dst_tab_c)
    unset(_dst_lex_c)
    unset(_dst_lex_h)
endmacro()

