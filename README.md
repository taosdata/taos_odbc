# ODBC Driver for TDengine 3.0 (TAOS) #
English | [简体中文](README.cn.md)

### Supported platform
- Linux
- macOS
- Windows

### Features
- **on-going implementation of ODBC driver for TDengine 3.0 (TAOS)**
- **currently exported ODBC functions are**:
```
ConfigDSN              (windows port only)
ConfigDriver           (windows port only)
ConfigTranslator       (windows port only)
SQLAllocHandle
SQLBindCol
SQLBindParameter
SQLBrowseConnect
SQLBulkOperations
SQLColAttribute
SQLColumnPrivileges
SQLColumns             (not fully implemented yet)
SQLCompleteAsync
SQLConnect
SQLCopyDesc
SQLDescribeCol
SQLDescribeParam
SQLDisconnect
SQLDriverConnect
SQLEndTran
SQLExecDirect
SQLExecute
SQLExtendedFetch
SQLFetch
SQLFetchScroll
SQLForeignKeys
SQLFreeHandle
SQLFreeStmt
SQLGetConnectAttr
SQLGetCursorName
SQLGetData
SQLGetDescField
SQLGetDescRec
SQLGetDiagField
SQLGetDiagRec
SQLGetEnvAttr
SQLGetInfo
SQLGetStmtAttr
SQLGetTypeInfo
SQLMoreResults
SQLNativeSql
SQLNumParams
SQLNumResultCols
SQLParamData
SQLPrepare
SQLPrimaryKeys
SQLProcedureColumns
SQLProcedures
SQLPutData
SQLRowCount
SQLSetConnectAttr
SQLSetCursorName
SQLSetDescField
SQLSetDescRec
SQLSetEnvAttr
SQLSetPos
SQLSetStmtAttr
SQLSpecialColumns
SQLStatistics
SQLTablePrivileges
SQLTables (post-filter workaround, to be removed when taosc is right in place)
```
- **enable ODBC-aware software to communicate with TDengine**
- **enable any programming language with ODBC-bindings/ODBC-plugings to communicate with TDengine, potentially**
- **still going on**...

### Requirements
- cmake, 3.16.3 or above
- flex, 2.6.4 or above. NOTE: win_flex_bison on windows platform to be installed.
- bison, 3.5.1 or above. NOTE: win_flex_bison on windows platform to be installed.
- odbc driver manager, such as unixodbc(2.3.6 or above) in linux. NOTE: odbc driver manager is pre-installed on windows platform
- iconv, should've been already included in libc. NOTE: win_iconv would be downloaded when building this project
- valgrind, if you wish to debug and profile executables, such as detecting potential memory leakages
- node, if you wish to enable nodejs-test-cases
  - node odbc, 2.4.4 or above, https://www.npmjs.com/package/odbc
- rust, if you wish to enable rust-test-cases
  - odbc, 0.17.0 or above, https://docs.rs/odbc/latest/odbc/
  - env_logger, 0.8.2 or above, https://docs.rs/env_logger/latest/env_logger/
  - json

### Installing TDengine 3.0
- please visit https://tdengine.com
- better use TDengine-git-commit "ea249127afb42ac3a31d8d9f63243c3d1b950b5d" or above, otherwise, you might come across memory leakage in windows platform, which is introduced by `taos_stmt_get_tag_fields/taos_stmt_get_col_fields`. please check detail: https://github.com/taosdata/TDengine/issues/18804 and https://github.com/taosdata/TDengine/pull/19245

### Installing prerequisites, use Ubuntu 20.04 as an example
```
sudo apt install flex bison unixodbc unixodbc-dev && echo -=Done=-
```

### Building and Installing, use Ubuntu 20.04 as an example
```
rm -rf debug && cmake -B debug -DCMAKE_BUILD_TYPE=Debug && cmake --build debug && sudo cmake --install debug && echo -=Done=-
```

### Test
```
pushd debug >/dev/null && TAOS_TEST_CASES=$(pwd)/../tests/taos/taos_test.cases ODBC_TEST_CASES=$(pwd)/../tests/c/odbc_test.cases ctest --output-on-failure && echo -=Done=-; popd >/dev/null
```

### Test with environment variable `TAOS_ODBC_LOG_LEVEL` and `TAOS_ODBC_LOGGER`
- TAOS_ODBC_LOG_LEVEL: VERBOSE/DEBUG/INFO/WARN/ERROR/FATAL, from low to high. the lower the level is, the more info to log
- TAOS_ODBC_LOGGER: stderr/temp/syslog
    - stderr: log to `stderr`
    - temp: log to `env('TEMP')/taos_odbc.log` or `/tmp/taos_odbc.log` if env('TEMP') not exists
    - syslog: log to `syslog`

in case when some test cases fail and you wish to have more debug info, such as when and how taos_xxx API is called under the hood, you can
```
pushd debug >/dev/null && TAOS_TEST_CASES=$(pwd)/../tests/taos/taos_test.cases ODBC_TEST_CASES=$(pwd)/../tests/c/odbc_test.cases TAOS_ODBC_LOG_LEVEL=ERROR TAOS_ODBC_LOGGER=stderr ctest --output-on-failure && echo -=Done=-; popd >/dev/null
```

### To make your daily life better
```
export TAOS_TEST_CASES=$(pwd)/tests/taos/taos_test.cases
export ODBC_TEST_CASES=$(pwd)/tests/c/odbc_test.cases
export TAOS_ODBC_LOG_LEVEL=ERROR
export TAOS_ODBC_LOGGER=stderr
```
and then, you can
```
pushd debug >/dev/null && ctest --output-on-failure && echo -=Done=-; popd >/dev/null
```

### Installing prerequisites, use MacOS Big Sur as an example
```
brew install flex bison unixodbc && echo -=Done=-
```

### Building and Installing, use MacOS Big Sur as an example
```
rm -rf debug && cmake -B debug -DCMAKE_BUILD_TYPE=Debug && cmake --build debug && sudo cmake --install debug && echo -=Done=-
```

### Test
```
pushd debug >/dev/null && TAOS_TEST_CASES=$(pwd)/../tests/taos/taos_test.cases ODBC_TEST_CASES=$(pwd)/../tests/c/odbc_test.cases ctest --output-on-failure && echo -=Done=-; popd >/dev/null
```

### Test with environment variable `TAOS_ODBC_LOG_LEVEL` and `TAOS_ODBC_LOGGER`
- TAOS_ODBC_LOG_LEVEL: VERBOSE/DEBUG/INFO/WARN/ERROR/FATAL, from low to high. the lower the level is, the more info to log
- TAOS_ODBC_LOGGER: stderr/temp
    - stderr: log to `stderr`
    - temp: log to `env('TEMP')/taos_odbc.log` or `/tmp/taos_odbc.log` if env('TEMP') not exists

in case when some test cases fail and you wish to have more debug info, such as when and how taos_xxx API is called under the hood, you can
```
pushd debug >/dev/null && TAOS_TEST_CASES=$(pwd)/../tests/taos/taos_test.cases ODBC_TEST_CASES=$(pwd)/../tests/c/odbc_test.cases TAOS_ODBC_LOG_LEVEL=ERROR TAOS_ODBC_LOGGER=stderr ctest --output-on-failure && echo -=Done=-; popd >/dev/null
```

### To make your daily life better
```
export TAOS_TEST_CASES=$(pwd)/tests/taos/taos_test.cases
export ODBC_TEST_CASES=$(pwd)/tests/c/odbc_test.cases
export TAOS_ODBC_LOG_LEVEL=ERROR
export TAOS_ODBC_LOGGER=stderr
```
and then, you can
```
pushd debug >/dev/null && ctest --output-on-failure && echo -=Done=-; popd >/dev/null
```

### Installing prerequisites, use Windows 11 as an example
1. download and install win_flex_bison 2.5.25.
```
https://github.com/lexxmark/winflexbison/releases/download/v2.5.25/win_flex_bison-2.5.25.zip
```
2. check to see if it's installed: 
```
win_flex --version
```

### Building and Installing, use Windows 11 as an example
3. Open Command Prompt as an Administrator. https://www.makeuseof.com/windows-run-command-prompt-admin/
4. change to the root directory of this project
5. setup building environment, if you install visual studio community 2022 on a 64-windows-platform, then:
```
"\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvarsall.bat" x64
```
6. generate make files
```
cmake --no-warn-unused-cli -DCMAKE_EXPORT_COMPILE_COMMANDS:BOOL=TRUE -B build -G "Visual Studio 17 2022" -A x64
```
7. building project
```
cmake --build build --config Debug -j 4
```
8. installing taos_odbc, this would install taos_odbc.dll into C:\Program Files\taos_odbc\bin\
```
cmake --install build --config Debug
cmake --build build --config Debug --target install_templates
```
9. check and see if a new TAOS_ODBC_DSN registry has been setup in win_registry
```
HKEY_LOCAL_MACHINE\SOFTWARE\ODBC\ODBCINST.INI\TAOS_ODBC_DRIVER
HKEY_CURRENT_USER\Software\ODBC\Odbc.ini\TAOS_ODBC_DSN
```

### Test
10. setup testing environment variable `TAOS_ODBC_LOG_LEVEL` and `TAOS_ODBC_LOGGER`
- TAOS_ODBC_LOG_LEVEL: VERBOSE/DEBUG/INFO/WARN/ERROR/FATAL, from low to high. the lower the level is, the more info to log
- TAOS_ODBC_LOGGER: stderr/temp
    - stderr: log to `stderr`
    - temp: log to `env('TEMP')\taos_odbc.log` or `C:\Windows\Temp\taos_odbc.log` if env('TEMP') not exists
```
set TAOS_TEST_CASES=%cd%\tests\taos\taos_test.cases
set ODBC_TEST_CASES=%cd%\tests\c\odbc_test.cases
set TAOS_ODBC_LOG_LEVEL=ERROR
set TAOS_ODBC_LOGGER=stderr
```
11. testing
```
ctest --test-dir build --output-on-failure -C Debug
```

### Tips
- `cmake --help` or `man cmake`
- `ctest --help` or `man ctest`
- `valgrind --help` or `man valgrind`

### Layout of source code, directories only
```
<root>
├── cmake
├── common
├── inc
├── sh
├── src
│   ├── core
│   ├── inc
│   ├── os_port
│   ├── parser
│   ├── tests
│   └── utils
├── templates
├── tests
│   ├── c
│   ├── cpp
│   ├── node
│   ├── rust
│   │   └── main
│   │       └── src
│   └── taos
└── valgrind
```

## TDengine references
- https://tdengine.com
- https://github.com/taosdata/TDengine

## ODBC references
- https://learn.microsoft.com/en-us/sql/odbc/reference/introduction-to-odbc?view=sql-server-ver16
- https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/odbc-api-reference?view=sql-server-ver16

