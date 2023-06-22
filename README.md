# ODBC Driver for TDengine 3.0 (TAOS) #
English | [简体中文](README.cn.md)

### Supported platform
- Linux
- macOS
- Windows

### Features
- **on-going implementation of ODBC driver for TDengine 3.0 (TAOS)**
- **currently exported ODBC functions are**:

| ODBC/Setup API | linux (ubuntu 22.04) | macosx (ventura 13.2.1) | windows 11 | note |
| :----- | :---- | :---- | :---- | :---- |
| ConfigDSN | ✗ | ✗ | ✓ | |
| ConfigDriver | ✗ | ✗ | ✓ | |
| ConfigTranslator | ✗ | ✗ | ✓ | |
| SQLAllocHandle | ✓ | ✓ | ✓ | |
| SQLBindCol  | ✓ | ✓ | ✓ | Column-Wise Binding only |
| SQLBindParameter | ✓ | ✓ | ✓ | Column-Wise Binding only |
| SQLBrowseConnect | ✗ | ✗ | ✗ | |
| SQLBulkOperations | ✗ | ✗ | ✗ | TDengine has no counterpart |
| SQLCloseCursor | ✓ | ✓ | ✓ | |
| SQLColAttribute | ✓ | ✓ | ✓ | partially and on-going |
| SQLColumnPrivileges | ✗ | ✗ | ✗ | TDengine seems have no strict counterpart |
| SQLColumns | ✓ | ✓ | ✓ | |
| SQLCompleteAsync | ✗ | ✗ | ✗ | |
| SQLConnect | ✓ | ✓ | ✓ | |
| SQLCopyDesc | ✗ | ✗ | ✗ | |
| SQLDescribeCol | ✓ | ✓ | ✓ | |
| SQLDescribeParam | ✓ | ✓ | ✓ | partially and on-going |
| SQLDisconnect | ✓ | ✓ | ✓ | |
| SQLDriverConnect | ✓ | ✓ | ✓ | |
| SQLEndTran | ✓ | ✓ | ✓ | TDengine is non-transactional, thus this is at most simulating |
| SQLExecDirect | ✓ | ✓ | ✓ | |
| SQLExecute | ✓ | ✓ | ✓ | |
| SQLExtendedFetch | ✗ | ✗ | ✗ | |
| SQLFetch | ✓ | ✓ | ✓ | |
| SQLFetchScroll | ✓ | ✓ | ✓ | TDengine has no counterpart, just implement SQL_FETCH_NEXT |
| SQLForeignKeys | ✗ | ✗ | ✗ | TDengine has no counterpart |
| SQLFreeHandle | ✓ | ✓ | ✓ | |
| SQLFreeStmt | ✓ | ✓ | ✓ | |
| SQLGetConnectAttr | ✓ | ✓ | ✓ | partially and on-going |
| SQLGetCursorName | ✗ | ✗ | ✗ | TDengine has no counterpart |
| SQLGetData | ✓ | ✓ | ✓ | |
| SQLGetDescField | ✗ | ✗ | ✗ | |
| SQLGetDescRec | ✗ | ✗ | ✗ | |
| SQLGetDiagField | ✓ | ✓ | ✓ | |
| SQLGetDiagRec | ✓ | ✓ | ✓ | |
| SQLGetEnvAttr | ✓ | ✓ | ✓ | partially and on-going |
| SQLGetInfo | ✓ | ✓ | ✓ | partially and on-going |
| SQLGetStmtAttr | ✓ | ✓ | ✓ | partially and on-going |
| SQLGetTypeInfo | ✓ | ✓ | ✓ | |
| SQLMoreResults | ✓ | ✓ | ✓ | |
| SQLNativeSql | ✗ | ✗ | ✗ | TDengine has no counterpart |
| SQLNumParams | ✓ | ✓ | ✓ | TDengine has partially support, thus workaround in some cases |
| SQLNumResultCols | ✓ | ✓ | ✓ | |
| SQLParamData | ✗ | ✗ | ✗ | |
| SQLPrepare | ✓ | ✓ | ✓ | TDengine has partially support, thus workaround in some cases |
| SQLPrimaryKeys | ✓ | ✓ | ✓ | |
| SQLProcedureColumns | ✗ | ✗ | ✗ | TDengine has no counterpart |
| SQLProcedures | ✗ | ✗ | ✗ | TDengine has no counterpart |
| SQLPutData | ✗ | ✗ | ✗ | |
| SQLRowCount | ✓ | ✓ | ✓ | |
| SQLSetConnectAttr | ✓ | ✓ | ✓ | partially and on-going |
| SQLSetCursorName | ✗ | ✗ | ✗ | TDengine has no counterpart |
| SQLSetDescField | ✗ | ✗ | ✗ | |
| SQLSetDescRec | ✗ | ✗ | ✗ | |
| SQLSetEnvAttr | ✓ | ✓ | ✓ | partially and on-going |
| SQLSetPos | ✗ | ✗ | ✗ | TDengine has no counterpart |
| SQLSetStmtAttr | ✓ | ✓ | ✓ | partially and on-going |
| SQLSpecialColumns | ✗ | ✗ | ✗ | TDengine has no counterpart |
| SQLStatistics | ✗ | ✗ | ✗ | TDengine has no counterpart |
| SQLTablePrivileges | ✗ | ✗ | ✗ | TDengine has no strict counterpart |
| SQLTables | ✓ | ✓ | ✓ | |

- **enable ODBC-aware software to communicate with TDengine**
- **enable any programming language with ODBC-bindings/ODBC-plugings to communicate with TDengine. programming languages listed as follows are demonstrated in test-cases:**

| programming language | ODBC-API or bindings/plugins |
| :----- | :---- |
| C/C++ | ODBC-API |
| CSharp | System.Data.Odbc |
| Erlang | odbc module |
| Go | github.com/alexbrainman/odbc, database/sql |
| Haskell | HDBC, HDBC-odbc |
| Common Lisp | plain-odbc |
| Nodejs | odbc |
| Python3 | pyodbc |
| Rust | odbc |


- **On Windows, "ODBC Data Sources (64bit)" pre-installed tool can be used to manage DSN**
- **Support TDengine data subscription feature，refer to samples/c/demo_topic.c**
- **still going on**...

### Requirements
- cmake, 3.16.3 or above
- flex, 2.6.4 or above. NOTE: win_flex_bison on windows platform to be installed.
- bison, 3.5.1 or above. NOTE: win_flex_bison on windows platform to be installed.
- odbc driver manager, such as unixodbc(2.3.6 or above) in linux. NOTE: odbc driver manager is pre-installed on windows platform
- iconv, should've been already included in libc. NOTE: win_iconv would be downloaded when building this project
- valgrind, if you wish to debug and profile executables, such as detecting potential memory leakages
- node, v12.0 or above if you wish to enable nodejs-test-cases
  - node odbc, 2.4.4 or above, https://www.npmjs.com/package/odbc
- rust, v1.63 or above if you wish to enable rust-test-cases
  - odbc, 0.17.0 or above, https://docs.rs/odbc/latest/odbc/
  - env_logger, 0.8.2 or above, https://docs.rs/env_logger/latest/env_logger/
  - json
- python3, v3.10 or above if you wish to enable python3-test-cases
  - pyodbc, 4.0.39 or above, https://www.python.org/
- go, v1.17 or above if you wish to enable go-test-cases
  - github.com/alexbrainman/odbc, https://go.dev/
- erlang, v12.2 or above if you wish to enable erlang-test-cases
  - https://www.erlang.org/doc/apps/odbc/getting_started.html, https://erlang.org/
- haskell, cabal v3.6 or above, ghc v9.2 or above,  if you wish to enable haskell-test-cases
  - https://www.haskell.org/ or https://www.haskell.org/ghcup/
- common lisp, sbcl v2.1.11 or above if you wish to enable common-lisp-test-cases
  - plain-odbc, https://plain-odbc.common-lisp.dev/, https://lisp-lang.org/ or https://lisp-lang.org/learn/getting-started/
- R, v4.3 or above, if you wish to enable R-test-cases
  - https://www.r-project.org/

### Installing TDengine 3.0
- please visit https://tdengine.com
- better use TDengine-git-commit "ea249127afb42ac3a31d8d9f63243c3d1b950b5d" or above, otherwise, you might come across memory leakage in windows platform, which is introduced by `taos_stmt_get_tag_fields/taos_stmt_get_col_fields`. please check detail: https://github.com/taosdata/TDengine/issues/18804 and https://github.com/taosdata/TDengine/pull/19245

### Installing prerequisites, use Ubuntu 20.04 as an example
```
sudo apt install flex bison unixodbc unixodbc-dev && echo -=Done=-
```

### Building and Installing, use Ubuntu 20.04 as an example
```
rm -rf debug &&
cmake -B debug -DCMAKE_BUILD_TYPE=Debug &&
cmake --build debug &&
sudo cmake --install debug &&
cmake --build debug --target install_templates &&
echo -=Done=-
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
rm -rf debug &&
cmake -B debug -DCMAKE_BUILD_TYPE=Debug &&
cmake --build debug &&
sudo cmake --install debug &&
cmake --build debug --target install_templates &&
echo -=Done=-
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
5. optionally, setup building environment, if you install visual studio community 2022 on a 64-windows-platform, then:
```
"\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvarsall.bat" x64
```
6. generate make files
```
cmake --no-warn-unused-cli -DCMAKE_EXPORT_COMPILE_COMMANDS:BOOL=TRUE -B build -G "Visual Studio 17 2022" -A x64
```
**TroubleShooting**: if compiler error occurs during the following steps, such as: <path_to_winbase.h>: warning C5105: macro expansion producing 'defined' has undefined behavior, you can retry as below:
```
cmake --no-warn-unused-cli -DCMAKE_EXPORT_COMPILE_COMMANDS:BOOL=TRUE -B build -G "Visual Studio 17 2022" -A x64 -DDISABLE_C5105:BOOL=ON
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
├── benchmark
├── cmake
├── common
├── inc
├── samples
│   └── c
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
│   ├── cs
│   ├── erl
│   ├── go
│   ├── hs
│   │   └── app
│   ├── lisp
│   ├── node
│   ├── python
│   ├── R
│   ├── rust
│   │   └── main
│   │       └── src
│   ├── taos
│   └── ws
└── valgrind
```

## TDengine references
- https://tdengine.com
- https://github.com/taosdata/TDengine
- https://github.com/taosdata/TDengine/blob/main/docs/en/07-develop/07-tmq.mdx#create-a-consumer

## ODBC references
- https://learn.microsoft.com/en-us/sql/odbc/reference/introduction-to-odbc?view=sql-server-ver16
- https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/odbc-api-reference?view=sql-server-ver16

