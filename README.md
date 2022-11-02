# ODBC Driver for TDengine TAOS 3.0 #
English | [简体中文](README.cn.md)
- **on-going implementation of ODBC driver for TAOS 3.0**
- **currently exported ODBC functions are**:
```
SQLAllocHandle
SQLBindCol
SQLBindParameter
SQLColAttribute
SQLConnect
SQLDescribeCol
SQLDescribeParam
SQLDisconnect
SQLDriverConnect
SQLEndTran
SQLExecDirect
SQLExecute
SQLFetch
SQLFetchScroll
SQLFreeHandle
SQLFreeStmt
SQLGetData
SQLGetDiagField
SQLGetDiagRec
SQLGetInfo
SQLNumParams
SQLNumResultCols
SQLPrepare
SQLRowCount
SQLSetConnectAttr
SQLSetEnvAttr
SQLSetStmtAttr
```
- **enable ODBC-aware software to communicate with TAOS, at this very beginning, we support linux only**
- **enable any programming language with ODBC-bindings/ODBC-plugings to communicate with TAOS, potentially**
- **still going on**...

### Supported platform
- Linux

### Requirements
- flex, 2.6.4 or above
- bison, 3.5.1 or above
- odbc driver manager, such as unixodbc(2.3.6 or above) in linux
- iconv, should've been already included in libc
- valgrind, if you wish to debug and profile executables, such as detecting potential memory leakages
- node, if you wish to enable nodejs-test-cases
  - node odbc, 2.4.4 or above, https://www.npmjs.com/package/odbc
- rust, if you wish to enable rust-test-cases
  - odbc, 0.17.0 or above, https://docs.rs/odbc/latest/odbc/
  - env_logger, 0.8.2 or above, https://docs.rs/env_logger/latest/env_logger/
  - json

### Installing TDengine TAOS 3.0
- please visit https://tdengine.com

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

### Test with TAOS_ODBC_DEBUG
in case when some test cases fail and you wish to have more debug info, such as when and how taos_xxx API is called under the hood, you can
```
pushd debug >/dev/null && TAOS_TEST_CASES=$(pwd)/../tests/taos/taos_test.cases ODBC_TEST_CASES=$(pwd)/../tests/c/odbc_test.cases TAOS_ODBC_DEBUG= ctest --output-on-failure && echo -=Done=-; popd >/dev/null
```

### To make your daily life better
```
export TAOS_TEST_CASES=$(pwd)/tests/taos/taos_test.cases
export ODBC_TEST_CASES=$(pwd)/tests/c/odbc_test.cases
export TAOS_ODBC_DEBUG=
```
and then, you can
```
pushd debug >/dev/null && ctest --output-on-failure && echo -=Done=-; popd >/dev/null
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
├── examples
├── inc
├── samples
├── sh
├── src
│   ├── core
│   ├── inc
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
├── tools
└── valgrind
```

## TDengine references
- https://tdengine.com
- https://github.com/taosdata/TDengine

## ODBC references
- https://learn.microsoft.com/en-us/sql/odbc/reference/introduction-to-odbc?view=sql-server-ver16
- https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/odbc-api-reference?view=sql-server-ver16

