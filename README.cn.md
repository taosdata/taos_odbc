# TDengine 3.0 TAOS ODBC 驱动 #
[English](README.md) | 简体中文

### (目前)支持的平台
- Linux
- macOS
- Windows

### 特性
- **开发中的TDengine 3.0 TAOS ODBC驱动**
- **目前驱动中已经导出的SQLxxx函数如下**:
```
ConfigDSN              (windows 平台仅有)
ConfigDriver           (windows 平台仅有)
ConfigTranslator       (windows 平台仅有)
SQLAllocHandle
SQLBindCol
SQLBindParameter
SQLBulkOperations
SQLColAttribute
SQLColumnPrivileges
SQLColumns
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
SQLTables (暂时使用post-filter来补救一下，等待taosc有新的实现后再移除)
```
- **ODBC应用程序将可以利用该驱动实现对TDengine时序数据库的操作**
- **相信，任何具有ODBC-绑定/插件的编程语言、框架，利用该驱动，也可实现对TDengine时序数据库的操作**
- **持续开发中**...

### 所需的依赖
- cmake, 3.16.3 或以上
- flex, 2.6.4 或以上. 注: windows平台上需要安装win_flex_bison, 参见后续说明.
- bison, 3.5.1 或以上. 注: windows平台上需要安装win_flex_bison, 参见后续说明.
- odbc 驱动管理器, 例如Linux平台上的unixodbc(2.3.6 或以上). 注: windows平台上odbc驱动管理器已经预装.
- iconv, 应该不需要单独安装了，基本上libc都已经内建了. 注: 在Windows平台编译的过程中, 会自动下载并编译安装win_iconv.
- valgrind, 如果您想对程序进行性能分析及内存泄漏探查的话
- node, 如果您想同时跑nodejs测试程序的话
  - node odbc, 2.4.4 或以上, https://www.npmjs.com/package/odbc
- rust, 如果您想同时跑rust测试程序的话
  - odbc, 0.17.0 或以上, https://docs.rs/odbc/latest/odbc/
  - env_logger, 0.8.2 或以上, https://docs.rs/env_logger/latest/env_logger/
  - json

### 安装TDengine 3.0
- 请参考TDengine官方说明，https://tdengine.com
- 最好使用TDengine-git-commit "ea249127afb42ac3a31d8d9f63243c3d1b950b5d"或以上, 否则，在windows平台上，您可能会遇到由`taos_stmt_get_tag_fields/taos_stmt_get_col_fields`所引致的内存泄漏的问题.具体细节，您可以参见: https://github.com/taosdata/TDengine/issues/18804 and https://github.com/taosdata/TDengine/pull/19245

### 安装必需的依赖项，以Ubuntu 20.04为例
```
sudo apt install flex bison unixodbc unixodbc-dev && echo -=Done=-
```

### 编译及安装, 以Ubuntu 20.04为例
```
rm -rf debug && cmake -B debug -DCMAKE_BUILD_TYPE=Debug && cmake --build debug && sudo cmake --install debug && echo -=Done=-
```

### 测试
```
pushd debug >/dev/null && TAOS_TEST_CASES=$(pwd)/../tests/taos/taos_test.cases ODBC_TEST_CASES=$(pwd)/../tests/c/odbc_test.cases ctest --output-on-failure && echo -=Done=-; popd >/dev/null
```

### 带上TAOS_ODBC_DEBUG环境变量进行测试
当测试程序出现失败的时候，你可能期望看到更多的调试信息，那么你可以这样
```
pushd debug >/dev/null && TAOS_TEST_CASES=$(pwd)/../tests/taos/taos_test.cases ODBC_TEST_CASES=$(pwd)/../tests/c/odbc_test.cases TAOS_ODBC_DEBUG= ctest --output-on-failure && echo -=Done=-; popd >/dev/null
```

### 让每天的生活简单一点
```
export TAOS_TEST_CASES=$(pwd)/tests/taos/taos_test.cases
export ODBC_TEST_CASES=$(pwd)/tests/c/odbc_test.cases
export TAOS_ODBC_DEBUG=
```
或者，你也可以这样
```
pushd debug >/dev/null && ctest --output-on-failure && echo -=Done=-; popd >/dev/null
```

### 安装必需的依赖项，以MacOS Big Sur为例
```
brew install flex bison unixodbc && echo -=Done=-
```

### 编译及安装, 以MacOS Big Sur为例
```
rm -rf debug && cmake -B debug -DCMAKE_BUILD_TYPE=Debug && cmake --build debug && sudo cmake --install debug && echo -=Done=-
```

### 测试
```
pushd debug >/dev/null && TAOS_TEST_CASES=$(pwd)/../tests/taos/taos_test.cases ODBC_TEST_CASES=$(pwd)/../tests/c/odbc_test.cases ctest --output-on-failure && echo -=Done=-; popd >/dev/null
```

### 带上TAOS_ODBC_DEBUG环境变量进行测试
当测试程序出现失败的时候，你可能期望看到更多的调试信息，那么你可以这样
```
pushd debug >/dev/null && TAOS_TEST_CASES=$(pwd)/../tests/taos/taos_test.cases ODBC_TEST_CASES=$(pwd)/../tests/c/odbc_test.cases TAOS_ODBC_DEBUG= ctest --output-on-failure && echo -=Done=-; popd >/dev/null
```

### 让每天的生活简单一点
```
export TAOS_TEST_CASES=$(pwd)/tests/taos/taos_test.cases
export ODBC_TEST_CASES=$(pwd)/tests/c/odbc_test.cases
export TAOS_ODBC_DEBUG=
```
或者，你也可以这样
```
pushd debug >/dev/null && ctest --output-on-failure && echo -=Done=-; popd >/dev/null
```

### 安装必需的依赖项，以Windows 11为例
1. 下载并安装win_flex_bison 2.5.25.
```
https://github.com/lexxmark/winflexbison/releases/download/v2.5.25/win_flex_bison-2.5.25.zip
```
2. 检查是否成功安装
```
win_flex --version
```

### Building and Installing, use Windows 11 as an example
3. 以管理员身份运行命令提示符. https://www.makeuseof.com/windows-run-command-prompt-admin/
4. cd至本工程的根目录.
5. 设置编译环境, 如果你是在64位的windows 11上安装的visual studio community 2022, 那么:
```
"\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvarsall.bat" x64
```
6. 构建Makefiles
```
cmake --no-warn-unused-cli -DCMAKE_EXPORT_COMPILE_COMMANDS:BOOL=TRUE -B build -G "Visual Studio 17 2022" -A x64
```
7. 开始编译构建
```
cmake --build build --config Debug -j 4
```
8. 安装taos_odbc, 安装好后, C:\Program Files\taos_odbc\bin\目录下会安装有taos_odbc.dll
```
cmake --install build --config Debug
cmake --build build --config Debug --target install_templates
```
9. 检查windows注册表项，看下相关的TAOS_ODBC_DSN条目等是否存在
```
HKEY_LOCAL_MACHINE\SOFTWARE\ODBC\ODBCINST.INI\TAOS_ODBC_DRIVER
HKEY_CURRENT_USER\Software\ODBC\Odbc.ini\TAOS_ODBC_DSN
```

### 测试
10. 设置相关的测试环境
```
set TAOS_TEST_CASES=%cd%\tests\taos\taos_test.cases
set ODBC_TEST_CASES=%cd%\tests\c\odbc_test.cases
set TAOS_ODBC_DEBUG=1
```
11. 开始测试
```
ctest --test-dir build --output-on-failure -C Debug
```

### 小提示
- `cmake --help` or `man cmake`
- `ctest --help` or `man ctest`
- `valgrind --help` or `man valgrind`

### 源代码目录结构
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

## TDengine 引用及出处
- https://tdengine.com
- https://github.com/taosdata/TDengine

## ODBC 引用及出处
- https://learn.microsoft.com/en-us/sql/odbc/reference/introduction-to-odbc?view=sql-server-ver16
- https://learn.microsoft.com/en-us/sql/odbc/reference/syntax/odbc-api-reference?view=sql-server-ver16

