#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
CURRENT_DATE=$(date +"%Y-%m-%d_%H:%M:%S")
REPORT_FILE="$SCRIPT_DIR/coverage_$CURRENT_DATE.log"

function buildTaosODBC {
  echo "Start build taos_odbc..."
  cd $ROOT_DIR
  rm -rf debug
  cmake -B debug  -DCOVER=true -DCMAKE_BUILD_TYPE=Debug
  cmake --build debug
  sudo cmake --install debug
  cmake --build debug --target install_templates
}

function lcovFunc {
  echo "collect data by lcov"
  cd $ROOT_DIR
  cd debug

  # collect data
  lcov -d . --capture --rc lcov_branch_coverage=1 --rc genhtml_branch_coverage=1 --no-external -b $ROOT_DIR -o coverage.info --exclude "$ROOT_DIR/debug/*" --exclude "*/tests/*"

  echo "generate result"
  lcov -l --rc lcov_branch_coverage=1 coverage.info | tee -a $REPORT_FILE
}

function runTest {
  echo "start run ctest..."
  cd $ROOT_DIR
  cd debug
  export TAOS_TEST_CASES=$(pwd)/tests/taos/taos_test.cases
  export ODBC_TEST_CASES=$(pwd)/tests/c/odbc_test.cases
  export TAOS_ODBC_LOG_LEVEL=ERROR
  export TAOS_ODBC_LOGGER=stderr
  ctest --output-on-failure
}

function main {
  date >> $REPORT_FILE
  echo "Run Coverage Test"

  buildTaosODBC
  runTest
  lcovFunc

  date >> $REPORT_FILE
  echo "End of Coverage Test"
}

main
