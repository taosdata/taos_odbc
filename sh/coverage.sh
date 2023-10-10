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
  lcov -d . --capture --rc lcov_branch_coverage=1 --rc genhtml_branch_coverage=1 --no-external -b $ROOT_DIR -o coverage.info

  echo "generate result"
  lcov -l --rc lcov_branch_coverage=1 coverage.info | tee -a $REPORT_FILE
}

function main {
  date >> $REPORT_FILE
  echo "Run Coverage Test"

  # buildTaosODBC

  lcovFunc

  date >> $REPORT_FILE
  echo "End of Coverage Test"
}

main
