#!/bin/bash

CURRENT_DIR="$(cd $(dirname $0); pwd)"
CTEST_COMMAND=${CURRENT_DIR}/../../deps/3rd/usr/local/oceanbase/devtools/bin/ctest
export LD_LIBRARY_PATH=${CURRENT_DIR}/../../deps/3rd/usr/local/oceanbase/devtools/lib64:${CURRENT_DIR}/../../deps/3rd/usr/local/oceanbase/deps/devel/lib/mariadb:${CURRENT_DIR}/../../deps/3rd/usr/local/oceanbase/deps/devel/lib
export CTEST_OUTPUT_ON_FAILURE=ON
${CTEST_COMMAND} "$@"
