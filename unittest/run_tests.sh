#!/bin/bash
#

TOPDIR=`readlink -f \`dirname $0\``
CTEST_COMMAND=${TOPDIR}/../../deps/3rd/usr/local/oceanbase/devtools/bin/ctest

${CTEST_COMMAND} "$@"
