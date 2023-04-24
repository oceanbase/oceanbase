#!/usr/bin/env bash

WORK_DIR=$(readlink -f "$(dirname ${BASH_SOURCE[0]})")

ALIAS_TESTBENCH_EXIST=$(cat ~/.bashrc | grep "alias testbench=" | head -n 1)

if [[ "${ALIAS_TESTBENCH_EXIST}" == "" ]]; then
    echo "alias testbench='python ${WORK_DIR}/cmd.py'" >>~/.bashrc
fi
