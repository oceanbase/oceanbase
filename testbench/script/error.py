# coding: utf-8
# OceanBase Deploy.
# Copyright (C) 2021 OceanBase
#
# This file is part of OceanBase Deploy.
#
# OceanBase Deploy is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# OceanBase Deploy is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with OceanBase Deploy.  If not, see <https://www.gnu.org/licenses/>.


from __future__ import absolute_import, division, print_function


class LockError(Exception):
    pass


class ErrorCode(object):

    def __init__(self, code, msg):
        self.code = code
        self.msg = msg
        self._str_ = ('TESTBENCH-%04d: ' % code) + msg

    def format(self, *args, **kwargs):
        return self._str_.format(*args, **kwargs)

    def __str__(self):
        return self._str_


class InitDirFailedErrorMessage(object):

    PATH_ONLY = ': {path}.'
    NOT_EMPTY = ': {path} is not empty.'
    CREATE_FAILED = ': create {path} failed.'
    PERMISSION_DENIED = ': {path} permission denied .'


EC_CONFIG_CONFLICT_PORT = ErrorCode(
    1000, 'Configuration conflict {server1}:{port} port is used for {server2}\'s {key}')
EC_CONFLICT_PORT = ErrorCode(1001, '{server}:{port} port is already used')
EC_FAIL_TO_INIT_PATH = ErrorCode(1002, 'Fail to init {server} {key}{msg}')
EC_CLEAN_PATH_FAILED = ErrorCode(1003, 'Fail to clean {server}:{path}')
EC_CONFIG_CONFLICT_DIR = ErrorCode(
    1004, 'Configuration conflict {server1}: {path} is used for {server2}\'s {key}')
EC_SOME_SERVER_STOPED = ErrorCode(
    1005, 'Some of the servers in the cluster have been stopped')
EC_FAIL_TO_CONNECT = ErrorCode(1006, 'Failed to connect to {component}')
EC_ULIMIT_CHECK = ErrorCode(
    1007, '({server}) {key} must not be less than {need} (Current value: {now})')

EC_OBSERVER_NOT_ENOUGH_MEMORY = ErrorCode(
    2000, '({ip}) not enough memory. (Free: {free}, Need: {need})')
EC_OBSERVER_NOT_ENOUGH_MEMORY_ALAILABLE = ErrorCode(
    2000, '({ip}) not enough memory. (Available: {available}, Need: {need})')
EC_OBSERVER_NOT_ENOUGH_MEMORY_CACHED = ErrorCode(
    2000, '({ip}) not enough memory. (Free: {free}, Buff/Cache: {cached}, Need: {need})')
EC_OBSERVER_CAN_NOT_MIGRATE_IN = ErrorCode(2001, 'server can not migrate in')
EC_OBSERVER_FAIL_TO_START = ErrorCode(
    2002, 'Failed to start {server} observer')
EC_OBSERVER_NOT_ENOUGH_DISK_4_CLOG = ErrorCode(
    2003, '({ip}) {path} not enough disk space for clog. Use redo_dir to set other disk for clog, or reduce the value of datafile_size')
EC_OBSERVER_INVALID_MODFILY_GLOBAL_KEY = ErrorCode(
    2004, 'Invalid: {key} is not a single server configuration item')

EC_MYSQLTEST_PARSE_CMD_FAILED = ErrorCode(3000, 'parse cmd failed: {path}')
EC_MYSQLTEST_FAILE_NOT_FOUND = ErrorCode(3001, '{file} not found in {path}')
EC_TPCC_LOAD_DATA_FAILED = ErrorCode(3002, 'Failed to load data.')
EC_TPCC_RUN_TEST_FAILED = ErrorCode(3003, 'Failed to run TPC-C benchmark.')

EC_OBAGENT_RELOAD_FAILED = ErrorCode(4000, 'Fail to reload {server}')
EC_OBAGENT_SEND_CONFIG_FAILED = ErrorCode(
    4001, 'Fail to send config file to {server}')

# WARN CODE
WC_ULIMIT_CHECK = ErrorCode(
    1007, '({server}) The recommended number of {key} is {need} (Current value: {now})')
