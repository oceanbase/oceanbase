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
import atexit

import os
import time
from enum import Enum

from tool import FileUtil
from manager import Manager
from error import LockError


class LockType(Enum):
    GLOBAL = "global"
    CLUSTER = "cluster"
    SCHEDULER = "scheduler"
    RESULT = "result"
    REPOSITORY = "repository"


class MixLock(object):
    def __init__(self, path, stdio=None):
        self.path = path
        self.stdio = stdio
        self._lock_obj = None
        self._sh_cnt = 0
        self._ex_cnt = 0

    def __del__(self):
        self._unlock()

    @property
    def lock_obj(self):
        if self._lock_obj is None or self._lock_obj.closed:
            self._lock_obj = FileUtil.open(self.path, _type="w")
        return self._lock_obj

    @property
    def locked(self):
        return self._sh_cnt or self._ex_cnt

    def _ex_lock(self):
        if self.lock_obj:
            try:
                FileUtil.exclusive_lock_obj(self.lock_obj, stdio=self.stdio)
            except Exception as e:
                raise LockError(e)

    def _sh_lock(self):
        if self.lock_obj:
            try:
                FileUtil.share_lock_obj(self.lock_obj, stdio=self.stdio)
            except Exception as e:
                raise LockError(e)

    def sh_lock(self):
        if not self.locked:
            self._sh_lock()
        self._sh_cnt += 1
        self.stdio and getattr(self.stdio, "verbose", print)(
            "share lock `%s`, count %s" % (self.path, self._sh_cnt)
        )
        return True

    def ex_lock(self):
        if self._ex_cnt == 0:
            try:
                self._ex_lock()
            except Exception as e:
                if self._sh_cnt:
                    self.lock_escalation(LockManager.TRY_TIMES)
                else:
                    raise LockError(e)
        self._ex_cnt += 1
        self.stdio and getattr(self.stdio, "verbose", print)(
            "exclusive lock `%s`, count %s" % (self.path, self._ex_cnt)
        )
        return True

    def lock_escalation(self, try_times):
        self.stdio and getattr(self.stdio, "start_loading", print)(
            "waiting for the lock"
        )
        try:
            self._lock_escalation(try_times)
            self.stdio and getattr(self.stdio, "stop_loading", print)("succeed")
        except Exception as e:
            self.stdio and getattr(self.stdio, "stop_loading", print)("fail")
            raise LockError(e)

    def _lock_escalation(self, try_times):
        stdio = self.stdio
        while try_times:
            try:
                if try_times % 1000:
                    self.stdio = None
                else:
                    self.stdio = stdio
                try_times -= 1
                self._ex_lock()
                break
            except KeyboardInterrupt:
                self.stdio = stdio
                raise Exception("fail to get lock")
            except Exception as e:
                if try_times:
                    time.sleep(LockManager.TRY_INTERVAL)
                else:
                    self.stdio = stdio
                    raise LockError(e)
        self.stdio = stdio

    def _sh_unlock(self):
        if self._sh_cnt == 0:
            if self._ex_cnt == 0:
                self._unlock()

    def _ex_unlock(self):
        if self._ex_cnt == 0:
            if self._sh_cnt > 0:
                self._sh_lock()
            else:
                self._unlock()

    def sh_unlock(self):
        if self._sh_cnt > 0:
            self._sh_cnt -= 1
            self.stdio and getattr(self.stdio, "verbose", print)(
                "share lock %s release, count %s" % (self.path, self._sh_cnt)
            )
            self._sh_unlock()
        return self.locked is False

    def ex_unlock(self):
        if self._ex_cnt > 0:
            self._ex_cnt -= 1
            self.stdio and getattr(self.stdio, "verbose", print)(
                "exclusive lock %s release, count %s" % (self.path, self._ex_cnt)
            )
            self._ex_unlock()
        return self.locked is False

    def _unlock(self):
        if self._lock_obj:
            FileUtil.unlock(self._lock_obj, stdio=self.stdio)
            self._lock_obj.close()
            self._lock_obj = None
            self._sh_cnt = 0
            self._ex_cnt = 0


class Lock(object):
    def __init__(self, mix_lock):
        self.mix_lock = mix_lock

    def lock(self):
        raise NotImplementedError

    def unlock(self):
        raise NotImplementedError


class SHLock(Lock):
    def lock(self):
        self.mix_lock.sh_lock()

    def unlock(self):
        self.mix_lock.sh_unlock()


class EXLock(Lock):
    def lock(self):
        self.mix_lock.ex_lock()

    def unlock(self):
        self.mix_lock.ex_unlock()


class LockManager(Manager):
    TRY_TIMES = 6000
    TRY_INTERVAL = 0.01

    RELATIVE_PATH = "lock/"
    GLOBAL_FN = LockType.GLOBAL.value
    CLUSTER_FN = LockType.CLUSTER.value
    SCHEDULER_FN = LockType.SCHEDULER.value
    RESULT_FN_PREFIX = LockType.RESULT.value
    REPOSITORY_FN = LockType.REPOSITORY.value
    LOCKS = {}

    def __init__(self, home_path, stdio=None):
        super(LockManager, self).__init__(home_path, stdio)
        self.locks = []
        self.global_path = os.path.join(self.path, self.GLOBAL_FN)
        self.cluster_path = os.path.join(self.path, self.CLUSTER_FN)
        self.repository_path = os.path.join(self.path, self.REPOSITORY_FN)
        self.scheduler_path = os.path.join(self.path, self.SCHEDULER_FN)

    @staticmethod
    def set_try_times(try_times):
        LockManager.TRY_TIMES = try_times

    @staticmethod
    def set_try_interval(try_interval):
        LockManager.TRY_INTERVAL = try_interval

    def __del__(self):
        for lock in self.locks[::-1]:
            lock.unlock()
        self.locks = None

    def _get_mix_lock(self, path):
        if path not in self.LOCKS:
            self.LOCKS[path] = MixLock(path, stdio=self.stdio)
        return self.LOCKS[path]

    @classmethod
    def shutdown(cls):
        for path in cls.LOCKS:
            cls.LOCKS[path] = None
        cls.LOCKS = None

    def _lock(self, path, clz):
        mix_lock = self._get_mix_lock(path)
        lock = clz(mix_lock)
        lock.lock()
        self.locks.append(lock)
        return True

    def _sh_lock(self, path):
        return self._lock(path, SHLock)

    def _ex_lock(self, path):
        return self._lock(path, EXLock)

    def global_ex_lock(self):
        return self._ex_lock(self.global_path)

    def global_sh_lock(self):
        return self._sh_lock(self.global_path)

    def cluster_ex_lock(self):
        return self._ex_lock(self.cluster_path)

    def cluster_sh_lock(self):
        return self._sh_lock(self.cluster_path)

    def repository_ex_lock(self):
        return self._ex_lock(self.repository_path)

    def repository_sh_lock(self):
        return self._sh_lock(self.repository_path)

    def scheduler_ex_lock(self):
        return self._ex_lock(self.scheduler_path)

    def scheduler_sh_lock(self):
        return self._sh_lock(self.scheduler_path)

    def _result_lock_fp(self, trace_id):
        return os.path.join(self.path, "{}_{}".format(self.RESULT_FN_PREFIX, trace_id))

    def result_ex_lock(self, trace_id):
        return self._ex_lock(self._result_lock_fp(trace_id))

    def result_sh_lock(self, trace_id):
        return self._sh_lock(self._result_lock_fp(trace_id))


atexit.register(LockManager.shutdown)
