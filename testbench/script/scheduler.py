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

import os

from tool import FileUtil, YamlLoader, OrderedDict
from manager import Manager

yaml = YamlLoader()


class Workload(object):
    def __init__(self, type, conf):
        self._type = type
        self._conf = conf
        self._repo_path = None

    @property
    def type(self):
        return self._type

    @property
    def cmd(self):
        strs = []
        for key, value in self._conf.items():
            strs.append("{}={}".format(key, value))
        return ",".join(strs)


class SchedulerManager(Manager):
    RELATIVE_PATH = "scheduler/"
    CONFIG_YAML_NAME = "config.yaml"

    def __init__(self, home_path, trace_id, lock_manager=None, stdio=None):
        super(SchedulerManager, self).__init__(home_path, stdio)
        self._component = None
        self._component_config = None
        self._workload_config = None
        self._repo_path = None
        self._lock_manager = lock_manager
        self._trace_id = trace_id
        self.stdio = stdio
        if self.yaml_init:
            self._load_config()

    @property
    def component(self):
        return self._component

    @property
    def trace_path(self):
        return os.path.join(self.path, self._trace_id)

    @property
    def yaml_path(self):
        return os.path.join(self.trace_path, self.CONFIG_YAML_NAME)

    @property
    def yaml_init(self):
        return os.path.exists(self.yaml_path)

    @property
    def repo(self):
        return self._repo_path

    @repo.setter
    def repo(self, value):
        self._repo_path = value

    @property
    def cmd(self):
        if self._workload_config and self._repo_path:
            return self._parse_cmd()

    def _parse_cmd(self):
        type_str = OrderedDict(
            {
                "distributed_transaction": "-d",
                "contention": "-c",
                "deadlock": "-D",
            }
        )
        cmds = ["-H", self.trace_path]
        for workload in self._workload_config:
            cmds.append(type_str[workload.type])
            cmds.append(workload.cmd)
        return "cd {}; {} {}".format(self.trace_path, self.repo, " ".join(cmds))

    def _lock(self, read_only=False):
        if self._lock_manager:
            if read_only:
                return (
                    self._lock_manager.scheduler_sh_lock()
                    and self._lock_manager.result_sh_lock(self._trace_id)
                )
            else:
                return (
                    self._lock_manager.scheduler_ex_lock()
                    and self._lock_manager.result_ex_lock(self._trace_id)
                )

    def create_yaml(self, src_yaml_path):
        self._lock()
        dst_yaml_path = self.yaml_path
        if not FileUtil.copy(src_yaml_path, dst_yaml_path, self.stdio):
            self.stdio.error(
                "Fail to copy yaml config file {} to {}.".format(
                    src_yaml_path, dst_yaml_path
                )
            )
            return False
        self.stdio.verbose("copy yaml config file to {}.".format(dst_yaml_path))
        return self._load_config()

    def _load_config(self):
        self._lock(read_only=True)
        yaml_loader = YamlLoader(stdio=self.stdio)
        f = open(self.yaml_path, "rb")
        src_data = yaml_loader.load(f)
        if len(src_data.keys()) <= 0:
            self.stdio.error(
                "There should be exactly one component in the workload configuration file."
            )
            return False

        self._component = src_data.keys()[0]
        self._component_config = src_data[self._component]
        self._workload_config = []
        for type, config in self._component_config.items():
            self._workload_config.append(Workload(type, config))
        return True
