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

from tool import DirectoryUtil, FileUtil, YamlLoader, OrderedDict
from manager import Manager

yaml = YamlLoader()


class Server(object):
    def __init__(self, conf):
        self._conf = OrderedDict(conf)
        self._repo_path = None
        self._cmd = None
        self._cmds = None
        self._stdout = None
        self._stderr = None

    @property
    def conf(self):
        return self._conf

    @property
    def repo(self):
        return self._repo_path

    @repo.setter
    def repo(self, value):
        self._repo_path = value

    @property
    def cmd(self):
        if self._cmd:
            return self._cmd
        if self._cmds and self.get_conf("home_path") and self._repo_path:
            self._cmd = self._parse_cmd()
            return self._cmd
        return None

    @property
    def pid_path(self):
        return self._get_pid_path()

    def _parse_cmd(self):
        return "cd {}; {} {}".format(
            self.get_conf("work_space"), self.repo, " ".join(self._cmds)
        )

    def _get_default_homepath(self):
        return os.path.join(self.path, "cluster")

    def _get_pid_path(self):
        return os.path.join(self.get_conf("work_space"), "run", "observer.pid")

    def get_conf(self, key):
        return self._conf[key]

    def set_conf(self, key, value):
        self._conf[key] = value

    def parse_config(self):
        if not self.get_conf("home_path"):
            self.set_conf("home_path", self._get_default_homepath())
        self.set_conf(
            "work_space",
            os.path.join(self.get_conf("home_path"), self.get_conf("server_name")),
        )
        self.set_conf("data_dir", "{}/store".format(self.get_conf("work_space")))
        not_opt_str = OrderedDict(
            {
                "mysql_port": "-p",
                "rpc_port": "-P",
                "zone": "-z",
                "nodaemon": "-N",
                "appname": "-n",
                "cluster_id": "-c",
                "data_dir": "-d",
                "devname": "-i",
                "syslog_level": "-l",
                "ipv6": "-6",
                "mode": "-m",
                "scn": "-f",
                "rs_list_opt": "-r",
            }
        )
        not_cmd_opt = [
            "home_path",
            "obconfig_url",
            "root_password",
            "proxyro_password",
            "redo_dir",
            "clog_dir",
            "ilog_dir",
            "slog_dir",
            "$_zone_idc",
            "production_mode",
            "ip_addr",
            "server_name",
            "work_space",
        ]

        def get_value(key):
            return "'{}'".format(self.get_conf(key))

        opt_str = []
        for key in self.conf:
            if key not in not_cmd_opt and key not in not_opt_str:
                value = get_value(key)
                opt_str.append("{}={}".format(key, value))

        self._cmds = []
        for key in not_opt_str:
            if key in self.conf:
                value = get_value(key)
                self._cmds.append("{} {}".format(not_opt_str[key], value))
        self._cmds.append("-o {}".format(",".join(opt_str)))


class ClusterManager(Manager):
    RELATIVE_PATH = "cluster/"
    CLUSTER_YAML_NAME = "config.yaml"

    def __init__(self, home_path, lock_manager=None, stdio=None):
        super(ClusterManager, self).__init__(home_path, stdio)
        self._component = None
        self._component_config = None
        self._server_config = OrderedDict()
        self._root_service = None
        self._lock_manager = lock_manager
        self.stdio = stdio
        if self.yaml_init:
            self._load_config()

    @property
    def component(self):
        return self._component

    @property
    def root_service(self):
        return self._root_service

    @property
    def yaml_path(self):
        return os.path.join(self.path, self.CLUSTER_YAML_NAME)

    @property
    def yaml_init(self):
        return os.path.exists(self.yaml_path)

    def _lock(self, read_only=False):
        if self._lock_manager:
            if read_only:
                return self._lock_manager.cluster_sh_lock()
            else:
                return self._lock_manager.cluster_ex_lock()
        return True

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
        self._load_config()
        return True

    def _load_config(self):
        self._lock(read_only=True)
        yaml_loader = YamlLoader(stdio=self.stdio)
        if not self._parse_cluster_config(yaml_loader):
            return False
        if not self._parse_server_config():
            return False
        return True

    def _parse_cluster_config(self, yaml_loader):
        f = open(self.yaml_path, "rb")
        src_data = yaml_loader.load(f)
        if len(src_data.keys()) <= 0:
            self.stdio.error(
                "There should be exactly one component in the cluster configuration file."
            )
            return False
        self._component = src_data.keys()[0]
        self._component_config = src_data[self._component]
        global_config = OrderedDict()
        if "global" not in self._component_config:
            self.stdio.warn(
                "Cannot find global parameters in the cluster configuration file."
            )
        else:
            global_config = self._component_config["global"]

        for name, config in self._component_config.items():
            if name != "global":
                server_config = Server(global_config)
                for key, value in config.items():
                    server_config.set_conf(key, value)
                self._server_config[name] = server_config

        if len(self._server_config) == 0:
            self.stdio.error(
                "Server number should not be 0. Please check the syntax of your configuration file."
            )
            return False
        return True

    def _parse_server_config(self):
        root_servers = OrderedDict()
        for _, server in self._server_config.items():
            if not self._root_service:
                self._root_service = server
            server.set_conf("ip_addr", "127.0.0.1")
            zone = server.get_conf("zone")
            if zone not in root_servers:
                root_servers[zone] = "{}:{}:{}".format(
                    server.get_conf("ip_addr"),
                    server.get_conf("rpc_port"),
                    server.get_conf("mysql_port"),
                )
        rs_list_opt = ";".join([root_servers[zone] for zone in root_servers])

        for name, server in self._server_config.items():
            server.set_conf("server_name", name)
            server.set_conf("rs_list_opt", rs_list_opt)
            server.parse_config()
        return True

    def traverse_server(self, callback):
        if not self.yaml_init:
            self.stdio.error("Cannot find yaml config file.")
            raise RuntimeError
        ret = OrderedDict()
        for name, server in self._server_config.items():
            ret[name] = callback(name, server)
        return ret

    def destroy_cluster(self):
        self._lock()
        if not self.yaml_init:
            self.stdio.error("Cannot find yaml config file.")
            raise RuntimeError
        for name, server in self._server_config.items():
            workspace = server.get_conf("work_space")
            if not self._rm(workspace):
                self.stdio.error(
                    "Fail to clear workspace {} for server {}.".format(workspace, name)
                )
                return False

        if not self._rm(self.path):
            self.stdio.error("Fail to clear cluster directory {}.".format(self.path))
            return False
        return True
