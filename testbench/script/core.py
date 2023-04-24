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

from cluster import ClusterManager
from repository import RepositoryManager
from lock import LockManager
from scheduler import SchedulerManager
from client import LocalClient, MySQLClient, MySQL
from tool import FileUtil, DirectoryUtil


class TestBench(object):
    def __init__(self, home_path, opts, stdio=None):
        self._opts = opts
        self._home_path = home_path
        self._lock_manager = None
        self._cluster_manager = None
        self._scheduler_manager = None
        self._repository_manager = None
        self.stdio = stdio

    @property
    def lock_manager(self):
        if not self._lock_manager:
            self._lock_manager = LockManager(self._home_path, self.stdio)
        return self._lock_manager

    @property
    def cluster_manager(self):
        if not self._cluster_manager:
            self._cluster_manager = ClusterManager(
                self._home_path, self.lock_manager, self.stdio
            )
        return self._cluster_manager

    @property
    def scheduler_manager(self):
        if not self._scheduler_manager:
            self._scheduler_manager = SchedulerManager(
                self._home_path,
                getattr(self._opts, "traceid", ""),
                self.lock_manager,
                self.stdio,
            )
        return self._scheduler_manager

    @property
    def repository_manager(self):
        if not self._repository_manager:
            self._repository_manager = RepositoryManager(self._home_path, self.stdio)
        return self._repository_manager

    def _create_workspace(self):
        def mkdir_workspace(name, server):
            if not DirectoryUtil.mkdir(server.get_conf("work_space")):
                self.stdio.error(
                    "Fail to make work space directory for {}".format(name)
                )
                return False

        success = True
        for ret in self.cluster_manager.traverse_server(mkdir_workspace):
            if not ret:
                success = False
        return success

    def deploy_cluster(self):
        config = getattr(self._opts, "config", "")
        if not self.cluster_manager.create_yaml(config):
            return False
        return self._create_workspace()

    def start_cluster(self):
        component = self.cluster_manager.component
        repo = self.repository_manager.get_repository(component)
        if not repo:
            self.stdio.error(
                "Fail to find binary file for component {}".format(component)
            )
            return False

        def set_repo(_, server):
            server.repo = repo

        self.cluster_manager.traverse_server(set_repo)
        for name, cmd in self.cluster_manager.traverse_server(
            lambda _, server: server.cmd
        ).items():
            self.stdio.verbose("start command {}".format(cmd))
            ret = LocalClient.execute_command(cmd, stdio=self.stdio)
            self.stdio.verbose("{} return code: {}".format(name, ret.code))
            self.stdio.verbose("{} stdout: {}".format(name, ret.stdout))
            self.stdio.verbose("{} stderr: {}".format(name, ret.stderr))
            if ret.stderr or ret.code:
                return False
        return True

    def bootstrap(self):
        rs = self.cluster_manager.root_service
        cursor = MySQLClient.connect(rs, stdio=self.stdio)
        if not cursor:
            self.stdio.error("Fail to get database connection in bootstrap.")
            return False

        def is_bootstrap():
            try:
                sql = 'select column_value from oceanbase.__all_core_table where table_name = "__all_global_stat" and column_name = "baseline_schema_version"'
                self.stdio.verbose("check bootstrap - {}".format(sql))
                cursor.execute(sql)
                return int(cursor.fetchone().get("column_value")) > 0
            except MySQL.DatabaseError as e:
                self.stdio.verbose("bootstrap exception {}:{}".format(e.args))
                return False

        bootstrap_sqls = []
        add_server_sqls = []
        zone_configs = {}

        def get_bootstrap(_, server):
            zone = server.get_conf("zone")
            ip = server.get_conf("ip_addr")
            port = server.get_conf("rpc_port")
            if zone in zone_configs:
                add_server_sqls.append(
                    'alter system add server "{}:{}" zone "{}"' % (ip, port, zone)
                )
            else:
                zone_configs[zone] = {}
                bootstrap_sqls.append(
                    'region "sys_region" zone "{}" server "{}:{}"'.format(
                        zone, ip, port
                    )
                )

        def try_bootstrap():
            try:
                self.cluster_manager.traverse_server(get_bootstrap)
                sql = "alter system bootstrap {}".format(",".join(bootstrap_sqls))
                self.stdio.verbose("system bootstrap - {}".format(sql))
                cursor.execute(sql)
                for sql in add_server_sqls:
                    self.stdio.verbose("add server - {}".format(sql))
                    cursor.execute(sql)
                return is_bootstrap()
            except:
                return False

        return try_bootstrap()

    def stop_server(self):
        def kill_server(name, server):
            if not os.path.exists(server.pid_path):
                self.stdio.warn("Found server {} not active".format(name))
                return False
            pid = FileUtil.open(server.pid_path).readline().strip("\n")
            if LocalClient.execute_command(
                "kill -9 {}".format(pid), stdio=self.stdio
            ).code:
                self.stdio.warn("Fail to stop server {} by pid {}".format(name, pid))
                return False

        success = True
        for ret in self.cluster_manager.traverse_server(kill_server):
            if not ret:
                success = False
        repo = self.cluster_manager.root_service.repo
        if (
            not success
            and LocalClient.execute_command(
                'pkill -9 -u `whoami` -f "^{}"'.format(repo), stdio=self.stdio
            ).code
        ):
            self.stdio.error("Fail to stop servers by pkill")
            return False
        return True

    def destroy_cluster(self):
        if not self.stop_server():
            return False
        return self.cluster_manager.destroy_cluster()

    def display_cluster(self):
        def get_status(name, server):
            status = {}
            status["name"] = name
            status["ip"] = server.get_conf("ip_addr")
            status["port"] = server.get_conf("mysql_port")
            status["zone"] = server.get_conf("zone")
            try:
                pid = FileUtil.open(server.pid_path).readline().strip("\n")
                if pid and LocalClient.execute_command(
                    "ls /proc/{}".format(pid), stdio=self.stdio
                ):
                    status["status"] = "active"
                else:
                    status["status"] = "inactive"
            except:
                status["status"] = "inactive"
            return status

        status = self.cluster_manager.traverse_server(get_status)
        self.stdio.print_list(
            status.values(),
            ["name", "ip", "port", "zone", "status"],
            lambda x: [x["name"], x["ip"], x["port"], x["zone"], x["status"]],
            title="Cluster Status",
        )
        return True

    def start_scheduler(self):
        config = getattr(self._opts, "config", "")
        if not self.scheduler_manager.create_yaml(config):
            self.stdio.error(
                "Fail to load workload config for testbench {}".format(config)
            )
            return False
        component = self.scheduler_manager.component
        repo = self.repository_manager.get_repository(component)
        if not repo:
            self.stdio.error(
                "Fail to find binary file for component {}".format(component)
            )
            return False

        self.scheduler_manager.repo = repo
        ret = LocalClient.execute_command(self.scheduler_manager.cmd, stdio=self.stdio)
        self.stdio.verbose("return code: {}".format(ret.code))
        self.stdio.verbose("stdout: {}".format(ret.stdout))
        self.stdio.verbose("stderr: {}".format(ret.stderr))
        if ret.stderr or ret.code:
            return False
        return True

    def create_tenant(self):
        tenant_name = "tb"
        unit_name = "tb_unit"
        pool_name = "tb_pool"
        rs = self.cluster_manager.root_service
        cursor = MySQLClient.connect(rs, stdio=self.stdio)
        if not cursor:
            self.stdio.error("Fail to get database connection in create tenant")
            return False

        # check tenant existence
        def is_tenant_exist():
            sql = "select * from oceanbase.DBA_OB_TENANTS where TENANT_NAME = '{}'".format(
                tenant_name
            )
            try:
                self.stdio.verbose("execute sql command {}".format(sql))
                cursor.execute(sql)
                if cursor.fetchone():
                    return True
            except MySQL.DatabaseError as e:
                self.stdio.error("Check tenant existence exception {}".format(e.args))
                return False
            return False

        if is_tenant_exist():
            self.stdio.error("Tenant {} already exists".format(tenant_name))
            return False

        # get zone information
        zone_svr_num = {}
        sql = "select zone, count(*) num from oceanbase.__all_server where status = 'active' group by zone"
        try:
            self.stdio.verbose("execute sql command {}".format(sql))
            cursor.execute(sql)
            res = cursor.fetchall()
            for row in res:
                zone_svr_num[str(row["zone"])] = row["num"]
        except MySQL.DatabaseError as e:
            self.stdio.error("Get zone information exception {}".format(e.args))
            return False
        zone = zone_svr_num.keys()
        zone_list = "('{}')".format("','".join(zone))
        zone_num = len(zone)
        unit_num = min(zone_svr_num.items(), key=lambda x: x[1])[1]

        # get server information
        sql = "select * from oceanbase.GV$OB_SERVERS where zone in {}".format(zone_list)
        try:
            self.stdio.verbose("execute sql command {}".format(sql))
            cursor.execute(sql)
        except MySQL.DatabaseError as e:
            self.stdio.error("Get server information exception {}".format(e.args))
            return False
        svr_stats = cursor.fetchall()
        cpu_avail = svr_stats[0]["CPU_CAPACITY_MAX"] - svr_stats[0]["CPU_ASSIGNED_MAX"]
        mem_avail = svr_stats[0]["MEM_CAPACITY"] - svr_stats[0]["MEM_ASSIGNED"]
        disk_avail = (
            svr_stats[0]["DATA_DISK_CAPACITY"] - svr_stats[0]["DATA_DISK_IN_USE"]
        )
        log_disk_avail = (
            svr_stats[0]["LOG_DISK_CAPACITY"] - svr_stats[0]["LOG_DISK_ASSIGNED"]
        )
        for svr_stat in svr_stats[1:]:
            cpu_avail = min(
                svr_stat["CPU_CAPACITY_MAX"] - svr_stat["CPU_ASSIGNED_MAX"], cpu_avail
            )
            mem_avail = min(
                svr_stat["MEM_CAPACITY"] - svr_stat["MEM_ASSIGNED"], mem_avail
            )
            disk_avail = min(
                svr_stat["DATA_DISK_CAPACITY"] - svr_stat["DATA_DISK_IN_USE"],
                disk_avail,
            )
            log_disk_avail = min(
                svr_stat["LOG_DISK_CAPACITY"] - svr_stat["LOG_DISK_ASSIGNED"],
                log_disk_avail,
            )

        # create resource unit
        sql = "create resource unit {} memory_size {}, max_cpu {}, min_cpu {}, log_disk_size {}".format(
            unit_name, mem_avail, cpu_avail, cpu_avail, log_disk_avail
        )
        try:
            self.stdio.verbose("execute sql command {}".format(sql))
            cursor.execute(sql)
        except MySQL.DatabaseError as e:
            self.stdio.error("Create resource unit exception {}".format(e.args))
            return False

        # create resource pool
        sql = "create resource pool {} unit={}, unit_num={}, zone_list={}".format(
            pool_name, unit_name, unit_num, zone_list
        )
        try:
            self.stdio.verbose("execute sql command {}".format(sql))
            cursor.execute(sql)
        except MySQL.DatabaseError as e:
            self.stdio.error("Create resource pool exception {}".format(e.args))
            return False

        # create tenant
        mode = "mysql"
        replica_num = zone_num
        primary_zone = "RANDOM"
        sql = "create tenant {} replica_num={}, zone_list={}, primary_zone='{}', resource_pool_list=('{}') set ob_compatibility_mode='{}'".format(
            tenant_name, replica_num, zone_list, primary_zone, pool_name, mode
        )
        try:
            self.stdio.verbose("execute sql command {}".format(sql))
            cursor.execute(sql)
        except MySQL.DatabaseError as e:
            self.stdio.error("Create tenant exception {}".format(e.args))
            return False
        return is_tenant_exist()
