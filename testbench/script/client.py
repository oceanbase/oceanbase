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


from stdio import SafeStdio
from tool import FileUtil
import os
import sys
import time
import warnings
from glob import glob

from subprocess32 import Popen, PIPE

warnings.filterwarnings("ignore")

if sys.version_info.major == 2:
    import MySQLdb as MySQL
else:
    import pymysql as MySQL


class CmdReturn(object):
    def __init__(self, code, stdout, stderr):
        self.code = code
        self.stdout = stdout
        self.stderr = stderr

    def __bool__(self):
        return self.code == 0

    def __nonzero__(self):
        return self.__bool__()


class LocalClient(SafeStdio):
    @staticmethod
    def execute_command(command, env=None, timeout=None, stdio=None):
        stdio.verbose("local execute: {} ".format(command))
        try:
            p = Popen(command, env=env, shell=True, stdout=PIPE, stderr=PIPE)
            output, error = p.communicate(timeout=timeout)
            code = p.returncode
            output = output.decode(errors="replace")
            error = error.decode(errors="replace")
            verbose_msg = "exited code %s" % code
            if code:
                verbose_msg += ", error output:\n%s" % error
            stdio.verbose(verbose_msg)
        except Exception as e:
            output = ""
            error = str(e)
            code = 255
            verbose_msg = "exited code 255, error output:\n%s" % error
            stdio.verbose(verbose_msg)
            stdio.exception("")
        return CmdReturn(code, output, error)

    @staticmethod
    def put_file(local_path, remote_path, stdio=None):
        if LocalClient.execute_command(
            "mkdir -p %s && cp -f %s %s"
            % (os.path.dirname(remote_path), local_path, remote_path),
            stdio=stdio,
        ):
            return True
        return False

    @staticmethod
    def put_dir(local_dir, remote_dir, stdio=None):
        if os.path.isdir(local_dir):
            local_dir = os.path.join(local_dir, "*")
        if os.path.exists(os.path.dirname(local_dir)) and not glob(local_dir):
            stdio.verbose("%s is empty" % local_dir)
            return True
        if LocalClient.execute_command(
            "mkdir -p %s && cp -frL %s %s" % (remote_dir, local_dir, remote_dir),
            stdio=stdio,
        ):
            return True
        return False

    @staticmethod
    def write_file(content, file_path, mode="w", stdio=None):
        stdio.verbose("write {} to {}".format(content, file_path))
        try:
            with FileUtil.open(file_path, mode, stdio=stdio) as f:
                f.write(content)
                f.flush()
            return True
        except:
            stdio.exception("")
            return False

    @staticmethod
    def get_file(local_path, remote_path, stdio=None):
        return LocalClient.put_file(remote_path, local_path, stdio=stdio)

    @staticmethod
    def get_dir(local_path, remote_path, stdio=None):
        return LocalClient.put_dir(remote_path, local_path, stdio=stdio)


class MySQLClient(SafeStdio):
    @staticmethod
    def connect(server, user="root", password="", stdio=None):
        ip = server.get_conf("ip_addr")
        port = server.get_conf("mysql_port")
        name = server.get_conf("server_name")
        stdio.verbose(
            "try to connect to server {} {} -P{} -u{} -p{}".format(
                name, ip, port, user, password
            )
        )
        count = 10
        while count:
            count -= 1
            try:
                if sys.version_info.major == 2:
                    db = MySQL.connect(
                        host=ip, user=user, port=int(port), passwd=str(password)
                    )
                    cursor = db.cursor(cursorclass=MySQL.cursors.DictCursor)
                else:
                    db = MySQL.connect(
                        host=ip,
                        user=user,
                        port=int(port),
                        password=str(password),
                        cursorclass=MySQL.cursors.DictCursor,
                    )
                    cursor = db.cursor()
                return cursor
            except:
                stdio.exception("")
                time.sleep(1)
        stdio.error("Fail to connect to server {}".format(name))
        return None
