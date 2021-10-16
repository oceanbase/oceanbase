#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import time
import socket
import resource
import traceback
import psutil
import argparse
from collections import defaultdict

if os.name == 'posix' and sys.version_info[0] < 3:
    from subprocess32 import Popen, PIPE
    reload(sys)
    sys.setdefaultencoding('utf8')
else:
    from subprocess import Popen, PIPE

__version__ = 0.1

K = 1024
M = 1024 ** 2
G = 1024 ** 3

def execute_command(command, env=None, timeout=None):
    try:
        p = Popen(command, env=env, shell=True, stdout=PIPE, stderr=PIPE)
        output, error = p.communicate(timeout=timeout)
        code = p.returncode
        output = output.decode(errors='replace')
        error = error.decode(errors='replace')
    except Exception as e:
        output = ''
        error = traeback.format_exc()
        code = 255
    return code, output, error

class EnvChecker(object):
    def __init__(self):
        self.args = None
        self.parser = argparse.ArgumentParser(
            "env_checker",
            usage = """
    # default usage without any option will print basic environment info
    python env_checker.py

    # for detailed info, specify one or more option
    python env_checker.py [options]
            """,
            description = "check environment info on host"
        )

        self.parser.add_argument('-v', '--version',
                            action='version',
                            version='{prog}-{version}'.format(prog=self.parser.prog, version=__version__))

        self.parser.add_argument("-a", "--all", action='store_true', default=False, help='show all info')
        self.parser.add_argument("-u", "--ulimit", action='store_true', default=False, help='show ulimit info')
        self.parser.add_argument("-i", "--io", action='store_true', default=False, help='show io throughput info')
        self.parser.add_argument("-f", "--fd", action='store_true', default=False, help='show fd usage info')
        self.parser.add_argument("-b", "--block", action='store_true', default=False, help='show bad block info')
        self.parser.add_argument("-g", "--gcc", action='store_true', default=False, help='show gcc version info')

    def parse_args(self):
        self.args = self.parser.parse_args()

    def get_cpu_info(self):
        cpu_info_dict = dict()
        cpu_info_dict["count"] = "{0} cores".format(psutil.cpu_count())
        cpu_info_dict["frequency"] = "{0} MHz".format(psutil.cpu_freq().max)
        cpu_info_dict["current_usage_percent"] = "{0}".format(psutil.cpu_percent())
        return cpu_info_dict

    def get_memory_info(self):
        memory_info = psutil.virtual_memory()
        memory_info_dict = dict()
        memory_info_dict["memory_total"] = "{0} GB".format(memory_info.total / G)
        memory_info_dict["memory_used"] = "{0} GB".format(memory_info.used / G)
        memory_info_dict["memory_free"] = "{0} GB".format(memory_info.free / G)
        memory_info_dict["memory_cached"] = "{0} GB".format(memory_info.cached / G)
        memory_info_dict["memory_available"] = "{0} GB".format(memory_info.available / G)
        memory_info_dict["page_size"] = "{0} KB".format(resource.getpagesize() / K)
        return memory_info_dict

    def get_network_info(self):
        net_info_dict = defaultdict(lambda: dict())
        net_stats = psutil.net_if_stats()
        for dev, stat in net_stats.items():
            if stat.isup:
                net_info_dict["bandwidth"] = "{0} Mb".format(stat.speed)
        net_addrs = psutil.net_if_addrs()
        for dev in net_addrs.keys():
            addrs = net_addrs[dev]
            for addr_info in addrs:
                # AddressFamily.AF_INET
                if addr_info.family == socket.AF_INET:
                    net_info_dict[dev]["IPv4 address"] = addr_info.address
                # AddressFamily.AF_INET6
                if addr_info.family == socket.AF_INET6:
                    net_info_dict[dev]["IPv6 address"] = addr_info.address
        return net_info_dict

    def get_disk_info(self):
        disk_info_list = list()
        all_disks = psutil.disk_partitions()
        for disk in all_disks:
            disk_info = dict()
            disk_usage = psutil.disk_usage(disk.mountpoint)
            disk_info["device"] = disk.device
            disk_info["mountpoint"] = disk.mountpoint
            disk_info["filesystem"] = disk.fstype
            disk_usage_dict = dict()
            disk_usage_dict["total"] = "{0} GB".format(disk_usage.total / G)
            disk_usage_dict["free"] = "{0} GB".format(disk_usage.free / G)
            disk_usage_dict["used"] = "{0} GB".format(disk_usage.used / G)
            disk_usage_dict["used_percent"] = disk_usage.percent
            disk_info["usage"] = disk_usage_dict
            disk_info_list.append(disk_info)
        return disk_info_list

    def get_os_info(self):
        os_info = os.uname()
        os_info_dict = dict()
        os_info_dict["sysname"] = os_info[0]
        os_info_dict["release"] = os_info[2]
        os_info_dict["machine"] = os_info[4]
        return os_info_dict

    def get_cpu_usage(self):
        last_worktime, last_idletime
        fd = open("/proc/stat", "r")
        line = ""
        while not "cpu " in line: line = f.readline()
        f.close()
        spl = line.split(" ")
        worktime = int(spl[2]) + int(spl[3]) + int(spl[4])
        idletime = int(spl[5])
        dworktime = (worktime - last_worktime)
        didletime = (idletime - last_idletime)
        rate = float(dworktime) / (didletime + dworktime)
        last_worktime = worktime
        last_idletime = idletime
        if (last_worktime == 0): return 0
        return rate

    def get_bad_blocks(self):
        disk_badblocks = dict()
        all_disks = psutil.disk_partitions()
        for disk in all_disks:
            cmd = "badblocks -b 4096 -c 128 {0}".format(disk.device)
            status, output, error = execute_command(cmd)
            if status != 0:
                print("get bad blocks for device {0} failed: {1}".format(disk.device, error))
            else:
                disk_badblocks[disk.device] = output
        return disk_badblocks

    def show_fd_info(self):
        cmd = "lsof -n | awk '{print $1\" \"$2}' | sort | uniq -c | sort -nr"
        status, output, error = execute_command(cmd)
        if status != 0:
            print("get fd info failed: {0}".format(error))
        else:
            total_count = 0
            limit = 10
            idx = 0
            results = output.split("\n")
            for result in results:
                result_parts = result.strip().split()
                if len(result_parts) == 0 or not (result_parts[0].isdigit() and result_parts[-1].isdigit()):
                    continue
                else:
                    total_count += int(result_parts[-1])
                    if idx < limit:
                        print("process: {0}, Pid: {1}, fd count: {2}".format(result_parts[1], result_parts[-1], result_parts[0]))
                        idx += 1
            print("total opened fds: {0}".format(total_count))

    def get_io_throuput_info(self):
        disk_throughput = defaultdict(lambda: dict())
        all_disks = psutil.disk_partitions()
        for disk in all_disks:
            cmdBufferedWrite = "dd if=/dev/zero of={0}/test_file bs=4k count=100000".format(disk.mountpoint)
            cmdDirectWrite = "dd if=/dev/zero of={0}/test_file bs=4k count=100000 oflag=dsync".format(disk.mountpoint)
            cmdBufferedRead = "dd of=/dev/null if={0}/test_file bs=4k".format(disk.mountpoint)
            cmdDirectRead = "dd of=/dev/null if={0}/test_file bs=4k iflag=dsync".format(disk.mountpoint)
            t_start = time.time()
            status, output, error = execute_command(cmdBufferedWrite)
            t_end = time.time()
            elapsed_time = t_end - t_start
            if status != 0:
                print("get throughput for device {0} failed: {1}".format(disk.device, error))
            else:
                disk_throughput[disk.device]["bufferdwrite"] = "{0}M/s".format(400 / elapsed_time)

            t_start = time.time()
            status, output, error = execute_command(cmdDirectWrite)
            t_end = time.time()
            elapsed_time = t_end - t_start
            if status != 0:
                print("get throughput for device {0} failed: {1}".format(disk.device, error))
            else:
                disk_throughput[disk.device]["directwrite"] = "{0}M/s".format(400 / elapsed_time)

            t_start = time.time()
            status, output, error = execute_command(cmdBufferedRead)
            t_end = time.time()
            elapsed_time = t_end - t_start
            if status != 0:
                print("get throughput for device {0} failed: {1}".format(disk.device, error))
            else:
                disk_throughput[disk.device]["bufferdread"] = "{0}M/s".format(400 / elapsed_time)

            t_start = time.time()
            status, output, error = execute_command(cmdDirectRead)
            t_end = time.time()
            elapsed_time = t_end - t_start
            if status != 0:
                print("get throughput for device {0} failed: {1}".format(disk.device, error))
            else:
                disk_throughput[disk.device]["directread"] = "{0}M/s".format(400 / elapsed_time)
        return disk_throughput

    def get_gcc_version(self):
        cmd = "gcc -v 2>&1"
        status, output, error = execute_command(cmd)
        if status != 0:
            print("get gcc version failed: {0}".format(error))
        else:
            return output

    def get_ulimit(self):
        cmd = "ulimit -a"
        status, output, error = execute_command(cmd)
        if status != 0:
            print("get gcc version failed: {0}".format(error))
        else:
            return output

    def print_system_info(self):
        print('==================== os info ====================')
        print_dict(self.get_os_info())
        print("")

        print('==================== cpu info ===================')
        cpu_info_dict = self.get_cpu_info()
        print_dict(cpu_info_dict)
        print("")

        print('==================== mem info ===================')
        memory_info_dict = self.get_memory_info()
        print_dict(memory_info_dict)
        print("")

        print('==================== net info ===================')
        net_info_dict = self.get_network_info()
        print_dict(net_info_dict)
        print("")

        print('=================== disk info ===================')
        disk_info_list = self.get_disk_info()
        for disk_info_dict in disk_info_list:
            print_dict(disk_info_dict)
            print("")
        print("")

        if self.args.gcc or self.args.all:
            print('==================== gcc info ===================')
            print(self.get_gcc_version())
            print("")

        if self.args.ulimit or self.args.all:
            print('================== ulimit info ==================')
            ulimit_info = self.get_ulimit()
            print(ulimit_info)
            print("")

        if self.args.io or self.args.all:
            print('==================== io info ====================')
            io_throuput_dict = self.get_io_throuput_info()
            print_dict(io_throuput_dict)
            print("")

        if self.args.fd or self.args.all:
            print('==================== fd info ====================')
            self.show_fd_info()
            print("")

        if self.args.block or self.args.all:
            print('================= badblock info =================')
            badblock_info_dict = self.get_bad_blocks()
            print_dict(badblock_info_dict)
            print("")

def print_dict(d, indent=0):
    if len(d) == 0:
        print("no result")
    for k, v in d.items():
        if isinstance(v, dict):
            print(k)
            print_dict(v, indent=indent + 2)
        else:
            print("{0}{1}: {2}".format(" " * indent, k, v))

if '__main__' == __name__:
    env_checker = EnvChecker()
    env_checker.parse_args()
    env_checker.print_system_info()
