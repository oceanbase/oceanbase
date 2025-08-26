import argparse
from typing import Sequence, Union
import subprocess
import os

def parse_arguments(args: Union[Sequence[str],None]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=
'''
共享存储版本的ob_admin, 目前支持的命令有fetch_log\n
fetch_log命令使用ls_ctrl从日志服务节点拉取clog日志, 并使用ob_admin解析日志。示例用法如下:\n
python3 ss_ob_admin.py fetch_log --host 127.0.0.1:7891 --work_dir $PWD/clog --cluster_id 1 --tenant_id 1 --ls_id 1 --start_point 1753092197921000000 --end_point 1753093397921000000 dump_filter 'tablet_id=226'\n
python3 ss_ob_admin.py fetch_log --host 127.0.0.1:7891 --work_dir $PWD/clog --cluster_id 1 --tenant_id 1 --ls_id 1 --point_type lsn --start_point 0 --end_point 128000000 dump_log\n
python3 ss_ob_admin.py fetch_log --host 127.0.0.1:7891 --work_dir $PWD/clog --cluster_id 1 --tenant_id 1 --ls_id 1 --point_type scn --start_point 1753092197921000000 dump_tx_format\n
\n
注意:\n
1. ls_ctrl和ob_admin必须和ss_ob_admin.py在同一目录下\n
2. work_dir必须为空目录\n
3. 如果work_dir以相对路径给出, 被认为是相对于ss_ob_admin.py的路径\n
''',
        formatter_class=argparse.RawTextHelpFormatter)
    subparsers = parser.add_subparsers(dest='command')

    fetch_log_parser = subparsers.add_parser('fetch_log', help='从日志服务集群拉取日志并解析')
    fetch_log_parser.add_argument('--host', required=True, help='ln地址')
    fetch_log_parser.add_argument('--work_dir', required=True, help='工作目录，必须为空')
    fetch_log_parser.add_argument('--cluster_id', required=True, help='集群 ID')
    fetch_log_parser.add_argument('--tenant_id', required=True, help='租户 ID')
    fetch_log_parser.add_argument('--ls_id', required=True, help='日志流 ID')
    fetch_log_parser.add_argument('--point_type', required=False, choices=['lsn', 'scn'], help='位点类型（可选）, 默认scn')
    fetch_log_parser.add_argument('--start_point', required=True, help='起始点')
    fetch_log_parser.add_argument('--end_point', required=False, help='结束点（可选）')
    fetch_log_parser.add_argument('parse_clog_method', choices=['dump_log', 'dump_tx_format', 'dump_filter'], help='解析方法')
    fetch_log_parser.add_argument('FILTER_RULE', nargs='?', help='过滤规则（可选）')

    args = parser.parse_args(args)
    return args

def call_ls_ctrl(args: argparse.Namespace):
    cmd = list()
    cmd.append('./ls_ctrl')
    cmd.append('--host')
    cmd.append(args.host)
    cmd.append('fetch')
    cmd.append('--work-dir')
    cmd.append(args.work_dir)
    cmd.append('--cluster-id')
    cmd.append(args.cluster_id)
    cmd.append('--tenant-id')
    cmd.append(args.tenant_id)
    cmd.append('--ls-id')
    cmd.append(args.ls_id)
    if args.point_type:
        cmd.append('--point-type')
        cmd.append(args.point_type)
    cmd.append('--start-point')
    cmd.append(args.start_point)
    if args.end_point:
        cmd.append('--end-point')
        cmd.append(args.end_point)

    print(' '.join(cmd))
    result = subprocess.call(cmd)
    return result

def call_ob_admin(args: argparse.Namespace):
    cmd = list()
    cmd.append('./ob_admin')
    cmd.append('log_service_log_tool')
    cmd.append(args.parse_clog_method)
    if args.FILTER_RULE:
        cmd.append(args.FILTER_RULE)
    for clog_file in os.listdir(args.work_dir):
        cmd.append(os.path.join(args.work_dir, clog_file))
    print(' '.join(cmd))
    result = subprocess.call(cmd)
    return result

def main(args: Union[Sequence[str],None] = None):
    args: argparse.Namespace = parse_arguments(args)
    print(args)
    if args.command is None:
        print('one of command {dump_all|dump_hex|dump_format|state_clog|dump_filter <FILTER_RULE>} should be given')
        exit(1)
    elif args.command == 'fetch_log':
        if args.parse_clog_method == 'dump_filter' and args.FILTER_RULE is None:
            print('dump_filter <FILTER_RULE> should be given')
            exit(1)
        result = call_ls_ctrl(args)
        if result != 0:
            print('call ls_ctrl failed')
            exit(1)
        if len(os.listdir(args.work_dir)) == 0:
            print('no clog file found in work_dir'.format(args.work_dir))
            exit(1)
        result = call_ob_admin(args)
        if result != 0:
            print('call ob_admin failed')
            exit(1)
        print('fetch_log success')
    else:
        print('unknown command')
        exit(1)

if __name__ == '__main__':
    import sys
    base_dir = os.path.dirname(sys.argv[0])
    if base_dir != '':
        os.chdir(base_dir)
    main()
