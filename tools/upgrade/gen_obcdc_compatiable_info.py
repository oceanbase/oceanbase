#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# RUN THIS SCRIPT AFTER oceanbase_upgrade_dep.yml MODIFIED
#
# PREPARE: install yaml with `pip install PyYaml` before run this script
# RUN: run this script in tools/upgrade/ directory

import yaml
import os

def get_yaml(filename):
    yaml_data = None
    with open(filename, 'r', encoding='utf-8') as f:
        yaml_data = yaml.safe_load(f)
    return yaml_data

def dump_to_yaml_obj(content):
   return yaml.safe_dump(content, explicit_start=True, explicit_end=True, default_flow_style=False, encoding="utf-8")

def write_yaml_to_file(content, filename):
    with open(filename, 'w',) as f :
        yaml.safe_dump(content, f, explicit_start=True, explicit_end=True, default_flow_style=False, encoding="utf-8")
        print('Dump yaml format data to file successfully. File path: {0}'.format(filename))

def collect_can_upgrade_from(target_ver):
    target_set = set()
    target_set.add(target_ver)
    if target_ver in can_upgrade_from_map.keys():
        can_upgrade_to_target_set = can_upgrade_from_map[target_ver]
        for target in can_upgrade_to_target_set:
            target_set.update(collect_can_upgrade_from(target))
    return target_set


if __name__ == '__main__':
    dir_path = os.path.dirname(os.path.realpath(__file__))
    ob_upgrade_deps_file = dir_path + "/oceanbase_upgrade_dep.yml"
    obcdc_compatiable_info_file = dir_path + "/obcdc_compatiable_ob_info.yaml"
    ob_upgrade_deps = get_yaml(ob_upgrade_deps_file)
    support_ob_versions = []
    cur_ob_version = ''

    # key: upgraded_to_version
    # value: direct_upgrade_from_version_set
    can_upgrade_from_map = {}
    detect_cur_ob_ver_succ = False

    if ob_upgrade_deps is not None:
        for ob_ver in ob_upgrade_deps:
            cur_ver = str(ob_ver['version'])
            if 'can_be_upgraded_to' not in ob_ver.keys():
                if detect_cur_ob_ver_succ:
                    print("duplicate detect cur_ob_version, last_detect_version: {cur_ob_version}, current_decect_info: {ob_ver}")
                    break
                else:
                    detect_cur_ob_ver_succ = True
                    cur_ob_version = cur_ver
                    if can_upgrade_to not in can_upgrade_from_map.keys():
                        can_upgrade_from_set = set()
                        can_upgrade_from_map[can_upgrade_to] = can_upgrade_from_set
            else:
                can_upgrade_to_list = list(ob_ver['can_be_upgraded_to'])
                for can_upgrade_to in can_upgrade_to_list:
                    if can_upgrade_to not in can_upgrade_from_map.keys():
                        can_upgrade_from_set = set()
                        can_upgrade_from_set.add(cur_ver)
                        can_upgrade_from_map[can_upgrade_to] = can_upgrade_from_set
                    else:
                        can_upgrade_from_map[can_upgrade_to].add(cur_ver)


        if detect_cur_ob_ver_succ:
            can_upgrade_from_set = can_upgrade_from_map[cur_ob_version]
            can_upgrade_from_set.update(collect_can_upgrade_from(cur_ob_version))
            res_map = {}
            res_list = []
            res_map["compatiable_ob_version"] = sorted(list(can_upgrade_from_set))
            res_map["obcdc_version"] = cur_ob_version
            res_list.append(res_map)
            print(dump_to_yaml_obj(res_list))
            write_yaml_to_file(res_list, obcdc_compatiable_info_file)
