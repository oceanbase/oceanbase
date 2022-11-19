/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER_OMT

#include "lib/file/file_directory_utils.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/utility.h"
#include "lib/oblog/ob_log.h"
#include "share/ob_errno.h"
#include "share/config/ob_server_config.h"
#include "share/resource_manager/ob_resource_plan_info.h"
#include "share/resource_manager/ob_resource_manager.h"
#include "share/resource_manager/ob_cgroup_ctrl.h"

#include <stdlib.h>
#include <stdio.h>

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace share
{
ObCgSet ObCgSet::instance_; 
}
}

// 创建cgroup初始目录结构，将所有线程加入other组
int ObCgroupCtrl::init()
{
  int ret = OB_SUCCESS;
  if (GCONF.enable_cgroup == false) {
    // not init cgroup when config set to false
  } else if (OB_FAIL(init_cgroup_root_dir_(root_cgroup_))) {
    LOG_WARN("init cgroup dir failed", K(ret), K(root_cgroup_));
  } else if (OB_FAIL(init_cgroup_dir_(other_cgroup_))) {
    LOG_WARN("init other cgroup dir failed", K(ret), K_(other_cgroup));
  } else {
    char procs_path[PATH_BUFSIZE];
    char pid_value[VALUE_BUFSIZE];
    snprintf(procs_path, PATH_BUFSIZE, "%s/cgroup.procs", other_cgroup_);
    snprintf(pid_value, VALUE_BUFSIZE, "%d", getpid());
    if(OB_FAIL(write_string_to_file_(procs_path, pid_value))) {
      LOG_WARN("add tid to cgroup failed", K(ret), K(procs_path), K(pid_value));
    } else {
      valid_ = true;
    }
  }

  return ret;
}

int ObCgroupCtrl::remove_tenant_cgroup(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  char tenant_path[PATH_BUFSIZE];
  char tenant_task_path[PATH_BUFSIZE];
  char other_task_path[PATH_BUFSIZE];
  snprintf(tenant_path, PATH_BUFSIZE, "%s/tenant_%04lu", root_cgroup_, tenant_id);
  snprintf(tenant_task_path, PATH_BUFSIZE, "%s/tenant_%04lu/tasks", root_cgroup_, tenant_id);
  snprintf(other_task_path, PATH_BUFSIZE, "%s/tasks", other_cgroup_);
  FILE* tenant_task_file = nullptr;
  if (OB_ISNULL(tenant_task_file = fopen(tenant_task_path, "r"))) {
    ret = OB_IO_ERROR;
    LOG_WARN("open tenant task path failed", K(ret), K(tenant_task_path), K(errno), KERRMSG);
  } else {
    char tid_buf[VALUE_BUFSIZE];
    while (fgets(tid_buf, VALUE_BUFSIZE, tenant_task_file)) {
      if (OB_FAIL(write_string_to_file_(other_task_path, tid_buf))) {
        LOG_WARN("remove tenant task failed", K(ret), K(other_task_path), K(tenant_id));
        break;
      }
    }
    fclose(tenant_task_file);
  }
  if (OB_SUCC(ret) && OB_FAIL(FileDirectoryUtils::delete_directory(tenant_path))) {
    LOG_WARN("remove tenant cgroup directory failed", K(ret), K(tenant_path), K(tenant_id));
  } else {
    LOG_INFO("remove tenant cgroup directory success", K(tenant_path), K(tenant_id));
  }
  return ret;
}

int ObCgroupCtrl::get_task_path(
    char *task_path,
    int path_bufsize,
    const uint64_t tenant_id,
    int level,
    const char *group)
{
  int ret = OB_SUCCESS;
  switch(level) {
    case 0:
      // 仅用于支持 500 - 999 租户
      snprintf(task_path, path_bufsize, "%s/tenant_%04lu/tasks",
               root_cgroup_, tenant_id);
      break;
    case 1:
      snprintf(task_path, path_bufsize, "%s/tenant_%04lu/%s/tasks",
               root_cgroup_, tenant_id, group);
      break;
    case 2:
      snprintf(task_path, path_bufsize, "%s/tenant_%04lu/%s/tasks",
               root_cgroup_, tenant_id, group);
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      break;
  }
  return ret;
}

int ObCgroupCtrl::get_task_path(
    char *task_path,
    int path_bufsize,
    const uint64_t tenant_id,
    int level,
    const ObString &group)
{
  int ret = OB_SUCCESS;
  switch(level) {
    case 0:
      // 仅用于支持 500 - 999 租户
      snprintf(task_path, path_bufsize, "%s/tenant_%04lu/tasks",
               root_cgroup_, tenant_id);
      break;
    case 1:
      snprintf(task_path, path_bufsize, "%s/tenant_%04lu/%.*s/tasks",
               root_cgroup_, tenant_id, group.length(), group.ptr());
      break;
    case 2:
      snprintf(task_path, path_bufsize, "%s/tenant_%04lu/%.*s/tasks",
               root_cgroup_, tenant_id, group.length(), group.ptr());
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      break;
  }
  return ret;
}

int ObCgroupCtrl::get_group_path(
    char *group_path,
    int path_bufsize,
    const uint64_t tenant_id,
    int64_t group_id)
{
  int ret = OB_SUCCESS;
  char *group_name = nullptr;
  share::ObGroupName g_name;
  if (INT64_MAX == group_id) {
    snprintf(group_path, path_bufsize, "%s/tenant_%04lu",
              root_cgroup_, tenant_id);
  } else if (group_id < OBCG_MAXNUM) {
    ObCgSet &set = ObCgSet::instance();
    group_name = const_cast<char*>(set.name_of_id(group_id));
    snprintf(group_path, path_bufsize, "%s/tenant_%04lu/%s",
          root_cgroup_, tenant_id, group_name);
  } else if (OB_FAIL(get_group_info_by_group_id(tenant_id, group_id, g_name))){
    LOG_WARN("get group_name by id failed", K(group_id), K(ret));
  } else {
    group_name = const_cast<char*>(g_name.get_group_name().ptr());
    snprintf(group_path, path_bufsize, "%s/tenant_%04lu/%s",
          root_cgroup_, tenant_id, group_name);
  }
  return ret;
}

int ObCgroupCtrl::get_group_path(
    char *group_path,
    int path_bufsize,
    const uint64_t tenant_id,
    int level,
    const char *group)
{
  int ret = OB_SUCCESS;
  switch(level) {
    case 1:
      snprintf(group_path, path_bufsize, "%s/tenant_%04lu/%s",
               root_cgroup_, tenant_id, group);
      break;
    case 2:
      snprintf(group_path, path_bufsize, "%s/tenant_%04lu/%s",
               root_cgroup_, tenant_id, group);
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      break;
  }
  return ret;
}

int ObCgroupCtrl::get_group_path(
    char *group_path,
    int path_bufsize,
    const uint64_t tenant_id,
    int level,
    const common::ObString &group)
{
  int ret = OB_SUCCESS;
  switch(level) {
    case 1:
      snprintf(group_path, path_bufsize, "%s/tenant_%04lu/%.*s",
               root_cgroup_, tenant_id, group.length(), group.ptr());
      break;
    case 2:
      snprintf(group_path, path_bufsize, "%s/tenant_%04lu/%.*s",
               root_cgroup_, tenant_id, group.length(), group.ptr());
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      break;
  }
  return ret;
}

int ObCgroupCtrl::create_user_tenant_group_dir(
    const uint64_t tenant_id,
    int level,
    const common::ObString &group)
{
  int ret = OB_SUCCESS;
  char tenant_cg_dir[PATH_BUFSIZE];
  bool exist_cgroup = false;
  if (OB_ISNULL(group) || level < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid args", K(group), K(level));
  } else if (OB_FAIL(get_group_path(tenant_cg_dir, PATH_BUFSIZE, tenant_id, level, group))) {
    LOG_WARN("fail get group path", K(tenant_id), K(ret));
  } else if (OB_FAIL(FileDirectoryUtils::is_exists(tenant_cg_dir, exist_cgroup))) {
    LOG_WARN("fail check file exist", K(tenant_cg_dir), K(ret));
  } else if (!exist_cgroup && OB_FAIL(init_cgroup_dir_(tenant_cg_dir))) {
    // note: 支持并发创建同一个目录，都返回 OB_SUCCESS
    LOG_WARN("init tenant cgroup dir failed", K(ret), K(tenant_cg_dir), K(tenant_id));
  }
  return ret;
}

int ObCgroupCtrl::add_thread_to_cgroup(const pid_t tid, const uint64_t tenant_id, int64_t group_id)
{
  int ret = OB_SUCCESS;
  char group_path[PATH_BUFSIZE];
  bool exist_cgroup = false;

  if (OB_FAIL(get_group_path(group_path, PATH_BUFSIZE, tenant_id, group_id))) {
    LOG_WARN("fail get group path", K(tenant_id), K(ret));
  } else if (OB_FAIL(FileDirectoryUtils::is_exists(group_path, exist_cgroup))) {
    LOG_WARN("fail check file exist", K(group_path), K(ret));
  } else if (!exist_cgroup && OB_FAIL(init_cgroup_dir_(group_path))) {
    LOG_WARN("init tenant cgroup dir failed", K(ret), K(group_path), K(tenant_id));
  } else {
    char task_path[PATH_BUFSIZE];
    char tid_value[VALUE_BUFSIZE];
    snprintf(tid_value, VALUE_BUFSIZE, "%d", tid);
    snprintf(task_path, PATH_BUFSIZE, "%s/tasks", group_path);
    if(OB_FAIL(write_string_to_file_(task_path, tid_value))) {
      LOG_WARN("add tid to cgroup failed", K(ret), K(task_path), K(tid_value), K(tenant_id));
    } else {
      LOG_INFO("add tid to cgroup success", K(task_path), K(tid_value), K(tenant_id));
    }
  }

  return ret;
}


// TODO: 查表，通过 group id 找到 group 名字
int ObCgroupCtrl::get_group_info_by_group_id(const uint64_t tenant_id,
                                             int64_t group_id,
                                             share::ObGroupName &group_name)
{
  int ret = OB_SUCCESS;
  ObResourceMappingRuleManager &rule_mgr = G_RES_MGR.get_mapping_rule_mgr();
  if (OB_FAIL(rule_mgr.get_group_name_by_id(tenant_id, group_id, group_name))) {
    LOG_WARN("fail get group name", K(group_name));
  }
  return ret;
}

int ObCgroupCtrl::remove_thread_from_cgroup(const pid_t tid, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  char task_path[PATH_BUFSIZE];
  char tid_value[VALUE_BUFSIZE];
  // 把该tid加入other_cgroup目录的tasks文件中就会从其它tasks中删除
  snprintf(task_path, PATH_BUFSIZE, "%s/tasks", other_cgroup_);
  snprintf(tid_value, VALUE_BUFSIZE, "%d", tid);
  if(OB_FAIL(write_string_to_file_(task_path, tid_value))) {
    LOG_WARN("remove tid from cgroup failed", K(ret), K(task_path), K(tid_value), K(tenant_id));
  } else {
    LOG_INFO("remove tid from cgroup success", K(task_path), K(tid_value), K(tenant_id));
  }
  return ret;
}

int ObCgroupCtrl::set_cpu_shares(const int32_t cpu_shares, const uint64_t tenant_id, int64_t group_id)
{
  int ret = OB_SUCCESS;
  char group_path[PATH_BUFSIZE];
  bool exist_cgroup = false;
  if (OB_FAIL(get_group_path(group_path, PATH_BUFSIZE, tenant_id, group_id))) {
    LOG_WARN("fail get group path", K(tenant_id), K(ret));
  } else if (OB_FAIL(FileDirectoryUtils::is_exists(group_path, exist_cgroup))) {
    LOG_WARN("fail check file exist", K(group_path), K(ret));
  } else if (!exist_cgroup && OB_FAIL(init_cgroup_dir_(group_path))) {
    LOG_WARN("init tenant cgroup dir failed", K(ret), K(group_path), K(tenant_id));
  } else {
    char cpu_shares_path[PATH_BUFSIZE];
    char cpu_shares_value[VALUE_BUFSIZE];
    snprintf(cpu_shares_path, PATH_BUFSIZE, "%s/cpu.shares", group_path);
    snprintf(cpu_shares_value, VALUE_BUFSIZE, "%d", cpu_shares);
    if(OB_FAIL(write_string_to_file_(cpu_shares_path, cpu_shares_value))) {
      LOG_WARN("set cpu shares failed", K(ret),
              K(cpu_shares_path), K(cpu_shares_value));
    } else {
      LOG_INFO("set cpu shares quota success",
              K(cpu_shares_path), K(cpu_shares_value));
    }
  }
  return ret;
}

int ObCgroupCtrl::set_cpu_shares(const uint64_t tenant_id,
                                 const int level,
                                 const ObString &group,
                                 const int32_t cpu_shares)
{
  UNUSED(level);
  char cgroup_path[PATH_BUFSIZE];
  snprintf(cgroup_path, PATH_BUFSIZE, "%s/tenant_%04lu/%.*s",
           root_cgroup_, tenant_id, group.length(), group.ptr());
  return set_cpu_shares(cgroup_path, cpu_shares);
}

int ObCgroupCtrl::set_cpu_shares(const char *cgroup_path, const int32_t cpu_shares)
{
  int ret = OB_SUCCESS;
  char cpu_shares_path[PATH_BUFSIZE];
  char cpu_shares_value[VALUE_BUFSIZE];
  snprintf(cpu_shares_path, PATH_BUFSIZE, "%s/cpu.shares", cgroup_path);
  snprintf(cpu_shares_value, VALUE_BUFSIZE, "%d", cpu_shares);
  if(OB_FAIL(write_string_to_file_(cpu_shares_path, cpu_shares_value))) {
    LOG_WARN("set cpu shares failed", K(ret),
             K(cpu_shares_path), K(cpu_shares_value));
  } else {
    LOG_INFO("set cpu shares quota success",
             K(cpu_shares_path), K(cpu_shares_value));
  }
  return ret;
}

int ObCgroupCtrl::get_cpu_shares(int32_t &cpu_shares, const uint64_t tenant_id, int64_t group_id)
{
  int ret = OB_SUCCESS;
  char group_path[PATH_BUFSIZE];
  bool exist_cgroup = false;
  if (OB_FAIL(get_group_path(group_path, PATH_BUFSIZE, tenant_id, group_id))) {
    LOG_WARN("fail get group path", K(tenant_id), K(ret));
  } else if (OB_FAIL(FileDirectoryUtils::is_exists(group_path, exist_cgroup))) {
    LOG_WARN("fail check file exist", K(group_path), K(ret));
  } else if (!exist_cgroup && OB_FAIL(init_cgroup_dir_(group_path))) {
    LOG_WARN("init tenant cgroup dir failed", K(ret), K(group_path), K(tenant_id));
  } else {
    char cpu_shares_path[PATH_BUFSIZE];
    char cpu_shares_value[VALUE_BUFSIZE + 1];
    snprintf(cpu_shares_path, PATH_BUFSIZE, "%s/cpu.shares", group_path);
    if(OB_FAIL(get_string_from_file_(cpu_shares_path, cpu_shares_value))) {
      LOG_WARN("get cpu shares failed",
          K(ret), K(cpu_shares_path), K(cpu_shares_value), K(tenant_id));
    } else {
      cpu_shares_value[VALUE_BUFSIZE] = '\0';
      cpu_shares = atoi(cpu_shares_value);
    }
  }
  return ret;
}

int ObCgroupCtrl::set_cpu_cfs_quota(const int32_t cfs_quota_us, const uint64_t tenant_id, int64_t group_id)
{
  int ret = OB_SUCCESS;
  char group_path[PATH_BUFSIZE];
  bool exist_cgroup = false;
  if (OB_FAIL(get_group_path(group_path, PATH_BUFSIZE, tenant_id, group_id))) {
    LOG_WARN("fail get group path", K(tenant_id), K(ret));
  } else if (OB_FAIL(FileDirectoryUtils::is_exists(group_path, exist_cgroup))) {
    LOG_WARN("fail check file exist", K(group_path), K(ret));
  } else if (!exist_cgroup && OB_FAIL(init_cgroup_dir_(group_path))) {
    LOG_WARN("init tenant cgroup dir failed", K(ret), K(group_path), K(tenant_id));
  } else {
    char cfs_path[PATH_BUFSIZE];
    char cfs_value[VALUE_BUFSIZE];
    snprintf(cfs_path, PATH_BUFSIZE, "%s/cpu.cfs_quota_us", group_path);
    snprintf(cfs_value, VALUE_BUFSIZE, "%d", cfs_quota_us);
    if(OB_FAIL(write_string_to_file_(cfs_path, cfs_value))) {
      LOG_WARN("set cpu cfs quota failed", K(ret), K(cfs_path), K(cfs_value), K(tenant_id));
    } else {
      LOG_INFO("set cpu cfs quota success", K(cfs_path), K(cfs_value), K(tenant_id));
    }
  }
  return ret;
}

int ObCgroupCtrl::set_cpu_cfs_quota(const uint64_t tenant_id,
                                    const int level,
                                    const ObString &group,
                                    const int32_t cfs_quota_us)
{
  UNUSED(level);
  int ret = OB_SUCCESS;
  char cfs_path[PATH_BUFSIZE];
  char cfs_value[VALUE_BUFSIZE];
  snprintf(cfs_path, PATH_BUFSIZE, "%s/tenant_%04lu/%.*s/cpu.cfs_quota_us",
           root_cgroup_, tenant_id, group.length(), group.ptr());
  snprintf(cfs_value, VALUE_BUFSIZE, "%d", cfs_quota_us);
  if(OB_FAIL(write_string_to_file_(cfs_path, cfs_value))) {
    LOG_WARN("set cpu cfs quota failed", K(ret), K(cfs_path), K(cfs_value), K(tenant_id));
  } else {
    LOG_INFO("set cpu cfs quota success", K(cfs_path), K(cfs_value), K(tenant_id));
  }
  return ret;
}

int ObCgroupCtrl::get_cpu_cfs_quota(int32_t &cfs_quota_us, const uint64_t tenant_id, int64_t group_id)
{
  int ret = OB_SUCCESS;
  char group_path[PATH_BUFSIZE];
  bool exist_cgroup = false;
  if (OB_FAIL(get_group_path(group_path, PATH_BUFSIZE, tenant_id, group_id))) {
    LOG_WARN("fail get group path", K(tenant_id), K(ret));
  } else if (OB_FAIL(FileDirectoryUtils::is_exists(group_path, exist_cgroup))) {
    LOG_WARN("fail check file exist", K(group_path), K(ret));
  } else if (!exist_cgroup && OB_FAIL(init_cgroup_dir_(group_path))) {
    LOG_WARN("init tenant cgroup dir failed", K(ret), K(group_path), K(tenant_id));
  } else {
    char cfs_path[PATH_BUFSIZE];
    char cfs_value[VALUE_BUFSIZE +1];
    snprintf(cfs_path, PATH_BUFSIZE, "%s/cpu.cfs_quota_us", group_path);
    if(OB_FAIL(get_string_from_file_(cfs_path, cfs_value))) {
      LOG_WARN("get cpu cfs quota failed", K(ret), K(cfs_path), K(cfs_value), K(tenant_id));
    } else {
      cfs_value[VALUE_BUFSIZE] = '\0';
      cfs_quota_us = atoi(cfs_value);
    }
  }
  return ret;
}

// TODO: 这个值可以不变，全局共享一个相同的值，那么就不需要每次都从文件系统读了
int ObCgroupCtrl::get_cpu_cfs_period(const uint64_t tenant_id,
                                     const int level,
                                     const ObString &group,
                                     int32_t &cfs_period_us)
{
  UNUSED(level);
  int ret = OB_SUCCESS;
  char cfs_path[PATH_BUFSIZE];
  char cfs_value[VALUE_BUFSIZE + 1];
  snprintf(cfs_path, PATH_BUFSIZE, "%s/tenant_%04lu/%.*s/cpu.cfs_period_us",
           root_cgroup_, tenant_id, group.length(), group.ptr());
  if(OB_FAIL(get_string_from_file_(cfs_path, cfs_value))) {
    LOG_WARN("get cpu cfs quota failed", K(ret), K(cfs_path), K(cfs_value), K(tenant_id));
  } else {
    cfs_value[VALUE_BUFSIZE] = '\0';
    cfs_period_us = atoi(cfs_value);
  }
  return ret;
}

int ObCgroupCtrl::get_cpu_cfs_period(int32_t &cfs_period_us, const uint64_t tenant_id, int64_t group_id)
{
  int ret = OB_SUCCESS;
  char group_path[PATH_BUFSIZE];
  bool exist_cgroup = false;
  if (OB_FAIL(get_group_path(group_path, PATH_BUFSIZE, tenant_id, group_id))) {
    LOG_WARN("fail get group path", K(tenant_id), K(ret));
  } else if (OB_FAIL(FileDirectoryUtils::is_exists(group_path, exist_cgroup))) {
    LOG_WARN("fail check file exist", K(group_path), K(ret));
  } else if (!exist_cgroup && OB_FAIL(init_cgroup_dir_(group_path))) {
    LOG_WARN("init tenant cgroup dir failed", K(ret), K(group_path), K(tenant_id));
  } else {
    char cfs_path[PATH_BUFSIZE];
    char cfs_value[VALUE_BUFSIZE + 1];
    snprintf(cfs_path, PATH_BUFSIZE, "%s/cpu.cfs_period_us", group_path);
    if(OB_FAIL(get_string_from_file_(cfs_path, cfs_value))) {
      LOG_WARN("get cpu cfs quota failed", K(ret), K(cfs_path), K(cfs_value), K(tenant_id));
    } else {
      cfs_value[VALUE_BUFSIZE] = '\0';
      cfs_period_us = atoi(cfs_value);
    }
  }
  return ret;
}

int ObCgroupCtrl::get_cpu_usage(const uint64_t tenant_id, int32_t &cpu_usage)
{
  int ret = OB_SUCCESS;
  int64_t cur_time = ObTimeUtility::current_time();
  int64_t cur_usage = 0;

  char usage_path[PATH_BUFSIZE];
  char usage_value[VALUE_BUFSIZE + 1];
  snprintf(usage_path, PATH_BUFSIZE, "%s/tenant_%04lu/cpuacct.usage", root_cgroup_, tenant_id);
  if(OB_FAIL(get_string_from_file_(usage_path, usage_value))) {
    LOG_WARN("get cpu usage failed",
        K(ret), K(usage_path), K(usage_value), K(tenant_id));
  } else {
    usage_value[VALUE_BUFSIZE] = '\0';
    cur_usage = std::stoull(usage_value);
  }

  cpu_usage = 0;
  if (0 == last_usage_check_time_) {
    last_cpu_usage_ = cur_usage;
  } else if (cur_time - last_usage_check_time_ > 1000000) {
    cpu_usage = (cur_usage - last_cpu_usage_) / (cur_time - last_usage_check_time_);
  }

  return ret;
}

int ObCgroupCtrl::get_cpu_time(const uint64_t tenant_id, int64_t &cpu_time)
{
  int ret = OB_SUCCESS;

  char usage_path[PATH_BUFSIZE];
  char usage_value[VALUE_BUFSIZE + 1];
  snprintf(usage_path, PATH_BUFSIZE, "%s/tenant_%lu/cpuacct.usage", root_cgroup_, tenant_id);
  MEMSET(usage_value, 0, VALUE_BUFSIZE);
  if(OB_FAIL(get_string_from_file_(usage_path, usage_value))) {
    LOG_WARN("get cpu usage failed",
        K(ret), K(usage_path), K(usage_value), K(tenant_id));
  } else {
    usage_value[VALUE_BUFSIZE] = '\0';
    cpu_time = std::stoull(usage_value) / 1000;
  }
  return ret;
}

int ObCgroupCtrl::init_cgroup_root_dir_(const char *cgroup_path)
{
  int ret = OB_SUCCESS;
  char current_path[PATH_BUFSIZE];
  char value_buf[VALUE_BUFSIZE];
  bool exist_cgroup = false;
  if (OB_ISNULL(cgroup_path)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid arguments.", K(cgroup_path), K(ret));
  } else if (OB_FAIL(FileDirectoryUtils::is_exists(cgroup_path, exist_cgroup))) {
    LOG_WARN("fail check file exist", K(cgroup_path), K(ret));
  } else if (!exist_cgroup) {
    ret = OB_FILE_NOT_EXIST;
    LOG_WARN("no cgroup directory found. disable cgroup support", K(cgroup_path), K(ret));
  } else {
    // 设置 mems 和 cpus
    //  cpu 可能是不连续的，自己探测很复杂。
    //  这里总是继承父级的设置，无需自己探测。
    //  后继 child cgroup 无需再设置 mems 和 cpus
    if (OB_SUCC(ret)) {
      snprintf(current_path, PATH_BUFSIZE, "%s/cgroup.clone_children", cgroup_path);
      snprintf(value_buf, VALUE_BUFSIZE, "1");
      if (OB_FAIL(write_string_to_file_(current_path, value_buf))) {
        LOG_WARN("fail set value to file", K(current_path), K(ret));
      }
    }
  }
  return ret;
}

int ObCgroupCtrl::init_cgroup_dir_(const char *cgroup_path)
{
  int ret = OB_SUCCESS;
  char current_path[PATH_BUFSIZE];
  char value_buf[VALUE_BUFSIZE];
  if (OB_ISNULL(cgroup_path)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid arguments.", K(cgroup_path), K(ret));
  } else if (OB_FAIL(FileDirectoryUtils::create_directory(cgroup_path))) {
    LOG_WARN("create tenant cgroup dir failed", K(ret), K(cgroup_path));
  } else {
    // 设置 mems 和 cpus
    //  cpu 可能是不连续的，自己探测很复杂。
    //  这里总是继承父级的设置，无需自己探测。
   // 后继 child cgroup 无需再设置 mems 和 cpus
    if (OB_SUCC(ret)) {
      snprintf(current_path, PATH_BUFSIZE, "%s/cgroup.clone_children", cgroup_path);
      snprintf(value_buf, VALUE_BUFSIZE, "1");
      if (OB_FAIL(write_string_to_file_(current_path, value_buf))) {
        LOG_WARN("fail set value to file", K(current_path), K(ret));
      }
      LOG_DEBUG("debug print clone", K(current_path), K(value_buf));
    }
  }
  return ret;
}

int ObCgroupCtrl::write_string_to_file_(const char *filename, const char *content)
{
  int ret = OB_SUCCESS;
  int fd = -1;
  int tmp_ret = -1;
  if ((fd = ::open(filename, O_WRONLY)) < 0) {
    ret = OB_IO_ERROR;
    LOG_WARN("open file error", K(filename), K(errno), KERRMSG, K(ret));
  } else if ((tmp_ret = write(fd, content, strlen(content))) < 0) {
    ret = OB_IO_ERROR;
    LOG_WARN("write file error",
        K(filename), K(content), K(ret), K(errno), KERRMSG, K(ret));
  } else {
    // do nothing
  }
  if (fd >= 0 && 0 != close(fd)) {
    ret = OB_IO_ERROR;
    LOG_WARN("close file error",
        K(filename), K(fd), K(errno), KERRMSG, K(ret));
  }
  return ret;
}

int ObCgroupCtrl::get_string_from_file_(const char *filename, char content[VALUE_BUFSIZE])
{
  int ret = OB_SUCCESS;
  int fd = -1;
  int tmp_ret = -1;
  if ((fd = ::open(filename, O_RDONLY)) < 0) {
    ret = OB_IO_ERROR;
    LOG_WARN("open file error", K(filename), K(errno), KERRMSG, K(ret));
  } else if ((tmp_ret = read(fd, content, VALUE_BUFSIZE)) < 0) {
    ret = OB_IO_ERROR;
    LOG_WARN("read file error",
        K(filename), K(content), K(ret), K(errno), KERRMSG, K(ret));
  } else {
    // do nothing
  }
  if (fd >= 0 && 0 != close(fd)) {
    ret = OB_IO_ERROR;
    LOG_WARN("close file error",
        K(filename), K(fd), K(errno), KERRMSG, K(ret));
  }
  return ret;
}
