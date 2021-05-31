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

#include "ob_cgroup_ctrl.h"
#include "lib/file/file_directory_utils.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/utility.h"
#include "lib/oblog/ob_log.h"
#include "share/ob_errno.h"
#include "share/config/ob_server_config.h"
#include "share/resource_manager/ob_resource_plan_info.h"
#include "share/resource_manager/ob_resource_manager.h"

#include <stdlib.h>
#include <stdio.h>

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::omt;

// Create the initial directory structure of the cgroup and add all threads to the other group
int ObCgroupCtrl::init()
{
  int ret = OB_SUCCESS;
  // In order to separate the resources of the system from the resources of the user at a macro level,
  // the top layer is divided into two parts: sys and user
  // cgroup ---  sys
  //         |    |--- other
  //         |    |--- tenant_1
  //         |    |--- tenant_2
  //         |    |--- ....
  //         |    |--- tenant_999
  //         |
  //         |_  user
  //              |--- tenant_1001
  //              |--- tenant_1002
  //              |___ ...
  //
  // 1. init observer root cgroup
  if (OB_FAIL(init_cgroup_root_dir_(root_cgroup_))) {
    LOG_WARN("init cgroup dir failed", K(ret), K(root_cgroup_));
    // 2. create sys cgroup shares default value is DEFAULT_SYS_SHARE
  } else if (OB_FAIL(init_cgroup_dir_(sys_cgroup_))) {
    LOG_WARN("init tenants cgroup dir failed", K(ret), K_(sys_cgroup));
  } else if (OB_FAIL(set_cpu_shares(sys_cgroup_, DEFAULT_SYS_SHARE))) {
    LOG_WARN("fail set sys cgroup share", K(ret));
    // 3. create tenant user cgroup
  } else if (OB_FAIL(init_cgroup_dir_(user_cgroup_))) {
    LOG_WARN("init tenants cgroup dir failed", K(ret), K_(user_cgroup));
  } else if (OB_FAIL(set_cpu_shares(user_cgroup_, DEFAULT_USER_SHARE))) {
    LOG_WARN("fail set user cgroup share", K(ret));
    // 4. Create cgroups other than tenant threads
  } else if (OB_FAIL(init_cgroup_dir_(other_cgroup_))) {
    LOG_WARN("init other cgroup dir failed", K(ret), K_(other_cgroup));
    // 5. Add all threads to other cgroup
  } else {
    char procs_path[PATH_BUFSIZE];
    char pid_value[VALUE_BUFSIZE];
    snprintf(procs_path, PATH_BUFSIZE, "%s/cgroup.procs", other_cgroup_);
    snprintf(pid_value, VALUE_BUFSIZE, "%d", getpid());
    if (OB_FAIL(write_string_to_file_(procs_path, pid_value))) {
      LOG_WARN("add tid to cgroup failed", K(ret), K(procs_path), K(pid_value));
    } else {
      valid_ = true;
    }
  }

  return ret;
}

int ObCgroupCtrl::create_tenant_cgroup(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  char tenant_cg_dir[PATH_BUFSIZE];
  // System tenants join the sys group, and user tenants join the user cgroup
  // to separate resources from a macro perspective
  const char* top_cgroup = (tenant_id <= OB_MAX_RESERVED_TENANT_ID ? sys_cgroup_ : user_cgroup_);
  snprintf(tenant_cg_dir, PATH_BUFSIZE, "%s/tenant_%lu", top_cgroup, tenant_id);
  if (OB_FAIL(init_cgroup_dir_(tenant_cg_dir))) {
    LOG_WARN("init tenant cgroup dir failed", K(ret), K(tenant_cg_dir), K(tenant_id));
  } else {
    LOG_INFO("init tenant cgroup dir success", K(tenant_cg_dir), K(tenant_id));
    // For tenants, the following layout is established by default:
    // The user can later change the default value through the PL package and add a new GROUP
    // |_tenant_10xx
    // |     |
    // |     |_OTHER_GROUPS
    // |     |   |_..... 100% cpu
    // |     |_SYS_GROUP
    // |         |_..... 0% cpu (actually 2%, impl. by OS kernel)
    if (OB_FAIL(init_default_user_tenant_group(tenant_id, 1, "OTHER_GROUPS", 100))) {
      LOG_WARN("fail init user tenant group", K(tenant_id));
    } else if (OB_FAIL(init_default_user_tenant_group(tenant_id, 1, "SYS_GROUP", 0))) {
      LOG_WARN("fail init user tenant group", K(tenant_id));
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
  const char* top_cgroup = (tenant_id <= OB_MAX_RESERVED_TENANT_ID ? sys_cgroup_ : user_cgroup_);
  snprintf(tenant_path, PATH_BUFSIZE, "%s/tenant_%lu", top_cgroup, tenant_id);
  snprintf(tenant_task_path, PATH_BUFSIZE, "%s/tenant_%lu/tasks", top_cgroup, tenant_id);
  snprintf(other_task_path, PATH_BUFSIZE, "%s/tasks", other_cgroup_);
  FILE* tenant_task_file = nullptr;
  int other_task_fd = -1;
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
    char* task_path, int path_bufsize, const uint64_t tenant_id, int level, const char* group)
{
  int ret = OB_SUCCESS;
  const char* top_cgroup = (tenant_id <= OB_MAX_RESERVED_TENANT_ID ? sys_cgroup_ : user_cgroup_);
  switch (level) {
    case 0:
      // Only used to support 500-999 tenants
      snprintf(task_path, path_bufsize, "%s/tenant_%lu/tasks", top_cgroup, tenant_id);
      break;
    case 1:
      snprintf(task_path, path_bufsize, "%s/tenant_%lu/%s/tasks", top_cgroup, tenant_id, group);
      break;
    case 2:
      snprintf(task_path, path_bufsize, "%s/tenant_%lu/%s/tasks", top_cgroup, tenant_id, group);
      break;
    // case 2:
    //   snprintf(task_path, path_bufsize, "%s/tenant_%lu/p2/%s/tasks",
    //            top_cgroup, tenant_id, group);
    //   break;
    // case 3:
    //  snprintf(task_path, path_bufsize, "%s/tenant_%lu/p2/p3/%s/tasks",
    //           top_cgroup, tenant_id, group);
    //  break;
    default:
      ret = OB_ERR_UNEXPECTED;
      break;
  }
  return ret;
}

int ObCgroupCtrl::get_task_path(
    char* task_path, int path_bufsize, const uint64_t tenant_id, int level, const ObString& group)
{
  int ret = OB_SUCCESS;
  const char* top_cgroup = (tenant_id <= OB_MAX_RESERVED_TENANT_ID ? sys_cgroup_ : user_cgroup_);
  switch (level) {
    case 0:
      // Only used to support 500-999 tenants
      snprintf(task_path, path_bufsize, "%s/tenant_%lu/tasks", top_cgroup, tenant_id);
      break;
    case 1:
      snprintf(task_path, path_bufsize, "%s/tenant_%lu/%.*s/tasks", top_cgroup, tenant_id, group.length(), group.ptr());
      break;
    case 2:
      snprintf(task_path, path_bufsize, "%s/tenant_%lu/%.*s/tasks", top_cgroup, tenant_id, group.length(), group.ptr());
      break;
    // case 2:
    //   snprintf(task_path, path_bufsize, "%s/tenant_%lu/p2/%s/tasks",
    //            top_cgroup, tenant_id, group);
    //   break;
    // case 3:
    //  snprintf(task_path, path_bufsize, "%s/tenant_%lu/p2/p3/%s/tasks",
    //           top_cgroup, tenant_id, group);
    //  break;
    default:
      ret = OB_ERR_UNEXPECTED;
      break;
  }
  return ret;
}

int ObCgroupCtrl::get_group_path(
    char* group_path, int path_bufsize, const uint64_t tenant_id, int level, const char* group)
{
  int ret = OB_SUCCESS;
  const char* top_cgroup = (tenant_id <= OB_MAX_RESERVED_TENANT_ID ? sys_cgroup_ : user_cgroup_);
  switch (level) {
    case 1:
      snprintf(group_path, path_bufsize, "%s/tenant_%lu/%s", top_cgroup, tenant_id, group);
      break;
    case 2:
      snprintf(group_path, path_bufsize, "%s/tenant_%lu/%s", top_cgroup, tenant_id, group);
      break;
    // case 2:
    //  snprintf(group_path, path_bufsize, "%s/tenant_%lu/p2/%s",
    //           top_cgroup, tenant_id, group);
    //  break;
    default:
      ret = OB_ERR_UNEXPECTED;
      break;
  }
  return ret;
}

int ObCgroupCtrl::get_group_path(
    char* group_path, int path_bufsize, const uint64_t tenant_id, int level, const common::ObString& group)
{
  int ret = OB_SUCCESS;
  const char* top_cgroup = (tenant_id <= OB_MAX_RESERVED_TENANT_ID ? sys_cgroup_ : user_cgroup_);
  switch (level) {
    case 1:
      snprintf(group_path, path_bufsize, "%s/tenant_%lu/%.*s", top_cgroup, tenant_id, group.length(), group.ptr());
      break;
    case 2:
      snprintf(group_path, path_bufsize, "%s/tenant_%lu/%.*s", top_cgroup, tenant_id, group.length(), group.ptr());
      break;
    // case 2:
    //  snprintf(group_path, path_bufsize, "%s/tenant_%lu/p2/%s",
    //           top_cgroup, tenant_id, group);
    //  break;
    default:
      ret = OB_ERR_UNEXPECTED;
      break;
  }
  return ret;
}

int ObCgroupCtrl::create_user_tenant_group_dir(const uint64_t tenant_id, int level, const common::ObString& group)
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
    // note: support concurrent creation of the same directory, all return OB_SUCCESS
    LOG_WARN("init tenant cgroup dir failed", K(ret), K(tenant_cg_dir), K(tenant_id));
  }
  return ret;
}

int ObCgroupCtrl::init_default_user_tenant_group(
    const uint64_t tenant_id, int level, const char* group, int32_t cpu_shares)
{
  int ret = OB_SUCCESS;
  char tenant_cg_dir[PATH_BUFSIZE];
  bool exist_cgroup = false;
  if (OB_ISNULL(group) || level < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid args", KP(group), K(level));
  } else if (OB_FAIL(get_group_path(tenant_cg_dir, PATH_BUFSIZE, tenant_id, level, group))) {
    LOG_WARN("fail get group path", K(tenant_id), K(ret));
  } else if (OB_FAIL(FileDirectoryUtils::is_exists(tenant_cg_dir, exist_cgroup))) {
    LOG_WARN("fail check file exist", K(tenant_cg_dir), K(ret));
  } else if (!exist_cgroup && OB_FAIL(init_cgroup_dir_(tenant_cg_dir))) {
    // note: support concurrent creation of the same directory, all return OB_SUCCESS
    LOG_WARN("init tenant cgroup dir failed", K(ret), K(tenant_cg_dir), K(tenant_id));
  } else if (OB_FAIL(set_cpu_shares(tenant_cg_dir, cpu_shares))) {
    LOG_WARN("fail set cpu shares", K(tenant_cg_dir), K(cpu_shares), K(ret));
  }
  return ret;
}

int ObCgroupCtrl::add_thread_to_cgroup(const uint64_t tenant_id, int level, const char* group, const pid_t tid)
{
  int ret = OB_SUCCESS;
  char task_path[PATH_BUFSIZE];
  char tid_value[VALUE_BUFSIZE];
  snprintf(tid_value, VALUE_BUFSIZE, "%d", tid);
  if (OB_FAIL(get_task_path(task_path, PATH_BUFSIZE, tenant_id, level, group))) {
    LOG_WARN("fail get task path", K(ret));
  } else if (OB_FAIL(write_string_to_file_(task_path, tid_value))) {
    LOG_WARN("add tid to cgroup failed", K(ret), K(task_path), K(tid_value), K(tenant_id));
  } else {
    LOG_INFO("add tid to cgroup success", K(task_path), K(tid_value), K(tenant_id));
  }
  return ret;
}

int ObCgroupCtrl::add_thread_to_cgroup(const uint64_t tenant_id, int64_t group_id, const pid_t tid)
{
  int ret = OB_SUCCESS;
  int level = 1;
  char task_path[PATH_BUFSIZE];
  char tid_value[VALUE_BUFSIZE];
  share::ObGroupName group_name;
  snprintf(tid_value, VALUE_BUFSIZE, "%d", tid);
  if (OB_FAIL(get_group_info_by_group_id(tenant_id, group_id, group_name, level))) {
    LOG_WARN("fail get group name via id", K(group_id), K(tenant_id), K(ret));
  } else if (OB_FAIL(get_task_path(task_path, PATH_BUFSIZE, tenant_id, level, group_name))) {
    LOG_WARN("fail get task path", K(ret));
  } else if (OB_FAIL(write_string_to_file_(task_path, tid_value))) {
    LOG_WARN("add tid to cgroup failed", K(ret), K(task_path), K(tid_value), K(tenant_id));
  } else {
    LOG_INFO("add tid to cgroup success", K(task_path), K(tid_value), K(tenant_id));
  }
  return ret;
}

// TODO: Look up the table and find the group name through the group id
int ObCgroupCtrl::get_group_info_by_group_id(
    const uint64_t tenant_id, int64_t group_id, share::ObGroupName& group_name, int& level)
{
  int ret = OB_SUCCESS;
  ObResourceMappingRuleManager& rule_mgr = G_RES_MGR.get_mapping_rule_mgr();
  if (OB_FAIL(rule_mgr.get_group_name_by_id(tenant_id, group_id, group_name))) {
    LOG_WARN("fail get group name", K(group_name));
  } else {
    // TODO: level?
    level = 1;
  }
  return ret;
}

int ObCgroupCtrl::add_thread_to_cgroup(const uint64_t tenant_id, const pid_t tid)
{
  int ret = OB_SUCCESS;
  int level = 1;
  const char* group = "OTHER_GROUPS";
  if (OB_FAIL(add_thread_to_cgroup(tenant_id, level, group, tid))) {
    LOG_WARN("fail add tid to default cgroup", K(tenant_id), K(level), K(group), K(tid), K(ret));
  }
  return ret;
}

int ObCgroupCtrl::remove_thread_from_cgroup(const uint64_t tenant_id, const pid_t tid)
{
  int ret = OB_SUCCESS;
  char task_path[PATH_BUFSIZE];
  char tid_value[VALUE_BUFSIZE];
  // Adding the tid to the tasks file in the other_cgroup directory and delete it from other tasks
  snprintf(task_path, PATH_BUFSIZE, "%s/tasks", other_cgroup_);
  snprintf(tid_value, VALUE_BUFSIZE, "%d", tid);
  if (OB_FAIL(write_string_to_file_(task_path, tid_value))) {
    LOG_WARN("remove tid from cgroup failed", K(ret), K(task_path), K(tid_value), K(tenant_id));
  } else {
    LOG_INFO("remove tid from cgroup success", K(task_path), K(tid_value), K(tenant_id));
  }
  return ret;
}

int ObCgroupCtrl::set_cpu_shares(const uint64_t tenant_id, const int32_t cpu_shares)
{
  int ret = OB_SUCCESS;
  char cgroup_path[PATH_BUFSIZE];
  const char* top_cgroup = (tenant_id <= OB_MAX_RESERVED_TENANT_ID ? sys_cgroup_ : user_cgroup_);
  snprintf(cgroup_path, PATH_BUFSIZE, "%s/tenant_%lu", top_cgroup, tenant_id);
  return set_cpu_shares(cgroup_path, cpu_shares);
}

int ObCgroupCtrl::set_cpu_shares(
    const uint64_t tenant_id, const int level, const ObString& group, const int32_t cpu_shares)
{
  UNUSED(level);
  int ret = OB_SUCCESS;
  char cgroup_path[PATH_BUFSIZE];
  const char* top_cgroup = (tenant_id <= OB_MAX_RESERVED_TENANT_ID ? sys_cgroup_ : user_cgroup_);
  snprintf(cgroup_path, PATH_BUFSIZE, "%s/tenant_%lu/%.*s", top_cgroup, tenant_id, group.length(), group.ptr());
  return set_cpu_shares(cgroup_path, cpu_shares);
}

int ObCgroupCtrl::set_cpu_shares(const char* cgroup_path, const int32_t cpu_shares)
{
  int ret = OB_SUCCESS;
  char cpu_shares_path[PATH_BUFSIZE];
  char cpu_shares_value[VALUE_BUFSIZE];
  snprintf(cpu_shares_path, PATH_BUFSIZE, "%s/cpu.shares", cgroup_path);
  snprintf(cpu_shares_value, VALUE_BUFSIZE, "%d", cpu_shares);
  if (OB_FAIL(write_string_to_file_(cpu_shares_path, cpu_shares_value))) {
    LOG_WARN("set cpu shares failed", K(ret), K(cpu_shares_path), K(cpu_shares_value));
  } else {
    LOG_INFO("set cpu shares quota success", K(cpu_shares_path), K(cpu_shares_value));
  }
  return ret;
}

int ObCgroupCtrl::get_cpu_shares(const uint64_t tenant_id, int32_t& cpu_shares)
{
  int ret = OB_SUCCESS;
  char cpu_shares_path[PATH_BUFSIZE];
  char cpu_shares_value[VALUE_BUFSIZE];
  const char* top_cgroup = (tenant_id <= OB_MAX_RESERVED_TENANT_ID ? sys_cgroup_ : user_cgroup_);
  snprintf(cpu_shares_path, PATH_BUFSIZE, "%s/tenant_%lu/cpu.shares", top_cgroup, tenant_id);
  if (OB_FAIL(get_string_from_file_(cpu_shares_path, cpu_shares_value))) {
    LOG_WARN("get cpu shares failed", K(ret), K(cpu_shares_path), K(cpu_shares_value), K(tenant_id));
  } else {
    cpu_shares = atoi(cpu_shares_value);
  }
  return ret;
}

int ObCgroupCtrl::set_cpu_cfs_quota(const uint64_t tenant_id, const int32_t cfs_quota_us)
{
  int ret = OB_SUCCESS;
  char cfs_path[PATH_BUFSIZE];
  char cfs_value[VALUE_BUFSIZE];
  const char* top_cgroup = (tenant_id <= OB_MAX_RESERVED_TENANT_ID ? sys_cgroup_ : user_cgroup_);
  snprintf(cfs_path, PATH_BUFSIZE, "%s/tenant_%lu/cpu.cfs_quota_us", top_cgroup, tenant_id);
  snprintf(cfs_value, VALUE_BUFSIZE, "%d", cfs_quota_us);
  if (OB_FAIL(write_string_to_file_(cfs_path, cfs_value))) {
    LOG_WARN("set cpu cfs quota failed", K(ret), K(cfs_path), K(cfs_value), K(tenant_id));
  } else {
    LOG_INFO("set cpu cfs quota success", K(cfs_path), K(cfs_value), K(tenant_id));
  }
  return ret;
}

int ObCgroupCtrl::set_cpu_cfs_quota(
    const uint64_t tenant_id, const int level, const ObString& group, const int32_t cfs_quota_us)
{
  UNUSED(level);
  int ret = OB_SUCCESS;
  char cfs_path[PATH_BUFSIZE];
  char cfs_value[VALUE_BUFSIZE];
  const char* top_cgroup = (tenant_id <= OB_MAX_RESERVED_TENANT_ID ? sys_cgroup_ : user_cgroup_);
  snprintf(cfs_path,
      PATH_BUFSIZE,
      "%s/tenant_%lu/%.*s/cpu.cfs_quota_us",
      top_cgroup,
      tenant_id,
      group.length(),
      group.ptr());
  snprintf(cfs_value, VALUE_BUFSIZE, "%d", cfs_quota_us);
  if (OB_FAIL(write_string_to_file_(cfs_path, cfs_value))) {
    LOG_WARN("set cpu cfs quota failed", K(ret), K(cfs_path), K(cfs_value), K(tenant_id));
  } else {
    LOG_INFO("set cpu cfs quota success", K(cfs_path), K(cfs_value), K(tenant_id));
  }
  return ret;
}

int ObCgroupCtrl::get_cpu_cfs_quota(const uint64_t tenant_id, int32_t& cfs_quota_us)
{
  int ret = OB_SUCCESS;
  char cfs_path[PATH_BUFSIZE];
  char cfs_value[VALUE_BUFSIZE];
  const char* top_cgroup = (tenant_id <= OB_MAX_RESERVED_TENANT_ID ? sys_cgroup_ : user_cgroup_);
  snprintf(cfs_path, PATH_BUFSIZE, "%s/tenant_%lu/cpu.cfs_quota_us", top_cgroup, tenant_id);
  if (OB_FAIL(get_string_from_file_(cfs_path, cfs_value))) {
    LOG_WARN("get cpu cfs quota failed", K(ret), K(cfs_path), K(cfs_value), K(tenant_id));
  } else {
    cfs_quota_us = atoi(cfs_value);
  }
  return ret;
}

int ObCgroupCtrl::get_cpu_cfs_period(
    const uint64_t tenant_id, const int level, const ObString& group, int32_t& cfs_period_us)
{
  UNUSED(level);
  int ret = OB_SUCCESS;
  char cfs_path[PATH_BUFSIZE];
  char cfs_value[VALUE_BUFSIZE];
  const char* top_cgroup = (tenant_id <= OB_MAX_RESERVED_TENANT_ID ? sys_cgroup_ : user_cgroup_);
  snprintf(cfs_path,
      PATH_BUFSIZE,
      "%s/tenant_%lu/%.*s/cpu.cfs_period_us",
      top_cgroup,
      tenant_id,
      group.length(),
      group.ptr());
  if (OB_FAIL(get_string_from_file_(cfs_path, cfs_value))) {
    LOG_WARN("get cpu cfs quota failed", K(ret), K(cfs_path), K(cfs_value), K(tenant_id));
  } else {
    cfs_period_us = atoi(cfs_value);
  }
  return ret;
}

int ObCgroupCtrl::get_cpu_usage(const uint64_t tenant_id, int32_t& cpu_usage)
{
  int ret = OB_SUCCESS;
  int64_t cur_time = ObTimeUtility::current_time();
  int64_t cur_usage = 0;

  char usage_path[PATH_BUFSIZE];
  char usage_value[VALUE_BUFSIZE];
  const char* top_cgroup = (tenant_id <= OB_MAX_RESERVED_TENANT_ID ? sys_cgroup_ : user_cgroup_);
  snprintf(usage_path, PATH_BUFSIZE, "%s/tenant_%lu/cpuacct.usage", top_cgroup, tenant_id);
  if (OB_FAIL(get_string_from_file_(usage_path, usage_value))) {
    LOG_WARN("get cpu usage failed", K(ret), K(usage_path), K(usage_value), K(tenant_id));
  } else {
    cur_usage = atoi(usage_value);
  }

  cpu_usage = 0;
  if (0 == last_usage_check_time_) {
    last_cpu_usage_ = cur_usage;
  } else if (cur_time - last_usage_check_time_ > 1000000) {
    cpu_usage = (cur_usage - last_cpu_usage_) / (cur_time - last_usage_check_time_);
  }

  return ret;
}

int ObCgroupCtrl::init_cgroup_root_dir_(const char* cgroup_path)
{
  int ret = OB_SUCCESS;
  bool result = false;
  char current_path[PATH_BUFSIZE];
  char parent_path[PATH_BUFSIZE];
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
    // set mems and cpus
    //  The cpu may be discontinuous, and self-detection is very complicated.
    //  Here always inherit the parent's settings, no need to detect by yourself
    //  Subsequent child cgroups do not need to set mems and cpus
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

int ObCgroupCtrl::init_cgroup_dir_(const char* cgroup_path)
{
  int ret = OB_SUCCESS;
  bool result = false;
  char current_path[PATH_BUFSIZE];
  char parent_path[PATH_BUFSIZE];
  char value_buf[VALUE_BUFSIZE];
  if (OB_ISNULL(cgroup_path)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid arguments.", K(cgroup_path), K(ret));
  } else if (OB_FAIL(FileDirectoryUtils::create_directory(cgroup_path))) {
    LOG_WARN("create tenant cgroup dir failed", K(ret), K(cgroup_path));
  } else {
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

int ObCgroupCtrl::write_string_to_file_(const char* filename, const char* content)
{
  int ret = OB_SUCCESS;
  int fd = -1;
  int tmp_ret = -1;
  if ((fd = ::open(filename, O_WRONLY)) < 0) {
    ret = OB_IO_ERROR;
    LOG_WARN("open file error", K(filename), K(errno), KERRMSG, K(ret));
  } else if ((tmp_ret = write(fd, content, strlen(content))) < 0) {
    ret = OB_IO_ERROR;
    LOG_WARN("write file error", K(filename), K(content), K(ret), K(errno), KERRMSG, K(ret));
  } else {
    // do nothing
  }
  if (fd > 0 && 0 != close(fd)) {
    ret = OB_IO_ERROR;
    LOG_WARN("close file error", K(filename), K(fd), K(errno), KERRMSG, K(ret));
  }
  return ret;
}

int ObCgroupCtrl::get_string_from_file_(const char* filename, char content[VALUE_BUFSIZE])
{
  int ret = OB_SUCCESS;
  int fd = -1;
  int tmp_ret = -1;
  if ((fd = ::open(filename, O_RDONLY)) < 0) {
    ret = OB_IO_ERROR;
    LOG_WARN("open file error", K(filename), K(errno), KERRMSG, K(ret));
  } else if ((tmp_ret = read(fd, content, VALUE_BUFSIZE)) < 0) {
    ret = OB_IO_ERROR;
    LOG_WARN("read file error", K(filename), K(content), K(ret), K(errno), KERRMSG, K(ret));
  } else {
    // do nothing
  }
  if (fd > 0 && 0 != close(fd)) {
    ret = OB_IO_ERROR;
    LOG_WARN("close file error", K(filename), K(fd), K(errno), KERRMSG, K(ret));
  }
  return ret;
}
