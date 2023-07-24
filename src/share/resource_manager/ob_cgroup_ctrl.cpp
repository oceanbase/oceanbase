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
#include "share/io/ob_io_manager.h"
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

//集成IO参数
int OBGroupIOInfo::init(int64_t min_percent, int64_t max_percent, int64_t weight_percent)
{
  int ret = OB_SUCCESS;
  if (min_percent < 0 || min_percent > 100 ||
      max_percent < 0 || max_percent > 100 ||
      weight_percent < 0 || weight_percent > 100 ||
      min_percent > max_percent) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("invalid io config", K(ret), K(min_percent), K(max_percent), K(weight_percent));
  } else {
    min_percent_ = min_percent;
    max_percent_ = max_percent;
    weight_percent_ = weight_percent;
  }
  return ret;
}
bool OBGroupIOInfo::is_valid() const
{
  return min_percent_ >= 0 && max_percent_ >= min_percent_ && max_percent_ <= 100 && weight_percent_ >= 0 && weight_percent_ <= 100;
}

// 创建cgroup初始目录结构，将所有线程加入other组
int ObCgroupCtrl::init()
{
  int ret = OB_SUCCESS;
  if (GCONF.enable_cgroup == false) {
    // not init cgroup when config set to false
  } else if (OB_FAIL(init_cgroup_root_dir_(root_cgroup_))) {
	  if (OB_FILE_NOT_EXIST == ret) {
      LOG_WARN("init cgroup dir failed", K(ret), K(root_cgroup_));
    } else {
      LOG_ERROR("init cgroup dir failed", K(ret), K(root_cgroup_));
    }
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

int ObCgroupCtrl::which_type_dir_(const char *curr_path, int &type)
{
  DIR *dir = nullptr;
  struct dirent *subdir = nullptr;
  char sub_path[PATH_BUFSIZE];
  bool tmp_result = false;
  int ret = OB_SUCCESS;
  if (OB_FAIL(FileDirectoryUtils::is_directory(curr_path, tmp_result))) {
    LOG_WARN("judge is directory failed", K(curr_path));
  } else if (false == tmp_result) {
    type = NOT_DIR;
  } else if (NULL == (dir = opendir(curr_path))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("open dir failed", K(curr_path));
  } else {
    type = LEAF_DIR;
    while (OB_SUCCESS == ret && NULL != (subdir = readdir(dir))) {
      tmp_result = false;
      if (0 == strcmp(subdir->d_name, ".") || 0 == strcmp(subdir->d_name, "..")) {
        // skip . and ..
      } else {
        snprintf(sub_path, PATH_BUFSIZE, "%s/%s", curr_path, subdir->d_name);
        if (OB_FAIL(FileDirectoryUtils::is_directory(sub_path, tmp_result))) {
          LOG_WARN("judge is directory failed", K(sub_path));
        } else if (true == tmp_result) {
          type = REGULAR_DIR;
          break;
        }
      }
    }
  }
  return ret;
}

int ObCgroupCtrl::remove_dir_(const char *curr_dir)
{
  int ret = OB_SUCCESS;
  char group_task_path[PATH_BUFSIZE];
  char parent_task_path[PATH_BUFSIZE];
  snprintf(group_task_path, PATH_BUFSIZE, "%s/tasks", curr_dir);
  snprintf(parent_task_path, PATH_BUFSIZE, "%s/../tasks", curr_dir);
  FILE* group_task_file = nullptr;
  if (OB_ISNULL(group_task_file = fopen(group_task_path, "r"))) {
    ret = OB_IO_ERROR;
    LOG_WARN("open group failed", K(ret), K(group_task_path), K(errno), KERRMSG);
  } else {
    char tid_buf[VALUE_BUFSIZE];
    while (fgets(tid_buf, VALUE_BUFSIZE, group_task_file)) {
      if (OB_FAIL(write_string_to_file_(parent_task_path, tid_buf))) {
        LOG_WARN("remove tenant task failed", K(ret), K(parent_task_path));
        break;
      }
    }
    fclose(group_task_file);
  }
  if (OB_SUCC(ret) && OB_FAIL(FileDirectoryUtils::delete_directory(curr_dir))) {
    LOG_WARN("remove group directory failed", K(ret), K(curr_dir));
  } else {
    LOG_INFO("remove group directory success", K(curr_dir));
  }
  return ret;
}

int ObCgroupCtrl::recursion_remove_group_(const char *curr_path)
{
  int ret = OB_SUCCESS;
  int type = NOT_DIR;
  if (OB_FAIL(which_type_dir_(curr_path, type))) {
    LOG_WARN("check dir type failed", K(ret), K(curr_path));
  } else if (NOT_DIR == type) {
    // not directory, skip
  } else if (LEAF_DIR == type) {
    if (OB_FAIL(remove_dir_(curr_path))) {
      LOG_WARN("remove sub group directory failed", K(ret), K(curr_path));
    } else {
      LOG_INFO("remove sub group directory success", K(curr_path));
    }
  } else {
    DIR *dir = nullptr;
    if (NULL == (dir = opendir(curr_path))){
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("open dir failed", K(curr_path));
    } else {
      struct dirent *subdir = nullptr;
      char sub_path[PATH_BUFSIZE];
      while (OB_SUCCESS == ret && (NULL != (subdir = readdir(dir)))) {
        if (0 == strcmp(subdir->d_name, ".") || 0 == strcmp(subdir->d_name, "..")) {
          // skip . and ..
        } else if (PATH_BUFSIZE <= snprintf(sub_path, PATH_BUFSIZE, "%s/%s", curr_path, subdir->d_name)) { // to prevent infinite recursion when path string is over size and cut off
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sub_path is oversize has been cut off", K(ret), K(sub_path), K(curr_path));
        } else if (OB_FAIL(recursion_remove_group_(sub_path))) {
            LOG_WARN("remove path failed", K(sub_path));
        }
      }
      if (OB_FAIL(remove_dir_(curr_path))) {
        LOG_WARN("remove sub group directory failed", K(ret), K(curr_path));
      } else {
        LOG_INFO("remove sub group directory success", K(curr_path));
      }
    }
  }
  return ret;
}

int ObCgroupCtrl::remove_tenant_cgroup(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  char tenant_path[PATH_BUFSIZE];
  if (OB_FAIL(get_group_path(tenant_path, PATH_BUFSIZE, tenant_id))) {
    LOG_WARN("fail get group path", K(tenant_id), K(ret));
  } else if (OB_FAIL(recursion_remove_group_(tenant_path))) {
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
  char tenant_path[PATH_BUFSIZE];
  if (is_meta_tenant(tenant_id)) {
    snprintf(tenant_path, PATH_BUFSIZE, "tenant_%04lu/tenant_%04lu", gen_user_tenant_id(tenant_id), tenant_id);
  } else {
    snprintf(tenant_path, PATH_BUFSIZE, "tenant_%04lu", tenant_id);
  }
  if (INT64_MAX == group_id) {
    snprintf(group_path, path_bufsize, "%s/%s",
              root_cgroup_, tenant_path);
  } else if (group_id < OBCG_MAXNUM) {
    ObCgSet &set = ObCgSet::instance();
    group_name = const_cast<char*>(set.name_of_id(group_id));
    snprintf(group_path, path_bufsize, "%s/%s/%s",
          root_cgroup_, tenant_path, group_name);
  } else if (OB_FAIL(get_group_info_by_group_id(tenant_id, group_id, g_name))){
    LOG_WARN("get group_name by id failed", K(group_id), K(ret));
  } else {
    group_name = const_cast<char*>(g_name.get_value().ptr());
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

int ObCgroupCtrl::add_self_to_cgroup(const uint64_t tenant_id, int64_t group_id)
{
  int ret = OB_SUCCESS;
  char group_path[PATH_BUFSIZE];
  bool exist_cgroup = false;

  if (OB_FAIL(get_group_path(group_path, PATH_BUFSIZE, tenant_id, group_id))) {
    LOG_WARN("fail get group path", K(tenant_id), K(ret), K(group_id));
  } else if (OB_FAIL(FileDirectoryUtils::is_exists(group_path, exist_cgroup))) {
    LOG_WARN("fail check file exist", K(group_path), K(ret));
  } else if (!exist_cgroup && OB_FAIL(init_cgroup_dir_(group_path))) {
    LOG_WARN("init tenant cgroup dir failed", K(ret), K(group_path), K(tenant_id));
  } else {
    char task_path[PATH_BUFSIZE];
    char tid_value[VALUE_BUFSIZE];
    snprintf(tid_value, VALUE_BUFSIZE, "%ld", syscall(__NR_gettid));
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
    LOG_WARN("fail get group name", K(tenant_id), K(group_id), K(group_name));
  }
  return ret;
}

int ObCgroupCtrl::remove_self_from_cgroup(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  char task_path[PATH_BUFSIZE];
  char tid_value[VALUE_BUFSIZE];
  // 把该tid加入other_cgroup目录的tasks文件中就会从其它tasks中删除
  snprintf(task_path, PATH_BUFSIZE, "%s/tasks", other_cgroup_);
  snprintf(tid_value, VALUE_BUFSIZE, "%ld", syscall(__NR_gettid));
  if(OB_FAIL(write_string_to_file_(task_path, tid_value))) {
    LOG_WARN("remove tid from cgroup failed", K(ret), K(task_path), K(tid_value), K(tenant_id));
  } else {
    LOG_INFO("remove tid from cgroup success", K(task_path), K(tid_value), K(tenant_id));
  }
  return ret;
}

int ObCgroupCtrl::add_self_to_group(const uint64_t tenant_id,
                                      const uint64_t group_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("invalid config", K(ret), K(tenant_id));
  } else if (OB_FAIL(add_self_to_cgroup(tenant_id, group_id))) {
    LOG_WARN("fail to add thread to group", K(ret), K(group_id), K(tenant_id));
  } else {
    LOG_INFO("set backup pid to group success", K(tenant_id), K(group_id));
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
    cpu_usage = static_cast<int32_t>(cur_usage - last_cpu_usage_) / (cur_time - last_usage_check_time_);
  }

  return ret;
}

int ObCgroupCtrl::get_cpu_time(const uint64_t tenant_id, int64_t &cpu_time)
{
  int ret = OB_SUCCESS;
  char tenant_path[PATH_BUFSIZE];
  if (OB_FAIL(get_group_path(tenant_path, PATH_BUFSIZE, tenant_id))) {
    LOG_WARN("fail get group path", K(tenant_id), K(ret));
  } else {
    char usage_path[PATH_BUFSIZE];
    char usage_value[VALUE_BUFSIZE + 1];
    snprintf(usage_path, PATH_BUFSIZE, "%s/cpuacct.usage", tenant_path);
    if(OB_FAIL(get_string_from_file_(usage_path, usage_value))) {
      LOG_WARN("get cpu usage failed",
          K(ret), K(usage_path), K(usage_value), K(tenant_id));
    } else {
      usage_value[VALUE_BUFSIZE] = '\0';
      cpu_time = std::stoull(usage_value) / 1000;
    }
  }
  return ret;
}

int ObCgroupCtrl::set_group_iops(const uint64_t tenant_id,
                                 const int level, // UNUSED
                                 const int64_t group_id,
                                 const OBGroupIOInfo &group_io)
{
  int ret = OB_SUCCESS;
  UNUSED(level);

  ObRefHolder<ObTenantIOManager> tenant_holder;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("invalid config", K(ret), K(tenant_id));
  } else if (OB_FAIL(OB_IO_MANAGER.get_tenant_io_manager(tenant_id, tenant_holder))) {
    LOG_WARN("get tenant io manager failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_holder.get_ptr()->modify_io_config(group_id,
                                                               group_io.min_percent_,
                                                               group_io.max_percent_,
                                                               group_io.weight_percent_))) {
    LOG_WARN("modify consumer group iops failed", K(ret), K(group_id), K(tenant_id), K(group_id));
  }
  return ret;
}

int ObCgroupCtrl::reset_all_group_iops(const uint64_t tenant_id,
                                       const int level) // UNUSED
{
  int ret = OB_SUCCESS;
  UNUSED(level);

  ObRefHolder<ObTenantIOManager> tenant_holder;
  // 删除plan, IO层代表对应的所有group资源为0,0,0, 但group对应的数据结构不会被释放以防用户后续复用
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("invalid config", K(ret), K(tenant_id));
  } else if (OB_FAIL(OB_IO_MANAGER.get_tenant_io_manager(tenant_id, tenant_holder))) {
    LOG_WARN("get tenant io manager failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_holder.get_ptr()->reset_all_group_config())) {
    LOG_WARN("reset consumer group iops failed", K(ret), K(tenant_id));
  } else {
    LOG_INFO("group iops control has reset, delete cur plan success", K(tenant_id));
  }
  return ret;
}

int ObCgroupCtrl::reset_group_iops(const uint64_t tenant_id,
                                   const int level, // UNUSED
                                   const common::ObString &consumer_group)
{
  int ret = OB_SUCCESS;
  UNUSED(level);

  uint64_t group_id = 0;
  share::ObGroupName group_name;
  group_name.set_value(consumer_group);
  ObResourceMappingRuleManager &rule_mgr = G_RES_MGR.get_mapping_rule_mgr();
  ObRefHolder<ObTenantIOManager> tenant_holder;

  // 删除directive, IO层代表对应的group资源为0,0,0, 但group对应的数据结构不会被释放以防用户后续复用
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("invalid config", K(ret), K(tenant_id));
  } else if (OB_FAIL(rule_mgr.get_group_id_by_name(tenant_id, group_name, group_id))) {
    if (OB_HASH_NOT_EXIST == ret) {
      //创建directive后立刻删除，可能还没有被刷到存储层或plan未生效，此时不再进行后续操作
      ret = OB_SUCCESS;
      LOG_INFO("delete directive success with no_releated_io_module", K(consumer_group), K(tenant_id));
    } else {
      LOG_WARN("fail get group id", K(ret), K(group_id), K(group_name));
    }
  } else if (OB_UNLIKELY(!is_user_group(group_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid group id", K(ret), K(group_id), K(group_name));
  } else if (OB_FAIL(OB_IO_MANAGER.get_tenant_io_manager(tenant_id, tenant_holder))) {
    LOG_WARN("get tenant io manager failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_holder.get_ptr()->reset_consumer_group_config(group_id))) {
    LOG_WARN("reset consumer group iops failed", K(ret), K(group_id), K(tenant_id), K(group_id));
  } else {
    LOG_INFO("group iops control has reset, delete directive success", K(consumer_group), K(tenant_id), K(group_id));
  }
  return ret;
}

int ObCgroupCtrl::delete_group_iops(const uint64_t tenant_id,
                                    const int level, // UNUSED
                                    const common::ObString &consumer_group)
{
  int ret = OB_SUCCESS;
  UNUSED(level);

  uint64_t group_id = 0;
  share::ObGroupName group_name;
  group_name.set_value(consumer_group);
  ObResourceMappingRuleManager &rule_mgr = G_RES_MGR.get_mapping_rule_mgr();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("invalid config", K(ret), K(tenant_id));
  } else if (OB_FAIL(rule_mgr.get_group_id_by_name(tenant_id, group_name, group_id))) {
    if (OB_HASH_NOT_EXIST == ret) {
      //创建group后立刻删除，可能还没有被刷到存储层或plan未生效，此时不再进行后续操作
      ret = OB_SUCCESS;
      LOG_INFO("delete group success with no_releated_io_module", K(consumer_group), K(tenant_id));
    } else {
      LOG_WARN("fail get group id", K(ret), K(group_id), K(group_name));
    }
  } else if (OB_UNLIKELY(!is_user_group(group_id))) {
    //OTHER_GROUPS and all cannot be deleted
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid group id", K(ret), K(group_id), K(group_name));
  } else {
    ObRefHolder<ObTenantIOManager> tenant_holder;
    if (OB_FAIL(OB_IO_MANAGER.get_tenant_io_manager(tenant_id, tenant_holder))) {
      LOG_WARN("get tenant io manager failed", K(ret), K(tenant_id));
    } else if (OB_FAIL(tenant_holder.get_ptr()->delete_consumer_group_config(group_id))) {
      LOG_WARN("stop group iops control failed", K(ret), K(tenant_id), K(group_id));
    } else {
      LOG_INFO("stop group iops_ctrl success when delete group", K(consumer_group), K(tenant_id), K(group_id));
    }
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
  int64_t write_size = -1;
  if ((fd = ::open(filename, O_WRONLY)) < 0) {
    ret = OB_IO_ERROR;
    LOG_WARN("open file error", K(filename), K(errno), KERRMSG, K(ret));
  } else if ((write_size = write(fd, content, static_cast<int32_t>(strlen(content)))) < 0) {
    ret = OB_IO_ERROR;
    LOG_WARN("write file error",
        K(filename), K(content), K(ret), K(errno), KERRMSG);
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
  int64_t read_size = -1;
  if ((fd = ::open(filename, O_RDONLY)) < 0) {
    ret = OB_IO_ERROR;
    LOG_WARN("open file error", K(filename), K(errno), KERRMSG, K(ret));
  } else if ((read_size = read(fd, content, VALUE_BUFSIZE)) < 0) {
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
