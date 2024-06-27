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

// cgroup config name
static const char *CPU_SHARES_FILE = "cpu.shares";
static const int32_t DEFAULT_CPU_SHARES = 1024;
static const char *TASKS_FILE = "tasks";
static const char *CPU_CFS_QUOTA_FILE = "cpu.cfs_quota_us";
static const char *CPU_CFS_PERIOD_FILE = "cpu.cfs_period_us";
static const char *CPUACCT_USAGE_FILE = "cpuacct.usage";
static const char *CPU_STAT_FILE = "cpu.stat";

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

bool ObCgroupCtrl::is_valid_group_name(ObString &group_name)
{
  bool bool_ret = true;
  const char *group_name_ptr = group_name.ptr();
  if (group_name.length() < 0 || group_name.length() > GROUP_NAME_BUFSIZE) {
    bool_ret = false;
  } else {
    for(int i = 0; i < group_name.length() && bool_ret; i++) {
      // only support [a-zA-Z0-9_]
      if (!((group_name_ptr[i] >= 'a' && group_name_ptr[i] <= 'z') ||
            (group_name_ptr[i] >= 'A' && group_name_ptr[i] <= 'Z') ||
            (group_name_ptr[i] >= '0' && group_name_ptr[i] <= '9') ||
            (group_name_ptr[i] == '_'))) {
        bool_ret = false;
      }
    }
  }
  return bool_ret;
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
  } else if (OB_FAIL(init_dir_(other_cgroup_))) {
    LOG_WARN("init other cgroup dir failed", K(ret), K_(other_cgroup));
  } else {
    char procs_path[PATH_BUFSIZE];
    char pid_value[VALUE_BUFSIZE];
    snprintf(procs_path, PATH_BUFSIZE, "%s/cgroup.procs", other_cgroup_);
    snprintf(pid_value, VALUE_BUFSIZE, "%d", getpid());
    if(OB_FAIL(write_string_to_file_(procs_path, pid_value))) {
      LOG_ERROR("add tid to cgroup failed", K(ret), K(procs_path), K(pid_value));
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
    int tmp_ret = OB_SUCCESS;
    while (fgets(tid_buf, VALUE_BUFSIZE, group_task_file)) {
      if (OB_TMP_FAIL(ObCgroupCtrl::write_string_to_file_(parent_task_path, tid_buf))) {
        LOG_WARN("remove tenant task failed", K(tmp_ret), K(parent_task_path));
      }
    }
    fclose(group_task_file);
  }

  if (OB_SUCCESS != ret) {
  } else if (OB_FAIL(FileDirectoryUtils::delete_directory(curr_dir))) {
    LOG_WARN("remove group directory failed", K(ret), K(curr_dir));
  } else {
    LOG_INFO("remove group directory success", K(curr_dir));
  }
  return ret;
}

class RemoveProccessor : public ObCgroupCtrl::DirProcessor
{
public:
  int handle_dir(const char *curr_path, bool is_top_dir)
  {
    UNUSED(is_top_dir);
    return ObCgroupCtrl::remove_dir_(curr_path);
  }
};
int ObCgroupCtrl::recursion_remove_group_(const char *curr_path)
{
  RemoveProccessor remove_process;
  return recursion_process_group_(curr_path, &remove_process, true /* is_top_dir */);
}

int ObCgroupCtrl::recursion_process_group_(const char *curr_path, DirProcessor *processor_ptr, bool is_top_dir)
{
  int ret = OB_SUCCESS;
  int type = NOT_DIR;
  if (OB_FAIL(which_type_dir_(curr_path, type))) {
    LOG_WARN("check dir type failed", K(ret), K(curr_path));
  } else if (NOT_DIR == type) {
    // not directory, skip
  } else {
    if (LEAF_DIR == type) {
      // no sub directory to handle
    } else {
      DIR *dir = nullptr;
      if (NULL == (dir = opendir(curr_path))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("open dir failed", K(ret), K(curr_path));
      } else {
        struct dirent *subdir = nullptr;
        char sub_path[PATH_BUFSIZE];
        int tmp_ret = OB_SUCCESS;
        while (OB_SUCCESS == tmp_ret && (NULL != (subdir = readdir(dir)))) {
          if (0 == strcmp(subdir->d_name, ".") || 0 == strcmp(subdir->d_name, "..")) {
            // skip . and ..
          } else if (PATH_BUFSIZE <= snprintf(sub_path, PATH_BUFSIZE, "%s/%s", curr_path, subdir->d_name)) {
            // to prevent infinite recursion when path string is over size and cut off
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("sub_path is oversize has been cut off", K(ret), K(sub_path), K(curr_path));
          } else if (OB_TMP_FAIL(recursion_process_group_(sub_path, processor_ptr))) {
            LOG_WARN("process path failed", K(sub_path));
          }
        }
      }
    }
    if (OB_SUCC(ret) && 0 != strcmp(root_cgroup_, curr_path) &&
        OB_FAIL(processor_ptr->handle_dir(curr_path, is_top_dir))) {
      LOG_WARN("process group directory failed", K(ret), K(curr_path));
    }
  }
  return ret;
}

int ObCgroupCtrl::remove_cgroup(const uint64_t tenant_id, uint64_t group_id, const char *base_path)
{
  int ret = OB_SUCCESS;
  char tenant_path[PATH_BUFSIZE];
  if (OB_FAIL(get_group_path(tenant_path, PATH_BUFSIZE, tenant_id, group_id, base_path))) {
    LOG_WARN("fail get group path", K(tenant_id), K(ret));
  } else if (OB_FAIL(recursion_remove_group_(tenant_path))) {
    LOG_WARN("remove cgroup directory failed", K(ret), K(tenant_path), K(tenant_id));
  } else {
    LOG_INFO("remove cgroup directory success", K(tenant_path), K(tenant_id));
  }
  return ret;
}

int ObCgroupCtrl::remove_both_cgroup(const uint64_t tenant_id, const uint64_t group_id, const char *base_path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(remove_cgroup(tenant_id, group_id))) {
    LOG_WARN("remove tenant cgroup directory failed", K(ret), K(tenant_id));
  } else if (OB_NOT_NULL(base_path) && 0 != STRLEN(base_path) && OB_FAIL(remove_cgroup(tenant_id, group_id, base_path))) {
    LOG_WARN("remove background tenant cgroup directory failed", K(ret), K(tenant_id));
  }
  return ret;
}

// return "root_cgroup_path/[base_path]/[user_tenant_path]/[meta_tenant_path]/[group_path]"
int ObCgroupCtrl::get_group_path(
    char *group_path,
    int path_bufsize,
    const uint64_t tenant_id,
    uint64_t group_id,
    const char *base_path)
{
  int ret = OB_SUCCESS;

  char root_cgroup_path[PATH_BUFSIZE] = "";
  char user_tenant_path[PATH_BUFSIZE] = "";
  char meta_tenant_path[PATH_BUFSIZE] = "";
  char group_name_path[PATH_BUFSIZE] = "";
  // gen root_cgroup_path
  if (is_server_tenant(tenant_id)) {
    // if tenant_id is 500, return "other"
    snprintf(root_cgroup_path, path_bufsize, "%s", other_cgroup_);
  } else {
    snprintf(root_cgroup_path, path_bufsize, "%s", root_cgroup_);

    if (OB_ISNULL(base_path)) {
      base_path = "";
    }

    // gen tenant_path
    if (!is_valid_tenant_id(tenant_id)) {
      // do nothing
      // if tenant_id is invalid, return "root_cgroup_path/[base_path]"
      // if tenant_id is invalid, group_id should be invalid.
      group_id = OB_INVALID_GROUP_ID;
    } else if (is_meta_tenant(tenant_id)) {
      // tenant is meta tenant
      snprintf(user_tenant_path, PATH_BUFSIZE, "tenant_%04lu", gen_user_tenant_id(tenant_id));
      snprintf(meta_tenant_path, PATH_BUFSIZE, "tenant_%04lu", tenant_id);
    } else {
      // tenant is user tenant
      snprintf(user_tenant_path, PATH_BUFSIZE, "tenant_%04lu", tenant_id);
    }

    // gen group path
    if (!is_valid_id(group_id)) {
      // do nothing
      // if group is invalid, return "root_cgroup_path/[base_path]/[user_tenant_path]/[meta_tenant_path]/"
    } else {
      const char *group_name;
      share::ObGroupName g_name;
      if (group_id < OBCG_MAXNUM) {
        // if group is system group
        ObCgSet &set = ObCgSet::instance();
        group_name = set.name_of_id(group_id);
      } else if (OB_FAIL(get_group_info_by_group_id(tenant_id, group_id, g_name))) {
        LOG_WARN("get group_name by id failed", K(group_id), K(ret));
      } else {
        // if group is resource group
        group_name = g_name.get_value().ptr();
      }
      if (OB_SUCC(ret)) {
        if (OB_ISNULL(group_name)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("group name is null", K(ret), K(tenant_id), K(group_id));
        } else {
          snprintf(group_name_path, path_bufsize, "%s", group_name);
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    snprintf(group_path,
        path_bufsize,
        "%s/%s/%s/%s/%s",
        root_cgroup_path,
        base_path,
        user_tenant_path,
        meta_tenant_path,
        group_name_path);
  }
  return ret;
}

int ObCgroupCtrl::add_self_to_cgroup(const uint64_t tenant_id, uint64_t group_id, const char *base_path)
{
  int ret = OB_SUCCESS;
  char group_path[PATH_BUFSIZE];
  char tid_value[VALUE_BUFSIZE + 1];

  snprintf(tid_value, VALUE_BUFSIZE, "%ld", gettid());
  if (OB_FAIL(get_group_path(group_path, PATH_BUFSIZE, tenant_id, group_id, base_path))) {
    LOG_WARN("fail get group path", K(tenant_id), K(ret));
  } else if (OB_FAIL(set_cgroup_config_(group_path, TASKS_FILE, tid_value))) {
    LOG_WARN("add tid to cgroup failed", K(ret), K(group_path), K(tid_value), K(tenant_id));
  } else {
    LOG_INFO("add tid to cgroup success", K(group_path), K(tid_value), K(tenant_id), K(group_id));
  }
  return ret;
}


// TODO: 查表，通过 group id 找到 group 名字
int ObCgroupCtrl::get_group_info_by_group_id(const uint64_t tenant_id,
                                             uint64_t group_id,
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
  char tid_value[VALUE_BUFSIZE];
  // 把该tid加入other_cgroup目录的tasks文件中就会从其它tasks中删除
  snprintf(tid_value, VALUE_BUFSIZE, "%ld", gettid());
  if (OB_FAIL(set_cgroup_config_(other_cgroup_, TASKS_FILE, tid_value))) {
    LOG_WARN("remove tid to cgroup failed", K(ret), K(other_cgroup_), K(tid_value), K(tenant_id));
  } else {
    LOG_INFO("remove tid to cgroup success", K(other_cgroup_), K(tid_value), K(tenant_id));
  }
  return ret;
}

int ObCgroupCtrl::get_cgroup_config_(const char *group_path, const char *config_name, char *config_value)
{
  int ret = OB_SUCCESS;
  char cgroup_config_path[PATH_BUFSIZE];
  snprintf(cgroup_config_path, PATH_BUFSIZE, "%s/%s", group_path, config_name);
  bool exist_cgroup = false;
  if (OB_FAIL(FileDirectoryUtils::is_exists(group_path, exist_cgroup))) {
    LOG_WARN("fail check file exist", K(group_path), K(ret));
  } else if (!exist_cgroup && OB_FAIL(init_full_dir_(group_path))) {
    LOG_WARN("init tenant cgroup dir failed", K(ret), K(group_path));
  } else if (OB_FAIL(get_string_from_file_(cgroup_config_path, config_value))) {
    LOG_WARN("get cgroup config value failed", K(ret), K(cgroup_config_path), K(config_value));
  }
  return ret;
}

int ObCgroupCtrl::set_cgroup_config_(const char *group_path, const char *config_name, char *config_value)
{
  int ret = OB_SUCCESS;
  char config_path[PATH_BUFSIZE];
  snprintf(config_path, PATH_BUFSIZE, "%s/%s", group_path, config_name);

  bool exist_cgroup = false;
  if (OB_FAIL(FileDirectoryUtils::is_exists(group_path, exist_cgroup))) {
    LOG_WARN("fail check file exist", K(group_path), K(ret));
  } else if (!exist_cgroup && OB_FAIL(init_full_dir_(group_path))) {
    LOG_WARN("init tenant cgroup dir failed", K(ret), K(group_path));
  } else if (OB_FAIL(write_string_to_file_(config_path, config_value))) {
    LOG_WARN("set cgroup config failed", K(ret), K(config_path), K(config_value));
  }
  return ret;
}

int ObCgroupCtrl::set_cpu_shares(const uint64_t tenant_id,
                                   const double cpu,
                                   const uint64_t group_id,
                                   const char *base_path)
{
  int ret = OB_SUCCESS;
  char group_path[PATH_BUFSIZE];
  char cpu_shares_value[VALUE_BUFSIZE + 1];

  int32_t cpu_shares = static_cast<int32_t>(cpu * DEFAULT_CPU_SHARES);
  snprintf(cpu_shares_value, VALUE_BUFSIZE, "%d", cpu_shares);
  if (OB_FAIL(get_group_path(group_path, PATH_BUFSIZE, tenant_id, group_id, base_path))) {
    LOG_WARN("fail get group path", K(tenant_id), K(ret));
  } else if (OB_FAIL(set_cgroup_config_(group_path, CPU_SHARES_FILE, cpu_shares_value))) {
    LOG_WARN("set cpu shares failed", K(ret), K(group_path), K(tenant_id));
  } else {
    LOG_INFO("set cpu shares success",
             K(group_path), K(cpu), K(cpu_shares_value), K(tenant_id));
  }
  return ret;
}


int ObCgroupCtrl::set_both_cpu_shares(const uint64_t tenant_id,
                                          const double cpu,
                                          const uint64_t group_id,
                                          const char *base_path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(set_cpu_shares(tenant_id, cpu, group_id))) {
    LOG_WARN("set cpu shares failed", K(ret), K(tenant_id), K(group_id), K(cpu));
  } else if (OB_NOT_NULL(base_path) && 0 != STRLEN(base_path) && OB_FAIL(set_cpu_shares(tenant_id, cpu, group_id, base_path))) {
    LOG_WARN("set background cpu shares failed", K(ret), K(tenant_id), K(group_id), K(cpu));
  }
  return ret;
}

int ObCgroupCtrl::get_cpu_shares(const uint64_t tenant_id, double &cpu, const uint64_t group_id, const char *base_path)
{
  int ret = OB_SUCCESS;
  char group_path[PATH_BUFSIZE];
  char cpu_shares_value[VALUE_BUFSIZE + 1];

  if (OB_FAIL(get_group_path(group_path, PATH_BUFSIZE, tenant_id, group_id, base_path))) {
    LOG_WARN("fail get group path", K(tenant_id), K(ret));
  } else if (OB_FAIL(get_cgroup_config_(group_path, CPU_SHARES_FILE, cpu_shares_value))) {
    LOG_WARN("get cpu shares failed", K(ret), K(group_path), K(tenant_id));
  } else {
    cpu_shares_value[VALUE_BUFSIZE] = '\0';
    cpu = 1.0 * atoi(cpu_shares_value) / DEFAULT_CPU_SHARES;
  }
  return ret;
}

int ObCgroupCtrl::compare_cpu(const double cpu1, const double cpu2, int &compare_ret) {
  int ret = OB_SUCCESS;
  compare_ret = 0;
  if (cpu1 != cpu2) {
    if (-1 == cpu1) {
      compare_ret = 1;
    } else if (-1 == cpu2) {
      compare_ret = -1;
    } else {
      compare_ret = cpu1 > cpu2 ? 1 : -1;
    }
  }
  return ret;
}

int ObCgroupCtrl::set_cpu_cfs_quota(const uint64_t tenant_id, const double cpu, const uint64_t group_id, const char *base_path)
{
  int ret = OB_SUCCESS;

  double target_cpu = cpu;
  double base_cpu = -1;

  // background quota limit
  if (is_valid_tenant_id(tenant_id) && OB_NOT_NULL(base_path) && 0 != STRLEN(base_path)) {
    int compare_ret = 0;
    if (OB_FAIL(get_cpu_cfs_quota(OB_INVALID_TENANT_ID, base_cpu, OB_INVALID_GROUP_ID, base_path))) {
      LOG_WARN("get background cpu cfs quota failed", K(ret), K(tenant_id));
    } else if (OB_FAIL(compare_cpu(target_cpu, base_cpu, compare_ret))) {
      LOG_WARN("compare cpu failed", K(ret), K(target_cpu), K(base_cpu));
    } else if (compare_ret > 0) {
      target_cpu = base_cpu;
    }
  }

  // tenant quota limit
  double tenant_cpu = -1;
  if (OB_SUCC(ret) && is_valid_group(group_id)) {
    int compare_ret = 0;
    if (OB_FAIL(get_cpu_cfs_quota(tenant_id, tenant_cpu, OB_INVALID_GROUP_ID, base_path))) {
      LOG_WARN("get tenant cpu cfs quota failed", K(ret), K(tenant_id));
    } else if (OB_FAIL(compare_cpu(target_cpu, tenant_cpu, compare_ret))) {
      LOG_WARN("compare cpu failed", K(ret), K(target_cpu), K(tenant_cpu));
    } else if (compare_ret > 0) {
      target_cpu = tenant_cpu;
    }
  }

  if (OB_SUCC(ret)) {
    double current_cpu = -1;
    char group_path[PATH_BUFSIZE];
    int compare_ret = 0;
    if (OB_FAIL(get_cpu_cfs_quota(tenant_id, current_cpu, group_id, base_path))) {
      LOG_WARN("get cpu cfs quota failed", K(ret), K(group_path), K(tenant_id), K(group_id));
    } else if (OB_FAIL(get_group_path(group_path, PATH_BUFSIZE, tenant_id, group_id, base_path))) {
      LOG_WARN("get group path failed", K(ret), K(group_path), K(tenant_id), K(group_id));
    } else if (OB_FAIL(compare_cpu(target_cpu, current_cpu, compare_ret))) {
      LOG_WARN("compare cpu failed", K(ret), K(target_cpu), K(current_cpu));
    } else if (0 == compare_ret) {
      // no need to change
    } else if (compare_ret < 0) {
      ret = recursion_dec_cpu_cfs_quota_(group_path, target_cpu);
    } else {
      ret = set_cpu_cfs_quota_by_path_(group_path, target_cpu);
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("set cpu cfs quota failed",
          K(ret),
          K(tenant_id),
          K(group_id),
          K(cpu),
          K(target_cpu),
          K(current_cpu),
          K(group_path));
    }
  }
  return ret;
}

class DecQuotaProcessor : public ObCgroupCtrl::DirProcessor
{
public:
  DecQuotaProcessor(const double cpu) : cpu_(cpu)
  {}
  int handle_dir(const char *curr_path, bool is_top_dir)
  {
    int ret = OB_SUCCESS;
    double current_cpu = -1;
    int compare_ret = 0;
    if (OB_FAIL(ObCgroupCtrl::get_cpu_cfs_quota_by_path_(curr_path, current_cpu))) {
      LOG_WARN("get cpu cfs quota failed", K(ret), K(curr_path));
    } else if ((!is_top_dir && -1 == current_cpu) ||
               (OB_SUCC(ObCgroupCtrl::compare_cpu(cpu_, current_cpu, compare_ret)) && compare_ret >= 0)) {
      // do nothing
    } else if (OB_FAIL(ObCgroupCtrl::set_cpu_cfs_quota_by_path_(curr_path, cpu_))) {
      LOG_WARN("set cpu cfs quota failed", K(curr_path), K(cpu_));
    }
    return ret;
  }

private:
  const double cpu_;
};

int ObCgroupCtrl::recursion_dec_cpu_cfs_quota_(const char *group_path, const double cpu)
{
  DecQuotaProcessor dec_quota_process(cpu);
  return recursion_process_group_(group_path, &dec_quota_process, true /* is_top_dir */);
}

int ObCgroupCtrl::set_cpu_cfs_quota_by_path_(const char *group_path, const double cpu)
{
  int ret = OB_SUCCESS;
  char cfs_period_value[VALUE_BUFSIZE + 1];
  char cfs_quota_value[VALUE_BUFSIZE + 1];
  if (OB_FAIL(get_cgroup_config_(group_path, CPU_CFS_PERIOD_FILE, cfs_period_value))) {
    LOG_WARN("get cpu cfs period failed", K(ret), K(group_path));
  } else {
    cfs_period_value[VALUE_BUFSIZE] = '\0';
    int32_t cfs_period_us_new = atoi(cfs_period_value);

    int32_t cfs_period_us = 0;
    int32_t cfs_quota_us = 0;
    uint32_t loop_times = 0;
    // to avoid kernel scaling cfs_period_us after get cpu_cfs_period,
    // we should check whether cfs_period_us has been changed after set cpu_cfs_quota.
    while (OB_SUCC(ret) && cfs_period_us_new != cfs_period_us) {
      if (loop_times > 3) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("cpu_cfs_period has been always changing, thread may be hung",
            K(ret),
            K(group_path),
            K(cfs_period_us),
            K(cfs_period_us_new));
      } else {
        cfs_period_us = cfs_period_us_new;
        if (-1 == cpu || cpu >= INT32_MAX / cfs_period_us) {
          cfs_quota_us = -1;
        } else {
          cfs_quota_us = static_cast<int32_t>(cfs_period_us * cpu);
        }
        snprintf(cfs_quota_value, VALUE_BUFSIZE, "%d", cfs_quota_us);
        if (OB_FAIL(set_cgroup_config_(group_path, CPU_CFS_QUOTA_FILE, cfs_quota_value))) {
          LOG_WARN("set cpu cfs quota failed", K(group_path), K(cfs_quota_us));
        } else if (OB_FAIL(get_cgroup_config_(group_path, CPU_CFS_PERIOD_FILE, cfs_period_value))) {
          LOG_ERROR("fail get cpu cfs period us", K(group_path));
        } else {
          cfs_period_value[VALUE_BUFSIZE] = '\0';
          cfs_period_us_new = atoi(cfs_period_value);
        }
      }
      loop_times++;
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("set cpu quota success", K(group_path), K(cpu), K(cfs_quota_us), K(cfs_period_us));
    }
  }
  return ret;
}

int ObCgroupCtrl::get_cpu_cfs_quota_by_path_(const char *group_path, double &cpu)
{
  int ret = OB_SUCCESS;
  char cfs_period_value[VALUE_BUFSIZE + 1];
  char cfs_quota_value[VALUE_BUFSIZE + 1];
  int32_t cfs_quota_us = 0;
  if (OB_FAIL(get_cgroup_config_(group_path, CPU_CFS_QUOTA_FILE, cfs_quota_value))) {
    LOG_WARN("get cpu cfs quota failed", K(ret), K(group_path));
  } else {
    cfs_quota_value[VALUE_BUFSIZE] = '\0';
    cfs_quota_us = atoi(cfs_quota_value);
    if (-1 == cfs_quota_us) {
      cpu = -1;
    } else if (OB_FAIL(get_cgroup_config_(group_path, CPU_CFS_PERIOD_FILE, cfs_period_value))) {
      LOG_WARN("get cpu cfs period failed", K(ret), K(group_path));
    } else {
      cfs_period_value[VALUE_BUFSIZE] = '\0';
      int32_t cfs_period_us = atoi(cfs_period_value);
      cpu = 1.0 * cfs_quota_us / cfs_period_us;
    }
  }
  return ret;
}

int ObCgroupCtrl::set_both_cpu_cfs_quota(const uint64_t tenant_id, const double cpu, const uint64_t group_id, const char *base_path)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(set_cpu_cfs_quota(tenant_id, cpu, group_id))) {
    LOG_WARN("set tenant cpu cfs quota failed", K(ret), K(tenant_id), K(group_id), K(cpu));
  } else if (OB_NOT_NULL(base_path) && 0 != STRLEN(base_path) && OB_FAIL(set_cpu_cfs_quota(tenant_id, cpu, group_id, base_path))) {
    LOG_WARN("set background tenant cpu cfs quota failed", K(ret), K(tenant_id), K(group_id), K(cpu));
  }
  return OB_SUCCESS;
}

int ObCgroupCtrl::get_cpu_cfs_quota(const uint64_t tenant_id, double &cpu, const uint64_t group_id, const char *base_path)
{
  int ret = OB_SUCCESS;
  char group_path[PATH_BUFSIZE];
  if (OB_FAIL(get_group_path(group_path, PATH_BUFSIZE, tenant_id, group_id, base_path))) {
    LOG_WARN("fail get group path", K(tenant_id), K(ret));
  } else if (OB_FAIL(get_cpu_cfs_quota_by_path_(group_path, cpu))) {
    LOG_WARN("get cpu cfs quota failed", K(ret), K(group_path), K(tenant_id));
  }
  return ret;
}

int ObCgroupCtrl::get_cpu_time(const uint64_t tenant_id, int64_t &cpu_time, const uint64_t group_id, const char *base_path)
{
  int ret = OB_SUCCESS;

  char group_path[PATH_BUFSIZE];
  char cpuacct_usage_value[VALUE_BUFSIZE + 1];

  if (OB_FAIL(get_group_path(group_path, PATH_BUFSIZE, tenant_id, group_id, base_path))) {
    LOG_WARN("fail get group path", K(tenant_id), K(ret));
  } else if (OB_FAIL(get_cgroup_config_(group_path, CPUACCT_USAGE_FILE, cpuacct_usage_value))) {
    LOG_WARN("get cpuacct.usage failed", K(ret), K(group_path), K(tenant_id));
  } else {
    cpuacct_usage_value[VALUE_BUFSIZE] = '\0';
    cpu_time = strtoull(cpuacct_usage_value, NULL, 10) / 1000;
  }
  return ret;
}

int ObCgroupCtrl::get_throttled_time(const uint64_t tenant_id, int64_t &throttled_time, const uint64_t group_id, const char *base_path)
{
  int ret = OB_SUCCESS;

  char group_path[PATH_BUFSIZE];
  char cpu_stat_value[VALUE_BUFSIZE + 1];

  if (OB_FAIL(get_group_path(group_path, PATH_BUFSIZE, tenant_id, group_id, base_path))) {
    LOG_WARN("fail get group path", K(tenant_id), K(ret));
  } else if (OB_FAIL(get_cgroup_config_(group_path, CPU_STAT_FILE, cpu_stat_value))) {
    LOG_WARN("get cpu.stat failed", K(ret), K(group_path), K(tenant_id));
  } else {
    cpu_stat_value[VALUE_BUFSIZE] = '\0';
    const char *LABEL_STR = "throttled_time ";
    char *found_ptr = strstr(cpu_stat_value, LABEL_STR);
    if (OB_ISNULL(found_ptr)) {
      ret = OB_IO_ERROR;
      LOG_WARN("get throttled_time failed", K(ret), K(group_path), K(tenant_id), K(group_id), K(cpu_stat_value));
    } else {
      found_ptr += strlen(LABEL_STR);
      throttled_time = strtoull(found_ptr, NULL, 10) / 1000;
    }
  }
  return ret;
}

int ObCgroupCtrl::set_group_iops(const uint64_t tenant_id,
                                 const uint64_t group_id,
                                 const OBGroupIOInfo &group_io)
{
  int ret = OB_SUCCESS;

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

int ObCgroupCtrl::reset_all_group_iops(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

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
                                   const common::ObString &consumer_group)
{
  int ret = OB_SUCCESS;

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
                                    const common::ObString &consumer_group)
{
  int ret = OB_SUCCESS;

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
    recursion_remove_group_(cgroup_path);
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

int ObCgroupCtrl::init_dir_(const char *curr_path)
{
  int ret = OB_SUCCESS;
  char current_path[PATH_BUFSIZE];
  char value_buf[VALUE_BUFSIZE];
  bool exist_cgroup = false;
  if (OB_ISNULL(curr_path)) {
    ret = OB_INVALID_ARGUMENT;
    LIB_LOG(WARN, "invalid arguments.", K(curr_path), K(ret));
  } else if (OB_FAIL(FileDirectoryUtils::is_exists(curr_path, exist_cgroup))) {
    LOG_WARN("fail check file exist", K(curr_path), K(ret));
  } else if (exist_cgroup) {
    // group dir already exist, do nothing
  } else if (OB_FAIL(FileDirectoryUtils::create_directory(curr_path))) {
    LOG_WARN("create tenant cgroup dir failed", K(ret), K(curr_path));
  } else {
    // 设置 mems 和 cpus
    //  cpu 可能是不连续的，自己探测很复杂。
    //  这里总是继承父级的设置，无需自己探测。
   // 后继 child cgroup 无需再设置 mems 和 cpus
    if (OB_SUCC(ret)) {
      snprintf(current_path, PATH_BUFSIZE, "%s/cgroup.clone_children", curr_path);
      snprintf(value_buf, VALUE_BUFSIZE, "1");
      if (OB_FAIL(write_string_to_file_(current_path, value_buf))) {
        LOG_WARN("fail set value to file", K(current_path), K(ret));
      }
      LOG_DEBUG("debug print clone", K(current_path), K(value_buf));
    }
  }
  return ret;
}

int ObCgroupCtrl::init_full_dir_(const char *curr_path)
{

  int ret = OB_SUCCESS;
  int64_t len = 0;
  if (OB_ISNULL(curr_path) || 0 == (len = strlen(curr_path))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(curr_path));
  } else {
    char dirpath[PATH_BUFSIZE + 1];
    strncpy(dirpath, curr_path, len);
    dirpath[len] = '\0';
    char *path = dirpath;

    // skip leading char '/'
    while (*path++ == '/');

    while (OB_SUCC(ret) && OB_NOT_NULL(path = strchr(path, '/'))) {
      *path = '\0';
      if (OB_FAIL(init_dir_(dirpath))) {
        LOG_WARN("init group dir failed.", K(ret), K(dirpath));
      } else {
        *path++ = '/';
        // skip '/'
        while (*path++ == '/')
          ;
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(init_dir_(dirpath))) {
        LOG_WARN("init group dir failed.", K(ret), K(dirpath));
      }
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
    LOG_ERROR("open file error", K(filename), K(errno), KERRMSG, K(ret));
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
