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
#include "share/io/ob_io_manager.h"
#include "share/resource_manager/ob_resource_manager.h"
#include "observer/omt/ob_tenant.h"


using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::omt;

namespace oceanbase
{

namespace lib
{

int SET_GROUP_ID(uint64_t group_id, bool is_background)
{
  int ret = OB_SUCCESS;

  // to do switch group

  THIS_WORKER.set_group_id_(group_id);
  int tmp_ret = OB_SUCCESS;
  if (OB_NOT_NULL(GCTX.cgroup_ctrl_) &&
      OB_TMP_FAIL(GCTX.cgroup_ctrl_->add_self_to_cgroup_(MTL_ID(), group_id, is_background))) {
    LOG_WARN("add self to cgroup fail", K(ret), K(MTL_ID()), K(group_id));
  }
  return ret;
}

int CONVERT_FUNCTION_TYPE_TO_GROUP_ID(const uint8_t function_type, uint64_t &group_id)
{
  return G_RES_MGR.get_mapping_rule_mgr().get_group_id_by_function_type(MTL_ID(), function_type, group_id);
}

}  // namespace lib

namespace share
{
ObCgSet ObCgSet::instance_;
}  // namespace share
}  // namespace oceanbase

// cpu shares base value
static const int32_t DEFAULT_CPU_SHARES = 1024;

// cgroup directory
static constexpr const char *const SYS_CGROUP_DIR = "/sys/fs/cgroup";
static constexpr const char *const OBSERVER_ROOT_CGROUP_DIR = "cgroup";
static constexpr const char *const OTHER_CGROUP_DIR = "cgroup/other";

// cgroup config name
static constexpr const char *const CPU_SHARES_FILE = "cpu.shares";
static constexpr const char *const TASKS_FILE = "tasks";
static constexpr const char *const CPU_CFS_QUOTA_FILE = "cpu.cfs_quota_us";
static constexpr const char *const CPU_CFS_PERIOD_FILE = "cpu.cfs_period_us";
static constexpr const char *const CPUACCT_USAGE_FILE = "cpuacct.usage";
static constexpr const char *const CPU_STAT_FILE = "cpu.stat";
static constexpr const char *const CGROUP_PROCS_FILE = "cgroup.procs";
static constexpr const char *const CGROUP_CLONE_CHILDREN_FILE = "cgroup.clone_children";

//集成IO参数
int ObGroupIOInfo::init(const char *name, const int64_t min_percent, const int64_t max_percent, const int64_t weight_percent,
                        const int64_t max_net_bandwidth_percent, const int64_t net_bandwidth_weight_percent)
{
  int ret = OB_SUCCESS;
  if (min_percent < 0 || min_percent > 100 ||
      max_percent < 0 || max_percent > 100 ||
      weight_percent < 0 || weight_percent > 100 ||
      min_percent > max_percent ||
      max_net_bandwidth_percent < 0 || max_net_bandwidth_percent > 100 ||
      net_bandwidth_weight_percent < 0 || net_bandwidth_weight_percent > 100) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("invalid io config", K(ret), K(min_percent), K(max_percent), K(weight_percent),
                                 K(max_net_bandwidth_percent), K(net_bandwidth_weight_percent));
  } else {
    group_name_ = name;
    min_percent_ = min_percent;
    max_percent_ = max_percent;
    weight_percent_ = weight_percent;
    max_net_bandwidth_percent_ = max_net_bandwidth_percent;
    net_bandwidth_weight_percent_ = net_bandwidth_weight_percent;
  }
  return ret;
}
bool ObGroupIOInfo::is_valid() const
{
  return min_percent_ >= 0 && min_percent_ <= 100 &&
         max_percent_ >= min_percent_ &&
         max_percent_ >= 0 && max_percent_ <= 100 &&
         weight_percent_ >= 0 && weight_percent_ <= 100 &&
         max_net_bandwidth_percent_ >= 0 && max_net_bandwidth_percent_ <= 100 &&
         net_bandwidth_weight_percent_ >= 0 && net_bandwidth_weight_percent_ <= 100;
}

bool ObCgroupCtrl::is_valid_group_name(ObString &group_name)
{
  bool bool_ret = true;
  const char *group_name_ptr = group_name.ptr();
  if (group_name.length() < 0 || group_name.length() > GROUP_NAME_BUFSIZE) {
    bool_ret = false;
  } else {
    for (int i = 0; i < group_name.length() && bool_ret; i++) {
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

// init cgroup
int ObCgroupCtrl::init()
{
  int ret = OB_SUCCESS;
  (void)check_cgroup_status();
  return ret;
}

// regist observer pid to cgroup.procs
int ObCgroupCtrl::regist_observer_to_cgroup(const char *cgroup_dir)
{
  int ret = OB_SUCCESS;
  char pid_value[VALUE_BUFSIZE];
  snprintf(pid_value, VALUE_BUFSIZE, "%d", getpid());
  if (OB_FAIL(set_cgroup_config_(cgroup_dir, CGROUP_PROCS_FILE, pid_value))) {
    LOG_WARN("add tid to cgroup failed", K(ret), K(cgroup_dir), K(pid_value));
  }
  return ret;
}

// dynamic check cgroup status
bool ObCgroupCtrl::check_cgroup_status()
{
  int tmp_ret = OB_SUCCESS;
  bool need_regist_cgroup = false;
  if (GCONF.enable_cgroup == false && is_valid() == false) {
    // clean remain cgroup dir
    if (OB_TMP_FAIL(check_cgroup_root_dir())) {
      LOG_WARN_RET(tmp_ret, "check cgroup root dir failed", K(tmp_ret));
    } else if (OB_TMP_FAIL(recursion_remove_group_(OBSERVER_ROOT_CGROUP_DIR, false /* if_remove_top */))) {
      LOG_WARN_RET(tmp_ret, "clean observer root cgroup failed", K(ret));
    }
  } else if ((GCONF.enable_cgroup == true && is_valid() == true)) {
    // In case symbolic link is deleted
    if (OB_TMP_FAIL(check_cgroup_root_dir())) {
      LOG_WARN_RET(tmp_ret, "check cgroup root dir failed", K(tmp_ret));
      valid_ = false;
    }
  } else if (GCONF.enable_cgroup == false && is_valid() == true) {
    valid_ = false;
    if (OB_TMP_FAIL(check_cgroup_root_dir())) {
      LOG_WARN_RET(tmp_ret, "check cgroup root dir failed", K(tmp_ret));
    } else if (OB_TMP_FAIL(regist_observer_to_cgroup(OBSERVER_ROOT_CGROUP_DIR))) {
      LOG_WARN_RET(tmp_ret, "regist observer thread to cgroup failed", K(tmp_ret), K(OBSERVER_ROOT_CGROUP_DIR));
    } else if (OB_TMP_FAIL(recursion_remove_group_(OBSERVER_ROOT_CGROUP_DIR, false /* if_remove_top */))) {
      LOG_WARN_RET(tmp_ret, "clean observer root cgroup failed", K(ret));
    }
  } else if (GCONF.enable_cgroup == true && is_valid() == false) {
    if (OB_TMP_FAIL(check_cgroup_root_dir())) {
      if (REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {
        LOG_WARN_RET(tmp_ret, "check cgroup root dir failed", K(tmp_ret));
      }
    } else if (OB_TMP_FAIL(recursion_remove_group_(OBSERVER_ROOT_CGROUP_DIR, false /* if_remove_top */))) {
      LOG_WARN_RET(tmp_ret, "clean observer root cgroup failed", K(ret));
    } else if (OB_TMP_FAIL(init_dir_(OBSERVER_ROOT_CGROUP_DIR))) {
      if (REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {
        LOG_WARN_RET(tmp_ret, "init cgroup dir failed", K(tmp_ret));
      }
    } else if (OB_TMP_FAIL(regist_observer_to_cgroup(OTHER_CGROUP_DIR))) {
      LOG_WARN_RET(tmp_ret, "regist observer thread to cgroup failed", K(tmp_ret), K(OTHER_CGROUP_DIR));
    } else {
      need_regist_cgroup = true;
      valid_ = true;
      LOG_INFO("init cgroup success");
    }
  }
  return need_regist_cgroup;
}

int ObCgroupCtrl::check_cgroup_root_dir()
{
  int ret = OB_SUCCESS;
  bool exist_cgroup = false;
  int link_len = 0;
  char real_cgroup_path[PATH_BUFSIZE];
  if (OB_FAIL(FileDirectoryUtils::is_exists(OBSERVER_ROOT_CGROUP_DIR, exist_cgroup))) {
    ret = OB_ERR_SYS;
    LOG_WARN("fail check file exist", K(OBSERVER_ROOT_CGROUP_DIR), K(ret));
  } else if (!exist_cgroup) {
    ret = OB_FILE_NOT_EXIST;
    LOG_WARN("dir not exist", K(OBSERVER_ROOT_CGROUP_DIR), K(ret));
  } else if (-1 == (link_len = readlink(OBSERVER_ROOT_CGROUP_DIR, real_cgroup_path, PATH_BUFSIZE))) {
    ret = OB_ERR_SYS;
    LOG_WARN("fail to read link", K(OBSERVER_ROOT_CGROUP_DIR), K(ret));
  } else if (0 != access(OBSERVER_ROOT_CGROUP_DIR, R_OK | W_OK)) {
    // access denied
    ret = OB_ERR_SYS;
    LOG_WARN("no permission to access", K(OBSERVER_ROOT_CGROUP_DIR), K(ret));
  }
  return ret;
}

void ObCgroupCtrl::destroy()
{
  if (is_valid()) {
    valid_ = false;
    recursion_remove_group_(OBSERVER_ROOT_CGROUP_DIR, false /* if_remove_top */);
  }
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
    closedir(dir);
  }
  return ret;
}

int ObCgroupCtrl::remove_dir_(const char *curr_dir, bool is_delete_group)
{
  int ret = OB_SUCCESS;
  char group_task_path[PATH_BUFSIZE];
  char target_task_path[PATH_BUFSIZE];
  snprintf(group_task_path, PATH_BUFSIZE, "%s/tasks", curr_dir);
  if (is_delete_group) {
    snprintf(target_task_path, PATH_BUFSIZE, "%s/../OBCG_DEFAULT/tasks", curr_dir);
    FILE *group_task_file = nullptr;
    if (OB_ISNULL(group_task_file = fopen(group_task_path, "r"))) {
      ret = OB_IO_ERROR;
      LOG_WARN("open group failed", K(ret), K(group_task_path), K(errno), KERRMSG);
    } else {
      char tid_buf[VALUE_BUFSIZE];
      int tmp_ret = OB_SUCCESS;
      while (fgets(tid_buf, VALUE_BUFSIZE, group_task_file)) {
        if (OB_TMP_FAIL(ObCgroupCtrl::write_string_to_file_(target_task_path, tid_buf))) {
          LOG_WARN("remove tenant task failed", K(tmp_ret), K(target_task_path));
        }
      }
      fclose(group_task_file);
    }
  }
  if (OB_SUCCESS != ret) {
  } else if (OB_FAIL(FileDirectoryUtils::delete_directory(curr_dir))) {
    LOG_WARN("remove group directory failed", K(ret), K(curr_dir));
  } else {
    LOG_INFO("remove group directory success", K(curr_dir));
  }
  return ret;
}

int ObCgroupCtrl::recursion_remove_group_(const char *curr_path, bool if_remove_top)
{
  class RemoveProccessor : public ObCgroupCtrl::DirProcessor
  {
  public:
    int handle_dir(const char *curr_path, bool is_top_dir)
    {
      int ret = OB_SUCCESS;
      if (!is_top_dir) {
        ret = ObCgroupCtrl::remove_dir_(curr_path);
      }
      return ret;
    }
  };
  RemoveProccessor remove_process;
  return recursion_process_group_(curr_path, &remove_process, !if_remove_top /* is_top_dir */);
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
        closedir(dir);
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(processor_ptr->handle_dir(curr_path, is_top_dir))) {
      LOG_WARN("process group directory failed", K(ret), K(curr_path));
    }
  }
  return ret;
}

int ObCgroupCtrl::remove_cgroup_(const uint64_t tenant_id, uint64_t group_id, const bool is_background)
{
  int ret = OB_SUCCESS;
  char group_path[PATH_BUFSIZE];
  if (OB_FAIL(get_group_path(group_path, PATH_BUFSIZE, tenant_id, group_id, is_background))) {
    LOG_WARN("fail get group path", K(tenant_id), K(ret));
  } else if (is_valid_group(group_id)) {
    ret = remove_dir_(group_path, true /* is_delete_group */);
  } else {
    ret = recursion_remove_group_(group_path);
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("remove cgroup directory failed", K(ret), K(group_path), K(tenant_id));
    ret = OB_SUCCESS;
    // ignore failure
  } else {
    LOG_INFO("remove cgroup directory success", K(group_path), K(tenant_id));
  }
  return ret;
}

int ObCgroupCtrl::remove_cgroup(const uint64_t tenant_id, const uint64_t group_id, const bool is_background)
{
  int ret = OB_SUCCESS;
  if (!(is_background && GCONF.enable_global_background_resource_isolation)) {
    if (OB_FAIL(remove_cgroup_(tenant_id, group_id, false /* is_background */))) {
      LOG_WARN("remove tenant cgroup directory failed", K(ret), K(tenant_id));
    }
  }
  if (OB_SUCC(ret) && GCONF.enable_global_background_resource_isolation) {
    if (OB_FAIL(remove_cgroup_(tenant_id, group_id, true /* is_background */))) {
      LOG_WARN("remove background tenant cgroup directory failed", K(ret), K(tenant_id));
    }
  }
  return ret;
}

// return "root_cgroup_path/[base_path]/[user_tenant_path]/[meta_tenant_path]/[group_path]"
int ObCgroupCtrl::get_group_path(
    char *group_path, int path_bufsize, const uint64_t tenant_id, uint64_t group_id, const bool is_background /* = false*/)
{
  int ret = OB_SUCCESS;

  char root_cgroup_path[PATH_BUFSIZE] = "";
  char user_tenant_path[PATH_BUFSIZE] = "";
  char meta_tenant_path[PATH_BUFSIZE] = "";
  char group_name_path[PATH_BUFSIZE] = "";
  if (!is_valid()) {
    ret = OB_INVALID_CONFIG;
  } else if (!is_valid_tenant_id(tenant_id)) {
    // if tenant_id is invalid, return "root_cgroup_path/[background_path]"
    // if tenant_id is invalid, group_id should be invalid.
    group_id = OB_INVALID_GROUP_ID;
    // gen root_cgroup_path
    if (is_background) {
      // background base, return "cgroup/background"
      snprintf(root_cgroup_path, path_bufsize, "%s", OBSERVER_ROOT_CGROUP_DIR);
    } else {
      // if tenant_id is invalid and not background, return "cgroup/other"
      snprintf(root_cgroup_path, path_bufsize, "%s", OTHER_CGROUP_DIR);
    }
  } else if (is_server_tenant(tenant_id)) {
    // if tenant_id is 500, return "cgroup/other"
    snprintf(root_cgroup_path, path_bufsize, "%s", OTHER_CGROUP_DIR);
  } else {
    snprintf(root_cgroup_path, path_bufsize, "%s", OBSERVER_ROOT_CGROUP_DIR);

    // gen tenant_path
    if (is_meta_tenant(tenant_id)) {
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
      const char *group_name = nullptr;
      share::ObGroupName g_name;
      int tmp_ret = OB_SUCCESS;
      const int64_t WARN_LOG_INTERVAL = 3600 * 1000 * 1000L;  // 1 hour
      ObCgSet &set = ObCgSet::instance();

      if (is_resource_manager_group(group_id)) {
        // if group is resource group
        if (OB_TMP_FAIL(get_group_info_by_group_id(tenant_id, group_id, g_name))) {
          if (REACH_TIME_INTERVAL(WARN_LOG_INTERVAL)) {
            LOG_WARN("fail to get group_name", K(tmp_ret), K(tenant_id), K(group_id), K(lbt()));
          }
        } else {
          group_name = g_name.get_value().ptr();
        }
      } else if (group_id >= OBCG_MAXNUM) {
        // impossible
        tmp_ret = OB_INVALID_CONFIG;
        if (REACH_TIME_INTERVAL(WARN_LOG_INTERVAL)) {
          LOG_WARN("group_id is invalid", K(tmp_ret), K(tenant_id), K(group_id), K(lbt()));
        }
      }

      if (OB_SUCCESS != tmp_ret) {
        group_id = OBCG_DEFAULT;
      }

      if (group_id < OBCG_MAXNUM) {
        group_name = set.name_of_id(group_id);
      }

      if (OB_ISNULL(group_name)) {
        tmp_ret = OB_ERR_UNEXPECTED;
        group_name = "OBCG_DEFAULT";
        if (REACH_TIME_INTERVAL(WARN_LOG_INTERVAL)) {
          LOG_WARN("group_name is null", K(tmp_ret), K(tenant_id), K(group_id), K(group_name), K(lbt()));
        }
      }

      if (OB_SUCCESS == tmp_ret && is_resource_manager_group(group_id)) {
        snprintf(group_name_path, path_bufsize, "OBRM_%s", group_name);  // resource manager
      } else {
        snprintf(group_name_path, path_bufsize, "%s", group_name);
      }
    }
  }
  if (OB_SUCC(ret)) {
    snprintf(group_path,
        path_bufsize,
        "%s/%s/%s/%s/%s",
        root_cgroup_path,
        is_background ? "background" : "",
        user_tenant_path,
        meta_tenant_path,
        group_name_path);
  }
  return ret;
}

int ObCgroupCtrl::add_self_to_cgroup_(const uint64_t tenant_id, const uint64_t group_id, const bool is_background)
{
  return add_thread_to_cgroup_(GETTID(), tenant_id, group_id, is_background);
}

int ObCgroupCtrl::add_thread_to_cgroup_(
    const int64_t tid, const uint64_t tenant_id, const uint64_t group_id, const bool is_background)
{
  int ret = OB_SUCCESS;
  if (is_valid()) {
    char group_path[PATH_BUFSIZE];
    char tid_value[VALUE_BUFSIZE + 1];

    snprintf(tid_value, VALUE_BUFSIZE, "%ld", tid);
    if (OB_FAIL(get_group_path(group_path,
            PATH_BUFSIZE,
            tenant_id,
            group_id,
            is_background && GCONF.enable_global_background_resource_isolation))) {
      LOG_WARN("fail get group path", K(tenant_id), K(ret));
    } else if (OB_FAIL(set_cgroup_config_(group_path, TASKS_FILE, tid_value))) {
      LOG_WARN("add tid to cgroup failed", K(ret), K(group_path), K(tid_value), K(tenant_id));
    } else {
      LOG_INFO("add tid to cgroup success", K(group_path), K(tid_value), K(tenant_id), K(group_id));
    }
  }
  return ret;
}

// find group name by group id
int ObCgroupCtrl::get_group_info_by_group_id(
    const uint64_t tenant_id, uint64_t group_id, share::ObGroupName &group_name)
{
  int ret = OB_SUCCESS;
  ObResourceMappingRuleManager &rule_mgr = G_RES_MGR.get_mapping_rule_mgr();
  if (OB_FAIL(rule_mgr.get_group_name_by_id(tenant_id, group_id, group_name))) {
    if (REACH_TIME_INTERVAL(3600 * 1000 * 1000L)) {
      LOG_WARN("fail get group name", K(tenant_id), K(group_id), K(group_name));
    }
  }
  return ret;
}

int ObCgroupCtrl::get_cgroup_config_(const char *group_path, const char *config_name, char *config_value)
{
  int ret = OB_SUCCESS;
  char cgroup_config_path[PATH_BUFSIZE];
  snprintf(cgroup_config_path, PATH_BUFSIZE, "%s/%s", group_path, config_name);
  bool exist_cgroup = false;
  if (OB_FAIL(check_cgroup_root_dir())) {
    LOG_WARN("check cgroup root dir failed", K(ret));
  } else if (OB_FAIL(FileDirectoryUtils::is_exists(group_path, exist_cgroup))) {
    LOG_WARN("fail check file exist", K(group_path), K(ret));
  } else if (!exist_cgroup) {
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
  if (OB_FAIL(check_cgroup_root_dir())) {
    LOG_WARN("check cgroup root dir failed", K(ret));
  } else if (OB_FAIL(FileDirectoryUtils::is_exists(group_path, exist_cgroup))) {
    LOG_WARN("fail check file exist", K(group_path), K(ret));
  } else if (!exist_cgroup && OB_FAIL(init_full_dir_(group_path))) {
    LOG_WARN("init tenant cgroup dir failed", K(ret), K(group_path));
  } else if (OB_FAIL(write_string_to_file_(config_path, config_value))) {
    LOG_WARN("set cgroup config failed", K(ret), K(config_path), K(config_value));
  }
  return ret;
}

int ObCgroupCtrl::set_cpu_shares_(
    const uint64_t tenant_id, const double cpu, const uint64_t group_id, const bool is_background)
{
  int ret = OB_SUCCESS;
  char group_path[PATH_BUFSIZE];
  char cpu_shares_value[VALUE_BUFSIZE + 1];

  int32_t cpu_shares = static_cast<int32_t>(cpu * DEFAULT_CPU_SHARES);
  snprintf(cpu_shares_value, VALUE_BUFSIZE, "%d", cpu_shares);
  if (OB_FAIL(get_group_path(group_path, PATH_BUFSIZE, tenant_id, group_id, is_background))) {
    LOG_WARN("fail get group path", K(tenant_id), K(ret));
  } else if (OB_FAIL(set_cgroup_config_(group_path, CPU_SHARES_FILE, cpu_shares_value))) {
    LOG_WARN("set cpu shares failed", K(ret), K(group_path), K(tenant_id));
  } else {
    _LOG_INFO("set cpu shares success, "
              "group_path=%s, cpu=%.2f, "
              "cpu_shares_value=%s, tenant_id=%lu",
              group_path, cpu,
              cpu_shares_value, tenant_id);
  }
  return ret;
}

int ObCgroupCtrl::set_cpu_shares(
    const uint64_t tenant_id, const double cpu, const uint64_t group_id, const bool is_background)
{
  int ret = OB_SUCCESS;
  if (!(is_background && GCONF.enable_global_background_resource_isolation)) {
    if (OB_FAIL(set_cpu_shares_(tenant_id, cpu, group_id, false /* is_background */))) {
      LOG_WARN("set cpu shares failed", K(ret), K(tenant_id));
    }
  }
  if (OB_SUCC(ret) && GCONF.enable_global_background_resource_isolation) {
    if (OB_FAIL(set_cpu_shares_(tenant_id, cpu, group_id, true /* is_background */))) {
      LOG_WARN("set background cpu shares failed", K(ret), K(tenant_id));
    }
  }
  return ret;
}

int ObCgroupCtrl::get_cpu_shares(
    const uint64_t tenant_id, double &cpu, const uint64_t group_id, const bool is_background)
{
  int ret = OB_SUCCESS;
  cpu = 0;
  char group_path[PATH_BUFSIZE];
  char cpu_shares_value[VALUE_BUFSIZE + 1];

  if (OB_FAIL(get_group_path(group_path,
          PATH_BUFSIZE,
          tenant_id,
          group_id,
          is_background && GCONF.enable_global_background_resource_isolation))) {
    LOG_WARN("fail get group path", K(tenant_id), K(ret));
  } else if (OB_FAIL(get_cgroup_config_(group_path, CPU_SHARES_FILE, cpu_shares_value))) {
    LOG_WARN("get cpu shares failed", K(ret), K(group_path), K(tenant_id));
  } else {
    cpu_shares_value[VALUE_BUFSIZE] = '\0';
    cpu = 1.0 * atoi(cpu_shares_value) / DEFAULT_CPU_SHARES;
  }
  return ret;
}

int ObCgroupCtrl::compare_cpu(const double cpu1, const double cpu2, int &compare_ret)
{
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

int ObCgroupCtrl::set_cpu_cfs_quota_(
    const uint64_t tenant_id, const double cpu, const uint64_t group_id, const bool is_background)
{
  int ret = OB_SUCCESS;

  double target_cpu = cpu;
  double base_cpu = -1;

  if (-1 != target_cpu) {
    // background quota limit
    if (is_valid_tenant_id(tenant_id) && is_background) {
      int compare_ret = 0;
      if (OB_FAIL(get_cpu_cfs_quota(OB_INVALID_TENANT_ID, base_cpu, OB_INVALID_GROUP_ID, is_background))) {
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
      if (OB_FAIL(get_cpu_cfs_quota(tenant_id, tenant_cpu, OB_INVALID_GROUP_ID, is_background))) {
        LOG_WARN("get tenant cpu cfs quota failed", K(ret), K(tenant_id));
      } else if (OB_FAIL(compare_cpu(target_cpu, tenant_cpu, compare_ret))) {
        LOG_WARN("compare cpu failed", K(ret), K(target_cpu), K(tenant_cpu));
      } else if (compare_ret > 0) {
        target_cpu = tenant_cpu;
      }
    }
  }

  if (OB_SUCC(ret)) {
    double current_cpu = -1;
    char group_path[PATH_BUFSIZE];
    int compare_ret = 0;
    if (OB_FAIL(get_cpu_cfs_quota(tenant_id, current_cpu, group_id, is_background))) {
      LOG_WARN("get cpu cfs quota failed", K(ret), K(group_path), K(tenant_id), K(group_id));
    } else if (OB_FAIL(get_group_path(group_path, PATH_BUFSIZE, tenant_id, group_id, is_background))) {
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
      _LOG_WARN("set cpu cfs quota failed, "
                "tenant_id=%lu, group_id=%lu, "
                "cpu=%.2f, target_cpu=%.2f, "
                "current_cpu=%.2f, group_path=%s",
                tenant_id, group_id,
                cpu, target_cpu,
                current_cpu, group_path);
    } else {
      _LOG_INFO("set cpu cfs quota success, "
                "tenant_id=%lu, group_id=%lu, "
                "cpu=%.2f, target_cpu=%.2f, "
                "current_cpu=%.2f, group_path=%s",
                tenant_id, group_id,
                cpu, target_cpu,
                current_cpu, group_path);
    }
  }
  return ret;
}

int ObCgroupCtrl::recursion_dec_cpu_cfs_quota_(const char *group_path, const double cpu)
{
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
                (OB_SUCC(ObCgroupCtrl::compare_cpu(cpu_, current_cpu, compare_ret)) && compare_ret > 0)) {
        // do nothing
      } else if (OB_FAIL(ObCgroupCtrl::set_cpu_cfs_quota_by_path_(curr_path, cpu_))) {
        LOG_WARN("set cpu cfs quota failed", K(curr_path), K(cpu_));
      }
      return ret;
    }

  private:
    const double cpu_;
  };
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
      _LOG_INFO("set cpu quota success, "
                "group_path=%s, cpu=%.2f, "
                "cfs_quota_us=%d, cfs_period_us=%d",
                group_path, cpu,
                cfs_quota_us, cfs_period_us);
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

int ObCgroupCtrl::set_cpu_cfs_quota(
    const uint64_t tenant_id, const double cpu, const uint64_t group_id, const bool is_background)
{
  int ret = OB_SUCCESS;
  if (!(is_background && GCONF.enable_global_background_resource_isolation)) {
    if (OB_FAIL(set_cpu_cfs_quota_(tenant_id, cpu, group_id, false /* is_background */))) {
      LOG_WARN("set cpu quota failed", K(ret), K(tenant_id));
    }
  }
  if (OB_SUCC(ret) && GCONF.enable_global_background_resource_isolation) {
    if (OB_FAIL(set_cpu_cfs_quota_(tenant_id, cpu, group_id, true /* is_background */))) {
      LOG_WARN("set background cpu quota directory failed", K(ret), K(tenant_id));
    }
  }
  return OB_SUCCESS;
}

int ObCgroupCtrl::get_cpu_cfs_quota(
    const uint64_t tenant_id, double &cpu, const uint64_t group_id, const bool is_background)
{
  int ret = OB_SUCCESS;
  cpu = 0;
  char group_path[PATH_BUFSIZE];
  if (OB_FAIL(get_group_path(group_path,
          PATH_BUFSIZE,
          tenant_id,
          group_id,
          is_background && GCONF.enable_global_background_resource_isolation))) {
    LOG_WARN("fail get group path", K(tenant_id), K(ret));
  } else if (OB_FAIL(get_cpu_cfs_quota_by_path_(group_path, cpu))) {
    LOG_WARN("get cpu cfs quota failed", K(ret), K(group_path), K(tenant_id));
  }
  return ret;
}

int ObCgroupCtrl::get_cpu_time_(
    const uint64_t tenant_id, int64_t &cpu_time, const uint64_t group_id, const bool is_background)
{
  int ret = OB_SUCCESS;
  cpu_time = 0;
  char group_path[PATH_BUFSIZE];
  char cpuacct_usage_value[VALUE_BUFSIZE + 1];

  if (OB_FAIL(get_group_path(group_path,
          PATH_BUFSIZE,
          tenant_id,
          group_id,
          is_background && GCONF.enable_global_background_resource_isolation))) {
    LOG_WARN("fail get group path", K(tenant_id), K(ret));
  } else if (OB_FAIL(get_cgroup_config_(group_path, CPUACCT_USAGE_FILE, cpuacct_usage_value))) {
    LOG_WARN("get cpuacct.usage failed", K(ret), K(group_path), K(tenant_id));
  } else {
    cpuacct_usage_value[VALUE_BUFSIZE] = '\0';
    cpu_time = strtoull(cpuacct_usage_value, NULL, 10) / 1000;
  }
  return ret;
}

int ObCgroupCtrl::get_cpu_time(const uint64_t tenant_id, int64_t &cpu_time, const uint64_t group_id)
{
  int ret = OB_SUCCESS;
  cpu_time = 0;
  if (OB_FAIL(get_cpu_time_(tenant_id, cpu_time, group_id, false /* is_background */))) {
    LOG_WARN("get cpu_time failed", K(ret), K(tenant_id), K(group_id));
  } else if (GCONF.enable_global_background_resource_isolation) {
    int64_t background_cpu_time = 0;
    if (OB_FAIL(get_cpu_time_(tenant_id, background_cpu_time, group_id, true /* is_background */))) {
      LOG_WARN("get background_cpu_time failed", K(ret), K(tenant_id), K(group_id));
    }
    cpu_time += background_cpu_time;
  }
  return ret;
}

int ObCgroupCtrl::get_throttled_time_(
    const uint64_t tenant_id, int64_t &throttled_time, const uint64_t group_id, const bool is_background)
{
  int ret = OB_SUCCESS;
  throttled_time = 0;
  char group_path[PATH_BUFSIZE];
  char cpu_stat_value[VALUE_BUFSIZE + 1];

  if (OB_FAIL(get_group_path(group_path,
          PATH_BUFSIZE,
          tenant_id,
          group_id,
          is_background && GCONF.enable_global_background_resource_isolation))) {
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

int ObCgroupCtrl::get_throttled_time(const uint64_t tenant_id, int64_t &throttled_time, const uint64_t group_id)
{
  int ret = OB_SUCCESS;
  throttled_time = 0;
  if (OB_FAIL(get_throttled_time_(tenant_id, throttled_time, group_id, false /* is_background */))) {
    LOG_WARN("get throttled_time failed", K(ret), K(tenant_id), K(group_id));
  } else if (GCONF.enable_global_background_resource_isolation) {
    int64_t background_throttled_time = 0;
    if (OB_FAIL(get_throttled_time_(tenant_id, background_throttled_time, group_id, true /* is_background */))) {
      LOG_WARN("get background_throttled_time failed", K(ret), K(tenant_id), K(group_id));
    }
    throttled_time += background_throttled_time;
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
    LOG_WARN("invalid arguments.", K(curr_path), K(ret));
  } else if (OB_FAIL(check_cgroup_root_dir())) {
    LOG_WARN("check cgroup root dir failed", K(ret));
  } else if (0 == strcmp(OBSERVER_ROOT_CGROUP_DIR, curr_path)) {
    // do nothing
  } else if (OB_FAIL(FileDirectoryUtils::is_exists(curr_path, exist_cgroup))) {
    LOG_WARN("fail check file exist", K(curr_path), K(ret));
  } else if (!exist_cgroup && OB_FAIL(FileDirectoryUtils::create_directory(curr_path))) {
    LOG_WARN("create tenant cgroup dir failed", K(ret), K(curr_path));
  }
  if (OB_SUCC(ret)) {
    char cgroup_clone_children_value[VALUE_BUFSIZE + 1];
    snprintf(cgroup_clone_children_value, VALUE_BUFSIZE, "1");
    if (OB_FAIL(set_cgroup_config_(curr_path, CGROUP_CLONE_CHILDREN_FILE, cgroup_clone_children_value))) {
      LOG_WARN("set cgroup_clone_children failed", K(ret), K(curr_path));
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
