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

#ifndef OB_CGROUP_CTRL_H
#define OB_CGROUP_CTRL_H

#include <stdint.h>
#include <sys/types.h>

namespace oceanbase {
namespace common {
class ObString;
}
namespace share {
class ObGroupName;
}
namespace omt {

class ObCgroupCtrl {
public:
  ObCgroupCtrl() : valid_(false), last_cpu_usage_(0), last_usage_check_time_(0)
  {}
  ~ObCgroupCtrl()
  {}
  int init();
  // Tid will be automatically deleted from cgroup tasks after the process exits
  void destroy()
  {}
  bool is_valid()
  {
    return valid_;
  }

  int create_tenant_cgroup(const uint64_t tenant_id);

  int remove_tenant_cgroup(const uint64_t tenant_id);

  int add_thread_to_cgroup(const uint64_t tenant_id, const pid_t tid);

  int add_thread_to_cgroup(const uint64_t tenant_id, int level, const char* group, const pid_t tid);

  int add_thread_to_cgroup(const uint64_t tenant_id, int64_t group_id, const pid_t tid);

  int remove_thread_from_cgroup(const uint64_t tenant_id, const pid_t tid);

  int set_cpu_shares(const uint64_t tenant_id, const int32_t cpu_shares);
  int get_cpu_shares(const uint64_t tenant_id, int32_t& cpu_shares);
  int set_cpu_shares(
      const uint64_t tenant_id, const int level, const common::ObString& group, const int32_t cpu_shares);

  int set_cpu_cfs_quota(const uint64_t tenant_id, const int32_t cfs_quota_us);
  int get_cpu_cfs_quota(const uint64_t tenant_id, int32_t& cfs_quota_us);
  int set_cpu_cfs_quota(
      const uint64_t tenant_id, const int level, const common::ObString& group, const int32_t cfs_quota_us);
  int get_cpu_cfs_period(
      const uint64_t tenant_id, const int level, const common::ObString& group, int32_t& cfs_period_us);
  int get_cpu_usage(const uint64_t tenant_id, int32_t& cpu_usage);
  int create_user_tenant_group_dir(const uint64_t tenant_id, int level, const common::ObString& group);

private:
  // The observer's initialization script will iterator the cgroup soft connection,
  // the directory layout is as follows:
  //  ---bin/
  //   |-etc/
  //   |-...
  //   |_cgroup --> /sys/fs/cgroup/oceanbase/observer_name
  //                        |
  //                        |- sys
  //                        |   |- other
  //                        |   |- tenant_1
  //                        |   |- ...
  //                        |   |_ tenant_999
  //                        |
  //                        |_ user
  //                            |- tenant_1001
  //                            |- tenant_1002
  //                            |_ ...
  //
  const char* root_cgroup_ = "cgroup";
  const char* sys_cgroup_ = "cgroup/sys";
  const char* other_cgroup_ = "cgroup/sys/other";
  const char* user_cgroup_ = "cgroup/user";
  // 10:1, Ensure that SYS has sufficient resources under the full load scenario of the system
  static const int32_t DEFAULT_SYS_SHARE = 10240;
  static const int32_t DEFAULT_USER_SHARE = 1024;
  static const int32_t PATH_BUFSIZE = 512;
  static const int32_t VALUE_BUFSIZE = 32;
  static const int32_t GROUP_NAME_BUFSIZE = 129;
  // Before using ObCgroupCtrl, you need to determine whether the group_ctrl object is valid,
  // if it is false, skip the cgroup mechanism
  // It is false. The possible reason is that the cgroup directory does not have operation permissions,
  // and the operating system does not support cgroups.
  bool valid_;
  int64_t last_cpu_usage_;
  int64_t last_usage_check_time_;

private:
  int init_cgroup_root_dir_(const char* cgroup_path);
  int init_cgroup_dir_(const char* cgroup_path);
  int write_string_to_file_(const char* filename, const char* content);
  int get_string_from_file_(const char* filename, char content[VALUE_BUFSIZE]);
  int get_task_path(char* task_path, int path_bufsize, const uint64_t tenant_id, int level, const char* group);
  int get_task_path(
      char* task_path, int path_bufsize, const uint64_t tenant_id, int level, const common::ObString& group);
  int get_group_path(char* group_path, int path_bufsize, const uint64_t tenant_id, int level, const char* group);
  int get_group_path(
      char* group_path, int path_bufsize, const uint64_t tenant_id, int level, const common::ObString& group);
  int set_cpu_shares(const char* cgroup_path, const int32_t cpu_shares);
  int get_group_info_by_group_id(
      const uint64_t tenant_id, int64_t group_id, share::ObGroupName& group_name, int& level);
  int init_default_user_tenant_group(const uint64_t tenant_id, int level, const char* group, int32_t cpu_shares);
};

}  // namespace omt
}  // namespace oceanbase

#endif  // OB_CGROUP_CTRL_H
