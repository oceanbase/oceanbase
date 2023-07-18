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
namespace oceanbase
{
namespace common
{
class ObString;
}
namespace share
{
class ObGroupName;

enum ObCgId
{
#define CGID_DEF(name, id, worker_concurrency...) name = id,
#include "ob_group_list.h"
#undef CGID_DEF
  OBCG_MAXNUM,
};

class ObCgWorkerConcurrency
{
public:
  ObCgWorkerConcurrency() : worker_concurrency_(1) {}
  void set_worker_concurrency(uint64_t worker_concurrency = 1) { worker_concurrency_ = worker_concurrency; }
  uint64_t worker_concurrency_;
};

class ObCgSet
{
  ObCgSet()
  {
#define CGID_DEF(name, id, worker_concurrency...) names_[id] = #name; worker_concurrency_[id].set_worker_concurrency(worker_concurrency);
#include "ob_group_list.h"
#undef CGID_DEF
  }

public:

  const char *name_of_id(int64_t id) const
  {
    const char *name = "OBCG_DEFAULT";
    if (id >= 0 && id < OBCG_MAXNUM) {
      name = names_[id];
    }
    return name;
  }

  uint64_t get_worker_concurrency(int64_t id) const
  {
    uint64_t worker_concurrency = 1;
    if (id > 0 && id < OBCG_MAXNUM) {
      worker_concurrency = worker_concurrency_[id].worker_concurrency_;
    }
    return worker_concurrency;
  }

  static ObCgSet &instance()
  {
    return instance_;
  }

private:
  static ObCgSet instance_;

  const char *names_[OBCG_MAXNUM];
  ObCgWorkerConcurrency worker_concurrency_[OBCG_MAXNUM];
};

struct OBGroupIOInfo final
{
public:
  OBGroupIOInfo()
  : min_percent_(0),
    max_percent_(100),
    weight_percent_(0)
  {}
  int init(const int64_t min_percent, const int64_t max_percent, const int64_t weight_percent);
  void reset();
  bool is_valid() const;
public:
  uint64_t min_percent_;
  uint64_t max_percent_;
  uint64_t weight_percent_;
};

class ObCgroupCtrl
{
public:
  ObCgroupCtrl()
      : valid_(false),
        last_cpu_usage_(0),
        last_usage_check_time_(0)
  {}
  ~ObCgroupCtrl() {}
  int init();
  void destroy() { /* 进程退出后tid会自动从cgroup tasks中删除 */ }
  bool is_valid() { return valid_; }

  // 删除租户cgroup规则
  int remove_tenant_cgroup(const uint64_t tenant_id);

  int add_self_to_cgroup(const uint64_t tenant_id, int64_t group_id = INT64_MAX);

  // 从指定租户cgroup组移除指定tid
  int remove_self_from_cgroup(const uint64_t tenant_id);

  // 后台线程绑定接口
  int add_self_to_group(const uint64_t tenant_id,
                          const uint64_t group_id);
  // 设定指定租户cgroup组的cpu.shares
  int set_cpu_shares(const int32_t cpu_shares, const uint64_t tenant_id, int64_t group_id = INT64_MAX);
  int get_cpu_shares(int32_t &cpu_shares, const uint64_t tenant_id, int64_t group_id = INT64_MAX);
  int set_cpu_shares(const uint64_t tenant_id,
                     const int level,
                     const common::ObString &group,
                     const int32_t cpu_shares);
  // 设定指定租户cgroup组的cpu.cfs_quota_us
  int set_cpu_cfs_quota(const int32_t cfs_quota_us, const uint64_t tenant_id, int64_t group_id = INT64_MAX);
  int get_cpu_cfs_quota(int32_t &cfs_quota_us, const uint64_t tenant_id, int64_t group_id = INT64_MAX);
  int set_cpu_cfs_quota(const uint64_t tenant_id,
                        const int level,
                        const common::ObString &group,
                        const int32_t cfs_quota_us);
  // 获取某个租户的group 的 period 值，用于计算 cfs_quota_us
  int get_cpu_cfs_period(const uint64_t tenant_id,
                         const int level,
                         const common::ObString &group,
                         int32_t &cfs_period_us);
  int get_cpu_cfs_period(int32_t &cfs_period_us,const uint64_t tenant_id, int64_t group_id);
  // 获取某个cgroup组的cpuacct.usage, 即cpu time
  int get_cpu_time(const uint64_t tenant_id, int64_t &cpu_time);
  // 获取某段时间内cpu占用率
  int get_cpu_usage(const uint64_t tenant_id, int32_t &cpu_usage);


  // 设定指定租户cgroup组的iops，直接更新到租户io_config
  int set_group_iops(const uint64_t tenant_id,
                     const int level, // UNUSED
                     const int64_t group_id,
                     const OBGroupIOInfo &group_io);
  // 删除正在使用的plan反应到IO层：重置所有IOPS
  int reset_all_group_iops(const uint64_t tenant_id,
                           const int level);// UNUSED
  // 删除directive反应到IO层：重置IOPS
  int reset_group_iops(const uint64_t tenant_id,
                       const int level, // UNUSED
                       const common::ObString &consumer_group);
  // 删除group反应到IO层：停用对应的group结构
  int delete_group_iops(const uint64_t tenant_id,
                        const int level, // UNUSED
                        const common::ObString &consumer_group);

  // 根据 consumer group 动态创建 cgroup
  int create_user_tenant_group_dir(
      const uint64_t tenant_id,
      int level,
      const common::ObString &group);
private:
  const char *root_cgroup_  = "cgroup";
  const char *other_cgroup_ = "cgroup/other";
  // 10:1, 确保系统满负载场景下 SYS 能有足够资源
  static const int32_t DEFAULT_SYS_SHARE = 1024;
  static const int32_t DEFAULT_USER_SHARE = 4096;
  static const int32_t PATH_BUFSIZE = 512;
  static const int32_t VALUE_BUFSIZE = 32;
  static const int32_t GROUP_NAME_BUFSIZE = 129;
  // 使用 ObCgroupCtrl 之前需要判断 group_ctrl 对象是否 valid，若为 false 则跳过 cgroup 机制
  //  为 false 可能的原因是 cgroup 目录没有操作权限、操作系统不支持 cgroup 等。
  bool valid_;
  int64_t last_cpu_usage_;
  int64_t last_usage_check_time_;

private:
  int init_cgroup_root_dir_(const char *cgroup_path);
  int init_cgroup_dir_(const char *cgroup_path);
  int write_string_to_file_(const char *filename, const char *content);
  int get_string_from_file_(const char *filename, char content[VALUE_BUFSIZE]);
  int get_task_path(
      char *task_path,
      int path_bufsize,
      const uint64_t tenant_id,
      int level,
    const char *group);
  int get_task_path(
      char *task_path,
      int path_bufsize,
      const uint64_t tenant_id,
      int level,
      const common::ObString &group);
  int get_group_path(
      char *group_path,
      int path_bufsize,
      const uint64_t tenant_id,
      int64_t group_id = INT64_MAX);
  int get_group_path(
      char *group_path,
      int path_bufsize,
      const uint64_t tenant_id,
      int level,
      const char *group);
  int get_group_path(
      char *group_path,
      int path_bufsize,
      const uint64_t tenant_id,
      int level,
      const common::ObString &group);
  int set_cpu_shares(const char *cgroup_path, const int32_t cpu_shares);
  int get_group_info_by_group_id(const uint64_t tenant_id,
                                 int64_t group_id,
                                 share::ObGroupName &group_name);
  enum { NOT_DIR = 0, LEAF_DIR, REGULAR_DIR };
  int which_type_dir_(const char *curr_path, int &result);
  int remove_dir_(const char *curr_dir);
  int recursion_remove_group_(const char *curr_path);
};

}  // share
}  // oceanbase

#endif  // OB_CGROUP_CTRL_H
