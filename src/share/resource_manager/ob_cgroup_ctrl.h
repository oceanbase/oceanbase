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
#include "share/io/ob_io_define.h"
namespace oceanbase
{
namespace common
{
class ObString;
}
namespace omt
{
class ObTenant;
}
namespace share
{
class ObGroupName;
class ObTenantBase;
class ObResourcePlanManager;

typedef enum  : uint64_t {
  DEFAULT = 0,
  CRITICAL = 1 << 0,
  INVALID = UINT64_MAX
} group_flags_t;

typedef enum  : uint64_t {
  NORMAL_EXPAND = 0,
  QUICK_EXPAND
} group_expand_mode_t;

enum ObCgId
{
#define CGID_DEF(name, id, ...) name = id,
#include "ob_group_list.h"
#undef CGID_DEF
  OBCG_MAXNUM,
};

class ObCgInfo
{
public:
  ObCgInfo() : name_(nullptr), is_critical_(false), worker_concurrency_(1), is_quick_expand_(false) {}
  void set_name(const char *name) { name_ = name; }
  void set_args(group_flags_t flags = group_flags_t::DEFAULT, uint64_t worker_concurrency = 1, group_expand_mode_t expand_mode = group_expand_mode_t::NORMAL_EXPAND)
  {
    set_flags(flags);
    set_worker_concurrency(worker_concurrency);
    set_expand_mode(expand_mode);
  }
  void set_flags(group_flags_t flags = group_flags_t::DEFAULT)
  {
    if (DEFAULT == flags || share::INVALID == flags) {
      // do nothing
    } else {
      if (CRITICAL & flags) {
        is_critical_ = true;
      }
    }
  }
  void set_worker_concurrency(uint64_t worker_concurrency = 1) { worker_concurrency_ = worker_concurrency; }
  void set_expand_mode(group_expand_mode_t expand_mode = group_expand_mode_t::NORMAL_EXPAND)
  {
    if (expand_mode == group_expand_mode_t::QUICK_EXPAND) {
      is_quick_expand_ = true;
    }
  }
  const char *name_;
  bool is_critical_;
  uint64_t worker_concurrency_;
  bool is_quick_expand_;
};

class ObCgSet
{
  ObCgSet()
  {
#define CGID_DEF(name, id, args...) group_infos_[id].set_name(#name); group_infos_[id].set_args(args);
#include "ob_group_list.h"
#undef CGID_DEF
  }

public:

  const char *name_of_id(int64_t id) const
  {
    const char *name = "OBCG_DEFAULT";
    if (id >= 0 && id < OBCG_MAXNUM) {
      name = group_infos_[id].name_;
    }
    return name;
  }

  uint64_t get_worker_concurrency(int64_t id) const
  {
    uint64_t worker_concurrency = 1;
    if (id >= 0 && id < OBCG_MAXNUM) {
      worker_concurrency = group_infos_[id].worker_concurrency_;
    }
    return worker_concurrency;
  }

  bool is_group_critical(int64_t id) const
  {
    bool is_group_critical = false;
    if (id >= 0 && id < OBCG_MAXNUM) {
      is_group_critical = group_infos_[id].is_critical_;
    }
    return is_group_critical;
  }

  bool is_group_quick_expand(int64_t id) const
  {
    bool is_group_quick_expand = false;
    if (id >= 0 && id < OBCG_MAXNUM) {
      is_group_quick_expand = group_infos_[id].is_quick_expand_;
    }
    return is_group_quick_expand;
  }

  static ObCgSet &instance()
  {
    return instance_;
  }

private:
  static ObCgSet instance_;
  ObCgInfo group_infos_[OBCG_MAXNUM];
};

struct ObGroupIOInfo final
{
public:
  ObGroupIOInfo()
  : group_name_(nullptr),
    min_percent_(0),
    max_percent_(100),
    weight_percent_(0),
    max_net_bandwidth_percent_(100),
    net_bandwidth_weight_percent_(0)
  {}
  int init(const char *name,
           const int64_t min_percent, const int64_t max_percent, const int64_t weight_percent,
           const int64_t max_net_bandwidth_percent, const int64_t net_bandwidth_weight_percent);
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(group_name), K_(min_percent), K_(max_percent), K_(weight_percent), K_(max_net_bandwidth_percent), K_(net_bandwidth_weight_percent));
public:
  const char *group_name_;
  uint64_t min_percent_;
  uint64_t max_percent_;
  uint64_t weight_percent_;
  uint64_t max_net_bandwidth_percent_;
  uint64_t net_bandwidth_weight_percent_;
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
  int regist_observer_to_cgroup(const char * cgroup_dir);
  bool check_cgroup_status();
  static int check_cgroup_root_dir();
  static int check_pid_in_procs();
  void destroy();
  bool is_valid() { return valid_; }

  bool is_valid_group_name(common::ObString &group_name);
  static int compare_cpu(const double cpu1, const double cpu2, int &compare_ret);

  // 删除租户cgroup规则
  int remove_cgroup(const uint64_t tenant_id, const uint64_t group_id = OB_INVALID_GROUP_ID, const bool is_background = false);
  // 设定指定租户cgroup组的cpu.shares
  int set_cpu_shares(const uint64_t tenant_id, const double cpu, const uint64_t group_id = OB_INVALID_GROUP_ID, const bool is_background = false);
  int get_cpu_shares(const uint64_t tenant_id, double &cpu, const uint64_t group_id = OB_INVALID_GROUP_ID, const bool is_background = false);
  // 设定指定租户cgroup组的cpu.cfs_quota_us
  int set_cpu_cfs_quota(const uint64_t tenant_id, const double cpu, const uint64_t group_id = OB_INVALID_GROUP_ID, const bool is_background = false);
  int get_cpu_cfs_quota(const uint64_t tenant_id, double &cpu, const uint64_t group_id = OB_INVALID_GROUP_ID, const bool is_background = false);
  // 获取某个cgroup组的cpuacct.usage, 即cpu time
  int get_cpu_time(const uint64_t tenant_id, int64_t &cpu_time, const uint64_t group_id = OB_INVALID_GROUP_ID);
  int get_throttled_time(const uint64_t tenant_id, int64_t &throttled_time, const uint64_t group_id = OB_INVALID_GROUP_ID);
  int get_group_info_by_group_id(const uint64_t tenant_id,
                                 uint64_t group_id,
                                 share::ObGroupName &group_name);

  int get_group_path(
      char *group_path,
      int path_bufsize,
      const uint64_t tenant_id,
      uint64_t group_id = OB_INVALID_GROUP_ID,
      const bool is_background = false);

  class DirProcessor
  {
  public:
    DirProcessor() = default;
    ~DirProcessor() = default;
    virtual int handle_dir(const char *group_path, const bool is_top_dir=false) = 0;
  };

private:
  static const int32_t PATH_BUFSIZE = 512;
  static const int32_t VALUE_BUFSIZE = 64;
  static const int32_t GROUP_NAME_BUFSIZE = 129;
  // 使用 ObCgroupCtrl 之前需要判断 group_ctrl 对象是否 valid，若为 false 则跳过 cgroup 机制
  //  为 false 可能的原因是 cgroup 目录没有操作权限、操作系统不支持 cgroup 等。
  bool valid_;
  int64_t last_cpu_usage_;
  int64_t last_usage_check_time_;

private:
  friend class oceanbase::omt::ObTenant;
  friend class oceanbase::share::ObTenantBase;
  friend class oceanbase::share::ObResourcePlanManager;
  friend int oceanbase::lib::SET_GROUP_ID(uint64_t group_id, bool is_background);
  int add_self_to_cgroup_(const uint64_t tenant_id, const uint64_t group_id = OBCG_DEFAULT, const bool is_background = false);
  int add_thread_to_cgroup_(const int64_t tid,const uint64_t tenant_id, const uint64_t group_id = OBCG_DEFAULT, const bool is_background = false);
  static int init_dir_(const char *curr_dir);
  static int init_full_dir_(const char *curr_path);
  static int write_string_to_file_(const char *filename, const char *content);
  static int get_string_from_file_(const char *filename, char content[VALUE_BUFSIZE]);
  enum { NOT_DIR = 0, LEAF_DIR, REGULAR_DIR };
  int which_type_dir_(const char *curr_path, int &result);
  int recursion_remove_group_(const char *curr_path, bool if_remove_top = true);
  int recursion_process_group_(const char *curr_path, DirProcessor *processor_ptr, bool is_top_dir = false);
  int remove_cgroup_(const uint64_t tenant_id, const uint64_t group_id = OB_INVALID_GROUP_ID, const bool is_background = false);
  static int remove_dir_(const char *curr_dir);
  static int get_cgroup_config_(const char *group_path, const char *config_name, char *config_value);
  static int set_cgroup_config_(const char *group_path, const char *config_name, char *config_value);
  int set_cpu_shares_(const uint64_t tenant_id, const double cpu, const uint64_t group_id = OB_INVALID_GROUP_ID, const bool is_background = false);
  int set_cpu_cfs_quota_(const uint64_t tenant_id, const double cpu, const uint64_t group_id = OB_INVALID_GROUP_ID, const bool is_background = false);
  static int set_cpu_cfs_quota_by_path_(const char *group_path, const double cpu);
  static int get_cpu_cfs_quota_by_path_(const char *group_path, double &cpu);
  static int dec_cpu_cfs_quota_(const char *curr_path, const double cpu);
  int recursion_dec_cpu_cfs_quota_(const char *curr_path, const double cpu);
  int get_cpu_time_(const uint64_t tenant_id, int64_t &cpu_time, const uint64_t group_id = OB_INVALID_GROUP_ID, const bool is_background = false);
  int get_throttled_time_(const uint64_t tenant_id, int64_t &throttled_time, const uint64_t group_id = OB_INVALID_GROUP_ID, const bool is_background = false);
};

}  // share
}  // oceanbase

#endif  // OB_CGROUP_CTRL_H
