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
namespace share
{
class ObGroupName;

typedef enum  : uint64_t {
  DEFAULT = 0,
  CRITICAL = 1 << 0,
  INVALID = UINT64_MAX
} group_flags_t;

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
  ObCgInfo() : name_(nullptr), is_critical_(false), worker_concurrency_(1) {}
  void set_name(const char *name) { name_ = name; }
  void set_args(group_flags_t flags = DEFAULT, uint64_t worker_concurrency = 1)
  {
    set_flags(flags);
    set_worker_concurrency(worker_concurrency);
  }
  void set_flags(group_flags_t flags = DEFAULT)
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
  const char *name_;
  bool is_critical_;
  uint64_t worker_concurrency_;
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

  static ObCgSet &instance()
  {
    return instance_;
  }

private:
  static ObCgSet instance_;
  ObCgInfo group_infos_[OBCG_MAXNUM];
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

  bool is_valid_group_name(common::ObString &group_name);
  static int compare_cpu(const double cpu1, const double cpu2, int &compare_ret);

  // 删除租户cgroup规则
  int remove_cgroup(const uint64_t tenant_id, const uint64_t group_id = OB_INVALID_GROUP_ID, const char *base_path = "");
  int remove_both_cgroup(const uint64_t tenant_id, const uint64_t group_id = OB_INVALID_GROUP_ID, const char *base_path = "");
  static int remove_dir_(const char *curr_dir);

  int add_self_to_cgroup(const uint64_t tenant_id, uint64_t group_id = OBCG_DEFAULT, const char *base_path = "");

  // 从指定租户cgroup组移除指定tid
  int remove_self_from_cgroup(const uint64_t tenant_id);

  static int get_cgroup_config_(const char *group_path, const char *config_name, char *config_value);
  static int set_cgroup_config_(const char *group_path, const char *config_name, char *config_value);
  // 设定指定租户cgroup组的cpu.shares
  int set_cpu_shares(const uint64_t tenant_id, const double cpu, const uint64_t group_id = OB_INVALID_GROUP_ID, const char *base_path = "");
  int set_both_cpu_shares(const uint64_t tenant_id, const double cpu, const uint64_t group_id = OB_INVALID_GROUP_ID, const char *base_path = "");
  int get_cpu_shares(const uint64_t tenant_id, double &cpu, const uint64_t group_id = OB_INVALID_GROUP_ID, const char *base_path = "");
  // 设定指定租户cgroup组的cpu.cfs_quota_us
  int set_cpu_cfs_quota(const uint64_t tenant_id, const double cpu, const uint64_t group_id = OB_INVALID_GROUP_ID, const char *base_path = "");
  static int set_cpu_cfs_quota_by_path_(const char *group_path, const double cpu);
  static int get_cpu_cfs_quota_by_path_(const char *group_path, double &cpu);
  static int dec_cpu_cfs_quota_(const char *curr_path, const double cpu);
  int recursion_dec_cpu_cfs_quota_(const char *curr_path, const double cpu);
  int set_both_cpu_cfs_quota(const uint64_t tenant_id, const double cpu, const uint64_t group_id = OB_INVALID_GROUP_ID, const char *base_path = "");
  int get_cpu_cfs_quota(const uint64_t tenant_id, double &cpu, const uint64_t group_id = OB_INVALID_GROUP_ID, const char *base_path = "");
  // 获取某个cgroup组的cpuacct.usage, 即cpu time
  int get_cpu_time(const uint64_t tenant_id, int64_t &cpu_time, const uint64_t group_id = OB_INVALID_GROUP_ID, const char *base_path = "");
  int get_throttled_time(const uint64_t tenant_id, int64_t &throttled_time, const uint64_t group_id = OB_INVALID_GROUP_ID, const char *base_path = "");
  // 设定指定租户cgroup组的iops，直接更新到租户io_config
  int set_group_iops(const uint64_t tenant_id,
                     const uint64_t group_id,
                     const OBGroupIOInfo &group_io);
  // 删除正在使用的plan反应到IO层：重置所有IOPS
  int reset_all_group_iops(const uint64_t tenant_id);
  // 删除directive反应到IO层：重置IOPS
  int reset_group_iops(const uint64_t tenant_id,
                       const common::ObString &consumer_group);
  // 删除group反应到IO层：停用对应的group结构
  int delete_group_iops(const uint64_t tenant_id,
                        const common::ObString &consumer_group);
  int get_group_info_by_group_id(const uint64_t tenant_id,
                                 uint64_t group_id,
                                 share::ObGroupName &group_name);

  class DirProcessor
  {
  public:
    DirProcessor() = default;
    ~DirProcessor() = default;
    virtual int handle_dir(const char *group_path, bool is_top_dir=false) = 0;
  };

private:
  const char *root_cgroup_  = "cgroup";
  const char *other_cgroup_ = "cgroup/other";
  // 10:1, 确保系统满负载场景下 SYS 能有足够资源
  static const int32_t DEFAULT_SYS_SHARE = 1024;
  static const int32_t DEFAULT_USER_SHARE = 4096;
  static const int32_t PATH_BUFSIZE = 512;
  static const int32_t VALUE_BUFSIZE = 64;
  static const int32_t GROUP_NAME_BUFSIZE = 129;
  // 使用 ObCgroupCtrl 之前需要判断 group_ctrl 对象是否 valid，若为 false 则跳过 cgroup 机制
  //  为 false 可能的原因是 cgroup 目录没有操作权限、操作系统不支持 cgroup 等。
  bool valid_;
  int64_t last_cpu_usage_;
  int64_t last_usage_check_time_;

private:
  int init_cgroup_root_dir_(const char *cgroup_path);
  static int init_dir_(const char *curr_dir);
  static int init_full_dir_(const char *curr_path);
  static int write_string_to_file_(const char *filename, const char *content);
  static int get_string_from_file_(const char *filename, char content[VALUE_BUFSIZE]);
  int get_group_path(
      char *group_path,
      int path_bufsize,
      const uint64_t tenant_id,
      uint64_t group_id = OB_INVALID_GROUP_ID,
      const char *base_path = "");
  enum { NOT_DIR = 0, LEAF_DIR, REGULAR_DIR };
  int which_type_dir_(const char *curr_path, int &result);
  int recursion_remove_group_(const char *curr_path);
  int recursion_process_group_(const char *curr_path, DirProcessor *processor_ptr, bool is_top_dir = false);
};

}  // share
}  // oceanbase

#endif  // OB_CGROUP_CTRL_H
