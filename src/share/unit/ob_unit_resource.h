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

#ifndef OCEANBASE_SHARE_UNIT_OB_UNIT_RESOURCE_H_
#define OCEANBASE_SHARE_UNIT_OB_UNIT_RESOURCE_H_

#include "lib/utility/ob_print_utils.h"       // DECLARE_TO_STRING
#include "lib/utility/ob_unify_serialize.h"   // OB_UNIS_VERSION
#include "lib/utility/utility.h"              // max() / min()

namespace oceanbase
{
namespace share
{

#define __UR_TO_STR(x) #x
#define _UR_TO_STR(x) __UR_TO_STR(x)

#define _UNIT_MIN_CPU 1
#define UNIT_MIN_CPU_STR _UR_TO_STR(_UNIT_MIN_CPU)

// ObUnitResource is a collocations of unit all resource configs.
class ObUnitResource
{
  OB_UNIS_VERSION(1);

public:
  static const int64_t MB = (1<<20);
  static const int64_t GB = (1<<30);

  ////////////////////////// CPU ////////////////////////////
  // CPU resource is shared by META and USER tenant.
  // META tenant worker threads is auto configured by default percentage.
  //
  // NOTE: double constant need be 'constexpr' type
  static constexpr double UNIT_MIN_CPU = _UNIT_MIN_CPU;
  static constexpr double META_TENANT_MIN_CPU = UNIT_MIN_CPU;
  static constexpr double USER_TENANT_MIN_CPU = UNIT_MIN_CPU;
  static constexpr double META_TENANT_CPU_PERCENTAGE = 10;
  static constexpr double CPU_EPSILON = 0.00001;

  ////////////////////////// MEMORY ////////////////////////////
  // MEMORY is isolated by META and USER tenant
  // MEMORY_SIZE = (META + USER)
  //
  // META MEMORY is auto configured by default percentage and min memory limit.
  static const int64_t META_TENANT_MEMORY_PERCENTAGE = 10;
  static const int64_t META_TENANT_MIN_MEMORY = 512LL * MB;
  static const int64_t USER_TENANT_MIN_MEMORY = 512LL * MB;
  static const int64_t UNIT_MIN_MEMORY = META_TENANT_MIN_MEMORY + USER_TENANT_MIN_MEMORY;

  // For now, META tenant memory can not be too small.
  // So define safe memory for META tenant
  //
  // 1. If unit memory is bigger than UNIT_SAFE_MIN_MEMORY,
  //    min memory of META tenant is META_TENANT_SAFE_MIN_MEMORY.
  // 2. If unit memory is little than UNIT_SAFE_MIN_MEMORY,
  //    min momory of META tenant is META_TENANT_MIN_MEMORY
  //
  // See gen_meta_tenant_memory() for more infos.
  static const int64_t META_TENANT_SAFE_MIN_MEMORY = 1 * GB;
  static const int64_t UNIT_SAFE_MIN_MEMORY = 2 * META_TENANT_SAFE_MIN_MEMORY;

  ////////////////////////// LOG DISK ////////////////////////////
  // Unit LOG DISK SIZE is limited by LS replica. Every LS replica need 512M.
  //
  // META_TENANT: 512M              -> only SYS LS on Meta tenant
  // USER_TENANT: 512M * 3 = 1.5G   -> 3 LS at least for every USER tenant unit
  static const int64_t META_TENANT_MIN_LOG_DISK_SIZE = 512LL * MB;
  static const int64_t USER_TENANT_MIN_LOG_DISK_SIZE = 3LL * 512LL * MB;   // 1.5G
  static const int64_t UNIT_MIN_LOG_DISK_SIZE = META_TENANT_MIN_LOG_DISK_SIZE + USER_TENANT_MIN_LOG_DISK_SIZE; // 2G
  // META tenant LOG_DISK_SIZE is auto configured by default percentage and min log disk limit.
  static const int64_t META_TENANT_LOG_DISK_SIZE_PERCENTAGE = 10;
  // default factor of mapping MEMORY_SIZE to LOG_DISK_SIZE
  // MEMORY_SIZE * FACTOR = LOG_DISK_SIZE
  static const int64_t MEMORY_TO_LOG_DISK_FACTOR = 3;
  static const int64_t INVALID_LOG_DISK_SIZE = -1;

  ////////////////////////// IOPS ////////////////////////////
  // IOPS is shared by META and USER tenant.
  static const int64_t UNIT_MIN_IOPS = 1024;
  static const int64_t META_TENANT_MIN_IOPS = UNIT_MIN_IOPS;
  static const int64_t USER_TENANT_MIN_IOPS = UNIT_MIN_IOPS;
  static const int64_t INVALID_IOPS_WEIGHT = -1;
  static const int64_t DEFAULT_IOPS_WEIGHT = 0;
  // default factor of mapping CPU to IOPS
  // MIN_CPU * FACTOR = IOPS
  static constexpr double CPU_TO_IOPS_FACTOR = 10000;

public:
  ObUnitResource() { reset(); }
  ObUnitResource(const ObUnitResource &r) { *this = r; }
  ObUnitResource(
      const double max_cpu,
      const double min_cpu,
      const int64_t memory_size,
      const int64_t log_disk_size,
      const int64_t max_iops,
      const int64_t min_iops,
      const int64_t iops_weight);
  virtual ~ObUnitResource() {}

  ///////////////////////////////////  init/check funcs ////////////////////////////////////
  void reset();
  // direct set, do not check valid
  void set(
      const double max_cpu,
      const double min_cpu,
      const int64_t memory_size,
      const int64_t log_disk_size,
      const int64_t max_iops,
      const int64_t min_iops,
      const int64_t iops_weight);

  // Init from user specified resources and check valid for creating unit
  //
  // user_spec indicates that which resource is specified by user.
  // invalid resources in user_spec need be auto configured.
  //
  // @param [in] user_spec  user specified unit resource
  //
  // @ret OB_MISS_ARGUMENT                      if max_cpu and memory_size are not specified
  // @ret OB_INVALID_ARGUMENT                   invalid resource value
  // @ret OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT    resource value is blow limit
  // @ret OB_SUCCESS                            SUCCESS
  int init_and_check_valid_for_unit(const ObUnitResource &user_spec);

  // same with 'init_and_check_valid_for_unit'
  // user must specify max_cpu and memory_size, other resource can be invalid
  int init_and_check_valid_for_unit(
      const double max_cpu,
      const int64_t memory_size,
      const double min_cpu = 0,
      const int64_t log_disk_size = 0,
      const int64_t max_iops = 0,
      const int64_t min_iops = 0,
      const int64_t iops_weight = -1)
  {
    return init_and_check_valid_for_unit(ObUnitResource(
        max_cpu,
        min_cpu,
        memory_size,
        log_disk_size,
        max_iops,
        min_iops,
        iops_weight));
  }

  // update based on user specified and check valid for resource unit
  //
  // NOTE:
  //    1. if not valid, self will not be affected.
  //    2. self must also be valid for unit
  //
  // @param [in] user_spec  user specified unit resource
  //
  // @ret OB_INVALID_ARGUMENT                   invalid resource value
  // @ret OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT    resource value is blow limit
  // @ret OB_SUCCESS                            SUCCESS
  int update_and_check_valid_for_unit(const ObUnitResource &user_spec);

  // basic valid function
  bool is_valid() const
  {
    return is_max_cpu_valid()
        && is_min_cpu_valid()
        && max_cpu_ >= min_cpu_
        && is_memory_size_valid()
        && is_log_disk_size_valid()
        && is_max_iops_valid()
        && is_min_iops_valid()
        && max_iops_ >= min_iops_
        && is_iops_weight_valid();
  }

  // is valid to create resource unit
  bool is_valid_for_unit() const
  {
    return is_max_cpu_valid_for_unit()
        && is_min_cpu_valid_for_unit()
        && max_cpu_ >= min_cpu_
        && is_memory_size_valid_for_unit()
        && is_log_disk_size_valid_for_unit()
        && is_max_iops_valid_for_unit()
        && is_min_iops_valid_for_unit()
        && max_iops_ >= min_iops_
        && is_iops_weight_valid_for_unit();
  }

  // is valid for meta tenant
  bool is_valid_for_meta_tenant() const
  {
    return is_max_cpu_valid_for_meta_tenant()
        && is_min_cpu_valid_for_meta_tenant()
        && max_cpu_ >= min_cpu_
        && is_memory_size_valid_for_meta_tenant()
        && is_log_disk_size_valid_for_meta_tenant()
        && is_max_iops_valid_for_meta_tenant()
        && is_min_iops_valid_for_meta_tenant()
        && max_iops_ >= min_iops_
        && is_iops_weight_valid_for_meta_tenant();
  }

  // is valid for user tenant
  bool is_valid_for_user_tenant() const
  {
    return is_max_cpu_valid_for_user_tenant()
        && is_min_cpu_valid_for_user_tenant()
        && max_cpu_ >= min_cpu_
        && is_memory_size_valid_for_user_tenant()
        && is_log_disk_size_valid_for_user_tenant()
        && is_max_iops_valid_for_user_tenant()
        && is_min_iops_valid_for_user_tenant()
        && max_iops_ >= min_iops_
        && is_iops_weight_valid_for_user_tenant();
  }

  ///////////////////////////////////  compare funcs ///////////////////////////////////
  bool operator==(const ObUnitResource &config) const;

  ///////////////////////////////////  assign funcs ////////////////////////////////////
  ObUnitResource &operator=(const ObUnitResource &unit);

  ///////////////////////////////////  compute func //////////////////////////////////////
  ObUnitResource operator+(const ObUnitResource &config) const;
  ObUnitResource operator-(const ObUnitResource &config) const;
  ObUnitResource &operator+=(const ObUnitResource &config);
  ObUnitResource &operator-=(const ObUnitResource &config);
  ObUnitResource operator*(const int64_t count) const;

  ////////////////////////////////////  get / valid func ////////////////////////////////////
  // * is_xxx_valid():                    basic valid func for resource
  // * is_xxx_valid_for_unit()            resource valid for create resource unit
  // * is_xxx_valid_for_meta_tenant()     resource valid for meta tenant
  // * is_xxx_valid_for_user_tenant()     resource valid for user tenant
  double max_cpu() const { return max_cpu_; }
  bool is_max_cpu_valid() const { return max_cpu_ > 0; }
  bool is_max_cpu_valid_for_unit() const { return max_cpu_ >= UNIT_MIN_CPU; }
  bool is_max_cpu_valid_for_meta_tenant() const { return max_cpu_ >= META_TENANT_MIN_CPU; }
  bool is_max_cpu_valid_for_user_tenant() const { return max_cpu_ >= USER_TENANT_MIN_CPU; }

  double min_cpu() const { return min_cpu_; }
  bool is_min_cpu_valid() const { return min_cpu_ > 0; }
  bool is_min_cpu_valid_for_unit() const { return min_cpu_ >= UNIT_MIN_CPU; }
  bool is_min_cpu_valid_for_meta_tenant() const { return min_cpu_ >= META_TENANT_MIN_CPU; }
  bool is_min_cpu_valid_for_user_tenant() const { return min_cpu_ >= USER_TENANT_MIN_CPU; }

  int64_t memory_size() const { return memory_size_; }
  bool is_memory_size_valid() const { return memory_size_ > 0; }
  bool is_memory_size_valid_for_unit() const { return memory_size_ >= UNIT_MIN_MEMORY; }
  bool is_memory_size_valid_for_meta_tenant() const { return memory_size_ >= META_TENANT_MIN_MEMORY; }
  bool is_memory_size_valid_for_user_tenant() const { return memory_size_ >= USER_TENANT_MIN_MEMORY; }

  int64_t log_disk_size() const { return log_disk_size_; }
  bool is_log_disk_size_valid() const { return log_disk_size_ >= 0; }
  bool is_log_disk_size_valid_for_unit() const { return 0 == log_disk_size_ || log_disk_size_ >= UNIT_MIN_LOG_DISK_SIZE; }
  bool is_log_disk_size_valid_for_meta_tenant() const { return log_disk_size_ >= META_TENANT_MIN_LOG_DISK_SIZE; }
  bool is_log_disk_size_valid_for_user_tenant() const { return log_disk_size_ >= USER_TENANT_MIN_LOG_DISK_SIZE; }

  int64_t max_iops() const { return max_iops_; }
  bool is_max_iops_valid() const { return max_iops_ > 0; }
  bool is_max_iops_valid_for_unit() const { return max_iops_ >= UNIT_MIN_IOPS; }
  bool is_max_iops_valid_for_meta_tenant() const { return max_iops_ >= META_TENANT_MIN_IOPS; }
  bool is_max_iops_valid_for_user_tenant() const { return max_iops_ >= META_TENANT_MIN_IOPS; }

  int64_t min_iops() const { return min_iops_; }
  bool is_min_iops_valid() const { return min_iops_ > 0; }
  bool is_min_iops_valid_for_unit() const { return min_iops_ >= UNIT_MIN_IOPS; }
  bool is_min_iops_valid_for_meta_tenant() const { return min_iops_ >= META_TENANT_MIN_IOPS; }
  bool is_min_iops_valid_for_user_tenant() const { return min_iops_ >= META_TENANT_MIN_IOPS; }

  int64_t iops_weight() const { return iops_weight_; }
  bool is_iops_weight_valid() const { return iops_weight_ >= 0; }
  bool is_iops_weight_valid_for_unit() const { return is_iops_weight_valid(); }
  bool is_iops_weight_valid_for_meta_tenant() const { return is_iops_weight_valid(); }
  bool is_iops_weight_valid_for_user_tenant() const { return is_iops_weight_valid(); }

  ////////////////////////////////////// other functions ////////////////////////////////////

  // devide meta tenant resource from self
  //
  // 1. generate meta tenant resource
  // 2. deduct meta tenant resource from self, self become user tenant resource
  //
  // NOTE:
  // 1. self must be valid for unit before divide_meta_tenant
  // 2. after divide_meta_tenant, self become user tenant resource,
  //    is valid for user tenant, but may not be valid for unit
  //    ex. MEMORY 1G, after divide, meta MEMORY is 512M, self MEMORY is 512M
  int divide_meta_tenant(ObUnitResource &meta_resource);

  // generate sys tenant default unit resource based on configuration
  int gen_sys_tenant_default_unit_resource(const bool is_hidden_sys = false);

  /////////////////////////////////// static functions ///////////////////////////////////

  // get default LOG_DISK_SIZE based on MEMORY_SIZE
  static int64_t get_default_log_disk_size(const int64_t memory_size)
{
    return memory_size * MEMORY_TO_LOG_DISK_FACTOR;
  }

  // default IOPS = INT64_MAX
  static int64_t get_default_iops()
  {
    return INT64_MAX;
  }

  // get default IOPS_WEIGHT based on CPU
  static int64_t get_default_iops_weight(const double cpu)
  {
    return static_cast<int64_t>(cpu);
  }

  // generate meta tenant cpu by unit cpu
  static double gen_meta_tenant_cpu(const double unit_cpu)
  {
    double meta_cpu = unit_cpu * META_TENANT_CPU_PERCENTAGE / 100;
    if (meta_cpu < META_TENANT_MIN_CPU) {
      meta_cpu = META_TENANT_MIN_CPU;
    }
    return meta_cpu;
  }
  // generate meta tenant memory by unit memory
  static int64_t gen_meta_tenant_memory(const int64_t unit_memory)
  {
    const int64_t MIN_MEMORY = (unit_memory >= UNIT_SAFE_MIN_MEMORY) ?
                               META_TENANT_SAFE_MIN_MEMORY :
                               META_TENANT_MIN_MEMORY;

    const int64_t meta_memory_by_percent = unit_memory * META_TENANT_MEMORY_PERCENTAGE / 100;

    return max(meta_memory_by_percent, MIN_MEMORY);
  }
  // generate meta tenant log disk by unit log disk
  static int64_t gen_meta_tenant_log_disk_size(const int64_t unit_log_disk)
  {
    int64_t meta_log_disk = unit_log_disk * META_TENANT_LOG_DISK_SIZE_PERCENTAGE / 100;
    return max(meta_log_disk, META_TENANT_MIN_LOG_DISK_SIZE);
  }
  // generate meta tenant iops by unit iops
  static int64_t gen_meta_tenant_iops(const int64_t unit_iops)
  {
    return unit_iops;
  }
  // generate meta tenant iops_weight by unit iops_weight
  static int64_t gen_meta_tenant_iops_weight(const int64_t unit_iops_weight)
  {
    return unit_iops_weight;
  }

public:
  DECLARE_TO_STRING;

private:
  int init_and_check_cpu_(const ObUnitResource &user_spec);
  int init_and_check_mem_(const ObUnitResource &user_spec);
  int init_and_check_log_disk_(const ObUnitResource &user_spec);
  int init_and_check_iops_(const ObUnitResource &user_spec);
  int update_and_check_cpu_(const ObUnitResource &user_spec);
  int update_and_check_mem_(const ObUnitResource &user_spec);
  int update_and_check_log_disk_(const ObUnitResource &user_spec);
  int update_and_check_iops_(const ObUnitResource &user_spec);

protected:
  double  max_cpu_;
  double  min_cpu_;
  int64_t memory_size_;
  int64_t log_disk_size_;
  int64_t max_iops_;
  int64_t min_iops_;
  int64_t iops_weight_;
};


}//end namespace share
}//end namespace oceanbase

#endif //OCEANBASE_SHARE_UNIT_OB_UNIT_RESOURCE_H_
