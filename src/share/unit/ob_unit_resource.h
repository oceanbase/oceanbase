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
  static const int64_t UNIT_MIN_MEMORY = META_TENANT_MIN_MEMORY + USER_TENANT_MIN_MEMORY; // 1G

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
  // unit_min_log_disk_size and mem_to_log_disk_default_factor are different in SS and SN mode,
  //   check GCTX.is_shared_storage_mode() and decide which to use.
  // default factor of mapping MEMORY_SIZE to LOG_DISK_SIZE
  //   MEMORY_SIZE * FACTOR = LOG_DISK_SIZE
  // shared-nothing mode:
  static const int64_t UNIT_MIN_LOG_DISK_SIZE_SN = META_TENANT_MIN_LOG_DISK_SIZE + USER_TENANT_MIN_LOG_DISK_SIZE; // 2G
  static const int64_t MEMORY_TO_LOG_DISK_FACTOR_SN = 3;
  // shared-storage mode:
  static const int64_t UNIT_MIN_LOG_DISK_SIZE_SS = 3LL * GB; // 3G
  static const int64_t MEMORY_TO_LOG_DISK_FACTOR_SS = 1;
  // META tenant LOG_DISK_SIZE is auto configured by default percentage and min log disk limit.
  static const int64_t META_TENANT_LOG_DISK_SIZE_PERCENTAGE = 10;
  static const int64_t INVALID_LOG_DISK_SIZE = -1;

  ////////////////////////// DATA DISK /////////////////////////
  // DATA DISK SIZE is only valid in shared-storage mode, representing cache disk allowed to use.
  // META tenant DATA_DISK_SIZE is auto configured by default percentage and min and max data disk limit.
  static const int64_t META_TENANT_DATA_DISK_SIZE_PERCENTAGE = 10;
  static const int64_t HIDDEN_SYS_TENANT_MIN_DATA_DISK_SIZE = 2LL * GB;  // 2G
  static const int64_t META_TENANT_MIN_DATA_DISK_SIZE = 1LL * GB;  // 1G
  static const int64_t META_TENANT_MAX_DATA_DISK_SIZE = 4LL * GB;  // 4G
  static const int64_t USER_TENANT_MIN_DATA_DISK_SIZE = 1LL * GB;  // 1G
  static const int64_t UNIT_MIN_DATA_DISK_SIZE = META_TENANT_MIN_DATA_DISK_SIZE + USER_TENANT_MIN_DATA_DISK_SIZE;  // 2G
  // default factor of mapping MEMORY_SIZE to DATA_DISK_SIZE
  // MEMORY_SIZE * FACTOR = DATA_DISK_SIZE
  static const int64_t MEMORY_TO_DATA_DISK_FACTOR = 2;
  // 0 is the default value in ObUnitResource and __all_unit table.
  // In SN mode, 0 means not effective;
  // In SS mode, 0 means no restriction by tenant. or no data_disk_size for virtual tenant.
  static const int64_t DEFAULT_DATA_DISK_SIZE = 0;
  static const int64_t INVALID_DATA_DISK_SIZE = -1;

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

  ////////////////////// NET BANDWIDTH /////////////////////////
  static const int64_t UNIT_MIN_NET_BANDWIDTH = 1L * MB;                        // 1M
  static const int64_t TENANT_MIN_NET_BANDWIDTH = UNIT_MIN_NET_BANDWIDTH;       // 1M
  static const int64_t INVALID_NET_BANDWIDTH = 0;
  static const int64_t DEFAULT_NET_BANDWIDTH = INT64_MAX;
  static const int64_t INVALID_NET_BANDWIDTH_WEIGHT = -1;
  static const int64_t DEFAULT_NET_BANDWIDTH_WEIGHT = 0;

public:
  ObUnitResource() { reset(); }
  ObUnitResource(const ObUnitResource &r) { *this = r; }
  ObUnitResource(
      const double max_cpu,
      const double min_cpu,
      const int64_t memory_size,
      const int64_t log_disk_size,
      const int64_t data_disk_size,
      const int64_t max_iops,
      const int64_t min_iops,
      const int64_t iops_weight,
      const int64_t max_net_bandwidth,
      const int64_t net_bandwidth_weight);
  virtual ~ObUnitResource() {}

  ///////////////////////////////////  init/check funcs ////////////////////////////////////
  void reset();
  void reset_all_invalid();
  // direct set, do not check valid
  void set(
      const double max_cpu,
      const double min_cpu,
      const int64_t memory_size,
      const int64_t log_disk_size,
      const int64_t data_disk_size,
      const int64_t max_iops,
      const int64_t min_iops,
      const int64_t iops_weight,
      const int64_t max_net_bandwidth,
      const int64_t net_bandwidth_weight);

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
  bool is_valid() const;

  // is valid to create resource unit
  bool is_valid_for_unit() const;

  // is valid for meta tenant
  bool is_valid_for_meta_tenant() const
  {
    return is_max_cpu_valid_for_meta_tenant()
        && is_min_cpu_valid_for_meta_tenant()
        && max_cpu_ >= min_cpu_
        && is_memory_size_valid_for_meta_tenant()
        && is_log_disk_size_valid_for_meta_tenant()
        && is_data_disk_size_valid_for_meta_tenant()
        && is_max_iops_valid_for_meta_tenant()
        && is_min_iops_valid_for_meta_tenant()
        && max_iops_ >= min_iops_
        && is_iops_weight_valid_for_meta_tenant()
        && is_max_net_bandwidth_valid_for_meta_tenant()
        && is_net_bandwidth_weight_valid_for_meta_tenant();
  }

  // is valid for user tenant
  bool is_valid_for_user_tenant() const
  {
    return is_max_cpu_valid_for_user_tenant()
        && is_min_cpu_valid_for_user_tenant()
        && max_cpu_ >= min_cpu_
        && is_memory_size_valid_for_user_tenant()
        && is_log_disk_size_valid_for_user_tenant()
        && is_data_disk_size_valid_for_user_tenant()
        && is_max_iops_valid_for_user_tenant()
        && is_min_iops_valid_for_user_tenant()
        && max_iops_ >= min_iops_
        && is_iops_weight_valid_for_user_tenant()
        && is_max_net_bandwidth_valid_for_user_tenant()
        && is_net_bandwidth_weight_valid_for_user_tenant();
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
  bool is_log_disk_size_valid_for_unit() const;
  bool is_log_disk_size_valid_for_meta_tenant() const { return log_disk_size_ >= META_TENANT_MIN_LOG_DISK_SIZE; }
  bool is_log_disk_size_valid_for_user_tenant() const { return log_disk_size_ >= USER_TENANT_MIN_LOG_DISK_SIZE; }

  int64_t data_disk_size() const { return data_disk_size_; }
  void set_data_disk_size(const int64_t data_disk_size) { data_disk_size_ = data_disk_size; }
  // this func only checks value itself is valid
  bool is_data_disk_size_valid() const { return data_disk_size_ >= 0; }
  bool is_data_disk_size_valid_for_unit() const;
  bool is_data_disk_size_valid_for_meta_tenant() const;
  bool is_data_disk_size_valid_for_user_tenant() const;
  int check_data_disk_size_supported() const;

  int64_t max_iops() const { return max_iops_; }
  bool is_max_iops_valid() const { return max_iops_ > 0; }
  bool is_max_iops_valid_for_unit() const { return max_iops_ >= UNIT_MIN_IOPS; }
  bool is_max_iops_valid_for_meta_tenant() const { return max_iops_ >= META_TENANT_MIN_IOPS; }
  bool is_max_iops_valid_for_user_tenant() const { return max_iops_ >= USER_TENANT_MIN_IOPS; }

  int64_t min_iops() const { return min_iops_; }
  bool is_min_iops_valid() const { return min_iops_ > 0; }
  bool is_min_iops_valid_for_unit() const { return min_iops_ >= UNIT_MIN_IOPS; }
  bool is_min_iops_valid_for_meta_tenant() const { return min_iops_ >= META_TENANT_MIN_IOPS; }
  bool is_min_iops_valid_for_user_tenant() const { return min_iops_ >= USER_TENANT_MIN_IOPS; }

  int64_t iops_weight() const { return iops_weight_; }
  bool is_iops_weight_valid() const { return iops_weight_ >= 0; }
  bool is_iops_weight_valid_for_unit() const { return is_iops_weight_valid(); }
  bool is_iops_weight_valid_for_meta_tenant() const { return is_iops_weight_valid(); }
  bool is_iops_weight_valid_for_user_tenant() const { return is_iops_weight_valid(); }

  int64_t max_net_bandwidth() const { return max_net_bandwidth_; }
  bool is_max_net_bandwidth_valid() const { return max_net_bandwidth_ > 0; }
  bool is_max_net_bandwidth_valid_for_unit() const { return max_net_bandwidth_ >= UNIT_MIN_NET_BANDWIDTH; }
  bool is_max_net_bandwidth_valid_for_meta_tenant() const { return max_net_bandwidth_ >= TENANT_MIN_NET_BANDWIDTH; }
  bool is_max_net_bandwidth_valid_for_user_tenant() const { return max_net_bandwidth_ >= TENANT_MIN_NET_BANDWIDTH; }

  int64_t net_bandwidth_weight() const { return net_bandwidth_weight_; }
  bool is_net_bandwidth_weight_valid() const { return net_bandwidth_weight_ >= 0; }
  bool is_net_bandwidth_weight_valid_for_unit() const { return is_net_bandwidth_weight_valid(); }
  bool is_net_bandwidth_weight_valid_for_meta_tenant() const { return is_net_bandwidth_weight_valid(); }
  bool is_net_bandwidth_weight_valid_for_user_tenant() const { return is_net_bandwidth_weight_valid(); }
  int check_net_bandwidth_supported() const;
  int check_net_bandwidth_supported(
      const ObUnitResource &user_spec,
      const bool &need_check_if_user_spec,
      bool &is_during_upgrade) const;

  ////////////////////////////////////// comparison functions ////////////////////////////////////
  bool has_expanded_resource_than(const ObUnitResource &other) const;
  bool has_shrunk_resource_than(const ObUnitResource &other) const;
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
  static int64_t get_default_log_disk_size(const int64_t memory_size);

  // get default DATA_DISK_SIZE based on MEMORY_SIZE
  static int64_t get_default_data_disk_size(const int64_t memory_size)
  {
    return memory_size * MEMORY_TO_DATA_DISK_FACTOR;
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

  // default NET_BANDWIDTH = INT64_MAX
  static int64_t get_default_net_bandwidth()
  {
    return DEFAULT_NET_BANDWIDTH; // INT64_MAX
  }

  // get default NET_BANDWIDTH_WEIGHT based on CPU
  static int64_t get_default_net_bandwidth_weight(const double cpu)
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
    int64_t meta_tenant_memory = 0;
    if (unit_memory < UNIT_SAFE_MIN_MEMORY) {
      meta_tenant_memory = META_TENANT_MIN_MEMORY;
    } else {
      const int64_t meta_memory_by_percent = unit_memory * META_TENANT_MEMORY_PERCENTAGE / 100;
      meta_tenant_memory = max(meta_memory_by_percent, META_TENANT_SAFE_MIN_MEMORY);
    }
    return meta_tenant_memory;
  }
  // generate meta tenant log disk by unit log disk
  static int64_t gen_meta_tenant_log_disk_size(const int64_t unit_log_disk)
  {
    int64_t meta_log_disk = unit_log_disk * META_TENANT_LOG_DISK_SIZE_PERCENTAGE / 100;
    return max(meta_log_disk, META_TENANT_MIN_LOG_DISK_SIZE);
  }
  // generate meta tenant data disk by unit data disk
  static int64_t gen_meta_tenant_data_disk_size(const int64_t unit_data_disk)
  {
    int64_t meta_data_disk = DEFAULT_DATA_DISK_SIZE;
    if (0 == unit_data_disk) {
      meta_data_disk = 0;
    } else {
      meta_data_disk = unit_data_disk * META_TENANT_DATA_DISK_SIZE_PERCENTAGE / 100;
      meta_data_disk = min(max(meta_data_disk, META_TENANT_MIN_DATA_DISK_SIZE),
               META_TENANT_MAX_DATA_DISK_SIZE);
    }
    return meta_data_disk;
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
  // generate meta tenant net_bandwidth by unit net_bandwidth
  static int64_t gen_meta_tenant_net_bandwidth(const int64_t unit_net_bandwidth)
  {
    return unit_net_bandwidth;
  }
  // generate meta tenant net_bandwidth_weight by unit net_bandwidth_weight
  static int64_t gen_meta_tenant_net_bandwidth_weight(const int64_t unit_net_bandwidth_weight)
  {
    return unit_net_bandwidth_weight;
  }

public:
  DECLARE_TO_STRING;

private:
  int init_and_check_cpu_(const ObUnitResource &user_spec);
  int init_and_check_mem_(const ObUnitResource &user_spec);
  int init_and_check_log_disk_(const ObUnitResource &user_spec);
  int init_update_and_check_data_disk_(const ObUnitResource &user_spec, const bool is_update);
  int init_and_check_iops_(const ObUnitResource &user_spec);
  int init_and_check_net_bandwidth_(const ObUnitResource &user_spec);
  int update_and_check_cpu_(const ObUnitResource &user_spec);
  int update_and_check_mem_(const ObUnitResource &user_spec);
  int update_and_check_log_disk_(const ObUnitResource &user_spec);
  int update_and_check_iops_(const ObUnitResource &user_spec);
  int update_and_check_net_bandwidth_(const ObUnitResource &user_spec);

protected:
  double  max_cpu_;
  double  min_cpu_;
  int64_t memory_size_;
  int64_t log_disk_size_;
  int64_t max_iops_;
  int64_t min_iops_;
  int64_t iops_weight_;
  // The members below were added after 4.0 and have different behavior from others.
  // Considering upgrade compatability, these rules need to be followed:
  // 1. Define a valid value as DEFAULT value for each member, using for both inner_table column
  //    and data structure ObUnitResource, to make sure it is valid after upgrading from lower
  //    version.
  // 2. Define an INVALID value for each member, only for ObResourceResolver to mark when it's
  //    not specified by user. The resolver needs to invoke reset_all_invalid() to set all
  //    members to this invalid value.
  // 3. Default constructor, reset() method will set these members as DEFAULT values, 
  //    reset_all_invalid() will set these members as INVALID values.
  int64_t data_disk_size_;                    // DEFAULT: 0,         INVALID: -1
  int64_t max_net_bandwidth_;                 // DEFAULT: INT64_MAX, INVALID: 0
  int64_t net_bandwidth_weight_;              // DEFAULT: 0,         INVALID: -1
};


}//end namespace share
}//end namespace oceanbase

#endif //OCEANBASE_SHARE_UNIT_OB_UNIT_RESOURCE_H_
