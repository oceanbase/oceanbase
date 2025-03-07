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

#define USING_LOG_PREFIX    SHARE

#include "ob_unit_resource.h"
#include "share/ob_server_struct.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/shared_storage/ob_disk_space_manager.h"
#endif

namespace oceanbase
{
using namespace common;
namespace share
{

#define CALCULATE_CONFIG(l, op, r, result) \
  do { \
    result.max_cpu_ = l.max_cpu_ op r.max_cpu_; \
    result.min_cpu_ = l.min_cpu_ op r.min_cpu_; \
    result.memory_size_ = l.memory_size_ op r.memory_size_; \
    result.log_disk_size_ = l.log_disk_size_ op r.log_disk_size_; \
    result.max_iops_ = l.max_iops_ op r.max_iops_; \
    result.min_iops_ = l.min_iops_ op r.min_iops_; \
    result.iops_weight_ = l.iops_weight_ op r.iops_weight_; \
    result.data_disk_size_ = l.data_disk_size_ op r.data_disk_size_; \
    result.max_net_bandwidth_ = l.max_net_bandwidth_ op r.max_net_bandwidth_; \
    result.net_bandwidth_weight_ = l.net_bandwidth_weight_ op r.net_bandwidth_weight_; \
  } while (false)

#define CALCULATE_CONFIG_WITH_CONSTANT(l, op, c, result) \
  do { \
    result.max_cpu_ = l.max_cpu_ op static_cast<double>(c); \
    result.min_cpu_ = l.min_cpu_ op static_cast<double>(c); \
    result.memory_size_ = l.memory_size_ op (c); \
    result.log_disk_size_ = l.log_disk_size_ op (c); \
    result.max_iops_ = l.max_iops_ op (c); \
    result.min_iops_ = l.min_iops_ op (c); \
    result.iops_weight_ = l.iops_weight_ op (c); \
    result.data_disk_size_ = l.data_disk_size_ op (c); \
    result.max_net_bandwidth_ = l.max_net_bandwidth_ op (c); \
    result.net_bandwidth_weight_ = l.net_bandwidth_weight_ op (c); \
  } while (false)

#define UNIT_MIN_LOG_DISK_SIZE (GCTX.is_shared_storage_mode() ? \
        ObUnitResource::UNIT_MIN_LOG_DISK_SIZE_SS : ObUnitResource::UNIT_MIN_LOG_DISK_SIZE_SN)

ObUnitResource::ObUnitResource(
    const double max_cpu,
    const double min_cpu,
    const int64_t memory_size,
    const int64_t log_disk_size,
    const int64_t data_disk_size,
    const int64_t max_iops,
    const int64_t min_iops,
    const int64_t iops_weight,
    const int64_t max_net_bandwidth,
    const int64_t net_bandwidth_weight) :
    max_cpu_(max_cpu),
    min_cpu_(min_cpu),
    memory_size_(memory_size),
    log_disk_size_(log_disk_size),
    max_iops_(max_iops),
    min_iops_(min_iops),
    iops_weight_(iops_weight),
    data_disk_size_(data_disk_size),
    max_net_bandwidth_(max_net_bandwidth),
    net_bandwidth_weight_(net_bandwidth_weight)
{
}

void ObUnitResource::reset()
{
  reset_all_invalid();
  // following members are reset as DEFAULT values
  data_disk_size_ = DEFAULT_DATA_DISK_SIZE;
  max_net_bandwidth_ = DEFAULT_NET_BANDWIDTH;
  net_bandwidth_weight_ = DEFAULT_NET_BANDWIDTH_WEIGHT;
}

void ObUnitResource::reset_all_invalid()
{
  max_cpu_ = 0;
  min_cpu_ = 0;
  memory_size_ = 0;
  log_disk_size_ = INVALID_LOG_DISK_SIZE;
  max_iops_ = 0;
  min_iops_ = 0;
  iops_weight_ = INVALID_IOPS_WEIGHT;
  data_disk_size_ = INVALID_DATA_DISK_SIZE;
  max_net_bandwidth_ = INVALID_NET_BANDWIDTH;
  net_bandwidth_weight_ = INVALID_NET_BANDWIDTH_WEIGHT;
}

void ObUnitResource::set(
    const double max_cpu,
    const double min_cpu,
    const int64_t memory_size,
    const int64_t log_disk_size,
    const int64_t data_disk_size,
    const int64_t max_iops,
    const int64_t min_iops,
    const int64_t iops_weight,
    const int64_t max_net_bandwidth,
    const int64_t net_bandwidth_weight)
{
  max_cpu_ = max_cpu;
  min_cpu_ = min_cpu;
  memory_size_ = memory_size;
  log_disk_size_ = log_disk_size;
  max_iops_ = max_iops;
  min_iops_ = min_iops;
  iops_weight_ = iops_weight;
  data_disk_size_ = data_disk_size;
  max_net_bandwidth_ = max_net_bandwidth;
  net_bandwidth_weight_ = net_bandwidth_weight;
}

bool ObUnitResource::is_valid() const
{
  return is_max_cpu_valid()
      && is_min_cpu_valid()
      && max_cpu_ >= min_cpu_
      && is_memory_size_valid()
      && is_log_disk_size_valid()
      // Default value of data_disk_size was -1 in version 4.3.0.1.
      // So SKIP checking validity of data_disk_size in SN mode to avoid compatibility problem.
      && (!GCTX.is_shared_storage_mode() || is_data_disk_size_valid())
      && is_max_iops_valid()
      && is_min_iops_valid()
      && max_iops_ >= min_iops_
      && is_iops_weight_valid()
      && is_max_net_bandwidth_valid()
      && is_net_bandwidth_weight_valid();
}

bool ObUnitResource::is_valid_for_unit() const
{
  return is_max_cpu_valid_for_unit()
      && is_min_cpu_valid_for_unit()
      && max_cpu_ >= min_cpu_
      && is_memory_size_valid_for_unit()
      && is_log_disk_size_valid_for_unit()
      // Default value of data_disk_size was -1 in version 4.3.0.1.
      // So SKIP checking validity of data_disk_size in SN mode to avoid compatibility problem.
      && (!GCTX.is_shared_storage_mode() || is_data_disk_size_valid_for_unit())
      && is_max_iops_valid_for_unit()
      && is_min_iops_valid_for_unit()
      && max_iops_ >= min_iops_
      && is_iops_weight_valid_for_unit()
      && is_max_net_bandwidth_valid_for_unit()
      && is_net_bandwidth_weight_valid_for_unit();
}

int ObUnitResource::check_data_disk_size_supported() const
{
  int ret = OB_SUCCESS;
  uint64_t sys_data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(OB_SYS_TENANT_ID, sys_data_version))) {
    LOG_WARN("failed to get data_version", KR(ret));
  } else if (sys_data_version < DATA_VERSION_4_3_3_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "sys tenant's data version is below 4.3.3, DATA_DISK_SIZE");
    LOG_WARN("sys tenant's data version is below 4.3.3.0, DATA_DISK_SIZE not supported",
              KR(ret), K(sys_data_version));
  } else if (!GCTX.is_shared_storage_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("user specifying data_disk_size in shared-nothing mode is not supported", KR(ret),
        K(GCTX.is_shared_storage_mode()));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "DATA_DISK_SIZE in shared-nothing mode");
  }
  return ret;
}

bool ObUnitResource::is_data_disk_size_valid_for_unit() const
{
  bool b_ret = false;
  if (GCTX.is_shared_storage_mode()) {
    // data_disk_size can be 0 only for hidden_sys unit
    b_ret = 0 == data_disk_size_ || data_disk_size_ >= UNIT_MIN_DATA_DISK_SIZE;
  } else {
    b_ret = DEFAULT_DATA_DISK_SIZE == data_disk_size_;
  }
  return b_ret;
}

bool ObUnitResource::is_data_disk_size_valid_for_meta_tenant() const
{
  bool b_ret = false;
  if (GCTX.is_shared_storage_mode()) {
    // data_disk_size can be 0 only for hidden_sys unit
    b_ret = data_disk_size_ >= META_TENANT_MIN_DATA_DISK_SIZE
            && data_disk_size_ <= META_TENANT_MAX_DATA_DISK_SIZE;
  } else {
    b_ret = DEFAULT_DATA_DISK_SIZE == data_disk_size_;
  }
  return b_ret;
}

bool ObUnitResource::is_data_disk_size_valid_for_user_tenant() const
{
  bool b_ret = false;
  if (GCTX.is_shared_storage_mode()) {
    // data_disk_size can be 0 only for hidden_sys unit
    b_ret = data_disk_size_ >= USER_TENANT_MIN_DATA_DISK_SIZE;
  } else {
    b_ret = DEFAULT_DATA_DISK_SIZE == data_disk_size_;
  }
  return b_ret;
}

int64_t ObUnitResource::get_default_log_disk_size(const int64_t memory_size)
{
  int64_t mem_to_log_disk_factor = GCTX.is_shared_storage_mode() ?
                                   MEMORY_TO_LOG_DISK_FACTOR_SS : MEMORY_TO_LOG_DISK_FACTOR_SN;
  return max(memory_size * mem_to_log_disk_factor, UNIT_MIN_LOG_DISK_SIZE);
}

bool ObUnitResource::is_log_disk_size_valid_for_unit() const
{
  return 0 == log_disk_size_ || log_disk_size_ >= UNIT_MIN_LOG_DISK_SIZE;
}

int ObUnitResource::init_and_check_cpu_(const ObUnitResource &user_spec)
{
  int ret = OB_SUCCESS;
  const double unit_min_cpu = UNIT_MIN_CPU;
  // max_cpu must be specified
  if (! user_spec.is_max_cpu_valid()) {
    ret = OB_MISS_ARGUMENT;
    LOG_WARN("missing max_cpu argument", KR(ret), K(user_spec));
    LOG_USER_ERROR(OB_MISS_ARGUMENT, "MAX_CPU");
  } else if (user_spec.max_cpu() < unit_min_cpu) {
    ret = OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT;
    LOG_WARN("max_cpu is below limit", KR(ret), K(user_spec), K(unit_min_cpu));
    LOG_USER_ERROR(OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT, "MAX_CPU", UNIT_MIN_CPU_STR);
  } else {
    // max_cpu valid
    max_cpu_ = user_spec.max_cpu();

    if (user_spec.is_min_cpu_valid()) {
      if (user_spec.min_cpu() < unit_min_cpu) {
        ret = OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT;
        LOG_WARN("min_cpu is below limit", KR(ret), K(user_spec), K(unit_min_cpu));
        LOG_USER_ERROR(OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT, "MIN_CPU", UNIT_MIN_CPU_STR);
      } else if (user_spec.min_cpu() > user_spec.max_cpu()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("min_cpu greater than max_cpu", KR(ret), K(user_spec), K(unit_min_cpu));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "MIN_CPU, MIN_CPU is greater than MAX_CPU");
      } else {
        // min_cpu valid
        min_cpu_ = user_spec.min_cpu();
      }
    } else {
      // user not specified, default min_cpu = max_cpu
      min_cpu_ = max_cpu_;
    }
  }

  LOG_INFO("ObUnitResource init_and_check: CPU", KR(ret), K(max_cpu_), K(min_cpu_), K(user_spec),
      KPC(this), K(unit_min_cpu));
  return ret;
}

int ObUnitResource::init_and_check_mem_(const ObUnitResource &user_spec)
{
  int ret = OB_SUCCESS;
  const int64_t unit_min_memory = UNIT_MIN_MEMORY;
  // memory_size must be specified
  if (! user_spec.is_memory_size_valid()) {
    ret = OB_MISS_ARGUMENT;
    LOG_WARN("missing 'memory_size' argument", KR(ret), K(user_spec));
    LOG_USER_ERROR(OB_MISS_ARGUMENT, "MEMORY_SIZE");
  } else if (user_spec.memory_size() < unit_min_memory) {
    ret = OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT;
    LOG_WARN("memory_size is below limit", KR(ret), K(user_spec), K(unit_min_memory));
    ObCStringHelper helper;
    LOG_USER_ERROR(OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT, "MEMORY_SIZE",
        helper.convert(unit_min_memory));
  } else {
    // memory_size valid
    memory_size_ = user_spec.memory_size();
  }

  LOG_INFO("ObUnitResource init_and_check: MEMORY", KR(ret), K(memory_size_), K(user_spec),
      KPC(this), K(unit_min_memory));
  return ret;
}

int ObUnitResource::init_and_check_log_disk_(const ObUnitResource &user_spec)
{
  int ret = OB_SUCCESS;
  const int64_t unit_min_log_disk_size = UNIT_MIN_LOG_DISK_SIZE;
  // user specify log_disk_size
  if (user_spec.is_log_disk_size_valid()) {
    if (0 == user_spec.log_disk_size()) {
      // log_disk_size is only allowed to be 0 for hidden SYS
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Log_disk_size can only be specified as 0 for hidden SYS, not for normal unit.", KR(ret), K(user_spec));
    } else if (user_spec.log_disk_size() < unit_min_log_disk_size) {
      ret = OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT;
      LOG_WARN("log_disk_size is below limit", KR(ret), K(user_spec),
          K(unit_min_log_disk_size));
      ObCStringHelper helper;
      LOG_USER_ERROR(OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT, "LOG_DISK_SIZE",
          helper.convert(unit_min_log_disk_size));
    } else {
      // log_disk_size valid
      log_disk_size_ = user_spec.log_disk_size();
    }
  } else {
    // user not specify log_disk_size
    // use the default value
    log_disk_size_ = get_default_log_disk_size(memory_size_);
  }

  LOG_INFO("ObUnitResource init_and_check: LOG_DISK", KR(ret), K(log_disk_size_), K(user_spec),
      KPC(this));
  return ret;
}

int ObUnitResource::init_and_check_data_disk_(const ObUnitResource &user_spec)
{
  int ret = OB_SUCCESS;
  if (0 == user_spec.data_disk_size_) {
    if (! GCTX.is_shared_storage_mode()) {
      // for upgrade compatability in shared-nothing mode
      data_disk_size_ = DEFAULT_DATA_DISK_SIZE;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("user specified data_disk_size should not be 0", KR(ret));
    }
  } else if (user_spec.is_data_disk_size_valid()) {
    // user specify data_disk_size
    if (OB_FAIL(check_data_disk_size_supported())) {
      LOG_WARN("failed to check data_disk_size supported", KR(ret), K(user_spec));
    } else {
      const int64_t unit_min_data_disk_size = UNIT_MIN_DATA_DISK_SIZE;
      if (user_spec.data_disk_size() < unit_min_data_disk_size) {
        ret = OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT;
        LOG_WARN("data_disk_size is below limit", KR(ret), K(user_spec),
            K(unit_min_data_disk_size));
        ObCStringHelper helper;
        LOG_USER_ERROR(OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT, "DATA_DISK_SIZE",
            helper.convert(unit_min_data_disk_size));
      } else {
        data_disk_size_ = user_spec.data_disk_size();
      }
    }
  } else {
    // user not specify data_disk_size
    // use the default value
    if (GCTX.is_shared_storage_mode()) {
      data_disk_size_ = get_default_data_disk_size(memory_size_);
    } else {
      data_disk_size_ = DEFAULT_DATA_DISK_SIZE;
    }
  }

  LOG_INFO("ObUnitResource init_and_check: DATA_DISK", KR(ret), K(data_disk_size_), K(user_spec),
      KPC(this));
  return ret;
}

int ObUnitResource::init_and_check_iops_(const ObUnitResource &user_spec)
{
  int ret = OB_SUCCESS;
  const int64_t unit_min_iops = UNIT_MIN_IOPS;
  // max_iops and min_iops are not specified, auto configure by min_cpu
  if (! user_spec.is_max_iops_valid() && ! user_spec.is_min_iops_valid()) {
    max_iops_ = get_default_iops();
    min_iops_ = max_iops_;

    // if iops_weight is not specified, auto configure by min_cpu
    // NOTE: default min_iops may be too large that exceeds disk IOPS upper limit.
    //       so configure iops_weight to support islation by iops weight
    if (! user_spec.is_iops_weight_valid()) {
      iops_weight_ = get_default_iops_weight(min_cpu_);
    } else {
      // user speicified
      iops_weight_ = user_spec.iops_weight();
    }
  } else {
    // at least one of min_iops and max_iops are specified, use user specified value
    max_iops_ = user_spec.max_iops();
    min_iops_ = user_spec.min_iops();

    // min_iops == max_iops if only one is specified
    if (! user_spec.is_max_iops_valid()) {
      max_iops_ = user_spec.min_iops();
    } else if (! user_spec.is_min_iops_valid()) {
      min_iops_ = user_spec.max_iops();
    }

    if (max_iops_ < min_iops_) {
      // it must be: two are all specified.
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("max_iops is little than min_iops", KR(ret), K(min_iops_), K(max_iops_), K(user_spec));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "MAX_IOPS, MAX_IOPS is little than MIN_IOPS");
    } else if (min_iops_ < unit_min_iops) {
      // NEED check which one is invalid
      if (user_spec.is_min_iops_valid()) {
        ret = OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT;
        LOG_WARN("min_iops is below limit", KR(ret), K(min_iops_), K(max_iops_), K(user_spec), K(unit_min_iops));
        ObCStringHelper helper;
        LOG_USER_ERROR(OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT, "MIN_IOPS",
            helper.convert(unit_min_iops));
      } else {
        ret = OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT;
        LOG_WARN("max_iops is below limit", KR(ret), K(min_iops_), K(max_iops_), K(user_spec), K(unit_min_iops));
        ObCStringHelper helper;
        LOG_USER_ERROR(OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT, "MAX_IOPS",
            helper.convert(unit_min_iops));
      }
    } else {
      // min_iops_ and max_iops_ are all valid
    }

    // init iops_weight
    if (OB_SUCCESS == ret) {
      if (user_spec.is_iops_weight_valid()) {
        // user specified
        iops_weight_ = user_spec.iops_weight();
      } else {
        // not specified, init to min_cpu
        iops_weight_ = get_default_iops_weight(min_cpu_);
      }
    }
  }

  LOG_INFO("ObUnitResource init_and_check: IOPS", KR(ret), K(min_iops_), K(max_iops_),
      K(iops_weight_), K(user_spec), KPC(this), K(unit_min_iops));
  return ret;
}

// This function only checks data_version
int ObUnitResource::check_net_bandwidth_supported() const
{
  int ret = OB_SUCCESS;
  bool is_during_upgrade = false;
  if (OB_FAIL(check_net_bandwidth_supported(*this, false/*need_check_user_spec*/,
                                            is_during_upgrade))) {
    LOG_WARN("failed to check_net_bandwidth supported", KR(ret), KPC(this));
  }
  return ret;
}

// This function checks data_version, and also whether user specified
//  net_bandwidth if need_check_user_spec is true.
int ObUnitResource::check_net_bandwidth_supported(
    const ObUnitResource &user_spec,
    const bool &need_check_user_spec,
    bool &is_during_upgrade) const
{
  int ret = OB_SUCCESS;
  uint64_t sys_data_version = 0;
  is_during_upgrade = false;
  if (OB_FAIL(GET_MIN_DATA_VERSION(OB_SYS_TENANT_ID, sys_data_version))) {
    LOG_WARN("failed to get data_version", KR(ret));
  } else if (sys_data_version < DATA_VERSION_4_3_3_0) {
    is_during_upgrade = true;
    bool is_user_specified = false;
    if (! need_check_user_spec) {
      // Assume user specified, no need to check
      is_user_specified = true;
    } else {
      // Check max_net_bw and net_bw_weight both INVALID, or both DEFAULT value.
      if (!user_spec.is_max_net_bandwidth_valid() && !user_spec.is_net_bandwidth_weight_valid()) {
        // both INVALID value, good
      } else if (DEFAULT_NET_BANDWIDTH == user_spec.max_net_bandwidth_
                && DEFAULT_NET_BANDWIDTH_WEIGHT == user_spec.net_bandwidth_weight_) {
        // both DEFAULT value, good
      } else {
        is_user_specified = true;
      }
    }
    if (OB_SUCC(ret) && is_user_specified) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "sys tenant's data version is below 4.3.3, "
                     "MAX_NET_BANDWIDTH and NET_BANDWIDTH_WEIGHT");
      LOG_WARN("sys tenant's data version is below 4.3.3.0, net_bandwidth not supported",
                KR(ret), K(sys_data_version), K(user_spec));
    }
  }
  return ret;
}

int ObUnitResource::init_and_check_net_bandwidth_(const ObUnitResource &user_spec)
{
  int ret = OB_SUCCESS;
  bool is_during_upgrade = false;
  if (OB_FAIL(check_net_bandwidth_supported(user_spec, true/*need_check_user_spec*/,
                                            is_during_upgrade))) {
    LOG_WARN("failed to check_net_bandwidth_supported", KR(ret), K(user_spec));
  } else if (is_during_upgrade) {
    // during upgrade, set DEFAULT value
    max_net_bandwidth_ = DEFAULT_NET_BANDWIDTH;
    net_bandwidth_weight_ = DEFAULT_NET_BANDWIDTH_WEIGHT;
  } else {
    const int64_t unit_min_net_bandwidth = UNIT_MIN_NET_BANDWIDTH;
    if (! user_spec.is_max_net_bandwidth_valid()) {
      // max_net_bandwidth not specified, set by DEFAULT value INT64_MAX
      max_net_bandwidth_ = get_default_net_bandwidth();

      // if net_bandwidth_weight is not specified, auto configure net_bandwidth_weight by min_cpu
      if (! user_spec.is_net_bandwidth_weight_valid()) {
        net_bandwidth_weight_ = get_default_net_bandwidth_weight(min_cpu_);
      } else {
        // user speicified
        net_bandwidth_weight_ = user_spec.net_bandwidth_weight();
      }
    } else {
      // max_net_bandwidth is specified, use user specified value
      if (user_spec.max_net_bandwidth_ < unit_min_net_bandwidth) {
        ret = OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT;
        LOG_WARN("max_net_bandwidth is below limit", KR(ret), K(user_spec), K(unit_min_net_bandwidth));
        ObCStringHelper helper;
        LOG_USER_ERROR(OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT, "MAX_NET_BANDWIDTH", helper.convert(unit_min_net_bandwidth));
      } else {
        max_net_bandwidth_ = user_spec.max_net_bandwidth();

        // if net_bandwidth_weight is not specified, set as DEFAULT value
        if (! user_spec.is_net_bandwidth_weight_valid()) {
          // not specified, init to DEFAULT value
          net_bandwidth_weight_ = get_default_iops_weight(min_cpu_);
        } else {
          // user specified
          net_bandwidth_weight_ = user_spec.net_bandwidth_weight();
        }
      }
    }
  }

  LOG_INFO("ObUnitResource init_and_check: NET_BANDWIDTH", KR(ret), K(max_net_bandwidth_),
      K(net_bandwidth_weight_), K(user_spec), KPC(this));
  return ret;
}

int ObUnitResource::init_and_check_valid_for_unit(const ObUnitResource &user_spec)
{
  int ret = OB_SUCCESS;

  // reset before init
  reset();

  // check CPU
  ret = init_and_check_cpu_(user_spec);

  // check MEMORY
  if (OB_SUCCESS == ret) {
    ret = init_and_check_mem_(user_spec);
  }

  // check LOGDISK
  if (OB_SUCCESS == ret) {
    ret = init_and_check_log_disk_(user_spec);
  }

  // check DATADISK
  if (OB_SUCCESS == ret) {
    ret = init_and_check_data_disk_(user_spec);
  }

  // check IOPS
  if (OB_SUCCESS == ret) {
    ret = init_and_check_iops_(user_spec);
  }

 // check NET_BANDWIDTH
  if (OB_SUCCESS == ret) {
    ret = init_and_check_net_bandwidth_(user_spec);
  }

  LOG_INFO("init unit resource by user spec and check valid", KR(ret), K(user_spec), KPC(this));

  if (OB_FAIL(ret)) {
    // reset self after fail
    reset();
  }
  return ret;
}

int ObUnitResource::update_and_check_cpu_(const ObUnitResource &user_spec)
{
  int ret = OB_SUCCESS;
  const double unit_min_cpu = UNIT_MIN_CPU;

  if (! is_valid_for_unit()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected, should be valid for unit", KR(ret), KPC(this));
  } else if (! user_spec.is_min_cpu_valid() && ! user_spec.is_max_cpu_valid()) {
    // not specified, need not update
  } else {
    double new_min_cpu = min_cpu_;
    double new_max_cpu = max_cpu_;

    // user specify min_cpu
    if (user_spec.is_min_cpu_valid()) {
      new_min_cpu = user_spec.min_cpu();
    }

    // user specify max_cpu
    if (user_spec.is_max_cpu_valid()) {
      new_max_cpu = user_spec.max_cpu();
    }

    if (new_max_cpu < new_min_cpu) {
      // specify max_cpu, report max_cpu error
      if (user_spec.is_max_cpu_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("max_cpu is little than min_cpu", KR(ret), K(new_max_cpu), K(new_min_cpu), K(user_spec),
            KPC(this), K(unit_min_cpu));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "MAX_CPU, MAX_CPU is little than MIN_CPU");
      } else if (user_spec.is_min_cpu_valid()) {
        ret = OB_INVALID_ARGUMENT;
        // min_cpu is specified, report min_cpu error
        LOG_WARN("min_cpu greater than max_cpu", KR(ret), K(new_min_cpu), K(new_max_cpu), K(user_spec),
            KPC(this), K(unit_min_cpu));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "MIN_CPU, MIN_CPU is greater than MAX_CPU");
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("cpu resource is invalid", K(new_max_cpu), K(new_min_cpu), K(new_max_cpu), K(user_spec), KPC(this));
      }
    } else if (new_min_cpu < unit_min_cpu) {
      ret = OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT;
      LOG_WARN("min_cpu is below limit", KR(ret), K(new_min_cpu), K(user_spec), KPC(this), K(unit_min_cpu));
      LOG_USER_ERROR(OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT, "MIN_CPU", UNIT_MIN_CPU_STR);
    } else {
      // all is valid
      min_cpu_ = new_min_cpu;
      max_cpu_ = new_max_cpu;
    }

    LOG_INFO("ObUnitResource update_and_check: CPU", KR(ret), K(max_cpu_), K(min_cpu_), K(user_spec),
        KPC(this));
  }

  return ret;
}

int ObUnitResource::update_and_check_mem_(const ObUnitResource &user_spec)
{
  int ret = OB_SUCCESS;
  const int64_t unit_min_memory = UNIT_MIN_MEMORY;

  if (! user_spec.is_memory_size_valid()) {
    // memory not specified, need not update
  } else {
    if (user_spec.memory_size() < unit_min_memory) {
      ret = OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT;
      LOG_WARN("memory_size is below limit", KR(ret), K(user_spec), K(unit_min_memory));
      ObCStringHelper helper;
      LOG_USER_ERROR(OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT, "MEMORY_SIZE",
          helper.convert(unit_min_memory));
    } else {
      // memory_size valid
      memory_size_ = user_spec.memory_size();
    }

    LOG_INFO("ObUnitResource update_and_check: MEMORY", KR(ret), K(memory_size_), K(user_spec),
        KPC(this));
  }
  return ret;
}

int ObUnitResource::update_and_check_log_disk_(const ObUnitResource &user_spec)
{
  int ret = OB_SUCCESS;
  const int64_t unit_min_log_disk_size = UNIT_MIN_LOG_DISK_SIZE;
  if (! user_spec.is_log_disk_size_valid()) {
    // not specified, need not update
  } else {
    if (0 == user_spec.log_disk_size()) {
      // log_disk_size is only allowed to be 0 for hidden SYS
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Log_disk_size can only be specified as 0 for hidden SYS, not for normal unit.", KR(ret), K(user_spec));
    } else if (user_spec.log_disk_size() < unit_min_log_disk_size) {
      ret = OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT;
      LOG_WARN("log_disk_size is below limit", KR(ret), K(user_spec), K(unit_min_log_disk_size));
      ObCStringHelper helper;
      LOG_USER_ERROR(OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT, "LOG_DISK_SIZE",
          helper.convert(unit_min_log_disk_size));
    } else {
      log_disk_size_ = user_spec.log_disk_size();
    }

    LOG_INFO("ObUnitResource update_and_check: LOG_DISK", KR(ret), K(log_disk_size_), K(user_spec),
        KPC(this));
  }
  return ret;
}

int ObUnitResource::update_and_check_data_disk_(const ObUnitResource &user_spec)
{
  int ret = OB_SUCCESS;
  if (0 == user_spec.data_disk_size_) {
    if (! GCTX.is_shared_storage_mode()) {
      // for upgrade compatability in shared-nothing mode
      data_disk_size_ = DEFAULT_DATA_DISK_SIZE;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("user specified data_disk_size should not be 0", KR(ret));
    }
  } else if (! user_spec.is_data_disk_size_valid()) {
    // not specified, do not need to update
  } else {
    const int64_t unit_min_data_disk_size = UNIT_MIN_DATA_DISK_SIZE;
    if (OB_FAIL(check_data_disk_size_supported())) {
      LOG_WARN("failed to check data_disk_size supported", KR(ret), K(user_spec));
    } else if (user_spec.data_disk_size() < unit_min_data_disk_size) {
      ret = OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT;
      LOG_WARN("data_disk_size is below limit", KR(ret), K(user_spec),
          K(unit_min_data_disk_size));
      ObCStringHelper helper;
      LOG_USER_ERROR(OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT, "DATA_DISK_SIZE",
          helper.convert(unit_min_data_disk_size));
    } else {
      data_disk_size_ = user_spec.data_disk_size();
    }

    LOG_INFO("ObUnitResource update_and_check: DATA_DISK", KR(ret), K(data_disk_size_), K(user_spec),
        KPC(this));
  }
  return ret;
}

int ObUnitResource::update_and_check_iops_(const ObUnitResource &user_spec)
{
  int ret = OB_SUCCESS;
  const int64_t unit_min_iops = UNIT_MIN_IOPS;

  if (! user_spec.is_max_iops_valid() &&
      ! user_spec.is_min_iops_valid() &&
      ! user_spec.is_iops_weight_valid()) {
    // not specified, need not update
  } else {
    int64_t new_min_iops = min_iops_;
    int64_t new_max_iops = max_iops_;

    if (user_spec.is_max_iops_valid()) {
      new_max_iops = user_spec.max_iops();
    }

    if (user_spec.is_min_iops_valid()) {
      new_min_iops = user_spec.min_iops();
    }

    if (new_max_iops < new_min_iops) {
      if (user_spec.is_max_iops_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("max_iops is little than min_iops", KR(ret), K(new_min_iops), K(new_max_iops),
            K(user_spec), KPC(this));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "MAX_IOPS, MAX_IOPS is little than MIN_IOPS");
      } else if (user_spec.is_min_iops_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("min_iops is greater than max_iops", KR(ret), K(new_min_iops), K(new_max_iops),
            K(user_spec), KPC(this));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "MIN_IOPS, MIN_IOPS is greater than MAX_IOPS");
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected, user not specify max_iops and min_iops, but max_iops < min_iops",
            K(new_max_iops), K(new_min_iops), K(user_spec), KPC(this));
      }
    } else if (new_min_iops < unit_min_iops) {
      // min_iops must be specified, so report error on min_iops
      ret = OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT;
      LOG_WARN("min_iops is below limit", KR(ret), K(min_iops_), K(max_iops_), K(user_spec), K(unit_min_iops));
      ObCStringHelper helper;
      LOG_USER_ERROR(OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT, "MIN_IOPS", helper.convert(unit_min_iops));
    } else {
      // all valid
      min_iops_ = new_min_iops;
      max_iops_ = new_max_iops;
    }

    if (OB_SUCCESS == ret) {
      if (user_spec.is_iops_weight_valid()) {
        // user specified
        iops_weight_ = user_spec.iops_weight();
      }
    }
  }

  LOG_INFO("ObUnitResource update_and_check: IOPS", KR(ret), K(min_iops_), K(max_iops_),
      K(iops_weight_), K(user_spec), KPC(this));
  return ret;
}

int ObUnitResource::update_and_check_net_bandwidth_(const ObUnitResource &user_spec)
{
  int ret = OB_SUCCESS;
  bool is_during_upgrade = false;
  if (OB_FAIL(check_net_bandwidth_supported(user_spec, true/*need_check_user_spec*/,
                                            is_during_upgrade))) {
    LOG_WARN("failed to check_net_bandwidth_supported", KR(ret), K(user_spec));
  } else if (is_during_upgrade) {
    // during upgrade, max_net_bandwidth_ & net_bandiwidth_weigth_ keep unchanged
  } else {
    const int64_t unit_min_net_bandwidth = UNIT_MIN_NET_BANDWIDTH;
    if (user_spec.is_max_net_bandwidth_valid()) {
      if (user_spec.max_net_bandwidth_ < unit_min_net_bandwidth) {
        ret = OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT;
        LOG_WARN("max_net_bandwidth is below limit", KR(ret), K(user_spec), K(unit_min_net_bandwidth));
        ObCStringHelper helper;
        LOG_USER_ERROR(OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT, "MAX_NET_BANDWIDTH", helper.convert(unit_min_net_bandwidth));
      } else {
        // user specified
        max_net_bandwidth_ = user_spec.max_net_bandwidth();
      }
    }
    if (OB_SUCC(ret)) {
      if (user_spec.is_net_bandwidth_weight_valid()) {
        // user specified
        net_bandwidth_weight_ = user_spec.net_bandwidth_weight();
      }
    }
  }

  LOG_INFO("ObUnitResource update_and_check: NET_BANDWIDTH", KR(ret), K(max_net_bandwidth_),
      K(net_bandwidth_weight_), K(user_spec), KPC(this));
  return ret;
}

int ObUnitResource::update_and_check_valid_for_unit(const ObUnitResource &user_spec)
{
  int ret = OB_SUCCESS;

  // generate a self copy
  const ObUnitResource self_copy = *this;

  if (OB_FAIL(! is_valid_for_unit())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("self is not valid for unit", KR(ret), KPC(this));
  }

  // check CPU
  if (OB_SUCCESS == ret) {
    ret = update_and_check_cpu_(user_spec);
  }

  // check MEMORY
  if (OB_SUCCESS == ret) {
    ret = update_and_check_mem_(user_spec);
  }

  // check LOG_DISK_SIZE
  if (OB_SUCCESS == ret) {
    ret = update_and_check_log_disk_(user_spec);
  }

  // check DATA_DISK_SIZE
  if (OB_SUCCESS == ret) {
    ret = update_and_check_data_disk_(user_spec);
  }

  // check IOPS
  if (OB_SUCCESS == ret) {
    ret = update_and_check_iops_(user_spec);
  }

  // check NET_BANDWIDTH
  if (OB_SUCCESS == ret) {
    ret = update_and_check_net_bandwidth_(user_spec);
  }

  LOG_INFO("update unit resource by user spec and check valid for unit", KR(ret), K(user_spec), KPC(this));

  if (OB_FAIL(ret)) {
    // reset self after fail
    *this = self_copy;
  }
  return ret;
}

ObUnitResource &ObUnitResource::operator=(const ObUnitResource &other)
{
  if (this != &other) {
    max_cpu_ = other.max_cpu_;
    min_cpu_ = other.min_cpu_;
    memory_size_ = other.memory_size_;
    log_disk_size_ = other.log_disk_size_;
    max_iops_ = other.max_iops_;
    min_iops_ = other.min_iops_;
    iops_weight_ = other.iops_weight_;
    data_disk_size_ = other.data_disk_size_;
    max_net_bandwidth_ = other.max_net_bandwidth_;
    net_bandwidth_weight_ = other.net_bandwidth_weight_;
  }
  return *this;
}

ObUnitResource ObUnitResource::operator+(const ObUnitResource &r) const
{
  ObUnitResource result;
  CALCULATE_CONFIG((*this), +, r, result);
  return result;
}

ObUnitResource ObUnitResource::operator-(const ObUnitResource &r) const
{
  ObUnitResource result;
  CALCULATE_CONFIG((*this), -, r, result);
  return result;
}

ObUnitResource &ObUnitResource::operator+=(const ObUnitResource &r)
{
  CALCULATE_CONFIG((*this), +, r, (*this));
  return *this;
}

ObUnitResource &ObUnitResource::operator-=(const ObUnitResource &r)
{
  CALCULATE_CONFIG((*this), -, r, (*this));
  return *this;
}

ObUnitResource ObUnitResource::operator*(const int64_t count) const
{
  ObUnitResource result;
  CALCULATE_CONFIG_WITH_CONSTANT((*this), *, count, result);
  return result;
}

#undef CALCULATE_CONFIG

#define COMPARE_INT_CONFIG(left, op, right) \
        ((left).memory_size_ op (right).memory_size_) \
        && ((left).log_disk_size_ op (right).log_disk_size_) \
        && ((left).max_iops_ op (right).max_iops_) \
        && ((left).min_iops_ op (right).min_iops_) \
        && ((left).iops_weight_ op (right).iops_weight_) \
        && ((left).data_disk_size_ op (right).data_disk_size_) \
        && ((left).max_net_bandwidth_ op (right).max_net_bandwidth_) \
        && ((left).net_bandwidth_weight_ op (right).net_bandwidth_weight_)

bool ObUnitResource::operator==(const ObUnitResource &config) const
{
  bool result = false;
  result = std::fabs(this->max_cpu_ - config.max_cpu_) < CPU_EPSILON
      && std::fabs(this->min_cpu_ - config.min_cpu_) < CPU_EPSILON
      && COMPARE_INT_CONFIG((*this), ==, config);
  return result;
}

#undef COMPARE_INT_CONFIG

DEF_TO_STRING(ObUnitResource)
{
  int64_t pos = 0;
  J_OBJ_START();
  // cpu, mem
  (void)databuff_printf(buf, buf_len, pos, "min_cpu:%.6g, max_cpu:%.6g, memory_size:\"%.9gGB\", ",
      min_cpu_, max_cpu_, (double)memory_size_/1024/1024/1024);
  // log_disk
  if (log_disk_size_ > 0) {
    (void)databuff_printf(buf, buf_len, pos, "log_disk_size:\"%.9gGB\", ", (double)log_disk_size_/1024/1024/1024);
  } else {
    (void)databuff_printf(buf, buf_len, pos, "log_disk_size:%ld, ", log_disk_size_);
  }
  // data_disk
  if (data_disk_size_ > 0) {
    (void)databuff_printf(buf, buf_len, pos, "data_disk_size:\"%.9gGB\", ", (double)data_disk_size_/1024/1024/1024);
  } else {
    (void)databuff_printf(buf, buf_len, pos, "data_disk_size:%ld, ", data_disk_size_);
  }
  // iops
  (void)databuff_printf(buf, buf_len, pos, "min_iops:%ld, max_iops:%ld, iops_weight:%ld, ", min_iops_, max_iops_, iops_weight_);
  // net bandwidth
  if (INT64_MAX == max_net_bandwidth_) {
    (void)databuff_printf(buf, buf_len, pos, "max_net_bandwidth:INT64_MAX, ");
  } else {
    (void)databuff_printf(buf, buf_len, pos, "max_net_bandwidth:\"%.9gGB\", ", (double)max_net_bandwidth_/1024/1024/1024);
  }
  (void)databuff_printf(buf, buf_len, pos, "net_bandwidth_weight:%ld, ", net_bandwidth_weight_);
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER(ObUnitResource,
                    max_cpu_,
                    min_cpu_,
                    memory_size_,
                    log_disk_size_,
                    max_iops_,
                    min_iops_,
                    iops_weight_,
                    data_disk_size_,
                    max_net_bandwidth_,
                    net_bandwidth_weight_);


bool ObUnitResource::has_expanded_resource_than(const ObUnitResource &other) const
{
  // check if any of max_cpu, min_cpu, memory_size, log_disk_size, data_disk_size is greater than other
  bool b_ret = false;
  if ((is_max_cpu_valid() && other.is_max_cpu_valid() && max_cpu_ > other.max_cpu())
      || (is_min_cpu_valid() && other.is_min_cpu_valid() && min_cpu_ > other.min_cpu())
      || (is_memory_size_valid() && other.is_memory_size_valid() && memory_size_ > other.memory_size())
      || (is_log_disk_size_valid() && other.is_log_disk_size_valid() && log_disk_size_ > other.log_disk_size())
      || (is_data_disk_size_valid() && other.is_data_disk_size_valid() && data_disk_size_ > other.data_disk_size()))
  {
    b_ret = true;
  } else {
    b_ret = false;
  }
  return b_ret;
}

bool ObUnitResource::has_shrunk_resource_than(const ObUnitResource &other) const
{
  // check if any of max_cpu, min_cpu, memory_size, log_disk_size, data_disk_size is smaller than other
  bool b_ret = false;
  if ((is_max_cpu_valid() && other.is_max_cpu_valid() && max_cpu_ < other.max_cpu())
      || (is_min_cpu_valid() && other.is_min_cpu_valid() && min_cpu_ < other.min_cpu())
      || (is_memory_size_valid() && other.is_memory_size_valid() && memory_size_ < other.memory_size())
      || (is_log_disk_size_valid() && other.is_log_disk_size_valid() && log_disk_size_ < other.log_disk_size())
      || (is_data_disk_size_valid() && other.is_data_disk_size_valid() && data_disk_size_ < other.data_disk_size()))
  {
    b_ret = true;
  } else {
    b_ret = false;
  }
  return b_ret;
}

int ObUnitResource::divide_meta_tenant(ObUnitResource &meta_resource)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! is_valid_for_unit())) {
    ret = OB_RESOURCE_UNIT_VALUE_INVALID;
    LOG_WARN("self is invalid for unit, can not divide meta tenant", KR(ret), KPC(this));
  } else {
    // copy of original resource
    const ObUnitResource unit_resource = *this;

    ///////////////////// CPU ///////////////////
    // CPU is shared by USER and META
    // USER resource is equal to UNIT, META resource is extra allocated
    const double m_min_cpu = gen_meta_tenant_cpu(min_cpu_);
    const double m_max_cpu = gen_meta_tenant_cpu(max_cpu_);
    const double u_min_cpu = min_cpu_;
    const double u_max_cpu = max_cpu_;

    ///////////////////// MEMORY ///////////////////
    const int64_t m_memory_size = gen_meta_tenant_memory(memory_size_);
    const int64_t u_memory_size = memory_size_ - m_memory_size;

    ///////////////////// LOG_DISK ///////////////////
    const int64_t m_log_disk_size = gen_meta_tenant_log_disk_size(log_disk_size_);
    const int64_t u_log_disk_size = log_disk_size_ - m_log_disk_size;

    ///////////////////// DATA_DISK ///////////////////
    const int64_t m_data_disk_size = !GCTX.is_shared_storage_mode() ? DEFAULT_DATA_DISK_SIZE :
                                      gen_meta_tenant_data_disk_size(data_disk_size_);
    const int64_t u_data_disk_size = !GCTX.is_shared_storage_mode() ? DEFAULT_DATA_DISK_SIZE :
                                      (data_disk_size_ - m_data_disk_size);

    ///////////////////// IOPS ///////////////////
    // IOPS is shared by USER and META
    // USER resource is equal to UNIT, META resource is extra allocated
    const int64_t m_min_iops = gen_meta_tenant_iops(min_iops_);
    const int64_t m_max_iops = gen_meta_tenant_iops(max_iops_);
    const int64_t m_iops_weight = gen_meta_tenant_iops_weight(iops_weight_);
    const int64_t u_min_iops = min_iops_;
    const int64_t u_max_iops = max_iops_;
    const int64_t u_iops_weight = iops_weight_;

    ///////////////////// NET BANDWIDTH ///////////////////
    const int64_t m_max_net_bandwidth = gen_meta_tenant_net_bandwidth(max_net_bandwidth_);
    const int64_t m_net_bandwidth_weight = gen_meta_tenant_net_bandwidth_weight(net_bandwidth_weight_);
    const int64_t u_max_net_bandwidth = max_net_bandwidth_;
    const int64_t u_net_bandwidth_weight = net_bandwidth_weight_;

    //////////////////// check valid //////////////
    const ObUnitResource meta_ur(
        m_max_cpu,
        m_min_cpu,
        m_memory_size,
        m_log_disk_size,
        m_data_disk_size,
        m_max_iops,
        m_min_iops,
        m_iops_weight,
        m_max_net_bandwidth,
        m_net_bandwidth_weight);

    const ObUnitResource user_ur(
        u_max_cpu,
        u_min_cpu,
        u_memory_size,
        u_log_disk_size,
        u_data_disk_size,
        u_max_iops,
        u_min_iops,
        u_iops_weight,
        u_max_net_bandwidth,
        u_net_bandwidth_weight);

    if (! meta_ur.is_valid_for_meta_tenant() ||
        ! user_ur.is_valid_for_user_tenant()) {
      ret = OB_RESOURCE_UNIT_VALUE_INVALID;
      LOG_WARN("meta tenant or user tenant resource are invalid after divide from unit resource",
          KR(ret), K(meta_ur), K(user_ur), KPC(this));
    } else {
      meta_resource = meta_ur;
      *this = user_ur;
    }

    LOG_INFO("divide meta tenant resource finish", KR(ret), K(meta_resource),
        "user_resource", *this, K(unit_resource));
  }
  return ret;
}

int ObUnitResource::gen_sys_tenant_default_unit_resource(const bool is_hidden_sys)
{
  int ret = OB_SUCCESS;

  reset();
  memory_size_ = is_hidden_sys ? GMEMCONF.get_hidden_sys_memory() :
                                 max(UNIT_MIN_MEMORY, GCONF.__min_full_resource_pool_memory);
  max_cpu_ = GCONF.get_sys_tenant_default_max_cpu();
  min_cpu_ = GCONF.get_sys_tenant_default_min_cpu();
  // for hidden SYS tenant, log_disk_size is 0, data_disk_size is determined by hidden_sys_memory or _ss_hidden_sys_tenant_data_disk_size
  // for real SYS tenant, log_disk_size and data_disk_size are determined
  // by real_memory_size (including extra_memory)
  int64_t real_memory_size = memory_size_ + GMEMCONF.get_extra_memory();
  log_disk_size_ = is_hidden_sys ? 0 : max(real_memory_size, UNIT_MIN_LOG_DISK_SIZE);
  max_iops_ = get_default_iops();
  min_iops_ = max_iops_;
  iops_weight_ = get_default_iops_weight(min_cpu_);
  max_net_bandwidth_ = get_default_net_bandwidth();
  net_bandwidth_weight_ = get_default_net_bandwidth_weight(min_cpu_);
  int64_t hidden_sys_data_disk_size = 0;
#ifdef OB_BUILD_SHARED_STORAGE
  if (GCTX.is_shared_storage_mode()) {
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(OB_SERVER_DISK_SPACE_MGR.gen_hidden_sys_data_disk_size(hidden_sys_data_disk_size))) {
      LOG_WARN("fail to generate hidden sys data_disk_size", KR(ret), K(hidden_sys_data_disk_size));
    }
  }
#endif
  if (OB_FAIL(ret)) {
  } else {
    data_disk_size_ = !GCTX.is_shared_storage_mode() ? DEFAULT_DATA_DISK_SIZE :
                        (is_hidden_sys ? hidden_sys_data_disk_size :
                        max(get_default_data_disk_size(memory_size_), UNIT_MIN_DATA_DISK_SIZE));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(! is_valid_for_unit())) {
    ret = OB_RESOURCE_UNIT_VALUE_INVALID;
    LOG_ERROR("sys tenant default unit resource is not valid for unit", KR(ret), K(is_hidden_sys), KPC(this));
  }

  LOG_INFO("gen_sys_tenant_default_unit_resource", KR(ret), K(is_hidden_sys), KPC(this), K(lbt()));

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

}//end namespace share
}//end namespace oceanbase
