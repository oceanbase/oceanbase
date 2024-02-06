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

#include "lib/worker.h"           // LOG_USER_ERROR
#include "lib/oblog/ob_log.h"     // LOG_USER_ERROR
#include "share/ob_errno.h"
#include "share/config/ob_server_config.h"    // GCONF

#include "share/unit/ob_unit_resource.h"

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
  } while (false)

ObUnitResource::ObUnitResource(
    const double max_cpu,
    const double min_cpu,
    const int64_t memory_size,
    const int64_t log_disk_size,
    const int64_t max_iops,
    const int64_t min_iops,
    const int64_t iops_weight) :
    max_cpu_(max_cpu),
    min_cpu_(min_cpu),
    memory_size_(memory_size),
    log_disk_size_(log_disk_size),
    max_iops_(max_iops),
    min_iops_(min_iops),
    iops_weight_(iops_weight)
{
}

void ObUnitResource::reset()
{
  max_cpu_ = 0;
  min_cpu_ = 0;
  memory_size_ = 0;
  log_disk_size_ = INVALID_LOG_DISK_SIZE;
  max_iops_ = 0;
  min_iops_ = 0;
  iops_weight_ = INVALID_IOPS_WEIGHT;
}

void ObUnitResource::set(
    const double max_cpu,
    const double min_cpu,
    const int64_t memory_size,
    const int64_t log_disk_size,
    const int64_t max_iops,
    const int64_t min_iops,
    const int64_t iops_weight)
{
  max_cpu_ = max_cpu;
  min_cpu_ = min_cpu;
  memory_size_ = memory_size;
  log_disk_size_ = log_disk_size;
  max_iops_ = max_iops;
  min_iops_ = min_iops;
  iops_weight_ = iops_weight;
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
    LOG_USER_ERROR(OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT, "MEMORY_SIZE", to_cstring(unit_min_memory));
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
      LOG_USER_ERROR(OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT, "LOG_DISK_SIZE",
          to_cstring(unit_min_log_disk_size));
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
        LOG_USER_ERROR(OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT, "MIN_IOPS", to_cstring(unit_min_iops));
      } else {
        ret = OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT;
        LOG_WARN("max_iops is below limit", KR(ret), K(min_iops_), K(max_iops_), K(user_spec), K(unit_min_iops));
        LOG_USER_ERROR(OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT, "MAX_IOPS", to_cstring(unit_min_iops));
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
        // not specified, init to 0
        iops_weight_ = DEFAULT_IOPS_WEIGHT;
      }
    }
  }

  LOG_INFO("ObUnitResource init_and_check: IOPS", KR(ret), K(min_iops_), K(max_iops_),
      K(iops_weight_), K(user_spec), KPC(this), K(unit_min_iops));
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

  // check IOPS
  if (OB_SUCCESS == ret) {
    ret = init_and_check_iops_(user_spec);
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
      LOG_USER_ERROR(OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT, "MEMORY_SIZE", to_cstring(unit_min_memory));
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
      LOG_USER_ERROR(OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT, "LOG_DISK_SIZE", to_cstring(unit_min_log_disk_size));
    } else {
      log_disk_size_ = user_spec.log_disk_size();
    }

    LOG_INFO("ObUnitResource update_and_check: LOG_DISK", KR(ret), K(log_disk_size_), K(user_spec),
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
      LOG_USER_ERROR(OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT, "MIN_IOPS", to_cstring(unit_min_iops));
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

  // check IOPS
  if (OB_SUCCESS == ret) {
    ret = update_and_check_iops_(user_spec);
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
        && ((left).iops_weight_ op (right).iops_weight_)

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
  (void)databuff_printf(buf, buf_len, pos,
      "min_cpu:%.6g, max_cpu:%.6g, memory_size:\"%.9gGB\", "
      "log_disk_size:\"%.9gGB\", min_iops:%ld, max_iops:%ld, iops_weight:%ld",
      min_cpu_, max_cpu_, (double)memory_size_/1024/1024/1024,
      is_log_disk_size_valid() ? (double)log_disk_size_/1024/1024/1024 : log_disk_size_,
      min_iops_, max_iops_, iops_weight_);
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
                    iops_weight_);


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

    ///////////////////// IOPS ///////////////////
    // IOPS is shared by USER and META
    // USER resource is equal to UNIT, META resource is extra allocated
    const int64_t m_min_iops = gen_meta_tenant_iops(min_iops_);
    const int64_t m_max_iops = gen_meta_tenant_iops(max_iops_);
    const int64_t m_iops_weight = gen_meta_tenant_iops_weight(iops_weight_);
    const int64_t u_min_iops = min_iops_;
    const int64_t u_max_iops = max_iops_;
    const int64_t u_iops_weight = iops_weight_;

    //////////////////// check valid //////////////
    const ObUnitResource meta_ur(
        m_max_cpu,
        m_min_cpu,
        m_memory_size,
        m_log_disk_size,
        m_max_iops,
        m_min_iops,
        m_iops_weight);

    const ObUnitResource user_ur(
        u_max_cpu,
        u_min_cpu,
        u_memory_size,
        u_log_disk_size,
        u_max_iops,
        u_min_iops,
        u_iops_weight);

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
  // for hidden SYS tenant, log_disk_size is 0
  // for real SYS tenant, log_disk_size is determined by real_memory_size (including extra_memory)
  int64_t real_memory_size = memory_size_ + GMEMCONF.get_extra_memory();
  log_disk_size_ = is_hidden_sys ? 0 : max(real_memory_size, UNIT_MIN_LOG_DISK_SIZE);
  max_iops_ = get_default_iops();
  min_iops_ = max_iops_;
  iops_weight_ = get_default_iops_weight(min_cpu_);

  if (OB_UNLIKELY(! is_valid_for_unit())) {
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
