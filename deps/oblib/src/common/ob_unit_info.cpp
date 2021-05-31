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

#include "common/ob_unit_info.h"
#include <cmath>

namespace oceanbase {
using namespace common;
namespace share {
const char* const ObUnit::unit_status_strings[ObUnit::UNIT_STATUS_MAX] = {
    "ACTIVE",
    "DELETING",
};

ObUnit::ObUnit()
{
  reset();
}

void ObUnit::reset()
{
  unit_id_ = OB_INVALID_ID;
  resource_pool_id_ = OB_INVALID_ID;
  group_id_ = OB_INVALID_ID;
  zone_.reset();
  server_.reset();
  migrate_from_server_.reset();
  is_manual_migrate_ = false;
  status_ = UNIT_STATUS_MAX;
  replica_type_ = REPLICA_TYPE_FULL;
}

bool ObUnit::is_valid() const
{
  // it's ok for migrate_from_server to be invalid
  return OB_INVALID_ID != unit_id_ && OB_INVALID_ID != resource_pool_id_ && OB_INVALID_ID != group_id_ &&
         !zone_.is_empty() && server_.is_valid() && server_ != migrate_from_server_ &&
         (status_ >= UNIT_STATUS_ACTIVE && status_ < UNIT_STATUS_MAX) &&
         ObReplicaTypeCheck::is_replica_type_valid(replica_type_);
}

int ObUnit::get_unit_status_str(const char*& status_str) const
{
  int ret = OB_SUCCESS;
  if (status_ >= UNIT_STATUS_MAX || status_ < UNIT_STATUS_ACTIVE) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(status_));
  } else {
    status_str = unit_status_strings[status_];
  }
  return ret;
}

DEF_TO_STRING(ObUnit)
{
  int64_t pos = 0;
  J_KV(K_(unit_id),
      K_(resource_pool_id),
      K_(group_id),
      K_(zone),
      K_(server),
      K_(migrate_from_server),
      K_(is_manual_migrate),
      K_(status),
      K_(replica_type));
  return pos;
}

OB_SERIALIZE_MEMBER(ObUnit, unit_id_, resource_pool_id_, group_id_, zone_, server_, migrate_from_server_,
    is_manual_migrate_, status_, replica_type_);

ObUnitConfig::ObUnitConfig()
{
  reset();
}

#define CALCULATE_CONFIG(l, op, r, result)                              \
  do {                                                                  \
    result.max_cpu_ = l.max_cpu_ op r.max_cpu_;                         \
    result.min_cpu_ = l.min_cpu_ op r.min_cpu_;                         \
    result.max_memory_ = l.max_memory_ op r.max_memory_;                \
    result.min_memory_ = l.min_memory_ op r.min_memory_;                \
    result.max_iops_ = l.max_iops_ op r.max_iops_;                      \
    result.min_iops_ = l.min_iops_ op r.min_iops_;                      \
    result.max_disk_size_ = l.max_disk_size_ op r.max_disk_size_;       \
    result.max_session_num_ = l.max_session_num_ op r.max_session_num_; \
  } while (false)

ObUnitConfig ObUnitConfig::operator+(const ObUnitConfig& r) const
{
  ObUnitConfig result;
  CALCULATE_CONFIG((*this), +, r, result);
  return result;
}

ObUnitConfig ObUnitConfig::operator-(const ObUnitConfig& r) const
{
  ObUnitConfig result;
  CALCULATE_CONFIG((*this), -, r, result);
  return result;
}

ObUnitConfig& ObUnitConfig::operator+=(const ObUnitConfig& r)
{
  CALCULATE_CONFIG((*this), +, r, (*this));
  return *this;
}

ObUnitConfig& ObUnitConfig::operator-=(const ObUnitConfig& r)
{
  CALCULATE_CONFIG((*this), -, r, (*this));
  return *this;
}

ObUnitConfig ObUnitConfig::operator*(const int64_t count) const
{
  ObUnitConfig result;
  result.max_cpu_ = max_cpu_ * static_cast<double>(count);
  result.min_cpu_ = min_cpu_ * static_cast<double>(count);
  result.max_memory_ = max_memory_ * count;
  result.min_memory_ = min_memory_ * count;
  result.max_iops_ = max_iops_ * count;
  result.min_iops_ = min_iops_ * count;
  result.max_disk_size_ = max_disk_size_ * count;
  result.max_session_num_ = max_session_num_ * count;
  return result;
}

#undef CALCULATE_CONFIG

#define COMPARE_INT_CONFIG(left, op, right)                                                         \
  ((left).max_memory_ op(right).max_memory_) && ((left).min_memory_ op(right).min_memory_) &&       \
      ((left).max_disk_size_ op(right).max_disk_size_) && ((left).max_iops_ op(right).max_iops_) && \
      ((left).min_iops_ op(right).min_iops_) && ((left).max_session_num_ op(right).max_session_num_)

/* only consider cpu and memory just now
        && left.min_memory_ op right.min_memory_ \
        && left.max_iops_ op right.max_iops_ \
        && left.min_iops_ op right.min_iops_ \
        && left.max_disk_size_ op right.max_disk_size_ \
        && left.max_session_num_ op right.max_session_num_; \
*/

bool ObUnitConfig::operator==(const ObUnitConfig& config) const
{
  bool result = false;
  result = std::fabs(this->max_cpu_ - config.max_cpu_) < CPU_EPSILON &&
           std::fabs(this->min_cpu_ - config.min_cpu_) < CPU_EPSILON && COMPARE_INT_CONFIG((*this), ==, config);
  return result;
}

#undef COMPARE_INT_CONFIG

void ObUnitConfig::reset()
{
  unit_config_id_ = OB_INVALID_ID;
  name_.reset();
  max_cpu_ = 0;
  min_cpu_ = 0;
  max_memory_ = 0;
  min_memory_ = 0;
  max_disk_size_ = 0;
  max_iops_ = 0;
  min_iops_ = 0;
  max_session_num_ = 0;
}

bool ObUnitConfig::is_valid() const
{
  return !name_.is_empty() && max_cpu_ > 0 && min_cpu_ >= 0 && max_memory_ > 0 && min_memory_ >= 0 &&
         max_disk_size_ > 0 && max_iops_ > 0 && min_iops_ >= 0 && max_session_num_ > 0;
}

DEF_TO_STRING(ObUnitConfig)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(unit_config_id),
      K_(name),
      K_(max_cpu),
      K_(min_cpu),
      K_(max_memory),
      K_(min_memory),
      K_(max_disk_size),
      K_(max_iops),
      K_(min_iops),
      K_(max_session_num));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER(ObUnitConfig, name_, max_cpu_, min_cpu_, max_memory_, min_memory_, max_disk_size_, max_iops_,
    min_iops_, max_session_num_);

ObResourcePool::ObResourcePool()
{
  reset();
}

void ObResourcePool::reset()
{
  resource_pool_id_ = OB_INVALID_ID;
  name_.reset();
  unit_count_ = 0;
  unit_config_id_ = OB_INVALID_ID;
  zone_list_.reset();
  tenant_id_ = OB_INVALID_ID;
  replica_type_ = REPLICA_TYPE_FULL;
}

bool ObResourcePool::is_valid() const
{
  return !name_.is_empty() && unit_count_ > 0;
}

int ObResourcePool::assign(const ObResourcePool& other)
{
  int ret = OB_SUCCESS;
  resource_pool_id_ = other.resource_pool_id_;
  name_ = other.name_;
  unit_count_ = other.unit_count_;
  unit_config_id_ = other.unit_config_id_;
  if (OB_FAIL(copy_assign(zone_list_, other.zone_list_))) {
    SHARE_LOG(WARN, "failed to assign zone_list_", K(ret));
  }
  tenant_id_ = other.tenant_id_;
  replica_type_ = other.replica_type_;
  return ret;
}

DEF_TO_STRING(ObResourcePool)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(resource_pool_id),
      K_(name),
      K_(unit_count),
      K_(unit_config_id),
      K_(zone_list),
      K_(tenant_id),
      K_(replica_type));
  J_OBJ_END();
  return pos;
}

OB_SERIALIZE_MEMBER(
    ObResourcePool, resource_pool_id_, name_, unit_count_, unit_config_id_, zone_list_, tenant_id_, replica_type_);

int ObUnitInfo::assign(const ObUnitInfo& other)
{
  int ret = OB_SUCCESS;
  unit_ = other.unit_;
  config_ = other.config_;
  if (OB_FAIL(copy_assign(pool_, other.pool_))) {
    SHARE_LOG(WARN, "failed to assign pool_", K(ret));
  }
  return ret;
}

}  // end namespace share
}  // end namespace oceanbase
