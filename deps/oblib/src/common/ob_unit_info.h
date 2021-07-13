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

#ifndef OCEANBASE_SHARE_OB_UNIT_INFO_H_
#define OCEANBASE_SHARE_OB_UNIT_INFO_H_

#include "lib/net/ob_addr.h"
#include "lib/string/ob_fixed_length_string.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_se_array.h"
#include "common/ob_zone.h"

namespace oceanbase {
namespace share {
struct ObUnit {
  OB_UNIS_VERSION(1);

public:
  enum Status {
    UNIT_STATUS_ACTIVE = 0,
    UNIT_STATUS_DELETING,
    UNIT_STATUS_MAX,
  };

public:
  static const char* const unit_status_strings[UNIT_STATUS_MAX];

public:
  ObUnit();
  ~ObUnit()
  {}
  inline bool operator<(const ObUnit& unit) const;
  void reset();
  bool is_valid() const;
  bool is_manual_migrate() const
  {
    return is_manual_migrate_;
  }
  int get_unit_status_str(const char*& status) const;
  Status get_unit_status()
  {
    return status_;
  }

  DECLARE_TO_STRING;

  uint64_t unit_id_;
  uint64_t resource_pool_id_;
  uint64_t group_id_;
  common::ObZone zone_;
  common::ObAddr server_;
  common::ObAddr migrate_from_server_;  // used when migrate unit
  bool is_manual_migrate_;
  Status status_;
  common::ObReplicaType replica_type_;
};

inline bool ObUnit::operator<(const ObUnit& unit) const
{
  return resource_pool_id_ < unit.resource_pool_id_;
}

typedef common::ObFixedLengthString<common::MAX_UNIT_CONFIG_LENGTH> ObUnitConfigName;
struct ObUnitConfig {
  OB_UNIS_VERSION(1);

public:
  static constexpr double CPU_EPSILON = 0.00001;
  ObUnitConfig();
  ~ObUnitConfig()
  {}
  ObUnitConfig operator+(const ObUnitConfig& config) const;
  ObUnitConfig operator-(const ObUnitConfig& config) const;
  ObUnitConfig& operator+=(const ObUnitConfig& config);
  ObUnitConfig& operator-=(const ObUnitConfig& config);
  ObUnitConfig operator*(const int64_t count) const;
  bool operator==(const ObUnitConfig& config) const;
  void reset();
  bool is_valid() const;

  DECLARE_TO_STRING;

  uint64_t unit_config_id_;
  ObUnitConfigName name_;
  double max_cpu_;
  double min_cpu_;
  int64_t max_memory_;
  int64_t min_memory_;
  int64_t max_disk_size_;
  int64_t max_iops_;
  int64_t min_iops_;
  int64_t max_session_num_;
};

typedef common::ObFixedLengthString<common::MAX_RESOURCE_POOL_LENGTH> ObResourcePoolName;
struct ObResourcePool {
  OB_UNIS_VERSION(1);

public:
  static const int64_t DEFAULT_ZONE_COUNT = 5;

  ObResourcePool();
  ~ObResourcePool()
  {}
  void reset();
  bool is_valid() const;
  int assign(const ObResourcePool& other);
  DECLARE_TO_STRING;

  uint64_t resource_pool_id_;
  ObResourcePoolName name_;
  int64_t unit_count_;
  uint64_t unit_config_id_;
  common::ObSEArray<common::ObZone, DEFAULT_ZONE_COUNT> zone_list_;
  uint64_t tenant_id_;
  common::ObReplicaType replica_type_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObResourcePool);
};

struct ObUnitInfo {
  ObUnitInfo() : unit_(), config_(), pool_()
  {}
  ~ObUnitInfo()
  {}

  void reset()
  {
    unit_.reset();
    config_.reset();
    pool_.reset();
  }
  bool is_valid() const
  {
    return unit_.is_valid() && config_.is_valid() && pool_.is_valid();
  }
  int assign(const ObUnitInfo& other);
  TO_STRING_KV(K_(unit), K_(config), K_(pool));

  ObUnit unit_;
  ObUnitConfig config_;
  ObResourcePool pool_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObUnitInfo);
};

}  // end namespace share
}  // end namespace oceanbase

#endif  // OCEANBASE_SHARE_OB_UNIT_INFO_H_
