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

#ifndef OCEANBASE_SHARE_UNIT_OB_UNIT_INFO_H_
#define OCEANBASE_SHARE_UNIT_OB_UNIT_INFO_H_

#include "lib/net/ob_addr.h"
#include "lib/string/ob_fixed_length_string.h"
#include "lib/container/ob_array.h"
#include "common/ob_zone.h"

#include "share/unit/ob_unit_config.h"     // ObUnitConfig
#include "share/unit/ob_resource_pool.h"   // ObResourcePool

namespace oceanbase
{
namespace share
{
struct ObUnit
{
  OB_UNIS_VERSION(1);
public:
  enum Status
  {
    UNIT_STATUS_ACTIVE = 0,
    UNIT_STATUS_DELETING,
    UNIT_STATUS_MAX,
  };
public:
  static const char *const unit_status_strings[UNIT_STATUS_MAX];
  static Status str_to_unit_status(const ObString &str);
public:
  ObUnit();
  ~ObUnit() {}
  inline bool operator <(const ObUnit &unit) const;
  void reset();
  bool is_valid() const;
  bool is_manual_migrate() const { return is_manual_migrate_; }
  bool is_active_status() const { return UNIT_STATUS_ACTIVE == status_; }
  int get_unit_status_str(const char *&status) const;
  Status get_unit_status() const { return status_; }

  DECLARE_TO_STRING;

  uint64_t unit_id_;
  uint64_t resource_pool_id_;
  /*
   * unit_group_id -1 means invalid id
   * unit_group_id 0 means this unit not organised into any group
   * unit_group_id positive integer means a good unit group ID
   */
  uint64_t unit_group_id_;
  common::ObZone zone_;
  common::ObAddr server_;
  common::ObAddr migrate_from_server_;           // used when migrate unit
  bool is_manual_migrate_;
  Status status_;
  common::ObReplicaType replica_type_;
};

inline bool ObUnit::operator <(const ObUnit &unit) const
{
  return resource_pool_id_ < unit.resource_pool_id_;
}

struct ObUnitInfo
{
  ObUnitInfo() : unit_(), config_(), pool_() {}
  ~ObUnitInfo() {}

  void reset() { unit_.reset(); config_.reset(); pool_.reset(); }
  bool is_valid() const { return unit_.is_valid() && config_.is_valid() && pool_.is_valid(); }
  int assign(const ObUnitInfo &other);
  TO_STRING_KV(K_(unit), K_(config), K_(pool));

  ObUnit unit_;
  ObUnitConfig config_;
  ObResourcePool pool_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObUnitInfo);
};

struct ObSimpleUnitGroup
{
public:
 ObSimpleUnitGroup(uint64_t unit_group_id, ObUnit::Status status) : unit_group_id_(unit_group_id), status_(status) {}
 ObSimpleUnitGroup(){reset();}
 ~ObSimpleUnitGroup() {}
 void reset()
 {
  unit_group_id_ = OB_INVALID_ID;
  status_ = ObUnit::UNIT_STATUS_MAX;
 }
 bool is_valid() const;
 int assign(const ObSimpleUnitGroup &other)
 {
  unit_group_id_ = other.unit_group_id_;
  status_ = other.status_;
  return OB_SUCCESS;
 }
 bool is_active() const
 { return ObUnit::UNIT_STATUS_ACTIVE == status_; }
 uint64_t get_unit_group_id() const { return unit_group_id_; }
 ObUnit::Status get_status() const  { return status_; }
 TO_STRING_KV(K_(unit_group_id), K_(status));
private:
  uint64_t unit_group_id_;
  ObUnit::Status status_;
};

}//end namespace share
}//end namespace oceanbase

#endif //OCEANBASE_SHARE_UNIT_OB_UNIT_INFO_H_
