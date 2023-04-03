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

#ifndef OCEANBASE_SHARE_UNIT_INFO_BUILDER_H_
#define OCEANBASE_SHARE_UNIT_INFO_BUILDER_H_

#include "lib/container/ob_array.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_unit_table_operator.h"
#include "../share/partition_table/fake_part_property_getter.h"

namespace oceanbase
{
using namespace share::host;
namespace share
{
const ObZone ZONE = "test_zone";
const ObZone ZONE11 = "zone1";
static int64_t TEST_UNIT_MEMORY = OB_UNIT_MIN_MEMORY * 3;
class UnitInfoBuilder
{
public:
  UnitInfoBuilder(common::ObMySQLProxy &proxy) : proxy_(proxy)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ut_operator_.init(proxy))) {
      SHARE_LOG(WARN, "unit table operator init failed", K(ret));
    }
  }
  virtual ~UnitInfoBuilder() {}

  UnitInfoBuilder &add_unit(const uint64_t unit_id, const uint64_t pool_id,
                            const uint64_t group_id,
                            const common::ObAddr &server,
                            const common::ObZone &zone = ZONE,
                            const common::ObAddr &from_server = E);
  UnitInfoBuilder &add_config(const uint64_t config_id, const double max_cpu);
  UnitInfoBuilder &add_pool(const uint64_t pool_id, const int64_t unit_count,
                            const uint64_t config_id,
                            const common::ObArray<common::ObZone> &zone_list,
                            const uint64_t tenant_id = OB_SYS_TENANT_ID);
  static bool unit_equal(const ObUnit &l, const ObUnit &r);
  static bool config_equal(const ObUnitConfig &l, const ObUnitConfig &r);
  static bool pool_equal(const ObResourcePool &l, const ObResourcePool &r);
  // write infos to system table
  int write_table();

  common::ObIArray<ObUnitConfig> &get_configs() { return configs_; }
  common::ObIArray<ObUnit> &get_units() { return units_; }
  common::ObIArray<ObResourcePool> &get_pools() { return pools_; }
  const common::ObIArray<ObUnitConfig> &get_configs() const { return configs_; }
  const common::ObIArray<ObUnit> &get_units() const { return units_; }
  const common::ObIArray<ObResourcePool> &get_pools() const { return pools_; }

private:
  common::ObMySQLProxy &proxy_;
  ObUnitTableOperator ut_operator_;
  common::ObArray<ObUnitConfig> configs_;
  common::ObArray<ObResourcePool> pools_;
  common::ObArray<ObUnit> units_;
};

UnitInfoBuilder &UnitInfoBuilder::add_config(
    const uint64_t config_id, const double max_cpu)
{
  int ret = OB_SUCCESS;
  ObUnitConfig config;
  config.unit_config_id_ = config_id;

  int n = 0;
  char name[MAX_UNIT_CONFIG_LENGTH];
  n = snprintf(name, sizeof(name), "config_%lu", config_id);
  if (n >= MAX_UNIT_CONFIG_LENGTH || n < 0) {
    ret = OB_BUF_NOT_ENOUGH;
    SHARE_LOG(WARN, "buf not enough", K(ret));
  } else if (OB_FAIL(config.name_.assign(name))) {
    SHARE_LOG(WARN, "assign failed", K(ret));
  } else {
    config.max_cpu_ = max_cpu;
    config.min_cpu_ = max_cpu;
    config.max_memory_ = TEST_UNIT_MEMORY;
    config.min_memory_ = TEST_UNIT_MEMORY;
    config.log_disk_size_ = OB_UNIT_MIN_LOG_DISK_SIZE;
    config.max_iops_ = OB_UNIT_MIN_IOPS;
    config.min_iops_ = OB_UNIT_MIN_IOPS;
    config.iops_weight_ = 0;
    config.max_session_num_ = OB_UNIT_MIN_SESSION_NUM;
    if (OB_FAIL(configs_.push_back(config))) {
      SHARE_LOG(WARN, "push_back failed", K(ret));
    }
  }
  return *this;
}

UnitInfoBuilder &UnitInfoBuilder::add_pool(
    const uint64_t pool_id, const int64_t unit_count,
    const uint64_t config_id, const common::ObArray<common::ObZone> &zone_list,
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObResourcePool pool;
  pool.resource_pool_id_ = pool_id;
  int n = 0;
  char name[MAX_RESOURCE_POOL_LENGTH];
  n = snprintf(name, sizeof(name), "pool_%lu", pool_id);
  if (n >= MAX_RESOURCE_POOL_LENGTH || n < 0) {
    ret = OB_BUF_NOT_ENOUGH;
    SHARE_LOG(WARN, "buf not enough", K(ret));
  } else if (OB_FAIL(pool.name_.assign(name))) {
    SHARE_LOG(WARN, "assign failed", K(ret));
  } else {
    pool.unit_count_ = unit_count;
    pool.unit_config_id_ = config_id;
    pool.tenant_id_ = tenant_id;
    if (OB_FAIL(pool.zone_list_.assign(zone_list))) {
      SHARE_LOG(WARN, "assign failed", K(zone_list));
    } else if (OB_FAIL(pools_.push_back(pool))) {
      SHARE_LOG(WARN, "push_back failed", K(ret));
    }
  }
  return *this;
}

UnitInfoBuilder &UnitInfoBuilder::add_unit(
    const uint64_t unit_id, const uint64_t pool_id,
    const uint64_t group_id,
    const common::ObAddr &server,
    const common::ObZone &zone,
    const common::ObAddr &from_server)
{
  int ret = OB_SUCCESS;
  ObUnit unit;
  unit.unit_id_ = unit_id;
  unit.resource_pool_id_ = pool_id;
  unit.group_id_ = group_id;
  unit.zone_ = zone;
  unit.server_ = server;
  unit.migrate_from_server_ = from_server;
  unit.status_ = ObUnit::UNIT_STATUS_ACTIVE;
  if (OB_FAIL(units_.push_back(unit))) {
    SHARE_LOG(WARN, "push_back failed", K(ret));
  }
  return *this;
}

bool UnitInfoBuilder::unit_equal(const ObUnit &l, const ObUnit &r)
{
  bool equal = true;
  equal = (l.unit_id_ == r.unit_id_)
      && (l.resource_pool_id_ == r.resource_pool_id_)
      && (l.group_id_ == r.group_id_)
      && (l.zone_ == r.zone_) && (l.server_ == r.server_)
      && (l.migrate_from_server_ == r.migrate_from_server_);
  return equal;
}

bool UnitInfoBuilder::config_equal(const ObUnitConfig &l,
                                   const ObUnitConfig &r)
{
  bool equal = true;
  equal = (l.unit_config_id_ == r.unit_config_id_)
      && (l.name_ == r.name_) && (l.max_cpu_ == r.max_cpu_)
      && (l.max_memory_ == r.max_memory_)
      && (l.max_iops_ == r.max_iops_)
      && (l.log_disk_size_ == r.log_disk_size_)
      && (l.max_session_num_ == r.max_session_num_);
  return equal;
}

bool UnitInfoBuilder::pool_equal(const ObResourcePool &l,
                                 const ObResourcePool &r)
{
  bool equal = true;
  equal = (l.resource_pool_id_ == r.resource_pool_id_)
      && (l.name_ == r.name_) && (l.unit_count_ == r.unit_count_)
      && (l.unit_config_id_ == r.unit_config_id_)
      && (l.tenant_id_ == r.tenant_id_);
  if (l.zone_list_.count() != r.zone_list_.count()) {
    equal = false;
  } else {
    for (int64_t i = 0; i < l.zone_list_.count(); ++i) {
      if (l.zone_list_.at(i) != r.zone_list_.at(i)) {
        equal = false;
        break;
      }
    }
  }
  return equal;
}

int UnitInfoBuilder::write_table()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < units_.count(); ++i) {
    if (OB_FAIL(ut_operator_.update_unit(proxy_, units_.at(i)))) {
      SHARE_LOG(WARN, "write_unit failed", K(ret));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < configs_.count(); ++i) {
    if (OB_FAIL(ut_operator_.update_unit_config(proxy_, configs_.at(i)))) {
      SHARE_LOG(WARN, "write_config failed", K(ret));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < pools_.count(); ++i) {
    if (OB_FAIL(ut_operator_.update_resource_pool(proxy_, pools_.at(i)))) {
      SHARE_LOG(WARN, "write_pool failed", K(ret));
    }
  }
  return ret;
}

}//end namespace share
}//end namespace oceanbase

#endif //OCEANBASE_SHARE_UNIT_INFO_BUILDER_H_
