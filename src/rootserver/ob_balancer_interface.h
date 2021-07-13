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

#ifndef _OB_BALANCER_INTERFACE_H
#define _OB_BALANCER_INTERFACE_H 1

#include "ob_root_utils.h"
#include "lib/container/ob_iarray.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase {

namespace share {
namespace schema {
class ObTableSchema;
class ObSchemaGetterGuard;
}  // namespace schema
}  // namespace share

namespace common {
class ObIAllocator;
}

namespace rootserver {
class UnitStat;

namespace balancer {
class BalancerStringUtil {
public:
  static bool has_prefix(const common::ObString& str, const common::ObString& prefix);
  static int substract_numeric_suffix(const common::ObString& str, common::ObString& prefix, int64_t& digit_len);
  static int substract_extra_suffix(const common::ObString& str, common::ObString& prefix, common::ObString& suffix);
};

class ITenantStatFinder {
public:
  virtual uint64_t get_tenant_id() const = 0;

  virtual int get_all_pg_idx(const common::ObPartitionKey& pkey, const int64_t all_tg_idx, int64_t& all_pg_idx) = 0;
  // Find all_tg_ by tablegroup_id and table_id
  // Return: the index of the corresponding item in all_tg_
  virtual int get_all_tg_idx(uint64_t tablegroup_id, uint64_t table_id, int64_t& all_tg_idx) = 0;
  // Get the id of the unit where the primary table is located in the partition group described by <all_tg_idx,
  // part_idx>
  virtual int get_primary_partition_unit(
      const common::ObZone& zone, const int64_t all_tg_idx, const int64_t part_idx, uint64_t& unit_id) = 0;

  virtual int get_partition_entity_ids_by_tg_idx(const int64_t tablegroup_idx, common::ObIArray<uint64_t>& tids) = 0;

  // Get the sum of data_size of all partitions under pg
  virtual int get_partition_group_data_size(
      const common::ObZone& zone, const int64_t all_tg_idx, const int64_t part_idx, int64_t& data_size) = 0;

  virtual int get_gts_switch(bool& on) = 0;

  virtual int get_primary_partition_key(const int64_t all_pg_idx, common::ObPartitionKey& pkey) = 0;
};

class TenantSchemaGetter : public ITenantStatFinder {
public:
  TenantSchemaGetter(const int64_t tenant_id) : ITenantStatFinder(), tenant_id_(tenant_id)
  {}
  virtual ~TenantSchemaGetter()
  {}

public:
  virtual uint64_t get_tenant_id() const override
  {
    return tenant_id_;
  };
  virtual int get_all_pg_idx(
      const common::ObPartitionKey& pkey, const int64_t all_tg_idx, int64_t& all_pg_idx) override;
  virtual int get_all_tg_idx(uint64_t tablegroup_id, uint64_t table_id, int64_t& all_tg_idx) override;
  virtual int get_primary_partition_unit(
      const common::ObZone& zone, const int64_t all_tg_idx, const int64_t part_idx, uint64_t& unit_id) override;
  virtual int get_partition_entity_ids_by_tg_idx(
      const int64_t tablegroup_idx, common::ObIArray<uint64_t>& tids) override;
  virtual int get_partition_group_data_size(
      const common::ObZone& zone, const int64_t all_tg_idx, const int64_t part_idx, int64_t& data_size) override;
  virtual int get_gts_switch(bool& on) override;
  virtual int get_primary_partition_key(const int64_t all_pg_idx, common::ObPartitionKey& pkey) override;

private:
  uint64_t tenant_id_;
};

class StatFinderUtil {
public:
  // Find the schema of all physical tables from all_tg
  // NOTE: 1. May return empty set
  //       2. Filtered tables without partitions, such as virtual tables, views, etc.
  static int get_partition_entity_schemas_by_tg_idx(ITenantStatFinder& stat_finder,
      share::schema::ObSchemaGetterGuard& schema_guard, uint64_t tenant_id, int64_t tablegroup_idx,
      common::ObIArray<const share::schema::ObPartitionSchema*>& partition_entity_schemas);

  // Obtaining the table list from StatFinder is a snapshot, not because of map_balancer
  // The result of each call in the running process is different
  static int get_need_balance_table_schemas_in_tenant(share::schema::ObSchemaGetterGuard& schema_guard,
      uint64_t tenant_id, common::ObIArray<const share::schema::ObTableSchema*>& tables);
};

class IUnitProvider {
public:
  // Take the partition position of tablegroup **primary table** from all_tg_ according to all_tg_idx
  virtual int find_unit(int64_t all_tg_idx, int64_t part_idx, uint64_t& unit_id) const = 0;
  // Get the number of tenant units in each zone
  virtual int64_t count() const = 0;
  // Get the id of the unit_idxth unit
  // If unit does not exist, return common::OB_INVALID_ID
  virtual uint64_t get_unit_id(int64_t unit_idx) const = 0;

  virtual int get_units(common::ObIArray<UnitStat*>& unit_stat) const = 0;

  virtual int get_unit_by_id(const uint64_t unit_id, UnitStat*& unit_stat) const = 0;

  virtual int get_avg_load(double& avg_load) const = 0;
};

}  // end namespace balancer
}  // end namespace rootserver
}  // end namespace oceanbase

#endif /* _OB_BALANCER_INTERFACE_H */
