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

#ifndef _OB_PARTITION_DISK_BALANCER_H
#define _OB_PARTITION_DISK_BALANCER_H 1

#include "lib/hash/ob_refered_map.h"
#include "rootserver/ob_partition_leader_count_balancer.h"
#include "ob_balance_group_data.h"

namespace oceanbase {

namespace share {
namespace schema {
class ObMultiVersionSchemaService;
class ObTableSchema;
class ObSchemaGetterGuard;
}  // namespace schema
}  // namespace share

namespace common {
class ObIAllocator;
}

namespace rootserver {
namespace balancer {

class RowDiskBalanceStat {
public:
  struct RowItemDiskStat {
    RowItemDiskStat(int64_t data_size) : key_(0), data_size_(data_size)
    {}
    RowItemDiskStat() : key_(0), data_size_(0)
    {}
    int64_t key_;  // for ObReferedMap
    int64_t data_size_;
    const int64_t& get_key() const
    {
      return key_;
    }
    void set_key(int64_t key)
    {
      key_ = key;
    }
    TO_STRING_KV(K_(key), K_(data_size));
  };
  typedef RowItemDiskStat Stat;
  typedef common::hash::ObReferedMap<int64_t, Stat> UnitLoadMap;

public:
  RowDiskBalanceStat(ITenantStatFinder& stat_finder, common::ObZone& zone, SquareIdMap& map, int64_t row_idx)
      : item_disk_info_(),
        total_load_(0),
        unit_load_map_(),
        stat_finder_(stat_finder),
        zone_(zone),
        map_(map),
        row_idx_(row_idx)
  {}
  ~RowDiskBalanceStat()
  {}
  int init();
  int64_t size() const
  {
    return item_disk_info_.count();
  }
  int get_item_load(int64_t idx, int64_t& load) const;
  int get_unit_load(int64_t unit_id, int64_t& load) const;
  int get_unit_load(int64_t unit_id, Stat*& unit_load);
  int get_total_load(int64_t& load) const;
  TO_STRING_KV(K_(item_disk_info), K_(zone), K_(row_idx), K_(map));

private:
  common::ObSEArray<Stat, 16> item_disk_info_;
  Stat total_load_;
  UnitLoadMap unit_load_map_;
  ITenantStatFinder& stat_finder_;
  common::ObZone& zone_;
  SquareIdMap& map_;
  int64_t row_idx_;
};

class UnitDiskBalanceStat {
public:
  struct UnitItemDiskStat {
    UnitItemDiskStat(UnitStat* unit_stat) : key_(0), unit_stat_(unit_stat)
    {}
    UnitItemDiskStat() : unit_stat_(NULL)
    {}
    int64_t key_;  // for ObReferedMap
    UnitStat* unit_stat_;
    const int64_t& get_key() const
    {
      return key_;
    }
    void set_key(int64_t key)
    {
      key_ = key;
    }
    TO_STRING_KV(K_(key), K_(unit_stat));
  };
  typedef UnitItemDiskStat Stat;
  typedef common::hash::ObReferedMap<int64_t, Stat> UnitLoadMap;

public:
  UnitDiskBalanceStat(IUnitProvider& unit_provider)
      : inited_(false), unit_provider_(unit_provider), unit_load_map_(), avg_load_(0)
  {}
  ~UnitDiskBalanceStat()
  {}
  int init();
  int get_unit_load(uint64_t unit_id, UnitStat*& unit_stat);
  int get_avg_load(double& avg);
  int get_max_min_load_unit(UnitStat*& max_u, UnitStat*& min_u);
  void debug_dump();

private:
  bool inited_;
  IUnitProvider& unit_provider_;
  UnitLoadMap unit_load_map_;
  double avg_load_;
};

// Dynamically adjust disk balance
class DynamicAverageDiskBalancer : public IdMapBalancer {
public:
  DynamicAverageDiskBalancer(SquareIdMap& map, ITenantStatFinder& stat_finder, IUnitProvider& unit_provider,
      common::ObZone& zone, share::schema::ObSchemaGetterGuard& schema_guard,
      const HashIndexCollection& hash_index_collection)
      : map_(map),
        stat_finder_(stat_finder),
        unit_stat_(unit_provider),
        zone_(zone),
        count_balancer_(map, unit_provider, zone, schema_guard, stat_finder, hash_index_collection)
  {}
  virtual ~DynamicAverageDiskBalancer()
  {}

  virtual int balance() override;

private:
  int one_row_balance(
      UnitDiskBalanceStat& unit_stat, UnitStat& max_u, UnitStat& min_u, int64_t row_idx, int64_t& task_cnt);
  int test_and_exchange(UnitDiskBalanceStat& unit_stat, UnitStat& max_u, UnitStat& min_u, RowDiskBalanceStat& row_stat,
      int64_t idx_a, SquareIdMap::Item& a, int64_t idx_b, SquareIdMap::Item& b, bool& exchangable);
  void exchange(SquareIdMap::Item& a, SquareIdMap::Item& b);
  int update_unit_stat(UnitDiskBalanceStat& unit_stat);

private:
  SquareIdMap& map_;
  ITenantStatFinder& stat_finder_;
  UnitDiskBalanceStat unit_stat_;
  common::ObZone zone_;
  PartitionLeaderCountBalancer count_balancer_;
};

}  // end namespace balancer
}  // end namespace rootserver
}  // end namespace oceanbase

#endif /* _OB_PARTITION_DISK_BALANCER_H */
