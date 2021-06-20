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

#ifndef _OB_PARTITION_LEADER_COUNT_BALANCER_H
#define _OB_PARTITION_LEADER_COUNT_BALANCER_H 1

#include "lib/hash/ob_refered_map.h"
#include "rootserver/ob_partition_count_balancer.h"
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

enum class BalanceReplicaType : int64_t {
  BRY_FULL_LEADER = 0,
  BRY_FULL_FOLLOWER,
  BRY_READONLY,
  BRY_DATA,
  BRY_LOGONLY,
  BRY_MAX,
};

enum class BalanceTaskType : int64_t {
  BTY_CURRENT_TASK = 0,
  BTY_ACCUMULATED_TASK,
  BTY_MAX,
};

class RowLeaderBalanceStat {
public:
  RowLeaderBalanceStat(SquareIdMap& map, IUnitProvider& unit_provider, const int64_t row_idx)
      : map_(map), unit_(unit_provider), row_idx_(row_idx), row_unit_()
  {}

public:
  int init();
  int prepare_for_next(const BalanceReplicaType bry_type);
  int64_t get_unit_count() const
  {
    return unit_.count();
  }
  const SquareIdMap::Item* begin()
  {
    return map_.map_ + row_idx_ * map_.col_size_;
  }
  const SquareIdMap::Item* end()
  {
    return map_.map_ + (1 + row_idx_) * map_.col_size_;
  }
  TO_STRING_KV(K_(row_idx), K_(row_unit));

public:
  struct UnitCounter {
    UnitCounter() : unit_id_(common::OB_INVALID_ID), accumulated_count_(0), current_count_(0)
    {}
    UnitCounter(const uint64_t unit_id, const int64_t accumulated_count, const int64_t current_count)
        : unit_id_(unit_id), accumulated_count_(accumulated_count), current_count_(current_count)
    {}
    uint64_t unit_id_;
    int64_t accumulated_count_;
    int64_t current_count_;
    TO_STRING_KV(K_(unit_id), K_(accumulated_count), K_(current_count));
  };
  const common::ObIArray<UnitCounter>& get_unit() const
  {
    return row_unit_;
  }

  int get_task_min_max_unit(
      const BalanceTaskType bty, uint64_t& min_unit_id, double& min_value, uint64_t& max_unit_id, double& max_value);
  int check_gts_monopolized_unit_without_element(UnitCounter& unit_counter, bool& is_gts_monopolized_without_element);
  int get_task_unit_cnt_value(const BalanceTaskType bty, UnitCounter& unit_counter, double& unit_cnt_value);
  int check_move_task_element_waterdown_difference(const BalanceTaskType bty, const uint64_t left_unit_id,
      const double left_cnt_value, const uint64_t right_unit_id, const double right_cnt_value, bool& make_sense);
  int check_move_task_element_retain_difference(
      const BalanceTaskType bry, const uint64_t left_unit_id, const uint64_t right_unit_id, bool& make_sense);
  int get_unit_task_cnt_value_if_move(const BalanceTaskType bty, const uint64_t left_unit_id,
      const uint64_t right_unit_id, double& new_left_cnt_value, double& new_rigth_cnt_value);
  int update_task_count(const uint64_t from_unit_id, const uint64_t to_unit_id, const bool update_accumulated_task);
  int finish_this_curr_task_balance();

protected:
  static const double EPSILON;
  SquareIdMap& map_;
  IUnitProvider& unit_;
  const int64_t row_idx_;
  common::ObSEArray<UnitCounter, 16> row_unit_;
};

class PartitionLeaderCountBalancer : public IdMapBalancer {
public:
  PartitionLeaderCountBalancer(SquareIdMap& map, IUnitProvider& unit_provider, common::ObZone& zone,
      share::schema::ObSchemaGetterGuard& schema_guard, balancer::ITenantStatFinder& stat_finder,
      const HashIndexCollection& hash_index_collection)
      : map_(map),
        unit_provider_(unit_provider),
        zone_(zone),
        raw_count_balancer_(map, unit_provider),
        schema_guard_(schema_guard),
        stat_finder_(stat_finder),
        hash_index_collection_(hash_index_collection)
  {}
  virtual ~PartitionLeaderCountBalancer()
  {}

public:
  virtual int balance() override;

private:
  int do_update_map_item_replica_attributes(SquareIdMap::Item& item);
  int update_map_item_replica_attributes();
  int intra_row_balance();
  int inter_row_balance();
  int one_row_balance(const int64_t row_idx, bool& balance);
  int do_move_curr_task_element_between_unit(const BalanceReplicaType bry_type, const uint64_t max_unit_id,
      const uint64_t min_unit_id, const int64_t row_idx, RowLeaderBalanceStat& leader_balance_stat,
      const bool update_accumulated_task);
  int current_task_one_row_balance(const BalanceReplicaType balance_replica_type, const int64_t row_idx,
      RowLeaderBalanceStat& leader_balance_stat, bool& balance);
  int accumulated_task_one_row_balance(const BalanceReplicaType balance_replica_type, const int64_t row_idx,
      RowLeaderBalanceStat& leader_balance_stat, bool& balance);

private:
  SquareIdMap& map_;
  IUnitProvider& unit_provider_;
  common::ObZone zone_;
  AverageCountBalancer raw_count_balancer_;
  share::schema::ObSchemaGetterGuard& schema_guard_;
  balancer::ITenantStatFinder& stat_finder_;
  const HashIndexCollection& hash_index_collection_;
};
}  // end namespace balancer
}  // end namespace rootserver
}  // end namespace oceanbase

#endif /* _OB_PARTITION_LEADER_COUNT_BALANCER_H */
