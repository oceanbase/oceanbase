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

#define USING_LOG_PREFIX RS_LB

#include "ob_partition_leader_count_balancer.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/ob_allocator.h"
#include "share/config/ob_server_config.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_part_mgr_util.h"
#include "ob_balance_info.h"

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver::balancer;

const double RowLeaderBalanceStat::EPSILON = 0.000000001;

int RowLeaderBalanceStat::init()
{
  int ret = OB_SUCCESS;
  // push unit id to unit_leader_counter_
  for (int64_t i = 0; OB_SUCC(ret) && i < unit_.count(); ++i) {
    const uint64_t unit_id = unit_.get_unit_id(i);
    if (OB_INVALID_ID == unit_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid unit", K(ret), K(i), "cnt", unit_.count());
    } else if (OB_FAIL(row_unit_.push_back(UnitCounter(unit_id, 0, 0)))) {
      LOG_WARN("fail to push back", K(ret));
    }
  }
  return ret;
}

int RowLeaderBalanceStat::prepare_for_next(const BalanceReplicaType bry_type)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < row_unit_.count(); ++i) {
    UnitCounter& unit_counter = row_unit_.at(i);
    unit_counter.current_count_ = 0;
  }
  for (const SquareIdMap::Item* item = begin(); OB_SUCC(ret) && item != end(); ++item) {
    if (OB_UNLIKELY(nullptr == item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("item ptr is null", K(ret));
    } else {
      bool found = false;
      for (int64_t i = 0; !found && i < row_unit_.count(); ++i) {
        UnitCounter& unit_counter = row_unit_.at(i);
        if (unit_counter.unit_id_ == item->dest_unit_id_) {
          switch (bry_type) {
            case BalanceReplicaType::BRY_FULL_LEADER:
              if (item->designated_leader_ && REPLICA_TYPE_FULL == item->replica_type_ &&
                  100 == item->memstore_percent_) {
                ++unit_counter.current_count_;
                found = true;
              }
              break;
            case BalanceReplicaType::BRY_FULL_FOLLOWER:
              if (!item->designated_leader_ && REPLICA_TYPE_FULL == item->replica_type_ &&
                  100 == item->memstore_percent_) {
                ++unit_counter.current_count_;
                found = true;
              }
              break;
            case BalanceReplicaType::BRY_READONLY:
              if (REPLICA_TYPE_READONLY == item->replica_type_) {
                ++unit_counter.current_count_;
                found = true;
              }
              break;
            case BalanceReplicaType::BRY_DATA:
              if (REPLICA_TYPE_FULL == item->replica_type_ && 0 == item->memstore_percent_) {
                ++unit_counter.current_count_;
                found = true;
              }
              break;
            case BalanceReplicaType::BRY_LOGONLY:
              if (REPLICA_TYPE_LOGONLY == item->replica_type_) {
                ++unit_counter.current_count_;
                found = true;
              }
              break;
            default:
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid argument", K(ret), K(bry_type));
              break;
          }
        }
      }
    }
  }
  return ret;
}

int RowLeaderBalanceStat::check_gts_monopolized_unit_without_element(
    UnitCounter& unit_counter, bool& is_gts_monopolized_without_element)
{
  int ret = OB_SUCCESS;
  UnitStat* unit_stat = NULL;
  if (OB_FAIL(unit_.get_unit_by_id(unit_counter.unit_id_, unit_stat))) {
    LOG_WARN("fail to get unit by id", K(ret), K(unit_counter));
  } else if (OB_UNLIKELY(NULL == unit_stat)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit stat ptr is null", K(ret));
  } else {
    const double capacity_ratio = unit_stat->capacity_ratio_;
    if (capacity_ratio > EPSILON) {
      is_gts_monopolized_without_element = false;
    } else if (unit_counter.current_count_ > 0) {
      is_gts_monopolized_without_element = false;
    } else {
      is_gts_monopolized_without_element = true;
    }
  }
  return ret;
}

int RowLeaderBalanceStat::get_task_unit_cnt_value(
    const BalanceTaskType bty, UnitCounter& unit_counter, double& unit_cnt_value)
{
  int ret = OB_SUCCESS;
  UnitStat* unit_stat = NULL;
  bool is_gts_monopolized_without_element = false;
  if (OB_FAIL(check_gts_monopolized_unit_without_element(unit_counter, is_gts_monopolized_without_element))) {
    LOG_WARN("fail to check gts monopolized unit without element", K(ret));
  } else if (is_gts_monopolized_without_element) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("gts monopolized unit without element shou not be here", K(ret), K(unit_counter));
  } else if (OB_FAIL(unit_.get_unit_by_id(unit_counter.unit_id_, unit_stat))) {
    LOG_WARN("fail to get unit by id", K(ret), K(unit_counter));
  } else if (OB_UNLIKELY(NULL == unit_stat)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unit stat ptr is null", K(ret));
  } else {
    const double capacity_ratio = unit_stat->capacity_ratio_;
    // When the capacity ratio is 0, gts monopolizes the unit,
    // and monopolization needs to remove all partitions except __all_dummy
    if (capacity_ratio <= EPSILON) {
      unit_cnt_value = DBL_MAX;
    } else if (BalanceTaskType::BTY_CURRENT_TASK == bty) {
      unit_cnt_value = static_cast<double>(unit_counter.current_count_) / capacity_ratio;
    } else if (BalanceTaskType::BTY_ACCUMULATED_TASK == bty) {
      unit_cnt_value = static_cast<double>(unit_counter.accumulated_count_) / capacity_ratio;
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(bty));
    }
  }
  return ret;
}

int RowLeaderBalanceStat::get_task_min_max_unit(
    const BalanceTaskType bty, uint64_t& min_unit_id, double& min_value, uint64_t& max_unit_id, double& max_value)
{
  int ret = OB_SUCCESS;
  int64_t count = row_unit_.count();
  if (OB_UNLIKELY(count <= 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shall not be here", K(ret), K(count));
  } else {
    // Use a random strategy to choose the starting point of the search to avoid obvious deviations
    int64_t rand_offset = rand() % count;
    min_value = DBL_MAX;
    max_value = -min_value;
    for (int64_t i = rand_offset; OB_SUCC(ret) && i < rand_offset + count; ++i) {
      UnitCounter& u = row_unit_.at(i % count);
      double unit_cnt_value = 0.0;
      bool is_gts_monopolized_without_element = false;
      if (OB_FAIL(check_gts_monopolized_unit_without_element(u, is_gts_monopolized_without_element))) {
        LOG_WARN("fail to check gts monopolize unit without element", K(ret));
      } else if (is_gts_monopolized_without_element) {
        // bypass, ignore gts monpolized without element
      } else if (OB_FAIL(get_task_unit_cnt_value(bty, u, unit_cnt_value))) {
        LOG_WARN("fail to get unit cnt value", K(ret));
      } else {
        if (unit_cnt_value > max_value) {
          max_value = unit_cnt_value;
          max_unit_id = u.unit_id_;
        }
        if (unit_cnt_value < min_value) {
          min_value = unit_cnt_value;
          min_unit_id = u.unit_id_;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (min_value > max_value) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("value unexpected", K(ret), K(min_value), K(max_value), K(min_unit_id), K(max_unit_id));
    }
  }
  return ret;
}

int RowLeaderBalanceStat::get_unit_task_cnt_value_if_move(const BalanceTaskType bty, const uint64_t left_unit_id,
    const uint64_t right_unit_id, double& new_left_cnt_value, double& new_right_cnt_value)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == left_unit_id || OB_INVALID_ID == right_unit_id || left_unit_id == right_unit_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(left_unit_id), K(right_unit_id));
  } else {
    int64_t left_idx = -1;
    int64_t right_idx = -1;
    UnitStat* left_unit_stat = NULL;
    UnitStat* right_unit_stat = NULL;
    for (int64_t i = 0; (left_idx < 0 || right_idx < 0) && i < row_unit_.count(); ++i) {
      UnitCounter& u = row_unit_.at(i);
      if (u.unit_id_ == left_unit_id) {
        left_idx = i;
      } else if (u.unit_id_ == right_unit_id) {
        right_idx = i;
      }
    }
    if (left_idx < 0 || right_idx < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unit counter not found", K(ret));
    } else if (OB_FAIL(unit_.get_unit_by_id(left_unit_id, left_unit_stat))) {
      LOG_WARN("fail to get unit by id", K(ret));
    } else if (OB_FAIL(unit_.get_unit_by_id(right_unit_id, right_unit_stat))) {
      LOG_WARN("fail to get unit by id", K(ret));
    } else if (OB_UNLIKELY(NULL == left_unit_stat || NULL == right_unit_stat)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("left or right unit stat ptr is null",
          KP(left_unit_stat),
          KP(right_unit_stat),
          K(left_unit_id),
          K(right_unit_id),
          K(ret));
    } else {
      UnitCounter left_u = row_unit_.at(left_idx);
      UnitCounter right_u = row_unit_.at(right_idx);
      if (BalanceTaskType::BTY_CURRENT_TASK == bty) {
        --left_u.current_count_;
        ++right_u.current_count_;
      } else if (BalanceTaskType::BTY_ACCUMULATED_TASK == bty) {
        --left_u.accumulated_count_;
        ++right_u.accumulated_count_;
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(bty));
      }
      if (OB_SUCC(ret)) {
        new_left_cnt_value = 0.0;
        new_right_cnt_value = 0.0;
        if (OB_FAIL(get_task_unit_cnt_value(bty, left_u, new_left_cnt_value))) {
          LOG_WARN("fail to get unit cnt value", K(ret), K(left_u));
        } else if (OB_FAIL(get_task_unit_cnt_value(bty, right_u, new_right_cnt_value))) {
          LOG_WARN("fail to get unit cnt value", K(ret), K(right_u));
        }
      }
    }
  }
  return ret;
}

int RowLeaderBalanceStat::check_move_task_element_waterdown_difference(const BalanceTaskType bty,
    const uint64_t left_unit_id, const double left_cnt_value, const uint64_t right_unit_id,
    const double right_cnt_value, bool& make_sense)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == left_unit_id || OB_INVALID_ID == right_unit_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(left_unit_id), K(right_unit_id));
  } else if (left_cnt_value < right_cnt_value) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(
        "value unexpected", K(ret), K(*this), K(left_unit_id), K(right_unit_id), K(left_cnt_value), K(right_cnt_value));
  } else if (left_unit_id == right_unit_id) {
    make_sense = false;
  } else if (DBL_MAX == left_cnt_value) {
    make_sense = true;
  } else {
    double new_left_cnt_value = 0.0;
    double new_right_cnt_value = 0.0;
    if (OB_FAIL(get_unit_task_cnt_value_if_move(
            bty, left_unit_id, right_unit_id, new_left_cnt_value, new_right_cnt_value))) {
      LOG_WARN("fail to get unit cnt value if move", K(ret));
    } else {
      double diff_orig = fabs(left_cnt_value - right_cnt_value);
      double diff_new = fabs(new_left_cnt_value - new_right_cnt_value);
      make_sense = (diff_orig - diff_new > EPSILON);
    }
  }
  return ret;
}

int RowLeaderBalanceStat::check_move_task_element_retain_difference(
    const BalanceTaskType bty, const uint64_t left_unit_id, const uint64_t right_unit_id, bool& make_sense)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == left_unit_id || OB_INVALID_ID == right_unit_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(left_unit_id), K(right_unit_id));
  } else if (left_unit_id == right_unit_id) {
    make_sense = false;
  } else {
    int64_t left_idx = -1;
    int64_t right_idx = -1;
    double left_cnt_value = 0.0;
    double right_cnt_value = 0.0;
    double new_left_cnt_value = 0.0;
    double new_right_cnt_value = 0.0;
    for (int64_t i = 0; i < row_unit_.count() && (left_idx < 0 || right_idx < 0); ++i) {
      UnitCounter& u = row_unit_.at(i);
      if (u.unit_id_ == left_unit_id) {
        left_idx = i;
      } else if (u.unit_id_ == right_unit_id) {
        right_idx = i;
      }
    }
    if (left_idx < 0 || right_idx < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unit counter not found", K(ret));
    } else {
      UnitCounter old_left_u = row_unit_.at(left_idx);
      UnitCounter old_right_u = row_unit_.at(right_idx);
      UnitCounter new_left_u(old_left_u.unit_id_, old_left_u.current_count_ - 1, old_left_u.accumulated_count_ - 1);
      UnitCounter new_right_u(old_right_u.unit_id_, old_right_u.current_count_ + 1, old_right_u.accumulated_count_ + 1);
      if (OB_FAIL(get_task_unit_cnt_value(bty, old_left_u, left_cnt_value))) {
        LOG_WARN("fail to get task unit cnt value", K(ret));
      } else if (OB_FAIL(get_task_unit_cnt_value(bty, old_right_u, right_cnt_value))) {
        LOG_WARN("fail to get task unit cnt value", K(ret));
      } else if (OB_FAIL(get_task_unit_cnt_value(bty, new_left_u, new_left_cnt_value))) {
        LOG_WARN("fail to get task unit cnt value", K(ret));
      } else if (OB_FAIL(get_task_unit_cnt_value(bty, new_right_u, new_right_cnt_value))) {
        LOG_WARN("fail to get task unit cnt value", K(ret));
      } else {
        const double diff_old = fabs(left_cnt_value - right_cnt_value);
        const double diff_new = fabs(new_left_cnt_value - new_right_cnt_value);
        make_sense = (diff_old - diff_new > -EPSILON);
      }
    }
  }
  return ret;
}

int RowLeaderBalanceStat::update_task_count(
    const uint64_t from_unit_id, const uint64_t to_unit_id, const bool update_accumulated_task)
{
  int ret = OB_SUCCESS;
  if (from_unit_id != to_unit_id) {
    for (int64_t i = 0; i < row_unit_.count(); ++i) {
      UnitCounter& u = row_unit_.at(i);
      if (u.unit_id_ == from_unit_id) {
        --u.current_count_;
        if (update_accumulated_task) {
          --u.accumulated_count_;
        }
      } else if (u.unit_id_ == to_unit_id) {
        ++u.current_count_;
        if (update_accumulated_task) {
          ++u.accumulated_count_;
        }
      }
    }
  }
  return ret;
}

int RowLeaderBalanceStat::finish_this_curr_task_balance()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < row_unit_.count(); ++i) {
    UnitCounter& u = row_unit_.at(i);
    u.accumulated_count_ += u.current_count_;
  }
  return ret;
}

int PartitionLeaderCountBalancer::balance()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(raw_count_balancer_.balance())) {
    LOG_WARN("fail to do partition count balancer", K(ret));
  } else if (OB_FAIL(update_map_item_replica_attributes())) {
    LOG_WARN("fail to update map item attributes", K(ret));
  } else if (OB_FAIL(intra_row_balance())) {
    LOG_WARN("fail to do intra row balance", K(ret));
  } else if (OB_FAIL(inter_row_balance())) {
    LOG_WARN("fail to do inter row balance", K(ret));
  }
  return ret;
}

int PartitionLeaderCountBalancer::do_update_map_item_replica_attributes(SquareIdMap::Item& item)
{
  int ret = OB_SUCCESS;
  const common::ObPartitionKey& pkey = item.get_partition_key();
  const uint64_t schema_id = pkey.get_table_id();
  uint64_t tablegroup_id = OB_INVALID_ID;
  common::ObSEArray<share::ObZoneReplicaAttrSet, 7> actual_zone_locality;
  common::ObSEArray<share::ObZoneReplicaAttrSet, 7> designated_zone_locality;
  bool compensate_readonly_all_server = false;
  if (!is_tablegroup_id(schema_id)) {
    const ObTableSchema* table_schema = nullptr;
    if (OB_FAIL(schema_guard_.get_table_schema(schema_id, table_schema))) {
      LOG_WARN("fail to get table schema", K(ret), "table_id", schema_id);
    } else if (OB_UNLIKELY(nullptr == table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid table schema", K(ret), "table_id", schema_id);
    } else if (OB_FAIL(table_schema->get_zone_replica_attr_array_inherit(schema_guard_, actual_zone_locality))) {
      LOG_WARN("fail to get zone replica attr array inherit", K(ret));
    } else if (OB_FAIL(table_schema->check_is_duplicated(schema_guard_, compensate_readonly_all_server))) {
      LOG_WARN("fail to check duplicate scope cluter", K(ret), K(schema_id));
    } else {
      tablegroup_id = table_schema->get_tablegroup_id();
    }
  } else {
    compensate_readonly_all_server = false;
    const ObTablegroupSchema* tg_schema = nullptr;
    if (OB_FAIL(schema_guard_.get_tablegroup_schema(schema_id, tg_schema))) {
      LOG_WARN("fail to get tablegroup schema", K(ret), "tg_id", schema_id);
    } else if (OB_UNLIKELY(nullptr == tg_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid tg schema", K(ret), "tg_id", schema_id);
    } else if (OB_FAIL(tg_schema->get_zone_replica_attr_array_inherit(schema_guard_, actual_zone_locality))) {
      LOG_WARN("fail to get zone locality", K(ret), "tg_id", schema_id);
    } else {
      tablegroup_id = tg_schema->get_tablegroup_id();
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObLocalityUtil::generate_designated_zone_locality(compensate_readonly_all_server,
            item.get_tablegroup_id(),
            item.get_all_pg_idx(),
            hash_index_collection_,
            stat_finder_,
            actual_zone_locality,
            designated_zone_locality))) {
      LOG_WARN("fail to generate designated zone locality", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < designated_zone_locality.count(); ++i) {
      const share::ObZoneReplicaAttrSet& this_locality = designated_zone_locality.at(i);
      if (!has_exist_in_array(this_locality.zone_set_, zone_)) {
        // by pass
      } else if (this_locality.get_full_replica_num() > 0) {
        const ObIArray<ReplicaAttr>& attr_array = this_locality.replica_attr_set_.get_full_replica_attr_array();
        if (attr_array.count() <= 0 || attr_array.count() > 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("attr array count unexpected", K(ret), K_(zone), K(item));
        } else {
          item.replica_type_ = REPLICA_TYPE_FULL;
          item.memstore_percent_ = attr_array.at(0).memstore_percent_;
        }
      } else if (this_locality.get_logonly_replica_num() > 0) {
        const ObIArray<ReplicaAttr>& attr_array = this_locality.replica_attr_set_.get_logonly_replica_attr_array();
        if (attr_array.count() <= 0 || attr_array.count() > 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("attr array count unexpected", K(ret), K_(zone), K(item));
        } else {
          item.replica_type_ = REPLICA_TYPE_LOGONLY;
          item.memstore_percent_ = attr_array.at(0).memstore_percent_;
        }
      } else if (this_locality.get_readonly_replica_num() > 0) {
        const ObIArray<ReplicaAttr>& attr_array = this_locality.replica_attr_set_.get_readonly_replica_attr_array();
        if (attr_array.count() <= 0 || attr_array.count() > 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("attr array count unexpected", K(ret), K_(zone), K(item));
        } else {
          item.replica_type_ = REPLICA_TYPE_READONLY;
          item.memstore_percent_ = attr_array.at(0).memstore_percent_;
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("replica attr unexpected", K(ret), K(this_locality));
      }
    }
  }
  return ret;
}

int PartitionLeaderCountBalancer::update_map_item_replica_attributes()
{
  int ret = OB_SUCCESS;
  SquareIdMap::iterator null_iter(nullptr);
  for (SquareIdMap::iterator iter = map_.begin(); OB_SUCC(ret) && iter != map_.end(); ++iter) {
    if (null_iter == iter) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null iter", K(ret));
    } else if (OB_FAIL(do_update_map_item_replica_attributes(*iter))) {
      LOG_WARN("fail to do update map item replica attributes", K(ret));
    }
  }
  return ret;
}

int PartitionLeaderCountBalancer::one_row_balance(const int64_t row_idx, bool& balanced)
{
  int ret = OB_SUCCESS;
  RowLeaderBalanceStat row_leader_stat(map_, unit_provider_, row_idx);
  if (OB_FAIL(row_leader_stat.init())) {
    LOG_WARN("fail to init row leader stat", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < static_cast<int64_t>(BalanceReplicaType::BRY_MAX); ++i) {
      // Before starting balance for each type of task, set balanced to false
      balanced = false;
      if (row_leader_stat.get_unit_count() <= 1) {
        balanced = true;  // ignore since we only have one unit
      } else if (OB_FAIL(row_leader_stat.prepare_for_next(static_cast<BalanceReplicaType>(i)))) {
        LOG_WARN("fail ot init row leader stat", K(ret));
      } else if (OB_FAIL(current_task_one_row_balance(
                     static_cast<BalanceReplicaType>(i), row_idx, row_leader_stat, balanced))) {
        LOG_WARN("fail to do current task one row balance", K(ret));
      } else if (!balanced) {
        LOG_WARN("still unbalance after many current task balance action");
        break;
      } else if (OB_FAIL(accumulated_task_one_row_balance(
                     static_cast<BalanceReplicaType>(i), row_idx, row_leader_stat, balanced))) {
        LOG_WARN("fail to do accumulated task one row balance", K(ret));
      } else if (!balanced) {
        LOG_WARN("still unbalance after many accmulated task balance action");
        break;
      } else {
      }  // good,
    }
  }
  return ret;
}

int PartitionLeaderCountBalancer::do_move_curr_task_element_between_unit(const BalanceReplicaType bry_type,
    const uint64_t max_unit_id, const uint64_t min_unit_id, const int64_t row_idx,
    RowLeaderBalanceStat& row_balance_stat, const bool update_accumulated_task)
{
  int ret = OB_SUCCESS;
  const int64_t random_offset = rand();
  const int64_t start_idx = random_offset;
  const int64_t end_idx = start_idx + map_.get_col_size();
  int64_t max_item_idx = -1;
  // The highest priority is to find min_unit_id as the initial position of max_item,
  // and max_unit_id as the initial position of min_item for exchange
  // Such an operation is actually zero overhead when the actual partition is migrated
  // (the partition does not actually need to be migrated)
  for (int64_t cur_idx = start_idx; OB_SUCC(ret) && cur_idx < end_idx && max_item_idx < 0; ++cur_idx) {
    const int64_t base_offset = row_idx * map_.get_col_size();
    const int64_t map_idx = base_offset + cur_idx % map_.get_col_size();
    SquareIdMap::Item& item = map_.map_[map_idx];
    if (item.dest_unit_id_ == max_unit_id  // item on the current largest unit
        && item.unit_id_ == min_unit_id    // item on the current smallest unit
        && max_item_idx < 0) {
      switch (bry_type) {
        case BalanceReplicaType::BRY_FULL_LEADER:
          if (item.designated_leader_ && REPLICA_TYPE_FULL == item.replica_type_ && 100 == item.memstore_percent_) {
            max_item_idx = map_idx;
          }
          break;
        case BalanceReplicaType::BRY_FULL_FOLLOWER:
          if (!item.designated_leader_ && REPLICA_TYPE_FULL == item.replica_type_ && 100 == item.memstore_percent_) {
            max_item_idx = map_idx;
          }
          break;
        case BalanceReplicaType::BRY_READONLY:
          if (REPLICA_TYPE_READONLY == item.replica_type_) {
            max_item_idx = map_idx;
          }
          break;
        case BalanceReplicaType::BRY_DATA:
          if (REPLICA_TYPE_FULL == item.replica_type_ && 0 == item.memstore_percent_) {
            max_item_idx = map_idx;
          }
          break;
        case BalanceReplicaType::BRY_LOGONLY:
          if (REPLICA_TYPE_LOGONLY == item.replica_type_) {
            max_item_idx = map_idx;
          }
          break;
        default:
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(ret), K(bry_type));
          break;
      }
    }
  }
  // The second priority,
  // as long as max_unit_id is the current position of max_item and min_unit_id is the current position of min_item.
  for (int64_t cur_idx = start_idx; OB_SUCC(ret) && cur_idx < end_idx && max_item_idx < 0; ++cur_idx) {
    const int64_t base_offset = row_idx * map_.get_col_size();
    const int64_t map_idx = base_offset + cur_idx % map_.get_col_size();
    SquareIdMap::Item& item = map_.map_[map_idx];
    if (item.dest_unit_id_ == max_unit_id  // item on the current largest unit
        && max_item_idx < 0) {
      switch (bry_type) {
        case BalanceReplicaType::BRY_FULL_LEADER:
          if (item.designated_leader_ && REPLICA_TYPE_FULL == item.replica_type_ && 100 == item.memstore_percent_) {
            max_item_idx = map_idx;
          }
          break;
        case BalanceReplicaType::BRY_FULL_FOLLOWER:
          if (!item.designated_leader_ && REPLICA_TYPE_FULL == item.replica_type_ && 100 == item.memstore_percent_) {
            max_item_idx = map_idx;
          }
          break;
        case BalanceReplicaType::BRY_READONLY:
          if (REPLICA_TYPE_READONLY == item.replica_type_) {
            max_item_idx = map_idx;
          }
          break;
        case BalanceReplicaType::BRY_DATA:
          if (REPLICA_TYPE_FULL == item.replica_type_ && 0 == item.memstore_percent_) {
            max_item_idx = map_idx;
          }
          break;
        case BalanceReplicaType::BRY_LOGONLY:
          if (REPLICA_TYPE_LOGONLY == item.replica_type_) {
            max_item_idx = map_idx;
          }
          break;
        default:
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid argument", K(ret), K(bry_type));
          break;
      }
    }
  }
  if (max_item_idx < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("item not found", K(ret), K(max_item_idx));
  } else {
    map_.map_[max_item_idx].dest_unit_id_ = min_unit_id;
    if (OB_FAIL(row_balance_stat.update_task_count(max_unit_id, min_unit_id, update_accumulated_task))) {
      LOG_WARN("fail to update leader row stat", K(ret), K(max_unit_id), K(min_unit_id));
    }
  }
  return ret;
}

int PartitionLeaderCountBalancer::current_task_one_row_balance(const BalanceReplicaType balance_replica_type,
    const int64_t row_idx, RowLeaderBalanceStat& row_balance_stat, bool& balanced)
{
  int ret = OB_SUCCESS;
  const int64_t max_iteration = map_.get_col_size();
  int64_t iteration = 0;
  balanced = false;
  bool this_balanced = false;
  while (OB_SUCC(ret) && iteration++ <= max_iteration && !this_balanced) {
    uint64_t min_unit_id = OB_INVALID_ID;
    uint64_t max_unit_id = OB_INVALID_ID;
    double min_cnt_value = 0.0;
    double max_cnt_value = 0.0;
    bool make_sense = false;
    if (OB_FAIL(row_balance_stat.get_task_min_max_unit(
            BalanceTaskType::BTY_CURRENT_TASK, min_unit_id, min_cnt_value, max_unit_id, max_cnt_value))) {
      LOG_WARN("fail to get min max unit", K(ret));
    } else if (OB_FAIL(row_balance_stat.check_move_task_element_waterdown_difference(BalanceTaskType::BTY_CURRENT_TASK,
                   max_unit_id,
                   max_cnt_value,
                   min_unit_id,
                   min_cnt_value,
                   make_sense))) {
      LOG_WARN("fail to check move element make sense", K(ret));
    } else if (!make_sense) {
      this_balanced = true;
    } else {
      if (OB_FAIL(do_move_curr_task_element_between_unit(balance_replica_type,
              max_unit_id,
              min_unit_id,
              row_idx,
              row_balance_stat,
              false /*do not update accumulate task*/))) {
        LOG_WARN("fail to do exchange role between unit", K(ret));
      }
    }
  }
  if (iteration > max_iteration && !this_balanced) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("too many iterations", K(ret), K(iteration), K(max_iteration));
  } else if (OB_FAIL(row_balance_stat.finish_this_curr_task_balance())) {
    LOG_WARN("fail to finish this curr task balance", K(ret));
  } else {
    balanced = true;
  }
  return ret;
}

int PartitionLeaderCountBalancer::accumulated_task_one_row_balance(const BalanceReplicaType balance_replica_type,
    const int64_t row_idx, RowLeaderBalanceStat& row_balance_stat, bool& balanced)
{
  int ret = OB_SUCCESS;
  const int64_t max_iteration = map_.get_col_size();
  int64_t iteration = 0;
  balanced = false;
  bool this_balanced = false;
  while (OB_SUCC(ret) && iteration++ <= max_iteration && !this_balanced) {
    uint64_t min_unit_id = OB_INVALID_ID;
    uint64_t max_unit_id = OB_INVALID_ID;
    double min_cnt_value = 0.0;
    double max_cnt_value = 0.0;
    bool make_sense = false;
    if (OB_FAIL(row_balance_stat.get_task_min_max_unit(
            BalanceTaskType::BTY_ACCUMULATED_TASK, min_unit_id, min_cnt_value, max_unit_id, max_cnt_value))) {
      LOG_WARN("fail to get min max unit", K(ret));
    } else if (OB_FAIL(
                   row_balance_stat.check_move_task_element_waterdown_difference(BalanceTaskType::BTY_ACCUMULATED_TASK,
                       max_unit_id,
                       max_cnt_value,
                       min_unit_id,
                       min_cnt_value,
                       make_sense))) {
      LOG_WARN("fail to check move element make sense", K(ret));
    } else if (!make_sense) {
      this_balanced = true;
    } else if (OB_FAIL(row_balance_stat.check_move_task_element_retain_difference(
                   BalanceTaskType::BTY_CURRENT_TASK, max_unit_id, min_unit_id, make_sense))) {
      LOG_WARN("fail to check task move element retain difference", K(ret));
    } else if (!make_sense) {
      this_balanced = true;
    } else {
      if (OB_FAIL(do_move_curr_task_element_between_unit(balance_replica_type,
              max_unit_id,
              min_unit_id,
              row_idx,
              row_balance_stat,
              true /*update accumualted task*/))) {
        LOG_WARN("fail to do move curr task element between unit", K(ret));
      }
    }
  }
  if (iteration > max_iteration && !this_balanced) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("too many iterations", K(ret), K(iteration), K(max_iteration));
  } else {
    balanced = true;
  }
  return ret;
}

int PartitionLeaderCountBalancer::intra_row_balance()
{
  int ret = OB_SUCCESS;
  const int64_t row_size = map_.get_row_size();
  if (unit_provider_.count() <= 1) {
    // only one unit, no need to do balance
  } else {
    for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < row_size; ++row_idx) {
      bool balance = false;
      if (OB_FAIL(one_row_balance(row_idx, balance))) {
        LOG_WARN("fail to do one row balance", K(ret));
      } else if (!balance) {
        LOG_WARN("row still not balance", K(row_idx), K_(map));
      }
    }
  }
  return ret;
}

int PartitionLeaderCountBalancer::inter_row_balance()
{
  int ret = OB_SUCCESS;
  // At present, neither cout balance nor disk balance is balanced between rows,
  // and success is returned directly here.
  return ret;
}
