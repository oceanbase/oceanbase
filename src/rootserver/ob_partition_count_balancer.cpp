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

#include "ob_partition_count_balancer.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/ob_allocator.h"
#include "ob_balance_info.h"

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::rootserver::balancer;

const double BalanceStat::EPSILON = 0.0000000001;

int BalanceStat::init()
{
  int ret = OB_SUCCESS;
  // 1. init
  for (int64_t i = 0; OB_SUCC(ret) && i < unit_.count(); ++i) {
    uint64_t unit_id = unit_.get_unit_id(i);
    if (OB_INVALID_ID == unit_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid unit", K(i), "cnt", unit_.count(), K(ret));
    } else if (OB_FAIL(row_unit_.push_back(UnitCounter(unit_id, 0)))) {
      LOG_WARN("fail init row_unit", K(i), "count", unit_.count(), K(ret));
    }
  }
  // 2. stat
  for (const SquareIdMap::Item* item = begin(); OB_SUCC(ret) && item < end(); ++item) {
    bool found = false;
    for (int64_t i = 0; i < row_unit_.count(); ++i) {
      UnitCounter& u = row_unit_.at(i);
      if (u.unit_id_ == item->dest_unit_id_) {
        u.count_++;
        found = true;
        break;
      }
    }
    if (!found) {
      int64_t unit_id = static_cast<int64_t>(item->dest_unit_id_);  // for print
      LOG_DEBUG("item's unit not found, may be unit_id = -1 or unit deleted", K(unit_id), K(*item), K_(row_unit));
      // For a replica that has no unit id assigned, it will come to this code
      // The expected behavior is: load balancing ignores such replica.
      // As it does not exist, only balance the remaining replicas with unit id
    }
  }
  return ret;
}

int BalanceStat::check_gts_monopolized_unit_without_element(
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
    if (capacity_ratio > 0) {
      is_gts_monopolized_without_element = false;
    } else if (unit_counter.count_ > 0) {
      is_gts_monopolized_without_element = false;
    } else {
      is_gts_monopolized_without_element = true;
    }
  }
  return ret;
}

int BalanceStat::get_unit_cnt_value(UnitCounter& unit_counter, double& unit_cnt_value)
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
    if (capacity_ratio <= 0) {
      unit_cnt_value = DBL_MAX;
    } else {
      unit_cnt_value = static_cast<double>(unit_counter.count_) / capacity_ratio;
    }
  }
  return ret;
}

int BalanceStat::get_unit_cnt_value_if_move(
    const uint64_t left_unit_id, const uint64_t right_unit_id, double& new_left_cnt_value, double& new_right_cnt_value)
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
      --left_u.count_;
      ++right_u.count_;
      new_left_cnt_value = 0.0;
      new_right_cnt_value = 0.0;
      if (OB_FAIL(get_unit_cnt_value(left_u, new_left_cnt_value))) {
        LOG_WARN("fail to get unit cnt value", K(ret), K(left_u));
      } else if (OB_FAIL(get_unit_cnt_value(right_u, new_right_cnt_value))) {
        LOG_WARN("fail to get unit cnt value", K(ret), K(right_u));
      }
    }
  }
  return ret;
}

// move from left to right
int BalanceStat::check_move_element_waterdown_difference(const uint64_t left_unit_id, const double left_cnt_value,
    const uint64_t right_unit_id, const double right_cnt_value, bool& make_sense)
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
    if (OB_FAIL(get_unit_cnt_value_if_move(left_unit_id, right_unit_id, new_left_cnt_value, new_right_cnt_value))) {
      LOG_WARN("fail to get unit cnt value if move", K(ret));
    } else {
      double diff_orig = fabs(left_cnt_value - right_cnt_value);
      double diff_new = fabs(new_left_cnt_value - new_right_cnt_value);
      make_sense = (diff_orig - diff_new > EPSILON);
    }
  }
  return ret;
}

int BalanceStat::check_move_element_retain_difference(const uint64_t left_unit_id, const double left_cnt_value,
    const uint64_t right_unit_id, const double right_cnt_value, bool& make_sense)
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
    if (OB_FAIL(get_unit_cnt_value_if_move(left_unit_id, right_unit_id, new_left_cnt_value, new_right_cnt_value))) {
      LOG_WARN("fail to get unit cnt if move", K(ret));
    } else {
      double diff_orig = fabs(left_cnt_value - right_cnt_value);
      double diff_new = fabs(new_left_cnt_value - new_right_cnt_value);
      make_sense = (diff_orig - diff_new > -EPSILON);
    }
  }
  return ret;
}

int BalanceStat::get_min_max_unit(uint64_t& min_unit_id, double& min_value, uint64_t& max_unit_id, double& max_value)
{
  int ret = OB_SUCCESS;
  int64_t count = row_unit_.count();
  if (count <= 1) {
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
        // bypass, ignore gts monpolized without elemen
      } else if (OB_FAIL(get_unit_cnt_value(u, unit_cnt_value))) {
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

// note: When min_cnt = max_cnt, only fill max_unit_ids
int BalanceStat::get_min_max_unit(
    ObIArray<uint64_t>& min_unit_ids, double& min_value, ObIArray<uint64_t>& max_unit_ids, double& max_value)
{
  int ret = OB_SUCCESS;
  int64_t count = row_unit_.count();
  if (count <= 1) {
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
        LOG_WARN("fail to check gts monopolized unit without element", K(ret));
      } else if (is_gts_monopolized_without_element) {
        // by pass
      } else if (OB_FAIL(get_unit_cnt_value(u, unit_cnt_value))) {
        LOG_WARN("fail to get unit cnt value", K(ret));
      } else {
        if (unit_cnt_value > max_value) {
          max_value = unit_cnt_value;
        }
        if (unit_cnt_value < min_value) {
          min_value = unit_cnt_value;
        }
      }
    }
    for (int64_t i = rand_offset; OB_SUCC(ret) && i < rand_offset + count; ++i) {
      UnitCounter& u = row_unit_.at(i % count);
      double unit_cnt_value = 0.0;
      bool is_gts_monopolized_without_element = false;
      if (OB_FAIL(check_gts_monopolized_unit_without_element(u, is_gts_monopolized_without_element))) {
        LOG_WARN("fail to check gts monopolized unit without element", K(ret));
      } else if (is_gts_monopolized_without_element) {
        // by pass
      } else if (OB_FAIL(get_unit_cnt_value(u, unit_cnt_value))) {
        LOG_WARN("fail to get unit cnt value", K(ret));
      } else if (fabs(max_value - unit_cnt_value) < EPSILON) {
        if (OB_FAIL(max_unit_ids.push_back(u.unit_id_))) {
          LOG_WARN("fail push back id", K(u), K(ret));
        }
      } else if (fabs(unit_cnt_value - min_value) < EPSILON) {
        if (OB_FAIL(min_unit_ids.push_back(u.unit_id_))) {
          LOG_WARN("fail push back id", K(u), K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (min_value > max_value) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected value", K(ret), K(min_value), K(max_value), K(min_unit_ids), K(max_unit_ids));
    }
  }
  return ret;
}

int BalanceStat::update(const uint64_t from_unit_id, const uint64_t to_unit_id)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(from_unit_id != to_unit_id)) {
    for (int64_t i = 0; i < row_unit_.count(); ++i) {
      UnitCounter& u = row_unit_.at(i);
      if (u.unit_id_ == from_unit_id) {
        u.count_--;
      } else if (u.unit_id_ == to_unit_id) {
        u.count_++;
      }
    }
  }
  return ret;
}

int AverageCountBalancer::balance()
{
  // Equalization step
  // 1. Assign initial values to unit_id and dest_unit_id in SquareIdMap
  // 2. Perform line balance according to dest_unit_id value, global balance
  // 3. SquareIdMap is updated after the balance is completed,
  // and the load balancing execution logic is responsible for the migration according to the new SquareIdMap
  int ret = OB_SUCCESS;
  uint64_t row_size = map_.get_row_size();

  // step 1. fill unit to map
  if (OB_FAIL(map_.assign_unit(unit_))) {
    LOG_WARN("fail assign_unit", K_(map), K(ret));
  } else if (unit_.count() > 1) {
    // step 2. row balance
    if (OB_SUCC(ret)) {
      for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < row_size; ++row_idx) {
        bool balanced = false;
        if (OB_FAIL(one_row_balance(row_idx, balanced))) {
          LOG_WARN("fail balance one row", K(row_idx), K(map_), K(ret));
        } else if (!balanced) {
          LOG_WARN("row still not balanced", K(row_idx), K_(map));
        }
      }
    }
    // step 3. cross row balance
    // UPDATE: Disable balance between rows.
    bool enable_overall_balance = false;
    if (OB_SUCC(ret) && enable_overall_balance) {
      bool balanced = false;
      if (OB_FAIL(overall_balance(balanced))) {
        LOG_WARN("fail do overall balance", K_(map), K(balanced), K(ret));
      } else if (!balanced) {
        LOG_WARN("map still not balanced", K_(map));
      }
    }
  } else {
  }  // one unit, no need to do balance
  return ret;
}

int AverageCountBalancer::one_row_balance(int64_t row_idx, bool& balanced)
{
  int ret = OB_SUCCESS;
  int64_t max_iteration = map_.get_col_size();
  int64_t iteration = 0;

  balanced = false;
  RowBalanceStat row_stat(map_, unit_, row_idx);
  if (OB_FAIL(row_stat.init())) {
    LOG_WARN("fail init row stat", K(ret));
  }
  while (OB_SUCC(ret) && iteration++ <= max_iteration && !balanced && row_stat.get_unit_count() > 1) {
    uint64_t min_unit_id = OB_INVALID_ID;
    uint64_t max_unit_id = OB_INVALID_ID;
    double min_cnt_value = 0.0;
    double max_cnt_value = 0.0;
    bool make_sense = false;
    if (OB_FAIL(row_stat.get_min_max_unit(min_unit_id, min_cnt_value, max_unit_id, max_cnt_value))) {
      LOG_WARN("fail to get_min_max_unit", K(ret));
    } else if (OB_FAIL(row_stat.check_move_element_waterdown_difference(
                   max_unit_id, max_cnt_value, min_unit_id, min_cnt_value, make_sense))) {
      LOG_WARN("fail to check move element make sense", K(ret));
    } else if (!make_sense) {
      balanced = true;
    } else {
      LOG_DEBUG("iterate", K(iteration), K(min_unit_id), K(max_unit_id), K(min_cnt_value), K(max_cnt_value));
      // Any item in the unit id
      int64_t rand_offset = rand();
      int64_t cur_idx = rand_offset;
      int64_t end_idx = cur_idx + map_.get_col_size();
      for (/*nop*/; OB_SUCC(ret) && cur_idx < end_idx; ++cur_idx) {
        int64_t base_offset = row_idx * map_.get_col_size();
        int64_t idx = base_offset + cur_idx % map_.get_col_size();
        SquareIdMap::Item& item = map_.map_[idx];
        if (item.dest_unit_id_ == max_unit_id) {
          // migrate unit from max_unit_id to min_unit_id
          map_.map_[idx].dest_unit_id_ = min_unit_id;
          if (OB_FAIL(row_stat.update(max_unit_id, min_unit_id))) {
            LOG_WARN("fail update row stat", K(max_unit_id), K(min_unit_id), K(ret));
          }
          break;
        }
      }
    }
  }
  if (iteration > max_iteration && !balanced) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("too many iterations. unexpected", K(iteration), K(max_iteration), K(ret));
  }
  return ret;
}

int AverageCountBalancer::overall_balance(bool& balanced)
{
  int ret = OB_SUCCESS;
  int64_t iteration = 0;
  int64_t max_iteration = map_.get_col_size() * map_.get_row_size();

  balanced = false;

  OverallBalanceStat overall_stat(map_, unit_);
  if (OB_FAIL(overall_stat.init())) {
    LOG_WARN("fail init overall stat", K(ret));
  }
  while (OB_SUCC(ret) && iteration++ <= max_iteration && !balanced && overall_stat.get_unit_count() > 1) {
    uint64_t min_unit_id = OB_INVALID_ID;
    uint64_t max_unit_id = OB_INVALID_ID;
    double min_cnt_value = 0.0;
    double max_cnt_value = 0.0;
    bool make_sense = false;
    if (OB_FAIL(overall_stat.get_min_max_unit(min_unit_id, min_cnt_value, max_unit_id, max_cnt_value))) {
      LOG_WARN("fail to get_min_max_unit", K(ret));
    } else if (OB_FAIL(overall_stat.check_move_element_waterdown_difference(
                   max_unit_id, max_cnt_value, min_unit_id, min_cnt_value, make_sense))) {
      LOG_WARN("fail to check move element make sense", K(ret));
    } else if (!make_sense) {
      balanced = true;
      LOG_DEBUG("overall balanced", K(balanced), K(overall_stat));
    } else {
      LOG_DEBUG("imbalance, iterate",
          K(iteration),
          K(min_unit_id),
          K(max_unit_id),
          K(min_cnt_value),
          K(max_cnt_value),
          K(overall_stat));
      bool migrated = false;
      for (int64_t row_idx = 0; OB_SUCC(ret) && !migrated && row_idx < map_.get_row_size(); ++row_idx) {
        RowBalanceStat row_stat(map_, unit_, row_idx);
        double row_min_cnt_value = 0;
        double row_max_cnt_value = 0;
        ObSEArray<uint64_t, 16> row_min_unit_ids;
        ObSEArray<uint64_t, 16> row_max_unit_ids;
        if (OB_FAIL(row_stat.init())) {
          LOG_WARN("fail init row stat", K(row_idx), K(ret));
        } else if (OB_FAIL(row_stat.get_min_max_unit(
                       row_min_unit_ids, row_min_cnt_value, row_max_unit_ids, row_max_cnt_value))) {
          LOG_WARN("fail get_min_max_unit", K(ret));
        } else {
          //  Find both min_unit_id and max_unit_id in ids and it becomes
          bool found_min = false;
          bool found_max = false;
          make_sense = false;
          ARRAY_FOREACH(row_min_unit_ids, i)
          {
            if (row_min_unit_ids[i] == min_unit_id) {
              found_min = true;
            }
          }
          if (found_min) {
            ARRAY_FOREACH(row_max_unit_ids, i)
            {
              if (row_max_unit_ids[i] == max_unit_id) {
                found_max = true;
              }
            }
          }
          if (found_min && found_max) {
            if (OB_FAIL(row_stat.check_move_element_retain_difference(
                    max_unit_id, row_max_cnt_value, min_unit_id, row_min_cnt_value, make_sense))) {
              LOG_WARN("fail to check move element retain difference", K(ret));
            }
          }
          if (found_min && found_max && make_sense) {
            int64_t rand_offset = rand();
            int64_t cur_idx = rand_offset;
            int64_t end_idx = cur_idx + map_.get_col_size();
            for (/*nop*/; OB_SUCC(ret) && cur_idx < end_idx; ++cur_idx) {
              int64_t base_offset = row_idx * map_.get_col_size();
              int64_t idx = base_offset + cur_idx % map_.get_col_size();
              SquareIdMap::Item& item = map_.map_[idx];
              if (item.dest_unit_id_ == max_unit_id) {
                // migrate unit from max_unit_id to min_unit_id
                map_.map_[idx].dest_unit_id_ = min_unit_id;
                migrated = true;
                if (OB_FAIL(overall_stat.update(max_unit_id, min_unit_id))) {
                  LOG_WARN("fail update overall_stat", K_(map), K(row_idx), K(max_unit_id), K(min_unit_id), K(ret));
                }
                break;
              }
            }
          }
        }
      } /* loop row_idx */
    }
  }
  if (iteration > max_iteration && !balanced) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("too many iterations. unexpected", K(iteration), K(max_iteration), K(overall_stat), K(ret));
  }

  return ret;
}
