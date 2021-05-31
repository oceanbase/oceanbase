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

#include "ob_partition_disk_balancer.h"
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

int RowDiskBalanceStat::get_item_load(int64_t idx, int64_t& load) const
{
  int ret = OB_SUCCESS;
  if (idx < 0 || idx >= item_disk_info_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid idx", K(idx), "max", item_disk_info_.count(), K(ret));
  } else {
    load = item_disk_info_.at(idx).data_size_;
  }
  return ret;
}

int RowDiskBalanceStat::get_total_load(int64_t& load) const
{
  load = total_load_.data_size_;
  return OB_SUCCESS;
}

int RowDiskBalanceStat::get_unit_load(int64_t unit_id, int64_t& load) const
{
  int ret = OB_SUCCESS;
  Stat unit_load;
  if (OB_FAIL(unit_load_map_.get(unit_id, unit_load))) {
    LOG_WARN("fail get unit load", K(unit_id), K(ret));
  } else {
    load = unit_load.data_size_;
  }
  return ret;
}

int RowDiskBalanceStat::get_unit_load(int64_t unit_id, Stat*& unit_load)
{
  int ret = OB_SUCCESS;
  UnitLoadMap::Item* item = NULL;
  if (OB_FAIL(unit_load_map_.locate(unit_id, item))) {
    LOG_WARN("fail get unit load", K(unit_id), K(ret));
  } else {
    unit_load = &item->v_;
  }
  return ret;
}

int RowDiskBalanceStat::init()
{
  int ret = OB_SUCCESS;
  int64_t col_size = map_.get_col_size();
  if (OB_INVALID_INDEX == row_idx_) {
    ret = OB_NOT_INIT;
    LOG_WARN("row_idx_ not inited", K_(row_idx), K(ret));
  } else if (col_size <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid col size", K(col_size), K(ret));
  } else if (OB_FAIL(item_disk_info_.reserve(col_size))) {
    LOG_WARN("fail reserve memory", K(col_size), K(ret));
  } else if (unit_load_map_.init(col_size)) {
    LOG_WARN("fail init unit load map", K(col_size), K(ret));
  }
  int64_t sum_load = 0;
  for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < col_size; ++col_idx) {
    if (col_idx < 0 || col_idx >= col_size) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected index", K(col_idx), K(col_size), K_(map), K(ret));
    } else {
      SquareIdMap::Item* item = NULL;
      if (OB_FAIL(map_.get(row_idx_, col_idx, item))) {
        LOG_WARN("fail get item from map", K_(row_idx), K(col_idx), K_(map), K(ret));
      } else if (OB_ISNULL(item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL unexpected", K(ret));
      } else {
        int64_t data_size = 0;
        // Sum of partition data_size of multiple tables under tg
        if (OB_FAIL(stat_finder_.get_partition_group_data_size(zone_, item->all_tg_idx_, item->part_idx_, data_size))) {
          LOG_WARN("fail get pg data size", K_(zone), K(ret));
        } else {
          if (OB_SUCC(ret)) {
            Stat disk(data_size);
            if (OB_FAIL(item_disk_info_.push_back(disk))) {
              LOG_WARN("fail push back item to item_disk", K(disk));
            } else {
              sum_load += data_size;
            }
          }
          if (OB_SUCC(ret)) {
            RowDiskBalanceStat::UnitLoadMap::Item* unit_disk = NULL;
            uint64_t unit_id = item->dest_unit_id_;
            if (OB_INVALID_ID == unit_id) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unit_id is invalid", K(unit_id), K(*item), K(ret));
            } else if (unit_load_map_.locate(unit_id, unit_disk)) {
              LOG_WARN("fail locate unit", K(unit_id), K(ret));
            } else if (OB_ISNULL(unit_disk)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("NULL unexpected", K(unit_id), K(ret));
            } else {
              unit_disk->v_.data_size_ += data_size;
            }
          }
        }
      }
    }
  }
  return ret;
}
//////////////////////////////////////////////////////
//////////////// DynamicAverageDiskBalancer /////////////////
//////////////////////////////////////////////////////

int UnitDiskBalanceStat::init()
{
  int ret = OB_SUCCESS;
  ObArray<UnitStat*> unit_stats;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(unit_provider_.get_units(unit_stats))) {
    LOG_WARN("fail get units", K(ret));
  } else if (OB_FAIL(unit_provider_.get_avg_load(avg_load_))) {
    LOG_WARN("fail get avg units load", K(ret));
  } else if (OB_FAIL(unit_load_map_.init(unit_stats.count()))) {
    LOG_WARN("fail create map", K(ret));
  } else {
    // Build a map index on unit_stats to speed up the search
    ARRAY_FOREACH_X(unit_stats, i, cnt, OB_SUCC(ret))
    {
      UnitDiskBalanceStat::UnitLoadMap::Item* item = NULL;
      UnitStat* us = unit_stats.at(i);
      if (OB_ISNULL(us)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL unexpected", K(ret));
      } else if (OB_FAIL(unit_load_map_.locate(us->get_unit_id(), item))) {
        LOG_WARN("fail locate unit", "unit_id", us->get_unit_id(), K(ret));
      } else if (OB_ISNULL(item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL unexpected", "unit_id", us->get_unit_id(), K(ret));
      } else {
        item->v_.unit_stat_ = us;
      }
    }
  }
  if (OB_SUCC(ret)) {
    inited_ = true;
  }
  return ret;
}

void UnitDiskBalanceStat::debug_dump()
{
  int ret = OB_SUCCESS;
  ObArray<UnitStat*> unit_stats;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(unit_provider_.get_units(unit_stats))) {
    LOG_WARN("fail get units", K(ret));
  } else {
    for (int i = 0; i < unit_stats.count(); ++i) {
      UnitStat& us = *unit_stats.at(i);
      LOG_INFO("unit stat",
          "unit_id",
          us.get_unit_id(),
          "disk_used",
          us.load_factor_.get_disk_usage(),
          "capacity",
          us.capacity_.get_disk_capacity(),
          "load",
          (double)us.load_factor_.get_disk_usage() / (double)us.capacity_.get_disk_capacity());
    }
  }
}

int UnitDiskBalanceStat::get_unit_load(uint64_t unit_id, UnitStat*& stat)
{
  int ret = OB_SUCCESS;
  Stat unit_load;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(unit_load_map_.get(unit_id, unit_load))) {
    LOG_WARN("fail get unit load", K(unit_id), K(ret));
  } else {
    stat = unit_load.unit_stat_;
  }
  return ret;
}

int UnitDiskBalanceStat::get_max_min_load_unit(UnitStat*& max_u, UnitStat*& min_u)
{
  int ret = OB_SUCCESS;
  ObArray<UnitStat*> unit_stats;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(unit_provider_.get_units(unit_stats))) {
    LOG_WARN("fail get units", K(ret));
  } else {
    max_u = NULL;
    min_u = NULL;
    ARRAY_FOREACH_X(unit_stats, i, cnt, OB_SUCC(ret))
    {
      UnitStat* u = unit_stats.at(i);
      if (NULL == u || NULL == u->server_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL unexpected", K(i), K(cnt), KP(u), K(ret));
      } else if (u->capacity_ratio_ <= 0) {
        // bypass the unit which is excluded by gts
      } else if (u->server_->can_migrate_out() && u->server_->can_migrate_in()) {
        if (NULL == max_u) {
          max_u = u;
        }
        if (NULL == min_u) {
          min_u = u;
        }

        double u_load = u->get_disk_usage() / u->get_disk_limit();
        double max_load = max_u->get_disk_usage() / max_u->get_disk_limit();
        double min_load = min_u->get_disk_usage() / min_u->get_disk_limit();
        if (NULL != max_u && u_load > max_load) {
          max_u = u;
        } else if (NULL != min_u && u_load < min_load) {
          min_u = u;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (GCONF.balancer_tolerance_percentage >= 100) {
        max_u = min_u = NULL;
      } else if (NULL == max_u || NULL == min_u) {
        LOG_DEBUG("can not find swapable unit", KP(max_u), KP(min_u));
        max_u = min_u = NULL;
      } else if (max_u == min_u) {
        LOG_DEBUG("can not find swapable unit", KP(max_u), KP(min_u));
        max_u = min_u = NULL;
      } else {
        double tolerance = static_cast<double>(GCONF.balancer_tolerance_percentage) / 100;
        double max_load = max_u->get_disk_usage() / max_u->get_disk_limit();
        if (max_load < avg_load_ + tolerance) {
          LOG_DEBUG("already balanced", KP(max_u), KP(min_u));
          max_u = min_u = NULL;
        }
      }
    }
  }
  return ret;
}

int UnitDiskBalanceStat::get_avg_load(double& avg_load)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
  } else {
    avg_load = avg_load_;
  }
  return ret;
}

// Dynamic load balancing algorithms should focus on the overall situation,
// and should not only be balanced within the line.
// resion:
// 1. The industry cannot always be balanced
// 2. When absolute balance cannot be achieved in the industry,
//    UNIT cannot be truly balanced either
// 3. Only by considering all copies together,
//    can unit equilibrium be achieved cost-effectively and efficiently
// implement:
// 1. Balance the number in the line
// 2. Try to exchange partitions to make the UNIT disk occupancy more balanced
// Known issues:
// The exchange occurs in the row (partition level),
// no partition is selected at the table level to exchange,
// There is no strategy such as "large partition priority exchange"
int DynamicAverageDiskBalancer::balance()
{
  int ret = OB_SUCCESS;
  // 1. Do the number balance first
  if (OB_SUCC(ret)) {
    if (OB_FAIL(count_balancer_.balance())) {
      LOG_WARN("fail do pre count balance", K_(map), K(ret));
    } else {
      // map_.dump2("debug. after dynamic count balancer", true);
    }
  }

  // 2. According to the result of the number balance, recalculate the disk usage of each unit
  if (OB_SUCC(ret)) {
    if (OB_FAIL(unit_stat_.init())) {
      LOG_WARN("fail init unit stat", K(ret));
    } else if (OB_FAIL(update_unit_stat(unit_stat_))) {
      LOG_WARN("fail update unit stat to reflect count balance result", K_(map), K(ret));
    } else {
      // unit_stat_.debug_dump();
    }
  }

  // 3. Perform UNIT disk balancing on the basis of number balancing,
  //    By exchanging items, the disk usage of the unit after migration is as close as possible to the mean value.
  //    The detailed algorithm is:
  //    1). The set of units whose disk usage is greater than avg + threshold is LARGE
  //    2). The set of units whose disk usage is less than avg is SMALL
  //    3). Through the exchange algorithm,
  //        the disk usage of all units is as far as possible to fall within the range of [avg, avg + threshold]
  //        specific methods:
  //        Look for exchangeable cells in row, so that one cell_lg belongs to LARGE and one cell_sm belongs to SMALL
  //        And satisfied:
  //        a. cell_lg.data_size > cell_sm.data_size
  //        b. After the swap, the disk usage of cell_lg.unit is greater than avg:
  //           cell_lg.unit.data_size - cell_lg.data_size + cell_sm.data_size > avg
  //        c. After the swap, the disk usage of cell_sm.unit is less than avg + threshold:
  //           cell_sm.unit.data_size - cell_sm.data_size + cell_lg.data_size < avg + threshold
  if (OB_SUCC(ret)) {
    UnitStat* max_u = NULL;
    UnitStat* min_u = NULL;
    int64_t task_cnt = 0;
    const int64_t row_size = map_.get_row_size();
    do {
      task_cnt = 0;
      if (OB_FAIL(unit_stat_.get_max_min_load_unit(max_u, min_u))) {
        LOG_WARN("fail get max min load unit", K(ret));
      } else if (NULL != max_u && NULL != min_u) {
        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < row_size; ++row_idx) {
          if (OB_FAIL(one_row_balance(unit_stat_, *max_u, *min_u, row_idx, task_cnt))) {
            LOG_WARN("fail balance one row", K(row_idx), K(map_), K(ret));
          }
        }
      }
    } while (task_cnt > 0);
    // Adjust to the optimal one-time as much as possible to avoid the additional cost of multiple migrations
  }
  return ret;
}

int DynamicAverageDiskBalancer::one_row_balance(
    UnitDiskBalanceStat& unit_stat, UnitStat& max_u, UnitStat& min_u, int64_t row_idx, int64_t& task_cnt)
{
  int ret = OB_SUCCESS;
  int64_t col_size = map_.get_col_size();

  RowDiskBalanceStat row_stat(stat_finder_, zone_, map_, row_idx);
  if (OB_FAIL(row_stat.init())) {
    LOG_WARN("fail init row stat", K(ret));
  } else if (row_stat.size() != col_size) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row stat should have same size as map width", "size", row_stat.size(), K(col_size), K(ret));
  }

  for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < col_size; ++col_idx) {
    SquareIdMap::Item* item = NULL;
    if (OB_FAIL(map_.get(row_idx, col_idx, item))) {
      LOG_WARN("fail get item from map", K(row_idx), K(col_idx), K_(map), K(ret));
    } else if (OB_ISNULL(item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL unexpected", K(ret));
    } else if (item->dest_unit_id_ == max_u.get_unit_id()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < col_size; ++i) {
        SquareIdMap::Item* replace = NULL;
        if (OB_FAIL(map_.get(row_idx, i, replace))) {
          LOG_WARN("fail get item from map", K(row_idx), K(col_idx), K(i), K_(map), K(ret));
        } else if (OB_ISNULL(replace)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL unexpected", K(ret));
        } else if (replace->dest_unit_id_ == min_u.get_unit_id() &&
                   item->is_designated_leader() == replace->is_designated_leader() &&
                   item->get_replica_type() == replace->get_replica_type() &&
                   item->get_memstore_percent() == replace->get_memstore_percent() &&
                   REPLICA_TYPE_LOGONLY != replace->get_replica_type()) {
          bool exchangable = false;
          if (OB_FAIL(test_and_exchange(unit_stat, max_u, min_u, row_stat, col_idx, *item, i, *replace, exchangable))) {
            LOG_WARN("fail test_exchangable", K(ret));
          } else if (exchangable) {
            task_cnt++;
          }
        }
      }
    }
  }
  return ret;
}

int DynamicAverageDiskBalancer::test_and_exchange(UnitDiskBalanceStat& unit_stat, UnitStat& max_u, UnitStat& min_u,
    RowDiskBalanceStat& row_stat, int64_t idx_a, SquareIdMap::Item& a, int64_t idx_b, SquareIdMap::Item& b,
    bool& exchangable)
{
  int ret = OB_SUCCESS;
  exchangable = false;
  const int64_t min_absolute_load = 3 * 1024 * 1024 * 1024LL;  // 3G
  if (a.dest_unit_id_ != b.dest_unit_id_ && GCONF.balancer_tolerance_percentage < 100) {
    double avg_load = 0;
    int64_t item_load_max = 0;
    int64_t item_load_min = 0;
    int64_t tolerance_percent = GCONF.balancer_tolerance_percentage;
    if (OB_FAIL(unit_stat.get_avg_load(avg_load))) {
      LOG_WARN("fail get total load", K(ret));
    } else if (OB_FAIL(row_stat.get_item_load(idx_a, item_load_max))) {
      LOG_WARN("fail get item load", K(idx_a), K(a), K(ret));
    } else if (OB_FAIL(row_stat.get_item_load(idx_b, item_load_min))) {
      LOG_WARN("fail get item load", K(idx_b), K(b), K(ret));
    } else if (item_load_max - item_load_min < min_absolute_load &&
               (static_cast<double>(item_load_max - item_load_min) <
                   static_cast<double>(item_load_max) * tolerance_percent / 100)) {
      exchangable = false;
    } else {
      // The following calculations all use absolute values,
      // instead of using the floating-point relative value of [0,1]
      int64_t unit_load_max = static_cast<int64_t>(max_u.get_disk_usage());
      int64_t unit_load_min = static_cast<int64_t>(min_u.get_disk_usage());
      int64_t new_unit_load_max = unit_load_max - item_load_max + item_load_min;
      int64_t new_unit_load_min = unit_load_min - item_load_min + item_load_max;

      double tolerance = static_cast<double>(GCONF.balancer_tolerance_percentage) / 100;
      int64_t threshold_max = static_cast<int64_t>(tolerance * max_u.get_disk_limit());
      int64_t threshold_min = static_cast<int64_t>(tolerance * min_u.get_disk_limit());
      int64_t avg_load_max = static_cast<int64_t>(avg_load * max_u.get_disk_limit());
      int64_t avg_load_min = static_cast<int64_t>(avg_load * min_u.get_disk_limit());

      // Migrate the unit outside the equilibrium interval to the interval by exchanging item
      //
      //  a.unit              b.unit
      // -----------        ---------------   High water mark
      //
      //               or
      //
      // -----------        ---------------   Average load line
      //  b.unit              a.unit
      //
      if ((unit_load_max > avg_load_max + threshold_max && unit_load_min < avg_load_min &&
              new_unit_load_max > avg_load_max && new_unit_load_min < avg_load_min + threshold_min &&
              item_load_max > item_load_min)) {
        LOG_INFO("SWAP ITEM TO LOWER a LOAD",
            "unit_max",
            a.dest_unit_id_,
            "unit_min",
            b.dest_unit_id_,
            K(unit_load_max),
            K(unit_load_min),
            K(new_unit_load_max),
            K(new_unit_load_min),
            K(tolerance),
            K(threshold_max),
            K(threshold_min),
            K(avg_load_max),
            "upper_load_max",
            avg_load_max + threshold_max,
            K(avg_load_min),
            "upper_load_min",
            avg_load_min + threshold_min);
        exchangable = true;
        max_u.load_factor_.set_disk_used(new_unit_load_max);
        min_u.load_factor_.set_disk_used(new_unit_load_min);
        exchange(a, b);
        LOG_DEBUG("exchage",
            K(a),
            K(b),
            K(idx_a),
            K(idx_b),
            K(unit_load_max),
            K(unit_load_min),
            K(item_load_max),
            K(item_load_min));
      }
    }
  }
  return ret;
}

void DynamicAverageDiskBalancer::exchange(SquareIdMap::Item& a, SquareIdMap::Item& b)
{
  uint64_t tmp = a.dest_unit_id_;
  a.dest_unit_id_ = b.dest_unit_id_;
  b.dest_unit_id_ = tmp;
}

int DynamicAverageDiskBalancer::update_unit_stat(UnitDiskBalanceStat& unit_stat)
{
  int ret = OB_SUCCESS;
  int64_t row_size = map_.get_row_size();
  int64_t col_size = map_.get_col_size();

  for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < row_size; ++row_idx) {
    bool need_update = false;
    // Performance optimization:
    // When the balancer is executed repeatedly, except for a few occasions.
    // Most of the time, the number is generally balanced, and there is no need to perform unit update operations
    for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < col_size && !need_update; ++col_idx) {
      SquareIdMap::Item* item = NULL;
      if (OB_FAIL(map_.get(row_idx, col_idx, item))) {
        LOG_WARN("fail get item from map", K(row_idx), K(col_idx), K_(map), K(ret));
      } else if (OB_ISNULL(item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL unexpected", K(ret));
      } else if (item->unit_id_ != item->dest_unit_id_) {
        need_update = true;
      }
    }

    if (need_update) {
      RowDiskBalanceStat row_stat(stat_finder_, zone_, map_, row_idx);
      if (OB_FAIL(row_stat.init())) {
        LOG_WARN("fail init row stat", K(ret));
      } else if (row_stat.size() != col_size) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("row stat should have same size as map width", "size", row_stat.size(), K(col_size), K(ret));
      } else {
        for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < col_size; ++col_idx) {
          SquareIdMap::Item* item = NULL;
          int64_t item_data_size = 0;
          UnitStat* from_unit = NULL;
          UnitStat* to_unit = NULL;
          if (OB_FAIL(map_.get(row_idx, col_idx, item))) {
            LOG_WARN("fail get item from map", K(row_idx), K(col_idx), K_(map), K(ret));
          } else if (OB_ISNULL(item)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("NULL unexpected", K(ret));
          } else if (item->unit_id_ != item->dest_unit_id_) {
            if (OB_FAIL(row_stat.get_item_load(col_idx, item_data_size))) {
              LOG_WARN("fail get item load", K(col_idx), K(ret));
            } else if (OB_FAIL(unit_stat.get_unit_load(item->unit_id_, from_unit))) {
              LOG_WARN("fail get unit load", "from", item->unit_id_, K(ret));
            } else if (OB_FAIL(unit_stat.get_unit_load(item->dest_unit_id_, to_unit))) {
              LOG_WARN("fail get unit load", "to", item->dest_unit_id_, K(ret));
            } else {
              int64_t new_unit_load_from = static_cast<int64_t>(from_unit->get_disk_usage()) - item_data_size;
              int64_t new_unit_load_to = static_cast<int64_t>(to_unit->get_disk_usage()) + item_data_size;
              from_unit->load_factor_.set_disk_used(new_unit_load_from);
              to_unit->load_factor_.set_disk_used(new_unit_load_to);
            }
          }
        }
      }
    }
  }
  return OB_SUCCESS;
}
