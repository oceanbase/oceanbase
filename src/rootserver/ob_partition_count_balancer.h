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

#ifndef _OB_PARTITION_COUNT_BALANCER_H
#define _OB_PARTITION_COUNT_BALANCER_H 1

#include "ob_balancer_interface.h"
#include "ob_balance_group_data.h"

namespace oceanbase {

namespace common {
class ObIAllocator;
}

namespace rootserver {
namespace balancer {

class IdMapBalancer {
public:
  IdMapBalancer()
  {}
  virtual ~IdMapBalancer()
  {}
  virtual int balance() = 0;
};

class BalanceStat {
public:
  static const double EPSILON;

public:
  BalanceStat(SquareIdMap& map, IUnitProvider& unit) : map_(map), unit_(unit), row_unit_()
  {}
  virtual int init();
  int get_min_max_unit(uint64_t& min_unit_id, double& min_value, uint64_t& max_unit_id, double& max_value);
  int get_min_max_unit(common::ObIArray<uint64_t>& min_unit_ids, double& min_value,
      common::ObIArray<uint64_t>& max_unit_ids, double& max_value);
  // Moving the partition from left to right can reduce the load
  // difference between the two units after moving
  int check_move_element_waterdown_difference(const uint64_t left_unit_id, const double left_cnt_value,
      const uint64_t right_unit_id, const double right_cnt_value, bool& make_sense);
  // Move the partition from left to right, after moving it can ensure
  // that the load difference between the two units does not expand
  int check_move_element_retain_difference(const uint64_t left_unit_id, const double left_cnt_value,
      const uint64_t right_unit_id, const double right_cnt_value, bool& make_sense);
  int update(const uint64_t from_unit_id, const uint64_t to_unit_id);
  int64_t get_unit_count() const
  {
    return unit_.count();
  }
  virtual const SquareIdMap::Item* begin() = 0;
  virtual const SquareIdMap::Item* end() = 0;
  TO_STRING_KV(K_(row_unit));

protected:
  struct UnitCounter {
    UnitCounter() : unit_id_(common::OB_INVALID_ID), count_(0)
    {}
    UnitCounter(uint64_t unit_id, int64_t count) : unit_id_(unit_id), count_(count)
    {}
    uint64_t unit_id_;
    int64_t count_;
    TO_STRING_KV(K_(unit_id), K_(count));
  };
  int get_unit_cnt_value(UnitCounter& unit_counter, double& unit_cnt_value);
  int check_gts_monopolized_unit_without_element(UnitCounter& unit_counter, bool& is_gts_monopolized_without_element);
  int get_unit_cnt_value_if_move(const uint64_t left_unit_id, const uint64_t right_unit_id, double& new_left_cnt_value,
      double& new_right_cnt_value);

protected:
  SquareIdMap& map_;
  IUnitProvider& unit_;
  common::ObSEArray<UnitCounter, 16> row_unit_;
};

class RowBalanceStat : public BalanceStat {
public:
  RowBalanceStat(SquareIdMap& map, IUnitProvider& unit, int64_t row_idx) : BalanceStat(map, unit), row_idx_(row_idx)
  {}
  virtual const SquareIdMap::Item* begin()
  {
    return map_.map_ + row_idx_ * map_.col_size_;
  }
  virtual const SquareIdMap::Item* end()
  {
    return map_.map_ + (1 + row_idx_) * map_.col_size_;
  }
  INHERIT_TO_STRING_KV("stat", BalanceStat, K_(row_idx));

private:
  int64_t row_idx_;
};

class OverallBalanceStat : public BalanceStat {
public:
  OverallBalanceStat(SquareIdMap& map, IUnitProvider& unit) : BalanceStat(map, unit)
  {}
  virtual const SquareIdMap::Item* begin()
  {
    return map_.map_;
  }
  virtual const SquareIdMap::Item* end()
  {
    return map_.map_ + map_.col_size_ * map_.row_size_;
  }
};

class AverageCountBalancer : public IdMapBalancer {
public:
  AverageCountBalancer(SquareIdMap& map, IUnitProvider& unit_provider) : map_(map), unit_(unit_provider)
  {}
  virtual ~AverageCountBalancer()
  {}

  // Balance by number partition map
  virtual int balance() override;

private:
  int one_row_balance(int64_t row_idx, bool& balanced);
  int overall_balance(bool& balanced);

protected:
  SquareIdMap& map_;
  IUnitProvider& unit_;
};
}  // end namespace balancer
}  // end namespace rootserver
}  // end namespace oceanbase

#endif /* _OB_PARTITION_COUNT_BALANCER_H */
