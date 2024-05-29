/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SHARE_OB_RESOURCE_COMMON_H
#define OCEANBASE_SHARE_OB_RESOURCE_COMMON_H

#include <stdint.h>
#include "lib/container/ob_array_serialization.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"

namespace oceanbase
{
namespace share
{
enum ObLogicResourceType : int64_t
{
  INVALID_LOGIC_RESOURCE = 0,
#define DEF_RESOURCE_LIMIT_CALCULATOR(n, type, name, subhandler) \
  LOGIC_RESOURCE_##type = n,
#include "share/resource_limit_calculator/ob_resource_limit_calculator_def.h"
#undef DEF_RESOURCE_LIMIT_CALCULATOR

  MAX_LOGIC_RESOURCE
};

const char *get_logic_res_type_name(const int64_t type);
int64_t get_logic_res_type_by_name(const char *name);
bool is_valid_logic_res_type(const int64_t type);

enum ObPhyResourceType : int64_t
{
  INVALID_PHY_RESOURCE = 0,
#define DEF_PHY_RES(n, type, name)     \
  PHY_RESOURCE_##type = n,
#include "share/resource_limit_calculator/ob_resource_limit_calculator_def.h"
#undef DEF_PHY_RES

  MAX_PHY_RESOURCE
};

const char *get_phy_res_type_name(const int64_t type);
bool is_valid_phy_res_type(const int64_t type);

enum ObLogicResourceConstraintType : int64_t
{
  INVALID_CONSTRAINT_TYPE = 0,
#define DEF_RESOURCE_CONSTRAINT(n, type, name)  \
  type##_CONSTRAINT = n,
#include "share/resource_limit_calculator/ob_resource_limit_calculator_def.h"
#undef DEF_RESOURCE_CONSTRAINT

  MAX_CONSTRAINT
};

const char *get_constraint_type_name(const int64_t type);
bool is_valid_res_constraint_type(const int64_t type);

struct ObResourceInfo
{
public:
  ObResourceInfo() { reset(); }
  ~ObResourceInfo() { reset(); }
  void reset()
  {
    curr_utilization_ = 0;
    max_utilization_ = 0;
    reserved_value_ = 0;
    min_constraint_value_ = 0;
    min_constraint_type_ = INVALID_CONSTRAINT_TYPE;
  }

  DECLARE_TO_STRING;
public:
  int64_t curr_utilization_;
  int64_t max_utilization_;
  int64_t reserved_value_;
  int64_t min_constraint_value_;
  int64_t min_constraint_type_;
};

struct ObResoureConstraintValue
{
public:
ObResoureConstraintValue() :constraint_values_() {}
  ~ObResoureConstraintValue() {}
  void reset()
  {
    for (int64_t i = 0; i < constraint_values_.count(); i++) {
      constraint_values_[i] = INT64_MAX;
    }
  }
  void get_min_constraint(int64_t &type, int64_t &value)
  {
    int64_t min_type = 0;
    int64_t min_value = INT64_MAX;
    for (int64_t i = 0; i < constraint_values_.count(); i++) {
      if (constraint_values_[i] < min_value) {
        min_type = i;
        min_value = constraint_values_[i];
      }
    }
    type = min_type;
    value = min_value;
  }
  int get_type_value(const int64_t type, int64_t &value) const;
  int set_type_value(const int64_t type, const int64_t value);
  int get_copy_assign_ret() const
  { return constraint_values_.get_copy_assign_ret(); }
  DECLARE_TO_STRING;
public:
  common::ObSArray<int64_t> constraint_values_;
  DISALLOW_COPY_AND_ASSIGN(ObResoureConstraintValue);
};

struct ObMinPhyResourceResult
{
  OB_UNIS_VERSION_V(1);
public:
  ObMinPhyResourceResult() : min_phy_resource_value_() {}
  ~ObMinPhyResourceResult() { reset(); }
  int set_type_value(const int64_t type, const int64_t value);
  int get_type_value(const int64_t type, int64_t &value) const;
  int inc_update(const ObMinPhyResourceResult &res);
  int get_copy_assign_ret() const
  { return min_phy_resource_value_.get_copy_assign_ret(); }
  void reset()
  {
    for (int i = 0; i < min_phy_resource_value_.count(); i++) {
      min_phy_resource_value_[i] = 0;
    }
  }
  DECLARE_TO_STRING;
private:
  common::ObSArray<int64_t> min_phy_resource_value_;
};

} //end share
} //end oceanbase

#endif
