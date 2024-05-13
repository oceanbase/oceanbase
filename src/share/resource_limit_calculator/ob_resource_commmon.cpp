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

#define USING_LOG_PREFIX SHARE
#include "share/resource_limit_calculator/ob_resource_commmon.h"

namespace oceanbase
{
namespace share
{
#define OB_MIN(a, b)    ((a) < (b) ? (a) : (b))
const char *get_logic_res_type_name(const int64_t intype)
{
  const char *type_name = "invalid";
  switch (intype) {
#define DEF_RESOURCE_LIMIT_CALCULATOR(n, type, name, subhandler)   \
    case n:                                                        \
      type_name = #name;                                           \
      break;
#include "share/resource_limit_calculator/ob_resource_limit_calculator_def.h"
#undef DEF_RESOURCE_LIMIT_CALCULATOR
  default:
    break;
  }

  return type_name;
}

int64_t get_logic_res_type_by_name(const char *type_name)
{
  int64_t out_type = INVALID_LOGIC_RESOURCE;
#define DEF_RESOURCE_LIMIT_CALCULATOR(n, type, name, subhandler)   \
  if (strcasecmp(type_name, #name) == 0) {                             \
    out_type = n;                                                  \
  }
#include "share/resource_limit_calculator/ob_resource_limit_calculator_def.h"
#undef DEF_RESOURCE_LIMIT_CALCULATOR
  return out_type;
}

bool is_valid_logic_res_type(const int64_t intype)
{
  bool is_valid = true;
  switch (intype) {
#define DEF_RESOURCE_LIMIT_CALCULATOR(n, type, name, subhandler)   \
  case n:                                                          \
    is_valid = true;                                               \
    break;
#include "share/resource_limit_calculator/ob_resource_limit_calculator_def.h"
#undef DEF_RESOURCE_LIMIT_CALCULATOR
  default:
    is_valid = false;
    break;
  }
  return is_valid;
}

const char *get_phy_res_type_name(const int64_t intype)
{
  const char *type_name = "invalid";
  switch (intype) {
#define DEF_PHY_RES(n, type, name)              \
    case n:                                     \
      type_name = #name;                        \
      break;
#include "share/resource_limit_calculator/ob_resource_limit_calculator_def.h"
#undef DEF_PHY_RES
  default:
    break;
  }
  return type_name;
}

bool is_valid_phy_res_type(const int64_t intype)
{
  bool is_valid = true;
  switch (intype) {
#define DEF_PHY_RES(n, type, name)              \
  case n:                                       \
    is_valid = true;                            \
    break;
#include "share/resource_limit_calculator/ob_resource_limit_calculator_def.h"
#undef DEF_PHY_RES
  default:
    is_valid = false;
    break;
  }
  return is_valid;
}

const char *get_constraint_type_name(const int64_t intype)
{
  const char *type_name = "invalid";
  switch (intype) {
#define DEF_RESOURCE_CONSTRAINT(n, type, name)  \
    case n:                                     \
      type_name = #name;                        \
      break;
#include "share/resource_limit_calculator/ob_resource_limit_calculator_def.h"
#undef DEF_RESOURCE_CONSTRAINT
  default:
    break;
  }
  return type_name;
}

bool is_valid_res_constraint_type(const int64_t intype)
{
  bool is_valid = true;
  switch (intype) {
#define DEF_RESOURCE_CONSTRAINT(n, type, name)  \
  case n:                                       \
    is_valid = true;                            \
    break;
#include "share/resource_limit_calculator/ob_resource_limit_calculator_def.h"
#undef DEF_RESOURCE_CONSTRAINT
  default:
    is_valid = false;
    break;
  }
  return is_valid;
}

DEF_TO_STRING(ObResourceInfo)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(curr_utilization), K_(max_utilization), K_(reserved_value),
       K_(min_constraint_value), K_(min_constraint_type),
       "min_constraint_type", get_constraint_type_name(min_constraint_type_));
  J_OBJ_END();
  return pos;
}

int ObResoureConstraintValue::set_type_value(const int64_t type, const int64_t value)
{
  int ret = OB_SUCCESS;
  if (!is_valid_res_constraint_type(type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(type));
  } else {
    int64_t count = constraint_values_.count();
    while (OB_SUCC(ret) && count < MAX_CONSTRAINT) {
      if (OB_FAIL(constraint_values_.push_back(INT64_MAX))) {
        LOG_WARN("set type value failed", K(ret));
      } else {
        count = constraint_values_.count();
      }
    }
    if (OB_SUCC(ret)) {
      constraint_values_[type] = value;
    }
  }
  return ret;
}

int ObResoureConstraintValue::get_type_value(const int64_t type, int64_t &value) const
{
  int ret = OB_SUCCESS;
  if (!is_valid_res_constraint_type(type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(type));
  } else if (type < constraint_values_.count()) {
    value = constraint_values_[type];
  } else {
    value = INT64_MAX;
  }
  return ret;
}

DEF_TO_STRING(ObResoureConstraintValue)
{
  int64_t pos = 0;
  J_OBJ_START();
  for (int64_t i = CONFIGURATION_CONSTRAINT; i < constraint_values_.count(); i++) {
    J_KV(get_constraint_type_name(i), constraint_values_[i]);
    J_COMMA();
  }
  J_OBJ_END();
  return pos;
}

int ObMinPhyResourceResult::set_type_value(const int64_t type, const int64_t value)
{
  int ret = OB_SUCCESS;
  if (!is_valid_phy_res_type(type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument");
  } else {
    int64_t count = min_phy_resource_value_.count();
    while (OB_SUCC(ret) && count < MAX_PHY_RESOURCE) {
      if (OB_FAIL(min_phy_resource_value_.push_back(0))) {
        LOG_WARN("set type value failed", K(ret));
      } else {
        count = min_phy_resource_value_.count();
      }
    }
    if (OB_SUCC(ret)) {
      min_phy_resource_value_[type] = value;
    }
  }
  return ret;
}

int ObMinPhyResourceResult::get_type_value(const int64_t type, int64_t &value) const
{
  int ret = OB_SUCCESS;
  if (!is_valid_phy_res_type(type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument");
  } else if (type < min_phy_resource_value_.count()) {
    value = min_phy_resource_value_[type];
  } else {
    value = 0;
  }
  return ret;
}

int ObMinPhyResourceResult::inc_update(const ObMinPhyResourceResult &res)
{
  int ret = OB_SUCCESS;
  int64_t tmp_v = 0;
  int64_t tmp_nv = 0;
#define DEF_PHY_RES(n, type, name)                                      \
  if (OB_SUCC(ret)) {                                                   \
    if (OB_FAIL(get_type_value(n, tmp_v))) {                            \
      LOG_WARN("get type value failed", K(n), K(#name));                \
    } else if (OB_FAIL(res.get_type_value(n, tmp_nv))) {                \
      LOG_WARN("get input type value failed", K(n), K(#name));          \
    } else if (tmp_nv <= tmp_v) {                                       \
      /*do nothing*/                                                    \
    } else if (OB_FAIL(set_type_value(n, tmp_nv))) {                    \
      LOG_WARN("set type value failed", K(n), K(#name), K(tmp_nv));     \
    }                                                                   \
  }
#include "share/resource_limit_calculator/ob_resource_limit_calculator_def.h"
#undef DEF_PHY_RES
  return ret;
}

OB_SERIALIZE_MEMBER(ObMinPhyResourceResult, min_phy_resource_value_);

DEF_TO_STRING(ObMinPhyResourceResult)
{
  int64_t pos = 0;
  J_OBJ_START();
  for (int64_t i = PHY_RESOURCE_MEMSTORE; i < min_phy_resource_value_.count(); i++) {
    J_KV(get_phy_res_type_name(i), min_phy_resource_value_[i]);
    J_COMMA();
  }
  J_OBJ_END();
  return pos;
}

}// end namespace share
} // end namespace oceanbase
