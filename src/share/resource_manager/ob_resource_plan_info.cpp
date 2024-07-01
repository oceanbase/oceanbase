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

#define USING_LOG_PREFIX SHARE

#include "ob_resource_plan_info.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

ObString oceanbase::share::get_io_function_name(ObFunctionType function_type)
{
  ObString ret_name;
  switch (function_type) {
    case ObFunctionType::PRIO_COMPACTION_HIGH:
      ret_name = ObString("COMPACTION_HIGH");
      break;
    case ObFunctionType::PRIO_HA_HIGH:
      ret_name = ObString("HA_HIGH");
      break;
    case ObFunctionType::PRIO_COMPACTION_MID:
      ret_name = ObString("COMPACTION_MID");
      break;
    case ObFunctionType::PRIO_HA_MID:
      ret_name = ObString("HA_MID");
      break;
    case ObFunctionType::PRIO_COMPACTION_LOW:
      ret_name = ObString("COMPACTION_LOW");
      break;
    case ObFunctionType::PRIO_HA_LOW:
      ret_name = ObString("HA_LOW");
      break;
    case ObFunctionType::PRIO_DDL:
      ret_name = ObString("DDL");
      break;
    case ObFunctionType::PRIO_DDL_HIGH:
      ret_name = ObString("DDL_HIGH");
      break;
    case ObFunctionType::PRIO_OTHER_BACKGROUND:
      ret_name = ObString("OTHER_BACKGROUND");
      break;
    default:
      ret_name = ObString("OTHER_GROUPS");
      break;
  }
  return ret_name;
}

int ObPlanDirective::assign(const ObPlanDirective &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  mgmt_p1_ = other.mgmt_p1_;
  utilization_limit_ = other.utilization_limit_;
  min_iops_ = other.min_iops_;
  max_iops_ = other.max_iops_;
  weight_iops_ = other.weight_iops_;
  group_id_ = other.group_id_;
  ret = group_name_.assign(other.group_name_);
  return ret;
}

int ObResourceMappingRule::assign(const ObResourceMappingRule &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  group_id_ = other.group_id_;
  OZ(set_attr(other.attr_), other);
  OZ(set_value(other.value_), other);
  OZ(set_group(other.group_), other);
  return ret;
}

int ObResourceIdNameMappingRule::assign(const ObResourceIdNameMappingRule &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  group_id_ = other.group_id_;
  ret = group_name_.assign(other.group_name_);
  return ret;
}


int ObResourceUserMappingRule::assign(const ObResourceUserMappingRule &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  user_id_ = other.user_id_;
  group_id_ = other.group_id_;
  ret = group_name_.assign(other.group_name_);
  return ret;
}

int ObResourceColumnMappingRule::assign(const ObResourceColumnMappingRule &other)
{
  tenant_id_ = other.tenant_id_;
  database_id_ = other.database_id_;
  table_name_ = other.table_name_;
  column_name_ = other.column_name_;
  literal_value_ = other.literal_value_;
  user_name_ = other.user_name_;
  group_id_ = other.group_id_;
  case_mode_ = other.case_mode_;
  return OB_SUCCESS;
}

void ObResourceColumnMappingRule::reset_table_column_name(ObIAllocator &allocator)
{
  if (OB_NOT_NULL(table_name_.ptr())) {
    allocator.free(table_name_.ptr());
    table_name_.reset();
  }
  if (OB_NOT_NULL(column_name_.ptr())) {
    allocator.free(column_name_.ptr());
    column_name_.reset();
  }
}
void ObResourceColumnMappingRule::reset_user_name_literal(ObIAllocator &allocator)
{
  if (OB_NOT_NULL(literal_value_.ptr())) {
    allocator.free(literal_value_.ptr());
    literal_value_.reset();
  }
  if (OB_NOT_NULL(user_name_.ptr())) {
    allocator.free(user_name_.ptr());
    user_name_.reset();
  }
}

int ObResourceColumnMappingRule::write_string_values(uint64_t tenant_id,
                        common::ObString table_name, common::ObString column_name,
                        common::ObString literal_value, common::ObString user_name,
                        ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  void *table_name_buf = allocator.alloc(table_name.length());
  void *column_name_buf = allocator.alloc(column_name.length());
  void *literal_value_buf = allocator.alloc(literal_value.length());
  void *user_name_buf = user_name.empty() ? NULL : allocator.alloc(user_name.length());
  if (OB_ISNULL(table_name_buf) || OB_ISNULL(column_name_buf)
      || OB_ISNULL(literal_value_buf)
      || (!user_name.empty() && OB_ISNULL(user_name_buf))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(literal_value));
    if (NULL != table_name_buf) {
      allocator.free(table_name_buf);
    }
    if (NULL != column_name_buf) {
      allocator.free(column_name_buf);
    }
    if (NULL != literal_value_buf) {
      allocator.free(literal_value_buf);
    }
    if (NULL != user_name_buf) {
      allocator.free(user_name_buf);
    }
  } else {
    MEMCPY(table_name_buf, table_name.ptr(), table_name.length());
    MEMCPY(column_name_buf, column_name.ptr(), column_name.length());
    MEMCPY(literal_value_buf, literal_value.ptr(), literal_value.length());
    table_name_.assign(static_cast<char*>(table_name_buf), table_name.length());
    column_name_.assign(static_cast<char*>(column_name_buf), column_name.length());
    literal_value_.assign(static_cast<char*>(literal_value_buf), literal_value.length());
    if (NULL != user_name_buf) {
      MEMCPY(user_name_buf, user_name.ptr(), user_name.length());
      user_name_.assign(static_cast<char*>(user_name_buf), user_name.length());
    } else {
      user_name_.reset();
    }
  }

  return ret;
}
