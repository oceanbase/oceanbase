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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_expr_extra_info_factory.h"
#include "sql/engine/expr/ob_expr_calc_partition_id.h"
#include "sql/engine/expr/ob_expr_type_to_str.h"

namespace oceanbase {
using namespace common;
namespace sql {

#define REG_EXTRA_INFO(type, ExtraInfoClass)                                                \
  do {                                                                                      \
    static_assert(type > T_INVALID && type < T_MAX_OP, "invalid expr type for extra info"); \
    ALLOC_FUNS_[type] = ObExprExtraInfoFactory::alloc<ExtraInfoClass>;                      \
  } while (0)

ObExprExtraInfoFactory::AllocExtraInfoFunc ObExprExtraInfoFactory::ALLOC_FUNS_[T_MAX_OP] = {};

int ObExprExtraInfoFactory::alloc(
    common::ObIAllocator& alloc, const ObExprOperatorType& type, ObIExprExtraInfo*& extra_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!(type > T_INVALID && type < T_MAX_OP))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), K(type));
  } else if (OB_ISNULL(ALLOC_FUNS_[type])) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "this expr not register extra info", K(ret), K(type));
  } else if (OB_FAIL(ALLOC_FUNS_[type](alloc, extra_info, type))) {
    OB_LOG(WARN, "fail to alloc extra info", K(ret), K(type));
  } else if (OB_ISNULL(extra_info)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(ERROR, "fail to alloc extra info", K(ret), K(type));
  }

  return ret;
}

void ObExprExtraInfoFactory::register_expr_extra_infos()
{
  MEMSET(ALLOC_FUNS_, 0, sizeof(ALLOC_FUNS_));

  // Add the structure of ObExpr Extra info, you need to register here
  REG_EXTRA_INFO(T_FUN_SYS_CALC_PARTITION_ID, CalcPartitionIdInfo);
  REG_EXTRA_INFO(T_FUN_ENUM_TO_STR, ObEnumSetInfo);
  REG_EXTRA_INFO(T_FUN_SET_TO_STR, ObEnumSetInfo);
  REG_EXTRA_INFO(T_FUN_ENUM_TO_INNER_TYPE, ObEnumSetInfo);
  REG_EXTRA_INFO(T_FUN_SET_TO_INNER_TYPE, ObEnumSetInfo);
  REG_EXTRA_INFO(T_FUN_COLUMN_CONV, ObEnumSetInfo);
}

}  // end namespace sql
}  // end namespace oceanbase
