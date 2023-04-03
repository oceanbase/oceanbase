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

#ifndef _OB_ITEM_TYPE_STR_H
#define _OB_ITEM_TYPE_STR_H 1
#include "objit/common/ob_item_type.h"

namespace oceanbase
{
namespace sql
{
inline const char *ob_aggr_func_str(ObItemType aggr_func)
{
  const char *ret = "UNKNOWN_AGGR";
  switch (aggr_func) {
    case T_FUN_MAX:
      ret = "MAX";
      break;
    case T_FUN_MIN:
      ret = "MIN";
      break;
    case T_FUN_COUNT_SUM:
      ret = "COUNT_SUM";
      break;
    case T_FUN_SUM:
      ret = "SUM";
      break;
    case T_FUN_COUNT:
      ret = "COUNT";
      break;
    case T_FUN_AVG:
      ret = "AVG";
      break;
    case T_FUN_GROUPING:
    	ret = "GROUPING";
    	break;
    case T_FUN_GROUPING_ID:
      ret = "GROUPING_ID";
      break;
    case T_FUN_GROUP_ID:
      ret = "GROUP_ID";
      break;
    case T_FUN_GROUP_CONCAT:
      ret = "GROUP_CONCAT";
      break;
    case T_FUN_APPROX_COUNT_DISTINCT:
      ret = "APPROX_COUNT_DISTINCT";
      break;
    case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS:
      ret = "APPROX_COUNT_DISTINCT_SYNOPSIS";
      break;
    case T_FUN_APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE:
      ret = "APPROX_COUNT_DISTINCT_SYNOPSIS_MERGE";
      break;
    case T_FUN_VARIANCE:
      ret = "VARIANCE";
      break;
    case T_FUN_STDDEV:
      ret = "STDDEV";
      break;
    case T_FUN_SYS_BIT_AND:
      ret = "BIT_AND";
      break;
    case T_FUN_SYS_BIT_OR:
      ret = "BIT_OR";
      break;
    case T_FUN_SYS_BIT_XOR:
      ret = "BIT_XOR";
      break;
    default:
      break;
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_ITEM_TYPE_STR_H */
