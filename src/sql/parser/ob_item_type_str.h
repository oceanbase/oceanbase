/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
    case T_FUN_CK_GROUPCONCAT:
      ret = "GROUPCONCAT";
      break;
    case T_FUN_STRING_AGG:
      ret = "STRING_AGG";
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
    case T_FUN_ARG_MAX:
      ret = "ARG_MAX";
      break;
    case T_FUN_ARG_MIN:
      ret = "ARG_MIN";
      break;
    case T_FUN_CK_UNIQ:
      ret = "UNIQ";
      break;
    case T_FUN_ANY:
      ret = "ANY";
      break;
    case T_FUN_ARBITRARY:
      ret = "ARBITRARY";
      break;
    case T_FUN_SYS_COUNT_INROW:
      ret = "SYS_COUNT_INROW";
      break;
    case T_FUN_AVG_WEIGHTED:
      ret = "AVGWEIGHTED";
    case T_FUN_WINDOW_FUNNEL:
      ret = "WINDOW_FUNNEL";
      break;
    default:
      break;
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_ITEM_TYPE_STR_H */
