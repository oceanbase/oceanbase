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
#include "sql/engine/expr/ob_expr_autoinc_nextval.h"
#include "sql/engine/expr/ob_expr_calc_partition_id.h"
#include "sql/engine/expr/ob_expr_type_to_str.h"
#include "sql/engine/expr/ob_expr_dll_udf.h"
#include "sql/engine/expr/ob_expr_collection_construct.h"
#include "sql/engine/expr/ob_expr_obj_access.h"
#include "sql/engine/expr/ob_expr_subquery_ref.h"
#include "sql/engine/expr/ob_expr_pl_get_cursor_attr.h"
#include "sql/engine/expr/ob_expr_pl_integer_checker.h"
#include "sql/engine/expr/ob_expr_udf.h"
#include "sql/engine/expr/ob_expr_object_construct.h"
#include "sql/engine/expr/ob_expr_multiset.h"
#include "sql/engine/expr/ob_expr_coll_pred.h"
#include "sql/engine/expr/ob_expr_output_pack.h"
#include "sql/engine/expr/ob_expr_plsql_variable.h"
#include "sql/engine/expr/ob_pl_expr_subquery.h"
#include "sql/engine/expr/ob_expr_cast.h"
#include "sql/engine/expr/ob_expr_lrpad.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

#define REG_EXTRA_INFO(type, ExtraInfoClass)      \
  do {                                            \
    static_assert(type > T_INVALID && type < T_MAX_OP, "invalid expr type for extra info"); \
    ALLOC_FUNS_[type] = ObExprExtraInfoFactory::alloc<ExtraInfoClass>; \
  } while(0)

ObExprExtraInfoFactory::AllocExtraInfoFunc ObExprExtraInfoFactory::ALLOC_FUNS_[T_MAX_OP] = { };

int ObExprExtraInfoFactory::alloc(common::ObIAllocator &alloc,
                                  const ObExprOperatorType &type,
                                  ObIExprExtraInfo *&extra_info)
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

  // 添加ObExpr extra info的结构, 需要在这里进行注册
  REG_EXTRA_INFO(T_FUN_SYS_CALC_PARTITION_ID, CalcPartitionBaseInfo);
  REG_EXTRA_INFO(T_FUN_ENUM_TO_STR, ObEnumSetInfo);
  REG_EXTRA_INFO(T_FUN_SET_TO_STR, ObEnumSetInfo);
  REG_EXTRA_INFO(T_FUN_ENUM_TO_INNER_TYPE, ObEnumSetInfo);
  REG_EXTRA_INFO(T_FUN_SET_TO_INNER_TYPE, ObEnumSetInfo);
  REG_EXTRA_INFO(T_FUN_COLUMN_CONV, ObEnumSetInfo);
  REG_EXTRA_INFO(T_FUN_NORMAL_UDF, ObNormalDllUdfInfo);
  REG_EXTRA_INFO(T_FUN_PL_COLLECTION_CONSTRUCT, ObExprCollectionConstruct::ExtraInfo);
  REG_EXTRA_INFO(T_OBJ_ACCESS_REF, ObExprObjAccess::ExtraInfo);
  REG_EXTRA_INFO(T_REF_QUERY, ObExprSubQueryRef::ExtraInfo);
  REG_EXTRA_INFO(T_FUN_PL_GET_CURSOR_ATTR, ObExprPLGetCursorAttr::ExtraInfo);
  REG_EXTRA_INFO(T_FUN_PL_INTEGER_CHECKER, ObExprPLIntegerChecker::ExtraInfo);
  REG_EXTRA_INFO(T_FUN_UDF, ObExprUDFInfo);
  REG_EXTRA_INFO(T_FUN_PL_OBJECT_CONSTRUCT, ObExprObjectConstructInfo);
  REG_EXTRA_INFO(T_OP_MULTISET, ObExprMultiSetInfo);
  REG_EXTRA_INFO(T_OP_COLL_PRED, ObExprCollPredInfo);
  REG_EXTRA_INFO(T_OP_OUTPUT_PACK, ObOutputPackInfo);
  REG_EXTRA_INFO(T_FUN_PLSQL_VARIABLE, ObPLSQLVariableInfo);
  REG_EXTRA_INFO(T_FUN_SUBQUERY, ObExprPlSubQueryInfo);
  REG_EXTRA_INFO(T_FUN_SYS_AUTOINC_NEXTVAL, ObAutoincNextvalInfo);
  REG_EXTRA_INFO(T_FUN_SYS_CALC_TABLET_ID, CalcPartitionBaseInfo);
  REG_EXTRA_INFO(T_FUN_SYS_CALC_PARTITION_TABLET_ID, CalcPartitionBaseInfo);
  REG_EXTRA_INFO(T_FUN_SYS_LEAST, ObExprOperator::DatumCastExtraInfo);
  REG_EXTRA_INFO(T_FUN_SYS_GREATEST, ObExprOperator::DatumCastExtraInfo);
  REG_EXTRA_INFO(T_FUN_SYS_NULLIF, ObExprOperator::DatumCastExtraInfo);
  REG_EXTRA_INFO(T_FUN_SYS_CAST, ObExprCast::CastMultisetExtraInfo);
  REG_EXTRA_INFO(T_FUN_SYS_LPAD, ObExprOracleLRpadInfo);
  REG_EXTRA_INFO(T_FUN_SYS_RPAD, ObExprOracleLRpadInfo);
}

} // end namespace sql
} // end namespace oceanbase
