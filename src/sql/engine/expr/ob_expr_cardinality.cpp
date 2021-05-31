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
#include "sql/engine/expr/ob_expr_cardinality.h"
#include "sql/parser/ob_item_type.h"
#include "share/object/ob_obj_cast.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "share/schema/ob_schema_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase {
namespace sql {}
}  // namespace oceanbase

ObExprCardinality::ObExprCardinality(ObIAllocator& alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_CARDINALITY, N_CARDINALITY, 1, NOT_ROW_DIMENSION)
{}

ObExprCardinality::~ObExprCardinality()
{}

int ObExprCardinality::assign(const ObExprOperator& other)
{
  int ret = OB_SUCCESS;
  if (other.get_type() != T_OP_SET) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr operator is mismatch", K(other.get_type()));
  } else if (OB_FAIL(ObExprOperator::assign(other))) {
    LOG_WARN("assign parent expr failed", K(ret));
  } else {
    const ObExprCardinality& other_expr = static_cast<const ObExprCardinality&>(other);
  }
  return ret;
}

int ObExprCardinality::calc_result1(ObObj& result, const ObObj& obj, ObExprCtx& expr_ctx) const
{
  UNUSED(expr_ctx);
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode() && obj.get_meta().is_ext()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported", K(result), K(obj));
  }
  return ret;
}

int ObExprCardinality::calc_result_type1(ObExprResType& type, ObExprResType& type1, ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (lib::is_oracle_mode()) {
    if (type1.is_ext()) {
      type.set_number();
      type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObNumberType].scale_);
      type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObNumberType].precision_);
      type.set_calc_type(common::ObNumberType);
    } else {
      ret = OB_ERR_WRONG_TYPE_FOR_VAR;
      LOG_WARN("PLS-00306: wrong number or types of arguments in call stmt", K(ret), K(type1));
    }
  } else {
    ret = OB_ERR_WRONG_TYPE_FOR_VAR;
    LOG_WARN("PLS-00306: wrong number or types of arguments in call stmt", K(ret), K(type1));
  }
  return ret;
}