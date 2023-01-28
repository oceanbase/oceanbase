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
 * This file contains implementation of treat.
 * Authors:
 *   aozeliu.azl@oceanbase.com
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_treat.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/json_type/ob_json_parse.h"
#include "lib/json_type/ob_json_base.h"
namespace oceanbase
{
namespace sql
{

ObExprTreat::ObExprTreat(ObIAllocator &alloc)
    : ObFuncExprOperator::ObFuncExprOperator(alloc, T_FUN_SYS_TREAT,
                                             N_TREAT,
                                             2,
                                             NOT_ROW_DIMENSION)
{
}

ObExprTreat::~ObExprTreat()
{
}


int ObExprTreat::calc_result_type2(ObExprResType &type,
                                  ObExprResType &type1,
                                  ObExprResType &type2,
                                  ObExprTypeCtx &type_ctx) const
{
  INIT_SUCC(ret);
  ObObjType in_type = type1.get_type();
  const ObObj &param = type2.get_param();
  ParseNode parse_node;
  parse_node.value_ = param.get_int();
  ObObjType as_type = static_cast<ObObjType>(parse_node.int16_values_[OB_NODE_CAST_TYPE_IDX]);
  ObCollationType as_cs_type = static_cast<ObCollationType>(parse_node.int16_values_[OB_NODE_CAST_COLL_IDX]);

  if((ob_is_string_type(in_type) || ob_is_raw(in_type)) && ob_is_json(as_type)) {
    as_cs_type = CS_TYPE_INVALID == as_cs_type ? type_ctx.get_coll_type() : as_cs_type;
    type.set_type(ObVarcharType);
    type.set_collation_type(as_cs_type);
    type.set_collation_level(CS_LEVEL_EXPLICIT);
    type.set_length(OB_MAX_SQL_LENGTH);
    type.set_length_semantics(LS_CHAR);
    type.set_calc_type(ObJsonType);
  } else if(ob_is_extend(as_type)){
    type.set_type(ObExtendType);
    type.set_udt_id(type2.get_udt_id());
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("target type not json", K(ret), K(type1), K(type2));
  }
  return ret;
}

int ObExprTreat::cg_expr(ObExprCGCtx &expr_cg_ctx,
                        const ObRawExpr &raw_expr,
                        ObExpr &rt_expr) const
{
  INIT_SUCC(ret);
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  if (lib::is_oracle_mode()) {
    rt_expr.eval_func_ = eval_treat;
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("treat expr only support on oracle mode", K(ret));
  }
  return ret;
}

static int treat_string_as_json(const ObExpr &expr, ObEvalCtx &ctx, common::ObIAllocator &temp_allocator,
                                const ObString &in_str, ObDatum &res) {
  INIT_SUCC(ret);
  // just copy because oracle does this
  uint64_t length = in_str.length();
  char *buf = expr.get_str_res_mem(ctx, length);
  if (OB_NOT_NULL(buf)) {
    MEMCPY(buf, in_str.ptr(), length);
    res.set_string(buf, length);
    ret = OB_SUCCESS;
  } else {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed: alloc memory for json object result.", K(ret), K(length));
  }
  return ret;
}


int ObExprTreat::eval_treat(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res) {
  INIT_SUCC(ret);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  ObObjType in_type = expr.args_[0]->datum_meta_.type_;
  ObCollationType in_cs_type = expr.args_[0]->datum_meta_.cs_type_;
  ObObjType out_type = expr.datum_meta_.type_;
  ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
  ObDatum *child_res = nullptr;

  if (OB_FAIL(expr.args_[0]->eval(ctx, child_res))) {
    LOG_WARN("eval arg failed", K(ret), K(ctx));
  } else if (OB_ISNULL(child_res) || child_res->is_null()) {
    res.set_null();
  } else {
    if(ob_is_string_type(in_type)) {
      const ObString &in_str = child_res->get_string();
      if (OB_FAIL(treat_string_as_json(expr, ctx, temp_allocator, in_str, res))) {
        LOG_WARN("fail to parse string as json tree", K(ret), K(in_type), K(in_str));
      }
    } else if(ob_is_raw(in_type)) {
      if(OB_FAIL(ObDatumHexUtils::rawtohex(expr, child_res->get_string(), ctx, res))) {
        LOG_WARN("fail raw to hex", K(ret), K(in_type), K(child_res->get_string()));
      }
    } else if (ob_is_extend(in_type)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not surpport udt type", K(ret));
    } else {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("in type unexpected", K(ret), K(in_type), K(in_cs_type));
    }
  }
  return ret;
}

} // sql
} // oceanbase
