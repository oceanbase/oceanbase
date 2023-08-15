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
 */

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/expr/ob_expr_treat.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/object/ob_obj_cast.h"
#include "sql/engine/expr/ob_datum_cast.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/json_type/ob_json_parse.h"
#include "lib/json_type/ob_json_base.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"
#ifdef OB_BUILD_ORACLE_PL
#include "pl/sys_package/ob_json_pl_utils.h"
#include "pl/ob_pl_user_type.h"
#endif
namespace oceanbase
{
namespace sql
{

ObExprTreat::ObExprTreat(ObIAllocator &alloc)
    : ObFuncExprOperator::ObFuncExprOperator(alloc, T_FUN_SYS_TREAT,
                                             N_TREAT,
                                             2,
                                             VALID_FOR_GENERATED_COL,
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
#ifdef OB_BUILD_ORACLE_PL
  } else if(ob_is_extend(in_type) && pl::ObPlJsonUtil::is_pl_jsontype(type1.get_udt_id())
      && ob_is_extend(as_type) && pl::ObPlJsonUtil::is_pl_jsontype(type2.get_udt_id())){
    type.set_type(ObExtendType);
    type.set_udt_id(type2.get_udt_id());
#endif
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

#ifdef OB_BUILD_ORACLE_PL

static int treat_as_json_udt(const ObExpr &expr, ObEvalCtx &ctx, common::ObIAllocator &temp_allocator,
                                pl::ObPLOpaque *opaque, ObDatum &res) {
  INIT_SUCC(ret);
  ObJsonNode *json_doc = nullptr;
  pl::ObPLJsonBaseType *jsontype = nullptr;
  pl::ObPLJsonBaseType *new_jsontype = nullptr;
  ObObj res_obj;

  if(OB_ISNULL(jsontype = static_cast<pl::ObPLJsonBaseType*>(opaque))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cast to json type is null", K(ret), K(opaque));
  } else if(OB_ISNULL(json_doc = jsontype->get_data())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get json doc is null", K(ret), K(jsontype));
  } else {
    if (OB_FAIL(pl::ObPlJsonUtil::transform_JsonBase_2_PLJsonType(ctx.exec_ctx_, json_doc, new_jsontype))) {
      LOG_WARN("failed to transfrom ObJsonNode to ObPLJsonBaseType", K(ret));
    } else if(OB_ISNULL(new_jsontype)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to transfrom ObJsonNode to ObPLJsonBaseType", K(ret));
    } else {
      res_obj.set_extend(reinterpret_cast<int64_t>(new_jsontype), pl::PL_OPAQUE_TYPE);
      res.from_obj(res_obj);
    }
  }
  return ret;
}

#endif

int ObExprTreat::eval_treat(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res) {
  INIT_SUCC(ret);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  ObObjType in_type = expr.args_[0]->datum_meta_.type_;
  ObCollationType in_cs_type = expr.args_[0]->datum_meta_.cs_type_;
  ObObjType out_type = expr.datum_meta_.type_;
  ObCollationType out_cs_type = expr.datum_meta_.cs_type_;
  ObDatum *child_res = nullptr;

  ObString in_str;
  bool is_null = false;

  if (OB_FAIL(expr.args_[0]->eval(ctx, child_res))) {
    LOG_WARN("eval arg failed", K(ret), K(ctx));
  } else if (OB_ISNULL(child_res) || child_res->is_null()) {
    res.set_null();
  } else if (ob_is_extend(in_type)) {
  #ifdef OB_BUILD_ORACLE_PL
    pl::ObPLOpaque *opaque = reinterpret_cast<pl::ObPLOpaque *>(child_res->get_ext());
    if(OB_ISNULL(opaque)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not json type", K(ret), K(in_type), K(in_cs_type));
    } else if(opaque->is_json_type()) {
      // res = *child_res;
      if(OB_FAIL(treat_as_json_udt(expr, ctx, temp_allocator, opaque, res))) {
        LOG_WARN("as json udt fail", K(ret));
      }
    } else {
      ret = OB_ERR_INVALID_TYPE_FOR_OP;
      LOG_WARN("opaque type unexpected", K(ret), K(opaque->get_type()));
    }
  #else
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not surpport udt type", K(ret));
  #endif
  } else if (ob_is_raw(in_type)) {
    if(OB_FAIL(ObDatumHexUtils::rawtohex(expr, child_res->get_string(), ctx, res))) {
      LOG_WARN("fail raw to hex", K(ret), K(in_type), K(child_res->get_string()));
    }
  } else if (OB_FAIL(ObJsonExprHelper::get_json_or_str_data(expr.args_[0], ctx, temp_allocator, in_str, is_null))) {
    LOG_WARN("eval arg failed", K(ret));
  } else if (ob_is_string_type(in_type)) {
    if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(expr, ctx, res, in_str))) {
      LOG_WARN("fail to pack result", K(ret), K(in_str.length()));
    }
  } else {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("in type unexpected", K(ret), K(in_type), K(in_cs_type));
  }
  return ret;
}

} // sql
} // oceanbase
