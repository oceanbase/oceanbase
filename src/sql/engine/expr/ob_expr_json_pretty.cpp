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
 * This file is for implementation of func json_pretty
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_json_pretty.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{

ObExprJsonPretty::ObExprJsonPretty(ObIAllocator &alloc) 
      :ObFuncExprOperator(alloc,
      T_FUN_SYS_JSON_PRETTY,
      N_JSON_PRETTY, 
      1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprJsonPretty::~ObExprJsonPretty()
{
}

int ObExprJsonPretty::calc_result_type1(ObExprResType &type,
                                        ObExprResType &type1,
                                        common::ObExprTypeCtx &type_ctx) const
{
  UNUSED(type_ctx); // type_ctx session, collation, raw expr, oracle模式下可能需要从type_ctx来判断字符集
  INIT_SUCC(ret);

  type.set_type(ObLongTextType);
  type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  type.set_collation_level(CS_LEVEL_IMPLICIT);
  type.set_accuracy(ObAccuracy::DDL_DEFAULT_ACCURACY[ObLongTextType]);

  if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(type1, 1, N_JSON_PRETTY))) {
    LOG_WARN("wrong type for json doc.", K(ret), K(type1.get_type()));
  }

  return ret;
}

int ObExprJsonPretty::calc(ObEvalCtx &ctx, const ObDatum &data, ObDatumMeta meta, bool has_lob_header,
                           ObIAllocator *allocator, ObJsonBuffer &j_buf, bool &is_null)
{
  INIT_SUCC(ret);
  ObObjType type = meta.type_;
  ObCollationType cs_type = meta.cs_type_;
  ObIJsonBase *j_base = NULL;
  ObJsonInType j_in_type = ObJsonExprHelper::get_json_internal_type(type);
  common::ObString j_str = data.get_string(); // json text or json binary

  if (OB_ISNULL(allocator)) { // check allocator
    ret = OB_NOT_INIT;
    LOG_WARN("allcator is null", K(ret));
  } else if (data.is_null() || type == ObNullType) {
    is_null = true;
  } else if (type != ObJsonType && !ob_is_string_type(type)) {
    ret = OB_ERR_INVALID_TYPE_FOR_JSON;
    LOG_USER_ERROR(OB_ERR_INVALID_TYPE_FOR_JSON, 1, "json_pretty");
  } else if (OB_FAIL(ObJsonExprHelper::ensure_collation(type, cs_type))) {
    LOG_WARN("fail to ensure collation", K(ret), K(type), K(cs_type));
  } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(*allocator, data, meta, has_lob_header, j_str))) {
    LOG_WARN("fail to get real data.", K(ret), K(j_str));
  } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(allocator, j_str, j_in_type,
      j_in_type, j_base))) {
    if (ret == OB_ERR_INVALID_JSON_TEXT) {
      ret = OB_ERR_INVALID_JSON_TEXT_IN_PARAM;
    }
    LOG_WARN("fail to get json base", K(ret), K(type), K(j_str), K(j_in_type));
  } else if (OB_FAIL(j_base->print(j_buf, true, true, 0))) {
    LOG_WARN("fail to print json", K(ret), K(type), K(j_str), K(j_in_type));
  }

  return ret;
}

// for new sql engine
int ObExprJsonPretty::eval_json_pretty(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  ObExpr *arg = expr.args_[0];
  ObDatum *j_datum = NULL;
  ObJsonBuffer j_buf(&tmp_allocator);
  bool is_null = false;

  if (OB_FAIL(arg->eval(ctx, j_datum))) {
    ret = OB_ERR_INVALID_DATATYPE;
    LOG_WARN("error, eval json args datum failed", K(ret));
  } else if (OB_FAIL(calc(ctx, *j_datum, arg->datum_meta_, arg->obj_meta_.has_lob_header(), &tmp_allocator, j_buf, is_null))) {
    LOG_WARN("fail to calc json pretty result", K(ret), K(j_datum), K(arg->datum_meta_));
  } else if (is_null) {
    res.set_null();
  } else if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(expr, ctx, res, j_buf))) {
    LOG_WARN("fail to pack json result", K(ret));
  }

  return ret;
}

int ObExprJsonPretty::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_json_pretty;
  return OB_SUCCESS;
}

}
}
