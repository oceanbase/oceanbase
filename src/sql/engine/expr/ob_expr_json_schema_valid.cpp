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

#include "ob_expr_json_schema_valid.h"
#include "ob_expr_json_func_helper.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER((ObExprJsonSchemaValid, ObFuncExprOperator), json_schema_);

ObExprJsonSchemaValid::ObExprJsonSchemaValid(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_JSON_SCHEMA_VALID, N_JSON_SCHEMA_VALID, OB_JSON_SCHEMA_EXPR_ARG_NUM, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION),
      json_schema_(ObString::make_empty_string()) {}

ObExprJsonSchemaValid::~ObExprJsonSchemaValid()
{
}

int ObExprJsonSchemaValid::calc_result_type2(ObExprResType &type,
                                             ObExprResType &type1,
                                             ObExprResType &type2,
                                             ObExprTypeCtx &type_ctx) const
{
  INIT_SUCC(ret);
  UNUSED(type_ctx);

  // set the result type to bool
  type.set_int32();
  type.set_precision(DEFAULT_PRECISION_FOR_BOOL);
  type.set_scale(ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType].scale_);

  // 1st param is json schema (also json doc)
  // 2nd param is json schema (also json doc)
  if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(type1, 1, N_JSON_SCHEMA_VALID))) {
    LOG_WARN("wrong type for json doc.", K(ret), K(type1.get_type()));
  } else if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(type2, 2, N_JSON_SCHEMA_VALID))) {
    LOG_WARN("wrong type for json doc.", K(ret), K(type2.get_type()));
  }

  return ret;
}

int ObExprJsonSchemaValid::cg_expr(ObExprCGCtx &op_cg_ctx,
                                   const ObRawExpr &raw_expr,
                                   ObExpr &rt_expr) const
{
  INIT_SUCC(ret);
  const ObRawExpr *schema = raw_expr.get_param_expr(0);
  if (lib::is_mysql_mode() && OB_JSON_SCHEMA_EXPR_ARG_NUM == rt_expr.arg_cnt_
     && OB_NOT_NULL(schema) && (schema->is_const_expr() || schema->is_static_scalar_const_expr())
     && schema->get_expr_type() != T_OP_GET_USER_VAR) {
    ObIAllocator &alloc = *op_cg_ctx.allocator_;
    ObExprJsonSchemaValidInfo *info
                  = OB_NEWx(ObExprJsonSchemaValidInfo, (&alloc), alloc, T_FUN_SYS_JSON_SCHEMA_VALID);
    bool got_data = false;
    if (OB_ISNULL(info)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else if (OB_FAIL(info->init_json_schema_extra_info(alloc, op_cg_ctx, schema, got_data))) {
      LOG_WARN("allocate memory failed", K(ret));
    } else if (got_data) {
      rt_expr.extra_info_ = info;
    }
  }

  if (OB_SUCC(ret)) {
    rt_expr.eval_func_ = eval_json_schema_valid;
  }
  return ret;
}

int ObExprJsonSchemaValid::eval_json_schema_valid(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  const ObExprJsonSchemaValidInfo *info
                  = static_cast<ObExprJsonSchemaValidInfo *>(expr.extra_info_);
  ObIJsonBase* j_schema = nullptr;
  ObIJsonBase* j_doc = nullptr;
  bool is_null_result = false;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &temp_allocator = tmp_alloc_g.get_allocator();
  ObJsonBin j_schema_bin;
  if (OB_ISNULL(info)) {
    // schema is not const
    if (OB_FAIL(ObJsonExprHelper::get_json_schema(expr, ctx, temp_allocator, 0,
                                                  j_schema, is_null_result))) {
      LOG_WARN("get_json_doc failed", K(ret));
    }
  } else {
    // schema is const
    new (&j_schema_bin) ObJsonBin(info->json_schema_.ptr(), info->json_schema_.length(), &temp_allocator);
    if (OB_FAIL(j_schema_bin.reset_iter())) {
      LOG_WARN("fail to reset iter for new json bin", K(ret));
    } else {
      // schema validation only seek, do not need reserve parent stack
      j_schema_bin.set_seek_flag(true);
      j_schema = &j_schema_bin;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!is_null_result && OB_FAIL(ObJsonExprHelper::get_json_doc(expr, ctx, temp_allocator, 1,
                                             j_doc, is_null_result, false, false, true))) {
    LOG_WARN("get_json_doc failed", K(ret));
  } else if (is_null_result) {
    res.set_null();
  } else {
    ObJsonSchemaValidator validator(&temp_allocator, j_schema);
    bool is_valid = false;
    if (OB_FAIL(validator.schema_validator(j_doc, is_valid))) {
      LOG_WARN("failed in validator", K(ret));
    } else {
      res.set_int(static_cast<int64_t>(is_valid));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObExprJsonSchemaValidInfo)
{
  INIT_SUCC(ret);
  LST_DO_CODE(OB_UNIS_ENCODE, json_schema_);
  return ret;
}

OB_DEF_DESERIALIZE(ObExprJsonSchemaValidInfo)
{
  INIT_SUCC(ret);
  LST_DO_CODE(OB_UNIS_DECODE, json_schema_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObExprJsonSchemaValidInfo)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, json_schema_);
  return len;
}

int ObExprJsonSchemaValidInfo::init_json_schema_extra_info(ObIAllocator &alloc,
                                                          ObExprCGCtx &op_cg_ctx,
                                                          const ObRawExpr* schema,
                                                          bool& got_data)
{
  INIT_SUCC(ret);
  ObExecContext *exec_ctx = op_cg_ctx.session_->get_cur_exec_ctx();
  got_data = false;
  ObObj const_data;
  ObIJsonBase* j_schema = nullptr;
  if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(exec_ctx,
                                                        schema,
                                                        const_data,
                                                        got_data,
                                                        alloc))) {
    LOG_WARN("failed to calc offset expr", K(ret));
  } else if (!got_data || const_data.is_null()) {
    got_data = false;
  } else if (OB_FAIL(ObJsonExprHelper::get_const_json_schema(const_data, N_JSON_SCHEMA_VALID, &alloc, j_schema))) {
    LOG_WARN("parse json schema failed", K(ret));
  } else if (OB_FAIL(j_schema->get_raw_binary(json_schema_, &alloc))){
    LOG_WARN("fail to get binary string", K(ret));
  } else {
    got_data = true;
  }
  return ret;
}

int ObExprJsonSchemaValidInfo::deep_copy(common::ObIAllocator &allocator,
                                         const ObExprOperatorType type,
                                         ObIExprExtraInfo *&copied_info) const
{
  INIT_SUCC(ret);
  if (OB_FAIL(ObExprExtraInfoFactory::alloc(allocator, type, copied_info))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    ObExprJsonSchemaValidInfo &other = *static_cast<ObExprJsonSchemaValidInfo *>(copied_info);
    if (OB_FAIL(ob_write_string(allocator, json_schema_, other.json_schema_, true))) {
      LOG_WARN("fail to copy string", K(ret));
    }
  }
  return ret;
}



} /* sql */
} /* oceanbase */
