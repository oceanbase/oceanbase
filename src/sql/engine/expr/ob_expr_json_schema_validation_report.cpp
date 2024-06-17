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
#include "ob_expr_json_schema_validation_report.h"
#include "ob_expr_json_func_helper.h"

namespace oceanbase
{
using namespace common;
namespace sql
{
class ObJsonSchemaReportItem
{
public:
  static constexpr char* RESULT = const_cast<char*>("valid");
  static constexpr char* REASON = const_cast<char*>("reason");
  static constexpr char* REASON_BEGIN = const_cast<char*>("The JSON document location '");
  static constexpr char* REASON_MID = const_cast<char*>("' failed requirement '");
  static constexpr char* REASON_END = const_cast<char*>("' at JSON Schema location '");
  static constexpr char* SCHEMA_LOCATION = const_cast<char*>("schema-location");
  static constexpr char* DOC_LOCATION = const_cast<char*>("document-location");
  static constexpr char* FAILED_KEYWORD = const_cast<char*>("schema-failed-keyword");
};

OB_SERIALIZE_MEMBER((ObExprJsonSchemaValidationReport, ObFuncExprOperator), json_schema_);

ObExprJsonSchemaValidationReport::ObExprJsonSchemaValidationReport(common::ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_JSON_SCHEMA_VALIDATION_REPORT, N_JSON_SCHEMA_VALIDATION_REPORT, OB_JSON_SCHEMA_EXPR_ARG_NUM, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION),
      json_schema_(ObString::make_empty_string()) {}

ObExprJsonSchemaValidationReport::~ObExprJsonSchemaValidationReport()
{
}

int ObExprJsonSchemaValidationReport::calc_result_type2(ObExprResType &type,
                                                        ObExprResType &type1,
                                                        ObExprResType &type2,
                                                        ObExprTypeCtx &type_ctx) const
{
  INIT_SUCC(ret);
  UNUSED(type_ctx);

  // set the result type: json
  type.set_json();
  type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType]).get_length());

  // 1st param is json schema (also json doc)
  // 2nd param is json schema (also json doc)
  if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(type1, 1, N_JSON_SCHEMA_VALID))) {
    LOG_WARN("wrong type for json doc.", K(ret), K(type1.get_type()));
  } else if (OB_FAIL(ObJsonExprHelper::is_valid_for_json(type2, 2, N_JSON_SCHEMA_VALID))) {
    LOG_WARN("wrong type for json doc.", K(ret), K(type2.get_type()));
  }

  return ret;
}

int ObExprJsonSchemaValidationReport::cg_expr(ObExprCGCtx &op_cg_ctx,
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
                  = OB_NEWx(ObExprJsonSchemaValidInfo, (&alloc), alloc, T_FUN_SYS_JSON_SCHEMA_VALIDATION_REPORT);
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
    rt_expr.eval_func_ = eval_json_schema_validation_report;
  }
  return ret;
}

int ObExprJsonSchemaValidationReport::eval_json_schema_validation_report(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
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
    ObIJsonBase* validation_report = nullptr;
    bool is_valid = false;
    if (OB_FAIL(validator.schema_validator(j_doc, is_valid))) {
      LOG_WARN("failed in validator", K(ret));
    } else if (OB_FAIL(raise_validation_report(temp_allocator, validator, is_valid, validation_report))){
      LOG_WARN("failed to raise validation report", K(ret));
    } else {
      ObString raw_bin;
      if (OB_FAIL(validation_report->get_raw_binary(raw_bin, &temp_allocator))) {
        LOG_WARN("failed: json get binary", K(ret));
      } else if (OB_FAIL(ObJsonExprHelper::pack_json_str_res(expr, ctx, res, raw_bin))) {
        LOG_WARN("fail to pack json result", K(ret));
      }
    }
  }
  return ret;
}

int ObExprJsonSchemaValidationReport::raise_validation_report(ObIAllocator &allocator,
                                                              ObJsonSchemaValidator& validator,
                                                              const bool& is_valid,
                                                              ObIJsonBase*& validation_report)
{
  INIT_SUCC(ret);
  ObJsonObject* report_obj = nullptr;
  ObJsonBoolean* schema_result = nullptr;
  if (OB_ISNULL(report_obj = OB_NEWx(ObJsonObject, &allocator, &allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to init schema report.", K(ret));
  } else if (OB_FALSE_IT(validation_report = report_obj)) {
  } else if (OB_ISNULL(schema_result = OB_NEWx(ObJsonBoolean, &allocator, is_valid))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to init schema report result.", K(ret));
  } else if (OB_FAIL(report_obj->add(ObJsonSchemaReportItem::RESULT, schema_result, false, true, false))) {
    LOG_WARN("fail to add schema result.", K(ret));
  } else if (!is_valid) { // if not valid, need to describe the reason in detail
    // reason
    ObJsonBuffer reason(&allocator);
    ObJsonBuffer json_pointer(&allocator);
    ObJsonBuffer schema_pointer(&allocator);
    ObJsonString* reason_str = nullptr;
    ObJsonString* schema_loc_str = nullptr;
    ObJsonString* doc_loc_str = nullptr;
    ObJsonString* failed_keyword_str = nullptr;
    if (OB_FAIL(validator.get_json_or_schema_point(json_pointer, false))) {
      LOG_WARN("fail to get json pointer.", K(ret));
    } else if (OB_FAIL(validator.get_json_or_schema_point(schema_pointer, true))) {
      LOG_WARN("fail to get schema pointer.", K(ret));
    } else if (OB_FAIL(reason.append(ObJsonSchemaReportItem::REASON_BEGIN))
             || OB_FAIL(reason.append(json_pointer.ptr(), json_pointer.length()))
             || OB_FAIL(reason.append(ObJsonSchemaReportItem::REASON_MID))
             || OB_FAIL(reason.append(validator.get_failed_keyword()))
             || OB_FAIL(reason.append(ObJsonSchemaReportItem::REASON_END))
             || OB_FAIL(reason.append(schema_pointer.ptr(), schema_pointer.length()))) {
      LOG_WARN("fail to get reason.", K(ret));
    } else if (OB_ISNULL(reason_str = OB_NEWx(ObJsonString, &allocator, reason.ptr(), reason.length()))
            || OB_ISNULL(schema_loc_str = OB_NEWx(ObJsonString, &allocator, schema_pointer.ptr(), schema_pointer.length()))
            || OB_ISNULL(doc_loc_str = OB_NEWx(ObJsonString, &allocator, json_pointer.ptr(), json_pointer.length()))
            || OB_ISNULL(failed_keyword_str = OB_NEWx(ObJsonString, &allocator, validator.get_failed_keyword()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to init schema report value.", K(ret));
    } else if (OB_FAIL(report_obj->add(ObJsonSchemaReportItem::REASON, reason_str, false, true, false))) {
      LOG_WARN("fail to add reason.", K(ret));
    } else if (OB_FAIL(report_obj->add(ObJsonSchemaReportItem::SCHEMA_LOCATION, schema_loc_str, false, true, false))) {
      LOG_WARN("fail to add schema location.", K(ret));
    } else if (OB_FAIL(report_obj->add(ObJsonSchemaReportItem::DOC_LOCATION, doc_loc_str, false, true, false))) {
      LOG_WARN("fail to add document location.", K(ret));
    } else if (OB_FAIL(report_obj->add(ObJsonSchemaReportItem::FAILED_KEYWORD, failed_keyword_str, false, true, false))) {
      LOG_WARN("fail to add document location.", K(ret));
    }
  }
  return ret;
}

} /* sql */
} /* oceanbase */
