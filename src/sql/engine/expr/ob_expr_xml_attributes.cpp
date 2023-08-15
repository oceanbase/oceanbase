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
 * This file is for func xmlattributes.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_xml_attributes.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h" // may need json for kv pairs
#ifdef OB_BUILD_ORACLE_XML
#include "sql/engine/expr/ob_expr_xml_func_helper.h"
#endif
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprXmlAttributes::ObExprXmlAttributes(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_XML_ATTRIBUTES, N_XML_ATTRIBUTES, MORE_THAN_TWO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprXmlAttributes::~ObExprXmlAttributes() {}

int ObExprXmlAttributes::calc_result_typeN(ObExprResType &type,
                                          ObExprResType *types_stack,
                                          int64_t param_num,
                                          ObExprTypeCtx &type_ctx)
                                          const
{
  UNUSED(type_ctx);
  INIT_SUCC(ret);
  if (OB_UNLIKELY(param_num < 3)) {
      ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid param number", K(ret), K(param_num));
  } else if (!ob_is_integer_type(types_stack[0].get_type())) {
      ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid escaping opt type", K(ret), K(types_stack[0].get_type()));
  } else if (!ob_is_integer_type(types_stack[1].get_type())) {
      ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid escapint opt type", K(ret), K(types_stack[1].get_type()));
  } else {
    for (int i = 2; i < param_num && OB_SUCC(ret); i++) {
      if (ob_is_string_type(types_stack[i].get_type())) {
        if (types_stack[i].get_collation_type() == CS_TYPE_BINARY) {
          ret = OB_ERR_INVALID_XML_DATATYPE;
          LOG_USER_ERROR(OB_ERR_INVALID_XML_DATATYPE, "-", "BLOB");
          LOG_WARN("invalid type, expect char", K(ret), K(types_stack[i].get_type()));
        } else if (ob_is_text_tc(types_stack[i].get_type())) {
          ret = OB_ERR_INVALID_XML_DATATYPE;
          LOG_USER_ERROR(OB_ERR_INVALID_XML_DATATYPE, "-", ob_obj_type_str(types_stack[i].get_type()));
          LOG_WARN("invalid type, expect char", K(ret), K(types_stack[i].get_type()));
        } else if (ob_is_nstring(types_stack[i].get_type())) {
          types_stack[i].set_calc_type(ObVarcharType);
          types_stack[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        } else if (types_stack[i].get_charset_type() != CHARSET_UTF8MB4) {
          types_stack[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        }
      } else if (types_stack[i].get_type() == ObUserDefinedSQLType || ObObjType::ObExtendType == types_stack[i].get_type()) {
        ret = OB_ERR_INVALID_XML_DATATYPE;
        LOG_USER_ERROR(OB_ERR_INVALID_XML_DATATYPE, "-", "ANYDATA");
        LOG_WARN("invalid type, expect char", K(ret), K(types_stack[i].get_type()));
      } else {
        types_stack[i].set_calc_type(ObVarcharType);
        types_stack[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
      }
    }
  }
  if (OB_SUCC(ret)) {
      type.set_json();
    type.set_length((ObAccuracy::DDL_DEFAULT_ACCURACY[ObJsonType]).get_length());
  }
  return ret;
}

#ifdef OB_BUILD_ORACLE_XML
int ObExprXmlAttributes::eval_xml_attributes(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
  int attributes_escape = 1;
  int has_schema_check = 0;
  ObDatum *datum = NULL;
  int param_num = expr.arg_cnt_;
  if (param_num < 3) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("attributes arg cnt invalid", K(ret), K(param_num));
  } else if (OB_FAIL(expr.args_[0]->eval(ctx, datum))) {
    LOG_WARN("get escape value failed", K(ret));
  } else if (FALSE_IT(attributes_escape = datum->get_int())) {
  } else if (OB_FAIL(expr.args_[1]->eval(ctx, datum))) {
    LOG_WARN("get schema check value failed", K(ret));
  } else if (FALSE_IT(has_schema_check = datum->get_int())) {
  } else {
    ObJsonArray j_arr(&tmp_allocator);
    ObIJsonBase *j_base = &j_arr;
    for (int i = 2; OB_SUCC(ret) && i < param_num; i++) {
      ObIJsonBase *j_val;
      ObStringBuffer content(&tmp_allocator);
      if (OB_FAIL(ObJsonExprHelper::get_json_val(expr, ctx, &tmp_allocator, i, j_val))) {
        LOG_WARN("failed: get_json_val failed", K(ret));
      } else if (0 == i % 2 && attributes_escape) { // TODO Subsequent considerations to pass attributes with element
        // content
        if ((j_val->json_type() != ObJsonNodeType::J_NULL) &&
            OB_FAIL(ObXmlParserUtils::escape_xml_text(ObString(j_val->get_data_length(), j_val->get_data()), content))) {
          LOG_WARN("escape xml attributes failed", K(ret), K(attributes_escape));
        } else {
          void *node = tmp_allocator.alloc(sizeof(ObJsonString));
          if (OB_ISNULL(node)){
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc jsonString failed", K(ret));
          } else {
            ObJsonString *jstr = new (node) ObJsonString(content.ptr(), content.length());
            if (OB_FAIL(j_base->array_append(jstr))) {
              LOG_WARN("json_keys array append failed", K(ret), K(content));
            }
          }
        }
      } else if (OB_FAIL(j_base->array_append(j_val))) {
        LOG_WARN("failed: json array append json value", K(ret));
      }
    }
    // set result(json bin)
    if (OB_SUCC(ret)) {
      ObString raw_bin;
      if (OB_FAIL(j_base->get_raw_binary(raw_bin, &tmp_allocator))) {
        LOG_WARN("failed: get json raw binary", K(ret));
      } else {
        uint64_t length = raw_bin.length();
        char *buf = expr.get_str_res_mem(ctx, length);
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed: alloc memory for json array result", K(ret), K(length));
        } else {
          MEMMOVE(buf, raw_bin.ptr(), length);
          res.set_string(buf, length);
        }
      }
    }
  }
  return ret;
}
#endif

int ObExprXmlAttributes::cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_xml_attributes;
  return OB_SUCCESS;
}

}
}