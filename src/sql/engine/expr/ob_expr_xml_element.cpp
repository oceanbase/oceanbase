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
 * This file is for func xmlelement.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_xml_element.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_xml_func_helper.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprXmlElement::ObExprXmlElement(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_XML_ELEMENT, N_XML_ELEMENT, MORE_THAN_ZERO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprXmlElement::~ObExprXmlElement()
{
}

int ObExprXmlElement::calc_result_typeN(ObExprResType& type,
                                      ObExprResType* types_stack,
                                      int64_t param_num,
                                      ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(param_num < 3)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid param number", K(ret), K(param_num));
  } else if (!is_called_in_sql()) {
    ret = OB_ERR_SP_LILABEL_MISMATCH;
    LOG_WARN("expr call in pl semantics disallowed", K(ret), K(N_XML_ELEMENT));
    LOG_USER_ERROR(OB_ERR_SP_LILABEL_MISMATCH, static_cast<int>(strlen(N_XML_ELEMENT)), N_XML_ELEMENT);
  } else {
    // check opt_escaping
    if (!ob_is_integer_type(types_stack[0].get_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid escaping opt type", K(ret), K(types_stack[0].get_type()));
    } else {
      for (int i = 2; i < param_num && OB_SUCC(ret); i++) {
        const ObObjType obj_type = types_stack[i].get_type();
        if (i == 2) {
          if (ob_is_null(obj_type)) {
            // do nothing
          } else if (obj_type == ObNumberType) {
            // do nothing
          } else if (types_stack[i].get_collation_type() == CS_TYPE_BINARY) {
            ret = OB_ERR_INVALID_XML_DATATYPE;
            LOG_USER_ERROR(OB_ERR_INVALID_XML_DATATYPE, "Character", "-");
            LOG_WARN("Unsupport for string typee with binary charset input.", K(ret), K(obj_type));
          } else if (ob_is_string_tc(obj_type)) {
            types_stack[i].set_calc_type(ObVarcharType);
            types_stack[i].set_calc_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
          } else {
            ret = OB_ERR_INVALID_XML_DATATYPE;
            LOG_USER_ERROR(OB_ERR_INVALID_XML_DATATYPE, "Character", "-");
            LOG_WARN("Unsupport for string typee with binary charset input.", K(ret), K(obj_type));
          }
        } else if (i == 3 && ob_is_json(obj_type)) {
          // do nothing, result from xmlAttributes
        } else if (ob_is_string_type(obj_type)) {
          if (types_stack[i].get_collation_type() == CS_TYPE_BINARY) {
            types_stack[i].set_calc_collation_type(CS_TYPE_BINARY);
          } else if (types_stack[i].get_charset_type() != CHARSET_UTF8MB4) {
            types_stack[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
          }
        } else if (ObUserDefinedSQLType == types_stack[i].get_type()) {
          // xmltype, do noting
        } else if (ObExtendType == types_stack[i].get_type()) {
          types_stack[i].set_calc_type(ObUserDefinedSQLType);
        } else {
          types_stack[i].set_calc_type(ObVarcharType);
          types_stack[i].set_calc_collation_type(CS_TYPE_UTF8MB4_BIN);
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    type.set_sql_udt(ObXMLSqlType);
  }
  return ret;
}

int ObExprXmlElement::eval_xml_element(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  INIT_SUCC(ret);
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  uint64_t tenant_id = ObMultiModeExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session());
  MultimodeAlloctor tmp_allocator(tmp_alloc_g.get_allocator(), expr.type_, tenant_id, ret);
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(tenant_id, "XMLModule"));
  ObDatum *datum = NULL;
  int num_args = expr.arg_cnt_;
  ObString name_tag;
  int need_escape = 0;
  int is_name = 0;

  ObVector<ObObj, ElementObjCacheStatArena> value_vec;
  const ObIJsonBase *attr_json = NULL;
  ObString binary_str;
  ObString blob_locator;
  bool has_attribute = false;
  ObXmlDocument *res_doc = NULL;
  ObMulModeMemCtx* mem_ctx = nullptr;

  if (OB_ISNULL(ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get session failed.", K(ret));
  } else if (OB_FAIL(ObXmlUtil::create_mulmode_tree_context(&tmp_allocator, mem_ctx))) {
    LOG_WARN("fail to create tree memory context", K(ret));
  } else if (OB_UNLIKELY(num_args < 3)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid args num", K(ret), K(num_args));
  } else {
    expr.args_[0]->eval(ctx, datum);
    need_escape = datum->get_int();
    expr.args_[1]->eval(ctx, datum);
    is_name = datum->get_int();
    if (need_escape != 0 && need_escape != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid escaping opt", K(ret), K(need_escape));
    } else if (need_escape == 0 && is_name == 0) {
      ret = OB_ERR_XML_MISSING_COMMA;
      LOG_WARN("xml element param invalid", K(ret));
    } else if (OB_FAIL(tmp_allocator.eval_arg(expr.args_[2], ctx, datum))) {
      LOG_WARN("expr args 1 failed", K(ret), K(expr.args_[2]));
    } else if(expr.args_[2]->datum_meta_.type_ == ObNumberType) {
      ret = OB_ERR_INVALID_XML_DATATYPE;
      LOG_USER_ERROR(OB_ERR_INVALID_XML_DATATYPE, "Character", "-");
      LOG_WARN("Unsupport for string typee with binary charset input.", K(ret), K(expr.args_[2]->datum_meta_.type_));
    } else if (OB_FAIL(ObTextStringHelper::get_string(expr, tmp_allocator, 2, datum, name_tag))) {
      LOG_WARN("get xml plain text failed", K(ret));
    } else if (OB_FALSE_IT(tmp_allocator.add_baseline_size(name_tag.length()))) {
      LOG_WARN("failed to add base line size.", K(ret), K(name_tag));
    }
  }
  for (int i = 3; OB_SUCC(ret) && i < num_args && OB_SUCC(ret); i++) {
    ObObjType val_type = expr.args_[i]->datum_meta_.type_;
    ObItemType item_type = expr.args_[i]->type_;
    if (OB_FAIL(tmp_allocator.eval_arg(expr.args_[i], ctx, datum))) {
      LOG_WARN("expr args failed", K(ret), K(i));
    } else if (OB_FAIL(tmp_allocator.add_baseline_size(datum, val_type == ObUserDefinedSQLType || expr.args_[i]->obj_meta_.has_lob_header()))) {
      LOG_WARN("failed to add base line size.", K(ret));
    } else if (ob_is_json(val_type) && i == 3) {  // result from attribute
      has_attribute = true;
      void *buf = NULL;
      if (OB_ISNULL(buf = tmp_allocator.alloc(sizeof(ObJsonBin)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret), K(sizeof(ObJsonBin)));
      } else {
        ObJsonBin *j_bin = new (buf) ObJsonBin(datum->get_string().ptr(), datum->get_string().length(), &tmp_allocator);
        attr_json = static_cast<const ObIJsonBase*>(j_bin);
        if (OB_FAIL(j_bin->reset_iter())) {
          LOG_WARN("fail to reset iter", K(ret));
        } else if (attr_json->json_type() != ObJsonNodeType::J_ARRAY) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid json type", K(ret), K(attr_json->json_type()));
        }
      }
    } else if (OB_FAIL(ObXMLExprHelper::construct_element_value(tmp_allocator, expr.args_[i], datum, need_escape, value_vec))) {  // element value
      LOG_WARN("construct_element_valuev failed", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObXMLExprHelper::construct_doc(mem_ctx, name_tag, value_vec, attr_json, has_attribute, res_doc))) {
    LOG_WARN("construct_doc failed", K(ret));
  } else if (OB_ISNULL(res_doc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(" failed to pack result, as res_doc is nullptr", K(ret));
  } else if (OB_FAIL(res_doc->get_raw_binary(binary_str, &tmp_allocator))) {
    LOG_WARN("get raw binary failed", K(ret));
  } else if (OB_FALSE_IT(tmp_allocator.set_baseline_size(binary_str.length()))) {
  } else if (OB_FAIL(ObXMLExprHelper::pack_binary_res(expr, ctx, binary_str, blob_locator))) {
    LOG_WARN("pack binary res failed", K(ret), K(binary_str));
  } else {
    res.set_string(blob_locator.ptr(), blob_locator.length());
  }
  return ret;
}

int ObExprXmlElement::cg_expr(ObExprCGCtx &expr_cg_ctx,
                              const ObRawExpr &raw_expr,
                              ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_xml_element;
  return OB_SUCCESS;
}

}
}