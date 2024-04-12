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
#include "lib/xml/ob_xml_util.h"
#include "sql/engine/expr/ob_expr_xml_func_helper.h"

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
  common::ObArenaAllocator &tmp_allocator = tmp_alloc_g.get_allocator();
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
  ObXmlElement *element = NULL;
  ObXmlDocument *res_doc = NULL;
  ObMulModeMemCtx* mem_ctx = nullptr;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(ObXMLExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session()), "XMLModule"));

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
    } else if (OB_FAIL(expr.args_[2]->eval(ctx, datum))) {
      LOG_WARN("expr args 1 failed", K(ret), K(expr.args_[2]));
    } else if(expr.args_[2]->datum_meta_.type_ == ObNumberType) {
      ret = OB_ERR_INVALID_XML_DATATYPE;
      LOG_USER_ERROR(OB_ERR_INVALID_XML_DATATYPE, "Character", "-");
      LOG_WARN("Unsupport for string typee with binary charset input.", K(ret), K(expr.args_[2]->datum_meta_.type_));
    } else if (OB_FAIL(ObTextStringHelper::get_string(expr, tmp_allocator, 2, datum, name_tag))) {
      LOG_WARN("get xml plain text failed", K(ret));
    }
  }
  for (int i = 3; OB_SUCC(ret) && i < num_args && OB_SUCC(ret); i++) {
    ObObjType val_type = expr.args_[i]->datum_meta_.type_;
    ObItemType item_type = expr.args_[i]->type_;
    if (OB_FAIL(expr.args_[i]->eval(ctx, datum))) {
      LOG_WARN("expr args failed", K(ret), K(i));
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
    } else {  // element value
      ObString xml_value_data = datum->get_string();
      ObExpr *xml_arg = expr.args_[i];
      bool validity = false;
      if (val_type == ObUserDefinedSQLType) { // xmltype
        if (OB_FAIL(ObTextStringHelper::read_real_string_data(&tmp_allocator,
                                                              ObObjType::ObLongTextType,
                                                              ObCollationType::CS_TYPE_BINARY,
                                                              true, xml_value_data))) {
          LOG_WARN("fail to get real data.", K(ret), K(xml_value_data));
        // } else if (OB_FAIL(ObXMLExprHelper::check_xml_document_unparsed(mem_ctx, xml_value_data, validity))) {
        //   LOG_WARN("check document unparsed failed", K(ret), K(xml_value_data));
        // } else if (!validity) {
        //   ret = OB_ERR_XML_PARSE;
        //   LOG_WARN("input a unparsed document and parsing failed", K(ret), K(i), K(xml_value_data));
        } else {
          ObObj temp_value;
          temp_value.set_string(ObUserDefinedSQLType, xml_value_data);
          if (OB_FAIL(value_vec.push_back(temp_value))) {
            LOG_WARN("failed to push back temp value.", K(ret), K(temp_value));
          }
        }
      } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(tmp_allocator, *datum,
                                                              xml_arg->datum_meta_,
                                                              xml_arg->obj_meta_.has_lob_header(),
                                                              xml_value_data))) {
        LOG_WARN("fail to get real data.", K(ret), K(xml_value_data));
      } else if (expr.args_[i]->datum_meta_.cs_type_ == CS_TYPE_BINARY) { // binary
        const ObObjMeta obj_meta = expr.args_[i]->obj_meta_;
        ObObj obj;
        ObObj tmp_result;
        ObCastCtx cast_ctx(&tmp_allocator, NULL, CM_NONE, CS_TYPE_INVALID);
        obj.set_string(ObVarcharType, xml_value_data);
        if (OB_FAIL(ObHexUtils::rawtohex(obj, cast_ctx, tmp_result))) {
          LOG_WARN("fail to check xml binary syntax", K(ret));
        } else if (OB_FAIL(construct_value_array(tmp_allocator,
                                                  tmp_result.get_string(),
                                                  value_vec))) {
          LOG_WARN("construct value array failed", K(ret));
        }
      } else {  // varchar
        if (need_escape) {
          ObStringBuffer *escape_value = nullptr;
          if (OB_ISNULL(escape_value = OB_NEWx(ObStringBuffer, &tmp_allocator, (&tmp_allocator)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to allocate buffer", K(ret));
          } else if (OB_FAIL(ObXmlParserUtils::escape_xml_text(xml_value_data, *escape_value))) {
            LOG_WARN("escape xml value failed", K(ret), K(need_escape));
          } else if (OB_FAIL(construct_value_array(tmp_allocator,
                                                    ObString(escape_value->length(), escape_value->ptr()),
                                                    value_vec))) {
            LOG_WARN("construct value array failed", K(ret));
          }
        } else if (OB_FAIL(construct_value_array(tmp_allocator,
                                                  xml_value_data,
                                                  value_vec))) {
          LOG_WARN("construct value array failed", K(ret));
        }
      }
    }
  }

  ObStringBuffer tmp_buff(&tmp_allocator);
  ObString text_xml;
  ObXmlDocument *xml_doc = nullptr;

  bool tag_validity = false;
  bool doc_validity = true;
  ObXmlText *tag_name_start;
  ObXmlText *tag_name_end;
  ObString start_tag("<>");
  ObString end_tag("</>");
  if (OB_FAIL(ret)) {
  } else if (!has_attribute && name_tag.empty()) {
    if (OB_ISNULL(tag_name_start = OB_NEWx(ObXmlText, mem_ctx->allocator_, ObMulModeNodeType::M_TEXT, mem_ctx))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("new xml text failed", K(ret));
    } else if (OB_FALSE_IT(tag_name_start->set_text(start_tag))) {
    } else if (OB_ISNULL(tag_name_end = OB_NEWx(ObXmlText, mem_ctx->allocator_, ObMulModeNodeType::M_TEXT, mem_ctx))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("new xml text failed", K(ret));
    } else if (OB_FALSE_IT(tag_name_end->set_text(end_tag))) {
    }
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("ret failed", K(ret));
  } else if (OB_ISNULL(element = OB_NEWx(ObXmlElement, (mem_ctx->allocator_), ObMulModeNodeType::M_ELEMENT, mem_ctx))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate mem failed", K(ret));
  } else if (!has_attribute && name_tag.empty() && OB_FAIL(element->add_element(tag_name_start))) {
    LOG_WARN("element add start element failed", K(ret));
  } else if (OB_FAIL(construct_element(mem_ctx, name_tag, value_vec, attr_json, element, tag_validity))) {
    LOG_WARN("construct_element failed", K(ret));
  } else if (!has_attribute && name_tag.empty() && OB_FAIL(element->add_element(tag_name_end))) {
    LOG_WARN("element add end element failed", K(ret));
  } else if (OB_ISNULL(res_doc = OB_NEWx(ObXmlDocument, (mem_ctx->allocator_), ObMulModeNodeType::M_UNPARSED, mem_ctx))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate mem failed", K(ret));
  } else if (OB_FAIL(res_doc->add_element(element))) {
    LOG_WARN("res doc add element failed", K(ret));
  } else if (tag_validity && element->get_unparse() && OB_FAIL(ObXMLExprHelper::check_doc_validity(mem_ctx, res_doc, doc_validity))) { // && element.get_unparse()
    LOG_WARN("check doc validity failed", K(ret), K(tag_validity));
  } else if ((!tag_validity || !doc_validity)) {
    res_doc->set_xml_type(ObMulModeNodeType::M_UNPARSED);
  } else if (tag_validity && doc_validity) {
    static_cast<ObXmlElement*>(res_doc->at(0))->set_unparse(0);
    res_doc->set_xml_type(ObMulModeNodeType::M_DOCUMENT);
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(res_doc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(" failed to pack result, as res_doc is nullptr", K(ret));
  } else if (OB_FAIL(res_doc->get_raw_binary(binary_str, &tmp_allocator))) {
    LOG_WARN("get raw binary failed", K(ret));
  } else if (OB_FAIL(ObXMLExprHelper::pack_binary_res(expr, ctx, binary_str, blob_locator))) {
    LOG_WARN("pack binary res failed", K(ret), K(binary_str));
  } else {
    res.set_string(blob_locator.ptr(), blob_locator.length());
  }
  return ret;
}

int ObExprXmlElement::construct_value_array(ObIAllocator &allocator, const ObString &value, ObVector<ObObj, ElementObjCacheStatArena> &res_value)
{
  INIT_SUCC(ret);
  if (value.empty()) {
    // donothing
  } else {
    ObObj temp_value;
    uint32_t vec_size = res_value.size();
    if (res_value.size() == 0) {
      temp_value.set_string(ObVarcharType, value);
    } else {
      if (res_value[vec_size - 1].get_type() == ObUserDefinedSQLType) {
        temp_value.set_string(ObVarcharType, value);
      } else {
        char *new_value = NULL;
        ObString temp_from = res_value[vec_size - 1].get_string();
        if (OB_ISNULL(new_value = static_cast<char *>(allocator.alloc(temp_from.length() + value.length())))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("new value allocator failed", K(ret));
        } else if (OB_FAIL(res_value.remove(res_value.last()))) {
          LOG_WARN("res value remove failed", K(ret));
        } else {
          MEMCPY(new_value, temp_from.ptr(), temp_from.length());
          MEMCPY(new_value + temp_from.length(), value.ptr(), value.length());
          temp_value.set_string(ObVarcharType, new_value, temp_from.length() + value.length());
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(res_value.push_back(temp_value))) {
      LOG_WARN("failed to push back value.", K(ret), K(temp_value));
    }
  }
  return ret;
}
int ObExprXmlElement::construct_attribute(ObMulModeMemCtx* mem_ctx, const ObIJsonBase *attr, ObXmlElement *&element)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(attr)) {  // do nothing
  } else if (attr->json_type() != ObJsonNodeType::J_ARRAY) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid json type", K(ret), K(attr->json_type()));
  } else if (attr->element_count() % 2 != 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("attribute element count invalid", K(ret), K(attr->element_count()));
  } else {
    for (uint64_t i = 0; OB_SUCC(ret) && i < attr->element_count(); i += 2) {
      ObIJsonBase *jb_name = NULL;
      ObIJsonBase *jb_value = NULL;
      ObString value_str = NULL;
      ObString key_str = NULL;
      ObXmlAttribute *attribute = NULL;
      if (OB_FAIL(attr->get_array_element(i, jb_value))) {
        LOG_WARN("get attribute value failed", K(ret), K(i));
      } else if (OB_FAIL(attr->get_array_element(i + 1, jb_name))) {
        LOG_WARN("get attribute name failed", K(ret), K(i));
      } else if (OB_ISNULL(jb_name) || OB_ISNULL(jb_value)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("attribute name or value is null", K(ret), K(i), K(jb_name), K(jb_value));
      } else if (jb_name->json_type() == ObJsonNodeType::J_NULL ||
                    jb_value->json_type() == ObJsonNodeType::J_NULL) {
        LOG_DEBUG("name or content is null", K(jb_name->json_type()), K(jb_value->json_type()));
      } else if (OB_FAIL(ob_write_string(*mem_ctx->allocator_, ObString(jb_value->get_data_length(), jb_value->get_data()), value_str, false))) {
        LOG_WARN("write string value to string failed", K(ret), K(i), K(jb_name), K(jb_value));
      } else if (OB_FAIL(ob_write_string(*mem_ctx->allocator_, ObString(jb_name->get_data_length(), jb_name->get_data()), key_str, false))) {
        LOG_WARN("write string key to string failed", K(ret), K(i), K(jb_name), K(jb_value));
      } else if (OB_FAIL(element->add_attr_by_str(key_str, value_str, ObMulModeNodeType::M_ATTRIBUTE))) {
        LOG_WARN("add element failed", K(ret));
      }
    }
  }
  return ret;
}

int ObExprXmlElement::construct_element_children(ObMulModeMemCtx* mem_ctx,
                                                 ObVector<ObObj, ElementObjCacheStatArena> &value_vec,
                                                 ObXmlElement *&element,
                                                 ObXmlElement *valid_ele)
{
  int ret = OB_SUCCESS;
  ObXmlText *tag_value = NULL;
  if (OB_ISNULL(element)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("element node is NULL", K(ret));
  }
  // build xml text
  for (int i = 0; OB_SUCC(ret) && i < value_vec.size(); i++) {
    ObObj value = value_vec[i];
    ObIMulModeBase *node = NULL;
    ObXmlDocument *doc_node = NULL;
    if (value.get_type() == ObUserDefinedSQLType) {
      if (OB_FAIL(ObXMLExprHelper::add_binary_to_element(mem_ctx, value.get_string(), *element))) {
        LOG_WARN("add binary to element failed", K(ret), K(i));
      }
    } else {
      if (OB_ISNULL(tag_value = OB_NEWx(ObXmlText, mem_ctx->allocator_, ObMulModeNodeType::M_TEXT, mem_ctx))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("new xml text failed", K(ret));
      } else if (OB_FALSE_IT(tag_value->set_text(value.get_string()))) {
      } else if (OB_FAIL(element->add_element(tag_value))) {
        LOG_WARN("element add element failed", K(ret));
      } else {
        element->set_unparse(1);
      }
    }
  }

  // if the constructed element is not unparsed, use valid_ele to replace the element
  if (!element->get_unparse()) {
    if (OB_ISNULL(valid_ele)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("valid element is NULL, but validity is true", K(ret));
    }
    ObXmlNode *xml_node = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < element->size(); i++) {
      if (OB_ISNULL(xml_node = element->at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("xml node is NULL", K(ret));
      } else if (OB_FAIL(valid_ele->add_element(xml_node))) {
        LOG_WARN("fail to add element", K(ret));
      }
    } // end for

    if (OB_SUCC(ret)) {
      element = valid_ele;
    }
  }
  return ret;
}

int ObExprXmlElement::construct_element(ObMulModeMemCtx* mem_ctx,
                                        const ObString &name,
                                        ObVector<ObObj, ElementObjCacheStatArena> &value_vec,
                                        const ObIJsonBase *attr,
                                        ObXmlElement *&element,
                                        bool &validity)
{
  INIT_SUCC(ret);
  ObXmlElement *valid_ele = NULL;
  if (OB_ISNULL(element)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("element node is NULL", K(ret));
  } else if (OB_FAIL(element->init())) {
    LOG_WARN("element init failed", K(ret));
  } else if (FALSE_IT(element->set_xml_key(name))) {
  } else if (OB_FAIL(element->alter_member_sort_policy(false))) {
    LOG_WARN("fail to sort child element", K(ret));
  } else if (OB_FAIL(construct_attribute(mem_ctx, attr, element))) {
    LOG_WARN("fail to construct attribute", K(ret));
  } else if (OB_FAIL(ObXMLExprHelper::check_element_validity(mem_ctx, element, valid_ele, validity))) {
    LOG_WARN("check element validity failed", K(ret));
  } else if (OB_FAIL(construct_element_children(mem_ctx, value_vec, element, valid_ele))) {
    LOG_WARN("fail to construct element chidren", K(ret));
  }
  // set sort flag
  if (OB_SUCC(ret) && OB_FAIL(element->alter_member_sort_policy(true))) {
    LOG_WARN("fail to sort child element", K(ret));
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