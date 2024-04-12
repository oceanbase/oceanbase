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
 * This file is for func xmlserialize.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_expr_xml_serialize.h"
#include "sql/engine/expr/ob_expr_xml_func_helper.h"
#include "lib/xml/ob_xml_parser.h"
#include "lib/xml/ob_xml_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace sql
{
ObExprXmlSerialize::ObExprXmlSerialize(ObIAllocator &alloc)
    : ObFuncExprOperator(alloc, T_FUN_SYS_XML_SERIALIZE, N_XMLSERIALIZE, MORE_THAN_TWO, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
{
}

ObExprXmlSerialize::~ObExprXmlSerialize()
{
}

int ObExprXmlSerialize::calc_result_typeN(ObExprResType& type,
                                      ObExprResType* types,
                                      int64_t param_num,
                                      ObExprTypeCtx& type_ctx) const
{
  int ret = OB_SUCCESS;
  ObExprResType dst_type;
  if (OB_UNLIKELY(param_num != 10)) {
    ret = OB_ERR_PARAM_SIZE;
    LOG_WARN("invalid param number", K(ret), K(param_num));
  } else if (!is_called_in_sql()) {
    ret = OB_ERR_SP_LILABEL_MISMATCH;
    LOG_WARN("expr call in pl semantics disallowed", K(ret), K(N_XMLSERIALIZE));
    LOG_USER_ERROR(OB_ERR_SP_LILABEL_MISMATCH, static_cast<int>(strlen(N_XMLSERIALIZE)), N_XMLSERIALIZE);
  } else {
    if (!ob_is_integer_type(types[0].get_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid xml doc type", K(ret), K(types[0].get_type()));
    } else if (types[1].is_ext() && types[1].get_udt_id() == T_OBJ_XML) {
      types[1].get_calc_meta().set_sql_udt(ObXMLSqlType);
    }
    if (OB_SUCC(ret) && OB_FAIL(get_dest_type(types[2], dst_type))) {
      LOG_WARN("fail to get return datatype", K(ret), K(dst_type));
    }
    // opt encoding and version
    for (int32_t i = 3; i < 7 && OB_SUCC(ret); i += 2) {
      if (!ob_is_integer_type(types[i].get_type())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid opt type", K(ob_obj_type_str(types[i].get_type())), K(ret));
      } else if (!ob_is_string_type(types[i+1].get_type())) {
        types[i+1].set_calc_type(ObVarcharType);
        types[i+1].set_calc_collation_type(ObCharset::get_system_collation());
      }
    }

    // set return type
    if (OB_SUCC(ret)) {
      type.set_type(dst_type.get_type());
      if (dst_type.get_collation_type() == CS_TYPE_INVALID) {
        type.set_collation_type(CS_TYPE_UTF8MB4_BIN);
      } else {
        type.set_collation_type(dst_type.get_collation_type());
      }
      type.set_collation_level(CS_LEVEL_IMPLICIT);
      type.set_full_length(dst_type.get_length(), dst_type.get_length_semantics());
    }
  }
  return ret;
}

int ObExprXmlSerialize::get_dest_type(const ObExprResType as_type, ObExprResType &dst_type) const
{
  int ret = OB_SUCCESS;
  if (!as_type.is_int() && !as_type.get_param().is_int()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cast param type is unexpected", K(as_type));
  } else {
    const ObObj &param = as_type.get_param();
    ParseNode parse_node;
    parse_node.value_ = param.get_int();
    ObObjType obj_type = static_cast<ObObjType>(parse_node.int16_values_[OB_NODE_CAST_TYPE_IDX]);
    ObCollationType cs_type = static_cast<ObCollationType>(parse_node.int16_values_[OB_NODE_CAST_COLL_IDX]);
    dst_type.set_type(obj_type);
    dst_type.set_collation_type(cs_type);
    if (ob_is_varchar_type(dst_type.get_type(), dst_type.get_collation_type()) || ob_is_nvarchar2(obj_type)) {
      dst_type.set_full_length(parse_node.int32_values_[1], as_type.get_accuracy().get_length_semantics());
    } else if (ob_is_clob(dst_type.get_type(), dst_type.get_collation_type()) ||
               ob_is_blob(dst_type.get_type(), dst_type.get_collation_type())) {
      int64_t text_length = parse_node.int32_values_[1];
      dst_type.set_length(text_length < 0 ? ObAccuracy::DDL_DEFAULT_ACCURACY[obj_type].get_length() : text_length);
    } else {
      ret = OB_ERR_INVALID_DATATYPE;
      LOG_WARN("invalid datatype", K(ret), K(dst_type.get_type()), K(dst_type.get_collation_type()));
    }
  }
  return ret;
}


int ObExprXmlSerialize::eval_xml_serialize(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res)
{
  int ret = OB_SUCCESS;
  ObEvalCtx::TempAllocGuard tmp_alloc_g(ctx);
  common::ObArenaAllocator &allocator = tmp_alloc_g.get_allocator();
  int64_t xml_doc_type = OB_XML_DOC_TYPE_IMPLICIT;
  int64_t opt_encoding_type = OB_XML_NONE_ENCODING;
  int64_t opt_version_type = OB_XML_NONE_VERSION;
  int64_t opt_indent_type = OB_XML_INDENT_IMPLICT;
  int64_t opt_indent_size = OB_XML_INDENT_INVALID;
  int64_t opt_defaults = OB_XML_DEFAULTS_IMPLICIT;
  ObString opt_version_str;
  ObString opt_encoding_spec;
  ObObjType return_type = expr.datum_meta_.type_;
  ObCollationType return_cs_type = expr.datum_meta_.cs_type_;
  ObDatum *xml_datum = NULL;

  ObMulModeMemCtx* mem_ctx = nullptr;
  lib::ObMallocHookAttrGuard malloc_guard(lib::ObMemAttr(ObXMLExprHelper::get_tenant_id(ctx.exec_ctx_.get_my_session()), "XMLModule"));

  if (OB_ISNULL(ctx.exec_ctx_.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get session failed.", K(ret));
  } else if (OB_FAIL(ObXmlUtil::create_mulmode_tree_context(&allocator, mem_ctx))) {
    LOG_WARN("fail to create tree memory context", K(ret));
  } else if (OB_UNLIKELY(expr.arg_cnt_ != 10)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg_cnt_", K(ret), K(expr.arg_cnt_));
  } else if (OB_FAIL(get_and_check_int_from_expr(expr.args_[0], ctx, OB_XML_DOCUMENT, OB_XML_CONTENT, xml_doc_type))) {
    LOG_WARN("fail to get xml doc type", K(ret));
  } else if (!is_supported_return_type(return_type, return_cs_type)) {
    ret = OB_ERR_INVALID_DATATYPE;
    LOG_WARN("invalid return datatype", K(ret), K(return_type), K(return_cs_type));
  } else if (OB_FAIL(get_and_check_int_from_expr(expr.args_[3], ctx, 0, 1, opt_encoding_type))) {
    LOG_WARN("fail to get indent size", K(ret));
  } else if (opt_encoding_type == 1 &&
             OB_FAIL(get_and_check_encoding_spec(expr.args_[4], ctx, return_type, return_cs_type, opt_encoding_spec))) {
    LOG_WARN("fail to get encoding spec", K(ret));
  } else if (OB_FAIL(get_and_check_int_from_expr(expr.args_[5], ctx, 0, 1, opt_version_type))) {
    LOG_WARN("fail to get indent size", K(ret));
  } else if (opt_version_type == 1 &&
             OB_FAIL(ObXMLExprHelper::get_str_from_expr(expr.args_[6], ctx, opt_version_str, allocator))) {
    LOG_WARN("fail to get version", K(ret));
  } else if (OB_FAIL(get_and_check_int_from_expr(expr.args_[7], ctx, OB_XML_NO_INDENT, OB_XML_INDENT_IMPLICT, opt_indent_type))) {
    LOG_WARN("fail to get indent size", K(ret));
  } else if (opt_indent_type != OB_XML_INDENT_IMPLICT &&
             OB_FAIL(get_and_check_int_from_expr(expr.args_[8], ctx, OB_XML_INDENT_MIN, 255, opt_indent_size))) {
    LOG_WARN("fail to get indent size", K(ret));
  } else if (OB_FAIL(get_and_check_int_from_expr(expr.args_[9], ctx, OB_XML_DEFAULTS_IMPLICIT, OB_XML_SHOW_DEFAULTS, opt_defaults))) {
    LOG_WARN("fail to get defaults option", K(ret));
  } else if (OB_FAIL(ObXMLExprHelper::get_xmltype_from_expr(expr.args_[1], ctx, xml_datum))) {
    // according to xmlserialize behavior in oracle: it will check the xmltype data type after the checks IMPLICIT done
    LOG_WARN("fail to get xmltype data", K(ret));
  } else if (xml_datum->is_null()) {
    res.set_null();
  } else {
    ObIMulModeBase *xml_node = NULL;
    bool is_xml_doc = xml_doc_type == OB_XML_DOCUMENT;
    bool with_encoding = opt_encoding_type == 1;
    bool with_version = opt_version_type == 1;
    bool is_format = is_xml_doc || opt_indent_type != OB_XML_INDENT_IMPLICT ||
                     with_encoding ||
                     with_version ||
                     opt_defaults == OB_XML_SHOW_DEFAULTS;

    ObParameterPrint print_params;
    print_params.encode = opt_encoding_spec;
    print_params.version = opt_version_str;
    print_params.indent = opt_indent_type != OB_XML_INDENT_IMPLICT ? opt_indent_size : 2;
    ObString xml_serialize_str;
    ObCollationType cs_type = ctx.exec_ctx_.get_my_session()->get_nls_collation();
    ObCharsetType charset_type = CHARSET_INVALID;
    if (OB_FAIL(get_xml_base_by_doc_type(mem_ctx, is_xml_doc, cs_type, xml_datum, xml_node))) {
      LOG_WARN("fail to parse xml doc", K(ret), K(is_xml_doc));
    } else {
      uint32_t xml_format_type = (opt_indent_type == OB_XML_NO_INDENT) ? ObXmlFormatType::MERGE_EMPTY_TAG : ObXmlFormatType::WITH_FORMAT;
      if (opt_indent_type == OB_XML_INDENT_SIZE && opt_indent_size > OB_XML_INDENT_MAX) {
        ret = OB_INVALID_PRINT_OPTION;  // error code: ora-31188
        LOG_WARN("invalid specified print option ", K(opt_indent_size));
      } else if (with_encoding && (CHARSET_INVALID == (charset_type = ObXmlUtil::check_support_charset(opt_encoding_spec)))) {
        ret = OB_ERR_UTL_ENCODE_CHARSET_INVALID;
        LOG_WARN("invalid charset", K(opt_encoding_spec));
      } else if (OB_FAIL(print_format_xml_text(allocator,
                                               ctx,
                                               xml_node,
                                               with_encoding,
                                               with_version,
                                               xml_format_type,
                                               print_params,
                                               xml_serialize_str))) {
        LOG_WARN("fail to print format xml text", K(ret));
      } else if (ob_is_blob(return_type, return_cs_type)) {
        ObCollationType new_cs_type = with_encoding ? ObCharset::get_default_collation_oracle(charset_type) : cs_type;
        if (new_cs_type != CS_TYPE_UTF8MB4_BIN &&
            OB_FAIL(ObCharset::charset_convert(allocator, xml_serialize_str, CS_TYPE_UTF8MB4_BIN, new_cs_type, xml_serialize_str))) {
          LOG_WARN("charset convertion failed", K(ret), K(xml_serialize_str), K(cs_type), K(new_cs_type));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(transform_to_dst_type(expr, xml_serialize_str))) {
      LOG_WARN("fail to transfrom result to dest datatype", K(ret));
    } else if (OB_FAIL(ObXMLExprHelper::set_string_result(expr, ctx, res, xml_serialize_str))) {
      LOG_WARN("fail to set result", K(ret), K(xml_serialize_str));
    }
  }
  return ret;
}

bool ObExprXmlSerialize::is_supported_return_type(ObObjType val_type, ObCollationType cs_type)
{
  return ob_is_varchar_type(val_type, cs_type) ||
         ob_is_blob(val_type, cs_type) ||
         ob_is_clob(val_type, cs_type) ||
         ob_is_nvarchar2(val_type);
}

int ObExprXmlSerialize::print_format_xml_text(ObIAllocator &allocator,
                                              ObEvalCtx &ctx,
                                              ObIMulModeBase *xml_node,
                                              bool with_encoding,
                                              bool with_version,
                                              uint32_t format_flag,
                                              ObParameterPrint &print_params,
                                              ObString &res)
{
  int ret = OB_SUCCESS;
  ObStringBuffer buff(&allocator);

  ObString version = xml_node->get_version();
  ObString encoding = xml_node->get_encoding();

  if (with_encoding || with_version) {
    // if has encoding or vesion clause, do not show standalone attribute
    xml_node->set_standalone(0);
  }

  with_version = with_version && !print_params.version.empty();
  // set output prolog
  if (with_encoding && with_version) {
  } else if (with_encoding) {
    if (xml_node->has_flags(XML_DECL_FLAG) && !version.empty()) {
      print_params.version = version;
    } else {
      print_params.version = ObString::make_string("1.0");
    }
    with_version = true;
  } else if (with_version) {
    if (!encoding.empty() || xml_node->has_flags(XML_ENCODING_EMPTY_FLAG)) {
      with_encoding = true;
      print_params.encode = ObString(ObXmlUtil::get_charset_name(ctx.exec_ctx_.get_my_session()->get_local_collation_connection()));
    }
  } else {
    if (xml_node->has_flags(XML_DECL_FLAG)) {
      with_version = true;
      print_params.version = version;
    }
    if (!encoding.empty() || xml_node->has_flags(XML_ENCODING_EMPTY_FLAG)) {
      with_encoding = true;
      print_params.encode = ObString(ObXmlUtil::get_charset_name(ctx.exec_ctx_.get_my_session()->get_local_collation_connection()));
    }
  }
  ObNsSortedVector* ns_vec_point = nullptr;
  ObNsSortedVector ns_vec;

  if (OB_FAIL(ObXmlUtil::init_print_ns(&allocator, xml_node, ns_vec, ns_vec_point))) {
    LOG_WARN("fail to init ns vector by extend area", K(ret));
  } else if (OB_FAIL(xml_node->print_content(buff, with_encoding, with_version, format_flag, print_params, ns_vec_point))) {
    LOG_WARN("fail to print xml content", K(ret), K(with_encoding), K(with_version), K(format_flag));
  }
  if (OB_SUCC(ret)) {
    res.assign_ptr(buff.ptr(), buff.length());
  }
  return ret;
}

int ObExprXmlSerialize::get_and_check_int_from_expr(const ObExpr *expr,
                                                    ObEvalCtx &ctx,
                                                    int64_t start,
                                                    int64_t end,
                                                    int64_t &res)
{
  int ret = OB_SUCCESS;
  ObObjType val_type = expr->datum_meta_.type_;
  ObDatum *datum = NULL;
  if (OB_FAIL(expr->eval(ctx, datum))) {
    LOG_WARN("eval xml arg failed", K(ret));
  } else if (val_type != ObIntType) {
    ret = OB_ERR_REQUIRE_INTEGER;
    LOG_WARN("input type error", K(val_type));
  } else {
    int64_t value = datum->get_int();
    if (value < start || value > end) {
      ret = OB_NUMERIC_SCALE_OUT_RANGE;
      LOG_WARN("input value error", K(value));
    } else {
      res = value;
    }
  }

  return ret;
}

int ObExprXmlSerialize::get_and_check_encoding_spec(const ObExpr *expr,
                                                    ObEvalCtx &ctx,
                                                    ObObjType return_type,
                                                    ObCollationType return_cs_type,
                                                    ObString &encoding_spec)
{
  int ret = OB_SUCCESS;
  ObDatum *datum = NULL;
  ObObjType val_type = expr->datum_meta_.type_;
  if (OB_FAIL(expr->eval(ctx, datum))) {
    LOG_WARN("eval json arg failed", K(ret));
  } else if (!ob_is_string_type(val_type)) {
    ret = OB_ERR_INVALID_TYPE_FOR_OP;
    LOG_WARN("invalid encoding spec type ", K(val_type));
  } else if (!ob_is_blob(return_type, return_cs_type)) {
    ret = OB_ERR_INVALID_DATATYPE;
    LOG_WARN("invalid data type", K(ret), K(encoding_spec), K(return_type), K(return_cs_type));
  } else {
    encoding_spec = datum->get_string();
    char *ptr = str_toupper(encoding_spec.ptr(), encoding_spec.length());
  }
  return ret;
}

int ObExprXmlSerialize::transform_to_dst_type(const ObExpr &expr, ObString &xml_format_str)
{
  int ret = OB_SUCCESS;
  ObObjType obj_type = expr.datum_meta_.type_;
  ObCollationType cs_type = expr.datum_meta_.cs_type_;
  ObLength max_length = expr.max_length_;
  if (ob_is_varchar_type(obj_type, cs_type) || ob_is_nvarchar2(obj_type)) {
    ObObj obj;
    obj.set_string(ObVarcharType, xml_format_str);
    if (OB_FAIL(string_length_check_only(max_length, cs_type, obj))) {
      LOG_WARN("fail to check string len", K(ret), K(max_length), K(obj), K(cs_type));
    }
  } else if (ob_is_clob(obj_type, cs_type) || ob_is_blob(obj_type, cs_type)) {
    /* do nothing */
  } else {
    ret = OB_ERR_INVALID_DATATYPE;
    LOG_WARN("invalid return datatype", K(ret), K(obj_type), K(cs_type));
  }

  return ret;
}

int ObExprXmlSerialize::string_length_check_only(const ObLength max_len_char,
                                                 const ObCollationType cs_type,
                                                 const ObObj &obj)
{
  int ret = OB_SUCCESS;
  const char *str = obj.get_string_ptr();
  const int32_t str_len_byte = obj.get_string_len();
  const int32_t str_len_char = static_cast<int32_t>(ObCharset::strlen_char(cs_type, str, str_len_byte));
  if (OB_UNLIKELY(max_len_char <= 0)) {
    if (OB_UNLIKELY(0 == max_len_char && str_len_byte > 0)) {
      ret = OB_XML_CHAR_LEN_TOO_SMALL;
      LOG_WARN("char type length is too long", K(obj), K(max_len_char), K(str_len_char));
    }
  } else if (OB_UNLIKELY(str_len_char > max_len_char)) {
    ret = OB_XML_CHAR_LEN_TOO_SMALL;
    LOG_WARN("string length is too long", K(max_len_char), K(str_len_char), K(obj));
  }
  return ret;
}

int ObExprXmlSerialize::get_xml_base_by_doc_type(ObMulModeMemCtx* mem_ctx,
                                                 bool is_xml_doc,
                                                 ObCollationType cs_type,
                                                 ObDatum *xml_datum,
                                                 ObIMulModeBase *&node)
{
  int ret = OB_SUCCESS;
  // temporary use for xml clob
  ObString xml_text;
  ObDatumMeta xml_meta;
  xml_meta.type_ = ObLongTextType;
  xml_meta.cs_type_ = CS_TYPE_UTF8MB4_BIN;
  ObXmlDocument *xml_node = NULL;
  ObIMulModeBase *tmp_node = NULL;
  ObNodeMemType expect_type = ObNodeMemType::BINARY_TYPE;

  if (OB_ISNULL(xml_datum)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("xml datum is NULL", K(ret));
  } else if (OB_FAIL(ObXMLExprHelper::get_xml_base(mem_ctx, xml_datum, CS_TYPE_INVALID, expect_type, node))) {
    LOG_WARN("fail to get xml base", K(ret), K(cs_type), K(expect_type));
  } else if (is_xml_doc) {// document
    ObString check_xml;
    ObStringBuffer buff(mem_ctx->allocator_);
    ObXmlDocument *xml_doc = nullptr;
    ObNsSortedVector* ns_vec_point = nullptr;
    ObNsSortedVector ns_vec;
    if (OB_FAIL(ObXmlUtil::init_print_ns(mem_ctx->allocator_, node, ns_vec, ns_vec_point))) {
      LOG_WARN("fail to init ns vector by extend area", K(ret));
    } else if (OB_FAIL(node->print_document(buff, CS_TYPE_INVALID, ObXmlFormatType::NO_FORMAT, 2, ns_vec_point))) {
      LOG_WARN("fail to serialize unparse string", K(ret));
    } else {
      check_xml.assign_ptr(buff.ptr(), buff.length());
    }
    if (OB_FAIL(ret)) {
    } else if (check_xml.empty()) {
      /* return an empty xml content node*/
      if (OB_ISNULL(xml_doc = OB_NEWx(ObXmlDocument, (mem_ctx->allocator_), ObMulModeNodeType::M_CONTENT, (mem_ctx)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to create an empty xml content node", K(ret), K(xml_text));
      } else {
        node = xml_doc;
      }
    } else if (OB_FAIL(ObXmlParserUtils::parse_document_text(mem_ctx, check_xml, xml_doc))) {
      if (ret == OB_ERR_PARSER_SYNTAX) {
          ret = OB_ERR_XML_FRAMENT_CONVERT;
          LOG_USER_ERROR(OB_ERR_XML_FRAMENT_CONVERT);
        }
        LOG_WARN("fail to parse xml doc", K(check_xml), K(ret));
    } else if (node->type() == M_CONTENT) {
      // fix issue-49263291
      // case like select xmlserialize(DOCUMENT xmlparse(CONTENT '<?xml-stylesheet type="text/css" ?><a>aaa</a>') ) as res from dual;
      if (node->size() > 1) {
        ret = OB_ERR_XML_FRAMENT_CONVERT;
        LOG_USER_ERROR(OB_ERR_XML_FRAMENT_CONVERT);
      }
    }
  }
  return ret;
}

int ObExprXmlSerialize::cg_expr(ObExprCGCtx &expr_cg_ctx,
                                const ObRawExpr &raw_expr,
                                ObExpr &rt_expr) const
{
  UNUSED(expr_cg_ctx);
  UNUSED(raw_expr);
  rt_expr.eval_func_ = eval_xml_serialize;
  return OB_SUCCESS;
}

}
}