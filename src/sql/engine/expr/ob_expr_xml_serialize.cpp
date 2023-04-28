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
#include "ob_expr_xml_func_helper.h"
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
    if (ob_is_varchar_type(dst_type.get_type(), dst_type.get_collation_type())) {
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