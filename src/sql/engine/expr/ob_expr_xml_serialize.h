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
 * This file contains implementation for xmlserialize.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_XML_SERIALIZE_H_
#define OCEANBASE_SQL_OB_EXPR_XML_SERIALIZE_H_

#include "sql/engine/expr/ob_expr_operator.h"
#ifdef OB_BUILD_ORACLE_XML
#include "lib/xml/ob_multi_mode_interface.h"
#include "lib/xml/ob_xml_tree.h"
#endif
using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
class ObExprXmlSerialize : public ObFuncExprOperator
{
public:
  explicit ObExprXmlSerialize(common::ObIAllocator &alloc);
  virtual ~ObExprXmlSerialize();
  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num,
                                common::ObExprTypeCtx& type_ctx)
                                const override;
#ifdef OB_BUILD_ORACLE_XML
  static int eval_xml_serialize(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
#else
  static int eval_xml_serialize(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res) { return OB_NOT_SUPPORTED; }
#endif
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  int get_dest_type(const ObExprResType as_type, ObExprResType &dst_type) const;
#ifdef OB_BUILD_ORACLE_XML
private:
  static bool is_supported_return_type(ObObjType val_type, ObCollationType cs_type);

  static int get_xml_base_by_doc_type(ObMulModeMemCtx* mem_ctx,
                                      bool is_xml_doc,
                                      ObCollationType cs_type,
                                      ObDatum *xml_datum,
                                      ObIMulModeBase *&node);
  static int get_and_check_int_from_expr(const ObExpr *expr,
                                         ObEvalCtx &ctx,
                                         int64_t start,
                                         int64_t end,
                                         int64_t &res);
  static int transform_to_dst_type(const ObExpr &expr, ObString &xml_format_str);
  static int string_length_check_only(const ObLength max_length_char,
                                      const ObCollationType cs_type,
                                      const ObObj &obj);
  static int get_and_check_encoding_spec(const ObExpr *expr,
                                         ObEvalCtx &ctx,
                                         ObObjType return_type,
                                         ObCollationType return_cs_type,
                                         ObString &encoding_spec);
  static int print_format_xml_text(ObIAllocator &allocator,
                                   ObEvalCtx &ctx,
                                   ObIMulModeBase *xml_doc,
                                   bool with_encoding,
                                   bool with_version,
                                   uint32_t format_flag,
                                   ObParameterPrint &print_params,
                                   ObString &res);

private:
  // xml_doc_type
  const static int64_t OB_XML_DOCUMENT = 0;
  const static int64_t OB_XML_CONTENT = 1;
  const static int64_t OB_XML_DOC_TYPE_IMPLICIT = 2;
  // encoding type
  const static int64_t OB_XML_NONE_ENCODING = 0;
  const static int64_t OB_XML_WITH_ENCODING = 1;
  // version type
  const static int64_t OB_XML_NONE_VERSION = 0;
  const static int64_t OB_XML_WITH_VERSION = 1;
  // indent type
  const static int64_t OB_XML_NO_INDENT = 1;
  const static int64_t OB_XML_INDENT = 2;
  const static int64_t OB_XML_INDENT_SIZE = 3;
  const static int64_t OB_XML_INDENT_IMPLICT = 4;
  // indent size
  const static int64_t OB_XML_INDENT_INVALID = -1;
  const static int64_t OB_XML_INDENT_MIN = 0;
  const static int64_t OB_XML_INDENT_MAX = 12;
  // default type
  const static int64_t OB_XML_DEFAULTS_IMPLICIT = 0;
  const static int64_t OB_XML_HIDE_DEFAULTS = 1;
  const static int64_t OB_XML_SHOW_DEFAULTS = 2;
#endif
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprXmlSerialize);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_XML_SERIALIZE_H_