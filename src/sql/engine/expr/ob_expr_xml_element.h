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
 * This file contains implementation for xmlelement.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_XML_ELEMENT_H_
#define OCEANBASE_SQL_OB_EXPR_XML_ELEMENT_H_

#include "sql/engine/expr/ob_expr_operator.h"
#include "lib/json_type/ob_json_base.h" // for ObIJsonBase may need json for kv pairs
#include "lib/json_type/ob_json_bin.h" // for ObJsonBin
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "lib/container/ob_vector.h"
#ifdef OB_BUILD_ORACLE_XML
#include "lib/xml/ob_xml_util.h"
#endif

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{
typedef PageArena<ObObj, ModulePageAllocator> ElementObjCacheStatArena;
class
ObExprXmlElement : public ObFuncExprOperator
{
public:
  explicit ObExprXmlElement(common::ObIAllocator &alloc);
  virtual ~ObExprXmlElement();
  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types,
                                int64_t param_num,
                                common::ObExprTypeCtx& type_ctx)
                                const override;
#ifdef OB_BUILD_ORACLE_XML
  static int eval_xml_element(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
#else
  static int eval_xml_element(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res) { return OB_NOT_SUPPORTED; }
#endif
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
private:
#ifdef OB_BUILD_ORACLE_XML
  static int get_keys_from_wrapper(ObIJsonBase *json_doc,
                                   ObIAllocator *allocator,
                                   ObString &str);
  static int construct_element(ObMulModeMemCtx* mem_ctx,
                                const ObString &name,
                                ObVector<ObObj, ElementObjCacheStatArena> &value_vec,
                                const ObIJsonBase *attr,
                                ObXmlElement *&element,
                                bool &validity);
  static int construct_attribute(ObMulModeMemCtx* mem_ctx,
                                 const ObIJsonBase *attr,
                                 ObXmlElement *&element);
  static int construct_element_children(ObMulModeMemCtx* mem_ctx,
                                        ObVector<ObObj, ElementObjCacheStatArena> &value_vec,
                                        ObXmlElement *&element,
                                        ObXmlElement *valid_ele);
  static int construct_value_array(ObIAllocator &allocator,
                                    const ObString &value,
                                    ObVector<ObObj, ElementObjCacheStatArena> &res_value);
#endif
  DISALLOW_COPY_AND_ASSIGN(ObExprXmlElement);
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_XML_ELEMENT_H_