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
 * This file is for func xmlparse.
 */

#ifndef OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_EXPR_XMLPARSE_H
#define OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_EXPR_XMLPARSE_H

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase
{

namespace sql
{

class ObExprXmlparse : public ObFuncExprOperator
{
  public:
  explicit ObExprXmlparse(common::ObIAllocator &alloc);
  virtual ~ObExprXmlparse();
  virtual int calc_result_typeN(ObExprResType& type,
                                ObExprResType* types_stack,
                                int64_t param_num,
                                common::ObExprTypeCtx& type_ctx)
                                const override;
#ifdef OB_BUILD_ORACLE_XML
  static int eval_xmlparse(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
#else
  static int eval_xmlparse(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res) { return OB_NOT_SUPPORTED; }
#endif
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr)
                      const override;

private:

  /* process xml_doc_type */
  const static uint8_t OB_XML_DOC_TYPE_COUNT = 3;
  const static uint8_t OB_XML_DOCUMENT = 0;
  const static uint8_t OB_XML_CONTENT = 1;
  const static uint8_t OB_XML_DOC_TYPE_IMPLICIT = 2;

  /* process xml wellformed */
  const static uint8_t OB_WELLFORMED_COUNT = 3;
  const static uint8_t OB_XML_WELLFORMED = 0;
  const static uint8_t OB_XML_NOT_WELLFORMED = 1;
  const static uint8_t OB_WELLFORMED_IMPLICIT = 2;

  static int get_clause_opt(const ObExpr &expr,
                            ObEvalCtx &ctx,
                            uint8_t index,
                            uint8_t &type,
                            uint8_t size_para);

private:
    DISALLOW_COPY_AND_ASSIGN(ObExprXmlparse);
};

} // sql
} // oceanbase


#endif // OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_EXPR_XMLPARSE_H
