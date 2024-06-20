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
 * This file is for func xmlsequence.
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_XML_SEQUENCE_H
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_XML_SEQUENCE_H

#include "sql/engine/expr/ob_expr_operator.h"
#include "pl/ob_pl_type.h"
#include "pl/ob_pl_user_type.h"

namespace oceanbase {

namespace sql
{

enum XmlSequenceUdtType
{
  XmlSequenceUdtID = 300026
};

class ObExprXmlSequence : public ObFuncExprOperator
{
public:
  explicit ObExprXmlSequence(common::ObIAllocator &alloc);
  virtual ~ObExprXmlSequence();

  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &text,
                                common::ObExprTypeCtx &type_ctx) const override;

#ifdef OB_BUILD_ORACLE_PL
  static int eval_xml_sequence(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
#else
  static int eval_xml_sequence(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res) { return OB_NOT_SUPPORTED; }
#endif
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr)
                      const override;
  struct ObSequenceExtraInfo : public ObIExprExtraInfo
  {
    OB_UNIS_VERSION(1);
  public:
    ObSequenceExtraInfo(common::ObIAllocator &alloc, ObExprOperatorType type)
        : ObIExprExtraInfo(alloc, type),
        type_(pl::ObPLType::PL_INVALID_TYPE),
        not_null_(false),
        elem_type_(),
        capacity_(common::OB_INVALID_SIZE),
        udt_id_(common::OB_INVALID_ID)
    {
    }
    virtual int deep_copy(common::ObIAllocator &allocator,
                        const ObExprOperatorType type,
                        ObIExprExtraInfo *&copied_info) const override;
    pl::ObPLType type_;
    bool not_null_;
    common::ObDataType elem_type_;
    int64_t capacity_;
    uint64_t udt_id_;
  };

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprXmlSequence);
};


}
}

#endif
