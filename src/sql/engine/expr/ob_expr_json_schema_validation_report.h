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
 * This file contains implementation for json_schema_validation_report.
 */

#ifndef OCEANBASE_SQL_OB_EXPR_JSON_SCHEMA_VALIDATION_REPORT_H_
#define OCEANBASE_SQL_OB_EXPR_JSON_SCHEMA_VALIDATION_REPORT_H_

#include "lib/json_type/ob_json_tree.h"
#include "lib/json_type/ob_json_bin.h"
#include "lib/json_type/ob_json_schema.h"
#include "sql/engine/expr/ob_expr_operator.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

class ObExprJsonSchemaValidationReport : public ObFuncExprOperator
{
  OB_UNIS_VERSION(1);
public:
  explicit ObExprJsonSchemaValidationReport(common::ObIAllocator &alloc);
  virtual ~ObExprJsonSchemaValidationReport();
  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx)
                                const override;
  static int eval_json_schema_validation_report(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                    ObExpr &rt_expr) const override;
  virtual bool need_rt_ctx() const override { return true; }
  static int raise_validation_report(ObIAllocator &allocator, ObJsonSchemaValidator& validator,
                                    const bool& is_valid, ObIJsonBase*& validation_report);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprJsonSchemaValidationReport);
  const static uint8_t OB_JSON_SCHEMA_EXPR_ARG_NUM = 2;
private:
  ObString json_schema_;
};

} // sql
} // oceanbase
#endif // OCEANBASE_SQL_OB_EXPR_JSON_SCHEMA_VALIDATION_REPORT_H_