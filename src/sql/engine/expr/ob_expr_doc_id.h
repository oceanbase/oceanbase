/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_EXPR_DOC_ID_H
#define OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_EXPR_DOC_ID_H

#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/engine/dml/ob_dml_ctx_define.h"

namespace oceanbase
{
namespace sql
{
class ObExprDocID final : public ObFuncExprOperator
{
public:
  explicit ObExprDocID(common::ObIAllocator &alloc);
  virtual ~ObExprDocID() {}
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const override;
  virtual int calc_resultN(common::ObObj &result,
                           const common::ObObj *objs_array,
                           int64_t param_num,
                           common::ObExprCtx &expr_ctx) const override;
  virtual common::ObCastMode get_cast_mode() const override { return CM_NULL_ON_WARN;}
  virtual int cg_expr(
      ObExprCGCtx &expr_cg_ctx,
      const ObRawExpr &raw_expr,
      ObExpr &rt_expr) const override;
  static int generate_doc_id(
      const ObExpr &raw_ctx,
      ObEvalCtx &eval_ctx,
      ObDatum &expr_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprDocID);
};

} // end namespace sql
} // end namespace oceanbase
#endif /* OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_EXPR_DOC_ID_H */
