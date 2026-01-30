/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_LOAD_FILE_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_LOAD_FILE_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprLoadFile : public ObFuncExprOperator
{
public:
  explicit ObExprLoadFile(common::ObIAllocator &alloc);
  virtual ~ObExprLoadFile();
  virtual int calc_result_type2(ObExprResType &type, ObExprResType &type1, ObExprResType &type2, common::ObExprTypeCtx &type_ctx) const override;
  static int eval_load_file(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int eval_load_file_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                        const ObBitVector &skip, const EvalBound &bound);
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;
  static const int64_t DEFAULT_DOCUMENT_AI_FILE_MAX_SIZE = 100 * 1024 * 1024; // 100MB
private:
  static int read_file_from_location(const common::ObString &location_name,
                                     const common::ObString &filename,
                                     const uint64_t tenant_id,
                                     ObExecContext &exec_ctx,
                                     common::ObIAllocator &alloc,
                                     common::ObString &file_data);
  static int build_file_path(const common::ObString &location_url,
                             const common::ObString &filename,
                             common::ObIAllocator &alloc,
                             common::ObString &full_path);
  DISALLOW_COPY_AND_ASSIGN(ObExprLoadFile);
};
}
}
#endif /*OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_LOAD_FILE_*/
