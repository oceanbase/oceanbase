
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
 */

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_CODEC_URL_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_CODEC_URL_

#include "lib/oblog/ob_log.h"
#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprURLCODEC : public ObFuncExprOperator
{
public:
  explicit ObExprURLCODEC(common::ObIAllocator &alloc, ObExprOperatorType type, const char *name) :
    ObFuncExprOperator(alloc, type, name, 1, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION)
  {}
  virtual ~ObExprURLCODEC(){};

  virtual int calc_result_type1(ObExprResType &type, ObExprResType &type_1,
                                common::ObExprTypeCtx &type_ctx) const override;

  static int eval_url_codec(EVAL_FUNC_ARG_DECL, bool is_encode);
  static int eval_url_codec_batch(BATCH_EVAL_FUNC_ARG_DECL, bool is_encode);
  static int eval_url_codec_vector(VECTOR_EVAL_FUNC_ARG_DECL, bool is_encode);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprURLCODEC);
};

class ObExprURLEncode : public ObExprURLCODEC
{
public:
  explicit ObExprURLEncode(common::ObIAllocator &alloc);
  virtual ~ObExprURLEncode(){};

  int cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const;

  static int eval_url_encode(EVAL_FUNC_ARG_DECL);
  static int eval_url_encode_batch(BATCH_EVAL_FUNC_ARG_DECL);
  static int eval_url_encode_vector(VECTOR_EVAL_FUNC_ARG_DECL);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprURLEncode);
};

class ObExprURLDecode : public ObExprURLCODEC
{
public:
  explicit ObExprURLDecode(common::ObIAllocator &alloc);
  virtual ~ObExprURLDecode(){};

  int cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr, ObExpr &rt_expr) const;

  static int eval_url_decode(EVAL_FUNC_ARG_DECL);
  static int eval_url_decode_batch(BATCH_EVAL_FUNC_ARG_DECL);
  static int eval_url_decode_vector(VECTOR_EVAL_FUNC_ARG_DECL);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprURLDecode);
};

}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_CODEC_URL_ */