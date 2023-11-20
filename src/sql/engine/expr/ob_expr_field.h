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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_FIELD_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_FIELD_

#include "sql/engine/expr/ob_expr_operator.h"
#include "share/object/ob_obj_cast.h"

namespace oceanbase
{
namespace sql
{
class ObExprField : public ObVectorExprOperator
{
public:

  explicit  ObExprField(common::ObIAllocator &alloc);
  virtual ~ObExprField() {};
  virtual int assign(const ObExprOperator &other);
public:
  //serialize and deserialize
  virtual int serialize(char *buf, const int64_t buf_len, int64_t &pos) const;
  virtual int deserialize(const char *buf, const int64_t data_len, int64_t &pos);
  virtual int64_t get_serialize_size() const;
public:
  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *type_stack,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;

  void set_need_cast(bool need_cast) {need_cast_ = need_cast;}

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int eval_field(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  DECLARE_SET_LOCAL_SESSION_VARS;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprField);
private:
  bool need_cast_;
};

} // namespace sql
} // namespace oceanbase
#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_FIELD_
