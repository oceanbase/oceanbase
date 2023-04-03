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

#ifndef DEV_SRC_SQL_ENGINE_EXPR_OB_ITER_EXPR_RANGE_PARAM_H_
#define DEV_SRC_SQL_ENGINE_EXPR_OB_ITER_EXPR_RANGE_PARAM_H_
#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{
class ObIterExprRangeParam : public ObIterExprOperator
{
  OB_UNIS_VERSION(1);
public:
  ObIterExprRangeParam()
    : ObIterExprOperator(),
      start_index_(common::OB_INVALID_INDEX),
      end_index_(common::OB_INVALID_INDEX)
  {
  }
  virtual ~ObIterExprRangeParam() {}

  void set_param_range(int64_t start_index, int64_t end_index)
  {
    start_index_ = start_index;
    end_index_ = end_index;
  }
  virtual int get_next_row(ObIterExprCtx &expr_ctx, const common::ObNewRow *&result) const override;
  TO_STRING_KV(K_(expr_id),
               K_(expr_type),
               K_(start_index),
               K_(end_index));
private:
  int64_t start_index_;
  int64_t end_index_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* DEV_SRC_SQL_ENGINE_EXPR_OB_ITER_EXPR_RANGE_PARAM_H_ */
