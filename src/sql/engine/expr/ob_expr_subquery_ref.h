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

#ifndef OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_EXPR_SUBQUERY_H_
#define OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_EXPR_SUBQUERY_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase {
namespace sql {

class ObSubQueryIterator;

class ObExprSubQueryRef : public ObExprOperator {
  OB_UNIS_VERSION(1);

public:
  // extra info stored in ObExpr::extra_
  struct ExtraInfo : public ObExprExtraInfoAccess<ExtraInfo> {
    const static uint32_t DEF_OP_ID = std::numeric_limits<uint32_t>::max();
    ExtraInfo() : op_id_(DEF_OP_ID), iter_idx_(0), is_scalar_(0)
    {}

    uint32_t op_id_;
    uint16_t iter_idx_;
    uint16_t is_scalar_;

    TO_STRING_KV(K(op_id_), K(iter_idx_), K(is_scalar_));
    bool is_valid() const
    {
      return DEF_OP_ID != op_id_;
    }
  } __attribute__((packed));

  static_assert(sizeof(ExtraInfo) <= sizeof(uint64_t), "too big extra info");

  explicit ObExprSubQueryRef(common::ObIAllocator& alloc);
  virtual ~ObExprSubQueryRef();

  virtual int assign(const ObExprOperator& other);

  virtual void reset();
  virtual int calc_result_type0(ObExprResType& type, common::ObExprTypeCtx& type_ctx) const;
  virtual int calc_result0(common::ObObj& result, common::ObExprCtx& expr_ctx) const;

  virtual int cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override;
  static int expr_eval(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  static int get_subquery_iter(ObEvalCtx& ctx, const ExtraInfo& extra_info, ObSubQueryIterator*& iter);

  void set_result_is_scalar(bool is_scalar)
  {
    result_is_scalar_ = is_scalar;
  }
  void set_scalar_result_type(const ObExprResType& result_type);
  void set_subquery_idx(int64_t subquery_idx)
  {
    subquery_idx_ = subquery_idx;
  }
  common::ObIArray<common::ObDataType>& get_row_desc()
  {
    return row_desc_;
  }
  int init_row_desc(int64_t capacity)
  {
    return row_desc_.init(capacity);
  }
  VIRTUAL_TO_STRING_KV(N_EXPR_TYPE, get_type_name(type_), N_EXPR_NAME, name_, N_PARAM_NUM, param_num_, N_DIM,
      row_dimension_, N_REAL_PARAM_NUM, real_param_num_, K_(scalar_result_type), K_(result_is_scalar), K_(subquery_idx),
      K_(row_desc));

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSubQueryRef);

private:
  bool result_is_scalar_;
  ObExprResType scalar_result_type_;
  int64_t subquery_idx_;
  common::ObFixedArray<common::ObDataType, common::ObIAllocator> row_desc_;
};
}  // namespace sql
}  // namespace oceanbase
#endif  // OCEANBASE_SRC_SQL_ENGINE_EXPR_OB_EXPR_SUBQUERY_H_
