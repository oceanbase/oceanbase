/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_VECTOR_OB_VECTOR_BASE_H_
#define OCEANBASE_SHARE_VECTOR_OB_VECTOR_BASE_H_

#include "share/vector/ob_i_vector.h"

namespace oceanbase
{
namespace sql
{
  class ObExpr;
  class ObEvalCtx;
}
namespace common
{

class ObVectorBase : public ObIVector
{
public:
  ObVectorBase() : max_row_cnt_(INT32_MAX), flags_(0), expr_(nullptr), eval_ctx_(nullptr) {}

  // TODO: check calling
  void set_max_row_cnt(int32_t max_row_cnt) { max_row_cnt_ = max_row_cnt; }
  int32_t get_max_row_cnt() const { return max_row_cnt_; }

  void set_expr_and_ctx(sql::ObExpr *expr, sql::ObEvalCtx *ctx)
  {
    expr_ = expr;
    eval_ctx_ = ctx;
  }

  OB_INLINE sql::ObExpr *get_expr() { return expr_; }
  OB_INLINE sql::ObEvalCtx *get_eval_ctx() { return eval_ctx_; }

  virtual bool is_collection_expr() const final { return is_collection_expr_; }
  virtual void set_has_compact_collection() final { collection_all_vector_fmt_ = false; }
  virtual bool has_compact_collection() const final { return !collection_all_vector_fmt_; }
  virtual void unset_has_compact_collection() final { collection_all_vector_fmt_ = true; }

  // Copy base metadata from src (max_row_cnt_, flags_). Optional: call only when the caller
  // explicitly wants the destination to adopt the source's capacity/flags; from_vector_xxx
  // payload copy does not call this so the destination keeps its own base.
  OB_INLINE int from_vector_base(const ObIVector *src_vec)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(src_vec)) {
      ret = OB_INVALID_ARGUMENT;
      SQL_ENG_LOG(WARN, "src_vec is null", K(ret));
    } else {
      const ObVectorBase *src_base = static_cast<const ObVectorBase *>(src_vec);
      //max_row_cnt_ = src_base->max_row_cnt_;
      //flags_ = src_base->flags_;
    }
    return ret;
  }
protected:
friend class sql::ObExpr;
  int32_t max_row_cnt_;   // max row capacity of this vector (e.g. batch size)
  union {
    struct {
      uint32_t is_collection_expr_: 1;
      uint32_t collection_all_vector_fmt_: 1;
      uint32_t reserved_: 30;
    };
    uint32_t flags_;      // collection/format flags
  };
  sql::ObExpr *expr_;
  sql::ObEvalCtx *eval_ctx_;
};

}
}
#endif // OCEANBASE_SHARE_VECTOR_OB_VECTOR_BASE_H_
