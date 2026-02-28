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

#ifndef _OB_SQL_EXPR_CONCAT_H_
#define _OB_SQL_EXPR_CONCAT_H_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
class ObExprConcat : public ObStringExprOperator
{
public:
  class ObExprConcatContext : public ObExprOperatorCtx
  {
  public:
    ObExprConcatContext()
        : ObExprOperatorCtx(),
          buf_array_(NULL),
          res_lengths_(NULL),
          capacity_(0),
          allocator_(NULL)
    {}
    ~ObExprConcatContext()
    {
      reset();
    }
    void reset() {
      if (NULL != allocator_) {
        if (NULL != buf_array_) {
          allocator_->free(buf_array_);
          buf_array_ = NULL;
        }
        if (NULL != res_lengths_) {
          allocator_->free(res_lengths_);
          res_lengths_ = NULL;
        }
        allocator_ = NULL;
      }
      capacity_ = 0;
    }
    OB_INLINE char **get_buf_array() { return buf_array_; }
    OB_INLINE void set_buf_array(char **buf_array) { buf_array_ = buf_array; }
    OB_INLINE ObLength *get_res_lengths() { return res_lengths_; }
    OB_INLINE void set_res_lengths(ObLength *res_lengths) { res_lengths_ = res_lengths; }
    OB_INLINE int64_t get_capacity() { return capacity_; }
    OB_INLINE void set_capacity(int64_t capacity) { capacity_ = capacity; }
    OB_INLINE common::ObIAllocator *get_allocator() { return allocator_; }
    OB_INLINE void set_allocator(common::ObIAllocator *allocator) { allocator_ = allocator; }
    TO_STRING_KV(K_(buf_array), K_(res_lengths), K_(capacity));
    char **buf_array_;
    ObLength *res_lengths_;
    int64_t capacity_;
    common::ObIAllocator *allocator_;
  };
  virtual bool need_rt_ctx() const override { return true; }
public:
  explicit  ObExprConcat(common::ObIAllocator &alloc);
  virtual ~ObExprConcat();

  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const;
  static int calc(common::ObObj &result,
                  const common::ObObj &obj1,
                  const common::ObObj &obj2,
                  common::ObIAllocator *allocator,
                  const common::ObObjType result_type,
                  bool is_oracle_mode,
                  const int64_t max_result_len);
  // Check result length with %max_result_len (if %max_result_len greater than zero)
  // or max length of varchar.
  // %result type is set to varchar.
  static int calc(common::ObObj &result,
                  const common::ObString obj1,
                  const common::ObString obj2,
                  common::ObIAllocator *allocator,
                  bool is_oracle_mode,
                  const int64_t max_result_len);
  // Check result length with OB_MAX_PACKET_LENGTH.
  // %result type is set to ObLongTextType
  static int calc(common::ObObj &result,
                         const char *obj1_ptr,
                         const int32_t this_len,
                         const char *obj2_ptr,
                         const int32_t other_len,
                         common::ObIAllocator *allocator);
  static int calc_text(common::ObObj &result,
                                     const common::ObObj obj1,
                                     const common::ObObj obj2,
                                     ObIAllocator *allocator);

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  static int eval_concat(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);

  static int eval_concat_vector_with_text(const ObExpr &expr,
                                          ObEvalCtx &ctx,
                                          const ObBitVector &skip,
                                          const EvalBound &bound);

  template <typename ArgVec, typename ResVec, bool exist_text, bool ExistsNull, bool is_mysql_mode, bool all_rows_active>
  static int eval_concat_one_param(const ObExpr &expr,
                                   ObEvalCtx &ctx,
                                   const ObBitVector &skip,
                                   const EvalBound &bound);

  template <typename ArgVec, typename ResVec>
  static int eval_concat_vector_spec_vec(const ObExpr &expr,
                                         ObEvalCtx &ctx,
                                         const ObBitVector &skip,
                                         const EvalBound &bound);

  template <typename ArgVec>
  static int do_concat_memcpy_dispatch(const ObExpr &expr,
                                       ObEvalCtx &ctx,
                                       const ObBitVector &skip,
                                       const EvalBound &bound,
                                       char *buf_array[],
                                       ObLength res_lengths[],
                                       int64_t arg_idx);

  template <typename ArgVec, bool ExistsNull, bool all_rows_active, bool is_mysql_mode>
  static int do_concat_memcpy_inner(const ObExpr &expr,
                                    ObEvalCtx &ctx,
                                    const ObBitVector &skip,
                                    const EvalBound &bound,
                                    char *buf_array[],
                                    ObLength res_lengths[],
                                    int64_t arg_idx);

  template <typename ArgVec, bool ExistsNull, bool is_mysql_mode, bool all_rows_active>
  static int calc_concat_length_inner(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      const ObBitVector &skip,
                                      const EvalBound &bound,
                                      int64_t arg_idx,
                                      ObLength res_lengths[]);

  template <typename ArgVec>
  static int calc_concat_length_dispatch(const ObExpr &expr,
                                         ObEvalCtx &ctx,
                                         const ObBitVector &skip,
                                         const EvalBound &bound,
                                         int64_t arg_idx,
                                         ObLength res_lengths[]);

  template <typename ResVec, bool is_mysql_mode, bool all_rows_active>
  static int get_concat_result_buffer_inner(const ObExpr &expr,
                                            ObEvalCtx &ctx,
                                            const ObBitVector &skip,
                                            const EvalBound &bound,
                                            char *buf_array[],
                                            ObLength res_lengths[]);

  template <typename ResVec>
  static int get_concat_result_buffer_dispatch(const ObExpr &expr,
                                                ObEvalCtx &ctx,
                                                const ObBitVector &skip,
                                                const EvalBound &bound,
                                                char *buf_array[],
                                                ObLength res_lengths[]);

  static int eval_concat_text(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum, const int64_t &res_len);

  static int eval_concat_text_vector(const ObExpr &expr, ObEvalCtx &ctx, const int64_t &res_len, int64_t idx);

  static int eval_concat_vector_for_text(const ObExpr &expr,
                                         ObEvalCtx &ctx,
                                         const ObBitVector &skip,
                                         const EvalBound &bound);

  static int eval_concat_vector_by_column(const ObExpr &expr,
                                          ObEvalCtx &ctx,
                                          const ObBitVector &skip,
                                          const EvalBound &bound);

  static int eval_concat_vector(const ObExpr &expr,
                                ObEvalCtx &ctx,
                                const ObBitVector &skip,
                                const EvalBound &bound);

  // Get or create context and ensure capacity is sufficient
  static int get_or_create_concat_ctx(const ObExpr &expr,
                                       ObEvalCtx &ctx,
                                       int64_t required_capacity,
                                       ObExprConcatContext *&concat_ctx,
                                       char **&buf_array,
                                       ObLength *&res_lengths);

  DECLARE_SET_LOCAL_SESSION_VARS;

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprConcat);
};

}
}
#endif /* _OB_SQL_EXPR_CONCAT_H_ */
