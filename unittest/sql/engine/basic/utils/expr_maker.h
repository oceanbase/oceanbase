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
#ifndef OCEANBASE_UNITTEST_SQL_ENGINE_BASIC_UTILS_EXPR_MAKER_H_
#define OCEANBASE_UNITTEST_SQL_ENGINE_BASIC_UTILS_EXPR_MAKER_H_

#include <gtest/gtest.h>
#include <optional>
#include <string>
#include <vector>
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/ob_batch_rows.h"

namespace oceanbase
{
namespace sql
{
// Base interface for creating test expressions and preparing frame layout.
class ExprMaker
{
public:
  virtual ~ExprMaker() = default;
  // Assigns frame index before make()/gen() are used.
  virtual void set_frame_idx(int64_t frame_idx) = 0;
  // Returns the frame index used by this expression in ObEvalCtx::frames_.
  // Example: if this returns 1, all offsets produced by make() are based on frames_[1].
  virtual int64_t frame_idx() const = 0;
  // Returns required bytes for one frame block before make().
  // Parameter meaning: N/A (uses maker's ctor parameters).
  // Example: caller allocates frame_mem_size() bytes and zeroes them.
  virtual int64_t frame_mem_size() const = 0;
  // Initializes expression metadata and in-frame offsets so runtime eval can write vectors.
  // @param expr [out] expression object to be configured
  // @param eval_ctx [in/out] contains frame array where offsets point to
  // Example:
  //   maker.make(expr, eval_ctx);  // then expr.init_vector(...) can be called safely.
  virtual void make(ObExpr &expr, ObEvalCtx &eval_ctx) = 0;
};

// Concrete base with shared frame_idx, vec_format, layout helpers and related members.
// Subclasses use layout_frame() and compute_frame_mem_size() for frame setup.
// bitmap_count: VEC_FIXED -> 3 (eval_flags, pvt_skip, null_bitmap); others -> 2.
class ExprMakerImpl : public ExprMaker
{
protected:
  ExprMakerImpl(common::VectorFormat vec_format = common::VEC_UNIFORM_CONST)
      : frame_idx_(-1),
        vec_format_(vec_format)
  {}

  void set_frame_idx(int64_t frame_idx) override { frame_idx_ = frame_idx; }
  int64_t frame_idx() const override { return frame_idx_; }

  // Returns how many bitmaps are needed by the target vector format.
  // @param format vector format (e.g. VEC_FIXED, VEC_UNIFORM)
  // Example: VEC_FIXED -> 3, VEC_UNIFORM -> 2.
  static int64_t bitmap_count_for_format(common::VectorFormat format);

  // Shared layout: datum_off, eval_info_off, eval_flags_off, pvt_skip_off,
  // vector_header_off, [len_arr_off, ptr_arr_off, null_bitmap_off for VEC_DISCRETE],
  // [null_bitmap_off for VEC_FIXED], res_buf_off.
  // @param expr [out] expression to receive all *_off_ fields and metadata flags
  // @param eval_ctx [in/out] owns frame memory referenced by computed offsets
  // @param frame_idx frame slot to layout into
  // @param datum_count datum entry count to reserve in frame
  // @param skip_size logical row capacity for bit-vectors
  // @param vec_format target vector memory format
  // @param res_buf_size reserved payload bytes for values
  // Example:
  //   layout_frame(expr, eval_ctx, 0, 16, 16, VEC_DISCRETE, 16 * 32);
  static void layout_frame(ObExpr &expr, ObEvalCtx &eval_ctx, int64_t frame_idx,
                           int64_t datum_count, int64_t skip_size,
                           common::VectorFormat vec_format, int64_t res_buf_size);

  // Calculates frame byte size with the same rules as layout_frame().
  // @param datum_count number of ObDatum slots
  // @param skip_size row capacity used by skip/eval/null bitmaps
  // @param vec_format vector format deciding extra arrays (offset/len/ptr)
  // @param res_buf_size payload bytes for value storage
  // Example:
  //   int64_t bytes = compute_frame_mem_size(8, 8, VEC_FIXED, 8 * sizeof(int64_t));
  static int64_t compute_frame_mem_size(int64_t datum_count, int64_t skip_size,
                                        common::VectorFormat vec_format,
                                        int64_t res_buf_size);

  int64_t frame_idx_;
  common::VectorFormat vec_format_;
};

// Expression maker that can also produce one output batch (values + skip bits).
// gen() reads batch_size/skip from brs, writes skip/all_rows_active/size/end to brs.
class GenExprMaker : public ExprMakerImpl
{
public:
  virtual ~GenExprMaker() = default;
  // Default frame size used by integer-style generators.
  // Parameter meaning: N/A (derived from batch_size_ and vec_format_).
  int64_t frame_mem_size() const override;
  // Sets up expression to produce batch result vectors.
  // @param expr [out] configured with frame offsets + datum pointers
  // @param eval_ctx [in/out] frame memory owner
  // Example:
  //   maker.make(expr, eval_ctx);
  //   maker.gen(expr, brs, eval_ctx);
  void make(ObExpr &expr, ObEvalCtx &eval_ctx) override;
  // Produces one batch of values and updates brs metadata.
  // @param expr expression that receives vector values
  // @param brs [in/out] input requested size; output carries size/end/skip/all_rows_active
  // @param eval_ctx runtime frame context
  // Example:
  //   brs.size_ = 4; maker.gen(expr, brs, eval_ctx);  // returns <= 4 rows.
  virtual void gen(ObExpr &expr, ObBatchRows &brs, ObEvalCtx &eval_ctx) = 0;

protected:
  GenExprMaker(int64_t batch_size, common::VectorFormat vec_format)
      : ExprMakerImpl(vec_format),
        batch_size_(batch_size)
  {}

  // Fills expr datum metadata (type, collation, vec tc, basic funcs).
  // @param expr [in/out] expression metadata holder
  // Example:
  //   int64 generator sets type=ObIntType, vec tc=VEC_TC_INTEGER.
  virtual void setup_expr_meta(ObExpr &expr) = 0;

  int64_t batch_size_;
};

// Generates integer sequence values: row_id, row_id + 1, ...
// vec_format: VEC_FIXED (default) or VEC_UNIFORM.
// Each gen() produces brs.size_ numbers. If row_id_ >= max_row_count, sets brs.end_=true.
// e.g SeqIntGenExprMaker(batch_size, max_row_count=6)
//     gen with brs.size_=3 -> [0,1,2]; gen with brs.size_=3 -> [3,4,5]; gen -> brs.end_=true
class SeqIntGenExprMaker : public GenExprMaker
{
public:
  SeqIntGenExprMaker(int64_t batch_size, int64_t max_row_count,
                     common::VectorFormat vec_format = common::VEC_FIXED)
      : GenExprMaker(batch_size, vec_format),
        max_row_count_(max_row_count),
        row_id_(0)
  {}
  // Returns frame bytes for max(batch_size_, max_row_count_) integer values.
  int64_t frame_mem_size() const override;
  // Emits integer sequence [row_id_, row_id_ + batch_size).
  // @param expr expression to write values into
  // @param brs [in/out] requested size in; produced size/end out
  // @param eval_ctx frame/evaluation context
  // Example:
  //   max_row_count=5, brs.size_=3 -> output {0,1,2}, next call -> {3,4}, end=true.
  void gen(ObExpr &expr, ObBatchRows &brs, ObEvalCtx &eval_ctx) override;

protected:
  // Configures int64 expression metadata for sequence output.
  void setup_expr_meta(ObExpr &expr) override;

private:
  int64_t max_row_count_;
  int64_t row_id_;
};

// Generates fixed batches of values. Each inner vector is returned by one gen() call.
// std::optional for null: nullopt = null value; has_value = row value.
// skip_mask[i][j]=true means row j in batch i is skipped (optional, default no skips).
// vec_format: VEC_FIXED (default) or VEC_UNIFORM for int64_t; VEC_DISCRETE for std::string.
// e.g. FixedGenExprMaker(batch_size, {{1, 2, 3}, {4, 5, 6}})
//      gen() #1 -> {1, 2, 3}, gen() #2 -> {4, 5, 6}. Extra gen() calls set brs.end_=true.
template <typename T>
class FixedGenExprMaker : public GenExprMaker
{
public:
  FixedGenExprMaker(int64_t batch_size,
                    const std::vector<std::vector<std::optional<T>>> &values,
                    common::VectorFormat vec_format = common::VEC_FIXED,
                    const std::vector<std::vector<bool>> &skip_mask = {})
      : GenExprMaker(batch_size, vec_format),
        values_(values),
        skip_mask_(skip_mask),
        batch_idx_(0)
  {
    if (skip_mask.size() > 0) {
      EXPECT_EQ(values.size(), skip_mask.size());
    }
  }
  // Returns frame bytes required by this generator.
  // For string type, reserves larger payload buffer for variable-length data.
  int64_t frame_mem_size() const override;
  // Builds frame offsets/metadata and binds datum pointers.
  // @param expr [out] expression initialized for target type/format
  // @param eval_ctx [in/out] frame memory context
  // Example:
  //   maker.make(expr, eval_ctx);  // ready for repeated gen() calls.
  void make(ObExpr &expr, ObEvalCtx &eval_ctx) override;
  // Emits one predefined batch from values_[batch_idx_].
  // @param expr expression to write to
  // @param brs [in/out] input capacity; output batch size/end/skip
  // @param eval_ctx runtime context
  // Example:
  //   values={{1,2},{3}} => first gen outputs {1,2}, second outputs {3}, third end=true.
  void gen(ObExpr &expr, ObBatchRows &brs, ObEvalCtx &eval_ctx) override;

protected:
  // Runtime cache used while filling one batch.
  struct GenRuntimeCtx
  {
    ObIVector *vec_;
    ObDatum *datums_;
    char *res_buf_;
    uint32_t *offsets_;
    uint32_t cur_;
  };

  // Configures expression metadata by template type (int64/string).
  void setup_expr_meta(ObExpr &expr) override;
  // Initializes vector object + runtime pointers used by row-level setters.
  // @param expr expression that owns vector and frame offsets
  // @param brs current batch metadata
  // @param eval_ctx runtime context
  // @param batch_size rows to materialize in this call
  // @param rt [out] helper cache for row writing
  // Example:
  //   prepare_gen_runtime(...); set_row_value(rt, 0, 100); finish_gen_runtime(...).
  void prepare_gen_runtime(ObExpr &expr, ObBatchRows &brs, ObEvalCtx &eval_ctx,
                           int64_t batch_size, GenRuntimeCtx &rt) const;
  // Marks one row as skipped in brs.
  // @param brs batch row flags
  // @param row_idx row index to skip
  void set_row_skipped(ObBatchRows &brs, int64_t row_idx) const;
  // Writes one row value (or null) to vector and backing storage.
  // @param rt runtime cache from prepare_gen_runtime()
  // @param row_idx destination row index
  // @param opt optional value; nullopt means output NULL
  // Example:
  //   set_row_value(rt, 2, std::optional<int64_t>(42));
  void set_row_value(GenRuntimeCtx &rt, int64_t row_idx,
                     const std::optional<T> &opt) const;
  // Finalizes evaluated flags for produced rows.
  // @param expr expression with evaluated bit-vector
  // @param brs batch skip/all-active status
  // @param eval_ctx runtime context
  // @param batch_size produced row count
  void finish_gen_runtime(ObExpr &expr, const ObBatchRows &brs,
                          ObEvalCtx &eval_ctx, int64_t batch_size) const;

private:
  std::vector<std::vector<std::optional<T>>> values_;
  std::vector<std::vector<bool>> skip_mask_;
  int64_t batch_idx_;
};
extern template class FixedGenExprMaker<int64_t>;
extern template class FixedGenExprMaker<std::string>;

// Generates one logical stream of values. Each gen() returns the next N values,
// where N is current brs.size_ requested by the caller.
// skip_mask[i]=true means element i is skipped in output batch.
template <typename T>
class AllAtOnceGenExprMaker : public FixedGenExprMaker<T>
{
public:
  AllAtOnceGenExprMaker(int64_t batch_size,
                        const std::vector<T> &values,
                        common::VectorFormat vec_format = common::VEC_FIXED,
                        const std::vector<bool> &skip_mask = {})
      : FixedGenExprMaker<T>(batch_size, {}, vec_format, {}),
        values_(values),
        skip_mask_(skip_mask),
        cursor_(0)
  {
    if (!skip_mask_.empty()) {
      EXPECT_EQ(values_.size(), skip_mask_.size());
    }
  }
  // Emits rows from one flattened stream `values_` using a moving cursor.
  // @param expr expression to write to
  // @param brs [in/out] requested capacity; output has produced size/end/skip
  // @param eval_ctx runtime frame context
  // Example:
  //   values={a,b,c,d}, brs.size_=2 -> first gen {a,b}, second gen {c,d}, then end=true.
  void gen(ObExpr &expr, ObBatchRows &brs, ObEvalCtx &eval_ctx) override;

private:
  std::vector<T> values_;
  std::vector<bool> skip_mask_;
  int64_t cursor_;
};
extern template class AllAtOnceGenExprMaker<int64_t>;
extern template class AllAtOnceGenExprMaker<std::string>;

// Builds uniform constant expression (VEC_UNIFORM_CONST).
class ConstIntExprMaker : public ExprMakerImpl
{
public:
  ConstIntExprMaker(int64_t value, int64_t batch_size)
      : ExprMakerImpl(common::VEC_UNIFORM_CONST),
        value_(value),
        batch_size_(batch_size)
  {}
  // Returns frame bytes for one constant datum and per-batch metadata.
  int64_t frame_mem_size() const override;
  // Initializes an expression that always evaluates to the same int64 value.
  // @param expr [out] expression with const eval function
  // @param eval_ctx [in/out] frame context storing constant payload
  // Example:
  //   ConstIntExprMaker(7, 16) -> every row evaluation yields 7.
  void make(ObExpr &expr, ObEvalCtx &eval_ctx) override;

private:
  int64_t value_;
  int64_t batch_size_;
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_UNITTEST_SQL_ENGINE_BASIC_UTILS_EXPR_MAKER_H_
