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

#ifndef OCEANBASE_BASIC_OB_VECTOR_RESULT_HOLDER_H_
#define OCEANBASE_BASIC_OB_VECTOR_RESULT_HOLDER_H_

#include "lib/container/ob_se_array.h"
#include "lib/allocator/page_arena.h"
#include "share/datum/ob_datum.h"
#include "sql/engine/expr/ob_expr.h"
#include "share/vector/type_traits.h"
#include "share/vector/ob_i_vector.h"

namespace oceanbase
{

namespace common {
  class ObVectorBase;
  class ObBitmapNullVectorBase;
  class ObFixedLengthBase;
  class ObContinuousBase;
  class ObDiscreteBase;
  class ObUniformBase;
}
namespace sql
{

class ObVectorsResultHolder
{
public:
  ObVectorsResultHolder(ObIAllocator *tmp_alloc = nullptr) :
    exprs_(nullptr), eval_ctx_(nullptr), inited_(false), backup_cols_(nullptr), saved_(false),
    saved_size_(0), tmp_alloc_(tmp_alloc)
  {}
  int init(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &eval_ctx);
  int save(const int64_t batch_size);
  int restore() const;
  int restore_single_row(int64_t from_idx, int64_t to_idx) const;
  int drive_row_extended(int64_t from_idx, int64_t start_dst_idx, int64_t size);
  VectorFormat get_single_row_restore_format(VectorFormat src_format, const ObExpr *expr) const
  {
    // continuous format don't support restore single row, so if the backup vector is continuous format, we need to convert it to other format
    return src_format == VEC_CONTINUOUS ? expr->get_default_res_format() : src_format;
  }
  void clear_saved_size() { saved_size_ = 0; }

  void reset()
  {
    saved_ = false;
    saved_size_ = 0;
  }
  void destroy();
  static int calc_backup_size(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &eval_ctx, int32_t &mem_size);
private:
  template<VectorFormat>
  static int calc_col_backup_size(ObExpr *expr, int32_t batch_size, int32_t &mem_size);
  struct ObColResultHolder
  {
    ObColResultHolder(int64_t max_batch_size, const ObExpr *expr) :
                                                header_(), max_row_cnt_(max_batch_size), nulls_(nullptr),
                                                has_null_(false), datums_(nullptr), len_(-1),
                                                data_(nullptr), lens_(nullptr), ptrs_(nullptr),
                                                offsets_(nullptr), continuous_data_(nullptr),
                                                expr_(expr), frame_nulls_(nullptr), frame_datums_(nullptr),
                                                frame_data_(nullptr), frame_lens_(nullptr), frame_ptrs_(nullptr),
                                                frame_offsets_(nullptr), frame_continuous_data_(nullptr) {}
    void reset(common::ObIAllocator &alloc);
    int copy_vector_base(const ObVectorBase &vec);
    int copy_bitmap_null_base(const ObBitmapNullVectorBase &vec,
                              common::ObIAllocator &alloc,
                              const int64_t batch_size,
                              ObEvalCtx &eval_ctx);
    int copy_fixed_base(const ObFixedLengthBase &vec,
                        common::ObIAllocator &alloc,
                        const int64_t batch_size,
                        ObEvalCtx &eval_ctx);
    int copy_discrete_base(const ObDiscreteBase &vec,
                           common::ObIAllocator &alloc,
                           const int64_t batch_size,
                           ObEvalCtx &eval_ctx);
    int copy_continuous_base(const ObContinuousBase &vec,
                             common::ObIAllocator &alloc,
                             const int64_t batch_size,
                             ObEvalCtx &eval_ctx);
    int copy_uniform_base(const ObExpr *expr, const ObUniformBase &vec,
                          bool is_const, ObEvalCtx &eval_ctx,
                          common::ObIAllocator &alloc,
                          const int64_t batch_size);

    void restore_vector_base(ObVectorBase &vec) const;
    void restore_bitmap_null_base(ObBitmapNullVectorBase &vec, const int64_t batch_size, ObEvalCtx &eval_ctx) const;

    
    void restore_fixed_base(ObFixedLengthBase &vec, const int64_t batch_size, ObEvalCtx &eval_ctx) const;
    void restore_discrete_base(ObDiscreteBase &vec, const int64_t batch_size, ObEvalCtx &eval_ctx) const;
    void restore_continuous_base(ObContinuousBase &vec, const int64_t batch_size, ObEvalCtx &eval_ctx) const;
    void restore_uniform_base(const ObExpr *expr, ObUniformBase &vec,
                              bool is_const, ObEvalCtx &eval_ctx,
                              const int64_t batch_size) const;
    int save(ObIAllocator &alloc, const int64_t batch_size, ObEvalCtx *eval_ctx);
    int restore(const int64_t saved_size, ObEvalCtx *eval_ctx);
    
    void restore_bitmap_null_base_single_row(ObBitmapNullVectorBase &vec, int64_t from_idx, int64_t to_idx, ObEvalCtx &eval_ctx) const;
    void restore_base_single_row(ObVectorBase &vec, int64_t from_idx, int64_t to_idx, ObEvalCtx &eval_ctx) const;
    void restore_fixed_base_single_row(ObFixedLengthBase &vec, int64_t from_idx, int64_t to_idx, ObEvalCtx &eval_ctx) const;
    void restore_discrete_base_single_row(ObDiscreteBase &vec, int64_t from_idx, int64_t to_idx, ObEvalCtx &eval_ctx) const;
    void restore_continuous_base_single_row(ObExpr *expr, int64_t from_idx, int64_t to_idx, VectorFormat dst_fmt,ObEvalCtx &eval_ctx) const;
    void restore_uniform_base_single_row(const ObExpr *expr, ObUniformBase &vec,
                              int64_t from_idx, int64_t to_idx, ObEvalCtx &eval_ctx, bool is_const) const;
    int restore_nested_single_row(const ObExpr &expr, ObEvalCtx &eval_ctx, const VectorFormat extend_format,
                                  int64_t from_idx, int64_t to_idx) const;
    int extend_nested_rows(const ObExpr &expr, ObEvalCtx &eval_ctx, const VectorFormat extend_format,
                                 int64_t from_idx, int64_t start_dst_idx, int64_t size) const;
    
    void extend_fixed_base_vector(ObFixedLengthBase &vec, int64_t from_idx, int64_t start_dst_idx, int64_t size, ObEvalCtx &eval_ctx) const;

    void extend_discrete_base_vector(ObDiscreteBase &vec, int64_t from_idx, int64_t start_dst_idx, int64_t size, ObEvalCtx &eval_ctx) const;

    void extend_uniform_base_vector(const ObExpr *expr, ObUniformBase &vec, int64_t from_idx, int64_t start_dst_idx, int64_t size, ObEvalCtx &eval_ctx,  bool is_const) const;

    void extend_continuous_base_vector(ObExpr *expr, int64_t from_idx, int64_t start_dst_idx, int64_t size, VectorFormat dst_fmt, ObEvalCtx &eval_ctx) const;

    void convert_continuous_to_fixed(ObFixedLengthBase &vec, int64_t from_idx, int64_t to_idx, ObEvalCtx &eval_ctx) const;
    void convert_continuous_to_discrete(ObDiscreteBase &vec, int64_t from_idx, int64_t to_idx, ObEvalCtx &eval_ctx) const;
    void convert_continuous_to_uniform(ObExpr *expr, int64_t from_idx, int64_t to_idx, ObEvalCtx &eval_ctx) const;
    VectorHeader header_;
    int64_t max_row_cnt_;  //ObVectorBase

    sql::ObBitVector *nulls_; //ObBitmapNullVectorBase
    bool has_null_; //ObBitmapNullVectorBase

    ObDatum *datums_; //ObUniformBase

    ObLength len_; //ObFixedLengthBase
    char *data_; //ObFixedLengthBase

    ObLength *lens_; //ObDiscreteBase
    char **ptrs_; //ObDiscreteBase

    uint32_t *offsets_; //ObContinuousBase
    char *continuous_data_; //ObContinuousBase
    const ObExpr *expr_;
    //expr frame info
    sql::ObBitVector *frame_nulls_;  //ObBitmapNullVectorBase
    ObDatum *frame_datums_; //ObUniformBase
    char *frame_data_; //ObUniformBase
    ObLength *frame_lens_; //ObDiscreteBase
    char **frame_ptrs_; //ObDiscreteBase
    uint32_t *frame_offsets_; //ObContinuousBase
    char *frame_continuous_data_; //ObContinuousBase
  };
  const common::ObIArray<ObExpr *> *exprs_;
  ObEvalCtx *eval_ctx_;
  bool inited_;
  ObColResultHolder *backup_cols_;
  bool saved_;
  int64_t saved_size_;
  ObIAllocator *tmp_alloc_;
};


} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_BASIC_OB_VECTOR_RESULT_HOLDER_H_
