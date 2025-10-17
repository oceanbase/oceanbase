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

  // vector hold will backup/restore `batch_size` rows
  // operator must guarantee that all changes will happened within [0, batch_size] rows
  // otherwise, undefined behaviors are expected.
  // if vector holder is inited by following interface, use `save_actual_rows` to backup rows
  int init_for_actual_rows(const common::ObIArray<ObExpr *> &exprs, const int32_t batch_size, ObEvalCtx &eval_ctx);
  int save(const int64_t batch_size);
  int restore() const;
  int restore_single_row(int64_t from_idx, int64_t to_idx) const;
  int rows_extend(int64_t src_start_idx, int64_t src_end_idx, int64_t start_dst_idx, int64_t size);
  int driver_row_extend(int64_t from_idx, int64_t start_dst_idx, int64_t times);
  int driver_rows_extend(const ObBatchRows *driver_brs, int64_t &from_idx, int64_t &extend_rows_cnt, int64_t times);
  int driven_rows_extend(int64_t from_idx, int64_t end_idx, int64_t start_dst_idx, int64_t times);
  void clear_saved_size() { saved_size_ = 0; }

  void reset()
  {
    saved_ = false;
    saved_size_ = 0;
  }
  void destroy();
  static int calc_backup_size(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &eval_ctx, int32_t &mem_size);
  int check_vec_modified(const ObBitVector &skip);
private:
  int inner_init(const common::ObIArray<ObExpr *> &exprs, ObEvalCtx &eval_ctx, const int64_t max_row_cnt);
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
    VectorFormat get_extend_vec_format() const;
    int save(ObIAllocator &alloc, const int64_t batch_size, ObEvalCtx *eval_ctx);
    int restore(const int64_t saved_size, ObEvalCtx *eval_ctx);
    void restore_fixed_base_single_row(ObFixedLengthBase &vec, int64_t from_idx, int64_t to_idx) const;
    void restore_discrete_base_single_row(ObDiscreteBase &vec, int64_t from_idx, int64_t to_idx) const;
    void restore_continuous_base_single_row(ObVectorBase &vec, int64_t from_idx, int64_t to_idx, VectorFormat dst_fmt) const;
    void restore_uniform_base_single_row(ObUniformBase &vec, int64_t from_idx, int64_t to_idx, bool is_const) const;
    void extend_fixed_base_vector(ObFixedLengthBase &vec, const int64_t src_start_idx, const int64_t srt_end_idx, const int64_t size, const int64_t start_dst_idx) const;
    void extend_discrete_base_vector(ObDiscreteBase &vec, const int64_t src_start_idx, const int64_t srt_end_idx, const int64_t size, const int64_t start_dst_idx) const;
    void extend_uniform_base_vector(ObUniformBase &vec, const int64_t src_start_idx, const int64_t srt_end_idx, const int64_t size, const int64_t start_dst_idx, const bool is_const) const;
    void extend_continuous_base_vector(ObVectorBase &vec, const int64_t src_start_idx, const int64_t srt_end_idx, const int64_t size, const int64_t start_dst_idx, const VectorFormat dst_fmt) const;
    void convert_continuous_to_fixed(ObFixedLengthBase &vec, const int64_t from_idx, const int64_t to_idx) const;
    void convert_continuous_to_discrete(ObDiscreteBase &vec, const int64_t from_idx, const int64_t to_idx) const;
    void convert_continuous_to_uniform(ObUniformBase &vec, const int64_t from_idx, const int64_t to_idx) const;
    int check_uniform_base(const ObUniformBase &vec, const int64_t batch_size,
                           bool is_const, ObEvalCtx &eval_ctx, const ObBitVector &skip);
    int check_fixed_base(const ObFixedLengthBase &vec, const int64_t batch_size,
                         ObEvalCtx &eval_ctx);
    int check_discrete_base(const ObDiscreteBase &vec, const int64_t batch_size,
                            ObEvalCtx &eval_ctx);
    int check_continuous_base(const ObContinuousBase &vec, const int64_t batch_size,
                            ObEvalCtx &eval_ctx);
    int check_bitmap_null_base(const ObBitmapNullVectorBase &vec, const int64_t batch_size,
                              ObEvalCtx &eval_ctx);
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
