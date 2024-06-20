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

#ifndef OCEANBASE_SQL_ENGINE_SORT_SORT_EXPRS_GETTER_VEC_OP_H_
#define OCEANBASE_SQL_ENGINE_SORT_SORT_EXPRS_GETTER_VEC_OP_H_

#include "sql/engine/sort/ob_sort_basic_info.h"
#include "sql/engine/sort/ob_sort_compare_vec_op.h"

namespace oceanbase
{
namespace sql
{
class ObSortKeyFetcher
{
public:
  ObSortKeyFetcher(ObIAllocator &allocator) :
    allocator_(allocator), max_batch_size_(0), sk_col_res_list_(nullptr), sk_vec_ptrs_(allocator)
  {}
  ~ObSortKeyFetcher();
  void reset();
  int init(const common::ObIArray<ObExpr *> &sk_exprs,
           const ObIArray<ObSortFieldCollation> &sort_collations, const int64_t batch_size,
           ObEvalCtx &eval_ctx);
  const SortKeyColResult *get_sk_col_result_list() const
  {
    return sk_col_res_list_;
  }
  int fetch_payload(const ObBatchRows &input_brs);
  int fetch_payload(const uint16_t selector[], const int64_t selector_size);

private:
  int init_sk_col_result_list(const int64_t sk_cnt, const int64_t batch_size);
  int fetch_fixed_payload(const ObFixedLengthBase &vec, const uint16_t selector[],
                          const int64_t selector_size, SortKeyColResult &col_result);
  int fetch_fixed_payload(const ObFixedLengthBase &vec, const ObBatchRows &input_brs,
                          SortKeyColResult &col_result);
  int fetch_discrete_payload(const ObDiscreteBase &vec, const ObBatchRows &input_brs,
                             SortKeyColResult &col_result);
  int fetch_discrete_payload(const ObDiscreteBase &vec, const uint16_t selector[],
                             const int64_t selector_size, SortKeyColResult &col_result);
  int fetch_continuous_payload(const ObContinuousBase &vec, const ObBatchRows &input_brs,
                               SortKeyColResult &col_result);
  int fetch_continuous_payload(const ObContinuousBase &vec, const uint16_t selector[],
                               const int64_t selector_size, SortKeyColResult &col_result);
  int fetch_uniform_payload(const ObUniformBase &vec, const bool is_const,
                            const ObBatchRows &input_brs, SortKeyColResult &col_result);
  int fetch_uniform_payload(const ObUniformBase &vec, const bool is_const,
                            const uint16_t selector[], const int64_t selector_size,
                            SortKeyColResult &col_result);

private:
  ObIAllocator &allocator_;
  int64_t max_batch_size_;
  SortKeyColResult *sk_col_res_list_;
  common::ObFixedArray<ObIVector *, common::ObIAllocator> sk_vec_ptrs_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_SQL_ENGINE_SORT_SORT_EXPRS_GETTER_VEC_OP_H_ */
