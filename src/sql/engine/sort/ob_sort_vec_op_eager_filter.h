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

#ifndef OCEANBASE_SQL_ENGINE_SORT_SORT_VEC_OP_EAGER_FILTER_H_
#define OCEANBASE_SQL_ENGINE_SORT_SORT_VEC_OP_EAGER_FILTER_H_
#include "sql/engine/sort/ob_sort_compare_vec_op.h"
#include "sql/engine/sort/ob_sort_vec_op_store_row_factory.h"
namespace oceanbase {
namespace sql {
// ObSortVecOpEagerFilter implementation class
template <typename Compare, typename Store_Row, bool has_addon>
class ObSortVecOpEagerFilter {
public:
  static const int64_t MAX_BUCKET_NUM = 100;
  static const int64_t MIN_BUCKET_NUM = 10;
  static constexpr double FILTER_RATIO = 0.05; // limit the ratio of memory used

  typedef common::ObBinaryHeap<Store_Row *, Compare, MAX_BUCKET_NUM> BucketHeap;

  explicit ObSortVecOpEagerFilter(
      common::ObIAllocator &allocator,
      ObSortVecOpStoreRowFactory<Store_Row, has_addon> &store_row_factory)
      : allocator_(allocator), bucket_num_(0), bucket_size_(0),
        store_row_factory_(store_row_factory), bucket_heap_(nullptr),
        comp_(nullptr), is_inited_(false), is_by_pass_(false), output_brs_() {}

  int filter(common::ObFixedArray<ObExpr *, common::ObIAllocator> &exprs,
             ObEvalCtx &eval_ctx, const int64_t start_pos,
             const ObBatchRows &input_brs);
  int update_filter(const Store_Row *dumped_bucket, bool &updated);

private:
  int init_output_brs(const int64_t max_batch_size);

public:
  int init(Compare &comp, const int64_t dumped_rows_cnt, const int64_t topn_cnt,
           const int64_t max_batch_size) {
    int ret = OB_SUCCESS;
    if (is_inited()) {
      ret = OB_INIT_TWICE;
      SQL_ENG_LOG(WARN, "ObSortVecOpEagerFilter has already been initialized", K(ret));
    } else if (!comp.is_inited() ||
               OB_ISNULL(bucket_heap_ =
                             OB_NEWx(BucketHeap, &allocator_, comp)) ||
               topn_cnt < 0) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "failed to init ObSortVecOpEagerFilter", K(ret));
    } else {
      comp_ = &comp;
      bucket_num_ =
          min(static_cast<int64_t>(floor(dumped_rows_cnt * FILTER_RATIO)),
              MAX_BUCKET_NUM);
      // if the number of buckets is very small (e.g. 1), then this optimization
      // is unnecessary
      bucket_num_ = bucket_num_ < MIN_BUCKET_NUM ? 0 : bucket_num_;
      if (bucket_num_ != 0) {
        bucket_size_ = (topn_cnt + bucket_num_ - 1) / bucket_num_;
        if (OB_FAIL(init_output_brs(max_batch_size))) {
          SQL_ENG_LOG(WARN, "init ouput batch rows failed", K(ret));
        }
      } else {
        is_by_pass_ = true;
        SQL_ENG_LOG(INFO, "no need to use filter ", K(dumped_rows_cnt),
                    K(topn_cnt));
      }
      is_inited_ = true;
    }
    return ret;
  }
  inline bool is_inited() const { return is_inited_; }
  inline bool is_by_pass() const { return is_by_pass_; }
  inline const ObBatchRows &get_output_brs() const { return output_brs_; }
  int64_t bucket_size() const { return bucket_size_; }
  void reset();

protected:
  common::ObIAllocator &allocator_;
  int64_t bucket_num_;
  int64_t bucket_size_;
  ObSortVecOpStoreRowFactory<Store_Row, has_addon> &store_row_factory_;
  BucketHeap *bucket_heap_;
  Compare *comp_;
  bool is_inited_;
  bool is_by_pass_;
  ObBatchRows output_brs_;
};
} // end namespace sql
} // end namespace oceanbase
#endif /* OCEANBASE_SQL_ENGINE_SORT_SORT_VEC_OP_EAGER_FILTER_H_ */