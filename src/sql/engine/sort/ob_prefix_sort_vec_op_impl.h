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

#ifndef OCEANBASE_SQL_ENGINE_SORT_PREFIX_SORT_VEC_OP_IMPL_H_
#define OCEANBASE_SQL_ENGINE_SORT_PREFIX_SORT_VEC_OP_IMPL_H_

#include "sql/engine/sort/ob_sort_vec_op_impl.h"
#include "sql/engine/basic/ob_vector_result_holder.h"


namespace oceanbase {
namespace sql {
#define SK_UPCAST_PP(store_row) reinterpret_cast<ObCompactRow **>(store_row)
#define SK_DOWNCAST_P(store_row) static_cast<Store_Row *>(store_row)

// Vectorization 2.0 prefix sort implementation class
template <typename Compare, typename Store_Row, bool has_addon>
class ObPrefixSortVecImpl final : public ObSortVecOpImpl<Compare, Store_Row, has_addon>
{
  using CopyableComparer = typename ObSortVecOpImpl<Compare, Store_Row, has_addon>::CopyableComparer;

public:
  explicit ObPrefixSortVecImpl(ObMonitorNode &op_monitor_info, lib::MemoryContext &mem_context) :
    ObSortVecOpImpl<Compare, Store_Row, has_addon>(op_monitor_info, mem_context),
    full_sk_collations_(nullptr), base_sk_collations_(), prev_row_(nullptr),
    next_prefix_row_(nullptr), child_(nullptr), self_op_(nullptr), sort_row_count_(nullptr),
    selector_(nullptr), selector_size_(0), sort_prefix_rows_(0), prefix_pos_(0),
    im_sk_store_(&allocator_), im_addon_store_(&allocator_),
    im_sk_rows_(nullptr), im_addon_rows_(nullptr), immediate_pos_(0),
    brs_(nullptr)
  {}
  virtual ~ObPrefixSortVecImpl()
  {
    reset();
  }
  virtual void reset() override;
  virtual int init(ObSortVecOpContext &context) override;
  int get_next_batch(const int64_t max_cnt, int64_t &read_rows);
  int64_t get_prefix_pos() const
  {
    return prefix_pos_;
  }

protected:
#define USING_SORT_IMPL(name) using ObSortVecOpImpl<Compare, Store_Row, has_addon>::name;

  LST_DO_CODE(USING_SORT_IMPL, init, is_inited, add_batch, get_next_batch,
              get_next_batch_stored_rows, init_temp_row_store, adjust_topn_read_rows, attach_rows,
              allocator_, mem_context_, eval_ctx_, cmp_sk_exprs_, sk_exprs_, all_exprs_,
              addon_exprs_, addon_vec_ptrs_, sk_vec_ptrs_, comp_, part_cnt_, topn_cnt_,
              outputted_rows_cnt_, cmp_sort_collations_, exec_ctx_, enable_encode_sortkey_,
              sk_rows_, addon_rows_, sk_store_, addon_store_, sk_row_meta_, addon_row_meta_);

  // fetch rows in same prefix && do sort, set %next_prefix_row_ to nullptr
  // when all child rows are fetched.
  int fetch_rows_batch();
  int is_same_prefix(const Store_Row *store_row, const RowMeta &row_meta,
                     const common::ObIArray<ObExpr *> &cmp_sk_exprs, const int64_t batch_idx,
                     bool &same);
  int is_same_prefix(const common::ObIArray<ObExpr *> &cmp_sk_exprs, const int64_t l_batch_idx,
                     const int64_t r_batch_idx, bool &same);
  int add_immediate_prefix(const uint16_t selector[], const int64_t row_size);
  int add_immediate_prefix_store(const int64_t pos, const uint16_t selector[],
                                 const int64_t row_size);
  void attach_im_perfix_store(const int64_t max_cnt, int64_t &read_rows);
  DISALLOW_COPY_AND_ASSIGN(ObPrefixSortVecImpl);

private:
  const ObIArray<ObSortFieldCollation> *full_sk_collations_;
  common::ObArrayHelper<ObSortFieldCollation> base_sk_collations_;
  const Store_Row *prev_row_;
  // when got new prefix, save the row to to %next_prefix_row_
  Store_Row *next_prefix_row_;
  ObOperator *child_;
  ObOperator *self_op_;
  int64_t *sort_row_count_;
  // members for batch interface
  uint16_t *selector_;
  uint16_t selector_size_;
  int64_t sort_prefix_rows_;
  int64_t prefix_pos_;
  ObTempRowStore im_sk_store_;
  ObTempRowStore im_addon_store_;
  Store_Row **im_sk_rows_;
  Store_Row **im_addon_rows_;
  int64_t immediate_pos_;
  const ObBatchRows *brs_;
  ObVectorsResultHolder brs_holder_;
};

} // end namespace sql
} // end namespace oceanbase

#include "ob_prefix_sort_vec_op_impl.ipp"

#endif /* OCEANBASE_SQL_ENGINE_SORT_PREFIX_SORT_VEC_OP_IMPL_H_ */
