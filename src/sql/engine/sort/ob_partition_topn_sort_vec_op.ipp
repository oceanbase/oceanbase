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

namespace oceanbase {
namespace sql {

/************************* start ObPartitionTopNSort **********************************/
template <typename Compare, typename Store_Row, bool has_addon>
void ObPartitionTopNSort<Compare, Store_Row, has_addon>::reset()
{
  reuse();
  if (OB_NOT_NULL(pt_buckets_)) {
    pt_buckets_->reset();
    allocator_.free(pt_buckets_);
    pt_buckets_ = nullptr;
  }
}

template <typename Compare, typename Store_Row, bool has_addon>
void ObPartitionTopNSort<Compare, Store_Row, has_addon>::reuse()
{
  topn_nodes_.reset();
  reuse_part_topn_node();
  cur_topn_node_idx_ = 0;
  part_group_cnt_ = 0;
  topn_node_ = nullptr;
  pt_row_buffer_.reuse();
  row_count_ = 0;
  got_first_row_ = false;
}

template <typename Compare, typename Store_Row, bool has_addon>
void ObPartitionTopNSort<Compare, Store_Row, has_addon>::reuse_part_topn_node() {
  if (OB_ISNULL(pt_buckets_)) {
    // do nothing
  } else {
    for (int64_t i = 0; i < pt_buckets_->count(); ++i) {
      PartTopnNode *hash_node = pt_buckets_->at(i);
      while (hash_node != NULL) {
        TopnNode *topn_node = &hash_node->topn_node_;
        if (OB_NOT_NULL(topn_node)) {
          topn_node->ties_array_.reset();
          topn_node->rows_array_.reset();
        }
        PartTopnNode *cur_node = hash_node;
        hash_node = hash_node->hash_node_next_;
        cur_node->~PartTopnNode();
        allocator_.free(cur_node);
        pt_buckets_->at(i) = nullptr;
      }
    }
    topn_node_ = nullptr;
  }
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObPartitionTopNSort<Compare, Store_Row, has_addon>::init(ObSortVecOpContext &ctx,
                                                             ObIAllocator *page_allocator,
                                                             ObIArray<ObExpr *> *all_exprs,
                                                             const RowMeta *sk_row_meta,
                                                             const RowMeta *addon_row_meta)
{
  int ret = OB_SUCCESS;
  page_allocator_ = page_allocator;
  sk_collations_ = ctx.sk_collations_;
  sk_row_meta_ = sk_row_meta;
  sk_exprs_ = ctx.sk_exprs_;
  addon_row_meta_ = addon_row_meta;
  addon_exprs_ = ctx.addon_exprs_;
  all_exprs_ = all_exprs;
  eval_ctx_ = ctx.eval_ctx_;
  max_bucket_cnt_ = 0;
  part_cnt_ = ctx.part_cnt_;
  topn_cnt_ = ctx.topn_cnt_;
  is_fetch_with_ties_ = ctx.is_fetch_with_ties_;
  row_count_ = 0;
  cur_topn_node_idx_ = 0;
  part_group_cnt_ = 0;
  hash_collision_cnt_ = 0;
  got_first_row_ = false;
  if (is_fetch_with_ties_) {
    add_topn_row_func_ = &ObPartitionTopNSort<Compare, Store_Row, has_addon>::add_quick_select_row_with_ties;
    topn_sort_func_ = &ObPartitionTopNSort<Compare, Store_Row, has_addon>::finish_in_quick_select;
  } else {
    add_topn_row_func_ = &ObPartitionTopNSort<Compare, Store_Row, has_addon>::add_quick_select_row;
    topn_sort_func_ = &ObPartitionTopNSort<Compare, Store_Row, has_addon>::finish_in_quick_select;
  }
  if (OB_FAIL(init_bucket_array(ctx.est_rows_))) {
    SQL_ENG_LOG(WARN, "failed to init partition topn", K(ret));
  } else {
    pt_row_buffer_.init(ctx.est_rows_);
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObPartitionTopNSort<Compare, Store_Row, has_addon>::init_bucket_array(const int64_t est_rows)
{
  int ret = OB_SUCCESS;
  uint64_t bucket_cnt = next_pow2((2 * est_rows < MIN_BUCKET_COUNT) ? MIN_BUCKET_COUNT :
                                  (2 * est_rows > MAX_BUCKET_COUNT) ? MAX_BUCKET_COUNT : 2 * est_rows);
  if (OB_FAIL(prepare_bucket_array(bucket_cnt, pt_buckets_))) {
    SQL_ENG_LOG(WARN, "failed to prepare bucket array", K(ret));
  } else {
    max_bucket_cnt_ = bucket_cnt;
    pt_row_buffer_.init(est_rows);
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObPartitionTopNSort<Compare, Store_Row, has_addon>::resize_buckets()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == max_bucket_cnt_) || OB_ISNULL(pt_buckets_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "get unexpected bucket", K(max_bucket_cnt_), K(pt_buckets_));
  } else {
    int64_t new_bucket_cnt = max_bucket_cnt_ * 2;
    uint64_t shift_right = __builtin_clzll(new_bucket_cnt) + 1;
    BucketArray *new_pt_buckets = nullptr;
    if (OB_FAIL(prepare_bucket_array(new_bucket_cnt, new_pt_buckets))) {
      SQL_ENG_LOG(WARN, "failed to prepare bucket array", K(ret));
    } else {
      Store_Row *top_row = NULL;
      for (int64_t idx = 0; OB_SUCC(ret) && idx < max_bucket_cnt_; ++idx) {
        PartTopnNode *hash_node = pt_buckets_->at(idx);
        PartTopnNode *next_hash_node = NULL;
        while(hash_node != NULL) {
          next_hash_node = hash_node->hash_node_next_;
          TopnNode &cur_node = hash_node->topn_node_;
          if (OB_UNLIKELY(0 == cur_node.rows_array_.count())) {
            ret = OB_ERR_UNEXPECTED;
            SQL_ENG_LOG(WARN, "get unexpected topn node rows_array", K(cur_node.rows_array_));
          } else {
            const uint64_t hash_value = cur_node.hash_val_;
            int64_t new_pos = hash_value >> shift_right;
            hash_node->hash_node_next_ = new_pt_buckets->at(new_pos);
            new_pt_buckets->at(new_pos) = hash_node;
          }
          hash_node = next_hash_node;
        }
      }
      if (OB_SUCC(ret)) {
        pt_buckets_->reset();
        allocator_.free(pt_buckets_);
        pt_buckets_ = new_pt_buckets;
        max_bucket_cnt_ = new_bucket_cnt;
      } else {
        new_pt_buckets->reset();
        allocator_.free(new_pt_buckets);
        new_pt_buckets = nullptr;
      }
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObPartitionTopNSort<Compare, Store_Row, has_addon>::add_batch(const ObBatchRows &input_brs,
                                                                  const int64_t start_pos,
                                                                  int64_t *append_row_count,
                                                                  bool need_load_data,
                                                                  common::ObIArray<Store_Row *> *&rows)
{
  int ret = OB_SUCCESS;
  int64_t row_count = 0;
  const Store_Row *store_row = NULL;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
  batch_info_guard.set_batch_size(input_brs.size_);
  if (OB_FAIL(before_add_batch(input_brs))) {
    SQL_ENG_LOG(WARN, "failed to before add batch", K(ret));
  } else {
    for (int64_t i = start_pos; OB_SUCC(ret) && i < input_brs.size_; i++) {
      if (input_brs.skip_->at(i)) {
        continue;
      }
      batch_info_guard.set_batch_idx(i);
      if (OB_FAIL(add_part_topn_sort_row(store_row))) {
        SQL_ENG_LOG(WARN, "failed to add topn row", K(ret));
      }
      row_count++;
    }
    comp_.set_cmp_range(0, comp_.get_cnt());
    if (OB_NOT_NULL(append_row_count)) {
      *append_row_count = row_count;
    }
    if (OB_SUCC(ret)) {
      rows = &(topn_node_->rows_array_);
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObPartitionTopNSort<Compare, Store_Row, has_addon>::add_batch(const ObBatchRows &input_brs,
                                                                  const uint16_t selector[],
                                                                  const int64_t size,
                                                                  common::ObIArray<Store_Row *> *&rows,
                                                                  Store_Row **sk_rows)
{
  int ret = OB_SUCCESS;
  const Store_Row *store_row = NULL;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
  batch_info_guard.set_batch_size(size);
  if (OB_FAIL(before_add_batch(selector, size))) {
    SQL_ENG_LOG(WARN, "failed to before add batch", K(ret));
  } else {
    for (int64_t i = 0; i < size && OB_SUCC(ret); i++) {
      int64_t idx = selector[i];
      batch_info_guard.set_batch_idx(idx);
      if (OB_FAIL(add_part_topn_sort_row(store_row))) {
        SQL_ENG_LOG(WARN, "check need sort failed", K(ret));
      } else if (OB_NOT_NULL(store_row)) {
        sk_rows[i] = const_cast<Store_Row *>(store_row);
      } else if (OB_NOT_NULL(topn_node_) && OB_NOT_NULL(topn_node_->get_top())) {
        sk_rows[i] = const_cast<Store_Row *>(topn_node_->get_top());
      } else {
        ret = OB_ERR_UNEXPECTED;
      }
    }
  }
  if (OB_SUCC(ret)) {
    rows = &(topn_node_->rows_array_);
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObPartitionTopNSort<Compare, Store_Row, has_addon>::add_part_topn_sort_row(
                                             const Store_Row *&store_row)
{
  int ret = OB_SUCCESS;
  Store_Row *new_row = NULL;
  if (OB_FAIL(locate_current_topn_node())) {
    SQL_ENG_LOG(WARN, "failed to locate topn node", K(ret));
  } else if (OB_FAIL((this->*add_topn_row_func_)(store_row))) {
    SQL_ENG_LOG(WARN, "add topn sort row failed", K(ret));
  } else if (OB_UNLIKELY(part_group_cnt_ > ENLARGE_BUCKET_NUM_FACTOR * max_bucket_cnt_)
             && OB_FAIL(resize_buckets())) {
    SQL_ENG_LOG(WARN, "failed to enlarge partition topn buckets");
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObPartitionTopNSort<Compare, Store_Row, has_addon>::is_equal_part(const Store_Row *l,
                                                                  const ObEvalCtx &eval_ctx,
                                                                  const RowMeta &row_meta,
                                                                  bool &is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = true;
  if (OB_UNLIKELY(OB_ISNULL(l))) {
    is_equal = false;
  } else {
    const int64_t batch_idx = eval_ctx_->get_batch_idx();
    const SortKeyColResult *sk_col_result_list = sort_exprs_getter_.get_sk_col_result_list();
    int cmp_ret = 0;
    ObLength l_len = 0;
    ObLength r_len = 0;
    bool l_null = false;
    bool r_null = false;
    const char *l_data = nullptr;
    const char *r_data = nullptr;
    for (int64_t i = 1; is_equal && i <= part_cnt_; ++i) {
      const int64_t idx = sk_collations_->at(i).field_idx_;
      const ObExpr *e = sk_exprs_->at(idx);
      l_null = l->is_null(idx);
      r_null = sk_col_result_list[idx].is_null(batch_idx);
      if (l_null != r_null) {
        is_equal = false;
      } else if (l_null && r_null) {
        is_equal = true;
      } else {
        l->get_cell_payload(row_meta, idx, l_data, l_len);
        r_data = sk_col_result_list[idx].get_payload(batch_idx);
        r_len = sk_col_result_list[idx].get_length(batch_idx);
        if (l_len == r_len && (0 == memcmp(l_data, r_data, l_len))) {
          is_equal = true;
        } else {
          NullSafeRowCmpFunc &sort_cmp_fun = NULL_FIRST == sk_collations_->at(i).null_pos_ ?
                                                            e->basic_funcs_->row_null_first_cmp_ :
                                                            e->basic_funcs_->row_null_last_cmp_;
          if (OB_FAIL(sort_cmp_fun(e->obj_meta_, e->obj_meta_, l_data, l_len, l_null, r_data,
                                        r_len, r_null, cmp_ret))) {
            SQL_ENG_LOG(WARN, "failed to compare", K(ret));
          } else {
            is_equal = (0 == cmp_ret);
          }
        }
      }
    }
    if (!is_equal) {
      hash_collision_cnt_ += 1;
    }
  }
  return ret;
}

// used for partition topn sort
template <typename Compare, typename Store_Row, bool has_addon>
int ObPartitionTopNSort<Compare, Store_Row, has_addon>::do_sort() {
  int ret = OB_SUCCESS;
  topn_nodes_.reuse();
  for (int64_t idx = 0; OB_SUCC(ret) && idx < max_bucket_cnt_; ++idx) {
    PartTopnNode *hash_node = pt_buckets_->at(idx);
    int64_t cur_bucket_cnt = 0;
    int64_t hash_expr_cnt = 1;
    cur_bucket_cnt = topn_nodes_.count();
    while(hash_node != NULL) {
      TopnNode &cur_node = hash_node->topn_node_;
      if (0 == cur_node.rows_array_.count()) {
        //do nothing
      } else if (OB_FAIL(topn_nodes_.push_back(&cur_node))) {
        SQL_ENG_LOG(WARN, "failed to add into topn nodes", K(ret));
      }
      hash_node = hash_node->hash_node_next_;
    }
    if (OB_SUCC(ret)) {
      //sort topn nodes which is in the same bucket
      comp_.set_cmp_range(0, part_cnt_ + hash_expr_cnt);
      lib::ob_sort(topn_nodes_.begin() + cur_bucket_cnt, topn_nodes_.end(), TopnNodeComparer(comp_));
      //sort rows in topn node
      if (OB_SUCC(ret) && part_cnt_ + hash_expr_cnt < comp_.get_cnt()) {
        comp_.set_cmp_range(part_cnt_ + hash_expr_cnt, comp_.get_cnt());
        for (int64_t i = cur_bucket_cnt; OB_SUCC(ret) && i < topn_nodes_.count(); ++i) {
          if (OB_FAIL(finish_in_quick_select(topn_nodes_.at(i)))) {
            SQL_ENG_LOG(WARN, "failed to topn sort", K(ret));
          }
        }
      } else {
        //partition limit, do nothing
      }
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObPartitionTopNSort<Compare, Store_Row, has_addon>::locate_current_topn_node()
{
  int ret = OB_SUCCESS;
  PartTopnNode *new_topn_node = NULL;
  int64_t hash_idx = sk_collations_->at(0).field_idx_;
  uint64_t bucket_cnt = max_bucket_cnt_;
  uint64_t shift_right = __builtin_clzll(bucket_cnt) + 1;
  uint64_t pos = 0;
  const uint64_t hash_value = *(reinterpret_cast<const uint64_t *>(
                                sort_exprs_getter_.get_sk_col_result_list()[hash_idx].get_payload(eval_ctx_->get_batch_idx())));
  pos = hash_value >> shift_right; // high n bit
  PartTopnNode *exist = NULL;
  if (OB_FAIL(locate_current_topn_node_in_bucket(hash_value, pt_buckets_->at(pos), exist))) {
    SQL_ENG_LOG(WARN, "locate current topn node in bucket failed", K(ret));
  } else if (NULL == exist) {
    ObIAllocator &alloc = mem_context_->get_malloc_allocator();
    if (OB_ISNULL(exist = OB_NEWx(PartTopnNode, &alloc, comp_, &alloc))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "allocate memory failed", K(ret));
    } else {
      exist->hash_node_next_ = pt_buckets_->at(pos);
      pt_buckets_->at(pos) = exist;
      topn_node_ = &(exist->topn_node_);
      topn_node_->hash_val_ = hash_value;
      ++part_group_cnt_;
    }
  } else {
    topn_node_ = &(exist->topn_node_);
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObPartitionTopNSort<Compare, Store_Row, has_addon>::locate_current_topn_node_in_bucket(
                                                                uint64_t hash_value,
                                                                PartTopnNode *first_node,
                                                                PartTopnNode *&exist)
{
  int ret = OB_SUCCESS;
  exist = first_node;
  bool find_same_node = false;
  while (OB_SUCC(ret) && NULL != exist && !find_same_node) {
    if (exist->topn_node_.rows_array_.count() > 0) {
      const Store_Row *top_row = exist->topn_node_.get_top();
      if (hash_value != exist->topn_node_.hash_val_) {
        hash_collision_cnt_ += 1;
      } else if (OB_FAIL(is_equal_part(top_row, *eval_ctx_, *sk_row_meta_, find_same_node))) {
        SQL_ENG_LOG(WARN, "failed to compare", K(ret));
      }
    }
    if (!find_same_node) {
      exist = exist->hash_node_next_;
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObPartitionTopNSort<Compare, Store_Row, has_addon>::part_topn_node_next(int64_t &cur_node_idx,
                                                    int64_t &cur_topn_row_idx,
                                                    const Store_Row *&sk_row,
                                                    const Store_Row *&addon_row)
{
  int ret = OB_SUCCESS;
  TopnNode *cur_node = NULL;
  sk_row = NULL;
  if (cur_node_idx < 0) {
    ret = OB_ARRAY_OUT_OF_RANGE;
  } else {
    while (OB_SUCC(ret) && NULL == sk_row) {
      if (cur_node_idx >= topn_nodes_.count()) {
        ret = OB_ITER_END;
      } else {
        cur_node = topn_nodes_.at(cur_node_idx);
        if (OB_ISNULL(cur_node)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_ENG_LOG(WARN, "invalid cur node", K(ret));
        } else if (cur_topn_row_idx < cur_node->rows_array_.count()) {
          sk_row = cur_node->rows_array_.at(cur_topn_row_idx);
          if (has_addon) {
            addon_row = sk_row->get_addon_ptr(*sk_row_meta_);
          }
          cur_topn_row_idx += 1;
        } else if (cur_topn_row_idx - cur_node->rows_array_.count() < cur_node->ties_array_.count()) {
          sk_row = cur_node->ties_array_.at(cur_topn_row_idx - cur_node->rows_array_.count());
          if (has_addon) {
            addon_row = sk_row->get_addon_ptr(*sk_row_meta_);
          }
          cur_topn_row_idx += 1;
        } else {
          //switch node
          cur_node_idx += 1;
          cur_topn_row_idx = 0;
        }
      }
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObPartitionTopNSort<Compare, Store_Row, has_addon>::part_topn_next_stored_row(const Store_Row *&sk_row)
{
  int ret = OB_SUCCESS;
  TopnNode *cur_node = NULL;
  sk_row = NULL;
  if (cur_topn_node_idx_ < 0) {
    ret = OB_ARRAY_OUT_OF_RANGE;
  } else {
    while (OB_SUCC(ret) && NULL == sk_row) {
      if (cur_topn_node_idx_ >= topn_nodes_.count()) {
        ret = OB_ITER_END;
      } else {
        cur_node = topn_nodes_.at(cur_topn_node_idx_);
        if (OB_ISNULL(cur_node)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_ENG_LOG(WARN, "invalid cur node", K(ret));
        } else if (row_idx_ < cur_node->rows_array_.count()) {
          sk_row = cur_node->rows_array_.at(row_idx_);
          row_idx_ += 1;
        } else if (row_idx_ - cur_node->rows_array_.count() < cur_node->ties_array_.count()) {
          sk_row = cur_node->ties_array_.at(row_idx_ - cur_node->rows_array_.count());
          row_idx_ += 1;
        } else {
          //switch node
          cur_topn_node_idx_ += 1;
          row_idx_ = 0;
        }
      }
    }
  }
  if (ret == OB_ITER_END) {
    cur_topn_node_idx_ = 0;
    row_idx_ = 0;
    topn_nodes_.reset();
    sk_row = nullptr;
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObPartitionTopNSort<Compare, Store_Row, has_addon>::do_quick_select(TopnNode *node, int64_t k)
{
  int ret = OB_SUCCESS;
  common::ObIArray<Store_Row *> &rows = node->rows_array_;
  int64_t size = node->reuse_idx_ == -1 ? rows.count() : node->reuse_idx_;
  int64_t res_cnt = k;
  if (OB_UNLIKELY(k <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "failed to insert top", K(ret));
  } else if (OB_UNLIKELY(k >= size)) {
    // do nothing
  } else {
    int64_t left = 0;
    int64_t right = size - 1;
    while (OB_SUCC(ret) && left <= right) {
      int64_t mid = (left + right) / 2;
      // select pivot by median of three
      if (comp_(rows.at(mid), rows.at(left))) {
        std::swap(rows.at(left), rows.at(mid));
      }
      if (comp_(rows.at(right), rows.at(left))) {
        std::swap(rows.at(left), rows.at(right));
      }
      if (comp_(rows.at(right), rows.at(mid))) {
        std::swap(rows.at(mid), rows.at(right));
      }
      int64_t pivot = mid;
      int64_t swap_idx = left;
      std::swap(rows.at(right), rows.at(pivot));
      for (int64_t j = left; OB_SUCC(ret) && j < right; j++) {
        if (comp_(rows.at(j), rows.at(right))) {
          std::swap(rows.at(swap_idx), rows.at(j));
          swap_idx++;
        } else {
          ret = comp_.ret_;
        }
      }
      std::swap(rows.at(swap_idx), rows.at(right));
      int64_t find_count = swap_idx - left + 1;
      if (k < find_count) {
        right = swap_idx - 1;
      } else if (k > find_count) {
        left = swap_idx + 1;
        k -= find_count;
      } else {
        break;
      }
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObPartitionTopNSort<Compare, Store_Row, has_addon>::do_quick_select_with_ties(TopnNode *node, int64_t k)
{
  int ret = OB_SUCCESS;
  common::ObIArray<Store_Row *> &rows = node->rows_array_;
  common::ObIArray<Store_Row *> &ties_rows = node->ties_array_;
  int64_t size = node->reuse_idx_ == -1 ? rows.count() : node->reuse_idx_;
  int64_t res_cnt = k;
  if (OB_UNLIKELY(k <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "failed to insert top", K(ret));
  } else if (OB_UNLIKELY(k >= size)) {
    // do nothing
  } else {
    int64_t left = 0;
    int64_t right = size - 1;
    while (OB_SUCC(ret) && left <= right) {
      int64_t pivot = (left + right) / 2;
      int64_t swap_idx = left;
      std::swap(rows.at(right), rows.at(pivot));
      for (int64_t j = left; OB_SUCC(ret) && j < right; j++) {

        if (comp_(rows.at(j), rows.at(right))) {
          std::swap(rows.at(swap_idx), rows.at(j));
          swap_idx++;
        } else {
          ret = comp_.ret_;
        }
      }
      std::swap(rows.at(swap_idx), rows.at(right));
      int64_t find_count = swap_idx - left + 1;
      if (k < find_count) {
        right = swap_idx - 1;
      } else if (k > find_count) {
        left = swap_idx + 1;
        k -= find_count;
      } else {
        break;
      }
    }

    // 1. kth_row != ties_rows, clear ties_array
    // 2. find all rows equal to kth_row, add to ties_array
    Store_Row *pivot_row = rows.at(res_cnt - 1);
    if (OB_SUCC(ret) && ties_rows.count() != 0) {
      int cmp = comp_.with_ties_cmp(pivot_row, ties_rows.at(0));
      if (OB_FAIL(comp_.ret_) || cmp == 0) {
        // do nothing
      } else {
        ties_rows.reset();
      }
    }

    for (int64_t i = res_cnt; OB_SUCC(ret) && i < size; i++) {
      int cmp = comp_.with_ties_cmp(pivot_row, rows.at(i));
      if (OB_FAIL(comp_.ret_)) {
        // do nothing
      } else if (cmp == 0) {
        ties_rows.push_back(rows.at(i));
        rows.at(i) = nullptr;
      }
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObPartitionTopNSort<Compare, Store_Row, has_addon>::add_quick_select_row(const Store_Row *&sr)
{
  return add_quick_select_row_impl<false>(sr);
}
template <typename Compare, typename Store_Row, bool has_addon>
int ObPartitionTopNSort<Compare, Store_Row, has_addon>::add_quick_select_row_with_ties(const Store_Row *&sr)
{
  return add_quick_select_row_impl<true>(sr);
}
template <typename Compare, typename Store_Row, bool has_addon>
template <bool with_ties>
int ObPartitionTopNSort<Compare, Store_Row, has_addon>::add_quick_select_row_impl(const Store_Row *&sr)
{
  int ret = OB_SUCCESS;
  Store_Row *new_sk_row = nullptr;
  bool need_add_rows = false;
  bool need_add_ties_rows = false;
  if (OB_ISNULL(topn_node_->top_row_)) {
    need_add_rows = true;
  } else {
    if (with_ties) {
      int cmp = comp_.with_ties_cmp(topn_node_->top_row_, *eval_ctx_);
      if (OB_FAIL(comp_.ret_) || cmp < 0) {
        // do nothing
      } else if (cmp > 0) {
        need_add_rows = true;
      } else if (cmp == 0) {
        need_add_ties_rows = true;
      }
    } else if (comp_(topn_node_->top_row_, *eval_ctx_)) {
      need_add_rows = true;
    } else {
      ret = comp_.ret_;
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (need_add_ties_rows) {
    if (OB_FAIL(copy_to_row(new_sk_row))) {
      SQL_ENG_LOG(WARN, "failed to copy to row", K(ret));
    } else if (OB_ISNULL(new_sk_row)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "new_sk_row is null", K(ret));
    } else {
      topn_node_->ties_array_.push_back(new_sk_row);
      sr = new_sk_row;
    }
  } else if (need_add_rows) {
    if (topn_node_->reuse_idx_ != -1) {
      new_sk_row = topn_node_->rows_array_.at(topn_node_->reuse_idx_);
      if (OB_FAIL(copy_to_row(new_sk_row))) {
        SQL_ENG_LOG(WARN, "failed to copy to row", K(ret));
      } else if (OB_ISNULL(new_sk_row)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "new_sk_row is null", K(ret));
      } else {
        topn_node_->rows_array_.at(topn_node_->reuse_idx_) = new_sk_row;
        topn_node_->reuse_idx_ += 1;
      }
    } else {
      if (OB_FAIL(copy_to_row(new_sk_row))) {
        SQL_ENG_LOG(WARN, "failed to copy to row", K(ret));
      } else if (OB_ISNULL(new_sk_row)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "new_sk_row is null", K(ret));
      } else {
        topn_node_->rows_array_.push_back(new_sk_row);
      }
    }
    if (OB_SUCC(ret)) {
      sr = new_sk_row;
      // partition topn sort use stream process, so we need to do quick select when size == 2 * K
      // will select top k rows, and update top_row_ to filter for next batch.
      int64_t size = topn_node_->rows_array_.count();
      int64_t reuse_idx_ = topn_node_->reuse_idx_;
      int64_t k = topn_cnt_ - outputted_rows_cnt_;
      if ((reuse_idx_ == -1 && size >= 2 * k) || reuse_idx_ == 2 * k) {
        if (with_ties && OB_FAIL(do_quick_select_with_ties(topn_node_, k))) {
          SQL_ENG_LOG(WARN, "failed to do quick select with ties", K(ret));
        } else if (!with_ties && OB_FAIL(do_quick_select(topn_node_, k))) {
          SQL_ENG_LOG(WARN, "failed to do quick select", K(ret));
        }
        if (OB_SUCC(ret)) {
          topn_node_->reuse_idx_ = k;
          topn_node_->top_row_ = topn_node_->rows_array_.at(k - 1);
        }
      }
    }
  } else {
    ret = comp_.ret_;
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObPartitionTopNSort<Compare, Store_Row, has_addon>::finish_in_quick_select(TopnNode *node)
{
  int ret = OB_SUCCESS;
  common::ObIArray<Store_Row *> &rows = node->rows_array_;
  int64_t k = topn_cnt_ - outputted_rows_cnt_;

  if (is_fetch_with_ties_ && OB_FAIL(do_quick_select_with_ties(node, k))) {
    SQL_ENG_LOG(WARN, "failed to do quick select with ties", K(ret));
  } else if (!is_fetch_with_ties_ && OB_FAIL(do_quick_select(node, k))) {
    SQL_ENG_LOG(WARN, "failed to do quick select", K(ret));
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else {
    while (OB_SUCC(ret) && rows.count() > k) {
      Store_Row *pop_row = nullptr;
      if (OB_FAIL(rows.pop_back(pop_row))) {
        SQL_ENG_LOG(WARN, "failed to pop back row", K(ret));
      } else if (OB_ISNULL(pop_row)) {
        // do nothing
      }
    }
    if (OB_FAIL(ret) || rows.count() <= 1) {
      // do nothing
    } else {
      lib::ob_sort(&rows.at(0), &rows.at(0) + rows.count(), CopyableComparer(comp_));
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObPartitionTopNSort<Compare, Store_Row, has_addon>::copy_to_row(Store_Row *&sk_row)
{
  int ret = OB_SUCCESS;
  Store_Row *addon_row = nullptr;
  Store_Row *reclaim_addon_row = nullptr;
  Store_Row *reclaim_reuse_row = sk_row;
  if (nullptr != sk_row && has_addon) {
    addon_row = sk_row->get_addon_ptr(*sk_row_meta_);
    reclaim_addon_row = sk_row->get_addon_ptr(*sk_row_meta_);
  }
  if (OB_FAIL(copy_to_row(*sk_exprs_, *sk_row_meta_, true, sk_row))) {
    SQL_ENG_LOG(WARN, "failed to copy to row", K(ret));
  } else if (has_addon) {
    if (OB_FAIL(copy_to_row(*addon_exprs_, *addon_row_meta_, false, addon_row))) {
      SQL_ENG_LOG(WARN, "failed to copy to row", K(ret));
    } else {
      sk_row->set_addon_ptr(addon_row, *sk_row_meta_);
    }
  }
  if (OB_FAIL(ret)) {
    if (sk_row == reclaim_reuse_row) {
      sk_row = nullptr;
    }
    if (has_addon && reclaim_addon_row == addon_row) {
      addon_row = nullptr;
    }
    if (sk_row != nullptr) {
      sk_row = nullptr;
    }
    if (has_addon && addon_row != nullptr) {
      addon_row = nullptr;
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObPartitionTopNSort<Compare, Store_Row, has_addon>::copy_to_row(
  const common::ObIArray<ObExpr *> &exprs, const RowMeta &row_meta, bool is_sort_key, Store_Row *&row)
{
  int ret = OB_SUCCESS;
  char *buf = nullptr;
  int64_t row_size = 0;
  int64_t buffer_len = 0;
  Store_Row *dst = nullptr;
  Store_Row *reclaim_row = nullptr;
  // check to see whether this old row's space is adequate for new one
  if (OB_FAIL(ObTempRowStore::RowBlock::calc_row_size(exprs, row_meta, *eval_ctx_, row_size))) {
    SQL_ENG_LOG(WARN, "failed to calc copy size", K(ret));
  } else if (nullptr != row && row->get_max_size(row_meta) >= row_size) {
    buf = reinterpret_cast<char *>(row);
    buffer_len = row->get_max_size(row_meta);
    dst = row;
  } else {
    reclaim_row = row;
    buffer_len = row_size * 2;
    row_count_ += 1;
    if (OB_FAIL(pt_row_buffer_.get_mem(buffer_len, buf))) {
      SQL_ENG_LOG(WARN, "failed to get mem", K(ret));
    } else {
      dst = new (buf) Store_Row();
    }
  }
  if (OB_SUCC(ret)) {
    ObCompactRow *sr = static_cast<ObCompactRow *>(dst);
    if (OB_FAIL(build_row(exprs, row_meta, row_size, *eval_ctx_, sr))) {
      SQL_ENG_LOG(WARN, "build stored row failed", K(ret));
      if (row != dst) {
        reclaim_row = dst;
      }
    } else {
      row = dst;
      if (has_addon && is_sort_key) {
        row->set_addon_ptr(nullptr, row_meta);
      }
      row->set_max_size(buffer_len, row_meta);
    }
  }
  if (OB_FAIL(ret)) {
    row = nullptr;
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObPartitionTopNSort<Compare, Store_Row, has_addon>::build_row(
      const common::ObIArray<ObExpr *> &exprs, const RowMeta &row_meta, const int64_t row_size,
      ObEvalCtx &ctx, ObCompactRow *&stored_row)
{
  int ret = OB_SUCCESS;
  const int64_t batch_idx = ctx.get_batch_idx();
  stored_row->init(row_meta);
  stored_row->set_row_size(row_size);
  for (int64_t i = 0; i < exprs.count() && OB_SUCC(ret); ++i) {
    ObExpr *expr = exprs.at(i);
    ObIVector *vec = expr->get_vector(ctx);
    if (OB_FAIL(vec->to_row(row_meta, stored_row, batch_idx, i))) {
      SQL_ENG_LOG(WARN, "failed to to row", K(ret), K(expr));
    }
  }
  return ret;
}

/************************* end ObPartitionTopNSort **********************************/

} // end namespace sql
} // end namespace oceanbase
