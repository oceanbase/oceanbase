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

namespace oceanbase
{
namespace sql
{
/************************* start ObPrefixSortVecImpl **********************************/
template <typename Compare, typename Store_Row, bool has_addon>
void ObPrefixSortVecImpl<Compare, Store_Row, has_addon>::reset()
{
  full_sk_collations_ = nullptr;
  base_sk_collations_.reset();
  brs_holder_.reset();
  next_prefix_row_ = nullptr;
  prev_row_ = nullptr;
  child_ = nullptr;
  self_op_ = nullptr;
  sort_row_count_ = nullptr;
  selector_size_ = 0;
  sort_prefix_rows_ = 0;
  prefix_pos_ = 0;
  im_sk_store_.reset();
  im_addon_store_.reset();
  immediate_pos_ = 0;
  brs_ = nullptr;
  if (nullptr != mem_context_ && nullptr != selector_) {
    mem_context_->get_malloc_allocator().free(selector_);
    selector_ = nullptr;
  }
  if (nullptr != mem_context_ && nullptr != im_sk_rows_) {
    mem_context_->get_malloc_allocator().free(im_sk_rows_);
    im_sk_rows_ = nullptr;
  }
  if (nullptr != mem_context_ && nullptr != im_addon_rows_) {
    mem_context_->get_malloc_allocator().free(im_addon_rows_);
    im_addon_rows_ = nullptr;
  }
  ObSortVecOpImpl<Compare, Store_Row, has_addon>::reset();
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObPrefixSortVecImpl<Compare, Store_Row, has_addon>::init(ObSortVecOpContext &ctx)
{
  int ret = OB_SUCCESS;
  if (is_inited()) {
    ret = OB_INIT_TWICE;
    SQL_ENG_LOG(WARN, "init twice", K(ret));
  } else if (OB_INVALID_ID == ctx.tenant_id_ || OB_ISNULL(ctx.sk_collations_)
             || OB_ISNULL(ctx.eval_ctx_) || OB_ISNULL(ctx.op_) || ctx.prefix_pos_ <= 0
             || ctx.prefix_pos_ > ctx.sk_collations_->count()) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(ret), K(ctx.tenant_id_), K(ctx.prefix_pos_));
  } else {
    int64_t batch_size = ctx.eval_ctx_->max_batch_size_;
    full_sk_collations_ = ctx.sk_collations_;
    // NOTE: %cnt may be zero, some plan is wrong generated with prefix sort:
    // %prefix_pos == %sort_columns.count(), the sort operator should be
    // eliminated but not.
    //
    // To be compatible with this plan, we keep this behavior.
    const int64_t cnt = ctx.sk_collations_->count() - ctx.prefix_pos_;
    base_sk_collations_.init(
      cnt, const_cast<ObSortFieldCollation *>(&ctx.sk_collations_->at(0) + ctx.prefix_pos_), cnt);
    prev_row_ = nullptr;
    next_prefix_row_ = nullptr;
    prefix_pos_ = ctx.prefix_pos_;
    self_op_ = ctx.op_;
    child_ = ctx.op_->get_child();
    sort_row_count_ = ctx.sort_row_cnt_;
    ctx.base_sk_collations_ = &base_sk_collations_;
    if (OB_SUCCESS != (ret = ObSortVecOpImpl<Compare, Store_Row, has_addon>::init(ctx))) {
      SQL_ENG_LOG(WARN, "sort impl init failed", K(ret));
    } else if (OB_FAIL(init_temp_row_store(
                 *sk_exprs_, INT64_MAX, batch_size, false, false /*enable dump*/,
                 Store_Row::get_extra_size(true /*is_sort_key*/), NONE_COMPRESSOR, im_sk_store_))) {
      SQL_ENG_LOG(WARN, "failed to init temp row store", K(ret));
    } else if (OB_FAIL(init_temp_row_store(
                 *addon_exprs_, INT64_MAX, batch_size, false, false /*enable dump*/,
                 Store_Row::get_extra_size(false /*is_sort_key*/), NONE_COMPRESSOR, im_addon_store_))) {
      SQL_ENG_LOG(WARN, "failed to init temp row store", K(ret));
    } else {
      selector_ =
        (typeof(selector_))mem_context_->get_malloc_allocator().alloc(batch_size * sizeof(*selector_));
      im_sk_rows_ = (typeof(im_sk_rows_))mem_context_->get_malloc_allocator().alloc(
        batch_size * sizeof(*im_sk_rows_));
      im_addon_rows_ = (typeof(im_addon_rows_))mem_context_->get_malloc_allocator().alloc(
        batch_size * sizeof(*im_addon_rows_));
      if (nullptr == selector_ || nullptr == im_sk_rows_ || nullptr == im_addon_rows_) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_ENG_LOG(WARN, "allocate memory failed", K(ret), K(batch_size), KP(selector_),
                    KP(im_sk_rows_), KP(im_addon_rows_));
      } else if (OB_FAIL(brs_holder_.init(all_exprs_, *ctx.eval_ctx_))) {
        SQL_ENG_LOG(WARN, "init batch result holder failed", K(ret));
      } else if (OB_FAIL(fetch_rows_batch())) {
        SQL_ENG_LOG(WARN, "fetch rows in batch manner failed", K(ret));
      }
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObPrefixSortVecImpl<Compare, Store_Row, has_addon>::is_same_prefix(
  const Store_Row *r, const RowMeta &row_meta, const common::ObIArray<ObExpr *> &cmp_sk_exprs,
  const int64_t batch_idx, bool &same)
{
  int ret = OB_SUCCESS;
  same = true;
  int cmp_ret = 0;
  int64_t l_len = 0;
  int64_t r_len = 0;
  bool l_null = false;
  bool r_null = false;
  const char *l_data = nullptr;
  const char *r_data = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < prefix_pos_ && same; i++) {
    const ObSortFieldCollation &sort_collation = full_sk_collations_->at(i);
    const int64_t idx = sort_collation.field_idx_;
    const ObExpr *e = cmp_sk_exprs.at(idx);
    auto &sort_cmp_fun = NULL_FIRST == sort_collation.null_pos_ ?
                           e->basic_funcs_->row_null_first_cmp_ :
                           e->basic_funcs_->row_null_last_cmp_;
    ObIVector *vec = e->get_vector(*eval_ctx_);
    l_null = vec->is_null(batch_idx);
    r_null = r->is_null(idx);
    l_len = vec->get_length(batch_idx);
    r_len = r->get_length(row_meta, idx);
    l_data = vec->get_payload(batch_idx);
    r_data = r->get_cell_payload(row_meta, idx);
    if (OB_FAIL(sort_cmp_fun(e->obj_meta_, e->obj_meta_, l_data, l_len, l_null, r_data, r_len,
                             r_null, cmp_ret))) {
      SQL_ENG_LOG(WARN, "failed to compare", K(ret));
    } else {
      same = (0 == cmp_ret);
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObPrefixSortVecImpl<Compare, Store_Row, has_addon>::is_same_prefix(
  const common::ObIArray<ObExpr *> &cmp_sk_exprs, const int64_t l_batch_idx, const int64_t r_batch_idx,
  bool &same)
{
  int ret = OB_SUCCESS;
  int cmp_ret = 0;
  same = true;
  int64_t l_len = 0;
  int64_t r_len = 0;
  bool l_null = false;
  bool r_null = false;
  const char *l_data = nullptr;
  const char *r_data = nullptr;
  for (int64_t i = 0; OB_SUCC(ret) && i < prefix_pos_ && same; i++) {
    const ObSortFieldCollation &sort_collation = full_sk_collations_->at(i);
    const int64_t idx = sort_collation.field_idx_;
    const ObExpr *e = cmp_sk_exprs.at(idx);
    auto &sort_cmp_fun = NULL_FIRST == sort_collation.null_pos_ ?
                           e->basic_funcs_->row_null_first_cmp_ :
                           e->basic_funcs_->row_null_last_cmp_;
    ObIVector *vec = e->get_vector(*eval_ctx_);
    l_null = vec->is_null(l_batch_idx);
    r_null = vec->is_null(r_batch_idx);
    l_len = vec->get_length(l_batch_idx);
    r_len = vec->get_length(r_batch_idx);
    l_data = vec->get_payload(l_batch_idx);
    r_data = vec->get_payload(r_batch_idx);
    if (OB_FAIL(sort_cmp_fun(e->obj_meta_, e->obj_meta_, l_data, l_len, l_null, r_data, r_len,
                             r_null, cmp_ret))) {
      SQL_ENG_LOG(WARN, "failed to compare", K(ret));
    } else {
      same = (0 == cmp_ret);
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObPrefixSortVecImpl<Compare, Store_Row, has_addon>::add_immediate_prefix(
  const uint16_t selector[], const int64_t row_size)
{
  int ret = OB_SUCCESS;
  int64_t pos = im_sk_store_.get_row_cnt();
  if (OB_FAIL(add_immediate_prefix_store(pos, selector, row_size))) {
    SQL_ENG_LOG(WARN, "failed to add immediate prefix store", K(ret));
  } else if (!comp_.is_inited()
             && OB_FAIL(comp_.init(cmp_sk_exprs_, sk_row_meta_, addon_row_meta_,
                                   cmp_sort_collations_, exec_ctx_,
                                   enable_encode_sortkey_ && !(part_cnt_ > 0)))) {
    SQL_ENG_LOG(WARN, "init compare failed", K(ret));
  } else {
    lib::ob_sort(im_sk_rows_ + pos, im_sk_rows_ + pos + selector_size_,
              CopyableComparer(comp_));
    if (OB_SUCCESS != comp_.ret_) {
      ret = comp_.ret_;
      SQL_ENG_LOG(WARN, "compare failed", K(ret));
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObPrefixSortVecImpl<Compare, Store_Row, has_addon>::add_immediate_prefix_store(
  const int64_t pos, const uint16_t selector[], const int64_t row_size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(im_sk_store_.add_batch(sk_vec_ptrs_, selector, row_size,
                                     SK_UPCAST_PP(im_sk_rows_ + pos)))) {
    SQL_ENG_LOG(WARN, "add store row failed", K(ret), K(get_memory_used()), K(get_memory_limit()));
  } else if (has_addon) {
    if (OB_FAIL(im_addon_store_.add_batch(addon_vec_ptrs_, selector, row_size,
                                          SK_UPCAST_PP(im_addon_rows_ + pos)))) {
      SQL_ENG_LOG(WARN, "add store row failed", K(ret), K(get_memory_used()),
                  K(get_memory_limit()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < row_size; i++) {
      SK_DOWNCAST_P(im_sk_rows_[i + pos])
        ->set_addon_ptr(im_addon_rows_[i + pos], im_sk_store_.get_row_meta());
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
void ObPrefixSortVecImpl<Compare, Store_Row, has_addon>::attach_im_perfix_store(
  const int64_t max_cnt, int64_t &read_rows)
{
  if (immediate_pos_ < im_sk_store_.get_row_cnt()) {
    int64_t cnt = std::min(max_cnt - read_rows, im_sk_store_.get_row_cnt() - immediate_pos_);
    MEMCPY(sk_rows_ + read_rows, im_sk_rows_ + immediate_pos_, cnt * sizeof(*sk_rows_));
    if (has_addon) {
      for (int64_t i = 0; i < cnt; i++) {
        Store_Row *addon_row =
          im_sk_rows_[immediate_pos_ + i]->get_addon_ptr(im_sk_store_.get_row_meta());
        addon_rows_[i + read_rows] = addon_row;
      }
    }
    immediate_pos_ += cnt;
    read_rows += cnt;
  }
}

// Fetch rows from child until new prefix found or iterate end.
// One batch rows form child are split into three part:
// 1. Sort prefix: same prefix with rows in ObSortVecOpImpl
// 2. Immediate prefixes: prefix which boundaries in current batch, added to
//    %immediate_prefix_store_ and sorted by std::sort.
// 3. Next prefix: last prefix of current batch, rows keep in expression, added
// to
//    ObSortVecOpImpl in next fetch_rows_batch().
//
// E.g.:
//
//    1 <-- sort prefix start
//    1
//    1 <-- sort prefix end
//    2 <-- immediate prefixes start
//    3
//    3
//    3 <-- immediate prefixes end
//    4 <-- next prefix start
//    4
//    4 <-- next prefix end
//
template <typename Compare, typename Store_Row, bool has_addon>
int ObPrefixSortVecImpl<Compare, Store_Row, has_addon>::fetch_rows_batch()
{
  int ret = OB_SUCCESS;
  ObSortVecOpImpl<Compare, Store_Row, has_addon>::reuse();
  sort_prefix_rows_ = 0;
  if (OB_FAIL(brs_holder_.restore())) {
    SQL_ENG_LOG(WARN, "restore batch result failed", K(ret));
  } else if (selector_size_ > 0) {
    // next prefix rows in previous fetch_rows_batch().
    if (OB_FAIL(add_batch(*brs_, selector_, selector_size_))) {
      SQL_ENG_LOG(WARN, "add batch failed", K(ret));
    } else {
      sort_prefix_rows_ += selector_size_;
      prev_row_ = sk_rows_[selector_size_ - 1];
    }
    selector_size_ = 0;
  }
  immediate_pos_ = 0;
  if (im_sk_store_.get_row_cnt() > 0 || im_addon_store_.get_row_cnt() > 0) {
    im_sk_store_.reset();
    im_addon_store_.reset();
  }

  bool found_new_prefix = false;
  while (OB_SUCC(ret) && !found_new_prefix) {
    self_op_->clear_evaluated_flag();
    if (OB_FAIL(child_->get_next_batch(self_op_->get_spec().max_batch_size_, brs_))) {
      SQL_ENG_LOG(WARN, "get next batch failed", K(ret));
    } else {
      // evaluate all expression and set projected, no need to evaluate any
      // more.
      for (int64_t i = 0; OB_SUCC(ret) && i < all_exprs_.count(); i++) {
        ObExpr *e = all_exprs_.at(i);
        if (OB_FAIL(e->eval_vector(*eval_ctx_, *brs_))) {
          SQL_ENG_LOG(WARN, "eval batch failed", K(ret));
        } else {
          e->get_eval_info(*eval_ctx_).projected_ = true;
        }
      }
      selector_size_ = 0;
      int64_t new_prefix = -1;
      for (int64_t i = 0; OB_SUCC(ret) && i < brs_->size_; i++) {
        if (brs_->skip_->at(i)) {
          continue;
        }
        *sort_row_count_ += 1;
        if (new_prefix < 0) {
          bool is_same = false;
          if (nullptr != prev_row_
              && OB_FAIL(is_same_prefix(prev_row_, *sk_row_meta_, *sk_exprs_, i, is_same))) {
            SQL_ENG_LOG(WARN, "check same prefix failed", K(ret));
          } else if (nullptr == prev_row_ && OB_FAIL(is_same_prefix(*sk_exprs_, 0, i, is_same))) {
            SQL_ENG_LOG(WARN, "check same prefix failed", K(ret));
          } else if (is_same) {
            selector_[selector_size_++] = i;
          } else {
            if (0 == selector_size_) {
              // do nothing
            } else if (OB_FAIL(add_batch(*brs_, selector_, selector_size_))) {
              SQL_ENG_LOG(WARN, "add batch failed", K(ret));
            } else {
              sort_prefix_rows_ += selector_size_;
              prev_row_ = sk_rows_[selector_size_ - 1];
            }
            new_prefix = i;
            selector_size_ = 1;
            selector_[0] = i;
          }
          continue;
        }
        if (new_prefix >= 0) {
          bool is_same = false;
          if (OB_FAIL(is_same_prefix(*sk_exprs_, new_prefix, i, is_same))) {
            SQL_ENG_LOG(WARN, "check same prefix failed", K(ret));
          } else if (!is_same) {
            if (OB_FAIL(add_immediate_prefix(selector_, selector_size_))) {
              SQL_ENG_LOG(WARN, "add immediate prefix failed", K(ret));
            } else {
              new_prefix = i;
              selector_size_ = 1;
              selector_[0] = i;
            }
          } else {
            selector_[selector_size_++] = i;
          }
        }
      } // end for

      if (selector_size_ > 0 && OB_SUCC(ret)) {
        if (new_prefix < 0) {
          if (OB_FAIL(add_batch(*brs_, selector_, selector_size_))) {
            SQL_ENG_LOG(WARN, "add batch failed", K(ret));
          } else {
            sort_prefix_rows_ += selector_size_;
            prev_row_ = sk_rows_[selector_size_ - 1];
          }
          selector_size_ = 0;
        } else {
          if (brs_->end_) {
            // add last immediate prefix rows
            if (OB_FAIL(add_immediate_prefix(selector_, selector_size_))) {
              SQL_ENG_LOG(WARN, "add immediate prefix failed", K(ret));
            }
            selector_size_ = 0;
          }
        }
      }
      found_new_prefix = new_prefix >= 0 || brs_->end_;
      if (found_new_prefix && OB_SUCC(ret)) {
        // child not iterate end, need backup child expression datums
        const int64_t cnt = std::min(sort_prefix_rows_ + im_sk_store_.get_row_cnt(),
                                      (int64_t)self_op_->get_spec().max_batch_size_);
        OZ(brs_holder_.save(cnt));
      }
    }
  } // end while

  if (OB_SUCC(ret) && sort_prefix_rows_ > 0) {
    ret = ObSortVecOpImpl<Compare, Store_Row, has_addon>::sort();
  }

  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObPrefixSortVecImpl<Compare, Store_Row, has_addon>::get_next_batch(const int64_t max_cnt,
                                                                       int64_t &read_rows)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "not init", K(ret));
  } else {
    read_rows = 0;
    // Read rows from sort prefix or immediate prefixes,
    // fetch_rows_batch() and try again if no row read
    const int64_t max_loop_cnt = 2;
    for (int64_t loop = 0; OB_SUCC(ret) && loop < max_loop_cnt && 0 == read_rows; loop++) {
      if (sort_prefix_rows_ > 0) {
        if (OB_FAIL(get_next_batch_stored_rows(max_cnt, read_rows))) {
          if (OB_ITER_END == ret) {
            sort_prefix_rows_ = 0;
            ret = OB_SUCCESS;
          } else {
            SQL_ENG_LOG(WARN, "get next batch stored rows failed", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        attach_im_perfix_store(max_cnt, read_rows);
        if (read_rows > 0) {
          if (OB_FAIL(adjust_topn_read_rows(sk_rows_, read_rows))) {
            SQL_ENG_LOG(WARN, "adjust read rows with ties failed", K(ret));
          } else if (OB_FAIL(attach_rows(read_rows))) {
            SQL_ENG_LOG(WARN, "failed to attach rows", K(ret));
          }
        } else {
          if (0 == loop && (nullptr == brs_ || !brs_->end_) && outputted_rows_cnt_ < topn_cnt_) {
            if (OB_FAIL(fetch_rows_batch())) {
              SQL_ENG_LOG(WARN, "fetch rows in batch manner failed", K(ret));
            }
          } else {
            ret = OB_ITER_END;
          }
        }
      }
    }
  }
  return ret;
}

/************************* end ObPrefixSortVecImpl **********************************/
#undef SK_UPCAST_PP
#undef SK_DOWNCAST_P

} // namespace sql
} // namespace oceanbase