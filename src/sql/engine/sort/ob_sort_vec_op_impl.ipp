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
/**************************** start ObSortVecOpImpl ************************************/
template <typename Compare, typename Store_Row, bool has_addon>
void ObSortVecOpImpl<Compare, Store_Row, has_addon>::reset()
{
  sql_mem_processor_.unregister_profile();
  reuse();
  all_exprs_.reset();
  sk_vec_ptrs_.reset();
  addon_vec_ptrs_.reset();
  inmem_row_size_ = 0;
  local_merge_sort_ = false;
  need_rewind_ = false;
  sorted_ = false;
  got_first_row_ = false;
  comp_.reset();
  max_bucket_cnt_ = 0;
  max_node_cnt_ = 0;
  part_cnt_ = 0;
  topn_cnt_ = INT64_MAX;
  outputted_rows_cnt_ = 0;
  is_fetch_with_ties_ = false;
  rows_ = nullptr;
  ties_array_pos_ = 0;
  sort_exprs_getter_.reset();
  if (0 != ties_array_.count()) {
    for (int64_t i = 0; i < ties_array_.count(); ++i) {
      store_row_factory_.free_row_store(ties_array_[i]);
      ties_array_[i] = nullptr;
    }
  }
  ties_array_.reset();
  if (nullptr != mem_context_) {
    if (nullptr != imms_heap_) {
      imms_heap_->~IMMSHeap();
      mem_context_->get_malloc_allocator().free(imms_heap_);
      imms_heap_ = nullptr;
    }
    if (nullptr != ems_heap_) {
      ems_heap_->~EMSHeap();
      mem_context_->get_malloc_allocator().free(ems_heap_);
      ems_heap_ = nullptr;
    }
    if (nullptr != sk_rows_) {
      mem_context_->get_malloc_allocator().free(sk_rows_);
      sk_rows_ = nullptr;
    }
    if (nullptr != addon_rows_) {
      mem_context_->get_malloc_allocator().free(addon_rows_);
      addon_rows_ = nullptr;
    }
    if (nullptr != buckets_) {
      mem_context_->get_malloc_allocator().free(buckets_);
      buckets_ = nullptr;
    }
    if (nullptr != part_hash_nodes_) {
      mem_context_->get_malloc_allocator().free(part_hash_nodes_);
      part_hash_nodes_ = nullptr;
    }
    if (nullptr != topn_filter_) {
      topn_filter_->reset();
      mem_context_->get_malloc_allocator().free(topn_filter_);
      topn_filter_ = nullptr;
    }
    if (nullptr != topn_heap_) {
      for (int64_t i = 0; i < topn_heap_->count(); ++i) {
        store_row_factory_.free_row_store(topn_heap_->at(i));
        topn_heap_->at(i) = nullptr;
      }
      topn_heap_->~TopnHeap();
      mem_context_->get_malloc_allocator().free(topn_heap_);
      topn_heap_ = nullptr;
    }
    if (nullptr != last_ties_row_) {
      if (has_addon) {
        Store_Row *new_addon_row = last_ties_row_ -> get_addon_ptr(sk_store_.get_row_meta());
        if (nullptr != new_addon_row) {
          mem_context_->get_malloc_allocator().free(new_addon_row);
        }
      }
      mem_context_->get_malloc_allocator().free(last_ties_row_);
      last_ties_row_ = nullptr;
    }
    // can not destroy mem_entify here, the memory may hold by %iter_ or
    // %datum_store_
  }
  sk_store_.reset();
  addon_store_.reset();
  inited_ = false;
  io_event_observer_ = nullptr;
}

template <typename Compare, typename Store_Row, bool has_addon>
void ObSortVecOpImpl<Compare, Store_Row, has_addon>::reuse()
{
  sorted_ = false;
  quick_sort_array_.reuse();
  sk_store_.reset();
  addon_store_.reset();
  inmem_row_size_ = 0;
  mem_check_interval_mask_ = 1;
  row_idx_ = 0;
  next_stored_row_func_ = &ObSortVecOpImpl<Compare, Store_Row, has_addon>::array_next_stored_row;
  ties_array_pos_ = 0;
  if (0 != ties_array_.count()) {
    for (int64_t i = 0; i < ties_array_.count(); ++i) {
      store_row_factory_.free_row_store(ties_array_[i]);
      ties_array_[i] = nullptr;
    }
  }
  ties_array_.reset();
  while (!sort_chunks_.is_empty()) {
    SortVecOpChunk *chunk = sort_chunks_.remove_first();
    chunk->~SortVecOpChunk();
    if (nullptr != mem_context_) {
      mem_context_->get_malloc_allocator().free(chunk);
    }
  }
  if (nullptr != imms_heap_) {
    imms_heap_->reset();
  }
  heap_iter_begin_ = false;
  if (nullptr != ems_heap_) {
    ems_heap_->reset();
  }
  if (nullptr != topn_heap_) {
    for (int64_t i = 0; i < topn_heap_->count(); ++i) {
      store_row_factory_.free_row_store(topn_heap_->at(i));
      topn_heap_->at(i) = nullptr;
    }
    topn_heap_->reset();
  }
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::init_vec_ptrs(
  const common::ObIArray<ObExpr *> &exprs,
  common::ObFixedArray<ObIVector *, common::ObIAllocator> &vec_ptrs, ObEvalCtx *eval_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(vec_ptrs.init(exprs.count()))) {
    SQL_ENG_LOG(WARN, "failed to init sort key vector ptrs", K(ret));
  } else {
    for (int64_t i = 0; i < exprs.count() && OB_SUCC(ret); i++) {
      if (OB_FAIL(vec_ptrs.push_back(exprs.at(i)->get_vector(*eval_ctx)))) {
        SQL_ENG_LOG(WARN, "failed to add vector ptr", K(ret), K(i));
      }
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::init_temp_row_store(
  const common::ObIArray<ObExpr *> &exprs, const int64_t mem_limit, const int64_t batch_size,
  const bool need_callback, const bool enable_dump, const int64_t extra_size, ObCompressorType compress_type,
  ObTempRowStore &row_store)
{
  int ret = OB_SUCCESS;
  const bool enable_trunc = true;
  const bool reorder_fixed_expr = true;
  ObMemAttr mem_attr(tenant_id_, ObModIds::OB_SQL_SORT_ROW, ObCtxIds::WORK_AREA);
  if (OB_FAIL(row_store.init(exprs, batch_size, mem_attr, mem_limit, enable_dump,
                             extra_size /* row_extra_size */, compress_type, reorder_fixed_expr,
                             enable_trunc))) {
    SQL_ENG_LOG(WARN, "init row store failed", K(ret));
  } else {
    row_store.set_dir_id(sql_mem_processor_.get_dir_id());
    row_store.set_allocator(mem_context_->get_malloc_allocator());
    row_store.set_io_event_observer(io_event_observer_);
    if (need_callback) {
      row_store.set_callback(&sql_mem_processor_);
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::init_sort_temp_row_store(
  const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_temp_row_store(*sk_exprs_, INT64_MAX, batch_size, false /*need_callback*/,
                                  false /*enable dump*/, Store_Row::get_extra_size(true /*is_sk*/),
                                  compress_type_, sk_store_))) {
    SQL_ENG_LOG(WARN, "failed to init temp row store", K(ret));
  } else if (FALSE_IT(sk_row_meta_ = &sk_store_.get_row_meta())) {
  } else if (has_addon) {
    if (OB_FAIL(init_temp_row_store(*addon_exprs_, INT64_MAX, batch_size, false /*need_callback*/,
                                    false /*enable dump*/,
                                    Store_Row::get_extra_size(false /*is_sk*/), compress_type_, addon_store_))) {
      SQL_ENG_LOG(WARN, "failed to init temp row store", K(ret));
    } else {
      addon_row_meta_ = &addon_store_.get_row_meta();
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::init_eager_topn_filter(
    const common::ObIArray<Store_Row *> *dumped_rows,
    const int64_t max_batch_size) {
  int ret = OB_SUCCESS;
  if (OB_ISNULL(topn_filter_)) {
    char *buf = reinterpret_cast<char *>(allocator_.alloc(
        sizeof(ObSortVecOpEagerFilter<Compare, Store_Row, has_addon>)));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "init eager topn filter failed: allocate memory failed",
                  K(ret));
    } else {
      topn_filter_ = new (buf) ObSortVecOpEagerFilter<Compare, Store_Row, has_addon>(
          allocator_, store_row_factory_);
    }
    if (OB_FAIL(topn_filter_->init(comp_, dumped_rows->count(), topn_cnt_,
                                   max_batch_size))) {
      SQL_ENG_LOG(WARN, "init eager topn filter failed", K(ret));
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::merge_sk_addon_exprs(
  const common::ObIArray<ObExpr *> *sk_exprs, const common::ObIArray<ObExpr *> *addon_exprs)
{
  int ret = OB_SUCCESS;
  int64_t exprs_cnt =
    (addon_exprs != nullptr) ? sk_exprs->count() + addon_exprs->count() : sk_exprs->count();
  if (OB_FAIL(all_exprs_.init(exprs_cnt))) {
    SQL_ENG_LOG(WARN, "failed to init all exprs", K(ret));
  } else if (OB_FAIL(append(all_exprs_, *sk_exprs))) {
    SQL_ENG_LOG(WARN, "failed to append sort key", K(ret));
  } else if ((nullptr != addon_exprs) && OB_FAIL(append(all_exprs_, *addon_exprs))) {
    SQL_ENG_LOG(WARN, "failed to append addon fields", K(ret));
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::init(ObSortVecOpContext &ctx)
{
  int ret = OB_SUCCESS;
  if (is_inited()) {
    ret = OB_INIT_TWICE;
    SQL_ENG_LOG(WARN, "init twice", K(ret));
  } else {
    local_merge_sort_ = ctx.in_local_order_;
    need_rewind_ = ctx.need_rewind_;
    enable_encode_sortkey_ = ctx.enable_encode_sortkey_;
    tenant_id_ = ctx.tenant_id_;
    addon_collations_ = ctx.addon_collations_;
    sk_collations_ = ctx.prefix_pos_ > 0 ? ctx.base_sk_collations_ : ctx.sk_collations_;
    sk_exprs_ = ctx.sk_exprs_;
    addon_exprs_ = ctx.addon_exprs_;
    cmp_sk_exprs_ = enable_encode_sortkey_ ? addon_exprs_ : sk_exprs_;
    cmp_sort_collations_ = enable_encode_sortkey_ ? addon_collations_ : sk_collations_;
    eval_ctx_ = ctx.eval_ctx_;
    exec_ctx_ = ctx.exec_ctx_;
    part_cnt_ = ctx.part_cnt_;
    topn_cnt_ = ctx.topn_cnt_;
    use_heap_sort_ = is_topn_sort();
    is_fetch_with_ties_ = ctx.is_fetch_with_ties_;
    compress_type_ = ctx.compress_type_;
    int64_t batch_size = eval_ctx_->max_batch_size_;
    if (OB_FAIL(merge_sk_addon_exprs(sk_exprs_, addon_exprs_))) {
      SQL_ENG_LOG(WARN, "failed to merge sort key and addon exprs", K(ret));
    } else if (OB_FAIL(init_vec_ptrs(*sk_exprs_, sk_vec_ptrs_, eval_ctx_))) {
      SQL_ENG_LOG(WARN, "failed to init sort key vector ptrs", K(ret));
    } else if (has_addon && OB_FAIL(init_vec_ptrs(*addon_exprs_, addon_vec_ptrs_, eval_ctx_))) {
      SQL_ENG_LOG(WARN, "failed to init addon fields vector ptrs", K(ret));
    } else if (OB_FAIL(init_sort_temp_row_store(batch_size))) {
      SQL_ENG_LOG(WARN, "failed to init sort temp row store", K(ret));
    } else if (use_heap_sort_
               && OB_FAIL(sort_exprs_getter_.init(*cmp_sk_exprs_, *cmp_sort_collations_, batch_size,
                                                  *eval_ctx_))) {
      SQL_ENG_LOG(WARN, "failed to init sort key column result array", K(ret));
    } else if (OB_FAIL(comp_.init(cmp_sk_exprs_, sk_row_meta_, addon_row_meta_,
                                  cmp_sort_collations_, ctx.exec_ctx_,
                                  ctx.enable_encode_sortkey_ && !(ctx.part_cnt_ > 0)))) {
      SQL_ENG_LOG(WARN, "failed to init compare functions", K(ret));
    } else if (is_topn_sort()
               && OB_ISNULL(topn_heap_ = OB_NEWx(TopnHeap, (&mem_context_->get_malloc_allocator()),
                                                 comp_, &mem_context_->get_malloc_allocator()))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "allocate memory failed", K(ret));
    } else if (is_topn_sort() && ctx.enable_pd_topn_filter_
               && OB_FAIL(pd_topn_filter_.init(ctx, mem_context_))) {
      SQL_ENG_LOG(WARN, "failed to init pushdown topn filter", K(ret));
    } else if (batch_size > 0
               && OB_ISNULL(sk_rows_ = SK_DOWNCAST_PP(mem_context_->get_malloc_allocator().alloc(
                              sizeof(*sk_rows_) * batch_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "allocate memory failed", K(ret));
    } else if (batch_size > 0
               && OB_ISNULL(addon_rows_ = SK_DOWNCAST_PP(mem_context_->get_malloc_allocator().alloc(
                              sizeof(*addon_rows_) * batch_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "allocate memory failed", K(ret));
    } else {
      quick_sort_array_.set_block_allocator(
        ModulePageAllocator(mem_context_->get_malloc_allocator(), "SortOpRows"));
      profile_.set_exec_ctx(exec_ctx_);
      op_monitor_info_.otherstat_2_id_ = ObSqlMonitorStatIds::SORT_MERGE_SORT_ROUND;
      op_monitor_info_.otherstat_2_value_ = 1;
      ObPhysicalPlanCtx *plan_ctx = nullptr;
      const ObPhysicalPlan *phy_plan = nullptr;
      if (OB_ISNULL(plan_ctx = GET_PHY_PLAN_CTX(*exec_ctx_))) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "deserialized exec ctx without phy plan ctx set. Unexpected", K(ret));
      } else if (OB_ISNULL(phy_plan = plan_ctx->get_phy_plan())) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "error unexpected, phy plan must not be nullptr", K(ret));
      } else if (phy_plan->get_ddl_task_id() > 0) {
        op_monitor_info_.otherstat_5_id_ = ObSqlMonitorStatIds::DDL_TASK_ID;
        op_monitor_info_.otherstat_5_value_ = phy_plan->get_ddl_task_id();
      }
    }
    if (OB_SUCC(ret)) {
      inited_ = true;
      is_topn_filter_enabled_ = EVENT_CALL(EventTable::EN_SORT_IMPL_TOPN_EAGER_FILTER) == OB_SUCCESS;
      if (!is_topn_sort()) {
        rows_ = &quick_sort_array_;
      } else {
        rows_ = &(const_cast<common::ObIArray<Store_Row *> &>(topn_heap_->get_heap_data()));
      }
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::add_batch(const ObBatchRows &input_brs,
                                                              bool &sort_need_dump)
{
  return add_batch(input_brs, 0, sort_need_dump, nullptr);
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::add_batch(const ObBatchRows &input_brs,
                                                              const int64_t start_pos,
                                                              int64_t *append_row_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(use_heap_sort_ && need_dump())) {
    bool dumped = false;
    if (OB_FAIL(preprocess_dump(dumped))) {
      SQL_ENG_LOG(WARN, "failed preprocess dump", K(ret));
    } else if (dumped && OB_FAIL(do_dump())) {
      SQL_ENG_LOG(WARN, "failed to do topn dump", K(ret));
    }
  }
  bool need_load_data = true;
  if (OB_SUCC(ret)) {
    const ObBatchRows *input_brs_ptr = nullptr;
    if (is_topn_sort() && OB_NOT_NULL(topn_filter_) && OB_LIKELY(!topn_filter_->is_by_pass())) {
      need_load_data = false;
      if (OB_FAIL(load_data_to_comp(input_brs))) {
        SQL_ENG_LOG(WARN, "failed to load data", K(ret));
      } else if (OB_FAIL(topn_filter_->filter(all_exprs_, *eval_ctx_, start_pos,
                                              input_brs))) {
        SQL_ENG_LOG(WARN, "failed to do topn filter", K(ret));
      } else {
        input_brs_ptr = &(topn_filter_->get_output_brs());
      }
    } else {
      input_brs_ptr = &input_brs;
    }
    if (OB_SUCC(ret)) {
      if (use_heap_sort_) {
        ret = add_heap_sort_batch(*input_brs_ptr, start_pos, append_row_count,
                                  need_load_data);
      } else {
        ret = add_quick_sort_batch(*input_brs_ptr, start_pos, append_row_count);
      }
    }
  } else {
    SQL_ENG_LOG(WARN, "failed to add batch", K(ret));
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::add_batch(const ObBatchRows &input_brs,
                                                              const uint16_t selector[],
                                                              const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(use_heap_sort_ && need_dump())) {
    bool dumped = false;
    if (OB_FAIL(preprocess_dump(dumped))) {
      SQL_ENG_LOG(WARN, "failed preprocess dump", K(ret));
    } else if (dumped && OB_FAIL(do_dump())) {
      SQL_ENG_LOG(WARN, "failed to do topn dump", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (use_heap_sort_) {
    ret = add_heap_sort_batch(input_brs, selector, size);
  } else {
    ret = add_quick_sort_batch(selector, size);
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::add_stored_row(const ObCompactRow *store_row)
{
  int ret = OB_SUCCESS;
  ObCompactRow *sk_store_row = nullptr;
  if (OB_FAIL(sk_store_.add_row(store_row, sk_store_row))) {
    SQL_ENG_LOG(WARN, "add store row failed", K(ret), K(mem_context_->used()),
                K(get_memory_limit()));
  } else if (OB_FAIL(after_add_row(SK_DOWNCAST_P(sk_store_row)))) {
    SQL_ENG_LOG(WARN, "after add row process failed", K(ret));
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::add_stored_row(const ObCompactRow *sk_row,
                                                                   const ObCompactRow *addon_row)
{
  int ret = OB_SUCCESS;
  ObCompactRow *sk_store_row = nullptr;
  ObCompactRow *addon_store_row = nullptr;
  if (OB_FAIL(sk_store_.add_row(sk_row, sk_store_row))) {
    SQL_ENG_LOG(WARN, "add store row failed", K(ret), K(mem_context_->used()),
                K(get_memory_limit()));
  } else if (OB_FAIL(addon_store_.add_row(addon_row, addon_store_row))) {
    SQL_ENG_LOG(WARN, "add store row failed", K(ret), K(mem_context_->used()),
                K(get_memory_limit()));
  } else if (FALSE_IT(SK_DOWNCAST_P(sk_store_row)
                        ->set_addon_ptr(SK_DOWNCAST_P(addon_store_row), *sk_row_meta_))) {
  } else if (OB_FAIL(after_add_row(SK_DOWNCAST_P(sk_store_row)))) {
    SQL_ENG_LOG(WARN, "after add row process failed", K(ret));
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::add_batch_stored_row(
  int64_t &row_size, const ObCompactRow **sk_stored_rows, const ObCompactRow **addon_stored_rows)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(before_add_row())) {
    SQL_ENG_LOG(WARN, "before add row process failed", K(ret));
  } else if (has_addon) {
    for (int64_t i = 0; OB_SUCC(ret) && i < row_size; i++) {
      if (OB_FAIL(add_stored_row(sk_stored_rows[i], addon_stored_rows[i]))) {
        SQL_ENG_LOG(WARN, "add store row failed", K(ret));
      }
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < row_size; i++) {
      if (OB_FAIL(add_stored_row(sk_stored_rows[i]))) {
        SQL_ENG_LOG(WARN, "add store row failed", K(ret));
      }
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::after_add_row(Store_Row *sr)
{
  int ret = OB_SUCCESS;
  if (local_merge_sort_ && rows_->count() > 0 && nullptr != rows_->at(rows_->count() - 1)) {
    const bool less = comp_(sr, rows_->at(rows_->count() - 1));
    if (OB_SUCCESS != comp_.ret_) {
      ret = comp_.ret_;
      SQL_ENG_LOG(WARN, "compare failed", K(ret));
    } else if (less) {
      // If new is less than previous row, add nullptr to separate different
      // local order rows.
      if (OB_FAIL(rows_->push_back(nullptr))) {
        SQL_ENG_LOG(WARN, "array push back failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(rows_->push_back(sr))) {
      SQL_ENG_LOG(WARN, "array push back failed", K(ret), K(rows_->count()));
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::build_row(
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

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::add_quick_sort_batch(
  const ObBatchRows &input_brs, const int64_t start_pos /* 0 */, int64_t *append_row_count)
{
  int ret = OB_SUCCESS;
  int64_t stored_rows_cnt = 0;
  if (OB_FAIL(before_add_row())) {
    SQL_ENG_LOG(WARN, "before add row process failed", K(ret));
  } else if (OB_FAIL(add_sort_batch_row(input_brs, start_pos, stored_rows_cnt))) {
    SQL_ENG_LOG(WARN, "failed to add sort batch store row", K(ret), K(stored_rows_cnt));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stored_rows_cnt; i++) {
      if (OB_FAIL(after_add_row(sk_rows_[i]))) {
        SQL_ENG_LOG(WARN, "after add row process failed", K(ret));
      }
    }
    if (OB_NOT_NULL(append_row_count)) {
      *append_row_count = stored_rows_cnt;
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::add_quick_sort_batch(const uint16_t selector[],
                                                                         const int64_t row_size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(before_add_row())) {
    SQL_ENG_LOG(WARN, "before add row process failed", K(ret));
  } else if (OB_FAIL(add_sort_batch_row(selector, row_size))) {
    SQL_ENG_LOG(WARN, "failed to add batch store row", K(ret), K(row_size));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < row_size; i++) {
      if (OB_FAIL(after_add_row(sk_rows_[i]))) {
        SQL_ENG_LOG(WARN, "after add row process failed", K(ret));
      }
    }
  }
  return ret;
}

// copy exprs values to topn heap top row.
template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::copy_to_topn_row(Store_Row *&new_row)
{
  int ret = OB_SUCCESS;
  Store_Row *top_row = topn_heap_->top();
  if (OB_FAIL(copy_to_row(top_row))) {
    SQL_ENG_LOG(WARN, "failed to copy to row", K(ret));
  } else {
    new_row = top_row;
    topn_heap_->top() = top_row;
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::adjust_topn_heap(const Store_Row *&store_row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(topn_heap_->top())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "unexpected error.top of the heap is nullptr", K(ret),
                K(topn_heap_->count()));
  } else if (!topn_heap_->empty()) {
    if (comp_(topn_heap_->top(), *eval_ctx_)) {
      Store_Row *new_row = nullptr;
      if (OB_FAIL(copy_to_topn_row(new_row))) {
        SQL_ENG_LOG(WARN, "failed to generate new row", K(ret));
      } else if (OB_FAIL(topn_heap_->replace_top(new_row))) {
        SQL_ENG_LOG(WARN, "failed to replace top", K(ret));
      } else {
        store_row = new_row;
        if (pd_topn_filter_.enabled()) {
          pd_topn_filter_.set_need_update(true);
        }
      }
    } else {
      ret = comp_.ret_;
    }
  }
  return ret;
}

// for order by c1 desc fetch next 5 rows with ties:
//  row < heap.top: add row to ties_array_
//  row = heap.top: add row to ties_array_
//  row > heap.top: 1. replace heap top use row;
//                  2. if previous heap.top = new heap.top, add previous
//                  heap.top to ties_array_
//                     else reset ties_array_.
template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::adjust_topn_heap_with_ties(
  const Store_Row *&store_row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(topn_heap_->top())) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "unexpected error.top of the heap is nullptr", K(ret),
                K(topn_heap_->count()));
  } else if (!topn_heap_->empty()) {
    int cmp = comp_.with_ties_cmp(topn_heap_->top(), *eval_ctx_);
    bool is_alloced = false;
    bool add_ties_array = false;
    Store_Row *new_row = nullptr;
    Store_Row *copy_pre_heap_top_row = nullptr;
    Store_Row *pre_heap_top_row = topn_heap_->top();
    if (OB_FAIL(comp_.ret_) || cmp < 0) {
      /* do nothing */
    } else if (0 == cmp) {
      // equal to heap top, add row to ties array
      if (OB_FAIL(copy_to_row(new_row))) {
        SQL_ENG_LOG(WARN, "failed to generate new row", K(ret));
      } else if (OB_FAIL(ties_array_.push_back(new_row))) {
        SQL_ENG_LOG(WARN, "failed to push back ties array", K(ret));
      } else {
        store_row = new_row;
        SQL_ENG_LOG(DEBUG, "in memory topn sort with ties add ties array", KPC(new_row));
      }
    } else if (OB_FAIL(store_row_factory_.generate_new_row(
                   pre_heap_top_row, copy_pre_heap_top_row))) {
      SQL_ENG_LOG(WARN, "failed to generate new row", K(ret));
    } else if (OB_FAIL(copy_to_topn_row(new_row))) {
      SQL_ENG_LOG(WARN, "failed to generate new row", K(ret));
    } else if (OB_FAIL(topn_heap_->replace_top(new_row))) {
      SQL_ENG_LOG(WARN, "failed to replace top", K(ret));
    } else if (OB_FALSE_IT(cmp = comp_.with_ties_cmp(copy_pre_heap_top_row, topn_heap_->top()))) {
    } else if (OB_FAIL(comp_.ret_)) {
      /* do nothing */
    } else if (0 != cmp) {
      // previous heap top not equal to new heap top, clear ties array
      SQL_ENG_LOG(DEBUG, "in memory topn sort with ties clear ties array", KPC(new_row),
                  KPC(copy_pre_heap_top_row));
      if (0 != ties_array_.count()) {
        for (int64_t i = 0; i < ties_array_.count(); ++i) {
          store_row_factory_.free_row_store(ties_array_[i]);
        }
      }
      ties_array_.reset();
      store_row = new_row;
    } else if (OB_FAIL(ties_array_.push_back(copy_pre_heap_top_row))) {
      SQL_ENG_LOG(WARN, "failed to push back ties array", K(ret));
    } else {
      // previous heap top equal to new heap top, add previous heap top to ties
      // array
      store_row = new_row;
      add_ties_array = true;
      SQL_ENG_LOG(DEBUG, "in memory topn sort with ties add ties array", KPC(new_row),
                  KPC(copy_pre_heap_top_row));
    }
    if (!add_ties_array && OB_NOT_NULL(copy_pre_heap_top_row)) {
      store_row_factory_.free_row_store(copy_pre_heap_top_row);
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::copy_to_row(Store_Row *&sk_row)
{
  int ret = OB_SUCCESS;
  Store_Row *addon_row = nullptr;
  if (nullptr != sk_row && has_addon) {
    addon_row = sk_row->get_addon_ptr(*sk_row_meta_);
  }
  if (OB_FAIL(copy_to_row(*sk_exprs_, *sk_row_meta_, sk_row))) {
    SQL_ENG_LOG(WARN, "failed to copy to row", K(ret));
  } else if (has_addon) {
    if (OB_FAIL(copy_to_row(*addon_exprs_, *addon_row_meta_, addon_row))) {
      store_row_factory_.free_row_store(*sk_row_meta_, sk_row);
      SQL_ENG_LOG(WARN, "failed to copy to row", K(ret));
    } else {
      sk_row->set_addon_ptr(addon_row, *sk_row_meta_);
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::generate_last_ties_row(
  const Store_Row *orign_row)
{
  int ret = OB_SUCCESS;
  Store_Row *new_sk_row = nullptr;
  CK(nullptr == last_ties_row_);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(store_row_factory_.deep_copy_row(sk_row_meta_, orign_row, new_sk_row))) {
    SQL_ENG_LOG(WARN, "failed to copy to row", K(ret));
  } else if (has_addon) {
    Store_Row *new_addon_row = nullptr;
    Store_Row *ori_addon_row = SK_RC_DOWNCAST_P(orign_row)->get_addon_ptr(*sk_row_meta_);
    if (OB_FAIL(store_row_factory_.deep_copy_row(addon_row_meta_, ori_addon_row, new_addon_row))) {
      store_row_factory_.free_row_store(*sk_row_meta_, new_sk_row);
      SQL_ENG_LOG(WARN, "failed to copy to row", K(ret));
    } else {
      new_sk_row->set_addon_ptr(new_addon_row, *sk_row_meta_);
    }
  }
  if (OB_SUCC(ret)) {
    last_ties_row_ = new_sk_row;
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::get_next_batch_stored_rows(int64_t max_cnt,
                                                                               int64_t &read_rows)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "get next batch failed", K(ret));
  } else {
    read_rows = 0;
    blk_holder_.release();
    const int32_t extra_offset = sk_row_meta_->extra_off_;
    for (int64_t i = 0; OB_SUCC(ret) && i < max_cnt; i++) {
      const Store_Row *sr = nullptr;
      if (OB_FAIL((this->*next_stored_row_func_)(sr))) {
        // next_stored_row_func_ is safe to return OB_ITER_END twice.
        if (OB_ITER_END == ret) {
          if (read_rows > 0) {
            ret = OB_SUCCESS;
          }
          break;
        } else {
          SQL_ENG_LOG(WARN, "get stored rows failed", K(ret));
        }
      } else {
        sk_rows_[read_rows] = SK_RC_DOWNCAST_P(sr);
        __builtin_prefetch(sr, 0/* read */, 2 /*high temp locality*/);
        ++read_rows;
      }
    }
    for (int64_t i = 0; has_addon && OB_SUCC(ret) && i < read_rows; i++) {
      addon_rows_[i] = SK_RC_DOWNCAST_P(SK_DOWNCAST_P(sk_rows_[i])->get_addon_ptr(extra_offset));
      __builtin_prefetch(addon_rows_[i], 0 /* read */, 2 /*high temp locality*/);
    }
    if (OB_ITER_END == ret && !need_rewind_) {
      reuse();
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::attach_rows(const ObExprPtrIArray &exprs,
                                                                ObEvalCtx &ctx,
                                                                const RowMeta &row_meta,
                                                                const ObCompactRow **srows,
                                                                const int64_t read_rows)
{
  int ret = OB_SUCCESS;
  for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < exprs.count(); col_idx++) {
    if (T_FUN_SYS_ENCODE_SORTKEY == exprs.at(col_idx)->type_) {
      // skip, do nothing
      continue;
    } else if (OB_FAIL(exprs.at(col_idx)->init_vector_default(ctx, read_rows))) {
      LOG_WARN("fail to init vector", K(ret));
    } else {
      ObIVector *vec = exprs.at(col_idx)->get_vector(ctx);
      if (VEC_UNIFORM_CONST != vec->get_format()) {
        ret = vec->from_rows(row_meta, srows, read_rows, col_idx);
        exprs.at(col_idx)->set_evaluated_projected(ctx);
      }
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::attach_rows(const int64_t read_rows)
{
  int ret = OB_SUCCESS;
  if (!is_separate_encode_sk()
      && OB_FAIL(attach_rows(*sk_exprs_, *eval_ctx_, *sk_row_meta_, SK_UPCAST_CONST_PP(sk_rows_),
                             read_rows))) {
    SQL_ENG_LOG(WARN, "failed to attach rows", K(ret));
  } else if (has_addon) {
    if (OB_FAIL(attach_rows(*addon_exprs_, *eval_ctx_, *addon_row_meta_,
                            SK_UPCAST_CONST_PP(addon_rows_), read_rows))) {
      SQL_ENG_LOG(WARN, "failed to attach rows", K(ret));
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::add_sort_batch_row(const ObBatchRows &input_brs,
                                                                       const int64_t start_pos,
                                                                       int64_t &append_row_count)
{
  int ret = OB_SUCCESS;
  int64_t stored_rows_cnt = 0;
  if (OB_FAIL(sk_store_.add_batch(*sk_exprs_, *eval_ctx_, input_brs, stored_rows_cnt,
                                  SK_UPCAST_PP(sk_rows_), start_pos))) {
    SQL_ENG_LOG(WARN, "add store row failed", K(ret), K(mem_context_->used()), K(get_memory_limit()));
  } else if (has_addon &&
             OB_FAIL(addon_store_.add_batch(*addon_exprs_, *eval_ctx_, input_brs, stored_rows_cnt,
                                            SK_UPCAST_PP(addon_rows_), start_pos))) {
    SQL_ENG_LOG(WARN, "add store row failed", K(ret), K(mem_context_->used()), K(get_memory_limit()));
  } else {
    append_row_count = stored_rows_cnt;
    for (int64_t i = 0; OB_SUCC(ret) && i < stored_rows_cnt; i++) {
      inmem_row_size_ += sk_rows_[i]->get_row_size();
      if (has_addon) {
        sk_rows_[i]->set_addon_ptr(addon_rows_[i], *sk_row_meta_);
        inmem_row_size_ += addon_rows_[i]->get_row_size();
      }
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::add_sort_batch_row(const uint16_t selector[],
                                                                       const int64_t row_size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sk_store_.add_batch(sk_vec_ptrs_, selector, row_size, SK_UPCAST_PP(sk_rows_)))) {
    SQL_ENG_LOG(WARN, "add store row failed", K(ret), K(mem_context_->used()), K(get_memory_limit()));
  } else if (has_addon &&
             OB_FAIL(addon_store_.add_batch(addon_vec_ptrs_, selector, row_size,
                                            SK_UPCAST_PP(addon_rows_)))) {
    SQL_ENG_LOG(WARN, "add store row failed", K(ret), K(mem_context_->used()), K(get_memory_limit()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < row_size; i++) {
      inmem_row_size_ += sk_rows_[i]->get_row_size();
      if (has_addon) {
        sk_rows_[i]->set_addon_ptr(addon_rows_[i], *sk_row_meta_);
        inmem_row_size_ += addon_rows_[i]->get_row_size();
      }
    }
  }
  return ret;
}

// copy exprs values to row.
// if row space is enough reuse the space, else use the alloc get new space.
template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::copy_to_row(
  const common::ObIArray<ObExpr *> &exprs, const RowMeta &row_meta, Store_Row *&row)
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
    if (topn_cnt_ < 256) {
      buffer_len = row_size > 256 ? row_size * 4 : 1024;
    } else {
      buffer_len = row_size * 2;
    }
    if (OB_ISNULL(buf = reinterpret_cast<char *>(allocator_.alloc(buffer_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("alloc buf failed", K(ret));
    } else {
      sql_mem_processor_.alloc(buffer_len);
      inmem_row_size_ += buffer_len;
      dst = new (buf) Store_Row();
      reclaim_row = row;
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
      row->set_max_size(buffer_len, row_meta);
    }
  }
  if (nullptr != reclaim_row) {
    sql_mem_processor_.alloc(-1 * reclaim_row->get_max_size(row_meta));
    inmem_row_size_ -= reclaim_row->get_max_size(row_meta);
    allocator_.free(reclaim_row);
    reclaim_row = nullptr;
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::add_heap_sort_row(const Store_Row *&store_row)
{
  int ret = OB_SUCCESS;
  if (topn_heap_->count() == topn_cnt_ - outputted_rows_cnt_) {
    if (is_fetch_with_ties_ && OB_FAIL(adjust_topn_heap_with_ties(store_row))) {
      SQL_ENG_LOG(WARN, "failed to adjust topn heap with ties", K(ret));
    } else if (!is_fetch_with_ties_ && OB_FAIL(adjust_topn_heap(store_row))) {
      SQL_ENG_LOG(WARN, "failed to adjust topn heap", K(ret));
    }
  } else { // push back array
    Store_Row *new_sk_row = nullptr;
    int64_t topn_heap_size = topn_heap_->count();
    if (OB_FAIL(copy_to_row(new_sk_row))) {
      SQL_ENG_LOG(WARN, "failed to copy to row", K(ret));
    } else if (OB_FAIL(topn_heap_->push(new_sk_row))) {
      SQL_ENG_LOG(WARN, "failed to push back row", K(ret));
      if (topn_heap_->count() == topn_heap_size) {
        store_row_factory_.free_row_store(new_sk_row);
      }
    } else {
      store_row = new_sk_row;
      SQL_ENG_LOG(DEBUG, "in memory topn sort check add row", KPC(new_sk_row));
    }
    if (OB_SUCC(ret) && topn_heap_->count() == topn_cnt_) {
      // the first time reach heap capacity, set_need_update to update topn filter data;
      if (pd_topn_filter_.enabled()) {
        pd_topn_filter_.set_need_update(true);
      }
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::batch_eval_vector(
  const common::ObIArray<ObExpr *> &exprs, const ObBatchRows &input_brs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); ++i) {
    ObExpr *expr = exprs.at(i);
    if (OB_FAIL(expr->eval_vector(*eval_ctx_, input_brs))) {
      SQL_ENG_LOG(WARN, "eval failed", K(ret));
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::load_data_to_comp(const ObBatchRows &input_brs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(batch_eval_vector(all_exprs_, input_brs))) {
    SQL_ENG_LOG(WARN, "failed to eval vector", K(ret));
  } else if (OB_FAIL(sort_exprs_getter_.fetch_payload(input_brs))) {
    SQL_ENG_LOG(WARN, "failed to batch fetch sort key payload", K(ret));
  } else if(FALSE_IT(comp_.set_sort_key_col_result_list(
                  sort_exprs_getter_.get_sk_col_result_list()))) {
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::add_heap_sort_batch(
  const ObBatchRows &input_brs, const int64_t start_pos /* 0 */, int64_t *append_row_count,
  bool need_load_data)
{
  int ret = OB_SUCCESS;
  int64_t row_count = 0;
  const Store_Row *store_row = nullptr;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
  batch_info_guard.set_batch_size(input_brs.size_);
  if (OB_FAIL(update_max_available_mem_size_periodically())) {
    SQL_ENG_LOG(WARN, "failed to update max available mem size periodically", K(ret));
  } else if (need_load_data && OB_FAIL(load_data_to_comp(input_brs))) {
    SQL_ENG_LOG(WARN, "failed to load data", K(ret));
  } else {
    for (int64_t i = start_pos; OB_SUCC(ret) && i < input_brs.size_; i++) {
      if (input_brs.skip_->exist(i)) {
        continue;
      }
      batch_info_guard.set_batch_idx(i);
      if (OB_FAIL(add_heap_sort_row(store_row))) {
        SQL_ENG_LOG(WARN, "failed to add topn row", K(ret));
      }
      row_count++;
    }
    if (OB_SUCC(ret) && pd_topn_filter_.need_update()) {
      if (OB_FAIL(pd_topn_filter_.update_filter_data(topn_heap_->top(), sk_row_meta_))) {
        LOG_WARN("failed to update filter data", K(ret));
      }
    }
    if (OB_NOT_NULL(append_row_count)) {
      *append_row_count = row_count;
    }
  }
  return ret;
}

// if less than heap size or replace heap top, store row is new row
// otherwise, store row will not change as last result obtained.
// here, strored_rows_ only used to fetch prev_row in prefix sort batch.
template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::add_heap_sort_batch(
  const ObBatchRows &input_brs, const uint16_t selector[], const int64_t size)
{
  int ret = OB_SUCCESS;
  const Store_Row *store_row = nullptr;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
  batch_info_guard.set_batch_size(eval_ctx_->max_batch_size_);
  if (OB_FAIL(update_max_available_mem_size_periodically())) {
    SQL_ENG_LOG(WARN, "failed to update max available mem size periodically", K(ret));
  } else if (OB_FAIL(sort_exprs_getter_.fetch_payload(selector, size))) {
    SQL_ENG_LOG(WARN, "failed to batch fetch sort key payload", K(ret));
  } else {
    comp_.set_sort_key_col_result_list(sort_exprs_getter_.get_sk_col_result_list());
  }
  for (int64_t i = 0; i < size && OB_SUCC(ret); i++) {
    int64_t idx = selector[i];
    batch_info_guard.set_batch_idx(idx);
    if (OB_FAIL(add_heap_sort_row(store_row))) {
      SQL_ENG_LOG(WARN, "check need sort failed", K(ret));
    } else if (OB_NOT_NULL(store_row)) {
      sk_rows_[i] = const_cast<Store_Row *>(store_row);
    } else if (OB_NOT_NULL(topn_heap_) && OB_NOT_NULL(topn_heap_->top())) {
      sk_rows_[i] = topn_heap_->top();
    } else {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "failed to add heap sort batch", K(ret));
    }
  }
  if (OB_SUCC(ret) && pd_topn_filter_.need_update()) {
    if (OB_FAIL(pd_topn_filter_.update_filter_data(topn_heap_->top(), sk_row_meta_))) {
      LOG_WARN("failed to update filter data", K(ret));
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::update_max_available_mem_size_periodically()
{
  int ret = OB_SUCCESS;
  if (!got_first_row_) {
    got_first_row_ = true;
    // heap sort will extend rowsize twice to reuse the space
    int64_t size = OB_INVALID_ID == input_rows_ ? 0 : input_rows_ * input_width_ * 2;
    if (OB_FAIL(sql_mem_processor_.init(&mem_context_->get_malloc_allocator(), tenant_id_, size,
                                        op_monitor_info_.op_type_, op_monitor_info_.op_id_,
                                        &eval_ctx_->exec_ctx_))) {
      SQL_ENG_LOG(WARN, "failed to init sql mem processor", K(ret));
    }
  } else {
    bool updated = false;
    if (OB_FAIL(sql_mem_processor_.update_max_available_mem_size_periodically(
          &mem_context_->get_malloc_allocator(),
          [&](int64_t cur_cnt) { return topn_heap_->count() > cur_cnt; }, updated))) {
      SQL_ENG_LOG(WARN, "failed to get max available memory size", K(ret));
    } else if (updated && OB_FAIL(sql_mem_processor_.update_used_mem_size(mem_context_->used()))) {
      SQL_ENG_LOG(WARN, "failed to update used memory size", K(ret));
    }
  }
  return ret;
}
template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::eager_topn_filter(
    common::ObIArray<Store_Row *> *sorted_dumped_rows) {
  int ret = OB_SUCCESS;
  if (is_topn_sort() && is_topn_filter_enabled()) {
    // init topn_filter_ if necessarily
    if (OB_FAIL(init_eager_topn_filter(sorted_dumped_rows,
                                       eval_ctx_->max_batch_size_))) {
      SQL_ENG_LOG(WARN, "failed to prepare for topn filter", K(ret));
    } else if (OB_FAIL(eager_topn_filter_update(sorted_dumped_rows))) {
      SQL_ENG_LOG(WARN, "failed to update eager topn filter", K(ret));
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::eager_topn_filter_update(
    const common::ObIArray<Store_Row *> *sorted_dumped_rows) {
  int ret = OB_SUCCESS;
  if (topn_filter_->is_by_pass()) {
    // do nothing
  } else {
    int64_t dumped_rows_count = sorted_dumped_rows->count();
    int64_t bucket_size = topn_filter_->bucket_size();
    bool updated = true;
    for (int64_t i = bucket_size - 1;
        OB_SUCC(ret) && updated && i < dumped_rows_count; i += bucket_size) {
      if (OB_FAIL(
              topn_filter_->update_filter(sorted_dumped_rows->at(i), updated))) {
        SQL_ENG_LOG(WARN, "failed to eager topn filter update", K(ret));
      }
    }
  }
  return ret;
}

// 如果发现需要dump，则
// 1 重新获取可用内存大小
// 2 检查是否还需要dump
// 3 如果需要dump，分三种情况
//   3.0 cache_size <= mem_bound
//   全内存(这里表示之前预估不准确，同时有足够内存可用)
//       申请是否有更多内存可用，决定是否需要dump
//       3.0.1 申请内存大于等于cache size，则不dump
//       3.0.2 申请内存小于cache size，则dump，返回的是算one-pass size
//   3.1 未超过cache size，则直接dump
//   3.2 超过了cache size，则采用2*size方式申请内存，one-pass内存
//       然后继续，和之前逻辑一样
//       所以这里会导致最开始dump的partition one-pass内存较少，后面倍数cache
//       size关系的one-pass更大
template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::preprocess_dump(bool &dumped)
{
  int ret = OB_SUCCESS;
  dumped = false;
  if (OB_FAIL(sql_mem_processor_.get_max_available_mem_size(&mem_context_->get_malloc_allocator()))) {
    SQL_ENG_LOG(WARN, "failed to get max available memory size", K(ret));
  } else if (OB_FAIL(sql_mem_processor_.update_used_mem_size(mem_context_->used()))) {
    SQL_ENG_LOG(WARN, "failed to update used memory size", K(ret));
  } else {
    dumped = need_dump();
    if (dumped) {
      if (!sql_mem_processor_.is_auto_mgr()) {
        // 如果dump在非auto管理模式也需要注册到workarea
        if (OB_FAIL(sql_mem_processor_.extend_max_memory_size(&mem_context_->get_malloc_allocator(),
                                                              [&](int64_t max_memory_size) {
                                                                UNUSED(max_memory_size);
                                                                return need_dump();
                                                              },
                                                              dumped, mem_context_->used()))) {
          SQL_ENG_LOG(WARN, "failed to extend memory size", K(ret));
        }
      } else if (profile_.get_cache_size() < profile_.get_global_bound_size()) {
        // in-memory：所有数据都可以缓存，即global bound
        // size比较大，则继续看是否有更多内存可用
        if (OB_FAIL(sql_mem_processor_.extend_max_memory_size(&mem_context_->get_malloc_allocator(),
                                                              [&](int64_t max_memory_size) {
                                                                UNUSED(max_memory_size);
                                                                return need_dump();
                                                              },
                                                              dumped, mem_context_->used()))) {
          SQL_ENG_LOG(WARN, "failed to extend memory size", K(ret));
        }
        LOG_TRACE("trace sort need dump", K(dumped), K(mem_context_->used()), K(get_memory_limit()),
                  K(profile_.get_cache_size()), K(profile_.get_expect_size()));
      } else {
        // one-pass
        if (profile_.get_cache_size() <= sk_store_.get_mem_hold() + sk_store_.get_file_size()
                                           + addon_store_.get_mem_hold()
                                           + addon_store_.get_file_size()) {
          // 总体数据量超过cache
          // size，说明估算的cache不准确，需要重新估算one-pass
          // size，按照2*cache_size处理
          if (OB_FAIL(sql_mem_processor_.update_cache_size(&mem_context_->get_malloc_allocator(),
                                                           profile_.get_cache_size()
                                                             * EXTEND_MULTIPLE))) {
            SQL_ENG_LOG(WARN, "failed to update cache size", K(ret), K(profile_.get_cache_size()));
          } else {
            dumped = need_dump();
          }
        } else {
        }
      }
      SQL_ENG_LOG(INFO, "trace sort need dump", K(dumped), K(mem_context_->used()),
                  K(get_memory_limit()), K(profile_.get_cache_size()),
                  K(profile_.get_expect_size()), K(sql_mem_processor_.get_data_size()));
    }
  }
  if (OB_SUCC(ret) && dumped && OB_NOT_NULL(rows_) && rows_->empty()) {
    dumped = false; //no data to dump, try to add a batch of data directly
    LOG_TRACE("Insufficient memory, unable to store a batch of data", K(mem_context_->used()), K(get_memory_limit()),
                  K(profile_.get_cache_size()), K(profile_.get_expect_size()));
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::imms_heap_next(const Store_Row *&sk_row)
{
  Store_Row **sr = nullptr;
  const auto f = [](Store_Row **&r, bool &is_end) {
    r += 1;
    is_end = (nullptr == *r);
    return OB_SUCCESS;
  };

  int ret = heap_next(*imms_heap_, f, sr);
  if (OB_SUCC(ret)) {
    sk_row = *sr;
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::is_equal_part(const Store_Row *l,
                                                                  const Store_Row *r,
                                                                  const RowMeta &row_meta,
                                                                  bool &is_equal)
{
  int ret = OB_SUCCESS;
  is_equal = true;
  if (OB_ISNULL(l) && OB_ISNULL(r)) {
    // do nothing
  } else if (OB_ISNULL(l) || OB_ISNULL(r)) {
    is_equal = false;
  } else {
    int64_t hash_idx = sk_collations_->at(0).field_idx_;
    const uint64_t l_hash_value =
      *(reinterpret_cast<const uint64_t *>(l->get_cell_payload(row_meta, hash_idx)));
    const uint64_t r_hash_value =
      *(reinterpret_cast<const uint64_t *>(r->get_cell_payload(row_meta, hash_idx)));
    if (l_hash_value != r_hash_value) {
      is_equal = false;
    } else {
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
        auto &sort_cmp_fun = NULL_FIRST == sk_collations_->at(i).null_pos_ ?
                               e->basic_funcs_->row_null_first_cmp_ :
                               e->basic_funcs_->row_null_last_cmp_;
        l_null = l->is_null(idx);
        r_null = r->is_null(idx);
        if (l_null != r_null) {
          is_equal = false;
        } else if (l_null && r_null) {
          is_equal = true;
        } else {
          l->get_cell_payload(row_meta, idx, l_data, l_len);
          r->get_cell_payload(row_meta, idx, r_data, r_len);
          if (l_len == r_len && (0 == memcmp(l_data, r_data, l_len))) {
            is_equal = true;
          } else if (OB_FAIL(sort_cmp_fun(e->obj_meta_, e->obj_meta_, l_data, l_len, l_null, r_data,
                                          r_len, r_null, cmp_ret))) {
            SQL_ENG_LOG(WARN, "failed to compare", K(ret));
          } else {
            is_equal = (0 == cmp_ret);
          }
        }
      }
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
void ObSortVecOpImpl<Compare, Store_Row, has_addon>::set_blk_holder(
  ObTempRowStore::BlockHolder *blk_holder)
{
  DLIST_FOREACH_NORET(chunk, sort_chunks_)
  {
    chunk->sk_row_iter_.set_blk_holder(blk_holder);
    chunk->addon_row_iter_.set_blk_holder(blk_holder);
  }
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::do_partition_sort(
  const RowMeta &row_meta, common::ObIArray<Store_Row *> &rows, const int64_t rows_begin,
  const int64_t rows_end)
{
  int ret = OB_SUCCESS;
  CK(part_cnt_ > 0);
  int64_t hash_expr_cnt = 1;
  uint64_t node_cnt = rows_end - rows_begin;
  uint64_t bucket_cnt = next_pow2(std::max(16L, rows.count()));
  uint64_t shift_right = __builtin_clzll(bucket_cnt) + 1;

  if (OB_SUCC(ret)) {
    if (rows_end - rows_begin <= 0) {
      // do nothing
    } else if (rows_begin < 0 || rows_end > rows.count()) {
      ret = OB_INVALID_ARGUMENT;
      SQL_ENG_LOG(WARN, "invalid argument", K(rows_begin), K(rows_end), K(rows.count()), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (max_bucket_cnt_ < bucket_cnt) {
      if (nullptr != buckets_) {
        allocator_.free(buckets_);
        buckets_ = nullptr;
        max_bucket_cnt_ = 0;
      }
      buckets_ = (PartHashNode **)allocator_.alloc(sizeof(PartHashNode *) * bucket_cnt);
      if (OB_ISNULL(buckets_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_ENG_LOG(WARN, "failed to alloc memory", K(ret));
      } else {
        max_bucket_cnt_ = bucket_cnt;
        MEMSET(buckets_, 0, sizeof(PartHashNode *) * bucket_cnt);
      }
    } else {
      MEMSET(buckets_, 0, sizeof(PartHashNode *) * bucket_cnt);
    }
  }

  if (OB_SUCC(ret)) {
    if (max_node_cnt_ < node_cnt) {
      if (nullptr != part_hash_nodes_) {
        allocator_.free(part_hash_nodes_);
        part_hash_nodes_ = nullptr;
        max_node_cnt_ = 0;
      }
      part_hash_nodes_ = (PartHashNode *)allocator_.alloc(sizeof(PartHashNode) * node_cnt);
      if (OB_ISNULL(part_hash_nodes_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_ENG_LOG(WARN, "failed to alloc memory", K(ret));
      } else {
        max_node_cnt_ = node_cnt;
      }
    }
  }
  for (int64_t i = rows_begin; OB_SUCC(ret) && i < rows_end; ++i) {
    if (OB_ISNULL(rows.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "failed to get rows", K(ret));
    } else {
      int64_t hash_idx = sk_collations_->at(0).field_idx_;
      const uint64_t hash_value =
        *(reinterpret_cast<const uint64_t *>(rows.at(i)->get_cell_payload(row_meta, hash_idx)));
      uint64_t pos = hash_value >> shift_right; // high n bit
      PartHashNode &insert_node = part_hash_nodes_[i - rows_begin];
      PartHashNode *&bucket = buckets_[pos];
      insert_node.store_row_ = rows.at(i);
      PartHashNode *exist = bucket;
      bool equal = false;
      while (nullptr != exist && OB_SUCC(ret)) {
        if (OB_FAIL(is_equal_part(exist->store_row_, rows.at(i), row_meta, equal))) {
          SQL_ENG_LOG(WARN, "failed to check equal", K(ret));
        } else if (equal) {
          break;
        } else {
          exist = exist->hash_node_next_;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (nullptr == exist) { // insert at first node with hash_node_next.
        insert_node.part_row_next_ = nullptr;
        insert_node.hash_node_next_ = bucket;
        bucket = &insert_node;
      } else { // insert at second node with part_row_next.
        insert_node.part_row_next_ = exist->part_row_next_;
        exist->part_row_next_ = &insert_node;
      }
    }
  }

  int64_t rows_idx = rows_begin;
  ObArray<PartHashNode *> bucket_nodes;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(bucket_nodes.prepare_allocate(16))) {
      SQL_ENG_LOG(WARN, "failed to prepare allocate bucket nodes", K(ret));
    }
  }
  for (int64_t bucket_idx = 0; OB_SUCC(ret) && bucket_idx < bucket_cnt; ++bucket_idx) {
    int64_t bucket_part_cnt = 0;
    PartHashNode *bucket_node = buckets_[bucket_idx];
    if (nullptr == bucket_node) {
      continue; // no rows add here
    }
    while (OB_SUCC(ret) && nullptr != bucket_node) {
      if (OB_LIKELY(bucket_part_cnt < bucket_nodes.count())) {
        bucket_nodes.at(bucket_part_cnt) = bucket_node;
      } else {
        if (OB_FAIL(bucket_nodes.push_back(bucket_node))) {
          SQL_ENG_LOG(WARN, "failed to push back bucket node", K(ret));
        }
      }
      bucket_node = bucket_node->hash_node_next_;
      bucket_part_cnt++;
    }
    comp_.set_cmp_range(0, part_cnt_ + hash_expr_cnt);
    lib::ob_sort(&bucket_nodes.at(0), &bucket_nodes.at(0) + bucket_part_cnt, HashNodeComparer(comp_));
    comp_.set_cmp_range(part_cnt_ + hash_expr_cnt, comp_.get_cnt());
    for (int64_t i = 0; OB_SUCC(ret) && i < bucket_part_cnt; ++i) {
      int64_t rows_last = rows_idx;
      PartHashNode *part_node = bucket_nodes.at(i);
      while (nullptr != part_node) {
        rows.at(rows_idx++) = part_node->store_row_;
        part_node = part_node->part_row_next_;
      }
      if (comp_.cmp_start_ != comp_.cmp_end_) {
        if (enable_encode_sortkey_) {
          bool can_encode = true;
          ObAdaptiveQS<Store_Row> aqs(rows, row_meta, allocator_);
          if (OB_FAIL(aqs.init(rows, allocator_, rows_last, rows_idx, can_encode))) {
            SQL_ENG_LOG(WARN, "failed to init aqs", K(ret));
          } else if (can_encode) {
            aqs.sort(rows_last, rows_idx);
          } else {
            enable_encode_sortkey_ = false;
            comp_.fallback_to_disable_encode_sortkey();
            lib::ob_sort(&rows.at(0) + rows_last, &rows.at(0) + rows_idx, CopyableComparer(comp_));
          }
        } else {
          lib::ob_sort(&rows.at(0) + rows_last, &rows.at(0) + rows_idx, CopyableComparer(comp_));
        }
      }
    }
    comp_.set_cmp_range(0, comp_.get_cnt());
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::sort_inmem_data()
{
  int ret = OB_SUCCESS;
  const int64_t curr_time = ObTimeUtility::fast_current_time();
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "not init", K(ret));
  } else if (!rows_->empty()) {
    if (!use_heap_sort_ && (local_merge_sort_ || sorted_)) {
      // row already in order, do nothing.
    } else {
      int64_t begin = 0;
      if (need_imms()) {
        // is increment sort (rows add after sort()), sort the last add rows
        for (int64_t i = rows_->count() - 1; i >= 0; i--) {
          if (nullptr == rows_->at(i)) {
            begin = i + 1;
            break;
          }
        }
      }
      if (part_cnt_ > 0) {
        OZ(do_partition_sort(*sk_row_meta_, *rows_, begin, rows_->count()));
      } else if (enable_encode_sortkey_) {
        bool can_encode = true;
        ObAdaptiveQS<Store_Row> aqs(*rows_, *sk_row_meta_, mem_context_->get_malloc_allocator());
        if (OB_FAIL(aqs.init(*rows_, mem_context_->get_malloc_allocator(), begin, rows_->count(),
                             can_encode))) {
          SQL_ENG_LOG(WARN, "failed to init aqs", K(ret));
        } else if (can_encode) {
          aqs.sort(begin, rows_->count());
        } else {
          enable_encode_sortkey_ = false;
          comp_.fallback_to_disable_encode_sortkey();
          lib::ob_sort(&rows_->at(begin), &rows_->at(0) + rows_->count(), CopyableComparer(comp_));
        }
      } else {
        lib::ob_sort(&rows_->at(begin), &rows_->at(0) + rows_->count(), CopyableComparer(comp_));
      }
      if (OB_SUCCESS != comp_.ret_) {
        ret = comp_.ret_;
        SQL_ENG_LOG(WARN, "compare failed", K(ret));
      }
      op_monitor_info_.otherstat_1_id_ = ObSqlMonitorStatIds::SORT_SORTED_ROW_COUNT;
      op_monitor_info_.otherstat_1_value_ += rows_->count();
    }
    if (OB_SUCC(ret) && need_imms()) {
      if (nullptr == imms_heap_) {
        if (OB_ISNULL(imms_heap_ = OB_NEWx(IMMSHeap, (&mem_context_->get_malloc_allocator()), comp_,
                                           &mem_context_->get_malloc_allocator()))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SQL_ENG_LOG(WARN, "allocate memory failed", K(ret));
        }
      } else {
        imms_heap_->reset();
      }
      // add null sentry row first
      if (OB_FAIL(ret)) {
      } else if (nullptr != rows_->at(rows_->count() - 1) && OB_FAIL(rows_->push_back(nullptr))) {
        SQL_ENG_LOG(WARN, "array push back failed", K(ret));
      } else {
        int64_t merge_ways = rows_->count() - sk_store_.get_row_cnt();
        LOG_TRACE("do local merge sort ways", K(merge_ways), K(rows_->count()),
                  K(sk_store_.get_row_cnt()));
        if (merge_ways > INMEMORY_MERGE_SORT_WARN_WAYS) {
          // only log warning msg
          SQL_ENG_LOG(WARN, "too many merge ways", K(ret), K(merge_ways), K(rows_->count()),
                      K(sk_store_.get_row_cnt()));
        }
        Store_Row **prev = nullptr;
        for (int64_t i = 0; OB_SUCC(ret) && i < rows_->count(); i++) {
          if (nullptr == prev || nullptr == *prev) {
            if (OB_FAIL(imms_heap_->push(&rows_->at(i)))) {
              SQL_ENG_LOG(WARN, "heap push back failed", K(ret));
            }
          }
          op_monitor_info_.otherstat_1_id_ = ObSqlMonitorStatIds::SORT_SORTED_ROW_COUNT;
          op_monitor_info_.otherstat_1_value_ += 1;
          prev = &rows_->at(i);
        }
        if (OB_FAIL(ret)) {
        } else if (pd_topn_filter_.enabled()
            && OB_FAIL(pd_topn_filter_.update_filter_data(*imms_heap_->top(), sk_row_meta_))) {
          LOG_WARN("failed to update filter data", K(ret));
        }
        heap_iter_begin_ = false;
      }
    }
    const int64_t sort_cpu_time = ObTimeUtility::fast_current_time() - curr_time;
    op_monitor_info_.otherstat_3_id_ = ObSqlMonitorStatIds::SORT_INMEM_SORT_TIME;
    op_monitor_info_.otherstat_3_value_ += sort_cpu_time;
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::do_dump()
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "not init", K(ret));
  } else if (rows_->empty()) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FAIL(sort_inmem_data())) {
    SQL_ENG_LOG(WARN, "sort in-memory data failed", K(ret));
  } else {
    const int64_t level = 0;
    if (!need_imms()) {
      int64_t row_pos = 0;
      int64_t ties_array_pos = 0;
      auto input = [&](const Store_Row *&sk_row, const Store_Row *&addon_row) {
        int ret = OB_SUCCESS;
        if (row_pos >= rows_->count() && ties_array_pos >= ties_array_.count()) {
          ret = OB_ITER_END;
        } else if (row_pos < rows_->count()) {
          sk_row = rows_->at(row_pos);
          if (has_addon) {
            addon_row = sk_row->get_addon_ptr(*sk_row_meta_);
          }
          row_pos += 1;
        } else {
          sk_row = ties_array_.at(ties_array_pos);
          if (has_addon) {
            addon_row = sk_row->get_addon_ptr(*sk_row_meta_);
          }
          ties_array_pos += 1;
        }
        return ret;
      };
      if (OB_FAIL(build_chunk(level, input))) {
        SQL_ENG_LOG(WARN, "build chunk failed");
      } else if (OB_FAIL(eager_topn_filter(rows_))) {
        SQL_ENG_LOG(WARN, "eager_topn_filter failed", K(ret));
      }
    } else {
      auto input = [&](const Store_Row *&sk_row, const Store_Row *&addon_row) {
        int ret = OB_SUCCESS;
        if (OB_FAIL(imms_heap_next(sk_row))) {
          if (OB_ITER_END != ret) {
            SQL_ENG_LOG(WARN, "get row from memory heap failed", K(ret));
          }
        } else if (has_addon) {
          addon_row = sk_row->get_addon_ptr(*sk_row_meta_);
        }
        if (OB_SUCC(ret) && is_topn_sort() && is_topn_filter_enabled()) {
          sorted_dumped_rows_ptrs_.push_back(SK_RC_DOWNCAST_P(sk_row));
        }
        return ret;
      };
      if (OB_FAIL(build_chunk(level, input))) {
        SQL_ENG_LOG(WARN, "build chunk failed", K(ret));
      } else if (OB_FAIL(eager_topn_filter(&sorted_dumped_rows_ptrs_))) {
        SQL_ENG_LOG(WARN, "eager_topn_filter failed", K(ret));
      }
    }
    // reset sorted_dumped_rows_ptrs_ to reuse this array in next time
    if (0 != sorted_dumped_rows_ptrs_.count()) {
        for (int64_t i = 0; i < sorted_dumped_rows_ptrs_.count(); ++i) {
          sorted_dumped_rows_ptrs_[i] = nullptr;
        }
    }
    sorted_dumped_rows_ptrs_.reset();

    if (OB_SUCC(ret) && use_heap_sort_) {
      if (nullptr != mem_context_ && nullptr != topn_heap_) {
        for (int64_t i = 0; i < topn_heap_->count(); ++i) {
          store_row_factory_.free_row_store(topn_heap_->at(i));
          topn_heap_->at(i) = nullptr;
        }
        topn_heap_->~TopnHeap();
        mem_context_->get_malloc_allocator().free(topn_heap_);
        topn_heap_ = nullptr;
      }
      if (0 != ties_array_.count()) {
        for (int64_t i = 0; i < ties_array_.count(); ++i) {
          store_row_factory_.free_row_store(ties_array_[i]);
          ties_array_[i] = nullptr;
        }
      }
      ties_array_.reset();
      got_first_row_ = false;
      use_heap_sort_ = false;
      rows_ = &quick_sort_array_;
    }

    if (OB_SUCC(ret)) {
      heap_iter_begin_ = false;
      row_idx_ = 0;
      quick_sort_array_.reset();
      sk_store_.reset();
      addon_store_.reset();
      inmem_row_size_ = 0;
      mem_check_interval_mask_ = 1;
      sql_mem_processor_.set_number_pass(level + 1);
      sql_mem_processor_.reset();
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::before_add_row()
{
  int ret = OB_SUCCESS;
  int64_t sort_force_dump_rows = -EVENT_CALL(EventTable::EN_SORT_IMPL_FORCE_DO_DUMP);

  if (!is_inited()) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "not init", K(ret));
  } else if (OB_UNLIKELY(!got_first_row_)) {
    if (!comp_.is_inited()
        && OB_FAIL(comp_.init(cmp_sk_exprs_, sk_row_meta_, addon_row_meta_, cmp_sort_collations_,
                              exec_ctx_, enable_encode_sortkey_ && !(part_cnt_ > 0)))) {
      SQL_ENG_LOG(WARN, "init compare failed", K(ret));
    } else {
      got_first_row_ = true;
      int64_t size = OB_INVALID_ID == input_rows_ ? 0 : input_rows_ * input_width_;
      if (OB_FAIL(sql_mem_processor_.init(&mem_context_->get_malloc_allocator(), tenant_id_, size,
                                          op_monitor_info_.op_type_, op_monitor_info_.op_id_,
                                          exec_ctx_))) {
        SQL_ENG_LOG(WARN, "failed to init sql mem processor", K(ret));
      } else {
        sk_store_.set_dir_id(sql_mem_processor_.get_dir_id());
        sk_store_.set_callback(&sql_mem_processor_);
        sk_store_.set_io_event_observer(io_event_observer_);
        addon_store_.set_dir_id(sql_mem_processor_.get_dir_id());
        addon_store_.set_callback(&sql_mem_processor_);
        addon_store_.set_io_event_observer(io_event_observer_);
      }
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (sort_force_dump_rows > 0 && rows_->count() >= sort_force_dump_rows) {
    if (OB_FAIL(do_dump())) {
      SQL_ENG_LOG(WARN, "dump failed", K(ret));
    }
  } else if (!rows_->empty()) {
    bool updated = false;
    if (OB_FAIL(sql_mem_processor_.update_max_available_mem_size_periodically(
          &mem_context_->get_malloc_allocator(),
          [&](int64_t cur_cnt) { return rows_->count() > cur_cnt; }, updated))) {
      SQL_ENG_LOG(WARN, "failed to update max available mem size periodically", K(ret));
    } else if (updated && OB_FAIL(sql_mem_processor_.update_used_mem_size(mem_context_->used()))) {
      SQL_ENG_LOG(WARN, "failed to update used memory size", K(ret));
    } else if (GCONF.is_sql_operator_dump_enabled()) {
      if (rows_->count() >= MAX_ROW_CNT) {
        // 最大2G，超过2G会扩容到4G，4G申请会失败
        if (OB_FAIL(do_dump())) {
          SQL_ENG_LOG(WARN, "dump failed", K(ret));
        }
      } else if (need_dump()) {
        bool dumped = false;
        if (OB_FAIL(preprocess_dump(dumped))) {
          SQL_ENG_LOG(WARN, "failed preprocess dump", K(ret));
        } else if (dumped && OB_FAIL(do_dump())) {
          SQL_ENG_LOG(WARN, "dump failed", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret) && sorted_) {
    if (!need_rewind_) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "can not add row after sort if no need rewind", K(ret));
    } else {
      sorted_ = false;
      // add null sentry row
      if (!rows_->empty() && nullptr != rows_->at(rows_->count() - 1)) {
        if (OB_FAIL(rows_->push_back(nullptr))) {
          SQL_ENG_LOG(WARN, "array push back failed", K(ret));
        }
      }
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::build_ems_heap(int64_t &merge_ways)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "not init", K(ret));
  } else if (sort_chunks_.get_size() < 2) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "empty or one way, merge sort not needed", K(ret));
  } else if (OB_FAIL(sql_mem_processor_.get_max_available_mem_size(
               &mem_context_->get_malloc_allocator()))) {
    SQL_ENG_LOG(WARN, "failed to get max available memory size", K(ret));
  } else {
    SortVecOpChunk *first = sort_chunks_.get_first();
    if (first->level_ != first->get_next()->level_) {
      LOG_TRACE("only one chunk in current level, move to next level directly", K(first->level_));
      first->level_ = first->get_next()->level_;
    }
    int64_t max_ways = 1;
    SortVecOpChunk *c = first->get_next();
    // get max merge ways in same level
    for (int64_t i = 0; first->level_ == c->level_
         && i < std::min(sort_chunks_.get_size(), (int32_t)MAX_MERGE_WAYS) - 1; i++) {
      max_ways += 1;
      c = c->get_next();
    }

    if (nullptr == ems_heap_) {
      if (OB_ISNULL(ems_heap_ = OB_NEWx(EMSHeap, (&mem_context_->get_malloc_allocator()), comp_,
                                        &mem_context_->get_malloc_allocator()))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_ENG_LOG(WARN, "allocate memory failed", K(ret));
      }
    } else {
      ems_heap_->reset();
    }
    if (OB_SUCC(ret)) {
      merge_ways = get_memory_limit() / ObTempBlockStore::BLOCK_SIZE;
      merge_ways = std::max(2L, merge_ways);
      if (merge_ways < max_ways) {
        bool dumped = false;
        int64_t need_size = max_ways * ObTempBlockStore::BLOCK_SIZE;
        if (OB_FAIL(sql_mem_processor_.extend_max_memory_size(
              &mem_context_->get_malloc_allocator(),
              [&](int64_t max_memory_size) { return max_memory_size < need_size; }, dumped,
              mem_context_->used()))) {
          SQL_ENG_LOG(WARN, "failed to extend memory size", K(ret));
        }
        merge_ways = std::max(merge_ways, get_memory_limit() / ObTempBlockStore::BLOCK_SIZE);
      }
      merge_ways = std::min(merge_ways, max_ways);
      LOG_TRACE("do merge sort ", K(first->level_), K(merge_ways), K(sort_chunks_.get_size()),
                K(get_memory_limit()), K(sql_mem_processor_.get_profile()));
    }

    if (OB_SUCC(ret)) {
      SortVecOpChunk *chunk = sort_chunks_.get_first();
      for (int64_t i = 0; i < merge_ways && OB_SUCC(ret); i++) {
        chunk->reset_row_iter();
        if (OB_FAIL(chunk->init_row_iter())) {
          SQL_ENG_LOG(WARN, "init iterator failed", K(ret));
        } else if (OB_FAIL(chunk->get_next_row()) || nullptr == chunk->sk_row_) {
          if (OB_ITER_END == ret || OB_SUCCESS == ret) {
            ret = OB_ERR_UNEXPECTED;
            SQL_ENG_LOG(WARN, "row store is not empty, iterate end is unexpected", K(ret),
                        KP(chunk->sk_row_));
          }
          SQL_ENG_LOG(WARN, "get next row failed", K(ret));
        } else if (OB_FAIL(ems_heap_->push(chunk))) {
          SQL_ENG_LOG(WARN, "heap push failed", K(ret));
        } else {
          chunk = chunk->get_next();
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    heap_iter_begin_ = false;
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
template <typename Heap, typename NextFunc, typename Item>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::heap_next(Heap &heap, const NextFunc &func,
                                                              Item &item)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "not init", K(ret));
  } else {
    if (heap_iter_begin_) {
      if (!heap.empty()) {
        Item it = heap.top();
        bool is_end = false;
        if (OB_FAIL(func(it, is_end))) {
          SQL_ENG_LOG(WARN, "get next item fail");
        } else {
          if (is_end) {
            if (OB_FAIL(heap.pop())) {
              SQL_ENG_LOG(WARN, "heap pop failed", K(ret));
            }
          } else {
            if (OB_FAIL(heap.replace_top(it))) {
              SQL_ENG_LOG(WARN, "heap replace failed", K(ret));
            }
          }
        }
      }
    } else {
      heap_iter_begin_ = true;
    }
  }
  if (OB_SUCC(ret)) {
    if (heap.empty()) {
      ret = OB_ITER_END;
    } else {
      item = heap.top();
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::ems_heap_next(SortVecOpChunk *&chunk)
{
  const auto f = [](SortVecOpChunk *&c, bool &is_end) {
    int ret = OB_SUCCESS;
    if (OB_FAIL(c->get_next_row())) {
      if (OB_ITER_END == ret) {
        is_end = true;
        ret = OB_SUCCESS;
      } else {
        SQL_ENG_LOG(WARN, "get next row failed", K(ret));
      }
    }
    return ret;
  };
  return heap_next(*ems_heap_, f, chunk);
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::rewind()
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "not init", K(ret));
  } else if (!need_rewind_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "inited with non rewind support", K(ret));
  } else {
    if (&ObSortVecOpImpl<Compare, Store_Row, has_addon>::array_next_stored_row
        == next_stored_row_func_) {
      row_idx_ = 0;
    } else {
      if (OB_FAIL(sort())) {
        SQL_ENG_LOG(WARN, "sort rows failed", K(ret));
      }
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::imms_heap_next_stored_row(const Store_Row *&sr)
{
  return imms_heap_next(sr);
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::ems_heap_next_stored_row(const Store_Row *&sr)
{
  int ret = OB_SUCCESS;
  SortVecOpChunk *chunk = nullptr;
  if (OB_FAIL(ems_heap_next(chunk))) {
    if (OB_ITER_END != ret) {
      SQL_ENG_LOG(WARN, "get next heap row failed", K(ret));
    }
  } else if (nullptr == chunk || nullptr == chunk->sk_row_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "nullptr chunk or store row", K(ret));
  } else {
    sr = chunk->sk_row_;
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::sort()
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    SQL_ENG_LOG(WARN, "not init", K(ret));
  } else if (!rows_->empty()) {
    // in memory sort
    if (sort_chunks_.is_empty()) {
      if (OB_FAIL(sort_inmem_data())) {
        SQL_ENG_LOG(WARN, "sort in-memory data failed", K(ret));
      } else {
        if (!need_imms()) {
          row_idx_ = 0;
          next_stored_row_func_ =
            &ObSortVecOpImpl<Compare, Store_Row, has_addon>::array_next_stored_row;
        } else {
          next_stored_row_func_ =
            &ObSortVecOpImpl<Compare, Store_Row, has_addon>::imms_heap_next_stored_row;
        }
      }
    } else if (OB_FAIL(do_dump())) {
      SQL_ENG_LOG(WARN, "dump failed");
    }
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (sort_chunks_.get_size() >= 2) {
    blk_holder_.release();
    set_blk_holder(nullptr);
    // do merge sort
    int64_t ways = 0;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(build_ems_heap(ways))) {
        SQL_ENG_LOG(WARN, "build heap failed", K(ret));
      } else {
        // last merge round,
        if (ways == sort_chunks_.get_size()) {
          break;
        }
        auto input = [&](const Store_Row *&sk_row, const Store_Row *&addon_row) {
          int ret = OB_SUCCESS;
          SortVecOpChunk *chunk = nullptr;
          if (OB_FAIL(ems_heap_next(chunk))) {
            if (OB_ITER_END != ret) {
              SQL_ENG_LOG(WARN, "get next heap row failed", K(ret));
            }
          } else if (nullptr == chunk) {
            ret = OB_ERR_UNEXPECTED;
            SQL_ENG_LOG(WARN, "get chunk from heap is nullptr", K(ret));
          } else {
            sk_row = chunk->sk_row_;
            if (has_addon) {
              addon_row = chunk->addon_row_;
            }
          }
          return ret;
        };
        const int64_t level = sort_chunks_.get_first()->level_ + 1;
        op_monitor_info_.otherstat_2_id_ = ObSqlMonitorStatIds::SORT_MERGE_SORT_ROUND;
        op_monitor_info_.otherstat_2_value_ = level;
        if (OB_FAIL(build_chunk(level, input))) {
          SQL_ENG_LOG(WARN, "build chunk failed", K(ret));
        } else {
          sql_mem_processor_.set_number_pass(level + 1);
          for (int64_t i = 0; i < ways; i++) {
            SortVecOpChunk *c = sort_chunks_.remove_first();
            c->~SortVecOpChunk();
            mem_context_->get_malloc_allocator().free(c);
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      set_blk_holder(&blk_holder_);
      next_stored_row_func_ = &ObSortVecOpImpl::ems_heap_next_stored_row;
    }
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::array_next_stored_row(const Store_Row *&sk_row)
{
  int ret = OB_SUCCESS;
  if (row_idx_ >= rows_->count() && ties_array_pos_ >= ties_array_.count()) {
    ret = OB_ITER_END;
  } else if (row_idx_ < rows_->count()) {
    sk_row = rows_->at(row_idx_);
    row_idx_ += 1;
  } else {
    sk_row = ties_array_.at(ties_array_pos_);
    ties_array_pos_ += 1;
  }
  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::adjust_topn_read_rows(Store_Row **stored_rows,
                                                                          int64_t &read_cnt)
{
  int ret = OB_SUCCESS;
  int64_t start_check_pos = -1;

  if (outputted_rows_cnt_ >= topn_cnt_ && !is_fetch_with_ties_) {
    read_cnt = 0;
  } else if (outputted_rows_cnt_ >= topn_cnt_ && is_fetch_with_ties_) {
    start_check_pos = 0;
  } else if (outputted_rows_cnt_ < topn_cnt_ && !is_fetch_with_ties_) {
    read_cnt = min(read_cnt, topn_cnt_ - outputted_rows_cnt_);
  } else if (outputted_rows_cnt_ < topn_cnt_ && is_fetch_with_ties_) {
    if (read_cnt >= topn_cnt_ - outputted_rows_cnt_) {
      start_check_pos = topn_cnt_ - outputted_rows_cnt_;
      if (OB_FAIL(generate_last_ties_row(stored_rows[start_check_pos - 1]))) {
        SQL_ENG_LOG(WARN, "failed to generate last ties row", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (start_check_pos >= 0) {
      int64_t with_ties_row_cnt = 0;
      for (int64_t i = start_check_pos; i < read_cnt; ++i) {
        if (0 == comp_.with_ties_cmp(stored_rows[i], last_ties_row_)) {
          with_ties_row_cnt++;
        } else {
          break;
        }
      }
      read_cnt = start_check_pos + with_ties_row_cnt;
    }
    if (OB_UNLIKELY(OB_FAIL(comp_.ret_))) {
      ret = comp_.ret_;
    } else if (0 == read_cnt) {
      ret = OB_ITER_END;
    } else {
      outputted_rows_cnt_ += read_cnt;
    }
  }

  return ret;
}

template <typename Compare, typename Store_Row, bool has_addon>
int ObSortVecOpImpl<Compare, Store_Row, has_addon>::get_next_batch(const int64_t max_cnt,
                                                                   int64_t &read_rows)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_next_batch_stored_rows(max_cnt, read_rows))) {
    if (OB_ITER_END != ret) {
      SQL_ENG_LOG(WARN, "failed to get next batch stored rows", K(ret));
    }
  } else if (read_rows > 0 && OB_FAIL(adjust_topn_read_rows(sk_rows_, read_rows))) {
    SQL_ENG_LOG(WARN, "failed to adjust read rows with ties", K(ret));
  } else if (OB_FAIL(attach_rows(read_rows))) {
    SQL_ENG_LOG(WARN, "failed to attach rows", K(ret));
  }
  return ret;
}

/***************************** end ObSortVecOpImpl ************************************/

#undef SK_RC_DOWNCAST_P
#undef SK_DOWNCAST_P
#undef SK_CONST_UPCAST_P
#undef SK_UPCAST_PP
#undef SK_DOWNCAST_PP
#undef SK_UPCAST_CONST_PP

} // namespace sql
} // namespace oceanbase