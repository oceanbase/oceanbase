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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/aggregate/ob_hash_groupby_op.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/engine/px/ob_px_util.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "lib/charset/ob_charset.h"
#include "src/sql/engine/expr/ob_expr_util.h"

namespace oceanbase
{
using namespace common;
using namespace common::hash;

namespace sql
{

OB_SERIALIZE_MEMBER(ObGroupByDupColumnPair, org_expr, dup_expr);

OB_SERIALIZE_MEMBER((ObHashGroupBySpec, ObGroupBySpec),
  group_exprs_,cmp_funcs_, est_group_cnt_,
  org_dup_cols_, new_dup_cols_, dist_col_group_idxs_,
  distinct_exprs_);

DEF_TO_STRING(ObHashGroupBySpec)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_NAME("groupby_spec");
  J_COLON();
  pos += ObGroupBySpec::to_string(buf + pos, buf_len - pos);
  J_COMMA();
  J_KV(K_(group_exprs));
  J_OBJ_END();
  return pos;
}

int ObHashGroupBySpec::add_group_expr(ObExpr *expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FAIL(group_exprs_.push_back(expr))) {
    LOG_WARN("failed to push_back expr");
  }
  return ret;
}

//When there is stored_row_ reserved_cells, use stored_row_'s reserved_cells_ for calc equal.
//Other use row_.
int ObGroupRowHashTable::init(ObIAllocator *allocator,
                              lib::ObMemAttr &mem_attr,
                              const ObIArray<ObExpr *> &gby_exprs,
                              ObEvalCtx *eval_ctx,
                              const common::ObIArray<ObCmpFunc> *cmp_funcs,
                              int64_t initial_size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObExtendHashTable<ObGroupRowItem>::init(
              allocator, mem_attr, initial_size))) {
    LOG_WARN("failed to init extended hash table", K(ret));
  } else {
    gby_exprs_ = &gby_exprs;
    eval_ctx_ = eval_ctx;
    cmp_funcs_ = cmp_funcs;
  }
  return ret;
}

int ObGroupRowHashTable::likely_equal(
  const ObGroupRowItem &left, const ObGroupRowItem &right,
  bool &result) const
{
  // All rows is in one group, when no group by columns (group by const expr),
  // so the default %result is true
  int ret = OB_SUCCESS;
  result = true;
  ObDatum *l_cells = nullptr;
  if (!left.is_expr_row_) {
    l_cells = left.groupby_store_row_->cells();
  }
  const int64_t group_col_count = cmp_funcs_->count();
  for (int64_t i = 0; result && i < group_col_count; ++i) {
    ObDatum *l = nullptr;
    ObDatum *r = nullptr;
    if (left.is_expr_row_) {
      ObExpr *e = gby_exprs_->at(i);
      if (nullptr != e) {
        l = &e->locate_expr_datum(*eval_ctx_, left.batch_idx_);
      }
    } else {
      l = &l_cells[i];
    }
    // already evaluated when calc hash
    ObExpr *e = gby_exprs_->at(i);
    if (nullptr != e ) {
      r = &e->locate_expr_datum(*eval_ctx_, right.batch_idx_);
    }
    const bool l_isnull = nullptr == l || l->is_null();
    const bool r_isnull = nullptr == r || r->is_null();
    if (l_isnull != r_isnull) {
      result = false;
    } else if (l_isnull && r_isnull) {
      result = true;
    } else {
      // we try memcmp fist since it is likely equal
      if (l->len_ == r->len_
          && (0 == memcmp(l->ptr_, r->ptr_, l->len_))) {
        result = true;
      } else {
        int cmp_res = 0;
        if (OB_FAIL(cmp_funcs_->at(i).cmp_func_(*l, *r, cmp_res))) {
          LOG_WARN("failed to do cmp", K(ret), KPC(l), KPC(r));
        } else {
          result = (0 == cmp_res);
        }
      }
    }
  }
  return ret;
}

void ObHashGroupByOp::reset()
{
  curr_group_id_ = common::OB_INVALID_INDEX;
  cur_group_item_idx_ = 0;
  cur_group_item_buf_ = nullptr;
  part_shift_ = sizeof(uint64_t) * CHAR_BIT / 2;
  local_group_rows_.reuse();
  group_rows_arr_.reuse();
  sql_mem_processor_.reset();
  destroy_all_parts();
  group_store_.reset();
  gri_cnt_per_batch_ = 0;
  first_batch_from_store_ = true;
  is_init_distinct_data_ = false;
  use_distinct_data_ = false;
  reset_distinct_info();
  bypass_ctrl_.reset();
  by_pass_nth_group_ = 0;
  by_pass_child_brs_ = nullptr;
  by_pass_group_row_ = nullptr;
  by_pass_group_batch_ = nullptr;
  by_pass_batch_size_ = 0;
  force_by_pass_ = false;
  if (nullptr != last_child_row_) {
    last_child_row_->reset();
  }
  by_pass_brs_holder_.reset();
}

int ObHashGroupByOp::inner_open()
{
  int ret = OB_SUCCESS;
  reset();
  void *store_row_buf = nullptr;
  if (OB_FAIL(ObGroupByOp::inner_open())) {
    LOG_WARN("failed to inner_open", K(ret));
  } else if (OB_FAIL(init_mem_context())) {
    LOG_WARN("init memory entity failed", K(ret));
  } else if (!local_group_rows_.is_inited()) {
    //create bucket
    int64_t est_group_cnt = MY_SPEC.est_group_cnt_;
    int64_t est_hash_mem_size = 0;
    int64_t estimate_mem_size = 0;
    int64_t init_size = 0;
    ObMemAttr attr(ctx_.get_my_session()->get_effective_tenant_id(),
                   ObModIds::OB_HASH_NODE_GROUP_ROWS,
                   ObCtxIds::WORK_AREA);
    if (OB_FAIL(ObPxEstimateSizeUtil::get_px_size(&ctx_,
                                                  MY_SPEC.px_est_size_factor_,
                                                  est_group_cnt,
                                                  est_group_cnt))) {
      LOG_WARN("failed to get px size", K(ret));
    } else if (FALSE_IT(est_hash_mem_size = estimate_hash_bucket_size(est_group_cnt))) {
    } else if (FALSE_IT(estimate_mem_size = est_hash_mem_size + MY_SPEC.width_ * est_group_cnt)) {
    } else if (OB_FAIL(sql_mem_processor_.init(&mem_context_->get_malloc_allocator(),
                                               ctx_.get_my_session()->get_effective_tenant_id(),
                                               estimate_mem_size,
                                               MY_SPEC.type_,
                                               MY_SPEC.id_,
                                               &ctx_))) {
      LOG_WARN("failed to init sql mem processor", K(ret));
    } else if (FALSE_IT(aggr_processor_.set_dir_id(sql_mem_processor_.get_dir_id()))) {
    } else if (FALSE_IT(aggr_processor_.set_io_event_observer(&io_event_observer_))) {
    } else if (FALSE_IT(init_size =
        estimate_hash_bucket_cnt_by_mem_size(est_group_cnt,
                                             sql_mem_processor_.get_mem_bound(),
                                             est_hash_mem_size * 1. / estimate_mem_size))) {
    } else if (FALSE_IT(init_size = std::max((int64_t)MIN_GROUP_HT_INIT_SIZE, init_size))) {
    } else if (FALSE_IT(init_size = std::min((int64_t)MAX_GROUP_HT_INIT_SIZE, init_size))) {
    } else if (FALSE_IT(init_size = MY_SPEC.by_pass_enabled_ ?
                                    std::min(init_size, (int64_t)INIT_BKT_SIZE_FOR_ADAPTIVE_GBY) : init_size)) {
    } else if (OB_FAIL(append(dup_groupby_exprs_, MY_SPEC.group_exprs_))) {
      LOG_WARN("failed to append groupby exprs", K(ret));
    } else if (OB_FAIL(append(all_groupby_exprs_, dup_groupby_exprs_))) {
      LOG_WARN("failed to append groupby exprs", K(ret));
    } else if (OB_FAIL(local_group_rows_.init(
                &mem_context_->get_malloc_allocator(),
                attr,
                dup_groupby_exprs_,
                &eval_ctx_,
                &MY_SPEC.cmp_funcs_,
                init_size))) {
      LOG_WARN("fail to init hash map", K(ret));
    } else if (OB_FAIL(sql_mem_processor_.update_used_mem_size(get_mem_used_size()))) {
      LOG_WARN("fail to update_used_mem_size", "size", get_mem_used_size(), K(ret));
    } else if (OB_FAIL(group_store_.init(0,
            ctx_.get_my_session()->get_effective_tenant_id(),
            ObCtxIds::WORK_AREA,
            ObModIds::OB_HASH_NODE_GROUP_ROWS,
            false /* disable dump */,
            0))) {
      LOG_WARN("failed to init group store", K(ret));
    } else if (MY_SPEC.by_pass_enabled_ && OB_FAIL(init_by_pass_op())) {
      LOG_WARN("failed to init by pass op", K(ret));
    } else if (bypass_ctrl_.by_pass_ctrl_enabled_ &&
               MY_SPEC.llc_ndv_est_enabled_ &&
               OB_FAIL(llc_est_.init_llc_map(mem_context_->get_arena_allocator()))) {
      LOG_WARN("failed to init llc map", K(ret));
    } else {
      llc_est_.enabled_ = MY_SPEC.by_pass_enabled_ && MY_SPEC.llc_ndv_est_enabled_ && !force_by_pass_;
      LOG_TRACE("gby switch", K(MY_SPEC.id_), K(llc_est_.enabled_), K(MY_SPEC.by_pass_enabled_), K(MY_SPEC.llc_ndv_est_enabled_), K(bypass_ctrl_.by_pass_ctrl_enabled_), K(ret));
      enable_dump_ = (!(aggr_processor_.has_distinct() || aggr_processor_.has_order_by())
                     && GCONF.is_sql_operator_dump_enabled());
      group_store_.set_dir_id(sql_mem_processor_.get_dir_id());
      group_store_.set_callback(&sql_mem_processor_);
      group_store_.set_allocator(mem_context_->get_malloc_allocator());
      group_store_.set_io_event_observer(&io_event_observer_);
      op_monitor_info_.otherstat_1_value_ = init_size;
      op_monitor_info_.otherstat_1_id_ = ObSqlMonitorStatIds::HASH_INIT_BUCKET_COUNT;
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(
                                        ctx_.get_my_session()->get_effective_tenant_id()));
      if (tenant_config.is_valid()) {
        force_dump_ = tenant_config->_force_hash_groupby_dump;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tenant config", K(ret));
      }
      LOG_TRACE("trace init hash table", K(init_size), K(MY_SPEC.est_group_cnt_), K(est_group_cnt),
        K(est_hash_mem_size), K(estimate_mem_size),
        K(profile_.get_expect_size()),
        K(profile_.get_cache_size()),
        K(sql_mem_processor_.get_mem_bound()));
    }
    if (OB_SUCC(ret)) {
      if (is_vectorized()) {
        int64_t max_size = MY_SPEC.max_batch_size_;
        int64_t mem_size = max_size * (sizeof(ObChunkDatumStore::StoredRow *)
                                        + sizeof(uint64_t)
                                        + sizeof(uint64_t)
                                        + sizeof(ObGroupRowItem *)
                                        + sizeof(ObGroupRowItem *)
                                        + sizeof(uint16_t)
                                        + sizeof(bool))
                          + ObBitVector::memory_size(max_size);
        char *buf= (char *)mem_context_->get_arena_allocator().alloc(mem_size);
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret), K(mem_size), K(max_size));
        } else {
          MEMSET(buf, 0, mem_size);
          int64_t batch_rows_from_dump_pos = 0;
          int64_t hash_vals_pos = batch_rows_from_dump_pos + max_size * sizeof(ObChunkDatumStore::StoredRow *);
          int64_t base_hash_value_pos = hash_vals_pos + max_size * sizeof(uint64_t);
          int64_t gris_per_batch_pos = base_hash_value_pos;
          if (ObThreeStageAggrStage::FIRST_STAGE == MY_SPEC.aggr_stage_) {
            gris_per_batch_pos = gris_per_batch_pos + max_size * sizeof(uint64_t);
          }
          int64_t batch_row_gri_ptrs_pos = gris_per_batch_pos + max_size * sizeof(ObGroupRowItem *);
          int64_t selector_array_pos = batch_row_gri_ptrs_pos + max_size * sizeof(ObGroupRowItem *);
          int64_t is_dumped_pos = selector_array_pos + max_size * sizeof(uint16_t);
          int64_t dumped_batch_rows_pos = is_dumped_pos + max_size * sizeof(bool);

          batch_rows_from_dump_ = reinterpret_cast<const ObChunkDatumStore::StoredRow **>
                                   (buf + batch_rows_from_dump_pos);
          hash_vals_ = reinterpret_cast<uint64_t*>(buf + hash_vals_pos);
          base_hash_vals_ = reinterpret_cast<uint64_t*>(buf + base_hash_value_pos);
          gris_per_batch_ = reinterpret_cast<const ObGroupRowItem **>(buf + gris_per_batch_pos);
          batch_row_gri_ptrs_ = reinterpret_cast<const ObGroupRowItem **>(buf + batch_row_gri_ptrs_pos);
          selector_array_ = reinterpret_cast<uint16_t*>(buf + selector_array_pos);
          is_dumped_ = reinterpret_cast<bool*>(buf + is_dumped_pos);
          dumped_batch_rows_.skip_ = to_bit_vector(buf + dumped_batch_rows_pos);
          gri_cnt_per_batch_ = 0;
        }
      }
    }
  }
  const ObIArray<ObExpr*> &group_exprs = all_groupby_exprs_;
  if (OB_SUCC(ret) && group_exprs.count() <= 2 && !group_rows_arr_.inited_ && MY_SPEC.max_batch_size_ > 0) {
    bool need_init = true;
    for (int64_t i = 0; i < group_exprs.count() && need_init; i++) {
      ObExpr *expr = group_exprs.at(i);
      if (ob_is_string_tc(expr->datum_meta_.type_) &&
          1 == expr->max_length_ &&
          ObCharset::is_bin_sort(expr->datum_meta_.cs_type_)) {
      } else {
        need_init = false;
      }
    }
    if (need_init && OB_FAIL(group_rows_arr_.init(ctx_.get_allocator(), eval_ctx_, group_exprs))) {
      LOG_WARN("all group rows init failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (ObThreeStageAggrStage::FIRST_STAGE == MY_SPEC.aggr_stage_) {
      no_non_distinct_aggr_ = (0 == MY_SPEC.aggr_infos_.count());
    } else if (ObThreeStageAggrStage::SECOND_STAGE == MY_SPEC.aggr_stage_) {
      if (OB_FAIL(append(distinct_origin_exprs_, MY_SPEC.group_exprs_))) {
        LOG_WARN("failed to append distinct_origin_exprs", K(ret));
      } else {
        // fill distinct exprs
        for (int64_t i = 0; i < MY_SPEC.distinct_exprs_.count() && OB_SUCC(ret); ++i) {
          ObExpr *expr = MY_SPEC.distinct_exprs_.at(i);
          if (!has_exist_in_array(distinct_origin_exprs_, expr)) {
            if (OB_FAIL(distinct_origin_exprs_.push_back(expr))) {
              LOG_WARN("failed to push back expr", K(ret));
            }
          }
        }
        n_distinct_expr_ = distinct_origin_exprs_.count();
        // fill other exprs
        // the distinct_origin_exprs_ will insert hash table to distinct data
        for (int64_t i = 0; i < child_->get_spec().output_.count() && OB_SUCC(ret); ++i) {
          ObExpr *expr = child_->get_spec().output_.at(i);
          if (!has_exist_in_array(distinct_origin_exprs_, expr)) {
            if (OB_FAIL(distinct_origin_exprs_.push_back(expr))) {
              LOG_WARN("failed to push back expr", K(ret));
            }
          }
        }
        if (OB_SUCC(ret) && is_vectorized()) {
          int64_t max_size = MY_SPEC.max_batch_size_;
          int64_t distinct_selector_pos = 0;
          int64_t distinct_hash_value_pos = distinct_selector_pos + max_size * sizeof(uint16_t);
          int64_t distinct_skip_pos = distinct_hash_value_pos + max_size * sizeof(uint64_t);
          int64_t total_size = distinct_skip_pos + ObBitVector::memory_size(max_size);
          char *buf= (char *)mem_context_->get_arena_allocator().alloc(total_size);
          if (OB_ISNULL(buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("allocate memory failed", K(ret), K(total_size), K(max_size));
          } else {
            MEMSET(buf, 0, total_size);
            distinct_selector_ = reinterpret_cast<uint16_t*>(buf + distinct_selector_pos);
            distinct_hash_values_ = reinterpret_cast<uint64_t*>(buf + distinct_hash_value_pos);
            distinct_skip_ = to_bit_vector(buf + distinct_skip_pos);
          }
        }
      }
    }
  }
  return ret;
}

int ObHashGroupByOp::init_group_store()
{
  int ret = OB_SUCCESS;
  group_store_.reset();
  if (OB_FAIL(group_store_.init(0,
          ctx_.get_my_session()->get_effective_tenant_id(),
          ObCtxIds::WORK_AREA,
          ObModIds::OB_HASH_NODE_GROUP_ROWS,
          false /* disable dump */,
          0))) {
    LOG_WARN("failed to init group store", K(ret));
  } else {
    group_store_.set_dir_id(sql_mem_processor_.get_dir_id());
    group_store_.set_callback(&sql_mem_processor_);
    group_store_.set_allocator(mem_context_->get_malloc_allocator());
    group_store_.set_io_event_observer(&io_event_observer_);
  }
  return ret;
}

int ObHashGroupByOp::inner_close()
{
  sql_mem_processor_.unregister_profile();
  distinct_sql_mem_processor_.unregister_profile();
  curr_group_id_ = common::OB_INVALID_INDEX;
  return ObGroupByOp::inner_close();
}

int ObHashGroupByOp::init_mem_context()
{
  int ret = OB_SUCCESS;
  if (NULL == mem_context_) {
    lib::ContextParam param;
    param.set_mem_attr(ctx_.get_my_session()->get_effective_tenant_id(),
        ObModIds::OB_HASH_NODE_GROUP_ROWS,
        ObCtxIds::WORK_AREA);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
      LOG_WARN("memory entity create failed", K(ret));
    }
  }
  return ret;
}

void ObHashGroupByOp::destroy()
{
  sql_mem_processor_.unregister_profile_if_necessary();
  distinct_sql_mem_processor_.unregister_profile_if_necessary();
  dup_groupby_exprs_.reset();
  all_groupby_exprs_.reset();
  distinct_origin_exprs_.reset();
  local_group_rows_.destroy();
  sql_mem_processor_.destroy();
  distinct_sql_mem_processor_.destroy();
  is_dumped_ = nullptr;
  if (NULL != mem_context_) {
    destroy_all_parts();
    if (nullptr != last_child_row_) {
      mem_context_->get_malloc_allocator().free(last_child_row_);
    }
  }
  group_store_.reset();
  ObGroupByOp::destroy();
  distinct_data_set_.~ObHashPartInfrastructure();
  // 内存最后释放，ObHashCtx依赖于mem_context
  if (NULL != mem_context_) {
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = NULL;
  }
}

int ObHashGroupByOp::inner_switch_iterator()
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(ObGroupByOp::inner_switch_iterator())) {
    LOG_WARN("failed to rescan", K(ret));
  }
  return ret;
}

int ObHashGroupByOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  reset();
  llc_est_.reset();
  if (OB_FAIL(ObGroupByOp::inner_rescan())) {
    LOG_WARN("failed to rescan", K(ret));
  } else {
    iter_end_ = false;
    llc_est_.enabled_ = MY_SPEC.by_pass_enabled_ && MY_SPEC.llc_ndv_est_enabled_ && !force_by_pass_; // keep lc_est_.enabled_ same as inner_open()
  }
  return ret;

}

int ObHashGroupByOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  bool force_by_pass = false;
  LOG_DEBUG("before inner_get_next_row",
            K(get_aggr_used_size()), K(get_aggr_used_size()),
            K(get_hash_table_used_size()),
            K(get_dumped_part_used_size()), K(get_dump_part_hold_size()),
            K(get_mem_used_size()), K(get_mem_bound_size()),
            K(agged_dumped_cnt_), K(agged_group_cnt_), K(agged_row_cnt_));
  if (iter_end_) {
    ret = OB_ITER_END;
  } else if (curr_group_id_ < 0 && !bypass_ctrl_.by_passing()) {
    if (bypass_ctrl_.by_pass_ctrl_enabled_ && force_by_pass_) {
      curr_group_id_ = 0;
      bypass_ctrl_.start_by_pass();
      LOG_TRACE("force by pass open");
    } else if (OB_FAIL(load_data())) {
      LOG_WARN("load data failed", K(ret));
    } else {
      curr_group_id_ = 0;
    }
  } else {
    ++curr_group_id_;
  }

  if (OB_SUCC(ret) && !bypass_ctrl_.by_passing() && !bypass_ctrl_.processing_ht()) {
    if (OB_UNLIKELY(curr_group_id_ >= local_group_rows_.size())) {
      if (dumped_group_parts_.is_empty() && !is_init_distinct_data_) {
        op_monitor_info_.otherstat_2_value_ = agged_group_cnt_;
        op_monitor_info_.otherstat_2_id_ = ObSqlMonitorStatIds::HASH_ROW_COUNT;
        op_monitor_info_.otherstat_3_value_ =
            max(local_group_rows_.get_bucket_num(), op_monitor_info_.otherstat_3_value_);
        op_monitor_info_.otherstat_3_id_ = ObSqlMonitorStatIds::HASH_BUCKET_COUNT;
        ret = OB_ITER_END;
        iter_end_ = true;
        reset();
      } else {
        if (OB_FAIL(load_data())) {
          LOG_WARN("load data failed", K(ret));
        } else {
          op_monitor_info_.otherstat_3_value_ =
            max(local_group_rows_.get_bucket_num(), op_monitor_info_.otherstat_3_value_);
          op_monitor_info_.otherstat_3_id_ = ObSqlMonitorStatIds::HASH_BUCKET_COUNT;
          curr_group_id_ = 0;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    clear_evaluated_flag();
    if (curr_group_id_ >= local_group_rows_.size()) {
      if (bypass_ctrl_.processing_ht()
          && bypass_ctrl_.rebuild_times_exceeded()) {
        bypass_ctrl_.start_by_pass();
        bypass_ctrl_.reset_rebuild_times();
        bypass_ctrl_.reset_state();
        calc_avg_group_mem();
      }
      if (!bypass_ctrl_.by_passing()) {
        if (OB_FAIL(by_pass_restart_round())) {
          LOG_WARN("failed to restart ", K(ret));
        } else if (OB_FAIL(init_by_pass_group_row_item())) {
          LOG_WARN("failed to init by pass row", K(ret));
        } else if (OB_FAIL(load_data())) {
          if (OB_ITER_END == ret) {
            iter_end_ = true;
          } else {
            LOG_WARN("failed to laod data", K(ret));
          }
        } else if (curr_group_id_ >= local_group_rows_.size()) {
          ret = OB_ITER_END;
          iter_end_ = true;
          reset();
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (bypass_ctrl_.by_passing()) {
      if (OB_FAIL(load_one_row())) {
        LOG_WARN("failed to load one row", K(ret));
      }
    } else if (FALSE_IT(clear_evaluated_flag())) {
    } else if (OB_FAIL(restore_groupby_datum())) {
      LOG_WARN("failed to restore_groupby_datum", K(ret));
    } else if (OB_FAIL(aggr_processor_.collect(curr_group_id_))) {
      LOG_WARN("failed to collect result", K(ret));
    }
  }
  LOG_DEBUG("after inner_get_next_row",
            K(get_aggr_used_size()), K(get_aggr_used_size()),
            K(get_hash_table_used_size()),
            K(get_dumped_part_used_size()), K(get_dump_part_hold_size()),
            K(get_mem_used_size()), K(get_mem_bound_size()),
            K(agged_dumped_cnt_), K(agged_group_cnt_), K(agged_row_cnt_));
  return ret;
}

// select c1,count(distinct c2),sum(distinct c3),max(c4) from group by c1;
// groupby exprs:   [c1, aggr_code, c2]
// purmutation:   0 - [c1, aggr_code, c2, null]
//                1 - [c1, aggr_code, null, c3]
int ObHashGroupByOp::next_duplicate_data_permutation(
  int64_t &nth_group, bool &last_group, const ObBatchRows *child_brs, bool &insert_group_ht)
{
  int ret = OB_SUCCESS;
  last_group = true;
  insert_group_ht = true;
  if (ObThreeStageAggrStage::NONE_STAGE == MY_SPEC.aggr_stage_) {
    // non-three stage aggregation
    LOG_DEBUG("debug write aggr code", K(ret), K(last_group), K(nth_group));
  } else if (ObThreeStageAggrStage::FIRST_STAGE == MY_SPEC.aggr_stage_) {
    int64_t first_idx = MY_SPEC.aggr_code_idx_ + 1;
    int64_t start_idx = nth_group < MY_SPEC.dist_col_group_idxs_.count() ?
                        0 == nth_group ? first_idx : MY_SPEC.dist_col_group_idxs_.at(nth_group - 1):
                        all_groupby_exprs_.count();
    int64_t end_idx = nth_group < MY_SPEC.dist_col_group_idxs_.count() ?
                      MY_SPEC.dist_col_group_idxs_.at(nth_group):
                      all_groupby_exprs_.count();
    // firstly set distinct expr to  null
    for (int64_t i = first_idx; i < all_groupby_exprs_.count(); i++) {
      dup_groupby_exprs_.at(i) = nullptr;
      LOG_DEBUG("debug set groupby_expr null", K(i),
        K(MY_SPEC.group_exprs_.count()), K(all_groupby_exprs_.count()));
    }
    last_group = nth_group >= MY_SPEC.dist_col_group_idxs_.count() ? true : false;
    // secondly fill the distinct expr
    for (int64_t i = start_idx; i < end_idx && OB_SUCC(ret); i++) {
      // set original distinct expr???
      if (0 > i - first_idx) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status: start_idx is invalid", K(ret), K(i), K(first_idx), K(end_idx));
      } else {
        dup_groupby_exprs_.at(i) = MY_SPEC.org_dup_cols_.at(i - first_idx);
        LOG_DEBUG("debug set groupby_expr", K(i), K(MY_SPEC.group_exprs_.count()),
          K(all_groupby_exprs_.count()), K(i - first_idx));
      }
    }
    // set aggregate code
    if (OB_FAIL(ret)) {
    } else if (0 >= first_idx) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: first distinct idx is not zero", K(ret));
    } else if (OB_NOT_NULL(child_brs)) {
      const int64_t aggr_code = last_group ? MY_SPEC.dist_col_group_idxs_.count() : nth_group;
      ObExpr *aggr_code_expr = all_groupby_exprs_.at(first_idx - 1);
      ObDatum *datums = aggr_code_expr->locate_datums_for_update(eval_ctx_, child_brs->size_);
      ObBitVector &eval_flags = aggr_code_expr->get_evaluated_flags(eval_ctx_);
      for (int64_t i = 0; i < child_brs->size_; ++i) {
        if (child_brs->skip_->exist(i)) {
          continue;
        }
        datums[i].set_int(aggr_code);
        eval_flags.set(i);
      }
      aggr_code_expr->set_evaluated_projected(eval_ctx_);
      LOG_DEBUG("debug write aggr code", K(ret), K(aggr_code), K(first_idx));
    } else {
      // disable rowsets
      const int64_t aggr_code = last_group ? MY_SPEC.dist_col_group_idxs_.count() : nth_group;
      ObExpr *aggr_code_expr = all_groupby_exprs_.at(first_idx - 1);
      ObDatum &datum = aggr_code_expr->locate_datum_for_write(eval_ctx_);
      datum.set_int(aggr_code);
      aggr_code_expr->set_evaluated_projected(eval_ctx_);

      LOG_DEBUG("debug write aggr code", K(ret), K(aggr_code), K(first_idx));
    }
    LOG_DEBUG("debug write aggr code", K(ret), K(last_group), K(nth_group), K(first_idx),
      K(no_non_distinct_aggr_), K(start_idx), K(end_idx));
  } else if (ObThreeStageAggrStage::SECOND_STAGE == MY_SPEC.aggr_stage_) {
    if (!use_distinct_data_) {
      //get the aggr_code and then insert group hash-table or insert duplicate hash-table
      if (nullptr == child_brs) {
        ObDatum *datum = nullptr;
        int64_t aggr_code = -1;
        ObExpr *aggr_code_expr = all_groupby_exprs_.at(MY_SPEC.aggr_code_idx_);
        if (OB_FAIL(aggr_code_expr->eval(eval_ctx_, datum))) {
          LOG_WARN("failed to eval aggr_code_expr", K(ret));
        } else {
          aggr_code = datum->get_int();
          insert_group_ht = (aggr_code >= MY_SPEC.dist_aggr_group_idxes_.count());
          LOG_DEBUG("debug insert group hash table", K(insert_group_ht), K(aggr_code),
            K(MY_SPEC.dist_aggr_group_idxes_.count()));
        }
      } else {
        distinct_skip_->reset(child_brs->size_);
      }
    }
  } else {
    // ret = OB_ERR_UNEXPECTED;
    // LOG_WARN("unexpected status: invalid aggr stage", K(ret), K(MY_SPEC.aggr_stage_));
  }
  ++nth_group;
  return ret;
}

int ObHashGroupByOp::init_distinct_info(bool is_part)
{
  int ret = OB_SUCCESS;
  int64_t est_rows = is_part
                    ? distinct_data_set_.get_cur_part_row_cnt(InputSide::LEFT)
                    : MY_SPEC.rows_;
  int64_t est_size = is_part ? distinct_data_set_.get_cur_part_file_size(InputSide::LEFT) : est_rows * MY_SPEC.width_;
  int64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
  if (!is_part && OB_FAIL(ObPxEstimateSizeUtil::get_px_size(
      &ctx_, MY_SPEC.px_est_size_factor_, est_rows, est_rows))) {
    LOG_WARN("failed to get px size", K(ret));
  } else if (OB_FAIL(distinct_sql_mem_processor_.init(
                  &mem_context_->get_malloc_allocator(),
                  tenant_id,
                  est_size,
                  MY_SPEC.type_,
                  MY_SPEC.id_,
                  &ctx_))) {
    LOG_WARN("failed to init sql mem processor", K(ret));
  } else if (is_part) {
    // do nothing
  } else if (OB_FAIL(distinct_data_set_.init(tenant_id,
      GCONF.is_sql_operator_dump_enabled(),
      true, true, 1, &distinct_sql_mem_processor_))) {
    LOG_WARN("failed to init hash partition infrastructure", K(ret));
  } else if (FALSE_IT(distinct_data_set_.set_io_event_observer(&io_event_observer_))) {
  } else if (0 == distinct_origin_exprs_.count()
          || 0 == n_distinct_expr_
          || distinct_origin_exprs_.count() < n_distinct_expr_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: distinct origin exprs is empty", K(ret));
  } else if (OB_FAIL(hash_funcs_.init(n_distinct_expr_))) {
    LOG_WARN("failed to init hash funcs", K(ret));
  } else if (OB_FAIL(cmp_funcs_.init(n_distinct_expr_))) {
    LOG_WARN("failed to init cmp funcs", K(ret));
  } else if (OB_FAIL(sort_collations_.init(n_distinct_expr_))) {
    LOG_WARN("failed to init sort collations", K(ret));
  } else {
    for (int64_t i = 0; i < n_distinct_expr_ && OB_SUCC(ret); ++i) {
      ObHashFunc hash_func;
      ObExpr *expr = distinct_origin_exprs_.at(i);
      ObOrderDirection order_direction = default_asc_direction();
      bool is_ascending = is_ascending_direction(order_direction);
      ObSortFieldCollation field_collation(i,
        expr->datum_meta_.cs_type_,
        is_ascending,
        (is_null_first(order_direction) ^ is_ascending) ? NULL_LAST : NULL_FIRST);
      ObCmpFunc cmp_func;
      cmp_func.cmp_func_ = ObDatumFuncs::get_nullsafe_cmp_func(
                            expr->datum_meta_.type_,
                            expr->datum_meta_.type_,
                            NULL_LAST,//这里null last还是first无所谓
                            expr->datum_meta_.cs_type_,
                            expr->datum_meta_.scale_,
                            lib::is_oracle_mode(),
                            expr->obj_meta_.has_lob_header(),
                            expr->datum_meta_.precision_,
                            expr->datum_meta_.precision_);
      hash_func.hash_func_ = expr->basic_funcs_->murmur_hash_v2_;
      hash_func.batch_hash_func_ = expr->basic_funcs_->murmur_hash_v2_batch_;
      if (OB_FAIL(hash_funcs_.push_back(hash_func))) {
        LOG_WARN("failed to push back hash function", K(ret));
      } else if (OB_FAIL(cmp_funcs_.push_back(cmp_func))) {
        LOG_WARN("failed to push back hash function", K(ret));
      } else if (OB_FAIL(sort_collations_.push_back(field_collation))) {
        LOG_WARN("failed to push back hash function", K(ret));
      }
    }

    int64_t est_bucket_num = distinct_data_set_.est_bucket_count(est_rows, MY_SPEC.width_,
                                MIN_GROUP_HT_INIT_SIZE, MAX_GROUP_HT_INIT_SIZE);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(distinct_data_set_.set_funcs(&hash_funcs_, &sort_collations_,
        &cmp_funcs_, &eval_ctx_))) {
      LOG_WARN("failed to set funcs", K(ret));
    } else if (OB_FAIL(distinct_data_set_.start_round())) {
      LOG_WARN("failed to start round", K(ret));
    } else if (OB_FAIL(distinct_data_set_.init_hash_table(est_bucket_num,
        MIN_GROUP_HT_INIT_SIZE, MAX_GROUP_HT_INIT_SIZE))) {
      LOG_WARN("failed to init hash table", K(ret));
    } else {
      is_init_distinct_data_ = true;
      LOG_DEBUG("debug distinct_origin_exprs_", K(distinct_origin_exprs_.count()),
        K(hash_funcs_.count()), K(cmp_funcs_.count()), K(sort_collations_.count()),
        K(MY_SPEC.aggr_stage_), K(n_distinct_expr_));
      if (is_vectorized()) {
        if (OB_FAIL(distinct_data_set_.init_my_skip(MY_SPEC.max_batch_size_))) {
          LOG_WARN("failed to init hp skip", K(ret));
        } else if (OB_FAIL(distinct_data_set_.init_items(MY_SPEC.max_batch_size_))) {
          LOG_WARN("failed to init items", K(ret));
        } else if (OB_FAIL(distinct_data_set_.init_distinct_map(MY_SPEC.max_batch_size_))) {
          LOG_WARN("failed to init distinct map", K(ret));
        }
      }
    }
  }
  return ret;
}

void ObHashGroupByOp::reset_distinct_info()
{
  hash_funcs_.destroy();
  cmp_funcs_.destroy();
  sort_collations_.destroy();
  distinct_data_set_.destroy_my_skip();
  distinct_data_set_.destroy_items();
  distinct_data_set_.destroy_distinct_map();
  distinct_data_set_.reset();
}

int ObHashGroupByOp::finish_insert_distinct_data()
{
  int ret = OB_SUCCESS;
  // get dumped partition
  if (is_init_distinct_data_) {
    if (OB_FAIL(distinct_data_set_.finish_insert_row())) {
      LOG_WARN("failed to finish to insert row", K(ret));
    } else if (OB_FAIL(distinct_data_set_.open_hash_table_part())) {
      LOG_WARN("failed to open hash table part", K(ret));
    } else {
      LOG_DEBUG("finish insert row and open hash table part", K(ret), K(is_init_distinct_data_));
    }
  }
  LOG_DEBUG("open hash table part", K(ret), K(is_init_distinct_data_));
  return ret;
}

int ObHashGroupByOp::insert_distinct_data()
{
  int ret = OB_SUCCESS;
  bool has_exists = false;
  bool inserted = false;
  const ObChunkDatumStore::StoredRow *store_row = nullptr;
  if (!is_init_distinct_data_ && OB_FAIL(init_distinct_info(false))) {
    LOG_WARN("failed to init distinct info", K(ret));
  } else if (OB_FAIL(distinct_data_set_.insert_row(distinct_origin_exprs_, has_exists, inserted))) {
    LOG_WARN("failed to insert row", K(ret));
  } else {
    LOG_DEBUG("debug insert distinct row", K(has_exists), K(inserted));
  }
  return ret;
}

int ObHashGroupByOp::insert_all_distinct_data()
{
  int ret = OB_SUCCESS;
  bool has_exists = false;
  bool inserted = false;
  const ObChunkDatumStore::StoredRow *store_row = nullptr;
  while (OB_SUCC(ret)) {
    clear_evaluated_flag();
    ret = distinct_data_set_.get_left_next_row(store_row, distinct_origin_exprs_);
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
      // get dumped partition
      if (OB_FAIL(distinct_data_set_.finish_insert_row())) {
        LOG_WARN("failed to finish to insert row", K(ret));
      } else if (OB_FAIL(distinct_data_set_.close_cur_part(InputSide::LEFT))) {
        LOG_WARN("failed to close cur part", K(ret));
      } else {
        break;
      }
    } else if (OB_FAIL(ret)) {
    } else if (OB_FAIL(try_check_status())) {
      LOG_WARN("failed to check status", K(ret));
    } else if (OB_FAIL(distinct_data_set_.insert_row(distinct_origin_exprs_, has_exists, inserted))) {
      LOG_WARN("failed to insert row", K(ret));
    }
  } //end of while
  return ret;
}

int ObHashGroupByOp::get_next_distinct_row()
{
  int ret = OB_SUCCESS;
  const ObChunkDatumStore::StoredRow *store_row = nullptr;
  clear_evaluated_flag();
  if (OB_FAIL(distinct_data_set_.get_next_hash_table_row(store_row, &distinct_origin_exprs_))) {
    clear_evaluated_flag();
    if (OB_ITER_END == ret) {
      // 返回了一批distinct数据，开始处理下一批dump数据
      if (OB_FAIL(distinct_data_set_.end_round())) {
        LOG_WARN("failed to end round", K(ret));
      } else if (OB_FAIL(distinct_data_set_.start_round())) {
        LOG_WARN("failed to open round", K(ret));
      } else if (OB_FAIL(distinct_data_set_.get_next_partition(InputSide::LEFT))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to create dumped partitions", K(ret));
        }
      } else if (OB_FAIL(distinct_data_set_.open_cur_part(InputSide::LEFT))) {
        LOG_WARN("failed to open cur part");
      } else if (OB_FAIL(init_distinct_info(true))) {
        LOG_WARN("failed to init hash partition infrastruct", K(ret));
      } else if (OB_FAIL(distinct_data_set_.resize(
          distinct_data_set_.get_cur_part_row_cnt(InputSide::LEFT)))) {
        LOG_WARN("failed to init hash table", K(ret));
      } else if (OB_FAIL(insert_all_distinct_data())) {
        if (OB_ITER_END == ret) {
          ret = OB_ERR_UNEXPECTED;
        }
        LOG_WARN("failed to build distinct data", K(ret));
      } else if (OB_FAIL(distinct_data_set_.open_hash_table_part())) {
        LOG_WARN("failed to open hash table part", K(ret));
      } else if (OB_FAIL(distinct_data_set_.get_next_hash_table_row(store_row, &distinct_origin_exprs_))) {
        LOG_WARN("failed to get next row in hash table", K(ret));
      }
    } else {
      LOG_WARN("failed to get next row in hash table", K(ret));
    }
  } else {
    LOG_DEBUG("debug get distinct rows", K(ROWEXPR2STR(eval_ctx_, distinct_origin_exprs_)));
  }
  return ret;
}

int ObHashGroupByOp::load_data()
{
  int ret = OB_SUCCESS;
  ObChunkDatumStore::Iterator row_store_iter;
  DatumStoreLinkPartition *cur_part = NULL;
  int64_t part_id = 0;
  int64_t part_shift = part_shift_; // low half bits for hash table lookup.
  int64_t input_rows = get_input_rows();
  int64_t input_size = get_input_size();
  static_assert(MAX_PARTITION_CNT <= (1 << (CHAR_BIT)), "max partition cnt is too big");

  if (!dumped_group_parts_.is_empty() || (is_init_distinct_data_ && !use_distinct_data_)) {
    force_dump_ = false;
    cur_group_item_idx_ = 0;
    cur_group_item_buf_ = nullptr;
    aggr_processor_.reuse();
    sql_mem_processor_.reset();
    if (!dumped_group_parts_.is_empty()) {
      cur_part = dumped_group_parts_.remove_first();
      if (OB_ISNULL(cur_part)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("pop head partition failed", K(ret));
      } else if (OB_UNLIKELY(cur_part->part_shift_ >= sizeof(uint64_t) * CHAR_BIT)) { // part_id means level, max_part_id means max_level
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("reach max recursion depth", K(ret), K(part_id), K(cur_part->part_shift_));
      } else if (OB_FAIL(row_store_iter.init(&cur_part->datum_store_))) {
        LOG_WARN("init chunk row store iterator failed", K(ret));
      } else {
        input_rows = cur_part->datum_store_.get_row_cnt();
        part_id = cur_part->part_id_;
        part_shift = part_shift_ = cur_part->part_shift_;
        input_size = cur_part->datum_store_.get_file_size();
      }
    } else {
      input_rows = distinct_data_set_.estimate_total_count();
      part_shift_ = part_shift = sizeof(uint64_t) * CHAR_BIT / 2;
      input_size = input_rows * MY_SPEC.width_;
      // it's processed, then set false
      use_distinct_data_ = true;
      is_init_distinct_data_ = false;
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(local_group_rows_.resize(
                  &mem_context_->get_malloc_allocator(), max(2, input_rows)))) {
        LOG_WARN("failed to reuse extended hash table", K(ret));
      } else if (OB_FAIL(sql_mem_processor_.init(&mem_context_->get_malloc_allocator(),
                                                 ctx_.get_my_session()->get_effective_tenant_id(),
                                                 input_size,
                                                 MY_SPEC.type_,
                                                 MY_SPEC.id_,
                                                 &ctx_))) {
        LOG_WARN("failed to init sql mem processor", K(ret));
      } else if (OB_FAIL(init_group_store())) {
        LOG_WARN("failed to init group store", K(ret));
      } else {
        LOG_TRACE("scan new partition", K(part_id), K(input_rows), K(input_size),
                                        K(local_group_rows_.size()), K(get_mem_used_size()),
                                        K(get_aggr_used_size()), K(get_mem_bound_size()),
                                        K(get_hash_table_used_size()), K(get_dumped_part_used_size()),
                                        K(get_extra_size()));
      }
    }
  }

  // We use sort based group by for aggregation which need distinct or sort right now,
  // disable operator dump for compatibility.
  const bool is_dump_enabled = (!(aggr_processor_.has_distinct() || aggr_processor_.has_order_by())
                                && GCONF.is_sql_operator_dump_enabled());
  ObGroupRowItem curr_gr_item;
  const ObGroupRowItem *exist_curr_gr_item = NULL;
  DatumStoreLinkPartition *parts[MAX_PARTITION_CNT] = {};
  int64_t part_cnt = 0;
  int64_t est_part_cnt = 0;
  bool check_dump = false;
  ObGbyBloomFilter *bloom_filter = NULL;
  const ObChunkDatumStore::StoredRow *srow = NULL;
  for (int64_t loop_cnt = 0; OB_SUCC(ret); ++loop_cnt) {
    bypass_ctrl_.gby_process_state(local_group_rows_.get_probe_cnt(),
                                   local_group_rows_.size(),
                                   get_actual_mem_used_size());
    if (bypass_ctrl_.processing_ht()) {
      CK (OB_NOT_NULL(last_child_row_));
      OZ (last_child_row_->save_store_row(child_->get_spec().output_, eval_ctx_));
      break;
    }
    if (NULL == cur_part) {
      if (use_distinct_data_) {
        ret = get_next_distinct_row();
      } else {
        ret = child_->get_next_row();
      }
    } else {
      ret = row_store_iter.get_next_row(child_->get_spec().output_, eval_ctx_, &srow);
    }
    clear_evaluated_flag();

    if (common::OB_SUCCESS != ret) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get input row failed", K(ret));
      }
      break;
    }
    if (OB_FAIL(try_check_status())) {
      LOG_WARN("failed to check status", K(ret));
    }

    bool start_dump = (bloom_filter != NULL);
    if (OB_SUCC(ret)) {
      if (!start_dump
          && OB_FAIL(update_mem_status_periodically(loop_cnt,
                                                    input_rows,
                                                    est_part_cnt,
                                                    check_dump))) {
        LOG_WARN("failed to update usable memory size periodically", K(ret));
      }
    }

    int64_t nth_dup_data = 0;
    bool last_group = false;
    bool insert_group_ht = false;
    bool can_insert_ht = !is_dump_enabled
                          || local_group_rows_.size() < MIN_INMEM_GROUPS
                          || (!start_dump && !need_start_dump(input_rows, est_part_cnt, check_dump));
    do {
      // one-dup-data
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(next_duplicate_data_permutation(nth_dup_data, last_group, nullptr, insert_group_ht))) {
        LOG_WARN("failed to get next duplicate data purmutation", K(ret));
      } else if (last_group && no_non_distinct_aggr_) {
        // no groupby exprs, don't calculate the last duplicate data for non-distinct aggregate
        break;
      } else if (!insert_group_ht) {
        // duplicate data
        if (OB_FAIL(insert_distinct_data())) {
          LOG_WARN("failed to process duplicate data", K(ret));
        } else {
          break;
        }
      } else if (OB_FAIL(calc_groupby_exprs_hash(dup_groupby_exprs_,
          srow, curr_gr_item.hash_))) {
        LOG_WARN("failed to get_groupby_exprs_hash", K(ret));
      }
      curr_gr_item.is_expr_row_ = true;
      curr_gr_item.batch_idx_ = 0;
      LOG_DEBUG("finish calc_groupby_exprs_hash", K(curr_gr_item));
      if (OB_FAIL(ret)) {
      } else if ((!start_dump || bloom_filter->exist(curr_gr_item.hash()))
                && NULL != (exist_curr_gr_item = local_group_rows_.get(curr_gr_item))) {
        ++agged_row_cnt_;
        if (OB_ISNULL(exist_curr_gr_item->group_row_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("group_row is null", K(ret));
        } else if (last_group && OB_FAIL(aggr_processor_.process(*exist_curr_gr_item->group_row_))) {
          LOG_WARN("fail to process row", K(ret), KPC(exist_curr_gr_item));
        }
      } else {
        if (can_insert_ht) {
          ++agged_row_cnt_;
          ++agged_group_cnt_;
          ObGroupRowItem *tmp_gr_item = NULL;
          if (OB_FAIL(init_group_row_item(curr_gr_item.hash(), tmp_gr_item))) {
            LOG_WARN("failed to init_group_row_item", K(ret));
          } else if (last_group && OB_FAIL(aggr_processor_.prepare(*tmp_gr_item->group_row_))) {
            LOG_WARN("fail to prepare row", K(ret), KPC(tmp_gr_item->group_row_));
          } else if (OB_FAIL(local_group_rows_.set(*tmp_gr_item))) {
            LOG_WARN("hash table set failed", K(ret));
          }
        } else {
          if (OB_UNLIKELY(!start_dump)) {
            if (OB_FAIL(setup_dump_env(part_id, max(input_rows, loop_cnt), parts, part_cnt,
                                      bloom_filter))) {
              LOG_WARN("setup dump environment failed", K(ret));
            } else {
              start_dump = (bloom_filter != NULL);
              sql_mem_processor_.set_number_pass(part_id + 1);
            }
          }
          if (OB_SUCC(ret)) {
            ++agged_dumped_cnt_;
            const int64_t part_idx = (curr_gr_item.hash() >> part_shift) & (part_cnt - 1);
            // TODO bin.lb: copy store row to new row store directly.
            ObChunkDatumStore::StoredRow *stored_row = NULL;
            if (OB_FAIL(parts[part_idx]->datum_store_.add_row(child_->get_spec().output_,
                                                              &eval_ctx_,
                                                              &stored_row))) {
              LOG_WARN("add row failed", K(ret));
            } else if (1 != nth_dup_data) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected status: dup date must empty", K(ret));
            } else {
              *static_cast<uint64_t *>(stored_row->get_extra_payload()) = curr_gr_item.hash();
              LOG_DEBUG("finish dump", K(part_idx), K(curr_gr_item), KPC(stored_row));
              // only dump one row
              break;
            }
          }
        }
      }
    } while (!last_group && OB_SUCC(ret));
  }

  //必须先reset，否则等自动析构时候，内存都被释放了，会有问题
  row_store_iter.reset();
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
    LOG_DEBUG("debug iter end load data", K(ret), K(cur_part), K(use_distinct_data_));
  }

  if (OB_SUCC(ret) && llc_est_.enabled_) {
    if (OB_FAIL(local_group_rows_.add_hashval_to_llc_map(llc_est_))) {
      LOG_WARN("failed add llc map from ht", K(ret));
    } else {
      llc_est_.est_cnt_ += agged_row_cnt_;
      LOG_TRACE("llc map succ to add hash val from insert state", K(ret), K(llc_est_.est_cnt_), K(agged_row_cnt_), K(agged_group_cnt_));
    }
  }
  if (OB_SUCC(ret) && NULL == cur_part && !use_distinct_data_ &&
      OB_FAIL(finish_insert_distinct_data())) {
    LOG_WARN("failed to finish insert distinct data", K(ret));
  }

  // cleanup_dump_env() must be called whether success or not
  int tmp_ret = cleanup_dump_env(common::OB_SUCCESS == ret, part_id, parts, part_cnt, bloom_filter);
  if (OB_SUCCESS != tmp_ret) {
    LOG_WARN("cleanup dump environment failed", K(tmp_ret), K(ret));
    ret = OB_SUCCESS == ret ? tmp_ret : ret;
  }

  if (NULL != mem_context_ && NULL != cur_part) {
    cur_part->~DatumStoreLinkPartition();
    mem_context_->get_malloc_allocator().free(cur_part);
    cur_part = NULL;
  }
  IGNORE_RETURN sql_mem_processor_.update_used_mem_size(get_mem_used_size());

  return ret;
}

int ObHashGroupByOp::alloc_group_item(ObGroupRowItem *&item)
{
  int ret = OB_SUCCESS;
  item = NULL;
  if (0 == (cur_group_item_idx_ % BATCH_GROUP_ITEM_SIZE) &&
      OB_ISNULL(cur_group_item_buf_ = (char *)aggr_processor_.get_aggr_alloc().alloc(
        sizeof(ObGroupRowItem) * BATCH_GROUP_ITEM_SIZE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else if (OB_ISNULL(cur_group_item_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: group_item_buf is null", K(ret));
  } else {
    item = new (cur_group_item_buf_) ObGroupRowItem();
    cur_group_item_buf_ += sizeof(ObGroupRowItem);
    ++cur_group_item_idx_;
    cur_group_item_idx_ %= BATCH_GROUP_ITEM_SIZE;
  }
  return ret;
}

int ObHashGroupByOp::alloc_group_row(const int64_t group_id, ObGroupRowItem &item)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(aggr_processor_.init_one_group(group_id))) {
    LOG_WARN("failed to init one group", K(group_id), K(ret));
  } else if (OB_FAIL(aggr_processor_.get_group_row(group_id, item.group_row_))) {
    LOG_WARN("failed to get group_row", K(ret));
  } else if (OB_ISNULL(item.group_row_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("group_row is null", K(ret));
  }
  return ret;
}


int ObHashGroupByOp::init_group_row_item(const uint64_t &hash_val,
                                         ObGroupRowItem *&gr_row_item)
{
  int ret = common::OB_SUCCESS;
    ObChunkDatumStore::StoredRow *store_row = NULL;
  if (OB_FAIL(alloc_group_item(gr_row_item))) {
    LOG_WARN("alloc group item failed", K(ret));
  } else if (OB_FAIL(alloc_group_row(local_group_rows_.size(), *gr_row_item))) {
    LOG_WARN("init group row failed", K(ret));
  } else if (OB_FAIL(group_store_.add_row(dup_groupby_exprs_, &eval_ctx_, &store_row))) {
    LOG_WARN("add to row store failed", K(ret));
  } else {
    gr_row_item->hash_ = hash_val;
    gr_row_item->group_row_->groupby_store_row_ = store_row;
    gr_row_item->groupby_store_row_ = store_row;
  }

  return ret;
}

int ObHashGroupByOp::update_mem_status_periodically(const int64_t nth_cnt,
    const int64_t input_row, int64_t &est_part_cnt, bool &need_dump)
{
  int ret = common::OB_SUCCESS;
  bool updated = false;
  need_dump = false;
  if (OB_FAIL(sql_mem_processor_.update_max_available_mem_size_periodically(
                    &mem_context_->get_malloc_allocator(),
                    [&](int64_t cur_cnt){ return nth_cnt > cur_cnt; },
                    updated))) {
    LOG_WARN("failed to update usable memory size periodically", K(ret));
  } else if (updated) {
    if (OB_FAIL(sql_mem_processor_.update_used_mem_size(get_mem_used_size()))) {
      LOG_WARN("failed to update used memory size", K(ret));
    } else {
      double data_ratio = sql_mem_processor_.get_data_ratio();;
      est_part_cnt = detect_part_cnt(input_row);
      calc_data_mem_ratio(est_part_cnt, data_ratio);
      need_dump = is_need_dump(data_ratio);
    }
  }
  return ret;
}

int64_t ObHashGroupByOp::detect_part_cnt(const int64_t rows) const
{
  const double group_mem_avg = (double)get_data_size() / local_group_rows_.size();
  int64_t data_size = rows * ((double)agged_group_cnt_ / agged_row_cnt_) * group_mem_avg;
  int64_t mem_bound = get_mem_bound_size();
  int64_t part_cnt = (data_size + mem_bound) / mem_bound;
  part_cnt = next_pow2(part_cnt);
  // 这里只有75%的利用率，后续改成segment array看下是否可以去掉
  int64_t availble_mem_size = mem_bound - get_mem_used_size();
  int64_t est_dump_size = part_cnt * ObChunkRowStore::BLOCK_SIZE;
  if (0 < availble_mem_size) {
    while (est_dump_size > availble_mem_size) {
      est_dump_size >>= 1;
    }
    part_cnt = est_dump_size / ObChunkRowStore::BLOCK_SIZE;
  } else {
    // 内存使用过多
    part_cnt = MIN_PARTITION_CNT;
  }
  part_cnt = next_pow2(part_cnt);
  part_cnt = std::max(part_cnt, (int64_t)MIN_PARTITION_CNT);
  part_cnt = std::min(part_cnt, (int64_t)MAX_PARTITION_CNT);
  LOG_TRACE("trace detect partition cnt", K(data_size), K(group_mem_avg), K(get_mem_used_size()),
    K(get_mem_bound_size()), K(agged_group_cnt_), K(agged_row_cnt_),
    K(local_group_rows_.size()), K(part_cnt), K(get_aggr_used_size()),
    K(get_hash_table_used_size()), K(get_dumped_part_used_size()), K(get_aggr_hold_size()),
    K(get_dump_part_hold_size()), K(rows), K(availble_mem_size), K(est_dump_size),
    K(get_data_size()));
  return part_cnt;
}

void ObHashGroupByOp::calc_data_mem_ratio(const int64_t part_cnt, double &data_ratio)
{
  int64_t est_extra_size = (get_mem_used_size() + part_cnt * FIX_SIZE_PER_PART);
  int64_t data_size = get_mem_used_size();
  data_ratio = data_size * 1.0 / est_extra_size;
  sql_mem_processor_.set_data_ratio(data_ratio);
  LOG_TRACE("trace calc data ratio", K(data_ratio), K(est_extra_size), K(part_cnt),
            K(data_size), K(get_aggr_used_size()),
            K(profile_.get_expect_size()), K(profile_.get_cache_size()));
}

void ObHashGroupByOp::adjust_part_cnt(int64_t &part_cnt)
{
  int64_t mem_used = get_mem_used_size();
  int64_t mem_bound = get_mem_bound_size();
  int64_t dumped_remain_mem_size = mem_bound - mem_used;
  int64_t max_part_cnt = dumped_remain_mem_size / FIX_SIZE_PER_PART;
  if (max_part_cnt <= MIN_PARTITION_CNT) {
    part_cnt = MIN_PARTITION_CNT;
  } else {
    while (part_cnt > max_part_cnt) {
      part_cnt >>= 1;
    }
  }
  LOG_TRACE("trace adjust part cnt", K(part_cnt), K(max_part_cnt), K(dumped_remain_mem_size),
            K(mem_bound), K(mem_used));
}

int ObHashGroupByOp::calc_groupby_exprs_hash(ObIArray<ObExpr*> &groupby_exprs,
                                             const ObChunkDatumStore::StoredRow *srow,
                                             uint64_t &hash_value)
{
  int ret = OB_SUCCESS;
  hash_value = 99194853094755497L;
  ObDatum *result = NULL;
  if (NULL == srow) {
    for (int64_t i = 0; OB_SUCC(ret) && i < groupby_exprs.count(); ++i) {
      ObExpr *expr = groupby_exprs.at(i);
      if (OB_ISNULL(expr)) {
      } else if (OB_FAIL(expr->eval(eval_ctx_, result))) {
        LOG_WARN("eval failed", K(ret));
      } else {
        ObExprHashFuncType hash_func = expr->basic_funcs_->murmur_hash_v2_;
        if (OB_FAIL(hash_func(*result, hash_value, hash_value))) {
          LOG_WARN("hash failed", K(ret));
        }
      }
    }
  } else {
    hash_value = *static_cast<uint64_t *>(srow->get_extra_payload());
    for (int64_t i = 0; OB_SUCC(ret) && i < groupby_exprs.count(); ++i) {
      ObExpr *expr = groupby_exprs.at(i);
      if (OB_ISNULL(expr)) {
      } else if (OB_FAIL(expr->eval(eval_ctx_, result))) {
        LOG_WARN("eval failed", K(ret));
      }
    }
  }
  return ret;
}

bool ObHashGroupByOp::need_start_dump(const int64_t input_rows, int64_t &est_part_cnt,
    const bool check_dump)
{
  bool actual_need_dump = false;
  bool need_dump = false;
  const int64_t mem_used = get_mem_used_size();
  const int64_t mem_bound = get_mem_bound_size();
  double data_ratio = sql_mem_processor_.get_data_ratio();
  if (OB_UNLIKELY(0 == est_part_cnt)) {
    est_part_cnt = detect_part_cnt(input_rows);
    calc_data_mem_ratio(est_part_cnt, data_ratio);
  }
  // We continue do aggregation after we start dumping, reserve 1/8 memory for it.
  if (is_need_dump(data_ratio) || check_dump) {
    int ret = OB_SUCCESS;
    need_dump = true;
    // 在认为要发生dump时，尝试扩大获取更多内存，再决定是否dump
    if (OB_FAIL(sql_mem_processor_.extend_max_memory_size(
        &mem_context_->get_malloc_allocator(),
        [&](int64_t max_memory_size) {
            UNUSED(max_memory_size);
            data_ratio = sql_mem_processor_.get_data_ratio();;
            est_part_cnt = detect_part_cnt(input_rows);
            calc_data_mem_ratio(est_part_cnt, data_ratio);
            return is_need_dump(data_ratio);
          },
        need_dump,
        mem_used))) {
      need_dump = true;
      LOG_WARN("failed to extend max memory size", K(ret), K(need_dump));
    } else if (OB_FAIL(sql_mem_processor_.update_used_mem_size(mem_used))) {
      LOG_WARN("failed to update used memory size", K(ret), K(mem_used),K(mem_bound),K(need_dump));
    } else {
      est_part_cnt = detect_part_cnt(input_rows);
      calc_data_mem_ratio(est_part_cnt, data_ratio);
      LOG_TRACE("trace extend max memory size", K(ret), K(data_ratio),
                K(get_aggr_used_size()), K(get_extra_size()), K(mem_used), K(mem_bound),
                K(need_dump), K(est_part_cnt), K(get_mem_bound_size()),
                K(profile_.get_expect_size()), K(profile_.get_cache_size()),
                K(MY_SPEC.id_), K(get_hash_table_used_size()), K(get_data_size()),
                K(agged_group_cnt_));
    }
  }
  if ((need_dump || force_dump_)) {
    actual_need_dump = true;
    if (bypass_ctrl_.scaled_llc_est_ndv_) {
      if (bypass_ctrl_.is_max_mem_insert_state()) {
        bypass_ctrl_.set_analyze_state();
      } else {
        // do nothing
      }
      actual_need_dump = false;
      int ret = OB_SUCCESS; // no use
      LOG_TRACE("max insert is about to dump, stop it", K(ret), K(get_actual_mem_used_size()), K(get_mem_bound_size()), K(sql_mem_processor_.get_data_ratio()));
    } else if (MY_SPEC.by_pass_enabled_) {
      bypass_ctrl_.start_process_ht();
      bypass_ctrl_.set_max_rebuild_times();
      actual_need_dump = false;
    } else {
      // do nothing
    }
  }
  return actual_need_dump;
}

int ObHashGroupByOp::setup_dump_env(const int64_t part_id, const int64_t input_rows,
    DatumStoreLinkPartition **parts, int64_t &part_cnt, ObGbyBloomFilter *&bloom_filter)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(input_rows < 0) || OB_ISNULL(parts)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(input_rows), KP(parts));
  } else {
    int64_t pre_part_cnt = 0;
    part_cnt = pre_part_cnt = detect_part_cnt(input_rows);
    adjust_part_cnt(part_cnt);
    MEMSET(parts, 0, sizeof(parts[0]) * part_cnt);
    part_shift_ += min(__builtin_ctz(part_cnt), 8);
    if (OB_SUCC(ret) && NULL == bloom_filter) {
      ModulePageAllocator mod_alloc(
          ObModIds::OB_HASH_NODE_GROUP_ROWS,
          ctx_.get_my_session()->get_effective_tenant_id(),
          ObCtxIds::WORK_AREA);
      mod_alloc.set_allocator(&mem_context_->get_malloc_allocator());
      void *mem = mem_context_->get_malloc_allocator().alloc(sizeof(ObGbyBloomFilter));
      if (OB_ISNULL(mem)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else if (FALSE_IT(bloom_filter = new(mem)ObGbyBloomFilter(mod_alloc))) {
      } else if (OB_FAIL(bloom_filter->init(local_group_rows_.size()))) {
        LOG_WARN("bloom filter init failed", K(ret));
      } else {
        auto cb_func = [&](ObGroupRowItem &item) {
          int ret = OB_SUCCESS;
          if (OB_FAIL(bloom_filter->set(item.hash()))) {
            LOG_WARN("add hash value to bloom failed", K(ret));
          }
          return ret;
        };
        if (OB_FAIL(local_group_rows_.foreach(cb_func))) {
          LOG_WARN("fill bloom filter failed", K(ret));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < part_cnt; i++) {
      void *mem = mem_context_->get_malloc_allocator().alloc(sizeof(DatumStoreLinkPartition));
      if (OB_ISNULL(mem)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        parts[i] = new (mem) DatumStoreLinkPartition(&mem_context_->get_malloc_allocator());
        parts[i]->part_id_ = part_id + 1;
        parts[i]->part_shift_ = part_shift_;
        const int64_t extra_size = sizeof(uint64_t); // for hash value
        if (OB_FAIL(parts[i]->datum_store_.init(1 /* memory limit, dump immediately */,
            ctx_.get_my_session()->get_effective_tenant_id(),
            ObCtxIds::WORK_AREA,
            ObModIds::OB_HASH_NODE_GROUP_ROWS,
            true /* enable dump */,
            extra_size))) {
          LOG_WARN("init chunk row store failed", K(ret));
        } else {
          parts[i]->datum_store_.set_dir_id(sql_mem_processor_.get_dir_id());
          parts[i]->datum_store_.set_callback(&sql_mem_processor_);
          parts[i]->datum_store_.set_io_event_observer(&io_event_observer_);
        }
      }
    }
    // 如果hash group发生dump，则后续每个partition都会alloc一个buffer页，约64K，
    // 如果32个partition，则需要2M内存. 同时dump逻辑需要一些内存，需要更新
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql_mem_processor_.get_max_available_mem_size(&mem_context_->get_malloc_allocator()))) {
      LOG_WARN("failed to get max available memory size", K(ret));
    } else if (OB_FAIL(sql_mem_processor_.update_used_mem_size(get_mem_used_size()))) {
      LOG_WARN("failed to update mem size", K(ret));
    }
    LOG_TRACE("trace setup dump", K(part_cnt), K(pre_part_cnt), K(part_id));
  }
  return ret;
}

int ObHashGroupByOp::cleanup_dump_env(const bool dump_success, const int64_t part_id,
    DatumStoreLinkPartition **parts, int64_t &part_cnt, ObGbyBloomFilter *&bloom_filter)
{
  int ret = OB_SUCCESS;
  // add spill partitions to partition list
  if (dump_success && part_cnt > 0) {
    int64_t part_rows[part_cnt];
    int64_t part_file_size[part_cnt];
    for (int64_t i = 0; OB_SUCC(ret) && i < part_cnt; i++) {
      DatumStoreLinkPartition *&p = parts[i];
      if (p->datum_store_.get_row_cnt() > 0) {
        if (OB_FAIL(p->datum_store_.dump(false, true))) {
          LOG_WARN("failed to dump partition", K(ret), K(i));
        } else if (OB_FAIL(p->datum_store_.finish_add_row(true /* do dump */))) {
          LOG_WARN("do dump failed", K(ret));
        } else {
          part_rows[i] = p->datum_store_.get_row_cnt();
          part_file_size[i] = p->datum_store_.get_file_size();
          if (!dumped_group_parts_.add_first(p)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("add to list failed", K(ret));
          } else {
            p = NULL;
          }
        }
      } else {
        part_rows[i] = 0;
        part_file_size[i] = 0;
      }
    }
    LOG_TRACE("hash group by dumped", K(part_id),
        K(local_group_rows_.size()),
        K(get_mem_used_size()),
        K(get_aggr_used_size()),
        K(get_mem_bound_size()),
        K(part_cnt),
        "part rows", ObArrayWrap<int64_t>(part_rows, part_cnt),
        "part file sizes", ObArrayWrap<int64_t>(part_file_size, part_cnt));
  }

  LOG_TRACE("trace hash group by info", K(part_id),
          K(local_group_rows_.size()),
          K(get_mem_used_size()),
          K(get_aggr_used_size()),
          K(get_mem_bound_size()));

  ObArrayWrap<DatumStoreLinkPartition *> part_array(parts, part_cnt);
  if (NULL != mem_context_) {
    FOREACH_CNT(p, part_array) {
      if (NULL != *p) {
        (*p)->~DatumStoreLinkPartition();
        mem_context_->get_malloc_allocator().free(*p);
        *p = NULL;
      }
    }
    if (NULL != bloom_filter) {
      bloom_filter->~ObGbyBloomFilter();
      mem_context_->get_malloc_allocator().free(bloom_filter);
      bloom_filter = NULL;
    }
  }

  return ret;
}

void ObHashGroupByOp::destroy_all_parts()
{
  if (NULL != mem_context_) {
    while (!dumped_group_parts_.is_empty()) {
      DatumStoreLinkPartition *p = dumped_group_parts_.remove_first();
      if (NULL != p) {
        p->~DatumStoreLinkPartition();
        mem_context_->get_malloc_allocator().free(p);
      }
    }
  }
}

int ObHashGroupByOp::restore_groupby_datum()
{
  int ret = OB_SUCCESS;
  ObAggregateProcessor::GroupRow *group_row = NULL;
  if (OB_FAIL(aggr_processor_.get_group_row(curr_group_id_, group_row))) {
    LOG_WARN("failed to get group_row", K(curr_group_id_), K(ret));
  } else if (OB_ISNULL(group_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("group_row or groupby_datums_ is null", K(curr_group_id_), KP(group_row), K(ret));
  } else if (OB_FAIL(group_row->groupby_store_row_->to_expr(all_groupby_exprs_, eval_ctx_))) {
    LOG_WARN("failed to convert store row to expr", K(ret));
  }
  LOG_DEBUG("finish restore_groupby_datum", K(MY_SPEC.group_exprs_), K(ret),
    K(ROWEXPR2STR(eval_ctx_, all_groupby_exprs_)));
  return ret;
}

int ObHashGroupByOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  bool force_by_pass = false;
  LOG_DEBUG("before inner_get_next_batch",
            K(get_aggr_used_size()), K(get_aggr_used_size()),
            K(get_hash_table_used_size()),
            K(get_dumped_part_used_size()), K(get_dump_part_hold_size()),
            K(get_mem_used_size()), K(get_mem_bound_size()),
            K(agged_dumped_cnt_), K(agged_group_cnt_), K(agged_row_cnt_),
            K(max_row_cnt), K(MY_SPEC.max_batch_size_));
  int64_t op_max_batch_size = min(max_row_cnt, MY_SPEC.max_batch_size_);
  if (iter_end_) {
    brs_.size_ = 0;
    brs_.end_ = true;
  } else if (curr_group_id_ < 0 && !bypass_ctrl_.by_passing()) {
    if (bypass_ctrl_.by_pass_ctrl_enabled_ && force_by_pass_) {
      curr_group_id_ = 0;
      bypass_ctrl_.start_by_pass();
      LOG_TRACE("force by pass open");
    } else if (OB_FAIL(load_data_batch(MY_SPEC.max_batch_size_))) {
      LOG_WARN("load data failed", K(ret));
    } else {
      curr_group_id_ = 0;
    }
  }

  if (OB_SUCC(ret) && !brs_.end_ && !bypass_ctrl_.by_passing() && !bypass_ctrl_.processing_ht()) {
    if (OB_UNLIKELY(curr_group_id_ >= local_group_rows_.size())) {
      if (dumped_group_parts_.is_empty() && !is_init_distinct_data_) {
        op_monitor_info_.otherstat_2_value_ = agged_group_cnt_;
        op_monitor_info_.otherstat_2_id_ = ObSqlMonitorStatIds::HASH_ROW_COUNT;
        op_monitor_info_.otherstat_3_value_ =
            max(local_group_rows_.get_bucket_num(), op_monitor_info_.otherstat_3_value_);
        op_monitor_info_.otherstat_3_id_ = ObSqlMonitorStatIds::HASH_BUCKET_COUNT;
        iter_end_ = true;
        brs_.end_ = true;
        brs_.size_ = 0;
        reset();
      } else {
        if (OB_FAIL(load_data_batch(MY_SPEC.max_batch_size_))) {
          LOG_WARN("load data failed", K(ret));
        } else {
          op_monitor_info_.otherstat_3_value_ =
            max(local_group_rows_.get_bucket_num(), op_monitor_info_.otherstat_3_value_);
          op_monitor_info_.otherstat_3_id_ = ObSqlMonitorStatIds::HASH_BUCKET_COUNT;
          curr_group_id_ = 0;
        }
      }
    }
  }

  if (OB_SUCC(ret) && !brs_.end_) {
    clear_evaluated_flag();
    if (curr_group_id_ >= local_group_rows_.size()) {
      if (bypass_ctrl_.processing_ht()
          && bypass_ctrl_.rebuild_times_exceeded()) {
        bypass_ctrl_.start_by_pass();
        bypass_ctrl_.reset_rebuild_times();
        bypass_ctrl_.reset_state();
        by_pass_brs_holder_.restore();
        calc_avg_group_mem();
      }
      if (!bypass_ctrl_.by_passing()) {
        if (OB_FAIL(by_pass_restart_round())) {
          LOG_WARN("failed to restart", K(ret));
        } else if (OB_FAIL(init_by_pass_group_batch_item())) {
          LOG_WARN("failed to init by pass row", K(ret));
        } else if (OB_FAIL(load_data_batch(MY_SPEC.max_batch_size_))) {
          LOG_WARN("failed to laod data", K(ret));
        } else if (curr_group_id_ >= local_group_rows_.size()) {
          iter_end_ = true;
          brs_.end_ = true;
          brs_.size_ = 0;
          reset();
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (brs_.end_) {
    } else if (bypass_ctrl_.by_passing()) {
      if (OB_FAIL(by_pass_prepare_one_batch(op_max_batch_size))) {
        LOG_WARN("failed to prepare batch", K(ret));
      }
    } else if (FALSE_IT(clear_evaluated_flag())) {
    } else if (OB_FAIL(aggr_processor_.collect_result_batch(all_groupby_exprs_,
                                                            op_max_batch_size,
                                                            brs_,
                                                            curr_group_id_))) {
      LOG_WARN("failed to collect batch result", K(ret), K(curr_group_id_));
    }
  }

  LOG_DEBUG("after inner_get_next_batch",
            K(get_aggr_used_size()), K(get_aggr_used_size()),
            K(get_hash_table_used_size()),
            K(get_dumped_part_used_size()), K(get_dump_part_hold_size()),
            K(get_mem_used_size()), K(get_mem_bound_size()),
            K(agged_dumped_cnt_), K(agged_group_cnt_), K(agged_row_cnt_),
            K(curr_group_id_), K(brs_));
  return ret;
}

int ObHashGroupByOp::load_data_batch(int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  ObChunkDatumStore::Iterator row_store_iter;
  DatumStoreLinkPartition *cur_part = NULL;
  int64_t part_id = 0;
  int64_t part_shift = part_shift_; // low half bits for hash table lookup.
  int64_t input_rows = get_input_rows();
  int64_t input_size = get_input_size();
  static_assert(MAX_PARTITION_CNT <= (1 << (CHAR_BIT)), "max partition cnt is too big");

  if (!dumped_group_parts_.is_empty() || (is_init_distinct_data_ && !use_distinct_data_)) {
    // not force dump for dumped data, avoid too many recursion
    force_dump_ = false;
    if (OB_FAIL(switch_part(cur_part, row_store_iter, part_id,
                            part_shift, input_rows, input_size))) {
      LOG_WARN("fail to switch part", K(ret));
    }
  }

  // We use sort based group by for aggregation which need distinct or sort right now,
  // disable operator dump for compatibility.
  DatumStoreLinkPartition *parts[MAX_PARTITION_CNT] = {};
  int64_t part_cnt = 0;
  int64_t est_part_cnt = 0;
  bool check_dump = false;
  ObGbyBloomFilter *bloom_filter = NULL;
  const ObChunkDatumStore::StoredRow **store_rows = NULL;
  int64_t loop_cnt = 0;
  int64_t last_batch_size = 0;

  while (OB_SUCC(ret)) {
    bypass_ctrl_.gby_process_state(local_group_rows_.get_probe_cnt(),
                                   local_group_rows_.size(),
                                   get_actual_mem_used_size());
    if (bypass_ctrl_.processing_ht()) {
      by_pass_brs_holder_.save(max_row_cnt);
      break;
    }
    const ObBatchRows *child_brs = NULL;
    start_calc_hash_idx_ = 0;
    has_calc_base_hash_ = false;
    if (OB_FAIL(next_batch(NULL != cur_part, row_store_iter, max_row_cnt, child_brs))) {
      LOG_WARN("fail to get next batch", K(ret));
    } else if (child_brs->size_ > 0) {
      if (NULL != cur_part) {
        store_rows = batch_rows_from_dump_;
      }
      clear_evaluated_flag();
      loop_cnt += child_brs->size_;
      if (OB_FAIL(try_check_status())) {
        LOG_WARN("failed to check status", K(ret));
      }
      const bool start_dump = (bloom_filter != NULL);
      check_dump = false;
      if (OB_SUCC(ret) && !start_dump) {
        if (OB_FAIL(update_mem_status_periodically(loop_cnt,
                                                   input_rows,
                                                   est_part_cnt,
                                                   check_dump))) {
          LOG_WARN("failed to update usable memory size periodically", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(group_child_batch_rows(store_rows, input_rows, check_dump, part_id, part_shift,
                                             loop_cnt, *child_brs, part_cnt, parts, est_part_cnt,
                                             bloom_filter))) {
          LOG_WARN("fail to group child batch rows", K(ret));
        } else if (no_non_distinct_aggr_) {
        } else if (OB_FAIL(aggr_processor_.eval_aggr_param_batch(*child_brs))) {
          LOG_WARN("fail to eval aggr param batch", K(ret), K(*child_brs));
        }
        // prefetch the result datum that they will be processed
        for (int64_t j = 0; OB_SUCC(ret) && j < gri_cnt_per_batch_; ++j) {
          aggr_processor_.prefetch_group_rows(*gris_per_batch_[j]->group_row_);
        } // end for
        //batch calc aggr for each group
        uint16_t offset_in_selector = 0;
        for (int64_t i = 0; OB_SUCC(ret) && i < gri_cnt_per_batch_; i++) {
          LOG_DEBUG("process batch begin", K(i), K(*gris_per_batch_[i]));
          OB_ASSERT(OB_NOT_NULL(gris_per_batch_[i]->group_row_));
          if (OB_FAIL(aggr_processor_.process_batch(
              child_brs,
              *gris_per_batch_[i]->group_row_,
              selector_array_ + offset_in_selector, gris_per_batch_[i]->group_row_count_in_batch_))) {
            LOG_WARN("fail to process row", K(ret), KPC(gris_per_batch_[i]));
          } else {
            offset_in_selector += gris_per_batch_[i]->group_row_count_in_batch_;
            // reset
            const_cast<ObGroupRowItem *>(gris_per_batch_[i])->group_row_count_in_batch_ = 0;
          }
        }
      }
      gri_cnt_per_batch_ = 0;
      if (child_brs->end_) {
        break;
      }
    } else {
      break;
    }
  } // while end

  row_store_iter.reset();
  if (OB_SUCC(ret) && llc_est_.enabled_) {
    if (OB_FAIL(local_group_rows_.add_hashval_to_llc_map(llc_est_))) {
      LOG_WARN("failed add llc map from ht", K(ret));
    } else {
      llc_est_.est_cnt_ += agged_row_cnt_;
      LOG_TRACE("llc map succ to add hash val from insert state", K(ret), K(llc_est_.est_cnt_), K(agged_row_cnt_), K(agged_group_cnt_));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (NULL == cur_part && !use_distinct_data_ && OB_FAIL(finish_insert_distinct_data())) {
    LOG_WARN("failed to finish insert distinct data", K(ret));
  }

  // cleanup_dump_env() must be called whether success or not
  int tmp_ret = cleanup_dump_env(common::OB_SUCCESS == ret, part_id, parts, part_cnt, bloom_filter);
  if (OB_SUCCESS != tmp_ret) {
    LOG_WARN("cleanup dump environment failed", K(tmp_ret), K(ret));
    ret = OB_SUCCESS == ret ? tmp_ret : ret;
  }

  if (NULL != mem_context_ && NULL != cur_part) {
    cur_part->~DatumStoreLinkPartition();
    mem_context_->get_malloc_allocator().free(cur_part);
    cur_part = NULL;
  }
  IGNORE_RETURN sql_mem_processor_.update_used_mem_size(get_mem_used_size());

  return ret;
}

int ObHashGroupByOp::switch_part(DatumStoreLinkPartition *&cur_part,
                                 ObChunkDatumStore::Iterator &row_store_iter,
                                 int64_t &part_id,
                                 int64_t &part_shift,
                                 int64_t &input_rows,
                                 int64_t &input_size)
{
  int ret = OB_SUCCESS;
  cur_group_item_idx_ = 0;
  cur_group_item_buf_ = nullptr;
  aggr_processor_.reuse();
  sql_mem_processor_.reset();
  if (!dumped_group_parts_.is_empty()) {
    cur_part = dumped_group_parts_.remove_first();
    if (OB_ISNULL(cur_part)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pop head partition failed", K(ret));
    } else if (OB_UNLIKELY(cur_part->part_shift_ >= sizeof(uint64_t) * CHAR_BIT)) { // part_id means level, max_part_id means max_level
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("reach max recursion depth", K(ret), K(part_id), K(cur_part->part_shift_));
    } else if (OB_FAIL(row_store_iter.init(&cur_part->datum_store_))) {
      LOG_WARN("init chunk row store iterator failed", K(ret));
    } else {
      input_rows = cur_part->datum_store_.get_row_cnt();
      part_id = cur_part->part_id_;
      part_shift = part_shift_ = cur_part->part_shift_;
      input_size = cur_part->datum_store_.get_file_size();
    }
  } else {
    if (is_init_distinct_data_ && !use_distinct_data_) {
      use_distinct_data_ = true;
      is_init_distinct_data_ = false;
      input_rows = distinct_data_set_.estimate_total_count();
      part_shift_ = part_shift = sizeof(uint64_t) * CHAR_BIT / 2;
      input_size = input_rows * MY_SPEC.width_;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: distinct data has got", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(local_group_rows_.resize(
                &mem_context_->get_malloc_allocator(), max(2, input_rows)))) {
      LOG_WARN("failed to reuse extended hash table", K(ret));
    } else if (OB_FAIL(sql_mem_processor_.init(&mem_context_->get_malloc_allocator(),
                                               ctx_.get_my_session()->get_effective_tenant_id(),
                                               input_size,
                                               MY_SPEC.type_,
                                               MY_SPEC.id_,
                                               &ctx_))) {
      LOG_WARN("failed to init sql mem processor", K(ret));
    } else if (OB_FAIL(init_group_store())) {
      LOG_WARN("failed to init group store", K(ret));
    } else {
      LOG_TRACE("scan new partition", K(part_id), K(input_rows), K(input_size),
                                      K(local_group_rows_.size()), K(get_mem_used_size()),
                                      K(get_aggr_used_size()), K(get_mem_bound_size()),
                                      K(get_hash_table_used_size()), K(get_dumped_part_used_size()),
                                      K(get_extra_size()));
    }
  }
  return ret;
}

int ObHashGroupByOp::next_batch(bool is_from_row_store,
                                ObChunkDatumStore::Iterator &row_store_iter,
                                int64_t max_row_cnt,
                                const ObBatchRows *&child_brs)
{
  int ret = OB_SUCCESS;
  if (!is_from_row_store) {
    if (use_distinct_data_) {
      int64_t read_size = 0;
      child_brs = &dumped_batch_rows_;
      if (OB_FAIL(get_next_batch_distinct_rows(max_row_cnt, child_brs))) {
        LOG_WARN("failed to get next batch distinct rows", K(ret));
      }
    } else if (OB_FAIL(child_->get_next_batch(max_row_cnt, child_brs))) {
      LOG_WARN("fail to get next batch", K(ret));
    }
    LOG_TRACE("get_row from child", K(*child_brs), K(ret));
  } else {
    int64_t read_size = 0;
    child_brs = &dumped_batch_rows_;
    if (OB_FAIL(row_store_iter.get_next_batch(child_->get_spec().output_, eval_ctx_,
                                  max_row_cnt, read_size, batch_rows_from_dump_))) {
      if (OB_ITER_END == ret) {
        const_cast<ObBatchRows *>(child_brs)->size_ = 0;
        const_cast<ObBatchRows *>(child_brs)->end_ = true;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get next batch", K(ret));
      }
    } else {
      LOG_TRACE("get_row from row_store", K(read_size));
      const_cast<ObBatchRows *>(child_brs)->size_ = read_size;
      const_cast<ObBatchRows *>(child_brs)->end_ = false;
      if (first_batch_from_store_) {
        const_cast<ObBatchRows *>(child_brs)->skip_->reset(MY_SPEC.max_batch_size_);
        first_batch_from_store_ = false;
      }
    }
  }
  return ret;
}

void ObHashGroupByOp::calc_groupby_exprs_hash_batch(
  ObIArray<ObExpr *> &groupby_exprs, const ObBatchRows &child_brs)
{
  uint64_t seed = 99194853094755497L;
  int64_t ret = OB_SUCCESS;
  if (ObThreeStageAggrStage::FIRST_STAGE == MY_SPEC.aggr_stage_ && !has_calc_base_hash_) {
    // first calc hash values of groupby_expr
    for (int64_t i = 0; i < MY_SPEC.aggr_code_idx_ + 1; ++i) {
      ObExpr *expr = groupby_exprs.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status: groupby exprs is null", K(ret));
      } else {
        ObBatchDatumHashFunc hash_func = expr->basic_funcs_->murmur_hash_v2_batch_;
        const bool is_batch_seed = (i > 0);
        hash_func(base_hash_vals_,
                  expr->locate_batch_datums(eval_ctx_), expr->is_batch_result(),
                  *child_brs.skip_, child_brs.size_,
                  is_batch_seed ? base_hash_vals_ : &seed,
                  is_batch_seed);
      }
    }
    has_calc_base_hash_ = true;
    start_calc_hash_idx_ = MY_SPEC.aggr_code_idx_ + 1;
  }
  if (0 < start_calc_hash_idx_) {
    for (int64_t i = 0; i < child_brs.size_; ++i) {
      if (!child_brs.skip_->exist(i)) {
        hash_vals_[i] = base_hash_vals_[i];
      }
    }
  }
  for (int64_t i = start_calc_hash_idx_; i < groupby_exprs.count(); ++i) {
    ObExpr *expr = groupby_exprs.at(i);
    if (OB_ISNULL(expr)) {
    } else {
      ObBatchDatumHashFunc hash_func = expr->basic_funcs_->murmur_hash_v2_batch_;
      const bool is_batch_seed = (i > 0);
      hash_func(hash_vals_,
                expr->locate_batch_datums(eval_ctx_), expr->is_batch_result(),
                *child_brs.skip_, child_brs.size_,
                is_batch_seed ? hash_vals_ : &seed,
                is_batch_seed);
    }
  }
}

int ObHashGroupByOp::eval_groupby_exprs_batch(const ObChunkDatumStore::StoredRow **store_rows,
                                                   const ObBatchRows &child_brs)
{
  int ret = OB_SUCCESS;
  if (NULL == store_rows) {
    for (int64_t i = 0; OB_SUCC(ret) && i < all_groupby_exprs_.count(); ++i) {
      ObExpr *expr = all_groupby_exprs_.at(i); // expr ptr check in cg, not check here
      if (OB_INVALID_INDEX_INT64 != MY_SPEC.aggr_code_idx_ && i == MY_SPEC.aggr_code_idx_) {
        // aggr_code column, then skip, set values in process duplicate data
        continue;
      } else if (OB_FAIL(expr->eval_batch(eval_ctx_, *child_brs.skip_, child_brs.size_))) {
        LOG_WARN("eval failed", K(ret));
      } else if (ObThreeStageAggrStage::FIRST_STAGE == MY_SPEC.aggr_stage_ && i > MY_SPEC.aggr_code_idx_) {
        // distinct old value
        int64_t dup_idx = i - MY_SPEC.aggr_code_idx_ - 1;
        if (dup_idx >= MY_SPEC.org_dup_cols_.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected status: duplicate distinct column is invalid", K(dup_idx), K(i),
                   K(MY_SPEC.aggr_code_idx_), K(ret));
        } else if (OB_FAIL(MY_SPEC.org_dup_cols_.at(dup_idx)->eval_batch(
                    eval_ctx_, *child_brs.skip_, child_brs.size_))) {
          LOG_WARN("eval failed", K(ret));
        }
      }
    }
    // don't calculate hash if group_rows_arr_ is valid, calculte later if new GroupRowItem found
  } else {
    // In dumped partition
    // for for performance, batch eval group_expr here, then we get datum derectly when compare
    for (int64_t i = 0; OB_SUCC(ret) && i < all_groupby_exprs_.count(); ++i) {
      ObExpr *expr = all_groupby_exprs_.at(i); // expr ptr check in cg, not check here
      if (OB_INVALID_INDEX_INT64 != MY_SPEC.aggr_code_idx_ && i == MY_SPEC.aggr_code_idx_) {
        // aggr_code column, then skip, set values in process duplicate data
        continue;
      } else if (OB_FAIL(expr->eval_batch(eval_ctx_, *child_brs.skip_, child_brs.size_))) {
        LOG_WARN("eval failed", K(ret));
      } else if (ObThreeStageAggrStage::FIRST_STAGE == MY_SPEC.aggr_stage_ && i > MY_SPEC.aggr_code_idx_) {
        // distinct old value
        int64_t dup_idx = i - MY_SPEC.aggr_code_idx_ - 1;
        if (dup_idx >= MY_SPEC.org_dup_cols_.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected status: duplicate distinct column is invalid", K(dup_idx), K(i),
                   K(MY_SPEC.aggr_code_idx_), K(ret));
        } else if (OB_FAIL(MY_SPEC.org_dup_cols_.at(dup_idx)->eval_batch(
                    eval_ctx_, *child_brs.skip_, child_brs.size_))) {
          LOG_WARN("eval failed", K(ret));
        }
      }
    }
    for (int64_t i = 0; i < child_brs.size_; i++) {
      OB_ASSERT(OB_NOT_NULL(store_rows[i]));
      hash_vals_[i] = *static_cast<uint64_t *>(store_rows[i]->get_extra_payload());
    }
  }
  return ret;
}

int ObHashGroupByOp::batch_process_duplicate_data(
  const ObChunkDatumStore::StoredRow **store_rows,
  const int64_t input_rows,
  const bool check_dump,
  const int64_t part_id,
  const int64_t part_shift,
  const int64_t loop_cnt,
  const ObBatchRows &child_brs,
  int64_t &part_cnt,
  DatumStoreLinkPartition **parts,
  int64_t &est_part_cnt,
  ObGbyBloomFilter *&bloom_filter,
  bool &process_check_dump)
{
  int ret = OB_SUCCESS;
  int64_t nth_dup_data = 0;
  bool last_group = false;
  bool insert_group_ht = false;
  const ObGroupRowItem *exist_curr_gr_item = NULL;
  bool tmp_check_dump = false;
  bool force_check_dump = check_dump;
  ObGroupRowItem curr_gr_item;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
  batch_info_guard.set_batch_size(child_brs.size_);
  MEMSET(is_dumped_, 0, sizeof(bool) * child_brs.size_);
  bool can_insert_ht = NULL == bloom_filter
                        && (!enable_dump_
                        || local_group_rows_.size() < MIN_INMEM_GROUPS
                        || process_check_dump
                        || !need_start_dump(input_rows, est_part_cnt, force_check_dump));
  do {
    // firstly process duplicate data
    if (OB_FAIL(next_duplicate_data_permutation(nth_dup_data, last_group, &child_brs, insert_group_ht))) {
      LOG_WARN("failed to get next duplicate data purmutation", K(ret));
    } else if (last_group) {
      // last group is origin data, process later
    } else if (group_rows_arr_.is_valid_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: is_valid must be false in three stage", K(ret));
    } else {
      // TODO: because groupby expr add distinct exprs, so the group count may exceed the input rows
      //       and evenly several times, so we should implement don't calculate aggregate functions
      //       and output the duplicate data
      int64_t tmp_group_cnt = 0;
      if (nullptr == store_rows) {
        calc_groupby_exprs_hash_batch(dup_groupby_exprs_, child_brs);
        local_group_rows_.prefetch(child_brs, hash_vals_);
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < child_brs.size_; i++) {
        if (child_brs.skip_->exist(i) || is_dumped_[i]) {
          continue;
        }
        curr_gr_item.is_expr_row_ = true;
        curr_gr_item.batch_idx_ = i;
        curr_gr_item.hash_ = hash_vals_[i];
        exist_curr_gr_item = (NULL == bloom_filter || bloom_filter->exist(hash_vals_[i]))
                            ? local_group_rows_.get(curr_gr_item) : NULL;
        if (bloom_filter == NULL && OB_FAIL(update_mem_status_periodically(agged_group_cnt_,
                                                    input_rows,
                                                    est_part_cnt,
                                                    tmp_check_dump))) {
          LOG_WARN("failed to update usable memory size periodically", K(ret));
        } else if (NULL != exist_curr_gr_item) {
          agged_row_cnt_++;
          LOG_DEBUG("exist item", K(gri_cnt_per_batch_), K(*exist_curr_gr_item),
            K(i), K(agged_row_cnt_));
        } else if (can_insert_ht) {
          ++agged_row_cnt_;
          ++agged_group_cnt_;
          ObGroupRowItem *tmp_gr_item = NULL;
          if (OB_FAIL(alloc_group_item(tmp_gr_item))) {
            LOG_WARN("failed to alloc group item", K(ret));
          } else {
            tmp_gr_item->hash_ = hash_vals_[i];
            tmp_gr_item->is_expr_row_ = true;
            tmp_gr_item->batch_idx_ = i;
            if (OB_FAIL(set_group_row_item(*tmp_gr_item, i))) {
              LOG_WARN("hash table set failed", K(ret));
            } else {
              selector_array_[tmp_group_cnt] = i;
              batch_row_gri_ptrs_[tmp_group_cnt] = tmp_gr_item;
              ++tmp_group_cnt;
            }
          }
        } else {
          // need dump
          is_dumped_[i] = true;
          curr_gr_item.hash_ = hash_vals_[i];
          if (OB_UNLIKELY(NULL == bloom_filter)) {
            if (OB_FAIL(setup_dump_env(part_id, max(input_rows, loop_cnt), parts, part_cnt,
                                      bloom_filter))) {
              LOG_WARN("setup dump environment failed", K(ret));
            } else {
              sql_mem_processor_.set_number_pass(part_id + 1);
            }
          }
          if (OB_SUCC(ret)) {
            ++agged_dumped_cnt_;
            const int64_t part_idx = (curr_gr_item.hash() >> part_shift) & (part_cnt - 1);
            ObChunkDatumStore::StoredRow *stored_row = NULL;
            batch_info_guard.set_batch_idx(i);
            if (OB_FAIL(parts[part_idx]->datum_store_.add_row(child_->get_spec().output_,
                                                              &eval_ctx_,
                                                              &stored_row))) {
              LOG_WARN("add row failed", K(ret));
            } else if (1 != nth_dup_data) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected status: dump is not expected", K(ret));
            } else {
              *static_cast<uint64_t *>(stored_row->get_extra_payload()) = curr_gr_item.hash();
            }
            LOG_DEBUG("finish dump", K(part_idx), K(curr_gr_item), KPC(stored_row),
                                    K(agged_dumped_cnt_), K(agged_row_cnt_),
                                    K(parts[part_idx]->datum_store_.get_row_cnt()), K(i));
          }
        }
      } // for end
      if (OB_SUCC(ret) && 0 < tmp_group_cnt) {
        if (OB_FAIL(group_store_.add_batch(dup_groupby_exprs_, eval_ctx_, *child_brs.skip_,
            child_brs.size_, selector_array_, tmp_group_cnt,
            const_cast<ObChunkDatumStore::StoredRow**>(batch_rows_from_dump_)))) {
          LOG_WARN("failed to add row", K(ret));
        } else {
          ObAggregateProcessor::GroupRow *group_row = NULL;
          ObGroupRowItem *curr_gr_item = NULL;
          for (int64_t i = 0; i < tmp_group_cnt && OB_SUCC(ret); i++) {
            curr_gr_item = const_cast<ObGroupRowItem*>(batch_row_gri_ptrs_[i]);
            if (OB_FAIL(alloc_group_row(local_group_rows_.size() - tmp_group_cnt + i,
                                        *curr_gr_item))) {
              LOG_WARN("alloc group row failed", K(ret));
            } else {
              curr_gr_item->group_row_->groupby_store_row_
                  = const_cast<ObChunkDatumStore::StoredRow*>(batch_rows_from_dump_[i]);
              curr_gr_item->groupby_store_row_
                  = const_cast<ObChunkDatumStore::StoredRow*>(batch_rows_from_dump_[i]);
            }
          }
        }
      }
      process_check_dump = true;
    }
  } while (!last_group && OB_SUCC(ret));
  return ret;
}

int ObHashGroupByOp::batch_insert_distinct_data(const ObBatchRows &child_brs)
{
  int ret = OB_SUCCESS;
  ObBitVector *output_vec = nullptr;
  if (!is_init_distinct_data_ && OB_FAIL(init_distinct_info(false))) {
    LOG_WARN("failed to init hash partition infras", K(ret));
  } else if (OB_FAIL(distinct_data_set_.calc_hash_value_for_batch(distinct_origin_exprs_,
                                                          child_brs.size_,
                                                          distinct_skip_,
                                                          distinct_hash_values_,
                                                          dup_groupby_exprs_.count(),
                                                          hash_vals_))) {
    LOG_WARN("failed to calc hash values batch for child", K(ret));
  } else if (OB_FAIL(distinct_data_set_.insert_row_for_batch(distinct_origin_exprs_,
                                              distinct_hash_values_,
                                              child_brs.size_,
                                              distinct_skip_,
                                              output_vec))) {
    LOG_WARN("failed to insert batch rows, no dump", K(ret));
  }
  return ret;
}

int ObHashGroupByOp::batch_insert_all_distinct_data(const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  bool got_batch = false;
  int64_t read_rows = -1;
  ObBitVector *output_vec = nullptr;
  while(OB_SUCC(ret) && !got_batch) {
    const ObBatchRows *child_brs = nullptr;
    clear_evaluated_flag();
    if (OB_FAIL(distinct_data_set_.get_left_next_batch(distinct_origin_exprs_,
                                              batch_size,
                                              read_rows,
                                              distinct_hash_values_))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next batch from hp infra", K(ret));
      } else {
        ret = OB_SUCCESS;
        if (OB_FAIL(distinct_data_set_.finish_insert_row())) {
          LOG_WARN("failed to finish insert row", K(ret));
        } else if (OB_FAIL(distinct_data_set_.close_cur_part(InputSide::LEFT))) {
          LOG_WARN("failed to close curr part", K(ret));
        } else {
          //if true, means we have process a full partition, then break the loop and return rows
          break;
        }
      }
    } else if (OB_FAIL(try_check_status())) {
      LOG_WARN("failed to check status", K(ret));
    } else if (OB_FAIL(distinct_data_set_.insert_row_for_batch(distinct_origin_exprs_,
                                                      distinct_hash_values_,
                                                      read_rows,
                                                      nullptr,
                                                      output_vec))) {
      LOG_WARN("failed to insert batch rows, dump", K(ret));
    }
  }
  return ret;
}

int ObHashGroupByOp::get_next_batch_distinct_rows(
  const int64_t batch_size, const ObBatchRows *&child_brs)
{
  int ret = OB_SUCCESS;
  int64_t read_rows = 0;
  if (OB_FAIL(distinct_data_set_.get_next_hash_table_batch(distinct_origin_exprs_,
                                                  batch_size,
                                                  read_rows,
                                                  nullptr))) {
    //Ob_ITER_END means data from child_ or current
    //partition in infra is run out， read_size <= batch_size
    if (OB_ITER_END == ret) {
      if (OB_FAIL(distinct_data_set_.end_round())) {
        LOG_WARN("failed to end round", K(ret));
      } else if (OB_FAIL(distinct_data_set_.start_round())) {
        LOG_WARN("failed to start round", K(ret));
      } else if (OB_FAIL(distinct_data_set_.get_next_partition(InputSide::LEFT))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get dumped partitions", K(ret));
        }
      } else if (OB_FAIL(distinct_data_set_.open_cur_part(InputSide::LEFT))) {
        LOG_WARN("failed to open cur part", K(ret));
      } else if (OB_FAIL(init_distinct_info(true))) {
        LOG_WARN("failed to init hash partition infrastruct", K(ret));
      } else if (OB_FAIL(distinct_data_set_.resize(distinct_data_set_.get_cur_part_row_cnt(InputSide::LEFT)))) {
        LOG_WARN("failed to init hashtable", K(ret));
      } else if (OB_FAIL(batch_insert_all_distinct_data(batch_size))) {
        LOG_WARN("failed to build distinct data for batch", K(ret));
      } else if (OB_FAIL(distinct_data_set_.open_hash_table_part())) {
        LOG_WARN("failed to open hash table part", K(ret));
      } else if (OB_FAIL(distinct_data_set_.get_next_hash_table_batch(distinct_origin_exprs_,
                                                              batch_size,
                                                              read_rows,
                                                              nullptr))) {
        LOG_WARN("failed to get next row in hash table", K(ret));
      }
    } else {
      LOG_WARN("failed to get next batch in hash table", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    const_cast<ObBatchRows *>(child_brs)->size_ = read_rows;
    const_cast<ObBatchRows *>(child_brs)->end_ = false;
    if (first_batch_from_store_) {
      const_cast<ObBatchRows *>(child_brs)->skip_->reset(MY_SPEC.max_batch_size_);
      first_batch_from_store_ = false;
    }
  } else if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
    const_cast<ObBatchRows *>(child_brs)->size_ = 0;
    const_cast<ObBatchRows *>(child_brs)->end_ = true;
  }
  return ret;
}

int ObHashGroupByOp::group_child_batch_rows(const ObChunkDatumStore::StoredRow **store_rows,
                                            const int64_t input_rows,
                                            const bool check_dump,
                                            const int64_t part_id,
                                            const int64_t part_shift,
                                            const int64_t loop_cnt,
                                            const ObBatchRows &child_brs,
                                            int64_t &part_cnt,
                                            DatumStoreLinkPartition **parts,
                                            int64_t &est_part_cnt,
                                            ObGbyBloomFilter *&bloom_filter)
{
  int ret = OB_SUCCESS;
  const ObGroupRowItem *exist_curr_gr_item = NULL;
  ObGroupRowItem curr_gr_item;
  LOG_DEBUG("group child batch", K(child_brs), K(part_id), K(agged_dumped_cnt_),
                                    K(agged_row_cnt_), K(agged_group_cnt_));
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
  batch_info_guard.set_batch_size(child_brs.size_);
  bool batch_hash_calculated = !group_rows_arr_.is_valid_;
  bool process_check_dump = false;
  bool force_check_dump = check_dump;
  int64_t aggr_code = -1;
  ObExpr *aggr_code_expr = MY_SPEC.aggr_code_expr_;
  ObDatum *aggr_code_datum = nullptr;
  int64_t distinct_data_idx = 0;
  // mem prefetch for hashtable
  if (OB_FAIL(eval_groupby_exprs_batch(store_rows, child_brs))) {
    LOG_WARN("fail to calc groupby exprs hash batch", K(ret));
  } else if (OB_FAIL(batch_process_duplicate_data(store_rows, input_rows, force_check_dump, part_id,
      part_shift, loop_cnt, child_brs, part_cnt, parts, est_part_cnt, bloom_filter,
      process_check_dump))) {
    LOG_WARN("failed to batch process duplicate data", K(ret));
  } else if (no_non_distinct_aggr_) {
    // no groupby exprs, don't calculate the last duplicate data for non-distinct aggregate
  } else {
    if (!group_rows_arr_.is_valid_ && nullptr == store_rows) {
      calc_groupby_exprs_hash_batch(dup_groupby_exprs_, child_brs);
      local_group_rows_.prefetch(child_brs, hash_vals_);
      batch_hash_calculated = true;
    }
    uint16_t new_groups = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < child_brs.size_; i++) {
      batch_info_guard.set_batch_idx(i);
      if (child_brs.skip_->exist(i) || is_dumped_[i]) {
        batch_row_gri_ptrs_[i] = nullptr;
        if (nullptr != distinct_skip_) {
          distinct_skip_->set(i);
        }
        continue;
      } else {
        if (ObThreeStageAggrStage::SECOND_STAGE != MY_SPEC.aggr_stage_ || use_distinct_data_) {
        } else if (OB_FAIL(aggr_code_expr->eval(eval_ctx_, aggr_code_datum))) {
          LOG_WARN("failed to eval aggr_code_expr", K(ret));
        } else {
          aggr_code = aggr_code_datum->get_int();
          if (aggr_code < MY_SPEC.dist_aggr_group_idxes_.count()) {
            if (!batch_hash_calculated) {
              calc_groupby_exprs_hash_batch(dup_groupby_exprs_, child_brs);
              batch_hash_calculated = true;
            }
            ++distinct_data_idx;
            // don't process
            child_brs.skip_->set(i);
            continue;
          } else {
            distinct_skip_->set(i);
          }
        }
        if (group_rows_arr_.is_valid_) {
          exist_curr_gr_item = group_rows_arr_.get(i);
        } else {
          curr_gr_item.is_expr_row_ = true;
          curr_gr_item.batch_idx_ = i;
          curr_gr_item.hash_ = hash_vals_[i];
          exist_curr_gr_item = (NULL == bloom_filter || bloom_filter->exist(hash_vals_[i]))
                                ? local_group_rows_.get(curr_gr_item) : NULL;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (NULL != exist_curr_gr_item) {
        // in local group
        if (0 == exist_curr_gr_item->group_row_count_in_batch_) {
          gris_per_batch_[gri_cnt_per_batch_++] = exist_curr_gr_item;
        }
        ++agged_row_cnt_;
        batch_row_gri_ptrs_[i] = exist_curr_gr_item;
        const_cast<ObGroupRowItem *>(exist_curr_gr_item)->group_row_count_in_batch_++;
        LOG_DEBUG("exist item", K(gri_cnt_per_batch_), K(*exist_curr_gr_item),
                                K(i), K(agged_row_cnt_));
      } else if (NULL == bloom_filter
                  && (!enable_dump_
                    || local_group_rows_.size() < MIN_INMEM_GROUPS
                    || process_check_dump
                    || !need_start_dump(input_rows, est_part_cnt, force_check_dump))) {
        // add new local group
        if (!batch_hash_calculated) {
          calc_groupby_exprs_hash_batch(dup_groupby_exprs_, child_brs);
          batch_hash_calculated = true;
        }
        ++agged_row_cnt_;
        ++agged_group_cnt_;
        ObGroupRowItem *tmp_gr_item = NULL;
        if (OB_FAIL(alloc_group_item(tmp_gr_item))) {
          LOG_WARN("failed to alloc group item", K(ret));
        } else {
          tmp_gr_item->is_expr_row_ = true;
          tmp_gr_item->batch_idx_ = i;
          tmp_gr_item->hash_ = hash_vals_[i];
          tmp_gr_item->group_row_count_in_batch_++;
          batch_row_gri_ptrs_[i] = tmp_gr_item;
          if (OB_FAIL(set_group_row_item(*tmp_gr_item, i))) {
            LOG_WARN("hash table set failed", K(ret));
          } else {
            selector_array_[new_groups++] = i;
            gris_per_batch_[gri_cnt_per_batch_++] = tmp_gr_item;
            LOG_DEBUG("new group row", K(gri_cnt_per_batch_), K(*tmp_gr_item), K(i), K(agged_row_cnt_));
          }
        }
      } else {
        if (!batch_hash_calculated) {
          calc_groupby_exprs_hash_batch(dup_groupby_exprs_, child_brs);
          batch_hash_calculated = true;
        }
        // need dump
        // set group_rows_arr_ invalid if need dump.
        group_rows_arr_.is_valid_ = false;
        batch_row_gri_ptrs_[i] = NULL;
        curr_gr_item.hash_ = hash_vals_[i];
        if (OB_UNLIKELY(NULL == bloom_filter)) {
          if (OB_FAIL(setup_dump_env(part_id, max(input_rows, loop_cnt), parts, part_cnt,
                                    bloom_filter))) {
            LOG_WARN("setup dump environment failed", K(ret));
          } else {
            sql_mem_processor_.set_number_pass(part_id + 1);
          }
        }
        if (OB_SUCC(ret)) {
          ++agged_dumped_cnt_;
          const int64_t part_idx = (curr_gr_item.hash() >> part_shift) & (part_cnt - 1);
          ObChunkDatumStore::StoredRow *stored_row = NULL;
          batch_info_guard.set_batch_idx(i);
          if (OB_FAIL(parts[part_idx]->datum_store_.add_row(child_->get_spec().output_,
                                                            &eval_ctx_,
                                                            &stored_row))) {
            LOG_WARN("add row failed", K(ret));
          } else if (process_check_dump) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected status: check dump is processed", K(ret));
          } else {
            *static_cast<uint64_t *>(stored_row->get_extra_payload()) = curr_gr_item.hash();
          }
          LOG_DEBUG("finish dump", K(part_idx), K(curr_gr_item), KPC(stored_row),
                                  K(agged_dumped_cnt_), K(agged_row_cnt_),
                                  K(parts[part_idx]->datum_store_.get_row_cnt()), K(i));
        }
      }
      // only once force check dump
      force_check_dump = false;
    } // for end

    if (OB_SUCC(ret) && new_groups > 0) {
      if (OB_FAIL(group_store_.add_batch(dup_groupby_exprs_, eval_ctx_, *child_brs.skip_,
                                         child_brs.size_, selector_array_, new_groups,
                                         const_cast<ObChunkDatumStore::StoredRow**>(
                                             batch_rows_from_dump_)))) {
        LOG_WARN("failed to add row", K(ret));
      } else {
        ObAggregateProcessor::GroupRow *group_row = NULL;
        ObGroupRowItem *curr_gr_item = NULL;
        for (int64_t i = 0; i < new_groups && OB_SUCC(ret); i++) {
          curr_gr_item = const_cast<ObGroupRowItem*>(batch_row_gri_ptrs_[selector_array_[i]]);
          if (OB_FAIL(alloc_group_row(local_group_rows_.size() - new_groups + i,
                                      *curr_gr_item))) {
            LOG_WARN("alloc group row failed", K(ret));
          } else {
            curr_gr_item->group_row_->groupby_store_row_
                = const_cast<ObChunkDatumStore::StoredRow*>(batch_rows_from_dump_[i]);
            curr_gr_item->groupby_store_row_
                = const_cast<ObChunkDatumStore::StoredRow*>(batch_rows_from_dump_[i]);
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      uint16_t group_rows_sum = 0;
      for (int64_t i = 0; i < gri_cnt_per_batch_; ++i) {
        const_cast<ObGroupRowItem *>(gris_per_batch_[i])->group_row_offset_in_selector_ = group_rows_sum;
        group_rows_sum += gris_per_batch_[i]->group_row_count_in_batch_;
      }
      for (int64_t i = 0; i < child_brs.size_; ++i) {
        if (child_brs.skip_->exist(i)) {
          // do nothing
        } else if (OB_ISNULL(batch_row_gri_ptrs_[i])) {
          // dumpped row, do nothing
        } else {
          // sort all child rows order by groups to keep all same group rows are together
          // eg:
          //        row_no  row_values
          //  rows: 0       (1,1,1)
          //        1       (2,2,2)
          //        2       (3,3,3)
          //        3       (1,1,1)
          //        4       (3,3,3)
          //        5       (1,1,1)
          //  so selector_array_ is [0,3,5,1,2,4]
          //  and (1,1,1) -> [0,3,5]
          //      (2,2,2) -> [1]
          //      (3,3,3) -> [2,4]
          selector_array_[const_cast<ObGroupRowItem *>(batch_row_gri_ptrs_[i])->group_row_offset_in_selector_++] = i;
        }
      }
    }

    // process distinct data
    if (OB_SUCC(ret) && 0 < distinct_data_idx) {
      if (OB_FAIL(batch_insert_distinct_data(child_brs))) {
        LOG_WARN("failed to batch insert distinct data", K(ret));
      }
    }
  }
  return ret;
}

int ObHashGroupByOp::init_by_pass_group_row_item()
{
  int ret = OB_SUCCESS;
  ObChunkDatumStore::StoredRow *store_row = NULL;
  char *group_item_buf = nullptr;
  const int64_t group_id = 0;
  if (OB_ISNULL(by_pass_group_row_)) {
    if (OB_ISNULL(group_item_buf = (char *)mem_context_->get_arena_allocator().alloc(sizeof(ObGroupRowItem)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else if (OB_ISNULL(group_item_buf)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: group_item_buf is null", K(ret));
    } else {
      by_pass_group_row_ = new (group_item_buf) ObGroupRowItem();
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(aggr_processor_.generate_group_row(by_pass_group_row_->group_row_, group_id))) {
    LOG_WARN("generate group row failed", K(ret));
  } else if (OB_ISNULL(by_pass_group_row_->group_row_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("generate wrong group row", K(ret));
  }
  return ret;
}

int ObHashGroupByOp::init_by_pass_group_batch_item()
{
  int ret = OB_SUCCESS;
  const int64_t group_id = 0;
  if (by_pass_batch_size_ <= 0) {
    if (OB_ISNULL(by_pass_group_batch_ =
        static_cast<ObAggregateProcessor::GroupRow **> (mem_context_->get_arena_allocator()
                                        .alloc(sizeof(ObAggregateProcessor::GroupRow *) * MY_SPEC.max_batch_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret));
    } else {
      by_pass_batch_size_ = MY_SPEC.max_batch_size_;
    }
  }
  CK (by_pass_batch_size_ > 0);
  for (int64_t i = 0; OB_SUCC(ret) && i < by_pass_batch_size_; ++i) {
    if (OB_FAIL(aggr_processor_.generate_group_row(by_pass_group_batch_[i], group_id))) {
      LOG_WARN("generate group row failed", K(ret));
    } else if (OB_ISNULL(by_pass_group_batch_[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("generate wrong group row", K(ret), K(i));
    }
  }
  return ret;
}

int ObHashGroupByOp::load_one_row()
{
  int ret = OB_SUCCESS;
  bool last_group = false;
  bool insert_group_ht = false;
  clear_evaluated_flag();
  bool got_row = false;
  while (OB_SUCC(ret) && !got_row) {
    if (OB_ISNULL(last_child_row_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("last child row not init", K(ret));
    } else if (ObThreeStageAggrStage::FIRST_STAGE == MY_SPEC.aggr_stage_
        && by_pass_nth_group_ <= MY_SPEC.dist_col_group_idxs_.count()
        && by_pass_nth_group_ > 0) {
      // next permutation
      if (OB_ISNULL(last_child_row_->store_row_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get last store row", K(ret));
      } else if (OB_FAIL(last_child_row_->store_row_->to_expr(child_->get_spec().output_, eval_ctx_))) {
        LOG_WARN("failed to restore last row", K(ret));
      } else if (OB_FAIL(by_pass_get_next_permutation(by_pass_nth_group_, last_group, insert_group_ht))) {
        LOG_WARN("failed to get next permutation row", K(ret));
      } else {
        if (no_non_distinct_aggr_ && last_group) {
          got_row = false;
        } else {
          got_row = true;
        }
      }
    } else if (FALSE_IT(by_pass_nth_group_ = 0)) {
    } else if (nullptr != last_child_row_->store_row_
              && OB_FAIL(last_child_row_->store_row_->to_expr(child_->get_spec().output_, eval_ctx_))) {
      LOG_WARN("failed to restore last row", K(ret));
    } else if (OB_FAIL(child_->get_next_row())) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("failed to get next row", K(ret));
      }
    } else if (OB_FAIL(last_child_row_->save_store_row(child_->get_spec().output_, eval_ctx_))) {
      LOG_WARN("failed to save last row", K(ret));
    } else if (OB_FAIL(try_check_status())) {
      LOG_WARN("check status failed", K(ret));
    } else if (OB_FAIL(by_pass_get_next_permutation(by_pass_nth_group_, last_group, insert_group_ht))) {
      LOG_WARN("failed to get next permutation row", K(ret));
    } else {
      if (no_non_distinct_aggr_ && last_group) {
        got_row = false;
      } else {
        got_row = true;
      }
    }

    if (OB_SUCC(ret) && llc_est_.enabled_) {
      const ObChunkDatumStore::StoredRow *srow = NULL;
      uint64_t hash_val = 0;
      if (OB_FAIL(calc_groupby_exprs_hash(dup_groupby_exprs_, srow, hash_val))) {
        LOG_WARN("failed to calc hash", K(ret), K(llc_est_.enabled_));
      } else if (OB_FAIL(bypass_add_llc_map(hash_val,
                                            ObThreeStageAggrStage::FIRST_STAGE == MY_SPEC.aggr_stage_ ?
                                            by_pass_nth_group_ == MY_SPEC.dist_col_group_idxs_.count() : true))) {
        LOG_WARN("failed to add llc map", K(ret));
      }
    }

    if (OB_FAIL(ret) || no_non_distinct_aggr_) {
    } else if (OB_ISNULL(by_pass_group_row_)
                || OB_ISNULL(by_pass_group_row_->group_row_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("by pass group row is not init", K(ret));
    } else {
      ++agged_row_cnt_;
      ++agged_group_cnt_;
      if (OB_FAIL(aggr_processor_.single_row_agg(*by_pass_group_row_->group_row_, eval_ctx_))) {
        LOG_WARN("failed to do single row agg", K(ret));
      }
    }
  }
  return ret;
}

int ObHashGroupByOp::by_pass_prepare_one_batch(const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  bool last_group = false;
  bool insert_group_ht = false;
  LOG_TRACE("by pass prepare one batch", K(batch_size));
  if (ObThreeStageAggrStage::FIRST_STAGE == MY_SPEC.aggr_stage_
      && by_pass_nth_group_ <= MY_SPEC.dist_col_group_idxs_.count()
      && by_pass_nth_group_ > 0) {
    //consume curr batch
    if (OB_FAIL(by_pass_brs_holder_.restore())) {
      LOG_WARN("failed to restore child batch", K(ret));
    } else if (OB_FAIL(by_pass_get_next_permutation_batch(by_pass_nth_group_, last_group,
                                                          by_pass_child_brs_, brs_,
                                                          insert_group_ht))) {
      LOG_WARN("failed to get next permutation row", K(ret));
    }
  } else if (FALSE_IT(by_pass_nth_group_ = 0)) {
  } else if (OB_FAIL(by_pass_brs_holder_.restore())) {
    LOG_WARN("failed to restore child batch", K(ret));
  } else if (OB_FAIL(child_->get_next_batch(batch_size, by_pass_child_brs_))) {
    LOG_WARN("fail to get next batch", K(ret));
  } else if (by_pass_child_brs_->end_) {
    brs_.size_ = 0;
    brs_.end_ = true;
    by_pass_brs_holder_.reset();
  } else if (OB_FAIL(by_pass_brs_holder_.save(by_pass_child_brs_->size_))) {
    LOG_WARN("failed to save child batch", K(ret));
  } else if (OB_FAIL(try_check_status())) {
    LOG_WARN("check status failed", K(ret));
  } else if (OB_FAIL(by_pass_get_next_permutation_batch(by_pass_nth_group_, last_group,
                                                        by_pass_child_brs_, brs_,
                                                        insert_group_ht))) {
      LOG_WARN("failed to get next permutation row", K(ret));
  }

  if (OB_SUCC(ret) && llc_est_.enabled_) {
    const ObChunkDatumStore::StoredRow **store_rows = NULL;
    if (OB_FAIL(eval_groupby_exprs_batch(store_rows, brs_))) {
      LOG_WARN("fail to calc groupby exprs hash batch", K(ret));
    } else {
      calc_groupby_exprs_hash_batch(dup_groupby_exprs_, brs_);
      if (OB_FAIL(bypass_add_llc_map_batch(ObThreeStageAggrStage::FIRST_STAGE == MY_SPEC.aggr_stage_ ?
                                           by_pass_nth_group_ > MY_SPEC.dist_col_group_idxs_.count() : true))) {
        LOG_WARN("failed to add llc map batch", K(ret));
      }
    }
  }

  if (OB_FAIL(ret) || no_non_distinct_aggr_) {
  } else if (OB_ISNULL(by_pass_group_batch_)
             || by_pass_batch_size_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("by pass group row is not init", K(ret), K(by_pass_batch_size_));
  } else if (OB_FAIL(aggr_processor_.eval_aggr_param_batch(brs_))) {
    LOG_WARN("fail to eval aggr param batch", K(ret), K(brs_));
  } else {
    if (OB_FAIL(aggr_processor_.single_row_agg_batch(by_pass_group_batch_, eval_ctx_,
                                                     brs_.size_, brs_.skip_))) {
      LOG_WARN("failed to single row agg", K(ret));
    } else {
      int64_t aggr_cnt = brs_.size_ - brs_.skip_->accumulate_bit_cnt(brs_.size_);
      agged_row_cnt_ += aggr_cnt;
      agged_group_cnt_ += aggr_cnt;
    }
  }
  return ret;
}

int ObHashGroupByOp::by_pass_get_next_permutation(int64_t &nth_group, bool &last_group, bool &insert_group_ht)
{
  int ret = OB_SUCCESS;
  if (ObThreeStageAggrStage::NONE_STAGE == MY_SPEC.aggr_stage_) {
    last_group = true;
  } else if (OB_FAIL(next_duplicate_data_permutation(nth_group, last_group, nullptr, insert_group_ht))) {
    LOG_WARN("failed to get next permutation", K(ret));
  } else {
    CK (dup_groupby_exprs_.count() == all_groupby_exprs_.count());
    for (int64_t i = 0; OB_SUCC(ret) && i < dup_groupby_exprs_.count(); ++i) {
      ObDatum *datum = nullptr;
      if (nullptr == dup_groupby_exprs_.at(i)) {
        all_groupby_exprs_.at(i)->locate_expr_datum(eval_ctx_).set_null();
      } else if (OB_FAIL(dup_groupby_exprs_.at(i)->eval(eval_ctx_, datum))) {
        LOG_WARN("failed to eval dup exprs", K(ret), K(i));
      } else {
        all_groupby_exprs_.at(i)->locate_expr_datum(eval_ctx_).set_datum(*datum);
        all_groupby_exprs_.at(i)->set_evaluated_projected(eval_ctx_);
      }
    }
  }
  return ret;
}

int ObHashGroupByOp::by_pass_get_next_permutation_batch(int64_t &nth_group, bool &last_group,
                                                        const ObBatchRows *child_brs,
                                                        ObBatchRows &my_brs,
                                                        bool &insert_group_ht)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(child_brs));
  OX (my_brs.size_ = child_brs->size_);
  OX (my_brs.skip_->deep_copy(*child_brs->skip_, child_brs->size_));
  if (OB_FAIL(ret)) {
  } else if (ObThreeStageAggrStage::NONE_STAGE == MY_SPEC.aggr_stage_) {
    last_group = true;
  } else if (OB_FAIL(next_duplicate_data_permutation(nth_group, last_group, child_brs, insert_group_ht))) {
    LOG_WARN("failed to get next permutation", K(ret));
  } else {
    CK (dup_groupby_exprs_.count() == all_groupby_exprs_.count());
    if (no_non_distinct_aggr_ && last_group) {
      my_brs.skip_->set_all(child_brs->size_);
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < dup_groupby_exprs_.count(); ++i) {
        ObDatum *datum = nullptr;
        if (nullptr == dup_groupby_exprs_.at(i)) {
          for (int64_t j = 0; j < child_brs->size_; ++j) {
            if (child_brs->skip_->at(j)) {
              continue;
            }
            all_groupby_exprs_.at(i)->locate_expr_datum(eval_ctx_, j).set_null();
          }
        } else if (OB_FAIL(dup_groupby_exprs_.at(i)->eval_batch(eval_ctx_, *child_brs->skip_, child_brs->size_))) {
          LOG_WARN("failed to eval dup exprs", K(ret), K(i));
        } else {
          for (int64_t j = 0; j < child_brs->size_; ++j) {
            if (child_brs->skip_->at(j)) {
              continue;
            }
            all_groupby_exprs_.at(i)->locate_expr_datum(eval_ctx_, j).set_datum(dup_groupby_exprs_.at(i)->locate_expr_datum(eval_ctx_, j));
          }
          all_groupby_exprs_.at(i)->set_evaluated_projected(eval_ctx_);
        }
      }
    }
  }
  return ret;
}

int ObHashGroupByOp::init_by_pass_op()
{
  int ret = OB_SUCCESS;
  int err_sim = 0;
  void *store_row_buf = nullptr;
  aggr_processor_.set_support_fast_single_row_agg(MY_SPEC.support_fast_single_row_agg_);
  bypass_ctrl_.open_by_pass_ctrl();
  uint64_t cut_ratio = 0;
  uint64_t default_cut_ratio = ObAdaptiveByPassCtrl::INIT_CUT_RATIO;
  err_sim = OB_E(EventTable::EN_ADAPTIVE_GROUP_BY_SMALL_CACHE) 0;
  if (0 != err_sim) {
    if (INT32_MAX == std::abs(err_sim)) {
      force_by_pass_ = true;
    } else {
      bypass_ctrl_.set_small_row_cnt(std::min(INIT_L2_CACHE_SIZE,
                                      static_cast<int64_t> (std::abs(err_sim))));
    }
  }
  if (OB_ISNULL(mem_context_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("failed to get mem context", K(ret));
  } else if (OB_FAIL(ctx_.get_my_session()
                            ->get_sys_variable(share::SYS_VAR__GROUPBY_NOPUSHDOWN_CUT_RATIO,
                                                                    cut_ratio))) {
    LOG_WARN("failed to get no pushdown cut ratio", K(ret));
  } else if (0 == MY_SPEC.max_batch_size_ && OB_FAIL(init_by_pass_group_row_item())) {
    LOG_WARN("failed to init by pass group row item", K(ret));
  } else if (MY_SPEC.max_batch_size_ > 0 && OB_FAIL(init_by_pass_group_batch_item())) {
    LOG_WARN("failed to init by pass group batch item", K(ret));
  } else if (OB_ISNULL(store_row_buf = mem_context_->get_malloc_allocator()
                                          .alloc(sizeof(ObChunkDatumStore::LastStoredRow)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for store row", K(ret));
  } else if (MY_SPEC.max_batch_size_ > 0
              && OB_FAIL(by_pass_brs_holder_.init(child_->get_spec().output_, eval_ctx_))) {
    LOG_WARN("failed to init brs holder", K(ret));
  } else {
    if (nullptr != store_row_buf) {
      last_child_row_ = new(store_row_buf)ObChunkDatumStore::LastStoredRow(mem_context_->get_arena_allocator());
      last_child_row_->reuse_ = true;
    }
    LOG_TRACE("check by pass", K(MY_SPEC.id_), K(MY_SPEC.by_pass_enabled_), K(bypass_ctrl_.get_small_row_cnt()),
                               K(get_actual_mem_used_size()), K(INIT_L2_CACHE_SIZE), K(INIT_L3_CACHE_SIZE));
    // to avoid performance decrease, at least deduplicate 2/3
    bypass_ctrl_.set_cut_ratio(std::max(cut_ratio, default_cut_ratio));
    bypass_ctrl_.set_op_id(MY_SPEC.id_);
  }
  return ret;
}

int ObHashGroupByOp::by_pass_restart_round()
{
  int ret = OB_SUCCESS;
  int64_t est_group_cnt = MY_SPEC.est_group_cnt_;
  int64_t est_hash_mem_size = 0;
  int64_t estimate_mem_size = 0;
  curr_group_id_ = 0;
  cur_group_item_idx_ = 0;
  cur_group_item_buf_ = nullptr;
  gri_cnt_per_batch_ = 0;
  aggr_processor_.reuse();
  // if last round is in L2 cache, reuse the bucket
  // otherwise resize to init size to avoid L2 cache overflow
  if (bypass_ctrl_.need_resize_hash_table_) {
    OZ(local_group_rows_.resize(&mem_context_->get_malloc_allocator(), INIT_BKT_SIZE_FOR_ADAPTIVE_GBY));
  } else {
    OX(local_group_rows_.reuse());
  }
  OZ(init_group_store());
  if (OB_SUCC(ret)) {
    group_rows_arr_.reuse();
    bypass_ctrl_.reset_state();
    bypass_ctrl_.inc_rebuild_times();
    bypass_ctrl_.need_resize_hash_table_ = false;
    by_pass_brs_holder_.restore();
    if (nullptr != last_child_row_ 
        && nullptr != last_child_row_->store_row_) {
      OZ (last_child_row_->store_row_->to_expr(child_->get_spec().output_, eval_ctx_));
    }
  }
  return ret;
}

int64_t ObHashGroupByOp::get_input_rows() const
{
  int64_t res = child_->get_spec().rows_;
  if (ObThreeStageAggrStage::FIRST_STAGE == MY_SPEC.aggr_stage_) {
    res = std::max(res, res * (MY_SPEC.dist_col_group_idxs_.count() + 1));
  }
  return res;
}

int64_t ObHashGroupByOp::get_input_size() const
{
  int64_t res = child_->get_spec().rows_ * child_->get_spec().width_;
  if (ObThreeStageAggrStage::FIRST_STAGE == MY_SPEC.aggr_stage_) {
    res = std::max(res, res * (MY_SPEC.dist_col_group_idxs_.count() + 1));
  }
  return res;
}

void ObHashGroupByOp::calc_avg_group_mem()
{
  if (0 == llc_est_.avg_group_mem_ && llc_est_.enabled_)
  {
    int64_t row_cnt = local_group_rows_.size();
    int64_t part_cnt = detect_part_cnt(row_cnt);
    int64_t data_size = get_actual_mem_used_size();
    int64_t est_extra_size = data_size + part_cnt * ObHashGroupByOp::FIX_SIZE_PER_PART;
    double data_ratio = (0 == est_extra_size) ? 0 : (data_size * 1.0 / est_extra_size);
    llc_est_.avg_group_mem_ = (0 == row_cnt || 0 == data_ratio) ? 128 : (data_size * data_ratio / row_cnt);
  }
}

int ObGroupRowHashTable::add_hashval_to_llc_map(LlcEstimate &llc_est)
{
  int ret = OB_SUCCESS;
  if (!is_inited()) { //check if buckets_ is NULL
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObGroupRowHashTable is not inited", K(ret));
  } else {
    for (int64_t i = 0; i < get_bucket_num(); ++i) {
      ObGroupRowItem *item = buckets_->at(i).item_;
      if (OB_NOT_NULL(item)) {
        ObAggregateProcessor::llc_add_value(item->hash_, llc_est.llc_map_);
      }
    }
  }
  return ret;
}


int ObHashGroupByOp::check_llc_ndv()
{
  int ret = OB_SUCCESS;
  int64_t ndv = -1;
  int64_t global_bound_size = 0;
  bool ndv_ratio_is_small_enough = false;
  bool has_enough_mem_for_deduplication = false;
  ObTenantSqlMemoryManager * tenant_sql_mem_manager = NULL;
  tenant_sql_mem_manager = MTL(ObTenantSqlMemoryManager*);
  ObExprEstimateNdv::llc_estimate_ndv(ndv, llc_est_.llc_map_);
  if (0 == llc_est_.est_cnt_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect zero cnt", K(llc_est_.est_cnt_), K(ret));
  } else if (FALSE_IT(ndv_ratio_is_small_enough = (ndv * 1.0 / llc_est_.est_cnt_) < LlcEstimate::LLC_NDV_RATIO_)) {
  } else if (FALSE_IT(llc_est_.last_est_cnt_ = llc_est_.est_cnt_)) {
  } else if (OB_ISNULL(tenant_sql_mem_manager)) {
     uint64_t tenant_id  = MTL_ID();
     if (OB_MAX_RESERVED_TENANT_ID <  tenant_id) {
       ret = OB_ERR_UNEXPECTED;
       LOG_WARN("unexpect null ptr", K(ret));
     } else if (ndv_ratio_is_small_enough) {
       bypass_ctrl_.bypass_rebackto_insert(ndv);
       llc_est_.enabled_  = false;
     }
  } else {
    global_bound_size = tenant_sql_mem_manager->get_global_bound_size();
    has_enough_mem_for_deduplication = (global_bound_size * LlcEstimate::GLOBAL_BOUND_RATIO_) > (llc_est_.avg_group_mem_ * ndv);
    LOG_TRACE("check llc ndv", K(ndv_ratio_is_small_enough), K(ndv), K(llc_est_.est_cnt_), K(has_enough_mem_for_deduplication), K(llc_est_.avg_group_mem_), K(global_bound_size), K(get_actual_mem_used_size()));
    if (!has_enough_mem_for_deduplication) {
      //continue bypass and stop estimating llc ndv
      llc_est_.enabled_ = false;
      LOG_TRACE("stop check llc ndv and continue bypass", K(ndv_ratio_is_small_enough), K(ndv), K(llc_est_.est_cnt_), K(has_enough_mem_for_deduplication), K(llc_est_.avg_group_mem_), K(global_bound_size), K(get_actual_mem_used_size()));
    } else if (ndv_ratio_is_small_enough) {
      // go to llc_insert_state and stop bypass and stop estimating llc ndv
      bypass_ctrl_.bypass_rebackto_insert(ndv);
      llc_est_.enabled_  = false;
      LOG_TRACE("reback into deduplication state and stop bypass and stop estimating llc ndv", K(ndv_ratio_is_small_enough), K(ndv), K(llc_est_.est_cnt_), K(has_enough_mem_for_deduplication), K(llc_est_.avg_group_mem_), K(global_bound_size), K(get_actual_mem_used_size()));
    } else {
      //do nothing, continue bypass and estimate llc ndv
    }
  }
  return ret;
}

int ObHashGroupByOp::bypass_add_llc_map(uint64_t hash_val, bool ready_to_check_ndv) {
  int ret = OB_SUCCESS;
  llc_add_value(hash_val);
  if ((llc_est_.est_cnt_ - llc_est_.last_est_cnt_) > LlcEstimate::ESTIMATE_MOD_NUM_ && ready_to_check_ndv) {
    if (OB_FAIL(check_llc_ndv())) {
      LOG_WARN("failed to check llc ndv", K(ret));
    } else if (0 < bypass_ctrl_.scaled_llc_est_ndv_) { // means state of bypass_ctrl_ go to INSERT from BYPASS
      if (OB_ISNULL(last_child_row_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null ptr", K(ret), K(llc_est_.est_cnt_), K(bypass_ctrl_.scaled_llc_est_ndv_));
      }
    }
  }
  return ret;
}
int ObHashGroupByOp::bypass_add_llc_map_batch(bool ready_to_check_ndv) {
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && llc_est_.enabled_ && i < brs_.size_; ++i) {
    if (brs_.skip_->exist(i)) { continue; }
    llc_add_value(hash_vals_[i]);
  }
  if ((llc_est_.est_cnt_ - llc_est_.last_est_cnt_) > LlcEstimate::ESTIMATE_MOD_NUM_ && ready_to_check_ndv) {
    if (OB_FAIL(check_llc_ndv())) {
      LOG_WARN("failed to check llc ndv", K(ret));
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
