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

#include "sql/engine/aggregate/ob_hash_groupby_vec_op.h"
#include "sql/engine/px/ob_px_util.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "lib/charset/ob_charset.h"
#include "src/sql/engine/expr/ob_expr_util.h"
#include "src/sql/engine/basic/ob_hp_infras_vec_op.h"

namespace oceanbase
{
using namespace common;
using namespace common::hash;

namespace sql
{

OB_SERIALIZE_MEMBER(ObGroupByDupColumnPairVec, org_expr, dup_expr);

OB_SERIALIZE_MEMBER((ObHashGroupByVecSpec, ObGroupBySpec),
  group_exprs_,cmp_funcs_, est_group_cnt_,
  org_dup_cols_, new_dup_cols_, dist_col_group_idxs_,
  distinct_exprs_);

DEF_TO_STRING(ObHashGroupByVecSpec)
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

int ObHashGroupByVecSpec::add_group_expr(ObExpr *expr)
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

void ObHashGroupByVecOp::reset(bool for_rescan)
{
  curr_group_id_ = common::OB_INVALID_INDEX;
  cur_group_item_idx_ = 0;
  cur_group_item_buf_ = nullptr;
  part_shift_ = sizeof(uint64_t) * CHAR_BIT / 2;
  if (for_rescan) {
    local_group_rows_.reuse();
    batch_aggr_rows_table_.reset();
  }
  sql_mem_processor_.reset();
  destroy_all_parts();
  first_batch_from_store_ = true;
  is_init_distinct_data_ = false;
  use_distinct_data_ = false;
  reset_distinct_info();
  bypass_ctrl_.reset();
  by_pass_nth_group_ = 0;
  by_pass_child_brs_ = nullptr;
  by_pass_group_batch_ = nullptr;
  by_pass_batch_size_ = 0;
  force_by_pass_ = false;
  by_pass_vec_holder_.reset();
  reorder_aggr_rows_ = eval_ctx_.max_batch_size_ >= MIN_BATCH_SIZE_REORDER_AGGR_ROWS
                           && MY_SPEC.aggr_stage_ != ObThreeStageAggrStage::THIRD_STAGE;
}

int ObHashGroupByVecOp::inner_open()
{
  int ret = OB_SUCCESS;
  reset(false);
  void *store_row_buf = nullptr;
  if (OB_FAIL(BaseClass::inner_open())) {
    LOG_WARN("failed to inner_open", K(ret));
  } else if (OB_FAIL(init_mem_context())) {
    LOG_WARN("init memory entity failed", K(ret));
  } else if (!local_group_rows_.is_inited()) {

    int64_t max_row_size = ObCompactRow::calc_max_row_size(MY_SPEC.group_exprs_,
                           ObExtendHashTableVec<ObGroupRowBucket>::calc_extra_size(aggr_processor_.get_aggregate_row_size()));
    //create bucket
    int64_t est_group_cnt = MY_SPEC.est_group_cnt_;
    int64_t est_hash_mem_size = 0;
    int64_t estimate_mem_size = 0;
    int64_t init_size = 0;
    bool nullable = true;
    bool all_int64 = false;
    ObMemAttr attr(ctx_.get_my_session()->get_effective_tenant_id(),
                   ObModIds::OB_HASH_NODE_GROUP_ROWS,
                   ObCtxIds::WORK_AREA);
    if (OB_SUCC(ret) && MY_SPEC.group_exprs_.count() <= 2) {
      use_sstr_aggr_ = true;
      for (int64_t i = 0; i < MY_SPEC.group_exprs_.count() && use_sstr_aggr_; i++) {
        ObExpr *expr = MY_SPEC.group_exprs_.at(i);
        if (ob_is_string_tc(expr->datum_meta_.type_) &&
            1 == expr->max_length_ &&
            ObCharset::is_bin_sort(expr->datum_meta_.cs_type_)) {
        } else {
          use_sstr_aggr_ = false;
        }
      }
    }
    if (OB_FAIL(local_group_rows_.prepare_hash_table(max_row_size,
                                                     attr.tenant_id_,
                                                     mem_context_->get_arena_allocator()))) {
       LOG_WARN("failed to prepare hash table", K(ret));
    } else if (OB_FAIL(ObPxEstimateSizeUtil::get_px_size(&ctx_,
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
    } else if (OB_FAIL(group_expr_fixed_lengths_.prepare_allocate(dup_groupby_exprs_.count()))) {
      LOG_WARN("failed to alloc array", K(ret), K(dup_groupby_exprs_.count()));
    } else if (FALSE_IT(check_groupby_exprs(dup_groupby_exprs_, nullable, all_int64))) {
    } else if (OB_FAIL(local_group_rows_.init(
                &mem_context_->get_malloc_allocator(),
                attr,
                dup_groupby_exprs_,
                dup_groupby_exprs_.count(),
                &eval_ctx_,
                MY_SPEC.max_batch_size_,
                nullable,
                all_int64,
                MY_SPEC.id_,
                use_sstr_aggr_,
                aggr_processor_.get_aggregate_row_size() + sizeof(int64_t),
                init_size,
                true))) {
      LOG_WARN("fail to init hash map", K(ret));
    } else if (OB_FAIL(sql_mem_processor_.update_used_mem_size(get_mem_used_size()))) {
      LOG_WARN("fail to update_used_mem_size", "size", get_mem_used_size(), K(ret));
    } else if (OB_FAIL(local_group_rows_.init_group_store(
                 sql_mem_processor_.get_dir_id(), &sql_mem_processor_,
                 mem_context_->get_malloc_allocator(), &io_event_observer_, MY_SPEC.max_batch_size_,
                 aggr_processor_.get_aggregate_row_size()))) {
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
      //THIRD_STAGE have to process dup data but aggr_code is not in gby_expr, we can not judge same group by aggr ptr
      reorder_aggr_rows_ = eval_ctx_.max_batch_size_ >= MIN_BATCH_SIZE_REORDER_AGGR_ROWS
                           && MY_SPEC.aggr_stage_ != ObThreeStageAggrStage::THIRD_STAGE;
      enable_dump_ = (!(aggr_processor_.has_distinct() || aggr_processor_.has_order_by())
                     && GCONF.is_sql_operator_dump_enabled());
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
      LOG_TRACE("trace init hash table", K(MY_SPEC.id_), K(init_size), K(MY_SPEC.est_group_cnt_), K(est_group_cnt),
        K(est_hash_mem_size), K(estimate_mem_size),
        K(profile_.get_expect_size()),
        K(profile_.get_cache_size()),
        K(sql_mem_processor_.get_mem_bound()),
        K(aggr_processor_.get_aggregate_row_size()));
    }
    if (OB_SUCC(ret)) {
      int64_t max_size = MY_SPEC.max_batch_size_;
      int64_t mem_size = max_size
                           * (sizeof(ObCompactRow *)
                              + sizeof(uint64_t)
                              + sizeof(uint64_t)
                              + sizeof(bool)
                              + sizeof(aggregate::AggrRowPtr *)
                              + sizeof(aggregate::AggrRowPtr *)
                              + sizeof(ObCompactRow *)
                              + sizeof(uint16_t))
                         + sizeof(ObIVector *) * MY_SPEC.aggr_infos_.count()
                         + ObBitVector::memory_size(max_size);
      char *buf = (char *)mem_context_->get_arena_allocator().alloc(mem_size);
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret), K(mem_size), K(max_size));
      } else {
        MEMSET(buf, 0, mem_size);
        int64_t batch_rows_from_dump_pos = 0;
        int64_t hash_vals_pos = batch_rows_from_dump_pos + max_size * sizeof(ObCompactRow *);
        int64_t base_hash_value_pos = hash_vals_pos + max_size * sizeof(uint64_t);
        int64_t is_dumped_pos = base_hash_value_pos + max_size * sizeof(uint64_t);
        int64_t batch_old_rows_pos = is_dumped_pos + max_size * sizeof(bool);
        int64_t batch_new_rows_pos =
          batch_old_rows_pos + max_size * sizeof(aggregate::AggrRowPtr *);
        int64_t return_rows_pos = batch_new_rows_pos + max_size * sizeof(aggregate::AggrRowPtr *);
        int64_t old_row_pos = return_rows_pos + max_size * sizeof(ObCompactRow *);
        int64_t aggr_vectors_pos = old_row_pos + max_size * sizeof(uint16_t);
        int64_t dumped_batch_rows_pos = aggr_vectors_pos + MY_SPEC.aggr_infos_.count() * sizeof(ObIVector *);

        batch_rows_from_dump_ =
          reinterpret_cast<const ObCompactRow **>(buf + batch_rows_from_dump_pos);
        hash_vals_ = reinterpret_cast<uint64_t *>(buf + hash_vals_pos);
        base_hash_vals_ = reinterpret_cast<uint64_t *>(buf + base_hash_value_pos);
        is_dumped_ = reinterpret_cast<bool *>(buf + is_dumped_pos);
        batch_old_rows_ = reinterpret_cast<aggregate::AggrRowPtr *>(buf + batch_old_rows_pos);
        batch_new_rows_ = reinterpret_cast<aggregate::AggrRowPtr *>(buf + batch_new_rows_pos);
        return_rows_ = reinterpret_cast<const ObCompactRow **>(buf + return_rows_pos);
        old_row_selector_ = reinterpret_cast<uint16_t *> (buf + old_row_pos);
        aggr_vectors_ = reinterpret_cast<ObIVector **> (buf + aggr_vectors_pos);
        dumped_batch_rows_.skip_ = to_bit_vector(buf + dumped_batch_rows_pos);
      }
    }
    if (OB_SUCC(ret) && reorder_aggr_rows_) {
      if (OB_FAIL(batch_aggr_rows_table_.init(MY_SPEC.max_batch_size_,
                                              mem_context_->get_arena_allocator()))) {
        LOG_WARN("failed to init batch aggr rows table", K(ret));
      }
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

int ObHashGroupByVecOp::reinit_group_store()
{
  local_group_rows_.reset_group_store();
  return OB_SUCCESS;
}

int ObHashGroupByVecOp::inner_close()
{
  sql_mem_processor_.unregister_profile();
  distinct_sql_mem_processor_.unregister_profile();
  group_expr_fixed_lengths_.destroy();
  dump_vectors_.destroy();
  curr_group_id_ = common::OB_INVALID_INDEX;
  return ObGroupByVecOp::inner_close();
}

int ObHashGroupByVecOp::init_mem_context()
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

void ObHashGroupByVecOp::destroy()
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
  distinct_data_set_.destroy();
  if (NULL != mem_context_) {
    destroy_all_parts();
  }
  ObGroupByVecOp::destroy();
  // 内存最后释放，ObHashCtx依赖于mem_context
  if (NULL != mem_context_) {
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = NULL;
  }
}

int ObHashGroupByVecOp::inner_switch_iterator()
{
  int ret = OB_SUCCESS;
  reset(true);
  if (OB_FAIL(ObGroupByVecOp::inner_switch_iterator())) {
    LOG_WARN("failed to rescan", K(ret));
  }
  return ret;
}

int ObHashGroupByVecOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  reset(true);
  llc_est_.reset();
  if (OB_FAIL(ObGroupByVecOp::inner_rescan())) {
    LOG_WARN("failed to rescan", K(ret));
  } else {
    iter_end_ = false;
    llc_est_.enabled_ = MY_SPEC.by_pass_enabled_ && MY_SPEC.llc_ndv_est_enabled_ && !force_by_pass_; // keep lc_est_.enabled_ same as inner_open()
  }
  return ret;

}

int ObHashGroupByVecOp::inner_get_next_row()
{
  return OB_NOT_IMPLEMENT;
}

// select c1,count(distinct c2),sum(distinct c3),max(c4) from group by c1;
// groupby exprs:   [c1, aggr_code, c2]
// purmutation:   0 - [c1, aggr_code, c2, null]
//                1 - [c1, aggr_code, null, c3]
int ObHashGroupByVecOp::next_duplicate_data_permutation(
  int64_t &nth_group, bool &last_group, const ObBatchRows *child_brs, bool &insert_group_ht)
{
  int ret = OB_SUCCESS;
  last_group = true;
  insert_group_ht = true;
  if (OB_ISNULL(child_brs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get brs", K(ret));
  } else if (ObThreeStageAggrStage::NONE_STAGE == MY_SPEC.aggr_stage_) {
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
    } else {
      const int64_t aggr_code = last_group ? MY_SPEC.dist_col_group_idxs_.count() : nth_group;
      ObExpr *aggr_code_expr = all_groupby_exprs_.at(first_idx - 1);
      ObIVector *aggr_code_col = nullptr;
      if (OB_FAIL(aggr_code_expr->init_vector(eval_ctx_, VEC_FIXED, MY_SPEC.max_batch_size_))) {
        LOG_WARN("failed to init vector", K(ret));
      } else {
        ObFixedLengthFormat<int64_t> *aggr_code_col = static_cast<ObFixedLengthFormat<int64_t> *> (aggr_code_expr->get_vector(eval_ctx_));
        ObBitVector &eval_flags = aggr_code_expr->get_evaluated_flags(eval_ctx_);
        for (int64_t i = 0; i < child_brs->size_; ++i) {
          if (child_brs->skip_->exist(i)) {
            continue;
          }
          aggr_code_col->set_int(i, aggr_code);
          eval_flags.set(i);
        }
        aggr_code_expr->set_evaluated_projected(eval_ctx_);
        LOG_DEBUG("debug write aggr code", K(ret), K(aggr_code), K(first_idx));
      }
    }
    LOG_DEBUG("debug write aggr code", K(ret), K(last_group), K(nth_group), K(first_idx),
      K(no_non_distinct_aggr_), K(start_idx), K(end_idx), K(dup_groupby_exprs_));
  } else if (ObThreeStageAggrStage::SECOND_STAGE == MY_SPEC.aggr_stage_) {
    if (!use_distinct_data_) {
      //get the aggr_code and then insert group hash-table or insert duplicate hash-table
      distinct_skip_->reset(child_brs->size_);
    }
  } else {
    // ret = OB_ERR_UNEXPECTED;
    // LOG_WARN("unexpected status: invalid aggr stage", K(ret), K(MY_SPEC.aggr_stage_));
  }
  ++nth_group;
  return ret;
}

int ObHashGroupByVecOp::init_distinct_info(bool is_part)
{
  int ret = OB_SUCCESS;

  int64_t est_rows =
    is_part ? distinct_data_set_.get_cur_part_row_cnt(InputSide::LEFT) : MY_SPEC.rows_;
  int64_t est_size = is_part ? distinct_data_set_.get_cur_part_file_size(InputSide::LEFT) :
                               est_rows * MY_SPEC.width_;
  int64_t tenant_id = ctx_.get_my_session()->get_effective_tenant_id();
  if (!is_part
      && OB_FAIL(ObPxEstimateSizeUtil::get_px_size(&ctx_, MY_SPEC.px_est_size_factor_, est_rows,
                                                   est_rows))) {
    LOG_WARN("failed to get px size", K(ret));
  } else if (OB_FAIL(distinct_sql_mem_processor_.init(&mem_context_->get_malloc_allocator(),
                                                      tenant_id, est_size, MY_SPEC.type_,
                                                      MY_SPEC.id_, &ctx_))) {
    LOG_WARN("failed to init sql mem processor", K(ret));
  } else if (is_part) {
    // do nothing
  } else if (0 == distinct_origin_exprs_.count() || 0 == n_distinct_expr_
             || distinct_origin_exprs_.count() < n_distinct_expr_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: distinct origin exprs is empty", K(ret));
  } else if (OB_FAIL(distinct_data_set_.init(
               tenant_id, GCONF.is_sql_operator_dump_enabled(), true, true, 1,
               MY_SPEC.max_batch_size_, distinct_origin_exprs_, &distinct_sql_mem_processor_,
               MY_SPEC.compress_type_))) {
    LOG_WARN("failed to init hash partition infrastructure", K(ret));
  } else if (FALSE_IT(distinct_data_set_.set_io_event_observer(&io_event_observer_))) {
  } else if (OB_FAIL(hash_funcs_.init(n_distinct_expr_))) {// TODO: remove hash_funcs_
    LOG_WARN("failed to init hash funcs", K(ret));
  } else if (OB_FAIL(cmp_funcs_.init(n_distinct_expr_))) {// TODO: remove cmp_funcs_
    LOG_WARN("failed to init cmp funcs", K(ret));
  } else if (OB_FAIL(sort_collations_.init(n_distinct_expr_))) {
    LOG_WARN("failed to init sort collations", K(ret));
  } else {
    for (int64_t i = 0; i < n_distinct_expr_ && OB_SUCC(ret); ++i) {
      // TODO: remove cmp_funcs_ & hash_funcs_
      ObHashFunc hash_func;
      ObExpr *expr = distinct_origin_exprs_.at(i);
      ObOrderDirection order_direction = default_asc_direction();
      bool is_ascending = is_ascending_direction(order_direction);
      ObSortFieldCollation field_collation(
        i, expr->datum_meta_.cs_type_, is_ascending,
        (is_null_first(order_direction) ^ is_ascending) ? NULL_LAST : NULL_FIRST);
      ObCmpFunc cmp_func;
      cmp_func.cmp_func_ = ObDatumFuncs::get_nullsafe_cmp_func(
        expr->datum_meta_.type_, expr->datum_meta_.type_,
        NULL_LAST, //这里null last还是first无所谓
        expr->datum_meta_.cs_type_, expr->datum_meta_.scale_, lib::is_oracle_mode(),
        expr->obj_meta_.has_lob_header());
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

    int64_t est_bucket_num = distinct_data_set_.est_bucket_count(
      est_rows, MY_SPEC.width_, MIN_GROUP_HT_INIT_SIZE, MAX_GROUP_HT_INIT_SIZE);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(distinct_data_set_.set_funcs(&sort_collations_, &eval_ctx_))) {
      LOG_WARN("failed to set funcs", K(ret));
    } else if (OB_FAIL(distinct_data_set_.start_round())) {
      LOG_WARN("failed to start round", K(ret));
    } else if (OB_FAIL(distinct_data_set_.init_hash_table(est_bucket_num, MIN_GROUP_HT_INIT_SIZE,
                                                           MAX_GROUP_HT_INIT_SIZE))) {
      LOG_WARN("failed to init hash table", K(ret));
    } else {
      is_init_distinct_data_ = true;
      LOG_DEBUG("debug distinct_origin_exprs_", K(distinct_origin_exprs_.count()),
                K(hash_funcs_.count()), K(cmp_funcs_.count()), K(sort_collations_.count()),
                K(MY_SPEC.aggr_stage_), K(n_distinct_expr_));
      if (OB_FAIL(distinct_data_set_.init_my_skip(MY_SPEC.max_batch_size_))) {
        LOG_WARN("failed to init hp skip", K(ret));
      }
    }
  }
  return ret;
}

void ObHashGroupByVecOp::reset_distinct_info()
{
  hash_funcs_.destroy();
  cmp_funcs_.destroy();
  sort_collations_.destroy();
  distinct_data_set_.destroy_my_skip();
  distinct_data_set_.reset();
}

int ObHashGroupByVecOp::finish_insert_distinct_data()
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

int ObHashGroupByVecOp::alloc_group_row(const int64_t group_id, aggregate::AggrRowPtr row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid group row", K(ret));
  } else if (OB_FAIL(aggr_processor_.add_one_aggregate_row(
               row, aggr_processor_.get_aggregate_row_size(),
               ObThreeStageAggrStage::SECOND_STAGE == MY_SPEC.aggr_stage_))) {
    LOG_WARN("failed to init one group", K(group_id), K(ret));
  }
  return ret;
}

int ObHashGroupByVecOp::update_mem_status_periodically(const int64_t nth_cnt,
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
      double data_ratio = sql_mem_processor_.get_data_ratio();
      est_part_cnt = detect_part_cnt(input_row);
      calc_data_mem_ratio(est_part_cnt, data_ratio);
      need_dump = is_need_dump(data_ratio);
    }
  }
  return ret;
}

int64_t ObHashGroupByVecOp::detect_part_cnt(const int64_t rows) const
{
  const double group_mem_avg = (double)get_data_size() / local_group_rows_.size();
  int64_t data_size = rows * ((double)agged_group_cnt_ / agged_row_cnt_) * group_mem_avg;
  int64_t mem_bound = get_mem_bound_size();
  int64_t part_cnt = (data_size + mem_bound) / mem_bound;
  part_cnt = next_pow2(part_cnt);
  // 这里只有75%的利用率，后续改成segment array看下是否可以去掉
  int64_t availble_mem_size = mem_bound - get_mem_used_size();
  int64_t est_dump_size = part_cnt * ObTempRowStore::BLOCK_SIZE;
  if (0 < availble_mem_size) {
    while (est_dump_size > availble_mem_size) {
      est_dump_size >>= 1;
    }
    part_cnt = est_dump_size / ObTempRowStore::BLOCK_SIZE;
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

void ObHashGroupByVecOp::calc_data_mem_ratio(const int64_t part_cnt, double &data_ratio)
{
  int64_t est_extra_size = (get_mem_used_size() + part_cnt * FIX_SIZE_PER_PART);
  int64_t data_size = get_mem_used_size();
  data_ratio = data_size * 1.0 / est_extra_size;
  sql_mem_processor_.set_data_ratio(data_ratio);
  LOG_TRACE("trace calc data ratio", K(data_ratio), K(est_extra_size), K(part_cnt),
            K(data_size), K(get_aggr_used_size()),
            K(profile_.get_expect_size()), K(profile_.get_cache_size()));
}

void ObHashGroupByVecOp::calc_avg_group_mem()
{
  if (0 == llc_est_.avg_group_mem_ && llc_est_.enabled_)
  {
    int64_t row_cnt = local_group_rows_.size();
    int64_t part_cnt = detect_part_cnt(row_cnt);
    int64_t data_size = get_actual_mem_used_size();
    int64_t est_extra_size = data_size + part_cnt * FIX_SIZE_PER_PART;
    double data_ratio = (0 == est_extra_size) ? 0 : (data_size * 1.0 / est_extra_size);
    llc_est_.avg_group_mem_ = (0 == row_cnt || 0 == data_ratio) ? 128 : (data_size * data_ratio / row_cnt);
  }
}

void ObHashGroupByVecOp::adjust_part_cnt(int64_t &part_cnt)
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

bool ObHashGroupByVecOp::need_start_dump(const int64_t input_rows, int64_t &est_part_cnt,
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
            data_ratio = sql_mem_processor_.get_data_ratio();
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

int ObHashGroupByVecOp::setup_dump_env(const int64_t part_id, const int64_t input_rows,
    DatumStoreLinkPartition **parts, int64_t &part_cnt, ObGbyBloomFilterVec *&bloom_filter)
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
      void *mem = mem_context_->get_malloc_allocator().alloc(sizeof(ObGbyBloomFilterVec));
      if (OB_ISNULL(mem)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else if (FALSE_IT(bloom_filter = new(mem)ObGbyBloomFilterVec(mod_alloc))) {
      } else if (OB_FAIL(bloom_filter->init(local_group_rows_.size()))) {
        LOG_WARN("bloom filter init failed", K(ret));
      } else {
        auto cb_func = [&](int64_t hash_val) {
          int ret = OB_SUCCESS;
          if (OB_FAIL(bloom_filter->set(hash_val))) {
            LOG_WARN("add hash value to bloom failed", K(ret));
          }
          return ret;
        };
        if (OB_FAIL(local_group_rows_.foreach_bucket_hash(cb_func))) {
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
        ObMemAttr attr(ctx_.get_my_session()->get_effective_tenant_id(), ObModIds::OB_HASH_NODE_GROUP_ROWS, ObCtxIds::WORK_AREA);
        if (OB_FAIL(parts[i]->row_store_.init(child_->get_spec().output_,
                                              MY_SPEC.max_batch_size_,
                                              attr,
                                              1/* memory limit, dump immediately */,
                                              true,
                                              extra_size,
                                              MY_SPEC.compress_type_))) {
          LOG_WARN("init temp row store failed", K(ret));
        } else {
          parts[i]->row_store_.set_dir_id(sql_mem_processor_.get_dir_id());
          parts[i]->row_store_.set_callback(&sql_mem_processor_);
          parts[i]->row_store_.set_io_event_observer(&io_event_observer_);
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

  if (OB_SUCC(ret) && part_cnt <= MAX_BATCH_DUMP_PART_CNT && need_reinit_vectors_) {
    // only_init_once
    dump_vectors_.set_allocator(&mem_context_->get_allocator());
    if (OB_ISNULL(dump_rows_ = static_cast<ObCompactRow **>
                        (mem_context_->get_arena_allocator().alloc(sizeof(ObCompactRow *) * MY_SPEC.max_batch_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc dump_rows_", K(ret), K(MY_SPEC.max_batch_size_));
    } else if (OB_FAIL(dump_vectors_.prepare_allocate(child_->get_spec().output_.count()))) {
      LOG_WARN("failed to init dump_vectors prepare_allocate", K(ret), K(child_->get_spec().output_.count()));
    } else if (OB_ISNULL(dump_add_row_selectors_ =
                  static_cast<uint16_t **> (mem_context_->get_arena_allocator().alloc(sizeof(uint16_t *) * MAX_BATCH_DUMP_PART_CNT)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to init temp_add_row_selectors_", K(ret));
    } else if (OB_ISNULL(dump_add_row_selectors_item_cnt_ =
                      static_cast<uint16_t *> (mem_context_->get_arena_allocator().alloc(sizeof(uint16_t) * MAX_BATCH_DUMP_PART_CNT)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to init temp_add_row_selectors_item_cnt_", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < MAX_BATCH_DUMP_PART_CNT; ++i) {
        if (OB_ISNULL(dump_add_row_selectors_[i] = static_cast<uint16_t *> (mem_context_->get_arena_allocator().alloc(sizeof(uint16_t)
                                                              * MY_SPEC.max_batch_size_)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to init temp_add_row_selectors_item_cnt_[index]", K(ret), K(i));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < child_->get_spec().output_.count() ; i++) {
        ObIVector *col_vec = child_->get_spec().output_.at(i)->get_vector(eval_ctx_);
        dump_vectors_.at(i) = col_vec;
      }
      need_reinit_vectors_ = false;
    }
  }
  return ret;
}

int ObHashGroupByVecOp::cleanup_dump_env(const bool dump_success, const int64_t part_id,
    DatumStoreLinkPartition **parts, int64_t &part_cnt, ObGbyBloomFilterVec *&bloom_filter)
{
  int ret = OB_SUCCESS;
  // add spill partitions to partition list
  if (dump_success && part_cnt > 0) {
    int64_t part_rows[part_cnt];
    int64_t part_file_size[part_cnt];
    for (int64_t i = 0; OB_SUCC(ret) && i < part_cnt; i++) {
      DatumStoreLinkPartition *&p = parts[i];
      if (p->row_store_.get_row_cnt() > 0) {
        if (OB_FAIL(p->row_store_.dump(true))) {
          LOG_WARN("failed to dump partition", K(ret), K(i));
        } else if (OB_FAIL(p->row_store_.finish_add_row(true /* do dump */))) {
          LOG_WARN("do dump failed", K(ret));
        } else {
          part_rows[i] = p->row_store_.get_row_cnt();
          part_file_size[i] = p->row_store_.get_file_size();
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
      bloom_filter->~ObGbyBloomFilterVec();
      mem_context_->get_malloc_allocator().free(bloom_filter);
      bloom_filter = NULL;
    }
  }

  return ret;
}

void ObHashGroupByVecOp::destroy_all_parts()
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

int ObHashGroupByVecOp::inner_get_next_batch(const int64_t max_row_cnt)
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
        reset(false);
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
        by_pass_vec_holder_.restore();
        calc_avg_group_mem();
        if (llc_est_.enabled_ && llc_est_.est_cnt_ != agged_group_cnt_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected est_cnt number", K(ret), K(agged_group_cnt_), K(agged_row_cnt_), K(llc_est_.est_cnt_));
        }
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
          reset(false);
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (brs_.end_) {
    } else if (bypass_ctrl_.by_passing()) {
      if (OB_FAIL(by_pass_prepare_one_batch(op_max_batch_size))) {
        LOG_WARN("failed to prepare batch", K(ret));
      }
    } else if (ObThreeStageAggrStage::SECOND_STAGE == MY_SPEC.aggr_stage_) {
      clear_evaluated_flag();
      if (OB_FAIL(aggr_processor_.collect_group_results(local_group_rows_.get_row_meta(),
                                                        all_groupby_exprs_, op_max_batch_size, brs_,
                                                        curr_group_id_))) {
        LOG_WARN("failed to collect batch result", K(ret), K(curr_group_id_));
      } else {
        brs_.all_rows_active_ = true;
      }
    } else {
      int64_t read_rows = 0;
      clear_evaluated_flag();
      if (OB_FAIL(local_group_rows_.get_next_batch(return_rows_, op_max_batch_size, read_rows))) {
        LOG_WARN("failed to get batch", K(ret));
      } else if (OB_UNLIKELY(0 == read_rows)) {
        brs_.size_ = 0;
      } else if (OB_FAIL(aggr_processor_.collect_group_results(local_group_rows_.get_row_meta(),
                                                               all_groupby_exprs_, read_rows,
                                                               return_rows_, brs_))) {
        LOG_WARN("failed to get batch result", K(ret));
      } else {
        curr_group_id_ += read_rows;
      }
      if (OB_SUCC(ret)) {
        brs_.all_rows_active_ = true;
      }
    }
  }

#ifdef ENABLE_DEBUG_LOG
  // check all_row_active_
  if (OB_SUCC(ret) && brs_.size_ > 0) {
    bool is_all_false = brs_.skip_->is_all_false(brs_.size_);
    if (brs_.all_rows_active_ && !is_all_false) {
      ob_abort();
    }
  }
#endif
  LOG_DEBUG("after inner_get_next_batch",
            K(get_aggr_used_size()), K(get_aggr_used_size()),
            K(get_hash_table_used_size()),
            K(get_dumped_part_used_size()), K(get_dump_part_hold_size()),
            K(get_mem_used_size()), K(get_mem_bound_size()),
            K(agged_dumped_cnt_), K(agged_group_cnt_), K(agged_row_cnt_),
            K(curr_group_id_), K(brs_), K(local_group_rows_.size()), K(MY_SPEC.aggr_stage_));
  return ret;
}

int ObHashGroupByVecOp::load_data_batch(int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  ObTempRowStore::Iterator row_store_iter;
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
  ObGbyBloomFilterVec *bloom_filter = NULL;
  const ObCompactRow **store_rows = NULL;
  const RowMeta *meta = NULL;
  int64_t loop_cnt = 0;
  int64_t last_batch_size = 0;

  while (OB_SUCC(ret)) {
    bypass_ctrl_.gby_process_state(local_group_rows_.get_probe_cnt(),
                                   local_group_rows_.size(),
                                   get_actual_mem_used_size());
    if (bypass_ctrl_.processing_ht()) {
      by_pass_vec_holder_.save(last_batch_size);
      break;
    }
    const ObBatchRows *child_brs = NULL;
    start_calc_hash_idx_ = 0;
    has_calc_base_hash_ = false;
    reorder_aggr_rows_ &= batch_aggr_rows_table_.is_valid();
    if (OB_FAIL(next_batch(NULL != cur_part, row_store_iter, max_row_cnt, child_brs))) {
      LOG_WARN("fail to get next batch", K(ret));
    } else if (child_brs->size_ > 0) {
      last_batch_size = child_brs->size_;
      if (NULL != cur_part) {
        store_rows = batch_rows_from_dump_;
        meta = &cur_part->row_store_.get_row_meta();
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
        if (OB_FAIL(group_child_batch_rows(store_rows, meta, input_rows, check_dump, part_id, part_shift,
                                             loop_cnt, *child_brs, part_cnt, parts, est_part_cnt,
                                             bloom_filter))) {
          LOG_WARN("fail to group child batch rows", K(ret), K(start_dump));
        } else if (no_non_distinct_aggr_) {
        } else if (OB_FAIL(aggr_processor_.eval_aggr_param_batch(*child_brs))) {
          LOG_WARN("fail to eval aggr param batch", K(ret), K(*child_brs));
        }
        //batch calc aggr for each group
        int32_t start_agg_id = -1;
        int32_t end_agg_id = -1;
        bool calc_multi_groups = (MY_SPEC.aggr_stage_ == ObThreeStageAggrStage::FIRST_STAGE
                                  || MY_SPEC.aggr_stage_ == ObThreeStageAggrStage::NONE_STAGE);

        for (int64_t i = 0; !calc_multi_groups && i < MY_SPEC.aggr_infos_.count(); ++i) {
          if (MY_SPEC.aggr_infos_.at(i).param_exprs_.count() == 1) {
            aggr_vectors_[i] = MY_SPEC.aggr_infos_.at(i).param_exprs_.at(0)->get_vector(eval_ctx_);
          } else if (MY_SPEC.aggr_infos_.at(i).is_implicit_first_aggr()) {
            aggr_vectors_[i] = MY_SPEC.aggr_infos_.at(i).expr_->get_vector(eval_ctx_);
          } else {
            aggr_vectors_[i] = nullptr;
          }
        }
        if (OB_FAIL(ret)) {
        } else if (calc_multi_groups) { // do nothing
        } else if (OB_FAIL(aggr_processor_.prepare_adding_one_row())) {
          LOG_WARN("prepare add one row failed", K(ret));
        }
        if (OB_SUCC(ret) && calc_multi_groups) {
          if (OB_FAIL(process_multi_groups(batch_new_rows_, *child_brs))) {
            LOG_WARN("process multiple groups failed", K(ret));
          }
        }
        for (int64_t i = 0; !calc_multi_groups && OB_SUCC(ret) && i < child_brs->size_; i++) {
          if ((!batch_new_rows_[i])) {
            continue;
          }
          if (ObThreeStageAggrStage::FIRST_STAGE != MY_SPEC.aggr_stage_
              && ObThreeStageAggrStage::NONE_STAGE != MY_SPEC.aggr_stage_) {
            start_agg_id = -1;
            end_agg_id = -1;
          }
          if (start_agg_id != -1 && end_agg_id != -1) { // do nothing
          } else if (OB_FAIL(ObGroupByVecOp::calculate_3stage_agg_info(
                          batch_new_rows_[i], local_group_rows_.get_row_meta(),
                          i, start_agg_id, end_agg_id))) {
            LOG_WARN("calculate 3stage aggregate info failed", K(ret));
          } else if (OB_UNLIKELY(start_agg_id == -1 || end_agg_id == -1)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected aggregate idx", K(ret), K(start_agg_id), K(end_agg_id));
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(aggr_processor_.add_one_row(start_agg_id, end_agg_id,
                                                         batch_new_rows_[i], i,
                                                         child_brs->size_, aggr_vectors_))) {
            LOG_WARN("fail to process row", K(ret));
          }
        }
        start_agg_id = -1;
        end_agg_id = -1;
        if (OB_FAIL(ret)) {
        } else if (!reorder_aggr_rows_ || !batch_aggr_rows_table_.is_valid()) {
          if (calc_multi_groups) {
            if (OB_FAIL(process_multi_groups(batch_old_rows_, *child_brs))) {
              LOG_WARN("process multiple groups failed", K(ret));
            }
          }
          for (int64_t i = 0; !calc_multi_groups && OB_SUCC(ret) && i < child_brs->size_; i++) {
            if ((!batch_old_rows_[i])) {
              continue;
            }
            if (ObThreeStageAggrStage::FIRST_STAGE != MY_SPEC.aggr_stage_
                && ObThreeStageAggrStage::NONE_STAGE != MY_SPEC.aggr_stage_) {
              start_agg_id = -1;
              end_agg_id = -1;
            }
            if (start_agg_id != -1 && end_agg_id != -1) { // do nothing
            } else if (OB_FAIL(ObGroupByVecOp::calculate_3stage_agg_info(
                            batch_old_rows_[i], local_group_rows_.get_row_meta(),
                            i, start_agg_id, end_agg_id))) {
              LOG_WARN("calculate 3stage aggregate info failed", K(ret));
            } else if (OB_UNLIKELY(start_agg_id == -1 || end_agg_id == -1)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected aggregate idx", K(ret), K(start_agg_id), K(end_agg_id));
            }
            if (OB_SUCC(ret)) {
              if (OB_FAIL(aggr_processor_.add_one_row(start_agg_id, end_agg_id,
                                                      batch_old_rows_[i], i,
                                                      child_brs->size_, aggr_vectors_))) {
                LOG_WARN("fail to process row", K(ret));
              }
            }
          }
        } else {
          for (int64_t i = 0; OB_SUCC(ret)
                              && i < BatchAggrRowsTable::MAX_REORDER_GROUPS; ++i) {
            if (nullptr == batch_aggr_rows_table_.aggr_rows_[i]) {
              continue;
            }
            if (ObThreeStageAggrStage::FIRST_STAGE != MY_SPEC.aggr_stage_
                && ObThreeStageAggrStage::NONE_STAGE != MY_SPEC.aggr_stage_) {
              start_agg_id = -1;
              end_agg_id = -1;
            }
            if (start_agg_id != -1 && end_agg_id != -1) { // do nothing
            } else if (OB_FAIL(ObGroupByVecOp::calculate_3stage_agg_info(
                            batch_aggr_rows_table_.aggr_rows_[i], local_group_rows_.get_row_meta(),
                            batch_aggr_rows_table_.selectors_[i][0], start_agg_id, end_agg_id))) {
              LOG_WARN("calculate 3stage aggregate info failed", K(ret));
            } else if (OB_UNLIKELY(start_agg_id == -1 || end_agg_id == -1)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected aggregate idx", K(ret), K(start_agg_id), K(end_agg_id));
            }
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(aggr_processor_.add_batch_rows(start_agg_id, end_agg_id,
                                              batch_aggr_rows_table_.aggr_rows_[i],
                                              *child_brs,
                                              batch_aggr_rows_table_.selectors_[i],
                                              batch_aggr_rows_table_.selectors_item_cnt_[i]))) {
              LOG_WARN("failed to add batch rows", K(ret));
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (!calc_multi_groups && OB_FAIL(aggr_processor_.finish_adding_one_row())) {
          LOG_WARN("finish add one row failed", K(ret));
        }
      }
      if (child_brs->end_) {
        break;
      }
    } else {
      break;
    }
  } // while end

  row_store_iter.reset();
  if (OB_SUCC(ret) && llc_est_.enabled_ && local_group_rows_.is_sstr_aggr_valid()) {
    llc_est_.enabled_ = false;// do not need llc, ndv must less then 65535
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

int ObHashGroupByVecOp::switch_part(DatumStoreLinkPartition *&cur_part,
                                 ObTempRowStore::Iterator &row_store_iter,
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
    } else if (OB_FAIL(row_store_iter.init(&cur_part->row_store_))) {
      LOG_WARN("init row store iterator failed", K(ret));
    } else {
      input_rows = cur_part->row_store_.get_row_cnt();
      part_id = cur_part->part_id_;
      part_shift = part_shift_ = cur_part->part_shift_;
      input_size = cur_part->row_store_.get_file_size();
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
    } else if (OB_FAIL(reinit_group_store())) {
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

int ObHashGroupByVecOp::next_batch(bool is_from_row_store,
                                ObTempRowStore::Iterator &row_store_iter,
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

int ObHashGroupByVecOp::calc_groupby_exprs_hash_batch(
  ObIArray<ObExpr *> &groupby_exprs, const ObBatchRows &child_brs)
{
  uint64_t seed = HASH_SEED;
  int64_t ret = OB_SUCCESS;
  if (ObThreeStageAggrStage::FIRST_STAGE == MY_SPEC.aggr_stage_ && !has_calc_base_hash_) {
    // first calc hash values of groupby_expr
    for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.aggr_code_idx_ + 1; ++i) {
      ObExpr *expr = groupby_exprs.at(i);
      if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status: groupby exprs is null", K(ret));
      } else {
        const bool is_batch_seed = (i > 0);
        ObIVector *col_vec = expr->get_vector(eval_ctx_);
        if (VectorFormat::VEC_FIXED == col_vec->get_format()) {
          group_expr_fixed_lengths_.at(i) = static_cast<ObFixedLengthBase *> (col_vec)->get_length();
        } else {
          group_expr_fixed_lengths_.at(i) = 0;
        }
        if (OB_FAIL(col_vec->murmur_hash_v3(*expr, base_hash_vals_,
                                            *child_brs.skip_,
                                            EvalBound(child_brs.size_, child_brs.all_rows_active_),
                                            is_batch_seed ? base_hash_vals_ : &seed,
                                            is_batch_seed))) {
          LOG_WARN("failed to calc hash value", K(ret));
        }
      }
    }
    has_calc_base_hash_ = true;
    start_calc_hash_idx_ = MY_SPEC.aggr_code_idx_ + 1;
  }
  if (OB_SUCC(ret) && 0 < start_calc_hash_idx_) {
    for (int64_t i = 0; i < child_brs.size_; ++i) {
      if (!child_brs.skip_->exist(i)) {
        hash_vals_[i] = base_hash_vals_[i];
      }
    }
  }
  for (int64_t i = start_calc_hash_idx_; OB_SUCC(ret) && i < groupby_exprs.count(); ++i) {
    ObExpr *expr = groupby_exprs.at(i);
    if (OB_ISNULL(expr)) {
    } else {
      const bool is_batch_seed = (i > 0);
      ObIVector *col_vec = expr->get_vector(eval_ctx_);
      if (VectorFormat::VEC_FIXED == col_vec->get_format()) {
        group_expr_fixed_lengths_.at(i) = static_cast<ObFixedLengthBase *> (col_vec)->get_length();
      } else {
        group_expr_fixed_lengths_.at(i) = 0;
      }
      if (OB_FAIL(col_vec->murmur_hash_v3(*expr, hash_vals_, *child_brs.skip_,
                                          EvalBound(child_brs.size_, child_brs.all_rows_active_),
                                          is_batch_seed ? hash_vals_ : &seed, is_batch_seed))) {
        LOG_WARN("failed to calc hash value", K(ret));
      }
    }
  }
  return ret;
}

int ObHashGroupByVecOp::eval_groupby_exprs_batch(const ObCompactRow **store_rows,
                                                 const RowMeta *meta,
                                                 const ObBatchRows &child_brs)
{
  int ret = OB_SUCCESS;
  if (NULL == store_rows) {
    for (int64_t i = 0; OB_SUCC(ret) && i < all_groupby_exprs_.count(); ++i) {
      ObExpr *expr = all_groupby_exprs_.at(i); // expr ptr check in cg, not check here
      if (OB_INVALID_INDEX_INT64 != MY_SPEC.aggr_code_idx_ && i == MY_SPEC.aggr_code_idx_) {
        // aggr_code column, then skip, set values in process duplicate data
        continue;
      } else if (OB_FAIL(expr->eval_vector(eval_ctx_, child_brs))) {
        LOG_WARN("eval failed", K(ret));
      } else if (ObThreeStageAggrStage::FIRST_STAGE == MY_SPEC.aggr_stage_ && i > MY_SPEC.aggr_code_idx_) {
        // distinct old value
        int64_t dup_idx = i - MY_SPEC.aggr_code_idx_ - 1;
        if (dup_idx >= MY_SPEC.org_dup_cols_.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected status: duplicate distinct column is invalid", K(dup_idx), K(i),
                   K(MY_SPEC.aggr_code_idx_), K(ret));
        } else if (OB_FAIL(MY_SPEC.org_dup_cols_.at(dup_idx)->eval_vector(
                    eval_ctx_, child_brs))) {
          LOG_WARN("eval failed", K(ret));
        } else {
          LOG_DEBUG("org dup col evaluated", K(i), K(*MY_SPEC.org_dup_cols_.at(dup_idx)), KP(MY_SPEC.org_dup_cols_.at(dup_idx)));
        }
      }
    }
  } else {
    // In dumped partition
    // for for performance, batch eval group_expr here, then we get datum derectly when compare
    for (int64_t i = 0; OB_SUCC(ret) && i < all_groupby_exprs_.count(); ++i) {
      ObExpr *expr = all_groupby_exprs_.at(i); // expr ptr check in cg, not check here
      if (OB_INVALID_INDEX_INT64 != MY_SPEC.aggr_code_idx_ && i == MY_SPEC.aggr_code_idx_) {
        // aggr_code column, then skip, set values in process duplicate data
        continue;
      } else if (OB_FAIL(expr->eval_vector(eval_ctx_, child_brs))) {
        LOG_WARN("eval failed", K(ret));
      } else if (ObThreeStageAggrStage::FIRST_STAGE == MY_SPEC.aggr_stage_ && i > MY_SPEC.aggr_code_idx_) {
        // distinct old value
        int64_t dup_idx = i - MY_SPEC.aggr_code_idx_ - 1;
        if (dup_idx >= MY_SPEC.org_dup_cols_.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected status: duplicate distinct column is invalid", K(dup_idx), K(i),
                   K(MY_SPEC.aggr_code_idx_), K(ret));
        } else if (OB_FAIL(MY_SPEC.org_dup_cols_.at(dup_idx)->eval_vector(
                    eval_ctx_, child_brs))) {
          LOG_WARN("eval failed", K(ret));
        }
      }
    }
    OB_ASSERT(OB_NOT_NULL(meta));
    for (int64_t i = 0; i < child_brs.size_; i++) {
      OB_ASSERT(OB_NOT_NULL(store_rows[i]));
      hash_vals_[i] = *static_cast<uint64_t *>(store_rows[i]->get_extra_payload(*meta));
    }
  }
  return ret;
}

int ObHashGroupByVecOp::batch_process_duplicate_data(
  const ObCompactRow **store_rows,
  const RowMeta *meta,
  const int64_t input_rows,
  const bool check_dump,
  const int64_t part_id,
  const int64_t part_shift,
  const int64_t loop_cnt,
  const ObBatchRows &child_brs,
  int64_t &part_cnt,
  DatumStoreLinkPartition **parts,
  int64_t &est_part_cnt,
  ObGbyBloomFilterVec *&bloom_filter,
  bool &process_check_dump)
{
  int ret = OB_SUCCESS;
  int64_t nth_dup_data = 0;
  bool last_group = false;
  bool insert_group_ht = false;
  aggregate::AggrRowPtr exist_group_row = NULL;
  bool tmp_check_dump = false;
  bool force_check_dump = check_dump;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
  batch_info_guard.set_batch_size(child_brs.size_);
  MEMSET(is_dumped_, 0, sizeof(bool) * child_brs.size_);
  aggregate::AggrRowPtr new_row = nullptr;
  do {
    LOG_DEBUG("batch process duplicate data", K(store_rows), K(input_rows), K(child_brs),
             K(use_distinct_data_));
    // firstly process duplicate data
    if (OB_FAIL(next_duplicate_data_permutation(nth_dup_data, last_group, &child_brs, insert_group_ht))) {
      LOG_WARN("failed to get next duplicate data purmutation", K(ret));
    } else if (last_group) {
      // last group is origin data, process later
    } else {
      // TODO: because groupby expr add distinct exprs, so the group count may exceed the input rows
      //       and evenly several times, so we should implement don't calculate aggregate functions
      //       and output the duplicate data
      int64_t new_group_cnt = 0;
      memset(static_cast<void *> (batch_old_rows_), 0, sizeof(char *) * MY_SPEC.max_batch_size_);
      memset(static_cast<void *> (batch_new_rows_), 0, sizeof(char *) * MY_SPEC.max_batch_size_);
      if (nullptr == store_rows && !local_group_rows_.is_sstr_aggr_valid()) {
        ret = calc_groupby_exprs_hash_batch(dup_groupby_exprs_, child_brs);
        local_group_rows_.prefetch(child_brs, hash_vals_);
      }
      bool can_append_batch = (NULL == bloom_filter
                              && (!enable_dump_
                                || local_group_rows_.is_sstr_aggr_valid()
                                || local_group_rows_.size() < MIN_INMEM_GROUPS
                                || process_check_dump
                                || !need_start_dump(input_rows, est_part_cnt, force_check_dump)));
      int64_t orign_agged_group_cnt = agged_group_cnt_;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(local_group_rows_.process_batch(dup_groupby_exprs_,
                                                  child_brs,
                                                  is_dumped_,
                                                  hash_vals_,
                                                  group_expr_fixed_lengths_,
                                                  can_append_batch,
                                                  bloom_filter,
                                                  batch_old_rows_,
                                                  batch_new_rows_,
                                                  agged_row_cnt_,
                                                  agged_group_cnt_,
                                                  nullptr,
                                                  true))) {
        LOG_WARN("failed to get batch rows", K(ret));
      } else if (!can_append_batch) {
        if (OB_UNLIKELY(NULL == bloom_filter)) {
          if (OB_FAIL(setup_dump_env(part_id, max(input_rows, loop_cnt), parts, part_cnt,
                                    bloom_filter))) {
            LOG_WARN("setup dump environment failed", K(ret));
          } else {
            sql_mem_processor_.set_number_pass(part_id + 1);
          }
        }
        // need dump
        if (1 != nth_dup_data) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected status: dump is not expected", K(ret));
        } else if (part_cnt <= MAX_BATCH_DUMP_PART_CNT) {
          reuse_dump_selectors();
          for (int64_t i = 0; OB_SUCC(ret) && i < child_brs.size_; i++) {
            if (child_brs.skip_->exist(i)
                || nullptr != batch_new_rows_[i]
                || nullptr != batch_old_rows_[i]) {
              continue;
            }
            is_dumped_[i] = true;
            ++agged_dumped_cnt_;
            const int64_t part_idx = (hash_vals_[i] >> part_shift) & (part_cnt - 1);
            dump_add_row_selectors_[part_idx][dump_add_row_selectors_item_cnt_[part_idx]++] = i;
          }
          for (int64_t i = 0; OB_SUCC(ret) && i < part_cnt ; i++) {
            if (OB_FAIL(parts[i]->row_store_.add_batch(dump_vectors_, dump_add_row_selectors_[i],
                                        dump_add_row_selectors_item_cnt_[i], dump_rows_, nullptr))) {
              LOG_WARN("add row batch failed", K(ret));
            } else {
              // set dump rows hash_val
              for (int64_t j = 0; j < dump_add_row_selectors_item_cnt_[i]; j++) {
                *static_cast<uint64_t *>(dump_rows_[j][0].get_extra_payload(parts[i]->row_store_.get_row_meta())) =
                  hash_vals_[dump_add_row_selectors_[i][j]];
              }
            }
          }
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < child_brs.size_; ++i) {
            if (child_brs.skip_->exist(i)
                || nullptr != batch_new_rows_[i]
                || nullptr != batch_old_rows_[i]) {
              continue;
            }
            is_dumped_[i] = true;
            ++agged_dumped_cnt_;
            const int64_t part_idx = (hash_vals_[i] >> part_shift) & (part_cnt - 1);
            ObCompactRow *stored_row = nullptr;
            if (OB_FAIL(parts[part_idx]->row_store_.add_row(child_->get_spec().output_,
                                                            i,
                                                            eval_ctx_,
                                                            stored_row))) {
              LOG_WARN("add row failed", K(ret));
            } else {
              *static_cast<uint64_t *>(stored_row[0].get_extra_payload(parts[part_idx]->row_store_.get_row_meta())) = hash_vals_[i];
            }
            LOG_DEBUG("finish dump", K(part_idx), K(agged_dumped_cnt_), K(agged_row_cnt_),
                                    K(parts[part_idx]->row_store_.get_row_cnt()), K(i));
          }
        }
      }
      force_check_dump = false;
      new_group_cnt = agged_group_cnt_ - orign_agged_group_cnt;
      if (OB_SUCC(ret) && llc_est_.enabled_) {
        for (int64_t i = 0; OB_SUCC(ret) && i < child_brs.size_; i++) {
          if (batch_new_rows_[i]) {
            llc_add_value(hash_vals_[i]);
          }
        }
      }
      if (OB_SUCC(ret) && 0 < new_group_cnt) {
        int64_t curr_cnt = 0;
        for (int64_t i = 0; OB_SUCC(ret) && i < child_brs.size_; i++) {
          if (!batch_new_rows_[i]) {
            continue;
          }
          if (OB_FAIL(alloc_group_row(local_group_rows_.size() - new_group_cnt + curr_cnt,
                                      batch_new_rows_[i]))) {
            LOG_WARN("alloc group row failed", K(ret));
          }
          ++curr_cnt;
        }
      }
      process_check_dump = true;
    }
  } while (!last_group && OB_SUCC(ret));
  return ret;
}

int ObHashGroupByVecOp::batch_insert_distinct_data(const ObBatchRows &child_brs)
{
  int ret = OB_SUCCESS;
  ObBitVector *output_vec = nullptr;
  ObBatchRows tmp_brs;
  tmp_brs.size_ = child_brs.size_;
  tmp_brs.skip_ = distinct_skip_;
  if (!is_init_distinct_data_ && OB_FAIL(init_distinct_info(false))) {
    LOG_WARN("failed to init hash partition infras", K(ret));
  } else if (OB_FAIL(distinct_data_set_.calc_hash_value_for_batch(distinct_origin_exprs_,
                                                                   tmp_brs,
                                                                  //  distinct_skip_,
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

int ObHashGroupByVecOp::batch_insert_all_distinct_data(const int64_t batch_size)
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

int ObHashGroupByVecOp::get_next_batch_distinct_rows(
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

void ObHashGroupByVecOp::reuse_dump_selectors() {
  if (nullptr != dump_add_row_selectors_item_cnt_) {
    memset(dump_add_row_selectors_item_cnt_, 0, sizeof(uint16_t) * MAX_BATCH_DUMP_PART_CNT);
  }
}

int ObHashGroupByVecOp::group_child_batch_rows(const ObCompactRow **store_rows,
                                               const RowMeta *meta,
                                               const int64_t input_rows,
                                               const bool check_dump,
                                               const int64_t part_id,
                                               const int64_t part_shift,
                                               const int64_t loop_cnt,
                                               const ObBatchRows &child_brs,
                                               int64_t &part_cnt,
                                               DatumStoreLinkPartition **parts,
                                               int64_t &est_part_cnt,
                                               ObGbyBloomFilterVec *&bloom_filter)
{
  int ret = OB_SUCCESS;
  aggregate::AggrRowPtr exist_group_row = NULL;
  aggregate::AggrRowPtr new_row = nullptr;
  LOG_DEBUG("group child batch", K(child_brs), K(part_id), K(agged_dumped_cnt_), K(agged_row_cnt_),
            K(agged_group_cnt_), K(local_group_rows_.size()), K(MY_SPEC.aggr_stage_));
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
  batch_info_guard.set_batch_size(child_brs.size_);
  bool process_check_dump = false;
  bool force_check_dump = check_dump;
  int64_t aggr_code = -1;
  ObExpr *aggr_code_expr = MY_SPEC.aggr_code_expr_;
  int64_t distinct_data_idx = 0;
  ObIVector *aggr_code_vec = nullptr;
  // mem prefetch for hashtable
  if (OB_FAIL(eval_groupby_exprs_batch(store_rows, meta, child_brs))) {
    LOG_WARN("fail to calc groupby exprs hash batch", K(ret));
  } else if (OB_FAIL(batch_process_duplicate_data(store_rows, meta, input_rows, force_check_dump, part_id,
      part_shift, loop_cnt, child_brs, part_cnt, parts, est_part_cnt, bloom_filter,
      process_check_dump))) {
    LOG_WARN("failed to batch process duplicate data", K(ret));
  } else if (no_non_distinct_aggr_) {
    LOG_TRACE("no distinct aggr");
    // no groupby exprs, don't calculate the last duplicate data for non-distinct aggregate
  } else if ((ObThreeStageAggrStage::SECOND_STAGE == MY_SPEC.aggr_stage_ && !use_distinct_data_)
              && OB_FAIL(aggr_code_expr->eval_vector(eval_ctx_, child_brs))) {
    LOG_WARN("failed to eval aggr code", K(ret));
  } else if ((ObThreeStageAggrStage::SECOND_STAGE == MY_SPEC.aggr_stage_ && !use_distinct_data_)
              && FALSE_IT(aggr_code_vec = aggr_code_expr->get_vector(eval_ctx_))) {
  } else {
    if (nullptr == store_rows && !local_group_rows_.is_sstr_aggr_valid()) {
      ret = calc_groupby_exprs_hash_batch(dup_groupby_exprs_, child_brs);
      local_group_rows_.prefetch(child_brs, hash_vals_);
    }
    uint16_t new_groups = 0;
    memset(static_cast<void *>(batch_old_rows_), 0,
           sizeof(aggregate::AggrRowPtr *) * MY_SPEC.max_batch_size_);
    memset(static_cast<void *>(batch_new_rows_), 0,
           sizeof(aggregate::AggrRowPtr *) * MY_SPEC.max_batch_size_);
    if (reorder_aggr_rows_) {
      batch_aggr_rows_table_.reuse();
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < child_brs.size_; i++) {
      if (child_brs.skip_->exist(i) || is_dumped_[i]) {
        if (nullptr != distinct_skip_) {
          distinct_skip_->set(i);
        }
        continue;
      } else {
        if (ObThreeStageAggrStage::SECOND_STAGE == MY_SPEC.aggr_stage_ && !use_distinct_data_) {
          aggr_code = aggr_code_vec->get_int(i);
          if (aggr_code < MY_SPEC.dist_aggr_group_idxes_.count()) {
            ++distinct_data_idx;
            // don't process
            child_brs.skip_->set(i);
            continue;
          } else {
            distinct_skip_->set(i);
          }
        }
      }
    }
    bool can_append_batch = (NULL == bloom_filter
                            && (!enable_dump_
                              || local_group_rows_.is_sstr_aggr_valid()
                              || local_group_rows_.size() < MIN_INMEM_GROUPS
                              || process_check_dump
                              || !need_start_dump(input_rows, est_part_cnt, force_check_dump)));
    int64_t orign_agged_group_cnt = agged_group_cnt_;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(local_group_rows_.process_batch(dup_groupby_exprs_,
                                                child_brs,
                                                is_dumped_,
                                                hash_vals_,
                                                group_expr_fixed_lengths_,
                                                can_append_batch,
                                                bloom_filter,
                                                batch_old_rows_,
                                                batch_new_rows_,
                                                agged_row_cnt_,
                                                agged_group_cnt_,
                                                reorder_aggr_rows_
                                                  ? &batch_aggr_rows_table_ : nullptr,
                                                true))) {
      LOG_WARN("failed to get batch rows", K(ret));
    } else if (!can_append_batch) {
      // need dump
      if (OB_UNLIKELY(NULL == bloom_filter)) {
        if (OB_FAIL(setup_dump_env(part_id, max(input_rows, loop_cnt), parts, part_cnt,
                                  bloom_filter))) {
          LOG_WARN("setup dump environment failed", K(ret));
        } else {
          sql_mem_processor_.set_number_pass(part_id + 1);
        }
      }
      if (process_check_dump) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected status: check dump is processed", K(ret));
      } else if (part_cnt <= MAX_BATCH_DUMP_PART_CNT) {
        reuse_dump_selectors();
        for (int64_t i = 0; OB_SUCC(ret) && i < child_brs.size_; i++) {
          if (child_brs.skip_->exist(i)
              || is_dumped_[i]
              || nullptr != batch_new_rows_[i]
              || nullptr != batch_old_rows_[i]) {
            continue;
          }
          ++agged_dumped_cnt_;
          const int64_t part_idx = (hash_vals_[i] >> part_shift) & (part_cnt - 1);
          dump_add_row_selectors_[part_idx][dump_add_row_selectors_item_cnt_[part_idx]++] = i;
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < part_cnt ; i++) {
          if (OB_FAIL(parts[i]->row_store_.add_batch(dump_vectors_, dump_add_row_selectors_[i],
                                                    dump_add_row_selectors_item_cnt_[i], dump_rows_, nullptr))) {
            LOG_WARN("add row batch failed", K(ret));
          } else {
            // set dump rows hash_val
            for (int64_t j = 0; j < dump_add_row_selectors_item_cnt_[i]; j++) {
              *static_cast<uint64_t *>(dump_rows_[j][0].get_extra_payload(parts[i]->row_store_.get_row_meta())) =
                                                                                          hash_vals_[dump_add_row_selectors_[i][j]];
            }
          }
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < child_brs.size_; i++) {
          if (child_brs.skip_->exist(i)
              || is_dumped_[i]
              || nullptr != batch_new_rows_[i]
              || nullptr != batch_old_rows_[i]) {
            continue;
          }
          if (OB_SUCC(ret)) {
            ++agged_dumped_cnt_;
            const int64_t part_idx = (hash_vals_[i] >> part_shift) & (part_cnt - 1);
            ObCompactRow *stored_row = nullptr;
            if (OB_FAIL(parts[part_idx]->row_store_.add_row(child_->get_spec().output_,
                                                            i,
                                                            eval_ctx_,
                                                            stored_row))) {
              LOG_WARN("add row failed", K(ret));
            } else {
              *static_cast<uint64_t *>(stored_row[0].get_extra_payload(parts[part_idx]->row_store_.get_row_meta())) = hash_vals_[i];
            }
            LOG_DEBUG("finish dump", K(part_idx), K(agged_dumped_cnt_), K(agged_row_cnt_),
                                    K(parts[part_idx]->row_store_.get_row_cnt()), K(i));
          }
        }
      }
    }
    force_check_dump = false;
    new_groups = agged_group_cnt_ - orign_agged_group_cnt;
    if (OB_SUCC(ret) && llc_est_.enabled_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < child_brs.size_; i++) {
        if (batch_new_rows_[i]) {
          llc_add_value(hash_vals_[i]);
        }
      }
    }
    if (OB_SUCC(ret) && new_groups > 0) {
      int64_t curr_cnt = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < child_brs.size_; ++i) {
        if (!batch_new_rows_[i]) {
          continue;
        }
        if (OB_FAIL(alloc_group_row(local_group_rows_.size() - new_groups + curr_cnt, batch_new_rows_[i]))) {
          LOG_WARN("alloc group row failed", K(ret));
        }
        ++curr_cnt;
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

int ObHashGroupByVecOp::init_by_pass_group_batch_item()
{
  int ret = OB_SUCCESS;
  void *by_pass_buf = nullptr;
  if (by_pass_batch_size_ <= 0) {
    if (OB_ISNULL(by_pass_buf = static_cast<char *>(mem_context_->get_arena_allocator().alloc(
                    sizeof(aggregate::AggrRowPtr *) * MY_SPEC.max_batch_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", K(ret));
    } else {
      by_pass_batch_size_ = MY_SPEC.max_batch_size_;
      by_pass_group_batch_ = static_cast<aggregate::AggrRowPtr *>(by_pass_buf);
    }
  }
  CK(by_pass_batch_size_ > 0);
  if (OB_FAIL(ret)) {
  } else if (aggr_processor_.aggregates_cnt() <= 0) { // do nothing
  } else if (OB_FAIL(
               aggr_processor_.generate_group_rows(by_pass_group_batch_, by_pass_batch_size_))) {
    SQL_LOG(WARN, "generate by pass group rows failed", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < by_pass_batch_size_; i++) {
      if (OB_ISNULL(by_pass_group_batch_[i])) {
        ret = OB_ERR_UNEXPECTED;
        SQL_LOG(WARN, "unexpected null group row", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObHashGroupByVecOp::by_pass_prepare_one_batch(const int64_t batch_size)
{
  int ret = OB_SUCCESS;
  bool last_group = false;
  bool insert_group_ht = false;
  LOG_TRACE("by pass prepare one batch", K(batch_size));
  if (ObThreeStageAggrStage::FIRST_STAGE == MY_SPEC.aggr_stage_
      && by_pass_nth_group_ <= MY_SPEC.dist_col_group_idxs_.count()
      && by_pass_nth_group_ > 0) {
    //consume curr batch
    if (OB_FAIL(by_pass_vec_holder_.restore())) {
      LOG_WARN("failed to restore child batch", K(ret));
    } else if (OB_FAIL(by_pass_get_next_permutation_batch(by_pass_nth_group_, last_group,
                                                          by_pass_child_brs_, brs_, batch_size,
                                                          insert_group_ht))) {
      LOG_WARN("failed to get next permutation row", K(ret));
    }
  } else if (FALSE_IT(by_pass_nth_group_ = 0)) {
  } else if (OB_FAIL(by_pass_vec_holder_.restore())) {
    LOG_WARN("failed to restore child batch", K(ret));
  } else if (OB_FAIL(child_->get_next_batch(batch_size, by_pass_child_brs_))) {
    LOG_WARN("fail to get next batch", K(ret));
  } else if (by_pass_child_brs_->end_) {
    brs_.size_ = 0;
    brs_.end_ = true;
    by_pass_vec_holder_.reset();
  } else if (OB_FAIL(by_pass_vec_holder_.save(by_pass_child_brs_->size_))) {
    LOG_WARN("failed to save child batch", K(ret));
  } else if (OB_FAIL(try_check_status())) {
    LOG_WARN("check status failed", K(ret));
  } else if (OB_FAIL(by_pass_get_next_permutation_batch(
               by_pass_nth_group_, last_group, by_pass_child_brs_, brs_, batch_size, insert_group_ht))) {
    LOG_WARN("failed to get next permutation row", K(ret));
  }
  if (OB_SUCC(ret) && llc_est_.enabled_) {
    const ObCompactRow **store_rows = NULL;
    const RowMeta *meta = NULL;
    if (OB_FAIL(eval_groupby_exprs_batch(store_rows, meta, brs_))) {
      LOG_WARN("fail to calc groupby exprs hash batch", K(ret));
    } else if (OB_FAIL(calc_groupby_exprs_hash_batch(dup_groupby_exprs_, brs_))) {
      LOG_WARN("failed to calc groupby expr has", K(ret), K(dup_groupby_exprs_));
    } else if (OB_FAIL(bypass_add_llc_map_batch(ObThreeStageAggrStage::FIRST_STAGE == MY_SPEC.aggr_stage_ ?
                                                by_pass_nth_group_ > MY_SPEC.dist_col_group_idxs_.count() : true))) {
      LOG_WARN("failed to add llc map batch", K(ret));
    }
  }
  if (OB_FAIL(ret) || no_non_distinct_aggr_) {
    if (last_group && no_non_distinct_aggr_) {
      // in bypass && last_group && no_non_distinct_aggr_, brs_.skip_ will be set_all (all skip)
      brs_.all_rows_active_ = false;
    } else {
      brs_.all_rows_active_ = by_pass_child_brs_->all_rows_active_;
    }
  } else if (OB_UNLIKELY(by_pass_batch_size_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("by pass group row is not init", K(ret), K(by_pass_batch_size_));
  } else if (OB_FAIL(aggr_processor_.eval_aggr_param_batch(brs_))) {
    LOG_WARN("fail to eval aggr param batch", K(ret), K(brs_));
  } else if (OB_ISNULL(brs_.skip_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_LOG(WARN, "unexpected null skip", K(ret));
  } else {
    if (OB_FAIL(aggr_processor_.single_row_agg_batch(by_pass_group_batch_,
                                                     by_pass_child_brs_->size_, eval_ctx_,
                                                     *by_pass_child_brs_->skip_))) {
      LOG_WARN("failed to single row agg", K(ret));
    } else {
      int64_t aggr_cnt = brs_.size_ - brs_.skip_->accumulate_bit_cnt(by_pass_child_brs_->size_);
      brs_.all_rows_active_ = (aggr_cnt == brs_.size_);
      agged_row_cnt_ += aggr_cnt;
      agged_group_cnt_ += aggr_cnt;
    }
  }
  return ret;
}

int ObHashGroupByVecOp::by_pass_get_next_permutation_batch(int64_t &nth_group, bool &last_group,
                                                           const ObBatchRows *child_brs,
                                                           ObBatchRows &my_brs,
                                                           const int64_t batch_size,
                                                           bool &insert_group_ht)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(child_brs));
  OX(my_brs.size_ = child_brs->size_);
  OX(my_brs.skip_->deep_copy(*child_brs->skip_, child_brs->size_));
  if (OB_FAIL(ret)) {
  } else if (ObThreeStageAggrStage::NONE_STAGE == MY_SPEC.aggr_stage_) {
    last_group = true;
  } else if (OB_FAIL(next_duplicate_data_permutation(nth_group, last_group, child_brs,
                                                     insert_group_ht))) {
    LOG_WARN("failed to get next permutation", K(ret));
  } else if (no_non_distinct_aggr_ && last_group) {
    my_brs.skip_->set_all(child_brs->size_);
  } else {
    CK (dup_groupby_exprs_.count() == all_groupby_exprs_.count());
    LOG_DEBUG("next duplicate data permutation", K(all_groupby_exprs_), K(dup_groupby_exprs_));
    for (int64_t i = 0; OB_SUCC(ret) && i < dup_groupby_exprs_.count(); ++i) {
      if (nullptr == dup_groupby_exprs_.at(i)) {
        if (OB_FAIL(all_groupby_exprs_.at(i)->init_vector_for_write(
              eval_ctx_, all_groupby_exprs_.at(i)->get_default_res_format(), batch_size))) {
          LOG_WARN("init vector failed", K(ret));
        } else {
          ObIVector *col_vec = all_groupby_exprs_.at(i)->get_vector(eval_ctx_);
          for (int64_t j = 0; j < child_brs->size_; ++j) {
            if (child_brs->skip_->at(j)) { continue; }
            col_vec->set_null(j);
          }
          all_groupby_exprs_.at(i)->set_evaluated_projected(eval_ctx_);
        }
      } else if (OB_FAIL(dup_groupby_exprs_.at(i)->eval_vector(eval_ctx_, *child_brs))) {
        LOG_WARN("failed to eval dup exprs", K(ret), K(i));
      } else if (all_groupby_exprs_.at(i) == dup_groupby_exprs_.at(i)) {
      } else if (OB_FAIL(all_groupby_exprs_.at(i)->init_vector_for_write(
                   eval_ctx_, all_groupby_exprs_.at(i)->get_default_res_format(), batch_size))) {
        // all_groupby_exprs_.at(i) is default format is used here
        // consider `count(distinct 1) from t1 group by c1`
        // stage 1 groupby exprs will be `<c1, aggr_code, dup(1)>`
        // `dup(1)` is mocked column with batch results, not static const.
        // Therefore dup_groupby_exprs.at(i)'s format, which is uniform_const, should not be used
        // here.
        LOG_WARN("init vector for write failed", K(ret));
      } else {
        ObIVector *col_vec = all_groupby_exprs_.at(i)->get_vector(eval_ctx_);
        ObIVector *dup_col_vec = dup_groupby_exprs_.at(i)->get_vector(eval_ctx_);
        for (int64_t j = 0; j < child_brs->size_; ++j) {
          if (child_brs->skip_->at(j)) {
            continue;
          } else if (dup_col_vec->is_null(j)) {
            col_vec->set_null(j);
          } else {
            col_vec->set_payload_shallow(j, dup_col_vec->get_payload(j), dup_col_vec->get_length(j));
          }
        }
        all_groupby_exprs_.at(i)->set_evaluated_projected(eval_ctx_);
      }
    }
  }
  return ret;
}

int ObHashGroupByVecOp::init_by_pass_op()
{
  int ret = OB_SUCCESS;
  int err_sim = 0;
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
  if (aggr_processor_.support_fast_single_row_agg()) {
    if (OB_FAIL(aggr_processor_.init_fast_single_row_aggs())) {
      LOG_WARN("init fast aggregates failed", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(mem_context_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("failed to get mem context", K(ret));
  } else if (OB_FAIL(ctx_.get_my_session()
                            ->get_sys_variable(share::SYS_VAR__GROUPBY_NOPUSHDOWN_CUT_RATIO,
                                                                    cut_ratio))) {
    LOG_WARN("failed to get no pushdown cut ratio", K(ret));
  } else if (OB_FAIL(init_by_pass_group_batch_item())) {
    LOG_WARN("failed to init by pass group batch item", K(ret));
  } else if (MY_SPEC.max_batch_size_ > 0
              && OB_FAIL(by_pass_vec_holder_.init(child_->get_spec().output_, eval_ctx_))) {
    LOG_WARN("failed to init brs holder", K(ret));
  } else {
    LOG_TRACE("check by pass", K(MY_SPEC.id_), K(MY_SPEC.by_pass_enabled_), K(bypass_ctrl_.get_small_row_cnt()),
                               K(get_actual_mem_used_size()), K(INIT_L2_CACHE_SIZE), K(INIT_L3_CACHE_SIZE));
    // to avoid performance decrease, at least deduplicate 2/3
    bypass_ctrl_.set_cut_ratio(std::max(cut_ratio, default_cut_ratio));
    bypass_ctrl_.set_op_id(MY_SPEC.id_);
  }
  return ret;
}

int ObHashGroupByVecOp::by_pass_restart_round()
{
  int ret = OB_SUCCESS;
  int64_t est_group_cnt = MY_SPEC.est_group_cnt_;
  int64_t est_hash_mem_size = 0;
  int64_t estimate_mem_size = 0;
  curr_group_id_ = 0;
  cur_group_item_idx_ = 0;
  cur_group_item_buf_ = nullptr;
  aggr_processor_.reuse();
  // if last round is in L2 cache, reuse the bucket
  // otherwise resize to init size to avoid L2 cache overflow
  if (bypass_ctrl_.need_resize_hash_table_) {
    OZ(local_group_rows_.resize(&mem_context_->get_malloc_allocator(), INIT_BKT_SIZE_FOR_ADAPTIVE_GBY));
  } else {
    OX(local_group_rows_.reuse());
  }
  OZ(reinit_group_store());
  if (OB_SUCC(ret)) {
    bypass_ctrl_.reset_state();
    bypass_ctrl_.inc_rebuild_times();
    bypass_ctrl_.need_resize_hash_table_ = false;
    by_pass_vec_holder_.restore();
  }
  return ret;
}

int64_t ObHashGroupByVecOp::get_input_rows() const
{
  int64_t res = child_->get_spec().rows_;
  if (ObThreeStageAggrStage::FIRST_STAGE == MY_SPEC.aggr_stage_) {
    res = std::max(res, res * (MY_SPEC.dist_col_group_idxs_.count() + 1));
  }
  return res;
}

int64_t ObHashGroupByVecOp::get_input_size() const
{
  int64_t res = child_->get_spec().rows_ * child_->get_spec().width_;
  if (ObThreeStageAggrStage::FIRST_STAGE == MY_SPEC.aggr_stage_) {
    res = std::max(res, res * (MY_SPEC.dist_col_group_idxs_.count() + 1));
  }
  return res;
}

void ObHashGroupByVecOp::check_groupby_exprs(const ObIArray<ObExpr *> &groupby_exprs,
                                             bool &nullable,
                                             bool &all_int64)
{
  nullable = true; //TODO : close nullable check default
  all_int64 = true;
  for (int64_t i = 0; i < groupby_exprs.count(); ++i) {
    if (nullable || (groupby_exprs.at(i)->nullable_
                     && T_INNER_AGGR_CODE != groupby_exprs.at(i)->type_
                     && T_PSEUDO_DUP_EXPR != groupby_exprs.at(i)->type_)) {
      nullable = true;
    }
    int64_t dup_idx = i - MY_SPEC.aggr_code_idx_ - 1;
    if (nullable) {
    } else if (T_INNER_AGGR_CODE == groupby_exprs.at(i)->type_) {
    } else if (T_PSEUDO_DUP_EXPR == groupby_exprs.at(i)->type_
               && MY_SPEC.org_dup_cols_.at(dup_idx)->nullable_) {
      nullable = true;
    }
    if (all_int64 && !(groupby_exprs.at(i)->is_fixed_length_data_
                       && 8 == groupby_exprs.at(i)->res_buf_len_)) {
      all_int64 = false;
    }
  }
}

int ObHashGroupByVecOp::check_llc_ndv()
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

int ObHashGroupByVecOp::bypass_add_llc_map_batch(bool ready_to_check_ndv) {
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && llc_est_.enabled_ && i < brs_.size_; ++i) {
    if (brs_.skip_->exist(i)) { continue; }
    llc_add_value(hash_vals_[i]);
  }
  if ((llc_est_.est_cnt_ - llc_est_.last_est_cnt_) > LlcEstimate::ESTIMATE_MOD_NUM_ &&
       ready_to_check_ndv && OB_FAIL(check_llc_ndv())) {
    LOG_WARN("failed to check llc ndv", K(ret));
  }
  return ret;
}

int ObHashGroupByVecOp::process_multi_groups(aggregate::AggrRowPtr *agg_rows, const ObBatchRows &brs)
{
  int ret = OB_SUCCESS;
  int32_t start_agg_id = -1, end_agg_id = -1;
  for (int i = 0; OB_SUCC(ret) && i < brs.size_; i++) {
    if ((!agg_rows[i])) { continue; }
    if (OB_FAIL(ObGroupByVecOp::calculate_3stage_agg_info(
          agg_rows[i], local_group_rows_.get_row_meta(), i, start_agg_id, end_agg_id))) {
      LOG_WARN("calculate 3stage aggregate info failed", K(ret));
    } else if (OB_UNLIKELY(start_agg_id == -1 || end_agg_id == -1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected aggregate idx", K(ret), K(start_agg_id), K(end_agg_id));
    } else {
      break;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(aggr_processor_.add_batch_for_multi_groups(start_agg_id, end_agg_id, agg_rows,
                                                                brs.size_))) {
    LOG_WARN("failed to calculate multiple groups", K(ret));
  }
  return ret;
}

int LlcEstimate::init_llc_map(common::ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  char *llc_map;
  int64_t llc_map_size;
  if (OB_FAIL(ObAggregateProcessor::llc_init_empty(llc_map, llc_map_size, allocator))) {
    LOG_WARN("failed to init llc map", K(ret));
  } else {
    llc_map_.assign_ptr(llc_map, llc_map_size);
  }
  return ret;
}

int LlcEstimate::reset()
{
  int ret = OB_SUCCESS;
  if (!llc_map_.empty()) {
    MEMSET(llc_map_.ptr(), 0, llc_map_.length());
  }
  avg_group_mem_ = 0;
  est_cnt_ = 0;
  last_est_cnt_ = 0;
  enabled_ = false;
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
