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

namespace oceanbase {
using namespace common;
using namespace common::hash;

namespace sql {

OB_SERIALIZE_MEMBER((ObHashGroupBySpec, ObGroupBySpec), group_exprs_, cmp_funcs_, est_group_cnt_);

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

int ObHashGroupBySpec::add_group_expr(ObExpr* expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FAIL(group_exprs_.push_back(expr))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("failed to push_back expr");
  }
  return ret;
}

void ObHashGroupByOp::reset()
{
  curr_group_id_ = common::OB_INVALID_INDEX;
  local_group_rows_.reuse();
  sql_mem_processor_.reset();
  destroy_all_parts();
  group_store_.reset();
}

int ObHashGroupByOp::inner_open()
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(ObGroupByOp::inner_open())) {
    LOG_WARN("failed to inner_open", K(ret));
  } else if (OB_FAIL(init_mem_context())) {
    LOG_WARN("init memory entity failed", K(ret));
  } else if (!local_group_rows_.is_inited()) {
    // create bucket
    int64_t est_group_cnt = MY_SPEC.est_group_cnt_;
    int64_t est_hash_mem_size = 0;
    int64_t estimate_mem_size = 0;
    int64_t init_size = 0;
    ObMemAttr attr(
        ctx_.get_my_session()->get_effective_tenant_id(), ObModIds::OB_HASH_NODE_GROUP_ROWS, ObCtxIds::WORK_AREA);
    if (OB_FAIL(ObPxEstimateSizeUtil::get_px_size(
            &ctx_, MY_SPEC.px_est_size_factor_, MY_SPEC.est_group_cnt_, est_group_cnt))) {
      LOG_WARN("failed to get px size", K(ret));
    } else if (FALSE_IT(est_hash_mem_size = estimate_hash_bucket_size(est_group_cnt))) {
    } else if (FALSE_IT(estimate_mem_size = est_hash_mem_size + MY_SPEC.width_ * est_group_cnt)) {
    } else if (OB_FAIL(sql_mem_processor_.init(&ctx_.get_allocator(),
                   ctx_.get_my_session()->get_effective_tenant_id(),
                   estimate_mem_size,
                   MY_SPEC.type_,
                   MY_SPEC.id_,
                   &ctx_))) {
      LOG_WARN("failed to init sql mem processor", K(ret));
    } else if (FALSE_IT(init_size = estimate_hash_bucket_cnt_by_mem_size(est_group_cnt,
                            sql_mem_processor_.get_mem_bound(),
                            est_hash_mem_size * 1. / estimate_mem_size))) {
    } else if (FALSE_IT(init_size = std::max((int64_t)MIN_GROUP_HT_INIT_SIZE, init_size))) {
    } else if (FALSE_IT(init_size = std::min((int64_t)MAX_GROUP_HT_INIT_SIZE, init_size))) {
    } else if (OB_FAIL(local_group_rows_.init(
                   &mem_context_->get_malloc_allocator(), attr, &eval_ctx_, &MY_SPEC.cmp_funcs_, init_size))) {
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
    } else {
      group_store_.set_dir_id(sql_mem_processor_.get_dir_id());
      group_store_.set_callback(&sql_mem_processor_);
      group_store_.set_allocator(mem_context_->get_malloc_allocator());
      op_monitor_info_.otherstat_1_value_ = init_size;
      op_monitor_info_.otherstat_1_id_ = ObSqlMonitorStatIds::HASH_INIT_BUCKET_COUNT;
      LOG_TRACE("trace init hash table",
          K(init_size),
          K(MY_SPEC.est_group_cnt_),
          K(est_group_cnt),
          K(est_hash_mem_size),
          K(estimate_mem_size),
          K(profile_.get_expect_size()),
          K(profile_.get_cache_size()),
          K(sql_mem_processor_.get_mem_bound()));
    }
  }
  return ret;
}

int ObHashGroupByOp::inner_close()
{
  sql_mem_processor_.unregister_profile();
  curr_group_id_ = common::OB_INVALID_INDEX;
  return ObGroupByOp::inner_close();
}

int ObHashGroupByOp::init_mem_context()
{
  int ret = OB_SUCCESS;
  if (NULL == mem_context_) {
    lib::ContextParam param;
    param.set_mem_attr(
        ctx_.get_my_session()->get_effective_tenant_id(), ObModIds::OB_HASH_NODE_GROUP_ROWS, ObCtxIds::WORK_AREA);
    if (OB_FAIL(CURRENT_CONTEXT.CREATE_CONTEXT(mem_context_, param))) {
      LOG_WARN("memory entity create failed", K(ret));
    }
  }
  return ret;
}

void ObHashGroupByOp::destroy()
{
  local_group_rows_.destroy();
  sql_mem_processor_.destroy();
  if (NULL != mem_context_) {
    destroy_all_parts();
  }
  group_store_.reset();
  ObGroupByOp::destroy();
  if (NULL != mem_context_) {
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = NULL;
  }
}

int ObHashGroupByOp::switch_iterator()
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(ObGroupByOp::switch_iterator())) {
    LOG_WARN("failed to rescan", K(ret));
  }
  return ret;
}

int ObHashGroupByOp::rescan()
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_FAIL(ObGroupByOp::rescan())) {
    LOG_WARN("failed to rescan", K(ret));
  } else {
    iter_end_ = false;
  }
  return ret;
}

int ObHashGroupByOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("before inner_get_next_row",
      K(get_aggr_used_size()),
      K(get_aggr_used_size()),
      K(get_local_hash_used_size()),
      K(get_dumped_part_used_size()),
      K(get_dump_part_hold_size()),
      K(get_mem_used_size()),
      K(get_mem_bound_size()),
      K(agged_dumped_cnt_),
      K(agged_group_cnt_),
      K(agged_row_cnt_));
  if (iter_end_) {
    ret = OB_ITER_END;
  } else if (curr_group_id_ < 0) {
    if (OB_FAIL(load_data())) {
      LOG_WARN("load data failed", K(ret));
    } else {
      curr_group_id_ = 0;
    }
  } else {
    ++curr_group_id_;
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(curr_group_id_ >= local_group_rows_.size())) {
      if (dumped_group_parts_.is_empty()) {
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
    if (OB_FAIL(aggr_processor_.collect(curr_group_id_))) {
      LOG_WARN("failed to collect result", K(ret));
    } else if (OB_FAIL(restore_groupby_datum())) {
      LOG_WARN("failed to restore_groupby_datum", K(ret));
    }
  }
  LOG_DEBUG("after inner_get_next_row",
      K(get_aggr_used_size()),
      K(get_aggr_used_size()),
      K(get_local_hash_used_size()),
      K(get_dumped_part_used_size()),
      K(get_dump_part_hold_size()),
      K(get_mem_used_size()),
      K(get_mem_bound_size()),
      K(agged_dumped_cnt_),
      K(agged_group_cnt_),
      K(agged_row_cnt_));
  return ret;
}

int ObHashGroupByOp::load_data()
{
  int ret = OB_SUCCESS;
  ObChunkDatumStore::Iterator row_store_iter;
  DatumStoreLinkPartition* cur_part = NULL;
  int64_t part_id = 0;
  int64_t part_shift = sizeof(uint64_t) * CHAR_BIT / 2;  // low half bits for hash table lookup.
  int64_t input_rows = child_->get_spec().rows_;
  int64_t input_size = child_->get_spec().width_ * input_rows;
  static_assert(MAX_PARTITION_CNT <= (1 << (CHAR_BIT)), "max partition cnt is too big");

  if (!dumped_group_parts_.is_empty()) {
    aggr_processor_.reuse();
    sql_mem_processor_.reset();
    cur_part = dumped_group_parts_.remove_first();
    if (OB_ISNULL(cur_part)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("pop head partition failed", K(ret));
    } else if (OB_FAIL(row_store_iter.init(&cur_part->datum_store_))) {
      LOG_WARN("init chunk row store iterator failed", K(ret));
    } else {
      input_rows = cur_part->datum_store_.get_row_cnt();
      part_id = cur_part->part_id_;
      part_shift = part_shift + part_id * CHAR_BIT;
      input_size = cur_part->datum_store_.get_file_size();
      if (OB_FAIL(local_group_rows_.resize(&mem_context_->get_malloc_allocator(), max(2, input_rows)))) {
        LOG_WARN("failed to reuse extended hash table", K(ret));
      } else if (OB_FAIL(sql_mem_processor_.init(&ctx_.get_allocator(),
                     ctx_.get_my_session()->get_effective_tenant_id(),
                     input_size,
                     MY_SPEC.type_,
                     MY_SPEC.id_,
                     &ctx_))) {
        LOG_WARN("failed to init sql mem processor", K(ret));
      } else {
        LOG_TRACE("scan new partition",
            K(part_id),
            K(input_rows),
            K(input_size),
            K(local_group_rows_.size()),
            K(get_mem_used_size()),
            K(get_aggr_used_size()),
            K(get_mem_bound_size()));
      }
    }
  }

  // We use sort based group by for aggregation which need distinct or sort right now,
  // disable operator dump for compatibility.
  const bool is_dump_enabled =
      (!(aggr_processor_.has_distinct() || aggr_processor_.has_order_by()) && GCONF.is_sql_operator_dump_enabled());
  ObGroupRowItem curr_gr_item;
  curr_gr_item.group_exprs_ = const_cast<ExprFixedArray*>(&(MY_SPEC.group_exprs_));
  const ObGroupRowItem* exist_curr_gr_item = NULL;
  DatumStoreLinkPartition* parts[MAX_PARTITION_CNT] = {};
  int64_t part_cnt = 0;
  int64_t est_part_cnt = 0;
  bool check_dump = false;
  ObGbyBloomFilter* bloom_filter = NULL;
  const ObChunkDatumStore::StoredRow* srow = NULL;

  for (int64_t loop_cnt = 0; OB_SUCC(ret); ++loop_cnt) {
    if (NULL == cur_part) {
      ret = child_->get_next_row();
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
    if (loop_cnt % 1024 == 1023) {
      if (OB_FAIL(ctx_.check_status())) {
        LOG_WARN("check status failed", K(ret));
      }
    }

    const bool start_dump = (bloom_filter != NULL);
    if (OB_SUCC(ret)) {
      if (!start_dump && OB_FAIL(update_mem_status_periodically(loop_cnt, input_rows, est_part_cnt, check_dump))) {
        LOG_WARN("failed to update usable memory size periodically", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (NULL != srow) {
        curr_gr_item.groupby_datums_hash_ = *static_cast<uint64_t*>(srow->get_extra_payload());
      } else {
        if (OB_FAIL(calc_groupby_exprs_hash(curr_gr_item.groupby_datums_hash_))) {
          LOG_WARN("failed to get_groupby_exprs_hash", K(ret));
        }
      }
      LOG_DEBUG("finish calc_groupby_exprs_hash", K(curr_gr_item));
    }

    if (OB_FAIL(ret)) {
    } else if ((!start_dump || bloom_filter->exist(curr_gr_item.hash())) &&
               NULL != (exist_curr_gr_item = local_group_rows_.get(curr_gr_item))) {
      agged_row_cnt_ += 1;
      if (OB_ISNULL(exist_curr_gr_item->group_row_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("group_row is null", K(exist_curr_gr_item->group_id_), K(ret));
      } else if (OB_FAIL(aggr_processor_.process(*exist_curr_gr_item->group_row_))) {
        LOG_WARN("fail to process row", K(ret), KPC(exist_curr_gr_item));
      }
    } else {
      if (!is_dump_enabled || local_group_rows_.size() < MIN_INMEM_GROUPS ||
          (!start_dump && !need_start_dump(input_rows, est_part_cnt, check_dump))) {
        ++agged_row_cnt_;
        ++agged_group_cnt_;
        ObGroupRowItem* tmp_gr_item = NULL;
        if (OB_FAIL(init_group_row_item(curr_gr_item, tmp_gr_item))) {
          LOG_WARN("failed to init_group_row_item", K(ret));
        } else if (OB_FAIL(aggr_processor_.prepare(*tmp_gr_item->group_row_))) {
          LOG_WARN("fail to prepare row", K(ret), KPC(tmp_gr_item->group_row_));
        } else if (OB_FAIL(local_group_rows_.set(*tmp_gr_item))) {
          LOG_WARN("hash table set failed", K(ret));
        }
      } else {
        if (OB_UNLIKELY(!start_dump)) {
          if (OB_FAIL(setup_dump_env(part_id, max(input_rows, loop_cnt), parts, part_cnt, bloom_filter))) {
            LOG_WARN("setup dump environment failed", K(ret));
          } else {
            sql_mem_processor_.set_number_pass(part_id + 1);
          }
        }
        if (OB_SUCC(ret)) {
          ++agged_dumped_cnt_;
          const int64_t part_idx = (curr_gr_item.hash() >> part_shift) & (part_cnt - 1);
          // TODO : copy store row to new row store directly.
          ObChunkDatumStore::StoredRow* stored_row = NULL;
          if (OB_FAIL(parts[part_idx]->datum_store_.add_row(child_->get_spec().output_, &eval_ctx_, &stored_row))) {
            LOG_WARN("add row failed", K(ret));
          } else {
            *static_cast<uint64_t*>(stored_row->get_extra_payload()) = curr_gr_item.hash();
            LOG_DEBUG("finish dump", K(part_idx), K(curr_gr_item), KPC(stored_row));
          }
        }
      }
    }
  }

  // must reset. can't delay to destruction(memory released)
  row_store_iter.reset();
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
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

int ObHashGroupByOp::init_group_row_item(const ObGroupRowItem& curr_item, ObGroupRowItem*& gr_row_item)
{
  int ret = common::OB_SUCCESS;
  ObAggregateProcessor::GroupRow* group_row = NULL;
  gr_row_item = NULL;
  void* buf = NULL;
  if (OB_ISNULL(buf = (aggr_processor_.get_aggr_alloc().alloc(sizeof(ObGroupRowItem))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    gr_row_item = new (buf) ObGroupRowItem();
    gr_row_item->group_id_ = local_group_rows_.size();
    gr_row_item->groupby_datums_hash_ = curr_item.hash();
    if (OB_FAIL(aggr_processor_.init_one_group(gr_row_item->group_id_))) {
      LOG_WARN("failed to init one group", K(gr_row_item->group_id_), K(ret));
    } else if (OB_FAIL(aggr_processor_.get_group_row(gr_row_item->group_id_, group_row))) {
      LOG_WARN("failed to get group_row", K(gr_row_item->group_id_), K(ret));
    } else if (OB_ISNULL(group_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("group_row is null", K(gr_row_item->group_id_), K(ret));
    } else {
      gr_row_item->group_row_ = group_row;
    }
  }
  if (OB_SUCC(ret)) {
    ObChunkDatumStore::StoredRow* groupby_store_row = nullptr;
    if (OB_FAIL(group_store_.add_row(MY_SPEC.group_exprs_, &eval_ctx_, &groupby_store_row))) {
      LOG_WARN("failed to add row", K(ret));
    } else {
      group_row->groupby_store_row_ = groupby_store_row;
    }
  }
  return ret;
}

int ObHashGroupByOp::update_mem_status_periodically(
    const int64_t nth_cnt, const int64_t input_row, int64_t& est_part_cnt, bool& need_dump)
{
  int ret = common::OB_SUCCESS;
  bool updated = false;
  need_dump = false;
  if (OB_FAIL(sql_mem_processor_.update_max_available_mem_size_periodically(
          &ctx_.get_allocator(), [&](int64_t cur_cnt) { return nth_cnt > cur_cnt; }, updated))) {
    LOG_WARN("failed to update usable memory size periodically", K(ret));
  } else if (updated) {
    if (OB_FAIL(sql_mem_processor_.update_used_mem_size(get_mem_used_size()))) {
      LOG_WARN("failed to update used memory size", K(ret));
    } else {
      double data_ratio = sql_mem_processor_.get_data_ratio();
      ;
      est_part_cnt = detect_part_cnt(input_row);
      calc_data_mem_ratio(est_part_cnt, data_ratio);
      need_dump = (get_aggr_used_size() > get_mem_bound_size() * data_ratio);
    }
  }
  return ret;
}

int64_t ObHashGroupByOp::detect_part_cnt(const int64_t rows) const
{
  const double group_mem_avg = (double)get_aggr_used_size() / local_group_rows_.size();
  int64_t data_size = rows * ((double)agged_group_cnt_ / agged_row_cnt_) * group_mem_avg;
  int64_t mem_bound = get_mem_bound_size();
  const double part_skew_factor = 1.2;
  data_size = data_size * part_skew_factor;
  int64_t part_cnt = (data_size + mem_bound) / mem_bound;
  part_cnt = next_pow2(part_cnt);
  int64_t availble_mem_size = min(mem_bound - get_aggr_hold_size(), mem_bound * MAX_PART_MEM_RATIO);
  int64_t est_dump_size = part_cnt * ObChunkRowStore::BLOCK_SIZE;
  if (0 < availble_mem_size) {
    while (est_dump_size > availble_mem_size) {
      est_dump_size >>= 1;
    }
    part_cnt = est_dump_size / ObChunkRowStore::BLOCK_SIZE;
  } else {
    part_cnt = MIN_PARTITION_CNT;
  }
  part_cnt = next_pow2(part_cnt);
  part_cnt = std::max(part_cnt, (int64_t)MIN_PARTITION_CNT);
  part_cnt = std::min(part_cnt, (int64_t)MAX_PARTITION_CNT);
  LOG_TRACE("trace detect partition cnt",
      K(data_size),
      K(group_mem_avg),
      K(get_mem_used_size()),
      K(get_mem_bound_size()),
      K(part_skew_factor),
      K(agged_group_cnt_),
      K(agged_row_cnt_),
      K(local_group_rows_.size()),
      K(part_cnt),
      K(get_aggr_used_size()),
      K(get_local_hash_used_size()),
      K(get_dumped_part_used_size()),
      K(get_aggr_hold_size()),
      K(get_dump_part_hold_size()),
      K(rows),
      K(mem_context_),
      K(availble_mem_size),
      K(est_dump_size));
  return part_cnt;
}

void ObHashGroupByOp::calc_data_mem_ratio(const int64_t part_cnt, double& data_ratio)
{
  int64_t extra_size = (get_local_hash_used_size() + part_cnt * FIX_SIZE_PER_PART) * (1 + EXTRA_MEM_RATIO);
  int64_t data_size = max(get_aggr_used_size(), (get_mem_bound_size() - extra_size) * 0.8);
  data_ratio = data_size * 1.0 / (extra_size + data_size);
  sql_mem_processor_.set_data_ratio(data_ratio);
  LOG_TRACE("trace calc data ratio", K(data_ratio), K(extra_size), K(part_cnt), K(data_size), K(get_aggr_used_size()));
}

void ObHashGroupByOp::adjust_part_cnt(int64_t& part_cnt)
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
  LOG_TRACE(
      "trace adjust part cnt", K(part_cnt), K(max_part_cnt), K(dumped_remain_mem_size), K(mem_bound), K(mem_used));
}

int ObHashGroupByOp::calc_groupby_exprs_hash(uint64_t& hash_value)
{
  int ret = OB_SUCCESS;
  hash_value = 99194853094755497L;
  for (int64_t i = 0; OB_SUCC(ret) && i < MY_SPEC.group_exprs_.count(); ++i) {
    ObExpr* expr = MY_SPEC.group_exprs_.at(i);
    ObDatum* result = NULL;
    if (OB_ISNULL(expr) || OB_ISNULL(expr->basic_funcs_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr node is null", K(ret));
    } else if (OB_FAIL(expr->eval(eval_ctx_, result))) {
      LOG_WARN("eval failed", K(ret));
    } else {
      ObExprHashFuncType hash_func = expr->basic_funcs_->murmur_hash_;
      hash_value = hash_func(*result, hash_value);
    }
  }
  return ret;
}

bool ObHashGroupByOp::need_start_dump(const int64_t input_rows, int64_t& est_part_cnt, const bool check_dump)
{
  bool need_dump = false;
  const int64_t mem_used = get_mem_used_size();
  const int64_t mem_bound = get_mem_bound_size();
  double data_ratio = sql_mem_processor_.get_data_ratio();
  if (OB_UNLIKELY(0 == est_part_cnt)) {
    est_part_cnt = detect_part_cnt(input_rows);
    calc_data_mem_ratio(est_part_cnt, data_ratio);
  }
  // We continue do aggregation after we start dumping, reserve 1/8 memory for it.
  if (get_aggr_used_size() > data_ratio * mem_bound || check_dump) {
    int ret = OB_SUCCESS;
    need_dump = true;
    if (OB_FAIL(sql_mem_processor_.extend_max_memory_size(
            &ctx_.get_allocator(),
            [&](int64_t max_memory_size) { return get_aggr_used_size() > data_ratio * max_memory_size; },
            need_dump,
            mem_used))) {
      need_dump = true;
      LOG_WARN("failed to extend max memory size", K(ret), K(need_dump));
    } else if (OB_FAIL(sql_mem_processor_.update_used_mem_size(mem_used))) {
      LOG_WARN("failed to update used memory size", K(ret), K(mem_used), K(mem_bound), K(need_dump));
    } else {
      est_part_cnt = detect_part_cnt(input_rows);
      calc_data_mem_ratio(est_part_cnt, data_ratio);
      LOG_TRACE("trace extend max memory size",
          K(ret),
          K(data_ratio),
          K(get_aggr_used_size()),
          K(get_extra_size()),
          K(mem_used),
          K(mem_bound),
          K(need_dump),
          K(est_part_cnt));
    }
  }
  return need_dump;
}

int ObHashGroupByOp::setup_dump_env(const int64_t part_id, const int64_t input_rows, DatumStoreLinkPartition** parts,
    int64_t& part_cnt, ObGbyBloomFilter*& bloom_filter)
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
    const int64_t max_part_id = sizeof(uint64_t) / 2 - 1;  // half for partition half for hash table
    if (OB_UNLIKELY(part_id > max_part_id)) {              // part_id means level, max_part_id means max_level
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("reach max recursion depth", K(ret), K(part_id), K(max_part_id));
    }
    if (OB_SUCC(ret) && NULL == bloom_filter) {
      ModulePageAllocator mod_alloc(
          ObModIds::OB_HASH_NODE_GROUP_ROWS, ctx_.get_my_session()->get_effective_tenant_id(), ObCtxIds::WORK_AREA);
      mod_alloc.set_allocator(&mem_context_->get_malloc_allocator());
      void* mem = mem_context_->get_malloc_allocator().alloc(sizeof(ObGbyBloomFilter));
      if (OB_ISNULL(mem)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else if (FALSE_IT(bloom_filter = new (mem) ObGbyBloomFilter(mod_alloc))) {
      } else if (OB_FAIL(bloom_filter->init(local_group_rows_.size()))) {
        LOG_WARN("bloom filter init failed", K(ret));
      } else {
        auto cb_func = [&](ObGroupRowItem& item) {
          int ret = OB_SUCCESS;
          if (OB_FAIL(bloom_filter->set(item.hash()))) {
            LOG_WARN("add hash value to bloom failed", K(ret));
          }
          return ret;
        };
        if (OB_FAIL(local_group_rows_.foreach (cb_func))) {
          LOG_WARN("fill bloom filter failed", K(ret));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < part_cnt; i++) {
      void* mem = mem_context_->get_malloc_allocator().alloc(sizeof(DatumStoreLinkPartition));
      if (OB_ISNULL(mem)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        parts[i] = new (mem) DatumStoreLinkPartition(&mem_context_->get_malloc_allocator());
        parts[i]->part_id_ = part_id + 1;
        const int64_t extra_size = sizeof(uint64_t);  // for hash value
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
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql_mem_processor_.get_max_available_mem_size(&ctx_.get_allocator()))) {
      LOG_WARN("failed to get max available memory size", K(ret));
    } else if (OB_FAIL(sql_mem_processor_.update_used_mem_size(get_mem_used_size()))) {
      LOG_WARN("failed to update mem size", K(ret));
    }
    LOG_TRACE("trace setup dump", K(part_cnt), K(pre_part_cnt), K(part_id));
  }
  return ret;
}

int ObHashGroupByOp::cleanup_dump_env(const bool dump_success, const int64_t part_id, DatumStoreLinkPartition** parts,
    int64_t& part_cnt, ObGbyBloomFilter*& bloom_filter)
{
  int ret = OB_SUCCESS;
  // add spill partitions to partition list
  if (dump_success && part_cnt > 0) {
    int64_t part_rows[part_cnt];
    int64_t part_file_size[part_cnt];
    for (int64_t i = 0; OB_SUCC(ret) && i < part_cnt; i++) {
      DatumStoreLinkPartition*& p = parts[i];
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
    LOG_TRACE("hash group by dumped",
        K(part_id),
        K(local_group_rows_.size()),
        K(get_mem_used_size()),
        K(get_aggr_used_size()),
        K(get_mem_bound_size()),
        K(part_cnt),
        "part rows",
        ObArrayWrap<int64_t>(part_rows, part_cnt),
        "part file sizes",
        ObArrayWrap<int64_t>(part_file_size, part_cnt));
  }

  LOG_TRACE("trace hash group by info",
      K(part_id),
      K(local_group_rows_.size()),
      K(get_mem_used_size()),
      K(get_aggr_used_size()),
      K(get_mem_bound_size()));

  ObArrayWrap<DatumStoreLinkPartition*> part_array(parts, part_cnt);
  if (NULL != mem_context_) {
    FOREACH_CNT(p, part_array)
    {
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
      DatumStoreLinkPartition* p = dumped_group_parts_.remove_first();
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
  ObAggregateProcessor::GroupRow* group_row = NULL;
  if (OB_FAIL(aggr_processor_.get_group_row(curr_group_id_, group_row))) {
    LOG_WARN("failed to get group_row", K(curr_group_id_), K(ret));
  } else if (OB_ISNULL(group_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("group_row or groupby_datums_ is null", K(curr_group_id_), KP(group_row), K(ret));
  } else if (OB_FAIL(group_row->groupby_store_row_->to_expr(MY_SPEC.group_exprs_, eval_ctx_))) {
    LOG_WARN("failed to convert store row to expr", K(ret));
  }
  LOG_DEBUG("finish restore_groupby_datum", K(MY_SPEC.group_exprs_), K(ret));
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase
