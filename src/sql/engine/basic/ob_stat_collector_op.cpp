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
#include "lib/hash/ob_hashmap.h"
#include "sql/engine/basic/ob_stat_collector_op.h"
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "sql/engine/table/ob_block_sample_scan_op.h"
#include "sql/engine/table/ob_row_sample_scan_op.h"
#include "common/ob_smart_call.h"
#include "sql/engine/px/ob_px_sqc_handler.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

ObStatCollectorSpec::ObStatCollectorSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
  : ObOpSpec(alloc, type),
  is_none_partition_(false),
  sort_exprs_(alloc),
  sort_collations_(alloc),
  sort_cmp_funs_(alloc),
  type_(ObStatCollectorType::NOT_INIT_TYPE)
{}

OB_SERIALIZE_MEMBER((ObStatCollectorSpec, ObOpSpec),
                    is_none_partition_,
                    sort_exprs_,
                    sort_collations_,
                    sort_cmp_funs_,
                    type_);

ObStatCollectorOp::ObStatCollectorOp(ObExecContext &ctx_, const ObOpSpec &spec, ObOpInput *input)
  : ObOperator(ctx_, spec, input),
  sort_impl_(op_monitor_info_),
  iter_end_(false),
  by_pass_(false),
  exist_sample_row_(false),
  partition_row_count_map_(),
  non_partition_row_count_(0)
{}

void ObStatCollectorOp::destroy()
{
  sort_impl_.unregister_profile_if_necessary();
  sort_impl_.~ObSortOpImpl();
  partition_row_count_map_.destroy();
  ObOperator::destroy();
}

int ObStatCollectorOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret));
  } else if (ObStatCollectorType::SAMPLE_SORT == MY_SPEC.type_) {
    by_pass_ = false;
    if (MY_SPEC.sort_exprs_.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected sort expr", K(ret));
    } else if (OB_FAIL(sort_impl_.init(
        ctx_.get_my_session()->get_effective_tenant_id(),
        &MY_SPEC.sort_collations_,
        &MY_SPEC.sort_cmp_funs_,
        &eval_ctx_,
        &ctx_,
        false/*enable_encode_sortkey*/,
        false/*in_local_order*/,
        true/*need_rewind*/))) {
      LOG_WARN("fail to init sort impl", K(ret));
    } else if (FALSE_IT(sort_impl_.set_io_event_observer(&io_event_observer_))) {
    } else if (OB_FAIL(partition_row_count_map_.create(DEFAULT_HASH_MAP_BUCKETS_COUNT,
        "StatPartBucket",
        "StatPartNode"))) {
      LOG_WARN("fail to create partition count map", K(ret));
    }
  }
  return ret;
}

int ObStatCollectorOp::inner_close()
{
  sort_impl_.collect_memory_dump_info(op_monitor_info_);
  sort_impl_.unregister_profile();
  return OB_SUCCESS;
}

int ObStatCollectorOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  iter_end_ = false;
  if (ObStatCollectorType::SAMPLE_SORT == MY_SPEC.type_) {
    sort_impl_.reset();
    by_pass_ = true;
    non_partition_row_count_ = 0;
    OZ(set_no_need_sample());
  }
  OZ(ObOperator::inner_rescan());
  return ret;
}

int ObStatCollectorOp::set_no_need_sample()
{
  int ret = OB_SUCCESS;
  ObOperator *tsc = NULL;
  OZ(find_sample_scan(this, tsc));
  CK(OB_NOT_NULL(tsc));
  if (OB_SUCC(ret)) {
    static_cast<ObTableScanOp *>(tsc)->set_need_sample(false);
    if (static_cast<const ObTableScanSpec &>(tsc->get_spec()).report_col_checksum_) {
      static_cast<ObTableScanOp *>(tsc)->set_report_checksum(true);
    }
    OZ(static_cast<ObTableScanOp *>(tsc)->reset_sample_scan());
  }
  return ret;
}

int ObStatCollectorOp::find_sample_scan(ObOperator *op, ObOperator *&tsc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op) || OB_NOT_NULL(tsc)) {
    /*do nothing*/
  } else if (PHY_BLOCK_SAMPLE_SCAN == op->get_spec().get_type() ||
             PHY_ROW_SAMPLE_SCAN == op->get_spec().get_type()) {
    tsc = op;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < op->get_child_cnt(); ++i) {
      if (OB_FAIL(SMART_CALL(find_sample_scan(op->get_child(i), tsc)))) {
        LOG_WARN("fail to find sample scan", K(ret));
      }
    }
  }
  return ret;
}

int ObStatCollectorOp::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (iter_end_) {
    ret = OB_ITER_END;
  } else if (by_pass_) {
    clear_evaluated_flag();
    if (OB_FAIL(child_->get_next_row())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next row", K(ret));
      } else {
        iter_end_ = true;
      }
    }
  } else if (OB_FAIL(generate_sample_partition_range())) {
    LOG_WARN("fail to generate sample range", K(ret));
  }
  return ret;
}

int ObStatCollectorOp::inner_get_next_batch(const int64_t max_row_cnt)
{
  int ret = OB_SUCCESS;
  int64_t batch_cnt = min(max_row_cnt, MY_SPEC.max_batch_size_);
  if (iter_end_) {
    ret = OB_ITER_END;
  } else if (by_pass_) {
    const ObBatchRows *brs = nullptr;
    clear_evaluated_flag();
    if (OB_FAIL(child_->get_next_batch(batch_cnt, brs))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next row", K(ret));
      } else {
        iter_end_ = true;
      }
    } else if (OB_FAIL(brs_.copy(brs))) {
      LOG_WARN("fail to copy batch result", K(ret));
    }
  } else if (OB_FAIL(generate_sample_partition_range(batch_cnt))) {
    LOG_WARN("fail to generate sample range", K(ret));
  }
  return ret;
}

int ObStatCollectorOp::generate_sample_partition_range(int64_t batch_size)
{
  int ret = OB_SUCCESS;
  if (!is_vectorized()) {
    while (OB_SUCC(ret)) {
      clear_evaluated_flag();
      if (ctx_.get_expect_range_count() - 1 < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected range count", K(ctx_.get_expect_range_count()));
      } else if (0 == ctx_.get_expect_range_count() - 1) {
        break;
      } else if (OB_FAIL(child_->get_next_row())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next row", K(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (!exist_sample_row_) {
        exist_sample_row_ = true;
      }
      if (OB_FAIL(ret) || !exist_sample_row_) {
      } else if (OB_FAIL(collect_row_count_in_partitions())) {
        LOG_WARN("fail to collect row count", K(ret));
      } else if (OB_FAIL(sort_impl_.add_row(MY_SPEC.sort_exprs_))) {
        LOG_WARN("fail to add row", K(ret));
      }
    }
  } else {
    while (OB_SUCC(ret)) {
      clear_evaluated_flag();
      const ObBatchRows *input_brs = NULL;
      if (ctx_.get_expect_range_count() - 1 < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected range count", K(ctx_.get_expect_range_count()));
      } else if (0 == ctx_.get_expect_range_count() - 1) {
        break;
      } else if (OB_FAIL(child_->get_next_batch(batch_size, input_brs))) {
        LOG_WARN("fail to get next batch", K(ret));
      } else if (input_brs->end_) {
        break;
      } else if (!exist_sample_row_) {
        exist_sample_row_ = true;
      }
      if (OB_FAIL(ret) || !exist_sample_row_) {
      } else if (OB_FAIL(collect_row_count_in_partitions(true/*is_vec*/, input_brs, batch_size))) {
        LOG_WARN("fail to collect row count", K(ret));
      } else if (OB_FAIL(sort_impl_.add_batch(MY_SPEC.sort_exprs_,
          *input_brs->skip_, input_brs->size_, 0, nullptr))) {
        LOG_WARN("fail to add row", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && exist_sample_row_) {
    if (OB_FAIL(sort_impl_.sort())) {
      LOG_WARN("fail to do sort", K(ret));
    } else if (OB_FAIL(split_partition_range())) {
      LOG_WARN("fail to split partition range", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (is_vectorized()) {
      brs_.end_ = true;
      brs_.size_ = 0;
    } else {
      ret = OB_ITER_END;
    }
  }
  LOG_INFO("dynamic sample ranges", K(exist_sample_row_), "sample_ranges", ctx_.get_partition_ranges());
  return ret;
}

int ObStatCollectorOp::split_partition_range()
{
  int ret = OB_SUCCESS;
  int64_t expect_range_count = ctx_.get_expect_range_count() - 1;
  CK(expect_range_count > 0 && !MY_SPEC.sort_exprs_.empty());
  if (OB_FAIL(ret)) {
  } else {
    bool sort_iter_end = false;
    int64_t pre_tablet_id = OB_INVALID_ID;
    int64_t cur_tablet_id = OB_INVALID_ID;
    int64_t *count_ptr = nullptr;
    int64_t step = 0;
    int64_t cur_row_count = 0;
    int64_t border_vals_cnt = MY_SPEC.sort_exprs_.count() - (int64_t)(!is_none_partition());
    ObPxTabletRange::DatumKey border_vals;
    ObPxTabletRange partition_range;
    Ob2DArray<ObPxTabletRange> tmp_part_ranges;
    int64_t datum_len_sum = 0;
    OZ(border_vals.prepare_allocate(border_vals_cnt));
    while (OB_SUCC(ret) && !sort_iter_end) {
      if (OB_FAIL(sort_impl_.get_next_row(MY_SPEC.sort_exprs_))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("sort instance get next row failed", K(ret));
        } else {
          sort_iter_end = true;
          ret = OB_SUCCESS;
        }
      } else if (OB_FAIL(get_tablet_id(cur_tablet_id))) {
        LOG_WARN("failed to calc partition id", K(ret));
      } else if (cur_tablet_id != pre_tablet_id) {
        cur_row_count = 0;
        if (!is_none_partition() &&
            (OB_FAIL(partition_row_count_map_.get_refactored(cur_tablet_id, count_ptr)))) {
          LOG_WARN("fail to get partition id", K(ret));
        } else {
          if (pre_tablet_id != OB_INVALID_ID) {
            OZ(tmp_part_ranges.push_back(partition_range));
            partition_range.reset();
          }
          if (is_none_partition()) {
            step = max(1, non_partition_row_count_ / (expect_range_count + 1));
          } else {
            step = max(1, (*count_ptr) / (expect_range_count + 1));
          }
          pre_tablet_id = cur_tablet_id;
        }
      }
      if (OB_SUCC(ret) && sort_iter_end) {
        OZ(tmp_part_ranges.push_back(partition_range));
        partition_range.reset();
      }
      CK(0 != step);
      if (OB_SUCC(ret) && !sort_iter_end) {
        cur_row_count++;
        if (0 == cur_row_count % step) {
          for (int64_t i = is_none_partition()? 0 : 1;
              OB_SUCC(ret) && i < MY_SPEC.sort_exprs_.count(); ++i) {
            ObDatum *cur_datum = nullptr;
            if (OB_FAIL(MY_SPEC.sort_exprs_.at(i)->eval(eval_ctx_, cur_datum))) {
              LOG_WARN("eval expr to datum failed", K(ret), K(i));
            } else if (OB_ISNULL(cur_datum)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("current datum is null", K(ret), K(i));
            } else if (OB_FAIL(border_vals.at(i - (int64_t)!is_none_partition()).
                  deep_copy(*cur_datum, ctx_.get_allocator()))) {
              LOG_WARN("deep copy datum failed", K(ret), K(i), K(*cur_datum));
            } else {
              datum_len_sum += cur_datum->get_deep_copy_size();
            }
          }
          if (OB_SUCC(ret)) {
            partition_range.tablet_id_ = cur_tablet_id;
            partition_range.range_weights_ = step;
            OZ(partition_range.range_cut_.push_back(border_vals));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      void *buf = NULL;
      if (OB_LIKELY(datum_len_sum > 0)) {
        if (OB_ISNULL(ctx_.get_sqc_handler())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sqc handler is null", K(ret));
        } else if (OB_ISNULL(buf = ctx_.get_sqc_handler()->get_safe_allocator().alloc(datum_len_sum))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret), K(datum_len_sum));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(ctx_.set_partition_ranges(tmp_part_ranges,
                                                        static_cast<char*>(buf), datum_len_sum))) {
        LOG_WARN("set partition ranges failed", K(ret));
      }
    }
  }
  return ret;
}

int ObStatCollectorOp::collect_row_count_in_partitions(
    bool is_vectorized,
    const ObBatchRows *child_brs,
    int64_t batch_size)
{
  int ret = OB_SUCCESS;
  if (!is_vectorized) {
    if (is_none_partition()) {
      non_partition_row_count_++;
    } else if (OB_FAIL(update_partition_row_count())) {
      LOG_WARN("fail to update partition row count", K(ret));
    }
  } else {
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(eval_ctx_);
    batch_info_guard.set_batch_size(batch_size);
    for (int64_t i = 0; i < child_brs->size_ && OB_SUCC(ret); ++i) {
      if (child_brs->skip_->exist(i)) {
        /*do nothing*/
      } else {
        batch_info_guard.set_batch_idx(i);
        OZ(collect_row_count_in_partitions(false/*is_vectorized*/));
      }
    }
  }
  return ret;
}

int ObStatCollectorOp::update_partition_row_count()
{
  int ret = OB_SUCCESS;
  int64_t tablet_id = 0;
  int64_t *count_ptr = nullptr;
  if (OB_FAIL(get_tablet_id(tablet_id))) {
    LOG_WARN("fail to calc partition id", K(ret));
  } else if (OB_FAIL(partition_row_count_map_.get_refactored(tablet_id, count_ptr))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if (OB_ISNULL(count_ptr = (int64_t *)(ctx_.get_allocator().alloc(sizeof(int64_t))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else if (FALSE_IT(*count_ptr = 0)) {
      } else if (OB_FAIL(partition_row_count_map_.set_refactored(tablet_id, count_ptr))) {
        LOG_WARN("fail to set partition row count", K(ret));
      }
    } else {
      LOG_WARN("fail to get partition row count", K(ret));
    }
  }
  CK(OB_NOT_NULL(count_ptr));
  if (OB_SUCC(ret)) {
    (*count_ptr)++;
  }
  return ret;
}

int ObStatCollectorOp::get_tablet_id(int64_t &tablet_id)
{
  int ret = OB_SUCCESS;
  ObDatum *datum = nullptr;
  tablet_id = 0;
  if (!is_none_partition()) {
    if (OB_FAIL(MY_SPEC.sort_exprs_.at(0)->eval(eval_ctx_, datum))) {
      LOG_WARN("failed to eval datum", K(ret));
    } else if (ObExprCalcPartitionId::NONE_PARTITION_ID == (tablet_id = datum->get_int())) {
      ret = OB_NO_PARTITION_FOR_GIVEN_VALUE;
      LOG_WARN("fail to calc partition id", K(ret));
    }
  }
  return ret;
}

bool ObStatCollectorOp::is_none_partition()
{
  return MY_SPEC.is_none_partition_;
}
