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
#include "sql/engine/basic/ob_stat_collector_op.h"
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "storage/ddl/ob_column_clustered_dag.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;

ObStatCollectorSpec::ObStatCollectorSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
  : ObOpSpec(alloc, type),
  is_none_partition_(false),
  sort_exprs_(alloc),
  sort_collations_(alloc),
  sort_cmp_funs_(alloc),
  type_(ObStatCollectorType::NOT_INIT_TYPE),
  sort_exprs_inverted_(alloc),
  sort_collations_inverted_(alloc),
  sort_cmp_funs_inverted_(alloc)
{}

OB_SERIALIZE_MEMBER((ObStatCollectorSpec, ObOpSpec),
                    is_none_partition_,
                    sort_exprs_,
                    sort_collations_,
                    sort_cmp_funs_,
                    type_,
                    sort_exprs_inverted_,
                    sort_collations_inverted_,
                    sort_cmp_funs_inverted_);

int TabletDocidRow::assign(const TabletDocidRow &other)
{
  int ret = OB_SUCCESS;
  tablet_id_ = other.tablet_id_;
  if (OB_FAIL(border_vals_.assign(other.border_vals_))) {
    LOG_WARN("failed to assign border vals", K(ret));
  }
  return ret;
}

ObStatCollectorOp::ObStatCollectorOp(ObExecContext &ctx_, const ObOpSpec &spec, ObOpInput *input)
  : ObOperator(ctx_, spec, input),
  sort_impl_(op_monitor_info_),
  sort_impl_inverted_(op_monitor_info_),
  iter_end_(false),
  by_pass_(false),
  exist_sample_row_(false),
  partition_row_count_map_(),
  non_partition_row_count_(0),
  inverted_sample_row_count_(0)
{}

void ObStatCollectorOp::destroy()
{
  sort_impl_.unregister_profile_if_necessary();
  sort_impl_.~ObSortOpImpl();
  sort_impl_inverted_.unregister_profile_if_necessary();
  sort_impl_inverted_.~ObSortOpImpl();
  partition_row_count_map_.destroy();
  ObOperator::destroy();
}

int ObStatCollectorOp::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(child_)
    || OB_ISNULL(ctx_.get_my_session())
    || OB_ISNULL(ctx_.get_sqc_handler())
    || OB_UNLIKELY(!ctx_.get_sqc_handler()->valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("child is null", K(ret), KP(child_), KP(ctx_.get_my_session()), KPC(ctx_.get_sqc_handler()));
  } else if (ctx_.get_my_session()->get_ddl_info().is_partition_local_ddl()
    && FALSE_IT(ctx_.set_expect_range_count(ctx_.get_sqc_handler()->get_sqc_init_arg().sqc_.get_task_count()))) {
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
    } else if (!MY_SPEC.sort_collations_inverted_.empty()) { // fts task
      if (OB_FAIL(sort_impl_inverted_.init(
              ctx_.get_my_session()->get_effective_tenant_id(),
              &MY_SPEC.sort_collations_inverted_,
              &MY_SPEC.sort_cmp_funs_inverted_,
              &eval_ctx_,
              &ctx_,
              false/*enable_encode_sortkey*/,
              false/*in_local_order*/,
              true/*need_rewind*/))) {
        LOG_WARN("fail to init inverted sort impl", K(ret));
      } else if (FALSE_IT(sort_impl_inverted_.set_io_event_observer(&io_event_observer_))) {
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(partition_row_count_map_.create(DEFAULT_HASH_MAP_BUCKETS_COUNT,
        "StatPartBucket",
        "StatPartNode"))) {
          LOG_WARN("fail to create partition count map", K(ret));
        }
    }
  }
  return ret;
}

int ObStatCollectorOp::inner_close()
{
  sort_impl_.collect_memory_dump_info(op_monitor_info_);
  sort_impl_.unregister_profile();
  if (!MY_SPEC.sort_collations_inverted_.empty()) { // fts task
    sort_impl_inverted_.collect_memory_dump_info(op_monitor_info_);
    sort_impl_inverted_.unregister_profile();
  }
  return OB_SUCCESS;
}

int ObStatCollectorOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  iter_end_ = false;
  if (ObStatCollectorType::SAMPLE_SORT == MY_SPEC.type_) {
    sort_impl_.reset();
    if (!MY_SPEC.sort_collations_inverted_.empty()) {
      sort_impl_inverted_.reset();
    }
    by_pass_ = true;
    non_partition_row_count_ = 0;
    inverted_sample_row_count_ = 0;
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
  } else if (IS_SAMPLE_SCAN(op->get_spec().get_type())) {
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
  const bool use_dual_sort = !MY_SPEC.sort_collations_inverted_.empty(); // fts task
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
      } else if (use_dual_sort && OB_FAIL(sort_impl_inverted_.add_row(MY_SPEC.sort_exprs_inverted_))) {
        LOG_WARN("fail to add row for inverted sorter", K(ret));
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
      } else if (use_dual_sort &&
                 OB_FAIL(sort_impl_inverted_.add_batch(MY_SPEC.sort_exprs_inverted_,
                                                       *input_brs->skip_,
                                                       input_brs->size_, 0, nullptr))) {
        LOG_WARN("fail to add row to inverted sorter", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && exist_sample_row_) {
    if (OB_FAIL(sort_impl_.sort())) {
      LOG_WARN("fail to do sort", K(ret));
    } else if (use_dual_sort) { // fts task
      if (OB_FAIL(split_fts_forward_partition_range())) {
        LOG_WARN("fail to split FTS forward partition range", K(ret));
      } else if (OB_FAIL(sort_impl_inverted_.sort())) {
        LOG_WARN("fail to do inverted sort", K(ret));
      } else if (OB_FAIL(split_partition_range(true /*is_inverted*/))) {
        LOG_WARN("fail to split inverted partition range", K(ret));
      }
    } else if (OB_FAIL(split_partition_range(false /*is_inverted*/))) {
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

//fts forward table's partition range is cut only by doc_id column, not by forward table's rowkey (doc_id, word)
int ObStatCollectorOp::split_fts_forward_partition_range()
{
  int ret = OB_SUCCESS;
  int64_t expect_range_count = ctx_.get_expect_range_count() - 1;
  const ExprFixedArray &sort_exprs = MY_SPEC.sort_exprs_;
  CK(expect_range_count > 0 && !sort_exprs.empty());

  if (OB_FAIL(ret)) {
  } else {
    ObSortOpImpl &sort_impl = sort_impl_;
    ObArray<TabletDocidRow> tablet_docid_rows;
    ObHashMap<int64_t, int64_t> partition_docid_count_map;
    if (OB_FAIL(partition_docid_count_map.create(DEFAULT_HASH_MAP_BUCKETS_COUNT, "PDocidMap"))) {
      LOG_WARN("fail to create partition docid count map", K(ret));
    } else if (OB_FAIL(collect_tablet_docid_counts(sort_impl, sort_exprs, tablet_docid_rows, partition_docid_count_map))) {
      LOG_WARN("fail to collect tablet docid counts", K(ret));
    } else if (!tablet_docid_rows.empty()) {
      const int64_t expect_sampling_count = get_one_thread_sampling_count_by_parallel(ctx_.get_expect_range_count());
      int64_t pre_tablet_id = OB_INVALID_ID;
      int64_t cur_tablet_id = OB_INVALID_ID;
      int64_t cur_tablet_docid_count = 0;
      int64_t step = 0;
      int64_t tablet_docid_count = 0;
      ObPxTabletRange partition_range;
      Ob2DArray<ObPxTabletRange> tmp_part_ranges;
      int64_t datum_len_sum = 0;

      for (int64_t i = 0; OB_SUCC(ret) && i < tablet_docid_rows.count(); ++i) {
        auto &tablet_row = tablet_docid_rows.at(i);
        int64_t tablet_id = tablet_row.tablet_id_;
        ObPxTabletRange::DatumKey &row_data = tablet_row.border_vals_;
        if (tablet_id != pre_tablet_id) {
          cur_tablet_docid_count = 0;  // Reset counter for new tablet

          // Push previous partition range if exists
          if (pre_tablet_id != OB_INVALID_ID) {
            OZ(tmp_part_ranges.push_back(partition_range));
            partition_range.reset();
          }

          // Calculate step for this tablet
          if (OB_FAIL(ret)) {
          } else if (!is_none_partition() &&
              OB_FAIL(partition_docid_count_map.get_refactored(tablet_id, tablet_docid_count))) {
            LOG_WARN("fail to get partition docid count", K(ret), K(tablet_id));
          } else {
            if (is_none_partition()) {
              step = max(1, tablet_docid_rows.count() / expect_sampling_count);
            } else {
              step = max(1, tablet_docid_count / expect_sampling_count);
            }
            step = step < 10 ? step : 10;
            pre_tablet_id = tablet_id;
          }
        }

        CK(0 != step);
        if (OB_SUCC(ret)) {
          cur_tablet_docid_count++;
          if (0 == cur_tablet_docid_count % step) {
            partition_range.tablet_id_ = tablet_id;
            partition_range.range_weights_ = step;
            OZ(partition_range.range_cut_.push_back(row_data));
            for (int64_t j = 0; OB_SUCC(ret) && j < row_data.count(); ++j) {
              datum_len_sum += row_data.at(j).get_deep_copy_size();
            }
          }
        }
      }

      // last partition range
      if (OB_SUCC(ret) && pre_tablet_id != OB_INVALID_ID) {
        if (partition_range.is_valid()) {
          OZ(tmp_part_ranges.push_back(partition_range));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("partition range is not valid", K(ret), K(partition_range));
        }
      }

      // Set partition ranges
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
        if (OB_FAIL(ret)){
        } else if (OB_FAIL(ctx_.set_partition_ranges(tmp_part_ranges, static_cast<char*>(buf), datum_len_sum))) {
          ctx_.get_sqc_handler()->get_safe_allocator().free(buf);
          buf = nullptr;
          LOG_WARN("set partition ranges failed", K(ret));
        } else if (OB_FAIL(report_sample_ranges_to_dag(false /*is_inverted*/, tmp_part_ranges))) { // record fts forward sample ranges to dag
          LOG_WARN("report fts forward sample ranges to dag failed", K(ret));
        }
      }
      // free tablet_docid_rows array anyway, because set_partition_ranges and report_sample_ranges_to_dag do deep copy
      for (int64_t i = 0; i < tablet_docid_rows.count(); ++i) {
        ObPxTabletRange::DatumKey &border_vals = tablet_docid_rows.at(i).border_vals_;
        for (int64_t j = 0; j < border_vals.count(); ++j) {
          if (OB_NOT_NULL(border_vals.at(j).ptr_)) {
            border_vals.at(j).~ObDatum();
            ctx_.get_allocator().free(const_cast<char *>(border_vals.at(j).ptr_));
            border_vals.at(j).reset();
          }
        }
      }
    }
  }
  return ret;
}

int ObStatCollectorOp::split_partition_range(const bool is_inverted)
{
  int ret = OB_SUCCESS;
  int64_t expect_range_count = ctx_.get_expect_range_count() - 1;
  const ExprFixedArray &sort_exprs = is_inverted ? MY_SPEC.sort_exprs_inverted_ : MY_SPEC.sort_exprs_;
  CK(expect_range_count > 0 && !sort_exprs.empty());

  if (OB_FAIL(ret)) {
  } else {
    bool sort_iter_end = false;
    int64_t pre_tablet_id = OB_INVALID_ID;
    int64_t cur_tablet_id = is_inverted ? 0 : OB_INVALID_ID;
    int64_t *count_ptr = nullptr;
    int64_t step = 0;
    int64_t cur_row_count = 0;
    int64_t border_vals_cnt = is_inverted ? sort_exprs.count() : sort_exprs.count() - static_cast<int64_t>(!is_none_partition());
    ObSortOpImpl &sort_impl = is_inverted ? sort_impl_inverted_ : sort_impl_;
    ObPxTabletRange::DatumKey border_vals;
    ObPxTabletRange partition_range;
    Ob2DArray<ObPxTabletRange> tmp_part_ranges;
    int64_t datum_len_sum = 0;
    OZ(border_vals.prepare_allocate(border_vals_cnt));
    while (OB_SUCC(ret) && !sort_iter_end) {
      if (OB_FAIL(sort_impl.get_next_row(sort_exprs))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("sort instance get next row failed", K(ret));
        } else {
          sort_iter_end = true;
          ret = OB_SUCCESS;
        }
      } else if (!is_inverted && OB_FAIL(get_tablet_id(sort_exprs, cur_tablet_id))) {
        LOG_WARN("failed to calc partition id", K(ret));
      } else if (cur_tablet_id != pre_tablet_id) {
        cur_row_count = 0;
        if (!is_inverted && !is_none_partition() &&
            (OB_FAIL(partition_row_count_map_.get_refactored(cur_tablet_id, count_ptr)))) {
          LOG_WARN("fail to get partition id", K(ret));
        } else {
          const int64_t expect_sampling_count = get_one_thread_sampling_count_by_parallel(ctx_.get_expect_range_count());
          if (pre_tablet_id != OB_INVALID_ID) {
            OZ(tmp_part_ranges.push_back(partition_range));
            partition_range.reset();
          }
          if (is_none_partition()) {
            step = max(1, non_partition_row_count_ / expect_sampling_count);
          } else if (!is_none_partition() && is_inverted) {
            step = max(1, inverted_sample_row_count_ / expect_sampling_count);
          } else {
            step = max(1, (*count_ptr) / expect_sampling_count);
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
          int64_t start_idx = 0;
          if (!is_none_partition() && !is_inverted) {
            start_idx = 1;
          }
          for (int64_t i = start_idx; OB_SUCC(ret) && i < sort_exprs.count(); ++i) {
            ObDatum *cur_datum = nullptr;
            if (OB_FAIL(sort_exprs.at(i)->eval(eval_ctx_, cur_datum))) {
              LOG_WARN("eval expr to datum failed", K(ret), K(i));
            } else if (OB_ISNULL(cur_datum)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("current datum is null", K(ret), K(i));
            } else if (OB_FAIL(border_vals.at(i - start_idx).deep_copy(*cur_datum, ctx_.get_allocator()))) {
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
    if (OB_SUCC(ret) && !is_inverted) {
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
    if (OB_SUCC(ret) && is_inverted) { // fts task
      if (OB_FAIL(report_sample_ranges_to_dag(is_inverted, tmp_part_ranges))) {
        LOG_WARN("report sample ranges to dag failed", K(ret), K(is_inverted));
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
    } else {
      inverted_sample_row_count_++;
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

int ObStatCollectorOp::collect_tablet_docid_counts(
    ObSortOpImpl &sort_impl,
    const ExprFixedArray &sort_exprs,
    ObArray<TabletDocidRow> &tablet_docid_rows,
    common::hash::ObHashMap<int64_t, int64_t> &partition_docid_count_map)
{
  int ret = OB_SUCCESS;
  bool sort_iter_end = false;
  int64_t pre_doc_id = OB_INVALID_ID;
  int64_t pre_tablet_id = OB_INVALID_ID;
  int64_t cur_tablet_id = OB_INVALID_ID;
  int64_t cur_tablet_doc_id_count = 0;
  bool no_partition = is_none_partition();

  //calculate docid count for each tablet
  while (OB_SUCC(ret) && !sort_iter_end) {
    if (OB_FAIL(sort_impl.get_next_row(sort_exprs))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("sort instance get next row failed", K(ret));
      } else {
        sort_iter_end = true;
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(get_tablet_id(sort_exprs, cur_tablet_id))) {
      LOG_WARN("failed to get tablet id", K(ret));
    } else if (cur_tablet_id != pre_tablet_id) {
      if (pre_tablet_id != OB_INVALID_ID) {
        OZ(partition_docid_count_map.set_refactored(pre_tablet_id, cur_tablet_doc_id_count));
      }
      pre_doc_id = OB_INVALID_ID;
      cur_tablet_doc_id_count = 0;
      pre_tablet_id = cur_tablet_id;
    }

    if (OB_SUCC(ret) && sort_iter_end) {
      OZ(partition_docid_count_map.set_refactored(pre_tablet_id, cur_tablet_doc_id_count));
    }

    if (OB_SUCC(ret) && !sort_iter_end) {
      int64_t cur_doc_id = OB_INVALID_ID;
      ObDatum *cur_doc_id_datum = nullptr;
      if (no_partition) {
        if (sort_exprs.count() < 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sort exprs count is less than 1", K(ret), K(sort_exprs.count()));
        }
      } else {
        if (sort_exprs.count() < 2) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("sort exprs count is less than 2", K(ret), K(sort_exprs.count()));
        }
      }
      OZ(sort_exprs.at(no_partition ? 0 : 1)->eval(eval_ctx_, cur_doc_id_datum));//TODO@xuzhuo: use column type to distinguish doc_id column?
      if (OB_SUCC(ret)) {
        if (OB_ISNULL(cur_doc_id_datum)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("current datum is null", K(ret));
        } else {
          cur_doc_id = cur_doc_id_datum->get_int();
        }
      }

      if (OB_SUCC(ret)) {
        if (cur_doc_id != pre_doc_id) {
          cur_tablet_doc_id_count++;
          pre_doc_id = cur_doc_id;
          ObPxTabletRange::DatumKey new_border_vals;
          int64_t border_vals_cnt = sort_exprs.count() - static_cast<int64_t>(!no_partition);
          if (OB_FAIL(new_border_vals.prepare_allocate(border_vals_cnt))) {
            LOG_WARN("prepare allocate for new border vals failed", K(ret));
          } else {
            for (int64_t i = no_partition ? 0 : 1; OB_SUCC(ret) && i < sort_exprs.count(); ++i) {
              ObDatum *cur_datum = nullptr;
              if (OB_FAIL(sort_exprs.at(i)->eval(eval_ctx_, cur_datum))) {
                LOG_WARN("eval expr to datum failed", K(ret), K(i));
              } else if (OB_ISNULL(cur_datum)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("current datum is null", K(ret), K(i));
              } else if (OB_FAIL(new_border_vals.at(i - static_cast<int64_t>(!no_partition)).deep_copy(*cur_datum, ctx_.get_allocator()))) {
                LOG_WARN("deep copy datum failed", K(ret), K(i), K(*cur_datum));
              }
            }
            if (OB_FAIL(ret)) {
            } else {
              TabletDocidRow row;
              row.tablet_id_ = cur_tablet_id;
              if (OB_FAIL(row.border_vals_.assign(new_border_vals))) {
                LOG_WARN("failed to assign border vals", K(ret));
              } else if (OB_FAIL(tablet_docid_rows.push_back(row))) {
                LOG_WARN("failed to push back tablet docid rows", K(ret));
              } else {
                new_border_vals.reset();
              }
            }
          }

          if (OB_FAIL(ret)) {
            for (int64_t i = 0; i < new_border_vals.count(); ++i) {
              if (OB_NOT_NULL(new_border_vals.at(i).ptr_)) {
                new_border_vals.at(i).~ObDatum();
                ctx_.get_allocator().free(const_cast<char *>(new_border_vals.at(i).ptr_));
                new_border_vals.at(i).reset();
              }
            }
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    for (int64_t i = 0; i < tablet_docid_rows.count(); ++i) {
      ObPxTabletRange::DatumKey &border_vals = tablet_docid_rows.at(i).border_vals_;
      for (int64_t j = 0; j < border_vals.count(); ++j) {
        if (OB_NOT_NULL(border_vals.at(j).ptr_)) {
          border_vals.at(j).~ObDatum();
          ctx_.get_allocator().free(const_cast<char *>(border_vals.at(j).ptr_));
          border_vals.at(j).reset();
        }
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
  if (OB_FAIL(get_tablet_id(MY_SPEC.sort_exprs_, tablet_id))) {
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

int ObStatCollectorOp::get_tablet_id(const ExprFixedArray &sort_exprs, int64_t &tablet_id)
{
  int ret = OB_SUCCESS;
  ObDatum *datum = nullptr;
  tablet_id = 0;
  if (!is_none_partition()) {
    if (OB_FAIL(sort_exprs.at(0)->eval(eval_ctx_, datum))) {
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

int64_t ObStatCollectorOp::get_one_thread_sampling_count_by_parallel(const int64_t parallel)
{
  int64_t sampling_count = parallel;
  if (sampling_count < 192 && sampling_count > 0) {
    const int64_t factor_count = 6;
    const int64_t amplification_factors[factor_count] = { 96, 48, 24, 12, 6, 3 };
    const int64_t factor_idx = static_cast<int64_t>(LOG2(sampling_count)) - 1;
    if (OB_LIKELY(factor_idx >= 0 && factor_idx < factor_count)) {
      sampling_count *= amplification_factors[factor_idx];
    }
  }
  return sampling_count;
}

int ObStatCollectorOp::report_sample_ranges_to_dag(
    const bool is_inverted,
    const common::Ob2DArray<sql::ObPxTabletRange> &part_ranges)
{
  int ret = OB_SUCCESS;
  if (!need_report_sample_to_dag() || part_ranges.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no need to report sample ranges to dag", K(ret), K(is_inverted), K(part_ranges.count()));
  } else if (OB_ISNULL(ctx_.get_sqc_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sqc handler is null when report sample ranges", K(ret));
  } else {
    storage::ObColumnClusteredDag *dag = ctx_.get_sqc_handler()->get_sub_coord().get_ddl_dag();
    if (nullptr == dag) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ddl dag not found when reporting sample ranges", K(ret));
    } else {
      const int64_t expect_sampling_count = get_one_thread_sampling_count_by_parallel(ctx_.get_expect_range_count());
      if (OB_FAIL(dag->append_sample_ranges(is_inverted, part_ranges, expect_sampling_count))) {
        LOG_WARN("append sample ranges to dag failed", K(ret), K(is_inverted));
      } else {
        FLOG_INFO("stat collector reported sample ranges to dag", K(is_inverted), "range_cnt", part_ranges.at(0).range_cut_.count(), K(expect_sampling_count), K(part_ranges));
      }
    }
  }
  return ret;
}
