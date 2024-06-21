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

#ifndef OCEANBASE_SHARE_AGGREGATE_PROCESSOR_H_
#define OCEANBASE_SHARE_AGGREGATE_PROCESSOR_H_

#include "share/aggregate/agg_ctx.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/engine/basic/ob_compact_row.h"

namespace oceanbase
{
namespace share
{
namespace aggregate
{
class IAggregate;
class Processor
{
public:
  Processor(sql::ObEvalCtx &eval_ctx, ObIArray<ObAggrInfo> &aggr_infos, const lib::ObLabel &label,
            sql::ObMonitorNode &monitor_info, const int64_t tenant_id) :
    inited_(false),
    support_fast_single_row_agg_(false), has_distinct_(false), has_order_by_(false), dir_id_(-1),
    op_eval_infos_(nullptr),
    allocator_(label, OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id, ObCtxIds::WORK_AREA),
    agg_ctx_(eval_ctx, tenant_id, aggr_infos, label),
    aggregates_(allocator_, aggr_infos.count()),
    fast_single_row_aggregates_(allocator_, aggr_infos.count()), extra_rt_info_buf_(nullptr),
    cur_extra_rt_info_idx_(0), add_one_row_fns_(allocator_, aggr_infos.count()), row_selector_(nullptr)
  {}
  ~Processor() { destroy(); }
  int init();
  void destroy();
  void reuse();

  inline int32_t get_aggregate_row_size() const { return agg_ctx_.row_meta().row_size_; }

  int add_batch_rows(const int32_t start_agg_id, const int32_t end_agg_id, AggrRowPtr row,
                     const ObBatchRows &brs, uint16_t begin, uint16_t end);
  int add_batch_rows(const int32_t start_agg_id, const int32_t end_agg_id, AggrRowPtr row,
                     const ObBatchRows &brs, const uint16_t *selector_array, uint16_t count);

  int prepare_adding_one_row();

  int finish_adding_one_row();

  inline int add_one_row(const int32_t start_agg_id, const int32_t end_agg_id, AggrRowPtr row,
                  const int64_t batch_idx, const int64_t batch_size, ObIVector **aggr_vectors)
  {
    int ret = OB_SUCCESS;
    ObIVector *data_vec = nullptr;
    ObEvalCtx &ctx = agg_ctx_.eval_ctx_;
    for (int col_id = start_agg_id; OB_SUCC(ret) && col_id < end_agg_id; col_id++) {
      add_one_row_fn fn = add_one_row_fns_.at(col_id);
      if (OB_FAIL(
            fn(aggregates_.at(col_id), agg_ctx_, col_id, row, aggr_vectors[col_id], batch_idx, batch_size))) {
        SQL_LOG(WARN, "add one row failed", K(ret));
      }
    }
    return ret;
  }

  inline int add_batch_for_multi_groups(const int32_t start_agg_id, const int32_t end_agg_id,
                                        AggrRowPtr *agg_rows, const int64_t batch_size)
  {
    int ret = OB_SUCCESS;
    int size = 0;
    OB_ASSERT(batch_size <= agg_ctx_.eval_ctx_.max_batch_size_);
    if (OB_ISNULL(row_selector_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_LOG(WARN, "unexpected null selector", K(ret));
    }
    for (int i = 0; OB_SUCC(ret) && i < batch_size; i++) {
      if (OB_NOT_NULL(agg_rows[i])) { row_selector_[size++] = i; }
    }
    RowSelector iter(row_selector_, size);
    for (int col_id = start_agg_id; OB_SUCC(ret) && col_id < end_agg_id; col_id++) {
      if (OB_FAIL(aggregates_.at(col_id)->add_batch_for_multi_groups(agg_ctx_, agg_rows, iter,
                                                                     batch_size, col_id))) {
        SQL_LOG(WARN, "add batch for multi groups failed", K(ret));
      }
    }
    return ret;
  }

  int collect_group_results(const RowMeta &row_meta, const ObIArray<ObExpr *> &group_exprs,
                            const int32_t output_batch_size, ObBatchRows &output_brs,
                            int64_t &cur_group_id);

  int collect_group_results(const RowMeta &row_meta, const ObIArray<ObExpr *> &group_exprs,
                            const int32_t batch_size, const ObCompactRow **row,
                            ObBatchRows &output_brs);

  int collect_scalar_results(const RowMeta &row_meta, const ObCompactRow **rows,
                             const int32_t batch_size);

  int collect_empty_set(bool collect_for_third_stage) const;
  int single_row_agg_batch(AggrRowPtr *agg_rows, const int64_t batch_size,
                           ObEvalCtx &eval_ctx, const ObBitVector &skip);

  // TODO: remove aggr_cell_len for add_batch_rows
  // TODO: add eval_param_batch

  int eval_aggr_param_batch(const ObBatchRows &brs);

  int generate_group_rows(AggrRowPtr *row_arr, const int32_t batch_size);
  int add_one_aggregate_row(AggrRowPtr data, const int32_t row_size,
                                   bool push_agg_row = true);

  inline int64_t get_aggr_used_size() const
  {
    return agg_ctx_.allocator_.used();
  }

  inline int64_t get_aggr_hold_size() const
  {
    return agg_ctx_.allocator_.total();
  }
  inline void prefetch_aggregate_row(AggrRowPtr row) const
  {
    __builtin_prefetch(row, 0/* read */, 2 /*high temp locality*/);
  }
  inline void set_support_fast_single_row_agg(const bool support_fast_single_row_agg)
  {
    support_fast_single_row_agg_ = support_fast_single_row_agg;
  }

  inline bool support_fast_single_row_agg() const
  {
    return support_fast_single_row_agg_;
  }

  inline void set_dir_id(const int64_t dir_id)
  {
    dir_id_ = dir_id;
  }
  inline int64_t dir_id() const
  {
    return dir_id_;
  }

  inline void set_io_event_observer(sql::ObIOEventObserver *io_observer)
  {
    agg_ctx_.io_event_observer_ = io_observer;
  }

  inline void set_op_monitor_info(sql::ObMonitorNode *op_monitor_info)
  {
    agg_ctx_.op_monitor_info_ = op_monitor_info;
  }
  inline bool has_distinct() const
  {
    return has_distinct_;
  }

  inline bool has_order_by() const
  {
    return has_order_by_;
  }

  // FIXME: support all aggregate functions
  inline static bool all_supported_aggregate_functions(const ObIArray<sql::ObRawExpr *> &aggr_exprs,
                                                       bool is_scalar_gby = false)
  {
    bool supported = true;
    for (int i = 0; supported && i < aggr_exprs.count(); i++) {
      ObAggFunRawExpr *agg_expr = static_cast<ObAggFunRawExpr *>(aggr_exprs.at(i));
      OB_ASSERT(agg_expr != NULL);
      // TODO: remove distinct constraint @zongmei.zzm
      supported = aggregate::supported_aggregate_function(agg_expr->get_expr_type())
                  && !agg_expr->is_param_distinct();
    }
    return supported;
  }

  inline int64_t aggregates_cnt() const
  {
    return agg_ctx_.aggr_infos_.count();
  }

  inline void set_op_eval_infos(ObIArray<ObEvalInfo *> *eval_infos)
  {
    op_eval_infos_ = eval_infos;
  }

  inline void clear_op_evaluated_flag()
  {
    if (OB_NOT_NULL(op_eval_infos_)) {
      for (int i = 0; i < op_eval_infos_->count(); i++) {
        op_eval_infos_->at(i)->clear_evaluated_flag();
      }
    }
  }

  static const ObCompactRow &get_groupby_stored_row(const RowMeta &row_meta,
                                                    const AggrRowPtr agg_row)
  {
    const char *payload = (const char *)(agg_row) - row_meta.extra_off_;
    return *reinterpret_cast<const ObCompactRow *>(payload - sizeof(RowHeader));
  }

  int init_scalar_aggregate_row(ObCompactRow *&row, RowMeta &row_meta, ObIAllocator &allocator);

  int init_fast_single_row_aggs();

  RuntimeContext *get_rt_ctx() { return &agg_ctx_; }

  static int setup_rt_info(AggrRowPtr data, RuntimeContext &agg_ctx);

  ObIArray<IAggregate *> &get_aggregates() { return aggregates_; }

private:
  int setup_bypass_rt_infos(const int64_t batch_size);

  int llc_init_empty(ObExpr &expr, ObEvalCtx &eval_ctx) const;
  inline void clear_add_one_row_fns()
  {
    for (int i = 0; i < agg_ctx_.aggr_infos_.count(); i++) {
      add_one_row_fns_.at(i) = nullptr;
    }
  }
private:
  static const int32_t MAX_SUPPORTED_AGG_CNT = 1000000;
  friend class ObScalarAggregateVecOp;
  using add_one_row_fn = int (*)(IAggregate *, RuntimeContext &, const int32_t, AggrRowPtr,
                                 ObIVector *, const int64_t, const int64_t);
  bool inited_;
  bool support_fast_single_row_agg_;
  bool has_distinct_;
  bool has_order_by_;
  int64_t dir_id_;
  ObIArray<ObEvalInfo *> *op_eval_infos_;
  // used to allocate structure memory, such as aggregates_
  ObArenaAllocator allocator_;
  RuntimeContext agg_ctx_;
  ObFixedArray<IAggregate *, ObIAllocator> aggregates_;
  ObFixedArray<IAggregate *, ObIAllocator> fast_single_row_aggregates_;
  char *extra_rt_info_buf_;
  int32_t cur_extra_rt_info_idx_;
  ObFixedArray<add_one_row_fn, ObIAllocator> add_one_row_fns_;
  uint16_t *row_selector_;
  // ObFixedArray<typename T>
};
} // end aggregate
} // end share
} // end oceanbase
#endif // OCEANBASE_SHARE_AGGREGATE_PROCESSOR_H_
