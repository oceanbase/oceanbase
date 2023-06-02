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

#ifndef OCEANBASE_BASIC_OB_MERGE_GROUPBY_OP_H_
#define OCEANBASE_BASIC_OB_MERGE_GROUPBY_OP_H_

#include "common/row/ob_row_store.h"
#include "sql/engine/aggregate/ob_groupby_op.h"
#include "lib/utility/ob_hyperloglog.h"
#include "sql/engine/px/datahub/components/ob_dh_rollup_key.h"

namespace oceanbase
{
namespace sql
{

class ObMergeGroupBySpec : public ObGroupBySpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObMergeGroupBySpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObGroupBySpec(alloc, type),
      group_exprs_(alloc),
      rollup_exprs_(alloc),
      distinct_exprs_(alloc),
      is_duplicate_rollup_expr_(alloc),
      has_rollup_(false),
      is_parallel_(false),
      rollup_status_(ObRollupStatus::NONE_ROLLUP),
      rollup_id_expr_(nullptr),
      sort_exprs_(alloc),
      sort_collations_(alloc),
      sort_cmp_funcs_(alloc),
      enable_encode_sort_(false)
    {
    }

  DECLARE_VIRTUAL_TO_STRING;
  inline void set_rollup(const bool has_rollup) {  has_rollup_ = has_rollup; }
  inline int init_group_exprs(const int64_t count)
  {
    return group_exprs_.init(count);
  }
  inline int init_rollup_exprs(const int64_t count)
  {
    return rollup_exprs_.init(count);
  }
  inline int init_duplicate_rollup_expr(const int64_t count)
  {
    return is_duplicate_rollup_expr_.init(count);
  }
  int add_group_expr(ObExpr *expr);
  int add_rollup_expr(ObExpr *expr);

  int register_to_datahub(ObExecContext &ctx) const;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObMergeGroupBySpec);

public:
  ExprFixedArray group_exprs_;   //group by column
  ExprFixedArray rollup_exprs_;  //rollup column
  ExprFixedArray distinct_exprs_; // the distinct arguments of aggregate function
  common::ObFixedArray<bool, common::ObIAllocator> is_duplicate_rollup_expr_;
  bool has_rollup_;
  bool is_parallel_;
  ObRollupStatus rollup_status_;
  ObExpr *rollup_id_expr_;
  ExprFixedArray sort_exprs_;
  ObSortCollations sort_collations_;
  ObSortFuncs sort_cmp_funcs_;
  bool enable_encode_sort_;
};

// 输入数据已经按照groupby列排序
class ObMergeGroupByOp : public ObGroupByOp
{
public:
  ObMergeGroupByOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObGroupByOp(exec_ctx, spec, input),
      is_end_(false),
      cur_output_group_id_(common::OB_INVALID_INDEX),
      first_output_group_id_(0),
      last_child_output_(aggr_processor_.get_aggr_alloc()),
      curr_group_rowid_(-1),
      cur_group_row_(nullptr),
      all_groupby_exprs_(),
      distinct_col_idx_in_output_(),
      has_dup_group_expr_(false),
      is_first_calc_(true),
      cur_group_last_row_idx_(-1),
      use_sort_data_(false),
      inner_sort_(op_monitor_info_),
      rollup_hash_vals_(nullptr),
      ndv_calculator_(nullptr),
      global_rollup_key_(),
      partial_rollup_idx_(INT64_MAX),
      cur_grouping_id_(INT64_MAX),
      sort_batch_rows_(),
      first_batch_from_sort_(true),
      dir_id_(-1),
      group_batch_factor_(8),
      max_partial_rollup_idx_(INT64_MAX)
  {
  }
  void reset();
  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_rescan() override;
  virtual int inner_switch_iterator() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual void destroy() override;
  int init();
  int init_group_rows();
  int groupby_datums_eval_batch(const ObBitVector &skip, const int64_t size);
  OB_INLINE int aggregate_group_rows(const int64_t group_id, const ObBatchRows &brs,
                           const uint32_t start_idx, const uint32_t end_idx);
  int process_batch(const ObBatchRows &brs);
  int prepare_and_save_curr_groupby_datums(
      int64_t curr_group_rowid,
      ObAggregateProcessor::GroupRow *group_row,
      ObIArray<ObExpr*> &group_exprs,
      const int64_t group_count,
      int64_t extra_size);
  int check_same_group(ObAggregateProcessor::GroupRow *cur_group_row, int64_t &diff_pos);
  int check_unique_distinct_columns(ObAggregateProcessor::GroupRow *cur_group_row, bool &is_same_before_row);
  int check_unique_distinct_columns_for_batch(bool &is_same_before_row, int64_t cur_row_idx);
  int restore_groupby_datum(ObAggregateProcessor::GroupRow *cur_group_row, const int64_t diff_pos);
  int rollup_and_calc_results(const int64_t group_id, const ObExpr *diff_expr = NULL);
  int calc_batch_results(const bool is_iter_end, const int64_t max_output_size);
  int rewrite_rollup_column(ObExpr *&diff_expr);
  void set_rollup_expr_null(int64_t group_id);
  int64_t get_partial_rollup_key_idx() { return partial_rollup_idx_; }
  int get_n_shuffle_keys_for_exchange(int64_t &shuffle_n_keys);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObMergeGroupByOp);
  inline void inc_output_queue_cnt()
  {
    output_queue_cnt_++;
  }
  inline void set_output_queue_cnt(const int64_t new_cnt)
  {
    output_queue_cnt_ = new_cnt;
  }
  inline int64_t get_output_queue_cnt()
  {
    return output_queue_cnt_;
  }
  inline bool stop_batch_iterating(const ObBatchRows &brs, const int64_t output_size)
  {
    return (output_queue_cnt_ >= (group_batch_factor_ * output_size) || brs.end_);
  }
  inline bool is_output_queue_not_empty()
  {
    return (get_output_queue_cnt() > 0);
  }

  inline int get_groupby_store_row(int i, ObChunkDatumStore::LastStoredRow **store_row);
  inline int create_groupby_store_row(ObChunkDatumStore::LastStoredRow **store_row);

  int get_cur_group_row(int64_t group_row_id,
                        ObAggregateProcessor::GroupRow *&curr_group_row,
                        ObIArray<ObExpr*> &group_exprs,
                        const int64_t group_count);
  int gen_rollup_group_rows(int64_t start_diff_group_idx,
                            int64_t end_group_idx,
                            int64_t max_group_idx,
                            int64_t cur_group_row_id);
  int get_rollup_row(int64_t prev_group_row_id,
                    int64_t group_row_id,
                    ObAggregateProcessor::GroupRow *&curr_group_row,
                    bool &need_set_null,
                    int64_t idx);
  int get_empty_rollup_row(int64_t group_row_id,
                          ObAggregateProcessor::GroupRow *&curr_group_row);
  int set_null(int64_t idx, ObChunkDatumStore::StoredRow *rollup_store_row);

  // for rollup distributor and rollup collector
  int init_rollup_distributor();
  int get_child_next_row();
  int fill_groupby_id_expr(const int64_t group_id);
  int get_grouping_id();
  int process_parallel_rollup_key(ObRollupNDVInfo &ndv_info);
  int process_rollup_distributor();
  int collect_local_ndvs();
  int find_candidate_key(ObRollupNDVInfo &ndv_info);

  int batch_process_rollup_distributor(const int64_t max_row_cnt);
  int batch_collect_local_ndvs(const ObBatchRows *child_brs);
  int get_child_next_batch_row(const int64_t max_row_cnt, const ObBatchRows *&batch_rows);
  int set_all_null(int64_t start,
                  int64_t end,
                  int64_t max_group_idx,
                  ObChunkDatumStore::StoredRow *rollup_store_row);
  void sets(ObHyperLogLogCalculator &ndv_calculator,
            uint64_t *hash_vals,
            ObBitVector *skip,
            int64_t count);
  int advance_collect_result(int64_t group_id);
private:
  bool is_end_;
  // added to support groupby with rollup
  int64_t cur_output_group_id_;
  int64_t first_output_group_id_;
  int64_t max_output_group_id_;
  ObChunkDatumStore::LastStoredRow last_child_output_;
  int64_t curr_group_rowid_;
  int64_t output_queue_cnt_ = 0;
  ObBatchResultHolder brs_holder_;
  ObSEArray<ObChunkDatumStore::LastStoredRow *, 128> output_groupby_rows_;
  ObAggregateProcessor::GroupRow *cur_group_row_;
  ObSEArray<ObExpr *, 4> all_groupby_exprs_;
  ObSEArray<int64_t, 4> distinct_col_idx_in_output_;
  bool has_dup_group_expr_;
  bool is_first_calc_;
  int64_t cur_group_last_row_idx_;

  // for rollup distributor and rollup collector
  // For rollup distributor
  //    1. add row to sort for sort data in gropuby operator instead of separate sort operator
  //    2. calculate the NDV of group_exprs and rollup(exprs)
  //    3. send to QC and get the optimal rollup exprs
  //    4. rollup data
  // For rollup collector
  //    1. compute group and aggregate function and rollup data for base-row
  //    2. only calculate group and aggregate function for grouping row that has rollup
  static const int64_t ROLLUP_BASE_ROW_EXTRA_SIZE = 8;
  static const int64_t N_HYPERLOGLOG_BIT = 14;
  bool use_sort_data_;
  ObSEArray<ObExpr *, 4> inner_sort_exprs_;
  ObSortOpImpl inner_sort_;
  uint64_t *rollup_hash_vals_;   // hash_values for compute NDV
  ObHyperLogLogCalculator *ndv_calculator_;
  ObRollupNDVInfo global_rollup_key_;
  // for partial rollup, RD(Rollup Distributor) and RC(Rollup Collector)
  int64_t partial_rollup_idx_;
  int64_t cur_grouping_id_;
  ObBatchRows sort_batch_rows_;
  bool first_batch_from_sort_;

  int64_t dir_id_;
  // default is a magic number 8, may use a sophisticated way
  int64_t group_batch_factor_;
  int64_t max_partial_rollup_idx_;
};

OB_INLINE int ObMergeGroupByOp::aggregate_group_rows(const int64_t group_id,
                                           const ObBatchRows &brs,
                                           const uint32_t start_idx,
                                           const uint32_t end_idx)
{
  int ret = OB_SUCCESS;
  if (!(start_idx == 0 && end_idx == 0)) {
    ObAggregateProcessor::GroupRow *group_row = nullptr;
    if (OB_FAIL(aggr_processor_.get_group_row(group_id, group_row))) {
      SQL_LOG(WARN, "failed to get_group_row", K(group_id), K(ret));
    } else if (OB_FAIL(aggr_processor_.process_batch(*group_row, brs, start_idx,
                                                     end_idx))) {
      SQL_LOG(WARN, "failed to calc aggr", K(ret));
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_BASIC_OB_MERGE_GROUPBY_OP_H_
