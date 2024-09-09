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

#ifndef OCEANBASE_BASIC_OB_MERGE_GROUPBY_VEC_OP_H_
#define OCEANBASE_BASIC_OB_MERGE_GROUPBY_VEC_OP_H_

#include "common/row/ob_row_store.h"
#include "sql/engine/aggregate/ob_groupby_op.h"
#include "sql/engine/aggregate/ob_groupby_vec_op.h"
#include "lib/utility/ob_hyperloglog.h"
#include "sql/engine/px/datahub/components/ob_dh_rollup_key.h"
#include "sql/engine/sort/ob_sort_vec_op_provider.h"

namespace oceanbase
{
namespace sql
{
class ObMergeGroupByVecSpec : public ObGroupBySpec
{
  OB_UNIS_VERSION_V(1);

public:
  ObMergeGroupByVecSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type) :
    ObGroupBySpec(alloc, type), group_exprs_(alloc), rollup_exprs_(alloc), distinct_exprs_(alloc),
    is_duplicate_rollup_expr_(alloc), has_rollup_(false), is_parallel_(false),
    rollup_status_(ObRollupStatus::NONE_ROLLUP), rollup_id_expr_(nullptr), sort_exprs_(alloc),
    sort_collations_(alloc), sort_cmp_funcs_(alloc), enable_encode_sort_(false),
    est_rows_per_group_(0), enable_hash_base_distinct_(false)
  {}

  DECLARE_VIRTUAL_TO_STRING;
  inline void set_rollup(const bool has_rollup)
  {
    has_rollup_ = has_rollup;
  }
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
  DISALLOW_COPY_AND_ASSIGN(ObMergeGroupByVecSpec);

public:
  ExprFixedArray group_exprs_;    // group by column
  ExprFixedArray rollup_exprs_;   // rollup column
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
  int64_t est_rows_per_group_;
  bool enable_hash_base_distinct_;
};

class ObRowsGroupProcessor
{
public:
  ObRowsGroupProcessor() :
    groupby_exprs_(nullptr), group_expr_cnt_(0), last_group_row_idx_(-1), group_vectors_(),
    diff_row_idx_list_()
  {}
  void reuse();
  void reset();
  int prepare_process_next_batch();
  int init(ObEvalCtx &eval_ctx, const ObIArray<ObExpr *> &group_exprs);
  int find_next_new_group(const ObBatchRows &brs, const LastCompactRow &group_store_row,
                          bool &found_new_group, uint32_t &group_end_idx, int64_t &diff_group_idx);

private:
  void derive_last_group_info(int64_t &diff_group_idx, uint32_t &group_end_idx);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObRowsGroupProcessor);

private:
  const ObIArray<ObExpr *> *groupby_exprs_;
  int64_t group_expr_cnt_;
  int64_t last_group_row_idx_;
  ObSEArray<ObIVector *, 8> group_vectors_;
  ObSEArray<uint32_t, 8> diff_row_idx_list_;
};

class ObMergeGroupByVecOp : public ObGroupByVecOp
{
public:
  ObMergeGroupByVecOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input) :
    ObGroupByVecOp(exec_ctx, spec, input), mem_context_(nullptr), arena_alloc_(nullptr),
    is_end_(false), init_group_cnt_(0), cur_output_group_id_(common::OB_INVALID_INDEX),
    first_output_group_id_(0), last_child_output_(aggr_processor_.get_aggr_alloc()),
    cur_group_rowid_(-1), output_queue_cnt_(0), cur_aggr_row_(nullptr),
    cur_group_store_row_(nullptr), all_groupby_exprs_(),
    group_row_meta_(&exec_ctx.get_allocator()), distinct_col_idx_in_output_(),
    has_dup_group_expr_(false), is_group_first_calc_(true), cur_group_last_row_idx_(-1),
    use_sort_data_(false), inner_sort_(op_monitor_info_), rollup_hash_vals_(nullptr),
    ndv_calculator_(nullptr), global_rollup_key_(), partial_rollup_idx_(INT64_MAX),
    cur_grouping_id_(INT64_MAX), sort_batch_rows_(), first_batch_from_sort_(true), dir_id_(-1),
    group_batch_factor_(8), max_partial_rollup_idx_(INT64_MAX), rollup_context_(),
    agg_row_meta_(&exec_ctx.get_allocator()), group_rows_(), output_stored_rows_(nullptr),
    output_rollup_ids_(nullptr), profile_(ObSqlWorkAreaType::HASH_WORK_AREA),
    sql_mem_processor_(profile_, op_monitor_info_), hp_infras_mgr_(MTL_ID()), group_processor_()
  {}
  void reset();
  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_rescan() override;
  virtual int inner_switch_iterator() override;
  virtual int inner_get_next_row() override
  {
    return common::OB_NOT_IMPLEMENT;
  }
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual void destroy() override;
  int init_mem_context();
  int init();
  int gby_init();
  int rollup_init();
  int init_3stage_info();
  int init_one_group(const int64_t group_id, bool fill_pos = false);
  int init_group_rows(const int64_t row_count);
  int init_group_row_meta();
  int groupby_datums_eval_batch(const ObBatchRows &brs);
  int aggregate_group_rows(const ObBatchRows &brs, int32_t start_idx, int32_t end_idx);
  int add_batch_rows_for_3stage(const ObBatchRows &brs, int32_t start_idx, int32_t end_idx);
  int add_batch_rows_for_3th_stage(const ObBatchRows &brs, const uint32_t start_idx,
                                   const uint32_t end_idx);
  int process_batch(const ObBatchRows &brs);
  int prepare_and_save_curr_groupby_datums(const ObBatchRows &brs, int64_t curr_group_rowid,
                                           ObIArray<ObExpr *> &group_exprs,
                                           LastCompactRow *&cur_gby_store_row, int64_t extra_size);
  int process_unique_distinct_columns_for_batch(const LastCompactRow &cur_gby_store_row,
                                                const LastCompactRow &last_child_output,
                                                const uint32_t group_start_idx,
                                                const uint32_t group_end_idx, ObBatchRows &brs);
  int calc_batch_results(const bool is_iter_end, const int64_t max_output_size);
  int64_t get_partial_rollup_key_idx()
  {
    return partial_rollup_idx_;
  }
  int get_n_shuffle_keys_for_exchange(int64_t &shuffle_n_keys);

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObMergeGroupByVecOp);
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

  inline int get_groupby_store_row(int i, LastCompactRow *&store_row);
  inline LastCompactRow *get_groupby_store_row(int i)
  {
    LastCompactRow *store_row = nullptr;
    if (i < output_groupby_rows_.count()) {
      store_row = output_groupby_rows_.at(i);
    }
    return store_row;
  }
  inline int create_groupby_store_row(LastCompactRow *&store_row);
  int reuse_group(const int64_t group_id);
  int before_process_next_batch(const int64_t max_row_cnt);

  int get_cur_group_row(const ObBatchRows &brs, int64_t group_row_id,
                        aggregate::AggrRowPtrRef curr_aggr_row, LastCompactRow *&cur_gby_store_row,
                        ObIArray<ObExpr *> &group_exprs);
  int gen_rollup_group_rows(int64_t start_diff_group_idx, int64_t end_group_idx,
                            int64_t max_group_idx, int64_t cur_group_row_id);
  int get_rollup_row(int64_t prev_group_row_id, int64_t group_row_id,
                     aggregate::AggrRowPtrRef curr_aggr_row, LastCompactRow *&cur_gby_store_row,
                     aggregate::AggrRowPtrRef prev_aggr_row, LastCompactRow *&prev_gby_store_row,
                     bool &need_set_null, int64_t idx);
  int get_empty_rollup_row(int64_t group_row_id, aggregate::AggrRowPtrRef curr_aggr_row,
                           LastCompactRow *&curr_gby_store_row);
  int set_null(int64_t idx, LastCompactRow *rollup_store_row);

  // for rollup distributor and rollup collector
  int init_rollup_distributor();
  int get_grouping_id(const ObBatchRows &brs);
  int process_parallel_rollup_key(ObRollupNDVInfo &ndv_info);
  int find_candidate_key(ObRollupNDVInfo &ndv_info);

  int batch_process_rollup_distributor(const int64_t max_row_cnt);
  int batch_collect_local_ndvs(const ObBatchRows *child_brs);
  int get_child_next_batch_row(const int64_t max_row_cnt, const ObBatchRows *&batch_rows);
  int set_all_null(int64_t start, int64_t end, int64_t max_group_idx,
                   LastCompactRow *rollup_store_row);
  void sets(ObHyperLogLogCalculator &ndv_calculator, uint64_t *hash_vals, ObBitVector *skip,
            int64_t count);
  int advance_collect_result(int64_t group_id);
  int init_hsp_infras_group_mgr();
  inline int64_t get_group_rows_count() const
  {
    return group_rows_.count();
  }
  int swap_group_row(const int a, const int b);
  int get_aggr_row(const int64_t group_id, aggregate::AggrRowPtrRef aggr_row);
  int get_aggr_and_group_row(const int64_t group_id, aggregate::AggrRowPtrRef aggr_row,
                             LastCompactRow *&store_row);
  int process_rollup(const int64_t diff_group_idx, bool is_end = false);
  int rollup_batch_process(aggregate::AggrRowPtr aggr_row, aggregate::AggrRowPtr rollup_row,
                           int64_t diff_group_idx = -1, const int64_t max_group_cnt = INT64_MIN);
  int collect_group_results(const RowMeta &row_meta, const ObIArray<ObExpr *> &group_exprs,
                            const int32_t output_batch_size, ObBatchRows &output_brs,
                            int64_t &cur_group_id);
  int init_hp_infras_group_mgr();

private:
  static const int64_t ROLLUP_BASE_ROW_EXTRA_SIZE = 8;
  static const int64_t N_HYPERLOGLOG_BIT = 14;
  lib::MemoryContext mem_context_;
  common::ObArenaAllocator *arena_alloc_;
  bool is_end_;
  int64_t init_group_cnt_;
  int64_t cur_output_group_id_;
  int64_t first_output_group_id_;
  int64_t max_output_group_id_;
  LastCompactRow last_child_output_;
  int64_t cur_group_rowid_;
  int64_t output_queue_cnt_;
  ObVectorsResultHolder brs_holder_;
  ObSEArray<LastCompactRow *, 128> output_groupby_rows_;
  aggregate::AggrRowPtr cur_aggr_row_;
  LastCompactRow *cur_group_store_row_;
  ObSEArray<ObExpr *, 8> all_groupby_exprs_;
  RowMeta group_row_meta_;
  ObSEArray<int64_t, 8> distinct_col_idx_in_output_;
  bool has_dup_group_expr_;
  bool is_group_first_calc_;
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
  bool use_sort_data_;
  ObSEArray<ObExpr *, 4> inner_sort_exprs_;
  ObSortVecOpProvider inner_sort_;
  uint64_t *rollup_hash_vals_; // hash_values for compute NDV
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
  share::aggregate::RollupContext rollup_context_;
  RowMeta agg_row_meta_;
  common::ObSegmentArray<ObCompactRow *, OB_MALLOC_MIDDLE_BLOCK_SIZE, common::ModulePageAllocator>
    group_rows_;
  const ObCompactRow **output_stored_rows_;
  void **output_rollup_ids_;
  ObSqlWorkAreaProfile profile_;
  ObSqlMemMgrProcessor sql_mem_processor_;
  ObHashPartInfrasVecMgr hp_infras_mgr_;
  ObRowsGroupProcessor group_processor_;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_BASIC_OB_MERGE_GROUPBY_VEC_OP_H_
