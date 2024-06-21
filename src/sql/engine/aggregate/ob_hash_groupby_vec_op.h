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

#ifndef OCEANBASE_BASIC_OB_HASH_GROUPBY_VEC_H_
#define OCEANBASE_BASIC_OB_HASH_GROUPBY_VEC_H_

#include "common/row/ob_row_store.h"
#include "sql/engine/aggregate/ob_hash_agg_variant.h"
#include "lib/list/ob_list.h"
#include "lib/list/ob_dlink_node.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"
#include "sql/engine/aggregate/ob_adaptive_bypass_ctrl.h"
#include "sql/engine/basic/ob_vector_result_holder.h"
#include "sql/engine/aggregate/ob_groupby_vec_op.h"
#include "sql/engine/basic/ob_hp_infras_vec_op.h"
#include "src/sql/engine/expr/ob_expr_estimate_ndv.h"

namespace oceanbase
{
namespace sql
{

struct LlcEstimate
{
public:
  LlcEstimate()
    : avg_group_mem_(0), llc_map_(), est_cnt_(0), last_est_cnt_(0), enabled_(false)
  {}
  int init_llc_map(common::ObArenaAllocator &allocator);
  int reset();
  double avg_group_mem_;
  ObString llc_map_;
  uint64_t est_cnt_;
  uint64_t last_est_cnt_;
  bool enabled_;
  static constexpr const int64_t ESTIMATE_MOD_NUM_ = 4096;
  static constexpr const double LLC_NDV_RATIO_ = 0.3;
  static constexpr const double GLOBAL_BOUND_RATIO_ = 0.8;
};

struct ObGroupByDupColumnPairVec
{
  OB_UNIS_VERSION_V(1);
public:
  ObExpr *org_expr;
  ObExpr *dup_expr;
};

class ObHashGroupByVecSpec : public ObGroupBySpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObHashGroupByVecSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObGroupBySpec(alloc, type),
      group_exprs_(alloc), cmp_funcs_(alloc), est_group_cnt_(0),
      org_dup_cols_(alloc),
      new_dup_cols_(alloc),
      dist_col_group_idxs_(alloc),
      distinct_exprs_(alloc)
    {
    }

  DECLARE_VIRTUAL_TO_STRING;
  inline int init_group_exprs(const int64_t count)
  {
    return group_exprs_.init(count);
  }
  int add_group_expr(ObExpr *expr);
  inline void set_est_group_cnt(const int64_t cnt) { est_group_cnt_ = cnt; }
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObHashGroupByVecSpec);

public:
  ExprFixedArray group_exprs_;   //group by column
  ObCmpFuncs cmp_funcs_;
  int64_t est_group_cnt_;
  common::ObFixedArray<ObExpr*, common::ObIAllocator> org_dup_cols_;
  common::ObFixedArray<ObExpr*, common::ObIAllocator> new_dup_cols_;
  common::ObFixedArray<int64_t, common::ObIAllocator> dist_col_group_idxs_;
  ExprFixedArray distinct_exprs_; // the distinct arguments of aggregate function
};

// 输入数据已经按照groupby列排序
class ObHashGroupByVecOp : public ObGroupByVecOp
{
using BaseClass = ObGroupByVecOp;
public:
  struct DatumStoreLinkPartition : public common::ObDLinkBase<DatumStoreLinkPartition>
  {
  public:
    DatumStoreLinkPartition(common::ObIAllocator *alloc = nullptr)
      : row_store_(alloc), part_id_(0), part_shift_(0)
    {}
    ObTempRowStore row_store_;
    int64_t part_id_;
    int64_t part_shift_;
  };

public:
  static const int64_t MIN_PARTITION_CNT = 8;
  static const int64_t MAX_PARTITION_CNT = 256;
  static const int64_t MAX_BATCH_DUMP_PART_CNT = 64;
  static const int64_t INIT_BKT_SIZE_FOR_ADAPTIVE_GBY = 256;

  // min in memory groups
  static const int64_t MIN_INMEM_GROUPS = 4;
  static const int64_t MIN_GROUP_HT_INIT_SIZE = 1 << 10; // 1024
  static const int64_t MAX_GROUP_HT_INIT_SIZE = 2 << 20; // 1048576
  static constexpr const double MAX_PART_MEM_RATIO = 0.5;
  static constexpr const double EXTRA_MEM_RATIO = 0.25;
  static const int64_t MIN_BATCH_SIZE_REORDER_AGGR_ROWS = 256;
  static const int64_t FIX_SIZE_PER_PART = sizeof(DatumStoreLinkPartition) + ObTempRowStore::BLOCK_SIZE;
  static const uint64_t HASH_SEED = 99194853094755497L;


public:
  ObHashGroupByVecOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObGroupByVecOp(exec_ctx, spec, input),
      local_group_rows_(),
      curr_group_id_(common::OB_INVALID_INDEX),
      cur_group_item_idx_(0),
      cur_group_item_buf_(nullptr),
      mem_context_(NULL),
      agged_group_cnt_(0),
      agged_row_cnt_(0),
      agged_dumped_cnt_(0),
      part_shift_(sizeof(uint64_t) * CHAR_BIT / 2),
      profile_(ObSqlWorkAreaType::HASH_WORK_AREA),
      sql_mem_processor_(profile_, op_monitor_info_),
      iter_end_(false),
      enable_dump_(false),
      force_dump_(false),
      batch_rows_from_dump_(NULL),
      hash_vals_(NULL),
      first_batch_from_store_(true),
      dup_groupby_exprs_(),
      is_dumped_(nullptr),
      no_non_distinct_aggr_(false),
      start_calc_hash_idx_(0),
      base_hash_vals_(nullptr),
      has_calc_base_hash_(false),
      distinct_data_set_(),
      distinct_origin_exprs_(),
      n_distinct_expr_(0),
      hash_funcs_(exec_ctx.get_allocator()),
      sort_collations_(exec_ctx.get_allocator()),
      cmp_funcs_(exec_ctx.get_allocator()),
      is_init_distinct_data_(false),
      use_distinct_data_(false),
      distinct_selector_(nullptr),
      distinct_hash_values_(nullptr),
      distinct_skip_(nullptr),
      distinct_profile_(ObSqlWorkAreaType::HASH_WORK_AREA),
      distinct_sql_mem_processor_(distinct_profile_, op_monitor_info_),
      bypass_ctrl_(),
      by_pass_group_batch_(nullptr),
      by_pass_batch_size_(0),
      by_pass_nth_group_(0),
      by_pass_child_brs_(nullptr),
      force_by_pass_(false),
      batch_old_rows_(nullptr),
      batch_new_rows_(nullptr),
      return_rows_(nullptr),
      group_expr_fixed_lengths_(exec_ctx.get_allocator()),
      use_sstr_aggr_(false),
      aggr_vectors_(nullptr),
      reorder_aggr_rows_(false),
      old_row_selector_(nullptr),
      batch_aggr_rows_table_(),
      llc_est_(),
      dump_add_row_selectors_(nullptr),
      dump_add_row_selectors_item_cnt_(nullptr),
      dump_vectors_(nullptr),
      dump_rows_(nullptr),
      need_reinit_vectors_(true)
  {
  }
  void reset(bool for_rescan);
  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_rescan() override;
  virtual int inner_switch_iterator() override;
  virtual int inner_get_next_row() override;
  virtual void destroy() override;

  // for batch
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  void calc_avg_group_mem();
  OB_INLINE void llc_add_value(int64_t hash_value)
  {
    ObAggregateProcessor::llc_add_value(hash_value, llc_est_.llc_map_);
    ++llc_est_.est_cnt_;
  }
  int bypass_add_llc_map_batch(bool ready_to_check_ndv);
  int check_llc_ndv();
  OB_INLINE int64_t get_hash_groupby_row_count() const
  {
    return local_group_rows_.size();
  }

  OB_INLINE int64_t get_aggr_used_size() const
  { return aggr_processor_.get_aggr_used_size(); }
  OB_INLINE int64_t get_aggr_hold_size() const
  { return aggr_processor_.get_aggr_hold_size(); }
  OB_INLINE int64_t get_hash_table_used_size() const
  { return local_group_rows_.mem_used(); }
  OB_INLINE int64_t get_dumped_part_used_size() const
  { return (NULL == mem_context_ ? 0 : mem_context_->used()); }
  OB_INLINE int64_t get_dump_part_hold_size() const
  { return (NULL == mem_context_ ? 0 : mem_context_->hold()); }
  OB_INLINE int64_t get_extra_size() const
  { return get_dumped_part_used_size(); }
  OB_INLINE int64_t get_data_size() const
  { return get_aggr_used_size() + sql_mem_processor_.get_data_size(); }
  OB_INLINE int64_t get_mem_used_size() const
  {
    // Hash table used is double counted here to reserve memory for hash table extension
    return get_aggr_used_size() + get_extra_size() + get_hash_table_used_size();
  }
  OB_INLINE int64_t get_actual_mem_used_size() const
  {
    return get_aggr_used_size() + get_extra_size();
  }
  OB_INLINE int64_t get_mem_bound_size() const
  { return sql_mem_processor_.get_mem_bound(); }
  OB_INLINE bool is_need_dump(double data_ratio)
  {
    return (get_mem_used_size() > get_mem_bound_size() * data_ratio);
  }
  OB_INLINE int64_t estimate_hash_bucket_size(const int64_t bucket_cnt) const
  {
    return next_pow2(ObExtendHashTableVec<ObGroupRowBucket>::SIZE_BUCKET_SCALE * bucket_cnt)
           * local_group_rows_.get_bucket_size();
  }
  OB_INLINE int64_t estimate_hash_bucket_cnt_by_mem_size(const int64_t bucket_cnt,
      const int64_t max_mem_size, const double extra_ratio) const
  {
    int64_t mem_size = estimate_hash_bucket_size(bucket_cnt);
    int64_t max_hash_size = max_mem_size * extra_ratio;
    if (0 < max_hash_size) {
      while (mem_size > max_hash_size) {
        mem_size >>= 1;
      }
    }
    return (mem_size / local_group_rows_.get_bucket_size() / ObExtendHashTableVec<ObGroupRowBucket>::SIZE_BUCKET_SCALE);
  }
  int reinit_group_store();
  int update_mem_status_periodically(const int64_t nth_cnt, const int64_t input_row,
                                     int64_t &est_part_cnt, bool &need_dump);
  int64_t detect_part_cnt(const int64_t rows) const;
  void calc_data_mem_ratio(const int64_t part_cnt, double &data_ratio);
  void adjust_part_cnt(int64_t &part_cnt);
  int alloc_group_row(const int64_t group_id, aggregate::AggrRowPtr row);
  bool need_start_dump(const int64_t input_rows, int64_t &est_part_cnt, const bool check_dump);
  // Setup: memory entity, bloom filter, spill partitions
  int setup_dump_env(const int64_t part_id, const int64_t input_rows,
                     DatumStoreLinkPartition **parts, int64_t &part_cnt,
                     ObGbyBloomFilterVec *&bloom_filter);

  int cleanup_dump_env(const bool dump_success, const int64_t part_id,
                       DatumStoreLinkPartition **parts, int64_t &part_cnt,
                       ObGbyBloomFilterVec *&bloom_filter);
  void destroy_all_parts();
  int init_mem_context(void);

private:
  int finish_insert_distinct_data();
  int init_distinct_info(bool is_part);
  void reset_distinct_info();

  int batch_insert_distinct_data(const ObBatchRows &child_brs);
  int batch_insert_all_distinct_data(const int64_t batch_size);
  int get_next_batch_distinct_rows(const int64_t batch_size, const ObBatchRows *&child_brs);

  int next_duplicate_data_permutation(int64_t &nth_group, bool &last_group,
                                      const ObBatchRows *child_brs, bool &insert_group_ht);
  int batch_process_duplicate_data(const ObCompactRow **store_rows,
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
                                  bool &process_check_dump);
  int load_data_batch(int64_t max_row_cnt);
  int switch_part(DatumStoreLinkPartition *&cur_part,
                  ObTempRowStore::Iterator &row_store_iter,
                  int64_t &part_id,
                  int64_t &part_shift,
                  int64_t &input_rows,
                  int64_t &intput_size);
  int next_batch(bool is_from_row_store,
                 ObTempRowStore::Iterator &row_store_iter,
                 int64_t max_row_cnt,
                 const ObBatchRows *&child_brs);
  int eval_groupby_exprs_batch(const ObCompactRow **store_rows,
                               const RowMeta *meta,
                               const ObBatchRows &child_brs);
  int calc_groupby_exprs_hash_batch(ObIArray<ObExpr *> &groupby_exprs,
                                     const ObBatchRows &child_brs);

  int group_child_batch_rows(const ObCompactRow **store_rows,
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
                             ObGbyBloomFilterVec *&bloom_filter);
  // need dump for group and distinct data
  bool is_need_dump_all();
  int init_by_pass_group_batch_item();
  int by_pass_restart_round();
  int init_distinct_ht();
  int64_t get_input_rows() const;
  int64_t get_input_size() const;
  void check_groupby_exprs(const common::ObIArray<ObExpr *> &groupby_exprs, bool &nullable, bool &all_int64);
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObHashGroupByVecOp);

private:
  int by_pass_prepare_one_batch(const int64_t batch_size);
  int by_pass_get_next_permutation_batch(int64_t &nth_group, bool &last_group,
                                         const ObBatchRows *child_brs, ObBatchRows &my_brs,
                                         const int64_t batch_size, bool &insert_group_ht);
  void reuse_dump_selectors();
  int init_by_pass_op();

  int process_multi_groups(aggregate::AggrRowPtr *agg_rows, const ObBatchRows &brs);
  // Alloc one batch group_row_item at a time
  static const int64_t BATCH_GROUP_ITEM_SIZE = 16;
  // const int64_t EXTEND_BKT_NUM_PUSH_DOWN = INIT_L3_CACHE_SIZE / ObExtendHashTableVec<ObGroupRowBucket>::get_sizeof_aggr_row();
  ObAggrHashTableWapper local_group_rows_;
  int64_t curr_group_id_;

  // record current index of next group_row_item when alloc one batch group_row_item
  int64_t cur_group_item_idx_;
  char *cur_group_item_buf_;

  // memory allocator for group by partitions
  lib::MemoryContext mem_context_;
  ObDList<DatumStoreLinkPartition> dumped_group_parts_;

  int64_t agged_group_cnt_;
  int64_t agged_row_cnt_;
  int64_t agged_dumped_cnt_;
  int64_t part_shift_;
  ObSqlWorkAreaProfile profile_;
  ObSqlMemMgrProcessor sql_mem_processor_;
  bool iter_end_;
  bool enable_dump_;
  bool force_dump_;

  // for batch
  const ObCompactRow **batch_rows_from_dump_;
  ObBatchRows dumped_batch_rows_;
  uint64_t *hash_vals_;
  bool first_batch_from_store_;
  // for batch end

  // for three-stage
  ObSEArray<ObExpr*, 4> dup_groupby_exprs_;
  ObSEArray<ObExpr*, 4> all_groupby_exprs_;
  bool *is_dumped_;
  bool no_non_distinct_aggr_;
  int64_t start_calc_hash_idx_;
  uint64_t *base_hash_vals_;
  bool has_calc_base_hash_;

  // for second stage in three-stage
  ObHashPartInfrastructureVecImpl distinct_data_set_;
  // distinct exprs including groupby exprs
  ObSEArray<ObExpr*, 4> distinct_origin_exprs_;
  // The total distinct exprs that the left exprs need to save
  int64_t n_distinct_expr_;
  // for distinct data
  ObHashFuncs hash_funcs_;
  ObSortCollations sort_collations_;
  common::ObCmpFuncs cmp_funcs_;
  // whether it has distinct data
  bool is_init_distinct_data_;
  // whether it get distinct data when has distinct data
  bool use_distinct_data_;
  uint16_t *distinct_selector_;
  uint64_t *distinct_hash_values_;
  ObBitVector *distinct_skip_;
  // for simple process, use different profile and mem_processor to process AMM
  ObSqlWorkAreaProfile distinct_profile_;
  ObSqlMemMgrProcessor distinct_sql_mem_processor_;
  ObAdaptiveByPassCtrl bypass_ctrl_;
  aggregate::AggrRowPtr *by_pass_group_batch_;
  int64_t by_pass_batch_size_;
  int64_t by_pass_nth_group_;
  const ObBatchRows *by_pass_child_brs_;
  ObVectorsResultHolder by_pass_vec_holder_;
  bool force_by_pass_;
  // for vec2.0
  aggregate::AggrRowPtr *batch_old_rows_;
  aggregate::AggrRowPtr *batch_new_rows_;
  const ObCompactRow **return_rows_;
  common::ObFixedArray<int64_t, common::ObIAllocator> group_expr_fixed_lengths_;
  ObBitVector *batch_deduplicate_;
  bool disable_batch_add_;
  bool use_sstr_aggr_;
  ObIVector **aggr_vectors_;
  bool reorder_aggr_rows_;
  uint16_t *old_row_selector_;
  BatchAggrRowsTable batch_aggr_rows_table_;
  LlcEstimate llc_est_;
  uint16_t **dump_add_row_selectors_;
  uint16_t *dump_add_row_selectors_item_cnt_;
  common::ObFixedArray<ObIVector *, common::ObIAllocator> dump_vectors_;
  ObCompactRow **dump_rows_;
  bool need_reinit_vectors_;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_BASIC_OB_HASH_GROUPBY_VEC_H_
