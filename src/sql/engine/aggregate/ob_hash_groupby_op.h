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

#ifndef OCEANBASE_BASIC_OB_HASH_GROUPBY_OP_H_
#define OCEANBASE_BASIC_OB_HASH_GROUPBY_OP_H_

#include "common/row/ob_row_store.h"
#include "sql/engine/aggregate/ob_groupby_op.h"
#include "sql/engine/aggregate/ob_exec_hash_struct.h"
#include "lib/list/ob_list.h"
#include "lib/list/ob_dlink_node.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"
#include "sql/engine/basic/ob_hash_partitioning_infrastructure_op.h"
#include "sql/engine/aggregate/ob_aggregate_processor.h"
#include "sql/engine/aggregate/ob_adaptive_bypass_ctrl.h"
#include "sql/engine/aggregate/ob_hash_groupby_vec_op.h"

namespace oceanbase
{
namespace sql
{

struct ObGroupByDupColumnPair
{
  OB_UNIS_VERSION_V(1);
public:
  ObExpr *org_expr;
  ObExpr *dup_expr;
};

class ObHashGroupBySpec : public ObGroupBySpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObHashGroupBySpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
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
  DISALLOW_COPY_AND_ASSIGN(ObHashGroupBySpec);

public:
  ExprFixedArray group_exprs_;   //group by column
  ObCmpFuncs cmp_funcs_;
  int64_t est_group_cnt_;
  common::ObFixedArray<ObExpr*, common::ObIAllocator> org_dup_cols_;
  common::ObFixedArray<ObExpr*, common::ObIAllocator> new_dup_cols_;
  common::ObFixedArray<int64_t, common::ObIAllocator> dist_col_group_idxs_;
  ExprFixedArray distinct_exprs_; // the distinct arguments of aggregate function
};

//Used for calc hash for columns
class ObGroupRowItem
{
public:
  ObGroupRowItem()
    : next_(0),
      hash_(0),
      group_row_(NULL),
      groupby_store_row_(NULL),
      group_row_count_in_batch_(0),
      group_row_offset_in_selector_(0)
  {
  }

  ~ObGroupRowItem() {}
  inline uint64_t hash() const { return hash_; }
  inline int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  ObGroupRowItem *&next() { return next_; }

  TO_STRING_KV(KP(next_), K(hash_), KP_(group_row),
               K_(group_row_count_in_batch), K_(group_row_offset_in_selector));

public:
  ObGroupRowItem *next_;
  uint64_t hash_;
  union {
    struct {
      uint64_t batch_idx_:63; // batch index
      uint64_t is_expr_row_:1; // group by data is on expr, not in %group_row_
    };
    ObAggregateProcessor::GroupRow *group_row_;
  };

  // redundant GroupRow::groupby_store_row_ to simplify the batch prefetch
  ObChunkDatumStore::StoredRow *groupby_store_row_;

  uint16_t group_row_count_in_batch_;
  uint16_t group_row_offset_in_selector_;
};

class ObGroupRowHashTable : public ObExtendHashTable<ObGroupRowItem>
{
public:
  ObGroupRowHashTable() : ObExtendHashTable(), eval_ctx_(nullptr), cmp_funcs_(nullptr) {}

  OB_INLINE const ObGroupRowItem *get(const ObGroupRowItem &item);
  OB_INLINE void prefetch(const ObBatchRows &brs, uint64_t *hash_vals) const;
  int init(ObIAllocator *allocator,
          lib::ObMemAttr &mem_attr,
          const common::ObIArray<ObExpr *> &gby_exprs,
          ObEvalCtx *eval_ctx,
          const common::ObIArray<ObCmpFunc> *cmp_funcs,
          int64_t initial_size = INITIAL_SIZE);
  int add_hashval_to_llc_map(LlcEstimate &llc_est);
private:
  int likely_equal(const ObGroupRowItem &left, const ObGroupRowItem &right, bool &result) const;
private:
  const common::ObIArray<ObExpr *> *gby_exprs_;
  ObEvalCtx *eval_ctx_;
  const common::ObIArray<ObCmpFunc> *cmp_funcs_;
  static const int64_t HASH_BUCKET_PREFETCH_MAGIC_NUM = 4 * 1024;
};

OB_INLINE const ObGroupRowItem *ObGroupRowHashTable::get(const ObGroupRowItem &item)
{
  ObGroupRowItem *res = NULL;
  int ret = OB_SUCCESS;
  bool result = false;
  ++probe_cnt_;
  if (OB_UNLIKELY(NULL == buckets_)) {
    // do nothing
  } else {
    const uint64_t hash_val = item.hash();
    ObGroupRowItem *it = locate_bucket(*buckets_, hash_val).item_;
    while (NULL != it && OB_SUCC(ret)) {
      if (OB_FAIL(likely_equal(*it, item, result))) {
        LOG_WARN("failed to cmp", K(ret));
      } else if (result) {
        res = it;
        break;
      }
      it = it->next();
    }
  }
  return res;
}

OB_INLINE void ObGroupRowHashTable::prefetch(const ObBatchRows &brs, uint64_t *hash_vals) const
{
  if (OB_UNLIKELY(NULL == buckets_)) {
    // do nothing
  } else if (buckets_->count() <= HASH_BUCKET_PREFETCH_MAGIC_NUM) {
    // stop prefetching if hashtable is not big enough
  } else {
    auto mask = get_bucket_num() - 1;
    for(auto i = 0; i < brs.size_; i++) {
      if (brs.skip_->at(i)) {
        continue;
      }
      __builtin_prefetch((&buckets_->at(hash_vals[i] & mask)),
                         0/* read */, 2 /*high temp locality*/);
    }
    for(auto i = 0; i < brs.size_; i++) {
      if (brs.skip_->at(i)) {
        continue;
      }
      __builtin_prefetch((buckets_->at(hash_vals[i] & mask).item_),
                         0/* read */, 2 /*high temp locality*/);
    }
    for(auto i = 0; i < brs.size_; i++) {
      auto item = buckets_->at(hash_vals[i] & mask).item_;
      if (brs.skip_->at(i) || OB_ISNULL(item) || OB_ISNULL(item->groupby_store_row_)) {
        continue;
      }
      __builtin_prefetch(item->groupby_store_row_,
                         0/* read */, 2 /*high temp locality*/);
    }
  }
}

// 输入数据已经按照groupby列排序
class ObHashGroupByOp : public ObGroupByOp
{
public:
  struct DatumStoreLinkPartition : public common::ObDLinkBase<DatumStoreLinkPartition>
  {
  public:
    DatumStoreLinkPartition(common::ObIAllocator *alloc = nullptr)
      : datum_store_(ObModIds::OB_HASH_NODE_GROUP_ROWS, alloc), part_id_(0), part_shift_(0)
    {}
    ObChunkDatumStore datum_store_;
    int64_t part_id_;
    int64_t part_shift_;
  };

public:
  static const int64_t MIN_PARTITION_CNT = 8;
  static const int64_t MAX_PARTITION_CNT = 256;
  static const int64_t INIT_BKT_SIZE_FOR_ADAPTIVE_GBY = 256;

  // min in memory groups
  static const int64_t MIN_INMEM_GROUPS = 4;
  static const int64_t MIN_GROUP_HT_INIT_SIZE = 1 << 10; // 1024
  static const int64_t MAX_GROUP_HT_INIT_SIZE = 1 << 20; // 1048576
  static constexpr const double MAX_PART_MEM_RATIO = 0.5;
  static constexpr const double EXTRA_MEM_RATIO = 0.25;
  static const int64_t FIX_SIZE_PER_PART = sizeof(DatumStoreLinkPartition) + ObChunkRowStore::BLOCK_SIZE;


public:
  ObHashGroupByOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObGroupByOp(exec_ctx, spec, input),
      curr_group_id_(common::OB_INVALID_INDEX),
      cur_group_item_idx_(0),
      cur_group_item_buf_(nullptr),
      mem_context_(NULL),
      group_store_(ObModIds::OB_HASH_NODE_GROUP_ROWS),
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
      gri_cnt_per_batch_(0),
      gris_per_batch_(NULL),
      first_batch_from_store_(true),
      batch_row_gri_ptrs_(NULL),
      selector_array_(NULL),
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
      by_pass_group_row_(nullptr),
      by_pass_group_batch_(nullptr),
      by_pass_batch_size_(0),
      by_pass_nth_group_(0),
      last_child_row_(nullptr),
      by_pass_child_brs_(nullptr),
      force_by_pass_(false),
      llc_est_()
  {
  }
  void reset();
  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int inner_rescan() override;
  virtual int inner_switch_iterator() override;
  virtual int inner_get_next_row() override;
  virtual void destroy() override;
  int load_data();
  int load_one_row();

  // for batch
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  void calc_avg_group_mem();
  OB_INLINE void llc_add_value(int64_t hash_value)
  {
    ObAggregateProcessor::llc_add_value(hash_value, llc_est_.llc_map_);
    ++llc_est_.est_cnt_;
  }
  int bypass_add_llc_map(uint64_t hash_val, bool ready_to_check_ndv);
  int bypass_add_llc_map_batch(bool ready_to_check_ndv);
  int check_llc_ndv();
  int check_same_group(int64_t &diff_pos);
  int restore_groupby_datum(const int64_t diff_pos);
  int rollup_and_calc_results(const int64_t group_id);

  int64_t get_hash_groupby_row_count() const { return local_group_rows_.size(); }

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
    return next_pow2(ObGroupRowHashTable::SIZE_BUCKET_SCALE * bucket_cnt)
           * sizeof(void*);
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
    return (mem_size / sizeof(void*) / ObGroupRowHashTable::SIZE_BUCKET_SCALE);
  }
  int init_group_store();
  int update_mem_status_periodically(const int64_t nth_cnt, const int64_t input_row,
                                     int64_t &est_part_cnt, bool &need_dump);
  int64_t detect_part_cnt(const int64_t rows) const;
  void calc_data_mem_ratio(const int64_t part_cnt, double &data_ratio);
  void adjust_part_cnt(int64_t &part_cnt);
  int calc_groupby_exprs_hash(ObIArray<ObExpr*> &groupby_exprs,
                              const ObChunkDatumStore::StoredRow *srow,
                              uint64_t &hash_value);
  int alloc_group_item(ObGroupRowItem *&item);
  int alloc_group_row(const int64_t group_id, ObGroupRowItem &item);
  int init_group_row_item(const uint64_t &hash_val,
                          ObGroupRowItem *&gr_row_item);
  bool need_start_dump(const int64_t input_rows, int64_t &est_part_cnt, const bool check_dump);
  // Setup: memory entity, bloom filter, spill partitions
  int setup_dump_env(const int64_t part_id, const int64_t input_rows,
                     DatumStoreLinkPartition **parts, int64_t &part_cnt,
                     ObGbyBloomFilter *&bloom_filter);

  int cleanup_dump_env(const bool dump_success, const int64_t part_id,
                       DatumStoreLinkPartition **parts, int64_t &part_cnt,
                       ObGbyBloomFilter *&bloom_filter);
  void destroy_all_parts();
  int restore_groupby_datum();
  int init_mem_context(void);

private:
  int get_next_distinct_row();
  int insert_all_distinct_data();
  int insert_distinct_data();
  int finish_insert_distinct_data();
  int init_distinct_info(bool is_part);
  void reset_distinct_info();

  int batch_insert_distinct_data(const ObBatchRows &child_brs);
  int batch_insert_all_distinct_data(const int64_t batch_size);
  int get_next_batch_distinct_rows(const int64_t batch_size, const ObBatchRows *&child_brs);

  int next_duplicate_data_permutation(int64_t &nth_group, bool &last_group,
                                      const ObBatchRows *child_brs, bool &insert_group_ht);
  int batch_process_duplicate_data(const ObChunkDatumStore::StoredRow **store_rows,
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
                                  bool &process_check_dump);
  int load_data_batch(int64_t max_row_cnt);
  int switch_part(DatumStoreLinkPartition *&cur_part,
                  ObChunkDatumStore::Iterator &row_store_iter,
                  int64_t &part_id,
                  int64_t &part_shift,
                  int64_t &input_rows,
                  int64_t &intput_size);
  int next_batch(bool is_from_row_store,
                 ObChunkDatumStore::Iterator &row_store_iter,
                 int64_t max_row_cnt,
                 const ObBatchRows *&child_brs);
  int eval_groupby_exprs_batch(const ObChunkDatumStore::StoredRow **store_rows,
                               const ObBatchRows &child_brs);
  void calc_groupby_exprs_hash_batch(ObIArray<ObExpr *> &groupby_exprs,
                                     const ObBatchRows &child_brs);

  int group_child_batch_rows(const ObChunkDatumStore::StoredRow **store_rows,
                             const int64_t input_rows,
                             const bool check_dump,
                             const int64_t part_id,
                             const int64_t part_shift,
                             const int64_t loop_cnt,
                             const ObBatchRows &child_brs,
                             int64_t &part_cnt,
                             DatumStoreLinkPartition **parts,
                             int64_t &est_part_cnt,
                             ObGbyBloomFilter *&bloom_filter);
  int set_group_row_item(ObGroupRowItem &cur_item, int64_t batch_idx)
  {
    int ret = OB_SUCCESS;
    // set both hash table and array if array is valid.
    if (OB_FAIL(local_group_rows_.set(cur_item))) {
      SQL_ENG_LOG(WARN, "hash table set failed", K(ret));
    } else if (group_rows_arr_.is_valid_) {
      group_rows_arr_.set(cur_item, batch_idx);
    }
    return ret;
  }
  // need dump for group and distinct data
  bool is_need_dump_all();
  int init_by_pass_group_row_item();
  int init_by_pass_group_batch_item();
  int64_t get_curr_data_size() { return group_store_.get_mem_used(); }
  int64_t get_curr_hash_table_size() { return sizeof(ObGroupRowItem) * local_group_rows_.size(); }
  int by_pass_restart_round();
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObHashGroupByOp);

  struct ObGroupRowArray
  {
  public:
    ObGroupRowArray() : inited_(false), is_valid_(false), array_size_(0),
    groupby_cnt_(0), grs_array_(NULL) {}
    const ObGroupRowItem *get(int64_t batch_idx)
    {
      ObGroupRowItem *res = NULL;
      if (is_ascii_value(batch_idx)) {
        res = grs_array_[(groupby_values_[1] << 8) + groupby_values_[0]];
      } else {
        is_valid_ = false;
      }
      return res;
    }
    void set(ObGroupRowItem &cur_item, int64_t batch_idx)
    {
      if (is_ascii_value(batch_idx)) {
        grs_array_[(groupby_values_[1] << 8) + groupby_values_[0]] = &cur_item;
      } else {
        is_valid_ = false;
      }
    }
    bool is_ascii_value(const int64_t batch_idx)
    {
      bool res = true;
      for (int64_t i = 0; i < groupby_cnt_ && res; i++) {
        ObDatum &datum = groupby_datums_[i][batch_idx];
        if (datum.is_null() || 1 != datum.len_) {
          res = false;
        } else {
          groupby_values_[i] = static_cast<uint16_t>(static_cast<uint8_t>(*datum.ptr_));
        }
      }
      return res;
    }
    void reuse()
    {
      if (inited_) {
        is_valid_ = true;
        MEMSET(grs_array_, 0, sizeof(ObGroupRowItem *) * array_size_);
      }
    }
    int init(ObIAllocator &allocator, ObEvalCtx &eval_ctx, const ObIArray<ObExpr*> &group_exprs)
    {
      int ret = OB_SUCCESS;
      if (OB_UNLIKELY(group_exprs.count() > 2 || 0 == group_exprs.count())) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "only support group by one or two exprs", K(ret));
      } else if (FALSE_IT(array_size_ = 1 == group_exprs.count() ? 256 : 256 * 256)) {
      } else if (OB_ISNULL(grs_array_ = static_cast<ObGroupRowItem **>(allocator.alloc(
                                                    sizeof(ObGroupRowItem *) * array_size_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_ENG_LOG(WARN, "allocate group row array memory failed", K(ret));
      } else {
        groupby_cnt_ = group_exprs.count();
        inited_ = true;
        is_valid_ = true;
        ObExpr *first_expr = group_exprs.at(0);
        groupby_values_[0] = 0;
        groupby_values_[1] = 0;
        groupby_datums_[0] = first_expr->locate_batch_datums(eval_ctx);
        groupby_datums_[1] = 1 == group_exprs.count() ? NULL
                              : group_exprs.at(1)->locate_batch_datums(eval_ctx);
        MEMSET(grs_array_, 0, sizeof(ObGroupRowItem *) * array_size_);
      }
      return ret;
    }
    bool inited_; 
    bool is_valid_;
    int64_t array_size_;
    int64_t groupby_cnt_;
    uint16_t groupby_values_[2];
    ObDatum *groupby_datums_[2];
    // This is a (ObGroupRowItem *) array and size is 256 ^ group_exprs_count
    ObGroupRowItem **grs_array_;
  };

private:
  int by_pass_prepare_one_batch(const int64_t batch_size);
  int by_pass_get_next_permutation(int64_t &nth_group, bool &last_group, bool &insert_group_ht);
  int by_pass_get_next_permutation_batch(int64_t &nth_group, bool &last_group,
                                         const ObBatchRows *child_brs,
                                         ObBatchRows &my_brs,
                                         bool &insert_group_ht);
  int init_by_pass_op();
  int64_t get_input_rows() const;
  int64_t get_input_size() const;
  // Alloc one batch group_row_item at a time
  static const int64_t BATCH_GROUP_ITEM_SIZE = 16;
  const int64_t EXTEND_BKT_NUM_PUSH_DOWN = INIT_L3_CACHE_SIZE / sizeof(ObGroupRowItem);
  ObGroupRowHashTable local_group_rows_;
  // Optimization for group by c1, c2. type of c1 and c2 are both char(1).
  // In this case, if all values of c1 and c2 are ascii character,
  // there are only 256 available values of c1 or c2,
  // we can replace hash table with an array[256 * 256].
  // go back to hash table if null or non-ascii character found.
  ObGroupRowArray group_rows_arr_;
  int64_t curr_group_id_;

  // record current index of next group_row_item when alloc one batch group_row_item
  int64_t cur_group_item_idx_;
  char *cur_group_item_buf_;

  // memory allocator for group by partitions
  lib::MemoryContext mem_context_;
  ObDList<DatumStoreLinkPartition> dumped_group_parts_;
  ObChunkDatumStore group_store_;

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
  const ObChunkDatumStore::StoredRow **batch_rows_from_dump_;
  ObBatchRows dumped_batch_rows_;
  uint64_t *hash_vals_;
  int32_t gri_cnt_per_batch_;
  const ObGroupRowItem **gris_per_batch_;
  bool first_batch_from_store_;
  const ObGroupRowItem **batch_row_gri_ptrs_; // record ObGroupRowItem* of each row_id in a batch
  uint16_t *selector_array_;
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
  ObHashPartInfrastructure<ObHashPartCols, ObHashPartStoredRow> distinct_data_set_;
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
  ObGroupRowItem *by_pass_group_row_;
  ObAggregateProcessor::GroupRow **by_pass_group_batch_;
  int64_t by_pass_batch_size_;
  int64_t by_pass_nth_group_;
  ObChunkDatumStore::LastStoredRow *last_child_row_;
  const ObBatchRows *by_pass_child_brs_;
  ObBatchResultHolder by_pass_brs_holder_;
  bool force_by_pass_;
  LlcEstimate llc_est_;
};

} // end namespace sql
} // end namespace oceanbase

#endif // OCEANBASE_BASIC_OB_HASH_GROUPBY_OP_H_
