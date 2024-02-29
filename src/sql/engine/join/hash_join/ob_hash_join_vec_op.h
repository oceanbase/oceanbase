/** * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef SRC_SQL_ENGINE_JOIN_HASH_JOIN_OB_HASH_JOIN_VEC_OP_H_
#define SRC_SQL_ENGINE_JOIN_HASH_JOIN_OB_HASH_JOIN_VEC_OP_H_

#include "lib/container/ob_bit_set.h"
#include "lib/container/ob_2d_array.h"
#include "lib/lock/ob_scond.h"
#include "share/datum/ob_datum_funcs.h"
#include "sql/engine/join/ob_join_vec_op.h"
#include "sql/engine/join/hash_join/ob_hj_partition_mgr.h"
#include "sql/engine/join/hash_join/join_hash_table.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"
#include "sql/engine/aggregate/ob_exec_hash_struct.h"
#include "sql/engine/aggregate/ob_adaptive_bypass_ctrl.h"

namespace oceanbase
{
namespace sql
{
class ObHJPartitionMgr;

class ObHashJoinVecInput : public ObOpInput
{
  OB_UNIS_VERSION_V(1);
private:
  enum SyncValueMode {
    MIN_MODE,
    MAX_MODE,
    FIRST_MODE,
    MAX
  };
  using EventPred = std::function<void(int64_t n_times)>;
public:
  ObHashJoinVecInput(ObExecContext &ctx, const ObOpSpec &spec)
    : ObOpInput(ctx, spec),
      shared_hj_info_(0),
      task_id_(0)
  {}
  virtual ~ObHashJoinVecInput() {}
  virtual int init(ObTaskInfo &task_info)
  {
    int ret = OB_SUCCESS;
    UNUSED(task_info);
    return ret;
  }

  int sync_wait(ObExecContext &ctx, int64_t &sys_event, EventPred pred, bool ignore_interrupt = false, bool is_open = false);
  int64_t get_sync_val()
  {
    ObHJSharedTableInfo *shared_hj_info = reinterpret_cast<ObHJSharedTableInfo *>(shared_hj_info_);
    return ATOMIC_LOAD(&shared_hj_info->sync_val_);
  }
  int64_t get_total_memory_row_count()
  {
    ObHJSharedTableInfo *shared_hj_info = reinterpret_cast<ObHJSharedTableInfo *>(shared_hj_info_);
    return ATOMIC_LOAD(&shared_hj_info->total_memory_row_count_);
  }
  int64_t get_total_memory_size()
  {
    ObHJSharedTableInfo *shared_hj_info = reinterpret_cast<ObHJSharedTableInfo *>(shared_hj_info_);
    return ATOMIC_LOAD(&shared_hj_info->total_memory_size_);
  }
  int64_t get_null_in_naaj()
  {
    ObHJSharedTableInfo *shared_hj_info = reinterpret_cast<ObHJSharedTableInfo *>(shared_hj_info_);
    return ATOMIC_LOAD(&shared_hj_info->read_null_in_naaj_);
  }
  int64_t get_non_preserved_side_naaj()
  {
    ObHJSharedTableInfo *shared_hj_info = reinterpret_cast<ObHJSharedTableInfo *>(shared_hj_info_);
    return ATOMIC_LOAD(&shared_hj_info->non_preserved_side_is_not_empty_);
  }
  void sync_basic_info(int64_t n_times, int64_t row_count, int64_t input_size)
  {
    ObHJSharedTableInfo *shared_hj_info = reinterpret_cast<ObHJSharedTableInfo *>(shared_hj_info_);
    if (0 == n_times) {
      ATOMIC_SET(&shared_hj_info->total_memory_row_count_, row_count);
      ATOMIC_SET(&shared_hj_info->total_memory_size_, input_size);
    } else {
      ATOMIC_AAF(&shared_hj_info->total_memory_row_count_, row_count);
      ATOMIC_AAF(&shared_hj_info->total_memory_size_, input_size);
    }
    OB_LOG(DEBUG, "set basic info", K(shared_hj_info->total_memory_row_count_),
      K(shared_hj_info->total_memory_size_));
  }
  void sync_info_for_naaj(int64_t n_times, bool null_in_naal, bool non_preserved_side_naaj)
  {
    ObHJSharedTableInfo *shared_hj_info = reinterpret_cast<ObHJSharedTableInfo *>(shared_hj_info_);
    if (0 == n_times) {
      ATOMIC_SET(&shared_hj_info->read_null_in_naaj_, null_in_naal);
      ATOMIC_SET(&shared_hj_info->non_preserved_side_is_not_empty_, non_preserved_side_naaj);
    } else {
      if (!ATOMIC_LOAD(&shared_hj_info->read_null_in_naaj_)) {
        ATOMIC_SET(&shared_hj_info->read_null_in_naaj_, null_in_naal);
      }
      if (!ATOMIC_LOAD(&shared_hj_info->non_preserved_side_is_not_empty_)) {
        ATOMIC_SET(&shared_hj_info->non_preserved_side_is_not_empty_, non_preserved_side_naaj);
      }
    }
    OB_LOG(DEBUG, "set basic info", K(shared_hj_info->total_memory_row_count_),
      K(shared_hj_info->total_memory_size_));
  }

  int64_t &get_sqc_thread_count()
  {
    ObHJSharedTableInfo *shared_hj_info = reinterpret_cast<ObHJSharedTableInfo *>(shared_hj_info_);
    return shared_hj_info->sqc_thread_count_;
  }
  int64_t &get_process_cnt()
  {
    ObHJSharedTableInfo *shared_hj_info = reinterpret_cast<ObHJSharedTableInfo *>(shared_hj_info_);
    return shared_hj_info->process_cnt_;
  }
  int64_t &get_close_cnt()
  {
    ObHJSharedTableInfo *shared_hj_info = reinterpret_cast<ObHJSharedTableInfo *>(shared_hj_info_);
    return shared_hj_info->close_cnt_;
  }

  int64_t &get_open_cnt()
  {
    ObHJSharedTableInfo *shared_hj_info = reinterpret_cast<ObHJSharedTableInfo *>(shared_hj_info_);
    return shared_hj_info->open_cnt_;
  }

  ObHJSharedTableInfo *get_shared_hj_info()
  {
    return reinterpret_cast<ObHJSharedTableInfo *>(shared_hj_info_);
  }
  void set_error_code(int in_ret)
  {
    ObHJSharedTableInfo *shared_hj_info = reinterpret_cast<ObHJSharedTableInfo *>(shared_hj_info_);
    ATOMIC_SET(&shared_hj_info->ret_, in_ret);
  }

  void set_open_ret(int in_ret)
  {
    ObHJSharedTableInfo *shared_hj_info = reinterpret_cast<ObHJSharedTableInfo *>(shared_hj_info_);
    ATOMIC_SET(&shared_hj_info->open_ret_, in_ret);
  }

  virtual void reset() override
  {
    if (0 != shared_hj_info_) {
      ObHJSharedTableInfo *shared_hj_info = reinterpret_cast<ObHJSharedTableInfo *>(shared_hj_info_);
      int64_t task_cnt = shared_hj_info->sqc_thread_count_;

      shared_hj_info->total_memory_row_count_ = 0;
      shared_hj_info->total_memory_size_ = 0;
    }
  }
  int init_shared_hj_info(ObIAllocator &alloc, int64_t task_cnt)
  {
    int ret = OB_SUCCESS;
    ObHJSharedTableInfo *shared_hj_info = nullptr;
    if (OB_ISNULL(shared_hj_info = reinterpret_cast<ObHJSharedTableInfo *>(alloc.alloc(sizeof(ObHJSharedTableInfo))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      shared_hj_info_ = reinterpret_cast<int64_t>(shared_hj_info);
      shared_hj_info->sqc_thread_count_ = task_cnt;

      shared_hj_info->process_cnt_ = 0;
      shared_hj_info->close_cnt_ = 0;
      shared_hj_info->open_cnt_ = 0;
      shared_hj_info->ret_ = OB_SUCCESS;
      shared_hj_info->open_ret_ = OB_SUCCESS;
      shared_hj_info->read_null_in_naaj_ = false;
      new (&shared_hj_info->cond_)common::SimpleCond(common::ObWaitEventIds::SQL_SHARED_HJ_COND_WAIT);
      new (&shared_hj_info->lock_)ObSpinLock(common::ObLatchIds::SQL_SHARED_HJ_COND_LOCK);
      reset();
    }
    return ret;
  }

  void sync_set_min(int64_t n_times, int64_t val)
  {
    sync_set_val(n_times, val, SyncValueMode::MIN_MODE);
  }
  void sync_set_max(int64_t n_times, int64_t val)
  {
    sync_set_val(n_times, val, SyncValueMode::MAX_MODE);
  }
  void sync_set_first(int64_t n_times, int64_t val)
  {
    sync_set_val(n_times, val, SyncValueMode::FIRST_MODE);
  }
  void sync_set_val(int64_t n_times, int64_t val, SyncValueMode val_mode)
  {
    ObHJSharedTableInfo *shared_hj_info = reinterpret_cast<ObHJSharedTableInfo *>(shared_hj_info_);
    if (0 == n_times) {
      // the first
      ATOMIC_SET(&shared_hj_info->init_val_, val);
    } else {
      // others
      if (SyncValueMode::MIN_MODE == val_mode) {
        // set min
        if (ATOMIC_LOAD(&shared_hj_info->init_val_) > val) {
          ATOMIC_SET(&shared_hj_info->init_val_, val);
        }
      } else if (SyncValueMode::MAX_MODE == val_mode) {
        // set max
        if (ATOMIC_LOAD(&shared_hj_info->init_val_) < val) {
          ATOMIC_SET(&shared_hj_info->init_val_, val);
        }
      } else if (SyncValueMode::FIRST_MODE == val_mode) {
      } else {
        OB_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "the value mode is not supported", K(val_mode));
      }
      if (n_times + 1 >= shared_hj_info->sqc_thread_count_) {
        // last time, set final value
        ATOMIC_SET(&shared_hj_info->sync_val_, shared_hj_info->init_val_);
      }
    }
    OB_LOG(TRACE, "sync cur part_count", K(n_times),
      K(shared_hj_info->init_val_),
      K(shared_hj_info->sync_val_));
  }

  void set_task_id(int64_t task_id) { task_id_ = task_id; }
  int64_t get_task_id() { return task_id_; }
public:
  uint64_t shared_hj_info_;
  int64_t task_id_;
};

class ObHashJoinVecSpec : public ObJoinVecSpec
{
OB_UNIS_VERSION_V(1);
public:
  ObHashJoinVecSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type);

  // all_exprs组成:(all_left_exprs keys, all_right_exprs keys)
  // 前面的all_xxx_exprs keys没有去重
  // 同理hash_funcs也保存了left和right的join key对应hash function
  // 为什么保存left和right分别保存，因为可能存在类型不一致情况，如sint = usint
  ExprFixedArray join_conds_;
  ExprFixedArray build_keys_;
  ExprFixedArray probe_keys_;
  common::ObFixedArray<int64_t, common::ObIAllocator> build_key_proj_;
  common::ObFixedArray<int64_t, common::ObIAllocator> probe_key_proj_;
  bool can_prob_opt_;
  //is null aware anti join
  bool is_naaj_;
  //is single null aware anti join
  bool is_sna_;
  bool is_shared_ht_;
  // record which equal cond is null safe equal
  common::ObFixedArray<bool, common::ObIAllocator> is_ns_equal_cond_;
};

// hash join has no expression result overwrite problem:
//  LEFT: is block, do not care the overwrite.
//  RIGHT: overwrite with blank_right_row() in JS_FILL_LEFT state, right child also iterated end.
class ObHashJoinVecOp : public ObJoinVecOp
{
public:
  // EN_HASH_JOIN_OPTION switches:
  uint64_t HJ_TP_OPT_ENABLED = 1;
  //uint64_t HJ_TP_OPT_ENABLE_CACHE_AWARE = 2;
  //uint64_t HJ_TP_OPT_ENABLE_BLOOM_FILTER = 4;

  ObHashJoinVecOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input);
  ~ObHashJoinVecOp() {}

private:
  enum HashJoinDrainMode
  {
    NONE_DRAIN,
    BUILD_HT,
    RIGHT_DRAIN,
    MAX_DRAIN
  };
  enum HJState
  {
    INIT,
    PROCESS_PARTITION,
    LOAD_NEXT_PARTITION
  };
  enum HJProcessor
  {
    NONE = 0,
    NEST_LOOP = 1,
    RECURSIVE = 2,
    IN_MEMORY = 4
  };
  enum ObJoinState {
    JS_PROCESS_LEFT,
    JS_READ_RIGHT,
    JS_PROBE_RESULT,
    //JS_FILL_LEFT, // for left_outer,full_outer
    JS_LEFT_UNMATCH_RESULT,
    JS_JOIN_END
  };
  enum ObFuncType {
    FT_ITER_GOING = 0,
    FT_ITER_END,
    FT_TYPE_COUNT
  };
  enum HJLoopState {
    LOOP_START,
    LOOP_GOING,
    LOOP_RECURSIVE,
    LOOP_END
  };
public:
  virtual int inner_open() override;
  virtual int inner_rescan() override;
  virtual int do_drain_exch() override;
  virtual int inner_get_next_batch(const int64_t max_row_cnt) override;
  virtual int inner_get_next_row() { return common::OB_NOT_IMPLEMENT; };
  virtual void destroy() override;
  virtual int inner_close() override;
  inline JoinHashTable &get_hash_table()
  {
    return join_table_;
  }
private:
  int init_mem_context(uint64_t tenant_id);
  int init_join_table_ctx();
  int process_partition();
  int process_left(bool &need_not_read_right);
  int fill_left_unmatched_result();
  int recursive_postprocess();
  void part_rescan();
  void part_rescan(bool reset_all);
  void reset();
  void reset_base();
  int reuse_for_next_chunk();
  int load_next();
  int build_hash_table_for_nest_loop(int64_t &num_left_rows);
  int init_left_vectors();
  int nest_loop_process(int64_t &num_left_rows);
  int64_t calc_partition_count_by_cache_aware(
            int64_t row_count, int64_t max_part_count, int64_t global_mem_bound_size);
  int64_t calc_max_data_size(const int64_t extra_memory_size);
  int get_max_memory_size(int64_t input_size);
  int64_t calc_bucket_number(const int64_t row_count);
  int calc_basic_info(bool global_info = false);
  int get_processor_type();
  int build_hash_table_in_memory(int64_t &num_left_rows);
  int in_memory_process(int64_t &num_left_rows);
  int create_partition(bool is_left, int64_t part_id, ObHJPartition *&part);
  int init_join_partition();
  int force_dump(bool for_left);
  void update_remain_data_memory_size(
    int64_t row_count,
    int64_t total_mem_size, bool &need_dump);
  bool need_more_remain_data_memory_size(
    int64_t row_count,
    int64_t total_mem_size,
    double &data_ratio);
  int update_remain_data_memory_size_periodically(int64_t row_count, bool &need_dump, bool force_update = false);
  int dump_build_table(int64_t row_count, bool force_update = false);
  int split_partition(int64_t &num_left_rows);
  int prepare_hash_table();
  void trace_hash_table_collision(int64_t row_cnt);
  int build_hash_table_for_recursive();
  int recursive_process(int64_t &num_left_rows);
  int adaptive_process(int64_t &num_left_rows);
  int read_right_operate();
  int finish_dump(bool for_left, bool need_dump, bool force = false);
  int dump_right_partition_for_recursive();
  int probe();
  int probe_batch_output();
  //int convert_exprs(const ObHJStoredRow *store_row, const ObIArray<ObExpr*> &exprs, bool &has_fill);
  int dump_remain_partition();
  int update_dumped_partition_statistics(bool is_left);

  int fill_partition_batch(int64_t &num_left_rows);
  int get_next_left_row_batch(bool is_from_row_store,
                              const ObBatchRows *&child_brs);
  int get_next_right_batch();
  int calc_hash_value_and_skip_null(const ObIArray<ObExpr*> &join_keys,
                                    const ObBatchRows *brs,
                                    const bool is_from_row_store,
                                    uint64_t *hash_vals,
                                    const ObHJStoredRow **store_rows,
                                    bool is_left);
  int skip_rows_in_dumped_part();
  int convert_skip_to_selector();
  int inner_join_output();
  int left_semi_output();
  int right_anti_semi_output();
  int outer_join_output();
  int fill_left_join_result();
  void set_output_eval_info();
  int calc_part_selector(uint64_t *hash_vals, const ObBatchRows &child_brs);
  bool output_unmatch_left() {
    return LEFT_ANTI_JOIN == MY_SPEC.join_type_
           || LEFT_OUTER_JOIN == get_spec().join_type_
           || FULL_OUTER_JOIN == get_spec().join_type_;
  }
  // **** for vectorized end ***

  /********** for shared hash table hash join ***********/
  int set_shared_info();
  int sync_wait_processor_type();
  int sync_wait_part_count();
  int sync_wait_cur_dumped_partition_idx();
  int sync_wait_basic_info(uint64_t &build_ht_thread_ptr);
  int sync_wait_init_build_hash(const uint64_t build_ht_thread_ptr);
  int sync_wait_finish_build_hash();
  int sync_wait_fetch_next_batch();
  int sync_check_early_exit(bool &early_exit);
  int sync_set_early_exit();
  int do_sync_wait_all();
  int sync_wait_close();
  int sync_wait_open();
  /********** end for shared hash table hash join *******/
private:
  using PredFunc = std::function<bool(int64_t)>;
  OB_INLINE int64_t get_part_idx(const uint64_t hash_value)
  { return (hash_value >> part_shift_) & (part_count_ - 1); }
  OB_INLINE bool top_part_level() { return 0 == part_level_; }
  void set_processor(HJProcessor p) { hj_processor_ = p; }
  OB_INLINE  bool need_right_bitset() const
  {
    return (RIGHT_OUTER_JOIN == MY_SPEC.join_type_ || FULL_OUTER_JOIN == MY_SPEC.join_type_
            || RIGHT_SEMI_JOIN == MY_SPEC.join_type_ || RIGHT_ANTI_JOIN == MY_SPEC.join_type_);
  }
  OB_INLINE bool all_dumped() { return -1 == cur_dumped_partition_; }
  OB_INLINE int64_t get_mem_used() { return nullptr == mem_context_ ? 0 : mem_context_->used(); }
  // memory used for dump that it's really used
  OB_INLINE int64_t get_cur_mem_used()
  {
    return get_mem_used() - join_table_.get_mem_used() - dumped_fixed_mem_size_;
  }
  OB_INLINE int64_t get_data_mem_used() { return sql_mem_processor_.get_data_size(); }
  OB_INLINE bool need_dump(int64_t mem_used)
  {
    return mem_used > sql_mem_processor_.get_mem_bound() * data_ratio_;
  }
  OB_INLINE bool need_dump()
  {
    return get_cur_mem_used() > sql_mem_processor_.get_mem_bound() * data_ratio_;
  }
  int64_t get_need_dump_size(int64_t mem_used)
  {
    return mem_used - sql_mem_processor_.get_mem_bound() * data_ratio_ + 2 * 1024 * 1024;
  }
  OB_INLINE bool all_in_memory(int64_t size) const
  { return size < remain_data_memory_size_; }
  void clean_part_mgr()
  {
    if (nullptr != part_mgr_) {
      part_mgr_->reset();
      if (left_part_ != NULL) {
        part_mgr_->free(left_part_);
        left_part_ = NULL;
      }
      if (right_part_ != NULL) {
        part_mgr_->free(right_part_);
        right_part_ = NULL;
      }
    }
  }
  void reset_nest_loop()
  {
    nth_nest_loop_ = 0;
  }
  // 这里可能会放大，暂时这样
  int64_t get_extra_memory_size() const
  {
    int64_t bucket_cnt = profile_.get_bucket_size();
    const int64_t DEFAULT_EXTRA_SIZE = 2 * 1024 * 1024;
    int64_t res = join_table_.get_mem_used();
    return  res < 0 ? DEFAULT_EXTRA_SIZE : res;
  }

  void clean_nest_loop_chunk()
  {
    join_table_.reset();
    alloc_->reuse();
    reset_nest_loop();
    nest_loop_state_ = LOOP_START;
  }
  OB_INLINE void mark_return() { need_return_ = true; }
  int asyn_dump_partition(int64_t dumped_size,
                      bool is_left,
                      bool dump_all,
                      int64_t start_dumped_part_idx,
                      PredFunc pred);
  inline bool check_right_need_dump(int64_t part_idx)
  {
    return left_part_array_[part_idx]->is_dumped() || (is_shared_ && part_idx > cur_dumped_partition_);
  }
  inline bool is_left_naaj() { return LEFT_ANTI_JOIN == MY_SPEC.join_type_ && MY_SPEC.is_naaj_; }
  inline bool is_right_naaj() { return RIGHT_ANTI_JOIN == MY_SPEC.join_type_ && MY_SPEC.is_naaj_; }
  inline bool is_left_naaj_na() { return is_left_naaj() && !MY_SPEC.is_sna_; }
  inline bool is_right_naaj_na() { return is_right_naaj() && !MY_SPEC.is_sna_; }
  inline bool is_left_naaj_sna() { return is_left_naaj() && MY_SPEC.is_sna_; }
  inline bool is_right_naaj_sna() { return is_right_naaj() && MY_SPEC.is_sna_; }
  int check_join_key_for_naaj_batch_output(const int64_t batch_size);
  int check_join_key_for_naaj_batch(const bool is_left,
                                    bool &has_null,
                                    const ObBatchRows *child_brs);
private:
  static const int64_t RATIO_OF_BUCKETS = 2;
  // min row count for estimated row count
  static const int64_t MIN_ROW_COUNT = 10000;
  static const int64_t INIT_LTB_SIZE = 64;
  static const int64_t MIN_PART_COUNT = 8;
  static const int64_t PAGE_SIZE = ObChunkDatumStore::BLOCK_SIZE;
  static const int64_t MIN_MEM_SIZE = (MIN_PART_COUNT + 1) * PAGE_SIZE;
  // 目前最大层次为4，通过高位4个字节作为recursive处理，超过partition level采用nest loop方式处理
  static const int64_t MAX_PART_LEVEL = 4;
  static const int8_t ENABLE_HJ_NEST_LOOP = 0x01;
  static const int8_t ENABLE_HJ_RECURSIVE = 0x02;
  static const int8_t ENABLE_HJ_IN_MEMORY = 0x04;
  static const int8_t HJ_PROCESSOR_MASK =
                          ENABLE_HJ_NEST_LOOP | ENABLE_HJ_RECURSIVE | ENABLE_HJ_IN_MEMORY;
  // hard code seed, 24bit max prime number
  static const int64_t HASH_SEED = 16777213;
  static const int64_t MAX_NEST_LOOP_RIGHT_ROW_COUNT = 1000000000; // about 120M
  static const int64_t MIN_BATCH_ROW_CNT_NESTLOOP = 256;
  static const int64_t PRICE_PER_ROW = 48;
  static const int64_t MAX_PART_COUNT_PER_LEVEL = INIT_LTB_SIZE<< 1;

  int64_t max_output_cnt_;
  HJState hj_state_;
  HJProcessor hj_processor_;
  bool force_hash_join_spill_;
  int8_t hash_join_processor_;
  int64_t tenant_id_;
  ObSqlWorkAreaProfile profile_;
  ObSqlMemMgrProcessor sql_mem_processor_;

  ObJoinState state_;
  HashJoinDrainMode drain_mode_;
  int64_t remain_data_memory_size_;
  int64_t nth_nest_loop_;
  int64_t dumped_fixed_mem_size_;
  JoinTableCtx jt_ctx_;
  JoinHashTable join_table_;
  lib::MemoryContext mem_context_;
  common::ObIAllocator *alloc_; // for hash table
  HJLoopState nest_loop_state_;
  bool is_last_chunk_;
  bool need_return_;
  bool iter_end_;
  bool is_shared_;
  JoinHashTable *cur_join_table_;

  // ********* for fill partitions *********
  ObHJPartition **left_part_array_;
  ObHJPartition **right_part_array_;
  ObHJPartition *left_part_;
  ObHJPartition *right_part_;
  ObHJPartitionMgr *part_mgr_;
  int32_t part_level_;
  int32_t part_shift_;
  int64_t part_count_;
  int32_t part_round_;
  const ObHJStoredRow **part_stored_rows_;
  ObHJStoredRow **part_added_rows_;
  int64_t cur_dumped_partition_;
  uint64_t *hash_vals_;
  ObBitVector *null_skip_bitmap_;
  ObBatchRows child_brs_;
  // Record batch idx involved in each partition, selector_list_ in row batch;
  // For example, max_batch_size is 256, there are 10 rows of data, divided into 2 partitions,
  //  odd numbers are partition 0, and even numbers are partition 1
  //Then part_selectors_memory layout is:
  // selector_list of part_0: 0,2,4,6,8, ...(251 zeros),
  // selector_list of part_1: 1,3,5,7,9, ...(251 zeros)
  //
  // part_selector_sizes_ data is:
  // 5, 5
  uint16_t *part_selectors_;
  uint16_t *part_selector_sizes_;
  common::ObFixedArray<ObIVector *, common::ObIAllocator> left_vectors_;
  // ********* for fill partitions end *********

  ProbeBatchRows probe_batch_rows_;
  int64_t right_batch_traverse_cnt_;
  //Indicates that a NULL value appears on the non-reserved side of naaj.
  //In this case, the result will be returned directly to null.
  bool read_null_in_naaj_;
  //Indicates that the non-reserved side is not empty
  bool non_preserved_side_is_not_empty_;
  int64_t null_random_hash_value_;
  /*
  *for inner join && semi join, we can skip all rows which contain null join key
  *for left outer join, we can skip null join key on right side, random left side hash value
  *for right outer join, we can skip null join key on left side, random right side hash value
  *for full outer join, we do not skip null join key, only random its both side hash value
  *for anti join, we do not skip null join key && not change its hash value
  */
  bool skip_left_null_;
  bool skip_right_null_;

  double data_ratio_;
  OutputInfo output_info_;
  ObTempRowStore::IterationAge iter_age_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* SRC_SQL_ENGINE_JOIN_HASH_JOIN_OB_HASH_JOIN_VEC_OP_H_ */
