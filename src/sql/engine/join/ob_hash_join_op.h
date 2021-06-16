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

#ifndef SRC_SQL_ENGINE_JOIN_OB_HASH_JOIN_OP_H_
#define SRC_SQL_ENGINE_JOIN_OB_HASH_JOIN_OP_H_

#include "sql/engine/join/ob_join_op.h"
#include "share/datum/ob_datum_funcs.h"
#include "sql/engine/join/ob_hash_join_basic.h"
#include "lib/container/ob_bit_set.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"
#include "lib/container/ob_2d_array.h"
#include "sql/engine/aggregate/ob_exec_hash_struct.h"

namespace oceanbase {
namespace sql {

class ObHashJoinSpec : public ObJoinSpec {
  OB_UNIS_VERSION_V(1);

public:
  ObHashJoinSpec(common::ObIAllocator& alloc, const ObPhyOperatorType type);

  ExprFixedArray equal_join_conds_;
  ExprFixedArray all_join_keys_;
  common::ObHashFuncs all_hash_funcs_;
  bool has_join_bf_;
};

// hash join has no expression result overwrite problem:
//  LEFT: is block, do not care the overwrite.
//  RIGHT: overwrite with blank_right_row() in JS_FILL_LEFT state, right child also iterated end.

class ObHashJoinOp : public ObJoinOp {
public:
  ObHashJoinOp(ObExecContext& exec_ctx, const ObOpSpec& spec, ObOpInput* input);
  ~ObHashJoinOp()
  {}

  using BucketFunc = std::function<int64_t(int64_t, int64_t)>;
  using NextFunc = std::function<int(const ObHashJoinStoredJoinRow*& right_read_row)>;

private:
  enum HJState { INIT, NORMAL, NEXT_BATCH };
  enum HJProcessor { NONE = 0, NEST_LOOP = 1, RECURSIVE = 2, IN_MEMORY = 4 };
  enum ObJoinState {
    JS_JOIN_END,
    JS_READ_RIGHT,
    JS_READ_HASH_ROW,
    JS_LEFT_ANTI_SEMI,  // for anti_semi
    JS_FILL_LEFT,       // for left_outer,full_outer
    JS_STATE_COUNT
  };
  enum ObFuncType { FT_ITER_GOING = 0, FT_ITER_END, FT_TYPE_COUNT };
  enum HJLoopState { LOOP_START, LOOP_GOING, LOOP_RECURSIVE, LOOP_END };

private:
  struct HashTableCell {
    HashTableCell() = default;
    // do NOT init these members below in constructor, since we will set them
    // before using them. for performance.
    ObHashJoinStoredJoinRow* stored_row_;
    HashTableCell* next_tuple_;
    TO_STRING_KV(K_(stored_row), K(static_cast<void*>(next_tuple_)));
  };
  struct PartHashJoinTable {
    PartHashJoinTable()
        : buckets_(nullptr),
          all_cells_(nullptr),
          collision_cnts_(nullptr),
          nbuckets_(0),
          row_count_(0),
          inited_(false),
          ht_alloc_(nullptr)
    {}
    void reset()
    {
      if (OB_NOT_NULL(buckets_)) {
        buckets_->reset();
      }
      if (OB_NOT_NULL(collision_cnts_)) {
        collision_cnts_->reset();
      }
      if (OB_NOT_NULL(all_cells_)) {
        all_cells_->reset();
      }
      nbuckets_ = 0;
    }
    int init(ObIAllocator& alloc);
    void free(ObIAllocator* alloc)
    {
      reset();
      if (OB_NOT_NULL(buckets_)) {
        buckets_->destroy();
        alloc->free(buckets_);
        buckets_ = nullptr;
      }
      if (OB_NOT_NULL(collision_cnts_)) {
        collision_cnts_->destroy();
        alloc->free(collision_cnts_);
        collision_cnts_ = nullptr;
      }
      if (OB_NOT_NULL(all_cells_)) {
        all_cells_->destroy();
        alloc->free(all_cells_);
        all_cells_ = nullptr;
      }
      if (OB_NOT_NULL(ht_alloc_)) {
        ht_alloc_->reset();
        ht_alloc_->~ModulePageAllocator();
        alloc->free(ht_alloc_);
        ht_alloc_ = nullptr;
      }
      inited_ = false;
    }
    void inc_collision(int64_t bucket_id)
    {
      if (255 > collision_cnts_->at(bucket_id)) {
        ++collision_cnts_->at(bucket_id);
      }
    }
    void get_collision_info(uint8_t& min_cnt, uint8_t& max_cnt, int64_t& total_cnt, int64_t& used_bucket_cnt)
    {
      min_cnt = 255;
      max_cnt = 0;
      total_cnt = 0;
      used_bucket_cnt = 0;
      if (collision_cnts_->count() != nbuckets_) {
        SQL_ENG_LOG(
            WARN, "unexpected: collision_cnts_->count() != nbuckets_", K(collision_cnts_->count()), K(nbuckets_));
      } else {
        for (int64_t i = 0; i < nbuckets_; ++i) {
          int64_t cnt = collision_cnts_->at(i);
          if (0 < cnt) {
            ++used_bucket_cnt;
            total_cnt += cnt;
            if (min_cnt > cnt) {
              min_cnt = cnt;
            }
            if (max_cnt < cnt) {
              max_cnt = cnt;
            }
          }
        }
      }
      if (0 == used_bucket_cnt) {
        min_cnt = 0;  // the initial value is 255.
      }
    }
    using BucketArray =
        common::ObSegmentArray<HashTableCell*, OB_MALLOC_MIDDLE_BLOCK_SIZE, common::ModulePageAllocator>;
    using AllCellArray = common::ObSegmentArray<HashTableCell, OB_MALLOC_BIG_BLOCK_SIZE, common::ModulePageAllocator>;
    using CollisionCntArray = common::ObSegmentArray<uint8_t, OB_MALLOC_BIG_BLOCK_SIZE, common::ModulePageAllocator>;

    BucketArray* buckets_;
    AllCellArray* all_cells_;
    CollisionCntArray* collision_cnts_;
    int64_t nbuckets_;
    int64_t row_count_;
    bool inited_;
    ModulePageAllocator* ht_alloc_;
  };

  struct HistItem {
    int64_t hash_value_;
    ObHashJoinStoredJoinRow* store_row_;
    TO_STRING_KV(K_(hash_value), K(static_cast<void*>(store_row_)));
  };
  struct ResultItem {
    HistItem left_;
    HistItem right_;
    bool is_match_;
  };
  class HashJoinHistogram {
  public:
    HashJoinHistogram()
        : h1_(nullptr),
          h2_(nullptr),
          prefix_hist_count_(nullptr),
          prefix_hist_count2_(nullptr),
          hist_alloc_(nullptr),
          enable_bloom_filter_(false),
          bloom_filter_(nullptr),
          alloc_(nullptr),
          row_count_(0),
          bucket_cnt_(0)
    {}
    ~HashJoinHistogram()
    {
      reset();
    }
    void reset()
    {
      if (OB_NOT_NULL(alloc_)) {
        if (OB_NOT_NULL(h1_)) {
          h1_->reset();
          alloc_->free(h1_);
          h1_ = nullptr;
        }
        if (OB_NOT_NULL(h2_)) {
          h2_->reset();
          alloc_->free(h2_);
          h2_ = nullptr;
        }
        if (OB_NOT_NULL(prefix_hist_count_)) {
          prefix_hist_count_->reset();
          alloc_->free(prefix_hist_count_);
          prefix_hist_count_ = nullptr;
        }
        if (OB_NOT_NULL(prefix_hist_count2_)) {
          prefix_hist_count2_->reset();
          alloc_->free(prefix_hist_count2_);
          prefix_hist_count2_ = nullptr;
        }
        if (OB_NOT_NULL(bloom_filter_)) {
          bloom_filter_->~ObGbyBloomFilter();
          alloc_->free(bloom_filter_);
          bloom_filter_ = nullptr;
        }
        if (OB_NOT_NULL(hist_alloc_)) {
          hist_alloc_->reset();
          hist_alloc_->~ModulePageAllocator();
          alloc_->free(hist_alloc_);
          hist_alloc_ = nullptr;
        }
      }
      alloc_ = nullptr;
      row_count_ = 0;
      bucket_cnt_ = 0;
      enable_bloom_filter_ = false;
    }

    OB_INLINE int64_t get_bucket_idx(const uint64_t hash_value)
    {
      return hash_value & (bucket_cnt_ - 1);
    }
    int init(ObIAllocator* alloc, int64_t row_count, int64_t bucket_cnt, bool enable_bloom_filter);
    static int64_t calc_memory_size(int64_t row_count)
    {
      return next_pow2(row_count * RATIO_OF_BUCKETS) * (sizeof(HistItem) * 2 + sizeof(int64_t));
    }
    bool empty() const
    {
      return 0 == row_count_;
    }
    int reorder_histogram(BucketFunc bucket_func);
    int calc_prefix_histogram();
    void switch_histogram();
    void switch_prefix_hist_count();

  public:
    using HistItemArray = common::ObSegmentArray<HistItem, OB_MALLOC_BIG_BLOCK_SIZE, common::ModulePageAllocator>;
    using HistPrefixArray = common::ObSegmentArray<int64_t, OB_MALLOC_BIG_BLOCK_SIZE, common::ModulePageAllocator>;
    HistItemArray* h1_;
    HistItemArray* h2_;
    HistPrefixArray* prefix_hist_count_;
    HistPrefixArray* prefix_hist_count2_;
    ModulePageAllocator* hist_alloc_;
    bool enable_bloom_filter_;
    ObGbyBloomFilter* bloom_filter_;
    ObIAllocator* alloc_;
    int64_t row_count_;
    int64_t bucket_cnt_;
  };
  class PartitionSplitter {
  public:
    PartitionSplitter()
        : alloc_(nullptr),
          part_count_(0),
          hj_parts_(nullptr),
          max_level_(0),
          part_shift_(0),
          level1_bit_(0),
          level2_bit_(0),
          level_one_part_count_(0),
          level_two_part_count_(0),
          part_histogram_(),
          total_row_count_(0)
    {}
    ~PartitionSplitter()
    {
      reset();
    }
    void reset()
    {
      part_count_ = 0;
      hj_parts_ = nullptr;
      max_level_ = 0;
      part_shift_ = 0;
      level1_bit_ = 0;
      level2_bit_ = 0;
      level_one_part_count_ = 0;
      level_two_part_count_ = 0;
      part_histogram_.reset();
      total_row_count_ = 0;
      alloc_ = nullptr;
    }

    void set_part_count(int64_t part_shift, int64_t level1_part_count, int64_t level2_part_count)
    {
      part_shift_ = part_shift;
      level_one_part_count_ = level1_part_count;
      level_two_part_count_ = level2_part_count;
      level1_bit_ = __builtin_ctz(level1_part_count);
      level2_bit_ = __builtin_ctz(level2_part_count);
    }
    bool is_valid()
    {
      return nullptr != hj_parts_;
    }
    int init(ObIAllocator* alloc, int64_t part_count, ObHashJoinPartition* hj_parts, int64_t max_level,
        int64_t part_shift, int64_t level1_part_count, int64_t level2_part_count);
    int repartition_by_part_array(const int64_t part_level);
    int repartition_by_part_histogram(const int64_t part_level);
    int build_hash_table_by_part_hist(HashJoinHistogram* all_part_hists, bool enable_bloom_filter);
    int build_hash_table_by_part_array(HashJoinHistogram* all_part_hists, bool enable_bloom_filter);

    OB_INLINE int64_t get_part_idx(const uint64_t hash_value)
    {
      int64_t part_idx = 0;
      if (0 == level_two_part_count_) {
        part_idx = get_part_level_one_idx(hash_value);
      } else {
        int64_t level1_part_idx = get_part_level_one_idx(hash_value);
        part_idx = get_part_level_two_idx(hash_value);
        part_idx = part_idx + level1_part_idx * level_two_part_count_;
      }
      return part_idx;
    }
    OB_INLINE int64_t get_part_level_one_idx(const uint64_t hash_value)
    {
      return (hash_value >> part_shift_) & (level_one_part_count_ - 1);
    }
    OB_INLINE int64_t get_part_level_two_idx(const uint64_t hash_value)
    {
      return ((hash_value >> part_shift_) >> level1_bit_) & (level_two_part_count_ - 1);
    }
    OB_INLINE bool is_level_one(int64_t part_level)
    {
      return 1 == part_level;
    }
    OB_INLINE bool get_total_row_count()
    {
      return total_row_count_;
    }

  public:
    ObIAllocator* alloc_;
    int64_t part_count_;
    ObHashJoinPartition* hj_parts_;
    int64_t max_level_;
    int64_t part_shift_;
    int64_t level1_bit_;
    int64_t level2_bit_;
    int64_t level_one_part_count_;
    int64_t level_two_part_count_;
    HashJoinHistogram part_histogram_;
    int64_t total_row_count_;
  };

public:
  virtual int inner_open() override;
  virtual int rescan() override;
  virtual int inner_get_next_row() override;
  virtual void destroy() override;
  virtual int inner_close() override;

private:
  void calc_cache_aware_partition_count();
  int recursive_postprocess();
  int insert_batch_row(const int64_t cur_partition_in_memory);
  int insert_all_right_row(const int64_t row_count);
  OB_INLINE int64_t get_part_level_one_idx(const uint64_t hash_value)
  {
    return (hash_value >> part_shift_) & (level1_part_count_ - 1);
  }
  OB_INLINE int64_t get_part_level_two_idx(const uint64_t hash_value)
  {
    return ((hash_value >> part_shift_) >> level1_bit_) & (level2_part_count_ - 1);
  }
  OB_INLINE int64_t get_cache_aware_part_idx(const uint64_t hash_value)
  {
    int64_t part_idx = 0;
    if (0 == level2_part_count_) {
      part_idx = get_part_level_one_idx(hash_value);
    } else {
      int64_t level1_part_idx = get_part_level_one_idx(hash_value);
      part_idx = get_part_level_two_idx(hash_value);
      part_idx = part_idx + level1_part_idx * level2_part_count_;
    }
    return part_idx;
  }
  void init_system_parameters();
  inline int64_t get_level_one_part(int64_t hash_val)
  {
    return hash_val & (level1_part_count_ - 1);
  }
  inline int init_mem_context(uint64_t tenant_id);
  void part_rescan();
  int part_rescan(bool reset_all);
  void reset();
  void reset_base();

  int inner_join_read_hashrow_func_going();
  int other_join_read_hashrow_func_going();

  int inner_join_read_hashrow_func_end();
  int other_join_read_hashrow_func_end();

  int set_hash_function(int8_t hash_join_hasher);

  int next();
  int join_end_operate();
  int join_end_func_end();
  int get_next_left_row();
  int reuse_for_next_chunk();
  int load_next_chunk();
  int build_hash_table_for_nest_loop(int64_t& num_left_rows);
  int nest_loop_process(bool& need_not_read_right);
  int64_t calc_partition_count(int64_t input_size, int64_t part_size, int64_t max_part_count);
  int64_t calc_partition_count_by_cache_aware(int64_t row_count, int64_t max_part_count, int64_t global_mem_bound_size);
  int64_t calc_max_data_size(const int64_t extra_memory_size);
  int get_max_memory_size(int64_t input_size);
  int64_t calc_bucket_number(const int64_t row_count);
  int calc_basic_info();
  int get_processor_type();
  int build_hash_table_in_memory(int64_t& num_left_rows);
  int in_memory_process(bool& need_not_read_right);
  int init_join_partition();
  int force_dump(bool for_left);
  void update_remain_data_memory_size(int64_t row_count, int64_t total_mem_size, bool& need_dump);
  bool need_more_remain_data_memory_size(int64_t row_count, int64_t total_mem_size, double& data_ratio);
  int update_remain_data_memory_size_periodically(int64_t row_count, bool& need_dump);
  int dump_build_table(int64_t row_count);
  int split_partition(int64_t& num_left_rows);
  int prepare_hash_table();
  void trace_hash_table_collision(int64_t row_cnt);
  int build_hash_table_for_recursive();
  int split_partition_and_build_hash_table(int64_t& num_left_rows);
  int recursive_process(bool& need_not_read_right);
  int adaptive_process(bool& need_not_read_right);
  int get_next_right_row();
  int read_right_operate();
  int calc_hash_value(const ObIArray<ObExpr*>& join_keys, const ObIArray<ObHashFunc>& hash_funcs, uint64_t& hash_value);
  int calc_right_hash_value();
  int finish_dump(bool for_left, bool need_dump, bool force = false);
  int read_right_func_end();
  int calc_equal_conds(bool& is_match);
  int read_hashrow();
  int dump_probe_table();
  int read_hashrow_func_going();
  int read_hashrow_func_end();
  int find_next_matched_tuple(HashTableCell*& tuple);
  int left_anti_semi_operate();
  int left_anti_semi_going();
  int left_anti_semi_end();
  int find_next_unmatched_tuple(HashTableCell*& tuple);
  int fill_left_operate();
  int convert_exprs(const ObHashJoinStoredJoinRow* store_row, const ObIArray<ObExpr*>& exprs, bool& has_fill);
  int fill_left_going();
  int fill_left_end();
  int join_rows_with_right_null();
  int join_rows_with_left_null();
  int only_join_left_row();
  int only_join_right_row();
  int dump_remain_partition();

  int save_last_right_row();
  int restore_last_right_row();
  int get_next_batch_right_rows();
  int get_match_row(bool& is_matched);
  int get_next_right_row_for_batch(NextFunc next_func);

private:
  OB_INLINE int64_t get_part_idx(const uint64_t hash_value)
  {
    return (hash_value >> part_shift_) & (part_count_ - 1);
  }
  OB_INLINE int64_t get_bucket_idx(const uint64_t hash_value)
  {
    return hash_value & (hash_table_.nbuckets_ - 1);
  }
  OB_INLINE bool top_part_level()
  {
    return 0 == part_level_;
  }
  void set_processor(HJProcessor p)
  {
    hj_processor_ = p;
  }
  OB_INLINE bool need_right_bitset() const
  {
    return (RIGHT_OUTER_JOIN == MY_SPEC.join_type_ || FULL_OUTER_JOIN == MY_SPEC.join_type_ ||
            RIGHT_SEMI_JOIN == MY_SPEC.join_type_ || RIGHT_ANTI_JOIN == MY_SPEC.join_type_);
  }
  OB_INLINE bool all_dumped()
  {
    return -1 == cur_dumped_partition_;
  }
  OB_INLINE int64_t get_mem_used()
  {
    return nullptr == mem_context_ ? 0 : mem_context_->used();
  }
  OB_INLINE int64_t get_data_mem_used()
  {
    return sql_mem_processor_.get_data_size();
  }
  OB_INLINE bool need_dump()
  {
    return get_mem_used() > sql_mem_processor_.get_mem_bound();
  }
  OB_INLINE bool all_in_memory(int64_t size) const
  {
    return size < remain_data_memory_size_;
  }
  void clean_batch_mgr()
  {
    if (nullptr != batch_mgr_) {
      batch_mgr_->reset();
      if (left_batch_ != NULL) {
        batch_mgr_->free(left_batch_);
        left_batch_ = NULL;
      }
      if (right_batch_ != NULL) {
        batch_mgr_->free(right_batch_);
        right_batch_ = NULL;
      }
    }
  }
  void reset_nest_loop()
  {
    nth_nest_loop_ = 0;
    cur_nth_row_ = 0;
    nth_right_row_ = -1;
    reset_statistics();
  }
  void reset_statistics()
  {
    bitset_filter_cnt_ = 0;
    probe_cnt_ = 0;
    hash_equal_cnt_ = 0;
    hash_link_cnt_ = 0;
  }
  int64_t get_extra_memory_size() const
  {
    int64_t row_count = profile_.get_row_count();
    int64_t bucket_cnt = profile_.get_bucket_size();
    int64_t extra_memory_size = bucket_cnt * (sizeof(HashTableCell*) + sizeof(uint8_t));
    extra_memory_size += (row_count * sizeof(HashTableCell));
    return extra_memory_size;
  }
  void clean_nest_loop_chunk()
  {
    hash_table_.reset();
    if (nullptr != bloom_filter_) {
      bloom_filter_->reset();
    }
    right_bit_set_.reset();
    alloc_->reuse();
    reset_nest_loop();
    nest_loop_state_ = LOOP_START;
  }
  OB_INLINE void mark_return()
  {
    need_return_ = true;
  }
  int init_bloom_filter(ObIAllocator& alloc, int64_t bucket_cnt);
  void free_bloom_filter();

  bool can_use_cache_aware_opt();
  int read_hashrow_normal();
  int read_hashrow_for_cache_aware(NextFunc next_func);
  int init_histograms(HashJoinHistogram*& part_histograms, int64_t part_count);
  int partition_and_build_histograms();
  int repartition(PartitionSplitter& part_splitter, HashJoinHistogram*& part_histograms, ObHashJoinPartition* hj_parts,
      bool is_build_side);
  int get_next_probe_partition();

private:
  typedef int (ObHashJoinOp::*ReadFunc)();
  typedef int (ObHashJoinOp::*state_function_func_type)();
  typedef int (ObHashJoinOp::*state_operation_func_type)();
  static const int64_t RATIO_OF_BUCKETS = 2;
  // min row count for estimated row count
  static const int64_t MIN_ROW_COUNT = 10000;
  // max row count for estimated row count
  static const int64_t MAX_ROW_COUNT = 1000000;
  // max memory size limit --unused
  static const int64_t DEFAULT_MEM_LIMIT = 100 * 1024 * 1024;

  static const int64_t CACHE_AWARE_PART_CNT = 128;
  static const int64_t BATCH_RESULT_SIZE = 512;
  static const int64_t INIT_LTB_SIZE = 64;
  static const int64_t INIT_L2_CACHE_SIZE = 1 * 1024 * 1024;  // 1M
  static const int64_t MIN_PART_COUNT = 8;
  static const int64_t PAGE_SIZE = ObChunkDatumStore::BLOCK_SIZE;
  static const int64_t MIN_MEM_SIZE = (MIN_PART_COUNT + 1) * PAGE_SIZE;
  // use nested loop join if partition level exceeds MAX_PART_LEVEL.
  static const int64_t MAX_PART_LEVEL = 4;
  static const int64_t PART_SPLIT_LEVEL_ONE = 1;
  static const int64_t PART_SPLIT_LEVEL_TWO = 2;

  static const int8_t ENABLE_HJ_NEST_LOOP = 0x01;
  static const int8_t ENABLE_HJ_RECURSIVE = 0x02;
  static const int8_t ENABLE_HJ_IN_MEMORY = 0x04;
  static const int8_t HJ_PROCESSOR_MASK = ENABLE_HJ_NEST_LOOP | ENABLE_HJ_RECURSIVE | ENABLE_HJ_IN_MEMORY;
  static int8_t HJ_PROCESSOR_ALGO;

  static const int8_t DEFAULT_MURMUR_HASH = 0x01;
  static const int8_t ENABLE_WY_HASH = 0x02;
  static const int8_t ENABLE_XXHASH64 = 0x04;
  static const int8_t HASH_FUNCTION_MASK = DEFAULT_MURMUR_HASH | ENABLE_WY_HASH | ENABLE_XXHASH64;

  // hard code seed, 24bit max prime number
  static const int64_t HASH_SEED = 16777213;
  // about 120M
  static const int64_t MAX_NEST_LOOP_RIGHT_ROW_COUNT = 1000000000;
  static bool TEST_NEST_LOOP_TO_RECURSIVE;

  // make PART_COUNT and MAX_PAGE_COUNT configurable by unittest
  static int64_t PART_COUNT;
  static int64_t MAX_PAGE_COUNT;
  state_operation_func_type state_operation_func_[JS_STATE_COUNT];
  state_function_func_type state_function_func_[JS_STATE_COUNT][FT_TYPE_COUNT];
  HJState hj_state_;
  HJProcessor hj_processor_;
  ObHashJoinBufMgr* buf_mgr_;
  ObHashJoinBatchMgr* batch_mgr_;
  ObHashJoinBatch* left_batch_;
  ObHashJoinBatch* right_batch_;
  common::ObHashFuncs tmp_hash_funcs_;
  common::ObArrayHelper<common::ObHashFunc> left_hash_funcs_;
  common::ObArrayHelper<common::ObHashFunc> right_hash_funcs_;
  common::ObArrayHelper<ObExpr*> left_join_keys_;
  common::ObArrayHelper<ObExpr*> right_join_keys_;
  ReadFunc going_func_;
  ReadFunc end_func_;
  int32_t part_level_;
  int32_t part_shift_;
  int64_t part_count_;
  bool force_hash_join_spill_;
  int8_t hash_join_processor_;
  int64_t tenant_id_;
  int64_t input_size_;
  int64_t total_extra_size_;
  int64_t predict_row_cnt_;
  ObSqlWorkAreaProfile profile_;
  ObSqlMemMgrProcessor sql_mem_processor_;
  ObJoinState state_;
  uint64_t cur_right_hash_value_;  // cur right row's hash_value
  bool right_has_matched_;         // if cur right row has matched
  bool tuple_need_join_;           // for left_semi, left_anti_semi
  bool first_get_row_;
  int64_t cur_bkid_;  // for left,anti
  int64_t remain_data_memory_size_;
  int64_t nth_nest_loop_;
  int64_t cur_nth_row_;
  PartHashJoinTable hash_table_;
  HashTableCell* cur_tuple_;       // null or last matched tuple
  common::ObNewRow cur_left_row_;  // like cur_row_ in operator, get row from rowstore
  lib::MemoryContext* mem_context_;
  common::ObIAllocator* alloc_;  // for buckets
  ModulePageAllocator* bloom_filter_alloc_;
  ObGbyBloomFilter* bloom_filter_;
  common::ObBitSet<common::OB_DEFAULT_BITSET_SIZE, common::ModulePageAllocator, true> right_bit_set_;
  int64_t nth_right_row_;
  int64_t ltb_size_;
  int64_t l2_cache_size_;
  int64_t price_per_row_;
  int64_t max_partition_count_per_level_;
  int64_t cur_dumped_partition_;
  HJLoopState nest_loop_state_;
  bool is_last_chunk_;
  bool has_right_bitset_;
  ObHashJoinPartition* hj_part_array_;
  ObHashJoinPartition* right_hj_part_array_;
  // store row read from partition
  const ObHashJoinStoredJoinRow* left_read_row_;
  const ObHashJoinStoredJoinRow* right_read_row_;
  bool postprocessed_left_;
  bool has_fill_right_row_;
  bool has_fill_left_row_;
  ObChunkDatumStore::ShadowStoredRow<ObHashJoinStoredJoinRow> right_last_row_;
  bool need_return_;
  bool iter_end_;
  bool opt_cache_aware_;
  bool has_right_material_data_;
  bool enable_bloom_filter_;
  HashJoinHistogram* part_histograms_;
  int64_t cur_full_right_partition_;
  bool right_iter_end_;
  int64_t cur_bucket_idx_;
  int64_t max_bucket_idx_;
  bool enable_batch_;
  int64_t level1_bit_;
  int64_t level1_part_count_;
  int64_t level2_part_count_;
  PartitionSplitter right_splitter_;
  HashJoinHistogram* cur_left_hist_;
  HashJoinHistogram* cur_right_hist_;
  int64_t cur_probe_row_idx_;
  int64_t max_right_bucket_idx_;

  // statistics
  int64_t probe_cnt_;
  int64_t bitset_filter_cnt_;
  int64_t hash_link_cnt_;
  int64_t hash_equal_cnt_;
};

inline int ObHashJoinOp::init_mem_context(uint64_t tenant_id)
{
  int ret = common::OB_SUCCESS;
  if (OB_LIKELY(NULL == mem_context_)) {
    lib::ContextParam param;
    param.set_properties(lib::USE_TL_PAGE_OPTIONAL)
        .set_mem_attr(tenant_id, common::ObModIds::OB_ARENA_HASH_JOIN, common::ObCtxIds::WORK_AREA);
    if (OB_FAIL(CURRENT_CONTEXT.CREATE_CONTEXT(mem_context_, param))) {
      SQL_ENG_LOG(WARN, "create entity failed", K(ret));
    } else if (OB_ISNULL(mem_context_)) {
      SQL_ENG_LOG(WARN, "mem entity is null", K(ret));
    } else {
      alloc_ = &mem_context_->get_malloc_allocator();
    }
  }
  return ret;
}

}  // end namespace sql
}  // end namespace oceanbase

#endif /* SRC_SQL_ENGINE_JOIN_OB_HASH_JOIN_OP_H_ */
