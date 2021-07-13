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

#ifndef SRC_SQL_ENGINE_JOIN_OB_HASH_JOIN_H_
#define SRC_SQL_ENGINE_JOIN_OB_HASH_JOIN_H_

#include "common/row/ob_row.h"
#include "common/row/ob_row_store.h"
#include "sql/engine/join/ob_join.h"
#include "lib/container/ob_bit_set.h"
#include "sql/engine/join/ob_hj_partition.h"
#include "sql/engine/join/ob_hj_batch.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"
#include "lib/container/ob_2d_array.h"

namespace oceanbase {
namespace sql {
class ObExecContext;

class ObHashJoin : public ObJoin {
  OB_UNIS_VERSION_V(1);

public:
  static const int64_t RATIO_OF_BUCKETS = 2;
  // min row count for estimated row count
  static const int64_t MIN_ROW_COUNT = 10000;
  // max row count for estimated row count
  static const int64_t MAX_ROW_COUNT = 1000000;
  // max memory size limit --unused
  static const int64_t DEFAULT_MEM_LIMIT = 100 * 1024 * 1024;

  static const int64_t MAX_PART_COUNT = 256;
  static const int64_t MIN_PART_COUNT = 8;
  static const int64_t PAGE_SIZE = OB_MALLOC_MIDDLE_BLOCK_SIZE;
  static const int64_t MIN_MEM_SIZE = (MIN_PART_COUNT + 1) * PAGE_SIZE;
  static const int64_t MAX_PART_LEVEL = 4;

  static const int8_t ENABLE_HJ_NEST_LOOP = 0x01;
  static const int8_t ENABLE_HJ_RECURSIVE = 0x02;
  static const int8_t ENABLE_HJ_IN_MEMORY = 0x04;
  static const int8_t HJ_PROCESSOR_MASK = ENABLE_HJ_NEST_LOOP | ENABLE_HJ_RECURSIVE | ENABLE_HJ_IN_MEMORY;
  static int8_t HJ_PROCESSOR_ALGO;

  static const int8_t DEFAULT_MURMUR_HASH = 0x01;
  static const int8_t ENABLE_CRC64_V3 = 0x02;
  static const int8_t ENABLE_XXHASH64 = 0x04;
  static const int8_t HASH_FUNCTION_MASK = DEFAULT_MURMUR_HASH | ENABLE_CRC64_V3 | ENABLE_XXHASH64;

  // hard code seed, 24bit max prime number
  static const int64_t HASH_SEED = 16777213;
  // about 120M
  static const int64_t MAX_NEST_LOOP_RIGHT_ROW_COUNT = 1000000000;
  static bool TEST_NEST_LOOP_TO_RECURSIVE;

  // make PART_COUNT and MAX_PAGE_COUNT configurable by unittest
  static int64_t PART_COUNT;
  static int64_t MAX_PAGE_COUNT;

  class ObPartHashJoinCtx;
  typedef uint64_t (*hj_hash_fun)(const common::ObObj& obj, const uint64_t hash);
  struct HashFunctionInterface {
    virtual int hash(const common::ObObj& obj, uint64_t& hash_value) = 0;
  };

  struct StringHashFunction : public HashFunctionInterface {
    StringHashFunction()
    {}
    ~StringHashFunction()
    {}

    int hash(const common::ObObj& obj, uint64_t& hash_value);
  };

  struct DefaultHashFunction : public HashFunctionInterface {
    DefaultHashFunction() : hash_ptr_(nullptr)
    {}
    ~DefaultHashFunction()
    {
      reset();
    }

    void reset()
    {
      hash_ptr_ = nullptr;
    }
    int hash(const common::ObObj& obj, uint64_t& hash_value);
    hj_hash_fun hash_ptr_;
  };

private:
  enum ObJoinState {
    JS_JOIN_END,
    JS_READ_RIGHT,
    JS_READ_HASH_ROW,
    JS_LEFT_ANTI_SEMI,  // for anti_semi
    JS_FILL_LEFT,       // for left_outer,full_outer
    JS_STATE_COUNT
  };
  enum ObFuncType { FT_ITER_GOING = 0, FT_ITER_END, FT_TYPE_COUNT };

  struct HashTableCell {
    HashTableCell() = default;
    // optimize constructor performance
    join::ObStoredJoinRow* stored_row_;
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
      if (nullptr != buckets_) {
        buckets_->reset();
      }
      if (nullptr != collision_cnts_) {
        collision_cnts_->reset();
      }
      if (nullptr != all_cells_) {
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
        min_cnt = 0;  // min_cnt was initizlized to 255
      }
    }
    using BucketArray = common::ObSegmentArray<HashTableCell*, OB_MALLOC_BIG_BLOCK_SIZE, common::ModulePageAllocator>;
    BucketArray* buckets_;
    using AllCellArray = common::ObSegmentArray<HashTableCell, OB_MALLOC_BIG_BLOCK_SIZE, common::ModulePageAllocator>;
    AllCellArray* all_cells_;
    using CollisionCntArray = common::ObSegmentArray<uint8_t, OB_MALLOC_BIG_BLOCK_SIZE, common::ModulePageAllocator>;
    CollisionCntArray* collision_cnts_;
    int64_t nbuckets_;
    int64_t row_count_;
    bool inited_;
    ModulePageAllocator* ht_alloc_;
  };

  struct EqualConditionInfo {
    EqualConditionInfo()
        : col1_idx_(0), col2_idx_(0), obj_hash_(nullptr), cmp_type_(common::ObNullType), ctype_(common::CS_TYPE_INVALID)
    {}
    ~EqualConditionInfo()
    {
      reset();
    }

    void reset()
    {
      col1_idx_ = 0;
      col2_idx_ = 0;
      cmp_type_ = common::ObNullType;
      ctype_ = common::CS_TYPE_INVALID;
      obj_hash_ = nullptr;
    }
    TO_STRING_KV(K_(col1_idx), K_(col2_idx), K_(cmp_type), K_(ctype));
    int64_t col1_idx_;
    int64_t col2_idx_;
    DefaultHashFunction default_hash_fun_;
    StringHashFunction string_hash_fun_;
    HashFunctionInterface* obj_hash_;
    common::ObObjType cmp_type_;
    common::ObCollationType ctype_;  // collation type

  private:
    DISALLOW_COPY_AND_ASSIGN(EqualConditionInfo);
  };

  class DefaultHashJoin {
  public:
    DefaultHashJoin()
    {}
    ~DefaultHashJoin()
    {}
    virtual int read_hashrow_func_going(
        const ObHashJoin* hash_join, ObPartHashJoinCtx& join_ctx, const ObNewRow*& row) const;
    virtual int read_hashrow_func_end(
        const ObHashJoin* hash_join, ObPartHashJoinCtx& join_ctx, const ObNewRow*& row) const;
  };

  class HashInnerJoin : public DefaultHashJoin {
  public:
    HashInnerJoin()
    {}
    ~HashInnerJoin()
    {}

    int read_hashrow_func_going(const ObHashJoin* hash_join, ObPartHashJoinCtx& join_ctx, const ObNewRow*& row) const;
    int read_hashrow_func_end(const ObHashJoin* hash_join, ObPartHashJoinCtx& join_ctx, const ObNewRow*& row) const;
  };
  enum HJNestLoopState { START, GOING, RECURSIVE, END };

public:
  class ObHashJoinCtx : public ObJoinCtx {
    friend class ObHashJoin;

  public:
    enum HJState { INIT, NORMAL, NEXT_BATCH };

    enum HJProcessor { NONE = 0, NEST_LOOP = 1, RECURSIVE = 2, IN_MEMORY = 4 };
    explicit ObHashJoinCtx(ObExecContext& ctx)
        : ObJoinCtx(ctx),
          hj_state_(INIT),
          hj_processor_(NONE),
          buf_mgr_(NULL),
          batch_mgr_(NULL),
          left_op_(NULL),
          right_op_(NULL),
          part_level_(0),
          part_shift_(MAX_PART_LEVEL << 3),
          part_count_(0),
          force_hash_join_spill_(false),
          hash_join_hasher_(1),
          hash_join_processor_(7),
          hash_join_(nullptr),
          input_size_(0),
          total_extra_size_(0),
          predict_row_cnt_(1024),
          profile_(ObSqlWorkAreaType::HASH_WORK_AREA),
          sql_mem_processor_(profile_)
    {}
    virtual ~ObHashJoinCtx()
    {}

    void reset()
    {
      hj_state_ = INIT;
      hj_processor_ = NONE;
      part_level_ = 0;
      part_shift_ = MAX_PART_LEVEL << 3;
      part_count_ = 0;
      input_size_ = 0;
      predict_row_cnt_ = 1024;
      left_op_ = nullptr;
      right_op_ = nullptr;
    }
    virtual void destroy()
    {
      ObJoinCtx::destroy();
    }

  private:
    HJState hj_state_;
    HJProcessor hj_processor_;
    join::ObHJBufMgr* buf_mgr_;
    join::ObHJBatchMgr* batch_mgr_;
    join::ObHJBatch* left_op_;
    join::ObHJBatch* right_op_;
    int32_t part_level_;
    int32_t part_shift_;
    int64_t part_count_;
    bool force_hash_join_spill_;
    int8_t hash_join_hasher_;
    int8_t hash_join_processor_;
    DefaultHashJoin default_hash_join_;
    HashInnerJoin inner_hash_join_;
    DefaultHashJoin* hash_join_;
    int64_t input_size_;
    int64_t total_extra_size_;
    int64_t predict_row_cnt_;
    ObSqlWorkAreaProfile profile_;
    ObSqlMemMgrProcessor sql_mem_processor_;
  };

  class ObPartHashJoinCtx : public ObHashJoinCtx {
    friend class ObHashJoin;

  public:
    explicit ObPartHashJoinCtx(ObExecContext& ctx);
    virtual ~ObPartHashJoinCtx()
    {}
    void reset()
    {
      clean_batch_mgr();
      part_rescan();
      ObHashJoinCtx::reset();
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
    void part_rescan()
    {
      state_ = JS_READ_RIGHT;
      hash_value_ = 0;
      right_has_matched_ = false;
      tuple_need_join_ = false;
      first_get_row_ = true;
      cur_bkid_ = 0;
      hash_table_.reset();
      cur_tuple_ = NULL;
      need_cache_info_ = true;
      hj_bit_set_.reset();
      right_bit_set_.reset();
      nest_loop_state_ = HJNestLoopState::START;
      is_last_chunk_ = false;
      has_right_bitset_ = false;
      remain_data_memory_size_ = 0;
      reset_nest_loop();
      hj_processor_ = NONE;
      cur_dumped_partition_ = MAX_PART_COUNT;
      reset_statistics();
      predict_row_cnt_ = 1024;

      if (hj_part_array_ != NULL) {
        for (int64_t i = 0; i < part_count_; i++) {
          hj_part_array_[i].~ObHJPartition();
        }
        alloc_->free(hj_part_array_);
        hj_part_array_ = NULL;
      }

      if (right_hj_part_array_ != NULL) {
        for (int64_t i = 0; i < part_count_; i++) {
          right_hj_part_array_[i].~ObHJPartition();
        }
        alloc_->free(right_hj_part_array_);
        right_hj_part_array_ = NULL;
      }

      left_read_row_ = NULL;
      right_read_row_ = NULL;
    }
    virtual void destroy()
    {
      if (OB_LIKELY(nullptr != alloc_)) {
        alloc_ = nullptr;
      }
      if (OB_LIKELY(NULL != mem_context_)) {
        DESTROY_CONTEXT(mem_context_);
        mem_context_ = NULL;
      }
      equal_cond_info_.destroy();
      ObHashJoinCtx::destroy();
    }
    OB_INLINE bool all_dumped()
    {
      return -1 == cur_dumped_partition_;
    }
    OB_INLINE int64_t get_bucket_idx(const uint64_t hash_value)
    {
      return hash_value & (hash_table_.nbuckets_ - 1);
    }
    OB_INLINE int64_t get_part_idx(const uint64_t hash_value)
    {
      return (hash_value >> part_shift_) & (part_count_ - 1);
    }
    OB_INLINE bool top_part_level()
    {
      return 0 == part_level_;
    }
    void set_processor(HJProcessor p)
    {
      hj_processor_ = p;
    }
    void clean_batch_mgr();
    inline int init_mem_context(uint64_t tenant_id);
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

  private:
    ObJoinState state_;
    uint64_t hash_value_;     // cur right row's hash_value
    bool right_has_matched_;  // if cur right row has matched
    bool tuple_need_join_;    // for left_semi, left_anti_semi
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
    bool need_cache_info_;
    common::ObSEArray<EqualConditionInfo*, 16> equal_cond_info_;
    common::ObBitSet<common::OB_DEFAULT_BITSET_SIZE, common::ModulePageAllocator, true> hj_bit_set_;
    common::ObBitSet<common::OB_DEFAULT_BITSET_SIZE, common::ModulePageAllocator, true> right_bit_set_;
    int64_t nth_right_row_;
    int64_t cur_dumped_partition_;
    HJNestLoopState nest_loop_state_;
    bool is_last_chunk_;
    bool has_right_bitset_;
    join::ObHJPartition* hj_part_array_;
    join::ObHJPartition* right_hj_part_array_;
    // store row read from partition
    const join::ObStoredJoinRow* left_read_row_;
    const join::ObStoredJoinRow* right_read_row_;

    // statistics
    int64_t bitset_filter_cnt_;
    int64_t probe_cnt_;
    int64_t hash_equal_cnt_;
    int64_t hash_link_cnt_;
  };

  typedef int (ObHashJoin::*state_operation_func_type)(ObPartHashJoinCtx& join_ctx) const;
  typedef int (ObHashJoin::*state_function_func_type)(ObPartHashJoinCtx& join_ctx, const common::ObNewRow*& row) const;

public:
  explicit ObHashJoin(common::ObIAllocator& alloc);
  virtual ~ObHashJoin();
  virtual void reset();
  virtual void reuse();
  virtual int rescan(ObExecContext& exec_ctx) const;
  virtual int part_rescan(ObExecContext& exec_ctx, bool reset_all) const;

private:
  int init_join_ctx(ObExecContext& ctx) const;
  int init_join_partition(ObPartHashJoinCtx* join_ctx) const;
  // get next row
  virtual int inner_get_next_row(ObExecContext& ctx, const common::ObNewRow*& row) const;

  virtual int next(ObExecContext& exec_ctx, const common::ObNewRow*& row) const;
  // open
  virtual int inner_open(ObExecContext& exec_ctx) const;
  // close
  virtual int inner_close(ObExecContext& exec_ctx) const;
  // create optctx
  virtual int inner_create_operator_ctx(ObExecContext& exec_ctx, ObPhyOperatorCtx*& op_ctx) const;
  // beyond mem_limit need dump(todo)
  int dump_build_table(ObPartHashJoinCtx& join_ctx, int64_t row_count) const;
  int dump_probe_table(ObPartHashJoinCtx& join_ctx) const;
  int force_dump(ObPartHashJoinCtx& join_ctx, bool for_left) const;
  int finish_dump(ObPartHashJoinCtx& join_ctx, bool for_left, bool need_dump = true, bool force = false) const;

  int get_input_size(ObPartHashJoinCtx& join_ctx, int64_t& input_size) const;

  inline bool need_right_bitset() const
  {
    return (RIGHT_OUTER_JOIN == join_type_ || FULL_OUTER_JOIN == join_type_ || RIGHT_SEMI_JOIN == join_type_ ||
            RIGHT_ANTI_JOIN == join_type_);
  }

  // get hash value after casting the input to the desired type
  int get_cast_hash_value(const common::ObObj& obj, const EqualConditionInfo& info, ObPartHashJoinCtx& join_ctx,
      uint64_t& hash_value) const;

  // get hash value for a row
  int get_left_hash_value(ObPartHashJoinCtx& join_ctx, const common::ObNewRow& row, uint64_t& hash_value) const;
  int get_right_hash_value(ObPartHashJoinCtx& join_ctx, const common::ObNewRow& row, uint64_t& hash_value) const;
  int calc_hash_value(
      ObPartHashJoinCtx& join_ctx, const ObObj& cell, const EqualConditionInfo& info, uint64_t& hash_value) const;
  int setup_equal_condition_info(ObPartHashJoinCtx& join_ctx) const;
  int set_hash_function(EqualConditionInfo* info, int8_t hash_join_hasher) const;
  void clean_equal_condition_info(ObPartHashJoinCtx& join_ctx, ObExecContext& exec_ctx) const;
  // scan hash table to find next unmatched  tuple
  int find_next_unmatched_tuple(ObPartHashJoinCtx& join_ctx, HashTableCell*& tuple) const;
  int find_next_matched_tuple(ObPartHashJoinCtx& join_ctx, HashTableCell*& tuple) const;
  // convert tuple to a row
  int convert_tuple(ObPartHashJoinCtx& join_ctx, const HashTableCell& tuple) const;
  // read hash table to match cur row
  int read_hashrow(ObPartHashJoinCtx& join_ctx) const;
  // join row
  int read_hashrow_func_going(ObPartHashJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  // end
  int read_hashrow_func_end(ObPartHashJoinCtx& join_ctx, const common::ObNewRow*& row) const;

  // JS_READ_RIGHT state operation and calc hash value functions.
  int read_right_operate(ObPartHashJoinCtx& join_ctx) const;
  int calc_right_hash_value(ObPartHashJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  int read_right_func_end(ObPartHashJoinCtx& join_ctx, const common::ObNewRow*& row) const;

  // JS_LEFT_ANTI_SEMI state operation
  int left_anti_semi_operate(ObPartHashJoinCtx& join_ctx) const;
  int left_anti_semi_going(ObPartHashJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  int left_anti_semi_end(ObPartHashJoinCtx& join_ctx, const common::ObNewRow*& row) const;

  // JS_LEFT_ANTI_SEMI state operation
  int fill_left_operate(ObPartHashJoinCtx& join_ctx) const;
  int fill_left_going(ObPartHashJoinCtx& join_ctx, const common::ObNewRow*& row) const;
  int fill_left_end(ObPartHashJoinCtx& join_ctx, const common::ObNewRow*& row) const;

  // JS_JOIN_END state operation
  int join_end_operate(ObPartHashJoinCtx& join_ctx) const;
  int join_end_func_end(ObPartHashJoinCtx& join_ctx, const common::ObNewRow*& row) const;

  virtual int get_left_row(ObJoinCtx& join_ctx) const;
  virtual int get_right_row(ObJoinCtx& join_ctx) const;
  virtual int get_next_left_row(ObJoinCtx& join_ctx) const;
  virtual int get_next_right_row(ObJoinCtx& join_ctx) const;

  int64_t auto_calc_partition_count(int64_t input_size, int64_t min_need_size, int64_t max_part_count) const;
  int64_t calc_partition_count(int64_t input_size, int64_t part_size, int64_t max_part_count) const;

  bool all_in_memory(ObPartHashJoinCtx& join_ctx, int64_t size) const;
  int in_memory_process(ObPartHashJoinCtx& join_ctx, bool& need_not_read_right) const;
  int recursive_process(ObPartHashJoinCtx& join_ctx, bool& need_not_read_right) const;
  int nest_loop_process(ObPartHashJoinCtx& join_ctx, bool& need_not_read_right) const;
  int adaptive_process(ObPartHashJoinCtx& join_ctx, bool& need_not_read_right) const;
  int get_processor_type(ObPartHashJoinCtx& join_ctx) const;

  void trace_hash_table_collision(ObPartHashJoinCtx& join_ctx, int64_t row_cnt) const;
  int split_partition_and_build_hash_table(ObPartHashJoinCtx& join_ctx, int64_t& num_left_rows) const;
  int load_next_chunk(ObPartHashJoinCtx& join_ctx) const;
  int build_hash_table_for_nest_loop(ObPartHashJoinCtx& join_ctx, int64_t& num_left_rows) const;
  int build_hash_table_in_memory(ObPartHashJoinCtx& join_ctx, int64_t& num_left_rows) const;
  int build_hash_table_for_recursive(ObPartHashJoinCtx& join_ctx) const;
  int split_partition(ObPartHashJoinCtx& join_ctx, int64_t& num_left_rows) const;
  int prepare_hash_table(ObPartHashJoinCtx& join_ctx) const;
  void clean_nest_loop_chunk(ObPartHashJoinCtx& join_ctx) const;
  int reuse_for_next_chunk(ObPartHashJoinCtx& join_ctx) const;
  int dump_remain_partition(ObPartHashJoinCtx& join_ctx) const;

  int update_remain_data_memory_size_periodically(
      ObPartHashJoinCtx& join_ctx, int64_t row_count, bool& need_dump) const;
  bool need_more_remain_data_memory_size(
      ObPartHashJoinCtx& join_ctx, int64_t row_count, int64_t total_mem_size, double& data_ratio) const;
  int64_t calc_bucket_number(const int64_t row_count) const;
  void update_remain_data_memory_size(
      ObPartHashJoinCtx& join_ctx, int64_t row_count, int64_t total_mem_size, bool& need_dump) const;
  int64_t calc_max_data_size(ObPartHashJoinCtx& join_ctx, const int64_t extra_memory_size) const;
  int calc_basic_info(ObPartHashJoinCtx& join_ctx) const;
  int64_t get_extra_memory_size(ObPartHashJoinCtx& join_ctx) const;
  void set_partition_memory_limit(ObPartHashJoinCtx& join_ctx, const int64_t mem_limit) const;
  int get_max_memory_size(ObPartHashJoinCtx& join_ctx, int64_t input_size) const;

private:
  state_operation_func_type state_operation_func_[JS_STATE_COUNT];
  state_function_func_type state_function_func_[JS_STATE_COUNT][FT_TYPE_COUNT];
  int64_t mem_limit_;
};

inline int ObHashJoin::ObPartHashJoinCtx::init_mem_context(uint64_t tenant_id)
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

}  // namespace sql
}  // namespace oceanbase

#endif /* SRC_SQL_ENGINE_JOIN_OB_HASH_JOIN_H_ */
