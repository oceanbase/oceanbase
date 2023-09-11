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

#ifndef OB_HASH_PARTITIONING_INFRASTRUCTURE_OP_H_
#define OB_HASH_PARTITIONING_INFRASTRUCTURE_OP_H_

#include "lib/list/ob_dlist.h"
#include "sql/engine/basic/ob_hash_partitioning_basic.h"
#include "share/datum/ob_datum_funcs.h"
#include "sql/engine/sort/ob_sort_basic_info.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"
#include "sql/engine/expr/ob_expr.h"
#include "lib/hash/ob_array_hash_map.h"
#include "sql/engine/aggregate/ob_adaptive_bypass_ctrl.h"

namespace oceanbase
{
namespace sql
{

struct ObHashPartCols
{
  const static int64_t HASH_VAL_BIT = 63;
  const static int64_t HASH_VAL_MASK = UINT64_MAX >> (64 - HASH_VAL_BIT);
  ObHashPartCols() : next_(nullptr), hash_value_(0), store_row_(nullptr)
  {
    use_expr_ = true;
  }
  ObHashPartCols(const int64_t batch_idx) :
     next_(nullptr), hash_value_(0), store_row_(nullptr)
  {
    batch_idx_ = batch_idx;
    use_expr_ = true;
  }

  uint64_t hash() const
  {
    return hash_value_;
  }

  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }

  int equal(
      const ObHashPartCols &other,
      const common::ObIArray<ObSortFieldCollation> *sort_collations,
      const common::ObIArray<ObCmpFunc> *cmp_funcs,
      bool &result) const;
  int equal_distinct(
        const common::ObIArray<ObExpr *> *exprs,
        const ObHashPartCols &other,
        const common::ObIArray<ObSortFieldCollation> *sort_collations,
        const common::ObIArray<ObCmpFunc> *cmp_funcs,
        ObEvalCtx *eval_ctx,
        bool &result, ObEvalCtx::BatchInfoScopeGuard &batch_info_guard) const;

  ObHashPartCols *&next()
  { return *reinterpret_cast<ObHashPartCols **>(&next_); }

  int set_hash_value(uint64_t hash_value)
  {
    int ret = OB_SUCCESS;
    if (!use_expr_) {
      store_row_->set_hash_value(hash_value);
    }
    hash_value_ = (HASH_VAL_MASK & hash_value);
    return ret;
  }

  void* next_;
  uint64_t hash_value_;
  union {
    ObHashPartStoredRow *store_row_;
    struct {
      int64_t batch_idx_ : 63;
      int64_t use_expr_ : 1;
    };
  };

  TO_STRING_KV(K_(store_row));
};

template <typename Item>
class ObHashPartitionExtendHashTable
{
public:
  const static int64_t INITIAL_SIZE = 128;
  const static int64_t SIZE_BUCKET_PERCENT = 80;
  const static int64_t MAX_MEM_PERCENT = 40;
  const static int64_t EXTENDED_RATIO = 2;
  const int64_t INIT_BKT_NUM_PUSH_DOWM = INIT_L2_CACHE_SIZE / sizeof(ObHashPartCols);
  const int64_t EXTEND_BKT_NUM_PUSH_DOWN = INIT_L3_CACHE_SIZE / sizeof(ObHashPartCols);
  ObHashPartitionExtendHashTable() :
    size_(0), bucket_num_(0), min_bucket_num_(INITIAL_SIZE), max_bucket_num_(INT64_MAX),
    buckets_(nullptr), allocator_(nullptr),
    hash_funcs_(nullptr), sort_collations_(nullptr), cmp_funcs_(nullptr),
    eval_ctx_(nullptr), sql_mem_processor_(nullptr), is_push_down_(false),
    exprs_(nullptr)
  {
  }
  ~ObHashPartitionExtendHashTable() { destroy(); }

  int init(
    common::ObIAllocator *alloctor,
    const int64_t initial_size,
    ObSqlMemMgrProcessor *sql_mem_processor,
    const int64_t min_bucket,
    const int64_t max_bucket,
    bool is_push_down = false);
  // return the first item which equal to, NULL for none exist.
  int get(uint64_t hash_value, const Item &part_cols, const Item *&res) const;
  // Link item to hash table, extend buckets if needed.
  // (Do not check item is exist or not)
  int set(Item &item);
  // Link item to hash table, extend buckets if needed.
  // if exists, return OB_HASH_EXIST
  int set_distinct(Item &item, uint64_t hash_value);
  int check_and_extend();
  int extend(const int64_t new_bucket_num);
  int64_t size() const { return size_; }

  void reuse()
  {
    if (OB_NOT_NULL(buckets_)) {
      buckets_->set_all(nullptr);
    }
    size_ = 0;
    exprs_ = nullptr;
  }

  int resize(
    common::ObIAllocator *allocator,
    int64_t bucket_num,
    ObSqlMemMgrProcessor *sql_mem_processor);

  void destroy()
  {
    if (OB_NOT_NULL(buckets_)) {
      buckets_->destroy();
      if (nullptr != allocator_) {
        allocator_->free(buckets_);
      } else {
        SQL_ENG_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "buckets is not null", KP(buckets_));
      }
      buckets_ = nullptr;
    }
    if (OB_NOT_NULL(allocator_)) {
      ob_delete(allocator_);
      allocator_ = nullptr;
    }
    size_ = 0;
  }
  int64_t mem_used() const
  {
    return nullptr == buckets_ ? 0 : buckets_->mem_used();
  }

  template <typename CB>
  int foreach(CB &cb) const
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(buckets_)) {
      ret = OB_INVALID_ARGUMENT;
      SQL_ENG_LOG(WARN, "invalid null buckets", K(ret), K(buckets_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < get_bucket_num(); i++) {
      Item *item = buckets_->at(i);
      while (NULL != item && OB_SUCC(ret)) {
        if (OB_FAIL(cb(*item))) {
          SQL_ENG_LOG(WARN, "call back failed", K(ret));
        } else {
          item = item->next();
        }
      }
    }
    return ret;
  }

  inline int64_t get_bucket_num() const
  {
    return NULL == buckets_ ? 0 : buckets_->count();
  }

  void set_funcs(
    const common::ObIArray<ObHashFunc> *hash_funcs,
    const common::ObIArray<ObSortFieldCollation> *sort_collations,
    const common::ObIArray<ObCmpFunc> *sort_cmp_funs,
    ObEvalCtx *eval_ctx)
  {
    hash_funcs_ = hash_funcs;
    sort_collations_ = sort_collations;
    cmp_funcs_ = sort_cmp_funs;
    eval_ctx_ = eval_ctx;
  }
  void set_sql_mem_processor(ObSqlMemMgrProcessor *sql_mem_processor)
  { sql_mem_processor_ = sql_mem_processor; }
  ObEvalCtx *get_eval_ctx() { return eval_ctx_; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObHashPartitionExtendHashTable);
  using BucketArray =
    common::ObSegmentArray<Item*, OB_MALLOC_MIDDLE_BLOCK_SIZE, common::ModulePageAllocator>;
  static int64_t estimate_bucket_num(
    const int64_t bucket_num,
    const int64_t max_hash_mem,
    const int64_t min_bucket);
  int create_bucket_array(const int64_t bucket_num, BucketArray *&new_buckets);
public:
  int64_t size_;
  int64_t bucket_num_;
  int64_t min_bucket_num_;
  int64_t max_bucket_num_;
  BucketArray *buckets_;
  common::ModulePageAllocator *allocator_;
  const common::ObIArray<ObHashFunc> *hash_funcs_;
  const common::ObIArray<ObSortFieldCollation> *sort_collations_;
  const common::ObIArray<ObCmpFunc> *cmp_funcs_;
  ObEvalCtx *eval_ctx_;
  ObSqlMemMgrProcessor *sql_mem_processor_;
  bool is_push_down_;
  const common::ObIArray<ObExpr*> *exprs_;
};

template<typename HashCol, typename HashRowStore>
class ObHashPartInfrastructure
{
public:
  ObHashPartInfrastructure() :
    tenant_id_(UINT64_MAX), mem_context_(nullptr), alloc_(nullptr), arena_alloc_(nullptr),
    hash_table_(), preprocess_part_(), left_part_list_(), right_part_list_(),
    left_part_map_(), right_part_map_(), io_event_observer_(nullptr), sql_mem_processor_(nullptr),
    hash_funcs_(nullptr), sort_collations_(nullptr), cmp_funcs_(nullptr), eval_ctx_(nullptr),
    cur_left_part_(nullptr), cur_right_part_(nullptr),
    left_dumped_parts_(nullptr), right_dumped_parts_(nullptr),
    cur_dumped_parts_(nullptr), left_row_store_iter_(), right_row_store_iter_(),
    hash_table_row_store_iter_(),
    enable_sql_dumped_(false), unique_(false), need_pre_part_(false),
    ways_(InputWays::TWO), init_part_func_(nullptr), insert_row_func_(nullptr),
    cur_part_start_id_(0), start_round_(false), cur_side_(InputSide::LEFT),
    has_cur_part_dumped_(false), has_create_part_map_(false),
    est_part_cnt_(INT64_MAX), cur_level_(0), part_shift_(0), period_row_cnt_(0),
    left_part_cur_id_(0), right_part_cur_id_(0), my_skip_(nullptr),
    items_(nullptr), distinct_map_(), hash_col_buffer_(nullptr),
    hash_col_buffer_idx_(MAX_HASH_COL_CNT), is_push_down_(false),
    store_row_buffer_(nullptr), store_row_buffer_cnt_(0)
  {}
  ~ObHashPartInfrastructure();
public:
  enum InputWays
  {
    ONE = 1,
    TWO = 2
  };
  enum ProcessMode
  {
    Cache = 0,
    PreProcess = 1,
  };
public:
  struct ObIntraPartKey
  {
    ObIntraPartKey() : part_shift_(0), nth_way_(0), level_(0), nth_part_(0)
    {}

    uint64_t hash() const
    { return common::murmurhash(&part_key_, sizeof(part_key_), 0); }

    int hash(uint64_t &hash_val) const
    { hash_val = hash(); return OB_SUCCESS; }

    bool operator==(const ObIntraPartKey &other) const
    {
      return part_shift_ == other.part_shift_
          && nth_way_ == other.nth_way_
          && level_ == other.level_
          && nth_part_ == other.nth_part_;
    }

    bool is_left()
    { return InputSide::LEFT == nth_way_; }
    void set_left()
    { nth_way_ = InputSide::LEFT; }
    void set_right()
    { nth_way_ = InputSide::RIGHT; }

    TO_STRING_KV(K_(nth_way), K_(level), K_(nth_part));
    union
    {
      int64_t part_key_;
      struct {
        int32_t part_shift_ : 8;     // part_shift: max value is 64(bytes)
        int32_t nth_way_ : 8;     // 0: left, 1: right
        int32_t level_ : 16;
        // 这里采用递增方式来处理，原因是存在left和right任何一边可能没有数据
        // 但另外一个发生了dump，导致获取其中一路为空时，可能另外一路还遗留在part_list中
        // 从而只有(side, level, nth_part)不够表示唯一
        // 而每层假设128个partition，总共4层，则128^4=268435456，则part需要差不多32bit表示
        // 所以采用递增id方式来保证左右两边完全一致
        // 这里暂时假设应用层使用part比较随机
        // 其实如果保证按照类似先序遍历方式写入和读取，则可以保证同层次下的part只有一个，则不需要这么大的id
        int64_t nth_part_ : 32;
      };
    };
  };
  class ObIntraPartition : public common::ObDLinkBase<ObIntraPartition>
  {
  public:
    ObIntraPartition() :
      part_key_(), store_(ObModIds::OB_SQL_HASH_SET)
    {}
    ~ObIntraPartition()
    {
      store_.reset();
    }
  public:
    int init();

    TO_STRING_KV(K_(part_key));
  public:
    ObIntraPartKey part_key_;
    ObChunkDatumStore store_;
  };

private:
  bool is_left() const { return InputSide::LEFT == cur_side_; }
  bool is_right() const { return InputSide::RIGHT == cur_side_; }
  inline int init_mem_context(uint64_t tenant_id);

  typedef int (ObHashPartInfrastructure::*InitPartitionFunc)
      (ObIntraPartition *part, int64_t nth_part, int64_t limit, int32_t delta_shift);
  typedef int (ObHashPartInfrastructure::*InsertRowFunc)
      (const common::ObIArray<ObExpr*> &exprs, bool &exists, bool &inserted);

  void set_init_part_func();
  int direct_insert_row(
      const common::ObIArray<ObExpr*> &exprs, bool &exists, bool &inserted);
  int insert_row_with_hash_table(
      const common::ObIArray<ObExpr*> &exprs, bool &exists, bool &inserted);
  int insert_row_with_unique_hash_table(
      const common::ObIArray<ObExpr*> &exprs, bool &exists, bool &inserted);
  int do_insert_row_with_unique_hash_table(
      const common::ObIArray<ObExpr*> &exprs, uint64_t hash_value, bool &exists, bool &inserted);
  int do_insert_batch_with_unique_hash_table(const common::ObIArray<ObExpr *> &exprs,
                                          uint64_t *hash_values_for_batch,
                                          const int64_t batch_size,
                                          const ObBitVector *skip,
                                          ObBitVector *&output_vec);

  int get_next_left_partition();
  int get_next_right_partition();
  int get_cur_matched_partition(InputSide input_side);

  void est_partition_count();
  bool need_dump()
  {
    if (INT64_MAX == est_part_cnt_) {
      est_partition_count();
    }
    return (sql_mem_processor_->get_mem_bound() <= est_part_cnt_ * BLOCK_SIZE + get_mem_used());
  }

  int64_t get_mem_used() { return (nullptr == mem_context_) ? 0 : mem_context_->used();}

  OB_INLINE int64_t get_bucket_idx(const uint64_t hash_value)
  {
    return hash_value & (hash_table_.get_bucket_num() - 1);
  }
  // 高位4 byte作为partition hash value
  OB_INLINE int64_t get_part_idx(const uint64_t hash_value)
  {
    return (hash_value >> part_shift_) & (est_part_cnt_ - 1);
  }

  int append_dumped_parts(InputSide input_side);
  int append_all_dump_parts();

  void set_insert_row_func();
  void destroy();
  void destroy_cur_parts();
  void clean_dumped_partitions();
  void clean_cur_dumping_partitions();

  int update_mem_status_periodically();
public:
  int init(uint64_t tenant_id, bool enable_sql_dumped, bool unique, bool need_pre_part,
    int64_t ways, ObSqlMemMgrProcessor *sql_mem_processor);

  void reset();
  void switch_left()
  { cur_side_ = InputSide::LEFT; }
  void switch_right()
  { cur_side_ = InputSide::RIGHT; }

  int exists_row(const common::ObIArray<ObExpr*> &exprs, const HashCol *&exists_part_cols);
  int exists_batch(const common::ObIArray<ObExpr*> &exprs, const int64_t batch_size,
                   const ObBitVector *child_skip, ObBitVector *skip,
                   uint64_t *hash_values_for_batch);
  OB_INLINE int64_t get_bucket_num() const { return hash_table_.get_bucket_num(); }
  int resize(int64_t bucket_cnt);
  int init_hash_table(int64_t bucket_cnt,
    int64_t min_bucket = MIN_BUCKET_NUM,
    int64_t max_bucket = MAX_BUCKET_NUM);
  bool has_cur_part(InputSide input_side)
  {
    bool has_part = false;
    if (InputSide::LEFT == input_side) {
      has_part = nullptr != cur_left_part_ ? true : false;
    } else {
      has_part = nullptr != cur_right_part_ ? true : false;
    }
    return has_part;
  }
  // estimate all rowcount that is not distinct row count
  OB_INLINE int64_t estimate_total_count()
  {
    int ret = common::OB_SUCCESS;
    int64_t row_cnt = 0;
    if (!left_part_list_.is_empty()) {
      DLIST_FOREACH_X(node, left_part_list_, OB_SUCC(ret)) {
        ObIntraPartition *part = node;
        row_cnt += part->store_.get_row_cnt_on_disk();
      }
    }
    if (!right_part_list_.is_empty()) {
      DLIST_FOREACH_X(node, right_part_list_, OB_SUCC(ret)) {
        ObIntraPartition *part = node;
        row_cnt += part->store_.get_row_cnt_on_disk();
      }
    }
    row_cnt += get_hash_table_size();
    return row_cnt;
  }

  OB_INLINE int64_t get_cur_part_row_cnt(InputSide input_side)
  {
    int64_t row_cnt = 0;
    if (InputSide::LEFT == input_side) {
      row_cnt = OB_ISNULL(cur_left_part_) ? 0 : cur_left_part_->store_.get_row_cnt_on_disk();
    } else {
      row_cnt = OB_ISNULL(cur_right_part_) ? 0 : cur_right_part_->store_.get_row_cnt_on_disk();
    }
    return row_cnt;
  }

  OB_INLINE int64_t get_cur_part_file_size(InputSide input_side)
  {
    int64_t file_size = 0;
    if (InputSide::LEFT == input_side) {
      file_size = OB_ISNULL(cur_left_part_) ? 0 : cur_left_part_->store_.get_file_size();
    } else {
      file_size = OB_ISNULL(cur_right_part_) ? 0 : cur_right_part_->store_.get_file_size();
    }
    return file_size;
  }

  int insert_row_on_hash_table(
      const common::ObIArray<ObExpr*> &exprs, bool &exists, bool &inserted);
  int insert_row(const common::ObIArray<ObExpr*> &exprs, bool &exists, bool &inserted);
  int insert_row_for_batch(const common::ObIArray<ObExpr *> &batch_exprs,
                           uint64_t *hash_values_for_batch,
                           const int64_t batch_size,
                           const ObBitVector *skip,
                           ObBitVector *&output_vec);
  int insert_row_on_partitions(const common::ObIArray<ObExpr*> &exprs);
  int insert_batch_on_partitions(const common::ObIArray<ObExpr *> &exprs,
                                 const ObBitVector &skip,
                                 const int64_t batch_size,
                                 uint64_t *hash_values);
  //set the expr into hash table and prepare the pointer of store_rows
  //need to set the store_rows after this function
  int set_distinct_batch(const common::ObIArray<ObExpr *> &exprs,
                         uint64_t *hash_values_for_batch,
                         const int64_t batch_size,
                         const ObBitVector *skip,
                         uint16 *selector,
                         int64_t &selector_size,
                         ObBitVector &my_skip);
  //used when curr partition is dumped, probe and ignore the rows which is existed
  int probe_batch(const common::ObIArray<ObExpr *> &exprs,
                         uint64_t *hash_values_for_batch,
                         const int64_t batch_size,
                         const ObBitVector *skip,
                         ObBitVector &my_skip);
  //add exprs into datum store, and set items_
  int set_item_ptrs(const ObIArray<ObExpr *> &exprs, const int64_t batch_size,
                    uint64_t *hash_values_for_batch,
                    const uint16_t *selector, const int64_t selector_size,
                    ObBitVector &my_skip);
  int do_insert_row_with_unique_hash_table_by_pass(const common::ObIArray<ObExpr*> &exprs,
                                                   bool is_block,
                                                   bool can_insert,
                                                   bool &exists,
                                                   bool &inserted,
                                                   bool &full_by_pass);
  int do_insert_batch_with_unique_hash_table_by_pass(const common::ObIArray<ObExpr *> &exprs,
                                                     uint64_t *hash_values_for_batch,
                                                     const int64_t batch_size,
                                                     const ObBitVector *skip,
                                                     bool is_block,
                                                     bool can_insert,
                                                     int64_t &exists,
                                                     bool &full_by_pass,
                                                     ObBitVector *&output_vec);
  int64_t get_hash_bucket_num() { return hash_table_.get_bucket_num(); }
  int finish_insert_row(); 
  int start_round();
  int end_round();

  int open_hash_table_part();
  int close_hash_table_part();

  int open_cur_part(InputSide input_side);
  int close_cur_part(InputSide input_side);

  int64_t est_bucket_count(
    const int64_t rows,
    const int64_t width,
    const int64_t min_bucket_cnt = MIN_BUCKET_NUM,
    const int64_t max_bucket_cnt = MAX_BUCKET_NUM);

  int init_set_part(ObIntraPartition *part, int64_t nth_part, int64_t limit, int32_t delta_shift);
  int init_default_part(ObIntraPartition *part, int64_t nth_part, int64_t limit, int32_t delta_shift);

  int create_dumped_partitions(InputSide input_side);

  int get_next_pair_partition(InputSide input_side);
  int get_next_partition(InputSide input_side);
  int get_right_next_row(const ObChunkDatumStore::StoredRow *&store_row);
  int get_left_next_row(const ObChunkDatumStore::StoredRow *&store_row);
  int get_right_next_row(
      const ObChunkDatumStore::StoredRow *&store_row, const common::ObIArray<ObExpr*> &exprs);
  int get_left_next_row(
      const ObChunkDatumStore::StoredRow *&store_row, const common::ObIArray<ObExpr*> &exprs);
  int get_left_next_batch(const common::ObIArray<ObExpr *> &exprs,
                          const int64_t max_row_cnt,
                          int64_t &read_rows,
                          uint64_t *hash_values_for_batch);
  int get_right_next_batch(const common::ObIArray<ObExpr *> &exprs,
                           const int64_t max_row_cnt,
                           int64_t &read_rows);
  // 实现对hash table数据进行遍历，其实可以支持多种方式
  // 如：
  //   1）hash table的bucket遍历
  //   2）chunk datum store顺序遍历
  // 目前直接从store中拿数据，后续如果有需要，可以扩展该接口
  int get_next_hash_table_row(
    const ObChunkDatumStore::StoredRow *&store_row,
    const common::ObIArray<ObExpr*> *exprs);
  int get_next_hash_table_batch(const common::ObIArray<ObExpr *> &exprs,
                                const int64_t max_row_cnt,
                                int64_t &read_rows,
                                const ObChunkDatumStore::StoredRow **store_row);
  // int clean_partition();

  OB_INLINE bool has_left_dumped() { return OB_NOT_NULL(left_dumped_parts_); }
  OB_INLINE bool has_right_dumped() { return OB_NOT_NULL(right_dumped_parts_); }
  OB_INLINE bool has_dumped_partitions()
  { return !(left_part_list_.is_empty() && right_part_list_.is_empty()); }

  int calc_hash_value(const common::ObIArray<ObExpr*> &exprs, uint64_t &hash_value);
  int calc_hash_value_for_batch(const common::ObIArray<ObExpr *> &batch_exprs,
                                const int64_t batch_size,
                                const ObBitVector *skip,
                                uint64_t *hash_values_for_batch,
                                int64_t start_idx = 0,
                                uint64_t *hash_vals = nullptr);
  int init_my_skip(const int64_t batch_size) {
    int ret = OB_SUCCESS;
    void *data = nullptr;
    if (OB_ISNULL(alloc_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "allocator is not init", K(ret));
    } else if (OB_ISNULL(data = alloc_->alloc(ObBitVector::memory_size(batch_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "failed to init hp_infra skip", K(ret));
    } else {
      my_skip_ = to_bit_vector(data);
      my_skip_->reset(batch_size);
    }
    return ret;
  }

  void destroy_my_skip()
  {
    if (OB_NOT_NULL(mem_context_)) {
      if (OB_NOT_NULL(alloc_)) {
        alloc_->free(my_skip_);
        my_skip_ = nullptr;
      }
    }
  }

  int init_items(const int64_t batch_size) {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(alloc_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "allocator is not init", K(ret));
    } else if (OB_ISNULL(items_
              = static_cast<HashCol **> (alloc_->alloc(sizeof(HashCol *) * batch_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "failed to init items", K(ret));
    }
    return ret;
  }

  void destroy_items()
  {
    if (OB_NOT_NULL(mem_context_)) {
      if (OB_NOT_NULL(alloc_)) {
        alloc_->free(items_);
        items_ = nullptr;
      }
    }
  }

  int init_distinct_map(const int64_t batch_size) {
    int ret = OB_SUCCESS;
    if (distinct_map_.is_inited()) {
    } else if (OB_FAIL(distinct_map_.init("HashPartInfra", batch_size))) {
      SQL_ENG_LOG(WARN,"failed to init distinct map", K(ret), K(batch_size));
    }
    return ret;
  }

  void destroy_distinct_map()
  {
    distinct_map_.destroy();
  }

  int set_funcs(const common::ObIArray<ObHashFunc> *hash_funcs,
    const common::ObIArray<ObSortFieldCollation> *sort_collations,
    const common::ObIArray<ObCmpFunc> *cmp_funcs,
    ObEvalCtx *eval_ctx)
  {
    int ret = common::OB_SUCCESS;
    if (nullptr == hash_funcs || nullptr == sort_collations
        || nullptr == cmp_funcs || nullptr == eval_ctx) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "unexpected status: funcs is null", K(ret));
    } else {
      hash_funcs_ = hash_funcs;
      sort_collations_ = sort_collations;
      cmp_funcs_ = cmp_funcs;
      eval_ctx_ = eval_ctx;
      hash_table_.set_funcs(hash_funcs, sort_collations, cmp_funcs, eval_ctx);
    }
    return ret;
  }
  void reset_hash_table_for_by_pass()
  {
    hash_table_.reuse();
    hash_table_row_store_iter_.reset();
    preprocess_part_.store_.reset();
  }
  int64_t get_hash_table_size() const { return hash_table_.size(); }
  int64_t get_hash_store_mem_used() const { return preprocess_part_.store_.get_mem_used(); }
  void set_push_down() { is_push_down_ = true; }
  int process_dump(bool is_block, bool &full_by_pass);
  int extend_hash_table_l3()
  {
    return hash_table_.extend(EXTEND_BKT_NUM_PUSH_DOWN);
  }
  bool hash_table_full() { return get_hash_table_size() >= 0.8 * get_hash_bucket_num(); }
  inline void set_io_event_observer(ObIOEventObserver *observer)
  {
    io_event_observer_ = observer;
  }
private:
  static const int64_t BLOCK_SIZE = 64 * 1024;
  static const int64_t MIN_BUCKET_NUM = 128;
  static const int64_t MAX_BUCKET_NUM = 131072;   // 1M = 131072 * 8
  static const int64_t MAX_PART_LEVEL = 4;
  static const int64_t MAX_HASH_COL_CNT = 16;
  const int64_t EXTEND_BKT_NUM_PUSH_DOWN = INIT_L3_CACHE_SIZE / sizeof(ObHashPartCols);
  uint64_t tenant_id_;
  lib::MemoryContext mem_context_;
  common::ObIAllocator *alloc_;
  common::ObArenaAllocator *arena_alloc_;
  ObHashPartitionExtendHashTable<HashCol> hash_table_;
  ObIntraPartition preprocess_part_;
  common::ObDList<ObIntraPartition> left_part_list_;
  common::ObDList<ObIntraPartition> right_part_list_;
  common::hash::ObHashMap<ObIntraPartKey, ObIntraPartition*, common::hash::NoPthreadDefendMode>
                                                                                    left_part_map_;
  common::hash::ObHashMap<ObIntraPartKey, ObIntraPartition*, common::hash::NoPthreadDefendMode>
                                                                                    right_part_map_;
  ObIOEventObserver *io_event_observer_;
  ObSqlMemMgrProcessor *sql_mem_processor_;
  const common::ObIArray<ObHashFunc> *hash_funcs_;
  // 这里暂时以一份保存，如果是多元operator，part_col_idxs不一样，需要区分
  const common::ObIArray<ObSortFieldCollation> *sort_collations_;
  const common::ObIArray<ObCmpFunc> *cmp_funcs_;
  ObEvalCtx *eval_ctx_;
  ObIntraPartition *cur_left_part_;
  ObIntraPartition *cur_right_part_;
  ObIntraPartition **left_dumped_parts_;
  ObIntraPartition **right_dumped_parts_;
  ObIntraPartition **cur_dumped_parts_;
  ObChunkDatumStore::Iterator left_row_store_iter_;
  ObChunkDatumStore::Iterator right_row_store_iter_;
  ObChunkDatumStore::Iterator hash_table_row_store_iter_;
  // only init
  bool enable_sql_dumped_;
  bool unique_;
  bool need_pre_part_;
  InputWays ways_;
  InitPartitionFunc init_part_func_;
  InsertRowFunc insert_row_func_;
  // dynamic
  int64_t cur_part_start_id_;
  bool start_round_;
  InputSide cur_side_;
  bool has_cur_part_dumped_;
  bool has_create_part_map_;
  int64_t est_part_cnt_;
  int64_t cur_level_;
  int32_t part_shift_;
  int64_t period_row_cnt_;
  int64_t left_part_cur_id_;
  int64_t right_part_cur_id_;
  ObBitVector *my_skip_;
  HashCol **items_;
  common::ObArrayHashMap<HashCol, int> distinct_map_;
  ObHashPartCols *hash_col_buffer_;
  int64_t hash_col_buffer_idx_;
  bool is_push_down_;
  ObChunkDatumStore::StoredRow **store_row_buffer_;
  int64_t store_row_buffer_cnt_;
};

//////////////////// start ObHashPartInfrastructure //////////////////
template<typename HashCol, typename HashRowStore>
ObHashPartInfrastructure<HashCol, HashRowStore>::~ObHashPartInfrastructure()
{
  destroy();
}

template<typename HashCol, typename HashRowStore>
inline int ObHashPartInfrastructure<HashCol, HashRowStore>::init_mem_context(uint64_t tenant_id)
{
  int ret = common::OB_SUCCESS;
  if (OB_LIKELY(NULL == mem_context_)) {
    void *buf = nullptr;
    lib::ContextParam param;
    param.set_properties(lib::USE_TL_PAGE_OPTIONAL)
      .set_mem_attr(tenant_id, "HashPartInfra",
                    common::ObCtxIds::WORK_AREA)
      .set_ablock_size(lib::INTACT_MIDDLE_AOBJECT_SIZE);
    if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
      SQL_ENG_LOG(WARN, "create entity failed", K(ret));
    } else if (OB_ISNULL(mem_context_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "mem entity is null", K(ret));
    } else if (OB_ISNULL(buf = mem_context_->allocp(sizeof(ObArenaAllocator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "failed to allocate memory", K(ret));
    } else {
      arena_alloc_ = new (buf) ObArenaAllocator(mem_context_->get_malloc_allocator());
      arena_alloc_->set_label("HashPartInfra");
      alloc_ = &mem_context_->get_malloc_allocator();
    }
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::init(
  uint64_t tenant_id, bool enable_sql_dumped, bool unique, bool need_pre_part, int64_t ways,
  ObSqlMemMgrProcessor *sql_mem_processor)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_mem_context(tenant_id))) {
    SQL_ENG_LOG(WARN, "failed to init mem_context", K(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    enable_sql_dumped_ = enable_sql_dumped;
    unique_ = unique;
    need_pre_part_ = need_pre_part;
    if (1 == ways) {
      ways_ = InputWays::ONE;
    } else if (2 == ways) {
      ways_ = InputWays::TWO;
    } else {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "Invalid Argument", K(ret), K(ways));
    }
    sql_mem_processor_ = sql_mem_processor;
    init_part_func_ = &ObHashPartInfrastructure<HashCol, HashRowStore>::init_default_part;
    insert_row_func_ = &ObHashPartInfrastructure<HashCol, HashRowStore>::direct_insert_row;
    part_shift_ = sizeof(uint64_t) * CHAR_BIT / 2;
    set_insert_row_func();
    set_init_part_func();
    if (OB_ISNULL(hash_col_buffer_
          = static_cast<ObHashPartCols *>
            (arena_alloc_->alloc(MAX_HASH_COL_CNT * sizeof(ObHashPartCols))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "failed to alloc mem for hash col buffer", K(ret));
    } else {
      hash_col_buffer_idx_ = 0;
    }
  }
  return ret;
}


template<typename HashCol, typename HashRowStore>
void ObHashPartInfrastructure<HashCol, HashRowStore>::clean_cur_dumping_partitions()
{
  if (OB_NOT_NULL(left_dumped_parts_)) {
    for (int64_t i = 0; i < est_part_cnt_; ++i) {
      if (OB_NOT_NULL(left_dumped_parts_[i])) {
        left_dumped_parts_[i]->~ObIntraPartition();
        alloc_->free(left_dumped_parts_[i]);
        left_dumped_parts_[i] = nullptr;
      }
    }
    alloc_->free(left_dumped_parts_);
    left_dumped_parts_ = nullptr;
  }

  if (OB_NOT_NULL(right_dumped_parts_)) {
    for (int64_t i = 0; i < est_part_cnt_; ++i) {
      if (OB_NOT_NULL(right_dumped_parts_[i])) {
        right_dumped_parts_[i]->~ObIntraPartition();
        alloc_->free(right_dumped_parts_[i]);
        right_dumped_parts_[i] = nullptr;
      }
    }
    alloc_->free(right_dumped_parts_);
    right_dumped_parts_ = nullptr;
  }
}

template<typename HashCol, typename HashRowStore>
void ObHashPartInfrastructure<HashCol, HashRowStore>::clean_dumped_partitions()
{
  int ret = OB_SUCCESS;
  DLIST_FOREACH_REMOVESAFE_X(node, left_part_list_, OB_SUCC(ret)) {
    ObIntraPartition *part = node;
    ObIntraPartition *tmp_part = left_part_list_.remove(part);
    if (tmp_part != part) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(ERROR, "unexpected status: part it not match", K(ret), K(part), K(tmp_part));
    } else if (OB_FAIL(left_part_map_.erase_refactored(part->part_key_, &tmp_part))) {
      SQL_ENG_LOG(WARN, "failed to remove part from map", K(ret), K(part->part_key_));
    } else if (part != tmp_part) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(ERROR, "unexepcted status: part is not match", K(ret), K(part), K(tmp_part));
    }
    part->~ObIntraPartition();
    alloc_->free(part);
    part = nullptr;
  }
  DLIST_FOREACH_REMOVESAFE_X(node, right_part_list_, OB_SUCC(ret)) {
    ObIntraPartition *part = node;
    ObIntraPartition *tmp_part = right_part_list_.remove(part);
    if (tmp_part != part) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(ERROR, "unexpected status: part it not match", K(ret), K(part), K(tmp_part));
    } else if (OB_FAIL(right_part_map_.erase_refactored(part->part_key_, &tmp_part))) {
      SQL_ENG_LOG(WARN, "failed to remove part from map", K(ret), K(part->part_key_));
    } else if (part != tmp_part) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(ERROR, "unexepcted status: part is not match", K(ret), K(part), K(tmp_part));
    }
    part->~ObIntraPartition();
    alloc_->free(part);
    part = nullptr;
  }
  left_part_list_.reset();
  right_part_list_.reset();
  left_part_map_.destroy();
  right_part_map_.destroy();
  has_create_part_map_ = false;
}

template<typename HashCol, typename HashRowStore>
void ObHashPartInfrastructure<HashCol, HashRowStore>::destroy_cur_parts()
{
  if (OB_NOT_NULL(cur_left_part_)) {
    cur_left_part_->~ObIntraPartition();
    alloc_->free(cur_left_part_);
    cur_left_part_ = nullptr;
  }
  if (OB_NOT_NULL(cur_right_part_)) {
    cur_right_part_->~ObIntraPartition();
    alloc_->free(cur_right_part_);
    cur_right_part_ = nullptr;
  }
}

template<typename HashCol, typename HashRowStore>
void ObHashPartInfrastructure<HashCol, HashRowStore>::destroy()
{
  reset();
  distinct_map_.destroy();
  arena_alloc_ = nullptr;
  hash_table_.destroy();
  left_part_map_.destroy();
  right_part_map_.destroy();
  hash_col_buffer_ = nullptr;
  hash_col_buffer_idx_ = MAX_HASH_COL_CNT;
  store_row_buffer_ = nullptr;
  store_row_buffer_cnt_ = 0;
  if (OB_NOT_NULL(mem_context_)) {
    if (OB_NOT_NULL(alloc_)) {
      alloc_->free(my_skip_);
      my_skip_ = nullptr;
      alloc_->free(items_);
      items_ = nullptr;
    }
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = NULL;
  }
}

template<typename HashCol, typename HashRowStore>
void ObHashPartInfrastructure<HashCol, HashRowStore>::reset()
{
  left_row_store_iter_.reset();
  right_row_store_iter_.reset();
  hash_table_row_store_iter_.reset();
  destroy_cur_parts();
  clean_cur_dumping_partitions();
  clean_dumped_partitions();
  hash_table_.destroy();
  preprocess_part_.store_.reset();
  left_part_map_.clear();
  right_part_map_.clear();
  hash_funcs_ = nullptr;
  sort_collations_ = nullptr;
  cmp_funcs_ = nullptr;
  cur_left_part_ = nullptr;
  cur_right_part_ = nullptr;
  left_dumped_parts_ = nullptr;
  right_dumped_parts_ = nullptr;
  cur_dumped_parts_ = nullptr;
  cur_part_start_id_ = 0;
  start_round_ = false;
  cur_side_ = InputSide::LEFT;
  has_cur_part_dumped_ = false;
  est_part_cnt_ = INT64_MAX;
  cur_level_ = 0;
  part_shift_ = sizeof(uint64_t) * CHAR_BIT / 2;
  period_row_cnt_ = 0;
  left_part_cur_id_ = 0;
  right_part_cur_id_ = 0;
  hash_col_buffer_ = nullptr;
  hash_col_buffer_idx_ = MAX_HASH_COL_CNT;
  store_row_buffer_ = nullptr;
  store_row_buffer_cnt_ = 0;
  io_event_observer_ = nullptr;
  if (OB_NOT_NULL(arena_alloc_)) {
    arena_alloc_->reset();
  }
}

template<typename HashCol, typename HashRowStore>
void ObHashPartInfrastructure<HashCol, HashRowStore>::set_init_part_func()
{
  if (OB_NOT_NULL(sql_mem_processor_)) {
    const ObSqlWorkAreaProfile &profile = sql_mem_processor_->get_profile();
    ObPhyOperatorType type = profile.get_operator_type();
    switch (type) {
      case PHY_HASH_UNION:
      case PHY_HASH_INTERSECT:
      case PHY_HASH_EXCEPT:
        init_part_func_ = &ObHashPartInfrastructure<HashCol, HashRowStore>::init_set_part;
        break;
      default:
        break;
    }
  }
}

template<typename HashCol, typename HashRowStore>
void ObHashPartInfrastructure<HashCol, HashRowStore>::set_insert_row_func()
{
  if (unique_) {
    insert_row_func_ =
        &ObHashPartInfrastructure<HashCol, HashRowStore>::insert_row_with_unique_hash_table;
  } else {
    insert_row_func_ =
        &ObHashPartInfrastructure<HashCol, HashRowStore>::insert_row_with_hash_table;
  }
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::init_set_part(
  ObIntraPartition *part, int64_t nth_part, int64_t limit, int32_t delta_shift)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(part)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "unexpected status: part is null", K(ret));
  } else {
    part->part_key_.nth_way_ = InputSide::LEFT == cur_side_ ? 0 : 1;
    part->part_key_.part_shift_ = part_shift_ + delta_shift;
    part->part_key_.level_ = cur_level_ + 1;
    part->part_key_.nth_part_ = nth_part;
    if (OB_FAIL(part->store_.init(limit, tenant_id_, ObCtxIds::WORK_AREA,
                          ObModIds::OB_SQL_HASH_SET, true /* enable dump */,
                          sizeof(uint64_t)))) {
      SQL_ENG_LOG(WARN, "failed to init row store", K(ret));
    } else if (OB_ISNULL(sql_mem_processor_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "sql_mem_processor_ is null", K(ret));
    } else {
      part->store_.set_dir_id(sql_mem_processor_->get_dir_id());
      part->store_.set_allocator(*alloc_);
      part->store_.set_callback(sql_mem_processor_);
      part->store_.set_io_event_observer(io_event_observer_);
    }
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::init_default_part(
  ObIntraPartition *part, int64_t nth_part, int64_t limit, int32_t delta_shift)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(part)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "unexpected status: part is null", K(ret));
  } else {
    part->part_key_.nth_way_ = InputSide::LEFT == cur_side_ ? 0 : 1;
    part->part_key_.part_shift_ = part_shift_ + delta_shift;
    part->part_key_.level_ = cur_level_ + 1;
    part->part_key_.nth_part_ = nth_part;
    if (OB_FAIL(part->store_.init(limit, tenant_id_, ObCtxIds::WORK_AREA,
                          "HashInfraOp", true /* enable dump */,
                          sizeof(uint64_t)))) {
      SQL_ENG_LOG(WARN, "failed to init row store", K(ret));
    } else if (OB_ISNULL(sql_mem_processor_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "sql_mem_processor_ is null", K(ret));
    } else {
      part->store_.set_dir_id(sql_mem_processor_->get_dir_id());
      part->store_.set_allocator(*alloc_);
      part->store_.set_callback(sql_mem_processor_);
      part->store_.set_io_event_observer(io_event_observer_);
    }
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::init_hash_table(
  int64_t bucket_cnt, int64_t min_bucket, int64_t max_bucket)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(alloc_) || !start_round_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "allocator is null or it don'e start to round", K(ret), K(start_round_));
  } else if (OB_FAIL(hash_table_.init(alloc_, bucket_cnt, sql_mem_processor_,
                                      min_bucket, max_bucket, is_push_down_))) {
    SQL_ENG_LOG(WARN, "failed to init hash table", K(ret), K(bucket_cnt));
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::resize(int64_t bucket_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(alloc_) || !start_round_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "allocator is null or it don'e start to round", K(ret), K(start_round_));
  } else if (OB_FAIL(hash_table_.resize(alloc_, max(2, bucket_cnt), sql_mem_processor_))) {
    SQL_ENG_LOG(WARN, "failed to init hash table", K(ret), K(bucket_cnt));
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::start_round()
{
  int ret = OB_SUCCESS;
  if (start_round_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "current rount is not finish", K(ret));
  } else {
    if (need_pre_part_) {
      if (OB_FAIL((this->*init_part_func_)(&preprocess_part_, 0, INT64_MAX, 0))) {
        SQL_ENG_LOG(WARN, "failed to init preprocess part", K(ret));
      }
    }
    cur_left_part_ = nullptr;
    cur_right_part_ = nullptr;
    cur_part_start_id_ = max(left_part_cur_id_, right_part_cur_id_);
    start_round_ = true;
    cur_side_ = InputSide::LEFT;
    has_cur_part_dumped_ = false;
    est_part_cnt_ = INT64_MAX;
    period_row_cnt_ = 0;
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::append_dumped_parts(
  InputSide input_side)
{
  int ret = OB_SUCCESS;
  ObIntraPartition **dumped_parts = nullptr;
  if (InputSide::LEFT == input_side) {
    dumped_parts = left_dumped_parts_;
  } else {
    dumped_parts = right_dumped_parts_;
  }
  if (OB_NOT_NULL(dumped_parts)) {
    for (int64_t i = 0; i < est_part_cnt_ && OB_SUCC(ret); ++i) {
      if (dumped_parts[i]->store_.has_dumped()) {
        if (InputSide::LEFT == input_side) {
          if (OB_FAIL(left_part_map_.set_refactored(dumped_parts[i]->part_key_, dumped_parts[i]))) {
            SQL_ENG_LOG(WARN, "failed to push into hash table", K(ret), K(i),
              K(dumped_parts[i]->part_key_));
          } else {
            left_part_list_.add_last(dumped_parts[i]);
            dumped_parts[i] = nullptr;
          }
        } else {
          if (OB_FAIL(right_part_map_.set_refactored(
              dumped_parts[i]->part_key_, dumped_parts[i]))) {
            SQL_ENG_LOG(WARN, "failed to push into hash table", K(ret), K(i),
              K(dumped_parts[i]->part_key_));
          } else {
            right_part_list_.add_last(dumped_parts[i]);
            dumped_parts[i] = nullptr;
          }
        }
      } else {
        if (0 != dumped_parts[i]->store_.get_row_cnt_in_memory()
            || 0 != dumped_parts[i]->store_.get_row_cnt_on_disk()) {
          ret = OB_ERR_UNEXPECTED;
          SQL_ENG_LOG(WARN, "unexpected status: cur dumped partitions is not empty", K(ret));
        } else {
          dumped_parts[i]->~ObIntraPartition();
          alloc_->free(dumped_parts[i]);
          dumped_parts[i] = nullptr;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (InputSide::LEFT == input_side) {
        alloc_->free(left_dumped_parts_);
        left_dumped_parts_ = nullptr;
      } else {
        alloc_->free(right_dumped_parts_);
        right_dumped_parts_ = nullptr;
      }
    }
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::append_all_dump_parts()
{
  int ret = OB_SUCCESS;
  if (nullptr != left_dumped_parts_ && nullptr != right_dumped_parts_) {
    if (left_part_cur_id_ != right_part_cur_id_) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "unexpected status: part id is not match", K(ret),
        K(cur_part_start_id_), K(left_part_cur_id_), K(right_part_cur_id_));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(append_dumped_parts(InputSide::LEFT))) {
    SQL_ENG_LOG(WARN, "failed to append dumped parts", K(ret));
  } else if (OB_FAIL(append_dumped_parts(InputSide::RIGHT))) {
    SQL_ENG_LOG(WARN, "failed to append dumped parts", K(ret));
  } else {
    left_dumped_parts_ = nullptr;
    right_dumped_parts_ = nullptr;
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::end_round()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(cur_left_part_) || OB_NOT_NULL(cur_right_part_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "cur left or right part is not null", K(ret),
      K(cur_left_part_), K(cur_right_part_));
  } else if (OB_FAIL(append_all_dump_parts())) {
    SQL_ENG_LOG(WARN, "failed to append all dumped parts", K(ret));
  } else {
    left_row_store_iter_.reset();
    right_row_store_iter_.reset();
    hash_table_row_store_iter_.reset();
    preprocess_part_.store_.reset();
    start_round_ = false;
    cur_left_part_ = nullptr;
    cur_right_part_ = nullptr;
    cur_dumped_parts_ = nullptr;
    period_row_cnt_ = 0;
    hash_col_buffer_idx_ = MAX_HASH_COL_CNT;
    hash_col_buffer_ = nullptr;
    store_row_buffer_ = nullptr;
    store_row_buffer_cnt_ = 0;
    if (OB_NOT_NULL(arena_alloc_)) {
      arena_alloc_->reset();
    }
  }
  return ret;
}

// 暂时没有需求，不实现
// for material
template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::direct_insert_row(
  const common::ObIArray<ObExpr*> &exprs, bool &exists, bool &inserted)
{
  UNUSED(exprs);
  UNUSED(exists);
  UNUSED(inserted);
  return OB_NOT_SUPPORTED;
}

// 暂时没有需求，不实现
// for hash join
template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::insert_row_with_hash_table(
  const common::ObIArray<ObExpr*> &exprs, bool &exists, bool &inserted)
{
  UNUSED(exprs);
  UNUSED(exists);
  UNUSED(inserted);
  return OB_NOT_SUPPORTED;
}

// support M: max_memory_sie
//         P: part_cnt
//         DS: data_size (DS = M - P * SS - ES)
//         SS: slot size, 64K
//         ES: extra_size, like hashtable and so on
// optimal equation:
//            f(x) = P * P * DS, denote "one pass" can process max data size
// constraint:
//            P * SS <= DS => P * SS * 2 <= (M - ES) < M, so Part memory size is less than 1/2 M
// we solve the optimal solution
template<typename HashCol, typename HashRowStore>
void ObHashPartInfrastructure<HashCol, HashRowStore>::est_partition_count()
{
  static const int64_t MAX_PART_CNT = 128;
  static const int64_t MIN_PART_CNT = 8;
  int64_t max_mem_size = sql_mem_processor_->get_mem_bound();
  int64_t es = get_mem_used() - sql_mem_processor_->get_data_size();
  int64_t tmp_part_cnt = next_pow2((max_mem_size - es) / 2 / BLOCK_SIZE);
  est_part_cnt_ = tmp_part_cnt = (tmp_part_cnt > MAX_PART_CNT) ? MAX_PART_CNT : tmp_part_cnt;
  int64_t ds = max_mem_size - tmp_part_cnt * BLOCK_SIZE - es;
  int64_t max_f = tmp_part_cnt * tmp_part_cnt * ds;
  int64_t tmp_max_f = 0;
  while (tmp_part_cnt > 0) {
    if (ds >= tmp_part_cnt * BLOCK_SIZE && max_f > tmp_max_f) {
      est_part_cnt_ = tmp_part_cnt;
    }
    tmp_part_cnt >>= 1;
    ds = max_mem_size - tmp_part_cnt * BLOCK_SIZE - es;
    tmp_max_f = tmp_part_cnt * tmp_part_cnt * ds;
  }
  est_part_cnt_ = est_part_cnt_ < MIN_PART_CNT ? MIN_PART_CNT : est_part_cnt_;
}

template<typename HashCol, typename HashRowStore>
int64_t ObHashPartInfrastructure<HashCol, HashRowStore>::est_bucket_count(
  const int64_t rows,
  const int64_t width,
  const int64_t min_bucket_cnt,
  const int64_t max_bucket_cnt)
{
  if (INT64_MAX == est_part_cnt_) {
    est_partition_count();
  }
  int64_t est_bucket_mem_size = next_pow2(rows) * sizeof(void*);
  int64_t est_data_mem_size = rows * width;
  int64_t max_remain_mem_size = std::max(0l, sql_mem_processor_->get_mem_bound() - est_part_cnt_ * BLOCK_SIZE);
  int64_t est_bucket_num = rows;
  while (est_bucket_mem_size + est_data_mem_size > max_remain_mem_size && est_bucket_num > 0) {
    est_bucket_num >>= 1;
    est_bucket_mem_size = next_pow2(est_bucket_num) * sizeof(void*);
    est_data_mem_size = est_bucket_num * width;
  }
  est_bucket_num = est_bucket_num < min_bucket_cnt ? min_bucket_cnt :
                    (est_bucket_num > max_bucket_cnt ? max_bucket_cnt : est_bucket_num);
  sql_mem_processor_->get_profile().set_basic_info(rows, width * rows, est_bucket_num);
  return est_bucket_num;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::insert_row_on_partitions(
  const common::ObIArray<ObExpr*> &exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_dumped_parts_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "unexpected status: cur dumped partitions is null", K(ret));
  } else {
    uint64_t hash_value = 0;
    if (OB_FAIL(calc_hash_value(exprs, hash_value))) {
      SQL_ENG_LOG(WARN, "failed to calc hash value", K(ret));
    } else {
      ObChunkDatumStore::StoredRow *sr = nullptr;
      int64_t part_idx = get_part_idx(hash_value);
      if (OB_FAIL(cur_dumped_parts_[part_idx]->store_.add_row(exprs, eval_ctx_, &sr))) {
        SQL_ENG_LOG(WARN, "failed to add row", K(ret));
      } else {
        HashRowStore *store_row = static_cast<HashRowStore*>(sr);
        store_row->set_hash_value(hash_value);
        store_row->set_is_match(false);
      }
    }
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::
insert_batch_on_partitions(const common::ObIArray<ObExpr *> &exprs,
                           const ObBitVector &skip,
                           const int64_t batch_size,
                           uint64_t *hash_values)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_dumped_parts_) || OB_ISNULL(eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "unexpected status: cur dumped partitions is null", K(ret));
  } else {
    ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
    batch_info_guard.set_batch_idx(0);
    batch_info_guard.set_batch_size(batch_size);
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
      if (skip.at(i)) {
        continue;
      }
      batch_info_guard.set_batch_idx(i);
      ObChunkDatumStore::StoredRow *sr = nullptr;
      int64_t part_idx = get_part_idx(hash_values[i]);
      if (OB_FAIL(cur_dumped_parts_[part_idx]->store_.add_row(exprs, eval_ctx_, &sr))) {
        SQL_ENG_LOG(WARN, "failed to add row", K(ret));
      } else {
        HashRowStore *store_row = static_cast<HashRowStore *>(sr);
        uint64_t hash_value = 0;
        store_row->set_hash_value(hash_values[i]);
        store_row->set_is_match(false);
      }
    }
  }

  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::
set_distinct_batch(const common::ObIArray<ObExpr *> &exprs,
                   uint64_t *hash_values_for_batch,
                   const int64_t batch_size,
                   const ObBitVector *skip,
                   uint16_t *selector,
                   int64_t &selector_size,
                   ObBitVector &my_skip)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(eval_ctx_) || OB_ISNULL(selector)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "eval_ctx is not init ", K(ret));
  } else if (!is_push_down_ && OB_FAIL(hash_table_.check_and_extend())) {
    SQL_ENG_LOG(WARN, "failed to extended hash table", K(ret));
  }
  uint16_t idx = 0;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
  batch_info_guard.set_batch_idx(0);
  batch_info_guard.set_batch_size(batch_size);
  int64_t num_cnt = hash_table_.get_bucket_num() - 1;
  auto &buckets = hash_table_.buckets_;
  if (!is_push_down_) {
    for (int i = 0; i < batch_size; ++i) {
      if (OB_NOT_NULL(skip) && skip->at(i)) {
        continue;
      }
      int64_t bkt_idx = (hash_values_for_batch[i] & num_cnt);
      auto &curr_bkt = buckets->at(bkt_idx);
      __builtin_prefetch(curr_bkt, 0/* read */, 2 /*high temp locality*/);
    }
    for (int i = 0; i < batch_size; ++i) {
      int64_t bkt_idx = (hash_values_for_batch[i] & num_cnt);
      auto &curr_bkt = buckets->at(bkt_idx);
      if ((OB_NOT_NULL(skip) && skip->at(i))
          || nullptr == curr_bkt
          || curr_bkt->hash_value_ != hash_values_for_batch[i]) {
        continue;
      }
      __builtin_prefetch(curr_bkt->next(), 0/* read */, 2 /*high temp locality*/);
      if (!curr_bkt->use_expr_) {
        __builtin_prefetch(curr_bkt->store_row_, 0/* read */, 2 /*high temp locality*/);
      }
    }
  }
  for (int i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
    if (OB_NOT_NULL(skip) && skip->at(i)) {
      my_skip.set(i);
      continue;
    }
    batch_info_guard.set_batch_idx(i);
    ObHashPartCols *hash_col = nullptr;
    if (hash_col_buffer_idx_ < MAX_HASH_COL_CNT) {
      hash_col = &hash_col_buffer_[hash_col_buffer_idx_];
    } else if (OB_ISNULL(hash_col_buffer_
          = static_cast<ObHashPartCols *>
           (arena_alloc_->alloc(MAX_HASH_COL_CNT * sizeof(ObHashPartCols))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "failed to alloc mem for hash col buffer", K(ret));
    } else {
      hash_col_buffer_idx_ = 0;
      hash_col = &hash_col_buffer_[hash_col_buffer_idx_];
    }
    if (OB_SUCC(ret)) {
      ObHashPartCols *new_part_cols = new(hash_col) ObHashPartCols;
      hash_table_.exprs_ = &exprs;
      new_part_cols->set_hash_value(hash_values_for_batch[i]);
      new_part_cols->batch_idx_ = i;
      items_[i] = new_part_cols;
      if (OB_FAIL(hash_table_.set_distinct(*new_part_cols, hash_values_for_batch[i]))) {
        if (OB_HASH_EXIST == ret) {
          ret = OB_SUCCESS;
          my_skip.set(i);
        } else {
          SQL_ENG_LOG(WARN, "failed to set distinct value into hash table", K(ret));
        }
      } else {
        hash_col_buffer_idx_++;
        selector[idx++] = i;
      }
    }
  }
  if (OB_SUCC(ret)) {
    selector_size = idx;
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::
  probe_batch(const common::ObIArray<ObExpr *> &exprs,
              uint64_t *hash_values_for_batch,
              const int64_t batch_size,
              const ObBitVector *skip,
              ObBitVector &my_skip)
{
  int ret = OB_SUCCESS;
  const ObHashPartCols part_cols;
  hash_table_.exprs_ = &exprs;
  const HashCol *exists_part_cols = nullptr;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
  batch_info_guard.set_batch_idx(0);
  batch_info_guard.set_batch_size(batch_size);
  if (!is_push_down_ && OB_NOT_NULL(hash_table_.buckets_)) {
    for (int64_t i = 0; i < batch_size; ++i) {
      if (OB_NOT_NULL(skip) && skip->at(i)) {
        continue;
      }
      __builtin_prefetch(hash_table_.buckets_->at(hash_values_for_batch[i]
                                              & (hash_table_.get_bucket_num() - 1)), 0/* read */,
                                              2 /*high temp locality*/);
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
    //if skip is nullptr, means rows from datum store which are bushy
    if (OB_NOT_NULL(skip) && skip->at(i)) {
      my_skip.set(i);
      continue;
    }
    batch_info_guard.set_batch_idx(i);
    if (OB_FAIL(hash_table_.get(hash_values_for_batch[i],
                                      part_cols,
                                      exists_part_cols))) {
      SQL_ENG_LOG(WARN, "failed to get item", K(ret));
    } else if (OB_NOT_NULL(exists_part_cols)) {
      my_skip.set(i);
    }
  }
  return ret;
}
template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::
set_item_ptrs(const ObIArray<ObExpr *> &exprs,
              const int64_t batch_size,
              uint64_t *hash_values,
              const uint16_t *selector,
              const int64_t selector_size,
              ObBitVector &my_skip)
{
  int ret = OB_SUCCESS;
  ObChunkDatumStore::StoredRow **store_row = nullptr;
  //check selector and eval_ctx_ before
  if (OB_ISNULL(store_row_buffer_) || store_row_buffer_cnt_ < batch_size) {
    if (OB_ISNULL(store_row_buffer_ = static_cast<ObChunkDatumStore::StoredRow **>
          (arena_alloc_->alloc(sizeof(ObChunkDatumStore::StoredRow *) * batch_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "failed to alloc memory for store row", K(ret));
    } else {
      store_row_buffer_cnt_ = batch_size;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(preprocess_part_.store_.add_batch(exprs, *eval_ctx_,
                                                        my_skip, batch_size,
                                                        selector, selector_size, store_row_buffer_))) {
    SQL_ENG_LOG(WARN, "failed to add batch in store", K(ret));
  } else {
    for (int64_t i = 0; i < selector_size; ++i) {
      ObHashPartCols *new_part_cols = items_[selector[i]];
      //new_part_cols->use_expr_ = false;
      new_part_cols->store_row_ = static_cast<HashRowStore *> (store_row_buffer_[i]);
      new_part_cols->store_row_->set_hash_value(hash_values[selector[i]]);
      new_part_cols->store_row_->set_is_match(false);
    }
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::create_dumped_partitions(
  InputSide input_side)
{
  int ret = OB_SUCCESS;
  if (INT64_MAX == est_part_cnt_) {
    est_partition_count();
  }
  if (sizeof(uint64_t) * CHAR_BIT <= part_shift_) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_ENG_LOG(WARN, "too deep part level", K(ret), K(part_shift_));
  } else if (!has_create_part_map_) {
    has_create_part_map_ = true;
    if (OB_FAIL(left_part_map_.create(
        512, "HashInfraOp", "HashInfraOp", tenant_id_))) {
      SQL_ENG_LOG(WARN, "failed to create hash map", K(ret));
    } else if (OB_FAIL(right_part_map_.create(
        512, "HashInfraOp", "HashInfraOp", tenant_id_))) {
      SQL_ENG_LOG(WARN, "failed to create hash map", K(ret));
    }
  }
  has_cur_part_dumped_ = true;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(cur_dumped_parts_ = static_cast<ObIntraPartition**>(
      alloc_->alloc(sizeof(ObIntraPartition*) * est_part_cnt_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_ENG_LOG(WARN, "failed to allocate memory", K(ret));
  } else {
    sql_mem_processor_->set_number_pass(cur_level_ + 1);
    MEMSET(cur_dumped_parts_, 0, sizeof(ObIntraPartition*) * est_part_cnt_);
    int32_t delta_shift = min(__builtin_ctz(est_part_cnt_), 8);
    for (int64_t i = 0; i < est_part_cnt_ && OB_SUCC(ret); ++i) {
      void *mem = alloc_->alloc(sizeof(ObIntraPartition));
      if (OB_ISNULL(mem)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_ENG_LOG(WARN, "failed to allocate memory", K(ret));
      } else {
        cur_dumped_parts_[i] = new (mem) ObIntraPartition();
        ObIntraPartition *part = cur_dumped_parts_[i];
        if (OB_FAIL((this->*init_part_func_)(part, cur_part_start_id_ + i, 1, delta_shift))) {
          SQL_ENG_LOG(WARN, "failed to create part", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i < est_part_cnt_; ++i) {
        ObIntraPartition *part = cur_dumped_parts_[i];
        if (OB_NOT_NULL(part)) {
          part->~ObIntraPartition();
          alloc_->free(part);
          cur_dumped_parts_[i] = nullptr;
        }
      }
      alloc_->free(cur_dumped_parts_);
      cur_dumped_parts_ = nullptr;
    }
  }
  if (OB_SUCC(ret)) {
    if (InputSide::LEFT == input_side) {
      if (OB_NOT_NULL(left_dumped_parts_)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "unexpected status: left is dumped", K(ret));
      } else {
        left_dumped_parts_ = cur_dumped_parts_;
        left_part_cur_id_ = cur_part_start_id_ + est_part_cnt_;
        SQL_ENG_LOG(TRACE, "left is dumped", K(ret));
      }
    } else {
      if (OB_NOT_NULL(right_dumped_parts_)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "unexpected status: right is dumped", K(ret));
      } else {
        right_dumped_parts_ = cur_dumped_parts_;
        right_part_cur_id_ = cur_part_start_id_ + est_part_cnt_;
        SQL_ENG_LOG(TRACE, "right is dumped", K(ret));
      }
    }
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::exists_row(
  const common::ObIArray<ObExpr*> &exprs,
  const HashCol *&exists_part_cols)
{
  int ret = OB_SUCCESS;
  ObHashPartCols part_cols;
  hash_table_.exprs_ = &exprs;
  uint64_t hash_value = 0;
  exists_part_cols = nullptr;
  if (OB_FAIL(calc_hash_value(exprs, hash_value))) {
    SQL_ENG_LOG(WARN, "failed to calc hash value", K(ret));
  } else if (OB_FAIL(hash_table_.get(hash_value, part_cols, exists_part_cols))) {
    SQL_ENG_LOG(WARN, "failed to get item", K(ret));
  }
  return ret;
}

//exist function for hash intersect
template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::exists_batch(
                   const common::ObIArray<ObExpr*> &exprs,
                   const int64_t batch_size,
                   const ObBitVector *child_skip,
                   ObBitVector *skip,
                   uint64_t *hash_values_for_batch)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(hash_values_for_batch)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "hash values vector is not init", K(ret));
  } else if (OB_ISNULL(skip)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "skip vector is null", K(ret));
  } else if (OB_FAIL(calc_hash_value_for_batch(exprs, batch_size,
                                              child_skip, hash_values_for_batch))) {
    SQL_ENG_LOG(WARN, "failed to calc hash values", K(ret));
  } else {
    const ObHashPartCols part_cols;
    hash_table_.exprs_ = &exprs;
    const HashCol *exists_part_cols = nullptr;
    ObBitVector &skip_for_dump = *my_skip_;
    skip_for_dump.reset(batch_size);
    for (int i = 0; i < batch_size; ++i) {
      if (OB_NOT_NULL(child_skip) && child_skip->at(i)) {
        continue;
      }
      __builtin_prefetch(hash_table_.buckets_->at(hash_values_for_batch[i]
                                             & (hash_table_.get_bucket_num() - 1)), 0/* read */,
                                             2 /*high temp locality*/);
    }
    {
      ObEvalCtx::BatchInfoScopeGuard guard(*eval_ctx_);
      guard.set_batch_idx(0);
      for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
        //if nullptr, data is from dumped partition
        if (OB_NOT_NULL(child_skip) && child_skip->at(i)) {
          skip->set(i); //skip indicates rows need to return, only useful for intersect
          skip_for_dump.set(i); //skip_for_dump indicates rows need to dump
          continue;
        }
        guard.set_batch_idx(i);
        if (OB_FAIL(hash_table_.get(hash_values_for_batch[i], part_cols, exists_part_cols))) {
          SQL_ENG_LOG(WARN, "failed to get item", K(ret));
        } else if (OB_ISNULL(exists_part_cols)) {
          skip->set(i);
        } else if (exists_part_cols->store_row_->is_match()) {
          skip->set(i);
          //we dont need dumped this row
          skip_for_dump.set(i);
        } else {
          exists_part_cols->store_row_->set_is_match(true);
          //we dont need dumped this row
          skip_for_dump.set(i);
        }
      }
    }
    if (OB_SUCC(ret) && has_left_dumped()) {
      // dump right row if left is dumped
      if (!has_right_dumped()
          && OB_FAIL(create_dumped_partitions(InputSide::RIGHT))) {
        SQL_ENG_LOG(WARN, "failed to create dump partitions", K(ret));
      } else if (OB_FAIL(insert_batch_on_partitions(exprs, skip_for_dump,
                                                    batch_size, hash_values_for_batch))) {
        SQL_ENG_LOG(WARN, "failed to insert row into partitions", K(ret));
      }
    }
    my_skip_->reset(batch_size);
  }
  return ret;
}

// 目前仅支持类型一致情况
template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::calc_hash_value(
  const common::ObIArray<ObExpr*> &exprs,
  uint64_t &hash_value)
{
  int ret = OB_SUCCESS;
  hash_value = DEFAULT_PART_HASH_VALUE;
  if (OB_ISNULL(hash_funcs_) || OB_ISNULL(sort_collations_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "unexpect status: hash funcs is null", K(ret));
  } else if (0 != sort_collations_->count()) {
    ObDatum *datum = nullptr;
    for (int64_t i = 0; i < sort_collations_->count() && OB_SUCC(ret); ++i) {
      const int64_t idx = sort_collations_->at(i).field_idx_;
      if (OB_FAIL(exprs.at(idx)->eval(*eval_ctx_, datum))) {
        SQL_ENG_LOG(WARN, "failed to eval expr", K(ret));
      } else if (OB_FAIL(hash_funcs_->at(i).hash_func_(*datum, hash_value, hash_value))) {
        SQL_ENG_LOG(WARN, "failed to do hash", K(ret));
      }
    }
  }
  hash_value &= HashRowStore::get_hash_mask();
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::calc_hash_value_for_batch(
  const common::ObIArray<ObExpr *> &exprs,
  const int64_t batch_size,
  const ObBitVector *skip,
  uint64_t *hash_values_for_batch,
  int64_t start_idx,
  uint64_t *hash_vals)
{
  int ret = OB_SUCCESS;
  uint64_t default_hash_value = DEFAULT_PART_HASH_VALUE;
  if (OB_ISNULL(hash_funcs_) || OB_ISNULL(sort_collations_) || OB_ISNULL(skip)
      || OB_ISNULL(eval_ctx_)
      || OB_ISNULL(hash_values_for_batch)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "hash func or sort collation or hash values vector not init",
                K(hash_funcs_), K(sort_collations_), K(batch_size), K(skip),
                K(hash_values_for_batch), K(ret));
  } else if (0 == batch_size) {
    //do nothing
  } else if (0 != sort_collations_->count()) {
    //from child op, need eval
    for (int64_t j = start_idx; OB_SUCC(ret) && j < sort_collations_->count(); ++j) {
      const int64_t idx = sort_collations_->at(j).field_idx_;
      if (OB_FAIL(exprs.at(idx)->eval_batch(*eval_ctx_, *skip, batch_size))) {
        SQL_ENG_LOG(WARN, "failed to eval batch", K(ret), K(j));
      }
    }
    if (OB_SUCC(ret)) {
      if (nullptr != hash_vals) {
        for (int64_t i = 0; i < batch_size; ++i) {
          if (skip->exist(i)) {
            continue;
          }
          hash_values_for_batch[i] = hash_vals[i];
        }
      }
      for (int64_t j = start_idx; j < sort_collations_->count(); ++j) {
        bool is_batch_seed = (0 != j);
        const int64_t idx = sort_collations_->at(j).field_idx_;
        ObBatchDatumHashFunc hash_func = hash_funcs_->at(j).batch_hash_func_;
        ObDatum &curr_datum = exprs.at(idx)->locate_batch_datums(*eval_ctx_)[0];
        if (0 == j) {
          hash_func(hash_values_for_batch, &curr_datum, exprs.at(idx)->is_batch_result(),
                    *skip, batch_size, &default_hash_value, is_batch_seed);
        } else {
          hash_func(hash_values_for_batch, &curr_datum, exprs.at(idx)->is_batch_result(),
                    *skip, batch_size, hash_values_for_batch, is_batch_seed);
        }
      }
      for (int64_t i = 0; i < batch_size; ++i) {
        hash_values_for_batch[i] &= HashRowStore::get_hash_mask();
      }
    }
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::update_mem_status_periodically()
{
  int ret = OB_SUCCESS;
  bool updated = false;
  if (OB_FAIL(sql_mem_processor_->update_max_available_mem_size_periodically(
                    alloc_,
                    [&](int64_t cur_cnt){ return period_row_cnt_ > cur_cnt; },
                    updated))) {
    SQL_ENG_LOG(WARN, "failed to update usable memory size periodically", K(ret));
  } else if (updated) {
    //no error no will return , do not check
    sql_mem_processor_->update_used_mem_size(get_mem_used());
    est_partition_count();
  }
  return ret;
}

// for hash union, intersect, except
//  and hash groupby distinct
template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::insert_row_with_unique_hash_table(
  const common::ObIArray<ObExpr*> &exprs,
  bool &exists,
  bool &inserted)
{
  int ret = OB_SUCCESS;
  uint64_t hash_value = 0;
  if (OB_FAIL(calc_hash_value(exprs, hash_value))) {
    SQL_ENG_LOG(WARN, "failed to calc hash value", K(ret));
  } else if (OB_FAIL(do_insert_row_with_unique_hash_table(exprs, hash_value, exists, inserted))) {
    SQL_ENG_LOG(WARN, "failed to insert row into hash table", K(ret));
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::do_insert_row_with_unique_hash_table(
      const common::ObIArray<ObExpr*> &exprs, uint64_t hash_value, bool &exists, bool &inserted)
{
  int ret = OB_SUCCESS;
  const ObHashPartCols part_cols;
  hash_table_.exprs_ = &exprs;
  const HashCol tmp_cols;
  const HashCol *exists_part_cols = nullptr;
  if (!has_cur_part_dumped_ && OB_FAIL(update_mem_status_periodically())) {
    SQL_ENG_LOG(WARN, "failed to update memory status periodically", K(ret));
  } else if (OB_FAIL(hash_table_.get(hash_value, part_cols, exists_part_cols))) {
    SQL_ENG_LOG(WARN, "failed to get item", K(ret));
  } else {
    ObChunkDatumStore::StoredRow *sr = nullptr;
    bool dumped = false;
    void *buf = nullptr;
    exists = false;
    inserted = false;
    if (OB_ISNULL(exists_part_cols)) {
      bool dummy_is_block = false; // unused
      bool dummy_full_by_pass = false; // unused
      // not exists, need create and add
      if (!has_cur_part_dumped_) {
        if (OB_FAIL(process_dump(dummy_is_block, dummy_full_by_pass))) {
          SQL_ENG_LOG(WARN, "failed to process dump", K(ret));
        } else if (has_cur_part_dumped_) {
          // dumped
          if (OB_FAIL(insert_row_on_partitions(exprs))) {
            SQL_ENG_LOG(WARN, "failed to insert row on partitions", K(ret));
          }
        } else if (OB_FAIL(preprocess_part_.store_.add_row(exprs, eval_ctx_, &sr))) {
          SQL_ENG_LOG(WARN, "failed to add row into row store", K(ret));
        } else if (OB_ISNULL(buf = arena_alloc_->alloc(sizeof(HashCol)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SQL_ENG_LOG(WARN, "failed to allocate memory", K(ret));
        } else {
          HashCol *new_part_cols = new (buf) HashCol;
          //new_part_cols->use_expr_ = false;
          new_part_cols->store_row_ = static_cast<HashRowStore*>(sr);
          new_part_cols->set_hash_value(hash_value);
          new_part_cols->store_row_->set_is_match(false);
          if (OB_FAIL(hash_table_.set(*new_part_cols))) {
            SQL_ENG_LOG(WARN, "failed to set part cols", K(ret));
          } else {
            inserted = true;
            SQL_ENG_LOG(DEBUG, "insert exprs", K(hash_value), K(ROWEXPR2STR(*eval_ctx_, exprs)));
          }
        }
      } else {
        // dumped
        if (OB_FAIL(insert_row_on_partitions(exprs))) {
          SQL_ENG_LOG(WARN, "failed to insert row on partitions", K(ret));
        }
      }
    } else {
      // exists, return exists error
      exists = true;
      SQL_ENG_LOG(DEBUG, "insert exprs", K(hash_value), K(ROWEXPR2STR(*eval_ctx_, exprs)));
    }
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::
do_insert_row_with_unique_hash_table_by_pass(const common::ObIArray<ObExpr*> &exprs,
                                             bool is_block,
                                             bool can_insert,
                                             bool &exists,
                                             bool &inserted,
                                             bool &full_by_pass)
{
  int ret = OB_SUCCESS;
  ++period_row_cnt_;
  uint64_t hash_value = 0;
  if (OB_FAIL(calc_hash_value(exprs, hash_value))) {
    SQL_ENG_LOG(WARN, "failed to calc hash value", K(ret));
  } else {
    const ObHashPartCols part_cols;
    hash_table_.exprs_ = &exprs;
    const HashCol tmp_cols;
    const HashCol *exists_part_cols = nullptr;
    if (!has_cur_part_dumped_ && OB_FAIL(update_mem_status_periodically())) {
      SQL_ENG_LOG(WARN, "failed to update memory status periodically", K(ret));
    } else if (OB_FAIL(hash_table_.get(hash_value, part_cols, exists_part_cols))) {
      SQL_ENG_LOG(WARN, "failed to get item", K(ret));
    } else {
      ObChunkDatumStore::StoredRow *sr = nullptr;
      bool dumped = false;
      void *buf = nullptr;
      exists = false;
      inserted = false;
      if (OB_ISNULL(exists_part_cols)) {
        // not exists, need create and add
        if (!has_cur_part_dumped_) {
          if (OB_FAIL(process_dump(is_block, full_by_pass))) {
            SQL_ENG_LOG(WARN, "failed to process dump", K(ret));
          } else if (has_cur_part_dumped_) {
            // dumped
            if (OB_FAIL(insert_row_on_partitions(exprs))) {
              SQL_ENG_LOG(WARN, "failed to insert row on partitions", K(ret));
            }
          } else if (!can_insert) {
            inserted = true; //by pass for unblock disitnct
          } else {
            if (OB_FAIL(preprocess_part_.store_.add_row(exprs, eval_ctx_, &sr))) {
              SQL_ENG_LOG(WARN, "failed to add row into row store", K(ret));
            } else if (can_insert) { // for block && !can_insert, do not store in hash table
              if (OB_ISNULL(buf = arena_alloc_->alloc(sizeof(HashCol)))) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                SQL_ENG_LOG(WARN, "failed to allocate memory", K(ret));
              } else {
                HashCol *new_part_cols = new (buf) HashCol;
                //new_part_cols->use_expr_ = false;
                new_part_cols->store_row_ = static_cast<HashRowStore*>(sr);
                new_part_cols->set_hash_value(hash_value);
                new_part_cols->store_row_->set_is_match(false);
                if (OB_FAIL(hash_table_.set(*new_part_cols))) {
                  SQL_ENG_LOG(WARN, "failed to set part cols", K(ret));
                } else {
                  inserted = true;
                  SQL_ENG_LOG(DEBUG, "insert exprs", K(hash_value), K(ROWEXPR2STR(*eval_ctx_, exprs)));
                }
              }
            }
          }
        } else {
          // dumped
          if (OB_FAIL(insert_row_on_partitions(exprs))) {
            SQL_ENG_LOG(WARN, "failed to insert row on partitions", K(ret));
          }
        }
      } else {
        // exists, return exists error
        exists = true;
        SQL_ENG_LOG(DEBUG, "insert exprs", K(hash_value), K(ROWEXPR2STR(*eval_ctx_, exprs)));
      }
    }
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::
do_insert_batch_with_unique_hash_table(const common::ObIArray<ObExpr *> &exprs,
                                       uint64_t *hash_values_for_batch,
                                       const int64_t batch_size,
                                       const ObBitVector *skip,
                                       ObBitVector *&output_vec)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(my_skip_) || OB_ISNULL(eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "my_skip_ or eval_ctx_ is not init", K(ret), K(my_skip_), K(eval_ctx_));
  } else if (OB_ISNULL(hash_values_for_batch)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "hash values vector is nit init", K(ret));
  } else {
    my_skip_->reset(batch_size);
    output_vec = my_skip_;
    if (!has_cur_part_dumped_) {
      uint16_t selector[batch_size];
      int64_t selector_size = 0;
      bool dummy_is_block = false; // unused
      bool dummy_full_by_pass = false; // unused
      if (OB_FAIL(set_distinct_batch(exprs, hash_values_for_batch, batch_size,
                                           skip, selector, selector_size, *my_skip_))) {
        SQL_ENG_LOG(WARN, "failed to set distinct values into hash table", K(ret));
      } else if (OB_FAIL(set_item_ptrs(exprs, batch_size, hash_values_for_batch,
                                       selector, selector_size, *my_skip_))) {
        SQL_ENG_LOG(WARN, "failed to add datum store and set item ptr", K(ret));
      } else if (OB_FAIL(update_mem_status_periodically())) {
        SQL_ENG_LOG(WARN, "failed to update memory status periodically", K(ret));
      } else if (OB_FAIL(process_dump(dummy_is_block, dummy_full_by_pass))) {
        SQL_ENG_LOG(WARN, "failed to process dump", K(ret));
      }
    } else if (OB_FAIL(probe_batch(exprs, hash_values_for_batch, batch_size,
                                   skip, *my_skip_))) {
      SQL_ENG_LOG(WARN, "failed to probe distinct values for batch", K(ret));
    } else if (OB_FAIL(insert_batch_on_partitions(exprs, *my_skip_,
                                            batch_size, hash_values_for_batch))) {
      SQL_ENG_LOG(WARN, "failed to insert batch on partitions", K(ret));
    } else if (FALSE_IT(my_skip_->set_all(batch_size))) {
    }
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::
do_insert_batch_with_unique_hash_table_by_pass(const common::ObIArray<ObExpr *> &exprs,
                                               uint64_t *hash_values_for_batch,
                                               const int64_t batch_size,
                                               const ObBitVector *skip,
                                               bool is_block,
                                               bool can_insert,
                                               int64_t &exists,
                                               bool &full_by_pass,
                                               ObBitVector *&output_vec)
{
  int ret = OB_SUCCESS;
  ++period_row_cnt_;
  exists = 0;
  if (OB_ISNULL(my_skip_) || OB_ISNULL(eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "my_skip_ or eval_ctx_ is not init", K(ret), K(my_skip_), K(eval_ctx_));
  } else if (OB_ISNULL(hash_values_for_batch)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "hash values vector is nit init", K(ret));
  } else {
    my_skip_->reset(batch_size);
    output_vec = my_skip_;
    if (!has_cur_part_dumped_) {
      uint16_t selector[batch_size];
      int64_t selector_size = 0;
      if (!can_insert) {
       if (OB_FAIL(probe_batch(exprs, hash_values_for_batch, batch_size,skip, *my_skip_))) {
         SQL_ENG_LOG(WARN, "failed to probe batch for pass by", K(ret));
       } else {
         int64_t init_skip_cnt = nullptr == skip ? 0 : skip->accumulate_bit_cnt(batch_size);
         exists = (batch_size - init_skip_cnt)
                      - (batch_size - my_skip_->accumulate_bit_cnt(batch_size));
       }
      } else {
        if (OB_FAIL(set_distinct_batch(exprs, hash_values_for_batch, batch_size,
                                            skip, selector, selector_size, *my_skip_))) {
          SQL_ENG_LOG(WARN, "failed to set distinct values into hash table", K(ret));
        } else if (OB_FAIL(set_item_ptrs(exprs, batch_size, hash_values_for_batch,
                                        selector, selector_size, *my_skip_))) {
          SQL_ENG_LOG(WARN, "failed to add datum store and set item ptr", K(ret));
        } else if (OB_FAIL(update_mem_status_periodically())) {
          SQL_ENG_LOG(WARN, "failed to update memory status periodically", K(ret));
        } else if (OB_FAIL(process_dump(is_block, full_by_pass))) {
          SQL_ENG_LOG(WARN, "failed to process dump", K(ret));
        }
      }
    } else if (OB_FAIL(probe_batch(exprs, hash_values_for_batch, batch_size,
                                   skip, *my_skip_))) {
      SQL_ENG_LOG(WARN, "failed to probe distinct values for batch", K(ret));
    } else if (OB_FAIL(insert_batch_on_partitions(exprs, *my_skip_,
                                            batch_size, hash_values_for_batch))) {
      SQL_ENG_LOG(WARN, "failed to insert batch on partitions", K(ret));
    } else if (FALSE_IT(my_skip_->set_all(batch_size))) {
    }
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::insert_row_on_hash_table(
  const common::ObIArray<ObExpr*> &exprs, bool &exists, bool &inserted)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL((this->*insert_row_func_)(exprs, exists, inserted))) {
    // 暂时不打印trace或者warn，因为会返回exists或者not exists
    // LOG_TRACE("failed to insert row func", K(ret));
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::insert_row(
  const common::ObIArray<ObExpr*> &exprs, bool &exists, bool &inserted)
{
  int ret = OB_SUCCESS;
  ++period_row_cnt_;
  if (OB_FAIL(insert_row_on_hash_table(exprs, exists, inserted))) {
    // 暂时不打印trace或者warn，因为会返回exists或者not exists
    // LOG_TRACE("failed to insert row func", K(ret));
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::
insert_row_for_batch(const common::ObIArray<ObExpr *> &batch_exprs,
                     uint64_t *hash_values_for_batch,
                     const int64_t batch_size,
                     const ObBitVector *skip,
                     ObBitVector *&output_vec)
{
  int ret = OB_SUCCESS;
  ++period_row_cnt_;
  if (OB_FAIL(do_insert_batch_with_unique_hash_table(batch_exprs,
                                                  hash_values_for_batch,
                                                  batch_size,
                                                  skip,
                                                  output_vec))) {
    SQL_ENG_LOG(WARN, "failed to insert batch", K(ret));
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::finish_insert_row()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(cur_dumped_parts_)) {
    for (int64_t i = 0; i < est_part_cnt_ && OB_SUCC(ret); ++i) {
      SQL_ENG_LOG(TRACE, "trace dumped partition",
        K(cur_dumped_parts_[i]->store_.get_row_cnt_in_memory()),
        K(cur_dumped_parts_[i]->store_.get_row_cnt_on_disk()),
        K(i), K(est_part_cnt_), K(cur_dumped_parts_[i]->part_key_));
      if (OB_FAIL(cur_dumped_parts_[i]->store_.dump(false, true))) {
        SQL_ENG_LOG(WARN, "failed to dump row store", K(ret));
      } else if (OB_FAIL(cur_dumped_parts_[i]->store_.finish_add_row(true))) {
        SQL_ENG_LOG(WARN, "failed to finish add row", K(ret));
      }
    }
    cur_dumped_parts_ = nullptr;
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::get_next_left_partition()
{
  int ret = OB_SUCCESS;
  SQL_ENG_LOG(TRACE, "trace left part count", K(left_part_list_.get_size()));
  cur_left_part_ = left_part_list_.remove_last();
  if (OB_NOT_NULL(cur_left_part_)) {
    ObIntraPartition *tmp_part = nullptr;
    if (OB_FAIL(left_part_map_.erase_refactored(cur_left_part_->part_key_, &tmp_part))) {
      SQL_ENG_LOG(WARN, "failed to remove part from map", K(ret), K(cur_left_part_->part_key_));
    } else if (cur_left_part_ != tmp_part) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "unexepcted status: part is not match", K(ret),
        K(cur_left_part_), K(tmp_part));
    }
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::get_next_right_partition()
{
  int ret = OB_SUCCESS;
  SQL_ENG_LOG(TRACE, "trace right part count", K(right_part_list_.get_size()));
  cur_right_part_ = right_part_list_.remove_last();
  if (OB_NOT_NULL(cur_right_part_)) {
    ObIntraPartition *tmp_part = nullptr;
    if (OB_FAIL(right_part_map_.erase_refactored(cur_right_part_->part_key_, &tmp_part))) {
      SQL_ENG_LOG(WARN, "failed to remove part from map", K(ret), K(cur_right_part_->part_key_));
    } else if (cur_right_part_ != tmp_part) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "unexepcted status: part is not match",
        K(ret), K(cur_right_part_), K(tmp_part));
    }
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::get_cur_matched_partition(
  InputSide input_side)
{
  int ret = OB_SUCCESS;
  ObIntraPartition *part = nullptr;
  ObIntraPartition *matched_part = nullptr;
  if (InputSide::LEFT == input_side) {
    part = cur_left_part_;
  } else {
    part = cur_right_part_;
  }
  if (OB_ISNULL(part)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "unexpect status: part is null", K(ret));
  } else if (part->part_key_.is_left()) {
    ObIntraPartition *tmp_part = nullptr;
    ObIntraPartKey part_key = part->part_key_;
    part_key.set_right();
    if (OB_FAIL(right_part_map_.erase_refactored(part_key, &tmp_part))) {
    } else {
      matched_part = tmp_part;
      right_part_list_.remove(tmp_part);
    }
  } else {
    ObIntraPartition *tmp_part = nullptr;
    ObIntraPartKey part_key = part->part_key_;
    part_key.set_left();
    if (OB_FAIL(left_part_map_.erase_refactored(part_key, &tmp_part))) {
    } else {
      matched_part = tmp_part;
      left_part_list_.remove(tmp_part);
    }
  }
  if (OB_SUCC(ret) && nullptr != matched_part) {
    if (InputSide::LEFT == input_side) {
      cur_right_part_ = matched_part;
    } else {
      cur_left_part_ = matched_part;
    }
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::open_hash_table_part()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(hash_table_row_store_iter_.init(&preprocess_part_.store_))) {
    SQL_ENG_LOG(WARN, "failed to init row store iterator", K(ret));
  }
  return ret;
}

// close仅仅关闭iterator不会清理数据
template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::close_hash_table_part()
{
  int ret = OB_SUCCESS;
  hash_table_row_store_iter_.reset();
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::open_cur_part(InputSide input_side)
{
  int ret = OB_SUCCESS;
  if (!start_round_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "round is not start", K(ret), K(start_round_));
  } else if ((InputSide::LEFT == input_side && nullptr == cur_left_part_)
      || (InputSide::RIGHT == input_side && nullptr == cur_right_part_)) {
    SQL_ENG_LOG(WARN, "cur part is null", K(ret), K(input_side));
  } else if (InputSide::LEFT == input_side) {
    if (OB_FAIL(left_row_store_iter_.init(&cur_left_part_->store_))) {
      SQL_ENG_LOG(WARN, "failed to init row store iterator", K(ret));
    } else {
      cur_side_ = input_side;
      cur_level_ = cur_left_part_->part_key_.level_;
      part_shift_ = cur_left_part_->part_key_.part_shift_;
      SQL_ENG_LOG(TRACE, "trace open left part", K(ret), K(cur_left_part_->part_key_),
        K(cur_left_part_->store_.get_row_cnt_in_memory()),
        K(cur_left_part_->store_.get_row_cnt_on_disk()));
    }
  } else if (InputSide::RIGHT == input_side) {
    if (OB_FAIL(right_row_store_iter_.init(&cur_right_part_->store_))) {
      SQL_ENG_LOG(WARN, "failed to init row store iterator", K(ret));
    } else {
      cur_side_ = input_side;
      cur_level_ = cur_right_part_->part_key_.level_;
      part_shift_ = cur_right_part_->part_key_.part_shift_;
      SQL_ENG_LOG(TRACE, "trace open right part", K(ret), K(cur_right_part_->part_key_),
        K(cur_right_part_->store_.get_row_cnt_in_memory()),
        K(cur_right_part_->store_.get_row_cnt_on_disk()));
    }
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::close_cur_part(InputSide input_side)
{
  int ret = OB_SUCCESS;
  has_cur_part_dumped_ = false;
  ObIntraPartition *tmp_part = nullptr;
  if (!start_round_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "round is not start", K(ret), K(start_round_));
  } else if ((InputSide::LEFT == input_side && nullptr == cur_left_part_)
      || (InputSide::RIGHT == input_side && nullptr == cur_right_part_)) {
    SQL_ENG_LOG(WARN, "cur part is null", K(ret), K(input_side));
  } else if (InputSide::LEFT == input_side) {
    left_row_store_iter_.reset();
    tmp_part = cur_left_part_;
    cur_left_part_ = nullptr;
  } else if (InputSide::RIGHT == input_side) {
    right_row_store_iter_.reset();
    tmp_part = cur_right_part_;
    cur_right_part_ = nullptr;
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(tmp_part)) {
    tmp_part->~ObIntraPartition();
    alloc_->free(tmp_part);
    tmp_part = nullptr;
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::get_next_partition(InputSide input_side)
{
  int ret = OB_SUCCESS;
  if (InputSide::LEFT == input_side) {
    switch_left();
    if (OB_NOT_NULL(cur_left_part_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "unexpected status: cur partition is not null", K(ret));
    }
  } else {
    switch_right();
    if (OB_NOT_NULL(cur_right_part_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "unexpected status: cur partition is not null", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!has_create_part_map_) {
    // hash map is not created, so it can't dumped
    ret = OB_ITER_END;
  } else if (is_left()) {
    if (OB_FAIL(get_next_left_partition())) {
      if (OB_ITER_END != ret) {
        SQL_ENG_LOG(WARN, "failed to get next left partition");
      }
    } else if (OB_ISNULL(cur_left_part_)
        || InputSide::LEFT != cur_left_part_->part_key_.nth_way_) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "unexpected status: current part is wrong", K(ret), K(cur_left_part_));
    } else {
      cur_side_ = InputSide::LEFT;
    }
  } else {
    if (OB_FAIL(get_next_right_partition())) {
      if (OB_ITER_END != ret) {
        SQL_ENG_LOG(WARN, "failed to get next right partition");
      }
    } else if (OB_ISNULL(cur_right_part_)
        || InputSide::RIGHT != cur_right_part_->part_key_.nth_way_) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "unexpected status: current part is wrong", K(ret), K(cur_right_part_));
    } else {
      cur_side_ = InputSide::RIGHT;
    }
  }
  return ret;
}

// 按照input_side获取下一组的partition pair<left, right>
template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::get_next_pair_partition(
  InputSide input_side)
{
  int ret = OB_SUCCESS;
  // 这里暂时按照input_side去拿分区，其实如果需要拿最近添加的partition，应该拿left和right中最近加入的分区
  // 即可以取list中两者最后一个分区进行对比，哪个最近加入，获取哪个
  if (OB_FAIL(get_next_partition(input_side))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else {
      SQL_ENG_LOG(WARN, "failed to get next partition", K(ret));
    }
  } else if (OB_FAIL(get_cur_matched_partition(input_side))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      SQL_ENG_LOG(WARN, "failed to get next partition", K(ret));
    }
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::get_right_next_row(
  const ObChunkDatumStore::StoredRow *&store_row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_right_part_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "unexpected status: current partition is null", K(cur_right_part_));
  } else if (OB_FAIL(right_row_store_iter_.get_next_row(store_row))) {
    if (OB_ITER_END != ret) {
      SQL_ENG_LOG(WARN, "failed to get next row", K(ret));
    }
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::get_left_next_row(
  const ObChunkDatumStore::StoredRow *&store_row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_left_part_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "unexpected status: current partition is null", K(cur_left_part_));
  } else if (OB_FAIL(left_row_store_iter_.get_next_row(store_row))) {
    if (OB_ITER_END != ret) {
      SQL_ENG_LOG(WARN, "failed to get next row", K(ret));
    }
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::get_left_next_batch(
                          const common::ObIArray<ObExpr *> &exprs,
                          const int64_t max_row_cnt,
                          int64_t &read_rows,
                          uint64_t *hash_values_for_batch)
{
  int ret = OB_SUCCESS;
  //const ObChunkDatumStore::StoredRow **store_row = nullptr;
  ObChunkDatumStore::StoredRow *store_rows[max_row_cnt];
  if (OB_ISNULL(hash_values_for_batch)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "hash values vector is not init", K(ret));
  } else if (OB_ISNULL(cur_left_part_) || OB_ISNULL(eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "unexpected status: current partition is null", K(cur_left_part_));
  } else if (OB_FAIL(left_row_store_iter_.get_next_batch(exprs,
                                                         *eval_ctx_,
                                                         max_row_cnt,
                                                         read_rows,
                                                         const_cast<const ObChunkDatumStore::StoredRow **>(&store_rows[0])))) {
    if (OB_ITER_END != ret) {
      SQL_ENG_LOG(WARN, "failed to get next batch", K(ret));
    }
  }
  //we need to precalcucate the hash values for batch, if not iter_end
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; i < read_rows; ++i) {
      HashRowStore *sr = static_cast<HashRowStore *> (store_rows[i]);
      hash_values_for_batch[i] = (sr->get_hash_value() & HashRowStore::get_hash_mask());
    }
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::get_right_next_batch(
                          const common::ObIArray<ObExpr *> &exprs,
                           const int64_t max_row_cnt,
                           int64_t &read_rows)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_right_part_) || OB_ISNULL(eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "unexpected status: current partition is null", K(cur_right_part_));
  } else if (OB_FAIL(right_row_store_iter_.get_next_batch(exprs, *eval_ctx_, max_row_cnt, read_rows))) {
    if (OB_ITER_END != ret) {
      SQL_ENG_LOG(WARN, "failed to get next row", K(ret));
    }
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::get_next_hash_table_row(
  const ObChunkDatumStore::StoredRow *&store_row,
  const common::ObIArray<ObExpr*> *exprs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(hash_table_row_store_iter_.get_next_row(store_row))) {
    if (OB_ITER_END != ret) {
      SQL_ENG_LOG(WARN, "failed to get next row", K(ret));
    }
  } else if (nullptr != exprs
      && OB_FAIL(hash_table_row_store_iter_.convert_to_row(store_row, *exprs, *eval_ctx_))) {
    SQL_ENG_LOG(WARN, "failed to convert row to store row", K(ret));
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::get_next_hash_table_batch(
  const common::ObIArray<ObExpr *> &exprs,
  const int64_t max_row_cnt,
  int64_t &read_rows,
  const ObChunkDatumStore::StoredRow **store_row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "eval ctx is nullptr", K(ret));
  } else if (OB_FAIL(hash_table_row_store_iter_.get_next_batch(exprs,
                                                               *eval_ctx_,
                                                               max_row_cnt,
                                                               read_rows,
                                                               store_row))) {
    if (OB_ITER_END != ret) {
      SQL_ENG_LOG(WARN, "failed to get next batch", K(ret));
    }
    //save the error OB_ITER_END to help ObHashDistinctOp stop around
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::get_right_next_row(
  const ObChunkDatumStore::StoredRow *&store_row,
  const common::ObIArray<ObExpr*> &exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_right_part_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "unexpected status: current partition is null", K(cur_right_part_));
  } else if (OB_FAIL(right_row_store_iter_.get_next_row(store_row))) {
    if (OB_ITER_END != ret) {
      SQL_ENG_LOG(WARN, "failed to get next row", K(ret));
    }
  } else if (OB_FAIL(right_row_store_iter_.convert_to_row(store_row, exprs, *eval_ctx_))) {
    SQL_ENG_LOG(WARN, "failed to convert row to store row", K(ret));
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::get_left_next_row(
  const ObChunkDatumStore::StoredRow *&store_row,
  const common::ObIArray<ObExpr*> &exprs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_left_part_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "unexpected status: current partition is null", K(cur_left_part_));
  } else if (OB_FAIL(left_row_store_iter_.get_next_row(store_row))) {
    if (OB_ITER_END != ret) {
      SQL_ENG_LOG(WARN, "failed to get next row", K(ret));
    }
  } else if (OB_FAIL(left_row_store_iter_.convert_to_row(store_row, exprs, *eval_ctx_))) {
    SQL_ENG_LOG(WARN, "failed to convert row to store row", K(ret));
  }
  return ret;
}

template<typename HashCol, typename HashRowStore>
int ObHashPartInfrastructure<HashCol, HashRowStore>::process_dump(bool is_block, bool &full_by_pass)
{
  int ret = OB_SUCCESS;
  bool dumped = false;
  if (need_dump()) {
    if (OB_FAIL(sql_mem_processor_->extend_max_memory_size(
        alloc_,
        [&](int64_t max_memory_size)
        { UNUSED(max_memory_size); return need_dump(); },
        dumped, sql_mem_processor_->get_data_size()))) {
      SQL_ENG_LOG(WARN, "failed to extend max memory size", K(ret));
    } else if (dumped) {
      full_by_pass = true;
      if (!is_push_down_ || is_block) {
        if (enable_sql_dumped_) {
          has_cur_part_dumped_ = true;
          if (OB_FAIL(create_dumped_partitions(cur_side_))) {
            SQL_ENG_LOG(WARN, "failed to create dumped partitions", K(ret), K(est_part_cnt_));
          }
        } else {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SQL_ENG_LOG(WARN, "hash partitioning is out of memory", K(ret), K(get_mem_used()));
        }
      } else {
        // for unblock distinct, if by_pass, return row drictly
      }
    }
  }
  return ret;
}
//////////////////// end ObHashPartInfrastructure //////////////////

template <typename Item>
int ObHashPartitionExtendHashTable<Item>::init(
  common::ObIAllocator *allocator,
  const int64_t initial_size,
  ObSqlMemMgrProcessor *sql_mem_processor,
  const int64_t min_bucket,
  const int64_t max_bucket,
  bool is_push_down)
{
  int ret = common::OB_SUCCESS;
  sql_mem_processor_ = sql_mem_processor;
  min_bucket_num_ = min_bucket;
  max_bucket_num_ = max_bucket;
  is_push_down_ = is_push_down;
  if (initial_size < 2 || nullptr == allocator || OB_ISNULL(sql_mem_processor)) {
    ret = common::OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(ret), K(initial_size), K(allocator));
  } else {
    // because extend hash table when the element table reach one percent of hash table size(50% now)
    int64_t est_bucket_num = std::min(estimate_bucket_num(initial_size * EXTENDED_RATIO,
                                                    sql_mem_processor->get_mem_bound(),
                                                    min_bucket), static_cast<int64_t> (INT_MAX));
    allocator_ = OB_NEW(ModulePageAllocator, ObModIds::OB_SQL_HASH_SET, ObModIds::OB_SQL_HASH_SET);
    if (OB_ISNULL(allocator_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "failed to allocate memory", K(ret));
    } else if (FALSE_IT(allocator_->set_allocator(allocator))) {
    } else if (OB_FAIL(create_bucket_array(is_push_down
                                         ? INIT_BKT_NUM_PUSH_DOWM : est_bucket_num, buckets_))) {
      SQL_ENG_LOG(WARN, "failed to create bucket array", K(ret), K(est_bucket_num));
    } else {
      SQL_ENG_LOG(DEBUG, "debug init hash part table", K(ret),
        K(est_bucket_num), K(initial_size), K(sql_mem_processor->get_mem_bound()));
      size_ = 0;
    }
    if (OB_FAIL(ret)) {
      ob_delete(allocator_);
      allocator_ = nullptr;
    }
  }
  return ret;
}

template <typename Item>
int64_t ObHashPartitionExtendHashTable<Item>::estimate_bucket_num(
  const int64_t bucket_num,
  const int64_t max_hash_mem,
  const int64_t min_bucket)
{
  int64_t max_bound_size = std::max(0l, max_hash_mem * MAX_MEM_PERCENT / 100);
  int64_t est_bucket_num = common::next_pow2(bucket_num);
  int64_t est_size = est_bucket_num * sizeof(void*);
  while (est_size > max_bound_size && est_bucket_num > 0) {
    est_bucket_num >>= 1;
    est_size = est_bucket_num * sizeof(void*);
  }
  if (est_bucket_num < INITIAL_SIZE) {
    est_bucket_num = INITIAL_SIZE;
  }
  if (est_bucket_num < min_bucket) {
    est_bucket_num = min_bucket;
  }
  return est_bucket_num;
}

template <typename Item>
int ObHashPartitionExtendHashTable<Item>::create_bucket_array(
  const int64_t bucket_num,
  BucketArray *&new_buckets)
{
  int ret = OB_SUCCESS;
  void *buckets_buf = NULL;
  int64_t tmp_bucket_num = common::next_pow2(bucket_num);
  new_buckets = nullptr;
  if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "allocator is null", K(ret));
  } else if (OB_ISNULL(buckets_buf = allocator_->alloc(sizeof(BucketArray)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_ENG_LOG(WARN, "failed to allocate memory", K(ret));
  } else {
    new_buckets = new (buckets_buf) BucketArray(*allocator_);
    if (OB_FAIL(new_buckets->init(tmp_bucket_num))) {
      new_buckets->reset();
      allocator_->free(new_buckets);
      new_buckets = nullptr;
      SQL_ENG_LOG(DEBUG, "resize bucket array", K(ret), K(tmp_bucket_num));
    }
  }
  return ret;
}

template <typename Item>
int ObHashPartitionExtendHashTable<Item>::resize(
  common::ObIAllocator *allocator, int64_t bucket_num, ObSqlMemMgrProcessor *sql_mem_processor)
{
  int ret = OB_SUCCESS;
  int64_t est_max_bucket_num = 0;
  if (OB_ISNULL(sql_mem_processor_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "sql mem processor is null", K(ret));
  } else if (FALSE_IT(est_max_bucket_num = estimate_bucket_num(bucket_num * EXTENDED_RATIO,
      sql_mem_processor->get_mem_bound(), min_bucket_num_))) {
  } else if (est_max_bucket_num >= get_bucket_num()) {
    // 估算的bucket大于等于现在的，则重用
    reuse();
  } else {
    destroy();
    if (OB_FAIL(init(allocator, bucket_num, sql_mem_processor, min_bucket_num_, max_bucket_num_))) {
      SQL_ENG_LOG(WARN, "failed to reuse with bucket", K(bucket_num), K(ret));
    }
  }
  return ret;
}

template <typename Item>
int ObHashPartitionExtendHashTable<Item>::get(
  uint64_t hash_value, const Item &part_cols, const Item *&item) const
{
  int ret = OB_SUCCESS;
  item = NULL;
  if (NULL == buckets_) {
    // do nothing
  } else {
    common::hash::hash_func<Item> hf;
    bool equal_res = false;
    uint64_t bucket_hash_val = 0;
    Item *bucket = buckets_->at(hash_value & (get_bucket_num() - 1));
    ObEvalCtx::BatchInfoScopeGuard guard(*eval_ctx_);
    while (OB_SUCC(ret) && NULL != bucket) {
      if (OB_FAIL(hf(*bucket, bucket_hash_val))) {
        SQL_ENG_LOG(WARN, "fail to get bucket hash val", K(ret));
      } else if (hash_value == bucket_hash_val) {
        if (OB_FAIL(bucket->equal_distinct(exprs_, part_cols, sort_collations_,
                                           cmp_funcs_, eval_ctx_, equal_res, guard))) {
          SQL_ENG_LOG(WARN, "compare info is null", K(ret));
        } else if (equal_res) {
          item = bucket;
          break;
        }
      }
      bucket = bucket->next();
    }
  }
  return ret;
}

template <typename Item>
int ObHashPartitionExtendHashTable<Item>::set(Item &item)
{
  int ret = common::OB_SUCCESS;
  uint64_t hash_val = 0;
  common::hash::hash_func<Item> hf;
  if (!is_push_down_ && size_ >= get_bucket_num() * SIZE_BUCKET_PERCENT / 100) {
    int64_t extend_bucket_num =
      estimate_bucket_num(get_bucket_num() * 2, sql_mem_processor_->get_mem_bound(),
        min_bucket_num_);
    if (extend_bucket_num <= get_bucket_num()) {
    } else if (OB_FAIL(extend(get_bucket_num() * 2))) {
      SQL_ENG_LOG(WARN, "extend failed", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(buckets_)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(ret), K(buckets_));
  } else if (item.use_expr_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "unexpected status: store_row is null", K(ret));
  } else if (OB_FAIL(hf(item, hash_val))) {
    SQL_ENG_LOG(WARN, "hash failed", K(ret));
  } else {
    Item *&bucket = buckets_->at(hash_val & (get_bucket_num() - 1));
    item.next() = bucket;
    bucket = &item;
    size_ += 1;
  }
  return ret;
}

template <typename Item>
int ObHashPartitionExtendHashTable<Item>::set_distinct(Item &item, uint64_t hash_value)
{
  int ret = OB_SUCCESS;
  common::hash::hash_func<Item> hf;
  //if bucket != nullptr, check is duplicate and append distinct value
  bool need_insert = true;
  Item *&insert_bucket = buckets_->at(hash_value & (get_bucket_num() - 1));
  Item *bucket = insert_bucket;
  ObEvalCtx::BatchInfoScopeGuard guard(*eval_ctx_);
  uint64_t bucket_hash_val = 0;
  if (OB_NOT_NULL(bucket)) {
    while(OB_SUCC(ret) && OB_NOT_NULL(bucket)) {
      if (OB_FAIL(hf(*bucket, bucket_hash_val))) {
        SQL_ENG_LOG(WARN, "failed to do bucket hash", K(ret));
      } else {
        bool equal_res = (hash_value == bucket_hash_val);
        if (equal_res &&
            OB_FAIL(bucket->equal_distinct(exprs_, item, sort_collations_,
                                          cmp_funcs_, eval_ctx_, equal_res, guard))) {
          SQL_ENG_LOG(WARN, "failed to compare items", K(ret));
        } else if (equal_res) {
          need_insert = false;
          ret = OB_HASH_EXIST;
        } else {
          bucket = bucket->next();
        }
      }
    }
  }
  if (need_insert) {
    item.next() = insert_bucket;
    insert_bucket = &item;
    size_ += 1;
  }
  return ret;
}

template <typename Item>
int ObHashPartitionExtendHashTable<Item>::check_and_extend()
{
  int ret = OB_SUCCESS;
  if (size_ >= get_bucket_num() * 0.8) {
    int64_t extend_bucket_num =
      estimate_bucket_num(get_bucket_num() * 2, sql_mem_processor_->get_mem_bound(),
        min_bucket_num_);
    if (extend_bucket_num <= get_bucket_num()) {
    } else if (OB_FAIL(extend(get_bucket_num() * 2))) {
      SQL_ENG_LOG(WARN, "extend failed", K(ret));
    } else {
      SQL_ENG_LOG(DEBUG, "trace hash part extend", K(ret),
        K(size_), K(get_bucket_num()), K(sql_mem_processor_->get_mem_bound()));
    }
  }
  if (OB_ISNULL(buckets_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "null buckets", K(ret));
  }
  return ret;
}

template <typename Item>
int ObHashPartitionExtendHashTable<Item>::extend(const int64_t new_bucket_num)
{
  int ret = common::OB_SUCCESS;
  common::hash::hash_func<Item> hf;
  uint64_t hash_val = 0;
  BucketArray *new_buckets = NULL;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(buckets_)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(ret), K(buckets_));
  } else if (OB_FAIL(create_bucket_array(new_bucket_num, new_buckets))) {
    SQL_ENG_LOG(WARN, "failed to create bucket array", K(ret));
  } else {
    // rehash
    const int64_t tmp_new_bucket_num = new_buckets->count();
    const int64_t old_bucket_num = get_bucket_num();
    const int64_t BATCH_SIZE = 1024;
    for (int64_t i = 0; i < old_bucket_num && OB_SUCC(ret); i += BATCH_SIZE) {
      int64_t batch_size = min(old_bucket_num - i, BATCH_SIZE);
      for (int64_t j = 0; j < batch_size; ++j) {
        Item *bucket = buckets_->at(i + j);
        __builtin_prefetch(bucket, 0/* read */, 2 /*high temp locality*/);
      }
      for (int64_t j = 0; j < batch_size; ++j) {
        Item *bucket = buckets_->at(i + j);
        if (nullptr != bucket) {
          __builtin_prefetch(bucket->next(), 0/* read */, 2 /*high temp locality*/);
        }
      }
      for (int64_t j = 0; j < batch_size && OB_SUCC(ret); ++j) {
        Item *bucket = buckets_->at(i + j);
        if (nullptr != bucket) {
          do {
            Item *item = bucket;
            bucket = bucket->next();
            if (OB_FAIL(hf(*item, hash_val))) {
              SQL_ENG_LOG(WARN, "fail to get item hash val", K(ret));
            } else {
              Item *&new_bucket = new_buckets->at(hash_val & (tmp_new_bucket_num - 1));
              item->next() = new_bucket;
              new_bucket = item;
            }
          } while (nullptr != bucket && OB_SUCC(ret));
        }
      }
    }
    SQL_ENG_LOG(DEBUG, "trace hash part extend", K(ret),
      K(size_), K(tmp_new_bucket_num), K(get_bucket_num()));
    buckets_->destroy();
    allocator_->free(buckets_);
    buckets_ = new_buckets;
  }
  return ret;
}
///////////////////////////////////////////////////////////////////////////////////


}  // namespace sql
}  // namespace oceanbase

#endif /* OB_HASH_PARTITIONING_INFRASTRUCTURE_OP_H_ */
