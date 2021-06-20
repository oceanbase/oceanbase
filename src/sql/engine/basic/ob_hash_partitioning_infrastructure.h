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

#ifndef OB_HASH_PARTITIONING_INFRASTRUCTURE_H_
#define OB_HASH_PARTITIONING_INFRASTRUCTURE_H_

#include "lib/list/ob_dlist.h"
#include "sql/engine/basic/ob_chunk_row_store.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"
#include "sql/engine/basic/ob_hash_partitioning_basic.h"

namespace oceanbase {
namespace sql {

struct ObPartStoredRow : public sql::ObChunkRowStore::StoredRow {
  const static int64_t HASH_VAL_BIT = 63;
  const static int64_t HASH_VAL_MASK = UINT64_MAX >> (64 - HASH_VAL_BIT);
  struct ExtraInfo {
    uint64_t hash_val_ : HASH_VAL_BIT;
    uint64_t is_match_ : 1;
  };
  ExtraInfo& get_extra_info()
  {
    static_assert(sizeof(ObPartStoredRow) == sizeof(sql::ObChunkRowStore::StoredRow),
        "sizeof StoredJoinRow must be the save with StoredRow");
    return *reinterpret_cast<ExtraInfo*>(get_extra_payload());
  }
  const ExtraInfo& get_extra_info() const
  {
    return *reinterpret_cast<const ExtraInfo*>(get_extra_payload());
  }

  uint64_t get_hash_value() const
  {
    return get_extra_info().hash_val_;
  }
  void set_hash_value(const uint64_t hash_val)
  {
    get_extra_info().hash_val_ = hash_val & HASH_VAL_MASK;
  }
  bool is_match() const
  {
    return get_extra_info().is_match_;
  }
  void set_is_match(bool is_match)
  {
    get_extra_info().is_match_ = is_match;
  }
  static uint64_t get_hash_mask()
  {
    return HASH_VAL_MASK;
  }
};

struct TempHashPartCols;
struct HashPartCols {
  HashPartCols() : store_row_(nullptr), next_(nullptr)
  {}

  uint64_t hash() const
  {
    return nullptr == store_row_ ? DEFAULT_PART_HASH_VALUE : store_row_->get_hash_value();
  }

  bool equal(const HashPartCols& other, const common::ObIArray<common::ObColumnInfo>* hash_col_idxs) const;
  bool equal_temp(const TempHashPartCols& other, const common::ObIArray<common::ObColumnInfo>* hash_col_idxs) const;

  HashPartCols*& next()
  {
    return *reinterpret_cast<HashPartCols**>(&next_);
  }

  int set_hash_value(uint64_t hash_value)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(store_row_)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      store_row_->set_hash_value(hash_value);
    }
    return ret;
  }

  ObPartStoredRow* store_row_;
  void* next_;
  TO_STRING_KV(K_(store_row));
};

struct TempHashPartCols : public HashPartCols {
  TempHashPartCols() : HashPartCols(), row_(nullptr)
  {}
  TempHashPartCols(const common::ObNewRow* row) : HashPartCols(), row_(row)
  {}
  const common::ObNewRow* row_;
};

template <typename Item>
class ObPartitionExtendHashTable {
public:
  const static int64_t INITIAL_SIZE = 128;
  const static int64_t SIZE_BUCKET_PERCENT = 80;
  const static int64_t MAX_MEM_PERCENT = 40;
  ObPartitionExtendHashTable()
      : size_(0),
        bucket_num_(0),
        buckets_(nullptr),
        allocator_(nullptr),
        ref_allocator_(nullptr),
        part_col_idxs_(nullptr),
        sql_mem_processor_(nullptr)
  {}
  ~ObPartitionExtendHashTable()
  {
    destroy();
  }

  int init(common::ObIAllocator* alloctor, const int64_t initial_size, ObSqlMemMgrProcessor* sql_mem_processor_);
  // return the first item which equal to, NULL for none exist.
  int get(const Item& item, const Item*& res) const;
  int get(uint64_t hash_value, const TempHashPartCols& part_cols, const Item*& res) const;
  // Link item to hash table, extend buckets if needed.
  // (Do not check item is exist or not)
  int set(Item& item);
  int64_t size() const
  {
    return size_;
  }

  void reuse()
  {
    if (OB_NOT_NULL(buckets_)) {
      buckets_->set_all(nullptr);
    }
    size_ = 0;
  }

  int resize(common::ObIAllocator* allocator, int64_t bucket_num, ObSqlMemMgrProcessor* sql_mem_processor);

  void destroy()
  {
    if (OB_NOT_NULL(buckets_)) {
      buckets_->destroy();
      if (nullptr != allocator_) {
        allocator_->free(buckets_);
      } else {
        SQL_ENG_LOG(ERROR, "buckets is not null", KP(buckets_));
      }
      buckets_ = nullptr;
    }
    if (OB_NOT_NULL(allocator_)) {
      ob_delete(allocator_);
      allocator_ = nullptr;
    }
    ref_allocator_ = nullptr;
    size_ = 0;
  }
  int64_t mem_used() const
  {
    return nullptr == buckets_ ? 0 : buckets_->mem_used();
  }

  template <typename CB>
  int foreach (CB& cb) const
  {
    int ret = common::OB_SUCCESS;
    if (OB_ISNULL(buckets_)) {
      ret = OB_INVALID_ARGUMENT;
      SQL_ENG_LOG(WARN, "invalid null buckets", K(ret), K(buckets_));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < get_bucket_num(); i++) {
      Item* item = buckets_->at(i);
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

  void set_part_idxs(common::ObIArray<common::ObColumnInfo>* part_col_idxs)
  {
    part_col_idxs_ = part_col_idxs;
  }
  void set_sql_mem_processor(ObSqlMemMgrProcessor* sql_mem_processor)
  {
    sql_mem_processor_ = sql_mem_processor;
  }

private:
  DISALLOW_COPY_AND_ASSIGN(ObPartitionExtendHashTable);
  using BucketArray = common::ObSegmentArray<Item*, OB_MALLOC_BIG_BLOCK_SIZE, common::ModulePageAllocator>;
  int extend(const int64_t new_bucket_num);
  static int64_t estimate_bucket_num(const int64_t bucket_num, const int64_t max_hash_mem);
  int create_bucket_array(const int64_t bucket_num, BucketArray*& new_buckets);

private:
  int64_t size_;
  int64_t bucket_num_;
  BucketArray* buckets_;
  common::ModulePageAllocator* allocator_;
  common::ObIAllocator* ref_allocator_;
  common::ObIArray<common::ObColumnInfo>* part_col_idxs_;
  ObSqlMemMgrProcessor* sql_mem_processor_;
};

template <typename HashCol, typename HashRowStore>
class ObBasicHashPartInfrastructure {
public:
  ObBasicHashPartInfrastructure()
      : tenant_id_(UINT64_MAX),
        mem_context_(nullptr),
        alloc_(nullptr),
        arena_alloc_(nullptr),
        hash_table_(),
        preprocess_part_(),
        left_part_list_(),
        right_part_list_(),
        left_part_map_(),
        right_part_map_(),
        part_col_idxs_(),
        sql_mem_processor_(nullptr),
        cur_left_part_(nullptr),
        cur_right_part_(nullptr),
        left_dumped_parts_(nullptr),
        right_dumped_parts_(nullptr),
        cur_dumped_parts_(nullptr),
        left_row_store_iter_(),
        right_row_store_iter_(),
        enable_sql_dumped_(false),
        unique_(false),
        need_pre_part_(false),
        ways_(InputWays::TWO),
        init_part_func_(nullptr),
        insert_row_func_(nullptr),
        cur_part_start_id_(0),
        start_round_(false),
        cur_side_(InputSide::LEFT),
        has_cur_part_dumped_(false),
        has_create_part_map_(false),
        est_part_cnt_(INT64_MAX),
        cur_level_(0),
        part_shift_(0),
        period_row_cnt_(0),
        left_part_cur_id_(0),
        right_part_cur_id_(0)
  {}
  ~ObBasicHashPartInfrastructure();

public:
  enum InputWays { ONE = 1, TWO = 2 };
  enum ProcessMode {
    Cache = 0,
    PreProcess = 1,
  };

public:
  struct ObIntraPartKey {
    ObIntraPartKey() : nth_way_(0), level_(0), nth_part_(0)
    {}

    uint64_t hash() const
    {
      return common::murmurhash(&part_key_, sizeof(part_key_), 0);
    }

    bool operator==(const ObIntraPartKey& other) const
    {
      return nth_way_ == other.nth_way_ && level_ == other.level_ && nth_part_ == other.nth_part_;
    }

    bool is_left()
    {
      return InputSide::LEFT == nth_way_;
    }
    void set_left()
    {
      nth_way_ = InputSide::LEFT;
    }
    void set_right()
    {
      nth_way_ = InputSide::RIGHT;
    }

    TO_STRING_KV(K_(nth_way), K_(level), K_(nth_part));
    union {
      int64_t part_key_;
      struct {
        int32_t nth_way_ : 16;  // 0: left, 1: right
        int32_t level_ : 16;
        int64_t nth_part_ : 32;
      };
    };
  };
  class ObIntraPartition : public common::ObDLinkBase<ObIntraPartition> {
  public:
    ObIntraPartition() : part_key_(), store_()
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
    ObChunkRowStore store_;
  };

private:
  bool is_left() const
  {
    return InputSide::LEFT == cur_side_;
  }
  bool is_right() const
  {
    return InputSide::RIGHT == cur_side_;
  }
  inline int init_mem_context(uint64_t tenant_id);

  typedef int (ObBasicHashPartInfrastructure::*InitPartitionFunc)(
      ObIntraPartition* part, int64_t nth_part, int64_t limit);
  typedef int (ObBasicHashPartInfrastructure::*InsertRowFunc)(
      const common::ObNewRow* row, bool& exists, bool& inserted);

  void set_init_part_func();
  int direct_insert_row(const common::ObNewRow* row, bool& exists, bool& inserted);
  int insert_row_with_hash_table(const common::ObNewRow* row, bool& exists, bool& inserted);
  int insert_row_with_unique_hash_table(const common::ObNewRow* row, bool& exists, bool& inserted);

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

  int64_t get_mem_used()
  {
    return (nullptr == mem_context_) ? 0 : mem_context_->used();
  }

  OB_INLINE int64_t get_bucket_idx(const uint64_t hash_value)
  {
    return hash_value & (hash_table_.get_bucket_num() - 1);
  }
  // high 32 bits used as partition hash value
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
  int init(uint64_t tenant_id, bool enable_sql_dumped, bool unique, bool need_pre_part, int64_t ways,
      ObSqlMemMgrProcessor* sql_mem_processor);

  void reset();
  void switch_left()
  {
    cur_side_ = InputSide::LEFT;
  }
  void switch_right()
  {
    cur_side_ = InputSide::RIGHT;
  }

  int exists_row(const common::ObNewRow* row, const HashCol*& exists_part_cols);

  int resize(int64_t bucket_cnt);
  int init_hash_table(int64_t bucket_cnt);
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

  int init_part_col_idxs(const common::ObIArray<common::ObColumnInfo>& col_idx);
  int add_part_col_idx(const ObColumnInfo& col_idx);

  int insert_row_on_hash_table(const common::ObNewRow* row, bool& exists, bool& inserted);
  int insert_row(const common::ObNewRow* row, bool& exists, bool& inserted);
  int insert_row_on_partitions(const common::ObNewRow* row);
  int finish_insert_row();

  int start_round();
  int end_round();

  int open_cur_part(InputSide input_side);
  int close_cur_part(InputSide input_side);

  int64_t est_bucket_count(const int64_t rows, const int64_t width, const int64_t min_bucket_cnt = MIN_BUCKET_NUM,
      const int64_t max_bucket_cnt = MAX_BUCKET_NUM);

  int init_set_part(ObIntraPartition* part, int64_t nth_part, int64_t limit);
  int init_default_part(ObIntraPartition* part, int64_t nth_part, int64_t limit);

  int create_dumped_partitions(InputSide input_side);

  int get_next_pair_partition(InputSide input_side);
  int get_next_partition(InputSide input_side);
  int get_right_next_row(const ObChunkRowStore::StoredRow*& store_row);
  int get_left_next_row(const ObChunkRowStore::StoredRow*& store_row);
  int get_right_next_row(const ObChunkRowStore::StoredRow*& store_row, const common::ObNewRow*& row);
  int get_left_next_row(const ObChunkRowStore::StoredRow*& store_row, const common::ObNewRow*& row);

  // int clean_partition();

  OB_INLINE bool has_left_dumped()
  {
    return OB_NOT_NULL(left_dumped_parts_);
  }
  OB_INLINE bool has_right_dumped()
  {
    return OB_NOT_NULL(right_dumped_parts_);
  }
  OB_INLINE bool has_dumped_partitions()
  {
    return !(left_part_list_.is_empty() && right_part_list_.is_empty());
  }

  int calc_hash_value(const common::ObNewRow* row, uint64_t& hash_value);

private:
  static const int64_t BLOCK_SIZE = 64 * 1024;
  static const int64_t MIN_BUCKET_NUM = 128;
  static const int64_t MAX_BUCKET_NUM = 131072;  // 1M = 131072 * 8
  static const int64_t MAX_PART_LEVEL = 4;
  uint64_t tenant_id_;
  lib::MemoryContext* mem_context_;
  common::ObIAllocator* alloc_;
  common::ObArenaAllocator* arena_alloc_;
  ObPartitionExtendHashTable<HashCol> hash_table_;
  ObIntraPartition preprocess_part_;
  common::ObDList<ObIntraPartition> left_part_list_;
  common::ObDList<ObIntraPartition> right_part_list_;
  common::hash::ObHashMap<ObIntraPartKey, ObIntraPartition*, common::hash::NoPthreadDefendMode> left_part_map_;
  common::hash::ObHashMap<ObIntraPartKey, ObIntraPartition*, common::hash::NoPthreadDefendMode> right_part_map_;
  common::ObSEArray<common::ObColumnInfo, 8> part_col_idxs_;
  ObSqlMemMgrProcessor* sql_mem_processor_;
  ObIntraPartition* cur_left_part_;
  ObIntraPartition* cur_right_part_;
  ObIntraPartition** left_dumped_parts_;
  ObIntraPartition** right_dumped_parts_;
  ObIntraPartition** cur_dumped_parts_;
  ObChunkRowStore::Iterator left_row_store_iter_;
  ObChunkRowStore::Iterator right_row_store_iter_;
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
  int64_t part_shift_;
  int64_t period_row_cnt_;
  int64_t left_part_cur_id_;
  int64_t right_part_cur_id_;
};

//////////////////// start ObBasicHashPartInfrastructure //////////////////
template <typename HashCol, typename HashRowStore>
ObBasicHashPartInfrastructure<HashCol, HashRowStore>::~ObBasicHashPartInfrastructure()
{
  destroy();
}

template <typename HashCol, typename HashRowStore>
inline int ObBasicHashPartInfrastructure<HashCol, HashRowStore>::init_mem_context(uint64_t tenant_id)
{
  int ret = common::OB_SUCCESS;
  if (OB_LIKELY(NULL == mem_context_)) {
    void* buf = nullptr;
    lib::ContextParam param;
    param.set_properties(lib::USE_TL_PAGE_OPTIONAL)
        .set_mem_attr(tenant_id, common::ObModIds::OB_ARENA_HASH_JOIN, common::ObCtxIds::WORK_AREA);
    if (OB_FAIL(CURRENT_CONTEXT.CREATE_CONTEXT(mem_context_, param))) {
      SQL_ENG_LOG(WARN, "create entity failed", K(ret));
    } else if (OB_ISNULL(mem_context_)) {
      SQL_ENG_LOG(WARN, "mem entity is null", K(ret));
    } else if (OB_ISNULL(buf = mem_context_->allocp(sizeof(ObArenaAllocator)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "failed to allocate memory", K(ret));
    } else {
      arena_alloc_ = new (buf) ObArenaAllocator(mem_context_->get_malloc_allocator());
      arena_alloc_->set_label("BHashPartInfra");
      alloc_ = &mem_context_->get_malloc_allocator();
    }
  }
  return ret;
}

template <typename HashCol, typename HashRowStore>
int ObBasicHashPartInfrastructure<HashCol, HashRowStore>::init(uint64_t tenant_id, bool enable_sql_dumped, bool unique,
    bool need_pre_part, int64_t ways, ObSqlMemMgrProcessor* sql_mem_processor)
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
    init_part_func_ = &ObBasicHashPartInfrastructure<HashCol, HashRowStore>::init_default_part;
    insert_row_func_ = &ObBasicHashPartInfrastructure<HashCol, HashRowStore>::direct_insert_row;
    part_shift_ = sizeof(uint64_t) * CHAR_BIT / 2;
    set_insert_row_func();
    set_init_part_func();
  }
  return ret;
}

template <typename HashCol, typename HashRowStore>
void ObBasicHashPartInfrastructure<HashCol, HashRowStore>::clean_cur_dumping_partitions()
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

template <typename HashCol, typename HashRowStore>
void ObBasicHashPartInfrastructure<HashCol, HashRowStore>::clean_dumped_partitions()
{
  int ret = OB_SUCCESS;
  DLIST_FOREACH_REMOVESAFE_X(node, left_part_list_, OB_SUCC(ret))
  {
    ObIntraPartition* part = node;
    ObIntraPartition* tmp_part = left_part_list_.remove(part);
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
  DLIST_FOREACH_REMOVESAFE_X(node, right_part_list_, OB_SUCC(ret))
  {
    ObIntraPartition* part = node;
    ObIntraPartition* tmp_part = right_part_list_.remove(part);
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

template <typename HashCol, typename HashRowStore>
void ObBasicHashPartInfrastructure<HashCol, HashRowStore>::destroy_cur_parts()
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

template <typename HashCol, typename HashRowStore>
void ObBasicHashPartInfrastructure<HashCol, HashRowStore>::destroy()
{
  reset();
  arena_alloc_ = nullptr;
  hash_table_.destroy();
  left_part_map_.destroy();
  right_part_map_.destroy();
  if (OB_NOT_NULL(mem_context_)) {
    DESTROY_CONTEXT(mem_context_);
    mem_context_ = NULL;
  }
}

template <typename HashCol, typename HashRowStore>
void ObBasicHashPartInfrastructure<HashCol, HashRowStore>::reset()
{
  left_row_store_iter_.reset();
  right_row_store_iter_.reset();
  destroy_cur_parts();
  clean_cur_dumping_partitions();
  clean_dumped_partitions();
  hash_table_.destroy();
  preprocess_part_.store_.reset();
  left_part_map_.clear();
  right_part_map_.clear();
  part_col_idxs_.reset();
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
  if (OB_NOT_NULL(arena_alloc_)) {
    arena_alloc_->reset();
  }
}

template <typename HashCol, typename HashRowStore>
void ObBasicHashPartInfrastructure<HashCol, HashRowStore>::set_init_part_func()
{
  if (OB_NOT_NULL(sql_mem_processor_)) {
    const ObSqlWorkAreaProfile& profile = sql_mem_processor_->get_profile();
    ObPhyOperatorType type = profile.get_operator_type();
    switch (type) {
      case PHY_HASH_UNION:
      case PHY_HASH_INTERSECT:
      case PHY_HASH_EXCEPT:
        init_part_func_ = &ObBasicHashPartInfrastructure<HashCol, HashRowStore>::init_set_part;
        break;
      default:
        break;
    }
  }
}

template <typename HashCol, typename HashRowStore>
void ObBasicHashPartInfrastructure<HashCol, HashRowStore>::set_insert_row_func()
{
  if (unique_) {
    insert_row_func_ = &ObBasicHashPartInfrastructure<HashCol, HashRowStore>::insert_row_with_unique_hash_table;
  } else {
    insert_row_func_ = &ObBasicHashPartInfrastructure<HashCol, HashRowStore>::insert_row_with_hash_table;
  }
}

template <typename HashCol, typename HashRowStore>
int ObBasicHashPartInfrastructure<HashCol, HashRowStore>::init_set_part(
    ObIntraPartition* part, int64_t nth_part, int64_t limit)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(part)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "unexpected status: part is null", K(ret));
  } else {
    part->part_key_.nth_way_ = InputSide::LEFT == cur_side_ ? 0 : 1;
    part->part_key_.level_ = cur_level_ + 1;
    part->part_key_.nth_part_ = nth_part;
    if (OB_FAIL(part->store_.init(limit,
            tenant_id_,
            ObCtxIds::WORK_AREA,
            ObModIds::OB_SQL_HASH_SET,
            true /* enable dump */,
            ObChunkRowStore::WITHOUT_PROJECTOR,
            sizeof(uint64_t)))) {
      SQL_ENG_LOG(WARN, "failed to init row store", K(ret));
    } else if (OB_ISNULL(sql_mem_processor_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "sql_mem_processor_ is null", K(ret));
    } else {
      part->store_.set_dir_id(sql_mem_processor_->get_dir_id());
      part->store_.set_allocator(*alloc_);
      part->store_.set_callback(sql_mem_processor_);
    }
  }
  return ret;
}

template <typename HashCol, typename HashRowStore>
int ObBasicHashPartInfrastructure<HashCol, HashRowStore>::init_default_part(
    ObIntraPartition* part, int64_t nth_part, int64_t limit)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(part)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "unexpected status: part is null", K(ret));
  } else {
    part->part_key_.nth_way_ = InputSide::LEFT == cur_side_ ? 0 : 1;
    part->part_key_.level_ = cur_level_ + 1;
    part->part_key_.nth_part_ = nth_part;
    if (OB_FAIL(part->store_.init(limit,
            tenant_id_,
            ObCtxIds::WORK_AREA,
            ObModIds::OB_SQL_EXECUTOR,
            true /* enable dump */,
            ObChunkRowStore::WITHOUT_PROJECTOR,
            sizeof(uint64_t)))) {
      SQL_ENG_LOG(WARN, "failed to init row store", K(ret));
    } else if (OB_ISNULL(sql_mem_processor_)) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "sql_mem_processor_ is null", K(ret));
    } else {
      part->store_.set_dir_id(sql_mem_processor_->get_dir_id());
      part->store_.set_allocator(*alloc_);
      part->store_.set_callback(sql_mem_processor_);
    }
  }
  return ret;
}

template <typename HashCol, typename HashRowStore>
int ObBasicHashPartInfrastructure<HashCol, HashRowStore>::init_hash_table(int64_t bucket_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(alloc_) || !start_round_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "allocator is null or it don'e start to round", K(ret), K(start_round_));
  } else if (OB_FAIL(hash_table_.init(alloc_, bucket_cnt, sql_mem_processor_))) {
    SQL_ENG_LOG(WARN, "failed to init hash table", K(ret), K(bucket_cnt));
  } else {
    hash_table_.set_part_idxs(&part_col_idxs_);
  }
  return ret;
}

template <typename HashCol, typename HashRowStore>
int ObBasicHashPartInfrastructure<HashCol, HashRowStore>::resize(int64_t bucket_cnt)
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

template <typename HashCol, typename HashRowStore>
int ObBasicHashPartInfrastructure<HashCol, HashRowStore>::init_part_col_idxs(const ObIArray<ObColumnInfo>& col_idxs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < col_idxs.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(part_col_idxs_.push_back(col_idxs.at(i)))) {
      SQL_ENG_LOG(WARN, "failed to push back part column", K(ret), K(i));
    }
  }
  return ret;
}

template <typename HashCol, typename HashRowStore>
int ObBasicHashPartInfrastructure<HashCol, HashRowStore>::add_part_col_idx(const ObColumnInfo& col_idx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(part_col_idxs_.push_back(col_idx))) {
    SQL_ENG_LOG(WARN, "failed to push back part column", K(ret), K(col_idx));
  }
  return ret;
}

template <typename HashCol, typename HashRowStore>
int ObBasicHashPartInfrastructure<HashCol, HashRowStore>::start_round()
{
  int ret = OB_SUCCESS;
  if (start_round_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "current rount is not finish", K(ret));
  } else {
    if (need_pre_part_) {
      if (OB_FAIL((this->*init_part_func_)(&preprocess_part_, 0, INT64_MAX))) {
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

template <typename HashCol, typename HashRowStore>
int ObBasicHashPartInfrastructure<HashCol, HashRowStore>::append_dumped_parts(InputSide input_side)
{
  int ret = OB_SUCCESS;
  ObIntraPartition** dumped_parts = nullptr;
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
            SQL_ENG_LOG(WARN, "failed to push into hash table", K(ret), K(i), K(dumped_parts[i]->part_key_));
          } else {
            left_part_list_.add_last(dumped_parts[i]);
            dumped_parts[i] = nullptr;
          }
        } else {
          if (OB_FAIL(right_part_map_.set_refactored(dumped_parts[i]->part_key_, dumped_parts[i]))) {
            SQL_ENG_LOG(WARN, "failed to push into hash table", K(ret), K(i), K(dumped_parts[i]->part_key_));
          } else {
            right_part_list_.add_last(dumped_parts[i]);
            dumped_parts[i] = nullptr;
          }
        }
      } else {
        if (0 != dumped_parts[i]->store_.get_row_cnt_in_memory() ||
            0 != dumped_parts[i]->store_.get_row_cnt_on_disk()) {
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

template <typename HashCol, typename HashRowStore>
int ObBasicHashPartInfrastructure<HashCol, HashRowStore>::append_all_dump_parts()
{
  int ret = OB_SUCCESS;
  if (nullptr != left_dumped_parts_ && nullptr != right_dumped_parts_) {
    if (left_part_cur_id_ != right_part_cur_id_) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN,
          "unexpected status: part id is not match",
          K(ret),
          K(cur_part_start_id_),
          K(left_part_cur_id_),
          K(right_part_cur_id_));
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

template <typename HashCol, typename HashRowStore>
int ObBasicHashPartInfrastructure<HashCol, HashRowStore>::end_round()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(cur_left_part_) || OB_NOT_NULL(cur_right_part_)) {
    SQL_ENG_LOG(WARN, "cur left or right part is not null", K(ret), K(cur_left_part_), K(cur_right_part_));
  } else if (OB_FAIL(append_all_dump_parts())) {
    SQL_ENG_LOG(WARN, "failed to append all dumped parts", K(ret));
  } else {
    left_row_store_iter_.reset();
    right_row_store_iter_.reset();
    preprocess_part_.store_.reset();
    start_round_ = false;
    cur_left_part_ = nullptr;
    cur_right_part_ = nullptr;
    cur_dumped_parts_ = nullptr;
    period_row_cnt_ = 0;
    if (OB_NOT_NULL(arena_alloc_)) {
      arena_alloc_->reset();
    }
  }
  return ret;
}

// not implemented yet
// for material
template <typename HashCol, typename HashRowStore>
int ObBasicHashPartInfrastructure<HashCol, HashRowStore>::direct_insert_row(
    const common::ObNewRow* row, bool& exists, bool& inserted)
{
  UNUSED(row);
  UNUSED(exists);
  UNUSED(inserted);
  return OB_NOT_SUPPORTED;
}

// not implemented yet
// for hash join
template <typename HashCol, typename HashRowStore>
int ObBasicHashPartInfrastructure<HashCol, HashRowStore>::insert_row_with_hash_table(
    const common::ObNewRow* row, bool& exists, bool& inserted)
{
  UNUSED(row);
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
template <typename HashCol, typename HashRowStore>
void ObBasicHashPartInfrastructure<HashCol, HashRowStore>::est_partition_count()
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

template <typename HashCol, typename HashRowStore>
int64_t ObBasicHashPartInfrastructure<HashCol, HashRowStore>::est_bucket_count(
    const int64_t rows, const int64_t width, const int64_t min_bucket_cnt, const int64_t max_bucket_cnt)
{
  if (INT64_MAX == est_part_cnt_) {
    est_partition_count();
  }
  int64_t est_bucket_mem_size = next_pow2(rows) * sizeof(void*);
  int64_t est_data_mem_size = rows * width;
  int64_t max_remain_mem_size = sql_mem_processor_->get_mem_bound() - est_part_cnt_ * BLOCK_SIZE;
  int64_t est_bucket_num = rows;
  while (est_bucket_mem_size + est_data_mem_size > max_remain_mem_size) {
    est_bucket_num >>= 1;
    est_bucket_mem_size = next_pow2(est_bucket_num) * sizeof(void*);
    est_data_mem_size = est_bucket_num * width;
  }
  est_bucket_num = est_bucket_num < min_bucket_cnt
                       ? min_bucket_cnt
                       : (est_bucket_num > max_bucket_cnt ? max_bucket_cnt : est_bucket_num);
  sql_mem_processor_->get_profile().set_basic_info(rows, width * rows, est_bucket_num);
  return est_bucket_num;
}

template <typename HashCol, typename HashRowStore>
int ObBasicHashPartInfrastructure<HashCol, HashRowStore>::insert_row_on_partitions(const common::ObNewRow* row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_dumped_parts_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "unexpected status: cur dumped partitions is null", K(ret));
  } else {
    uint64_t hash_value = 0;
    if (OB_FAIL(calc_hash_value(row, hash_value))) {
      SQL_ENG_LOG(WARN, "failed to calc hash value", K(ret));
    } else {
      ObChunkRowStore::StoredRow* sr = nullptr;
      int64_t part_idx = get_part_idx(hash_value);
      if (OB_FAIL(cur_dumped_parts_[part_idx]->store_.add_row(*row, &sr))) {
        SQL_ENG_LOG(WARN, "failed to add row", K(ret));
      } else {
        HashRowStore* store_row = static_cast<HashRowStore*>(sr);
        store_row->set_hash_value(hash_value);
        store_row->set_is_match(false);
      }
    }
  }
  return ret;
}

template <typename HashCol, typename HashRowStore>
int ObBasicHashPartInfrastructure<HashCol, HashRowStore>::create_dumped_partitions(InputSide input_side)
{
  int ret = OB_SUCCESS;
  if (INT64_MAX == est_part_cnt_) {
    est_partition_count();
  }
  if (MAX_PART_LEVEL <= cur_level_ + 1) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "exceed max dumped level", K(ret), K(cur_level_));
  } else if (!has_create_part_map_) {
    has_create_part_map_ = true;
    if (OB_FAIL(left_part_map_.create(512, ObModIds::OB_SQL_EXECUTOR, ObModIds::OB_SQL_EXECUTOR, tenant_id_))) {
      SQL_ENG_LOG(WARN, "failed to create hash map", K(ret));
    } else if (OB_FAIL(right_part_map_.create(512, ObModIds::OB_SQL_EXECUTOR, ObModIds::OB_SQL_EXECUTOR, tenant_id_))) {
      SQL_ENG_LOG(WARN, "failed to create hash map", K(ret));
    }
  }
  has_cur_part_dumped_ = true;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(cur_dumped_parts_ =
                           static_cast<ObIntraPartition**>(alloc_->alloc(sizeof(ObIntraPartition*) * est_part_cnt_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_ENG_LOG(WARN, "failed to allocate memory", K(ret));
  } else {
    sql_mem_processor_->set_number_pass(cur_level_ + 1);
    MEMSET(cur_dumped_parts_, 0, sizeof(ObIntraPartition*) * est_part_cnt_);
    for (int64_t i = 0; i < est_part_cnt_ && OB_SUCC(ret); ++i) {
      void* mem = alloc_->alloc(sizeof(ObIntraPartition));
      if (OB_ISNULL(mem)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_ENG_LOG(WARN, "failed to allocate memory", K(ret));
      } else {
        cur_dumped_parts_[i] = new (mem) ObIntraPartition();
        ObIntraPartition* part = cur_dumped_parts_[i];
        if (OB_FAIL((this->*init_part_func_)(part, cur_part_start_id_ + i, 1))) {
          SQL_ENG_LOG(WARN, "failed to create part", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
      for (int64_t i = 0; i < est_part_cnt_; ++i) {
        ObIntraPartition* part = cur_dumped_parts_[i];
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
      }
    } else {
      if (OB_NOT_NULL(right_dumped_parts_)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_ENG_LOG(WARN, "unexpected status: right is dumped", K(ret));
      } else {
        right_dumped_parts_ = cur_dumped_parts_;
        right_part_cur_id_ = cur_part_start_id_ + est_part_cnt_;
      }
    }
  }
  return ret;
}

template <typename HashCol, typename HashRowStore>
int ObBasicHashPartInfrastructure<HashCol, HashRowStore>::exists_row(
    const common::ObNewRow* row, const HashCol*& exists_part_cols)
{
  int ret = OB_SUCCESS;
  TempHashPartCols part_cols(row);
  uint64_t hash_value = 0;
  exists_part_cols = nullptr;
  if (OB_FAIL(calc_hash_value(row, hash_value))) {
    SQL_ENG_LOG(WARN, "failed to calc hash value", K(ret));
  } else if (OB_FAIL(hash_table_.get(hash_value, part_cols, exists_part_cols))) {
    SQL_ENG_LOG(WARN, "failed to get item", K(ret));
  }
  return ret;
}

template <typename HashCol, typename HashRowStore>
int ObBasicHashPartInfrastructure<HashCol, HashRowStore>::calc_hash_value(
    const common::ObNewRow* row, uint64_t& hash_value)
{
  int ret = OB_SUCCESS;
  hash_value = 99194853094755497L;
  if (0 != part_col_idxs_.count()) {
    const ObObj* cells = row->cells_;
    const int32_t* projector = row->projector_;
    for (int64_t i = 0; i < part_col_idxs_.count() && OB_SUCC(ret); ++i) {
      int64_t real_index =
          row->projector_size_ > 0 ? projector[part_col_idxs_.at(i).index_] : part_col_idxs_.at(i).index_;
      const ObObj& cell = cells[real_index];
      if (cell.is_null()) {
        hash_value = cell.hash(hash_value);
      } else if (cell.get_type_class() == ObStringTC && cell.get_collation_type() != part_col_idxs_.at(i).cs_type_) {
        ret = OB_NOT_SUPPORTED;
        SQL_ENG_LOG(WARN,
            "failed to calc hash value",
            K(ret),
            K(cell),
            K(i),
            K(cell.get_collation_type()),
            K(part_col_idxs_.at(i).cs_type_));
      } else {
        hash_value = cell.is_string_type() ? cell.varchar_hash(part_col_idxs_.at(i).cs_type_, hash_value)
                                           : cell.hash(hash_value);
      }
    }
  }
  hash_value &= HashRowStore::get_hash_mask();
  return ret;
}

template <typename HashCol, typename HashRowStore>
int ObBasicHashPartInfrastructure<HashCol, HashRowStore>::update_mem_status_periodically()
{
  int ret = OB_SUCCESS;
  bool updated = false;
  if (OB_FAIL(sql_mem_processor_->update_max_available_mem_size_periodically(
          alloc_, [&](int64_t cur_cnt) { return period_row_cnt_ > cur_cnt; }, updated))) {
    SQL_ENG_LOG(WARN, "failed to update usable memory size periodically", K(ret));
  } else if (updated) {
    if (OB_FAIL(sql_mem_processor_->update_used_mem_size(get_mem_used()))) {
      SQL_ENG_LOG(WARN, "failed to update used memory size", K(ret));
    } else {
      est_partition_count();
    }
  }
  return ret;
}

// for hash union, intersect, except
//  and hash groupby distinct
template <typename HashCol, typename HashRowStore>
int ObBasicHashPartInfrastructure<HashCol, HashRowStore>::insert_row_with_unique_hash_table(
    const common::ObNewRow* row, bool& exists, bool& inserted)
{
  int ret = OB_SUCCESS;
  const TempHashPartCols part_cols(row);
  const HashCol tmp_cols;
  const HashCol* exists_part_cols = nullptr;
  uint64_t hash_value = 0;
  if (!has_cur_part_dumped_ && OB_FAIL(update_mem_status_periodically())) {
    SQL_ENG_LOG(WARN, "failed to update memory status periodically", K(ret));
  } else if (OB_FAIL(calc_hash_value(row, hash_value))) {
    SQL_ENG_LOG(WARN, "failed to calc hash value", K(ret));
  } else if (OB_FAIL(hash_table_.get(hash_value, part_cols, exists_part_cols))) {
    SQL_ENG_LOG(WARN, "failed to get item", K(ret));
  } else {
    ObChunkRowStore::StoredRow* sr = nullptr;
    bool dumped = false;
    void* buf = nullptr;
    exists = false;
    inserted = false;
    if (OB_ISNULL(exists_part_cols)) {
      // not exists, need create and add
      if (!has_cur_part_dumped_) {
        if (need_dump()) {
          if (OB_FAIL(sql_mem_processor_->extend_max_memory_size(
                  alloc_,
                  [&](int64_t max_memory_size) {
                    UNUSED(max_memory_size);
                    return need_dump();
                  },
                  dumped,
                  sql_mem_processor_->get_data_size()))) {
            SQL_ENG_LOG(WARN, "failed to extend max memory size", K(ret));
          } else if (dumped) {
            if (enable_sql_dumped_) {
              has_cur_part_dumped_ = true;
              if (OB_FAIL(create_dumped_partitions(cur_side_))) {
                SQL_ENG_LOG(WARN, "failed to create dumped partitions", K(ret), K(est_part_cnt_));
              }
            } else {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              SQL_ENG_LOG(WARN, "hash partitioning is out of memory", K(ret), K(get_mem_used()));
            }
          }
        }
        if (OB_FAIL(ret)) {
        } else if (has_cur_part_dumped_) {
          // dumped
          if (OB_FAIL(insert_row_on_partitions(row))) {
            SQL_ENG_LOG(WARN, "failed to insert row on partitions", K(ret));
          }
        } else if (OB_FAIL(preprocess_part_.store_.add_row(*row, &sr))) {
          SQL_ENG_LOG(WARN, "failed to add row into row store", K(ret));
        } else if (OB_ISNULL(buf = arena_alloc_->alloc(sizeof(HashCol)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SQL_ENG_LOG(WARN, "failed to allocate memory", K(ret));
        } else {
          HashCol* new_part_cols = new (buf) HashCol;
          new_part_cols->store_row_ = static_cast<HashRowStore*>(sr);
          new_part_cols->set_hash_value(hash_value);
          new_part_cols->store_row_->set_is_match(false);
          if (OB_FAIL(hash_table_.set(*new_part_cols))) {
            SQL_ENG_LOG(WARN, "failed to set part cols", K(ret));
          } else {
            inserted = true;
          }
        }
      } else {
        // dumped
        if (OB_FAIL(insert_row_on_partitions(row))) {
          SQL_ENG_LOG(WARN, "failed to insert row on partitions", K(ret));
        }
      }
    } else {
      // exists, return exists error
      exists = true;
    }
  }
  return ret;
}

template <typename HashCol, typename HashRowStore>
int ObBasicHashPartInfrastructure<HashCol, HashRowStore>::insert_row_on_hash_table(
    const common::ObNewRow* row, bool& exists, bool& inserted)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL((this->*insert_row_func_)(row, exists, inserted))) {
    // no print of log, as it will return exists or not exists
    // LOG_TRACE("failed to insert row func", K(ret));
  }
  return ret;
}

template <typename HashCol, typename HashRowStore>
int ObBasicHashPartInfrastructure<HashCol, HashRowStore>::insert_row(
    const common::ObNewRow* row, bool& exists, bool& inserted)
{
  int ret = OB_SUCCESS;
  ++period_row_cnt_;
  if (OB_FAIL(insert_row_on_hash_table(row, exists, inserted))) {
    // no print of log, as it will return exists or not exists
    // LOG_TRACE("failed to insert row func", K(ret));
  }
  return ret;
}

template <typename HashCol, typename HashRowStore>
int ObBasicHashPartInfrastructure<HashCol, HashRowStore>::finish_insert_row()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(cur_dumped_parts_)) {
    for (int64_t i = 0; i < est_part_cnt_ && OB_SUCC(ret); ++i) {
      SQL_ENG_LOG(TRACE,
          "trace dumped partition",
          K(cur_dumped_parts_[i]->store_.get_row_cnt_in_memory()),
          K(cur_dumped_parts_[i]->store_.get_row_cnt_on_disk()),
          K(i),
          K(est_part_cnt_),
          K(cur_dumped_parts_[i]->part_key_));
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

template <typename HashCol, typename HashRowStore>
int ObBasicHashPartInfrastructure<HashCol, HashRowStore>::get_next_left_partition()
{
  int ret = OB_SUCCESS;
  SQL_ENG_LOG(TRACE, "trace left part count", K(left_part_list_.get_size()));
  cur_left_part_ = left_part_list_.remove_last();
  if (OB_NOT_NULL(cur_left_part_)) {
    ObIntraPartition* tmp_part = nullptr;
    if (OB_FAIL(left_part_map_.erase_refactored(cur_left_part_->part_key_, &tmp_part))) {
      SQL_ENG_LOG(WARN, "failed to remove part from map", K(ret), K(cur_left_part_->part_key_));
    } else if (cur_left_part_ != tmp_part) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "unexepcted status: part is not match", K(ret), K(cur_left_part_), K(tmp_part));
    }
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

template <typename HashCol, typename HashRowStore>
int ObBasicHashPartInfrastructure<HashCol, HashRowStore>::get_next_right_partition()
{
  int ret = OB_SUCCESS;
  SQL_ENG_LOG(TRACE, "trace left part count", K(right_part_list_.get_size()));
  cur_right_part_ = right_part_list_.remove_last();
  if (OB_NOT_NULL(cur_right_part_)) {
    ObIntraPartition* tmp_part = nullptr;
    if (OB_FAIL(right_part_map_.erase_refactored(cur_right_part_->part_key_, &tmp_part))) {
      SQL_ENG_LOG(WARN, "failed to remove part from map", K(ret), K(cur_right_part_->part_key_));
    } else if (cur_right_part_ != tmp_part) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "unexepcted status: part is not match", K(ret), K(cur_right_part_), K(tmp_part));
    }
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}

template <typename HashCol, typename HashRowStore>
int ObBasicHashPartInfrastructure<HashCol, HashRowStore>::get_cur_matched_partition(InputSide input_side)
{
  int ret = OB_SUCCESS;
  ObIntraPartition* part = nullptr;
  ObIntraPartition* matched_part = nullptr;
  if (InputSide::LEFT == input_side) {
    part = cur_left_part_;
  } else {
    part = cur_right_part_;
  }
  if (OB_ISNULL(part)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "unexpect status: part is null", K(ret));
  } else if (part->part_key_.is_left()) {
    ObIntraPartition* tmp_part = nullptr;
    ObIntraPartKey part_key = part->part_key_;
    part_key.set_right();
    if (OB_FAIL(right_part_map_.erase_refactored(part_key, &tmp_part))) {
    } else {
      matched_part = tmp_part;
      right_part_list_.remove(tmp_part);
    }
  } else {
    ObIntraPartition* tmp_part = nullptr;
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

template <typename HashCol, typename HashRowStore>
int ObBasicHashPartInfrastructure<HashCol, HashRowStore>::open_cur_part(InputSide input_side)
{
  int ret = OB_SUCCESS;
  if (!start_round_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "round is not start", K(ret), K(start_round_));
  } else if ((InputSide::LEFT == input_side && nullptr == cur_left_part_) ||
             (InputSide::RIGHT == input_side && nullptr == cur_right_part_)) {
    SQL_ENG_LOG(WARN, "cur part is null", K(ret), K(input_side));
  } else if (InputSide::LEFT == input_side) {
    if (OB_FAIL(left_row_store_iter_.init(&cur_left_part_->store_, ObChunkRowStore::BLOCK_SIZE))) {
      SQL_ENG_LOG(WARN, "failed to init row store iterator", K(ret));
    } else {
      cur_side_ = input_side;
      cur_level_ = cur_left_part_->part_key_.level_;
      part_shift_ = cur_level_ * CHAR_BIT + sizeof(uint64_t) * CHAR_BIT / 2;
      SQL_ENG_LOG(TRACE,
          "trace open left part",
          K(ret),
          K(cur_left_part_->part_key_),
          K(cur_left_part_->store_.get_row_cnt_in_memory()),
          K(cur_left_part_->store_.get_row_cnt_on_disk()));
    }
  } else if (InputSide::RIGHT == input_side) {
    if (OB_FAIL(right_row_store_iter_.init(&cur_right_part_->store_, ObChunkRowStore::BLOCK_SIZE))) {
      SQL_ENG_LOG(WARN, "failed to init row store iterator", K(ret));
    } else {
      cur_side_ = input_side;
      cur_level_ = cur_right_part_->part_key_.level_;
      part_shift_ = cur_level_ * CHAR_BIT + sizeof(uint64_t) * CHAR_BIT / 2;
      SQL_ENG_LOG(TRACE,
          "trace open right part",
          K(ret),
          K(cur_right_part_->part_key_),
          K(cur_right_part_->store_.get_row_cnt_in_memory()),
          K(cur_right_part_->store_.get_row_cnt_on_disk()));
    }
  }
  return ret;
}

template <typename HashCol, typename HashRowStore>
int ObBasicHashPartInfrastructure<HashCol, HashRowStore>::close_cur_part(InputSide input_side)
{
  int ret = OB_SUCCESS;
  has_cur_part_dumped_ = false;
  ObIntraPartition* tmp_part = nullptr;
  if (!start_round_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "round is not start", K(ret), K(start_round_));
  } else if ((InputSide::LEFT == input_side && nullptr == cur_left_part_) ||
             (InputSide::RIGHT == input_side && nullptr == cur_right_part_)) {
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

template <typename HashCol, typename HashRowStore>
int ObBasicHashPartInfrastructure<HashCol, HashRowStore>::get_next_partition(InputSide input_side)
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
    } else if (OB_ISNULL(cur_left_part_) || InputSide::LEFT != cur_left_part_->part_key_.nth_way_) {
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
    } else if (OB_ISNULL(cur_right_part_) || InputSide::RIGHT != cur_right_part_->part_key_.nth_way_) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "unexpected status: current part is wrong", K(ret), K(cur_right_part_));
    } else {
      cur_side_ = InputSide::RIGHT;
    }
  }
  return ret;
}

template <typename HashCol, typename HashRowStore>
int ObBasicHashPartInfrastructure<HashCol, HashRowStore>::get_next_pair_partition(InputSide input_side)
{
  int ret = OB_SUCCESS;
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

template <typename HashCol, typename HashRowStore>
int ObBasicHashPartInfrastructure<HashCol, HashRowStore>::get_right_next_row(
    const ObChunkRowStore::StoredRow*& store_row)
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

template <typename HashCol, typename HashRowStore>
int ObBasicHashPartInfrastructure<HashCol, HashRowStore>::get_left_next_row(
    const ObChunkRowStore::StoredRow*& store_row)
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

template <typename HashCol, typename HashRowStore>
int ObBasicHashPartInfrastructure<HashCol, HashRowStore>::get_right_next_row(
    const ObChunkRowStore::StoredRow*& store_row, const common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  common::ObNewRow* tmp_row = nullptr;
  if (OB_ISNULL(cur_right_part_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "unexpected status: current partition is null", K(cur_right_part_));
  } else if (OB_FAIL(right_row_store_iter_.get_next_row(store_row))) {
    if (OB_ITER_END != ret) {
      SQL_ENG_LOG(WARN, "failed to get next row", K(ret));
    }
  } else if (OB_FAIL(right_row_store_iter_.convert_to_row(store_row, tmp_row))) {
    SQL_ENG_LOG(WARN, "failed to convert row to store row", K(ret));
  } else {
    row = static_cast<const ObNewRow*>(tmp_row);
  }
  return ret;
}

template <typename HashCol, typename HashRowStore>
int ObBasicHashPartInfrastructure<HashCol, HashRowStore>::get_left_next_row(
    const ObChunkRowStore::StoredRow*& store_row, const common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  common::ObNewRow* tmp_row = nullptr;
  if (OB_ISNULL(cur_left_part_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "unexpected status: current partition is null", K(cur_left_part_));
  } else if (OB_FAIL(left_row_store_iter_.get_next_row(store_row))) {
    if (OB_ITER_END != ret) {
      SQL_ENG_LOG(WARN, "failed to get next row", K(ret));
    }
  } else if (OB_FAIL(left_row_store_iter_.convert_to_row(store_row, tmp_row))) {
    SQL_ENG_LOG(WARN, "failed to convert row to store row", K(ret));
  } else {
    row = static_cast<const ObNewRow*>(tmp_row);
  }
  return ret;
}
//////////////////// end ObBasicHashPartInfrastructure //////////////////

template <typename Item>
int ObPartitionExtendHashTable<Item>::init(
    common::ObIAllocator* allocator, const int64_t initial_size, ObSqlMemMgrProcessor* sql_mem_processor)
{
  int ret = common::OB_SUCCESS;
  sql_mem_processor_ = sql_mem_processor;
  if (initial_size < 2 || nullptr == allocator || OB_ISNULL(sql_mem_processor)) {
    ret = common::OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(ret), K(initial_size), K(allocator));
  } else {
    int64_t est_bucket_num = estimate_bucket_num(initial_size, sql_mem_processor->get_mem_bound());
    allocator_ = OB_NEW(ModulePageAllocator, ObModIds::OB_SQL_HASH_SET, ObModIds::OB_SQL_HASH_SET);
    if (OB_ISNULL(allocator_)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "failed to allocate memory", K(ret));
    } else if (FALSE_IT(allocator_->set_allocator(allocator))) {
    } else if (OB_FAIL(create_bucket_array(est_bucket_num, buckets_))) {
      SQL_ENG_LOG(WARN, "failed to create bucket array", K(ret), K(est_bucket_num));
    } else {
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
int64_t ObPartitionExtendHashTable<Item>::estimate_bucket_num(const int64_t bucket_num, const int64_t max_hash_mem)
{
  int64_t max_bound_size = max_hash_mem * MAX_MEM_PERCENT / 100;
  int64_t est_bucket_num = common::next_pow2(bucket_num);
  int64_t est_size = est_bucket_num * sizeof(void*);
  while (est_size > max_bound_size) {
    est_bucket_num >>= 1;
    est_size = est_bucket_num * sizeof(void*);
  }
  if (est_bucket_num < INITIAL_SIZE) {
    est_bucket_num = INITIAL_SIZE;
  }
  return est_bucket_num;
}

template <typename Item>
int ObPartitionExtendHashTable<Item>::create_bucket_array(const int64_t bucket_num, BucketArray*& new_buckets)
{
  int ret = OB_SUCCESS;
  void* buckets_buf = NULL;
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
      SQL_ENG_LOG(WARN, "resize bucket array failed", K(ret), K(tmp_bucket_num));
    }
  }
  return ret;
}

template <typename Item>
int ObPartitionExtendHashTable<Item>::resize(
    common::ObIAllocator* allocator, int64_t bucket_num, ObSqlMemMgrProcessor* sql_mem_processor)
{
  int ret = OB_SUCCESS;
  int64_t est_max_bucket_num = 0;
  if (OB_ISNULL(sql_mem_processor_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "sql mem processor is null", K(ret));
  } else if (FALSE_IT(est_max_bucket_num = estimate_bucket_num(bucket_num, sql_mem_processor->get_mem_bound()))) {
  } else if (est_max_bucket_num >= get_bucket_num()) {
    // estimated bucket size greater than actual, reuse
    reuse();
  } else {
    destroy();
    if (OB_FAIL(init(allocator, bucket_num, sql_mem_processor))) {
      SQL_ENG_LOG(WARN, "failed to reuse with bucket", K(bucket_num), K(ret));
    }
  }
  return ret;
}

template <typename Item>
int ObPartitionExtendHashTable<Item>::get(
    uint64_t hash_value, const TempHashPartCols& part_cols, const Item*& item) const
{
  int ret = OB_SUCCESS;
  item = NULL;
  if (OB_ISNULL(part_col_idxs_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "part_col_idxs_ is null", K(ret));
  } else if (NULL == buckets_) {
    // do nothing
  } else {
    common::hash::hash_func<Item> hf;
    Item* bucket = buckets_->at(hash_value & (get_bucket_num() - 1));
    while (NULL != bucket) {
      if (hash_value == hf(*bucket) && bucket->equal_temp(part_cols, part_col_idxs_)) {
        item = bucket;
        break;
      }
      bucket = bucket->next();
    }
  }
  return ret;
}

template <typename Item>
int ObPartitionExtendHashTable<Item>::get(const Item& item, const Item*& res) const
{
  int ret = OB_SUCCESS;
  res = NULL;
  if (OB_ISNULL(part_col_idxs_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "part_col_idxs_ is null", K(ret));
  } else if (NULL == buckets_) {
    // do nothing
  } else {
    common::hash::hash_func<Item> hf;
    const uint64_t hash_val = hf(item);
    Item* bucket = buckets_->at(hash_val & (get_bucket_num() - 1));
    while (NULL != bucket) {
      if (hash_val == hf(*bucket) && bucket->equal(item, part_col_idxs_)) {
        res = bucket;
        break;
      }
      bucket = bucket->next();
    }
  }
  return ret;
}

template <typename Item>
int ObPartitionExtendHashTable<Item>::set(Item& item)
{
  int ret = common::OB_SUCCESS;
  common::hash::hash_func<Item> hf;
  if (size_ >= get_bucket_num() * SIZE_BUCKET_PERCENT) {
    int64_t extend_bucket_num = estimate_bucket_num(get_bucket_num() * 2, sql_mem_processor_->get_mem_bound());
    if (extend_bucket_num <= get_bucket_num()) {
    } else if (OB_FAIL(extend(get_bucket_num() * 2))) {
      SQL_ENG_LOG(WARN, "extend failed", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(buckets_)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(ret), K(buckets_));
  } else if (OB_ISNULL(item.store_row_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "unexpected status: store_row is null", K(ret));
  } else {
    Item*& bucket = buckets_->at(hf(item) & (get_bucket_num() - 1));
    item.next() = bucket;
    bucket = &item;
    size_ += 1;
  }
  return ret;
}

template <typename Item>
int ObPartitionExtendHashTable<Item>::extend(const int64_t new_bucket_num)
{
  int ret = common::OB_SUCCESS;
  common::hash::hash_func<Item> hf;
  BucketArray* new_buckets = NULL;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(buckets_)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(ret), K(buckets_));
  } else if (OB_FAIL(create_bucket_array(new_bucket_num, new_buckets))) {
    SQL_ENG_LOG(WARN, "failed to create bucket array", K(ret));
  } else if (get_bucket_num() * 2 != new_buckets->count()) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "unexpected extend", K(ret), K(new_bucket_num), K(get_bucket_num()));
  } else {
    // rehash
    const int64_t tmp_new_bucket_num = new_buckets->count();
    for (int64_t i = 0; i < get_bucket_num(); i++) {
      Item* bucket = buckets_->at(i);
      while (nullptr != bucket) {
        Item* item = bucket;
        bucket = bucket->next();
        Item*& new_bucket = new_buckets->at(hf(*item) & (tmp_new_bucket_num - 1));
        item->next() = new_bucket;
        new_bucket = item;
      }
    }
    buckets_->destroy();
    allocator_->free(buckets_);
    buckets_ = new_buckets;
  }
  return ret;
}
///////////////////////////////////////////////////////////////////////////////////

}  // namespace sql
}  // namespace oceanbase

#endif /* OB_HASH_PARTITIONING_INFRASTRUCTURE_H_ */
