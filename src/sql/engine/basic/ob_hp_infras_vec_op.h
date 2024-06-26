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

#ifndef OB_HASH_PARTITIONING_INFRASTRUCTURE_VEC_OP_H_
#define OB_HASH_PARTITIONING_INFRASTRUCTURE_VEC_OP_H_

#include "lib/list/ob_dlist.h"
#include "sql/engine/basic/ob_hash_partitioning_basic.h"
#include "share/datum/ob_datum_funcs.h"
#include "sql/engine/sort/ob_sort_basic_info.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"
#include "sql/engine/expr/ob_expr.h"
#include "lib/hash/ob_array_hash_map.h"
#include "sql/engine/aggregate/ob_adaptive_bypass_ctrl.h"
#include "sql/engine/aggregate/ob_exec_hash_struct_vec.h"

namespace oceanbase
{
namespace sql
{

/*
|        |extra: hash value + next ptr|       |
               compact row
*/
class ObHashPartItem : public ObCompactRow
{
public:
  // used for hash set.
  const static int64_t HASH_VAL_BIT = 63;
  const static int64_t HASH_VAL_MASK = UINT64_MAX >> (64 - HASH_VAL_BIT);
  struct ExtraInfo
  {
    uint64_t hash_val_:HASH_VAL_BIT;
    uint64_t is_match_:1;
  };
  // end.
  ObHashPartItem() : ObCompactRow() {}
  ~ObHashPartItem() {}
  ObHashPartItem *next(const RowMeta &row_meta)
  {
    return reinterpret_cast<ObHashPartItem *> (*reinterpret_cast<int64_t *>((static_cast<char *> (
        this->get_extra_payload(row_meta)) + sizeof(uint64_t))));
  }
  void set_next(const ObHashPartItem *next, const RowMeta &row_meta)
  {
    *reinterpret_cast<int64_t *>(static_cast<char *> (this->get_extra_payload(row_meta)) + sizeof(uint64_t))
        = reinterpret_cast<int64_t> (next);
  }
  uint64_t get_hash_value(const RowMeta &row_meta) const
  {
    return *reinterpret_cast<int64_t *>(this->get_extra_payload(row_meta));
  }
  void set_hash_value(const uint64_t hash_val, const RowMeta &row_meta)
  {
    *reinterpret_cast<uint64_t *>(this->get_extra_payload(row_meta)) = hash_val & HASH_VAL_MASK;
  }
  static int64_t get_extra_size() { return sizeof(uint64_t) + sizeof(ObHashPartItem *); }
  ExtraInfo &get_extra_info(const RowMeta &row_meta)
  {
    static_assert(sizeof(ObHashPartItem) == sizeof(ObCompactRow),
        "sizeof ObHashPartItem must be the save with ObCompactRow");
    return *reinterpret_cast<ExtraInfo *>(get_extra_payload(row_meta));
  }
  const ExtraInfo &get_extra_info(const RowMeta &row_meta) const
  { return *reinterpret_cast<const ExtraInfo *>(get_extra_payload(row_meta)); }

  bool is_match(const RowMeta &row_meta) const { return get_extra_info(row_meta).is_match_; }
  void set_is_match(const RowMeta &row_meta, bool is_match) { get_extra_info(row_meta).is_match_ = is_match; }
};

template<typename CompactRowItem>
struct ObHashPartBucketBase : public ObGroupRowBucketBase
{
  typedef CompactRowItem RowItemType;
  static const int64_t MAX_PAYLOAD_SIZE = 64;
  ObHashPartBucketBase() : ObGroupRowBucketBase() {}
  CompactRowItem &get_item() { CompactRowItem item; return item; };
  void set_item(CompactRowItem &item) {};
  TO_STRING_KV(K(info_));
};

template<typename CompactRowItem>
struct ObHashPartBucketGeneral final : public ObHashPartBucketBase<CompactRowItem>
{
  using RowItemType = typename ObHashPartBucketBase<CompactRowItem>::RowItemType;
  ObHashPartBucketGeneral() : ObHashPartBucketBase<CompactRowItem>(), item_(nullptr)
  {
  }
  CompactRowItem &get_item()
  { return *item_; }
  void set_item(CompactRowItem &item)
  { item_ = &item; }
  CompactRowItem *item_;
};

template<typename CompactRowItem, uint64_t LEN>
struct ObFixedLenHashPartBucket final : public ObHashPartBucketBase<CompactRowItem>
{
  using RowItemType = typename ObHashPartBucketBase<CompactRowItem>::RowItemType;
  ObFixedLenHashPartBucket() : ObHashPartBucketBase<CompactRowItem>(), item_()
  {
  }
  ~ObFixedLenHashPartBucket() {}
  union {
    CompactRowItem item_;
    char buffer_[LEN];
  };
  CompactRowItem &get_item() { return item_; }
  void set_item(CompactRowItem &item)
  {
    MEMCPY(buffer_, reinterpret_cast<char *> (&item), item.get_row_size());
  }
  OB_INLINE ObFixedLenHashPartBucket &operator =(const ObFixedLenHashPartBucket &other)
  {
    this->hash_ = other.hash_;
    MEMCPY(buffer_, other.buffer_,  LEN);
    return *this;
  }
};

using HPInfrasBktGeneral = ObHashPartBucketGeneral<ObHashPartItem>;
using HPInfrasFixedBktByte48 = ObFixedLenHashPartBucket<ObHashPartItem, 48>;
using HPInfrasFixedBktByte56 = ObFixedLenHashPartBucket<ObHashPartItem, 56>;
using HPInfrasFixedBktByte64 = ObFixedLenHashPartBucket<ObHashPartItem, 64>;

class ObIHashPartInfrastructure
{
public:
  ObIHashPartInfrastructure(int64_t tenant_id, lib::MemoryContext &mem_context) :
    tenant_id_(tenant_id), mem_context_(mem_context), alloc_(nullptr), arena_alloc_(nullptr),
    preprocess_part_(), left_part_list_(), right_part_list_(),
    left_part_map_(), right_part_map_(), io_event_observer_(nullptr), sql_mem_processor_(nullptr),
    sort_collations_(nullptr), eval_ctx_(nullptr),
    cur_left_part_(nullptr), cur_right_part_(nullptr),
    left_dumped_parts_(nullptr), right_dumped_parts_(nullptr),
    cur_dumped_parts_(nullptr), left_row_store_iter_(), right_row_store_iter_(),
    hash_table_row_store_iter_(), enable_sql_dumped_(false), unique_(false), need_pre_part_(false),
    ways_(InputWays::TWO), init_part_func_(nullptr), insert_row_func_(nullptr),
    cur_part_start_id_(0), start_round_(false), cur_side_(InputSide::LEFT),
    has_cur_part_dumped_(false), has_create_part_map_(false),
    est_part_cnt_(INT64_MAX), cur_level_(0), part_shift_(0), period_row_cnt_(0),
    left_part_cur_id_(0), right_part_cur_id_(0), my_skip_(nullptr),
    is_push_down_(false), exprs_(nullptr), is_inited_vec_(false), max_batch_size_(0),
    compressor_type_(NONE_COMPRESSOR)
  {}
  virtual ~ObIHashPartInfrastructure();
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
      part_key_(), store_()
    {}
    ~ObIntraPartition()
    {
      store_.destroy();
    }
  public:
    int init();

    TO_STRING_KV(K_(part_key));
  public:
    ObIntraPartKey part_key_;
    ObTempRowStore store_;
  };

public:
  virtual void reset();
  virtual void destroy();
  virtual int64_t get_hash_bucket_num() = 0;
  virtual int64_t get_bucket_num() const = 0;
  virtual int64_t get_each_bucket_size() const = 0;
  virtual void reset_hash_table_for_by_pass() = 0;
  virtual int64_t get_hash_table_size() const = 0;
  virtual int extend_hash_table_l3() = 0;
  virtual int resize(int64_t bucket_cnt) = 0;
  virtual int init_hash_table(int64_t bucket_cnt,
                              int64_t min_bucket = MIN_BUCKET_NUM,
                              int64_t max_bucket = MAX_BUCKET_NUM) = 0;
  virtual int exists_batch(const common::ObIArray<ObExpr*> &exprs, const ObBatchRows &brs, ObBitVector *skip,
              uint64_t *hash_values_for_batch) = 0;
  int init(uint64_t tenant_id, bool enable_sql_dumped, bool unique, bool need_pre_part,
    int64_t ways, int64_t max_batch_size, const common::ObIArray<ObExpr*> &exprs,
    ObSqlMemMgrProcessor *sql_mem_processor, const common::ObCompressorType compressor_type);
  void switch_left()
  { cur_side_ = InputSide::LEFT; }
  void switch_right()
  { cur_side_ = InputSide::RIGHT; }
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
  int insert_row_for_batch(const common::ObIArray<ObExpr *> &batch_exprs,
                           uint64_t *hash_values_for_batch,
                           const int64_t batch_size,
                           const ObBitVector *skip,
                           ObBitVector *&output_vec);
  int insert_batch_on_partitions(const common::ObIArray<ObExpr *> &exprs,
                                 const ObBitVector &skip,
                                 const int64_t batch_size,
                                 uint64_t *hash_values);
  int do_insert_batch_with_unique_hash_table_by_pass(const common::ObIArray<ObExpr *> &exprs,
                                                     uint64_t *hash_values_for_batch,
                                                     const int64_t batch_size,
                                                     const ObBitVector *skip,
                                                     bool is_block,
                                                     bool can_insert,
                                                     int64_t &exists,
                                                     bool &full_by_pass,
                                                     ObBitVector *&output_vec);
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
  int init_default_part(ObIntraPartition *part, int64_t nth_part, int64_t limit, int32_t delta_shift);

  int create_dumped_partitions(InputSide input_side);

  int get_next_pair_partition(InputSide input_side);
  int get_next_partition(InputSide input_side);
  int get_left_next_batch(const common::ObIArray<ObExpr *> &exprs,
                          const int64_t max_row_cnt,
                          int64_t &read_rows,
                          uint64_t *hash_values_for_batch);
  int get_right_next_batch(const common::ObIArray<ObExpr *> &exprs,
                           const int64_t max_row_cnt,
                           int64_t &read_rows);
  int get_next_hash_table_batch(const common::ObIArray<ObExpr *> &exprs,
                                const int64_t max_row_cnt,
                                int64_t &read_rows,
                                const ObCompactRow **store_row);
  // int clean_partition();

  OB_INLINE bool has_left_dumped() { return OB_NOT_NULL(left_dumped_parts_); }
  OB_INLINE bool has_right_dumped() { return OB_NOT_NULL(right_dumped_parts_); }
  OB_INLINE bool has_dumped_partitions()
  { return !(left_part_list_.is_empty() && right_part_list_.is_empty()); }
  int calc_hash_value_for_batch(const common::ObIArray<ObExpr *> &batch_exprs,
                                const ObBatchRows &child_brs,
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
      if (OB_NOT_NULL(alloc_) && OB_NOT_NULL(my_skip_)) {
        alloc_->free(my_skip_);
        my_skip_ = nullptr;
      }
    }
  }

  int set_funcs(const common::ObIArray<ObSortFieldCollation> *sort_collations,
                ObEvalCtx *eval_ctx)
  {
    int ret = common::OB_SUCCESS;
    if (nullptr == sort_collations
        || nullptr == eval_ctx) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "unexpected status: funcs is null", K(ret));
    } else {
      sort_collations_ = sort_collations;
      eval_ctx_ = eval_ctx;
    }
    return ret;
  }
  int64_t get_hash_store_mem_used() const { return preprocess_part_.store_.get_mem_used(); }
  const RowMeta &get_hash_store_row_meta() const { return preprocess_part_.store_.get_row_meta(); }
  void set_push_down() { is_push_down_ = true; }
  int process_dump(bool is_block, bool &full_by_pass);
  bool hash_table_full() { return get_hash_table_size() >= 0.8 * get_hash_bucket_num(); }
  inline void set_io_event_observer(ObIOEventObserver *observer)
  {
    io_event_observer_ = observer;
  }

protected:
  virtual int set_distinct_batch(const common::ObIArray<ObExpr *> &exprs,
                                  uint64_t *hash_values_for_batch,
                                  const int64_t batch_size,
                                  const ObBitVector *skip,
                                  ObBitVector &my_skip) = 0;
  virtual int probe_batch(uint64_t *hash_values_for_batch,
                          const int64_t batch_size,
                          const ObBitVector *skip,
                          ObBitVector &my_skip) = 0;
  bool is_left() const { return InputSide::LEFT == cur_side_; }
  bool is_right() const { return InputSide::RIGHT == cur_side_; }

  typedef int (ObIHashPartInfrastructure::*InitPartitionFunc)
      (ObIntraPartition *part, int64_t nth_part, int64_t limit, int32_t delta_shift);
  typedef int (ObIHashPartInfrastructure::*InsertRowFunc)
      (const common::ObIArray<ObExpr*> &exprs, bool &exists, bool &inserted);
  int direct_insert_row(
      const common::ObIArray<ObExpr*> &exprs, bool &exists, bool &inserted);
  int insert_row_with_hash_table(
      const common::ObIArray<ObExpr*> &exprs, bool &exists, bool &inserted);
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

    // test for dump once
    bool force_dump = !(EVENT_CALL(EventTable::EN_SQL_FORCE_DUMP) == OB_SUCCESS);
    if (force_dump && is_test_for_dump_ == true) {
      is_test_for_dump_ = false;
      return true;
    }

    return (sql_mem_processor_->get_mem_bound() <= est_part_cnt_ * BLOCK_SIZE + get_mem_used());
  }

  int64_t get_mem_used() { return (nullptr == mem_context_) ? 0 : mem_context_->used();}

  // 高位4 byte作为partition hash value
  OB_INLINE int64_t get_part_idx(const uint64_t hash_value)
  {
    return (hash_value >> part_shift_) & (est_part_cnt_ - 1);
  }

  int append_dumped_parts(InputSide input_side);
  int append_all_dump_parts();
  void destroy_cur_parts();
  void clean_dumped_partitions();
  void clean_cur_dumping_partitions();

  int update_mem_status_periodically();

protected:
  static const int64_t BLOCK_SIZE = 64 * 1024;
  static const int64_t MIN_BUCKET_NUM = 128;
  static const int64_t MAX_BUCKET_NUM = 131072;   // 1M = 131072 * 8
  static const int64_t MAX_PART_LEVEL = 4;
  uint64_t tenant_id_;
  lib::MemoryContext &mem_context_;
  common::ObIAllocator *alloc_;
  common::ObArenaAllocator *arena_alloc_;
  ObIntraPartition preprocess_part_;
  common::ObDList<ObIntraPartition> left_part_list_;
  common::ObDList<ObIntraPartition> right_part_list_;
  common::hash::ObHashMap<ObIntraPartKey, ObIntraPartition*, common::hash::NoPthreadDefendMode>
                                                                                    left_part_map_;
  common::hash::ObHashMap<ObIntraPartKey, ObIntraPartition*, common::hash::NoPthreadDefendMode>
                                                                                    right_part_map_;
  ObIOEventObserver *io_event_observer_;
  ObSqlMemMgrProcessor *sql_mem_processor_;
  // 这里暂时以一份保存，如果是多元operator，part_col_idxs不一样，需要区分
  const common::ObIArray<ObSortFieldCollation> *sort_collations_;
  ObEvalCtx *eval_ctx_;
  ObIntraPartition *cur_left_part_;
  ObIntraPartition *cur_right_part_;
  ObIntraPartition **left_dumped_parts_;
  ObIntraPartition **right_dumped_parts_;
  ObIntraPartition **cur_dumped_parts_;
  ObTempRowStore::Iterator left_row_store_iter_;
  ObTempRowStore::Iterator right_row_store_iter_;
  ObTempRowStore::Iterator hash_table_row_store_iter_;
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
  bool is_push_down_;
  const common::ObIArray<ObExpr*> *exprs_;
  bool is_inited_vec_;
  common::ObFixedArray<ObIVector *, common::ObIAllocator> vector_ptrs_;
  int64_t max_batch_size_;
  common::ObCompressorType compressor_type_;
  bool is_test_for_dump_ = true; // tracepoint for dump once.
};

template<typename HashBucket>
class ObHashPartInfrastructureVec final: public ObIHashPartInfrastructure
{
public:
  ObHashPartInfrastructureVec(int64_t tenant_id, lib::MemoryContext &mem_context)
    : ObIHashPartInfrastructure(tenant_id, mem_context),
      hash_table_(tenant_id)
  {
  }
  virtual ~ObHashPartInfrastructureVec();
  void reset() override;
  void destroy() override;
  int64_t get_hash_bucket_num() override { return hash_table_.get_bucket_num(); }
  int64_t get_bucket_num() const override { return hash_table_.get_bucket_num(); }
  int64_t get_each_bucket_size() const override { return sizeof(HashBucket); }
  void reset_hash_table_for_by_pass() override
  {
    hash_table_.reuse();
    hash_table_row_store_iter_.reset();
    preprocess_part_.store_.reset();
  }
  int64_t get_hash_table_size() const override { return hash_table_.size(); }
  int extend_hash_table_l3() override
  {
    return hash_table_.extend(EXTEND_BKT_NUM_PUSH_DOWN);
  }
  int resize(int64_t bucket_cnt) override;
  int init_hash_table(int64_t bucket_cnt,
                      int64_t min_bucket = MIN_BUCKET_NUM,
                      int64_t max_bucket = MAX_BUCKET_NUM) override;
  int exists_batch(const common::ObIArray<ObExpr*> &exprs,
                  const ObBatchRows &brs, ObBitVector *skip,
                uint64_t *hash_values_for_batch) override;
private:
  int set_distinct_batch(const common::ObIArray<ObExpr *> &exprs,
                         uint64_t *hash_values_for_batch,
                         const int64_t batch_size,
                         const ObBitVector *skip,
                         ObBitVector &my_skip) override;
  int probe_batch(uint64_t *hash_values_for_batch,
                  const int64_t batch_size,
                  const ObBitVector *skip,
                  ObBitVector &my_skip) override;
  int64_t est_extend_hash_bucket_num(const int64_t bucket_num,
                                     const int64_t max_hash_mem,
                                     const int64_t min_bucket);
  template<typename BucketType>
  int prefetch(uint64_t *hash_values_for_batch,
               const int64_t batch_size,
               const ObBitVector *skip)
  {
    int ret = common::OB_SUCCESS;
    int64_t num_cnt = hash_table_.get_bucket_num() - 1;
    const auto *buckets = hash_table_.get_buckets();
    for (int i = 0; i < batch_size; ++i) {
      if (OB_NOT_NULL(skip) && skip->at(i)) {
        continue;
      }
      int64_t bkt_idx = (hash_values_for_batch[i] & num_cnt);
      const auto &curr_bkt = buckets->at(bkt_idx);
      __builtin_prefetch(&curr_bkt, 0/* read */, 2 /*high temp locality*/);
    }
    return ret;
  }

  template<>
  int prefetch<HPInfrasBktGeneral>(uint64_t *hash_values_for_batch,
                                   const int64_t batch_size,
                                   const ObBitVector *skip)
  {
    int ret = common::OB_SUCCESS;
    int64_t num_cnt = hash_table_.get_bucket_num() - 1;
    const auto *buckets = hash_table_.get_buckets();
    for (int i = 0; i < batch_size; ++i) {
      if (OB_NOT_NULL(skip) && skip->at(i)) {
        continue;
      }
      int64_t bkt_idx = (hash_values_for_batch[i] & num_cnt);
      const auto &curr_bkt = buckets->at(bkt_idx);
      __builtin_prefetch(&curr_bkt, 0/* read */, 2 /*high temp locality*/);
    }
    return ret;
  }

private:
  const static int64_t EXTENDED_RATIO = 2;
  const static int64_t MAX_MEM_PERCENT = 40;
  const static int64_t INITIAL_SIZE = 128;
  const int64_t EXTEND_BKT_NUM_PUSH_DOWN = INIT_L3_CACHE_SIZE / sizeof(HashBucket);
  ObExtendHashTableVec<HashBucket> hash_table_;
};

class ObHashPartInfrastructureVecImpl final
{
public:
  enum BucketType : int32_t
  {
    INVALID_TYPE = -1,
    TYPE_GENERAL,
    BYTE_TYPE_48,
    BYTE_TYPE_56,
    BYTE_TYPE_64,
    TYPE_MAX
  };
  ObHashPartInfrastructureVecImpl()
    : mem_context_(nullptr), alloc_(nullptr), is_inited_(false),
      bkt_size_(0), bkt_type_(INVALID_TYPE), hp_infras_(nullptr)
  {
  }
  ~ObHashPartInfrastructureVecImpl()
  {
    destroy();
  }
  int check_status() const
  {
    int ret = OB_SUCCESS;
    if (nullptr == hp_infras_) {
  #ifndef NDEBUG
      ob_abort();
  #else
      ret = OB_NOT_INIT;
      SQL_ENG_LOG(WARN, "not inited", K(ret));
  #endif

    }
    return ret;
  }
  void reset();
  void destroy();
  bool is_inited() const { return is_inited_; }
  int init(uint64_t tenant_id, bool enable_sql_dumped, bool unique, bool need_pre_part,
    int64_t ways, int64_t max_batch_size, const common::ObIArray<ObExpr*> &exprs,
    ObSqlMemMgrProcessor *sql_mem_processor, const common::ObCompressorType compressor_type);
  int init_mem_context(uint64_t tenant_id);
  int decide_hp_infras_type(const common::ObIArray<ObExpr*> &exprs, BucketType &bkt_type, uint64_t &payload_len);
  template<typename BktType>
  int alloc_hp_infras_impl_instance(const int64_t tenant_id, ObIHashPartInfrastructure *&hp_infras);
  int init_hp_infras(const int64_t tenant_id,
                     const common::ObIArray<ObExpr*> &exprs,
                     ObIHashPartInfrastructure *&hp_infras);
  void set_io_event_observer(ObIOEventObserver *observer);
  void set_push_down();
  int64_t get_bucket_size() const;
  int64_t est_bucket_count(
    const int64_t rows,
    const int64_t width,
    const int64_t min_bucket_cnt = MIN_BUCKET_NUM,
    const int64_t max_bucket_cnt = MAX_BUCKET_NUM);
  int set_funcs(const common::ObIArray<ObSortFieldCollation> *sort_collations,
                ObEvalCtx *eval_ctx);
  int start_round();
  int end_round();
  void switch_left();
  void switch_right();
  bool has_cur_part(InputSide input_side);
  int get_right_next_batch(const common::ObIArray<ObExpr *> &exprs,
                          const int64_t max_row_cnt,
                          int64_t &read_rows);
  int exists_batch(const common::ObIArray<ObExpr*> &exprs,
              const ObBatchRows &brs, ObBitVector *skip,
                  uint64_t *hash_values_for_batch);
  const RowMeta &get_hash_store_row_meta() const;
  int init_hash_table(int64_t bucket_cnt,
                      int64_t min_bucket = MIN_BUCKET_NUM,
                      int64_t max_bucket = MAX_BUCKET_NUM);
  int init_my_skip(const int64_t batch_size);
  int calc_hash_value_for_batch(const common::ObIArray<ObExpr *> &batch_exprs,
                                const ObBatchRows &child_brs,
                                uint64_t *hash_values_for_batch,
                                int64_t start_idx = 0,
                                uint64_t *hash_vals = nullptr);
  int finish_insert_row();
  int open_cur_part(InputSide input_side);
  int close_cur_part(InputSide input_side);
  int64_t get_cur_part_row_cnt(InputSide input_side);

  int64_t get_cur_part_file_size(InputSide input_side);
  int get_next_pair_partition(InputSide input_side);
  int get_next_partition(InputSide input_side);
  int get_left_next_batch(const common::ObIArray<ObExpr *> &exprs,
                          const int64_t max_row_cnt,
                          int64_t &read_rows,
                          uint64_t *hash_values_for_batch);
  int get_next_hash_table_batch(const common::ObIArray<ObExpr *> &exprs,
                                const int64_t max_row_cnt,
                                int64_t &read_rows,
                                const ObCompactRow **store_row);
  int resize(int64_t bucket_cnt);
  int insert_row_for_batch(const common::ObIArray<ObExpr *> &batch_exprs,
                           uint64_t *hash_values_for_batch,
                           const int64_t batch_size,
                           const ObBitVector *skip,
                           ObBitVector *&output_vec);
  int do_insert_batch_with_unique_hash_table_by_pass(const common::ObIArray<ObExpr *> &exprs,
                                                     uint64_t *hash_values_for_batch,
                                                     const int64_t batch_size,
                                                     const ObBitVector *skip,
                                                     bool is_block,
                                                     bool can_insert,
                                                     int64_t &exists,
                                                     bool &full_by_pass,
                                                     ObBitVector *&output_vec);
  int open_hash_table_part();
  int64_t get_hash_bucket_num();
  int64_t get_bucket_num() const;
  void reset_hash_table_for_by_pass();
  int64_t get_hash_table_size() const;
  int extend_hash_table_l3();
  bool hash_table_full();
  int64_t get_hash_store_mem_used() const;
  void destroy_my_skip();
  int64_t estimate_total_count() const;
private:
  static const int64_t MIN_BUCKET_NUM = 128;
  static const int64_t MAX_BUCKET_NUM = 131072;   // 1M = 131072 * 8
  lib::MemoryContext mem_context_;
  common::ObIAllocator *alloc_;
  bool is_inited_;
  int64_t bkt_size_;
  BucketType bkt_type_;
  ObIHashPartInfrastructure *hp_infras_;
};

//////////////////// start ObHashPartInfrastructureVec //////////////////
template<typename HashBucket>
ObHashPartInfrastructureVec<HashBucket>::~ObHashPartInfrastructureVec()
{
  destroy();
}

template<typename HashBucket>
void ObHashPartInfrastructureVec<HashBucket>::reset()
{
  hash_table_.destroy();
  ObIHashPartInfrastructure::reset();
}

template<typename HashBucket>
void ObHashPartInfrastructureVec<HashBucket>::destroy()
{
  hash_table_.destroy();
  ObIHashPartInfrastructure::destroy();
}

template<typename HashBucket>
int ObHashPartInfrastructureVec<HashBucket>::resize(int64_t bucket_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(alloc_) || !start_round_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "allocator is null or it don'e start to round", K(ret), K(start_round_));
  } else if (OB_FAIL(hash_table_.resize(alloc_, max(2, bucket_cnt)))) {
    SQL_ENG_LOG(WARN, "failed to init hash table", K(ret), K(bucket_cnt));
  }
  return ret;
}

template <typename HashBucket>
int ObHashPartInfrastructureVec<HashBucket>::init_hash_table(int64_t initial_size,
                                                             int64_t min_bucket, int64_t max_bucket)
{
  int ret = OB_SUCCESS;
  lib::ObMemAttr mem_attr;
  mem_attr.tenant_id_ = tenant_id_;
  mem_attr.label_ = "HashPartInfra";
  mem_attr.ctx_id_ = common::ObCtxIds::WORK_AREA;
  bool nullable = true;
  bool all_int64 = false;
  int64_t hash_bucket_cnt = est_extend_hash_bucket_num(
    initial_size * EXTENDED_RATIO, sql_mem_processor_->get_mem_bound(), min_bucket);
  if (OB_ISNULL(alloc_) || !start_round_) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "allocator is null or it don'e start to round", K(ret), K(start_round_));
  } else if (OB_FAIL(hash_table_.init(alloc_, mem_attr, *exprs_, sort_collations_->count(),
                                      eval_ctx_, max_batch_size_, nullable, all_int64, 0, false, 0,
                                      hash_bucket_cnt, is_push_down_ ? false : true))) {
    SQL_ENG_LOG(WARN, "failed to init hash table", K(ret), K(hash_bucket_cnt));
  } else if (OB_FAIL(vector_ptrs_.prepare_allocate(exprs_->count()))) {
    SQL_ENG_LOG(WARN, "failed to alloc ptrs", K(ret));
  }
  return ret;
}

template <typename HashBucket>
int64_t ObHashPartInfrastructureVec<HashBucket>::est_extend_hash_bucket_num(
  const int64_t bucket_num, const int64_t max_hash_mem, const int64_t min_bucket)
{
  int64_t max_bound_size = std::max(0l, max_hash_mem * MAX_MEM_PERCENT / 100);
  int64_t est_bucket_num = common::next_pow2(bucket_num);
  int64_t est_size = est_bucket_num * sizeof(HashBucket);
  while (est_size > max_bound_size && est_bucket_num > 0) {
    est_bucket_num >>= 1;
    est_size = est_bucket_num * sizeof(HashBucket);
  }
  if (est_bucket_num < INITIAL_SIZE) {
    est_bucket_num = INITIAL_SIZE;
  }
  if (est_bucket_num < min_bucket) {
    est_bucket_num = min_bucket;
  }
  return est_bucket_num / 2;
}

template<typename HashBucket>
int ObHashPartInfrastructureVec<HashBucket>::
set_distinct_batch(const common::ObIArray<ObExpr *> &exprs,
                   uint64_t *hash_values_for_batch,
                   const int64_t batch_size,
                   const ObBitVector *skip,
                   ObBitVector &my_skip)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(eval_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "eval_ctx is not init ", K(ret));
  }
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
  batch_info_guard.set_batch_idx(0);
  batch_info_guard.set_batch_size(batch_size);
  auto sf = [&] (const IVectorPtrs &vectors,
                  const uint16_t selector[],
                  const int64_t size,
                  ObCompactRow **stored_rows) -> int {
    ret = preprocess_part_.store_.add_batch(vectors, selector, size, stored_rows);
    return ret;
  };
  if (!is_push_down_ && OB_FAIL(prefetch<HashBucket>(hash_values_for_batch, batch_size, skip))) {
    SQL_ENG_LOG(WARN, "failed to prefetch", K(ret));
  } else if (OB_FAIL(hash_table_.set_distinct_batch(preprocess_part_.store_.get_row_meta(), batch_size, skip,
                                             my_skip, hash_values_for_batch, sf))) {
    LOG_WARN("failed to set batch", K(ret));
  }
  return ret;
}

template <typename HashBucket>
int ObHashPartInfrastructureVec<HashBucket>::exists_batch(
                const common::ObIArray<ObExpr*> &exprs,
                const ObBatchRows &brs, ObBitVector *skip,
                uint64_t *hash_values_for_batch)
{
 int ret = OB_SUCCESS;
  if (OB_ISNULL(hash_values_for_batch)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "hash values vector is not init", K(ret));
  } else if (OB_ISNULL(skip)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_ENG_LOG(WARN, "skip vector is null", K(ret));
  } else if (OB_FAIL(calc_hash_value_for_batch(exprs, brs, hash_values_for_batch))) {
    SQL_ENG_LOG(WARN, "failed to calc hash values", K(ret));
  } else {
    const ObHashPartItem *exists_item = nullptr;
    ObBitVector &skip_for_dump = *my_skip_;
    skip_for_dump.reset(brs.size_);
    if (OB_FAIL(prefetch<HashBucket>(hash_values_for_batch, brs.size_, brs.skip_))) {
      SQL_ENG_LOG(WARN, "failed to prefetch", K(ret));
    } else {
      ObEvalCtx::BatchInfoScopeGuard guard(*eval_ctx_);
      for (int64_t i = 0; OB_SUCC(ret) && i < brs.size_; ++i) {
        //if nullptr, data is from dumped partition
        if (OB_NOT_NULL(brs.skip_) && brs.skip_->at(i)) {
          skip->set(i); //skip indicates rows need to return, only useful for intersect
          skip_for_dump.set(i); //skip_for_dump indicates rows need to dump
          continue;
        }
        guard.set_batch_idx(i);
        if (OB_FAIL(hash_table_.get(preprocess_part_.store_.get_row_meta(),
                                i, hash_values_for_batch[i],
                                exists_item))) {
          SQL_ENG_LOG(WARN, "failed to get item", K(ret));
        } else if (OB_ISNULL(exists_item)) {
          skip->set(i);
        } else if (exists_item->is_match(preprocess_part_.store_.get_row_meta())) {
          skip->set(i);
          //we dont need dumped this row
          skip_for_dump.set(i);
        } else {
          const_cast<ObHashPartItem*>(exists_item)->set_is_match(preprocess_part_.store_.get_row_meta(), true);
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
                                                    brs.size_, hash_values_for_batch))) {
        SQL_ENG_LOG(WARN, "failed to insert row into partitions", K(ret));
      }
    }
    my_skip_->reset(brs.size_);
  }
  return ret;
}

template<typename HashBucket>
int ObHashPartInfrastructureVec<HashBucket>::
probe_batch(uint64_t *hash_values_for_batch,
            const int64_t batch_size,
            const ObBitVector *skip,
            ObBitVector &my_skip)
{
  int ret = OB_SUCCESS;
  const ObHashPartItem *exists_item = nullptr;
  ObEvalCtx::BatchInfoScopeGuard batch_info_guard(*eval_ctx_);
  batch_info_guard.set_batch_idx(0);
  batch_info_guard.set_batch_size(batch_size);
  if (!is_push_down_ && OB_FAIL(prefetch<HashBucket>(hash_values_for_batch, batch_size, skip))) {
    SQL_ENG_LOG(WARN, "failed to prefetch", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < batch_size; ++i) {
    //if skip is nullptr, means rows from datum store which are bushy
    if (OB_NOT_NULL(skip) && skip->at(i)) {
      my_skip.set(i);
      continue;
    }
    batch_info_guard.set_batch_idx(i);
    if (OB_FAIL(hash_table_.get(preprocess_part_.store_.get_row_meta(),
                                i, hash_values_for_batch[i],
                                exists_item))) {
      SQL_ENG_LOG(WARN, "failed to get item", K(ret));
    } else if (OB_NOT_NULL(exists_item)) {
      my_skip.set(i);
    }
  }
  return ret;
}

//////////////////// end ObHashPartInfrastructureVec //////////////////
template<typename BktType>
int ObHashPartInfrastructureVecImpl::alloc_hp_infras_impl_instance(const int64_t tenant_id,
                                                                   ObIHashPartInfrastructure *&hp_infras)
{
  int ret = OB_SUCCESS;
  hp_infras = nullptr;
  void *buf = nullptr;
  bkt_size_ = sizeof(BktType);
  if (OB_ISNULL(buf
                = alloc_->alloc(sizeof(ObHashPartInfrastructureVec<BktType>)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SQL_ENG_LOG(WARN, "failed to create hash part infras instance", K(ret));
  } else {
    hp_infras = new (buf) ObHashPartInfrastructureVec<BktType>(tenant_id, mem_context_);
  }
  return ret;
}

template class ObHashPartInfrastructureVec<HPInfrasBktGeneral>;
template class ObHashPartInfrastructureVec<HPInfrasFixedBktByte48>;
template class ObHashPartInfrastructureVec<HPInfrasFixedBktByte56>;
template class ObHashPartInfrastructureVec<HPInfrasFixedBktByte64>;
///////////////////////////////////////////////////////////////////////////////////


}  // namespace sql
}  // namespace oceanbase

#endif /* OB_HASH_PARTITIONING_INFRASTRUCTURE_VEC_OP_H_ */
