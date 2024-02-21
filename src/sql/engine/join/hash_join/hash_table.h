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

#ifndef SRC_SQL_ENGINE_JOIN_HASH_JOIN_HASH_TABLE_H_
#define SRC_SQL_ENGINE_JOIN_HASH_JOIN_HASH_TABLE_H_

#include "sql/engine/join/hash_join/ob_hash_join_struct.h"
#include "lib/atomic/ob_atomic.h"

namespace oceanbase
{
namespace sql
{
//template<typename T>
//struct DirectItem {
//  static const bool split_null = true;
//  using CType = ObjTypeTrait<T>::c_type;
//  void set_matched() { /*do nothing;*/ }
//  bool is_matched() { /**/ }
//  bool used() { return NULL == next_; };
//  bool set_used(bool v);
//  void set_next(DirectItem *item);
//  DirectItem *get_next();
//  DirectItem *construct(key_project, left_stored_rows[i]) {};
//private:
//  union {
//    T hash_value_;
//    T key_;
//  }
//  uint64_t row_ptr_;
//  DirectItem *next_;
//};
//
//template<typename T>
//struct DirectFlagItem : public DirectItem {
//
//public:
//  uint8_t flag_;
//};

//template<typename T>
//struct DirectBucket : {
//  using Item = DirectItem<T>;
//public:
//  T hash_value();
//  Item *get_item();
//public:
//  Item item_;
//};

struct Int64Key {
  inline void init_data(const ObFixedArray<int64_t, common::ObIAllocator> *key_proj,
                 const RowMeta &row_meta,
                 const ObHJStoredRow *row) {
    OB_ASSERT(1 == key_proj->count());
    data_ = *(reinterpret_cast<const int64_t *>(row->get_cell_payload(row_meta, key_proj->at(0))));
  }
  inline bool operator==(const Int64Key &other) const {
    return data_ == other.data_;
  }
  TO_STRING_KV(K_(data));

public:
  int64_t data_;
}__attribute__ ((packed));

struct Int128Key {
  inline void init_data(const ObFixedArray<int64_t, common::ObIAllocator> *key_proj,
                 const RowMeta &row_meta,
                 const ObHJStoredRow *row) {
    OB_ASSERT(2 == key_proj->count());
    data_0_ = *(reinterpret_cast<const int64_t *>(row->get_cell_payload(row_meta, key_proj->at(0))));
    data_1_ = *(reinterpret_cast<const int64_t *>(row->get_cell_payload(row_meta, key_proj->at(1))));
  }
  inline bool operator==(const Int128Key &other) const {
    return data_0_ == other.data_0_ && data_1_ == data_1_;
  }
  TO_STRING_KV(K_(data_0), K_(data_1));

public:
  int64_t data_0_;
  int64_t data_1_;
}__attribute__ ((packed));

template<typename T>
struct NormalizedItem {
  using KeyType = T;
  inline void set_is_match(const RowMeta &row_meta, bool is_match) {
    UNUSED(row_meta);
    is_match_ = is_match;
  }
  inline bool is_match(const RowMeta &row_meta) const { return is_match_; }
  inline void set_next(const RowMeta &row_meta, NormalizedItem<T> *item) {
    UNUSED(row_meta);
    next_ = item;
  }
  inline NormalizedItem<T> *get_next(const RowMeta &row_meta) const {
    UNUSED(row_meta);
    return next_;
  }
  inline void init(JoinTableCtx &ctx,
                   const RowMeta &row_meta,
                   const ObHJStoredRow *row,
                   void *next_item) {
    key_.init_data(ctx.build_key_proj_, ctx.build_row_meta_, row);
    row_ptr_ = reinterpret_cast<uint64_t>(row);
    next_item_ptr_ = reinterpret_cast<uint64_t>(next_item);
  };
  // atomic init item, and set next ptr is END_ITEM
  inline void atomic_init(JoinTableCtx &ctx,
                          const RowMeta &row_meta,
                          const ObHJStoredRow *row) {
    key_.init_data(ctx.build_key_proj_, ctx.build_row_meta_, row);
    row_ptr_ = reinterpret_cast<uint64_t>(row);
    ATOMIC_STORE(&next_item_ptr_, END_ITEM);
  };
  inline ObHJStoredRow *get_stored_row() { return reinterpret_cast<ObHJStoredRow *>(row_ptr_); }

  TO_STRING_KV(K_(row_ptr), K_(is_match), KP_(next), K(key_));

public:
  union {
    T key_;
    int64_t key_data_;
  };
  union {
    struct {
      uint64_t row_ptr_ : 63;
      bool is_match_ : 1;
    };
    uint64_t v_;
  };
  union {
    NormalizedItem<T> *next_;
    uint64_t next_item_ptr_;
  };
};

template<typename T>
struct NormalizedBucket {
  using Item = NormalizedItem<T>;
  uint64_t hash_value() const { return hash_value_; }
  Item *get_item() { return &item_; }
  const Item *get_item() const { return &item_; }
  void set_item(Item *item) {
    item_ = *item;
  };
  void set_used(bool v) { used_ = v; };
  bool used() const { return used_; }

  TO_STRING_KV(K_(hash_value), K_(used), K_(item));
public:
  union {
    struct {
      uint64_t hash_value_:63;
      uint64_t used_:1;
    };
    uint64_t val_;
  };
  Item item_;
};

struct GenericItem: public ObHJStoredRow {
  static const bool split_null = false;
  void set_next(const RowMeta &row_meta, GenericItem *item) {
    ObHJStoredRow::set_next(row_meta, item);
  }
  GenericItem *get_next(const RowMeta &row_meta) {
    return static_cast<GenericItem *>(ObHJStoredRow::get_next(row_meta));
  }
  ObHJStoredRow *get_stored_row() { return reinterpret_cast<ObHJStoredRow *>(this); }
  inline void init(JoinTableCtx &ctx,
                   const RowMeta &row_meta,
                   const ObHJStoredRow *row,
                   void *next_item) {
    UNUSED(row_meta);
    UNUSED(ctx);
    UNUSED(row);
    set_next(row_meta, reinterpret_cast<GenericItem *>(next_item));
  };
}__attribute__ ((packed));

struct GenericBucket {
  using Item = GenericItem;
  GenericBucket() : val_(0), item_(NULL) {}

  // this interface is unused in Generic Mode
  uint64_t hash_value() const { return hash_value_; }
  const Item *get_item() const { return item_; }
  Item *&get_item() { return item_; }
  uint64_t &get_item_ptr() { return item_ptr_; }
  void set_item(Item *item) { item_ = item; }
  void set_used(bool v) { used_ = v; }
  bool used() const { return used_; }
  TO_STRING_KV(K_(hash_value), K_(used), KP_(item));
public:
  union {
    struct {
      uint64_t hash_value_:63;
      uint64_t used_:1;
    };
    uint64_t val_;
  };
  union {
    uint64_t item_ptr_;
    Item *item_;
  };
};

template<typename Item>
struct ProberBase {
  virtual ~ProberBase<Item>() {};
  int calc_join_conditions(JoinTableCtx &ctx,
                           ObHJStoredRow *left_row,
                           const int64_t batch_idx,
                           bool &matched);
  virtual int equal(JoinTableCtx &ctx,
                    Item *item,
                    const int64_t batch_idx,
                    bool &is_equal) = 0;
private:
  int convert_exprs_batch_one(const ExprFixedArray &exprs,
                              ObEvalCtx &eval_ctx,
                              const RowMeta &row_meta,
                              const ObHJStoredRow *row,
                              const int64_t batch_idx);
};

//template<typename Item>
//struct DirectProber: public ProberBase<Item> {
//  int equal(Item *item,  JoinTableCtx &ctx, const int64_t batch_idx, bool &is_equal) {
//    return item->key_ == *(reinterpret_cast<Item::CType *>(ctx.get_probe_key(batch_idx)));
//  }
//}

template<typename T>
struct NormalizedProber final: public ProberBase<NormalizedItem<T>> {
  using Item = NormalizedItem<T>;
  NormalizedProber<T>()  {}
  ~NormalizedProber() {}
  static int64_t get_normalized_key_size() {
    return sizeof(T);
  }
  int equal(JoinTableCtx &ctx, Item *item, const int64_t batch_idx, bool &is_equal) override {
    char *key_data = ctx.probe_batch_rows_->key_data_;
    is_equal = (item->key_ == *(reinterpret_cast<T *>(key_data + batch_idx * sizeof(T))));
    LOG_DEBUG("is equal", K(batch_idx), K(*item), K(item->key_),
              K(*(reinterpret_cast<T *>(key_data + batch_idx * sizeof(T)))));
    return OB_SUCCESS;
  }
};

struct GenericProber final: public ProberBase<GenericItem> {
  static int64_t get_normalized_key_size() {
    return 0;
  }
  int equal(JoinTableCtx &ctx, GenericItem *item,  const int64_t batch_idx, bool &is_equal) override;
};

struct IHashTable {

  virtual int init(ObIAllocator &alloc, const int64_t max_batch_size) = 0;
  virtual int build_prepare(int64_t row_count, int64_t bucket_count) = 0;
  virtual int insert_batch(JoinTableCtx &ctx,
                           ObHJStoredRow **stored_rows,
                           const int64_t size,
                           int64_t &used_buckets,
                           int64_t &collisions) = 0;
  virtual int probe_prepare(JoinTableCtx &ctx, OutputInfo &output_info) = 0;
  virtual int probe_batch(JoinTableCtx &ctx, OutputInfo &output_info) = 0;
  virtual int project_matched_rows(JoinTableCtx &ctx, OutputInfo &output_info) = 0;
  virtual int get_unmatched_rows(JoinTableCtx &ctx, OutputInfo &output_info) = 0;
  virtual void reset() = 0;
  virtual void free(ObIAllocator *alloc) = 0;

  virtual int64_t get_row_count() const = 0;
  virtual int64_t get_used_buckets() const = 0;
  virtual int64_t get_nbuckets() const = 0;
  virtual int64_t get_collisions() const = 0;
  virtual int64_t get_mem_used() const = 0;
  virtual int64_t get_one_bucket_size() const = 0;
  virtual int64_t get_normalized_key_size() const = 0;
  virtual void set_diag_info(int64_t used_buckets, int64_t collisions) = 0;
};

// Open addressing hash table implement:
//
//   buckets:
//   .......
//   +----------------------------+
//   | 0          | NULL          |
//   +----------------------------+        +----------+       +----------+
//   | hash_value | Item          |------->| Item     |------>| Item     |
//   +----------------------------+        +----------+       +----------+
//   | 0          | NULL          |
//   +----------------------------+
//   ......
//
// Buckets is array of <hash_value, store_row_ptr> pair, store rows linked in one bucket are
// the same hash value.
template <typename Bucket, typename Prober>
struct HashTable : public IHashTable
{
  using Item = typename Bucket::Item;

  HashTable()
      : buckets_(nullptr),
        nbuckets_(0),
        bit_cnt_(0),
        row_count_(0),
        collisions_(0),
        used_buckets_(0),
        inited_(false),
        ht_alloc_(nullptr),
        magic_(0),
        is_shared_(false),
        items_(NULL),
        item_pos_(0)
  {
  }
  int init(ObIAllocator &alloc, const int64_t max_batch_size) override;
  int build_prepare(int64_t row_count, int64_t bucket_count) override;
  virtual int insert_batch(JoinTableCtx &ctx,
                           ObHJStoredRow **stored_rows,
                           const int64_t size,
                           int64_t &used_buckets,
                           int64_t &collisions) override;
  int probe_prepare(JoinTableCtx &ctx, OutputInfo &output_info) override;
  int probe_batch(JoinTableCtx &ctx, OutputInfo &output_info) override {
    return ctx.probe_opt_  ? probe_batch_opt(ctx, output_info)
           : !ctx.need_probe_del_match() ? probe_batch_normal(ctx, output_info)
                                         : probe_batch_del_match(ctx, output_info);
  }
  int get_unmatched_rows(JoinTableCtx &ctx, OutputInfo &output_info) override;
  int project_matched_rows(JoinTableCtx &ctx, OutputInfo &output_info) override;
  void reset() override;
  void free(ObIAllocator *alloc);

  int64_t get_row_count() const override { return row_count_; };
  int64_t get_used_buckets() const override { return used_buckets_; }
  int64_t get_nbuckets() const override { return nbuckets_; }
  int64_t get_collisions() const override { return collisions_; }
  int64_t get_mem_used() const override {
    int64_t size = 0;
    size = sizeof(*this);
    if (NULL != buckets_) {
      size += buckets_->mem_used();
    }
    if (NULL != items_) {
      size += items_->mem_used();
    }
    return size;
  }
  int64_t get_one_bucket_size() const { return sizeof(Bucket); };
  int64_t get_normalized_key_size() const {
    return Prober::get_normalized_key_size();
  }
  virtual void set_diag_info(int64_t used_buckets, int64_t collisions) override {
    used_buckets_ += used_buckets;
    collisions_ += collisions;
  }
  using BucketArray =
    common::ObSegmentArray<Bucket, OB_MALLOC_MIDDLE_BLOCK_SIZE, common::ModulePageAllocator>;
  using ItemArray =
    common::ObSegmentArray<Item, OB_MALLOC_MIDDLE_BLOCK_SIZE, common::ModulePageAllocator>;
protected:
  Item *atomic_new_item() {
    int64_t idx = __sync_fetch_and_add(&item_pos_, 1);
    return &items_->at(idx);
  }
private:
  int init_probe_key_data(JoinTableCtx &ctx, OutputInfo &output_info);
  Item *new_item() { return &items_->at(item_pos_++); }
  int probe_batch_opt(JoinTableCtx &ctx, OutputInfo &output_info);
  int probe_batch_normal(JoinTableCtx &ctx, OutputInfo &output_info);
  int probe_batch_del_match(JoinTableCtx &ctx, OutputInfo &output_info);
  // Get stored row list which has the same hash value.
  // return NULL if not found.
  inline Item *get(const uint64_t hash_val);
  void get(uint64_t hash_val, Bucket *&bkt);
  // performance critical, do not double check the parameters
  void set(JoinTableCtx &ctx,
           const uint64_t hash_val,
           ObHJStoredRow *row,
           int64_t &used_buckets,
           int64_t &collisions);

  // mark delete, can not add row again after delete
  void del(const uint64_t hash_val, const RowMeta &row_meta, Item *item);
protected:
  static const int64_t MAGIC_CODE = 0x123654abcd134;
  BucketArray *buckets_;
  int64_t nbuckets_;
  int64_t bit_cnt_;
  int64_t row_count_;
  int64_t collisions_;
  int64_t used_buckets_;
  bool inited_;
  ModulePageAllocator *ht_alloc_;
  int64_t magic_;
  bool is_shared_;
  //TODO shengle support add null key rows for normalized mode
  //NullArray *null_key_rows_;
  // used for Normalized mode, record all next item
  ItemArray *items_;
  int64_t item_pos_;
  Prober prober_;
};

struct GenericSharedHashTable final : public HashTable<GenericBucket, GenericProber>
{
public:
  inline int insert_batch(JoinTableCtx &ctx,
                          ObHJStoredRow **stored_rows,
                          const int64_t size,
                          int64_t &used_buckets,
                          int64_t &collisions) override;
  inline virtual void set_diag_info(int64_t used_buckets, int64_t collisions) override {
    ATOMIC_AAF(&used_buckets_, used_buckets);
    ATOMIC_AAF(&collisions_, collisions);
  }
private:
  inline int atomic_set(const uint64_t hash_val,
                        const RowMeta &row_meta,
                        GenericItem *sr,
                        int64_t &used_buckets,
                        int64_t &collisions);
};

template <typename Bucket, typename Prober>
struct NormalizedSharedHashTable final : public HashTable<Bucket, Prober>
{
  using Item = typename Bucket::Item;
public:
  inline int insert_batch(JoinTableCtx &ctx,
                          ObHJStoredRow **stored_rows,
                          const int64_t size,
                          int64_t &used_buckets,
                          int64_t &collisions) override;
  inline void set_diag_info(int64_t used_buckets, int64_t collisions) override {
    ATOMIC_AAF(&this->used_buckets_, used_buckets);
    ATOMIC_AAF(&this->collisions_, collisions);
  }
private:
  inline int atomic_set(JoinTableCtx &ctx, const uint64_t hash_val,
                        ObHJStoredRow *sr, int64_t &used_buckets, int64_t &collisions);
};

//using DirectInt8Table = HashTable<DirectBucket<int8_t>, NormalizedProber<int8_t>>;
//using DirectInt16Table = HashTable<DirectBucket<int16_t>, NormalizedProber<int16_t>>;
//using NormalizedInt32Table = HashTable<NormalizedBucket<int32_t>, NormalizedProber<int32_t>>;
using NormalizedInt64Table = HashTable<NormalizedBucket<Int64Key>, NormalizedProber<Int64Key>>;
using NormalizedInt128Table = HashTable<NormalizedBucket<Int128Key>, NormalizedProber<Int128Key>>;
//using NormalizedFloatTable = HashTable<NormalizedBucket<float>, NormalizedProber<float>>;
//using NormalizedDoubleTable =  HashTable<NormalizedBucket<double>, NormalizedProber<double>>;
using GenericTable = HashTable<GenericBucket, GenericProber>;
using NormalizedSharedInt64Table = NormalizedSharedHashTable<NormalizedBucket<Int64Key>, NormalizedProber<Int64Key>>;
using NormalizedSharedInt128Table = NormalizedSharedHashTable<NormalizedBucket<Int128Key>, NormalizedProber<Int128Key>>;

} // end namespace sql
} // end namespace oceanbase

#include "sql/engine/join/hash_join/hash_table.ipp"

#endif /* SRC_SQL_ENGINE_JOIN_HASH_JOIN_HASH_TABLE_H_*/
