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

#ifndef SQL_ENGINE_AGGREGATE_OB_HASH_STRUCT
#define SQL_ENGINE_AGGREGATE_OB_HASH_STRUCT
#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_hashutils.h"
#include "common/row/ob_row_store.h"
#include "lib/utility/utility.h"
#include "lib/ob_define.h"
#include "sql/engine/basic/ob_chunk_row_store.h"
#include "lib/container/ob_2d_array.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/engine/ob_sql_mem_mgr_processor.h"

namespace oceanbase
{
namespace common{
class ObNewRow;
class ObRowStore;
}

namespace sql
{


// Auto extended hash table, extend to double buckets size if hash table is quarter filled.
template <typename Item>
class ObExtendHashTable
{
public:
  const static int64_t INITIAL_SIZE = 128;
  const static int64_t SIZE_BUCKET_SCALE = 2;
  const static int64_t MAX_MEM_PERCENT = 40;

  struct Bucket
  {
    uint64_t hash_;
    Item *item_;

    // keep trivial constructor make ObSegmentArray use memset to construct arrays.
    Bucket() = default;
    TO_STRING_KV(K(hash_), KP(item_));
  };
  using BucketArray = common::ObSegmentArray<Bucket,
                                             OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                             common::ModulePageAllocator>;

  ObExtendHashTable()
    : initial_bucket_num_(0),
      size_(0),
      buckets_(NULL),
      allocator_("ExtendHTBucket"),
      probe_cnt_(0)
  {
  }
  ~ObExtendHashTable() { destroy(); }

  int init(ObIAllocator *allocator, lib::ObMemAttr &mem_attr,
           int64_t initial_size = INITIAL_SIZE);
  bool is_inited() const { return NULL != buckets_; }
  // return the first item which equal to, NULL for none exist.
  const Item *get(const Item &item);
  // Link item to hash table, extend buckets if needed.
  // (Do not check item is exist or not)
  int set(Item &item);
  int64_t size() const { return size_; }
  int64_t get_probe_cnt() const { return probe_cnt_; }
  void reuse()
  {
    int ret = common::OB_SUCCESS;
    if (nullptr != buckets_) {
      int64_t bucket_num = get_bucket_num();
      buckets_->reuse();
      if (OB_FAIL(buckets_->init(bucket_num))) {
        SQL_ENG_LOG(ERROR, "resize bucket array failed", K(size_), K(bucket_num), K(get_bucket_num()));
      }
    }
    size_ = 0;
    probe_cnt_ = 0;
  }

  int resize(ObIAllocator *allocator, int64_t bucket_num);

  void destroy()
  {
    if (NULL != buckets_) {
      buckets_->destroy();
      allocator_.free(buckets_);
      buckets_ = NULL;
    }
    allocator_.set_allocator(nullptr);
    size_ = 0;
    initial_bucket_num_ = 0;
  }
  int64_t mem_used() const
  {
    return NULL == buckets_ ? 0 : buckets_->mem_used();
  }

  inline int64_t get_bucket_num() const
  {
    return NULL == buckets_ ? 0 : buckets_->count();
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
      Item *item = buckets_->at(i).item_;
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
protected:
  // Locate the bucket with the same hash value, or empty bucket if not found.
  // The returned empty bucket is the insert position for the %hash_val
  OB_INLINE const Bucket &locate_bucket(const BucketArray &buckets,
                                        const uint64_t hash_val) const
  {
    const int64_t cnt = buckets.count();
    int64_t pos = hash_val & (cnt - 1);
    const Bucket *bucket = &buckets.at(pos);
    // The extend logical make sure the bucket never full, loop count will always less than %cnt
    while (hash_val != bucket->hash_ && NULL != bucket->item_) {
      bucket = &buckets.at((++pos) & (cnt - 1));
    }
    return *bucket;
  }

protected:
  DISALLOW_COPY_AND_ASSIGN(ObExtendHashTable);
  int extend();
protected:
  lib::ObMemAttr mem_attr_;
  int64_t initial_bucket_num_;
  int64_t size_;
  BucketArray *buckets_;
  common::ModulePageAllocator allocator_;
  int64_t probe_cnt_;
};

template <typename Item>
int ObExtendHashTable<Item>::init(
  ObIAllocator *allocator,
  lib::ObMemAttr &mem_attr,
  const int64_t initial_size /* INITIAL_SIZE */)
{
  int ret = common::OB_SUCCESS;
  if (initial_size < 2) {
    ret = common::OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(ret));
  } else {
    mem_attr_ = mem_attr;
    allocator_.set_allocator(allocator);
    allocator_.set_label(mem_attr.label_);
    void *buckets_buf = NULL;
    if (OB_ISNULL(buckets_buf = allocator_.alloc(sizeof(BucketArray), mem_attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "failed to allocate memory", K(ret));
    } else {
      buckets_ = new(buckets_buf)BucketArray(allocator_);
      initial_bucket_num_ = common::next_pow2(initial_size * SIZE_BUCKET_SCALE);
      SQL_ENG_LOG(DEBUG, "debug bucket num", K(ret), K(buckets_->count()), K(initial_bucket_num_));
      size_ = 0;
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(extend())) {
      SQL_ENG_LOG(WARN, "extend failed", K(ret));
    }
  }
  return ret;
}

template <typename Item>
int ObExtendHashTable<Item>::resize(ObIAllocator *allocator, int64_t bucket_num)
{
  int ret = OB_SUCCESS;
  if (bucket_num < get_bucket_num() / 2) {
    destroy();
    if (OB_FAIL(init(allocator, mem_attr_, bucket_num))) {
      SQL_ENG_LOG(WARN, "failed to reuse with bucket", K(bucket_num), K(ret));
    }
  } else {
    reuse();
  }
  return ret;
}

template <typename Item>
const Item *ObExtendHashTable<Item>::get(const Item &item)
{
  Item *res = NULL;
  ++probe_cnt_;
  if (NULL == buckets_) {
    // do nothing
  } else {
    common::hash::hash_func<Item> hf;
    common::hash::equal_to<Item> eqf;
    const uint64_t hash_val = hf(item);
    Item *it = locate_bucket(*buckets_, hash_val).item_;
    while (NULL != it) {
      if (eqf(*it, item)) {
        res = it;
        break;
      }
      it = it->next();
    }
  }
  return res;
}

template <typename Item>
int ObExtendHashTable<Item>::set(Item &item)
{
  common::hash::hash_func<Item> hf;
  int ret = common::OB_SUCCESS;
  if (size_ * SIZE_BUCKET_SCALE >= get_bucket_num()) {
    if (OB_FAIL(extend())) {
      SQL_ENG_LOG(WARN, "extend failed", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_ISNULL(buckets_)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_ENG_LOG(WARN, "invalid argument", K(ret), K(buckets_));
  } else {
    uint64_t hash_val = 0;
    if (OB_FAIL(hf(item, hash_val))) {
      SQL_ENG_LOG(WARN, "hash failed", K(ret));
    } else {
      Bucket *bucket = const_cast<Bucket *>(&locate_bucket(*buckets_, hash_val));
      if (NULL == bucket->item_) {
        bucket->hash_ = hash_val;
      } else {
        item.next() = bucket->item_;
      }
      bucket->item_ = &item;
      size_ += 1;
    }
  }
  return ret;
}

template <typename Item>
int ObExtendHashTable<Item>::extend()
{
  common::hash::hash_func<Item> hf;
  int ret = common::OB_SUCCESS;
  int64_t pre_bucket_num = get_bucket_num();
  int64_t new_bucket_num = 0 == pre_bucket_num ?
                          (0 == initial_bucket_num_ ? INITIAL_SIZE : initial_bucket_num_)
                          : pre_bucket_num * 2;
  SQL_ENG_LOG(DEBUG, "extend hash table", K(ret), K(new_bucket_num), K(initial_bucket_num_),
              K(pre_bucket_num));
  if (new_bucket_num <= pre_bucket_num) {
  } else {
    BucketArray *new_buckets = NULL;
    void *buckets_buf = NULL;
    if (OB_ISNULL(buckets_buf = allocator_.alloc(sizeof(BucketArray), mem_attr_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_ENG_LOG(WARN, "failed to allocate memory", K(ret));
    } else {
      new_buckets = new(buckets_buf)BucketArray(allocator_);
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_ISNULL(buckets_)) {
      ret = OB_INVALID_ARGUMENT;
      SQL_ENG_LOG(WARN, "invalid argument", K(ret), K(buckets_));
    } else if (OB_FAIL(new_buckets->init(new_bucket_num))) {
      SQL_ENG_LOG(WARN, "resize bucket array failed", K(ret), K(new_bucket_num));
    } else {
      const int64_t size = get_bucket_num();
      for (int64_t i = 0; i < size; i++) {
        const Bucket &old = buckets_->at(i);
        if (NULL != old.item_) {
          const_cast<Bucket &>(locate_bucket(*new_buckets, old.hash_)) = old;
        }
      }
      buckets_->destroy();
      allocator_.free(buckets_);

      buckets_ = new_buckets;
    }
    if (OB_FAIL(ret)) {
      if (buckets_ == new_buckets) {
        SQL_ENG_LOG(ERROR, "unexpected status: failed allocate new bucket", K(ret));
      } else if (nullptr != new_buckets) {
        new_buckets->destroy();
        allocator_.free(new_buckets);
        new_buckets = nullptr;
      }
    }
  }
  return ret;
}


//Used for calc hash for columns
class ObHashCols
{
public:
  ObHashCols()
    : row_(NULL),
      stored_row_(NULL),
      hash_col_idx_(NULL),
      next_(NULL),
      hash_val_(0)
  {
  }
  ObHashCols(const common::ObNewRow *row, const common::ObIArray<common::ObColumnInfo> *hash_col_idx)
    : row_(row),
      stored_row_(NULL),
      hash_col_idx_(hash_col_idx),
      next_(NULL),
      hash_val_(0)
  {
  }

  ~ObHashCols()
  {}

  int init(const common::ObNewRow *row,
      const common::ObIArray<common::ObColumnInfo> *hash_col_idx,
      const uint64_t hash_val = 0)
  {
    row_ = row;
    stored_row_ = NULL;
    hash_col_idx_ = hash_col_idx;
    hash_val_ = hash_val;
    return common::OB_SUCCESS;
  }

  uint64_t hash() const
  {
    if (hash_val_ == 0) {
      hash_val_ = inner_hash();
    }
    return hash_val_;
  }

  uint64_t inner_hash() const;

  bool operator ==(const ObHashCols &other) const;

  void set_stored_row(const common::ObRowStore::StoredRow *stored_row);

  ObHashCols *&next() { return *reinterpret_cast<ObHashCols **>(&next_); };

  TO_STRING_KV(K_(row), K_(stored_row), K_(hash_col_idx), K_(next), K_(hash_val));

public:
  const common::ObNewRow *row_;
  const common::ObRowStore::StoredRow *stored_row_;
  const common::ObIArray<common::ObColumnInfo> *hash_col_idx_;
  void *next_;
  mutable uint64_t hash_val_;
};

class ObGbyHashCols : public ObHashCols
{
public:
  using ObHashCols::ObHashCols;
  ObGbyHashCols *&next() { return *reinterpret_cast<ObGbyHashCols **>(&next_); };
public:
  int64_t group_id_ = 0;
};

//This class not inherit ObPhyOperatorCtx, as multi-inheritance
//Used for build hash group row.
template <typename Item>
class ObHashCtx
{
public:
  explicit ObHashCtx()
      : group_rows_(),
        started_(false),
        bkt_created_(false)
  {
  }
  virtual ~ObHashCtx()
  {
    group_rows_.destroy();
  }
  virtual void destroy()
  {
    group_rows_.~ObExtendHashTable();
  }
  int64_t get_used_mem_size() const
  {
    return group_rows_.mem_used();
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObHashCtx);
protected:
  ObExtendHashTable<Item> group_rows_;
  bool started_;
  bool bkt_created_;
};

//
// We use 8 bit for one element and 2 hash functions, false positive probability is:
// (1/8 + 1/8 - 1/8 * 1/8)^2 ~= 5.5%, good enough for us.
//
// To reuse the hash value (hash_val) of hash table && partition, we restrict bit count (bit_cnt)
// to power of 2, and construct bloom filter hashes (h1, h2) from hash_val:
//   h1: return hash_val directly, only the low log2(bit_cnt) will be used.
//   h2: bswap high 32 bit of hash_val, then right shift log2(bit_cnt) bits.
//       bswap is needed here because the low bit of hight 32 bit may be used for partition, they
//       are the same in one partition.
//
class ObGbyBloomFilter
{
public:
  explicit ObGbyBloomFilter(const ModulePageAllocator &alloc)
    : bits_(alloc), cnt_(0), h2_shift_(0)
  {
  }

  int init(const int64_t size, int64_t ratio = 8)
  {
    int ret = common::OB_SUCCESS;
    if (size <= 0 || ratio <= 0) {
      ret = common::OB_INVALID_ARGUMENT;
      SQL_ENG_LOG(WARN, "invalid bit cnt", K(ret), K(size), K(ratio));
    } else {
      cnt_ = next_pow2(ratio * size);
      if (OB_FAIL(bits_.reserve(cnt_))) {
        SQL_ENG_LOG(WARN, "bit set reserve failed", K(ret));
      } else {
        h2_shift_ = sizeof(cnt_) * CHAR_BIT - __builtin_clzl(cnt_);
      }
    }
    return ret;
  }
  void reuse()
  {
    bits_.reuse();
    cnt_ = 0;
    h2_shift_ = 0;
  }
  void reset()
  {
    bits_.reset();
    cnt_ = 0;
    h2_shift_ = 0;
  }
private:
  inline uint64_t h1(const uint64_t hash_val)
  {
    return hash_val;
  }

  inline uint64_t h2(const uint64_t hash_val)
  {
#ifdef OB_LITTLE_ENDIAN
    const static int64_t idx = 1;
#else
    const static int64_t idx = 0;
#endif
    uint64_t v = hash_val;
    reinterpret_cast<uint32_t *>(&v)[idx]
        = __builtin_bswap32(reinterpret_cast<const uint32_t *>(&hash_val)[idx]);
    return v >> h2_shift_;
  }

public:
  int set(const uint64_t hash_val)
  {
    int ret = common::OB_SUCCESS;
    if (0 == cnt_) {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "invalied cnt", K(ret), K(cnt_));
    } else if (OB_FAIL(bits_.add_member(h1(hash_val) & (cnt_ - 1)))
        || OB_FAIL(bits_.add_member(h2(hash_val) & (cnt_ - 1)))) {
      SQL_ENG_LOG(WARN, "bit set add member failed", K(ret), K(cnt_));
    }
    return ret;
  }

  bool exist(const uint64_t hash_val)
  {
    return bits_.has_member(h1(hash_val) & (cnt_ - 1))
        && bits_.has_member(h2(hash_val) & (cnt_ - 1));
  }

private:
  ObSegmentBitSet<common::OB_MALLOC_MIDDLE_BLOCK_SIZE> bits_;
  int64_t cnt_; // power of 2
  int64_t h2_shift_;
};

}//ns sql
}//ns oceanbase

#endif
