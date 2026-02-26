/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_dict_encoding_hash_table.h"
#include <cmath>
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;

bool ObDictNodeCmp::operator()(const ObDictHashNode &lhs, const ObDictHashNode &rhs)
{
  bool res = false;
  int &ret = ret_;
  int cmp_ret = 0;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(cmp_func_.cmp_func_(lhs.datum_, rhs.datum_, cmp_ret))) {
    LOG_WARN("failed to compare datums", K(ret), K(lhs.datum_), K(rhs.datum_));
  } else {
    res = cmp_ret < 0;
  }
  return res;
}

ObDictEncodingHashTable::ObDictEncodingHashTable()
    : is_created_(false),
      is_sorted_(false),
      bucket_num_(0),
      node_num_(0),
      distinct_node_cnt_(0),
      buckets_(NULL),
      nodes_(NULL),
      row_refs_(nullptr),
      null_node_(),
      nop_node_(),
      alloc_(blocksstable::OB_ENCODING_LABEL_HASH_TABLE,
             OB_MALLOC_NORMAL_BLOCK_SIZE,
             MTL_ID())
{}

ObDictEncodingHashTable::~ObDictEncodingHashTable()
{
  reset();
}

int ObDictEncodingHashTable::create(const int64_t bucket_num, const int64_t node_num)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_created_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already created", K(ret));
  } else if (OB_UNLIKELY(0 >= bucket_num || 0 >= node_num)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(bucket_num), K(node_num));
  } else if (0 != (bucket_num  & (bucket_num - 1))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("bucket number must be power of 2", K(ret), K(bucket_num));
  } else {
    bucket_num_ = bucket_num;
    // if node_num only increase little, can still reuse hashtable
    node_num_ = max(bucket_num, node_num);

    const int64_t bucket_size = bucket_num_ * static_cast<int64_t>(sizeof(HashBucket));
    const int64_t nodes_size = node_num_ * static_cast<int64_t>(sizeof(HashNode));
    const int64_t refs_size = node_num_ * sizeof(*row_refs_);

    if (OB_ISNULL(buckets_ = reinterpret_cast<HashBucket *>(alloc_.alloc(bucket_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for bucket", K(ret), K(bucket_size));
    } else if (OB_ISNULL(nodes_ = reinterpret_cast<HashNode *>(alloc_.alloc(nodes_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for nodes", K(ret), K(nodes_size));
    } else if (OB_ISNULL(row_refs_ = reinterpret_cast<int32_t *>(alloc_.alloc(refs_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for row refs", K(ret), K(refs_size));
    } else {
      MEMSET(buckets_, 0, bucket_size);
      // nodes_ no need to memset;
      null_node_.reset();
      nop_node_.reset();
      is_created_ = true;
    }
  }
  return ret;
}

void ObDictEncodingHashTable::reset()
{
  alloc_.reuse();
  bucket_num_ = 0;
  node_num_ = 0;
  distinct_node_cnt_ = 0;
  buckets_ = NULL;
  nodes_ = NULL;
  row_refs_ = nullptr;
  null_node_.reset();
  nop_node_.reset();
  is_created_ = false;
  is_sorted_ = false;
}

void ObDictEncodingHashTable::reuse()
{
  MEMSET(buckets_, 0, bucket_num_ * sizeof(HashBucket));
  // nodes_ no need to reset
  // row_refs_ no need to reset
  null_node_.reset();
  nop_node_.reset();
  distinct_node_cnt_ = 0;
  is_sorted_ = false;
}

int ObDictEncodingHashTable::sort_dict(ObCmpFunc &cmp_func)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_created_)) {
    ret = common::OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_UNLIKELY(is_sorted_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dict has been sorted", K(ret), K_(row_count), K_(distinct_node_cnt));
  } else {
    lib::ob_sort(begin(), end(), ObDictNodeCmp(ret, cmp_func));
  }

  if (OB_SUCC(ret)) {
    // refs_permutation use the memory of buckets_ which is safe because the buckets_ will be not used when sort
    int32_t *refs_permutation = (int32_t *)buckets_;
    for (int64_t i = 0; i < distinct_node_cnt_; i++) {
      refs_permutation[nodes_[i].dict_ref_] = i;
    }
    // calc new dict_ref if dict is sorted
    if (null_node_.duplicate_cnt_ + nop_node_.duplicate_cnt_ > 0) { // has null
      for(int64_t i = 0; i < row_count_; i++) {
        if (row_refs_[i] < distinct_node_cnt_) {
          row_refs_[i] = refs_permutation[row_refs_[i]];
        }
      }
    } else {
      for(int64_t i = 0; i < row_count_; i++) {
        row_refs_[i] = refs_permutation[row_refs_[i]];
      }
    }
    is_sorted_ = true;
  }
  return ret;
}

int ObDictEncodingHashTableBuilder::build(const ObColDatums &col_datums, const ObColDesc &col_desc)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_created_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_UNLIKELY(col_datums.empty() || node_num_ < col_datums.count())) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K_(node_num), "row_count", col_datums.count());
  } else {
    row_count_ = col_datums.count();
    const uint64_t mask = (bucket_num_ - 1);
    ObObjTypeStoreClass store_class = get_store_class_map()[col_desc.col_type_.get_type_class()];
    const bool is_integer = store_class == ObIntSC || store_class == ObUIntSC;
    if (is_integer) {
      for (int64_t row_idx = 0; row_idx < row_count_; ++row_idx) {
        const ObDatum &datum = col_datums.at(row_idx);
        if (datum.is_null()) {
          null_node_.duplicate_cnt_++;
          row_refs_[row_idx] = NULL_REF;
        } else if (datum.is_nop()) {
          nop_node_.duplicate_cnt_++;
          // nop is considered as null in dict encoding
          // use nop bitmap to distinguish nop and null if needed
          row_refs_[row_idx] = NULL_REF;
        } else {
          uint64_t pos = (datum.get_uint64() * 0x5bd1e995ULL) & mask; // simple hash
          HashNode *node = buckets_[pos];
          while (nullptr != node) {
            if (node->datum_.get_uint64() == datum.get_uint64()) { // is_equal
              node->duplicate_cnt_++;
              row_refs_[row_idx] = node->dict_ref_;
              break;
            } else {
              node = node->next_;
            }
          }
          if (nullptr == node) {
            node = &nodes_[distinct_node_cnt_];
            node->init(datum, distinct_node_cnt_, buckets_[pos]);
            distinct_node_cnt_++;
            buckets_[pos] = node;
            row_refs_[row_idx] = node->dict_ref_;
          }
        }
      }
    } else {
      for (int64_t row_idx = 0; row_idx < row_count_; ++row_idx) {
        const ObDatum &datum = col_datums.at(row_idx);
        if (datum.is_null()) {
          null_node_.duplicate_cnt_++;
          row_refs_[row_idx] = NULL_REF;
        } else if (datum.is_nop()) {
          nop_node_.duplicate_cnt_++;
          // nop is considered as null in dict encoding
          // use nop bitmap to distinguish nop and null if needed
          row_refs_[row_idx] = NULL_REF;
        } else {
          // add to table
          uint64_t pos = ::murmurhash2(datum.ptr_, datum.len_, 0/*seed*/);
          pos = pos & mask;
          HashNode *node = buckets_[pos];
          bool is_equal = false;
          while (nullptr != node) {
            // binary equal for store types need char case or precision handling
            if (node->datum_.pack_ != datum.pack_) {
              is_equal = false;
            } else {
              is_equal = (0 == MEMCMP(node->datum_.ptr_, datum.ptr_, datum.len_));
            }
            if (is_equal) {
              node->duplicate_cnt_++;
              row_refs_[row_idx] = node->dict_ref_;
              break;
            } else {
              node = node->next_;
            }
          }
          if (nullptr == node) {
            node = &nodes_[distinct_node_cnt_];
            node->init(datum, distinct_node_cnt_, buckets_[pos]);
            distinct_node_cnt_++;
            buckets_[pos] = node;
            row_refs_[row_idx] = node->dict_ref_;
          }
        }
      }
    }

    if (OB_SUCC(ret) && (null_node_.duplicate_cnt_ + nop_node_.duplicate_cnt_ > 0)) {
      null_node_.datum_.set_null();
      null_node_.dict_ref_ = distinct_node_cnt_;
      for (int64_t i = 0; i < row_count_; i++) {
        if (row_refs_[i] == NULL_REF) {
          row_refs_[i] = distinct_node_cnt_; // use distinct_node_cnt_ as null replaced ref
        }
      }
    }
  }
  return ret;
}

ObDictEncodingHashTableFactory::ObDictEncodingHashTableFactory()
  : allocator_(OB_MALLOC_NORMAL_BLOCK_SIZE, ObMalloc(blocksstable::OB_ENCODING_LABEL_HT_FACTORY)),
    hashtables_()
{
  lib::ObMemAttr attr(MTL_ID(), blocksstable::OB_ENCODING_LABEL_HT_FACTORY);
  allocator_.set_attr(attr);
  hashtables_.set_attr(attr);
}

ObDictEncodingHashTableFactory::~ObDictEncodingHashTableFactory()
{
  clear();
}

int ObDictEncodingHashTableFactory::create(const int64_t bucket_num,
                                           const int64_t node_num,
                                           ObDictEncodingHashTable *&hashtable)
{
  int ret = OB_SUCCESS;
  hashtable = NULL;
  if (bucket_num <= 0 || node_num <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(bucket_num), K(node_num));
  } else if (hashtables_.count() > 0) {
    // we assume most time hashtables cached with same size, so only check one hashtable
    ObDictEncodingHashTable *cache_hashtable = hashtables_[hashtables_.count() - 1];
    if (NULL == cache_hashtable) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cache_hashtable is null", K(ret));
    } else if (!cache_hashtable->created()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("all hashtable assume already created", K(ret));
    } else if (cache_hashtable->get_bucket_num() >= bucket_num
        && cache_hashtable->get_node_num() >= node_num) {
      cache_hashtable->reuse();
      hashtable = cache_hashtable;
      hashtables_.pop_back();
    } else {
      // clear all cached hashtable
      clear();
    }
  }

  if (OB_SUCC(ret) && NULL == hashtable) {
    if (NULL == (hashtable = allocator_.alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc failed", K(ret));
    } else if (OB_FAIL(hashtable->create(bucket_num, node_num))) {
      LOG_WARN("hashtable create failed", K(ret), K(bucket_num), K(node_num));
      // free it directly
      allocator_.free(hashtable);
    }
  }
  return ret;
}

int ObDictEncodingHashTableFactory::recycle(const bool force_cache, ObDictEncodingHashTable *hashtable)
{
  int ret = OB_SUCCESS;
  if (NULL == hashtable) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(hashtable), K(ret));
  } else if (!hashtable->created()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hashtable not created", K(ret));
  } else if (!force_cache && hashtable->distinct_node_cnt() >= MAX_CACHED_HASHTABLE_SIZE) {
    allocator_.free(hashtable);
  } else {
    if (OB_FAIL(hashtables_.push_back(hashtable))) {
      LOG_WARN("push_back failed", K(ret));
      // free it
      allocator_.free(hashtable);
    }
  }
  return ret;
}

void ObDictEncodingHashTableFactory::clear()
{
  FOREACH(hashtable, hashtables_) {
    allocator_.free(*hashtable);
  }
  hashtables_.reuse();
}

} // end namespace blocksstable
} // end namespace oceanbase
