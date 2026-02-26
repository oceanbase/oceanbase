/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER
#include "ob_encoding_hash_util.h"
#include <cmath>
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{
using namespace common;

ObEncodingHashTable::ObEncodingHashTable() : is_created_(false), bucket_num_(0),
    node_num_(0), list_num_(0), node_cnt_(0), list_cnt_(0), buckets_(NULL), nodes_(NULL),
    lists_(NULL), alloc_(blocksstable::OB_ENCODING_LABEL_HASH_TABLE)
{
  MEMSET(&null_nodes_, 0, sizeof(null_nodes_));
  MEMSET(&nope_nodes_, 0, sizeof(nope_nodes_));
}

ObEncodingHashTable::~ObEncodingHashTable()
{
  reset();
}

int ObEncodingHashTable::create(const int64_t bucket_num, const int64_t node_num)
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
    // used to abort building ht, if not suitable for dict-based encoder
    list_num_ = lrint(static_cast<double>(node_num_) * DIST_NODE_THRESHOLD);

    const int64_t bucket_size = bucket_num_ * static_cast<int64_t>(sizeof(HashBucket));
    const int64_t nodes_size = node_num_ * static_cast<int64_t>(sizeof(HashNode));
    const int64_t lists_size = list_num_ * static_cast<int64_t>(sizeof(NodeList));

    if (OB_ISNULL(buckets_ = reinterpret_cast<HashBucket *>(alloc_.alloc(bucket_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for bucket", K(ret), K(bucket_size));
    } else if (OB_ISNULL(lists_ = reinterpret_cast<NodeList *>(alloc_.alloc(lists_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for lists", K(ret), K(lists_size));
    } else if (OB_ISNULL(nodes_ = reinterpret_cast<HashNode *>(alloc_.alloc(nodes_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for nodes", K(ret), K(nodes_size));
    } else {
      MEMSET(buckets_, 0, bucket_size);
      MEMSET(lists_, 0, lists_size);
      MEMSET(nodes_, 0, nodes_size);
      is_created_ = true;
    }
  }
  return ret;
}

void ObEncodingHashTable::reset()
{
  alloc_.reuse();
  bucket_num_ = 0;
  node_num_ = 0;
  node_cnt_ = 0;
  list_cnt_ = 0;
  buckets_ = NULL;
  nodes_ = NULL;
  lists_ = NULL;
  MEMSET(&null_nodes_, 0, sizeof(null_nodes_));
  MEMSET(&nope_nodes_, 0, sizeof(nope_nodes_));
  is_created_ = false;
}

void ObEncodingHashTable::reuse()
{
  MEMSET(buckets_, 0, bucket_num_ * sizeof(HashBucket));
  MEMSET(lists_, 0, list_cnt_ * sizeof(NodeList));
  // nodes no need to reuse
  MEMSET(&null_nodes_, 0, sizeof(null_nodes_));
  MEMSET(&nope_nodes_, 0, sizeof(nope_nodes_));
  node_cnt_ = 0;
  list_cnt_ = 0;
}

int ObEncodingHashTable::insert(const ObObj *cell, const int64_t row_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_created_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("hashtable is not inited", K(ret));
  } else if (OB_UNLIKELY(NULL == cell || 0 > row_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(cell), K(row_id));
  } else if (OB_UNLIKELY(node_cnt_ == node_num_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the count of nodes is more than expected", K(ret), K_(node_cnt));
  } else if (list_cnt_ == list_num_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the count of distinct node is more than threshold", K(ret), K_(list_cnt));
  } else {
    uint64_t hash_value = hashfunc_(cell);
    int64_t bucket_pos = hash_value % bucket_num_;
    HashBucket &bucket = buckets_[bucket_pos];
    NodeList *list = bucket;
    HashNode *node = &nodes_[node_cnt_];
    node->cell_ = cell;

    while (NULL != list) {
      if (equal_(list->header_->cell_, cell)) { // with the same cell value
        // add node
        node->dict_ref_ = list->insert_ref_;
        node->next_ = list->header_;
        list->header_ = node;
        ++list->size_;
        break;
      } else {
        list = list->next_;
      }
    }
    if (NULL == list) {
      // add list
      list = &lists_[list_cnt_];
      list->next_ = bucket;
      bucket = list;
      list->insert_ref_ = list_cnt_;
      ++list_cnt_;

      // add node
      node->dict_ref_ = list->insert_ref_;
      node->next_ = list->header_;
      list->header_ = node;
      ++list->size_;
    }

    ++node_cnt_;
  }
  return ret;
}

int ObEncodingHashTable::insert_null(const ObObj *cell, const int64_t row_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_created_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("hashtable is not inited", K(ret));
  } else if (OB_UNLIKELY(NULL == cell || 0 > row_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(cell), K(row_id));
  } else if (OB_UNLIKELY(node_cnt_ == node_num_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the count of nodes is more than expected", K(ret), K_(node_cnt));
  } else {
    HashNode *node = &nodes_[node_cnt_];
    // add first
    node->next_ = null_nodes_.header_;
    null_nodes_.header_ = node;
    ++null_nodes_.size_;
    ++node_cnt_;

  }
  return ret;
}

int ObEncodingHashTable::insert_nope(const ObObj *cell, const int64_t row_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_created_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("hashtable is not inited", K(ret));
  } else if (OB_UNLIKELY(NULL == cell || 0 > row_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(cell), K(row_id));
  } else if (OB_UNLIKELY(node_cnt_ == node_num_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the count of nodes is more than expected", K(ret), K_(node_cnt));
  } else {
    HashNode *node = &nodes_[node_cnt_];
    // add first
    node->next_ = nope_nodes_.header_;
    nope_nodes_.header_ = node;
    ++nope_nodes_.size_;
    ++node_cnt_;
  }
  return ret;
}

ObEncodingHashTableFactory::ObEncodingHashTableFactory()
  : allocator_(OB_MALLOC_NORMAL_BLOCK_SIZE, ObMalloc(blocksstable::OB_ENCODING_LABEL_HT_FACTORY)),
    hashtables_()
{
}

ObEncodingHashTableFactory::~ObEncodingHashTableFactory()
{
  clear();
}

int ObEncodingHashTableFactory::create(const int64_t bucket_num, const int64_t node_num,
                                       ObEncodingHashTable *&hashtable)
{
  int ret = OB_SUCCESS;
  hashtable = NULL;
  if (bucket_num <= 0 || node_num <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(bucket_num), K(node_num));
  } else if (hashtables_.count() > 0) {
    // we assume most time hashtables cached with same size, so only check one hashtable
    ObEncodingHashTable *cache_hashtable = hashtables_[hashtables_.count() - 1];
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

int ObEncodingHashTableFactory::recycle(ObEncodingHashTable *hashtable)
{
  int ret = OB_SUCCESS;
  if (NULL == hashtable) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(hashtable), K(ret));
  } else if (!hashtable->created()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hashtable not created", K(ret));
  } else {
    if (OB_FAIL(hashtables_.push_back(hashtable))) {
      LOG_WARN("push_back failed", K(ret));
      // free it
      allocator_.free(hashtable);
    }
  }
  return ret;
}

void ObEncodingHashTableFactory::clear()
{
  FOREACH(hashtable, hashtables_) {
    allocator_.free(*hashtable);
  }
  hashtables_.reuse();
}

int build_column_encoding_ctx(ObEncodingHashTable *ht,
                              const ObObjTypeStoreClass store_class,
                              const int64_t type_store_size,
                              ObColumnEncodingCtx &col_ctx)
{
  int ret = OB_SUCCESS;
  if (NULL == ht) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ht is null", K(ret));
  } else {
    col_ctx.null_cnt_ = ht->get_null_list().size_;
    col_ctx.nope_cnt_ = ht->get_nope_list().size_;
    col_ctx.ht_ = ht;

    switch (store_class) {
      case ObIntSC:
      case ObUIntSC: {
        const uint64_t integer_mask = INTEGER_MASK_TABLE[type_store_size];
        FOREACH(l, *ht) {
          const uint64_t v = l->header_->cell_->v_.uint64_ & integer_mask;
          if (v > col_ctx.max_integer_) {
            col_ctx.max_integer_ = v;
          }
        }
        break;
      }
      case ObNumberSC: {
        col_ctx.fix_data_size_ = -1;
        bool var_store = false;
        FOREACH(l, *ht) {
          const ObObj &cell = *l->header_->cell_;
          const int64_t len = sizeof(cell.nmb_desc_) +
              cell.nmb_desc_.len_ * sizeof(cell.v_.nmb_digits_[0]);
          col_ctx.var_data_size_ += len * l->size_;
          col_ctx.dict_var_data_size_ += len;
          if (!var_store) {
            if (col_ctx.fix_data_size_ < 0) {
              col_ctx.fix_data_size_ = len;
            } else if (len != col_ctx.fix_data_size_) {
              col_ctx.fix_data_size_ = -1;
              var_store = true;
            }
          }
        }
        break;
      }
      case ObStringSC: {
        col_ctx.fix_data_size_ = -1;
        col_ctx.max_string_size_ = -1;
        bool var_store = false;
        FOREACH(l, *ht) {
          const ObObj &cell = *l->header_->cell_;
          const int64_t len = cell.val_len_;
          col_ctx.max_string_size_ = len > col_ctx.max_string_size_ ? len : col_ctx.max_string_size_;
          col_ctx.var_data_size_ += len * l->size_;
          col_ctx.dict_var_data_size_ += len;
          if (!var_store) {
            if (col_ctx.fix_data_size_ < 0) {
              col_ctx.fix_data_size_ = len;
            } else if (len != col_ctx.fix_data_size_) {
              col_ctx.fix_data_size_ = -1;
              var_store = true;
            }
          }
        }
        break;
      }
      case ObTextSC:
      case ObJsonSC:
      case ObGeometrySC: { // geometry, json and text storage class have the same behavior currently
        col_ctx.fix_data_size_ = -1;
        col_ctx.max_string_size_ = -1;
        bool var_store = false;
        FOREACH(l, *ht) {
          const ObObj &cell = *l->header_->cell_;
          const int64_t len = cell.val_len_;
          col_ctx.max_string_size_ = len > col_ctx.max_string_size_ ? len : col_ctx.max_string_size_;
          col_ctx.var_data_size_ += len * l->size_;
          col_ctx.dict_var_data_size_ += len;
          if (!var_store) {
            if (col_ctx.fix_data_size_ < 0) {
              col_ctx.fix_data_size_ = len;
            } else if (len != col_ctx.fix_data_size_) {
              col_ctx.fix_data_size_ = -1;
              var_store = true;
            }
          }
        }
        break;
      }

      case ObOTimestampSC: {
        col_ctx.fix_data_size_ = -1;
        bool var_store = false;
        FOREACH(l, *ht) {
          const ObObj &cell = *l->header_->cell_;
          const int64_t len = cell.get_otimestamp_store_size();
          col_ctx.var_data_size_ += len * l->size_;
          col_ctx.dict_var_data_size_ += len;
          if (!var_store) {
            if (col_ctx.fix_data_size_ < 0) {
              col_ctx.fix_data_size_ = len;
            } else if (len != col_ctx.fix_data_size_) {
              col_ctx.fix_data_size_ = -1;
              var_store = true;
            }
          }
        }
        break;
      }

      case ObIntervalSC: {
        col_ctx.fix_data_size_ = -1;
        bool var_store = false;
        FOREACH(l, *ht) {
          const ObObj &cell = *l->header_->cell_;
          const int64_t len = cell.get_interval_store_size();
          col_ctx.var_data_size_ += len * l->size_;
          col_ctx.dict_var_data_size_ += len;
          if (!var_store) {
            if (col_ctx.fix_data_size_ < 0) {
              col_ctx.fix_data_size_ = len;
            } else if (len != col_ctx.fix_data_size_) {
              col_ctx.fix_data_size_ = -1;
              var_store = true;
            }
          }
        }
        break;
      }

      default:
        ret = OB_INNER_STAT_ERROR;
        LOG_WARN("not supported store class", K(ret), K(store_class));
    }
  }
  return ret;
}

} // table_load_backup
} // namespace observer
} // namespace oceanbase
