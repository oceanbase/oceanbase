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

#define USING_LOG_PREFIX STORAGE

#include "ob_encoding_hash_util.h"
#include <cmath>
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;

ObEncodingHashTable::ObEncodingHashTable() : is_created_(false), bucket_num_(0),
    node_num_(0), list_num_(0), node_cnt_(0), list_cnt_(0), buckets_(NULL), nodes_(NULL),
    lists_(NULL), alloc_(blocksstable::OB_ENCODING_LABEL_HASH_TABLE, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
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

int ObEncodingHashTableBuilder::build(const ObColDatums &col_datums, const ObColDesc &col_desc)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!is_created_)) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_UNLIKELY(col_datums.empty() || node_num_ < col_datums.count())) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K_(node_num),
        "row_count", col_datums.count());
  } else {
    ObObjTypeStoreClass store_class = get_store_class_map()[col_desc.col_type_.get_type_class()];
    const bool need_binary_hash =
        (store_class == ObTextSC || store_class == ObJsonSC || store_class == ObLobSC || store_class == ObGeometrySC);
    bool has_lob_header = col_desc.col_type_.is_lob_storage();
    sql::ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(
        col_desc.col_type_.get_type(), col_desc.col_type_.get_collation_type(),
        col_desc.col_type_.get_scale(), lib::is_oracle_mode(), has_lob_header);
    ObHashFunc hash_func;
    hash_func.hash_func_ = basic_funcs->murmur_hash_;
    const uint64_t mask = (bucket_num_ - 1);
    for (int64_t row_id = 0;
        OB_SUCC(ret) && row_id < col_datums.count() && list_cnt_ < list_num_;
        ++row_id) {
      const ObDatum &datum = col_datums.at(row_id);
      if (datum.is_null()) {
        add_to_list(null_nodes_, nodes_[row_id], datum);
      } else if (datum.is_nop()) {
        add_to_list(nope_nodes_, nodes_[row_id], datum);
      } else if (datum.is_ext()) {
        ret = common::OB_NOT_SUPPORTED;
        STORAGE_LOG(WARN, "not supported extend object type",
            K(ret), K(row_id), K(datum), K(*datum.extend_obj_));
      } else {
        uint64_t pos = 0;
        if (OB_FAIL(hash(datum, hash_func, need_binary_hash, pos))) {
          STORAGE_LOG(WARN, "hash failed", K(ret));
        } else {
          pos = pos & mask;
        }
        NodeList *list = buckets_[pos];
        while (OB_SUCC(ret) && nullptr != list) {
          bool is_equal = false;
          if (OB_FAIL(equal(*list->header_->datum_, datum, is_equal))) {
            LOG_WARN("check datum equality failed", K(ret), K(datum), KPC(list->header_->datum_), K(col_desc));
          } else if (is_equal) {
            add_to_list(*list, nodes_[row_id], datum);
            break;
          } else {
            list = list->next_;
          }
        }
        if (OB_SUCC(ret) && nullptr == list) {
          list = &lists_[list_cnt_];
          list->next_ = buckets_[pos];
          buckets_[pos] = list;
          list->insert_ref_ = list_cnt_++;

          add_to_list(*list, nodes_[row_id], datum);
        }
      }
      if (OB_SUCC(ret)) {
        node_cnt_++;
      }
    }
    if (OB_SUCC(ret)) {
      // update dict reference id of null and nope node.
      for (HashNode *n = null_nodes_.header_; NULL != n; n = n->next_) {
        n->dict_ref_ = list_cnt_;
      }
      for (HashNode *n = nope_nodes_.header_; NULL != n; n = n->next_) {
        n->dict_ref_ = list_cnt_ + 1;
      }
    }
  }
  return ret;
}

void ObEncodingHashTableBuilder::add_to_list(NodeList &list, HashNode &node, const ObDatum &datum)
{
  node.dict_ref_ = list.insert_ref_;
  node.datum_ = &datum;
  node.next_ = list.header_;
  list.header_ = &node;
  ++list.size_;
}

int ObEncodingHashTableBuilder::equal(
    const ObDatum &lhs,
    const ObDatum &rhs,
    bool &is_equal)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(lhs.is_ext() || rhs.is_ext())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ext datum in encoding hash table", K(ret), K(lhs), K(rhs));
  } else {
    is_equal = ObDatum::binary_equal(lhs, rhs);
  }
  return ret;
}

int ObEncodingHashTableBuilder::hash(
    const ObDatum &datum,
    const ObHashFunc &hash_func,
    const bool need_binary,
    uint64_t &res)
{
  const int64_t seed = 0;
  int ret = OB_SUCCESS;
  if (need_binary) {
    res = xxhash64(datum.ptr_, datum.len_, seed);
  } else {
    ret = hash_func.hash_func_(datum, seed, res);
  }
  return ret;
}

ObEncodingHashTableFactory::ObEncodingHashTableFactory()
  : allocator_(OB_MALLOC_NORMAL_BLOCK_SIZE, ObMalloc(blocksstable::OB_ENCODING_LABEL_HT_FACTORY)),
    hashtables_()
{
  lib::ObMemAttr attr(MTL_ID(), blocksstable::OB_ENCODING_LABEL_HT_FACTORY);
  allocator_.set_attr(attr);
  hashtables_.set_attr(attr);
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
  if (OB_ISNULL(ht)) {
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
          const uint64_t v = l->header_->datum_->get_uint64() & integer_mask;
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
          const ObDatum &datum = *l->header_->datum_;
          const int64_t len = sizeof(ObNumberDesc)
              + datum.num_->desc_.len_ * sizeof(datum.num_->digits_[0]);
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
      case ObStringSC:
      case ObTextSC:
      case ObJsonSC:
      case ObGeometrySC: { // geometry, json and text storage class have the same behavior currently
        col_ctx.fix_data_size_ = -1;
        col_ctx.max_string_size_ = -1;
        bool var_store = false;
        FOREACH(l, *ht) {
          const int64_t len = l->header_->datum_->len_;
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
          const int64_t len = l->header_->datum_->len_;
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
          const int64_t len = l->header_->datum_->len_;
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

} // end namespace blocksstable
} // end namespace oceanbase
