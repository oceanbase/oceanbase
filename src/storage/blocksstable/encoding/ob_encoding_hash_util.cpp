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
    lists_(NULL), skip_bit_(NULL), hash_val_(NULL),
    alloc_(blocksstable::OB_ENCODING_LABEL_HASH_TABLE, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
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
    const int64_t vec_size = sql::ObBitVector::memory_size(node_num_);

    if (OB_ISNULL(buckets_ = reinterpret_cast<HashBucket *>(alloc_.alloc(bucket_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for bucket", K(ret), K(bucket_size));
    } else if (OB_ISNULL(lists_ = reinterpret_cast<NodeList *>(alloc_.alloc(lists_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for lists", K(ret), K(lists_size));
    } else if (OB_ISNULL(nodes_ = reinterpret_cast<HashNode *>(alloc_.alloc(nodes_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for nodes", K(ret), K(nodes_size));
    } else if (OB_ISNULL(skip_bit_ = sql::to_bit_vector((char *)alloc_.alloc(vec_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for skip bit", K(ret), K(vec_size));
    } else if (OB_ISNULL(hash_val_ = reinterpret_cast<uint64_t *>(alloc_.alloc(node_num_ * sizeof(uint64_t))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for hash val", K(ret), K_(node_num));
    } else {
      MEMSET(buckets_, 0, bucket_size);
      MEMSET(lists_, 0, lists_size);
      MEMSET(nodes_, 0, nodes_size);
      MEMSET(hash_val_, 0, node_num_ * sizeof(uint64_t));
      skip_bit_->init(node_num_);
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
  skip_bit_ = NULL;
  hash_val_ = NULL;
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
  MEMSET(hash_val_, 0, node_num_ * sizeof(uint64_t));
  skip_bit_->init(node_num_);

  node_cnt_ = 0;
  list_cnt_ = 0;
}

int ObEncodingHashTableBuilder::add_to_table(const ObDatum &datum, const int64_t pos, const int64_t row_idx)
{
  int ret = OB_SUCCESS;
  NodeList *list = buckets_[pos];
  while (OB_SUCC(ret) && nullptr != list) {
    bool is_equal = false;
    if (OB_FAIL(equal(*list->header_->datum_, datum, is_equal))) {
      LOG_WARN("check datum equality failed", K(ret), K(datum), KPC(list->header_->datum_));
    } else if (is_equal) {
      add_to_list(*list, nodes_[row_idx], datum, node_cnt_);
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
    add_to_list(*list, nodes_[row_idx], datum, node_cnt_);
  }

  return ret;
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
        (store_class == ObTextSC || store_class == ObJsonSC || store_class == ObLobSC || store_class == ObGeometrySC || store_class == ObRoaringBitmapSC);
    const bool need_batch_hash = !need_binary_hash;
    bool has_lob_header = col_desc.col_type_.is_lob_storage();
    ObPrecision precision = PRECISION_UNKNOWN_YET;
    if (col_desc.col_type_.is_decimal_int()) {
      precision = col_desc.col_type_.get_stored_precision();
      OB_ASSERT(precision != PRECISION_UNKNOWN_YET);
      OB_ASSERT(precision >= 0 && precision <= OB_MAX_DECIMAL_PRECISION);
    }
    sql::ObExprBasicFuncs *basic_funcs = ObDatumFuncs::get_basic_func(
        col_desc.col_type_.get_type(), col_desc.col_type_.get_collation_type(),
        col_desc.col_type_.get_scale(), lib::is_oracle_mode(), has_lob_header, precision);
    ObHashFunc hash_func;
    hash_func.hash_func_ = basic_funcs->murmur_hash_v2_;
    hash_func.batch_hash_func_ = basic_funcs->murmur_hash_v2_batch_;

    const uint64_t mask = (bucket_num_ - 1);
    int64_t dimension_size = col_datums.get_dimension_size();
    int64_t datum_arr_cnt = col_datums.get_continuous_array_count();
    int64_t datum_array_size = 0;
    ObDatum *datum_arry = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < datum_arr_cnt; i++) {
      col_datums.get_continuous_array(i, datum_arry, datum_array_size);
      if (OB_ISNULL(datum_arry)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected null datum array", K(ret), K(i), K(datum_arr_cnt));
      } else {
        skip_bit_->init(datum_array_size);
        for (int64_t idx = 0; OB_SUCC(ret) && idx < datum_array_size && list_cnt_ < list_num_; ++idx) {
          int64_t row_id = i * dimension_size + idx;
          const ObDatum &datum = col_datums.at(row_id);
          if (datum.is_null()) {
            skip_bit_->set(idx);
            add_to_list(null_nodes_, nodes_[row_id], datum, node_cnt_);
          } else if (datum.is_nop()) {
            skip_bit_->set(idx);
            add_to_list(nope_nodes_, nodes_[row_id], datum, node_cnt_);
          } else if (datum.is_ext()) {
            ret = common::OB_NOT_SUPPORTED;
            STORAGE_LOG(WARN, "not supported extend object type",
                        K(ret), K(row_id), K(datum), K(*datum.extend_obj_));
          } else if (!need_batch_hash) {
            uint64_t pos = 0;
            if (OB_FAIL(hash(datum, hash_func, need_binary_hash, pos))) {
              STORAGE_LOG(WARN, "hash failed", K(ret));
            } else {
              pos = pos & mask;
              if (OB_FAIL(add_to_table(datum, pos, row_id))) {
                STORAGE_LOG(WARN, "fail to add to table", K(ret), K(row_id));
              }
            }
          }
        }
      }

      if (OB_SUCC(ret) && need_batch_hash && !skip_bit_->is_all_true(datum_array_size)) {
        const uint64_t seed = 0;
        MEMSET(hash_val_, 0, datum_array_size * sizeof(int64_t));
        hash_func.batch_hash_func_(
            hash_val_,
            datum_arry,
            true,
            *skip_bit_,
            datum_array_size,
            &seed,
            false);
        for (int64_t idx = 0; OB_SUCC(ret) && idx < datum_array_size && list_cnt_ < list_num_; ++idx) {
          if (!skip_bit_->at(idx)) {
            int64_t row_id = i * dimension_size + idx;
            uint64_t pos = hash_val_[idx] & mask;
            if (OB_FAIL(add_to_table(col_datums.at(row_id), pos, row_id))) {
              STORAGE_LOG(WARN, "fail to add to table", K(ret), K(row_id), K(pos));
            }
          }
        }
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

void ObEncodingHashTableBuilder::add_to_list(NodeList &list, HashNode &node, const ObDatum &datum, int64_t &node_cnt)
{
  node.dict_ref_ = list.insert_ref_;
  node.datum_ = &datum;
  node.next_ = list.header_;
  list.header_ = &node;
  ++list.size_;
  ++node_cnt;
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

int ObEncodingHashTableFactory::recycle(const bool force_cache, ObEncodingHashTable *hashtable)
{
  int ret = OB_SUCCESS;
  if (NULL == hashtable) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(hashtable), K(ret));
  } else if (!hashtable->created()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hashtable not created", K(ret));
  } else if (!force_cache && hashtable->get_node_cnt() >= MAX_CACHED_HASHTABLE_SIZE) {
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
      case ObDecimalIntSC:
        // wide int type is neither packed not delta coded yet, store as fixed-length buf for now
      case ObStringSC:
      case ObTextSC:
      case ObJsonSC:
      case ObGeometrySC:
      case ObRoaringBitmapSC: { // geometry, json and text storage class have the same behavior currently
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
        if (OB_UNLIKELY(ObDecimalIntSC == store_class && var_store)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("var store for store class invalid", K(ret), K(store_class), K(var_store), K(col_ctx));
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
