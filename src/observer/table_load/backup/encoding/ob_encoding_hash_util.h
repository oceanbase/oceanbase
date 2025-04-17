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

#pragma once
#include "lib/hash/xxhash.h"
#include <stdlib.h>
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/ob_pooled_allocator.h"
#include "common/object/ob_object.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "ob_encoding_util.h"
#include "observer/table_load/backup/ob_table_load_backup_block_sstable_struct.h"

namespace oceanbase
{
namespace observer
{
namespace table_load_backup
{

class ObEncodingHashTable;
class ObEncodingHashNode;
class ObEncodingHashNodeList;
class ObEncodingHashTableIterator;
class ObEncodingHashNodeListIterator;
class ObEncodingHashNodeListConstIterator;

inline uint64_t xxhash64(const void* input, int64_t length, uint64_t seed)
{
  return XXH64(input, static_cast<size_t>(length), seed);
}

struct EqualFunc
{
  bool operator()(const common::ObObj *left, const common::ObObj *right) const
  {
    int tmp_ret = -1;
    if (left->get_meta() != right->get_meta()) {
      // -1
    } else if (left->get_data_length() != right->get_data_length()) {
      // -1
    } else {
      tmp_ret = MEMCMP(left->get_data_ptr(), right->get_data_ptr(), left->get_data_length());
    }
    return (tmp_ret == 0);
  }
};

struct HashFunc
{
  uint64_t operator()(const common::ObObj *key) const
  {
    return xxhash64(key->get_data_ptr(), key->get_data_length(), 0);
  }
};

struct ObEncodingHashNode
{
  const common::ObObj *cell_;
  int64_t dict_ref_;
  ObEncodingHashNode *next_;

  TO_STRING_KV(KP_(cell), K_(dict_ref), KP_(next));
};

class ObEncodingHashNodeListIterator
{
  typedef ObEncodingHashNodeListIterator self_t;
public:
  typedef typename std::random_access_iterator_tag iterator_category;
  typedef ObEncodingHashNode value_type;
  typedef ObEncodingHashNode *value_ptr_t;
  typedef ObEncodingHashNode *pointer;
  typedef ObEncodingHashNode &reference;

  ObEncodingHashNodeListIterator() : node_(NULL)
  {
  }

  ObEncodingHashNodeListIterator(const self_t &other) :
      node_(other.node_)
  {
  }

  explicit ObEncodingHashNodeListIterator(value_ptr_t node) :
      node_(node)
  {
  }

  reference operator *() const
  {
    return *node_;
  }

  pointer operator ->() const
  {
    return node_;
  }

  bool operator ==(const self_t &iter) const
  {
    return node_ == iter.node_;
  }

  bool operator !=(const self_t &iter) const
  {
    return node_ != iter.node_;
  }

  self_t &operator ++()
  {
    if (NULL == node_) {
      // end
    } else {
      node_ = node_->next_;
    }
    return *this;
  }

  self_t operator ++(int)
  {
    self_t iter = *this;
    ++*this;
    return iter;
  }

private:
  value_ptr_t node_;
};

class ObEncodingHashNodeListConstIterator
{
  typedef ObEncodingHashNodeListConstIterator self_t;
public:
  typedef typename std::random_access_iterator_tag iterator_category;
  typedef const ObEncodingHashNode value_type;
  typedef const ObEncodingHashNode *value_ptr_t;
  typedef const ObEncodingHashNode *pointer;
  typedef const ObEncodingHashNode &reference;

  ObEncodingHashNodeListConstIterator() : node_(NULL)
  {
  }

  ObEncodingHashNodeListConstIterator(const self_t &other) :
      node_(other.node_)
  {
  }

  explicit ObEncodingHashNodeListConstIterator(value_ptr_t node) :
      node_(node)
  {
  }

  reference operator *() const
  {
    return *node_;
  }

  value_ptr_t operator ->() const
  {
    return node_;
  }

  bool operator ==(const self_t &iter) const
  {
    return node_ == iter.node_;
  }

  bool operator !=(const self_t &iter) const
  {
    return node_ != iter.node_;
  }

  self_t &operator ++()
  {
    if (NULL == node_) {
      // end
    } else {
      node_ = node_->next_;
    }
    return *this;
  }

  self_t operator ++(int)
  {
    self_t iter = *this;
    ++*this;
    return iter;
  }

private:
  value_ptr_t node_;
};

struct ObEncodingHashNodeList
{
  typedef ObEncodingHashNodeListIterator Iterator;
  typedef ObEncodingHashNodeListConstIterator ConstIterator;

  ObEncodingHashNodeList() : header_(NULL), next_(NULL), size_(0), insert_ref_(0)
  {
  }

  Iterator begin()
  {
    return Iterator(header_);
  }

  Iterator end()
  {
    return Iterator(NULL);
  }

  ConstIterator begin() const
  {
    return ConstIterator(header_);
  }

  ConstIterator end() const
  {
    return ConstIterator(NULL);
  }

  ObEncodingHashNode *header_;
  ObEncodingHashNodeList *next_;
  int64_t size_;
  int64_t insert_ref_;

  TO_STRING_KV(KP_(header), KP_(next), K_(size), K_(insert_ref));
};

class ObEncodingHashTable
{
public:
  typedef ObEncodingHashNodeList *HashBucket;
  typedef ObEncodingHashNodeList NodeList;
  typedef ObEncodingHashNode HashNode;
  typedef ObEncodingHashNodeList *Iterator;
  typedef const ObEncodingHashNodeList *ConstIterator;
  const double DIST_NODE_THRESHOLD = 1.0;

  ObEncodingHashTable();
  ~ObEncodingHashTable();

  int create(const int64_t bucket_num, const int64_t node_num);
  inline bool created() const { return is_created_; }
  void reset();
  void reuse();

  int insert(const common::ObObj *cell, const int64_t row_id);
  int insert_null(const common::ObObj *cell, const int64_t row_id);
  int insert_nope(const common::ObObj *cell, const int64_t row_id);

  NodeList &get_null_list() { return null_nodes_; }
  NodeList &get_nope_list() { return nope_nodes_; }
  int64_t get_node_cnt() const { return node_cnt_; }
  int64_t size() const { return list_cnt_; }
  // return distinct cell count, include NULL, NOPE
  OB_INLINE int64_t distinct_cnt() const;
  // return distinct value count, exclude NULL, NOPE
  int64_t distinct_val_cnt() const { return list_cnt_; }
  const HashNode *get_node_list() const { return nodes_; }
  inline int64_t get_bucket_num() const { return bucket_num_; }
  inline int64_t get_node_num() const { return node_num_; }
  Iterator begin() { return lists_; }

  Iterator end() { return lists_ + list_cnt_; }

  ConstIterator begin() const { return lists_; }

  ConstIterator end() const { return lists_ + list_cnt_; }

  TO_STRING_KV(K_(is_created), K_(bucket_num), K_(node_num), K_(list_num), K_(node_cnt), K_(list_cnt),
      KP_(buckets), KP_(nodes), KP_(lists), K_(null_nodes), K_(nope_nodes));

protected:
  bool is_created_;
  int64_t bucket_num_;
  int64_t node_num_;
  int64_t list_num_;
  int64_t node_cnt_;
  int64_t list_cnt_;
  HashBucket *buckets_;
  HashNode *nodes_;
  NodeList *lists_;
  NodeList null_nodes_;
  NodeList nope_nodes_;
  common::ObArenaAllocator alloc_;

  HashFunc hashfunc_;
  EqualFunc equal_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObEncodingHashTable);
};

class ObEncodingHashTableFactory
{
public:
  ObEncodingHashTableFactory();
  virtual ~ObEncodingHashTableFactory();

  int create(const int64_t bucket_num, const int64_t node_num,
             ObEncodingHashTable *&hashtable);
  int recycle(ObEncodingHashTable *hashtable);
private:
  void clear();

  common::ObPooledAllocator<ObEncodingHashTable> allocator_;
  common::ObArray<ObEncodingHashTable *> hashtables_;
};

extern int build_column_encoding_ctx(
    ObEncodingHashTable *ht, const ObObjTypeStoreClass store_class,
    const int64_t type_store_size, ObColumnEncodingCtx &ctx);

} // table_load_backup
} // namespace observer
} // namespace oceanbase
