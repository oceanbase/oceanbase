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

#ifndef OCEANBASE_ENCODING_OB_ENCODING_HASH_UTIL_H_
#define OCEANBASE_ENCODING_OB_ENCODING_HASH_UTIL_H_

#include "lib/hash/xxhash.h"
#include <stdlib.h>
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/ob_pooled_allocator.h"
#include "common/object/ob_object.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "ob_icolumn_encoder.h"
#include "ob_encoding_util.h"

namespace oceanbase
{
namespace blocksstable
{

class ObEncodingHashTable;
struct ObEncodingHashNode;
struct ObEncodingHashNodeList;
class ObEncodingHashTableIterator;
class ObEncodingHashNodeListIterator;
class ObEncodingHashNodeListConstIterator;

inline uint64_t xxhash64(const void* input, int64_t length, uint64_t seed)
{
  return XXH64(input, static_cast<size_t>(length), seed);
}

struct ObEncodingHashNode
{
  const ObDatum *datum_;
  int64_t dict_ref_;
  ObEncodingHashNode *next_;

  TO_STRING_KV(KP_(datum), K_(dict_ref), KP_(next));
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

  NodeList &get_null_list() { return null_nodes_; }
  NodeList &get_nope_list() { return nope_nodes_; }
  int64_t get_node_cnt() const { return node_cnt_; }
  int64_t size() const { return list_cnt_; }
  // return distinct cell count, include NULL, NOPE
  OB_INLINE int64_t distinct_cnt() const
  {
    return distinct_val_cnt()
        + (null_nodes_.size_ > 0 ? 1 : 0)
        + (nope_nodes_.size_ > 0 ? 1 : 0);
  }
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

private:
  DISALLOW_COPY_AND_ASSIGN(ObEncodingHashTable);
};

class ObEncodingHashTableBuilder : public ObEncodingHashTable
{
public:
  int build(const ObColDatums &col_datums, const ObColDesc &col_desc);
private:
  // binary equal for store types need char case or precision handling
  static int equal(
      const ObDatum &lhs,
      const ObDatum &rhs,
      bool &is_equal);
  static int hash(const ObDatum &datum, const ObHashFunc &hash_func, const bool need_binary, uint64_t &res);

  static void add_to_list(NodeList &list, HashNode &node, const ObDatum &datum);
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

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_ENCODING_OB_ENCODING_HASH_UTIL_H_
