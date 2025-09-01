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

#ifndef OCEANBASE_CS_ENCODING_OB_DICT_ENCODING_HASH_TABLE_H_
#define OCEANBASE_CS_ENCODING_OB_DICT_ENCODING_HASH_TABLE_H_

#include "lib/hash/xxhash.h"
#include <stdlib.h>
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/ob_pooled_allocator.h"
#include "common/object/ob_object.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"

namespace oceanbase
{
namespace blocksstable
{
struct ObDictHashNode
{
  ObDatum datum_;
  int32_t dict_ref_;
  int32_t duplicate_cnt_;
  ObDictHashNode *next_;

  OB_INLINE void init(const ObDatum datum, const int32_t dict_ref, ObDictHashNode *next)
  {
    datum_ = datum;
    dict_ref_ = dict_ref;
    duplicate_cnt_ = 1;
    next_ = next;
  }
  OB_INLINE void reset()
  {
    datum_.reset();
    dict_ref_ = -1;
    duplicate_cnt_ = 0;
    next_ = nullptr;
  }
  TO_STRING_KV(K_(datum), K_(dict_ref), K_(duplicate_cnt), KP_(next));
};

struct ObDictNodeCmp
{
  explicit ObDictNodeCmp(int &ret, const ObCmpFunc &cmp_func) : ret_(ret), cmp_func_(cmp_func) { }
  bool operator()(const ObDictHashNode &left, const ObDictHashNode &right);

private:
  int &ret_;
  const ObCmpFunc &cmp_func_;
};


class ObDictEncodingHashTable
{
public:
  typedef ObDictHashNode *HashBucket;
  typedef ObDictHashNode HashNode;
  typedef HashNode *Iterator;
  typedef const HashNode *ConstIterator;

  ObDictEncodingHashTable();
  ~ObDictEncodingHashTable();

  int create(const int64_t bucket_num, const int64_t node_num);
  inline bool created() const { return is_created_; }
  void reset();
  void reuse();

  OB_INLINE int64_t get_node_num() const { return node_num_; }

  // return distinct cell count, include NULL
  OB_INLINE int64_t distinct_cnt() const
  {
    return distinct_val_cnt() + (null_node_.duplicate_cnt_ > 0 ? 1 : 0);
  }
  // return distinct value count, exclude NULL, NOPE
  OB_INLINE int64_t distinct_val_cnt() const { return distinct_node_cnt_; }
  OB_INLINE int64_t distinct_node_cnt() const { return distinct_node_cnt_; }
  OB_INLINE int64_t get_bucket_num() const { return bucket_num_; }
  OB_INLINE int64_t get_null_cnt() const { return null_node_.duplicate_cnt_; }
  OB_INLINE int64_t get_nop_cnt() const { return nop_node_.duplicate_cnt_; }
  OB_INLINE const HashNode *get_null_node() const { return &null_node_; }
  OB_INLINE const HashNode *get_nop_node() const { return &nop_node_; }
  OB_INLINE int32_t *get_row_refs() const { return row_refs_; }
  OB_INLINE int32_t *get_refs_permutation() const { return (int32_t*)buckets_; }
  OB_INLINE int64_t get_row_count() const { return row_count_; }


  Iterator begin() { return nodes_; }

  Iterator end() { return nodes_ + distinct_node_cnt_; }

  ConstIterator begin() const { return nodes_; }

  ConstIterator end() const { return nodes_ + distinct_node_cnt_; }
  int sort_dict(ObCmpFunc &cmp_func);

  TO_STRING_KV(K_(is_created),
               K_(is_sorted),
               K_(bucket_num),
               K_(node_num),
               K_(distinct_node_cnt),
               KP_(buckets),
               KP_(nodes),
               K_(null_node),
               K_(nop_node));

  protected:
  static const int32_t NULL_REF = INT32_MAX;

  bool is_created_;
  bool is_sorted_;
  int64_t bucket_num_;
  int64_t node_num_;
  int32_t distinct_node_cnt_;
  int32_t row_count_;
  HashBucket *buckets_;
  HashNode *nodes_;
  int32_t *row_refs_;
  int32_t *refs_permutation_;
  HashNode null_node_;
  HashNode nop_node_;
  common::ObArenaAllocator alloc_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObDictEncodingHashTable);
};

class ObDictEncodingHashTableBuilder : public ObDictEncodingHashTable
{
public:
  int build(const ObColDatums &col_datums, const ObColDesc &col_desc);
};

class ObDictEncodingHashTableFactory
{
public:
  ObDictEncodingHashTableFactory();
  virtual ~ObDictEncodingHashTableFactory();

  int create(const int64_t bucket_num, const int64_t node_num,
             ObDictEncodingHashTable *&hashtable);
  int recycle(const bool force_cache, ObDictEncodingHashTable *hashtable);
  bool has_cached_dict_table() const { return hashtables_.count() > 0; }
  void clear();

private:

  // release hashtable too large to reduce memory usage
  static const int64_t MAX_CACHED_HASHTABLE_SIZE = UINT16_MAX;
  common::ObPooledAllocator<ObDictEncodingHashTable> allocator_;
  common::ObArray<ObDictEncodingHashTable *> hashtables_;
};


} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_CS_ENCODING_OB_DICT_ENCODING_HASH_TABLE_H_
