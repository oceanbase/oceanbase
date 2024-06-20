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

#ifndef  OCEANBASE_MEMTABLE_MVCC_OB_QUERY_ENGINE_
#define  OCEANBASE_MEMTABLE_MVCC_OB_QUERY_ENGINE_

#include "lib/container/ob_iarray.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "storage/memtable/mvcc/ob_keybtree.h"
#include "storage/memtable/ob_memtable_key.h"
#include "storage/memtable/ob_mt_hash.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace memtable
{
class ObMvccRow;

typedef keybtree::ObKeyBtree<ObStoreRowkeyWrapper, ObMvccRow *> ObMemtableKeyBtree;

// Interface for the memtable iterator. You can iter the desired key and value
// through the iterator. Among this, the key is ObMemtableKey and the value is
// ObMvccRow.
class ObIQueryEngineIterator
{
public:
  ObIQueryEngineIterator() {}
  virtual ~ObIQueryEngineIterator() {}
public:
  // next() will iterate to the next nonempty ObMvccRow, so user can expect the
  // real mvcc data from the value.
  virtual int next() = 0;
  // next_internal() will directly iterate to the next ObMvccRow, so there may be
  // no mvcc data on it and use it carefully!!!
  virtual int next_internal() = 0;
  // get_key() fetch the ObMemtableKey currently iterated to
  virtual const ObMemtableKey *get_key() const = 0;
  // get_value() fetch the ObMvccRow currently iterated to
  virtual ObMvccRow *get_value() const = 0;
  // is_reverse_scan() return the order of the scan
  virtual bool is_reverse_scan() const = 0;
  // reset() will reset the iteration, you need restart init the iterator
  virtual void reset() = 0;
};

// ObQueryEngine consists of hashtable and btree. We will maintain key and value
// into both of the hashtable and btree and use them to complete efficient
// point select and range query operations
class ObQueryEngine
{
public:
  // btree for range scan
  typedef keybtree::ObKeyBtree<ObStoreRowkeyWrapper, ObMvccRow *> KeyBtree;
  // Used for data allocation
  typedef keybtree::BtreeNodeAllocator<ObStoreRowkeyWrapper, ObMvccRow *> BtreeNodeAllocator;
  // Used for query
  typedef keybtree::BtreeIterator<ObStoreRowkeyWrapper, ObMvccRow *> BtreeIterator;
  // Used only for estimation.
  typedef keybtree::BtreeRawIterator<ObStoreRowkeyWrapper, ObMvccRow *> BtreeRawIterator;
  // hashtable for point select
  typedef ObMtHash KeyHash;

  // ObQueryEngine Iterator implements the iterator interface
  template <typename BtreeIterator>
  class Iterator : public ObIQueryEngineIterator
  {
  public:
    Iterator(): value_(NULL) {}
    ~Iterator() {}
  public:
    int next()
    {
      int ret = common::OB_SUCCESS;
      while (OB_SUCC(next_internal())
             && value_->is_empty()) {
        // get next non-empty mvcc row
      }
      return ret;
    }
    int next_internal()
    {
      int ret = common::OB_SUCCESS;
      ObStoreRowkeyWrapper key_wrapper(key_.get_rowkey());
      if (OB_FAIL(btree_iter_.get_next(key_wrapper, value_))) {
        if (common::OB_ITER_END != ret) {
          TRANS_LOG(WARN, "get_next from keybtree fail", "ret", ret, "value", value_);
        }
      } else {
        key_.encode(key_wrapper.get_rowkey());
        BTREE_ASSERT(((uint64_t)value_ & 7ULL) == 0);
        if (OB_ISNULL(value_)) {
          ret = common::OB_ITER_END;
        }
      }
      if (common::OB_ITER_END == ret) {
        btree_iter_.reset();
      }
      return ret;
    }
    bool is_reverse_scan() const { return btree_iter_.is_reverse_scan(); }
    ObMvccRow *get_value() const { return value_; }
    const ObMemtableKey *get_key() const { return &key_; }
    void reset()
    {
      btree_iter_.reset();
      key_.reset();
      value_ = nullptr;
    }
    BtreeIterator &get_read_handle() { return btree_iter_; }
  private:
    DISALLOW_COPY_AND_ASSIGN(Iterator);
    BtreeIterator btree_iter_;
    ObMemtableKey key_;
    ObMvccRow *value_;
  };

  template <typename BtreeIterator>
  class IteratorAlloc
  {
  public:
    IteratorAlloc() {}
    ~IteratorAlloc() {}
    Iterator<BtreeIterator> *alloc() { return op_reclaim_alloc(Iterator<BtreeIterator>); }
    void free(Iterator<BtreeIterator> *iter) { op_reclaim_free(iter); }
  private:
    DISALLOW_COPY_AND_ASSIGN(IteratorAlloc);
  };

public:
  enum {
    MAX_SAMPLE_ROW_COUNT = 500,
    ESTIMATE_CHILD_COUNT_THRESHOLD = 1024,
    MAX_RANGE_SPLIT_COUNT = 1024 * 1024
  };

  explicit ObQueryEngine(ObIAllocator &memstore_allocator)
    : is_inited_(false),
    memstore_allocator_(memstore_allocator),
    btree_allocator_(memstore_allocator_),
    keybtree_(btree_allocator_),
    keyhash_(memstore_allocator_) {}
  ~ObQueryEngine() { destroy(); }
  int init();
  void destroy();
  void pre_batch_destroy_keybtree();

  // ===================== Ob Query Engine User Operation Interface =====================
  // The concurrency control alogorithm of query engine is as following steps:
  // 1. Firstly, we use the atomic hashtable to ensure that only one thread can
  //    create the ObMvccRow and support efficient point select(through set())
  // 2. Then we can operate the ObMvccRow according to the operation semantics
  // 3. After above operation, we need atomically insert the ObMvccRow into the
  //    btree to support the efficient range query(through ensure())
  int set(const ObMemtableKey *key, ObMvccRow *value);
  int ensure(const ObMemtableKey *key, ObMvccRow *value);
  // get() will use the hashtable to support fast point select
  int get(const ObMemtableKey *parameter_key, ObMvccRow *&row, ObMemtableKey *returned_key);
  // scan() will use the btree to support fast range query
  int scan(const ObMemtableKey *start_key, const bool start_exclude, const ObMemtableKey *end_key,
           const bool end_exclude, ObIQueryEngineIterator *&ret_iter);
  void revert_iter(ObIQueryEngineIterator *iter);


  // ===================== Ob Query Engine Estimation =====================
  // Estimate the size and row count of the memtable, the estimzation is not
  // particularly precise and it is mainly used for sql optimizor
  int estimate_size(const ObMemtableKey *start_key,
                    const ObMemtableKey *end_key,
                    int64_t& total_bytes,
                    int64_t& total_rows);
  int split_range(const ObMemtableKey *start_key,
                  const ObMemtableKey *end_key,
                  int64_t part_count,
                  common::ObIArray<common::ObStoreRange> &range_array);
  int estimate_row_count(const transaction::ObTransID &tx_id,
                         const ObMemtableKey *start_key, const int start_exclude,
                         const ObMemtableKey *end_key, const int end_exclude,
                         int64_t &logical_row_count, int64_t &physical_row_count);

  // ===================== Ob Query Engine Debug Tool =====================
  // Check whether all nodes in the btree is cleanout or delay_cleanout and
  // return the count of the nodes. It is currenly used for case test, use it
  // carefully!!!
  void check_cleanout(bool &is_all_cleanout,
                      bool &is_all_delay_cleanout,
                      int64_t &count);
  // Dump the hash table and btree to the file.
  void dump2text(FILE *fd);
  int dump_keyhash(FILE *fd) const;
  int dump_keybtree(FILE *fd);
  // Btree statistics used for virtual table
  int64_t hash_size() const;
  int64_t hash_alloc_memory() const;
  int64_t btree_size() const;
  int64_t btree_alloc_memory() const;
private:
  int sample_rows(Iterator<BtreeRawIterator> *iter,
                  const ObMemtableKey *start_key,
                  const int start_exclude,
                  const ObMemtableKey *end_key,
                  const int end_exclude,
                  const transaction::ObTransID &tx_id,
                  int64_t &logical_row_count,
                  int64_t &physical_row_count,
                  double &ratio);
  int init_raw_iter_for_estimate(Iterator<BtreeRawIterator>*& iter,
                                 const ObMemtableKey *start_key,
                                 const ObMemtableKey *end_key);
  int find_split_range_level_(const ObMemtableKey *start_key,
                              const ObMemtableKey *end_key,
                              const int64_t range_count,
                              int64_t &top_level,
                              int64_t &btree_node_count);

  int inner_loop_find_level_(const ObMemtableKey *start_key,
                             const ObMemtableKey *end_key,
                             const int64_t level_node_threshold,
                             int64_t &top_level,
                             int64_t &btree_node_count,
                             int64_t &total_rows);

  int convert_keys_to_store_ranges_(const ObMemtableKey *start_key,
                                    const ObMemtableKey *end_key,
                                    const int64_t range_count,
                                    const common::ObIArray<ObStoreRowkeyWrapper> &key_array,
                                    ObIArray<ObStoreRange> &range_array);

private:
  DISALLOW_COPY_AND_ASSIGN(ObQueryEngine);

  bool is_inited_;
  // allocator for keyhash and btree
  ObIAllocator &memstore_allocator_;
  BtreeNodeAllocator btree_allocator_;
  // The btree optimized for fast range scan
  KeyBtree keybtree_;
  // The hashtable optimized for fast point select
  KeyHash keyhash_;
  // Iterator allocator for read and estimation
  IteratorAlloc<BtreeIterator> iter_alloc_;
  IteratorAlloc<BtreeRawIterator> raw_iter_alloc_;
};

} // namespace memtable
} // namespace oceanbase

#endif //OCEANBASE_MEMTABLE_MVCC_OB_QUERY_ENGINE_
