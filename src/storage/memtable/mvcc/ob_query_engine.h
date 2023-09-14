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
#include "storage/memtable/mvcc/ob_mvcc_row.h"
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

class ObIQueryEngineIterator
{
public:
  ObIQueryEngineIterator() {}
  virtual ~ObIQueryEngineIterator() {}
public:
  virtual int next(const bool skip_purge_memtable) = 0;
  virtual int next_internal(const bool skip_purge_memtable) = 0;
  virtual const ObMemtableKey *get_key() const = 0;
  virtual ObMvccRow *get_value() const = 0;
  virtual void reset() = 0;
  virtual void set_version(int64_t version) = 0;
  virtual uint8_t get_iter_flag() const = 0;
  virtual bool is_reverse_scan() const = 0;
};

class ObQueryEngine
{
#define NOT_PLACE_HOLDER(ptr) OB_UNLIKELY(PLACE_HOLDER != ptr)

public:
  enum {
    INIT_TABLE_INDEX_COUNT = (1 << 10),
    MAX_SAMPLE_ROW_COUNT = 500
  };
  typedef keybtree::ObKeyBtree<ObStoreRowkeyWrapper, ObMvccRow *> KeyBtree;
  typedef keybtree::BtreeIterator<ObStoreRowkeyWrapper, ObMvccRow *> BtreeIterator;
  typedef keybtree::BtreeNodeAllocator<ObStoreRowkeyWrapper, ObMvccRow *> BtreeNodeAllocator;
  typedef keybtree::BtreeRawIterator<ObStoreRowkeyWrapper, ObMvccRow *> BtreeRawIterator;
  typedef ObMtHash KeyHash;

  template <typename BtreeIterator>
  class Iterator : public ObIQueryEngineIterator
  {
  public:
    Iterator(): value_(NULL), iter_flag_(0), version_(0) {}
    ~Iterator() {}
  public:
    void set_version(int64_t version) { version_ = version; }
    int next(const bool skip_purge_memtable)
    {
      int ret = common::OB_SUCCESS;
      while (OB_SUCC(next_internal(skip_purge_memtable))
             && value_->is_empty())
// TODO: handora.qc
//             && !value_->row_lock_.is_locked())
        ;
      return ret;
    }
    int next_internal(const bool skip_purge_memtable)
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
        iter_flag_ = 0;
        if (OB_ISNULL(value_)) {
          ret = common::OB_ITER_END;
        } else {
          if (skip_purge_memtable || value_->is_partial(version_)) {
            iter_flag_ |= STORE_ITER_ROW_PARTIAL;
          }
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
      iter_flag_ = 0;
      version_ = 0;
    }
    BtreeIterator &get_read_handle() { return btree_iter_; }
    inline uint8_t get_iter_flag() const { return iter_flag_; }
  private:
    DISALLOW_COPY_AND_ASSIGN(Iterator);
    BtreeIterator btree_iter_;
    ObMemtableKey key_;
    ObMvccRow *value_;
    uint8_t iter_flag_;
    int64_t version_;
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
  enum { ESTIMATE_CHILD_COUNT_THRESHOLD = 1024, MAX_RANGE_SPLIT_COUNT = 1024 * 1024};
  explicit ObQueryEngine(ObIAllocator &memstore_allocator)
      : is_inited_(false),
        is_expanding_(false),
        tenant_id_(common::OB_SERVER_TENANT_ID),
        memstore_allocator_(memstore_allocator),
        btree_allocator_(memstore_allocator_),
        keybtree_(btree_allocator_),
        keyhash_(memstore_allocator_) {}
  ~ObQueryEngine() { destroy(); }
  int init(const uint64_t tenant_id);
  void destroy();
  void pre_batch_destroy_keybtree();
  int set(const ObMemtableKey *key, ObMvccRow *value);
  int get(const ObMemtableKey *parameter_key, ObMvccRow *&row, ObMemtableKey *returned_key);
  int ensure(const ObMemtableKey *key, ObMvccRow *value);
  int check_and_purge(const ObMemtableKey *key, ObMvccRow *row, int64_t version, bool &purged);
  int purge(const ObMemtableKey *key, int64_t version);
  int scan(const ObMemtableKey *start_key, const bool start_exclude, const ObMemtableKey *end_key,
           const bool end_exclude, const int64_t version, ObIQueryEngineIterator *&ret_iter);
  void revert_iter(ObIQueryEngineIterator *iter);
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

  int dump_keyhash(FILE *fd) const;
  int dump_keybtree(FILE *fd);
  int64_t hash_size() const;
  int64_t hash_alloc_memory() const;
  int64_t btree_size() const;
  int64_t btree_alloc_memory() const;
  void check_cleanout(bool &is_all_cleanout,
                      bool &is_all_delay_cleanout,
                      int64_t &count);
  void dump2text(FILE *fd);
private:
  int sample_rows(Iterator<BtreeRawIterator> *iter, const ObMemtableKey *start_key,
                  const int start_exclude, const ObMemtableKey *end_key, const int end_exclude,
                  const transaction::ObTransID &tx_id,
                  int64_t &logical_row_count, int64_t &physical_row_count, double &ratio);
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
  bool is_expanding_;
  uint64_t tenant_id_;
  ObIAllocator &memstore_allocator_;
  BtreeNodeAllocator btree_allocator_;
  IteratorAlloc<BtreeIterator> iter_alloc_;
  IteratorAlloc<BtreeRawIterator> raw_iter_alloc_;
  KeyBtree keybtree_;
  KeyHash keyhash_;
};

} // namespace memtable
} // namespace oceanbase

#endif //OCEANBASE_MEMTABLE_MVCC_OB_QUERY_ENGINE_
