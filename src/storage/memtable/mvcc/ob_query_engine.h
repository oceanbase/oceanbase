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

#ifndef OCEANBASE_MEMTABLE_MVCC_OB_QUERY_ENGINE_
#define OCEANBASE_MEMTABLE_MVCC_OB_QUERY_ENGINE_

#include "lib/container/ob_iarray.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/objectpool/ob_concurrency_objpool.h"
#include "storage/memtable/mvcc/ob_keybtree.h"
#include "storage/memtable/ob_memtable_key.h"
#include "storage/memtable/ob_mt_hash.h"

namespace oceanbase {
namespace common {
class ObIAllocator;
}
namespace memtable {
class ObMvccRow;

class ObIQueryEngineIterator {
public:
  ObIQueryEngineIterator()
  {}
  virtual ~ObIQueryEngineIterator()
  {}

public:
  virtual int next(const bool skip_purge_memtable) = 0;
  virtual int next_internal(const bool skip_purge_memtable) = 0;
  virtual const ObMemtableKey* get_key() const = 0;
  virtual ObMvccRow* get_value() const = 0;
  virtual void reset() = 0;
  virtual void set_version(int64_t version) = 0;
  virtual uint8_t get_iter_flag() const = 0;
  virtual bool is_reverse_scan() const = 0;
};

class ObQueryEngine {
public:
  enum { INIT_TABLE_INDEX_COUNT = (1 << 10), MAX_SAMPLE_ROW_COUNT = 500 };
  typedef keybtree::ObKeyBtree KeyBtree;
  typedef ObMtHash KeyHash;

  template <typename BtreeScanHandle>
  class Iterator : public ObIQueryEngineIterator {
  public:
    Iterator() : value_(NULL), iter_flag_(0), version_(0)
    {}
    ~Iterator()
    {}

  public:
    void set_version(int64_t version)
    {
      version_ = version;
    }
    int next(const bool skip_purge_memtable)
    {
      int ret = common::OB_SUCCESS;
      while (OB_SUCC(next_internal(skip_purge_memtable)) && !value_->row_lock_.is_locked() && value_->is_empty())
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
        // IN_GAP & BIG_GAP_HINT
        key_.encode(key_.get_table_id(), key_wrapper.get_rowkey());
        iter_flag_ = (uint8_t)((uint64_t)value_ & 3UL);
        value_ = (ObMvccRow*)((uint64_t)value_ & ~3UL);
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
    bool is_reverse_scan() const
    {
      return btree_iter_.is_reverse_scan();
    }
    ObMvccRow* get_value() const
    {
      return value_;
    }
    const ObMemtableKey* get_key() const
    {
      return &key_;
    }
    void reset()
    {
      btree_iter_.reset();
      key_.reset();
      value_ = nullptr;
      iter_flag_ = 0;
      version_ = 0;
    }
    BtreeScanHandle& get_read_handle()
    {
      return btree_iter_;
    }
    uint8_t get_iter_flag() const
    {
      return iter_flag_;
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(Iterator);
    BtreeScanHandle btree_iter_;
    ObMemtableKey key_;
    ObMvccRow* value_;
    uint8_t iter_flag_;
    int64_t version_;
  };

  template <typename BtreeScanHandle>
  class IteratorAlloc {
  public:
    IteratorAlloc()
    {}
    ~IteratorAlloc()
    {}
    Iterator<BtreeScanHandle>* alloc()
    {
      return op_reclaim_alloc(Iterator<BtreeScanHandle>);
    }
    void free(Iterator<BtreeScanHandle>* iter)
    {
      op_reclaim_free(iter);
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(IteratorAlloc);
  };

  class TableIndexNode {
  public:
    explicit TableIndexNode(keybtree::BtreeNodeAllocator& btree_allocator, common::ObIAllocator& memstore_allocator,
        const uint64_t table_id, int64_t obj_cnt)
        : is_inited_(false),
          keybtree_(btree_allocator),
          keyhash_(memstore_allocator),
          table_id_(table_id),
          obj_cnt_(obj_cnt)
    {}
    ~TableIndexNode()
    {
      destroy();
    }
    int init();
    void destroy();
    void dump2text(FILE* fd);
    KeyBtree& get_keybtree()
    {
      return keybtree_;
    }
    KeyHash& get_keyhash()
    {
      return keyhash_;
    }
    uint64_t get_table_id()
    {
      return table_id_;
    }
    int64_t get_obj_cnt()
    {
      return obj_cnt_;
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(TableIndexNode);
    bool is_inited_;
    KeyBtree keybtree_;
    KeyHash keyhash_;
    uint64_t table_id_;
    int64_t obj_cnt_;
  };

  class TableIndex {
  public:
    explicit TableIndex(keybtree::BtreeNodeAllocator& btree_allocator, common::ObIAllocator& memstore_allocator)
        : base_(nullptr),
          is_inited_(false),
          capacity_(0),
          btree_allocator_(btree_allocator),
          memstore_allocator_(memstore_allocator)
    {}
    ~TableIndex()
    {
      destroy();
    }
    // copy and expand on old_index, construct with default size if it's null
    int init(const TableIndex* old_index);
    void destroy();
    int64_t get_occupied_size() const;
    int dump_keyhash(FILE* fd) const;
    int64_t hash_size() const;
    int64_t hash_alloc_memory() const;
    int dump_keybtree(FILE* fd);
    int64_t btree_size() const;
    int64_t btree_alloc_memory() const;
    void dump2text(FILE* fd);
    uint64_t get_capacity() const
    {
      return capacity_;
    }
    /** return:
     *   OB_TABLE_NOT_EXIST            : table not exists
     *   OB_NOT_INIT                   : not inited
     *   OB_SUCCESS                    : return_ptr is valid
     * query_engine don't care if table is exists before,
     * but there are 2 scenes when there is no data after adding index for QueryEngine:
     * 1. table is in this memtable, but there is no data.
     * 2. table is not in this memtable.
     * For the former, query_engine can return OB_ITER_END/OB_ENTRY_NOT_EXIST like before;
     * For the latter, return OB_ITER_END/OB_ENTRY_NOT_EXIST to keep compatibility.
     **/
    int get(const uint64_t table_id, TableIndexNode*& return_ptr);
    /** return:
     *   OB_ARRAY_OUT_OF_RANGE     : table reach limit
     *   OB_NOT_INIT               : not inited
     *   OB_INIT_FAIL              : init failed
     *   OB_ALLOCATE_MEMORY_FAILED : memory reach limit
     *   OB_SUCCESS                : return_ptr is valid
     **/
    int set(const uint64_t table_id, const int64_t obj_cnt, TableIndexNode*& return_ptr);
    int get_active_table_ids(common::ObIArray<uint64_t>& table_ids);

  private:
    DISALLOW_COPY_AND_ASSIGN(TableIndex);
    static TableIndexNode* const PLACE_HOLDER;
    TableIndexNode** base_;
    bool is_inited_;
    uint64_t capacity_;
    keybtree::BtreeNodeAllocator& btree_allocator_;
    ObIAllocator& memstore_allocator_;
  };

public:
  enum { ESTIMATE_CHILD_COUNT_THRESHOLD = 1024, MAX_RANGE_SPLIT_COUNT = 1024 };
  explicit ObQueryEngine(ObIAllocator& memstore_allocator)
      : is_inited_(false),
        is_expanding_(false),
        tenant_id_(common::OB_SERVER_TENANT_ID),
        index_(nullptr),
        memstore_allocator_(memstore_allocator),
        btree_allocator_(memstore_allocator_)
  {}
  ~ObQueryEngine()
  {
    destroy();
  }
  int init(const uint64_t tenant_id);
  void destroy();
  int64_t get_occupied_size() const
  {
    return OB_NOT_NULL(index_) ? index_->get_occupied_size() : 0;
  }
  int set(const ObMemtableKey* key, ObMvccRow* value);
  int get(const ObMemtableKey* parameter_key, ObMvccRow*& row, ObMemtableKey* returned_key);
  int ensure(const ObMemtableKey* key, ObMvccRow* value);
  int skip_gap(const ObMemtableKey* start, const ObStoreRowkey*& end, int64_t version, bool is_reverse, int64_t& size);
  int check_and_purge(const ObMemtableKey* key, ObMvccRow* row, int64_t version, bool& purged);
  int get_sdr(const ObMemtableKey* key, ObMemtableKey* start_key, ObMemtableKey* end_key, int64_t& max_version);
  int purge(const ObMemtableKey* key, int64_t version);
  int scan(const ObMemtableKey* start_key, const int start_exclude, const ObMemtableKey* end_key, const int end_exclude,
      const int64_t version, ObIQueryEngineIterator*& ret_iter);
  void revert_iter(ObIQueryEngineIterator* iter);
  int prefix_exist(const ObMemtableKey* prefix_key, bool& may_exist);
  int estimate_size(const ObMemtableKey* start_key, const ObMemtableKey* end_key, int64_t& level, int64_t& branch_count,
      int64_t& total_bytes, int64_t& total_rows);
  int split_range(const ObMemtableKey* start_key, const ObMemtableKey* end_key, int64_t part_count,
      common::ObIArray<common::ObStoreRange>& range_array);
  int estimate_row_count(const ObMemtableKey* start_key, const int start_exclude, const ObMemtableKey* end_key,
      const int end_exclude, int64_t& logical_row_count, int64_t& physical_row_count);
  int dump_keyhash(FILE* fd) const
  {
    return OB_NOT_NULL(index_) ? index_->dump_keyhash(fd) : OB_SUCCESS;
  }
  int64_t hash_size() const
  {
    return OB_NOT_NULL(index_) ? index_->hash_size() : 0;
  }
  int64_t hash_alloc_memory() const
  {
    return OB_NOT_NULL(index_) ? index_->hash_alloc_memory() : 0;
  }
  int dump_keybtree(FILE* fd)
  {
    return OB_NOT_NULL(index_) ? index_->dump_keybtree(fd) : OB_SUCCESS;
  }
  int64_t btree_size() const
  {
    return OB_NOT_NULL(index_) ? index_->btree_size() : 0;
  }
  int64_t btree_alloc_memory() const
  {
    return OB_NOT_NULL(index_) ? index_->btree_alloc_memory() : 0;
  }
  void dump2text(FILE* fd, const uint64_t table_id);
  void dump2text(FILE* fd);
  int get_table_index_node(const uint64_t table_id, TableIndexNode*& return_ptr);
  int set_table_index_node(const uint64_t table_id, const int64_t obj_cnt, TableIndexNode*& return_ptr);
  int expand_index(TableIndex* old_index);
  int get_active_table_ids(common::ObIArray<uint64_t>& table_ids);

private:
  int sample_rows(Iterator<keybtree::TScanRawHandle>* iter, const ObMemtableKey* start_key, const int start_exclude,
      const ObMemtableKey* end_key, const int end_exclude, int64_t& logical_row_count, int64_t& physical_row_count,
      double& ratio);
  int init_raw_iter_for_estimate(
      Iterator<keybtree::TScanRawHandle>*& iter, const ObMemtableKey* start_key, const ObMemtableKey* end_key);

private:
  DISALLOW_COPY_AND_ASSIGN(ObQueryEngine);
  bool is_inited_;
  bool is_expanding_;
  uint64_t tenant_id_;
  TableIndex* index_;
  ObIAllocator& memstore_allocator_;
  keybtree::BtreeNodeAllocator btree_allocator_;
  IteratorAlloc<keybtree::TScanHandle> iter_alloc_;
  IteratorAlloc<keybtree::TScanRawHandle> raw_iter_alloc_;
};

}  // namespace memtable
}  // namespace oceanbase

#endif  // OCEANBASE_MEMTABLE_MVCC_OB_QUERY_ENGINE_
