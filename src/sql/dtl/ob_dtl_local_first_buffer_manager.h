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

#ifndef OB_DTL_LOCAL_FIRST_BUFFER_MANAGER_H
#define OB_DTL_LOCAL_FIRST_BUFFER_MANAGER_H

#include "lib/utility/ob_unify_serialize.h"
#include "lib/atomic/ob_atomic.h"
#include "sql/dtl/ob_dtl_flow_control.h"
#include "sql/dtl/ob_dtl_linked_buffer.h"
#include "sql/dtl/ob_dtl_tenant_mem_manager.h"
#include "lib/list/ob_dlist.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_mutex.h"
#include "lib/ob_define.h"
#include "share/config/ob_server_config.h"

namespace oceanbase {
namespace sql {
namespace dtl {


class ObDtlCacheBufferInfo : public common::ObDLinkBase<ObDtlCacheBufferInfo>
{
public:
  ObDtlCacheBufferInfo() :buffer_(nullptr), chid_(common::OB_INVALID_ID), ts_(0)
  {}

  ObDtlCacheBufferInfo(ObDtlLinkedBuffer *buffer, int64_t ts) :
    buffer_(buffer), chid_(common::OB_INVALID_ID), ts_(ts)
  {}

  void set_buffer(ObDtlLinkedBuffer *buffer);
  ObDtlLinkedBuffer *buffer() { return buffer_; }
  int64_t &ts() { return ts_; }
  int64_t &chid() { return chid_; }
  uint64_t get_key() const { return chid_; }
private:
  ObDtlLinkedBuffer *buffer_;
  int64_t chid_;
  int64_t ts_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDtlCacheBufferInfo);
};

class ObDtlBufferInfoManager
{
public:
  class ObDtlBufferInfoAllocator
  {
  public:
    ObDtlBufferInfoAllocator() :
      spin_lock_(common::ObLatchIds::DTL_CACHED_BUFFER_LIST_LOCK), allocator_(), free_list_()
    {}
    ~ObDtlBufferInfoAllocator();

    int init();
    void destroy();

    int alloc_buffer_info(int64_t chid, ObDtlCacheBufferInfo *&buffer_info);
    int free_buffer_info(ObDtlCacheBufferInfo *buffer_info);
  private:
    static const int64_t MAX_FREE_LIST_SIZE = 8;
    ObSpinLock spin_lock_;
    common::ObFIFOAllocator allocator_;
    ObDList<ObDtlCacheBufferInfo> free_list_;
  };
public:
  ObDtlBufferInfoManager(common::ObFIFOAllocator &allocator) :
    conrrent_allocators_(nullptr), allocator_(allocator)
  {}
  ~ObDtlBufferInfoManager() { destroy(); }

  int init();
  void destroy();

  int alloc_buffer_info(int64_t chid, ObDtlCacheBufferInfo *&buffer_info);
  int free_buffer_info(ObDtlCacheBufferInfo *buffer_info);
private:
  static int64_t get_hash_value(int64_t chid)
  {
    uint64_t val = common::murmurhash(&chid, sizeof(chid), 0);
    return val % ALLOCATOR_CNT;
  }
private:
  static const int64_t ALLOCATOR_CNT = 128;
  ObDtlBufferInfoAllocator *conrrent_allocators_;
  common::ObFIFOAllocator &allocator_;
};

template <class key_type, class value_type>
struct ob_dtl_key_value_fun
{
  key_type operator()(value_type &value) const
  {
    return value.get_key();
  }
};

template <class key_type, class value_type>
class ObDtlFirstBufferHashTableCell
{
public:
  ObDtlFirstBufferHashTableCell() :
    first_buffer_list_()
  {}
  ~ObDtlFirstBufferHashTableCell() { first_buffer_list_.reset(); }

  int init();
  int set_refactored(const key_type &key, value_type *val);
  int erase_refactored(const key_type &key, value_type *&val);
  template<typename callback_function>
  int get_refactored(const key_type &key, value_type *&val, callback_function &callback);

  int foreach_refactored(std::function<int(value_type *val)> op);
private:
  common::ObDList<value_type> first_buffer_list_;
  ob_dtl_key_value_fun<key_type, value_type> key_fun_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////
template <class key_type, class value_type>
int ObDtlFirstBufferHashTableCell<key_type, value_type>::init()
{
  int ret = OB_SUCCESS;
  first_buffer_list_.reset();
  SQL_DTL_LOG(DEBUG, "init first buffer list");
  return ret;
}

template <class key_type, class value_type>
int ObDtlFirstBufferHashTableCell<key_type, value_type>::set_refactored(const key_type &key, value_type *value)
{
  int ret = OB_SUCCESS;
  // first find channel by chid
  value_type *tmp_val = nullptr;
  if (0 < first_buffer_list_.get_size()) {
    DLIST_FOREACH(node, first_buffer_list_) {
      if (key == key_fun_(*node)) {
        tmp_val = node;
        break;
      }
    }
  }
  if (OB_NOT_NULL(tmp_val)) {
    ret = OB_HASH_EXIST;
  } else {
    first_buffer_list_.add_last(value);
  }
  return ret;
}

template <class key_type, class value_type>
int ObDtlFirstBufferHashTableCell<key_type, value_type>::erase_refactored(const key_type &key, value_type *&val)
{
  int ret = OB_SUCCESS;
  val = nullptr;
  if (0 < first_buffer_list_.get_size()) {
    DLIST_FOREACH_REMOVESAFE_X(node, first_buffer_list_, OB_SUCC(ret)) {
      if (key == key_fun_(*node)) {
        value_type *tmp = first_buffer_list_.remove(node);
        if (nullptr == tmp || tmp != node) {
          ret = OB_ERR_UNEXPECTED;
          SQL_DTL_LOG(WARN, "failed to remove channel", K(ret), K(key));
        } else {
          val = node;
        }
        break;
      }
    }
  }
  if (OB_ISNULL(val)) {
    ret = OB_HASH_NOT_EXIST;
  }
  return ret;
}

template <class key_type, class value_type>
template<typename callback_function>
int ObDtlFirstBufferHashTableCell<key_type, value_type>::get_refactored(const key_type &key, value_type *&val, callback_function &callback)
{
  int ret = OB_SUCCESS;
  val = nullptr;
  if (0 < first_buffer_list_.get_size()) {
    DLIST_FOREACH_X(node, first_buffer_list_, OB_SUCC(ret)) {
      if (key == key_fun_(*node)) {
        val = node;
        callback(val);
        break;
      }
    }
  }
  if (nullptr == val) {
    ret = OB_HASH_NOT_EXIST;
  }
  return ret;
}

template <class key_type, class value_type>
int ObDtlFirstBufferHashTableCell<key_type, value_type>::foreach_refactored(std::function<int(value_type *val)> op)
{
  int ret = OB_SUCCESS;
  if (0 < first_buffer_list_.get_size()) {
    DLIST_FOREACH_REMOVESAFE_X(node, first_buffer_list_, OB_SUCC(ret)) {
      if (OB_FAIL(op(node))) {
        SQL_DTL_LOG(WARN, "failed to refactor channel", K(ret));
      }
    }
    SQL_DTL_LOG(DEBUG, "trace hash table cell", K(first_buffer_list_.get_size()));
  }
  return ret;
}

class ObDtlFirstBufferConcurrentCell
{
public:
  ObDtlFirstBufferConcurrentCell(common::ObIAllocator &allocator) :
    n_lock_(0), lock_(nullptr), allocator_(allocator)
  {}
  ~ObDtlFirstBufferConcurrentCell() { reset(); }

  int init(int32_t n_lock);
  void reset();

  ObLatch &get_lock(int32_t idx);
private:
  int32_t n_lock_;
  ObLatch* lock_;
  common::ObIAllocator &allocator_;
};

template <class key_type, class value_type, class hash_function = common::hash::hash_func<key_type>>
class ObDtlFirstBufferHashTable
{
public:
  ObDtlFirstBufferHashTable(common::ObIAllocator &allocator) :
    bucket_num_(0), n_lock_(0),
    allocator_(allocator), concurrent_cell_(allocator), bucket_cells_(nullptr)
  {}
  ~ObDtlFirstBufferHashTable() { reset(); }

  void destroy() { reset(); }
  int init(int64_t bucket_num, int64_t n_lock);
  void reset();
  int set_refactored(const key_type &key, value_type *val);
  int erase_refactored(const key_type &key, value_type *&val);
  template<typename callback_function>
  int get_refactored(const key_type &key, value_type *&val, callback_function &callback);

  int foreach_refactored(std::function<int(value_type *val)> op);

  int64_t get_nth_lock(uint64_t hash_val) { return hash_val % n_lock_; }
  int64_t get_nth_bucket(uint64_t hash_val) { return hash_val % bucket_num_; }
  int64_t get_bucket_num() { return bucket_num_; }
private:
  int64_t bucket_num_;
  int64_t n_lock_;
  common::ObIAllocator &allocator_;
  ObDtlFirstBufferConcurrentCell concurrent_cell_;
  ObDtlFirstBufferHashTableCell<key_type, value_type>* bucket_cells_;
  hash_function hash_func_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////

template <class key_type, class value_type, class hash_function>
int ObDtlFirstBufferHashTable<key_type, value_type, hash_function>::init(int64_t bucket_num, int64_t n_conrrent_lock)
{
  int ret = OB_SUCCESS;
  bucket_num_ = bucket_num;
  n_lock_ = n_conrrent_lock;
  if (0 >= bucket_num || 0 >= n_conrrent_lock) {
    ret = OB_ERR_UNEXPECTED;
    SQL_DTL_LOG(WARN, "unexpected bucket number", K(bucket_num), K(n_conrrent_lock));
  } else if (OB_FAIL(concurrent_cell_.init(n_lock_))) {
    SQL_DTL_LOG(WARN, "failed to init concurrent cell");
  } else {
    char *buf = reinterpret_cast<char*>(allocator_.alloc(sizeof(ObDtlFirstBufferHashTableCell<key_type, value_type>) * bucket_num));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_DTL_LOG(WARN, "failed to alloc memory", K(ret));
    } else {
      bucket_cells_ = reinterpret_cast<ObDtlFirstBufferHashTableCell<key_type, value_type>*>(buf);
      for (int64_t i = 0; i < bucket_num; ++i) {
        ObDtlFirstBufferHashTableCell<key_type, value_type> *cell = new (buf) ObDtlFirstBufferHashTableCell<key_type, value_type>();
        if (OB_FAIL(cell->init())) {
          SQL_DTL_LOG(WARN, "failed to init hash table cell", K(ret));
        }
        buf += sizeof(ObDtlFirstBufferHashTableCell<key_type, value_type>);
      }
    }
  }
  return ret;
}

template <class key_type, class value_type, class hash_function>
void ObDtlFirstBufferHashTable<key_type, value_type, hash_function>::reset()
{
  concurrent_cell_.reset();
  if (OB_NOT_NULL(bucket_cells_)) {
    allocator_.free(bucket_cells_);
    bucket_cells_ = nullptr;
  }
}

template <class key_type, class value_type, class hash_function>
int ObDtlFirstBufferHashTable<key_type, value_type, hash_function>::set_refactored(const key_type &key, value_type *val)
{
  int ret = OB_SUCCESS;
  uint64_t hash_val = 0;
  if (OB_FAIL(hash_func_(key, hash_val))) {
    SQL_DTL_LOG(WARN, "failed to hash");
  } else {
    int64_t nth_lock = get_nth_lock(hash_val);
    int64_t nth_bucket = get_nth_bucket(hash_val);
    ObLatchWGuard guard(concurrent_cell_.get_lock(nth_lock), ObLatchIds::CONFIG_LOCK);
    ObDtlFirstBufferHashTableCell<key_type, value_type> &bucket_cell = bucket_cells_[nth_bucket];
    if (OB_FAIL(bucket_cell.set_refactored(key, val))) {
      SQL_DTL_LOG(WARN, "failed to set refactor");
    }
  }
  return ret;
}

template <class key_type, class value_type, class hash_function>
int ObDtlFirstBufferHashTable<key_type, value_type, hash_function>::erase_refactored(const key_type &key, value_type *&val)
{
  int ret = OB_SUCCESS;
  uint64_t hash_val = 0;
  if (OB_FAIL(hash_func_(key, hash_val))) {
    SQL_DTL_LOG(WARN, "failed to hash");
  } else {
    int64_t nth_lock = get_nth_lock(hash_val);
    int64_t nth_bucket = get_nth_bucket(hash_val);
    ObLatchWGuard guard(concurrent_cell_.get_lock(nth_lock), ObLatchIds::CONFIG_LOCK);
    ObDtlFirstBufferHashTableCell<key_type, value_type> &bucket_cell = bucket_cells_[nth_bucket];
    if (OB_FAIL(bucket_cell.erase_refactored(key, val))) {
      //SQL_DTL_LOG(WARN, "failed to set refactor");
    }
  }
  return ret;
}

template <class key_type, class value_type, class hash_function>
template<typename callback_function>
int ObDtlFirstBufferHashTable<key_type, value_type, hash_function>::get_refactored(const key_type &key, value_type *&val, callback_function &callback)
{
  int ret = OB_SUCCESS;
  uint64_t hash_val = 0;
  if (OB_FAIL(hash_func_(key, hash_val))) {
    SQL_DTL_LOG(WARN, "failed to hash");
  } else {
    int64_t nth_lock = get_nth_lock(hash_val);
    int64_t nth_bucket = get_nth_bucket(hash_val);
    ObLatchRGuard guard(concurrent_cell_.get_lock(nth_lock), ObLatchIds::CONFIG_LOCK);
    ObDtlFirstBufferHashTableCell<key_type, value_type> &bucket_cell = bucket_cells_[nth_bucket];
    if (OB_FAIL(bucket_cell.get_refactored(key, val, callback))) {
      // SQL_DTL_LOG(WARN, "failed to set refactor");
    }
  }
  return ret;
}

// one lock protect a batch element
template <class key_type, class value_type, class hash_function>
int ObDtlFirstBufferHashTable<key_type, value_type, hash_function>::foreach_refactored(std::function<int(value_type *val)> op)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < bucket_num_ && OB_SUCC(ret); ++i) {
    ObLatchRGuard guard(concurrent_cell_.get_lock(i % n_lock_), ObLatchIds::CONFIG_LOCK);
    ObDtlFirstBufferHashTableCell<key_type, value_type> &bucket_cell = bucket_cells_[i];
    if (OB_FAIL(bucket_cell.foreach_refactored(op))) {
      SQL_DTL_LOG(WARN, "failed to refactor all channels", K(ret), K(i), K(n_lock_));
    }
  }
  SQL_DTL_LOG(DEBUG, "trace refactor all elements", K(ret), K(bucket_num_));
  return ret;
}

class ObDtlLocalFirstBufferCache : public common::ObDLinkBase<ObDtlLocalFirstBufferCache>
{
public:
  class ObDtlBufferClean
  {
  public:
    ObDtlBufferClean(ObDtlTenantMemManager *tenant_mem_mgr, ObDtlBufferInfoManager *buffer_info_mgr) :
      tenant_mem_mgr_(tenant_mem_mgr), buffer_info_mgr_(buffer_info_mgr)
    {}
    ~ObDtlBufferClean() {}
    int operator()(sql::dtl::ObDtlCacheBufferInfo *cache_buffer);
  private:
    ObDtlTenantMemManager *tenant_mem_mgr_;
    ObDtlBufferInfoManager *buffer_info_mgr_;
  };
public:
  ObDtlLocalFirstBufferCache(common::ObIAllocator &allocator) :
    pins_(0), dfo_key_(), buffer_map_(allocator), tenant_mem_mgr_(nullptr),
    buffer_info_mgr_(nullptr), registered_(false), bitset_cnt_(0), first_buffer_cnt_(0)
  {}
  ~ObDtlLocalFirstBufferCache() { destroy(); }

  int init(int64_t bucket_num = CHANNEL_HASH_BUCKET_NUM, int64_t concurrent_cnt = CONCURRENT_CNT);
  void destroy();
  int clean();

  int cache_buffer(ObDtlCacheBufferInfo *&buffer);
  int get_buffer(int64_t chid, ObDtlCacheBufferInfo *&buffer);

  void set_buffer_mgr(ObDtlTenantMemManager *tenant_mem_mgr, ObDtlBufferInfoManager *buffer_info_mgr)
  {
    tenant_mem_mgr_ = tenant_mem_mgr;
    buffer_info_mgr_ = buffer_info_mgr;
  }

  OB_INLINE void inc_first_buffer_cnt() { ATOMIC_INC(&first_buffer_cnt_); }
  OB_INLINE void dec_first_buffer_cnt() { ATOMIC_DEC(&first_buffer_cnt_); }
  OB_INLINE int64_t get_first_buffer_cnt() const { return ATOMIC_LOAD(&first_buffer_cnt_); }

  OB_INLINE int64_t get_pins() const { return ATOMIC_LOAD(&pins_); }
  OB_INLINE int64_t pin() { return ATOMIC_AAF(&pins_, 1); }
  void release();
  void set_first_buffer_key(ObDtlDfoKey &key) { dfo_key_ = key; }
  ObDtlDfoKey &get_first_buffer_key() { return dfo_key_; }
  ObDtlDfoKey get_key() const { return dfo_key_; }
  void set_registered() { registered_ = true; }
  bool is_registered() { return registered_; }

  ObDtlFirstBufferHashTable<uint64_t, ObDtlCacheBufferInfo> &get_buffer_map() { return buffer_map_; }

  static int64_t get_hash_value(int64_t chid)
  {
    uint64_t val = common::murmurhash(&chid, sizeof(chid), 0);
    return val % CHANNEL_HASH_BUCKET_NUM;
  }

  int set_first_buffer(int64_t n)
  {
    int ret = common::OB_SUCCESS;
    int64_t index = n & (bitset_cnt_ - 1);
    if (0 > index || bitset_cnt_ - 1 < index) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(filter_bitset_.add_atomic_member(index))) { // must be atomic
      SQL_DTL_LOG(WARN, "failed to set first buffer", K(n), KP(n), K(index), K(bitset_cnt_));
    } else {
      SQL_DTL_LOG(DEBUG, "trace set first buffer", K(n), KP(n), K(index), K(bitset_cnt_),
        K(filter_bitset_.bit_count()), K(filter_bitset_.has_member(index)));
    }
    return ret;
  }
  int has_first_buffer(int64_t n, bool &has_first_buffer)
  {
    int ret = common::OB_SUCCESS;
    int64_t index = n & (bitset_cnt_ - 1);
    has_first_buffer = false;
    if (0 > index || bitset_cnt_ - 1 < index) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      has_first_buffer = filter_bitset_.has_member(index);
      SQL_DTL_LOG(DEBUG, "trace has first buffer", K(n), KP(n), K(index), K(bitset_cnt_),
        K(has_first_buffer), K(filter_bitset_.bit_count()));
    }
    return ret;
  }
private:
  OB_INLINE int64_t unpin()
  {
    return ATOMIC_SAF(&pins_, 1);
  }

private:
  static const int64_t CONCURRENT_CNT = 2048;
  static const int64_t CHANNEL_HASH_BUCKET_NUM = 64 * 1024;
  static const int64_t MAX_BITSET_CNT = 1024 * 1024; // 1024 * 1024
  static const int64_t TP_ENABLE_FAILED_SET_HT = -17;
  static const int64_t TP_ENABLE_FAILED_SET_FIRST_BUFFER = -18;
  int64_t pins_;
  ObDtlDfoKey dfo_key_;
  ObDtlFirstBufferHashTable<uint64_t, ObDtlCacheBufferInfo> buffer_map_;
  ObDtlTenantMemManager *tenant_mem_mgr_;
  ObDtlBufferInfoManager *buffer_info_mgr_;
  bool registered_;
  common::ObBitSet<16, common::ModulePageAllocator> filter_bitset_;
  int64_t bitset_cnt_;
  int64_t first_buffer_cnt_;
};

class ObDtlLocalFirstBufferCacheManager
{
public:
  ObDtlLocalFirstBufferCacheManager(uint64_t tenant_id, ObDtlTenantMemManager *tenant_mem_mgr);
  ~ObDtlLocalFirstBufferCacheManager();

  virtual int init();
  virtual void destroy();

  int cache_buffer(int64_t chid, ObDtlLinkedBuffer *&data_buffer, bool attach);
  int get_buffer_cache(ObDtlDfoKey &key, ObDtlLocalFirstBufferCache *&buf_cache);

  int get_cached_buffer(ObDtlLocalFirstBufferCache *buf_cache, int64_t chid, ObDtlLinkedBuffer *&linked_buffer);
  int erase_buffer_cache(ObDtlDfoKey &key, ObDtlLocalFirstBufferCache *&buf_cache);

  int register_first_buffer_cache(ObDtlLocalFirstBufferCache *buf_cache);
  int unregister_first_buffer_cache(ObDtlDfoKey &key, ObDtlLocalFirstBufferCache *org_buf_cache);
  int clean_first_buffer_cache(ObDtlDfoKey &key);
  
  ObDtlFirstBufferHashTable<ObDtlDfoKey, ObDtlLocalFirstBufferCache> &get_buffer_hash_table() { return hash_table_; }
private:
  //通过128个散列处理并发
  static const int64_t CONCURRENT_CNT = 1024;
  static const int64_t BUCKET_NUM = 64 * 1024;
  static const int64_t TP_ENABLE_FAILED_ALLOC_MEM = -16;
  uint64_t tenant_id_;
  common::ObFIFOAllocator allocator_;
  ObDtlTenantMemManager *tenant_mem_mgr_;
  ObDtlBufferInfoManager buffer_info_mgr_;
  ObDtlFirstBufferHashTable<ObDtlDfoKey, ObDtlLocalFirstBufferCache> hash_table_;
};

} // dtl
} // sql
} // oceanbase

#endif /* OB_DTL_LOCAL_FIRST_BUFFER_MANAGER_H */
