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

#ifndef __OB_CONNECTION_ALLOCATOR_H__
#define __OB_CONNECTION_ALLOCATOR_H__ 1

#include "lib/objectpool/ob_pool.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/container/ob_array.h"
#include "lib/list/ob_list.h"
#include "lib/hash/ob_hashmap.h"
namespace oceanbase
{
namespace common
{

// @note thread-safe
template <typename T>
class ObIConnectionAllocator
{
public:
  ObIConnectionAllocator()
      : lock_(common::ObLatchIds::INNER_CONN_POOL_LOCK), pool_(sizeof(T))
  {}
  virtual ~ObIConnectionAllocator() {}

  virtual int alloc(T *&conn, uint32_t sessid = 0) = 0;
  virtual int get_cached(T *&conn, uint32_t sessid = 0) = 0;
  virtual int put_cached(T *conn, uint32_t sessid = 0) = 0;
protected:
  inline void free(T *conn)
  {
    if (NULL != conn) {
      conn->~T();
      pool_.free(conn);
    }
  }
protected:
  // data members
  ObSpinLock lock_;
  ObPool<> pool_;
};

template <typename T>
class ObSimpleConnectionAllocator : public ObIConnectionAllocator<T>
{
public:
  ObSimpleConnectionAllocator() {}
  ~ObSimpleConnectionAllocator();

  virtual int alloc(T *&conn, uint32_t sessid = 0) override;
  virtual int get_cached(T *&conn, uint32_t sessid = 0) override;
  virtual int put_cached(T *conn, uint32_t sessid = 0) override;
  template<typename Function> void for_each(Function &fn);
private:
  // disallow copy
  ObSimpleConnectionAllocator(const ObSimpleConnectionAllocator &other);
  ObSimpleConnectionAllocator &operator=(const ObSimpleConnectionAllocator &other);
private:
  // data members
  ObArray<T *> cached_objs_;
};

template<typename T>
ObSimpleConnectionAllocator<T>::~ObSimpleConnectionAllocator()
{
  _OB_LOG(DEBUG, "free cached conn, size=%ld", cached_objs_.size());
  T *conn = NULL;
  while (OB_SUCCESS == cached_objs_.pop_back(conn)) {
    ObSimpleConnectionAllocator<T>::free(conn);
  }
}

template<typename T>
int ObSimpleConnectionAllocator<T>::alloc(T *&conn, uint32_t sessid)
{
  UNUSED(sessid);
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(ObIConnectionAllocator<T>::lock_);
  conn = NULL;
  if (OB_SUCCESS != cached_objs_.pop_back(conn)) {
    void* ptr = NULL;
    ptr = ObIConnectionAllocator<T>::pool_.alloc();
    if (NULL == ptr) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      _OB_LOG(ERROR, "no memory, ret=%d", ret);
    } else {
      conn = new(ptr) T();
    }
  } else if (NULL == conn) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    // exist conn, don't need alloc
    ret = OB_ERR_ALREADY_EXISTS;
    _OB_LOG(DEBUG, "conn already exists, sessid=%u, ret=%d", sessid, ret);
  }
  return ret;
}

template<typename T>
int ObSimpleConnectionAllocator<T>::get_cached(T *&conn, uint32_t sessid)
{
  UNUSED(sessid);
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(ObIConnectionAllocator<T>::lock_);
  conn = NULL;
  if (OB_FAIL(cached_objs_.pop_back(conn))) {
    _OB_LOG(WARN, "cached_objs_ pop_back failed, ret=%d", ret);
  } else if (NULL == conn) {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

template<typename T>
int ObSimpleConnectionAllocator<T>::put_cached(T *conn, uint32_t sessid)
{
  UNUSED(sessid);
  int ret = OB_SUCCESS;
  if (NULL != conn) {
    ObSpinLockGuard guard(ObIConnectionAllocator<T>::lock_);
    if (OB_FAIL(cached_objs_.push_back(conn))) {
      _OB_LOG(WARN, "failed to push conn into cached_objs_, ret=%d", ret);
      ObSimpleConnectionAllocator<T>::free(conn);
    } else {
      conn->reset();
    }
  }
  return ret;
}

template<typename T>
template<typename Function>
void ObSimpleConnectionAllocator<T>::for_each(Function &fn)
{
  const int64_t size = cached_objs_.size();
  for(int i = 0; i < size; i++) {
    T *obj = cached_objs_.at(i);
    if (OB_NOT_NULL(obj)) {
      fn(*obj);
    }
  }
}

template <typename T>
class ObLruConnectionAllocator : public ObIConnectionAllocator<T>
{
private:
  struct LruNode {
    LruNode() : conn_(NULL), sessid_(0) {}

    LruNode(T *conn, uint32_t sessid) : conn_(conn), sessid_(sessid) {}

    bool operator ==(const LruNode &node)
    {
      return (conn_ == node.conn_) && (sessid_ = node.sessid_);
    }

    T *conn_;
    uint32_t sessid_;
  };

  struct FreeConnsOp
  {
    explicit FreeConnsOp(ObLruConnectionAllocator<T> *free_ptr)
    {
      free_ptr_ = free_ptr;
    }

    inline int operator()(common::hash::HashMapPair<uint32_t, ObArray<T *>> &entry)
    {
      int ret = OB_SUCCESS;
      _OB_LOG(DEBUG, "free conns in session, sessid=%u", entry.first);
      T *conn = NULL;
      if (NULL == free_ptr_) {
        _OB_LOG(WARN, "free ptr_ is NULL, ret=%d", ret);
      } else {
        while (OB_SUCCESS == entry.second.pop_back(conn)) {
          free_ptr_->free(conn);
        }
      }
      return ret;
    }

    ObLruConnectionAllocator<T> *free_ptr_;
  };
public:
  ObLruConnectionAllocator()
      : is_inited_(false),
        is_session_share_conns_(false),
        allocator_("LruConnAlloc"),
        lru_list_(allocator_)
  {}

  virtual ~ObLruConnectionAllocator()
  {
    T *conn = NULL;
    while (OB_SUCCESS == free_conn_array_.pop_back(conn)) {
      ObLruConnectionAllocator<T>::free(conn);
    }
    if (is_inited_) {
      FreeConnsOp op(this);
      if (OB_SUCCESS != sessionid_to_conns_map_.foreach_refactored(op)) {
        _OB_LOG_RET(WARN, common::OB_ERR_UNEXPECTED, "failed to foreach sessionid_to_conns_map_");
      }
      sessionid_to_conns_map_.destroy();
    }
  }

  int init();
  // fail_recycled_conn_count means count of connection that cannot be recycled to the free_conn_array,
  // and such a connection will be directly freed.
  int free_session_conn_array(uint32_t sessid, int64_t &fail_recycled_conn_count, int64_t &succ_recycled_conn_count);

  virtual int alloc(T *&conn, uint32_t sessid = 0) override;
  virtual int get_cached(T *&conn, uint32_t sessid = 0) override;
  virtual int put_cached(T *conn, uint32_t sessid = 0) override;

private:
  // disallow copy
  ObLruConnectionAllocator(const ObLruConnectionAllocator &other);
  ObLruConnectionAllocator &operator=(const ObLruConnectionAllocator &other);

  inline int get_session_conn_array(uint32_t sessid,
                            ObArray<T *> *&conn_array);
  int conn_array_erase(ObArray<T *>* conn_array, T *conn);
  int lru_list_push_back(T *conn, uint32_t sessid);
  int lru_list_pop_front(T *&conn, uint32_t &sessid);
  int lru_list_erase(T *conn, uint32_t sessid);
private:
  // data members
  bool is_inited_;
  // When this value is true, it means that the free connection belonging
  // to a session will not be closed when it is allocated to other sessions.
  bool is_session_share_conns_;
  ObMalloc allocator_;
  /**
    Use list as a lru cache, it is convenient for us to find Least Recently Used connection.

    Why not use ObDList?
    Because ObDList does not have a suitable interface function to
    remove any node in the linked listBecause ObDList.
   */
  ObList<LruNode> lru_list_;
  // Store free connection that does not belong to any session.
  ObArray<T *> free_conn_array_;
  // Each session has an array for storing free conn belonging to this session
  // Use session id to manage array
  hash::ObHashMap<uint32_t, ObArray<T *>> sessionid_to_conns_map_;
  static int64_t HASH_BUCKET_NUM;
};

template<typename T> int64_t ObLruConnectionAllocator<T>::HASH_BUCKET_NUM = 64L;

template<typename T>
int ObLruConnectionAllocator<T>::get_session_conn_array(uint32_t sessid,
                            ObArray<T *> *&conn_array)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(is_inited_)) {
    conn_array = NULL;
    conn_array = const_cast<ObArray<T *> *>(sessionid_to_conns_map_.get(sessid));
    if (OB_ISNULL(conn_array)) {
      ObArray<T *> temp_conn_array;
      if (OB_FAIL(sessionid_to_conns_map_.set_refactored(sessid, temp_conn_array))) {
        _OB_LOG(WARN, "failed to set refactored, sessid=%u, ret=%d", sessid, ret);
      } else {
        conn_array = const_cast<ObArray<T *> *>(sessionid_to_conns_map_.get(sessid));
        if (OB_ISNULL(conn_array)) {
          ret = OB_ERR_UNEXPECTED;
          _OB_LOG(WARN, "failed to get refactored, sessid=%u, ret=%d", sessid, ret);
        }
      }
    }
  } else {
    ret = OB_NOT_INIT;
  }
  return ret;
}

template<typename T>
int ObLruConnectionAllocator<T>::lru_list_push_back(T *conn, uint32_t sessid)
{
  int ret = OB_SUCCESS;
  LruNode temp_node(conn, sessid);
  if (OB_FAIL(lru_list_.push_back(temp_node))) {
    _OB_LOG(WARN, "lru_list_ push_back() error, sessid=%u, ret=%d", sessid, ret);
  } else {
     _OB_LOG(DEBUG, "lru_list_ push_back() succ, sessid=%u, ret=%d", sessid, ret);
  }
  return ret;
}

template<typename T>
int ObLruConnectionAllocator<T>::lru_list_pop_front(T *&conn, uint32_t &sessid)
{
  int ret = OB_SUCCESS;
  LruNode least_used_node;
  if (OB_FAIL(lru_list_.pop_front(least_used_node))) {
    conn = NULL;
    _OB_LOG(WARN, "lru_list_ pop_front() error, sessid=%u, ret=%d", sessid, ret);
  } else if (NULL == least_used_node.conn_) {
    ret = OB_ERR_UNEXPECTED;
    _OB_LOG(WARN, "connection ptr is NULL, sessid=%u, ret=%d", sessid, ret);
  } else {
    conn = least_used_node.conn_;
    sessid = least_used_node.sessid_;
  }
  return ret;
}

template<typename T>
int ObLruConnectionAllocator<T>::lru_list_erase(T *conn, uint32_t sessid)
{
  int ret = OB_SUCCESS;
  LruNode erase_node(conn, sessid);
  if (OB_FAIL(lru_list_.erase(erase_node))) {
    _OB_LOG(WARN, "lru_list_ erase() error, ret=%d", ret);
  }
  return ret;
}

template<typename T>
int ObLruConnectionAllocator<T>::conn_array_erase(ObArray<T *>* conn_array, T *conn)
{
  int ret = OB_SUCCESS;
  int64_t erase_idx = 0;
  int64_t size = conn_array->size();
  for(; erase_idx < size; ++erase_idx) {
    if (conn == conn_array->at(erase_idx)) {
      break;
    }
  }
  ret = conn_array->remove(erase_idx);
  return ret;
}

template<typename T>
int ObLruConnectionAllocator<T>::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    // do nothing
  } else if (OB_FAIL(sessionid_to_conns_map_.create(HASH_BUCKET_NUM, SET_IGNORE_MEM_VERSION("sessid_conn_map")))) {
    _OB_LOG(WARN, "failed to init hashmap sessionid_to_conns_map_, ret=%d", ret);
  } else {
    is_inited_ = true;
  }
  return ret;
}

template<typename T>
int ObLruConnectionAllocator<T>::free_session_conn_array(uint32_t sessid, int64_t &fail_recycled_conn_count, int64_t &succ_recycled_conn_count)
{
  int ret = OB_SUCCESS;
  _OB_LOG(DEBUG, "free session conn array sessid=%u, ret=%d", sessid, ret);
  ObSpinLockGuard guard(ObIConnectionAllocator<T>::lock_);
  ObArray<T *> *conn_array = NULL;
  fail_recycled_conn_count = 0;
  succ_recycled_conn_count = 0;
  if (OB_FAIL(get_session_conn_array(sessid, conn_array))) {
    _OB_LOG(WARN, "failed to get conn_array, sessid=%u, ret=%d", sessid, ret);
  } else {
    T *conn = NULL;
    int64_t array_size = conn_array->size();
    int64_t succ_count = 0;
    _OB_LOG(DEBUG, "try to free conn_array, conn_array=%p, count=%ld", conn_array, array_size);
    while (OB_SUCC(ret) && OB_SUCCESS == conn_array->pop_back(conn)) {
      conn->close(); //close immedately
      _OB_LOG(TRACE, "close connection, conn=%p", conn);
      if (OB_FAIL(free_conn_array_.push_back(conn))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        _OB_LOG(WARN, "push_back conn to free_conn_array_ failed, sessid=%u, ret=%d", sessid, ret);
        ObLruConnectionAllocator<T>::free(conn);
      } else {
        ++succ_count;
      }
    }
    _OB_LOG(DEBUG, "free session conn array succ_count=%ld, ret=%d", succ_count, ret);
    succ_recycled_conn_count = succ_count;
    if (OB_FAIL(ret)) {
      fail_recycled_conn_count = array_size - succ_count;
      while (OB_SUCCESS == conn_array->pop_back(conn)) {
        conn->close(); //close immedately
        _OB_LOG(TRACE, "close connection, conn=%p", conn);
        ObLruConnectionAllocator<T>::free(conn);
      }
      if (OB_SUCCESS != sessionid_to_conns_map_.erase_refactored(sessid)) {
        _OB_LOG(WARN, "failed to erase refactored, sessid=%u, ret=%d", sessid, ret);
      }
    } else if (OB_FAIL(sessionid_to_conns_map_.erase_refactored(sessid))) {
      _OB_LOG(WARN, "failed to erase refactored, sessid=%u, ret=%d", sessid, ret);
    }
  }
  return ret;
}

template<typename T>
int ObLruConnectionAllocator<T>::alloc(T *&conn, uint32_t sessid)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(ObIConnectionAllocator<T>::lock_);
  conn = NULL;
  ObArray<T *> *conn_array = NULL;
  if (OB_FAIL(get_session_conn_array(sessid, conn_array))) {
    _OB_LOG(WARN, "failed to get conn_array, sessid=%u", sessid);
  } else if (OB_SUCCESS != conn_array->pop_back(conn)) {
    void *p = ObIConnectionAllocator<T>::pool_.alloc();
    if (NULL == p) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      _OB_LOG(ERROR, "no memory");
    } else {
      conn = new(p) T();
    }
  } else if (OB_UNLIKELY(NULL == conn)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(lru_list_erase(conn, sessid))) { //get conn from lru_list_'s front, delete node in lru_list_ respectly.
    ObLruConnectionAllocator<T>::free(conn);
    _OB_LOG(WARN, "lru_list failed to erase, sessid=%u, ret=%d", sessid, ret);
  } else {
    //exist conn, don't need alloc
    ret = OB_ERR_ALREADY_EXISTS;
    _OB_LOG(DEBUG, "conn already exists, sessid=%u, ret=%d", sessid, ret);
  }
  return ret;
}

template<typename T>
int ObLruConnectionAllocator<T>::get_cached(T *&conn, uint32_t sessid)
{
  int ret = OB_SUCCESS;
  ObSpinLockGuard guard(ObIConnectionAllocator<T>::lock_);
  conn = NULL;
  ObArray<T *> *conn_array = NULL;
  if (OB_FAIL(get_session_conn_array(sessid, conn_array))) {
    _OB_LOG(WARN, "failed to get conn_array, sessid=%u", sessid);
  } else if(OB_SUCCESS != conn_array->pop_back(conn)) {
    uint32_t other_sessid = 0;
    // if conn_array does not have conn
    // get_cached() will try find conn in free_conn_array_.
    // if free_conn_array_ does not have conn,
    // will get a conn from other session's conn_array
    // acording to lru_list_'s least used conn
    if (OB_SUCCESS == free_conn_array_.pop_back(conn)) { // free_conn_array_ have conn
      if (NULL == conn) {
        ret = OB_ERR_UNEXPECTED;
        _OB_LOG(WARN, "connection ptr is NULL, sessid=%u, ret=%d", sessid, ret);
      } else if (OB_FAIL(lru_list_erase(conn, sessid))){  // delete respective node from lru list.
        ObLruConnectionAllocator<T>::free(conn);
        _OB_LOG(WARN, "lru_list failed to erase, sessid=%u, ret=%d", sessid, ret);
      }
    } else if (OB_FAIL(lru_list_pop_front(conn, other_sessid))) {
      // do nothing
      _OB_LOG(WARN, "lru_list failed to pop_front, sessid=%u, ret=%d", sessid, ret);
    } else if (OB_FAIL(get_session_conn_array(other_sessid, conn_array))) { // get a conn from other session's conn_array
      ObLruConnectionAllocator<T>::free(conn);
      _OB_LOG(WARN, "failed to get conn_array from session, other_sessid=%u, sessid=%u, ret=%d", other_sessid, sessid, ret);
    } else if (OB_FAIL(conn_array_erase(conn_array, conn))) {
      ObLruConnectionAllocator<T>::free(conn);
      _OB_LOG(WARN, "failed to erase conn from conn_array, other_sessid=%u, sessid=%u, ret=%d", other_sessid, sessid, ret);
    } else if (!is_session_share_conns_) {
      conn->set_sessid(0);
      // get conn from other seesion, set sessid as 0,
      // and will close it at ObServerConnectionPool::acquire() and int ObOciServerConnectionPool::acquire(ObOciConnection *&conn, uint32_t sessid).
      _OB_LOG(TRACE, "get conn from other seesion, other_sessid=%u, sessid=%u, ret=%d", other_sessid, sessid, ret);
    }
  } else if (NULL == conn) {
    ret = OB_ERR_UNEXPECTED;
    _OB_LOG(WARN, "conn is NULL, sessid=%u, ret=%d", sessid, ret);
  } else if (OB_FAIL(lru_list_erase(conn, sessid))){
    ObLruConnectionAllocator<T>::free(conn);
    _OB_LOG(WARN, "failed to erase conn lru_list, sessid=%u, ret=%d", sessid, ret);
  } else {
    // do nothing.
  }
  _OB_LOG(DEBUG, "get_cached, sessid=%u, ret=%d", sessid, ret);
  return ret;
}

template<typename T>
int ObLruConnectionAllocator<T>::put_cached(T *conn, uint32_t sessid)
{
  int ret = OB_SUCCESS;
  if (NULL != conn) {
    ObSpinLockGuard guard(ObIConnectionAllocator<T>::lock_);
    ObArray<T *> *conn_array = NULL;
    if (OB_FAIL(get_session_conn_array(sessid, conn_array))) {
      ObLruConnectionAllocator<T>::free(conn);
      _OB_LOG(WARN, "failed get conn_array, sessid=%u, ret=%d", sessid, ret);
    } else if (OB_FAIL(conn_array->push_back(conn))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      _OB_LOG(WARN, "failed to push_back conn into conn_array, ret=%d", ret);
      ObLruConnectionAllocator<T>::free(conn);
    } else if (OB_FAIL(lru_list_push_back(conn, sessid))) {
      _OB_LOG(WARN, "failed to push conn into lru list, ret=%d", ret);
      if (OB_SUCCESS != conn_array->pop_back(conn)) {
        _OB_LOG(WARN, "failed to pop conn from conn_array, sessid=%u, ret=%d", sessid, ret);
      }
      ObLruConnectionAllocator<T>::free(conn);
    } else {
      conn->reset();
    }
  }
  _OB_LOG(TRACE, "put_cached, sessid=%u, ret=%d", sessid, ret);
  return ret;
}

} // end namespace common
} // end namespace oceanbase

#endif /* __OB_CONNECTION_ALLOCATOR_H__ */
