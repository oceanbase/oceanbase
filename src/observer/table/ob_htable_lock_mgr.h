/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OBSERVER_TABLE_OB_HTABLE_LOCK_MGR_H
#define OBSERVER_TABLE_OB_HTABLE_LOCK_MGR_H
#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/lock/ob_latch.h"
#include "storage/tx/ob_trans_define.h"

namespace oceanbase
{
namespace table
{

enum ObHTableLockMode
{
  SHARED, // for put, delete,
  EXCLUSIVE // for check_and_mutate, increment, append
};

struct ObHTableLockKey
{
  ObHTableLockKey() : table_id_(common::OB_INVALID_ID), key_() {};
  ObHTableLockKey(uint64_t table_id, common::ObString key) : table_id_(table_id), key_(key) {}
  ~ObHTableLockKey()
  {
    table_id_ = common::OB_INVALID_ID;
    key_.reset();
  }
  bool is_valid() { return table_id_ != common::OB_INVALID_ID && !key_.empty(); }
  uint64_t hash() const
  {
    int64_t hash_value = key_.hash();
    hash_value = common::murmurhash(&table_id_, sizeof(table_id_), hash_value);
    return hash_value;
  }
  inline int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }
  bool operator==(const ObHTableLockKey &other) const;
  TO_STRING_KV(K_(table_id), K_(key));

  uint64_t table_id_;
  common::ObString key_;
};

//  need to release all lock nodes it held before a lock handle is released
class ObHTableLockNode
{
  friend class ObHTableLockMgr;
  friend class ObHTableLockHandle;
public:
  explicit ObHTableLockNode(ObHTableLockMode lock_mode)
  : lock_mode_(lock_mode),
    lock_key_(nullptr),
    next_(nullptr)
  {}
  ~ObHTableLockNode() {}
  TO_STRING_KV(K_(lock_mode), KPC_(lock_key), KPC_(next));
  void set_lock_key(ObHTableLockKey *lock_key) { lock_key_ = lock_key; }
  void set_lock_mode(ObHTableLockMode lock_mode) { lock_mode_ = lock_mode; }
  ObHTableLockKey *get_lock_key() { return lock_key_; }
  ObHTableLockMode get_lock_mode() { return lock_mode_; }

private:
  ObHTableLockMode lock_mode_;
  ObHTableLockKey *lock_key_;
  ObHTableLockNode *next_;
};

// records all htable locks held by a transaction
// after the transaction end, all the lock held by the lock handle should be released
class ObHTableLockHandle
{
  friend class ObHTableLockMgr;
public:
  ObHTableLockHandle() : tx_id_(0), lock_nodes_(nullptr) {}

  ObHTableLockHandle(const transaction::ObTransID &tx_id) : tx_id_(tx_id), lock_nodes_(nullptr) {}

  ~ObHTableLockHandle()
  {
    tx_id_.reset();
    lock_nodes_ = nullptr;
  }
  void add_lock_node(ObHTableLockNode &lock_node);
  int find_lock_node(uint64_t table_id, const common::ObString &key, ObHTableLockNode *&lock_node);

  void set_tx_id(transaction::ObTransID tx_id) {
      tx_id_ = tx_id;
  }

private:
  transaction::ObTransID tx_id_;
  ObHTableLockNode *lock_nodes_;
};

class ObHTableLock
{
public:
  ObHTableLock() {}
  ~ObHTableLock() {}

  /* try_rdlock - try to add read lock
   *
   * for read lock, ObHTableLock only record ref count, ObHTableHandle record all locks it holds
   *
   * return:
   * OB_SUCCESS - OK
   * OB_TRY_LOCK_ROW_CONFLICT - if lock conflict
   */
  int try_rdlock();

  /* try_wrlock - try to add write lock
   *
   * return:
   * OB_SUCCESS - OK
   * OB_TRY_LOCK_ROW_CONFLICT - if lock conflict
   */
  int try_wrlock();

  /* unlock: unlock htable read/write lock
   *
   * return:
   * OB_SUCCESS - OK
   * OB_ERR_UNEXPECTED - doesn't hold by anyone currently
   */
  int unlock();

  OB_INLINE bool can_escalate_lock() { return latch_.is_rdlocked() && latch_.get_rdcnt() == 1; }
  OB_INLINE bool is_locked() { return latch_.is_locked(); }
  OB_INLINE bool is_rdlocked() { return latch_.is_rdlocked(); }
  OB_INLINE bool is_wrlocked() { return latch_.is_wrlocked(); }
private:
  common::ObLatch latch_;
};

class ObHTableLockMgr
{
public:
  ObHTableLockMgr() : is_inited_(false), spin_lock_(), lock_map_(), allocator_(MTL_ID()) {}
  ~ObHTableLockMgr() {}
  /**
   * acquire_handle - acquire htable lock handle
   *
   * this is the start of lifecycle of htable lock inside a transaction
   * Notice: use acquire_handle without tx_id must set tx_id later!
   *
   * @handle:   the htable lock handle returned
   *
   * Return:
   * OB_SUCCESS - OK
   */
  int acquire_handle(ObHTableLockHandle *&handle);

  /**
   * acquire_handle - acquire htable lock handle
   *
   * this is the start of lifecycle of htable lock inside a transaction
   *
   * @handle:   the htable lock handle returned
   *
   * Return:
   * OB_SUCCESS - OK
   */
  int acquire_handle(const transaction::ObTransID &tx_id, ObHTableLockHandle *&handle);

  /**
   * lock_row - lock the row in htable model
   *
   * @table_id:   the table(column family) where the row is to be locked
   * @key:        the target htable rowkey
   * @mode:       share or exclusive lock mode
   * @handle:     the target htable lock handle
   *
   * Return:
   * OB_SUCCESS - OK
   * OB_TRY_LOCK_ROW_CONFLICT - if lock conflict, need rollback transaction outside
   */
  int lock_row(const uint64_t table_id, const common::ObString& key, ObHTableLockMode mode, ObHTableLockHandle &handle);

  /**
   * release_handle - release the htable lock handle
   *
   * this is the end of lifecycle of htable lock inside a transaction
   * the HTableLockHandle object should not been access anymore after release.
   *
   * @handle:     the target htable lock handle
   *
   * Returns:
   * OB_SUCCESS - OK
   */
  int release_handle(ObHTableLockHandle &handle);
  static int mtl_init(ObHTableLockMgr *&htable_lock_mgr);
  static void mtl_destroy(ObHTableLockMgr *&htable_lock_mgr);
private:
  int init();
  int alloc_lock_node(ObHTableLockMode lock_mode, ObHTableLockNode *&lock_node);
  int alloc_lock_key(const uint64_t table_id, const common::ObString &key, ObHTableLockKey *&lock_key);
  int alloc_lock(ObHTableLockMode lock_mode, ObHTableLock *&lock);
  int internal_lock_row(const uint64_t table_id, const common::ObString& key, ObHTableLockMode lock_mode, ObHTableLockHandle &handle);
  // escalate read lock to write lock
  int rd2wrlock(ObHTableLockNode &lock_node);
  int release_node(ObHTableLockNode &lock_node);
private:
  static const uint64_t DEFAULT_BUCKET_NUM = 20480;
  typedef common::hash::ObHashMap<ObHTableLockKey *, ObHTableLock *, common::hash::ReadWriteDefendMode> ObHTableLockMap;

  bool is_inited_;
  common::SpinRWLock spin_lock_;
  ObHTableLockMap lock_map_;
  // alloc memory for ObHTableLockKey, ObHTableLockHandle, ObHTableLock, need free
  common::ObFIFOAllocator allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObHTableLockMgr);
};

// execute lock operation
class ObHTableLockOp
{
public:
  explicit ObHTableLockOp(ObHTableLockMode lock_mode)
  : lock_mode_(lock_mode),
    old_lock_key_(nullptr),
    is_called_(false),
    ret_code_(common::OB_SUCCESS)
  {}
  virtual ~ObHTableLockOp() {}
  void operator() (common::hash::HashMapPair<ObHTableLockKey *, ObHTableLock *> &entry);
  ObHTableLockKey *get_lock_key() { return old_lock_key_; }
  bool is_called() { return is_called_; }
  int get_ret() { return ret_code_; }

private:
  ObHTableLockMode lock_mode_;
  ObHTableLockKey *old_lock_key_;
  bool is_called_; // indicate whether this op was called or not
  bool lock_success_; // indicate whether lock op is success or not
  int ret_code_;
  DISALLOW_COPY_AND_ASSIGN(ObHTableLockOp);
};

// execute unlock operation firstly, and erase the lock pair from the map if no one hold it anymore
class ObHTableUnLockOpPred
{
public:
  explicit ObHTableUnLockOpPred() : ret_code_(common::OB_SUCCESS) {}
  virtual ~ObHTableUnLockOpPred() {}
  bool operator() (common::hash::HashMapPair<ObHTableLockKey *, ObHTableLock *> &entry);
  int get_ret() { return ret_code_; }

private:
  int ret_code_;
  DISALLOW_COPY_AND_ASSIGN(ObHTableUnLockOpPred);
};

// try to execute lock escalation operation
class ObHTableRd2WrLockOp
{
public:
  ObHTableRd2WrLockOp() : ret_code_(common::OB_SUCCESS) {}
  virtual ~ObHTableRd2WrLockOp() {}
  void operator() (common::hash::HashMapPair<ObHTableLockKey *, ObHTableLock *> &entry);
  int get_ret() { return ret_code_; }

private:
  int ret_code_;
  DISALLOW_COPY_AND_ASSIGN(ObHTableRd2WrLockOp);
};

#define HTABLE_LOCK_MGR (MTL(ObHTableLockMgr*))

} // end namespace table
} // end namespace oceanbase

#endif
