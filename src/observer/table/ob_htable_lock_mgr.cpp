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

#define USING_LOG_PREFIX SERVER
#include "ob_htable_lock_mgr.h"
using namespace oceanbase::common;

namespace oceanbase
{
namespace table
{

const char *OB_HTABLE_LOCK_MANAGER = "hTableLockMgr";

int ObHTableLockMgr::mtl_init(ObHTableLockMgr *&htable_lock_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(htable_lock_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("htable_lock_mgr is null", K(ret));
  } else if (OB_FAIL(htable_lock_mgr->init())) {
    LOG_WARN("failed to init htable lock manager", K(ret));
  }
  return ret;
}

int ObHTableLockMgr::init()
{
  int ret = OB_SUCCESS;
  const ObMemAttr attr(MTL_ID(), OB_HTABLE_LOCK_MANAGER);
  SpinWLockGuard guard(spin_lock_);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(lock_map_.create(DEFAULT_BUCKET_NUM, ObModIds::TABLE_PROC, ObModIds::TABLE_PROC, MTL_ID()))) {
    LOG_WARN("fail to create htable lock map", K(ret));
  } else if (OB_FAIL(allocator_.init(ObMallocAllocator::get_instance(), OB_MALLOC_MIDDLE_BLOCK_SIZE, attr))) {
    LOG_WARN("fail to init allocator", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObHTableLockMgr::mtl_destroy(ObHTableLockMgr *&htable_lock_mgr)
{
  if (nullptr != htable_lock_mgr) {
    LOG_INFO("trace ObHTableLockMgr destroy", K(MTL_ID()));
    htable_lock_mgr->lock_map_.destroy();
    common::ob_delete(htable_lock_mgr);
    htable_lock_mgr = nullptr;
  }
}

// acquire_handle without tx_id must set_tx_id later
int ObHTableLockMgr::acquire_handle(ObHTableLockHandle *&handle)
{
  int ret = OB_SUCCESS;
  void *buf = allocator_.alloc(sizeof(ObHTableLockHandle));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(sizeof(ObHTableLockHandle)));
  } else  {
    handle = new(buf) ObHTableLockHandle();
  }
  return ret;
}

int ObHTableLockMgr::acquire_handle(const transaction::ObTransID &tx_id, ObHTableLockHandle *&handle)
{
  int ret = OB_SUCCESS;
  void *buf = allocator_.alloc(sizeof(ObHTableLockHandle));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(sizeof(ObHTableLockHandle)));
  } else  {
    handle = new(buf) ObHTableLockHandle(tx_id);
  }
  return ret;
}

int ObHTableLockHandle::find_lock_node(uint64_t table_id, const common::ObString &key, ObHTableLockNode *&lock_node)
{
  int ret = OB_SUCCESS;
  ObHTableLockNode *cur_node = lock_nodes_;
  lock_node = nullptr;
  ObHTableLockKey target_lock_key(table_id, key);
  bool is_matched = false;
  while (OB_SUCC(ret) && cur_node != nullptr && !is_matched) {
    ObHTableLockKey *lock_key = cur_node->get_lock_key();
    if (OB_ISNULL(lock_key)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected null lock key", K(ret));
    } else if (!lock_key->is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_ERROR("invalid lock key", K(ret), KPC(lock_key));
    } else if (*lock_key == target_lock_key) {
      is_matched = true;
      lock_node = cur_node;
    } else {/* do nothing */}
    cur_node = cur_node->next_;
  }
  return ret;
}

// situations to consider when locking
// 1. add shared lock
// 2. add exclusive lock
// 3. add shared lock repeatly: do nothing
// 4. add exclusive lock repeatly: do nothing
// 5. escalate shared lock to exclusive lock: only allowed when no one else has the same read lock
// 6. add shared lock when holding exclusive lock: do nothing
int ObHTableLockMgr::lock_row(const uint64_t table_id, const common::ObString& key,  ObHTableLockMode mode, ObHTableLockHandle &handle)
{
  int ret = OB_SUCCESS;
  ObHTableLockNode *lock_node = nullptr;
  if (key.empty()) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("null lock row key", K(ret));
  } else if (OB_FAIL(handle.find_lock_node(table_id, key, lock_node))) {
    LOG_WARN("fail to find lock node", K(ret), K(table_id), K(key));
  } else if (OB_ISNULL(lock_node)) {
    if (OB_FAIL(internal_lock_row(table_id, key, mode, handle))) {
      LOG_WARN("fail to lock", K(ret), K(table_id), K(key), K(mode));
    } else {/* do nothing */}
  } else if (lock_node->lock_mode_ == SHARED && mode == EXCLUSIVE) {
    if (OB_FAIL(rd2wrlock(*lock_node))) {
      LOG_WARN("fail to escalate read lock to write lock", K(ret));
    } else {
      lock_node->lock_mode_ = EXCLUSIVE;
    }
  } else {/* do nothing */}

  return ret;
}

int ObHTableLockMgr::internal_lock_row(const uint64_t table_id, const common::ObString& key, ObHTableLockMode lock_mode, ObHTableLockHandle &handle)
{
  int ret = OB_SUCCESS;
  ObHTableLockKey tmp_lock_key(table_id, key);
  ObHTableLockOp lock_op(lock_mode);
  ObHTableLockNode *lock_node = nullptr;
  ObHTableLockKey *new_lock_key = nullptr;
  ObHTableLock *new_lock = nullptr;
  bool lock_exists = false;

  if (OB_FAIL(alloc_lock_node(lock_mode, lock_node))) {
    LOG_WARN("fail to alloc lock node", K(ret), K(lock_mode));
  } else {
    // suppose the lock exists firstly, try to get and add shared/exclusive lock
    if (OB_FAIL(lock_map_.atomic_refactored(&tmp_lock_key, lock_op))) {
      if (ret == OB_HASH_NOT_EXIST) {
        // lock not exists, try to add a new lock or use the lock added by others
        ret = OB_SUCCESS;
        if (OB_FAIL(alloc_lock_key(table_id, key, new_lock_key))) {
          LOG_WARN("fail to alloc new lock key", K(ret));
        } else if (OB_FAIL(alloc_lock(lock_mode, new_lock))) {
          LOG_WARN("fail to alloc new lock", K(ret));
        } else if (OB_FAIL(lock_map_.set_or_update(new_lock_key, new_lock, lock_op))) {
          LOG_WARN("fail to set or update lock", K(ret));
        }
      } else {
        LOG_WARN("fail to add read lock", K(ret), K(tmp_lock_key));
      }
    }
  }

  if (OB_SUCC(ret) && lock_op.is_called()) {
    ret = lock_op.get_ret();
    lock_exists = true;
  }

  // add lock key to lock node, and add lock node to lock handle
  if (OB_SUCC(ret)) {
    if (lock_exists) {
      ObHTableLockKey *old_lock_key = lock_op.get_lock_key();
      if (OB_ISNULL(old_lock_key)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("null old lock key but lock exist", K(ret));
      } else {
        lock_node->set_lock_key(old_lock_key);
        handle.add_lock_node(*lock_node);
      }
    } else {
      lock_node->set_lock_key(new_lock_key);
      handle.add_lock_node(*lock_node);
    }
  }

  if (OB_FAIL(ret) || lock_exists) {
    if (OB_NOT_NULL(new_lock_key)) {
      allocator_.free(new_lock_key->key_.ptr());
      allocator_.free(new_lock_key);
    }
    if (OB_NOT_NULL(new_lock)) {
      allocator_.free(new_lock);
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(lock_node)) {
      allocator_.free(lock_node);
    }
  }
  return ret;
}


int ObHTableLockMgr::alloc_lock_node(ObHTableLockMode lock_mode, ObHTableLockNode *&lock_node)
{
  int ret = OB_SUCCESS;
  void *buf = allocator_.alloc(sizeof(ObHTableLockNode));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(sizeof(ObHTableLockNode)));
  } else {
    lock_node = new(buf) ObHTableLockNode(lock_mode);
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(buf)) {
    allocator_.free(buf);
  }
  return ret;
}

int ObHTableLockMgr::alloc_lock_key(const uint64_t table_id, const ObString &key, ObHTableLockKey *&lock_key)
{
  int ret = OB_SUCCESS;
  ObHTableLockKey *tmp_lock_key = nullptr;
  void *buf = allocator_.alloc(sizeof(ObHTableLockKey));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(sizeof(ObHTableLockKey)));
  } else {
    tmp_lock_key = new(buf) ObHTableLockKey();
    if (OB_FAIL(ob_write_string(allocator_, key, tmp_lock_key->key_))) {
      LOG_WARN("fail to copy key", K(ret));
    } else {
      tmp_lock_key->table_id_ = table_id;
      lock_key = tmp_lock_key;
    }
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(buf)) {
    allocator_.free(buf);
  }
  return ret;
}

// alloc lock with initial lock mode
int ObHTableLockMgr::alloc_lock(ObHTableLockMode lock_mode, ObHTableLock *&lock)
{
  int ret = OB_SUCCESS;
  ObHTableLock *tmp_lock = nullptr;
  void *buf = allocator_.alloc(sizeof(ObHTableLock));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret), K(sizeof(ObHTableLock)));
  } else {
    tmp_lock = new(buf) ObHTableLock();
    if (lock_mode == ObHTableLockMode::SHARED) {
      if (OB_FAIL(tmp_lock->try_rdlock())) {
        LOG_WARN("fail to add read lock", K(ret));
      }
    } else if (lock_mode == ObHTableLockMode::EXCLUSIVE) {
      if (OB_FAIL(tmp_lock->try_wrlock())) {
        LOG_WARN("fail to add write lock", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid lock mode", K(ret), K(lock_mode));
    }
  }

  if (OB_SUCC(ret)) {
    lock = tmp_lock;
  } else if (OB_NOT_NULL(buf)) {
    allocator_.free(buf);
  } else {/*do nothing*/}

  return ret;
}

void ObHTableLockOp::operator() (common::hash::HashMapPair<ObHTableLockKey *, ObHTableLock *> &entry)
{
  int ret = OB_SUCCESS;
  is_called_ = true;
  lock_success_ = false;
  if (OB_ISNULL(entry.second)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null lock key", K(ret));
  } else {
    if (lock_mode_ == ObHTableLockMode::SHARED) {
      if (OB_FAIL(entry.second->try_rdlock())) {
        LOG_WARN("fail to add read lock", K(ret));
      }
    } else {
      if (OB_FAIL(entry.second->try_wrlock())) {
        LOG_WARN("fail to add write lock", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    old_lock_key_ = entry.first;
    lock_success_ = true;
  }
  ret_code_ = ret;
}

void ObHTableRd2WrLockOp::operator() (common::hash::HashMapPair<ObHTableLockKey *, ObHTableLock *> &entry)
{
  int ret = OB_SUCCESS;
  ObHTableLock *lock = entry.second;
  if (OB_ISNULL(lock)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected null lock key", K(ret));
  } else if (!lock->can_escalate_lock()) {
    ret = OB_TRY_LOCK_ROW_CONFLICT;
    LOG_WARN("can not escalate lock from share to exclusive", K(ret));
  } else if (OB_FAIL(lock->unlock())) {
    LOG_ERROR("fail to unlock during escalate the lock", K(ret));
  } else if (OB_FAIL(lock->try_wrlock())) {
    LOG_ERROR("fail to add write lock during escalate the lock", K(ret));
  } else {/* do nothing*/}

  ret_code_ = ret;
}

bool ObHTableUnLockOpPred::operator() (common::hash::HashMapPair<ObHTableLockKey *, ObHTableLock *> &entry)
{
  bool need_erase = false;
  int ret = OB_SUCCESS;
  ObHTableLock *lock = entry.second;
  if (OB_ISNULL(lock)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected null lock key", K(ret));
  } else if (OB_FAIL(lock->unlock())) {
    LOG_ERROR("fail to unlock", K(ret));
  } else if (!lock->is_locked()) {
    need_erase = true;
  }
  ret_code_ = ret;
  return need_erase;
}

// head insert new lock node into lock handle
void ObHTableLockHandle::add_lock_node(ObHTableLockNode &lock_node)
{
  ObHTableLockNode *orgin_lock_nodes = lock_nodes_;
  lock_nodes_ = &lock_node;
  lock_nodes_->next_ = orgin_lock_nodes;
}

int ObHTableLockMgr::rd2wrlock(ObHTableLockNode &lock_node)
{
  int ret = OB_SUCCESS;
  if (lock_node.get_lock_mode() != SHARED) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    ObHTableRd2WrLockOp rd2wrlockop;
    if (OB_FAIL(lock_map_.atomic_refactored(lock_node.get_lock_key(), rd2wrlockop))) {
      LOG_WARN("fail to escalate read lock to write lock", K(ret));
    } else {
      ret = rd2wrlockop.get_ret();
    }
  }
  return ret;
}

int ObHTableLockMgr::release_handle(ObHTableLockHandle &handle)
{
  int ret = OB_SUCCESS;
  ObHTableLockNode *cur = handle.lock_nodes_;
  ObHTableLockNode *next = nullptr;
  while(cur != nullptr) {
    next = cur->next_;
    if (OB_FAIL(release_node(*cur))) {
      LOG_ERROR("fail to release lock node", K(ret), KPC(cur));
    }
    cur = next;
  }
  allocator_.free(&handle);
  return ret;
}

int ObHTableLockMgr::release_node(ObHTableLockNode &lock_node)
{
  int ret = OB_SUCCESS;
  ObHTableUnLockOpPred unlock_op_pred;
  ObHTableLockKey *lock_key = lock_node.lock_key_;
  ObHTableLock *lock = nullptr;
  bool is_erased = false;
  if (OB_ISNULL(lock_key)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected null lock key", K(ret));
  } else if (!lock_key->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(ret), KPC(lock_key));
  } else if (OB_FAIL(lock_map_.erase_if(lock_key, unlock_op_pred, is_erased, &lock))) {
    LOG_ERROR("fail to erase lock", K(ret), "unlock_op_pred.ret_code_", unlock_op_pred.get_ret());
  } else if (OB_FAIL(unlock_op_pred.get_ret())) {
    LOG_ERROR("fail to unlock", K(ret));
  } else if (is_erased) {
    allocator_.free(lock_key->key_.ptr());
    allocator_.free(lock_key);
    if (OB_ISNULL(lock)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected null lock", K(ret));
    } else {
      allocator_.free(lock);
    }
  } else {/* do nothing */}

  allocator_.free(&lock_node);
  return ret;
}

bool ObHTableLockKey::operator==(const ObHTableLockKey &other) const
{
  return table_id_ == other.table_id_ && key_ == other.key_;
}

int ObHTableLock::try_rdlock()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(latch_.try_rdlock(common::ObLatchIds::TABLE_API_LOCK))) {
    // rewrite ret
    ret = OB_TRY_LOCK_ROW_CONFLICT;
    LOG_WARN("fail to try add read lock", K(ret));
  }
  return ret;
}

int ObHTableLock::try_wrlock()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(latch_.try_wrlock(common::ObLatchIds::TABLE_API_LOCK, NULL))) {
    // rewrite ret
    ret = OB_TRY_LOCK_ROW_CONFLICT;
    LOG_WARN("fail to try add write lock", K(ret));
  }
  return ret;
}

int ObHTableLock::unlock()
{
  return latch_.unlock(NULL);
}

} // end namespace table
} // end namespace oceanbase