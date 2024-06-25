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

#ifndef OCEANBASE_STORAGE_OB_I_MEMTABLE_MGR
#define OCEANBASE_STORAGE_OB_I_MEMTABLE_MGR

#include "lib/lock/ob_spin_rwlock.h"
#include "lib/lock/ob_qsync_lock.h"
#include "common/ob_tablet_id.h"
#include "storage/ob_i_table.h"
#include "storage/checkpoint/ob_checkpoint_diagnose.h"

namespace oceanbase
{
namespace logservice
{
class ObLogHandler;
}

namespace storage
{
class ObIMemtable;
class ObFreezer;

using ObTableHdlArray = common::ObIArray<ObTableHandleV2>;

enum LockType
{
  OB_LOCK_UNKNOWN = 0,
  OB_SPIN_RWLOCK,
  OB_QSYNC_LOCK,
  OB_LOCK_MAX
};

class MemtableMgrLock
{
public:
  MemtableMgrLock() : lock_type_(OB_LOCK_UNKNOWN), lock_(nullptr) {}
  MemtableMgrLock(const LockType lock_type, void *lock) : lock_type_(lock_type), lock_(lock) {}
  ~MemtableMgrLock() { reset(); }
  void reset()
  {
    lock_type_ = LockType::OB_LOCK_UNKNOWN;
    lock_ = NULL;
  }

  bool is_valid() const
  {
    return NULL != lock_
          && (lock_type_ > OB_LOCK_UNKNOWN && lock_type_ < OB_LOCK_MAX)
          && (OB_QSYNC_LOCK == lock_type_ ? static_cast<common::ObQSyncLock *>(lock_)->is_inited() : true);
  }

  int rdlock()
  {
    int ret = OB_SUCCESS;

    if (!is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected lock or lock_type", K(ret), K_(lock_type), KP_(lock));
    } else if (lock_type_ == OB_QSYNC_LOCK) {
      ret = static_cast<common::ObQSyncLock *>(lock_)->rdlock();
    } else if (lock_type_ == LockType::OB_SPIN_RWLOCK) {
      ret = static_cast<common::SpinRWLock *>(lock_)->rdlock();
    } else {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected lock_type", K(ret), K_(lock_type));
    }
    return ret;
  }

  void rdunlock()
  {
    if (lock_type_ == OB_QSYNC_LOCK) {
      static_cast<common::ObQSyncLock *>(lock_)->rdunlock();
    } else if (lock_type_ == LockType::OB_SPIN_RWLOCK) {
      static_cast<common::SpinRWLock *>(lock_)->unlock();
    } else {
      STORAGE_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "unexpected lock_type", K_(lock_type));
    }
  }

  int try_wrlock()
  {
    int ret = OB_SUCCESS;

    if (!is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected lock or lock_type", K(ret), K_(lock_type), KP_(lock));
    } else if (lock_type_ == LockType::OB_SPIN_RWLOCK) {
      ret = static_cast<common::SpinRWLock *>(lock_)->try_wrlock();
    } else {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected lock_type", K(ret), K_(lock_type));
    }
    return ret;
  }

  int wrlock()
  {
    int ret = OB_SUCCESS;

    if (!is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected lock or lock_type", K(ret), K_(lock_type), KP_(lock));
    } else if (lock_type_ == OB_QSYNC_LOCK) {
      ret = static_cast<common::ObQSyncLock *>(lock_)->wrlock();
    } else if (lock_type_ == LockType::OB_SPIN_RWLOCK) {
      ret = static_cast<common::SpinRWLock *>(lock_)->wrlock();
    } else {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(ERROR, "unexpected lock_type", K(ret), K_(lock_type));
    }
    return ret;
  }

  void wrunlock()
  {
    if (lock_type_ == OB_QSYNC_LOCK) {
      static_cast<common::ObQSyncLock *>(lock_)->wrunlock();
    } else if (lock_type_ == LockType::OB_SPIN_RWLOCK) {
      static_cast<common::SpinRWLock *>(lock_)->unlock();
    } else {
      STORAGE_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "unexpected lock_type", K_(lock_type));
    }
  }

public:
  LockType lock_type_;
  void *lock_;
};

class MemMgrRLockGuard
{
public:
  explicit MemMgrRLockGuard(const MemtableMgrLock &lock)
      : lock_(const_cast<MemtableMgrLock&>(lock)), ret_(OB_SUCCESS)
  {
    if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.rdlock()))) {
      COMMON_LOG_RET(WARN, ret_, "Fail to read lock, ", K_(ret));
    }
  }
  ~MemMgrRLockGuard()
  {
    if (OB_LIKELY(OB_SUCCESS == ret_)) {
      lock_.rdunlock();
    }
  }
  inline int get_ret() const { return ret_; }
private:
  MemtableMgrLock &lock_;
  int ret_;
private:
  DISALLOW_COPY_AND_ASSIGN(MemMgrRLockGuard);
};

class MemMgrWLockGuard
{
public:
  explicit MemMgrWLockGuard(const MemtableMgrLock &lock)
      : lock_(const_cast<MemtableMgrLock &>(lock)), ret_(OB_SUCCESS)
  {
    if (OB_UNLIKELY(OB_SUCCESS != (ret_ = lock_.wrlock()))) {
      COMMON_LOG_RET(WARN, ret_, "Fail to write lock, ", K_(ret));
    }
  }
  ~MemMgrWLockGuard()
  {
    if (OB_LIKELY(OB_SUCCESS == ret_)) {
      lock_.wrunlock();
    }
  }
  inline int get_ret() const { return ret_; }
private:
  MemtableMgrLock &lock_;
  int ret_;
private:
  DISALLOW_COPY_AND_ASSIGN(MemMgrWLockGuard);
};

struct CreateMemtableArg {
  int64_t schema_version_;
  share::SCN clog_checkpoint_scn_;
  share::SCN new_clog_checkpoint_scn_;
  bool for_replay_;
  bool for_inc_direct_load_;

  CreateMemtableArg(const int64_t schema_version,
                    const share::SCN clog_checkpoint_scn,
                    const share::SCN new_clog_checkpoint_scn,
                    const bool for_replay,
                    const bool for_inc_direct_load)
      : schema_version_(schema_version),
        clog_checkpoint_scn_(clog_checkpoint_scn),
        new_clog_checkpoint_scn_(new_clog_checkpoint_scn),
        for_replay_(for_replay),
        for_inc_direct_load_(for_inc_direct_load) {}

  TO_STRING_KV(K(schema_version_),
               K(clog_checkpoint_scn_),
               K(new_clog_checkpoint_scn_),
               K(for_replay_),
               K(for_inc_direct_load_));
};

class ObIMemtableMgr
{
public:
  ObIMemtableMgr()
    : is_inited_(false),
      ref_cnt_(0),
      tablet_id_(),
      freezer_(nullptr),
      memtable_head_(0),
      memtable_tail_(0),
      t3m_(nullptr),
      lock_()
  {
    memset(tables_, 0, sizeof(tables_));
  }
  virtual ~ObIMemtableMgr();

  int init(
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const lib::Worker::CompatMode compat_mode);

  int init(
      const ObTabletID &tablet_id,
      const share::ObLSID &ls_id,
      const int64_t max_saved_schema_version,
      const int64_t max_saved_medium_scn,
      const lib::Worker::CompatMode compat_mode,
      logservice::ObLogHandler *log_handler,
      ObFreezer *freezer,
      ObTenantMetaMemMgr *t3m);

  virtual void destroy() = 0;
  virtual int create_memtable(const CreateMemtableArg &arg) = 0;

  virtual int get_active_memtable(ObTableHandleV2 &handle) const;

  virtual int get_first_nonempty_memtable(ObTableHandleV2 &handle) const;

  virtual int get_all_memtables(ObTableHdlArray &handle);

  virtual int get_boundary_memtable(ObTableHandleV2 &handle) { return OB_NOT_SUPPORTED; }

  virtual int get_memtable_for_replay(const share::SCN &replay_scn, ObTableHandleV2 &handle)
  {
    return OB_SUCCESS;
  }

  int release_memtables(const share::SCN &scn);
  // force release all memtables
  // WARNING: this will release all the ref of memtable, make sure you will not use it again.
  int release_memtables();

  bool has_memtable()
  {
    MemMgrRLockGuard lock_guard(lock_);
    return has_memtable_();
  }

  int get_newest_clog_checkpoint_scn(share::SCN &clog_checkpoint_scn);

  int get_newest_snapshot_version(share::SCN &snapshot_version);

  common::ObTabletID get_tablet_id() const { return tablet_id_; }

  OB_INLINE int64_t dec_ref() { return ATOMIC_SAF(&ref_cnt_, 1 /* just sub 1 */); }
  OB_INLINE int64_t get_ref() const { return ATOMIC_LOAD(&ref_cnt_); }
  OB_INLINE void inc_ref() { ATOMIC_INC(&ref_cnt_); }

  virtual int init_storage_recorder(
      const ObTabletID &tablet_id,
      const share::ObLSID &ls_id,
      const int64_t max_saved_schema_version,
      const int64_t max_saved_medium_scn,
      const lib::Worker::CompatMode compat_mode,
      logservice::ObLogHandler *log_handler)
  { // do nothing
    UNUSED(tablet_id);
    UNUSED(ls_id);
    UNUSED(max_saved_schema_version);
    UNUSED(max_saved_medium_scn);
    UNUSED(compat_mode);
    UNUSED(log_handler);
    return OB_NOT_SUPPORTED;
  }
  virtual int reset_storage_recorder() { return common::OB_SUCCESS; }
  virtual int set_frozen_for_all_memtables() { return common::OB_SUCCESS; }
  virtual int set_is_tablet_freeze_for_active_memtable(ObTableHandleV2 &handle, const int64_t trace_id = checkpoint::INVALID_TRACE_ID) { return OB_NOT_SUPPORTED; }
  virtual int get_last_frozen_memtable(ObTableHandleV2 &handle) { return OB_NOT_SUPPORTED; }
  virtual int get_direct_load_memtables_for_write(ObTableHdlArray &handles) { return OB_NOT_SUPPORTED; }
  DECLARE_VIRTUAL_TO_STRING;
protected:
  static int64_t get_memtable_idx(const int64_t pos) { return pos & (MAX_MEMSTORE_CNT - 1); }
  virtual int release_head_memtable_(ObIMemtable *memtable, const bool force = false) = 0;
  int add_memtable_(ObTableHandleV2 &memtable);
  int get_ith_memtable(const int64_t pos, ObTableHandleV2 &handle) const;
  int64_t get_memtable_count_() const { return memtable_tail_ - memtable_head_; }
  bool has_memtable_() const { return get_memtable_count_() > 0; }
  //for test
  void set_freezer(ObFreezer *freezer) { freezer_ = freezer; }
  void reset_tables();
  void release_head_memtable();
  void release_tail_memtable();
  virtual int init(const ObTabletID &tablet_id,
                   const share::ObLSID &ls_id,
                   ObFreezer *freezer,
                   ObTenantMetaMemMgr *t3m) = 0;
protected:
  bool is_inited_;
  volatile int64_t ref_cnt_;
  common::ObTabletID tablet_id_;
  // FIXME : whether freeze_handler on base class or not ?
  ObFreezer *freezer_;
  int64_t memtable_head_;
  int64_t memtable_tail_;
  ObTenantMetaMemMgr *t3m_;
  ObIMemtable *tables_[MAX_MEMSTORE_CNT];
  mutable MemtableMgrLock lock_;
};

class ObMemtableMgrHandle final
{
public:
  ObMemtableMgrHandle();
  ObMemtableMgrHandle(ObIMemtableMgr *memtable_mgr, ObTabletMemtableMgrPool *pool);
  ~ObMemtableMgrHandle();

  bool is_valid() const;
  void reset();

  OB_INLINE ObIMemtableMgr *get_memtable_mgr() { return memtable_mgr_; }
  OB_INLINE const ObIMemtableMgr *get_memtable_mgr() const { return memtable_mgr_; }

  ObMemtableMgrHandle(const ObMemtableMgrHandle &other);
  ObMemtableMgrHandle &operator= (const ObMemtableMgrHandle &other);

  int set_memtable_mgr(ObIMemtableMgr *memtable_mgr, ObTabletMemtableMgrPool *pool = nullptr);

  TO_STRING_KV(KP_(memtable_mgr), KP_(pool));

private:
  ObIMemtableMgr *memtable_mgr_;
  ObTabletMemtableMgrPool *pool_;
};

}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_TABLET_MEMTABLE_MGR
