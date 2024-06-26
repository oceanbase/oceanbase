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

#ifndef OCEANBASE_STORAGE_OB_PROTECTED_MEMTABLE_MGR_HANDLE
#define OCEANBASE_STORAGE_OB_PROTECTED_MEMTABLE_MGR_HANDLE

#include "lib/lock/ob_spin_rwlock.h"
#include "ob_tablet_id.h"
#include "storage/ob_i_memtable_mgr.h"
#include "storage/tablet/ob_tablet_meta.h"
#include "storage/tablet/ob_tablet_memtable_mgr.h"

#define PROCESS_FOR_MEMTABLE_MGR(...) \
  do { \
    { \
      SpinRLockGuard guard(memtable_mgr_handle_lock_); \
      if (memtable_mgr_handle_.is_valid()) { \
        __VA_ARGS__ \
        break; \
      } \
    } \
    SpinWLockGuard guard(memtable_mgr_handle_lock_); \
    if (!memtable_mgr_handle_.is_valid()) { \
      STORAGE_LOG(INFO, "memtable_mgr_handle_ is not exist, need create", K(tablet_meta.ls_id_), K(tablet_meta.tablet_id_)); \
      if (OB_FAIL(create_tablet_memtable_mgr_(tablet_meta.ls_id_, tablet_meta.tablet_id_, tablet_meta.compat_mode_))) { \
        STORAGE_LOG(WARN, "failed to create_tablet_memtable_mgr", K(tablet_meta.ls_id_), K(tablet_meta.tablet_id_), K(tablet_meta.compat_mode_)); \
      } \
    } \
  } while (OB_SUCC(ret));

#define DELEGATE_FOR_MEMTABLE_MGR_WITH_CREATE(function)                                            \
  template <typename... Args>                                                                      \
  int function(const ObTabletMeta &tablet_meta, Args &&...args)                                    \
  {                                                                                                \
    int ret = OB_SUCCESS;                                                                          \
    PROCESS_FOR_MEMTABLE_MGR(                                                                      \
        { ret = memtable_mgr_handle_.get_memtable_mgr()->function(std::forward<Args>(args)...); }) \
    return ret;                                                                                    \
  }

#define DELEGATE_FOR_MEMTABLE_MGR(function, ignore_not_exist_error) \
template <typename ...Args> \
int function(Args &&...args) \
{ \
  int ret = OB_SUCCESS; \
  SpinRLockGuard guard(memtable_mgr_handle_lock_); \
  if (memtable_mgr_handle_.is_valid()) { \
    ret = memtable_mgr_handle_.get_memtable_mgr()->function(std::forward<Args>(args)...); \
  } else if (ignore_not_exist_error) { \
    ret = OB_SUCCESS; \
  } else { \
    ret = OB_ENTRY_NOT_EXIST; \
    STORAGE_LOG(DEBUG, "ObMemtableMgr is not exist, there is no memtable", KR(ret)); \
  } \
  return ret; \
}

#define READ_FOR_STORAGE_SCHEMA(recorder, value) \
int64_t get_##value##_from_##recorder() \
{ \
  bool ret = 0; \
  SpinRLockGuard guard(memtable_mgr_handle_lock_); \
  if (memtable_mgr_handle_.is_valid()) { \
    ObTabletMemtableMgr *tablet_memtable_mgr = static_cast<ObTabletMemtableMgr*>(memtable_mgr_handle_.get_memtable_mgr()); \
    ret = tablet_memtable_mgr->get_##recorder().get_##value(); \
  } \
  return ret; \
}

#define WRITE_FOR_STORAGE_SCHEMA(recorder, process) \
template <typename ...Args> \
int process(const ObTabletMeta &tablet_meta, \
    Args &&...args) \
{ \
  int ret = OB_SUCCESS; \
  PROCESS_FOR_MEMTABLE_MGR( \
  { \
    ObTabletMemtableMgr *tablet_memtable_mgr = static_cast<ObTabletMemtableMgr*>(memtable_mgr_handle_.get_memtable_mgr()); \
    ret = tablet_memtable_mgr->get_##recorder().process(std::forward<Args>(args)...); \
  }) \
  return ret; \
}

namespace oceanbase
{

namespace storage
{
class ObProtectedMemtableMgrHandle
{
public:
  ObProtectedMemtableMgrHandle()
    : memtable_mgr_handle_(),
      memtable_mgr_handle_lock_() {}
  ObProtectedMemtableMgrHandle(const ObMemtableMgrHandle &memtable_mgr_handle)
    : memtable_mgr_handle_(memtable_mgr_handle),
      memtable_mgr_handle_lock_() {}
  ObProtectedMemtableMgrHandle& operator=(const ObProtectedMemtableMgrHandle &r)
  {
    SpinWLockGuard guard(memtable_mgr_handle_lock_);
    memtable_mgr_handle_ = r.memtable_mgr_handle_;
    return *this;
  }
  int reset();
  TO_STRING_KV(K(memtable_mgr_handle_));

  int release_memtables_and_try_reset_memtable_mgr_handle(const ObTabletID &tablet_id,
      const share::SCN &scn);
  bool has_active_memtable()
  {
    bool ret = false;
    SpinRLockGuard guard(memtable_mgr_handle_lock_);
    if (memtable_mgr_handle_.is_valid()) {
      ret = static_cast<ObTabletMemtableMgr*>(memtable_mgr_handle_.get_memtable_mgr())->has_active_memtable();
    }
    return ret;
  }

  bool has_memtable()
  {
    bool ret = false;
    SpinRLockGuard guard(memtable_mgr_handle_lock_);
    if (memtable_mgr_handle_.is_valid()) {
      ret = static_cast<ObTabletMemtableMgr*>(memtable_mgr_handle_.get_memtable_mgr())->has_memtable();
    }
    return ret;
  }

  READ_FOR_STORAGE_SCHEMA(medium_info_recorder, max_saved_version);
  READ_FOR_STORAGE_SCHEMA(storage_schema_recorder, max_saved_version);
  READ_FOR_STORAGE_SCHEMA(storage_schema_recorder, max_column_cnt);

  WRITE_FOR_STORAGE_SCHEMA(medium_info_recorder, submit_medium_compaction_info);
  WRITE_FOR_STORAGE_SCHEMA(medium_info_recorder, replay_medium_compaction_log);
  WRITE_FOR_STORAGE_SCHEMA(storage_schema_recorder, try_update_storage_schema);
  WRITE_FOR_STORAGE_SCHEMA(storage_schema_recorder, replay_schema_log);

  DELEGATE_FOR_MEMTABLE_MGR(get_active_memtable, false);
  DELEGATE_FOR_MEMTABLE_MGR(init, false);
  DELEGATE_FOR_MEMTABLE_MGR(get_first_nonempty_memtable, false);
  DELEGATE_FOR_MEMTABLE_MGR(set_frozen_for_all_memtables, true);
  DELEGATE_FOR_MEMTABLE_MGR(get_all_memtables, true);
  DELEGATE_FOR_MEMTABLE_MGR(get_boundary_memtable, false);
  DELEGATE_FOR_MEMTABLE_MGR(set_is_tablet_freeze_for_active_memtable, false);
  DELEGATE_FOR_MEMTABLE_MGR(get_last_frozen_memtable, false);
  DELEGATE_FOR_MEMTABLE_MGR(get_memtable_for_replay, false);
  DELEGATE_FOR_MEMTABLE_MGR(get_direct_load_memtables_for_write, true);

  DELEGATE_FOR_MEMTABLE_MGR_WITH_CREATE(create_memtable);

private:
  DELEGATE_FOR_MEMTABLE_MGR(release_memtables, true);
  int try_reset_memtable_mgr_handle_();
  bool need_reset_without_lock_();
  bool need_reset_();
  int create_tablet_memtable_mgr_(const share::ObLSID &ls_id,
      const ObTabletID &tablet_id,
      lib::Worker::CompatMode compat_mode);
  ObMemtableMgrHandle memtable_mgr_handle_;
  mutable common::SpinRWLock memtable_mgr_handle_lock_;
};

}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_PROTECTED_MEMTABLE_MGR_HANDLE
