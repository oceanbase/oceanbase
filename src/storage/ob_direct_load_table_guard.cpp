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

#include "ob_direct_load_table_guard.h"

#include "storage/ob_protected_memtable_mgr_handle.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"

namespace oceanbase {
using namespace share;

namespace storage {

ObDirectLoadTableGuard::ObDirectLoadTableGuard(ObTablet &tablet, const share::SCN &scn, const bool for_replay)
    : has_acquired_memtable_(false),
      is_write_filtered_(false),
      for_replay_(for_replay),
      ddl_redo_scn_(scn),
      table_handle_(),
      construct_timestamp_(0)
{
  ls_id_ = tablet.get_tablet_meta().ls_id_;
  tablet_id_ = tablet.get_tablet_meta().tablet_id_;
  construct_timestamp_ = ObClockGenerator::getClock();
}

void ObDirectLoadTableGuard::reset()
{
  const int64_t MAX_HOLD_GUARD_TIME = 10LL * 1000LL * 1000LL; // ten seconds
  const int64_t reset_time = ObClockGenerator::getClock();
  bool need_print_debug_log = false;
  if (reset_time - construct_timestamp_ > MAX_HOLD_GUARD_TIME) {
    need_print_debug_log = true;
    STORAGE_LOG_RET(ERROR,
                    OB_ERR_TOO_MUCH_TIME,
                    "hold ObDirectLoadTableGuard for too long",
                    KTIME(reset_time),
                    KTIME(construct_timestamp_));
  }

  int ret = OB_SUCCESS;
  ObITabletMemtable *memtable = nullptr;
  if (table_handle_.is_valid()) {
    if (OB_FAIL(table_handle_.get_tablet_memtable(memtable))) {
      STORAGE_LOG(ERROR, "unexpected fail when get tablet memtable", KPC(this), K(table_handle_));
    } else {
      if (need_print_debug_log) {
        STORAGE_LOG(INFO, "Print Debug Info", KPC(memtable), KPC(this));
      }
      memtable->dec_write_ref();
    }
  }
}

int ObDirectLoadTableGuard::prepare_memtable(ObDDLKV *&res_memtable)
{
  int ret = OB_SUCCESS;
  res_memtable = nullptr;
  bool tried_freeze = false;
  bool need_retry = false;
  const int64_t INC_MACRO_BLOCK_COUNT_FREEZE_TRIGGER =
      10 * 10L * 1024L * 1024L * 1024L / OB_SERVER_BLOCK_MGR.get_macro_block_size();
  const int64_t INC_MACRO_BLOCK_MEMORY_FREEZE_TRIGGER = 50 * 1024 * 1024;  // 50M;
  const int64_t start_time = ObClockGenerator::getClock();

  // this do_while loop up to twice
  // the first time : may trigger independent freeze
  // the second time : just acquire direct load memtable to write
  do {
    need_retry = false;
    ObDDLKV *ddl_kv = nullptr;
    if (OB_FAIL(acquire_memtable_once_())) {
      STORAGE_LOG(WARN, "acquire direct load memtable failed", KR(ret), KPC(this));
    } else if (is_write_filtered_) {
      // TODO : @gengli.wzy change this log to debug level
      STORAGE_LOG(INFO, "filtered one ddl redo write", KPC(this));
    } else if (FALSE_IT(ddl_kv = static_cast<ObDDLKV*>(table_handle_.get_table()))) {
    } else if (!tried_freeze && (ddl_kv->get_macro_block_cnt() >= INC_MACRO_BLOCK_COUNT_FREEZE_TRIGGER ||
                                 ddl_kv->get_memory_used() >= INC_MACRO_BLOCK_MEMORY_FREEZE_TRIGGER)) {
      reset();              // ATTENTION!!! : must reset guard, or freeze cannot finish
      tried_freeze = true;  // only try independent freeze once
      need_retry = true;

      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(ddl_kv->freeze())) {
        // tmp_ret = OB_ENTRY_EXIST; means this ddl kv has been freezed by other thread
      } else {
        (void)async_freeze_();
        STORAGE_LOG(INFO, "trigger inc ddlkv freeze", K(ddl_kv->get_macro_block_cnt()), K(ddl_kv->get_memory_used()));
      }
    } else {
      res_memtable = ddl_kv;
      has_acquired_memtable_ = true;
    }
  } while (OB_SUCC(ret) && need_retry);

  return ret;
}

int ObDirectLoadTableGuard::acquire_memtable_once_()
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;

  if (has_acquired_memtable_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "direct load table guard cannot be inited twice", KR(ret), KPC(this));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    STORAGE_LOG(WARN, "failed to get log stream", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!ls_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected error, invalid ls handle", K(ret), K(ls_handle), KPC(this));
  } else {
    const int64_t start_time = ObClockGenerator::getClock();
    bool need_create_new_memtable = false;
    do {
      if (need_create_new_memtable) {
        need_create_new_memtable = false;
        if (OB_FAIL(do_create_memtable_(ls_handle))) {
          STORAGE_LOG(WARN, "create direct load memtable failed", KR(ret), KPC(this));
        } else if (is_write_filtered_) {
          break;
        }
      }

      if (OB_FAIL(ret)) {
        if ((OB_ALLOCATE_MEMORY_FAILED == ret || OB_MINOR_FREEZE_NOT_ALLOW == ret) &&
            (ObClockGenerator::getClock() - start_time < MAX_RETRY_CREATE_MEMTABLE_TIME)) {
          ret = OB_SUCCESS;
          need_create_new_memtable = true;
        }
      } else if (OB_FAIL(try_get_direct_load_memtable_for_write(ls_handle, need_create_new_memtable))) {
        STORAGE_LOG(WARN, "get direct load memtable for write failed", KR(ret), K(start_time));
      }
    } while (OB_SUCC(ret) && need_create_new_memtable);
  }

  return ret;
}

int ObDirectLoadTableGuard::do_create_memtable_(ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  SCN clog_checkpoint_scn;

  if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->get_tablet(
          tablet_id_, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    STORAGE_LOG(WARN, "fail to get tablet", K(ret), KPC(this));
  } else if (FALSE_IT(clog_checkpoint_scn = tablet_handle.get_obj()->get_tablet_meta().clog_checkpoint_scn_)) {
  } else if (ddl_redo_scn_ <= clog_checkpoint_scn) {
    is_write_filtered_ = true;
    ret = OB_SUCCESS;
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->create_memtable(
                 tablet_id_, 0 /* schema version */, true /* for_direct_load */, for_replay_, clog_checkpoint_scn))) {
    STORAGE_LOG(WARN, "fail to create a boundary memtable", K(ret), KPC(this));
  }
  return ret;
}

int ObDirectLoadTableGuard::try_get_direct_load_memtable_for_write(ObLSHandle &ls_handle,
                                                                   bool &need_create_new_memtable)
{
  int ret = OB_SUCCESS;;
  ObTabletHandle tablet_handle;
  ObSEArray<ObTableHandleV2, 2> local_table_handles;
  ObProtectedMemtableMgrHandle *protected_handle = nullptr;
  if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->get_tablet(
          tablet_id_, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    STORAGE_LOG(WARN, "fail to get tablet", K(ret), KPC(this));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_protected_memtable_mgr_handle(protected_handle))) {
    STORAGE_LOG(WARN, "get memtable mgr failed", KR(ret), KPC(this), K(table_handle_));
  } else if (OB_FAIL(protected_handle->get_direct_load_memtables_for_write(local_table_handles))) {
    STORAGE_LOG(WARN, "get direct load memtable for write failed", KR(ret), KPC(this));
  } else if (0 == local_table_handles.count()) {
    need_create_new_memtable = true;
    STORAGE_LOG(INFO, "need create new direct load memtable", K(ddl_redo_scn_));
  } else {
    int64_t table_count = local_table_handles.count();
    ObITabletMemtable *head_memtable = static_cast<ObITabletMemtable *>(local_table_handles.at(0).get_table());
    ObITabletMemtable *tail_memtable =
        static_cast<ObITabletMemtable *>(local_table_handles.at(table_count - 1).get_table());

    if (head_memtable->get_start_scn() >= ddl_redo_scn_) {
      is_write_filtered_ = true;
    } else if (tail_memtable->get_end_scn() < ddl_redo_scn_) {
      need_create_new_memtable = true;
      STORAGE_LOG(INFO, "need create new direct load memtable", K(ddl_redo_scn_), KPC(tail_memtable));
    } else {
      need_create_new_memtable = false;
      for (int64_t i = 0; i < local_table_handles.count(); i++) {
        ObITabletMemtable *direct_load_memtable =
            static_cast<ObITabletMemtable *>(local_table_handles.at(i).get_table());
        const SCN start_scn = direct_load_memtable->get_scn_range().start_scn_;
        const SCN end_scn = direct_load_memtable->get_scn_range().end_scn_;
        if (start_scn < ddl_redo_scn_ && end_scn >= ddl_redo_scn_) {
          table_handle_ = local_table_handles.at(i);
          direct_load_memtable->inc_write_ref();
          break;
        }
      }
    }

    (void)clear_write_ref(local_table_handles);
  }
  return ret;
}

void ObDirectLoadTableGuard::clear_write_ref(ObIArray<ObTableHandleV2> &table_handles)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < table_handles.count(); i++) {
    ObITabletMemtable *memtable = nullptr;
    if (OB_FAIL(table_handles.at(i).get_tablet_memtable(memtable))) {
      STORAGE_LOG(ERROR, "unexpected fail when get tablet memtable", KR(ret), KPC(this));
    } else {
      memtable->dec_write_ref();
    }
  }
}

void ObDirectLoadTableGuard::async_freeze_()
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  ObLSHandle ls_handle;
  if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    STORAGE_LOG(WARN, "failed to get ls", K(ret), "ls_id", ls_id_);
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "ls should not be null", K(ret), KPC(this));
  } else {
    const bool is_sync = false;
    (void)ls->tablet_freeze(tablet_id_, is_sync);
  }
}


}  // namespace storage
}  // namespace oceanbase