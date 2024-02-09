/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_ddl_replay_executor.h"
#include "storage/ddl/ob_ddl_clog.h"
#include "storage/ddl/ob_ddl_merge_task.h"
#include "storage/ls/ob_ls.h"
#include "storage/compaction/ob_schedule_dag_func.h"
#include "storage/ddl/ob_tablet_ddl_kv_mgr.h"
#include "storage/ddl/ob_direct_insert_sstable_ctx_new.h"

using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;
using namespace oceanbase::share;

ObDDLReplayExecutor::ObDDLReplayExecutor()
  : logservice::ObTabletReplayExecutor(), ls_(nullptr), scn_()
{}

int ObDDLReplayExecutor::check_need_replay_ddl_log_(
    const ObTabletHandle &tablet_handle,
    const share::SCN &ddl_start_scn,
    const share::SCN &scn,
    bool &need_replay) const
{
  int ret = OB_SUCCESS;
  need_replay = true;
  ObTablet *tablet = nullptr;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  ObMigrationStatus migration_status;
  ObTabletBindingMdsUserData ddl_data;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ls_->get_ls_meta().get_migration_status(migration_status))) {
    LOG_WARN("get migration status failed", K(ret), KPC(ls_));
  } else if (ObMigrationStatus::OB_MIGRATION_STATUS_ADD_FAIL == migration_status
      || ObMigrationStatus::OB_MIGRATION_STATUS_MIGRATE_FAIL == migration_status) {
    need_replay = false;
    LOG_INFO("ls migration failed, so ddl log skip replay", "ls_id", ls_->get_ls_id(), K(tablet_handle), K(migration_status));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is null", K(ret), K(tablet_handle));
  } else if (tablet->is_empty_shell()) {
    need_replay = false;
    if (REACH_COUNT_INTERVAL(1000L)) {
      LOG_INFO("no need to replay ddl log, because this tablet is empty shell",
          K(tablet_handle), "tablet_meta", tablet->get_tablet_meta());
    }
  } else if (tablet->get_tablet_meta().ha_status_.is_expected_status_deleted()) {
    need_replay = false;
    if (REACH_COUNT_INTERVAL(1000L)) {
      LOG_INFO("no need to replay ddl log, because tablet will be deleted",
          K(tablet_handle), "tablet_meta", tablet->get_tablet_meta());
    }
  } else if (scn <= tablet->get_tablet_meta().ddl_checkpoint_scn_) {
    need_replay = false;
    if (REACH_COUNT_INTERVAL(1000L)) {
      LOG_INFO("no need to replay ddl log, because the log ts is less than the ddl checkpoint ts",
          K(tablet_handle), K(scn), "ddl_checkpoint_ts", tablet->get_tablet_meta().ddl_checkpoint_scn_);
    }
  } else if (ddl_start_scn < tablet->get_tablet_meta().ddl_start_scn_) {
    need_replay = false;
    if (REACH_COUNT_INTERVAL(1000L)) {
      LOG_INFO("no need to replay ddl log, because the ddl start log ts is less than the value in ddl kv manager",
          K(tablet_handle), K(ddl_start_scn), "ddl_start_scn_in_tablet", tablet->get_tablet_meta().ddl_start_scn_);
    }
  } else if (OB_FAIL(tablet_handle.get_obj()->ObITabletMdsInterface::get_ddl_data(share::SCN::max_scn(), ddl_data))) {
    LOG_WARN("failed to get ddl data from tablet", K(ret), K(tablet_handle));
  } else if (ddl_data.lob_meta_tablet_id_.is_valid()) {
    ObTabletHandle lob_tablet_handle;
    const ObTabletID lob_tablet_id = ddl_data.lob_meta_tablet_id_;
    if (OB_FAIL(ls_->replay_get_tablet_no_check(lob_tablet_id, scn, lob_tablet_handle))) {
      LOG_WARN("get tablet handle failed", K(ret), K(lob_tablet_id), K(scn));
    } else if (lob_tablet_handle.get_obj()->is_empty_shell()) {
      need_replay = false;
      LOG_INFO("lob tablet is empty, skip replay", K(ret), K(tablet_id_), K(lob_tablet_id));
    }
  }
  return ret;
}


// ObDDLStartReplayExecutor
ObDDLStartReplayExecutor::ObDDLStartReplayExecutor()
  : ObDDLReplayExecutor(), log_(nullptr)
{

}

int ObDDLStartReplayExecutor::init(
    ObLS *ls,
    const ObDDLStartLog &log,
    const SCN &scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(ls)
          || OB_UNLIKELY(!log.is_valid())
          || OB_UNLIKELY(!scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KP(ls), K(log), K(scn), K(ret));
  } else {
    ls_ = ls;
    log_ = &log;
    scn_ = scn;
    is_inited_ = true;
  }

  return ret;
}

int ObDDLStartReplayExecutor::do_replay_(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObTenantDirectLoadMgr *tenant_direct_load_mgr = MTL(ObTenantDirectLoadMgr *);
  ObTabletDirectLoadMgrHandle direct_load_mgr_handle;
  const int64_t unused_context_id = -1;
  bool need_replay = true;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedoLogReplayer has not been inited", K(ret));
  } else if (OB_UNLIKELY(!log_->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K_(log));
  } else if (OB_FAIL(check_need_replay_ddl_log_(tablet_handle, scn_, scn_, need_replay))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("fail to check need replay ddl log", K(ret), K_(tablet_id), K_(scn));
    }
  } else if (!need_replay) {
    // do nothing
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("need replay but tablet handle is invalid", K(ret), K(need_replay), K(tablet_handle));
  } else if (OB_ISNULL(tenant_direct_load_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), K(MTL_ID()));
  } else {
    ObTabletDirectLoadInsertParam direct_load_param;
    const ObITable::TableKey &table_key = log_->get_table_key();
    direct_load_param.is_replay_ = true;
    bool is_major_sstable_exist = false;
    direct_load_param.common_param_.ls_id_ = tablet_handle.get_obj()->get_tablet_meta().ls_id_;
    direct_load_param.common_param_.tablet_id_ = table_key.tablet_id_;
    direct_load_param.common_param_.data_format_version_ = log_->get_data_format_version();
    direct_load_param.common_param_.direct_load_type_ = log_->get_direct_load_type();
    direct_load_param.common_param_.read_snapshot_ = table_key.get_snapshot_version();
    if (OB_FAIL(tenant_direct_load_mgr->create_tablet_direct_load(unused_context_id, log_->get_execution_id(), direct_load_param))) {
      LOG_WARN("create tablet manager failed", K(ret));
    } else if (OB_FAIL(tenant_direct_load_mgr->get_tablet_mgr_and_check_major(
            ls_->get_ls_id(),
            table_key.tablet_id_,
            true/* is_full_direct_load */,
            direct_load_mgr_handle,
            is_major_sstable_exist))) {
      if (OB_ENTRY_NOT_EXIST == ret && is_major_sstable_exist) {
        ret = OB_SUCCESS;
        LOG_INFO("ddl start log is expired, skip", K(ret), KPC(log_), K(scn_));
      } else {
        LOG_WARN("get tablet mgr failed", K(ret), K(table_key));
      }
    } else if (OB_FAIL(direct_load_mgr_handle.get_full_obj()->start(*tablet_handle.get_obj(),
            table_key, scn_, log_->get_data_format_version(), log_->get_execution_id(), SCN::min_scn()/*checkpoint_scn*/))) {
      LOG_WARN("direct load start failed", K(ret));
      if (OB_TASK_EXPIRED != ret) {
        LOG_WARN("start ddl log failed", K(ret), K_(log), K_(scn));
      } else {
        ret = OB_SUCCESS; // ignored expired ddl start log
      }
    } else {
      LOG_INFO("succeed to replay ddl start log", K(ret), KPC_(log), K_(scn));
    }
  }
  LOG_INFO("finish replay ddl start log", K(ret), K(need_replay), KPC_(log), K_(scn), "ddl_event_info", ObDDLEventInfo());
  return ret;
}


// ObDDLRedoReplayExecutor
ObDDLRedoReplayExecutor::ObDDLRedoReplayExecutor()
  : ObDDLReplayExecutor(), log_(nullptr)
{

}

int ObDDLRedoReplayExecutor::init(
    ObLS *ls,
    const ObDDLRedoLog &log,
    const SCN &scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(ls)
          || OB_UNLIKELY(!log.is_valid())
          || OB_UNLIKELY(!scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KP(ls), K(log), K(scn), K(ret));
  } else {
    ls_ = ls;
    log_ = &log;
    scn_ = scn;
    is_inited_ = true;
  }

  return ret;
}

int ObDDLRedoReplayExecutor::do_replay_(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  bool need_replay = true;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;

  ObDDLMacroBlock macro_block;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedoLogExecutor has not been inited", K(ret));
  } else if (OB_UNLIKELY(!log_->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K_(log));
  } else if (OB_FAIL(check_need_replay_ddl_log_(tablet_handle, log_->get_redo_info().start_scn_, scn_, need_replay))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("fail to check need replay ddl log", K(ret), K_(tablet_id), K_(scn));
    }
  } else if (!need_replay) {
    // do nothing
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("need replay but tablet handle is invalid", K(ret), K(need_replay), K(tablet_handle));
  } else if (OB_FAIL(tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (!table_store_wrapper.get_member()->get_major_sstables().empty()) {
    // major sstable already exist, means ddl commit success
    need_replay = false;
    if (REACH_TIME_INTERVAL(1000L * 1000L)) {
      LOG_INFO("no need to replay ddl log, because the major sstable already exist", K_(tablet_id));
    }
  } else {
    ObMacroBlockWriteInfo write_info;
    ObMacroBlockHandle macro_handle;
    const ObDDLMacroBlockRedoInfo &redo_info = log_->get_redo_info();
    write_info.buffer_ = redo_info.data_buffer_.ptr();
    write_info.size_= redo_info.data_buffer_.length();
    write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    write_info.io_timeout_ms_ = max(DDL_FLUSH_MACRO_BLOCK_TIMEOUT / 1000L, GCONF._data_storage_io_timeout / 1000L);
    if (OB_FAIL(ObBlockManager::async_write_block(write_info, macro_handle))) {
      LOG_WARN("fail to async write block", K(ret), K(write_info), K(macro_handle));
    } else if (OB_FAIL(macro_handle.wait())) {
      LOG_WARN("fail to wait macro block io finish", K(ret), K(write_info));
    } else if (OB_FAIL(macro_block.block_handle_.set_block_id(macro_handle.get_macro_id()))) {
      LOG_WARN("set macro block id failed", K(ret), K(macro_handle.get_macro_id()));
    } else {
      macro_block.block_type_ = redo_info.block_type_;
      macro_block.logic_id_ = redo_info.logic_id_;
      macro_block.scn_ = scn_;
      macro_block.buf_ = redo_info.data_buffer_.ptr();
      macro_block.size_ = redo_info.data_buffer_.length();
      macro_block.ddl_start_scn_ = redo_info.start_scn_;
      macro_block.table_key_ = redo_info.table_key_;
      macro_block.end_row_id_ = redo_info.end_row_id_;
      const int64_t snapshot_version = redo_info.table_key_.get_snapshot_version();
      const ObITable::TableKey &table_key = redo_info.table_key_;
      bool is_major_sstable_exist = false;
      uint64_t data_format_version = redo_info.data_format_version_;

      // to upgrade from lower version without `data_format_version` in redo log,
      // use data_format_version in start log instead.
      ObTabletDirectLoadMgrHandle direct_load_mgr_handle;
      ObTenantDirectLoadMgr *tenant_direct_load_mgr = MTL(ObTenantDirectLoadMgr *);
      if (OB_ISNULL(tenant_direct_load_mgr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected err", K(ret));
      } else if (OB_FAIL(tenant_direct_load_mgr->get_tablet_mgr_and_check_major(
              ls_->get_ls_id(),
              redo_info.table_key_.tablet_id_,
              true/* is_full_direct_load */,
              direct_load_mgr_handle,
              is_major_sstable_exist))) {
        if (OB_ENTRY_NOT_EXIST == ret && is_major_sstable_exist) {
          need_replay = false;
          ret = OB_SUCCESS;
          LOG_INFO("major sstable already exist, ship replay", K(ret), K(scn_), K(table_key));
        } else {
          LOG_WARN("get tablet mgr failed", K(ret), K(table_key));
        }
      } else if (data_format_version <= 0) {
        data_format_version = direct_load_mgr_handle.get_obj()->get_data_format_version();
      }
      if (OB_SUCC(ret) && need_replay) {
        if (OB_FAIL(ObDDLKVPendingGuard::set_macro_block(tablet_handle.get_obj(), macro_block,
            snapshot_version, data_format_version, direct_load_mgr_handle))) {
          LOG_WARN("set macro block into ddl kv failed", K(ret), K(tablet_handle), K(macro_block),
            K(snapshot_version), K(data_format_version));
        }
      }
    }
  }
  FLOG_INFO("finish replay ddl redo log", K(ret), K(need_replay), KPC_(log), K(macro_block), "ddl_event_info", ObDDLEventInfo());
  return ret;
}

// ObDDLCommitReplayExecutor
ObDDLCommitReplayExecutor::ObDDLCommitReplayExecutor()
  : ObDDLReplayExecutor(), log_(nullptr)
{

}

int ObDDLCommitReplayExecutor::init(
    ObLS *ls,
    const ObDDLCommitLog &log,
    const SCN &scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(ls)
          || OB_UNLIKELY(!log.is_valid())
          || OB_UNLIKELY(!scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KP(ls), K(log), K(scn), K(ret));
  } else {
    ls_ = ls;
    log_ = &log;
    scn_ = scn;
    is_inited_ = true;
  }

  return ret;
}

int ObDDLCommitReplayExecutor::do_replay_(ObTabletHandle &tablet_handle) //TODO(jianyun.sjy): check it
{
  int ret = OB_SUCCESS;
  ObITable::TableKey table_key;
  ObTabletFullDirectLoadMgr *data_direct_load_mgr = nullptr;
  ObTabletDirectLoadMgrHandle direct_load_mgr_handle;
  bool need_replay = true;
  bool is_major_sstable_exist = false;

  DEBUG_SYNC(BEFORE_REPLAY_DDL_PREPRARE);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedoLogReplayer has not been inited", K(ret));
  } else if (OB_UNLIKELY(!log_->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K_(log));
  } else if (OB_FAIL(check_need_replay_ddl_log_(tablet_handle, log_->get_start_scn(), scn_, need_replay))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("fail to check need replay ddl log", K(ret), K(table_key), K_(scn), K_(log));
    }
  } else if (!need_replay) {
    // do nothing
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("need replay but tablet handle is invalid", K(ret), K(need_replay), K(tablet_handle), K_(log), K_(scn));
  } else if (OB_FALSE_IT(table_key = log_->get_table_key())) {
  } else if (OB_ISNULL(MTL(ObTenantDirectLoadMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), K(MTL_ID()));
  } else if (OB_FAIL(MTL(ObTenantDirectLoadMgr *)->get_tablet_mgr_and_check_major(
          ls_->get_ls_id(),
          table_key.tablet_id_,
          true/* is_full_direct_load */,
          direct_load_mgr_handle,
          is_major_sstable_exist))) {
    if (OB_ENTRY_NOT_EXIST == ret && is_major_sstable_exist) {
      ret = OB_SUCCESS;
      LOG_INFO("ddl commit log is expired, skip", K(ret), KPC(log_), K(scn_));
    } else {
      LOG_WARN("get tablet mgr failed", K(ret), K(table_key));
    }
  } else if (OB_ISNULL(data_direct_load_mgr = direct_load_mgr_handle.get_full_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(table_key));
  } else if (OB_FAIL(data_direct_load_mgr->commit(*tablet_handle.get_obj(), log_->get_start_scn(), scn_, 0/*unused table_id*/, 0/*unused ddl_task_id*/, true/*is replay*/))) {
    if (OB_TABLET_NOT_EXIST == ret || OB_TASK_EXPIRED == ret) {
      ret = OB_SUCCESS; // exit when tablet not exist or task expired
    } else {
      LOG_WARN("replay ddl commit log failed", K(ret), K_(log), K_(scn));
    }
  } else {
    LOG_INFO("replay ddl commit log success", K(ret), K_(log), K_(scn));
  }
  LOG_INFO("finish replay ddl commit log", K(ret), K(need_replay), K_(log), K_(scn), "ddl_event_info", ObDDLEventInfo());
  return ret;
}


// ObSchemaChangeReplayExecutor
ObSchemaChangeReplayExecutor::ObSchemaChangeReplayExecutor()
  : logservice::ObTabletReplayExecutor(), log_(nullptr), scn_()
{

}

int ObSchemaChangeReplayExecutor::init(
    const ObTabletSchemaVersionChangeLog &log,
    const SCN &scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!log.is_valid())
          || OB_UNLIKELY(!scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(log), K(scn), K(ret));
  } else {
    log_ = &log;
    scn_ = scn;
    is_inited_ = true;
  }

  return ret;
}

int ObSchemaChangeReplayExecutor::do_replay_(ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(handle.get_obj()->replay_schema_version_change_log(log_->get_schema_version()))) {
    LOG_WARN("fail to replay schema version change log", K(ret), KPC_(log));
  } else {
    LOG_INFO("replay tablet schema version change log success", KPC_(log), K_(scn));
  }
  return ret;
}
