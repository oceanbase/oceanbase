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
  } else if (OB_FAIL(tablet->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get ddl kv manager failed", K(ret), K(tablet_handle));
    } else {
      need_replay = (ddl_start_scn == scn); // only replay start log if ddl kv mgr is null
      ret = OB_SUCCESS;
    }
  } else if (ddl_start_scn < ddl_kv_mgr_handle.get_obj()->get_start_scn()) {
    need_replay = false;
    if (REACH_COUNT_INTERVAL(1000L)) {
      LOG_INFO("no need to replay ddl log, because the ddl start log ts is less than the value in ddl kv manager",
          K(tablet_handle), K(ddl_start_scn), "ddl_start_scn_in_ddl_kv_mgr", ddl_kv_mgr_handle.get_obj()->get_start_scn());
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

int ObDDLStartReplayExecutor::do_replay_(ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;
  ObITable::TableKey table_key = log_->get_table_key();
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  bool need_replay = true;
  if (OB_FAIL(check_need_replay_ddl_log_(handle, scn_, scn_, need_replay))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("fail to check need replay ddl log", K(ret), K(handle), K(scn_));
    }
  } else if (!need_replay) {
    ret = OB_NO_NEED_UPDATE;
    LOG_WARN("skip replay ddl start", K(ret), "ls_id", ls_->get_ls_id(), K(handle));
  } else if (OB_FAIL(handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle, true/*try_create*/))) {
    LOG_WARN("create ddl kv mgr failed", K(ret), K(handle), KPC_(log), K_(scn));
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->ddl_start(*ls_,
                                                            *handle.get_obj(),
                                                            table_key,
                                                            scn_,
                                                            log_->get_data_format_version(),
                                                            log_->get_execution_id(),
                                                            SCN::min_scn()/*checkpoint_scn*/))) {
    if (OB_TASK_EXPIRED != ret) {
      LOG_WARN("start ddl log failed", K(ret), KPC_(log), K_(scn));
    } else {
      ret = OB_SUCCESS; // ignored expired ddl start log
    }
  } else {
    LOG_INFO("succeed to replay ddl start log", K(ret), KPC_(log), K_(scn));
  }
  LOG_INFO("finish replay ddl start log", K(ret), K(need_replay), KPC_(log), K_(scn));
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

int ObDDLRedoReplayExecutor::do_replay_(ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;
  const ObDDLMacroBlockRedoInfo &redo_info = log_->get_redo_info();
  bool need_replay = true;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;

  if (OB_FAIL(check_need_replay_ddl_log_(handle, redo_info.start_scn_, scn_, need_replay))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("fail to check need replay ddl log", K(ret), K(handle), K_(scn));
    }
  } else if (!need_replay) {
    ret = OB_NO_NEED_UPDATE;
    LOG_WARN("skip replay ddl redo", K(ret), "ls_id", ls_->get_ls_id(), K(handle));
  } else if (OB_UNLIKELY(!handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("need replay but tablet handle is invalid", K(ret), K(need_replay), K(handle));
  } else if (OB_FAIL(handle.get_obj()->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else {
    ObMacroBlockWriteInfo write_info;
    ObMacroBlockHandle macro_handle;
    write_info.buffer_ = redo_info.data_buffer_.ptr();
    write_info.size_= redo_info.data_buffer_.length();
    write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    const int64_t io_timeout_ms = max(DDL_FLUSH_MACRO_BLOCK_TIMEOUT / 1000L, GCONF._data_storage_io_timeout / 1000L);
    ObDDLMacroBlock macro_block;
    if (OB_FAIL(ObBlockManager::async_write_block(write_info, macro_handle))) {
      LOG_WARN("fail to async write block", K(ret), K(write_info), K(macro_handle));
    } else if (OB_FAIL(macro_handle.wait(io_timeout_ms))) {
      LOG_WARN("fail to wait macro block io finish", K(ret));
    } else if (OB_FAIL(macro_block.block_handle_.set_block_id(macro_handle.get_macro_id()))) {
      LOG_WARN("set macro block id failed", K(ret), K(macro_handle.get_macro_id()));
    } else {
      macro_block.block_type_ = redo_info.block_type_;
      macro_block.logic_id_ = redo_info.logic_id_;
      macro_block.scn_ = scn_;
      macro_block.buf_ = redo_info.data_buffer_.ptr();
      macro_block.size_ = redo_info.data_buffer_.length();
      macro_block.ddl_start_scn_ = redo_info.start_scn_;
      if (OB_FAIL(ObDDLKVPendingGuard::set_macro_block(handle.get_obj(), macro_block))) {
        LOG_WARN("set macro block into ddl kv failed", K(ret), K(handle), K(macro_block));
      }
    }
  }
  LOG_INFO("finish replay ddl redo log", K(ret), K(need_replay), KPC_(log));
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

int ObDDLCommitReplayExecutor::do_replay_(ObTabletHandle &handle) //TODO(jianyun.sjy): check it
{
  int ret = OB_SUCCESS;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  bool need_replay = true;

  if (OB_FAIL(check_need_replay_ddl_log_(handle, log_->get_start_scn(), scn_, need_replay))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("fail to check need replay ddl log", K(ret), K(handle), K_(scn), KPC_(log));
    }
  } else if (!need_replay) {
    ret = OB_NO_NEED_UPDATE;
    LOG_WARN("skip replay ddl commit", K(ret), "ls_id", ls_->get_ls_id(), K(handle));
  } else if (OB_FAIL(handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    LOG_WARN("get ddl kv mgr failed", K(ret), K_(scn), KPC_(log));
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->set_commit_scn(handle.get_obj()->get_tablet_meta(), scn_))) {
    LOG_WARN("failed to start prepare", K(ret), KPC_(log), K_(scn));
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->ddl_commit(*handle.get_obj(), log_->get_start_scn(), scn_))) {
    if (OB_TABLET_NOT_EXIST == ret || OB_TASK_EXPIRED == ret) {
      ret = OB_SUCCESS; // exit when tablet not exist or task expired
    } else {
      LOG_WARN("replay ddl commit log failed", K(ret), KPC_(log), K_(scn));
    }
  } else {
    LOG_INFO("replay ddl commit log success", K(ret), KPC_(log), K_(scn));
  }
  LOG_INFO("finish replay ddl commit log", K(ret), K(need_replay), K_(scn), KPC_(log));
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
