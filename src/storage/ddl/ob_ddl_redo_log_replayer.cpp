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

#define USING_LOG_PREFIX STORAGE

#include "ob_ddl_redo_log_replayer.h"
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

ObDDLRedoLogReplayer::ObDDLRedoLogReplayer()
  : is_inited_(false), ls_(nullptr), allocator_()
{
}

ObDDLRedoLogReplayer::~ObDDLRedoLogReplayer()
{
  destroy();
}

int ObDDLRedoLogReplayer::init(ObLS *ls)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDDLRedoLogReplayer has been inited twice", K(ret));
  } else if (OB_FAIL(allocator_.init(TOTAL_LIMIT, HOLD_LIMIT, OB_MALLOC_NORMAL_BLOCK_SIZE))) {
    LOG_WARN("fail to init allocator", K(ret));
  } else if (OB_FAIL(bucket_lock_.init(DEFAULT_HASH_BUCKET_COUNT))) {
    LOG_WARN("fail to init bucket lock", K(ret));
  } else {
    ls_ = ls;
    is_inited_ = true;
  }
  return ret;
}

int ObDDLRedoLogReplayer::replay_start(const ObDDLStartLog &log, const SCN &scn)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObITable::TableKey table_key = log.get_table_key();
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  bool need_replay = true;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedoLogReplayer has not been inited", K(ret));
  } else if (OB_UNLIKELY(!log.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(log));
  } else if (OB_FAIL(check_need_replay_ddl_log(table_key, scn, scn, need_replay, tablet_handle))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("fail to check need replay ddl log", K(ret), K(table_key), K(scn));
    }
  } else if (!need_replay) {
    // do nothing
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("need replay but tablet handle is invalid", K(ret), K(need_replay), K(tablet_handle));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle, true/*try_create*/))) {
    LOG_WARN("create ddl kv mgr failed", K(ret), K(table_key), K(log), K(scn));
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->ddl_start(*tablet_handle.get_obj(),
                                                            table_key,
                                                            scn,
                                                            log.get_cluster_version(),
                                                            log.get_execution_id(),
                                                            SCN::min_scn()/*checkpoint_scn*/))) {
    if (OB_TASK_EXPIRED != ret) {
      LOG_WARN("start ddl log failed", K(ret), K(log), K(scn));
    } else {
      ret = OB_SUCCESS; // ignored expired ddl start log
    }
  } else {
    LOG_INFO("succeed to replay ddl start log", K(ret), K(log), K(scn));
  }
  LOG_INFO("finish replay ddl start log", K(ret), K(need_replay), K(log), K(scn));
  return ret;
}

int ObDDLRedoLogReplayer::replay_redo(const ObDDLRedoLog &log, const SCN &scn)
{
  int ret = OB_SUCCESS;
  const ObDDLMacroBlockRedoInfo &redo_info = log.get_redo_info();
  const ObITable::TableKey table_key = redo_info.table_key_;
  bool need_replay = true;
  ObTabletHandle tablet_handle;
  ObDDLKV *ddl_kv = nullptr;

  DEBUG_SYNC(BEFORE_REPLAY_DDL_MACRO_BLOCK);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedoLogReplayer has not been inited", K(ret));
  } else if (OB_UNLIKELY(!log.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(log));
  } else if (OB_FAIL(check_need_replay_ddl_log(table_key, redo_info.start_scn_, scn, need_replay, tablet_handle))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("fail to check need replay ddl log", K(ret), K(table_key), K(scn));
    }
  } else if (!need_replay) {
    // do nothing
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("need replay but tablet handle is invalid", K(ret), K(need_replay), K(tablet_handle));
  } else if (!tablet_handle.get_obj()->get_table_store().get_major_sstables().empty()) {
    // major sstable already exist, means ddl commit success
    need_replay = false;
    if (REACH_TIME_INTERVAL(1000L * 1000L)) {
      LOG_INFO("no need to replay ddl log, because the major sstable already exist", K(table_key));
    }
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
      macro_block.scn_ = scn;
      macro_block.buf_ = redo_info.data_buffer_.ptr();
      macro_block.size_ = redo_info.data_buffer_.length();
      macro_block.ddl_start_scn_ = redo_info.start_scn_;
      if (OB_FAIL(ObDDLKVPendingGuard::set_macro_block(tablet_handle.get_obj(), macro_block))) {
        LOG_WARN("set macro block into ddl kv failed", K(ret), K(tablet_handle), K(macro_block));
      }
    }
  }
  LOG_INFO("finish replay ddl redo log", K(ret), K(need_replay), K(log));
  return ret;
}

int ObDDLRedoLogReplayer::replay_commit(const ObDDLCommitLog &log, const SCN &scn)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObITable::TableKey table_key = log.get_table_key();
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  ObDDLKV *ddl_kv = nullptr;
  bool need_replay = true;

  DEBUG_SYNC(BEFORE_REPLAY_DDL_PREPRARE);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedoLogReplayer has not been inited", K(ret));
  } else if (OB_UNLIKELY(!log.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(log));
  } else if (OB_FAIL(check_need_replay_ddl_log(table_key, log.get_start_scn(), scn, need_replay, tablet_handle))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("fail to check need replay ddl log", K(ret), K(table_key), K(scn), K(log));
    }
  } else if (!need_replay) {
    // do nothing
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("need replay but tablet handle is invalid", K(ret), K(need_replay), K(tablet_handle), K(log), K(scn));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    LOG_WARN("get ddl kv mgr failed", K(ret), K(log), K(scn));
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->set_commit_scn(scn))) {
    LOG_WARN("failed to start prepare", K(ret), K(log), K(scn));
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->ddl_commit(log.get_start_scn(), scn))) {
    if (OB_TABLET_NOT_EXIST == ret || OB_TASK_EXPIRED == ret) {
      ret = OB_SUCCESS; // exit when tablet not exist or task expired
    } else {
      LOG_WARN("replay ddl commit log failed", K(ret), K(log), K(scn));
    }
  } else {
    LOG_INFO("replay ddl commit log success", K(ret), K(log), K(scn));
  }
  LOG_INFO("finish replay ddl commit log", K(ret), K(need_replay), K(log), K(scn));
  return ret;
}

int ObDDLRedoLogReplayer::check_need_replay_ddl_log(const ObITable::TableKey &table_key,
                                                    const SCN &ddl_start_scn,
                                                    const SCN &scn,
                                                    bool &need_replay,
                                                    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  need_replay = true;
  ObTableHandleV2 table_handle;
  ObSSTable *sstable = nullptr;
  ObTablet *tablet = nullptr;
  ObSSTableArray major_sstables;
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
    LOG_INFO("ls migration failed, so ddl log skip replay", "ls_id", ls_->get_ls_id(), "tablet_id", table_key.tablet_id_, K(migration_status));
  } else if (OB_FAIL(ls_->get_tablet(table_key.tablet_id_, tablet_handle, ObTabletCommon::NO_CHECK_GET_TABLET_TIMEOUT_US))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      // we are sure that tablet does not exist, so we have no need to replay
      need_replay = false;
      LOG_INFO("tablet not exist, so ddl log skip replay", "ls_id", ls_->get_ls_id(), "tablet_id", table_key.tablet_id_);
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get tablet", K(ret), "tablet_id", table_key.tablet_id_, K(scn));
    }
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is null", K(ret), K(tablet_handle));
  } else if (tablet->get_tablet_meta().ha_status_.is_expected_status_deleted()) {
    need_replay = false;
    if (REACH_COUNT_INTERVAL(1000L)) {
      LOG_INFO("no need to replay ddl log, because tablet will be deleted",
          K(table_key), "tablet_meta", tablet->get_tablet_meta());
    }
  } else if (scn <= tablet->get_tablet_meta().ddl_checkpoint_scn_) {
    need_replay = false;
    if (REACH_COUNT_INTERVAL(1000L)) {
      LOG_INFO("no need to replay ddl log, because the log ts is less than the ddl checkpoint ts",
          K(table_key), K(scn), "ddl_checkpoint_ts", tablet->get_tablet_meta().ddl_checkpoint_scn_);
    }
  } else if (OB_FAIL(tablet->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("get ddl kv manager failed", K(ret), K(table_key));
    } else {
      need_replay = (ddl_start_scn == scn); // only replay start log if ddl kv mgr is null
      ret = OB_SUCCESS;
    }
  } else if (ddl_start_scn < ddl_kv_mgr_handle.get_obj()->get_start_scn()) {
    need_replay = false;
    if (REACH_COUNT_INTERVAL(1000L)) {
      LOG_INFO("no need to replay ddl log, because the ddl start log ts is less than the value in ddl kv manager",
          K(table_key), K(ddl_start_scn), "ddl_start_scn_in_ddl_kv_mgr", ddl_kv_mgr_handle.get_obj()->get_start_scn());
    }
  }
  return ret;
}

void ObDDLRedoLogReplayer::destroy()
{
  is_inited_ = false;
  ls_ = nullptr;
  allocator_.reset();
}
