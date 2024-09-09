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
#include "storage/tablet/ob_tablet.h"
#include "storage/meta_store/ob_tenant_storage_meta_service.h"
#include "storage/blockstore/ob_shared_object_reader_writer.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "close_modules/shared_storage/storage/compaction/ob_refresh_tablet_util.h"
#endif
using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;
using namespace oceanbase::share;

ERRSIM_POINT_DEF(EN_REPLAY_REDO_DDL_LOG_WAIT);

ObDDLReplayExecutor::ObDDLReplayExecutor()
  : logservice::ObTabletReplayExecutor(), ls_(nullptr), scn_()
{}

int ObDDLReplayExecutor::check_need_replay_ddl_log_(
    const ObLS *ls,
    const ObTabletHandle &tablet_handle,
    const share::SCN &ddl_start_scn,
    const share::SCN &scn,
    const uint64_t data_format_version,
    bool &need_replay)
{
  int ret = OB_SUCCESS;
  need_replay = true;
  ObTablet *tablet = nullptr;
  ObMigrationStatus migration_status;
  if (OB_UNLIKELY(nullptr == ls || !tablet_handle.is_valid() || (!ObDDLUtil::use_idempotent_mode(data_format_version) && !ddl_start_scn.is_valid_and_not_min()) || !scn.is_valid_and_not_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not init", K(ret), KP(ls), K(tablet_handle), K(ddl_start_scn), K(scn));
  } else if (OB_FAIL(check_need_replay_(ls, tablet_handle, need_replay))) {
    LOG_WARN("fail to check need replay", K(ret), KP(ls), K(tablet_handle));
  } else if (!need_replay) {
    // do nothing
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is null", K(ret), K(tablet_handle));
  } else if (scn <= tablet->get_tablet_meta().ddl_checkpoint_scn_) {
    need_replay = false;
    if (REACH_COUNT_INTERVAL(1000L)) {
      LOG_INFO("no need to replay ddl log, because the log ts is less than the ddl checkpoint ts",
          K(tablet_handle), K(scn), "ddl_checkpoint_ts", tablet->get_tablet_meta().ddl_checkpoint_scn_);
    }
  } else if (!ObDDLUtil::use_idempotent_mode(data_format_version) && (ddl_start_scn < tablet->get_tablet_meta().ddl_start_scn_)) {
    need_replay = false;
    if (REACH_COUNT_INTERVAL(1000L)) {
      LOG_INFO("no need to replay ddl log, because the ddl start log ts is less than the value in ddl kv manager",
          K(tablet_handle), K(ddl_start_scn), "ddl_start_scn_in_tablet", tablet->get_tablet_meta().ddl_start_scn_);
    }
  }
  return ret;
}

int ObDDLReplayExecutor::check_need_replay_ddl_inc_log_(
    const ObLS *ls,
    const ObTabletHandle &tablet_handle,
    const share::SCN &scn,
    bool &need_replay)
{
  int ret = OB_SUCCESS;
  need_replay = true;
  ObTablet *tablet = nullptr;
  ObMigrationStatus migration_status;
  if (OB_UNLIKELY(nullptr == ls || !tablet_handle.is_valid() || !scn.is_valid_and_not_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not init", K(ret), KP(ls), K(tablet_handle), K(scn));
  } else if (OB_FAIL(check_need_replay_(ls, tablet_handle, need_replay))) {
    LOG_WARN("fail to check need replay", K(ret), KP(ls), K(tablet_handle));
  } else if (!need_replay) {
    // do nothing
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is null", K(ret), K(tablet_handle));
  } else if (scn <= tablet->get_tablet_meta().clog_checkpoint_scn_) {
    need_replay = false;
    if (REACH_COUNT_INTERVAL(1000L)) {
      LOG_INFO("no need to replay ddl inc log, because the log ts is less than the clog checkpoint ts",
          K(tablet_handle), K(scn), "clog_checkpoint_ts", tablet->get_tablet_meta().clog_checkpoint_scn_);
    }
  }

  return ret;
}

int ObDDLReplayExecutor::check_need_replay_(
    const ObLS *ls,
    const ObTabletHandle &tablet_handle,
    bool &need_replay)
{
  int ret = OB_SUCCESS;
  need_replay = true;
  ObTablet *tablet = nullptr;
  ObMigrationStatus migration_status;
  if (OB_FAIL(ls->get_ls_meta().get_migration_status(migration_status))) {
    LOG_WARN("get migration status failed", K(ret), KPC(ls));
  } else if (ObMigrationStatus::OB_MIGRATION_STATUS_ADD_FAIL == migration_status
      || ObMigrationStatus::OB_MIGRATION_STATUS_MIGRATE_FAIL == migration_status) {
    need_replay = false;
    LOG_INFO("ls migration failed, so ddl log skip replay", "ls_id", ls->get_ls_id(), K(tablet_handle), K(migration_status));
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
  }

  return ret;
}

int ObDDLReplayExecutor::get_lob_meta_tablet_id(
    const ObTabletHandle &tablet_handle,
    const common::ObTabletID &possible_lob_meta_tablet_id,
    common::ObTabletID &lob_meta_tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_handle));
  } else if (ObDDLClog::COMPATIBLE_LOB_META_TABLET_ID == possible_lob_meta_tablet_id.id()) { // compatible code
    ObTabletBindingMdsUserData ddl_data;
    if (OB_FAIL(tablet_handle.get_obj()->ObITabletMdsInterface::get_ddl_data(share::SCN::max_scn(), ddl_data))) {
      LOG_WARN("failed to get ddl data from tablet", K(ret), K(tablet_handle));
    } else {
      lob_meta_tablet_id = ddl_data.lob_meta_tablet_id_;
    }
  } else {
    lob_meta_tablet_id = possible_lob_meta_tablet_id;
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
  ObTabletID lob_meta_tablet_id;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedoLogReplayer has not been inited", K(ret));
  } else if (OB_UNLIKELY(!log_->is_valid() || !tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K_(log), K(tablet_handle));
  } else if (OB_FAIL(ObDDLReplayExecutor::get_lob_meta_tablet_id(tablet_handle, log_->get_lob_meta_tablet_id(), lob_meta_tablet_id))) {
    LOG_WARN("get lob meta tablet id failed", K(ret));
  } else if (lob_meta_tablet_id.is_valid()) {
    ObTabletHandle lob_meta_tablet_handle;
    const bool replay_allow_tablet_not_exist = true;
    if (OB_FAIL(ls_->replay_get_tablet_no_check(lob_meta_tablet_id, scn_,
        replay_allow_tablet_not_exist, lob_meta_tablet_handle))) {
      LOG_WARN("get tablet handle failed", K(ret), K(lob_meta_tablet_id), K(scn_));
    } else if (OB_FAIL(replay_ddl_start(lob_meta_tablet_handle, true/*is_lob_meta_tablet*/))) {
      LOG_WARN("replay ddl start for lob meta tablet failed", K(ret), K(lob_meta_tablet_id), K(scn_));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(replay_ddl_start(tablet_handle, false/*is_lob_meta_tablet*/))) {
      LOG_WARN("replay ddl start for data tablet failed", K(ret));
    }
  }
  return ret;
}

int ObDDLStartReplayExecutor::replay_ddl_start(ObTabletHandle &tablet_handle, const bool is_lob_meta_tablet)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObTabletDirectLoadMgrHandle direct_load_mgr_handle;
  ObTenantDirectLoadMgr *tenant_direct_load_mgr = MTL(ObTenantDirectLoadMgr *);
  const int64_t unused_context_id = -1;
  bool need_replay = true;
  ObTabletID tablet_id;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedoLogReplayer has not been inited", K(ret));
  } else if (OB_UNLIKELY(!log_->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K_(log));
  } else if (OB_FAIL(check_need_replay_ddl_log_(ls_, tablet_handle, scn_, scn_, log_->get_data_format_version(), need_replay))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("fail to check need replay ddl log", K(ret), K(tablet_id), K_(scn));
    }
  } else if (!need_replay) {
    // do nothing
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("need replay but tablet handle is invalid", K(ret), K(need_replay), K(tablet_handle));
  } else if (FALSE_IT(tablet_id = tablet_handle.get_obj()->get_tablet_id())) {
  } else if (OB_ISNULL(tenant_direct_load_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), K(MTL_ID()));
  } else {
    ObTabletDirectLoadInsertParam direct_load_param;
    direct_load_param.is_replay_ = true;
    bool is_major_sstable_exist = false;
    const int64_t snapshot_version = log_->get_table_key().get_snapshot_version();
    direct_load_param.common_param_.ls_id_ = tablet_handle.get_obj()->get_tablet_meta().ls_id_;
    direct_load_param.common_param_.tablet_id_ = tablet_id;
    direct_load_param.common_param_.data_format_version_ = log_->get_data_format_version();
    direct_load_param.common_param_.direct_load_type_ = log_->get_direct_load_type();
    direct_load_param.common_param_.read_snapshot_ = snapshot_version;
    ObITable::TableKey table_key;
    if (is_lob_meta_tablet) {
      table_key.table_type_ = ObITable::MAJOR_SSTABLE;
      table_key.tablet_id_ = tablet_id;
      table_key.column_group_idx_ = 0;
      table_key.version_range_.base_version_ = 0;
      table_key.version_range_.snapshot_version_ = snapshot_version;
    } else {
      table_key = log_->get_table_key();
    }

    if (!is_lob_meta_tablet && ls_->is_cs_replica() && OB_FAIL(pre_process_for_cs_replica(direct_load_param, table_key, tablet_handle, tablet_id))) {
      LOG_WARN("pre process for cs replica failed", K(ret), K(direct_load_param), K(table_key), K(tablet_id));
    } else if (OB_FAIL(tenant_direct_load_mgr->replay_create_tablet_direct_load(tablet_handle, log_->get_execution_id(), direct_load_param))) {
      LOG_WARN("create tablet manager failed", K(ret));
    } else if (OB_FAIL(tenant_direct_load_mgr->get_tablet_mgr_and_check_major(
            ls_->get_ls_id(),
            tablet_id,
            true/* is_full_direct_load */,
            direct_load_mgr_handle,
            is_major_sstable_exist))) {
      if (OB_ENTRY_NOT_EXIST == ret && is_major_sstable_exist) {
        ret = OB_SUCCESS;
        LOG_INFO("ddl start log is expired, skip", K(ret), KPC(log_), K(scn_));
      } else {
        LOG_WARN("get tablet mgr failed", K(ret), K(tablet_id));
      }
    } else if (OB_FAIL(direct_load_mgr_handle.get_full_obj()->update(
            nullptr/*lob_direct_load_mgr*/, // replay is independent for data and lob meta tablet, force null here
            direct_load_param))) {
      LOG_WARN("update direct load mgr failed", K(ret));
    } else if (OB_FAIL(direct_load_mgr_handle.get_full_obj()->start(*tablet_handle.get_obj(),
            table_key, scn_, log_->get_data_format_version(), log_->get_execution_id(), SCN::min_scn()/*checkpoint_scn*/,
            direct_load_param.common_param_.replay_normal_in_cs_replica_))) {
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
  FLOG_INFO("finish replay ddl start log", K(ret), K(need_replay), K(tablet_id), KPC_(log), K_(scn), "ddl_event_info", ObDDLEventInfo());
  return ret;
}

int ObDDLStartReplayExecutor::pre_process_for_cs_replica(
    ObTabletDirectLoadInsertParam &direct_load_param,
    ObITable::TableKey &table_key,
    ObTabletHandle &tablet_handle,
    const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (log_->get_with_cs_replica()) {
    if (log_->get_table_key().is_row_store_major_sstable()) {
      table_key.table_type_ = ObITable::COLUMN_ORIENTED_SSTABLE; // for passing defence
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid table key for replay ddl start in cs replica", K(ret), K_(log));
    }
  } else {
    // ddl is concurrent with adding C replica
    const ObTablet *tablet = nullptr;
    ObStorageSchema *schema_on_tablet = nullptr;
    ObArenaAllocator tmp_arena("RplyStartTmp");
    if (OB_UNLIKELY(!tablet_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet handle is invalid", K(ret), K(tablet_handle));
    } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet is null", K(ret), K(tablet_id));
    } else if (tablet->is_row_store()) {
      // not be created to column store tablet in cs replica, means this tablet is not user data tablet, ignore
    } else if (log_->get_table_key().is_column_store_major_sstable()) {
      // column store tablet originally, ignore
    } else if (OB_FAIL(tablet->load_storage_schema(tmp_arena, schema_on_tablet))) {
      LOG_WARN("load storage schema failed", K(ret), KPC(tablet));
    } else if (schema_on_tablet->is_cs_replica_compat()) {
      direct_load_param.common_param_.replay_normal_in_cs_replica_ = true;
      LOG_TRACE("[CS-Replica] process concurrent ddl and ls migration", K(ret), K(tablet_id), KPC(tablet));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid storage schema status", K(ret), KPC(tablet), KPC(schema_on_tablet));
    }
    ObTabletObjLoadHelper::free(tmp_arena, schema_on_tablet);
  }
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
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedoLogExecutor has not been inited", K(ret));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tablet_handle));
  } else {
    const ObDDLMacroBlockRedoInfo &redo_info = log_->get_redo_info();
    ObMacroBlockWriteInfo write_info;
    ObDDLMacroBlock macro_block;
    bool can_skip = false;
    write_info.buffer_ = redo_info.data_buffer_.ptr();
    write_info.size_= redo_info.data_buffer_.length();
    write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    write_info.io_timeout_ms_ = max(DDL_FLUSH_MACRO_BLOCK_TIMEOUT / 1000L, GCONF._data_storage_io_timeout / 1000L);
    macro_block.block_type_ = redo_info.block_type_;
    macro_block.logic_id_ = redo_info.logic_id_;
    macro_block.scn_ = scn_;
    macro_block.ddl_start_scn_ = redo_info.start_scn_;
    macro_block.table_key_ = redo_info.table_key_;
    macro_block.end_row_id_ = redo_info.end_row_id_;
    macro_block.trans_id_ = redo_info.trans_id_;
    if (is_incremental_direct_load(redo_info.type_)) {
      if (OB_FAIL(do_inc_replay_(tablet_handle, write_info, macro_block))) {
        LOG_WARN("fail to do inc replay", K(ret));
        if (OB_TABLET_NOT_EXIST == ret || OB_NO_NEED_UPDATE == ret) {
          LOG_INFO("no need to replay ddl inc redo log", K(ret));
          ret = OB_SUCCESS;
        } else if (OB_EAGAIN != ret) {
          LOG_WARN("failed to do_inc_replay_", K(ret), K(log_), K(scn_));
          ret = OB_EAGAIN;
        }
      }
    } else if (OB_FAIL(filter_redo_log_(redo_info, tablet_handle, can_skip))) {
      LOG_WARN("fail to filter redo log", K(ret), K(redo_info), K_(ls));
    } else if (can_skip) {
    } else {
      if (OB_FAIL(do_full_replay_(tablet_handle, write_info, macro_block))) {
        LOG_WARN("fail to do full replay", K(ret));
      }
    }
  }

  return ret;
}

int ObDDLRedoReplayExecutor::do_inc_replay_(
    ObTabletHandle &tablet_handle,
    blocksstable::ObMacroBlockWriteInfo &write_info,
    storage::ObDDLMacroBlock &macro_block)
{
  int ret = OB_SUCCESS;
  bool need_replay = true;
  if (OB_FAIL(check_need_replay_ddl_inc_log_(ls_, tablet_handle, scn_, need_replay))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("fail to check need replay ddl log", K(ret), K(scn_), K(log_));
    }
  } else if (!need_replay) {
    // do nothing
  } else {
    ObStorageObjectOpt opt;
    opt.set_private_object_opt(tablet_handle.get_obj()->get_tablet_id().id());
    ObStorageObjectWriteInfo object_write_info;
    object_write_info.buffer_ = write_info.buffer_;
    object_write_info.size_= write_info.size_;
    object_write_info.offset_ = 0;
    object_write_info.io_desc_ = write_info.io_desc_;
    object_write_info.io_desc_.set_sealed();
    object_write_info.io_timeout_ms_ = write_info.io_timeout_ms_;
    object_write_info.mtl_tenant_id_ = MTL_ID();
    ObStorageObjectHandle macro_handle;
    const ObDDLMacroBlockRedoInfo &redo_info = log_->get_redo_info();
    const ObITable::TableKey &table_key = redo_info.table_key_;
    ObTabletID tablet_id = table_key.get_tablet_id();
    const int64_t snapshot_version = table_key .get_snapshot_version();
    const uint64_t data_format_version = redo_info.data_format_version_;
    if (OB_FAIL(ObObjectManager::async_write_object(opt, object_write_info, macro_handle))) {
      LOG_WARN("fail to async write block", K(ret), K(object_write_info), K(macro_handle));
    } else if (OB_FAIL(macro_handle.wait())) {
      LOG_WARN("fail to wait macro block io finish", K(ret), K(object_write_info));
    } else if (OB_FAIL(macro_block.block_handle_.set_block_id(macro_handle.get_macro_id()))) {
      LOG_WARN("set macro block id failed", K(ret), K(macro_handle.get_macro_id()));
    } else if (OB_FAIL(macro_block.set_data_macro_meta(macro_handle.get_macro_id(),
                                                       redo_info.data_buffer_.ptr(),
                                                       redo_info.data_buffer_.length(),
                                                       redo_info.block_type_,
                                                       true /*force to set macro meta*/))) {
      LOG_WARN("fail to set data macro meta", K(ret), K(macro_handle.get_macro_id()),
                                                      KP(redo_info.data_buffer_.ptr()),
                                                      K(redo_info.data_buffer_.length()),
                                                      K(redo_info.block_type_));
    } else if (OB_FAIL(tablet_handle.get_obj()->set_macro_block(macro_block, snapshot_version, data_format_version))) {
      LOG_WARN("fail to set_inc_macro_block", K(ret));
    }
    FLOG_INFO("finish replay ddl inc redo log", K(ret), KPC_(log), K(macro_block), "ddl_event_info", ObDDLEventInfo());
  }

  return ret;
}

int ObDDLRedoReplayExecutor::do_full_replay_(
    ObTabletHandle &tablet_handle,
    blocksstable::ObMacroBlockWriteInfo &write_info,
    storage::ObDDLMacroBlock &macro_block)
{
  int ret = OB_SUCCESS;
  ObMacroBlockHandle macro_handle;
  ObTabletID tablet_id = log_->get_redo_info().table_key_.get_tablet_id();
  bool need_replay = true;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  if (OB_FAIL(check_need_replay_ddl_log_(ls_, tablet_handle, log_->get_redo_info().start_scn_, scn_, log_->get_redo_info().data_format_version_, need_replay))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("fail to check need replay ddl log", K(ret), K_(tablet_id), K_(scn));
    }
  } else if (!need_replay) {
    // do nothing
  } else if (OB_FAIL(tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (!table_store_wrapper.get_member()->get_major_sstables().empty()) {
    // major sstable already exist, means ddl commit success
    need_replay = false;
    if (REACH_TIME_INTERVAL(1000L * 1000L)) {
      LOG_INFO("no need to replay ddl log, because the major sstable already exist", K_(tablet_id));
    }
  } else {
    const ObDDLMacroBlockRedoInfo &redo_info = log_->get_redo_info();
    ObStorageObjectOpt opt;
    opt.set_private_object_opt(tablet_handle.get_obj()->get_tablet_id().id());
    ObStorageObjectHandle macro_handle;
    ObStorageObjectWriteInfo write_info;
    write_info.buffer_ = redo_info.data_buffer_.ptr();
    write_info.size_= redo_info.data_buffer_.length();
    write_info.offset_ = 0;
    write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    write_info.io_desc_.set_sealed();
    write_info.io_timeout_ms_ = max(DDL_FLUSH_MACRO_BLOCK_TIMEOUT / 1000L, GCONF._data_storage_io_timeout / 1000L);
    write_info.mtl_tenant_id_ = MTL_ID();

    #ifdef OB_BUILD_SHARED_STORAGE
    if (GCTX.is_shared_storage_mode()){
      /* write gc occupy file*/
      if (OB_FAIL(ObDDLRedoLogWriter::write_gc_flag(tablet_handle,
                                                    redo_info.table_key_,
                                                    redo_info.parallel_cnt_,
                                                    redo_info.cg_cnt_))) {
        LOG_WARN("failed to write tablet gc flag file", K(ret));
      } else if (OB_FAIL(write_ss_block(write_info, macro_handle))) {
        LOG_WARN("failed to write shared storage block", K(ret));
      } else if (OB_FAIL(macro_block.block_handle_.set_block_id(log_->get_redo_info().macro_block_id_))) {
        LOG_WARN("set macro block id failed", K(ret), K(log_->get_redo_info().macro_block_id_));
      }
    } else
    #endif
    if (!GCTX.is_shared_storage_mode()) {
      if (OB_FAIL(ObObjectManager::async_write_object(opt, write_info, macro_handle))) {
        LOG_WARN("fail to async write block", K(ret), K(write_info), K(macro_handle));
      } else if (OB_FAIL(macro_handle.wait())) {
        LOG_WARN("fail to wait macro block io finish", K(ret), K(write_info));
      } else if (OB_FAIL(macro_block.block_handle_.set_block_id(macro_handle.get_macro_id()))) {
        LOG_WARN("set macro block id failed", K(ret), K(macro_handle.get_macro_id()));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(macro_block.set_data_macro_meta(macro_block.block_handle_.get_block_id(),
                                                       redo_info.data_buffer_.ptr(),
                                                       redo_info.data_buffer_.length(),
                                                       redo_info.block_type_))) {
      LOG_WARN("fail to set data macro meta", K(ret), K(macro_handle.get_macro_id()),
                                                      KP(redo_info.data_buffer_.ptr()),
                                                      K(redo_info.data_buffer_.length()),
                                                      K(redo_info.block_type_));
    } else {
      macro_block.block_type_ = redo_info.block_type_;
      macro_block.logic_id_ = redo_info.logic_id_;
      macro_block.scn_ = scn_;
      macro_block.ddl_start_scn_ = redo_info.start_scn_;
      macro_block.table_key_ = redo_info.table_key_;
      macro_block.end_row_id_ = redo_info.end_row_id_;
      const int64_t snapshot_version = redo_info.table_key_.get_snapshot_version();
      const ObITable::TableKey &table_key = redo_info.table_key_;
      bool is_major_sstable_exist = false;
      uint64_t data_format_version = redo_info.data_format_version_;
      ObTabletDirectLoadMgrHandle direct_load_mgr_handle;
      ObTenantDirectLoadMgr *tenant_direct_load_mgr = MTL(ObTenantDirectLoadMgr *);
      if (OB_ISNULL(tenant_direct_load_mgr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected err", K(ret));
    #ifdef OB_BUILD_SHARED_STORAGE
      } else if (ObDDLUtil::use_idempotent_mode(data_format_version)) {
        // Do not fetch direct load mgr.
    #endif
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
          if (OB_TASK_EXPIRED == ret) {
            need_replay = false;
            LOG_INFO("task expired, skip replay the redo", K(ret), K(macro_block), K(snapshot_version), K(data_format_version));
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("set macro block into ddl kv failed", K(ret), K(tablet_handle), K(macro_block),
                K(snapshot_version), K(data_format_version));
          }
        }
      }
    }
  }
  FLOG_INFO("finish replay ddl full redo log", K(ret), K(need_replay), KPC_(log), K(macro_block), "ddl_event_info", ObDDLEventInfo());

  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObDDLRedoReplayExecutor::write_ss_block(blocksstable::ObStorageObjectWriteInfo &write_info, blocksstable::ObStorageObjectHandle &macro_handle)
{
  int ret = OB_SUCCESS;
  bool is_object_exist = false;
  if (OB_FAIL(ObObjectManager::ss_is_exist_object(log_->get_redo_info().macro_block_id_, 0 /* ls epoch, not use on share object */, is_object_exist))) {
    LOG_WARN("failed to check is object exist", K(ret), K(log_->get_redo_info().macro_block_id_));
  } else if (is_object_exist) {
    // already exist, do nothing
  } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.async_write_object(log_->get_redo_info().macro_block_id_,
                                                              write_info,
                                                              macro_handle))) {
    LOG_WARN("fail to async write block", K(ret), K(log_->get_redo_info().macro_block_id_), K(write_info), K(macro_handle));
  } else if (OB_FAIL(macro_handle.wait())) {
    LOG_WARN("fail to wait", K(ret), K(write_info));
  }
  return ret;
}
#endif

int ObDDLRedoReplayExecutor::filter_redo_log_(
    const ObDDLMacroBlockRedoInfo &redo_info,
    const ObTabletHandle &tablet_handle,
    bool &can_skip)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  bool is_cs_replica = ls_->is_cs_replica();
  can_skip = false;
  if (redo_info.is_not_compat_cs_replica()) {
    // normal
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet handle", K(ret), K(tablet_handle));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is null", K(ret), K(tablet_handle));
  } else if (is_cs_replica && redo_info.is_cs_replica_row_store()) {
    can_skip = true;
  } else if (!is_cs_replica && redo_info.is_cs_replica_column_store()) {
    can_skip = true;
  } else if (is_cs_replica && redo_info.is_cs_replica_column_store() && tablet->is_row_store()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected tablet status", K(ret), K(redo_info), "ls_meta", ls_->get_ls_meta(), KPC(tablet));
  }
  LOG_TRACE("[CS-Replica] Finish filter redo log", K(ret), K(redo_info), K(is_cs_replica), K(can_skip), KPC(tablet), K(ls_));
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = EN_REPLAY_REDO_DDL_LOG_WAIT;
    if (OB_FAIL(ret)) {
      LOG_INFO("EN_REPLAY_REDO_DDL_LOG_WAIT replay ddl redo failed", K(ret), K(redo_info), K(can_skip));
    }
  }
#endif
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

int ObDDLCommitReplayExecutor::do_replay_(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObTabletID lob_meta_tablet_id;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedoLogReplayer has not been inited", K(ret));
  } else if (OB_UNLIKELY(!log_->is_valid() || !tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K_(log), K(tablet_handle));
  } else if (OB_FAIL(ObDDLReplayExecutor::get_lob_meta_tablet_id(tablet_handle, log_->get_lob_meta_tablet_id(), lob_meta_tablet_id))) {
    LOG_WARN("get lob meta tablet id failed", K(ret));
  } else if (lob_meta_tablet_id.is_valid()) {
    ObTabletHandle lob_meta_tablet_handle;
    const bool replay_allow_tablet_not_exist = true;
    if (OB_FAIL(ls_->replay_get_tablet_no_check(lob_meta_tablet_id, scn_,
        replay_allow_tablet_not_exist, lob_meta_tablet_handle))) {
      LOG_WARN("get tablet handle failed", K(ret), K(lob_meta_tablet_id), K(scn_));
    } else if (OB_FAIL(replay_ddl_commit(lob_meta_tablet_handle))) {
      LOG_WARN("replay ddl start for lob meta tablet failed", K(ret), K(lob_meta_tablet_id), K(scn_));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(replay_ddl_commit(tablet_handle))) {
      LOG_WARN("replay ddl commit for data tablet failed", K(ret));
    }
  }
  return ret;
}

int ObDDLCommitReplayExecutor::replay_ddl_commit(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObTabletID tablet_id;
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
  } else if (OB_FAIL(check_need_replay_ddl_log_(ls_, tablet_handle, log_->get_start_scn(), scn_, DATA_VERSION_4_3_0_0 /*ddl commit log is not idempotent_mode*/, need_replay))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("fail to check need replay ddl log", K(ret), K_(scn), K_(log), "tablet", PC(tablet_handle.get_obj()));
    }
  } else if (!need_replay) {
    // do nothing
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("need replay but tablet handle is invalid", K(ret), K(need_replay), K(tablet_handle), K_(log), K_(scn));
  } else if (OB_FALSE_IT(tablet_id = tablet_handle.get_obj()->get_tablet_id())) {
  } else if (OB_ISNULL(MTL(ObTenantDirectLoadMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), K(MTL_ID()));
  } else if (OB_FAIL(MTL(ObTenantDirectLoadMgr *)->get_tablet_mgr_and_check_major(
          ls_->get_ls_id(),
          tablet_id,
          true/* is_full_direct_load */,
          direct_load_mgr_handle,
          is_major_sstable_exist))) {
    if (OB_ENTRY_NOT_EXIST == ret && is_major_sstable_exist) {
      ret = OB_SUCCESS;
      LOG_INFO("ddl commit log is expired, skip", K(ret), KPC(log_), K(scn_));
    } else {
      LOG_WARN("get tablet mgr failed", K(ret), K(tablet_id));
    }
  } else if (OB_ISNULL(data_direct_load_mgr = direct_load_mgr_handle.get_full_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(tablet_id));
  } else if (OB_FAIL(data_direct_load_mgr->commit(*tablet_handle.get_obj(), log_->get_start_scn(), scn_, 0/*unused table_id*/, 0/*unused ddl_task_id*/, true/*is replay*/))) {
    if (OB_TABLET_NOT_EXIST == ret || OB_TASK_EXPIRED == ret) {
      ret = OB_SUCCESS; // exit when tablet not exist or task expired
    } else {
      LOG_WARN("replay ddl commit log failed", K(ret), K_(log), K_(scn));
    }
  } else {
    LOG_INFO("replay ddl commit log success", K(ret), K_(log), K_(scn));
  }
  FLOG_INFO("finish replay ddl commit log", K(ret), K(need_replay), K(tablet_id), KPC_(log), K_(scn), "ddl_event_info", ObDDLEventInfo());
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
// ObDDLFinishReplayExecutor
ObDDLFinishReplayExecutor::ObDDLFinishReplayExecutor()
  : ObDDLReplayExecutor(), log_(nullptr)
{
}

int ObDDLFinishReplayExecutor::init(
    ObLS *ls,
    const ObDDLFinishLog &log,
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
    LOG_WARN("invalid arguments", K(ret), KP(ls), K(log), K(scn));
  } else {
    ls_ = ls;
    log_ = &log;
    scn_ = scn;
    is_inited_ = true;
  }
  return ret;
}

int ObDDLFinishReplayExecutor::do_replay_(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedoLogReplayer has not been inited", K(ret));
  } else if (OB_UNLIKELY(!log_->is_valid() || !tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K_(log), K(tablet_handle));
  } else if (OB_FAIL(replay_ddl_finish(tablet_handle))) {
    LOG_WARN("replay ddl finish for data tablet failed", K(ret));
  }
  return ret;
}

/*
 * replay ddl finish log,
 * upload tablet metat to oss & clean ddl kv & update tablet local
 */
int ObDDLFinishReplayExecutor::replay_ddl_finish(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObTabletID tablet_id;
  share::SCN mock_start_scn;
  bool is_major_exist = false;
  bool is_object_exist = false;
  bool need_replay = true;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "DDLRepFinish"));
  char *buf = nullptr;
  int64_t buf_len = 0;
  share::SCN consistent_scn;
  ObLSRestoreHandler *restore_handler = nullptr;
  int64_t pre_meta_version = OB_INVALID_TIMESTAMP;
  const compaction::ObUpdateTabletMetaParam update_tablet_meta_param(
                                            0/*pre_warm_snapshot_version*/,
                                            true/*allow_dup_major*/,
                                            false/*init_major_ckm_info*/);
  bool ha_restore_full = false;
  /* check pararm & need skip for transfer & need skip for exist major */
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLRedoLogReplayer has not been inited", K(ret));
  } else if (OB_ISNULL(log_)) {
    LOG_WARN("log should not be null", K(ret));
  } else if (OB_UNLIKELY(!log_->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K_(log));
  } else if (OB_ISNULL(ls_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be null", K(ret));
  } else if (OB_FAIL(check_need_replay_ddl_log_(ls_, tablet_handle, mock_start_scn, scn_, log_->get_data_format_version(), need_replay))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("fail to check need replay ddl log", K(ret), K_(scn), K_(log), "tablet", PC(tablet_handle.get_obj()));
    }
  } else if (!need_replay) {
    // do nothing
  } else if (OB_FALSE_IT(restore_handler = ls_->get_ls_restore_handler())) {
  } else if (OB_ISNULL(restore_handler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("restore handler should not be null", K(ret));
  } else if (OB_FAIL(restore_handler->get_consistent_scn(consistent_scn))) {
    LOG_WARN("failed to get consistent_scn", K(ret), KPC(ls_));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("need replay but tablet handle is invalid", K(ret), K(tablet_handle), K_(log), K_(scn));
  } else if (FALSE_IT(ha_restore_full = tablet_handle.get_obj()->get_tablet_meta().ha_status_.is_restore_status_full())) {
  } else if (scn_ <= consistent_scn && !ha_restore_full) {
    need_replay = false;
  } else if (OB_FALSE_IT(tablet_id = tablet_handle.get_obj()->get_tablet_id())) {
  } else if (OB_FAIL(ObDDLUtil::is_major_exist(log_->get_ls_id(),
                                               log_->get_table_key().get_tablet_id(),
                                               is_major_exist))) {
    LOG_WARN("failed to check whether need repaly log", K(ret), K(log_));
  } else {
    need_replay = !is_major_exist;
  }

  /* write object to oss*/
  if (OB_FAIL(ret) || !need_replay) {
  } else if (OB_FAIL(ObObjectManager::ss_is_exist_object(log_->get_macro_block_id(), 0 /*mock ls epoch*/, is_object_exist))) {
    LOG_WARN("failed to check is object exist", K(ret), K(log_->get_macro_block_id()));
  } else if (is_object_exist) {
  } else if (OB_FAIL(ObDDLUtil::upload_block_for_ss(log_->get_data_buffer().ptr(),
                                                    log_->get_data_buffer().length(),
                                                    log_->get_macro_block_id()))) {
    LOG_WARN("failed to upload tablet meta", K(ret), K(log_));
  } else if (FALSE_IT(pre_meta_version = tablet_handle.get_obj()->get_tablet_meta().snapshot_version_)) {
  }

  /* update tablet table store*/
  if (OB_FAIL(ret) || !need_replay) {
  } else {
    int64_t pos = 0;
    ObTablet new_tablet;
    storage::ObMetaDiskAddr disk_addr;
    const ObString &data_buf = log_->get_data_buffer();
    const int64_t tablet_meta_size = OB_DEFAULT_MACRO_BLOCK_SIZE;
    if (OB_FAIL(disk_addr.set_block_addr(log_->get_macro_block_id(), sizeof(ObMacroBlockCommonHeader),
                                         tablet_meta_size, ObMetaDiskAddr::DiskType::RAW_BLOCK))) {
      LOG_WARN("failed to sed disk addr", K(ret));
    } else if (OB_FAIL(ObSharedObjectReadHandle::get_data(disk_addr,
                                                        log_->get_data_buffer().ptr(),
                                                        log_->get_data_buffer().length(),
                                                        buf, buf_len))) {
      LOG_WARN("failed to read from serialize info", K(ret));
    } else if (FALSE_IT(new_tablet.set_tablet_addr(disk_addr))) {
    } else if (OB_FAIL(new_tablet.deserialize_for_replay(allocator, buf, buf_len, pos))) {
      LOG_WARN("failed to deserialize tablet meat", K(ret));
    } else if (OB_FAIL(compaction::ObRefreshTabletUtil::update_tablet_meta(*ls_,
                       new_tablet, update_tablet_meta_param))) {
      LOG_WARN("failed to update tablet meta", K(ret));
    }

    /* update tablet meta gc info*/
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObDDLUtil::update_tablet_gc_info(tablet_id, pre_meta_version, new_tablet.get_tablet_meta().snapshot_version_))) {
      LOG_WARN("failed to update tablet gc info", K(ret), K(tablet_id));
    }
  }

  /* release ddl kv */
  if (OB_SUCC(ret)) {
    ObDDLKvMgrHandle ddl_kv_mgr_handle;
    if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
      // process the entry_not_exist if ddl kv mgr is destroyed.
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("create ddl kv mgr failed", K(ret));
      } else {
        ret = OB_SUCCESS; // ignore ddl kv mgr not exist
      }
    } else if (OB_FAIL(ObTabletDDLUtil::schedule_ddl_minor_merge_on_demand(true/*need_freeze*/, log_->get_ls_id(), ddl_kv_mgr_handle))) {
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("failed to schedule ddl minor merge", K(ret), "ls_id", log_->get_ls_id(), K(tablet_id));
      } else {
        ret = OB_SUCCESS;
        LOG_INFO("background will schedule ddl minor merge", K(tablet_id));
      }
    }
  }

  FLOG_INFO("finish replay ddl finish log", K(ret), K(tablet_id), KPC_(log), K_(scn), K(consistent_scn), K(ha_restore_full), "ddl_event_info", ObDDLEventInfo());
  return ret;
}
#endif

// ObDDLIncStartReplayExecutor
ObDDLIncStartReplayExecutor::ObDDLIncStartReplayExecutor()
  : ObDDLReplayExecutor(), log_(nullptr)
{
}

int ObDDLIncStartReplayExecutor::init(
    ObLS *ls,
    const ObDDLIncStartLog &log,
    const share::SCN &scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(ls)
          || OB_UNLIKELY(!log.is_valid())
          || OB_UNLIKELY(!scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(ls), K(log), K(scn));
  } else {
    ls_ = ls;
    log_ = &log;
    scn_ = scn;
    is_inited_ = true;
  }
  return ret;
}

struct SyncTabletFreezeHelper {
  SyncTabletFreezeHelper(ObLS *ls, const ObTabletID &tablet_id, const ObTabletID &lob_meta_tablet_id)
      : ls_(ls), tablet_id_(tablet_id), lob_meta_tablet_id_(lob_meta_tablet_id) {}
  int operator()()
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(ls_) || !tablet_id_.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KP(ls_), K(tablet_id_));
    } else {
      const bool is_sync = true;
      // try freeze for ten seconds
      int64_t abs_timeout_ts = ObClockGenerator::getClock() + 10LL * 1000LL * 1000LL;
      int tmp_ret = ls_->tablet_freeze(tablet_id_, is_sync, abs_timeout_ts);
      int tmp_ret_lob = OB_SUCCESS;
      if (lob_meta_tablet_id_.is_valid()) {
        abs_timeout_ts = ObClockGenerator::getClock() + 10LL * 1000LL * 1000LL;
        tmp_ret_lob = ls_->tablet_freeze(lob_meta_tablet_id_, is_sync, abs_timeout_ts);
      }
      if (OB_SUCCESS != (tmp_ret | tmp_ret_lob)) {
        ret = OB_EAGAIN;
        LOG_WARN("sync freeze failed", K(ret), K(tmp_ret), K(tmp_ret_lob), K(tablet_id_), K(lob_meta_tablet_id_));
      }
    }
    return ret;
  }

  ObLS *ls_;
  const ObTabletID &tablet_id_;
  const ObTabletID &lob_meta_tablet_id_;
};

int ObDDLIncStartReplayExecutor::do_replay_(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  bool need_replay = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLIncStartReplayExecutor has not been inited", K(ret));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tablet_handle));
  } else if (OB_FAIL(check_need_replay_ddl_inc_log_(ls_, tablet_handle, scn_, need_replay))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("fail to check need replay ddl log", K(ret), K(scn_), K(log_));
    }
  } else if (!need_replay) {
    // do nothing
  } else {
    SyncTabletFreezeHelper sync_tablet_freeze(
        ls_, log_->get_log_basic().get_tablet_id(), log_->get_log_basic().get_lob_meta_tablet_id());
    return sync_tablet_freeze();
  }

  return ret;
}

// ObDDLIncCommitReplayExecutor
ObDDLIncCommitReplayExecutor::ObDDLIncCommitReplayExecutor()
  : ObDDLReplayExecutor(), log_(nullptr)
{
}

int ObDDLIncCommitReplayExecutor::init(
    ObLS *ls,
    const ObDDLIncCommitLog &log,
    const share::SCN &scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(ls)
          || OB_UNLIKELY(!log.is_valid())
          || OB_UNLIKELY(!scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(ls), K(log), K(scn));
  } else {
    ls_ = ls;
    log_ = &log;
    scn_ = scn;
    is_inited_ = true;
  }

  return ret;
}

int ObDDLIncCommitReplayExecutor::do_replay_(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  bool need_replay = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLIncCommitReplayExecutor has not been inited", K(ret));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tablet_handle));
  } else if (OB_FAIL(check_need_replay_ddl_inc_log_(ls_, tablet_handle, scn_, need_replay))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("fail to check need replay ddl log", K(ret), K(scn_), K(log_));
    }
  } else if (!need_replay) {
    // do nothing
  } else {
    SyncTabletFreezeHelper sync_tablet_freeze(
        ls_, log_->get_log_basic().get_tablet_id(), log_->get_log_basic().get_lob_meta_tablet_id());
    return sync_tablet_freeze();
  }
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
