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
#include "storage/high_availability/ob_tablet_copy_finish_task.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "observer/report/ob_tablet_table_updater.h"
#include "share/ob_cluster_version.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/high_availability/ob_storage_ha_tablet_builder.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/column_store/ob_column_oriented_sstable.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/compaction/ob_refresh_tablet_util.h"
#endif

namespace oceanbase
{
using namespace share;
using namespace compaction;
namespace storage
{

/******************ObTabletCopyFinishTask*********************/
ObTabletCopyFinishTask::ObTabletCopyFinishTask()
  : ObITask(TASK_TYPE_MIGRATE_FINISH_PHYSICAL),
    is_inited_(false),
    lock_(common::ObLatchIds::BACKUP_LOCK),
    tablet_id_(),
    ls_(nullptr),
    reporter_(nullptr),
    ha_dag_(nullptr),
    arena_allocator_("TabCopyFinish"),
    minor_tables_handle_(),
    ddl_tables_handle_(),
    major_tables_handle_(),
    restore_action_(ObTabletRestoreAction::MAX),
    src_tablet_meta_(nullptr),
    copy_tablet_ctx_(nullptr),
    shared_table_key_array_(),
    last_meta_seq_array_()
{
}

ObTabletCopyFinishTask::~ObTabletCopyFinishTask()
{
}

int ObTabletCopyFinishTask::init(
    const common::ObTabletID &tablet_id,
    ObLS *ls,
    observer::ObIMetaReport *reporter,
    const ObTabletRestoreAction::ACTION &restore_action,
    const ObMigrationTabletParam *src_tablet_meta,
    ObICopyTabletCtx *copy_tablet_ctx)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet copy finish task init twice", K(ret));
  } else if (!tablet_id.is_valid()
             || OB_ISNULL(ls)
             || OB_ISNULL(reporter)
             || OB_ISNULL(src_tablet_meta)
             || !ObTabletRestoreAction::is_valid(restore_action)
             || OB_ISNULL(copy_tablet_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init tablet copy finish task get invalid argument", K(ret), K(tablet_id), KP(ls),
        KP(reporter), KP(src_tablet_meta), K(restore_action));
  } else if (OB_FAIL(copy_tablet_ctx->set_copy_tablet_status(ObCopyTabletStatus::TABLET_EXIST))) {
    LOG_WARN("failed to set copy tablet status", K(ret));
  } else {
    tablet_id_ = tablet_id;
    ls_ = ls;
    reporter_ = reporter;
    ha_dag_ = static_cast<ObStorageHADag *>(this->get_dag());
    restore_action_ = restore_action;
    src_tablet_meta_ = src_tablet_meta;
    copy_tablet_ctx_ = copy_tablet_ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletCopyFinishTask::process()
{
  int ret = OB_SUCCESS;
  bool only_contain_major = false;
  ObCopyTabletStatus::STATUS status = ObCopyTabletStatus::MAX_STATUS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
  } else if (ha_dag_->get_ha_dag_net_ctx()->is_failed()) {
    FLOG_INFO("ha dag net is already failed, skip physical copy finish task", K(tablet_id_), KPC(ha_dag_));
  } else if (OB_FAIL(get_tablet_status(status))) {
    LOG_WARN("failed to get tablet status", K(ret), K(tablet_id_));
  } else if (ObCopyTabletStatus::TABLET_NOT_EXIST == status) {
    FLOG_INFO("copy tablet from src do not exist, skip copy finish task", K(tablet_id_), K(status));
  } else if (OB_FAIL(check_log_replay_to_mds_sstable_end_scn_())) {
    LOG_WARN("failed to check log replay to mds sstable end scn", K(ret), K(tablet_id_));
  } else if (OB_FAIL(create_new_table_store_with_major_())) {
    LOG_WARN("failed to create new table store with major", K(ret), K_(tablet_id));
  } else if (OB_FAIL(create_new_table_store_with_minor_())) {
    LOG_WARN("failed to create new table store with minor", K(ret), K_(tablet_id));
  } else if (OB_FAIL(trim_tablet_())) {
    LOG_WARN("failed to trim tablet", K(ret), K_(tablet_id));
  } else if (OB_FAIL(check_tablet_valid_())) {
    LOG_WARN("failed to check tablet valid", K(ret), KPC(this));
  } else if (OB_FAIL(check_finish_copy_tablet_data_valid_())) {
    LOG_WARN("failed to update tablet data status", K(ret), K(tablet_id_));
  }

  if (OB_FAIL(ret)) {
    if (OB_TABLET_NOT_EXIST == ret) {
      FLOG_INFO("tablet is not exist, skip copy tablet", K(tablet_id_));
      //overwrite ret
      ret = OB_SUCCESS;
    }
  }

  int64_t sstable_count = mds_tables_handle_.get_count()
                          + minor_tables_handle_.get_count()
                          + ddl_tables_handle_.get_count()
                          + major_tables_handle_.get_count();

  SERVER_EVENT_ADD("storage_ha", "tablet_copy_finish_task",
        "tenant_id", MTL_ID(),
        "ls_id", ls_->get_ls_id().id(),
        "tablet_id", tablet_id_.id(),
        "ret", ret,
        "result", ha_dag_->get_ha_dag_net_ctx()->is_failed(),
        "sstable_count", sstable_count);

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, ha_dag_))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), K(tablet_id_));
    }
  }
  return ret;
}

int ObTabletCopyFinishTask::add_sstable(ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;
  ObTablesHandleArray *tables_handle_ptr = nullptr;
  common::SpinWLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
  } else if (!table_handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add sstable get invalid argument", K(ret), K(table_handle));
  } else if (OB_FAIL(get_tables_handle_ptr_(table_handle.get_table()->get_key(), tables_handle_ptr))) {
    LOG_WARN("failed to get tables handle ptr", K(ret), K(table_handle));
  } else if (OB_FAIL(tables_handle_ptr->add_table(table_handle))) {
    LOG_WARN("failed to add table", K(ret), K(table_handle));
  }
  return ret;
}

int ObTabletCopyFinishTask::add_sstable(ObTableHandleV2 &table_handle, const int64_t last_meta_macro_seq)
{
  int ret = OB_SUCCESS;
  ObTablesHandleArray *tables_handle_ptr = nullptr;
  common::SpinWLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
  } else if (!table_handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add sstable get invalid argument", K(ret), K(table_handle));
  } else if (!table_handle.get_table()->get_key().is_major_sstable()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table is not major", K(ret), K(table_handle));
  } else if (OB_FAIL(get_tables_handle_ptr_(table_handle.get_table()->get_key(), tables_handle_ptr))) {
    LOG_WARN("failed to get tables handle ptr", K(ret), K(table_handle));
  } else if (OB_FAIL(tables_handle_ptr->add_table(table_handle))) {
    LOG_WARN("failed to add table", K(ret), K(table_handle));
  } else if (OB_FAIL(last_meta_seq_array_.push_back(std::pair<ObITable::TableKey, int64_t>(table_handle.get_table()->get_key(), last_meta_macro_seq)))) {
    LOG_WARN("failed to add last meta seq", K(ret), K(table_handle), K(last_meta_macro_seq));
  }
  return ret;
}

int ObTabletCopyFinishTask::add_shared_sstable(const ObITable::TableKey &table_key)
{
  int ret = OB_SUCCESS;
  common::SpinWLockGuard guard(lock_);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
  } else if (!table_key.is_major_sstable()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shared table is not major", K(ret), K(table_key));
  } else if (OB_FAIL(shared_table_key_array_.push_back(table_key))) {
    LOG_WARN("failed to push back table key", K(ret));
  } else {
    LOG_INFO("add one shared table key", K_(tablet_id), K(table_key));
  }
  return ret;
}

int ObTabletCopyFinishTask::get_sstable(
    const ObITable::TableKey &table_key,
    ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *meta_mem_mgr = nullptr;
  bool found = false;
  ObTablesHandleArray *tables_handle_ptr = nullptr;
  common::SpinRLockGuard guard(lock_);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
  } else if (!table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get sstable get invalid argument", K(ret), K(table_key));
  } else if (OB_ISNULL(meta_mem_mgr = MTL(ObTenantMetaMemMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get meta mem mgr from MTL", K(ret));
  } else if (OB_FAIL(get_tables_handle_ptr_(table_key, tables_handle_ptr))) {
    LOG_WARN("failed to get tables handle ptr", K(ret), K(table_key));
  } else {
    ObTableHandleV2 tmp_table_handle;
    for (int64_t i = 0; OB_SUCC(ret) && i < tables_handle_ptr->get_count() && !found; ++i) {
      if (OB_FAIL(tables_handle_ptr->get_table(i, tmp_table_handle))) {
        LOG_WARN("failed to get table handle", K(ret), K(i));
      } else if (tmp_table_handle.get_table()->get_key() == table_key) {
        table_handle = tmp_table_handle;
        found = true;
      }
    }

    if (OB_SUCC(ret) && !found) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can not get sstable, unexected", K(ret), K(table_key), K(major_tables_handle_),
          K(minor_tables_handle_), K(ddl_tables_handle_));
    }
  }
  return ret;
}

int ObTabletCopyFinishTask::create_new_table_store_with_major_()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
  } else if (OB_FAIL(check_major_valid_())) {
    LOG_WARN("failed to check major valid", K(ret), K(shared_table_key_array_), K(major_tables_handle_));
  } else if (OB_FAIL(deal_with_shared_majors_())) {
    LOG_WARN("failed to deal with shared majors", K(ret), K(shared_table_key_array_));
  } else if (OB_FAIL(deal_with_no_shared_majors_())) {
    LOG_WARN("failed to deal with no shared majors", K(ret), K(major_tables_handle_));
  }
  return ret;
}

int ObTabletCopyFinishTask::create_new_table_store_with_minor_()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
  } else if (OB_FAIL(ObStorageHATabletBuilderUtil::build_table_with_minor_tables(ls_,
                                                                                 tablet_id_,
                                                                                 src_tablet_meta_,
                                                                                 mds_tables_handle_,
                                                                                 minor_tables_handle_,
                                                                                 ddl_tables_handle_,
                                                                                 restore_action_))) {
    LOG_WARN("failed to build table with minor tables", K(ret), K(mds_tables_handle_),
      K(minor_tables_handle_), K(ddl_tables_handle_), K(restore_action_));
  }
  return ret;
}

int ObTabletCopyFinishTask::check_finish_copy_tablet_data_valid_()
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  const ObTabletDataStatus::STATUS data_status = ObTabletDataStatus::COMPLETE;
  bool is_logical_sstable_exist = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
  } else if (OB_FAIL(ls_->ha_get_tablet(tablet_id_, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_id_));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(tablet_id_), KP(tablet));
  } else if (tablet->get_tablet_meta().has_next_tablet_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet here should only has one", K(ret), KPC(tablet));
  } else if (OB_FAIL(ObStorageHATabletBuilderUtil::check_remote_logical_sstable_exist(tablet, is_logical_sstable_exist))) {
    LOG_WARN("failed to check remote logical sstable exist", K(ret), KPC(tablet));
  } else if (is_logical_sstable_exist) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet still has remote logical sstable, unexpected !!!", K(ret), KPC(tablet));
  } else if (OB_FAIL(tablet->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else {
    const ObSSTableArray &major_sstables = table_store_wrapper.get_member()->get_major_sstables();
    if (OB_SUCC(ret)
        && tablet->get_tablet_meta().table_store_flag_.with_major_sstable()
        && tablet->get_tablet_meta().ha_status_.is_restore_status_full()
        && !tablet->get_tablet_meta().has_transfer_table()
        && major_sstables.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet should has major sstable, unexpected", K(ret), KPC(tablet));
    }

    if (OB_SUCC(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_ISNULL(MTL(observer::ObTabletTableUpdater*))) {
        tmp_ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("tablet table updater should not be null", K(tmp_ret));
      } else if (OB_SUCCESS != (tmp_ret = MTL(observer::ObTabletTableUpdater*)->submit_tablet_update_task(
          ls_->get_ls_id(), tablet_id_))) {
        LOG_WARN("failed to submit tablet update task", K(tmp_ret), KPC(ls_), K(tablet_id_));
      }
    }
  }
  return ret;
}

int ObTabletCopyFinishTask::get_tables_handle_ptr_(
    const ObITable::TableKey &table_key,
    ObTablesHandleArray *&tables_handle_ptr)
{
  int ret = OB_SUCCESS;
  tables_handle_ptr = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
  } else if (!table_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tables handle ptr get invalid argument", K(ret), K(table_key));
  } else if (table_key.is_major_sstable()) {
    tables_handle_ptr = &major_tables_handle_;
  } else if (table_key.is_minor_sstable()) {
    tables_handle_ptr = &minor_tables_handle_;
  } else if (table_key.is_ddl_sstable()) {
    tables_handle_ptr = &ddl_tables_handle_;
  } else if (table_key.is_mds_sstable()) {
    tables_handle_ptr = &mds_tables_handle_;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get tables handle ptr get unexpected table key", K(ret), K(table_key));
  }
  return ret;
}

int ObTabletCopyFinishTask::trim_tablet_()
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  const bool is_rollback = false;
  bool need_merge = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
  } else if (OB_FAIL(ls_->ha_get_tablet(tablet_id_, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_id_));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(tablet_id_));
  } else if (tablet->get_tablet_meta().has_next_tablet_
      && OB_FAIL(ls_->trim_rebuild_tablet(tablet_id_, is_rollback))) {
    LOG_WARN("failed to trim rebuild tablet", K(ret), K(tablet_id_));
  }
  return ret;
}

int ObTabletCopyFinishTask::set_tablet_status(const ObCopyTabletStatus::STATUS &status)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
  } else if (!ObCopyTabletStatus::is_valid(status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set tablet status get invalid argument", K(ret), K(status));
  } else {
    common::SpinWLockGuard guard(lock_);
    if (OB_FAIL(copy_tablet_ctx_->set_copy_tablet_status(status))) {
      LOG_WARN("failed to set copy tablet status", K(ret));
    }
  }
  return ret;
}

int ObTabletCopyFinishTask::check_tablet_valid_()
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet finish restore task do not init", K(ret));
  } else if (OB_FAIL(ls_->get_tablet(tablet_id_, tablet_handle,
      ObTabletCommon::DEFAULT_GET_TABLET_NO_WAIT, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_id_));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), KP(tablet), K(tablet_id_));
  } else if (OB_FAIL(tablet->check_valid(true/*ignore_ha_status*/))) {
    LOG_WARN("failed to check valid", K(ret), KPC(tablet));
  }
  return ret;
}

int ObTabletCopyFinishTask::get_tablet_status(ObCopyTabletStatus::STATUS &status)
{
  int ret = OB_SUCCESS;
  status = ObCopyTabletStatus::MAX_STATUS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
  } else {
    common::SpinRLockGuard guard(lock_);
    if (OB_FAIL(copy_tablet_ctx_->get_copy_tablet_status(status))) {
      LOG_WARN("failed to get copy tablet status", K(ret));
    }
  }
  return ret;
}

int ObTabletCopyFinishTask::check_shared_table_type_()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
  } else {
    for (int64_t i = 0; i < shared_table_key_array_.count() && OB_SUCC(ret); i++) {
      const ObITable::TableKey &table_key = shared_table_key_array_.at(i);
      if (!table_key.is_major_sstable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected table type", K(ret), K(table_key));
      }
    }
  }
  return ret;
}

int ObTabletCopyFinishTask::deal_with_shared_majors_()
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_SHARED_STORAGE
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
  } else {
    for (int64_t i = 0; i < shared_table_key_array_.count() && OB_SUCC(ret); i++) {
      const ObITable::TableKey &table_key = shared_table_key_array_.at(i);
      const ObDownloadTabletMetaParam download_tablet_meta_param(
                                      table_key.get_snapshot_version()/*snapshot_version*/,
                                      true/*allow_dup_major*/,
                                      false/*init_major_ckm_info*/,
                                      false/*need_prewarm*/);
      if (OB_FAIL(ObRefreshTabletUtil::download_major_compaction_tablet_meta(
                                       *ls_, tablet_id_, download_tablet_meta_param))) {
        LOG_WARN("failed to update tablet meta", K(ret), K_(tablet_id), KPC_(ls), K(table_key));
      } else {
        LOG_INFO("succeed to download major compaction tablet meta", K(tablet_id_), K(table_key));
      }
    }
  }
#endif
  return ret;
}

ERRSIM_POINT_DEF(EN_COPY_COLUMN_STORE_MAJOR_FAILED);
int ObTabletCopyFinishTask::deal_with_no_shared_majors_()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
  }  else if (OB_ISNULL(src_tablet_meta_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src tablet meta should not be null", K(ret));
  } else if (major_tables_handle_.empty()) {
    // do nothing
  } else {
    const bool need_replace_remote_sstable = ObTabletRestoreAction::is_restore_replace_remote_sstable(restore_action_);
    ObStorageHATabletBuilderUtil::BatchBuildTabletTablesExtraParam batch_extra_param;
    ObStorageHATabletBuilderUtil::BuildTabletTableExtraParam extra_param;

    batch_extra_param.need_replace_remote_sstable_ = need_replace_remote_sstable;
    ARRAY_FOREACH_X(last_meta_seq_array_, i, cnt, OB_SUCC(ret)) {
      extra_param.reset();
      extra_param.is_leader_restore_ = true;
      extra_param.table_key_ = last_meta_seq_array_.at(i).first;
      extra_param.start_meta_macro_seq_ = last_meta_seq_array_.at(i).second;
      if (OB_FAIL(batch_extra_param.add_extra_param(extra_param))) {
        LOG_WARN("failed to push back extra param", K(ret));
      }
    }

    if (FAILEDx(ObStorageHATabletBuilderUtil::build_tablet_with_major_tables(ls_,
                                                                             tablet_id_,
                                                                             major_tables_handle_,
                                                                             src_tablet_meta_->storage_schema_,
                                                                             batch_extra_param))) {
      LOG_WARN("failed to build tablet with major tables", K(ret), K(tablet_id_), K(major_tables_handle_), KPC(src_tablet_meta_), K(batch_extra_param));
    }

#ifdef ERRSIM
    if (OB_SUCC(ret) && need_replace_remote_sstable && !src_tablet_meta_->storage_schema_.is_row_store()) {
      ret = EN_COPY_COLUMN_STORE_MAJOR_FAILED ? : OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "errsim EN_COPY_COLUMN_STORE_MAJOR_FAILED", K(ret));
      }
    }
#endif
  }

  return ret;
}

int ObTabletCopyFinishTask::check_restore_major_valid_(
    const common::ObIArray<ObITable::TableKey> &shared_table_key_array,
    const ObTablesHandleArray &major_tables_handle)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  const int64_t major_count = shared_table_key_array.count() + major_tables_handle.get_count();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
  } else if (!ObTabletRestoreAction::is_restore_major(restore_action_)) {
    is_valid = true;
  } else if (0 == major_count || 1 == major_count) {
    is_valid = true;
  } else {
    if (!shared_table_key_array.empty() && !shared_table_key_array.at(0).is_column_store_sstable()) {
      is_valid = false;
    } else if (!major_tables_handle.empty()) {
      if (OB_ISNULL(major_tables_handle_.get_table(0))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("major table handle should not be nullptr", K(ret), K(major_tables_handle_));
      } else if (!major_tables_handle_.get_table(0)->is_column_store_sstable()) {
        is_valid = false;
      } else {
        is_valid = true;
      }
    } else {
      is_valid = true;
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (!is_valid) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("restore tablet should only has one major", K(ret),
              K(major_count), K(major_tables_handle_), K(shared_table_key_array_));
  }
  return ret;
}

int ObTabletCopyFinishTask::check_major_valid_()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
  } else if (OB_FAIL(check_shared_table_type_())) {
    LOG_WARN("failed to check table type", K(ret), K(shared_table_key_array_));
  } else if (OB_FAIL(check_restore_major_valid_(shared_table_key_array_, major_tables_handle_))) {
    LOG_WARN("failed to check shared table", K(ret), K(shared_table_key_array_), K(major_tables_handle_));
  }
  return ret;
}

int ObTabletCopyFinishTask::check_log_replay_to_mds_sstable_end_scn_()
{
  int ret = OB_SUCCESS;
  SCN max_end_scn(SCN::min_scn());
  SCN current_replay_scn;
  const int64_t total_timeout = 20_min;
  const int64_t wait_replay_timeout = 10_min;
  bool is_ls_deleted = false;
  SCN last_replay_scn;
  share::SCN readable_scn;
  const int64_t start_ts = ObTimeUtil::current_time();
  int64_t current_ts = 0;
  int64_t last_replay_ts = 0;
  const int64_t CHECK_CONDITION_INTERVAL = 200_ms;
  ObTimeoutCtx timeout_ctx;
  int32_t result = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
  } else if (tablet_id_.is_ls_inner_tablet()) {
    //do nothing
  } else if (OB_FAIL(timeout_ctx.set_timeout(total_timeout))) {
    LOG_WARN("failed to set timeout ctx", K(ret), K(tablet_id_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < mds_tables_handle_.get_count(); ++i) {
      const ObITable *table = mds_tables_handle_.get_table(i);
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mds table should not be NULL", K(ret), KP(table), K(mds_tables_handle_));
      } else {
        max_end_scn = SCN::max(table->get_end_scn(), max_end_scn);
      }
    }

    while (OB_SUCC(ret)) {
      if (timeout_ctx.is_timeouted()) {
        ret = OB_WAIT_REPLAY_TIMEOUT;
        LOG_WARN("wait log replay to mds sstable end scn already timeout", K(ret));
      } else if (ha_dag_->get_ha_dag_net_ctx()->is_failed()) {
        FLOG_INFO("ha dag net is already failed, skip physical copy finish task", K(tablet_id_), KPC(ha_dag_));
        ret = OB_CANCELED;
      } else if (ls_->is_stopped()) {
        ret = OB_NOT_RUNNING;
        LOG_WARN("ls is not running, stop check log replay scn", K(ret), K(tablet_id_));
      } else if (OB_FAIL(ObStorageHAUtils::check_ls_deleted(ls_->get_ls_id(), is_ls_deleted))) {
        LOG_WARN("failed to get ls status from inner table", K(ret));
      } else if (is_ls_deleted) {
        ret = OB_CANCELED;
        LOG_WARN("ls will be removed, no need run migration", K(ret), KPC(ls_), K(is_ls_deleted));
      } else if (OB_FAIL(ObStorageHAUtils::check_log_status(ls_->get_tenant_id(), ls_->get_ls_id(), result))) {
        LOG_WARN("failed to check log status", K(ret), KPC(ls_), K(tablet_id_));
      } else if (OB_SUCCESS != result) {
        LOG_INFO("can not replay log, it will retry", K(result), KPC(ha_dag_));
        if (OB_FAIL(ha_dag_->get_ha_dag_net_ctx()->set_result(result/*result*/, true/*need_retry*/, this->get_dag()->get_type()))) {
          LOG_WARN("failed to set result", K(ret), KPC(ha_dag_));
        } else {
          ret = result;
          LOG_WARN("log sync or replay error, need retry", K(ret), KPC(ha_dag_));
        }
      } else if (OB_FAIL(ls_->get_max_decided_scn(current_replay_scn))) {
        LOG_WARN("failed to get current replay log ts", K(ret), K(tablet_id_));
      } else if (current_replay_scn >= max_end_scn) {
        break;
      } else {
        current_ts = ObTimeUtility::current_time();
        if (REACH_TENANT_TIME_INTERVAL(60 * 1000 * 1000)) {
          LOG_INFO("replay log is not ready, retry next loop", K(tablet_id_),
              "current_replay_scn", current_replay_scn,
              "mds max end scn", max_end_scn);
        }

        if (current_replay_scn == last_replay_scn) {
          if (current_ts - last_replay_ts > wait_replay_timeout) {
            ret = OB_WAIT_REPLAY_TIMEOUT;
            LOG_WARN("failed to check log replay to mds end scn", K(ret), K(tablet_id_), K(current_replay_scn), K(max_end_scn));
          }
        } else if (last_replay_scn > current_replay_scn) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("last end log ts should not smaller than current end log ts", K(ret),
              K(last_replay_scn), K(current_replay_scn));
        } else {
          last_replay_scn = current_replay_scn;
          last_replay_ts = current_ts;
        }

        if (OB_SUCC(ret)) {
          ob_usleep(CHECK_CONDITION_INTERVAL);
          if (OB_FAIL(share::dag_yield())) {
            LOG_WARN("fail to yield dag", KR(ret));
          }
        }
      }
    }
  }

  LOG_INFO("finish check_log_replay_to_mds_sstable_end_scn_",
      K(ret), K(tablet_id_), "cost", ObTimeUtil::current_time() - start_ts);
  return ret;
}


}
}
