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
    last_meta_seq_array_(),
    is_leader_restore_(false)
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
    ObICopyTabletCtx *copy_tablet_ctx,
    const bool is_leader_restore)
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
    is_leader_restore_ = is_leader_restore;
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
    LOG_WARN("failed to check major valid", K(ret), K(major_tables_handle_));
  } else if (OB_FAIL(deal_with_major_sstables_())) {
    LOG_WARN("failed to deal with major sstables", K(ret), K(major_tables_handle_));
  }
  return ret;
}

int ObTabletCopyFinishTask::create_new_table_store_with_minor_()
{
  int ret = OB_SUCCESS;
  share::SCN mds_max_end_scn(SCN::min_scn());
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObTabletPointer *pointer = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
  } else if (OB_FAIL(ls_->ha_get_tablet(tablet_id_, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_id_));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(tablet_id_));
  } else if (OB_ISNULL(pointer = tablet->get_pointer_handle().get_resource_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet pointer should not be NULL", K(ret), KPC(tablet));
  } else if (OB_FAIL(get_mds_sstable_max_end_scn_(mds_max_end_scn))) {
    LOG_WARN("failed to get mds sstable max end scn", K(ret), K(mds_tables_handle_));
  } else {
    TabletMdsLockGuard<LockMode::EXCLUSIVE> guard;
    pointer->get_mds_truncate_lock_guard(guard);
      //Release mds node failed must do dag net retry.
      //Because ls still replay and mds may has residue node which makes data incorrect.
      //So should make ls offline and online
    if (OB_FAIL(ObStorageHATabletBuilderUtil::build_table_with_minor_tables(ls_,
                                                                            tablet_id_,
                                                                            src_tablet_meta_,
                                                                            mds_tables_handle_,
                                                                            minor_tables_handle_,
                                                                            ddl_tables_handle_,
                                                                            restore_action_))) {
      LOG_WARN("failed to build table with minor tables", K(ret), K(mds_tables_handle_),
        K(minor_tables_handle_), K(ddl_tables_handle_), K(restore_action_));
    } else if (mds_max_end_scn.is_min()) {
      //do nothing
    } else if (OB_FAIL(pointer->release_mds_nodes_redo_scn_below(tablet_id_, mds_max_end_scn))) {
      LOG_WARN("failed to relase mds node redo scn blow", K(ret), K(tablet_id_), K(mds_max_end_scn));
      int tmp_ret = OB_SUCCESS;
      const bool need_retry = false;
      if (OB_SUCCESS != (tmp_ret = ha_dag_->get_ha_dag_net_ctx()->set_result(ret, need_retry))) {
        LOG_ERROR("failed to set ha dag net ctx result", K(tmp_ret), K(ret));
      }
    }
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

ERRSIM_POINT_DEF(EN_COPY_COLUMN_STORE_MAJOR_FAILED);
int ObTabletCopyFinishTask::deal_with_major_sstables_()
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
      extra_param.is_leader_restore_ = is_leader_restore_;
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
    const ObTablesHandleArray &major_tables_handle)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  const int64_t major_count = major_tables_handle.get_count();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
  } else if (!ObTabletRestoreAction::is_restore_major(restore_action_)) {
    is_valid = true;
  } else if (0 == major_count || 1 == major_count) {
    is_valid = true;
  } else {
    if (!major_tables_handle.empty()) {
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
              K(major_count), K(major_tables_handle));
  }
  return ret;
}

int ObTabletCopyFinishTask::check_major_valid_()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
  } else if (OB_FAIL(check_restore_major_valid_(major_tables_handle_))) {
    LOG_WARN("failed to check major sstable valid", K(ret), K(major_tables_handle_));
  }
  return ret;
}

int ObTabletCopyFinishTask::get_mds_sstable_max_end_scn_(share::SCN &max_end_scn)
{
  int ret = OB_SUCCESS;
  max_end_scn.set_min();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
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
  }
  return ret;
}


}
}
