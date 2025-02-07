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
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/high_availability/ob_storage_ha_tablet_builder.h"
#include "storage/high_availability/ob_storage_ha_utils.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/compaction/ob_refresh_tablet_util.h"
#endif

namespace oceanbase
{
using namespace share;
using namespace compaction;
namespace storage
{

/******************ObTabletCopyFinishTaskParam*********************/
ObTabletCopyFinishTaskParam::ObTabletCopyFinishTaskParam()
  : ls_(nullptr),
    tablet_id_(),
    reporter_(nullptr),
    restore_action_(ObTabletRestoreAction::MAX),
    is_leader_restore_(false),
    src_tablet_meta_(nullptr),
    copy_tablet_ctx_(nullptr),
    is_only_replace_major_(false)

{
}

bool ObTabletCopyFinishTaskParam::is_valid() const
{
  return OB_NOT_NULL(ls_) && tablet_id_.is_valid() && OB_NOT_NULL(reporter_) && OB_NOT_NULL(copy_tablet_ctx_)
      && ObTabletRestoreAction::is_valid(restore_action_);
}

void ObTabletCopyFinishTaskParam::reset()
{
  ls_ = nullptr;
  tablet_id_.reset();
  reporter_ = nullptr;
  restore_action_ = ObTabletRestoreAction::MAX;
  is_leader_restore_ = false;
  src_tablet_meta_ = nullptr;
  copy_tablet_ctx_ = nullptr;
  is_only_replace_major_ = false;
}

/******************ObTabletCopyFinishTask*********************/
ObTabletCopyFinishTask::ObTabletCopyFinishTask()
  : ObITask(TASK_TYPE_MIGRATE_FINISH_PHYSICAL),
    is_inited_(false),
    lock_(common::ObLatchIds::BACKUP_LOCK),
    ha_dag_(nullptr),
    arena_allocator_("TabCopyFinish"),
    minor_tables_handle_(),
    ddl_tables_handle_(),
    major_tables_handle_(),
    mds_tables_handle_(),
    last_meta_seq_array_(),
    param_()
{
}

ObTabletCopyFinishTask::~ObTabletCopyFinishTask()
{
}

int ObTabletCopyFinishTask::init(
    const ObTabletCopyFinishTaskParam &param)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tablet copy finish task init twice", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init tablet copy finish task get invalid argument", K(ret), K(param));
  } else if (OB_FAIL(param.copy_tablet_ctx_->set_copy_tablet_status(ObCopyTabletStatus::TABLET_EXIST))) {
    LOG_WARN("failed to set copy tablet status", K(ret));
  } else {
    param_ = param;
    ha_dag_ = static_cast<ObStorageHADag *>(this->get_dag());
    is_inited_ = true;
  }
  return ret;
}

int ObTabletCopyFinishTask::process()
{
  int ret = OB_SUCCESS;
  bool only_contain_major = false;
  ObCopyTabletStatus::STATUS status = ObCopyTabletStatus::MAX_STATUS;
  ObCopyTabletRecordExtraInfo *extra_info = nullptr;

#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BLOCK_SPLIT_BEFORE_SSTABLES_SPLIT) OB_SUCCESS;
  if (OB_SUCC(ret)) {
    // do nothing.
  } else if (OB_DDL_TASK_EXECUTE_TOO_MUCH_TIME == ret ) { // ret=-4192, errsim trigger to test ddl-split orthogonal ls-migration.
    ret = OB_SUCCESS;
    if (tablet_id_.is_inner_tablet() || tablet_id_.is_ls_inner_tablet()) {
    } else if (GCONF.errsim_test_tablet_id.get_value() > 0 && tablet_id_.id() == GCONF.errsim_test_tablet_id.get_value()){
      LOG_INFO("[ERRSIM] stuck before create table store", K(tablet_id_), KPC(this));
      DEBUG_SYNC(BEFORE_MIGRATION_CREATE_TABLE_STORE);
    } else {
      LOG_INFO("start to process copy finish task", K(tablet_id_), KPC(this));
    }
  } else {
    ret = OB_SUCCESS; // other errsim errors of ddl split, ignored here.
  }
#endif

  if (OB_FAIL(ret)) {
    LOG_WARN("error found", K(ret));
  } else if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
  } else if (ha_dag_->get_ha_dag_net_ctx()->is_failed()) {
    FLOG_INFO("ha dag net is already failed, skip physical copy finish task", "tablet_id", param_.tablet_id_, KPC(ha_dag_));
  } else if (OB_FAIL(get_tablet_status(status))) {
    LOG_WARN("failed to get tablet status", K(ret), K(param_));
  } else if (ObCopyTabletStatus::TABLET_NOT_EXIST == status) {
    FLOG_INFO("copy tablet from src do not exist, skip copy finish task", "tablet_id", param_.tablet_id_, K(status));
  } else if (OB_FAIL(create_new_table_store_with_major_())) {
    LOG_WARN("failed to create new table store with major", K(ret), K_(param));
  } else if (OB_FAIL(create_new_table_store_with_minor_())) {
    LOG_WARN("failed to create new table store with minor", K(ret), K_(param));
  } else if (OB_FAIL(trim_tablet_())) {
    LOG_WARN("failed to trim tablet", K(ret), K_(param));
  } else if (OB_FAIL(check_tablet_valid_())) {
    LOG_WARN("failed to check tablet valid", K(ret), KPC(this));
  } else if (OB_FAIL(check_finish_copy_tablet_data_valid_())) {
    LOG_WARN("failed to update tablet data status", K(ret), K_(param));
  }

  if (OB_FAIL(ret)) {
    if (OB_TABLET_NOT_EXIST == ret) {
      FLOG_INFO("tablet is not exist, skip copy tablet", "tablet_id", param_.tablet_id_);
      //overwrite ret
      ret = OB_SUCCESS;
    }
  }

  int64_t sstable_count = mds_tables_handle_.get_count()
                          + minor_tables_handle_.get_count()
                          + ddl_tables_handle_.get_count()
                          + major_tables_handle_.get_count();

  int tmp_ret = OB_SUCCESS;
  char extra_info_str[MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH] = {0};
  int64_t pos = 0;
  if (OB_SUCCESS != (tmp_ret = param_.copy_tablet_ctx_->get_copy_tablet_record_extra_info(extra_info))) {
    LOG_WARN("failed to get copy tablet record extra info", K(tmp_ret), KP(extra_info));
  } else if (OB_ISNULL(extra_info)) {
    LOG_WARN("copy tablet record extra info is NULL", K(extra_info));
  } else if (FALSE_IT(extra_info->set_restore_action(param_.restore_action_))) {
  } else if (OB_SUCCESS != (tmp_ret = common::databuff_print_multi_objs(extra_info_str,
      MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH, pos, *extra_info))) {
    LOG_WARN("failed to print extra info", K(tmp_ret), K(extra_info));
  }

  SERVER_EVENT_ADD("storage_ha", "tablet_copy_finish_task",
        "tenant_id", MTL_ID(),
        "ls_id", param_.ls_->get_ls_id().id(),
        "tablet_id", param_.tablet_id_.id(),
        "ret", ret,
        "result", ha_dag_->get_ha_dag_net_ctx()->is_failed(),
        "sstable_count", sstable_count,
        extra_info_str);

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = ObStorageHADagUtils::deal_with_fo(ret, ha_dag_))) {
      LOG_WARN("failed to deal with fo", K(ret), K(tmp_ret), K(param_));
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
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
  } else if (param_.is_only_replace_major_) {
    FLOG_INFO("only replace major, no need build minor tables", K(ret), "tablet_id", param_.tablet_id_);
  } else if (OB_FAIL(param_.ls_->ha_get_tablet(param_.tablet_id_, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), "tablet_id", param_.tablet_id_);
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), "tablet_id", param_.tablet_id_);
  } else if (OB_FAIL(get_mds_sstable_max_end_scn_(mds_max_end_scn))) {
    LOG_WARN("failed to get mds sstable max end scn", K(ret), K(mds_tables_handle_));
  } else {
    ObStorageHATabletBuilderUtil::BatchBuildMinorSSTablesParam param;
    param.ls_ = param_.ls_;
    param.tablet_id_ = param_.tablet_id_;
    param.src_tablet_meta_ = param_.src_tablet_meta_;
    param.restore_action_ = param_.restore_action_;
    param.release_mds_scn_ = mds_max_end_scn;

    if (OB_FAIL(param.assign_sstables(mds_tables_handle_, minor_tables_handle_, ddl_tables_handle_))) {
      LOG_WARN("failed to assign sstables", K(ret), KPC(param_.ls_), "tablet_id", param_.tablet_id_);
    } else if (OB_FAIL(ObStorageHATabletBuilderUtil::build_table_with_minor_tables(param))) {
      LOG_WARN("failed to build table with minor tables", K(ret), K(mds_tables_handle_),
        K(minor_tables_handle_), K(ddl_tables_handle_), K(param_.restore_action_));
      if (OB_RELEASE_MDS_NODE_ERROR == ret) {
        //Release mds node failed must do dag net retry.
        //Because ls still replay and mds may has residue node which makes data incorrect.
        //So should make ls offline and online
        int tmp_ret = OB_SUCCESS;
        const bool need_retry = false;
        if (OB_SUCCESS != (tmp_ret = ha_dag_->get_ha_dag_net_ctx()->set_result(ret, need_retry))) {
          LOG_ERROR("failed to set ha dag net ctx result", K(tmp_ret), K(ret));
        }
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
  } else if (OB_FAIL(param_.ls_->ha_get_tablet(param_.tablet_id_, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), "tablet_id", param_.tablet_id_);
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), "tablet_id", param_.tablet_id_, KP(tablet));
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
          param_.ls_->get_ls_id(), param_.tablet_id_))) {
        LOG_WARN("failed to submit tablet update task", K(tmp_ret), KPC(param_.ls_), "tablet_id", param_.tablet_id_);
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
  } else if (OB_FAIL(param_.ls_->ha_get_tablet(param_.tablet_id_, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(param_));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(param_));
  } else if (tablet->get_tablet_meta().has_next_tablet_
      && OB_FAIL(param_.ls_->trim_rebuild_tablet(param_.tablet_id_, is_rollback))) {
    LOG_WARN("failed to trim rebuild tablet", K(ret), K(param_));
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
    if (OB_FAIL(param_.copy_tablet_ctx_->set_copy_tablet_status(status))) {
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
  } else if (OB_FAIL(param_.ls_->get_tablet(param_.tablet_id_, tablet_handle,
      ObTabletCommon::DEFAULT_GET_TABLET_NO_WAIT, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("failed to get tablet", K(ret), "tablet_id", param_.tablet_id_);
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), KP(tablet), K(param_));
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
    if (OB_FAIL(param_.copy_tablet_ctx_->get_copy_tablet_status(status))) {
      LOG_WARN("failed to get copy tablet status", K(ret));
    }
  }
  return ret;
}

ERRSIM_POINT_DEF(EN_COPY_COLUMN_STORE_MAJOR_FAILED);
int ObTabletCopyFinishTask::deal_with_major_sstables_()
{
  int ret = OB_SUCCESS;
  ObTablesHandleArray shared_major_sstables;
  ObTablesHandleArray local_major_sstables;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
  } else if (OB_ISNULL(param_.src_tablet_meta_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src tablet meta should not be null", K(ret));
  } else if (major_tables_handle_.empty()) {
    // do nothing
  } else if (OB_FAIL(classify_major_sstables_(shared_major_sstables, local_major_sstables))) {
    LOG_WARN("failed to split major sstables", K(ret));
  } else if (OB_FAIL(deal_with_shared_majors_(shared_major_sstables))) {
    // TODO(jyx441808): remove this logic when migration can fetch src macro id list for shared sstable
    LOG_WARN("failed to deal with shared majors", K(ret));
  } else if (local_major_sstables.empty()) {
    // do nothing
  } else {
    const bool need_replace_remote_sstable = ObTabletRestoreAction::is_restore_replace_remote_sstable(param_.restore_action_);
    ObStorageHATabletBuilderUtil::BatchBuildTabletTablesExtraParam batch_extra_param;
    ObStorageHATabletBuilderUtil::BuildTabletTableExtraParam extra_param;

    batch_extra_param.need_replace_remote_sstable_ = need_replace_remote_sstable;
    ARRAY_FOREACH_X(last_meta_seq_array_, i, cnt, OB_SUCC(ret)) {
      extra_param.reset();
      extra_param.is_leader_restore_ = param_.is_leader_restore_;
      extra_param.table_key_ = last_meta_seq_array_.at(i).first;
      extra_param.start_meta_macro_seq_ = last_meta_seq_array_.at(i).second;
      if (OB_FAIL(batch_extra_param.add_extra_param(extra_param))) {
        LOG_WARN("failed to push back extra param", K(ret));
      }
    }

    if (FAILEDx(ObStorageHATabletBuilderUtil::build_tablet_with_major_tables(param_.ls_,
                                                                             param_.tablet_id_,
                                                                             major_tables_handle_,
                                                                             param_.src_tablet_meta_->storage_schema_,
                                                                             batch_extra_param))) {
      LOG_WARN("failed to build tablet with major tables", K(ret), K(param_), K(batch_extra_param));
    }

#ifdef ERRSIM
    if (OB_SUCC(ret) && need_replace_remote_sstable && !param_.src_tablet_meta_->storage_schema_.is_row_store()) {
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
  } else if (!ObTabletRestoreAction::is_restore_major(param_.restore_action_)) {
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

int ObTabletCopyFinishTask::classify_major_sstables_(
    ObTablesHandleArray &shared_major_sstables,
    ObTablesHandleArray &local_major_sstables)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < major_tables_handle_.get_count(); ++i) {
      ObTableHandleV2 table_handle;
      ObSSTable *sstable = nullptr;
      ObSSTableMetaHandle meta_handle;
      if (OB_FAIL(major_tables_handle_.get_table(i, table_handle))) {
        LOG_WARN("failed to get table handle", K(ret), K(i));
      } else if (OB_FAIL(table_handle.get_sstable(sstable))) {
        LOG_WARN("failed to get sstable", K(ret), K(table_handle));
      } else if (OB_FAIL(sstable->get_meta(meta_handle))) {
        LOG_WARN("failed to get sstable meta", K(ret), KPC(sstable));
      } else if (!meta_handle.get_sstable_meta().get_basic_meta().table_shared_flag_.is_shared_sstable()
          || param_.is_leader_restore_) {
        if (OB_FAIL(local_major_sstables.add_table(table_handle))) {
          LOG_WARN("failed to add table", K(ret), KPC(sstable), K(table_handle));
        }
      } else {
        if (OB_FAIL(shared_major_sstables.add_table(table_handle))) {
          LOG_WARN("failed to add table", K(ret), KPC(sstable), K(table_handle));
        }
      }
    }
  }
  return ret;
}

int ObTabletCopyFinishTask::deal_with_shared_majors_(ObTablesHandleArray &major_tables_handle)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_SHARED_STORAGE
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet copy finish task do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < major_tables_handle.get_count(); ++i) {
      ObTableHandleV2 table_handle;
      ObSSTable *sstable = nullptr;
      if (OB_FAIL(major_tables_handle.get_table(i, table_handle))) {
        LOG_WARN("failed to get table handle", K(ret), K(i));
      } else if (OB_FAIL(table_handle.get_sstable(sstable))) {
        LOG_WARN("failed to get sstable", K(ret), K(table_handle));
      } else {
        const ObITable::TableKey &table_key = sstable->get_key();
        const ObDownloadTabletMetaParam download_tablet_meta_param(table_key.get_snapshot_version()/*snapshot_version*/,
                                                                   true/*allow_dup_major*/,
                                                                   false/*init_major_ckm_info*/,
                                                                   false/*need_prewarm*/);
        if (OB_FAIL(ObRefreshTabletUtil::download_major_compaction_tablet_meta(*param_.ls_, param_.tablet_id_, download_tablet_meta_param))) {
          LOG_WARN("failed to download major compaction tablet meta", K(ret), K(download_tablet_meta_param));
        } else {
          LOG_INFO("succeed to download major compaction tablet meta", "tablet_id", param_.tablet_id_, KPC(sstable));
        }
      }
    }
  }
#endif
  return ret;
}

}
}
