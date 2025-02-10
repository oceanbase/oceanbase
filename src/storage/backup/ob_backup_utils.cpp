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

#include "ob_backup_utils.h"
#include "share/backup/ob_archive_struct.h"
#include "storage/backup/ob_backup_factory.h"
#include "storage/backup/ob_backup_operator.h"
#include "share/ob_io_device_helper.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/high_availability/ob_storage_ha_utils.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "share/backup/ob_backup_data_table_operator.h"
#include "storage/ddl/ob_ddl_merge_task.h"
#include "share/backup/ob_backup_tablet_reorganize_helper.h"
#include "share/ob_tablet_reorganize_history_table_operator.h"
#include "lib/wait_event/ob_wait_event.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::palf;

namespace oceanbase {
namespace backup {

/* ObBackupUtils */
int ObBackupUtils::calc_start_replay_scn(
    const share::ObBackupSetTaskAttr &set_task_attr,
    const storage::ObBackupLSMetaInfosDesc &ls_meta_infos,
    const share::ObTenantArchiveRoundAttr &round_attr,
    share::SCN &start_replay_scn)
{
  int ret = OB_SUCCESS;
  SCN tmp_start_replay_scn = set_task_attr.start_scn_;
  // To ensure that restore can be successfully initiated,
  // we need to avoid clearing too many logs and the start_replay_scn less than the start_scn of the first piece.
  // so we choose the minimum palf_base_info.prev_log_info_.scn firstly, to ensure keep enough logs.
  // Then we choose the max(minimum palf_base_info.prev_log_info_.scn, round_attr.start_scn) as the start_replay_scn,
  // to ensure the start_replay_scn is greater than the start scn of first piece
  ARRAY_FOREACH_X(ls_meta_infos.ls_meta_packages_, i, cnt, OB_SUCC(ret)) {
    const palf::PalfBaseInfo &palf_base_info = ls_meta_infos.ls_meta_packages_.at(i).palf_meta_;
    tmp_start_replay_scn = SCN::min(tmp_start_replay_scn, palf_base_info.prev_log_info_.scn_);
  }
  if (OB_SUCC(ret)) {
    start_replay_scn = SCN::max(tmp_start_replay_scn, round_attr.start_scn_);
    LOG_INFO("calculate start replay scn finish", K(start_replay_scn), K(ls_meta_infos), K(round_attr));
  }
  return ret;
}

int ObBackupUtils::get_sstables_by_data_type(const storage::ObTabletHandle &tablet_handle, const share::ObBackupDataType &backup_data_type,
    const storage::ObTabletTableStore &tablet_table_store, const bool is_major_compaction_mview_dep_tablet, const share::SCN &mview_dep_scn,
    common::ObIArray<storage::ObSSTableWrapper> &sstable_array)
{
  int ret = OB_SUCCESS;
  sstable_array.reset();
  if (!backup_data_type.is_valid() || !tablet_table_store.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(backup_data_type), K(tablet_table_store));
  } else if (backup_data_type.is_sys_backup()) {
    if (OB_FAIL(fetch_sys_tablet_sstables_(tablet_handle, tablet_table_store, sstable_array))) {
      LOG_WARN("failed to fetch sys tablet sstable", K(ret), K(tablet_handle), K(tablet_table_store));
    }
  } else {
    if (OB_FAIL(fetch_minor_and_ddl_sstables_(tablet_handle, tablet_table_store, sstable_array))) {
      LOG_WARN("failed to fetch minor and ddl sstables", K(ret), K(tablet_handle), K(tablet_table_store));
    } else if (OB_FAIL(fetch_major_sstables_(tablet_handle, tablet_table_store, is_major_compaction_mview_dep_tablet, mview_dep_scn, sstable_array))) {
      LOG_WARN("failed to fetch major sstables", K(ret), K(tablet_handle), K(tablet_table_store));
    }
  }
  return ret;
}

int ObBackupUtils::fetch_sys_tablet_sstables_(const storage::ObTabletHandle &tablet_handle,
    const storage::ObTabletTableStore &tablet_table_store, common::ObIArray<ObSSTableWrapper> &sstable_array)
{
  int ret = OB_SUCCESS;
  const storage::ObSSTableArray *minor_sstable_array_ptr = NULL;
  const storage::ObSSTableArray *major_sstable_array_ptr = NULL;
  minor_sstable_array_ptr = &tablet_table_store.get_minor_sstables();
  major_sstable_array_ptr = &tablet_table_store.get_major_sstables();
  ObArray<storage::ObSSTableWrapper> minor_sstable_array;
  ObArray<storage::ObSSTableWrapper> major_sstable_array;
  if (OB_FAIL(minor_sstable_array_ptr->get_all_table_wrappers(minor_sstable_array))) {
    LOG_WARN("failed to get all tables", K(ret), KPC(minor_sstable_array_ptr));
  } else if (OB_FAIL(check_tablet_minor_sstable_validity_(tablet_handle, minor_sstable_array))) {
    LOG_WARN("failed to check tablet minor sstable validity", K(ret), K(tablet_handle), K(minor_sstable_array));
  } else if (OB_FAIL(major_sstable_array_ptr->get_all_table_wrappers(major_sstable_array, true/*unpack_table*/))) {
      LOG_WARN("failed to get all tables", K(ret), KPC(minor_sstable_array_ptr));
  } else if (OB_FAIL(append(sstable_array, minor_sstable_array))) {
    LOG_WARN("failed to append", K(ret), K(minor_sstable_array));
  } else if (OB_FAIL(append(sstable_array, major_sstable_array))) {
    LOG_WARN("failed to append", K(ret), K(major_sstable_array));
  }
  return ret;
}

int ObBackupUtils::fetch_minor_and_ddl_sstables_(const storage::ObTabletHandle &tablet_handle,
    const storage::ObTabletTableStore &tablet_table_store, common::ObIArray<ObSSTableWrapper> &sstable_array)
{
  int ret = OB_SUCCESS;
  const storage::ObSSTableArray *minor_sstable_array_ptr = NULL;
  const storage::ObSSTableArray *ddl_sstable_array_ptr = NULL;
  const storage::ObSSTableArray *mds_sstable_array_ptr = NULL;
  minor_sstable_array_ptr = &tablet_table_store.get_minor_sstables();
  ddl_sstable_array_ptr = &tablet_table_store.get_ddl_sstables();
  mds_sstable_array_ptr = &tablet_table_store.get_mds_sstables();
  ObArray<storage::ObSSTableWrapper> minor_sstable_array;
  ObArray<storage::ObSSTableWrapper> ddl_sstable_array;
  ObArray<storage::ObSSTableWrapper> mds_sstable_array;
  if (OB_FAIL(minor_sstable_array_ptr->get_all_table_wrappers(minor_sstable_array))) {
    LOG_WARN("failed to get all tables", K(ret), KPC(minor_sstable_array_ptr));
  } else if (OB_FAIL(ddl_sstable_array_ptr->get_all_table_wrappers(ddl_sstable_array, true/*unpack_table*/))) {
    LOG_WARN("failed to get all tables", K(ret), KPC(ddl_sstable_array_ptr));
  } else if (OB_FAIL(mds_sstable_array_ptr->get_all_table_wrappers(mds_sstable_array, true/*unpack_table*/))) {
    LOG_WARN("failed to get all tables", K(ret), KPC(mds_sstable_array_ptr));
  } else if (OB_FAIL(check_tablet_minor_sstable_validity_(tablet_handle, minor_sstable_array))) {
    LOG_WARN("failed to check tablet minor sstable validity", K(ret), K(tablet_handle), K(minor_sstable_array));
  } else if (OB_FAIL(check_tablet_ddl_sstable_validity_(tablet_handle, ddl_sstable_array))) {
    LOG_WARN("failed to check tablet ddl sstable validity", K(ret), K(tablet_handle), K(ddl_sstable_array));
  } else if (OB_FAIL(append(sstable_array, minor_sstable_array))) {
    LOG_WARN("failed to append", K(ret), K(minor_sstable_array));
  } else if (OB_FAIL(append(sstable_array, ddl_sstable_array))) {
    LOG_WARN("failed to append", K(ret), K(ddl_sstable_array));
  } else if (OB_FAIL(append(sstable_array, mds_sstable_array))) {
    LOG_WARN("failed to append", K(ret), K(mds_sstable_array));
  }
  return ret;
}

int ObBackupUtils::fetch_major_sstables_(const storage::ObTabletHandle &tablet_handle,
    const storage::ObTabletTableStore &tablet_table_store, const bool is_major_compaction_mview_dep_tablet,
    const share::SCN &mview_dep_scn, common::ObIArray<ObSSTableWrapper> &sstable_array)
{
  int ret = OB_SUCCESS;
  const storage::ObSSTableArray *major_sstable_array_ptr = NULL;
  major_sstable_array_ptr = &tablet_table_store.get_major_sstables();
  ObITable *last_major_sstable_ptr = NULL;
  bool with_major_sstable = true;
  const compaction::ObMediumCompactionInfoList *medium_info_list = nullptr;
  ObSSTableWrapper major_wrapper;
  ObArenaAllocator tmp_allocator("GetMediumList", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());

  if (OB_FAIL(check_tablet_with_major_sstable(tablet_handle, with_major_sstable))) {
    LOG_WARN("failed to check tablet with major sstable", K(ret));
  } else if (OB_ISNULL(last_major_sstable_ptr = major_sstable_array_ptr->get_boundary_table(true /*last*/))) {
    if (with_major_sstable) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("last major sstable should not be null", K(ret), K(tablet_handle));
    }
  } else if (OB_FAIL(tablet_handle.get_obj()->read_medium_info_list(tmp_allocator, medium_info_list))) {
    LOG_WARN("failed to load medium info list", K(ret));
  } else if (medium_info_list->get_last_compaction_scn() > 0
      && medium_info_list->get_last_compaction_scn() != last_major_sstable_ptr->get_snapshot_version()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("medium list is invalid for last major sstable", K(ret), KPC(medium_info_list),
        KPC(last_major_sstable_ptr), K(tablet_handle));
  } else if (is_major_compaction_mview_dep_tablet) {
    if (OB_FAIL(check_and_filter_major_sstables_for_mview_(mview_dep_scn, major_sstable_array_ptr, sstable_array))) {
      LOG_WARN("failed to check and filter major sstable for mview", K(ret));
    }
  } else { // not mview dep tablet
    if (last_major_sstable_ptr->is_co_sstable()) {
      if (OB_FAIL(static_cast<ObCOSSTableV2 *>(last_major_sstable_ptr)->get_all_tables(sstable_array))) {
        LOG_WARN("failed to get all cg tables from co table", K(ret), KPC(last_major_sstable_ptr));
      }
    } else {
      if (OB_FAIL(major_wrapper.set_sstable(static_cast<ObSSTable *>(last_major_sstable_ptr)))) {
        LOG_WARN("failed to set major wrapper", K(ret), KPC(last_major_sstable_ptr));
      } else if (OB_FAIL(sstable_array.push_back(major_wrapper))) {
        LOG_WARN("failed to push back", K(ret), K(major_wrapper));
      }
    }
  }
  return ret;
}

int ObBackupUtils::check_and_filter_major_sstables_for_mview_(
    const share::SCN &mview_dep_scn,
    const storage::ObSSTableArray *major_sstable_array_ptr,
    common::ObIArray<storage::ObSSTableWrapper> &sstable_array)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_major_sstables_for_mview_(mview_dep_scn, major_sstable_array_ptr))) {
    LOG_WARN("failed to check major sstable for mview", K(ret));
  } else if (OB_FAIL(filter_major_sstables_for_mview_(mview_dep_scn, major_sstable_array_ptr, sstable_array))) {
    LOG_WARN("failed to filter major sstables for mview", K(ret));
  } else {
    LOG_INFO("check and filter major sstable for mview", K(sstable_array));
  }
  return ret;
}

int ObBackupUtils::check_major_sstables_for_mview_(const share::SCN &mview_dep_scn,
    const storage::ObSSTableArray *major_sstable_array_ptr)
{
  int ret = OB_SUCCESS;
  common::ObArray<ObSSTableWrapper> tmp_sstable_array;
  if (OB_ISNULL(major_sstable_array_ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("major sstable array ptr should not be null", K(ret));
  } else if (OB_FAIL(major_sstable_array_ptr->get_all_table_wrappers(tmp_sstable_array))) {
    LOG_WARN("failed to get all table wrappers", K(ret), KPC(major_sstable_array_ptr));
  } else if (!mview_dep_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("mview_dep_scn is invalid", KR(ret), K(mview_dep_scn));
  } else if (mview_dep_scn.is_min()) {
    // do nothing
    // when major refresh mview first create, it's base table major sstable not match last_refresh_scn
  } else {
    bool exist = false;
    ARRAY_FOREACH_X(tmp_sstable_array, idx, cnt, OB_SUCC(ret)) {
      storage::ObSSTableWrapper &sstable_wrapper = tmp_sstable_array.at(idx);
      ObSSTable *sstable_ptr = NULL;
      if (OB_ISNULL(sstable_ptr = sstable_wrapper.get_sstable())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be null", K(ret));
      } else {
        const ObITable::TableKey &table_key = sstable_ptr->get_key();
        if (table_key.get_end_scn() == mview_dep_scn) {
          exist = true;
          break;
        }
      }
    }
    if (OB_SUCC(ret) && !exist) {
      ret = OB_BACKUP_MISSING_MVIEW_DEP_TABLET_SSTABLE;
      LOG_WARN("missing sstable for mview tablet", K(ret), K(mview_dep_scn), K(tmp_sstable_array));
    }
  }
  return ret;
}

int ObBackupUtils::filter_major_sstables_for_mview_(
    const share::SCN &mview_dep_scn,
    const storage::ObSSTableArray *major_sstable_array_ptr,
    common::ObIArray<storage::ObSSTableWrapper> &sstable_array)
{
  int ret = OB_SUCCESS;
  common::ObArray<ObSSTableWrapper> tmp_sstable_array;
  if (OB_ISNULL(major_sstable_array_ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("major sstable array ptr should not be null", K(ret));
  } else if (OB_FAIL(major_sstable_array_ptr->get_all_table_wrappers(tmp_sstable_array))) {
    LOG_WARN("failed to get all table wrappers", K(ret));
  } else {
    ARRAY_FOREACH_X(tmp_sstable_array, idx, cnt, OB_SUCC(ret)) {
      storage::ObSSTableWrapper &sstable_wrapper = tmp_sstable_array.at(idx);
      ObSSTable *sstable_ptr = NULL;
      if (OB_ISNULL(sstable_ptr = sstable_wrapper.get_sstable())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be null", K(ret));
      } else {
        const ObITable::TableKey &table_key = sstable_ptr->get_key();
        if (table_key.get_end_scn() < mview_dep_scn) {
          // skip
        } else if (OB_FAIL(sstable_array.push_back(sstable_wrapper))) {
          LOG_WARN("failed to push back", K(ret), K(sstable_wrapper));
        }
      }
    }
    if (OB_SUCC(ret) && sstable_array.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sstable array should not be empty", K(ret), K(mview_dep_scn), KPC(major_sstable_array_ptr));
    }
  }
  return ret;
}

int ObBackupUtils::check_tablet_with_major_sstable(const storage::ObTabletHandle &tablet_handle, bool &with_major)
{
  int ret = OB_SUCCESS;
  if (!tablet_handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tablet_handle));
  } else {
    with_major = tablet_handle.get_obj()->get_tablet_meta().table_store_flag_.with_major_sstable();
    if (!with_major) {
      LOG_INFO("tablet not with major", K(tablet_handle));
    }
  }
  return ret;
}

int ObBackupUtils::fetch_macro_block_logic_id_list(const storage::ObTabletHandle &tablet_handle,
    const blocksstable::ObSSTable &sstable, common::ObIArray<blocksstable::ObLogicMacroBlockId> &logic_id_list)
{
  int ret = OB_SUCCESS;
  logic_id_list.reset();
  ObArenaAllocator allocator;
  ObDatumRange datum_range;
  const storage::ObITableReadInfo *index_read_info = NULL;

  SMART_VAR(ObSSTableSecMetaIterator, meta_iter)
  {
    if (!sstable.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("get invalid args", K(ret), K(sstable));
    } else if (FALSE_IT(datum_range.set_whole_range())) {
    } else if (OB_FAIL(tablet_handle.get_obj()->get_sstable_read_info(&sstable, index_read_info))) {
      LOG_WARN("failed to get index read info ", KR(ret), K(sstable));
    } else if (OB_FAIL(meta_iter.open(datum_range,
                   ObMacroBlockMetaType::DATA_BLOCK_META,
                   sstable,
                   *index_read_info,
                   allocator))) {
      LOG_WARN("failed to open sec meta iterator", K(ret));
    } else {
      ObDataMacroBlockMeta data_macro_block_meta;
      ObLogicMacroBlockId logic_id;
      while (OB_SUCC(ret)) {
        data_macro_block_meta.reset();
        logic_id.reset();
        if (OB_FAIL(meta_iter.get_next(data_macro_block_meta))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("failed to get next", K(ret));
          }
        } else {
          logic_id = data_macro_block_meta.get_logic_id();
          if (OB_FAIL(logic_id_list.push_back(logic_id))) {
            LOG_WARN("failed to push back", K(ret), K(logic_id));
          }
        }
      }
    }
  }
  return ret;
}

int ObBackupUtils::fetch_macro_block_id_list_for_ddl_in_ss_mode(const storage::ObTabletHandle &tablet_handle,
    blocksstable::ObSSTable &sstable, common::ObIArray<blocksstable::MacroBlockId> &macro_id_list)
{
  int ret = OB_SUCCESS;
  macro_id_list.reset();
  ObBackupOtherBlockIdIterator id_iterator;
  if (OB_FAIL(id_iterator.init(tablet_handle.get_obj()->get_tablet_id(), sstable))) {
    LOG_WARN("failed to init id iterator", K(ret), K(tablet_handle), K(sstable));
  } else {
    MacroBlockId macro_id;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(id_iterator.get_next_id(macro_id))) {
        LOG_WARN("failed to get next id", K(ret));
      } else if (OB_FAIL(macro_id_list.push_back(macro_id))) {
        LOG_WARN("failed to push back", K(ret), K(macro_id));
      }
    }
  }
  return ret;
}

int ObBackupUtils::report_task_result(const int64_t job_id, const int64_t task_id, const uint64_t tenant_id,
    const share::ObLSID &ls_id, const int64_t turn_id, const int64_t retry_id, const share::ObTaskId trace_id,
    const share::ObTaskId &dag_id, const int64_t result, ObBackupReportCtx &report_ctx)
{
  int ret = OB_SUCCESS;
  common::ObAddr leader_addr;
  obrpc::ObBackupTaskRes backup_ls_res;
  backup_ls_res.job_id_ = job_id;
  backup_ls_res.task_id_ = task_id;
  backup_ls_res.tenant_id_ = tenant_id;
  backup_ls_res.src_server_ = GCTX.self_addr();
  backup_ls_res.ls_id_ = ls_id;
  backup_ls_res.result_ = result;
  backup_ls_res.trace_id_ = trace_id;
  backup_ls_res.dag_id_ = dag_id;
  const int64_t cluster_id = GCONF.cluster_id;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);

#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_META_REPORT_RESULT_FAILED) OB_SUCCESS;
  if (OB_FAIL(ret)) {
    SERVER_EVENT_SYNC_ADD("backup_errsim", "before report task result");
    LOG_WARN("errsim backup meta task failed", K(ret), K(backup_ls_res));
  }
#endif
  if (OB_FAIL(ret)) {
  } else if (job_id <= 0 || task_id <= 0 || OB_INVALID_ID == tenant_id || !ls_id.is_valid() || !report_ctx.is_valid()
      || trace_id.is_invalid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(job_id), K(task_id), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(report_ctx.location_service_->get_leader_with_retry_until_timeout(
      cluster_id, meta_tenant_id, ObLSID(ObLSID::SYS_LS_ID), leader_addr))) {
    LOG_WARN("failed to get leader address", K(ret));
  } else if (OB_FAIL(report_ctx.rpc_proxy_->to(leader_addr).report_backup_over(backup_ls_res))) {
    LOG_WARN("failed to post backup ls data res", K(ret), K(backup_ls_res));
  } else {
    SERVER_EVENT_ADD("backup_data", "report_result",
        "job_id", job_id,
        "task_id", task_id,
        "tenant_id", tenant_id,
        "ls_id", ls_id.id(),
        "turn_id", turn_id,
        "retry_id", retry_id,
        result);
    LOG_INFO("finish task post rpc result", K(backup_ls_res));
  }
  return ret;
}

int ObBackupUtils::check_tablet_minor_sstable_validity_(const storage::ObTabletHandle &tablet_handle,
    const common::ObIArray<storage::ObSSTableWrapper> &minor_sstable_array)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = NULL;
  ObITable *last_table_ptr = NULL;
  ObTabletID tablet_id;
  SCN start_scn = SCN::min_scn();
  SCN clog_checkpoint_scn = SCN::min_scn();
  if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet handle", K(ret), K(tablet_handle));
  } else {
    const ObTabletMeta &tablet_meta = tablet->get_tablet_meta();
    tablet_id = tablet_meta.tablet_id_;
    start_scn = tablet_meta.start_scn_;
    clog_checkpoint_scn = tablet_meta.clog_checkpoint_scn_;
  }

  if (OB_FAIL(ret)) {
  } else if ((minor_sstable_array.empty() && start_scn != clog_checkpoint_scn)
      || (!minor_sstable_array.empty() && start_scn >= clog_checkpoint_scn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("clog checkpoint ts unexpected", K(tablet_id), K(clog_checkpoint_scn), K(start_scn));
  }

  if (OB_FAIL(ret)) {
  } else if (minor_sstable_array.empty()) {
    // do nothing
  } else if (OB_ISNULL(last_table_ptr = minor_sstable_array.at(minor_sstable_array.count() - 1).get_sstable())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid table ptr", K(ret), K(minor_sstable_array));
  } else if (!last_table_ptr->is_minor_sstable()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table ptr not correct", K(ret), KPC(last_table_ptr));
  } else {
    const ObITable::TableKey &table_key = last_table_ptr->get_key();
    if (table_key.get_end_scn() < clog_checkpoint_scn) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tablet meta is not valid", K(ret), K(table_key), K(clog_checkpoint_scn));
    }
  }
  return ret;
}

int ObBackupUtils::check_tablet_ddl_sstable_validity_(const storage::ObTabletHandle &tablet_handle,
    const common::ObIArray<storage::ObSSTableWrapper> &ddl_sstable_array)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = NULL;
  ObITable *last_table_ptr = NULL;
  SCN compact_start_scn = SCN::min_scn();
  SCN compact_end_scn = SCN::min_scn();
  ObTableStoreIterator ddl_table_iter;
  bool is_data_complete = false;
  if (ddl_sstable_array.empty()) {
    // do nothing
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet handle", K(ret), K(tablet_handle));
  } else if (OB_ISNULL(last_table_ptr = ddl_sstable_array.at(ddl_sstable_array.count() - 1).get_sstable())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid table ptr", K(ret), K(ddl_sstable_array));
  } else if (!last_table_ptr->is_ddl_dump_sstable()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table ptr not correct", K(ret), KPC(last_table_ptr));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_sstables(ddl_table_iter))) {
    LOG_WARN("failed to get ddl sstables", K(ret), K(tablet_handle));
  } else if (OB_FAIL(ObTabletDDLUtil::check_data_continue(ddl_table_iter, is_data_complete, compact_start_scn, compact_end_scn))) {
    LOG_WARN("failed to check data integrity", K(ret), K(ddl_table_iter));
  } else if (!is_data_complete) {
    ret = OB_INVALID_TABLE_STORE;
    LOG_WARN("get invalid ddl table store", K(ret), K(tablet_handle), K(ddl_sstable_array), K(ddl_table_iter));
  } else {
    LOG_INFO("check data intergirty", K(tablet_handle), K(compact_start_scn),
        K(compact_end_scn), K(ddl_table_iter), K(is_data_complete));
  }
  return ret;
}

int ObBackupUtils::check_ls_validity(const uint64_t tenant_id, const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  const common::ObAddr &local_addr = GCTX.self_addr();
  bool in_member_list = false;
  common::ObAddr leader_addr;
  common::ObArray<common::ObAddr> addr_list;
  if (OB_INVALID_ID == tenant_id || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(get_ls_leader_(tenant_id, ls_id, leader_addr))) {
    LOG_WARN("failed to get ls leader", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(fetch_ls_member_list_(tenant_id, ls_id, leader_addr, addr_list))) {
    LOG_WARN("failed to fetch ls leader member list", K(ret), K(tenant_id), K(ls_id), K(leader_addr));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < addr_list.count(); ++i) {
      const common::ObAddr &addr = addr_list.at(i);
      if (addr == local_addr) {
        in_member_list = true;
        break;
      }
    }
  }
  if (OB_SUCC(ret) && !in_member_list) {
    ret = OB_REPLICA_CANNOT_BACKUP;
    LOG_WARN("not valid ls replica", K(ret), K(tenant_id), K(ls_id), K(leader_addr), K(local_addr), K(addr_list));
  }
  return ret;
}

int ObBackupUtils::check_ls_valid_for_backup(const uint64_t tenant_id, const share::ObLSID &ls_id, const int64_t local_rebuild_seq)
{
  int ret = OB_SUCCESS;
  storage::ObLS *ls = NULL;
  ObLSService *ls_service = NULL;
  ObLSHandle handle;
  int64_t cur_rebuild_seq = 0;
  ObLSMeta ls_meta;
  const bool check_archive = false;
  if (OB_INVALID_ID == tenant_id || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_ISNULL(ls_service = MTL_WITH_CHECK_TENANT(ObLSService *, tenant_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream service is NULL", K(ret));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream not exist", K(ret), K(ls_id));
  } else if (ls->is_stopped() || ls->is_offline()) {
    ret = OB_REPLICA_CANNOT_BACKUP;
    LOG_WARN("ls has stopped or offline, can not backup", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(ls->get_ls_meta(ls_meta))) {
    LOG_WARN("failed to get ls meta", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(ls_meta.check_valid_for_backup())) {
    LOG_WARN("failed to check valid for backup", K(ret), K(ls_meta));
  } else {
    cur_rebuild_seq = ls_meta.get_rebuild_seq();
    if (local_rebuild_seq != cur_rebuild_seq) {
      ret = OB_REPLICA_CANNOT_BACKUP;
      LOG_WARN("rebuild seq has changed, can not backup", K(ret), K(tenant_id), K(ls_id), K(local_rebuild_seq), K(cur_rebuild_seq));
    }
  }
  return ret;
}

int ObBackupUtils::get_ls_leader_(const uint64_t tenant_id, const share::ObLSID &ls_id, common::ObAddr &leader)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  static const int64_t DEFAULT_CHECK_LS_LEADER_TIMEOUT = 30 * 1000 * 1000L; // 30s
  const int64_t cluster_id = GCONF.cluster_id;
  if (OB_ISNULL(GCTX.location_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("location cache is NULL", K(ret));
  } else if (OB_INVALID_ID == tenant_id || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tenant_id), K(ls_id));
  } else {
    const int64_t retry_us = 200 * 1000;
    const int64_t start_ts = ObTimeUtility::current_time();
    do {
      if (OB_FAIL(GCTX.location_service_->nonblock_get_leader(cluster_id, tenant_id, ls_id, leader))) {
        if (OB_LS_LOCATION_NOT_EXIST == ret) {
          LOG_WARN("failed to get location and force renew", K(ret), K(tenant_id), K(ls_id), K(cluster_id));
          if (OB_TMP_FAIL(GCTX.location_service_->nonblock_renew(cluster_id, tenant_id, ls_id))) {
            LOG_WARN("failed to nonblock renew from location cache", K(tmp_ret), K(cluster_id), K(tenant_id), K(ls_id));
          }
          if (ObTimeUtility::current_time() - start_ts > DEFAULT_CHECK_LS_LEADER_TIMEOUT) {
            break;
          } else {
            ob_usleep(retry_us);
          }
        } else {
          LOG_WARN("failed to nonblock get leader", K(ret), K(cluster_id), K(tenant_id), K(ls_id));
        }
      } else {
        LOG_INFO("nonblock get leader", K(tenant_id), K(ls_id), K(leader), K(cluster_id));
      }
    } while (OB_LS_LOCATION_NOT_EXIST == ret);

    if (OB_SUCC(ret) && !leader.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("leader addr is invalid", K(ret), K(tenant_id), K(ls_id), K(leader), K(cluster_id));
    }
  }
  return ret;
}

int ObBackupUtils::fetch_ls_member_list_(const uint64_t tenant_id, const share::ObLSID &ls_id,
    const common::ObAddr &leader_addr, common::ObIArray<common::ObAddr> &addr_list)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = NULL;
  storage::ObStorageRpc *storage_rpc = NULL;
  storage::ObStorageHASrcInfo src_info;
  src_info.src_addr_ = leader_addr;
  src_info.cluster_id_ = GCONF.cluster_id;
  obrpc::ObFetchLSMemberListInfo member_info;
  if (OB_ISNULL(ls_service = MTL_WITH_CHECK_TENANT(ObLSService *, tenant_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream service is NULL", K(ret));
  } else if (OB_ISNULL(storage_rpc = ls_service->get_storage_rpc())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage rpc proxy is NULL", K(ret));
  } else if (OB_FAIL(storage_rpc->post_ls_member_list_request(tenant_id, src_info, ls_id, member_info))) {
    LOG_WARN("failed to post ls member list request", K(ret), K(tenant_id), K(src_info), K(ls_id));
  } else if (OB_FAIL(member_info.member_list_.get_addr_array(addr_list))) {
    LOG_WARN("failed to get addr array", K(ret), K(member_info));
  }
  return ret;
}

/* ObBackupTabletCtx */

ObBackupTabletCtx::ObBackupTabletCtx()
    : total_tablet_meta_count_(0),
      finish_tablet_meta_count_(0),
      reused_macro_block_count_(0),
      total_minor_macro_block_count_(0),
      finish_minor_macro_block_count_(0),
      total_major_macro_block_count_(0),
      finish_major_macro_block_count_(0),
      opened_rebuilder_count_(0),
      closed_rebuilder_count_(0),
      is_all_loaded_(false),
      other_block_mgr_(),
      linked_writer_()
{}

ObBackupTabletCtx::~ObBackupTabletCtx()
{}

void ObBackupTabletCtx::reuse()
{
  total_tablet_meta_count_ = 0;
  finish_tablet_meta_count_ = 0;
  reused_macro_block_count_ = 0;
  total_minor_macro_block_count_ = 0;
  finish_minor_macro_block_count_ = 0;
  total_major_macro_block_count_ = 0;
  finish_major_macro_block_count_ = 0;
  opened_rebuilder_count_ = 0;
  closed_rebuilder_count_ = 0;
  is_all_loaded_ = false;
}

void ObBackupTabletCtx::print_ctx()
{
  LOG_INFO("print ctx", K_(total_tablet_meta_count), K_(finish_tablet_meta_count),
      K_(reused_macro_block_count), K_(total_minor_macro_block_count),
      K_(finish_minor_macro_block_count), K_(total_major_macro_block_count),
      K_(finish_major_macro_block_count), K_(opened_rebuilder_count), K_(closed_rebuilder_count), K_(is_all_loaded));
}

bool ObBackupTabletCtx::is_finished() const
{
  bool bret = false;
  bret = total_minor_macro_block_count_ == finish_minor_macro_block_count_
      && total_major_macro_block_count_ == finish_major_macro_block_count_
      && opened_rebuilder_count_ == closed_rebuilder_count_;
  return bret;
}

/* ObBackupTabletStat */

ObBackupTabletStat::ObBackupTabletStat()
    : is_inited_(false),
      mutex_(common::ObLatchIds::BACKUP_LOCK),
      tenant_id_(OB_INVALID_ID),
      backup_set_id_(0),
      ls_id_(),
      stat_map_(),
      backup_data_type_()
{}

ObBackupTabletStat::~ObBackupTabletStat()
{
  reset();
}

int ObBackupTabletStat::init(const uint64_t tenant_id, const int64_t backup_set_id, const share::ObLSID &ls_id,
    const share::ObBackupDataType &backup_data_type)
{
  int ret = OB_SUCCESS;
  lib::ObMemAttr mem_attr(tenant_id, ObModIds::BACKUP);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup tablet stat init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id || backup_set_id <= 0 || !ls_id.is_valid() || !backup_data_type.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tenant_id), K(backup_set_id), K(ls_id), K(backup_data_type));
  } else if (OB_FAIL(stat_map_.create(DEFAULT_BUCKET_COUNT, mem_attr))) {
    LOG_WARN("failed to create stat map", K(ret));
  } else {
    tenant_id_ = tenant_id;
    backup_set_id_ = backup_set_id;
    ls_id_ = ls_id;
    backup_data_type_ = backup_data_type;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupTabletStat::prepare_tablet_sstables(const uint64_t tenant_id, const share::ObBackupDataType &backup_data_type,
    const common::ObTabletID &tablet_id, const storage::ObTabletHandle &tablet_handle,
    const common::ObIArray<storage::ObSSTableWrapper> &sstable_array, const int64_t total_tablet_meta_count)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  const bool create_if_not_exist = true;
  ObBackupTabletCtx *stat = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup tablet stat do not init", K(ret));
  } else if (OB_FAIL(get_tablet_stat_(tablet_id, create_if_not_exist, stat))) {
    LOG_WARN("failed to get tablet stat", K(ret), K(tablet_id));
  } else if (OB_ISNULL(stat)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stat should not be null", K(ret), K(tablet_id));
  } else if (backup_data_type.type_ != backup_data_type_.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup data type not match", K(backup_data_type), K(backup_data_type_));
  } else {
    stat->total_tablet_meta_count_ = total_tablet_meta_count;
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < sstable_array.count(); ++i) {
    ObSSTable *sstable_ptr = sstable_array.at(i).get_sstable();
    if (OB_ISNULL(sstable_ptr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table ptr should not be null", K(ret), K(i), K(sstable_array));
    } else if (!sstable_ptr->is_sstable()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table is not sstable", K(ret), KPC(sstable_ptr));
    } else {
      const ObITable::TableKey &table_key = sstable_ptr->get_key();
      if (OB_SUCC(ret)) {
        if (GCTX.is_shared_storage_mode() && sstable_ptr->is_ddl_dump_sstable()) {
          ObBackupOtherBlocksMgr &other_block_mgr = stat->other_block_mgr_;
          if (OB_FAIL(other_block_mgr.init(tenant_id, tablet_id, table_key, *sstable_ptr))) {
            LOG_WARN("failed to init backup other blocks mgr", K(ret), K(tenant_id), K(tablet_id), K(table_key), KPC(sstable_ptr));
          }
        }
      }
    }
  }
  return ret;
}

int ObBackupTabletStat::mark_items_pending(
    const share::ObBackupDataType &backup_data_type, const common::ObIArray<ObBackupProviderItem> &items)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup tablet stat do not init", K(ret));
  } else if (backup_data_type.type_ != backup_data_type_.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup data type not match", K(backup_data_type), K(backup_data_type_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < items.count(); ++i) {
      const ObBackupProviderItem &item = items.at(i);
      const ObBackupProviderItemType &item_type = item.get_item_type();
      if (OB_FAIL(do_with_stat_when_pending_(item))) {
        LOG_WARN("failed to do with stat when pending", K(ret), K(item));
      }
    }
  }
  return ret;
}

int ObBackupTabletStat::mark_items_reused(const share::ObBackupDataType &backup_data_type,
    const common::ObIArray<ObBackupProviderItem> &items, common::ObIArray<ObBackupDeviceMacroBlockId> &physical_ids)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup tablet stat do not init", K(ret));
  } else if (backup_data_type.type_ != backup_data_type_.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup data type not match", K(backup_data_type), K(backup_data_type_));
  } else if (items.count() != physical_ids.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("item count should be same", K(items.count()), K(physical_ids.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < items.count(); ++i) {
      const ObBackupProviderItem &item = items.at(i);
      const ObBackupDeviceMacroBlockId &physical_id = physical_ids.at(i);
      if (OB_FAIL(do_with_stat_when_reused_(item, physical_id))) {
        LOG_WARN("failed to do with stat when reused", K(ret), K(item));
      } else {
        LOG_DEBUG("backup reuse macro block", K_(tenant_id), K_(backup_set_id), K_(ls_id), K(item));
      }
    }
  }
  return ret;
}

int ObBackupTabletStat::mark_item_reused(const share::ObBackupDataType &backup_data_type,
    const ObITable::TableKey &table_key, const ObBackupMacroBlockIDPair &id_pair)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  ObBackupProviderItem item;
  ObBackupProviderItemType item_type = PROVIDER_ITEM_MACRO_ID;
  ObBackupMacroBlockId macro_id;
  macro_id.table_key_ = table_key;
  macro_id.macro_block_id_ = ObBackupProviderItem::get_fake_macro_id_();
  macro_id.logic_id_ = id_pair.logic_id_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup tablet stat do not init", K(ret));
  } else if (backup_data_type.type_ != backup_data_type_.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup data type not match", K(backup_data_type), K(backup_data_type_));
  } else if (OB_FAIL(item.set(item_type, backup_data_type, macro_id, table_key, ObTabletID(id_pair.logic_id_.tablet_id_)))) {
    LOG_WARN("failed to set provider item", K(ret), K(item));
  } else if (OB_FAIL(do_with_stat_when_reused_(item, id_pair.physical_id_))) {
    LOG_WARN("failed to do with stat when reused", K(ret));
  }
  return ret;
}

int ObBackupTabletStat::mark_item_finished(const share::ObBackupDataType &backup_data_type,
    const ObBackupProviderItem &item, const ObBackupDeviceMacroBlockId &physical_id)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  const ObTabletID &tablet_id = item.get_tablet_id();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup tablet stat do not init", K(ret));
  } else if (backup_data_type.type_ != backup_data_type_.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup data type not match", K(backup_data_type), K(backup_data_type_));
  } else if (OB_FAIL(do_with_stat_when_finish_(item, physical_id))) {
    LOG_WARN("failed to do with stat", K(ret), K(item), K(physical_id));
  } else {
    LOG_INFO("mark backup item finished", K(backup_data_type), K(item), K(physical_id));
  }
  return ret;
}

int ObBackupTabletStat::add_finished_tablet_meta_count(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  const bool create_if_not_exist = false;
  ObBackupTabletCtx *ctx = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup tablet stat do not init", K(ret));
  } else if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tablet_id));
  } else if (OB_FAIL(get_tablet_stat_(tablet_id, create_if_not_exist, ctx))) {
    LOG_WARN("failed to get tablet stat", K(ret), K(tablet_id));
  } else {
    ctx->finish_tablet_meta_count_++;
  }
  return ret;
}

int ObBackupTabletStat::add_opened_rebuilder_count(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  const bool create_if_not_exist = false;
  ObBackupTabletCtx *ctx = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup tablet stat do not init", K(ret));
  } else if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tablet_id));
  } else if (OB_FAIL(get_tablet_stat_(tablet_id, create_if_not_exist, ctx))) {
    LOG_WARN("failed to get tablet stat", K(ret), K(tablet_id));
  } else {
    ctx->opened_rebuilder_count_++;
  }
  return ret;
}

int ObBackupTabletStat::add_closed_rebuilder_count(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  const bool create_if_not_exist = false;
  ObBackupTabletCtx *ctx = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup tablet stat do not init", K(ret));
  } else if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tablet_id));
  } else if (OB_FAIL(get_tablet_stat_(tablet_id, create_if_not_exist, ctx))) {
    LOG_WARN("failed to get tablet stat", K(ret), K(tablet_id));
  } else {
    ctx->closed_rebuilder_count_++;
  }
  return ret;
}

int ObBackupTabletStat::check_can_release_tablet(const common::ObTabletID &tablet_id, bool &can_release)
{
  int ret = OB_SUCCESS;
  can_release = false;
  ObMutexGuard guard(mutex_);
  const bool create_if_not_exist = false;
  ObBackupTabletCtx *ctx = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup tablet stat do not init", K(ret));
  } else if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tablet_id));
  } else if (OB_FAIL(get_tablet_stat_(tablet_id, create_if_not_exist, ctx))) {
    LOG_WARN("failed to get tablet stat", K(ret), K(tablet_id));
  } else {
    can_release = ctx->finish_tablet_meta_count_ == ctx->total_tablet_meta_count_
	       && ctx->other_block_mgr_.is_finished();
  }
  return ret;
}

int ObBackupTabletStat::get_tablet_stat(const common::ObTabletID &tablet_id, ObBackupTabletCtx *&ctx)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  const bool create_if_not_exist = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup tablet stat do not init", K(ret));
  } else if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tablet_id));
  } else if (OB_FAIL(get_tablet_stat_(tablet_id, create_if_not_exist, ctx))) {
    LOG_WARN("failed to get tablet stat", K(ret), K(tablet_id));
  }
  return ret;
}

int ObBackupTabletStat::free_tablet_stat(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  const bool create_if_not_exist = false;
  ObBackupTabletCtx *ctx = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup tablet stat do not init", K(ret));
  } else if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tablet_id));
  } else if (OB_FAIL(get_tablet_stat_(tablet_id, create_if_not_exist, ctx))) {
    LOG_WARN("failed to get tablet stat", K(ret), K(tablet_id));
  } else {
    free_stat_(ctx);
    if (OB_FAIL(stat_map_.erase_refactored(tablet_id))) {
      LOG_WARN("failed to erase", K(ret), K(tablet_id));
    }
  }
  return ret;
}

int ObBackupTabletStat::print_tablet_stat() const
{
  int ret = OB_SUCCESS;
  PrintTabletStatOp op;
  if (OB_FAIL(stat_map_.foreach_refactored(op))) {
    LOG_WARN("failed to forearch", K(ret));
  }
  return ret;
}

void ObBackupTabletStat::set_backup_data_type(const share::ObBackupDataType &backup_data_type)
{
  ObMutexGuard guard(mutex_);
  backup_data_type_ = backup_data_type;
}

void ObBackupTabletStat::reuse()
{
  ObMutexGuard guard(mutex_);
  ObBackupTabletCtxMap::iterator iter;
  for (iter = stat_map_.begin(); iter != stat_map_.end(); ++iter) {
    if (OB_NOT_NULL(iter->second)) {
      free_stat_(iter->second);
    }
  }
  stat_map_.reuse();
}

void ObBackupTabletStat::reset()
{
  is_inited_ = false;
  reuse();
}

int ObBackupTabletStat::do_with_stat_when_pending_(const ObBackupProviderItem &item)
{
  int ret = OB_SUCCESS;
  ObBackupTabletCtx *stat = NULL;
  const bool create_if_not_exist = false;
  const ObTabletID &tablet_id = item.get_tablet_id();
  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(item));
  } else if (OB_FAIL(get_tablet_stat_(tablet_id, create_if_not_exist, stat))) {
    LOG_WARN("failed to get tablet stat", K(ret), K(tablet_id));
  } else if (OB_ISNULL(stat)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stat should not be null", K(ret), K_(backup_data_type), K(tablet_id));
  } else {
    const ObBackupProviderItemType type = item.get_item_type();
    const share::ObBackupDataType &backup_data_type = item.get_backup_data_type();
    if (PROVIDER_ITEM_MACRO_ID == type) {
      if (backup_data_type.is_sys_backup()) {
        ++stat->total_minor_macro_block_count_;
      } else if (backup_data_type.is_minor_backup()) {
        ++stat->total_minor_macro_block_count_;
      } else if (backup_data_type.is_major_backup()) {
        ++stat->total_major_macro_block_count_;
      }
    } else if (PROVIDER_ITEM_TABLET_AND_SSTABLE_META == type) {
      stat->is_all_loaded_ = true;
    }
  }
  return ret;
}

int ObBackupTabletStat::do_with_stat_when_reused_(
    const ObBackupProviderItem &item, const ObBackupDeviceMacroBlockId &physical_id)
{
  int ret = OB_SUCCESS;
  ObBackupTabletCtx *stat = NULL;
  const bool create_if_not_exist = false;
  const ObTabletID &tablet_id = item.get_tablet_id();
  const ObITable::TableKey &table_key = item.get_table_key();
  const ObLogicMacroBlockId &logic_id = item.get_logic_id();
  const ObBackupProviderItemType type = item.get_item_type();
  const ObBackupDataType &backup_data_type = item.get_backup_data_type();
  if (!backup_data_type.is_major_backup()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("reuse should only be major", K(ret), K(item));
  } else if (!item.is_valid() || !physical_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(item), K(physical_id));
  } else if (OB_FAIL(get_tablet_stat_(tablet_id, create_if_not_exist, stat))) {
    LOG_WARN("failed to get tablet stat", K(ret), K(tablet_id));
  } else if (OB_ISNULL(stat)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stat should not be null", K(ret), K_(backup_data_type), K(tablet_id));
  } else if (PROVIDER_ITEM_MACRO_ID != type) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only macro block can reuse", K(type));
  } else {
    ++stat->reused_macro_block_count_;
    ++stat->total_major_macro_block_count_;
  }
  return ret;
}

int ObBackupTabletStat::do_with_stat_when_finish_(
    const ObBackupProviderItem &item, const ObBackupDeviceMacroBlockId &physical_id)
{
  int ret = OB_SUCCESS;
  ObBackupTabletCtx *stat = NULL;
  const bool create_if_not_exist = false;
  const ObTabletID &tablet_id = item.get_tablet_id();
  if (!item.is_valid() || !physical_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(item), K(physical_id));
  } else if (OB_FAIL(get_tablet_stat_(tablet_id, create_if_not_exist, stat))) {
    LOG_WARN("failed to get tablet stat", K(ret), K(tablet_id));
  } else if (OB_ISNULL(stat)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stat should not be null", K(ret), K_(backup_data_type), K(tablet_id));
  } else {
    const ObBackupProviderItemType type = item.get_item_type();
    const ObBackupDataType &backup_data_type = item.get_backup_data_type();
    if (PROVIDER_ITEM_MACRO_ID == type) {
      if (backup_data_type.is_sys_backup()) {
        ++stat->finish_minor_macro_block_count_;
      } else if (backup_data_type.is_minor_backup()) {
        ++stat->finish_minor_macro_block_count_;
      } else if (backup_data_type.is_major_backup()) {
        ++stat->finish_major_macro_block_count_;
      }
    }
  }
  return ret;
}

int ObBackupTabletStat::get_tablet_stat_(
    const common::ObTabletID &tablet_id, const bool create_if_not_exist, ObBackupTabletCtx *&stat)
{
  int ret = OB_SUCCESS;
  int hash_ret = OB_SUCCESS;
  stat = NULL;
  hash_ret = stat_map_.get_refactored(tablet_id, stat);
  if (OB_HASH_NOT_EXIST == hash_ret) {
    if (create_if_not_exist) {
      if (OB_FAIL(alloc_stat_(stat))) {
        LOG_WARN("failed to alloc stat", K(ret));
      } else if (OB_FAIL(stat_map_.set_refactored(tablet_id, stat, 1))) {
        LOG_WARN("failed to set refactored", K(ret), K(tablet_id), KPC(stat));
        free_stat_(stat);
        stat = nullptr;
      }
    } else {
      ret = hash_ret;
    }
  } else if (OB_SUCCESS == hash_ret) {
    // do nothing
  } else {
    ret = hash_ret;
    LOG_WARN("get tablet stat meet error", K(ret));
  }
  return ret;
}

int ObBackupTabletStat::alloc_stat_(ObBackupTabletCtx *&stat)
{
  int ret = OB_SUCCESS;
  stat = NULL;
  ObBackupTabletCtx *tmp_ctx = NULL;
  if (OB_ISNULL(tmp_ctx = ObLSBackupFactory::get_backup_tablet_ctx(tenant_id_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate ctx", K(ret));
  } else {
    stat = tmp_ctx;
  }
  return ret;
}

void ObBackupTabletStat::free_stat_(ObBackupTabletCtx *&stat)
{
  if (OB_NOT_NULL(stat)) {
    ObLSBackupFactory::free(stat);
  }
}

void ObBackupTabletStat::report_event_(const common::ObTabletID &tablet_id, const ObBackupTabletCtx &tablet_ctx)
{
  const char *backup_event = NULL;
  if (backup_data_type_.is_sys_backup()) {
    backup_event = "backup_sys_tablet";
  } else if (backup_data_type_.is_user_backup()) {
    backup_event = "backup_user_tablet";
  }
}

int ObBackupTabletStat::PrintTabletStatOp::operator()(
    common::hash::HashMapPair<common::ObTabletID, ObBackupTabletCtx *> &entry)
{
  int ret = OB_SUCCESS;
  const common::ObTabletID &tablet_id = entry.first;
  const ObBackupTabletCtx *ctx = entry.second;
  LOG_INFO("backup tablet stat entry", K(tablet_id), KPC(ctx));
  return ret;
}

/* ObBackupTabletHolder */

ObBackupTabletHolder::ObBackupTabletHolder()
  : is_inited_(false),
    ls_id_(),
    holder_map_(),
    fifo_allocator_()
{}

ObBackupTabletHolder::~ObBackupTabletHolder()
{
  reset();
}

int ObBackupTabletHolder::init(const uint64_t tenant_id, const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_BUCKET_NUM = 1024;
  lib::ObMemAttr mem_attr(tenant_id, ObModIds::BACKUP);
  const int64_t block_size = common::OB_MALLOC_NORMAL_BLOCK_SIZE;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup tablet holder init twice", K(ret));
  } else if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(ls_id));
  } else if (OB_FAIL(holder_map_.create(MAX_BUCKET_NUM, mem_attr))) {
    LOG_WARN("failed to create tablet handle map", K(ret));
  } else if (OB_FAIL(fifo_allocator_.init(NULL, block_size, mem_attr))) {
    LOG_WARN("failed to init fifo allocator", K(ret));
  } else {
    is_inited_ = true;
    ls_id_ = ls_id;
  }
  return ret;
}

int ObBackupTabletHolder::alloc_tablet_ref(const uint64_t tenant_id, ObBackupTabletHandleRef *&tablet_handle)
{
  int ret = OB_SUCCESS;
  tablet_handle = NULL;
  void *buf = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet holder not init", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tenant_id));
  } else if (OB_ISNULL(buf = fifo_allocator_.alloc(sizeof(ObBackupTabletHandleRef)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  } else {
    ObMemAttr attr(tenant_id, "BackupTbtHldr");
    tablet_handle = new (buf) ObBackupTabletHandleRef;
    tablet_handle->allocator_.set_attr(attr);
  }
  return ret;
}

void ObBackupTabletHolder::free_tablet_ref(ObBackupTabletHandleRef *&tablet_handle)
{
  if (OB_NOT_NULL(tablet_handle)) {
    tablet_handle->~ObBackupTabletHandleRef();
    fifo_allocator_.free(tablet_handle);
    tablet_handle = NULL;
  }
}

int ObBackupTabletHolder::set_tablet(const common::ObTabletID &tablet_id, ObBackupTabletHandleRef *tablet_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet holder not init", K(ret));
  } else if (!tablet_id.is_valid() || OB_ISNULL(tablet_handle)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tablet_id), KP(tablet_handle));
  } else if (OB_FAIL(holder_map_.set_refactored(tablet_id, tablet_handle))) {
    if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_WARN("tablet handle hold before", K(tablet_id));
    } else {
      LOG_WARN("failed to set refactored", K(ret), K(tablet_id), KPC(tablet_handle));
    }
  }
  return ret;
}

int ObBackupTabletHolder::get_tablet(const common::ObTabletID &tablet_id, ObBackupTabletHandleRef *&tablet_handle)
{
  int ret = OB_SUCCESS;
  tablet_handle = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet holder not init", K(ret));
  } else if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tablet_id), K(tablet_handle));
  } else if (OB_FAIL(holder_map_.get_refactored(tablet_id, tablet_handle))) {
    LOG_WARN("failed to set refactored", K(ret), K(tablet_id), K(tablet_handle));
  }
  return ret;
}

int ObBackupTabletHolder::release_tablet(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObBackupTabletHandleRef *tablet_ref = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet holder not init", K(ret));
  } else if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tablet_id));
  } else if (OB_FAIL(holder_map_.erase_refactored(tablet_id, &tablet_ref))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_WARN("tablet handle do not exit", K(ret), K(tablet_id));
    } else {
      LOG_WARN("failed to erase refactored", K(ret), K(tablet_id));
    }
  } else {
    free_tablet_ref(tablet_ref);
  }
  return ret;
}

bool ObBackupTabletHolder::is_empty() const
{
  return 0 == holder_map_.size();
}

void ObBackupTabletHolder::reuse()
{
  holder_map_.reuse();
}

void ObBackupTabletHolder::reset()
{
  FOREACH(it, holder_map_) {
    ObBackupTabletHandleRef *&ref = it->second;
    free_tablet_ref(ref);
  }
  holder_map_.reuse();
  is_inited_ = false;
}

/* ObBackupDiskChecker */

ObBackupDiskChecker::ObBackupDiskChecker() : is_inited_(false), tablet_holder_(NULL)
{}

ObBackupDiskChecker::~ObBackupDiskChecker()
{}

int ObBackupDiskChecker::init(ObBackupTabletHolder &tablet_holder)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    tablet_holder_ = &tablet_holder;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupDiskChecker::check_disk_space()
{
  int ret = OB_SUCCESS;
  const int64_t required_size = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("do not init", K(ret));
  } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.check_space_full(required_size))) {
    LOG_WARN("failed to check space full", K(ret));
  }
  return ret;
}

/* ObBackupProviderItem */

ObBackupProviderItem::ObBackupProviderItem()
    : item_type_(PROVIDER_ITEM_MAX), backup_data_type_(),
      logic_id_(), macro_block_id_(),
      table_key_(), tablet_id_(),
      nested_offset_(0), nested_size_(0),
      timestamp_(0), need_copy_(true), macro_index_(),
      absolute_row_offset_(0)
{}

ObBackupProviderItem::~ObBackupProviderItem()
{}

int ObBackupProviderItem::set_with_fake(const ObBackupProviderItemType &item_type,
    const common::ObTabletID &tablet_id, const share::ObBackupDataType &backup_data_type)
{
  int ret = OB_SUCCESS;
  if (PROVIDER_ITEM_TABLET_AND_SSTABLE_META != item_type
      && PROVIDER_ITEM_TABLET_SSTABLE_INDEX_BUILDER_PREPARE != item_type) {
    ret = OB_ERR_SYS;
    LOG_WARN("get invalid args", K(ret), K(item_type));
  } else if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tablet_id));
  } else {
    item_type_ = item_type;
    backup_data_type_ = backup_data_type;
    logic_id_ = get_fake_logic_id_();
    macro_block_id_ = get_fake_macro_id_();
    table_key_ = get_fake_table_key_();
    tablet_id_ = tablet_id;
    if (!is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("provider item not valid", K(ret), KPC(this));
    }
  }
  return ret;
}

int ObBackupProviderItem::set(const ObBackupProviderItemType &item_type, const share::ObBackupDataType &backup_data_type,
    const ObBackupMacroBlockId &macro_id, const ObITable::TableKey &table_key, const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (PROVIDER_ITEM_MACRO_ID != item_type) {
    ret = OB_ERR_SYS;
    LOG_WARN("get invalid args", K(ret));
  } else if (!macro_id.is_valid() || !table_key.is_valid() || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(macro_id), K(table_key), K(tablet_id));
  } else {
    item_type_ = item_type;
    backup_data_type_ = backup_data_type;
    logic_id_ = macro_id.logic_id_;
    macro_block_id_ = macro_id.macro_block_id_;
    table_key_ = table_key;
    tablet_id_ = tablet_id;
    nested_offset_ = macro_id.nested_offset_;
    nested_size_ = macro_id.nested_size_;
    absolute_row_offset_ = macro_id.absolute_row_offset_;
    if (!is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("provider item not valid", K(ret), KPC(this));
    }
  }
  return ret;
}

int ObBackupProviderItem::set_for_ss_ddl(const blocksstable::MacroBlockId &macro_id,
    const storage::ObITable::TableKey &table_key, const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (!macro_id.is_valid() || !table_key.is_valid() || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(macro_id), K(table_key), K(tablet_id));
  } else {
    item_type_ = PROVIDER_ITEM_DDL_OTHER_BLOCK_ID;
    logic_id_ = get_fake_logic_id_();
    macro_block_id_ = macro_id;
    table_key_ = table_key;
    tablet_id_ = tablet_id;
    nested_offset_ = 0;
    nested_size_ = OB_DEFAULT_MACRO_BLOCK_SIZE;
  }
  return ret;
}

bool ObBackupProviderItem::operator==(const ObBackupProviderItem &other) const
{
  return item_type_ == other.item_type_ && backup_data_type_ == other.backup_data_type_ && logic_id_ == other.logic_id_
      && macro_block_id_ == other.macro_block_id_ && table_key_ == other.table_key_
      && tablet_id_ == other.tablet_id_ && nested_size_ == other.nested_size_ && nested_offset_ == other.nested_offset_
      && need_copy_ == other.need_copy_ && macro_index_ == other.macro_index_ && absolute_row_offset_ == other.absolute_row_offset_;
}

bool ObBackupProviderItem::operator!=(const ObBackupProviderItem &other) const
{
  return !(*this == other);
}

ObBackupProviderItemType ObBackupProviderItem::get_item_type() const
{
  return item_type_;
}

ObBackupDataType ObBackupProviderItem::get_backup_data_type() const
{
  return backup_data_type_;
}

blocksstable::ObLogicMacroBlockId ObBackupProviderItem::get_logic_id() const
{
  return logic_id_;
}

blocksstable::MacroBlockId ObBackupProviderItem::get_macro_block_id() const
{
  return macro_block_id_;
}

const ObITable::TableKey &ObBackupProviderItem::get_table_key() const
{
  return table_key_;
}

common::ObTabletID ObBackupProviderItem::get_tablet_id() const
{
  return tablet_id_;
}

int64_t ObBackupProviderItem::get_nested_offset() const
{
  return nested_offset_;
}

int64_t ObBackupProviderItem::get_nested_size() const
{
  return nested_size_;
}

int64_t ObBackupProviderItem::get_deep_copy_size() const
{
  return 0;
}

int ObBackupProviderItem::deep_copy(const ObBackupProviderItem &src)
{
  int ret = OB_SUCCESS;
  item_type_ = src.item_type_;
  backup_data_type_ = src.backup_data_type_;
  logic_id_ = src.logic_id_;
  macro_block_id_ = src.macro_block_id_;
  table_key_ = src.table_key_;
  tablet_id_ = src.tablet_id_;
  nested_offset_ = src.nested_offset_;
  nested_size_ = src.nested_size_;
  need_copy_ = src.need_copy_;
  macro_index_ = src.macro_index_;
  absolute_row_offset_ = src.absolute_row_offset_;
  return ret;
}

int ObBackupProviderItem::deep_copy(const ObBackupProviderItem &src, char *buf, int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  UNUSEDx(buf, len, pos);
  item_type_ = src.item_type_;
  backup_data_type_ = src.backup_data_type_;
  logic_id_ = src.logic_id_;
  macro_block_id_ = src.macro_block_id_;
  table_key_ = src.table_key_;
  tablet_id_ = src.tablet_id_;
  nested_offset_ = src.nested_offset_;
  nested_size_ = src.nested_size_;
  need_copy_ = src.need_copy_;
  macro_index_ = src.macro_index_;
  absolute_row_offset_ = src.absolute_row_offset_;
  return ret;
}

bool ObBackupProviderItem::is_valid() const
{
  bool bret = false;
  if (PROVIDER_ITEM_MACRO_ID != item_type_
      && PROVIDER_ITEM_DDL_OTHER_BLOCK_ID != item_type_
      && PROVIDER_ITEM_TABLET_AND_SSTABLE_META != item_type_
      && PROVIDER_ITEM_TABLET_SSTABLE_INDEX_BUILDER_PREPARE != item_type_) {
    bret = false;
  } else {
    bret = logic_id_.is_valid() && macro_block_id_.is_valid()
        && table_key_.is_valid() && tablet_id_.is_valid();
  }
  return bret;
}

void ObBackupProviderItem::reset()
{
  item_type_ = PROVIDER_ITEM_MAX;
  backup_data_type_.reset();
  logic_id_.reset();
  macro_block_id_.reset();
  table_key_.reset();
  tablet_id_.reset();
  nested_offset_ = 0;
  nested_size_ = 0;
  need_copy_ = true;
  macro_index_.reset();
  absolute_row_offset_ = 0;
}

ObITable::TableKey ObBackupProviderItem::get_fake_table_key_()
{
  ObITable::TableKey table_key;
  table_key.tablet_id_ = ObTabletID(1);
  table_key.table_type_ = ObITable::TableType::MAJOR_SSTABLE;
  table_key.version_range_.snapshot_version_ = 0;
  table_key.column_group_idx_ = 0;
  return table_key;
}

ObLogicMacroBlockId ObBackupProviderItem::get_fake_logic_id_()
{
  return ObLogicMacroBlockId(0/*data_seq*/, 1/*logic_version*/, 1/*tablet_id*/);
}

MacroBlockId ObBackupProviderItem::get_fake_macro_id_()
{
  return MacroBlockId(4096/*first_id*/, 0/*second_id*/, 0/*third_id*/);
}

/* ObBackupProviderItemCompare */

ObBackupProviderItemCompare::ObBackupProviderItemCompare(int &sort_ret) : result_code_(sort_ret), backup_data_type_()
{}

void ObBackupProviderItemCompare::set_backup_data_type(const share::ObBackupDataType &backup_data_type)
{
  backup_data_type_ = backup_data_type;
}

// the adapt of backup at sstable level instead of tablet is too costly for now
// so rewriting the sort function seem to be fine for now and will be rewrite when
// TODO(yangyi.yyy): is more available

// when backup minor, the logic_id will be sorted by table_key first, then logic_id,
// which lead to logic id of same sstable will be grouped together, and the logic id
// is sorted in the same sstable
// when backup major, the logic id will be the logic id directly
bool ObBackupProviderItemCompare::operator()(const ObBackupProviderItem *left, const ObBackupProviderItem *right)
{
  bool bret = false;
  if (OB_ISNULL(left) || OB_ISNULL(right)) {
    result_code_ = OB_INVALID_DATA;
    LOG_WARN_RET(result_code_, "provider item should not be null", K_(result_code), KP(left), KP(right));
  } else if (backup_data_type_.is_minor_backup()) {  // minor sstable is sorted by log ts range end log ts
    if (left->get_tablet_id().id() < right->get_tablet_id().id()) {
      bret = true;
    } else if (left->get_tablet_id().id() > right->get_tablet_id().id()) {
      bret = false;
    } else if (left->get_item_type() < right->get_item_type()) {
      bret = true;
    } else if (left->get_item_type() > right->get_item_type()) {
      bret = false;
    } else if (left->get_table_key().scn_range_.end_scn_ < right->get_table_key().scn_range_.end_scn_) {
      bret = true;
    } else if (left->get_table_key().scn_range_.end_scn_ > right->get_table_key().scn_range_.end_scn_) {
      bret = false;
    } else if (left->get_logic_id() < right->get_logic_id()) {
      bret = true;
    } else if (left->get_logic_id() > right->get_logic_id()) {
      bret = false;
    }
  } else {
    if (left->get_tablet_id().id() < right->get_tablet_id().id()) {
      bret = true;
    } else if (left->get_tablet_id().id() > right->get_tablet_id().id()) {
      bret = false;
    } else if (left->get_item_type() < right->get_item_type()) {
      bret = true;
    } else if (left->get_item_type() > right->get_item_type()) {
      bret = false;
    } else if (left->get_logic_id() < right->get_logic_id()) {
      bret = true;
    } else if (left->get_logic_id() > right->get_logic_id()) {
      bret = false;
    }
  }
  return bret;
}

/* ObBackupTmpFileQueue */

ObBackupTmpFileQueue::ObBackupTmpFileQueue()
    : is_inited_(false),
      tenant_id_(OB_INVALID_ID),
      tmp_file_(),
      read_offset_(0),
      read_count_(0),
      write_count_(0),
      buffer_writer_(ObModIds::BACKUP)
{}

ObBackupTmpFileQueue::~ObBackupTmpFileQueue()
{
  reset();
}

int ObBackupTmpFileQueue::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_INVALID_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tenant_id));
  } else if (OB_FAIL(tmp_file_.open(tenant_id))) {
    LOG_WARN("failed to open tmp file", K(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    read_count_ = 0;
    write_count_ = 0;
    is_inited_ = true;
  }
  return ret;
}

void ObBackupTmpFileQueue::reset()
{
  int tmp_ret = OB_SUCCESS;
  if (!tmp_file_.is_opened()) {
    // do nothing
  } else if (OB_TMP_FAIL(tmp_file_.close())) {
    LOG_ERROR_RET(tmp_ret, "failed to close tmp file", K(tmp_ret));
  }
}

int ObBackupTmpFileQueue::put_item(const ObBackupProviderItem &item)
{
  int ret = OB_SUCCESS;
  const int64_t need_write_size = item.get_serialize_size();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("buffer node do not init", K(ret));
  } else if (OB_FAIL(buffer_writer_.write_pod(need_write_size))) {
    LOG_WARN("failed to write serialize", K(ret), K(need_write_size));
  } else if (OB_FAIL(buffer_writer_.write_serialize(item))) {
    LOG_WARN("failed to write serialize", K(ret), K(item));
  } else if (OB_FAIL(tmp_file_.write(buffer_writer_.data(), buffer_writer_.pos()))) {
    LOG_WARN("failed to write to tmp file", K(ret), K_(buffer_writer));
  } else {
    buffer_writer_.reuse();
    write_count_++;
  }
  return ret;
}

int ObBackupTmpFileQueue::get_item(ObBackupProviderItem &item)
{
  int ret = OB_SUCCESS;
  item.reset();
  const int64_t timeout_ms = 5000;
  tmp_file::ObTmpFileIOInfo io_info;
  tmp_file::ObTmpFileIOHandle handle;
  io_info.fd_ = tmp_file_.get_fd();
  io_info.io_desc_.set_wait_event(common::ObWaitEventIds::BACKUP_TMP_FILE_QUEUE_WAIT);
  io_info.io_timeout_ms_ = timeout_ms;
  common::ObArenaAllocator allocator;
  int64_t item_size = 0;
  char *buf = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("buffer node do not init", K(ret));
  } else if (read_count_ == write_count_) {
    ret = OB_ITER_END;
    LOG_WARN("iter end", K(ret), K_(read_count), K_(write_count));
  } else if (OB_FAIL(get_next_item_size_(item_size))) {
    LOG_WARN("failed to get next item size", K(ret));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(item_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), K(item_size));
  } else if (FALSE_IT(io_info.buf_ = buf)) {
  } else if (FALSE_IT(io_info.size_ = item_size)) {
  } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.pread(MTL_ID(), io_info, read_offset_, handle))) {
    LOG_WARN("failed to pread from tmp file", K(ret), K(io_info), K_(read_offset), K(item_size));
  } else {
    blocksstable::ObBufferReader buffer_reader(buf, item_size);
    if (OB_FAIL(buffer_reader.read_serialize(item))) {
      LOG_WARN("failed to read serialize", K(ret), K_(read_offset), K_(read_count), K_(write_count));
    } else {
      read_offset_ += buffer_reader.pos();
      read_count_++;
    }
  }
  return ret;
}

int ObBackupTmpFileQueue::get_next_item_size_(int64_t &size)
{
  int ret = OB_SUCCESS;
  size = 0;
  const int64_t timeout_ms = 5000;
  tmp_file::ObTmpFileIOInfo io_info;
  tmp_file::ObTmpFileIOHandle handle;
  io_info.fd_ = tmp_file_.get_fd();
  io_info.io_desc_.set_wait_event(common::ObWaitEventIds::BACKUP_TMP_FILE_QUEUE_WAIT);
  io_info.size_ = sizeof(int64_t);
  io_info.io_timeout_ms_ = timeout_ms;
  common::ObArenaAllocator allocator;
  int64_t item_size = sizeof(int64_t);
  char *buf = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("buffer node do not init", K(ret));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(item_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  } else if (FALSE_IT(io_info.buf_ = buf)) {
  } else if (FALSE_IT(io_info.size_ = item_size)) {
  } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.pread(MTL_ID(), io_info, read_offset_, handle))) {
    LOG_WARN("failed to pread from tmp file", K(ret), K(io_info), K_(read_offset), K(item_size));
  } else {
    blocksstable::ObBufferReader buffer_reader(buf, item_size);
    if (OB_FAIL(buffer_reader.read_pod(size))) {
      LOG_WARN("failed to read serialize", K(ret), K_(read_offset), K_(read_count), K_(write_count));
    } else {
      read_offset_ += buffer_reader.pos();
    }
  }
  return ret;
}

/* ObBackupTabletProvider */

ObBackupTabletProvider::ObBackupTabletProvider()
    : is_inited_(),
      is_run_out_(false),
      meet_end_(false),
      mutex_(common::ObLatchIds::BACKUP_LOCK),
      param_(),
      backup_data_type_(),
      cur_task_id_(),
      ls_backup_ctx_(NULL),
      index_kv_cache_(NULL),
      sql_proxy_(NULL),
      index_builder_mgr_(NULL),
      meta_index_store_(),
      prev_item_(),
      has_prev_item_(false),
      item_queue_()
{}

ObBackupTabletProvider::~ObBackupTabletProvider()
{
  reset();
}

int ObBackupTabletProvider::init(const ObLSBackupParam &param, const share::ObBackupDataType &backup_data_type,
    ObLSBackupCtx &ls_backup_ctx, ObBackupIndexKVCache &index_kv_cache, common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  const lib::ObLabel label(ObModIds::BACKUP);
  const uint64_t tenant_id = param.tenant_id_;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("provider init twice", K(ret));
  } else if (!param.is_valid() || !backup_data_type.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(param), K(backup_data_type));
  } else if (OB_FAIL(param_.assign(param))) {
    LOG_WARN("failed to assign param", K(ret), K(param));
  } else if (OB_FAIL(item_queue_.init(tenant_id))) {
    LOG_WARN("failed to init queue", K(ret));
  } else {
    backup_data_type_ = backup_data_type;
    ls_backup_ctx_ = &ls_backup_ctx;
    index_kv_cache_ = &index_kv_cache;
    sql_proxy_ = &sql_proxy;
    index_builder_mgr_ = &ls_backup_ctx_->index_builder_mgr_;
    cur_task_id_ = 0;
    is_run_out_ = false;
    is_inited_ = true;
  }
  return ret;
}

void ObBackupTabletProvider::reset()
{
  ObMutexGuard guard(mutex_);
  is_inited_ = false;
  ls_backup_ctx_ = NULL;
  free_queue_item_();
}

void ObBackupTabletProvider::reuse()
{
  ObMutexGuard guard(mutex_);
  is_run_out_ = false;
  meet_end_ = false;
  cur_task_id_ = 0;
  if (OB_NOT_NULL(ls_backup_ctx_)) {
    ls_backup_ctx_->reuse();
  }
}

bool ObBackupTabletProvider::is_run_out()
{
  ObMutexGuard guard(mutex_);
  return is_run_out_;
}

void ObBackupTabletProvider::set_backup_data_type(const share::ObBackupDataType &backup_data_type)
{
  ObMutexGuard guard(mutex_);
  backup_data_type_ = backup_data_type;
}

ObBackupDataType ObBackupTabletProvider::get_backup_data_type() const
{
  ObMutexGuard guard(mutex_);
  return backup_data_type_;
}

int ObBackupTabletProvider::get_next_batch_items(common::ObIArray<ObBackupProviderItem> &items, int64_t &task_id)
{
  int ret = OB_SUCCESS;
  items.reset();
  ObMutexGuard guard(mutex_);
  const uint64_t tenant_id = param_.tenant_id_;
  const share::ObLSID &ls_id = param_.ls_id_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup tablet provider do not init", K(ret));
  } else if (OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls backup ctx should not be null", K(ret));
  } else if (OB_FAIL(prepare_batch_tablet_(tenant_id, ls_id))) {
    LOG_WARN("failed to prepare batch tablet", K(ret), K(tenant_id), K(ls_id));
  } else {
    ObArray<ObBackupProviderItem> tmp_items;
    int64_t batch_size = BATCH_SIZE;
#ifdef ERRSIM
    if (!ls_id.is_sys_ls()) {
      const int64_t errsim_batch_size = GCONF.errsim_tablet_batch_count;
      if (0 != errsim_batch_size) {
        batch_size = errsim_batch_size;
        LOG_INFO("get errsim batch size", K(errsim_batch_size));
      }
    }
#endif
    while (OB_SUCC(ret)) {
      tmp_items.reset();
      if (OB_SUCCESS != ls_backup_ctx_->get_result_code()) {
        LOG_INFO("backup task already failed", "result_code", ls_backup_ctx_->get_result_code());
        break;
      } else if (OB_FAIL(inner_get_batch_items_(batch_size, tmp_items))) {
        LOG_WARN("failed to inner get batch item", K(ret), K(batch_size));
      } else if (tmp_items.empty() && meet_end_) {
        is_run_out_ = true;
        LOG_INFO("no provider items");
        break;
      } else if (OB_FAIL(append(items, tmp_items))) {
        LOG_WARN("failed to append array", K(ret), K(tmp_items));
      } else if (OB_FAIL(remove_duplicates_(items))) {
        LOG_WARN("failed to remove duplicates", K(ret));
      } else if (items.count() >= batch_size) {
        break;
      } else if (OB_FAIL(prepare_batch_tablet_(tenant_id, ls_id))) {
        LOG_WARN("failed to prepare batch tablet", K(ret), K(tenant_id), K(ls_id));
      }
    }
    if (OB_SUCC(ret)) {
      task_id = cur_task_id_++;
    }
    LOG_INFO("get next batch items", K(ret), K_(backup_data_type), K_(param), K(items));
  }
  return ret;
}

int ObBackupTabletProvider::inner_get_batch_items_(
    const int64_t batch_size, common::ObIArray<ObBackupProviderItem> &items)
{
  int ret = OB_SUCCESS;
  items.reset();
  if (batch_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(batch_size));
  }
  while (OB_SUCC(ret) && items.count() < batch_size) {
    ObBackupProviderItem item;
    if (OB_FAIL(pop_item_from_queue_(item))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to pop item from queue", K(ret));
      }
    } else if (!item.is_valid()) {
      ret = OB_INVALID_DATA;
      LOG_WARN("next item is not valid", K(ret), K(item));
    } else if (OB_FAIL(items.push_back(item))) {
      LOG_WARN("failed to push back", K(ret), K(item));
    }
  }
  LOG_INFO("inner get batch item", K(items), K_(backup_data_type));
  return ret;
}

int ObBackupTabletProvider::prepare_batch_tablet_(const uint64_t tenant_id, const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  int64_t total_count = 0;
  while (OB_SUCC(ret) && total_count < BATCH_SIZE) {
    ObTabletID tablet_id;
    int64_t count = 0;
    if (OB_ISNULL(ls_backup_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("log stream backup ctx should not be null", K(ret));
    } else if (OB_FAIL(ls_backup_ctx_->next(tablet_id))) {
      if (OB_ITER_END == ret) {
        meet_end_ = true;
        LOG_INFO("tablet meet end", K(ret), K(tenant_id), K(ls_id), K_(backup_data_type));
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get next tablet", K(ret));
      }
    } else if (OB_FAIL(prepare_tablet_(tenant_id, ls_id, tablet_id, backup_data_type_, count))) {
      LOG_WARN("failed to prepare tablet", K(ret), K(tenant_id), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(ObBackupUtils::check_ls_valid_for_backup(tenant_id, ls_id, ls_backup_ctx_->rebuild_seq_))) {
      LOG_WARN("failed to check ls valid for backup", K(ret), K(tenant_id), K(ls_id));
    } else {
      total_count += count;
    }
  }
  return ret;
}

int ObBackupTabletProvider::prepare_tablet_(const uint64_t tenant_id, const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id, const share::ObBackupDataType &backup_data_type, int64_t &total_count)
{
  int ret = OB_SUCCESS;
  total_count = 0;
  ObArray<storage::ObSSTableWrapper> sstable_array;
  ObBackupTabletHandleRef *tablet_ref = NULL;
  bool is_normal = false;
  bool can_explain = false;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  bool need_skip_tablet = false;
  bool is_major_compaction_mview_dep_tablet = false;
  share::SCN mview_dep_scn;
  bool is_split_dst = false;
  if (OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls backup ctx should not be null", K(ret));
  } else if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tablet_id));
  } else if (OB_FAIL(get_tablet_handle_(tenant_id, ls_id, tablet_id, tablet_ref, is_split_dst))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      LOG_WARN("failed to get tablet handle", K(ret), K(tenant_id), K(ls_id), K(tablet_id));
      ret = OB_SUCCESS;
      ObBackupSkippedType skipped_type(ObBackupSkippedType::MAX_TYPE);
      bool need_report_skip = false;
      share::ObLSID split_ls_id;
      if (!is_split_dst) {
        if (OB_FAIL(get_tablet_skipped_type_(param_.tenant_id_, ls_id, tablet_id, skipped_type, split_ls_id))) {
          LOG_WARN("failed to get tablet skipped type", K(ret), K(param_), K(ls_id), K(tablet_id));
        } else if (OB_FAIL(check_need_report_tablet_skipped_(split_ls_id, tablet_id, skipped_type, need_report_skip))) {
          LOG_WARN("failed to check need report tablet skipped", K(ret));
        }
      } else {
        need_report_skip = true;
        skipped_type = ObBackupSkippedType(ObBackupSkippedType::REORGANIZED);
      }
      if (OB_FAIL(ret)) {
      } else if (!need_report_skip) {
        LOG_INFO("skip report tablet skipped", K(ret), K(tablet_id), K(skipped_type), K_(param));
      } else if (OB_FAIL(report_tablet_skipped_(tablet_id, skipped_type, backup_data_type))) {
        LOG_WARN("failed to report tablet skipped", K(ret), K(tablet_id), K_(param), K(skipped_type));
      } else {
        LOG_INFO("report tablet skipped", K(ret), K(tablet_id), K_(param), K(skipped_type));
      }
    } else {
      LOG_WARN("failed to get tablet handle", K(ret), K(tenant_id), K(ls_id), K(tablet_id));
    }
  } else if (OB_FAIL(check_tx_data_can_explain_user_data_(tablet_ref->tablet_handle_, can_explain))) {
    LOG_WARN("failed to check tx data can explain user data", K(ret), K(ls_id), K(tablet_id));
  } else if (!can_explain) {
    ret = OB_REPLICA_CANNOT_BACKUP;
    LOG_WARN("can not backup replica", K(ret), K(tablet_id), K(ls_id));
  } else if (OB_FAIL(check_tablet_replica_validity_(tenant_id, ls_id, tablet_id, backup_data_type))) {
    LOG_WARN("failed to check tablet replica validity", K(ret), K(tenant_id), K(ls_id), K(tablet_id), K(backup_data_type));
  } else if (OB_FAIL(tablet_ref->tablet_handle_.get_obj()->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_FAIL(ls_backup_ctx_->check_is_major_compaction_mview_dep_tablet(tablet_id, mview_dep_scn, is_major_compaction_mview_dep_tablet))) {
    LOG_WARN("failed to check is mview dep tablet", K(ret));
  } else if (OB_FAIL(fetch_tablet_sstable_array_(
      tablet_id, tablet_ref->tablet_handle_, *table_store_wrapper.get_member(), backup_data_type, is_major_compaction_mview_dep_tablet, mview_dep_scn, sstable_array))) {
    LOG_WARN("failed to fetch tablet sstable array", K(ret), K(tablet_id), KPC(tablet_ref), K(backup_data_type));
  } else if (OB_FAIL(add_prepare_tablet_item_(tablet_id))) {
    LOG_WARN("failed to prepare tablet item", K(ret), K(tablet_id));
  } else {
    ObITable::TableKey ss_ddl_table_key;
    bool has_ss_ddl = false;
    int64_t cur_tablet_meta_count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < sstable_array.count(); ++i) {
      int64_t count = 0;
      storage::ObSSTableWrapper &sstable_wrapper = sstable_array.at(i);
      ObSSTable *sstable_ptr = NULL;
      if (OB_ISNULL(sstable_ptr = sstable_wrapper.get_sstable())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be null", K(ret));
      } else {
        const ObITable::TableKey &table_key = sstable_ptr->get_key();
        if (GCTX.is_shared_storage_mode() && table_key.is_ddl_dump_sstable()) {
          if (OB_FAIL(fetch_ddl_macro_id_in_ss_mode_(tablet_id, tablet_ref->tablet_handle_, table_key, *sstable_ptr, total_count))) {
            LOG_WARN("failed to fetch ddl macro id in ss mode", K(ret));
          } else {
            has_ss_ddl = true;
            ss_ddl_table_key = table_key;
          }
        } else {
          if (OB_FAIL(fetch_all_logic_macro_block_id_(tablet_id, tablet_ref->tablet_handle_, table_key, *sstable_ptr, count))) {
            LOG_WARN("failed to fetch all logic macro block id", K(ret), K(tablet_id), KPC(tablet_ref), K(table_key));
          } else {
            total_count += count;
          }
        }
      }
    }
    if (FAILEDx(add_tablet_item_(tablet_id, has_ss_ddl, ss_ddl_table_key))) {
      LOG_WARN("failed to add tablet item if need", K(ret), K(tablet_id));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ls_backup_ctx_->tablet_stat_.prepare_tablet_sstables(
          tenant_id, backup_data_type, tablet_id, tablet_ref->tablet_handle_, sstable_array, cur_tablet_meta_count))) {
        LOG_WARN("failed to prepare tablet sstable", K(ret), K(backup_data_type), K(tablet_id), K(sstable_array));
      }
    }
  }
  LOG_INFO("prepare tablet", K(tenant_id), K(ls_id), K(tablet_id), K(sstable_array), K_(backup_data_type), K(total_count));
  return ret;
}

int ObBackupTabletProvider::check_tx_data_can_explain_user_data_(
    const storage::ObTabletHandle &tablet_handle,
    bool &can_explain)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  can_explain = true;
  // Only backup minor needs to check whether tx data can explain user data.
  // If tablet has no minor sstable, or has no uncommitted row in sstable, it's also no need to check tx_data.
  // The condition that tx_data can explain user data is that tx_data_table's filled_tx_scn is less than the
  // minimum tablet's minor sstable's filled_tx_scn.
  // TODO(zeyong): 4.3 But when transfer supports not killing transaction, minor sstable may have uncommitted rows and it's
  // filled_tx_scn may less than tx_data_table's filled_tx_scn, which is a bad case. Fix this in future by handora.qc.

  if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is nullptr.", K(ret), K(tablet_handle));
  } else if (!ls_backup_ctx_->backup_data_type_.is_user_backup()) {
  } else if (OB_FAIL(tablet->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (table_store_wrapper.get_member()->get_minor_sstables().empty()) {
  } else {
    const ObSSTableArray &sstable_array = table_store_wrapper.get_member()->get_minor_sstables();
    share::SCN min_filled_tx_scn = SCN::max_scn();
    ARRAY_FOREACH(sstable_array, i) {
      ObITable *table_ptr = sstable_array[i];
      ObSSTable *sstable = NULL;
      ObSSTableMetaHandle sst_meta_hdl;
      if (OB_ISNULL(table_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table ptr should not be null", K(ret));
      } else if (!table_ptr->is_sstable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table ptr type not expectedd", K(ret));
      } else if (FALSE_IT(sstable = static_cast<ObSSTable *>(table_ptr))) {
      } else if (OB_FAIL(sstable->get_meta(sst_meta_hdl))) {
        LOG_WARN("fail to get sstable meta", K(ret));
      } else if (!sst_meta_hdl.get_sstable_meta().contain_uncommitted_row()) { // just skip.
        // ls inner tablet and tablet created by transfer after backfill has no uncommited row.
      } else {
        min_filled_tx_scn = std::min(
          std::max(sst_meta_hdl.get_sstable_meta().get_filled_tx_scn(), sstable->get_end_scn()), min_filled_tx_scn);
      }
    }
    if (OB_SUCC(ret)) {
      can_explain = min_filled_tx_scn >= ls_backup_ctx_->backup_tx_table_filled_tx_scn_;
      if (!can_explain) {
        const ObTabletID &tablet_id = tablet->get_tablet_meta().tablet_id_;
        FLOG_WARN("tx data can't explain user data",
                 K(OB_REPLICA_CANNOT_BACKUP), K(can_explain),
                 K(tablet_id), K(min_filled_tx_scn),
                 "backup_tx_table_filled_tx_scn", ls_backup_ctx_->backup_tx_table_filled_tx_scn_, K(sstable_array));
      } else {
        if (REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
          LOG_INFO("tx data can explain user data", K(ret), K(tablet_handle));
        }
      }
    }
  }
  return ret;
}

int ObBackupTabletProvider::get_consistent_scn_(share::SCN &consistent_scn) const
{
  int ret = OB_SUCCESS;
  ObBackupSetFileDesc backup_set_file;
  if (backup_data_type_.is_sys_backup()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("need not get consistent scn during backup inner tablets", K(ret), K_(param), K_(backup_data_type));
  } else if (OB_FAIL(ObBackupSetFileOperator::get_backup_set_file(
                     *sql_proxy_,
                     false/*for update*/,
                     param_.backup_set_desc_.backup_set_id_,
                     OB_START_INCARNATION,
                     param_.tenant_id_,
                     param_.dest_id_,
                     backup_set_file))) {
    LOG_WARN("failed to get backup set", K(ret), K_(param), K_(backup_data_type));
  } else if (OB_FALSE_IT(consistent_scn = backup_set_file.consistent_scn_)) {
  } else if (!consistent_scn.is_valid_and_not_min()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("consistent scn is not valid", K(ret), K_(param), K_(backup_data_type), K(backup_set_file));
  }

  return ret;
}

int ObBackupTabletProvider::get_tablet_handle_(const uint64_t tenant_id, const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id, ObBackupTabletHandleRef *&tablet_ref, bool &is_split_dst)
{
  int ret = OB_SUCCESS;
  is_split_dst = false;
  tablet_ref = NULL;
  bool hold_tablet_success = false;
  if (OB_INVALID_ID == tenant_id || !ls_id.is_valid() || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tenant_id), K(ls_id), K(tablet_id));
  } else if (OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls backup ctx should not be null", K(ret));
  } else {
    const int64_t rebuild_seq = ls_backup_ctx_->rebuild_seq_;
    MTL_SWITCH(tenant_id) {
      if (OB_FAIL(ObBackupUtils::check_ls_valid_for_backup(tenant_id, ls_id, rebuild_seq))) {
        LOG_WARN("failed to check ls valid for backup", K(ret), K(tenant_id), K(ls_id), K(rebuild_seq));
      } else {
        // sync wait transfer in tablet replace table finish
        const int64_t ABS_TIMEOUT_TS = ObTimeUtil::current_time() + 5 * 60 * 1000 * 1000; //5min
        ObTabletStatus status(ObTabletStatus::MAX);
        while (OB_SUCC(ret)) {
          tablet_ref = NULL;
          if (ABS_TIMEOUT_TS < ObTimeUtil::current_time()) {
            ret = OB_TIMEOUT;
            LOG_WARN("backup get tablet handle timeout", K(ret), K(ls_id), K(tablet_id));
          } else if (OB_FAIL(get_tablet_status_(ls_id, tablet_id, status))) {
            LOG_WARN("failed to check tablet is normal", K(ret), K(tenant_id), K(ls_id), K(rebuild_seq));
          } else if (ObTabletStatus::SPLIT_DST == status) {
            is_split_dst = true;
            ret = OB_TABLET_NOT_EXIST;
            LOG_WARN("tablet is split dst when double check tablet status", K(ret), K(tenant_id), K(ls_id), K(tablet_id), K(status));
          } else if (ObTabletStatus::NORMAL != status && ObTabletStatus::SPLIT_SRC != status) {
            LOG_WARN("tablet status is not normal", K(tenant_id), K(ls_id), K(tablet_id), K(status));
            usleep(100 * 1000); // wait 100ms
          } else if (OB_FAIL(inner_get_tablet_handle_without_memtables_(tenant_id, ls_id, tablet_id, tablet_ref))) { // read readble commited, only get NORMAL and TRANSFER IN tablet.
            LOG_WARN("failed to inner get tablet handle without memtables", K(ret), K(tenant_id), K(ls_id), K(tablet_id));
          } else if (OB_FAIL(ObBackupUtils::check_ls_valid_for_backup(tenant_id, ls_id, rebuild_seq))) {
            LOG_WARN("failed to check ls valid for backup", K(ret), K(tenant_id), K(ls_id), K(rebuild_seq));
          } else if (tablet_id.is_ls_inner_tablet()) {
            // do nothing
            // Data of inner tablets is backed up with meta at the same replica. And.
            // the clog_checkpoint_scn < consistent_scn is allowed.
            break;
          } else if (tablet_ref->tablet_handle_.get_obj()->get_tablet_meta().has_transfer_table()) {
            LOG_INFO("transfer table is not replaced", K(ret), K(tenant_id), K(ls_id), K(tablet_id));
            usleep(100 * 1000); // wait 100ms
          } else if (OB_FAIL(get_tablet_status_(ls_id, tablet_id, status))) {
            LOG_WARN("failed to check tablet is normal", K(ret), K(tenant_id), K(ls_id), K(rebuild_seq));
          } else if (ObTabletStatus::NORMAL != status && ObTabletStatus::SPLIT_SRC != status) {
            if (ObTabletStatus::TRANSFER_OUT == status || ObTabletStatus::TRANSFER_OUT_DELETED == status || ObTabletStatus::DELETED == status
                || ObTabletStatus::SPLIT_SRC_DELETED == status) {
              ret = OB_TABLET_NOT_EXIST;
              LOG_WARN("tablet is deleted when double check tablet status", K(ret), K(tenant_id), K(ls_id), K(tablet_id), K(status));
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("tablet status double check failed", K(ret), K(tenant_id), K(ls_id), K(tablet_id), K(status));
            }
          } else {
            break;
          }
          if (OB_NOT_NULL(tablet_ref)) {
            ls_backup_ctx_->tablet_holder_.free_tablet_ref(tablet_ref);
          }
        }
      }
    }
    if (FAILEDx(hold_tablet_handle_(tablet_id, tablet_ref))) {
      LOG_WARN("failed to hold tablet handle", K(ret), K(tablet_id), KPC(tablet_ref));
    } else {
      hold_tablet_success = true;
    }
  }
  if (OB_NOT_NULL(ls_backup_ctx_) && OB_NOT_NULL(tablet_ref) && !hold_tablet_success) {
    ls_backup_ctx_->tablet_holder_.free_tablet_ref(tablet_ref);
  }
  return ret;
}

int ObBackupTabletProvider::inner_get_tablet_handle_without_memtables_(const uint64_t tenant_id, const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id, ObBackupTabletHandleRef *&tablet_ref)
{
  int ret = OB_SUCCESS;
  tablet_ref = NULL;
  storage::ObLS *ls = NULL;
  ObLSService *ls_service = NULL;
  ObLSHandle handle;
  const WashTabletPriority priority = WashTabletPriority::WTP_LOW;
  const ObTabletMapKey key(ls_id, tablet_id);
  if (OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls backup ctx should not be null", K(ret));
  } else if (OB_FAIL(ls_backup_ctx_->tablet_holder_.alloc_tablet_ref(tenant_id, tablet_ref))) {
    LOG_WARN("failed to alloc tablet ref", K(ret), K(tenant_id));
  } else if (OB_ISNULL(ls_service = MTL_WITH_CHECK_TENANT(ObLSService *, tenant_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream service is NULL", K(ret), K(tenant_id));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log stream not exist", K(ret), K(ls_id));
  } else if (ls->is_stopped()) {
    ret = OB_REPLICA_CANNOT_BACKUP;
    LOG_WARN("ls has stopped, can not backup", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(ls->get_tablet_without_memtables(
      priority, key, tablet_ref->allocator_, tablet_ref->tablet_handle_))) {
    LOG_WARN("failed to alloc tablet handle", K(ret), K(key));
  } else {
    LOG_INFO("get tablet handle without memtables", K(ret), K(ls_id), K(tablet_id));
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(ls_backup_ctx_) && OB_NOT_NULL(tablet_ref)) {
    ls_backup_ctx_->tablet_holder_.free_tablet_ref(tablet_ref);
  }
  return ret;
}

int ObBackupTabletProvider::get_tablet_skipped_type_(const uint64_t tenant_id, const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id, ObBackupSkippedType &skipped_type, share::ObLSID &split_ls_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t tablet_count = 0;
  int64_t tmp_ls_id = 0;
  bool tablet_reorganized = false;
  if (OB_FAIL(share::ObBackupTabletReorganizeHelper::check_tablet_has_reorganized(
      *sql_proxy_, tenant_id, tablet_id, split_ls_id, tablet_reorganized))) {
    LOG_WARN("failed to check tablet has reorganized", K(ret), K(tenant_id), K(tablet_id));
  } else if (tablet_reorganized) {
    LOG_INFO("tablet reorganized", K(tablet_id), K(split_ls_id));
  } else if (OB_FAIL(ObLSBackupOperator::get_tablet_to_ls_info(
      *sql_proxy_, tenant_id, tablet_id, tablet_count, tmp_ls_id))) {
    LOG_WARN("failed to get tablet to ls info", K(ret), K(tenant_id), K(tablet_id));
  }

  if (OB_SUCC(ret)) {
    if (tablet_reorganized) {
      skipped_type = ObBackupSkippedType(ObBackupSkippedType::REORGANIZED);
    } else if (0 == tablet_count) {
      skipped_type = ObBackupSkippedType(ObBackupSkippedType::DELETED);
    } else if (1 == tablet_count) {
      if (tmp_ls_id == ls_id.id()) {
        storage::ObLS *ls = NULL;
        ObLSService *ls_service = NULL;
        ObLSHandle handle;
        if (OB_ISNULL(ls_service = MTL_WITH_CHECK_TENANT(ObLSService *, tenant_id))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("log stream service is NULL", K(ret), K(tenant_id));
        } else if (OB_FAIL(ls_service->get_ls(ls_id, handle, ObLSGetMod::HA_MOD))) {
          LOG_WARN("failed to get log stream", K(ret), K(ls_id));
        } else if (OB_ISNULL(ls = handle.get_ls())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("log stream not exist", K(ret), K(ls_id));
        } else if (ls->is_stopped()) {
          ret = OB_REPLICA_CANNOT_BACKUP;
          LOG_WARN("ls has stopped, can not backup", K(ret), K(tenant_id), K(ls_id));
        } else {
          ret = OB_EAGAIN;
          LOG_WARN("tablet not exist, but __all_tablet_to_ls still exist",
              K(ret), K(tenant_id), K(ls_id), K(tablet_id));
        }
      } else {
        skipped_type = ObBackupSkippedType(ObBackupSkippedType::TRANSFER);
        LOG_INFO("tablet transfered, need change turn", K(ls_id), K(tablet_id));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet count should not greater than 1", K(ret), K(tablet_count));
    }
  }
  return ret;
}

int ObBackupTabletProvider::check_need_report_tablet_skipped_(const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id, const share::ObBackupSkippedType &skipped_type, bool &need_report_skip)
{
  int ret = OB_SUCCESS;
  need_report_skip = false;
  if (ObBackupSkippedType::DELETED == skipped_type.get_type()
      || ObBackupSkippedType::TRANSFER == skipped_type.get_type()) {
    need_report_skip = true;
  } else {
    ObArray<ReorganizeTabletPair> tablet_pairs;
    // TODO(yanfeng): a tablet can be max split into 64K tablet(oracle mode) or 8K tablet(mysql mode)
    // so the time complexity here is K * log N, where K is the max tablet split num, the N is the total tablet count
    // consider using two pointer search algorithm to optimize the search logic
    if (OB_FAIL(ObTabletReorganizeHistoryTableOperator::get_split_tablet_pairs_by_src(
        *sql_proxy_, param_.tenant_id_, ls_id, tablet_id, tablet_pairs))) {
      LOG_WARN("failed to get split tablet pairs", K(ret));
    } else {
      ObArray<ObTabletID> *tablet_id_ptr = &ls_backup_ctx_->data_tablet_id_list_;
      ARRAY_FOREACH_X(tablet_pairs, i, cnt, OB_SUCC(ret)) {
        const ReorganizeTabletPair &pair = tablet_pairs.at(i);
        bool exist = false;
        typedef common::ObArray<ObTabletID>::iterator Iter;
        Iter iter = std::lower_bound(tablet_id_ptr->begin(), tablet_id_ptr->end(), pair.dest_tablet_id_);
        if (iter != tablet_id_ptr->end()) {
          if (*iter != pair.dest_tablet_id_) {
            exist = false;
          } else {
            exist = true;
          }
        } else {
          exist = false;
        }

        if (!exist) {
          need_report_skip = true;
          break;
        }
      }
      LOG_INFO("need report skip", K(ls_id), K(tablet_id), K(need_report_skip), K(tablet_pairs), KPC(tablet_id_ptr));
    }
  }
  return ret;
}

int ObBackupTabletProvider::report_tablet_skipped_(const common::ObTabletID &tablet_id,
    const share::ObBackupSkippedType &skipped_type,
    const share::ObBackupDataType &backup_data_type)
{
  int ret = OB_SUCCESS;
  ObBackupSkippedTablet skipped_tablet;
  skipped_tablet.task_id_ = param_.task_id_;
  skipped_tablet.tenant_id_ = param_.tenant_id_;
  skipped_tablet.turn_id_ = param_.turn_id_;
  skipped_tablet.retry_id_ = param_.retry_id_;
  skipped_tablet.tablet_id_ = tablet_id;
  skipped_tablet.backup_set_id_ = param_.backup_set_desc_.backup_set_id_;
  skipped_tablet.ls_id_ = param_.ls_id_;
  skipped_tablet.skipped_type_ = skipped_type;
  skipped_tablet.data_type_ = backup_data_type;
  if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tablet_id));
  } else if (OB_FAIL(ObLSBackupOperator::report_tablet_skipped(param_.tenant_id_, skipped_tablet, *sql_proxy_))) {
    LOG_WARN("failed to report tablet skipped", K(ret), K_(param), K(tablet_id));
  } else {
    LOG_INFO("report tablet skipping", K(tablet_id));
  }
  return ret;
}

int ObBackupTabletProvider::hold_tablet_handle_(
    const common::ObTabletID &tablet_id, ObBackupTabletHandleRef *tablet_handle)
{
  int ret = OB_SUCCESS;
  if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tablet_id));
  } else if (OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls backup ctx should not be null", K(ret));
  } else if (OB_FAIL(ls_backup_ctx_->set_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("failed to hold tablet", K(ret), K(tablet_id), K(tablet_handle));
  } else {
    LOG_DEBUG("hold tablet handle", K(tablet_id), K(tablet_handle));
  }
  return ret;
}

int ObBackupTabletProvider::fetch_tablet_sstable_array_(const common::ObTabletID &tablet_id,
    const storage::ObTabletHandle &tablet_handle, const ObTabletTableStore &table_store,
    const share::ObBackupDataType &backup_data_type, const bool is_major_compaction_mview_dep_tablet,
    const share::SCN &mview_dep_scn, common::ObIArray<storage::ObSSTableWrapper> &sstable_array)
{
  int ret = OB_SUCCESS;
  sstable_array.reset();
  if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tablet_id));
  } else if (OB_FAIL(ObBackupUtils::get_sstables_by_data_type(tablet_handle, backup_data_type, table_store,
      is_major_compaction_mview_dep_tablet, mview_dep_scn, sstable_array))) {
    LOG_WARN("failed to get sstables by data type", K(ret), K(tablet_handle), K(backup_data_type), K(table_store));
  } else {
    LOG_INFO("fetch tablet sstable array", K(ret), K(tablet_id), K(backup_data_type), K(sstable_array));
  }
  return ret;
}

int ObBackupTabletProvider::prepare_tablet_logic_id_reader_(const common::ObTabletID &tablet_id,
    const storage::ObTabletHandle &tablet_handle, const ObITable::TableKey &table_key,
    const blocksstable::ObSSTable &sstable, ObITabletLogicMacroIdReader *&reader)
{
  int ret = OB_SUCCESS;
  ObITabletLogicMacroIdReader *tmp_reader = NULL;
  const ObTabletLogicIdReaderType type = TABLET_LOGIC_ID_READER;
  if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tablet_id));
  } else if (OB_ISNULL(tmp_reader = ObLSBackupFactory::get_tablet_logic_macro_id_reader(type, param_.tenant_id_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("faild to alloc memory", K(ret));
  } else if (OB_FAIL(tmp_reader->init(tablet_id, tablet_handle, table_key, sstable, BATCH_SIZE))) {
    LOG_WARN("failed to init reader", K(ret), K(tablet_id), K(tablet_handle), K(table_key));
  } else {
    reader = tmp_reader;
    tmp_reader = NULL;
  }
  if (OB_NOT_NULL(tmp_reader)) {
    ObLSBackupFactory::free(tmp_reader);
  }
  return ret;
}

int ObBackupTabletProvider::fetch_all_logic_macro_block_id_(const common::ObTabletID &tablet_id,
    const storage::ObTabletHandle &tablet_handle, const ObITable::TableKey &table_key,
    const blocksstable::ObSSTable &sstable, int64_t &total_count)
{
  int ret = OB_SUCCESS;
  total_count = 0;
  ObITabletLogicMacroIdReader *macro_id_reader = NULL;
  ObArray<ObBackupMacroBlockId> id_array;
  if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tablet_id));
  } else if (OB_FAIL(prepare_tablet_logic_id_reader_(tablet_id, tablet_handle, table_key, sstable, macro_id_reader))) {
    LOG_WARN("failed to prepare tablet logic id reader", K(ret), K(tablet_id), K(tablet_handle), K(table_key));
  } else if (OB_ISNULL(macro_id_reader)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro id reader should not be null", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      id_array.reset();
      if (OB_FAIL(macro_id_reader->get_next_batch(id_array))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get next batch macro block ids", KR(ret));
        }
      }
      if (OB_SUCC(ret)) {
        int64_t added_count = 0;
        if (OB_FAIL(add_macro_block_id_item_list_(tablet_id, table_key, id_array, added_count))) {
          LOG_WARN("failed to add macro block id list", K(ret), K(tablet_id), K(table_key), K(id_array));
        } else if (id_array.count() > 0) {
          total_count += added_count;
        } else {
          break;
        }
      }
    }
  }
  if (OB_NOT_NULL(macro_id_reader)) {
    ObLSBackupFactory::free(macro_id_reader);
  }
  return ret;
}

int ObBackupTabletProvider::fetch_ddl_macro_id_in_ss_mode_(const common::ObTabletID &tablet_id,
    const storage::ObTabletHandle &tablet_handle, const ObITable::TableKey &table_key,
    const blocksstable::ObSSTable &sstable, int64_t &total_count)
{
  int ret = OB_SUCCESS;
  ObBackupOtherBlockIdIterator other_block_iter;
  if (!GCTX.is_shared_storage_mode() && !table_key.is_ddl_dump_sstable()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table key is not ddl dump sstable", K(ret), K(table_key));
  } else if (OB_FAIL(other_block_iter.init(tablet_id, sstable))) {
    LOG_WARN("failed to init other block iter", K(ret), K(tablet_id), K(sstable));
  } else {
    blocksstable::MacroBlockId macro_id;
    ObBackupProviderItem item;
    int64_t local_count = 0;
    while (OB_SUCC(ret)) {
      macro_id.reset();
      if (OB_FAIL(other_block_iter.get_next_id(macro_id))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next id", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(item.set_for_ss_ddl(macro_id, table_key, tablet_id))) {
          LOG_WARN("failed to set for ss ddl", K(ret), K(macro_id), K(table_key), K(tablet_id));
        } else if (!item.is_valid()) {
          ret = OB_INVALID_DATA;
          LOG_WARN("backup item is not valid", K(ret), K(item));
        } else if (OB_FAIL(push_item_to_queue_(item))) {
          LOG_WARN("failed to push item to queue", K(ret), K(item));
        }  else {
          local_count++;
          total_count++;
        }
      }
    }
    if (local_count > 0) {
      LOG_INFO("fetch ddl macro id in ss mode", K(tablet_id), K(table_key), K(local_count));
    }
  }
  return ret;
}

int ObBackupTabletProvider::add_macro_block_id_item_list_(const common::ObTabletID &tablet_id,
    const ObITable::TableKey &table_key, const common::ObIArray<ObBackupMacroBlockId> &list, int64_t &added_count)
{
  int ret = OB_SUCCESS;
  added_count = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < list.count(); ++i) {
    const ObBackupMacroBlockId &macro_id = list.at(i);
    const ObLogicMacroBlockId logic_id = macro_id.logic_id_;
    ObBackupProviderItem item;
    bool need_skip = false;
    share::ObBackupDataType backup_data_type;
    if (OB_FAIL(get_backup_data_type_(table_key, backup_data_type))) {
      LOG_WARN("failed to get backup data type", K(ret), K(table_key));
    } else if (FAILEDx(item.set(PROVIDER_ITEM_MACRO_ID, backup_data_type, macro_id, table_key, tablet_id))) {
      LOG_WARN("failed to set item", K(ret), K(macro_id), K(table_key), K(tablet_id));
    } else if (!item.is_valid()) {
      ret = OB_INVALID_DATA;
      LOG_WARN("backup item is not valid", K(ret), K(item));
    } else if (OB_FAIL(push_item_to_queue_(item))) {
      LOG_WARN("failed to push item to queue", K(ret), K(item));
    } else {
      added_count += 1;
      LOG_INFO("add macro block id", K(tablet_id), K(table_key), K(macro_id));
    }
  }
  return ret;
}

int ObBackupTabletProvider::get_backup_data_type_(
    const storage::ObITable::TableKey &table_key, share::ObBackupDataType &backup_data_type)
{
  int ret = OB_SUCCESS;
  if (backup_data_type_.is_sys_backup()) {
    backup_data_type.set_sys_data_backup();
  } else if (table_key.is_minor_sstable() || table_key.is_ddl_sstable() || table_key.is_mds_sstable()) {
    backup_data_type.set_minor_data_backup();
  } else if (table_key.is_major_sstable()) {
    backup_data_type.set_major_data_backup();
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table key not expected", K(backup_data_type), K(table_key));
  }
  return ret;
}

bool ObBackupTabletProvider::is_same_type_(const storage::ObITable::TableKey &lhs, const storage::ObITable::TableKey &rhs)
{
  bool bret = false;
  if (lhs.is_major_sstable() && rhs.is_major_sstable()) {
    bret = true;
  } else if ((lhs.is_minor_sstable() || lhs.is_ddl_sstable())
      && (rhs.is_minor_sstable() || rhs.is_ddl_sstable())) {
    bret = true;
  } else {
    bret = false;
  }
  return bret;
}

int ObBackupTabletProvider::add_prepare_tablet_item_(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObBackupProviderItem item;
  ObBackupDataType backup_data_type;
  backup_data_type.set_user_data_backup();
  if (OB_FAIL(item.set_with_fake(PROVIDER_ITEM_TABLET_SSTABLE_INDEX_BUILDER_PREPARE, tablet_id, backup_data_type))) {
    LOG_WARN("failed to set item", K(ret), K(tablet_id), K(backup_data_type));
  } else if (OB_FAIL(push_item_to_queue_(item))) {
    LOG_WARN("failed to push item to queue", K(ret), K(item));
  } else {
    LOG_INFO("add tablet item", K(tablet_id), K(backup_data_type));
  }
  return ret;
}

int ObBackupTabletProvider::add_tablet_item_(const common::ObTabletID &tablet_id,
    const bool has_ss_ddl, const storage::ObITable::TableKey &table_key)
{
  int ret = OB_SUCCESS;
  ObBackupProviderItem item;
  ObBackupDataType backup_data_type;
  backup_data_type.set_user_data_backup();
  if (OB_FAIL(item.set_with_fake(PROVIDER_ITEM_TABLET_AND_SSTABLE_META, tablet_id, backup_data_type))) {
    LOG_WARN("failed to set item", K(ret), K(tablet_id), K(backup_data_type));
  } else if (has_ss_ddl && OB_FALSE_IT(item.table_key_ = table_key)) {
  } else if (!item.is_valid()) {
    ret = OB_INVALID_DATA;
    LOG_WARN("backup item is not valid", K(ret), K(item));
  } else if (OB_FAIL(push_item_to_queue_(item))) {
    LOG_WARN("failed to push item to queue", K(ret), K(item));
  } else {
    LOG_INFO("add tablet item", K(tablet_id), K(backup_data_type));
  }
  return ret;
}

int ObBackupTabletProvider::remove_duplicates_(common::ObIArray<ObBackupProviderItem> &array)
{
  int ret = OB_SUCCESS;
  const int64_t count = array.count();
  if (0 == count || 1 == count) {
    // do nothing
  } else {
    ObArray<ObBackupProviderItem> tmp_array;
    int64_t j = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < count - 1; i++) {
      if (array.at(i) != array.at(i + 1)) {
        array.at(j++) = array.at(i);
      }
    }
    array.at(j++) = array.at(count - 1);
    for (int64_t i = 0; OB_SUCC(ret) && i < j; i++) {
      if (OB_FAIL(tmp_array.push_back(array.at(i)))) {
        LOG_WARN("failed to push back", K(ret), K(i), K(j));
      }
    }
    if (OB_SUCC(ret)) {
      array.reset();
      if (OB_FAIL(array.assign(tmp_array))) {
        LOG_WARN("failed to assign", K(ret));
      }
    }
  }
  return ret;
}

int ObBackupTabletProvider::check_tablet_split_status_(const uint64_t tenant_id,
    const common::ObTabletID &tablet_id, bool &need_skip_tablet)
{
  int ret = OB_SUCCESS;
  need_skip_tablet = false;
  ReorganizeTabletPair pair;
  if (OB_FAIL(ObTabletReorganizeHistoryTableOperator::get_split_tablet_pairs_by_dest(
      *sql_proxy_, tenant_id, tablet_id, pair))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get split tablet pair by dest", K(ret), K(tablet_id));
    }
  } else {
    const common::ObTabletID &src_tablet_id = pair.src_tablet_id_;
    bool has_skipped = false;
    if (OB_FAIL(ObLSBackupOperator::check_tablet_skipped_by_reorganize(*sql_proxy_, tenant_id, src_tablet_id, has_skipped))) {
      LOG_WARN("failed to check tablet skipped by reorganize", K(ret), K(src_tablet_id));
    } else if (has_skipped) {
      LOG_INFO("skip check tablet split status", K(ret), K(pair));
    } else {
      ObArray<ObTabletID> *tablet_id_ptr = &ls_backup_ctx_->data_tablet_id_list_;
      bool src_exist = false;
      ARRAY_FOREACH_X(*tablet_id_ptr, i, cnt, OB_SUCC(ret)) {
        if (tablet_id_ptr->at(i) == src_tablet_id) {
          src_exist = true;
          break;
        }
      }
      LOG_INFO("tablet not skipped", K(*tablet_id_ptr), K(src_tablet_id), K(src_exist));
      if (OB_SUCC(ret) && src_exist) {
        ObBackupSkippedType skipped_type(ObBackupSkippedType::REORGANIZED);
        ObBackupDataType backup_data_type;
        backup_data_type.set_user_data_backup();
        if (OB_FAIL(report_tablet_skipped_(tablet_id, skipped_type, backup_data_type))) {
          LOG_WARN("failed to report tablet skipped", K(ret));
        } else {
          need_skip_tablet = true;
          LOG_INFO("report skip tablet", K(tablet_id), K(skipped_type));
        }
      }
    }
  }
  return ret;
}

int ObBackupTabletProvider::get_tenant_meta_index_turn_id_(int64_t &turn_id)
{
  int ret = OB_SUCCESS;
  ObBackupSetTaskAttr set_task_attr;
  if (OB_FAIL(share::ObBackupTaskOperator::get_backup_task(*GCTX.sql_proxy_, param_.job_id_, param_.tenant_id_, false, set_task_attr))) {
    LOG_WARN("failed to get backup task", K(ret));
  } else {
    turn_id = set_task_attr.minor_turn_id_;
  }
  return ret;
}

int ObBackupTabletProvider::get_tenant_meta_index_retry_id_(
    const share::ObBackupDataType &backup_data_type, const int64_t turn_id, int64_t &retry_id)
{
  int ret = OB_SUCCESS;
  const bool is_restore = false;
  const bool is_macro_index = false;
  const bool is_sec_meta = false;
  ObBackupTenantIndexRetryIDGetter retry_id_getter;
  if (OB_FAIL(retry_id_getter.init(param_.backup_dest_, param_.backup_set_desc_,
      backup_data_type, turn_id, is_restore, is_macro_index, is_sec_meta))) {
    LOG_WARN("failed to init retry id getter", K(ret), K(turn_id), K_(param));
  } else if (OB_FAIL(retry_id_getter.get_max_retry_id(retry_id))) {
    LOG_WARN("failed to get max retry id", K(ret));
  }
  return ret;
}

int ObBackupTabletProvider::check_tablet_replica_validity_(const uint64_t tenant_id, const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id, const share::ObBackupDataType &backup_data_type)
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  if (!backup_data_type.is_user_backup()) {
    // do nothing
  } else if (OB_ISNULL(sql_proxy_) || OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy should not be null", K(ret), KP_(sql_proxy), KP_(ls_backup_ctx));
  } else if (OB_INVALID_ID == tenant_id || !ls_id.is_valid() || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(tenant_id), K(ls_id), K(tablet_id));
  } else {
    const common::ObAddr &src_addr = GCTX.self_addr();
    if (OB_FAIL(ObStorageHAUtils::check_tablet_replica_validity(tenant_id, ls_id, src_addr, tablet_id, *sql_proxy_))) {
      LOG_WARN("failed to check tablet replica validity", K(ret), K(tenant_id), K(ls_id), K(src_addr), K(tablet_id));
    } else {
      ls_backup_ctx_->check_tablet_info_cost_time_ += ObTimeUtility::current_time() - start_ts;
    }
  }
  return ret;
}

int ObBackupTabletProvider::push_item_to_queue_(const ObBackupProviderItem &item)
{
  int ret = OB_SUCCESS;
  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(item));
  } else if (OB_FAIL(item_queue_.put_item(item))) {
    LOG_WARN("failed to push back", K(ret));
  }
  return ret;
}

int ObBackupTabletProvider::pop_item_from_queue_(ObBackupProviderItem &item)
{
  int ret = OB_SUCCESS;
  item.reset();
  if (OB_FAIL(item_queue_.get_item(item))) {
    LOG_WARN("failed to get item", K(ret));
  }
  return ret;
}

int ObBackupTabletProvider::get_tablet_status_(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    ObTabletStatus &status)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObTabletCreateDeleteMdsUserData user_data;
  if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ls_service", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id));
  } else if (!ls_handle.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls_handle is in_valid", K(ret), K(ls_handle));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->get_tablet(tablet_id, tablet_handle, 0 /*timeout*/, storage::ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_id), K(tablet_handle));
  } else if (!tablet_handle.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table handle is in_valid", K(ret), K(tablet_handle));
  } else if (OB_FAIL(tablet_handle.get_obj()->ObITabletMdsInterface::get_tablet_status(share::SCN::max_scn(), user_data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US))) {
    LOG_WARN("failed to get tablet status", KPC(tablet_handle.get_obj()));
  } else {
    status = user_data.get_tablet_status();
  }
  return ret;
}

void ObBackupTabletProvider::free_queue_item_()
{
  item_queue_.reset();
}

/* ObBackupMacroBlockTaskMgr */

ObBackupMacroBlockTaskMgr::ObBackupMacroBlockTaskMgr()
    : is_inited_(false),
      backup_data_type_(),
      batch_size_(0),
      mutex_(common::ObLatchIds::BACKUP_LOCK),
      cond_(),
      max_task_id_(0),
      file_id_(0),
      cur_task_id_(0),
      pending_list_(),
      ready_list_(),
      ls_backup_ctx_(NULL)
{}

ObBackupMacroBlockTaskMgr::~ObBackupMacroBlockTaskMgr()
{
  reset();
}

int ObBackupMacroBlockTaskMgr::init(const share::ObBackupDataType &backup_data_type, const int64_t batch_size,
    ObLSBackupCtx &ls_backup_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("task mgr init twice", K(ret));
  } else if (batch_size < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(batch_size));
} else if (OB_FAIL(cond_.init(ObWaitEventIds::BACKUP_MACRO_BLOCK_TASK_COND_WAIT))) {
    LOG_WARN("failed to init condition", K(ret));
  } else {
    backup_data_type_ = backup_data_type;
    batch_size_ = batch_size;
#ifdef ERRSIM
    if (0 != GCONF.errsim_backup_task_batch_size) {
      batch_size_ = GCONF.errsim_backup_task_batch_size;
    }
#endif
    max_task_id_ = 0;
    cur_task_id_ = 0;
    ls_backup_ctx_ = &ls_backup_ctx;
    is_inited_ = true;
  }
  return ret;
}

void ObBackupMacroBlockTaskMgr::set_backup_data_type(const share::ObBackupDataType &backup_data_type)
{
  ObMutexGuard guard(mutex_);
  backup_data_type_ = backup_data_type;
}

ObBackupDataType ObBackupMacroBlockTaskMgr::get_backup_data_type() const
{
  ObMutexGuard guard(mutex_);
  return backup_data_type_;
}

int ObBackupMacroBlockTaskMgr::receive(const int64_t task_id, const common::ObIArray<ObBackupProviderItem> &id_list)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("mgr do not init", K(ret));
  } else if (OB_FAIL(wait_task_(task_id))) {
    LOG_WARN("failed to wait task", K(ret), K(task_id));
  } else if (OB_FAIL(put_to_pending_list_(id_list))) {
    LOG_WARN("failed to put to pending list", K(ret), K(task_id), K(id_list));
  } else if (OB_FAIL(finish_task_(task_id))) {
    LOG_WARN("failed to finish task", K(ret), K(task_id));
  } else {
    LOG_INFO("receive id list", K(task_id), K(id_list.count()));
    max_task_id_ = std::max(max_task_id_, task_id);
  }
  return ret;
}

int ObBackupMacroBlockTaskMgr::deliver(common::ObIArray<ObBackupProviderItem> &id_list, int64_t &file_id)
{
  int ret = OB_SUCCESS;
  id_list.reset();
  ObThreadCondGuard guard(cond_);
  int64_t begin_ms = ObTimeUtility::fast_current_time();
  while (OB_SUCC(ret) && id_list.empty()) {
    bool has_enough = false;
    if (OB_ISNULL(ls_backup_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls backup ctx should not be null", K(ret));
    } else if (OB_SUCCESS != ls_backup_ctx_->get_result_code()) {
      ret = ls_backup_ctx_->get_result_code();
      LOG_INFO("ls backup ctx already failed", K(ret));
      break;
    } else if (OB_FAIL(get_from_ready_list_(id_list))) {
      LOG_WARN("failed to get from ready list", K(ret));
    } else if (!id_list.empty()) {
      break;
    } else if (OB_FAIL(try_peeking_enough_non_reused_items_(has_enough))) {
      LOG_WARN("failed to try peeking enough non reused items", K(ret));
    } else if (has_enough) {
      if (OB_FAIL(get_from_ready_list_(id_list))) {
        LOG_WARN("failed to get from ready list", K(ret));
      } else {
        break;
      }
    } else {
      if (OB_FAIL(cond_.wait(DEFAULT_WAIT_TIME_MS))) {
        int64_t duration_ms = ObTimeUtility::fast_current_time() - begin_ms;
        if (OB_TIMEOUT == ret) {
          LOG_WARN("waiting for task too slow", K(ret));
          ret = OB_SUCCESS;
        }
        if (duration_ms >= DEFAULT_WAIT_TIMEOUT_MS) {
          if (OB_FAIL(get_from_ready_list_(id_list))) {
            LOG_WARN("failed to get from ready list", K(ret));
          } else if (id_list.empty()) {
            ret = OB_EAGAIN;
          }
          break;
        }
      }
    }
  }
  if (OB_SUCC(ret) && !id_list.empty()) {
    file_id = file_id_;
    ++file_id_;
  }
  return ret;
}

int64_t ObBackupMacroBlockTaskMgr::get_pending_count() const
{
  ObMutexGuard guard(mutex_);
  return pending_list_.count();
}

int64_t ObBackupMacroBlockTaskMgr::get_ready_count() const
{
  ObMutexGuard guard(mutex_);
  return ready_list_.count();
}

bool ObBackupMacroBlockTaskMgr::has_remain() const
{
  ObMutexGuard guard(mutex_);
  return !pending_list_.empty() || !ready_list_.empty();
}

void ObBackupMacroBlockTaskMgr::reset()
{
  ObMutexGuard guard(mutex_);
  is_inited_ = false;
  cond_.destroy();
  max_task_id_ = 0;
  cur_task_id_ = 0;
  pending_list_.reset();
  ready_list_.reset();
}

void ObBackupMacroBlockTaskMgr::reuse()
{
  ObMutexGuard guard(mutex_);
  max_task_id_ = 0;
  file_id_ = 0;
  cur_task_id_ = 0;
  pending_list_.reset();
  ready_list_.reset();
}

int ObBackupMacroBlockTaskMgr::wait_task_(const int64_t task_id)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret) && task_id != ATOMIC_LOAD(&cur_task_id_)) {
    ObThreadCondGuard guard(cond_);
    if (OB_ISNULL(ls_backup_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls backup ctx should not be null", K(ret));
    } else if (OB_SUCCESS != ls_backup_ctx_->get_result_code()) {
      ret = ls_backup_ctx_->get_result_code();
      LOG_INFO("ls backup ctx already failed", K(ret));
      break;
    } else if (OB_FAIL(cond_.wait(DEFAULT_WAIT_TIME_MS))) {
      if (OB_TIMEOUT == ret) {
        ret = OB_SUCCESS;
        LOG_WARN("waiting for task too slow", K(ret), K(task_id), K(cur_task_id_));
      }
    }
  }
  return ret;
}

int ObBackupMacroBlockTaskMgr::try_peeking_enough_non_reused_items_(bool &has_enough)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  if (!all_item_is_reused(ready_list_)) {
    has_enough = true;
  } else {
    has_enough = false;
  }
  return ret;
}

int ObBackupMacroBlockTaskMgr::finish_task_(const int64_t task_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(task_idx != ATOMIC_LOAD(&cur_task_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("finish task order unexpected", K(ret), K(task_idx), K(cur_task_id_));
  } else {
    ObThreadCondGuard guard(cond_);
    ATOMIC_INC(&cur_task_id_);
    if (OB_FAIL(cond_.broadcast())) {
      LOG_ERROR("failed to broadcast condition", K(ret));
    }
  }
  return ret;
}

int ObBackupMacroBlockTaskMgr::transfer_list_without_lock_()
{
  int ret = OB_SUCCESS;
  if (ready_list_.count() > 0) {
    LOG_INFO("no need to transfer", K(ready_list_.count()), K(pending_list_.count()), K_(batch_size));
  } else {
    ready_list_.reset();
    ObArray<ObBackupProviderItem> tmp_pending_list;
    for (int64_t i = 0; OB_SUCC(ret) && i < pending_list_.count(); ++i) {
      const ObBackupProviderItem &item = pending_list_.at(i);
      if (i < batch_size_) {
        if (OB_FAIL(ready_list_.push_back(item))) {
          LOG_WARN("failed to push back", K(ret), K(item));
        }
      } else {
        if (OB_FAIL(tmp_pending_list.push_back(item))) {
          LOG_WARN("failed to push back", K(ret), K(item));
        }
      }
    }
    if (OB_SUCC(ret)) {
      pending_list_.reset();
      if (OB_FAIL(pending_list_.assign(tmp_pending_list))) {
        LOG_WARN("failed to push back", K(ret));
      } else {
        LOG_INFO("remaining pending count", K(pending_list_.count()));
      }
    }
  }
  return ret;
}

int ObBackupMacroBlockTaskMgr::get_from_ready_list_(common::ObIArray<ObBackupProviderItem> &list)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  list.reset();
  if (ready_list_.empty()) {
    if (OB_FAIL(transfer_list_without_lock_())) {
      LOG_WARN("failed to transfer list without lock", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(list.assign(ready_list_))) {
      LOG_WARN("failed to assign list", K(ret));
    } else {
      ready_list_.reset();
    }
  }
  return ret;
}

int ObBackupMacroBlockTaskMgr::put_to_pending_list_(const common::ObIArray<ObBackupProviderItem> &list)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  for (int64_t i = 0; OB_SUCC(ret) && i < list.count(); ++i) {
    const ObBackupProviderItem &item = list.at(i);
    if (!item.is_valid()) {
      ret = OB_INVALID_DATA;
      LOG_WARN("backup item is not valid", K(ret), K(i), K(item), "count", list.count());
    } else if (OB_FAIL(pending_list_.push_back(item))) {
      LOG_WARN("failed to push back", K(ret), K(i), K(item));
    }
  }
  if (OB_SUCC(ret)) {
    if (pending_list_.count() >= batch_size_ && 0 == ready_list_.count()) {
      if (OB_FAIL(transfer_list_without_lock_())) {
        LOG_WARN("failed to transfer list without lock", K(ret));
      }
    }
  }
  return ret;
}

bool ObBackupMacroBlockTaskMgr::all_item_is_reused(
     const common::ObIArray<ObBackupProviderItem> &list) const
{
  int ret = OB_SUCCESS;
  bool all_no_need_copy = true;
  ARRAY_FOREACH_N(list, idx, cnt) {
    if (list.at(idx).get_need_copy()) {
      all_no_need_copy = false;
      break;
    }
  }
  return all_no_need_copy;
}

OB_SERIALIZE_MEMBER(ObBackupProviderItem, item_type_, backup_data_type_, logic_id_, macro_block_id_, table_key_, tablet_id_,
  nested_offset_, nested_size_, timestamp_, need_copy_, macro_index_, absolute_row_offset_);

}  // namespace backup
}  // namespace oceanbase
