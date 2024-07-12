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

#include "storage/backup/ob_backup_utils.h"
#include "lib/container/ob_iarray.h"
#include "lib/lock/ob_mutex.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/time/ob_time_utility.h"
#include "share/rc/ob_tenant_base.h"
#include "share/backup/ob_archive_struct.h"
#include "storage/backup/ob_backup_factory.h"
#include "storage/backup/ob_backup_operator.h"
#include "storage/backup/ob_backup_reader.h"
#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tablet/ob_tablet_meta.h"
#include "storage/tablet/ob_tablet_table_store.h"
#include "storage/tablet/ob_tablet_create_delete_mds_user_data.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/high_availability/ob_storage_ha_utils.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "share/scn.h"
#include "share/backup/ob_backup_data_table_operator.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#include "storage/column_store/ob_column_oriented_sstable.h"
#include "storage/backup/ob_backup_data_store.h"
#include "storage/ddl/ob_ddl_merge_task.h"
#include "storage/tablet/ob_mds_schema_helper.h"

#include <algorithm>

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
    const storage::ObTabletTableStore &tablet_table_store, common::ObIArray<storage::ObSSTableWrapper> &sstable_array)
{
  int ret = OB_SUCCESS;
  sstable_array.reset();
  if (!backup_data_type.is_valid() || !tablet_table_store.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(backup_data_type), K(tablet_table_store));
  } else if (backup_data_type.is_sys_backup()) {
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
  } else if (backup_data_type.is_minor_backup()) {
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
  } else if (backup_data_type.is_major_backup()) {
    const storage::ObSSTableArray *major_sstable_array_ptr = NULL;
    major_sstable_array_ptr = &tablet_table_store.get_major_sstables();
    ObITable *last_major_sstable_ptr = NULL;
    ObArenaAllocator tmp_allocator("backup_medium", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    bool with_major_sstable = true;
    const share::ObLSID &ls_id = tablet_handle.get_obj()->get_ls_id();
    const common::ObTabletID &tablet_id = tablet_handle.get_obj()->get_tablet_id();
    const int64_t last_compaction_scn = tablet_handle.get_obj()->get_last_compaction_scn();
    ObSSTableWrapper major_wrapper;

    if (OB_ISNULL(last_major_sstable_ptr = major_sstable_array_ptr->get_boundary_table(true /*last*/))) {
      if (OB_FAIL(check_tablet_with_major_sstable(tablet_handle, with_major_sstable))) {
        LOG_WARN("failed to check tablet with major sstable", K(ret));
      }
      if (OB_SUCC(ret) && with_major_sstable) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("last major sstable should not be null", K(ret), K(tablet_handle));
      }
    } else if (OB_UNLIKELY(last_compaction_scn > 0
        && last_compaction_scn != last_major_sstable_ptr->get_snapshot_version())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("extra medium info is invalid for last major sstable", K(ret), K(ls_id), K(tablet_id),
          K(last_compaction_scn), KPC(last_major_sstable_ptr), K(tablet_handle));
    } else if (last_major_sstable_ptr->is_co_sstable()) {
      if (OB_FAIL(static_cast<ObCOSSTableV2 *>(last_major_sstable_ptr)->get_all_tables(sstable_array))) {
        LOG_WARN("failed to get all cg tables from co table", K(ret), KPC(last_major_sstable_ptr));
      }
    } else if (OB_FAIL(major_wrapper.set_sstable(static_cast<ObSSTable *>(last_major_sstable_ptr)))) {
      LOG_WARN("failed to set major wrapper", K(ret), KPC(last_major_sstable_ptr));
    } else if (OB_FAIL(sstable_array.push_back(major_wrapper))) {
      LOG_WARN("failed to push back", K(ret), K(major_wrapper));
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
      total_sstable_meta_count_(0),
      reused_macro_block_count_(0),
      total_macro_block_count_(0),
      total_check_count_(0),
      finish_tablet_meta_count_(0),
      finish_sstable_meta_count_(0),
      finish_macro_block_count_(0),
      finish_check_count_(0),
      is_all_loaded_(false),
      mappings_()
{}

ObBackupTabletCtx::~ObBackupTabletCtx()
{}

void ObBackupTabletCtx::reuse()
{
  total_tablet_meta_count_ = 0;
  total_sstable_meta_count_ = 0;
  reused_macro_block_count_ = 0;
  total_macro_block_count_ = 0;
  total_check_count_ = 0;
  finish_tablet_meta_count_ = 0;
  finish_sstable_meta_count_ = 0;
  finish_macro_block_count_ = 0;
  finish_check_count_ = 0;
  is_all_loaded_ = false;
  mappings_.reuse();
}

void ObBackupTabletCtx::print_ctx()
{
  LOG_INFO("print ctx", K_(total_tablet_meta_count), K_(finish_tablet_meta_count), K_(total_sstable_meta_count),
      K_(finish_sstable_meta_count), K_(reused_macro_block_count), K_(total_macro_block_count),
      K_(finish_macro_block_count), K_(is_all_loaded), "sstable_count", mappings_.sstable_count_);
  for (int64_t i = 0; i < mappings_.id_map_list_.count(); ++i) {
    ObBackupMacroBlockIDMapping *mapping = mappings_.id_map_list_[i];
    LOG_INFO("print backup macro block id mapping", K(i), KPC(mapping));
  }
}

int ObBackupTabletCtx::record_macro_block_physical_id(const storage::ObITable::TableKey &table_key,
    const blocksstable::ObLogicMacroBlockId &logic_id, const ObBackupPhysicalID &physical_id)
{
  int ret = OB_SUCCESS;
  bool found = false;
  if (!table_key.is_valid() || !logic_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(table_key), K(logic_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < mappings_.sstable_count_; ++i) {
    ObBackupMacroBlockIDMapping *mapping = mappings_.id_map_list_[i];
    if (OB_ISNULL(mapping)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("id mapping should not be null", K(ret), K(i), KPC(mapping));
    } else if (mapping->table_key_ == table_key) {
      found = true;
      int64_t idx = 0;
      if (!mapping->map_.created()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("id map not created", K(ret), K(i));
      } else if (OB_FAIL(mapping->map_.get_refactored(logic_id, idx))) {
        LOG_WARN("failed to get refactored", K(ret), K(i), K(logic_id));
      } else if (idx >= mapping->id_pair_list_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("idx out of range", K(ret), K(idx), K(i), "array_count", mapping->id_pair_list_.count());
      } else {
        ObBackupMacroBlockIDPair pair;
        pair.logic_id_ = logic_id;
        pair.physical_id_ = physical_id;
        mapping->id_pair_list_.at(idx) = pair;
        LOG_INFO("record macro block id", K(table_key), K(logic_id), K(physical_id));
      }
    }
  }
  if (OB_SUCC(ret) && !found) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table key do not exist",
        K(ret),
        K(table_key),
        "sstable_count",
        mappings_.sstable_count_,
        K(logic_id),
        K(physical_id));
  }
  return ret;
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
    const common::ObIArray<storage::ObSSTableWrapper> &sstable_array)
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
  } else if (FALSE_IT(stat->mappings_.version_ = ObBackupMacroBlockIDMappingsMeta::MAPPING_META_VERSION_V1)) {
  } else if (OB_FAIL(stat->mappings_.prepare_id_mappings(sstable_array.count()))) {
    LOG_WARN("failed to prepare id mappings", K(ret), K(sstable_array.count()), K(stat->mappings_));
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
      // TODO(COLUMN_STORE) Attention !!! MajorSSTable in column store canbe COSSTable, maybe should adapt here.
      const ObITable::TableKey &table_key = sstable_ptr->get_key();
      ObBackupMacroBlockIDMapping *id_mapping = stat->mappings_.id_map_list_[i];
      common::ObArray<ObLogicMacroBlockId> logic_id_list;
      if (OB_FAIL(ObBackupUtils::fetch_macro_block_logic_id_list(tablet_handle, *sstable_ptr, logic_id_list))) {
        LOG_WARN("failed to fetch macro block logic id list", K(ret), K(tablet_handle), KPC(sstable_ptr));
      } else if (OB_FAIL(id_mapping->prepare_tablet_sstable(tenant_id, table_key, logic_id_list))) {
        LOG_WARN("failed to prepare tablet sstable", K(ret), K(table_key), K(logic_id_list));
      } else {
        LOG_INFO("prepare tablet sstable", K(backup_data_type), K(tablet_id), K(table_key), K(logic_id_list));
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
    const common::ObIArray<ObBackupProviderItem> &items, common::ObIArray<ObBackupPhysicalID> &physical_ids)
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
      const ObBackupPhysicalID &physical_id = physical_ids.at(i);
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
  macro_id.macro_block_id_ = ObBackupProviderItem::get_fake_macro_id_();
  macro_id.logic_id_ = id_pair.logic_id_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup tablet stat do not init", K(ret));
  } else if (backup_data_type.type_ != backup_data_type_.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup data type not match", K(backup_data_type), K(backup_data_type_));
  } else if (OB_FAIL(item.set(item_type, macro_id, table_key, ObTabletID(id_pair.logic_id_.tablet_id_)))) {
    LOG_WARN("failed to set provider item", K(ret), K(item));
  } else if (OB_FAIL(do_with_stat_when_reused_(item, id_pair.physical_id_))) {
    LOG_WARN("failed to do with stat when reused", K(ret));
  }
  return ret;
}

int ObBackupTabletStat::mark_item_finished(const share::ObBackupDataType &backup_data_type,
    const ObBackupProviderItem &item, const ObBackupPhysicalID &physical_id, bool &is_all_finished)
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
  } else if (OB_FAIL(check_tablet_finished_(tablet_id, is_all_finished))) {
    LOG_WARN("failed to check tablet finished", K(ret), K(tablet_id));
  } else {
    LOG_INFO("mark backup item finished", K(backup_data_type), K(item), K(physical_id));
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
    if (PROVIDER_ITEM_CHECK == type) {
      ++stat->total_check_count_;
    } else if (PROVIDER_ITEM_MACRO_ID == type) {
      const ObITable::TableKey &table_key = item.get_table_key();
      const ObLogicMacroBlockId &logic_id = item.get_logic_id();
      const ObBackupPhysicalID &fake_physical_id = ObBackupPhysicalID::get_default();
      if (OB_FAIL(stat->record_macro_block_physical_id(table_key, logic_id, fake_physical_id))) {
        LOG_WARN("failed to record macro block physical id", K(ret), K(table_key), K(logic_id), K(fake_physical_id));
      } else {
        ++stat->total_macro_block_count_;
      }
    } else if (PROVIDER_ITEM_SSTABLE_META == type) {
      ++stat->total_sstable_meta_count_;
    } else if (PROVIDER_ITEM_TABLET_META == type) {
      ++stat->total_tablet_meta_count_;
      stat->is_all_loaded_ = true;
    }
  }
  return ret;
}

int ObBackupTabletStat::do_with_stat_when_reused_(
    const ObBackupProviderItem &item, const ObBackupPhysicalID &physical_id)
{
  int ret = OB_SUCCESS;
  ObBackupTabletCtx *stat = NULL;
  const bool create_if_not_exist = false;
  const ObTabletID &tablet_id = item.get_tablet_id();
  const ObITable::TableKey &table_key = item.get_table_key();
  const ObLogicMacroBlockId &logic_id = item.get_logic_id();
  const ObBackupProviderItemType type = item.get_item_type();
  if (!item.is_valid() || !physical_id.is_valid()) {
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
  } else if (OB_FAIL(stat->record_macro_block_physical_id(table_key, logic_id, physical_id))) {
    LOG_WARN("failed to record macro block physical id", K(ret), K(table_key), K(logic_id), K(physical_id));
  } else {
    ++stat->reused_macro_block_count_;
    ++stat->finish_macro_block_count_;
    ++stat->total_macro_block_count_;
  }
  return ret;
}

int ObBackupTabletStat::do_with_stat_when_finish_(
    const ObBackupProviderItem &item, const ObBackupPhysicalID &physical_id)
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
    if (PROVIDER_ITEM_CHECK == type) {
      ++stat->finish_check_count_;
    } else if (PROVIDER_ITEM_MACRO_ID == type) {
      const ObITable::TableKey &table_key = item.get_table_key();
      const ObLogicMacroBlockId &logic_id = item.get_logic_id();
      if (OB_FAIL(stat->record_macro_block_physical_id(table_key, logic_id, physical_id))) {
        LOG_WARN("failed to record macro block physical id", K(ret), K(table_key), K(logic_id), K(physical_id));
      } else {
        ++stat->finish_macro_block_count_;
      }
    } else if (PROVIDER_ITEM_SSTABLE_META == type) {
      ++stat->finish_sstable_meta_count_;
    } else if (PROVIDER_ITEM_TABLET_META == type) {
      ++stat->finish_tablet_meta_count_;
    }
  }
  return ret;
}

int ObBackupTabletStat::check_tablet_finished_(const common::ObTabletID &tablet_id, bool &is_finished)
{
  int ret = OB_SUCCESS;
  is_finished = false;
  ObBackupTabletCtx *stat = NULL;
  const bool create_if_not_exist = false;
  if (OB_FAIL(get_tablet_stat_(tablet_id, create_if_not_exist, stat))) {
    LOG_WARN("failed to get tablet stat", K(ret), K(stat));
  } else if (OB_ISNULL(stat)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stat should not be null", K(ret), K_(backup_data_type), K(tablet_id));
  } else if (!stat->is_all_loaded_) {
    // not all loaded, false
  } else {
    is_finished = stat->total_macro_block_count_ == stat->finish_macro_block_count_ &&
                  stat->total_sstable_meta_count_ == stat->finish_sstable_meta_count_ &&
                  stat->total_tablet_meta_count_ == stat->finish_tablet_meta_count_ &&
                  stat->total_check_count_ == stat->finish_check_count_;
    if (is_finished) {
      report_event_(tablet_id, *stat);
      LOG_INFO("tablet is finished", K(tablet_id), KPC(stat));
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
      } else {
        stat->mappings_.version_ = ObBackupMacroBlockIDMappingsMeta::MAPPING_META_VERSION_V1;
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
  } else if (backup_data_type_.is_minor_backup()) {
    backup_event = "backup_minor_tablet";
  } else if (backup_data_type_.is_major_backup()) {
    backup_event = "backup_major_tablet";
  }
  SERVER_EVENT_ADD("backup",
      backup_event,
      "tenant_id",
      tenant_id_,
      "backup_set_id",
      backup_set_id_,
      "ls_id",
      ls_id_.id(),
      "tablet_id",
      tablet_id.id(),
      "finished_macro_block_count",
      tablet_ctx.finish_macro_block_count_,
      "reused_macro_block_count",
      tablet_ctx.reused_macro_block_count_,
      tablet_ctx.total_macro_block_count_);
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
  } else if (OB_FAIL(THE_IO_DEVICE->check_space_full(required_size))) {
    LOG_WARN("failed to check space full", K(ret));
  }
  return ret;
}

/* ObBackupProviderItem */

ObBackupProviderItem::ObBackupProviderItem()
    : item_type_(PROVIDER_ITEM_MAX), logic_id_(),
      macro_block_id_(), table_key_(), tablet_id_(),
      nested_offset_(0), nested_size_(0)
{}

ObBackupProviderItem::~ObBackupProviderItem()
{}

int ObBackupProviderItem::set_with_fake(const ObBackupProviderItemType &item_type, const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (PROVIDER_ITEM_SSTABLE_META != item_type && PROVIDER_ITEM_TABLET_META != item_type
      && PROVIDER_ITEM_CHECK != item_type) {
    ret = OB_ERR_SYS;
    LOG_WARN("get invalid args", K(ret));
  } else if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tablet_id));
  } else {
    item_type_ = item_type;
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

int ObBackupProviderItem::set(const ObBackupProviderItemType &item_type, const ObBackupMacroBlockId &macro_id,
     const ObITable::TableKey &table_key, const common::ObTabletID &tablet_id)
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
    logic_id_ = macro_id.logic_id_;
    macro_block_id_ = macro_id.macro_block_id_;
    table_key_ = table_key;
    tablet_id_ = tablet_id;
    nested_offset_ = macro_id.nested_offset_;
    nested_size_ = macro_id.nested_size_;
    if (!is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("provider item not valid", K(ret), KPC(this));
    }
  }
  return ret;
}

bool ObBackupProviderItem::operator==(const ObBackupProviderItem &other) const
{
  return item_type_ == other.item_type_ && logic_id_ == other.logic_id_ &&
         macro_block_id_ == other.macro_block_id_ && table_key_ == other.table_key_ &&
         tablet_id_ == other.tablet_id_ && nested_size_ == other.nested_size_ && nested_offset_ == other.nested_offset_;
}

bool ObBackupProviderItem::operator!=(const ObBackupProviderItem &other) const
{
  return !(*this == other);
}

ObBackupProviderItemType ObBackupProviderItem::get_item_type() const
{
  return item_type_;
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

int ObBackupProviderItem::deep_copy(const ObBackupProviderItem &src, char *buf, int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  UNUSEDx(buf, len, pos);
  item_type_ = src.item_type_;
  logic_id_ = src.logic_id_;
  macro_block_id_ = src.macro_block_id_;
  table_key_ = src.table_key_;
  tablet_id_ = src.tablet_id_;
  nested_offset_ = src.nested_offset_;
  nested_size_ = src.nested_size_;
  return ret;
}

bool ObBackupProviderItem::is_valid() const
{
  bool bret = false;
  if (PROVIDER_ITEM_MACRO_ID != item_type_
      && PROVIDER_ITEM_SSTABLE_META != item_type_
      && PROVIDER_ITEM_TABLET_META != item_type_
      && PROVIDER_ITEM_CHECK != item_type_) {
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
  logic_id_.reset();
  macro_block_id_.reset();
  table_key_.reset();
  tablet_id_.reset();
  nested_offset_ = 0;
  nested_size_ = 0;
}

DEFINE_SERIALIZE(ObBackupProviderItem)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, item_type_))) {
    LOG_WARN("failed to encode key", K(ret));
  } else if (OB_FAIL(logic_id_.serialize(buf, buf_len, pos))) {
    LOG_WARN("failed to serialize logic id", K(ret));
  } else if (OB_FAIL(macro_block_id_.serialize(buf, buf_len, pos))) {
    LOG_WARN("failed to serialize macro block id", K(ret));
  } else if (OB_FAIL(table_key_.serialize(buf, buf_len, pos))) {
    LOG_WARN("failed to serialize table key", K(ret));
  } else if (OB_FAIL(tablet_id_.serialize(buf, buf_len, pos))) {
    LOG_WARN("failed to serialize tablet id", K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, nested_offset_))) {
    LOG_WARN("failed to serialize nested_offset_", K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, nested_size_))) {
    LOG_WARN("failed to serialize nested_size_", K(ret));
  }
  return ret;
}

DEFINE_DESERIALIZE(ObBackupProviderItem)
{
  int ret = OB_SUCCESS;
  int64_t item_type_value = 0;
  if (pos < data_len && OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &item_type_value))) {
    LOG_WARN("failed to decode key", K(ret), K(data_len), K(pos));
  } else if (pos < data_len && OB_FAIL(logic_id_.deserialize(buf, data_len, pos))) {
    LOG_WARN("failed to deserialize logic id", K(ret), K(data_len), K(pos));
  } else if (pos < data_len && OB_FAIL(macro_block_id_.deserialize(buf, data_len, pos))) {
    LOG_WARN("failed to deserialize macro block id", K(ret), K(data_len), K(pos));
  } else if (pos < data_len && OB_FAIL(table_key_.deserialize(buf, data_len, pos))) {
    LOG_WARN("failed to deserialize table key", K(ret), K(data_len), K(pos));
  } else if (pos < data_len && OB_FAIL(tablet_id_.deserialize(buf, data_len, pos))) {
    LOG_WARN("failed to deserialize tablet id", K(ret), K(data_len), K(pos));
  } else if (pos < data_len && OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &nested_offset_))) {
    LOG_WARN("failed to deserialize nested_offset_", K(ret), K(data_len), K(pos));
  } else if (pos < data_len && OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &nested_size_))) {
    LOG_WARN("failed to deserialize nested_size_", K(ret), K(data_len), K(pos));
  } else {
    item_type_ = static_cast<ObBackupProviderItemType>(item_type_value);
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObBackupProviderItem)
{
  int64_t size = 0;
  size += serialization::encoded_length_vi64(item_type_);
  size += logic_id_.get_serialize_size();
  size += macro_block_id_.get_serialize_size();
  size += table_key_.get_serialize_size();
  size += tablet_id_.get_serialize_size();
  size += serialization::encoded_length_vi64(nested_offset_);
  size += serialization::encoded_length_vi64(nested_size_);
  return size;
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

/* ObBackupTabletProvider */

ObBackupTabletProvider::ObBackupTabletProvider()
    : is_inited_(),
      is_run_out_(false),
      meet_end_(false),
      sort_ret_(0),
      mutex_(common::ObLatchIds::BACKUP_LOCK),
      param_(),
      backup_data_type_(),
      cur_task_id_(),
      external_sort_(),
      ls_backup_ctx_(NULL),
      index_kv_cache_(NULL),
      sql_proxy_(NULL),
      backup_item_cmp_(sort_ret_),
      meta_index_store_(),
      prev_item_(),
      has_prev_item_(false)
{}

ObBackupTabletProvider::~ObBackupTabletProvider()
{
  reset();
}

int ObBackupTabletProvider::init(const ObLSBackupParam &param, const share::ObBackupDataType &backup_data_type,
    ObLSBackupCtx &ls_backup_ctx, ObBackupIndexKVCache &index_kv_cache, common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("provider init twice", K(ret));
  } else if (!param.is_valid() || !backup_data_type.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(param), K(backup_data_type));
  } else if (FALSE_IT(backup_item_cmp_.set_backup_data_type(backup_data_type))) {
    LOG_WARN("failed to set backup data type", K(ret), K(backup_data_type));
  } else if (OB_FAIL(param_.assign(param))) {
    LOG_WARN("failed to assign param", K(ret), K(param));
  } else {
    backup_data_type_ = backup_data_type;
    ls_backup_ctx_ = &ls_backup_ctx;
    index_kv_cache_ = &index_kv_cache;
    sql_proxy_ = &sql_proxy;
    cur_task_id_ = 0;
    is_run_out_ = false;
    is_inited_ = true;
  }
  return ret;
}

void ObBackupTabletProvider::reset()
{
  ObMutexGuard guard(mutex_);
  is_inited_ = true;
  sort_ret_ = OB_SUCCESS;
  external_sort_.clean_up();
  ls_backup_ctx_ = NULL;
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
  backup_item_cmp_.set_backup_data_type(backup_data_type);
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
  const ObBackupProviderItem *next_item = NULL;
  if (batch_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(batch_size));
  }
  while (OB_SUCC(ret) && items.count() < batch_size) {
    if (OB_FAIL(external_sort_.get_next_item(next_item))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get next item", K(ret));
      }
    } else if (!next_item->is_valid()) {
      ret = OB_INVALID_DATA;
      LOG_WARN("next item is not valid", K(ret), KPC(next_item));
    } else if (OB_FAIL(items.push_back(*next_item))) {
      LOG_WARN("failed to push back", K(ret), K(next_item));
    } else if (has_prev_item_ && OB_FAIL(compare_prev_item_(*next_item))) {
      LOG_WARN("failed to compare prev item", K(ret), K(prev_item_), KPC(next_item));
    } else {
      has_prev_item_ = true;
      prev_item_ = *next_item;
    }
  }
  LOG_INFO("inner get batch item", K(items), K_(backup_data_type));
  return ret;
}

int ObBackupTabletProvider::prepare_batch_tablet_(const uint64_t tenant_id, const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  int64_t total_count = 0;
  const bool need_prepare = external_sort_.is_all_got();
  if (need_prepare) {
    external_sort_.clean_up();
    if (OB_FAIL(external_sort_.init(
            BUF_MEM_LIMIT, FILE_BUF_SIZE, EXPIRE_TIMESTAMP, OB_SYS_TENANT_ID, &backup_item_cmp_))) {
      LOG_WARN("failed to init external sort", K(ret));
    } else {
      LOG_INFO("reinit external sort", K(tenant_id), K(ls_id));
    }
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
    if (OB_SUCC(ret)) {
      if (OB_FAIL(external_sort_.do_sort(true /*final_merge*/))) {
        LOG_WARN("failed to do external sort", K(ret));
      } else {
        LOG_INFO("do sort");
      }
    }
  } else {
    LOG_INFO("no need prepare now", K(tenant_id), K(ls_id), "is_all_got", external_sort_.is_all_got(),
        "is_sorted", external_sort_.is_sorted());
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
  if (OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls backup ctx should not be null", K(ret));
  } else if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tablet_id));
  } else if (OB_FAIL(get_tablet_handle_(tenant_id, ls_id, tablet_id, tablet_ref))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      LOG_WARN("failed to get tablet handle", K(ret), K(tenant_id), K(ls_id), K(tablet_id));
      ret = OB_SUCCESS;
      ObBackupSkippedType skipped_type(ObBackupSkippedType::MAX_TYPE);
      if (OB_FAIL(get_tablet_skipped_type_(param_.tenant_id_, ls_id, tablet_id, skipped_type))) {
        LOG_WARN("failed to get tablet skipped type", K(ret), K(param_), K(ls_id), K(tablet_id));
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
  } else if (OB_FAIL(tablet_ref->tablet_handle_.get_obj()->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_FAIL(fetch_tablet_sstable_array_(
      tablet_id, tablet_ref->tablet_handle_, *table_store_wrapper.get_member(), backup_data_type, sstable_array))) {
    LOG_WARN("failed to fetch tablet sstable array", K(ret), K(tablet_id), KPC(tablet_ref), K(backup_data_type));
  } else if (OB_FAIL(add_check_tablet_item_(tablet_id))) {
    LOG_WARN("failed to add check tablet item", K(ret), K(tablet_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < sstable_array.count(); ++i) {
      int64_t count = 0;
      ObSSTable *sstable = sstable_array.at(i).get_sstable();
      if (OB_ISNULL(sstable)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table should not be null", K(ret));
      } else  {
        // TODO(COLUMN_STORE) Attention !!! MajorSSTable in column store canbe COSSTable, maybe should adapt here.
        const ObITable::TableKey &table_key = sstable->get_key();
        if (OB_FAIL(fetch_all_logic_macro_block_id_(tablet_id, tablet_ref->tablet_handle_, table_key, *sstable, count))) {
          LOG_WARN("failed to fetch all logic macro block id", K(ret), K(tablet_id), KPC(tablet_ref), K(table_key));
        } else {
          total_count += count;
        }
      }
    }
    if (OB_SUCC(ret) && !sstable_array.empty()) {
      if (OB_FAIL(add_sstable_item_(tablet_id))) {
        LOG_WARN("failed to add sstable item", K(ret), K(tablet_id));
      } else {
        total_count += 1;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ls_backup_ctx_->tablet_stat_.prepare_tablet_sstables(
          tenant_id, backup_data_type, tablet_id, tablet_ref->tablet_handle_, sstable_array))) {
        LOG_WARN("failed to prepare tablet sstable", K(ret), K(backup_data_type), K(tablet_id), K(sstable_array));
      } else if (OB_FAIL(add_tablet_item_(tablet_id))) {
        LOG_WARN("failed to add tablet item", K(ret), K(tablet_id));
      } else {
        total_count += 1;
      }
    }
  }
  LOG_INFO("prepare tablet", K(tenant_id), K(ls_id), K(tablet_id), K_(backup_data_type), K(total_count));
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
  } else if (!ls_backup_ctx_->backup_data_type_.is_minor_backup()) {
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
        LOG_WARN("tx data can't explain user data",
                 K(OB_REPLICA_CANNOT_BACKUP), K(can_explain),
                 K(tablet_id), K(min_filled_tx_scn),
                 "backup_tx_table_filled_tx_scn", ls_backup_ctx_->backup_tx_table_filled_tx_scn_);
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
    const common::ObTabletID &tablet_id, ObBackupTabletHandleRef *&tablet_ref)
{
  int ret = OB_SUCCESS;
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
        bool is_normal_tablet = false;
        while (OB_SUCC(ret)) {
          tablet_ref = NULL;
          if (ABS_TIMEOUT_TS < ObTimeUtil::current_time()) {
            ret = OB_TIMEOUT;
            LOG_WARN("backup get tablet handle timeout", K(ret), K(ls_id), K(tablet_id));
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
          } else if (OB_FAIL(check_tablet_status_(tablet_ref->tablet_handle_, is_normal_tablet))) {
            LOG_WARN("failed to check tablet is normal", K(ret), K(tenant_id), K(ls_id), K(rebuild_seq));
          } else if (!is_normal_tablet) {
            LOG_INFO("tablet status is not normal", K(tenant_id), K(ls_id), K(tablet_id));
            usleep(100 * 1000); // wait 100ms
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
    const common::ObTabletID &tablet_id, ObBackupSkippedType &skipped_type)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t tablet_count = 0;
  int64_t tmp_ls_id = 0;
  HEAP_VAR(ObMySQLProxy::ReadResult, res)
  {
    common::sqlclient::ObMySQLResult *result = NULL;
    if (OB_FAIL(sql.assign_fmt(
            "select count(*) as count, ls_id from %s where tablet_id = %ld",
            OB_ALL_TABLET_TO_LS_TNAME, tablet_id.id()))) {
      LOG_WARN("failed to assign sql", K(ret), K(tablet_id));
    } else if (OB_ISNULL(sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql proxy should not be null", K(ret));
    } else if (OB_FAIL(sql_proxy_->read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("failed to execute sql", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is NULL", KR(ret), K(sql));
    } else if (OB_FAIL(result->next())) {
      LOG_WARN("failed to get next result", KR(ret), K(sql));
    } else {
      EXTRACT_INT_FIELD_MYSQL(*result, "count", tablet_count, int64_t);
      EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "ls_id", tmp_ls_id, int64_t);
    }
  }
  if (OB_SUCC(ret)) {
    if (0 == tablet_count) {
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
        LOG_INFO("tablet transfered, need change turn", K(ls_id));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet count should not greater than 1", K(ret), K(tablet_count));
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
    const share::ObBackupDataType &backup_data_type, common::ObIArray<storage::ObSSTableWrapper> &sstable_array)
{
  int ret = OB_SUCCESS;
  sstable_array.reset();
  if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tablet_id));
  } else if (OB_FAIL(ObBackupUtils::get_sstables_by_data_type(tablet_handle, backup_data_type, table_store, sstable_array))) {
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

int ObBackupTabletProvider::add_macro_block_id_item_list_(const common::ObTabletID &tablet_id,
    const ObITable::TableKey &table_key, const common::ObIArray<ObBackupMacroBlockId> &list, int64_t &added_count)
{
  int ret = OB_SUCCESS;
  added_count = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < list.count(); ++i) {
    const ObBackupMacroBlockId &macro_id = list.at(i);
    ObBackupProviderItem item;
    bool need_skip = false;
    if (OB_FAIL(check_macro_block_need_skip_(macro_id.logic_id_, need_skip))) {
      LOG_WARN("failed to check macro block need skip", K(ret), K(macro_id));
    } else if (need_skip) {
      // do nothing
    } else if (OB_FAIL(item.set(PROVIDER_ITEM_MACRO_ID, macro_id, table_key, tablet_id))) {
      LOG_WARN("failed to set item", K(ret), K(macro_id), K(table_key), K(tablet_id));
    } else if (!item.is_valid()) {
      ret = OB_INVALID_DATA;
      LOG_WARN("backup item is not valid", K(ret), K(item));
    } else if (OB_FAIL(external_sort_.add_item(item))) {
      LOG_WARN("failed to add item", KR(ret), K(item));
    } else {
      added_count += 1;
      LOG_INFO("add macro block id", K(tablet_id), K(table_key), K(macro_id));
    }
  }
  return ret;
}

int ObBackupTabletProvider::add_check_tablet_item_(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObBackupProviderItem item;
  if (!backup_data_type_.is_major_backup()) {
    // do nothing
  } else if (OB_FAIL(item.set_with_fake(PROVIDER_ITEM_CHECK, tablet_id))) {
    LOG_WARN("failed to set item", K(ret), K(tablet_id));
  } else if (!item.is_valid()) {
    ret = OB_INVALID_DATA;
    LOG_WARN("backup item is not valid", K(ret), K(item));
  } else if (OB_FAIL(external_sort_.add_item(item))) {
    LOG_WARN("failed to add item", KR(ret), K(item));
  } else {
    LOG_INFO("add check tablet item", K(tablet_id));
  }
  return ret;
}

int ObBackupTabletProvider::add_sstable_item_(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObBackupProviderItem item;
  if (OB_FAIL(item.set_with_fake(PROVIDER_ITEM_SSTABLE_META, tablet_id))) {
    LOG_WARN("failed to set item", K(ret), K(tablet_id));
  } else if (!item.is_valid()) {
    ret = OB_INVALID_DATA;
    LOG_WARN("backup item is not valid", K(ret), K(item));
  } else if (OB_FAIL(external_sort_.add_item(item))) {
    LOG_WARN("failed to add item", KR(ret), K(item));
  } else {
    LOG_INFO("add sstable item", K(tablet_id));
  }
  return ret;
}

int ObBackupTabletProvider::add_tablet_item_(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObBackupProviderItem item;
  if (OB_FAIL(item.set_with_fake(PROVIDER_ITEM_TABLET_META, tablet_id))) {
    LOG_WARN("failed to set item", K(ret), K(tablet_id));
  } else if (!item.is_valid()) {
    ret = OB_INVALID_DATA;
    LOG_WARN("backup item is not valid", K(ret), K(item));
  } else if (OB_FAIL(external_sort_.add_item(item))) {
    LOG_WARN("failed to add item", KR(ret), K(item));
  } else {
    LOG_INFO("add tablet item", K(tablet_id));
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

int ObBackupTabletProvider::check_macro_block_need_skip_(const blocksstable::ObLogicMacroBlockId &logic_id, bool &need_skip)
{
  int ret = OB_SUCCESS;
  need_skip = false;
  if (!logic_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(logic_id));
  } else if (OB_ISNULL(ls_backup_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls backup ctx should not be null", K(ret));
  } else if (!ls_backup_ctx_->backup_retry_ctx_.has_need_skip_logic_id_) {
    need_skip = false;
  } else {
    // the reused pair list is sorted by logic_id
    const ObArray<ObBackupMacroBlockIDPair> &reused_pair_list = ls_backup_ctx_->backup_retry_ctx_.reused_pair_list_;
    if (OB_FAIL(inner_check_macro_block_need_skip_(logic_id, reused_pair_list, need_skip))) {
      LOG_WARN("failed to inner check macro block need skip", K(ret), K(logic_id), K(reused_pair_list));
    }
  }
  return ret;
}

int ObBackupTabletProvider::inner_check_macro_block_need_skip_(const blocksstable::ObLogicMacroBlockId &logic_id,
    const common::ObArray<ObBackupMacroBlockIDPair> &reused_pair_list, bool &need_skip)
{
  int ret = OB_SUCCESS;
  need_skip = false;
  ObBackupMacroBlockIDPair search_pair;
  search_pair.logic_id_ = logic_id;
  ObArray<ObBackupMacroBlockIDPair>::const_iterator iter = std::lower_bound(reused_pair_list.begin(),
                                                                            reused_pair_list.end(),
                                                                            search_pair);
  if (iter != reused_pair_list.end()) {
    need_skip = iter->logic_id_ == logic_id;
    LOG_INFO("logic id need skip", K(need_skip), "iter_logic_id", iter->logic_id_, K(logic_id));
  }
  return ret;
}

int ObBackupTabletProvider::check_tablet_status_(const storage::ObTabletHandle &tablet_handle, bool &is_normal)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = tablet_handle.get_obj();
  ObTabletCreateDeleteMdsUserData user_data;
  is_normal = false;
  if (OB_FAIL(tablet->ObITabletMdsInterface::get_tablet_status(share::SCN::max_scn(), user_data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US))) {
    LOG_WARN("failed to get tablet status", KPC(tablet));
  } else if (user_data.get_tablet_status() == ObTabletStatus::NORMAL) {
    is_normal = true;
  }
  LOG_INFO("check tablet status", K(ret), "tablet_id", tablet->get_tablet_meta().tablet_id_, K(is_normal), K(user_data));
  return ret;
}





int ObBackupTabletProvider::compare_prev_item_(const ObBackupProviderItem &cur_item)
{
  int ret = OB_SUCCESS;
  ObBackupProviderItemCompare compare(ret);
  compare.set_backup_data_type(backup_data_type_);
  bool bret = compare(&prev_item_, &cur_item); // check if smaller
  if (!bret) {
    if (prev_item_ == cur_item && PROVIDER_ITEM_MACRO_ID == cur_item.get_item_type()) {
      // macro id might be same
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("comparing item not match", K(ret), K(prev_item_), K(cur_item));
    }
  }
  return ret;
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
  } else if (OB_FAIL(cond_.init(ObWaitEventIds::IO_CONTROLLER_COND_WAIT))) {
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
    } else if (OB_FAIL(cond_.wait(DEFAULT_WAIT_TIME_MS))) {
      int64_t duration_ms = ObTimeUtility::fast_current_time() - begin_ms;
      if (duration_ms >= DEFAULT_WAIT_TIMEOUT_MS) {
        ret = OB_EAGAIN;
        break;
      }
      if (OB_TIMEOUT == ret) {
        LOG_WARN("waiting for task too slow", K(ret));
        ret = OB_SUCCESS;
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
    if (OB_FAIL(cond_.wait(DEFAULT_WAIT_TIME_MS))) {
      if (OB_TIMEOUT == ret) {
        ret = OB_SUCCESS;
        LOG_WARN("waiting for task too slow", K(ret), K(task_id), K(cur_task_id_));
      }
    }
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

/* ObBackupTabletChecker */

ObBackupTabletChecker::ObBackupTabletChecker()
  : is_inited_(false),
    param_(),
    sql_proxy_(NULL),
    index_kv_cache_(NULL),
    meta_index_store_()
{
}

ObBackupTabletChecker::~ObBackupTabletChecker()
{
}

int ObBackupTabletChecker::init(const ObLSBackupParam &param, const share::ObBackupDataType &backup_data_type,
    common::ObMySQLProxy &sql_proxy, ObBackupIndexKVCache &index_kv_cache)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup tablet checker init twice", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(param));
  } else if (OB_FAIL(param_.assign(param))) {
    LOG_WARN("failed to assign param", K(ret), K(param));
  } else {
    sql_proxy_ = &sql_proxy;
    index_kv_cache_ = &index_kv_cache;
    if (backup_data_type.is_major_backup()) {
      if (OB_FAIL(build_tenant_minor_meta_index_store_())) {
        LOG_WARN("failed to init meta index store", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObBackupTabletChecker::check_tablet_valid(const uint64_t tenant_id, const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id, const storage::ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup tablet checker do not init", K(ret));
  } else if (OB_FAIL(check_tablet_replica_validity_(tenant_id, ls_id, tablet_id))) {
    LOG_WARN("failed to check tablet replica validity", K(ret));
  } else if (OB_FAIL(check_tablet_continuity_(ls_id, tablet_id, tablet_handle))) {
    LOG_WARN("failed to check tablet continuity", K(ret));
  }
  return ret;
}

int ObBackupTabletChecker::check_tablet_replica_validity_(const uint64_t tenant_id, const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy should not be null", K(ret), KP_(sql_proxy));
  } else if (OB_INVALID_ID == tenant_id || !ls_id.is_valid() || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(tenant_id), K(ls_id), K(tablet_id));
  } else {
    const common::ObAddr &src_addr = GCTX.self_addr();
    if (OB_FAIL(ObStorageHAUtils::check_tablet_replica_validity(tenant_id, ls_id, src_addr, tablet_id, *sql_proxy_))) {
      LOG_WARN("failed to check tablet replica validity", K(ret), K(tenant_id), K(ls_id), K(src_addr), K(tablet_id));
    } else {
    }
  }
  return ret;
}

int ObBackupTabletChecker::check_tablet_continuity_(const share::ObLSID &ls_id, const common::ObTabletID &tablet_id,
    const storage::ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  const ObBackupMetaType meta_type = BACKUP_TABLET_META;
  ObBackupDataType backup_data_type;
  backup_data_type.set_minor_data_backup();
  ObBackupMetaIndex tablet_meta_index;
  ObBackupTabletMeta prev_backup_tablet_meta;
  share::ObBackupPath backup_path;
  if (OB_FAIL(meta_index_store_.get_backup_meta_index(tablet_id, meta_type, tablet_meta_index))) {
    LOG_WARN("failed to get backup meta index", K(ret), K(tablet_id));
  } else if (OB_FAIL(ObBackupPathUtil::get_macro_block_backup_path(param_.backup_dest_,
      param_.backup_set_desc_, tablet_meta_index.ls_id_, backup_data_type, tablet_meta_index.turn_id_,
      tablet_meta_index.retry_id_, tablet_meta_index.file_id_, backup_path))) {
    LOG_WARN("failed to get macro block backup path", K(ret), K_(param), K(backup_data_type), K(tablet_meta_index));
  } else if (OB_FAIL(ObLSBackupRestoreUtil::read_tablet_meta(backup_path.get_obstr(),
      param_.backup_dest_.get_storage_info(), backup_data_type, tablet_meta_index, prev_backup_tablet_meta))) {
    LOG_WARN("failed to read tablet meta", K(ret), K(backup_path), K_(param));
  } else {
    const ObTabletMeta &cur_tablet_meta = tablet_handle.get_obj()->get_tablet_meta();
    const int64_t cur_snapshot_version = cur_tablet_meta.report_status_.merge_snapshot_version_;
    const int64_t prev_backup_snapshot_version = prev_backup_tablet_meta.tablet_meta_.report_status_.merge_snapshot_version_;
    if ((prev_backup_snapshot_version <= 0 && prev_backup_tablet_meta.tablet_meta_.table_store_flag_.with_major_sstable())
        || (cur_snapshot_version <= 0 && cur_tablet_meta.table_store_flag_.with_major_sstable())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("prev or current snapshot version should not be invalid", K(ret), K(cur_tablet_meta), K(prev_backup_tablet_meta));
    } else if (cur_snapshot_version < prev_backup_snapshot_version) {
      ret = OB_BACKUP_MAJOR_NOT_COVER_MINOR;
      LOG_WARN("tablet is not valid", K(ret), K(cur_tablet_meta), K(prev_backup_tablet_meta));
    } else {
      LOG_DEBUG("tablet is valid", K(cur_tablet_meta), K(prev_backup_tablet_meta));
    }
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    const int64_t errsim_tablet_id = GCONF.errsim_backup_tablet_id;
    if (errsim_tablet_id == tablet_id.id() && 0 == param_.retry_id_) {
      ret = OB_E(EventTable::EN_BACKUP_CHECK_TABLET_CONTINUITY_FAILED) OB_SUCCESS;
      FLOG_WARN("errsim backup check tablet continuity", K(ret), K(ls_id), K(tablet_id));
      SERVER_EVENT_SYNC_ADD("backup_errsim", "check_tablet_continuity",
                            "ls_id", ls_id.id(), "tablet_id", tablet_id.id());
    }
  }
#endif
  return ret;
}

int ObBackupTabletChecker::build_tenant_minor_meta_index_store_()
{
  int ret = OB_SUCCESS;
  ObBackupDataType backup_data_type;
  backup_data_type.set_minor_data_backup();
  ObBackupRestoreMode mode = BACKUP_MODE;
  ObBackupIndexLevel index_level = BACKUP_INDEX_LEVEL_TENANT;
  ObBackupIndexStoreParam index_store_param;
  index_store_param.index_level_ = index_level;
  index_store_param.tenant_id_ = param_.tenant_id_;
  index_store_param.backup_set_id_ = param_.backup_set_desc_.backup_set_id_;
  index_store_param.ls_id_ = param_.ls_id_;
  index_store_param.is_tenant_level_ = true;
  index_store_param.backup_data_type_ = backup_data_type;
  int64_t retry_id = 0;
  if (meta_index_store_.is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("meta index store init twice", K(ret));
  } else if (OB_FAIL(get_tenant_meta_index_turn_id_(index_store_param.turn_id_))) {
    LOG_WARN("failed to find meta index turn id", K(ret), K(backup_data_type));
  } else if (OB_FAIL(get_tenant_meta_index_retry_id_(backup_data_type, index_store_param.turn_id_, retry_id))) {
    LOG_WARN("failed to find meta index retry id", K(ret), K(backup_data_type));
  } else if (FALSE_IT(index_store_param.retry_id_ = retry_id)) {
    // assign
  } else if (OB_FAIL(meta_index_store_.init(mode,
                                            index_store_param,
                                            param_.backup_dest_,
                                            param_.backup_set_desc_,
                                            false/*is_sec_meta*/,
                                            *index_kv_cache_))) {
    LOG_WARN("failed to init macro index store", K(ret), K_(param));
  }
  return ret;
}

int ObBackupTabletChecker::get_tenant_meta_index_turn_id_(int64_t &turn_id)
{
  int ret = OB_SUCCESS;
  ObBackupSetTaskAttr set_task_attr;
  if (OB_FAIL(share::ObBackupTaskOperator::get_backup_task(*sql_proxy_, param_.job_id_, param_.tenant_id_, false, set_task_attr))) {
    LOG_WARN("failed to get backup task", K(ret));
  } else {
    turn_id = set_task_attr.minor_turn_id_;
  }
  return ret;
}

int ObBackupTabletChecker::get_tenant_meta_index_retry_id_(
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

}  // namespace backup
}  // namespace oceanbase
