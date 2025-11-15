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

#define USING_LOG_PREFIX STORAGE_COMPACTION

#include "storage/ddl/ob_ddl_merge_schedule.h"
#include "storage/ddl/ob_ddl_merge_task.h"
#include "share/ob_ddl_checksum.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "share/ob_ddl_sim_point.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/ob_storage_schema_util.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/incremental/ob_ls_inc_sstable_uploader.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "share/scheduler/ob_partition_auto_split_helper.h"
#endif
#include "storage/compaction/ob_schedule_dag_func.h"
#include "storage/ddl/ob_ddl_merge_task_utils.h"
#include "storage/ddl/ob_ddl_merge_task_v2.h"
#include "storage/ddl/ob_direct_load_mgr_utils.h"
#include "storage/compaction/ob_uncommit_tx_info.h"
#include "storage/compaction/ob_partition_merge_policy.h"


using namespace oceanbase::observer;
using namespace oceanbase::share::schema;
using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;


namespace oceanbase
{
namespace storage
{
#ifdef OB_BUILD_SHARED_STORAGE
int ObDDLMergeScheduler::schedule_ddl_minor_merge_on_demand(
      const bool need_freeze,
      const share::ObLSID &ls_id,
      ObDDLKvMgrHandle &ddl_kv_mgr_handle)
{
  int ret = OB_SUCCESS;
  ObDDLTableMergeDagParam param;
  ObArray<ObDDLKVHandle> ddl_kvs_handle;
  if (OB_UNLIKELY(!ls_id.is_valid() || !ddl_kv_mgr_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(ddl_kv_mgr_handle));
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->get_ddl_kvs(!need_freeze/*frozen_only*/, ddl_kvs_handle))) {
    LOG_WARN("get freezed ddl kv failed", K(ret));
  } else if (ddl_kvs_handle.empty()) {
    LOG_TRACE("empty ddl kv", "tablet_id", ddl_kv_mgr_handle.get_obj()->get_tablet_id());
  } else if (need_freeze && OB_FAIL(ddl_kv_mgr_handle.get_obj()->freeze_ddl_kv(
          ddl_kvs_handle.at(0).get_obj()->get_ddl_start_scn(),
          ddl_kvs_handle.at(0).get_obj()->get_snapshot_version(),
          ddl_kvs_handle.at(0).get_obj()->get_data_format_version(),
          SCN::min_scn()/*freeze_scn*/))) {
    LOG_WARN("failed to freeze kv", K(ret), "tablet_id", ddl_kv_mgr_handle.get_obj()->get_tablet_id());
  } else {
    param.direct_load_type_    = SS_IDEM_DIRECT_LOAD_DDL;
    param.ls_id_               = ls_id;
    param.tablet_id_           = ddl_kv_mgr_handle.get_obj()->get_tablet_id();
    param.start_scn_           = ddl_kvs_handle.at(0).get_obj()->get_ddl_start_scn();
    param.snapshot_version_    = ddl_kvs_handle.at(0).get_obj()->get_snapshot_version();
    param.data_format_version_ = ddl_kvs_handle.at(0).get_obj()->get_data_format_version();
    if (OB_FAIL(compaction::ObScheduleDagFunc::schedule_ddl_table_merge_dag(param))) {
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("schedule ddl merge dag failed", K(ret), K(param));
      }
    }
  }
  LOG_TRACE("schedule tablet ddl minor merge", K(ret), K(param));
  return ret;
}
/*
 * ss mode noly need to chck merge dump
*/
int ObDDLMergeScheduler::check_need_merge_for_ss(ObTablet &tablet, ObArray<ObDDLKVHandle> &ddl_kvs, bool &need_schedule_merge, ObDDLKVType &ddl_kv_type)
{
  int ret = OB_SUCCESS;
  need_schedule_merge = false;
  ddl_kv_type = ObDDLKVType::DDL_KV_INVALID;

  if (ddl_kv_type != ObDDLKVType::DDL_KV_INVALID || need_schedule_merge) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument, return param should be invalid", K(ret), K(ddl_kv_type), K(need_schedule_merge));
  } else  if (0 == ddl_kvs.count()) {
    /* ddl kv mgr not need to merge, skip */
  } else if (ObDDLKVType::DDL_KV_FULL == ddl_kvs.at(0).get_obj()->get_ddl_kv_type() && GCTX.is_shared_storage_mode()) {
    need_schedule_merge = true;
    ddl_kv_type = ObDDLKVType::DDL_KV_FULL;
    LOG_INFO("ddl kv exist, need merge", K(ret), K(tablet.get_tablet_id()));
  }
  return ret;
}


/* func only used for ss mode, for ddl finish log */
int ObDDLMergeScheduler::finish_log_freeze_ddl_kv(const ObLSID &ls_id, ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  if (!ls_id.is_valid() || !tablet_handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_handle));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    // process the entry_not_exist if ddl kv mgr is destroyed.
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("create ddl kv mgr failed", K(ret));
    } else {
      ret = OB_SUCCESS; // ignore ddl kv mgr not exist
    }
  } else if (OB_FAIL(ObDDLMergeScheduler::schedule_ddl_minor_merge_on_demand(true/*need_freeze*/, ls_id, ddl_kv_mgr_handle))) {
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      LOG_WARN("failed to schedule ddl minor merge", K(ret), K(ls_id), K(tablet_handle.get_obj()->get_tablet_id()));
    } else {
      ret = OB_SUCCESS;
      LOG_INFO("background will schedule ddl minor merge", K(tablet_handle.get_obj()->get_tablet_id()));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (ddl_kv_mgr_handle.is_valid()) {
    if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->remove_idempotence_checker())) {
      LOG_WARN("failed to remove idempotence checker", K(ret));
    }
  }
  return ret;
}
#endif

/*
 * for idem mode, since start log not exist
 * when restart all observer, before build major and all ddl kvs have been dump
 * ddl kv mgr may not exist and may not schedule merge
 * add new check function to schedule merge, here are need to set type
 * 1. idem sn
 * 2. inc major
*/
int check_full_major_exist(const ObTablet &tablet, bool &full_major_exist)
{
  int ret = OB_SUCCESS;
  full_major_exist = false;
  const ObTabletMeta &tablet_meta = tablet.get_tablet_meta();
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fetch table store failed", K(ret));
  } else if (nullptr != table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(false/*first*/)) {
    full_major_exist = true;
  }
  return ret;
}

/*
 * for idem sn mode, both major merge & dump merge need to be check
*/
int ObDDLMergeScheduler::check_need_merge_for_idem_sn(ObTablet &tablet, ObArray<ObDDLKVHandle> &ddl_kvs, bool &need_schedule_merge, ObDDLKVType &ddl_kv_type)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator arena(ObMemAttr(MTL_ID(), "Ddl_Check_Maj"));
  ObTabletDDLCompleteMdsUserData user_data;
  if (ddl_kv_type != ObDDLKVType::DDL_KV_INVALID || need_schedule_merge) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument, return param should be invalid", K(ret), K(ddl_kv_type), K(need_schedule_merge));
  } else if (tablet.get_tablet_meta().has_transfer_table()) {
    if (REACH_THREAD_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
      LOG_INFO("The tablet in the transfer process does not do ddl major_merge", K(tablet.get_tablet_id()));
    }
  } else if (tablet.get_tablet_meta().ddl_data_format_version_ < DDL_IDEM_DATA_FORMAT_VERSION) {
    LOG_INFO("skip check need merge for idem sn, data format version is less than idem data format version", K(tablet.get_tablet_meta().ddl_data_format_version_));
  } else {
    bool is_full_major_exist = false;
    if (OB_FAIL(check_full_major_exist(tablet, is_full_major_exist))) {
      LOG_WARN("failed to check full major exist", K(ret), K(tablet.get_tablet_id()));
    } else if (ddl_kvs.empty()) {
      /* check for empty table */
      if (is_full_major_exist) {
        /* if major already exist skip replay */
      } else if (OB_FAIL(tablet.get_ddl_complete(share::SCN::max_scn(), arena, user_data))) {
        if (OB_EMPTY_RESULT == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get ddl complete", K(ret), K(tablet.get_tablet_id()));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (user_data.has_complete_ && is_full_direct_load(user_data.direct_load_type_)) {
        need_schedule_merge = true;
        ddl_kv_type = ObDDLKVType::DDL_KV_FULL;
      }
    } else {
      /* schedule merge when ddl kv exist */
      if (ObDDLKVType::DDL_KV_FULL == ddl_kvs.at(0).get_obj()->get_ddl_kv_type() && !GCTX.is_shared_storage_mode()) {
        need_schedule_merge = true;
        ddl_kv_type = ObDDLKVType::DDL_KV_FULL;
      }
    }

    if (OB_SUCC(ret) && need_schedule_merge) {
      /* try create ddl kv mgr, for emtpy table */
      ObDDLKvMgrHandle ddl_kv_mgr_handle;
      if (OB_FAIL(tablet.get_ddl_kv_mgr(ddl_kv_mgr_handle, true /* try create */))) {
        LOG_WARN("failed to get tablet ddl kv mgr", K(ret));
      }
      FLOG_INFO("schedule ddl merge", K(ret), K(tablet.get_tablet_id()));
    }
  }
  return ret;
}

/*
 * for nidem sn mode
 * check need to merge rely on direct load mgr
*/
int ObDDLMergeScheduler::check_need_merge_for_nidem_sn(ObTablet &tablet, ObArray<ObDDLKVHandle> &ddl_kvs, bool &need_schedule_merge, ObDDLKVType &ddl_kv_type)
{
  int ret = OB_SUCCESS;
  if (ddl_kv_type != ObDDLKVType::DDL_KV_INVALID || need_schedule_merge) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument, return param should be invalid", K(ret), K(ddl_kv_type), K(need_schedule_merge));
  } else if (!(tablet.get_tablet_meta().ddl_data_format_version_ < DDL_IDEM_DATA_FORMAT_VERSION)) {
    LOG_INFO("skip check need merge for nidem sn, data format version is not less than idem data format version", K(tablet.get_tablet_meta().ddl_data_format_version_));
  } else if (tablet.get_tablet_meta().has_transfer_table()) {
    if (REACH_THREAD_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
      LOG_INFO("The tablet in the transfer process does not do ddl major_merge", K(tablet.get_tablet_id()));
    }
  } else {
    bool is_major_sstable_exist = false;
    ObTenantDirectLoadMgr *tenant_direct_load_mgr = nullptr;
    ObTabletDirectLoadMgrHandle direct_load_mgr_handle;
    if (OB_ISNULL(tenant_direct_load_mgr = MTL(ObTenantDirectLoadMgr *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, tenant direct load mgr is null", K(ret));
    } else if (OB_FAIL(tenant_direct_load_mgr->get_tablet_mgr_and_check_major(
          tablet.get_ls_id(), tablet.get_tablet_id(), true, direct_load_mgr_handle, is_major_sstable_exist))) {
      if (OB_ENTRY_NOT_EXIST == ret && is_major_sstable_exist) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get tablet direct load mgr failed", K(ret), "tablet_id", tablet.get_tablet_id());
      }
    } else {
      need_schedule_merge = true;
      ddl_kv_type = ObDDLKVType::DDL_KV_FULL;
    }
  }
  return ret;
}

int ObDDLMergeScheduler::check_tablet_need_merge(ObTablet &tablet, ObDDLKvMgrHandle &ddl_kv_mgr_handle, bool &need_schedule_merge, ObDDLKVType &ddl_kv_type)
{
  int ret = OB_SUCCESS;
  need_schedule_merge = false;
  ObArray<ObDDLKVHandle> ddl_kv_handles;
  if (!ddl_kv_mgr_handle.is_valid()) {
    /* if ddl kv mgr handle is not valid, skip not need to get ddl kvs */
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->get_ddl_kvs(false /* for both frozen & active*/, ddl_kv_handles))) {
    LOG_WARN("failed to get ddl kv", K(ret));
  }

  if (OB_FAIL(ret)) {
  #ifdef OB_BUILD_SHARED_STORAGE
  } else if (!need_schedule_merge && GCTX.is_shared_storage_mode() &&
             OB_FAIL(check_need_merge_for_ss(tablet, ddl_kv_handles, need_schedule_merge, ddl_kv_type))) {
    LOG_WARN("failed to check need merge for ss", K(ret));
  #endif
  } else if (!need_schedule_merge && !GCTX.is_shared_storage_mode() && tablet.get_tablet_meta().ddl_data_format_version_ < DDL_IDEM_DATA_FORMAT_VERSION &&
             OB_FAIL(check_need_merge_for_nidem_sn(tablet, ddl_kv_handles, need_schedule_merge, ddl_kv_type))) {
    LOG_WARN("failed to check need merge for nidem sn", K(ret));
  } else if (!need_schedule_merge && !GCTX.is_shared_storage_mode() && tablet.get_tablet_meta().ddl_data_format_version_ >= DDL_IDEM_DATA_FORMAT_VERSION &&
             OB_FAIL(check_need_merge_for_idem_sn(tablet, ddl_kv_handles, need_schedule_merge, ddl_kv_type))) {
    LOG_WARN("failed to check need merge for idem sn", K(ret));
  } else if (!need_schedule_merge &&
             OB_FAIL(check_need_merge_for_inc_major(tablet, ddl_kv_handles, need_schedule_merge, ddl_kv_type))) {
    LOG_WARN("failed to check need merge for inc major", K(ret));
  }
  return ret;
}

int ObDDLMergeScheduler::freeze_ddl_kv(const share::SCN &rec_scn, ObLS *ls, ObDDLKvMgrHandle &ddl_kv_mgr_handle)
{
  int ret = OB_SUCCESS;
  ObArray<ObDDLKVHandle> ddl_kvs_handle;
  ObTabletDirectLoadMgrHandle direct_load_mgr_hdl;
  ObTenantDirectLoadMgr *tenant_direct_load_mgr = MTL(ObTenantDirectLoadMgr *);
  ObDDLTableMergeDagParam param;
  ObDDLKVHandle first_ddl_kv_handle;
  bool is_major_sstable_exist = false;
  if (!rec_scn.is_valid() || nullptr == ls || !ddl_kv_mgr_handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(rec_scn), KPC(ls), K(ddl_kv_mgr_handle));
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->get_ddl_kvs(false/*frozen_only*/, ddl_kvs_handle))) {
    LOG_WARN("get freezed ddl kv failed", K(ret), "tablet_id", ddl_kv_mgr_handle.get_obj()->get_tablet_id());
  } else if (ddl_kvs_handle.empty()) {
    /* if ddl kv is empty, do notging */
  } else if (OB_FALSE_IT(first_ddl_kv_handle = ddl_kvs_handle.at(0))) {
  } else if (OB_UNLIKELY(!first_ddl_kv_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl kv handle should not be empty here", K(ret), K(first_ddl_kv_handle));
  } else if (DDL_IDEM_DATA_FORMAT_VERSION > first_ddl_kv_handle.get_obj()->get_data_format_version()) {
    bool is_major_sstable_exist = false;
    ObTabletDirectLoadMgrHandle direct_load_mgr_hdl;
    if (OB_FAIL(tenant_direct_load_mgr->get_tablet_mgr_and_check_major(
                ls->get_ls_id(),
                ddl_kv_mgr_handle.get_obj()->get_tablet_id(),
                true/* is_full_direct_load */,
                direct_load_mgr_hdl,
                is_major_sstable_exist))) {
      if (OB_ENTRY_NOT_EXIST == ret && is_major_sstable_exist) {
        LOG_WARN("major sstable already exist, ddl kv may leak", K(ret), "tablet_id", ddl_kv_mgr_handle.get_obj()->get_tablet_id());
      } else {
        LOG_WARN("get tablet direct load mgr failed", K(ret), "tablet_id", ddl_kv_mgr_handle.get_obj()->get_tablet_id(), K(is_major_sstable_exist));
      }
    } else {
      DEBUG_SYNC(BEFORE_DDL_CHECKPOINT);
      param.ls_id_               = ls->get_ls_id();
      param.tablet_id_           = ddl_kv_mgr_handle.get_obj()->get_tablet_id();
      param.start_scn_           = direct_load_mgr_hdl.get_full_obj()->get_start_scn();
      param.rec_scn_             = rec_scn;
      param.direct_load_type_    = direct_load_mgr_hdl.get_full_obj()->get_direct_load_type();
      param.is_commit_           = false;
      param.data_format_version_ = direct_load_mgr_hdl.get_full_obj()->get_tenant_data_version();
      param.snapshot_version_    = direct_load_mgr_hdl.get_full_obj()->get_table_key().get_snapshot_version();
      LOG_INFO("schedule ddl merge dag", K(param));
      if (OB_FAIL(ObTabletDDLUtil::freeze_ddl_kv(param))) {
        LOG_WARN("try to freeze ddl kv failed", K(ret), K(param));
      }
    }
    (void)tenant_direct_load_mgr->gc_tablet_direct_load();
  } else if (first_ddl_kv_handle.get_obj()->is_freezed()) {
    /* if first ddl kv is freezed, do nothing */
  } else if (first_ddl_kv_handle.get_obj()->is_inc_major_ddl_kv()) {
    if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->freeze_ddl_kv(SCN::min_scn()/*start_scn*/,
                                                           0/*snapshot_version*/,
                                                           0/*data_format_version*/,
                                                           SCN::min_scn()/*freeze_scn*/,
                                                           ObDDLKVType::DDL_KV_INC_MAJOR))) {
      LOG_WARN("failed to freeze ddl kv", KR(ret));
    }
  } else {
    ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "DdlMrgSche"));

    ObTabletHandle tablet_handle;
    ObTabletCreateDeleteMdsUserData user_data;
    ObTabletDDLCompleteMdsUserData  ddl_complete;
    if (OB_FAIL(ls->get_tablet(ddl_kv_mgr_handle.get_obj()->get_tablet_id(),
                                     tablet_handle, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US,
                                     ObMDSGetTabletMode::READ_ALL_COMMITED))) {
        LOG_WARN("failed to get tablet handle", K(ret), K(ls->get_ls_id()), K(ddl_kv_mgr_handle.get_obj()->get_tablet_id()));
    } else if (!tablet_handle.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid tablet handle", K(ret), K(tablet_handle));
    } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_complete(share::SCN::max_scn(), allocator, ddl_complete))) {
      if (OB_EMPTY_RESULT == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("no ddl complete", K(ret), K(ls->get_ls_id()), K(ddl_kv_mgr_handle.get_obj()->get_tablet_id()));
      } else {
        LOG_WARN("failed to get ddl complete", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObDirectLoadMgrUtil::generate_merge_param(ddl_complete, *(tablet_handle.get_obj()), param))) {
      LOG_WARN("failed to generate merge param", K(ret));
    } else if (OB_FAIL(ObTabletDDLUtil::freeze_ddl_kv(param))) {
      LOG_WARN("try to freeze ddl kv failed", K(ret), K(param));
    }
    FLOG_INFO("schedule ddl dump merge task", K(ret), K(ls->get_ls_id()), K(tablet_handle.get_obj()->get_tablet_id()));
  }
  return ret;
}
int ObDDLMergeScheduler::schedule_ddl_merge(ObLS *ls,
                                            ObDDLKvMgrHandle &ddl_kv_mgr_handle)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  if (nullptr == ls || !ddl_kv_mgr_handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), KPC(ls), K(ddl_kv_mgr_handle));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls, ddl_kv_mgr_handle.get_obj()->get_tablet_id(), tablet_handle, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("get tablet handle failed", K(ret), K(ddl_kv_mgr_handle.get_obj()->get_tablet_id()));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet handle", K(ret), K(tablet_handle));
  } else if (OB_FAIL(schedule_ddl_merge(ls, tablet_handle, ddl_kv_mgr_handle))) {
    LOG_WARN("failed to schedule ddl merge", K(ret), KPC(ls), K(tablet_handle));
  }
  return ret;
}

int ObDDLMergeScheduler::schedule_ddl_merge(ObLS *ls,
                                            ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  const ObTabletID tablet_id = tablet_handle.is_valid() ? tablet_handle.get_obj()->get_tablet_meta().tablet_id_ : ObTabletID();
  if (OB_UNLIKELY(nullptr == ls || !tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), KPC(ls), K(tablet_handle));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_TRACE("kv mgr not exist", K(ret), K(tablet_handle.get_obj()->get_tablet_id()));
      ret = OB_SUCCESS; /* for empty table, ddl kv may not exist*/
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObDDLMergeScheduler::schedule_ddl_merge(ls, tablet_handle, ddl_kv_mgr_handle))) {
    LOG_WARN("failed to schedule ddl merge", K(ret), KPC(ls), K(tablet_handle));
  }
  return ret;
}


int ObDDLMergeScheduler::schedule_ddl_merge(ObLS *ls,
                                            ObTabletHandle &tablet_handle,
                                            ObDDLKvMgrHandle &optimal_ddl_kv_mgr_handle)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  ObTimeGuard time_guard("schedule_ddl_merge", 3 * 10 * 1000);

  bool need_schedule_merge = false;
  ObLSID ls_id;
  ObDDLKVType ddl_kv_type = ObDDLKVType::DDL_KV_INVALID; /* used for decided using which direct load type*/
  const ObTabletID tablet_id = tablet_handle.is_valid() ? tablet_handle.get_obj()->get_tablet_meta().tablet_id_ : ObTabletID();


  if (nullptr == ls || !tablet_handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), KPC(ls), K(tablet_id));
  } else if (FALSE_IT(ls_id = ls->get_ls_id())) {
  } else if (OB_FAIL(ObDDLMergeScheduler::check_tablet_need_merge(*tablet_handle.get_obj(), optimal_ddl_kv_mgr_handle, need_schedule_merge, ddl_kv_type))) {
    LOG_WARN("failed to check tablet need merge", K(ret), KPC(ls), K(tablet_id));
  } else if (need_schedule_merge) {
    LOG_INFO("need schedule merge", K(ret), KPC(ls), K(tablet_id), K(need_schedule_merge), K(ddl_kv_type));
  }

  if (OB_FAIL(ret)) {
  } else if (need_schedule_merge) {
    switch(ddl_kv_type) {
      case ObDDLKVType::DDL_KV_FULL:
        if (GCTX.is_shared_storage_mode()) {
        #ifdef OB_BUILD_SHARED_STORAGE
          if (OB_FAIL(ObDDLMergeScheduler::schedule_ddl_minor_merge_on_demand(false/*need_freeze*/, ls->get_ls_id(), optimal_ddl_kv_mgr_handle))) {
            if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
              LOG_WARN("failed to schedule tablet ddl merge", K(ret), K(ls_id), K(tablet_id));
            } else {
              LOG_TRACE("schedule ddl minor merge failed", K(ret), K(ls_id), K(tablet_id));
            }
          }
      #endif
        } else {
          if (OB_FAIL(schedule_tablet_ddl_major_merge(ls, tablet_handle))) {
            if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
              LOG_WARN("failed to schedule tablet ddl merge", K(ret), K(ls_id), K(tablet_id));
            } else {
              LOG_TRACE("schedule ddl major merge failed", K(ret), K(ls_id), K(tablet_id));
            }
          }
        }
        break;
      case ObDDLKVType::DDL_KV_INC_MAJOR:
        {
          if (OB_FAIL(schedule_tablet_ddl_inc_major_merge(ls, tablet_handle))) {
            if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
              LOG_WARN("failed to schedule tablet ddl merge", K(ret), K(ls_id), K(tablet_id));
            } else {
              LOG_TRACE("schedule ddl major merge failed", K(ret), K(ls_id), K(tablet_id));
            }
          }
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ddl kv type", K(ret), K(ddl_kv_type));
        break;
    }
  }
#ifdef OB_BUILD_SHARED_STORAGE
  bool is_ls_leader = false;
  if (OB_FAIL(ret)) {
  } else if (OB_TMP_FAIL(ObDDLUtil::is_ls_leader(*ls, is_ls_leader))) {
    LOG_WARN("failed to check if is ls leader", K(tmp_ret), KPC(ls));
    is_ls_leader = false;
  } else if (is_ls_leader && OB_TMP_FAIL(ObDDLMergeScheduler::schedule_task_if_split_src(tablet_handle))) {
    LOG_WARN("failed to check if it is split src tablet", K(tmp_ret), K(tablet_handle));
  }
#endif

#ifdef OB_BUILD_SHARED_STORAGE
  if (OB_SUCC(ret)) {
    if (GCTX.is_shared_storage_mode() && OB_TMP_FAIL(ObDDLMergeScheduler::schedule_ss_update_inc_major_and_gc_inc_major(ls,
                                                                                                   tablet_handle))) {
      LOG_WARN("fail to schedule ss update inc major and gc inc major", K(tmp_ret), K(ls_id), K(tablet_id));
    }

    if (GCTX.is_shared_storage_mode() && OB_TMP_FAIL(ObDDLMergeScheduler::schedule_ss_gc_inc_major_ddl_dump(ls,
                                                                                       tablet_handle))) {
      LOG_WARN("fail to schedule ss update inc major and gc inc major", K(tmp_ret), K(ls_id), K(tablet_id));
    }
  }
#endif

  LOG_TRACE("schedule ddl tablet merge", K(ret), K(ls_id), K(tablet_id));
  return ret;
}



/*
*  schedule to build ddl dump/major sstable in share nothing mode
*/
int ObDDLMergeScheduler::schedule_tablet_ddl_major_merge(
    ObLS *ls,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  bool need_schedule_merge = false;
  ObDDLTableMergeDagParam param;
  ObTabletDirectLoadMgrHandle direct_load_mgr_handle;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  ObTenantDirectLoadMgr *tenant_direct_load_mgr = MTL(ObTenantDirectLoadMgr *);
  bool is_major_sstable_exist = false;
  bool has_freezed_ddl_kv = false;
  SCN ddl_commit_scn;
  ObLSID ls_id;
  if (OB_UNLIKELY(nullptr == ls || !tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_handle));
  } else if (FALSE_IT(ls_id = ls->get_ls_id())) {
  } else if (GCTX.is_shared_storage_mode()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schdule ddl major merge func should not be used in share storage mode", K(ret), K(lbt()));
  } else if (tablet_handle.get_obj()->get_tablet_meta().has_transfer_table()) {
    if (REACH_THREAD_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
      LOG_INFO("The tablet in the transfer process does not do ddl major_merge", K(tablet_handle));
    }
  } else {
    need_schedule_merge = true;
  }

  /* schedule direct load mgr using commit log*/
  if (OB_FAIL(ret)) {
  } else if (!need_schedule_merge) {
  } else if (DDL_IDEM_DATA_FORMAT_VERSION <= tablet_handle.get_obj()->get_tablet_meta().ddl_data_format_version_) {
  } else if (OB_ISNULL(tenant_direct_load_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), K(MTL_ID()));
  } else if (OB_FAIL(tenant_direct_load_mgr->get_tablet_mgr_and_check_major(
          ls_id,
          tablet_handle.get_obj()->get_tablet_meta().tablet_id_,
          true, /* is_full_direct_load */
          direct_load_mgr_handle,
          is_major_sstable_exist))) {
    if (OB_ENTRY_NOT_EXIST == ret && is_major_sstable_exist) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("get tablet direct load mgr failed", K(ret), "tablet_id", tablet_handle.get_obj()->get_tablet_meta().tablet_id_);
    }
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    LOG_WARN("get ddl kv mgr failed", K(ret));
  } else if (FALSE_IT(ddl_commit_scn = direct_load_mgr_handle.get_full_obj()->get_commit_scn(tablet_handle.get_obj()->get_tablet_meta()))) {
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->try_flush_ddl_commit_scn(ls, tablet_handle, direct_load_mgr_handle, ddl_commit_scn))) {
    LOG_WARN("try flush ddl commit scn failed", K(ret), "tablet_id", tablet_handle.get_obj()->get_tablet_meta().tablet_id_);
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->check_has_freezed_ddl_kv(has_freezed_ddl_kv))) {
    LOG_WARN("check has freezed ddl kv failed", K(ret));
  } else if (OB_FAIL(direct_load_mgr_handle.get_full_obj()->prepare_ddl_merge_param(*tablet_handle.get_obj(), param))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("prepare major merge param failed", K(ret), "tablet_id", tablet_handle.get_obj()->get_tablet_meta().tablet_id_);
    }
  } else if (has_freezed_ddl_kv || param.is_commit_) {
    if (OB_FAIL(compaction::ObScheduleDagFunc::schedule_ddl_table_merge_dag(param))) {
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        LOG_WARN("schedule ddl merge dag failed", K(ret), K(param));
      }
    }
  }

  /* schedule merge using mds data */
  if (OB_FAIL(ret)) {
  } else if (!need_schedule_merge) {
  } else if (DDL_IDEM_DATA_FORMAT_VERSION <= tablet_handle.get_obj()->get_tablet_meta().ddl_data_format_version_) {
    /* schedule to build major sstable, getting merge param from mds data */
    bool has_freezed_ddl_kv = false;
    ObDDLTableMergeDagParam param;
    ObArenaAllocator arena(ObMemAttr(MTL_ID(), "DDL_Mrg_Par"));
    ObTabletDDLCompleteMdsUserData  ddl_complete;
    if (OB_FAIL(ObDDLUtil::is_major_exist(ls_id, tablet_handle.get_obj()->get_tablet_meta().tablet_id_, is_major_sstable_exist))) {
      LOG_WARN("failed to check major sstable exist", K(ret), K(ls_id), K(tablet_handle.get_obj()->get_tablet_meta().tablet_id_));
    } else if (is_major_sstable_exist) {
      LOG_INFO("major sstable already exist, don't need to schdule ddl merge", K(ret), K(ls_id), K(tablet_handle.get_obj()->get_tablet_meta().tablet_id_));
    } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
      LOG_WARN("get ddl kv mgr failed", K(ret));
    } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->check_has_freezed_ddl_kv(has_freezed_ddl_kv))) {
      LOG_WARN("check has freezed ddl kv failed", K(ret));
    } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_complete(share::SCN::max_scn(), arena, ddl_complete))) {
      if (OB_EMPTY_RESULT == ret) {
        ret = OB_SUCCESS;
      }
      LOG_WARN("failed to get ddl complete", K(ret), K(tablet_handle.get_obj()->get_tablet_meta().ddl_data_format_version_), K(has_freezed_ddl_kv));
    }
    if (OB_FAIL(ret)) {
    } else if (ddl_complete.has_complete_ || has_freezed_ddl_kv) {
      if (OB_FAIL(ObDirectLoadMgrUtil::generate_merge_param(ddl_complete, *(tablet_handle.get_obj()), param))) {
        LOG_WARN("failed to generate merge param", K(ret), K(ddl_complete));
      } else if (FALSE_IT(param.rec_scn_ = ddl_kv_mgr_handle.get_obj()->get_max_freeze_scn())) {
      } else if (OB_FAIL(compaction::ObScheduleDagFunc::schedule_ddl_table_merge_dag(param))) {
        LOG_WARN("try schedule ddl merge dag failed when ddl kv is full ", K(ret), K(param));
      } else {
        FLOG_INFO("schedule ddl merge task", K(ret), K(ls->get_ls_id()), K(tablet_handle.get_obj()->get_tablet_id()), K(param));
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
