/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
#include "lib/resource/ob_resource_mgr.h"
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
    } else if (ObDDLKVType::DDL_KV_FULL == ddl_kvs.at(0).get_obj()->get_ddl_kv_type()) {
      /* schedule merge when ddl kv already freeze */
      if (ddl_kvs.at(0).get_obj()->is_freezed()) {
        need_schedule_merge = true;
        ddl_kv_type = ObDDLKVType::DDL_KV_FULL;
      }

      /* schedule merge when ddl kv active && ddl complete mds exists,
       * ddl complete replay may be skipped when tablet_change_checkpoint_scn is copied
       * from the migration source, while redo replay rebuilds an active ddl kv */
      if (OB_FAIL(ret) || need_schedule_merge || is_full_major_exist) {
      } else if (OB_FAIL(tablet.get_ddl_complete(share::SCN::max_scn(), arena, user_data))) {
        if (OB_EMPTY_RESULT == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get ddl complete", K(ret), K(tablet.get_tablet_id()));
        }
      } else if (user_data.has_complete_ && is_full_direct_load(user_data.direct_load_type_)) {
        need_schedule_merge = true;
        ddl_kv_type = ObDDLKVType::DDL_KV_FULL;
      }

      /* schedule merge when ddl kv active && not enough mem */
      if (OB_FAIL(ret) || need_schedule_merge) {
      } else {
        bool need_for_mem = false;
        if (OB_FAIL(check_need_merge_for_memory(need_for_mem))) {
          LOG_WARN("failed to check need merge for memory", K(ret));
        } else if (need_for_mem) {
          need_schedule_merge = true;
          ddl_kv_type = ObDDLKVType::DDL_KV_FULL;
          ObDDLKvMgrHandle ddl_kv_mgr_handle;
          if (OB_FAIL(tablet.get_ddl_kv_mgr(ddl_kv_mgr_handle, true /* try create */))) {
            LOG_WARN("failed to get tablet ddl kv mgr", K(ret), K(tablet.get_tablet_id()));
          } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->freeze_ddl_kv(
                          ddl_kvs.at(0).get_obj()->get_ddl_start_scn(),
                          ddl_kvs.at(0).get_obj()->get_snapshot_version(),
                          ddl_kvs.at(0).get_obj()->get_data_format_version(),
                         share::SCN::min_scn()/*freeze_scn*/))) {
            LOG_WARN("failed to freeze ddl kv for memory", K(ret), K(tablet.get_tablet_id()), KPC(ddl_kvs.at(0).get_obj()));
          } else {
            need_schedule_merge = true;
            ddl_kv_type = ObDDLKVType::DDL_KV_FULL;
          }
        }
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

int ObDDLMergeScheduler::check_need_merge_for_memory(bool &need_schedule)
{
  int ret = OB_SUCCESS;
  need_schedule = false;

  static const int64_t MEM_HOLD_PERCENT = 15; // 15% of tenant memory limit
  static const int64_t DDL_MERGE_DAG_CNT_THRESHOLD = 1500;

  int64_t ctx_hold = 0;
  int64_t tenant_limit = 0;
  int64_t ddl_merge_dag_cnt = 0;

  lib::ObTenantResourceMgrHandle resource_handle;
  lib::ObTenantMemoryMgr *mem_mgr = nullptr;
  share::ObTenantDagScheduler *dag_scheduler = nullptr;

  if (OB_FAIL(lib::ObResourceMgr::get_instance().get_tenant_resource_mgr(MTL_ID(), resource_handle))) {
    LOG_WARN("get tenant resource mgr failed", K(ret), "tenant_id", MTL_ID());
  } else if (OB_ISNULL(mem_mgr = resource_handle.get_memory_mgr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant memory mgr is null", K(ret), "tenant_id", MTL_ID());
  } else if (OB_FAIL(mem_mgr->get_ctx_hold(common::ObCtxIds::DDL_KV_CTX_ID, ctx_hold))) {
    LOG_WARN("get ctx hold failed", K(ret), K(ctx_hold));
  } else if (OB_FALSE_IT(tenant_limit = mem_mgr->get_limit())) {
  } else if (tenant_limit <= 0 || tenant_limit == INT64_MAX) {
    // ignore if tenant limit is not set
  } else {
    const int64_t threshold = static_cast<int64_t>((static_cast<__int128>(tenant_limit) * MEM_HOLD_PERCENT) / 100);
    if (ctx_hold < threshold) {
      // not reach memory threshold
    } else if (OB_ISNULL(dag_scheduler = MTL(share::ObTenantDagScheduler *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant dag scheduler is null", K(ret), "tenant_id", MTL_ID());
    } else if (OB_FALSE_IT(ddl_merge_dag_cnt = dag_scheduler->get_dag_count(share::ObDagType::DAG_TYPE_DDL_KV_MERGE))) {
    } else if (ddl_merge_dag_cnt >= DDL_MERGE_DAG_CNT_THRESHOLD) {
      // already reach merge task cnt limit
    } else {
      need_schedule = true;
      LOG_INFO("need schedule merge for memory", K(ctx_hold), K(threshold), K(ddl_merge_dag_cnt));
    }
  }
  return ret;
}

int ObDDLMergeScheduler::check_tablet_need_merge(ObTablet &tablet, ObDDLKvMgrHandle &ddl_kv_mgr_handle, bool &need_schedule_merge, ObDDLKVType &ddl_kv_type)
{
  int ret = OB_SUCCESS;
  need_schedule_merge = false;
  ddl_kv_type = ObDDLKVType::DDL_KV_INVALID;
  ObArray<ObDDLKVHandle> ddl_kv_handles;
  if (!ddl_kv_mgr_handle.is_valid()) {
    /* if ddl kv mgr handle is not valid, skip not need to get ddl kvs */
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->get_ddl_kvs(false /* for both frozen & active*/, ddl_kv_handles))) {
    LOG_WARN("failed to get ddl kv", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (!need_schedule_merge && tablet.get_tablet_meta().ddl_data_format_version_ < DDL_IDEM_DATA_FORMAT_VERSION &&
             OB_FAIL(check_need_merge_for_nidem_sn(tablet, ddl_kv_handles, need_schedule_merge, ddl_kv_type))) {
    LOG_WARN("failed to check need merge for nidem sn", K(ret));
  } else if (!need_schedule_merge && tablet.get_tablet_meta().ddl_data_format_version_ >= DDL_IDEM_DATA_FORMAT_VERSION &&
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
        if (OB_FAIL(schedule_tablet_ddl_major_merge(ls, tablet_handle))) {
          if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
            LOG_WARN("failed to schedule tablet ddl merge", K(ret), K(ls_id), K(tablet_id));
          } else {
            LOG_TRACE("schedule ddl major merge failed", K(ret), K(ls_id), K(tablet_id));
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
    if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
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
      } else if (param.is_commit_ && OB_FAIL(ObDDLMergeTaskUtils::final_freeze_ddl_kv(ls_id, tablet_handle.get_obj()->get_tablet_id()))) {
        LOG_WARN("try to freeze ddl kv failed", K(ret));
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
