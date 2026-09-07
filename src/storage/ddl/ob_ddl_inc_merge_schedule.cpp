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
#include "storage/compaction/ob_schedule_dag_func.h"
#include "storage/ddl/ob_ddl_merge_task_utils.h"
#include "storage/ddl/ob_ddl_merge_task_v2.h"
#include "storage/ddl/ob_direct_load_mgr_utils.h"
#include "storage/compaction/ob_uncommit_tx_info.h"
#include "storage/compaction/ob_partition_merge_policy.h"

#include "storage/ddl/ob_inc_ddl_merge_task_utils.h"

using namespace oceanbase::observer;
using namespace oceanbase::share::schema;
using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::transaction;
using namespace oceanbase::compaction;

namespace oceanbase
{
namespace storage
{

ERRSIM_POINT_DEF(EN_COMPACTION_DISABLE_INC_MAJOR_SSTABLE);

ERRSIM_POINT_DEF(MERGE_DISABLE_INC_MAJOR_SSTABLE);
/*
 * check need to merge for inc major
 * only need to check from dump & kv
*/
int ObDDLMergeScheduler::check_need_merge_for_inc_major(ObTablet &tablet, ObArray<ObDDLKVHandle> &ddl_kvs, bool &need_schedule_merge, ObDDLKVType &ddl_kv_type)
{
  int ret = OB_SUCCESS;
  ObTabletDDLCompleteMdsUserData user_data;
  if (ObDDLKVType::DDL_KV_INVALID != ddl_kv_type || need_schedule_merge) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument, return param should be invalid", K(ret), K(ddl_kv_type), K(need_schedule_merge));
  } else if (tablet.get_tablet_meta().has_transfer_table()) {
    if (REACH_THREAD_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
      LOG_INFO("The tablet in the transfer process does not do ddl major_merge", K(tablet.get_tablet_id()));
    }
  } else {
    /* check need to merge from ddl kv */
    if (ddl_kvs.empty()) {
      /* skip, empty ddl kv, do nothing */
    } else if (ObDDLKVType::DDL_KV_INC_MAJOR == ddl_kvs.at(0).get_obj()->get_ddl_kv_type()) {
      /* full direct load, need to merge */
      need_schedule_merge = true;
      ddl_kv_type = ObDDLKVType::DDL_KV_INC_MAJOR;
    }

    /* check need to merge from dump */
    if (need_schedule_merge) {
      /* skip, do nothing */
    } else {
      SMART_VAR(ObTableStoreIterator, ddl_sstable_iter) {
      ObITable *first_ddl_sstable = nullptr;
      ObTxSEQ cur_seq_no;
      ObTransID cur_trans_id;
      if (OB_FAIL(tablet.get_inc_major_ddl_sstables(ddl_sstable_iter))) {
        LOG_WARN("failed to get ddl sstable", K(ret));
      } else if (ddl_sstable_iter.count() == 0) {
        /* skip, empty ddl sstable, do nothing */
      } else if (OB_FAIL(ddl_sstable_iter.get_boundary_table(false/*is_last*/, first_ddl_sstable))) {
          LOG_WARN("failed to get boundary table", KR(ret));
      } else {
        const ObSSTable *sstable = static_cast<const ObSSTable *>(first_ddl_sstable);
        if (OB_FAIL(ObIncMajorTxHelper::get_trans_id_and_seq_no_from_sstable(
            sstable, cur_trans_id, cur_seq_no))) {
          LOG_WARN("failed to get trans id and seq no from sstable", KR(ret), KPC(sstable));
        } else {
          need_schedule_merge = true;
          ddl_kv_type = ObDDLKVType::DDL_KV_INC_MAJOR;
        }
      }
    }
    }
  }
  return ret;
}

int ObDDLMergeScheduler::schedule_tablet_ddl_inc_major_merge(
    ObLSHandle &ls_handle,
    ObTabletHandle &tablet_handle)
{
  return schedule_tablet_ddl_inc_major_merge(ls_handle.get_ls(), tablet_handle);
}

int ObDDLMergeScheduler::schedule_tablet_ddl_inc_major_merge(
    ObLS *ls,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(schedule_tablet_ddl_inc_major_merge_for_sn(ls, tablet_handle))) {
    LOG_WARN("fail to schedule tablet ddl inc major merge for sn", KR(ret));
  }
  return ret;
}

int ObDDLMergeScheduler::schedule_tablet_ddl_inc_major_merge(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tablet_id));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ls service", KR(ret), KP(ls_service), K(MTL_ID()));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get ls", KR(ret), K(ls_id));
  } else if (OB_UNLIKELY(!ls_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid ls handle", KR(ret), K(ls_handle), K(ls_id));
  } else if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(ls_id, tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet handle", KR(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(schedule_tablet_ddl_inc_major_merge(ls_handle, tablet_handle))) {
    LOG_WARN("failed to schedule tablet ddl inc major merge", KR(ret), K(ls_id), K(tablet_id));
  }
  return ret;
}

ERRSIM_POINT_DEF(EN_GET_DDL_TYPE_FROM_EMPTY_DDL_KV);
int ObDDLMergeScheduler::schedule_tablet_ddl_inc_major_merge_for_sn(
    ObLS *ls,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObDDLTableMergeDagParam param;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  ObArray<ObDDLKVHandle> frozen_ddl_kvs;
  ObTableStoreIterator ddl_table_iter;
  ObTransID cur_trans_id;
  ObTxSEQ cur_seq_no;
  SCN trans_version;
  bool can_read = false;
  bool need_merge = false;
  ObITable::TableType table_type = ObITable::MAX_TABLE_TYPE;
  ObTabletID tablet_id;
  int64_t trans_state;

  if (OB_UNLIKELY(ls == nullptr || !tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(ls), K(tablet_handle));
  } else if (FALSE_IT(tablet_id = tablet_handle.get_obj()->get_tablet_id())) {
  } else if (OB_FAIL(EN_COMPACTION_DISABLE_INC_MAJOR_SSTABLE)) {
    FLOG_INFO("EN_COMPACTION_DISABLE_INC_MAJOR_SSTABLE: stop inc major merge", K(ret),
              K(ls->get_ls_id()), K(tablet_id));
    ret = OB_NO_NEED_MERGE;
  } else if (tablet_handle.get_obj()->get_tablet_meta().has_transfer_table()) {
    if (REACH_THREAD_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
      LOG_INFO("The tablet in the transfer process does not do ddl major_merge", K(tablet_handle), K(tablet_id));
    }
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle, true /* need create, since real need merge */))) {
    LOG_WARN("failed to get ddl kv mgr", KR(ret), K(tablet_id));
  } else if (OB_FAIL(ObIncDDLMergeTaskUtils::get_all_inc_major_ddl_sstables(tablet_handle.get_obj(), ddl_table_iter))) {
    LOG_WARN("failed to get all inc major ddl sstables", KR(ret), K(tablet_handle), K(tablet_id));
  } else if (OB_FAIL(ObIncDDLMergeTaskUtils::get_all_frozen_ddl_kvs(ddl_kv_mgr_handle, frozen_ddl_kvs))) {
    LOG_WARN("failed to get all frozen ddl kvs", KR(ret), K(ddl_kv_mgr_handle), K(tablet_id));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(MERGE_DISABLE_INC_MAJOR_SSTABLE)) {
    FLOG_INFO("MERGE_DISABLE_INC_MAJOR_SSTABLE: stop creating inc major sstable", K(ret), K(ls->get_ls_id()), K(tablet_id));
  } else if (ddl_table_iter.count() > 0) {
    ObITable *first_ddl_sstable = nullptr;
    if (OB_FAIL(ddl_table_iter.get_boundary_table(false/*is_last*/, first_ddl_sstable))) {
      LOG_WARN("failed to get boundary table", KR(ret));
    } else if (OB_ISNULL(first_ddl_sstable)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null ddl sstable", KR(ret), KP(first_ddl_sstable));
    } else {
      const ObSSTable *sstable = static_cast<const ObSSTable *>(first_ddl_sstable);
      if (OB_FAIL(ObIncMajorTxHelper::get_trans_id_and_seq_no_from_sstable(
          sstable, cur_trans_id, cur_seq_no))) {
        LOG_WARN("failed to get trans id and seq no from sstable", KR(ret), KPC(sstable));
      } else if (OB_FAIL(ObIncMajorTxHelper::check_inc_major_trans_can_read(ls,
          cur_trans_id, cur_seq_no, SCN::max_scn()/*read_scn*/, trans_state, can_read, trans_version))) {
        LOG_WARN("failed to check inc major trans can read", KR(ret),
            KPC(ls), K(tablet_id), K(cur_trans_id), K(cur_seq_no));
      } else if (can_read) {
        table_type = sstable->get_key().table_type_;
        need_merge = true;
      }
    }
  }

  if (OB_FAIL(ret) || need_merge) {
  } else if (!frozen_ddl_kvs.empty()) {
    ObDDLKV *ddl_kv_ptr = frozen_ddl_kvs.at(0).get_obj();
    cur_trans_id = ddl_kv_ptr->get_trans_id();
    cur_seq_no = ddl_kv_ptr->get_seq_no();
    if (OB_FAIL(check_ddl_kv_dump_delay(*ddl_kv_ptr))) {
      LOG_WARN("failed to check ddl kv dump delay", KR(ret), K(tablet_id), KPC(ddl_kv_ptr));
    } else if (OB_UNLIKELY(!ddl_kv_ptr->is_inc_major_ddl_kv())) {
      ret = OB_NO_NEED_MERGE;
      LOG_INFO("first ddl kv is not inc major, no need to merge", KR(ret), K(tablet_id), KPC(ddl_kv_ptr));
    } else if ((ddl_table_iter.count() == 0) && OB_FAIL(ObIncMajorTxHelper::check_inc_major_trans_can_read(ls,
        cur_trans_id, cur_seq_no, SCN::max_scn()/*read_scn*/, trans_state, can_read, trans_version))) {
      LOG_WARN("failed to check inc major trans can read", KR(ret),
          KPC(ls), K(tablet_id), K(cur_trans_id), K(cur_seq_no));
    } else {
      need_merge = true;
    }

    if (OB_FAIL(ret)) {
    } else if (ddl_kv_ptr->get_ddl_memtables().count() > 0) {
      table_type = ddl_kv_ptr->get_ddl_memtables().at(0)->get_key().table_type_;

#ifdef ERRSIM
      ret = EN_GET_DDL_TYPE_FROM_EMPTY_DDL_KV;
      if (OB_FAIL(ret)) {
        ret = OB_SUCCESS;
        FLOG_INFO("EN_GET_DDL_TYPE_FROM_EMPTY_DDL_KV", K(ret), "tablet_id", tablet_id);
        // try to get table type from empty ddl kv
        for (int64_t idx = 1; idx < frozen_ddl_kvs.count(); ++idx) {
          ObDDLKV *ddl_kv = frozen_ddl_kvs.at(idx).get_obj();
          if (ddl_kv->get_trans_id() != cur_trans_id || ddl_kv->get_seq_no() != cur_seq_no) {
            break;
          } else if (ddl_kv->get_ddl_memtables().count() == 0) {
            table_type = ddl_kv->get_key().table_type_;
            FLOG_INFO("EN_GET_DDL_TYPE_FROM_EMPTY_DDL_KV get table type from empty ddl kv", K(ret), K(table_type), "tablet_id", tablet_id);
            break;
          }
        }
      }
#endif
    } else { // empty ddl kv
      // get table type from ddl kv directly when ddl kv is empty
      table_type = ddl_kv_ptr->get_key().table_type_;
    }
  }

  if (OB_FAIL(ret) || !need_merge) {
  } else if (OB_UNLIKELY(!cur_trans_id.is_valid() || !cur_seq_no.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid cur_trans_id or cur_seq_no", KR(ret), K(cur_trans_id), K(cur_seq_no));
  } else {
    ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "DdlIncCommitCb"));
    ObTabletDDLCompleteMdsUserData user_data;
    if (can_read && OB_FAIL(check_inc_major_merge_delay(tablet_handle, cur_trans_id, cur_seq_no, trans_version))) {
      LOG_WARN("failed to check inc major merge delay", KR(ret),
          K(tablet_id), K(cur_trans_id), K(cur_seq_no), K(trans_version));
    } else if (OB_FAIL(tablet_handle.get_obj()->get_inc_major_direct_load_info(
        share::SCN::max_scn(), allocator, ObTabletDDLCompleteMdsUserDataKey(cur_trans_id), user_data))) {
      LOG_WARN("failed to get inc major direct load info", KR(ret), K(tablet_id), K(cur_trans_id));
    } else if (OB_UNLIKELY(!is_data_version_support_inc_major_direct_load(user_data.data_format_version_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected invalid data format version", KR(ret), K(user_data.data_format_version_));
    } else if (OB_UNLIKELY(can_read && !user_data.inc_major_commit_scn_.is_valid_and_not_min())) {
      can_read = false;
      FLOG_INFO("inc_major_commit_scn of MDS is not ready, set can_read to false",
          K(tablet_id), K(cur_trans_id), K(cur_seq_no), K(can_read), K(user_data));
    }

    if (OB_SUCC(ret)) {
      param.direct_load_type_    = DIRECT_LOAD_INCREMENTAL_MAJOR;
      param.ls_id_               = ls->get_ls_id();
      param.tablet_id_           = tablet_id;
      param.is_commit_           = can_read;
      param.start_scn_           = user_data.start_scn_;
      param.data_format_version_ = user_data.data_format_version_;
      param.snapshot_version_    = user_data.snapshot_version_;
      param.trans_id_ = cur_trans_id;
      param.seq_no_ = cur_seq_no;
      param.table_type_ = table_type;
      param.inc_major_trans_version_ = can_read ? trans_version.get_val_for_tx() : 0;

      if (OB_FAIL(ObScheduleDagFunc::schedule_ddl_table_merge_dag(param))) {
        if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
          LOG_WARN("schedule ddl merge dag failed", K(ret), K(param));
        }
      }
      LOG_INFO("schedule_tablet_ddl_inc_major_merge", K(ret), K(param), K(cur_trans_id), K(cur_seq_no),
          K(ddl_table_iter.count()), K(frozen_ddl_kvs.count()), K(user_data), K(common::lbt()));
    }
  }
  return ret;
}

int ObDDLMergeScheduler::check_ddl_kv_dump_delay(ObDDLKV &ddl_kv)
{
  int ret = OB_SUCCESS;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (OB_UNLIKELY(!tenant_config->_enable_inc_major_direct_load || !ddl_kv.is_freezed())) {
    // do nothing
  } else {
    const SCN &freeze_scn = ddl_kv.get_freeze_scn();
    const int64_t warn_time_interval = 2 * 60 * 60 * 1000 * 1000L; // 2 hours
    const int64_t freeze_time_us = freeze_scn.convert_to_ts();
    const int64_t current_time_us = ObTimeUtility::current_time();
    const int64_t time_interval = current_time_us - freeze_time_us;
    if (OB_UNLIKELY(time_interval > warn_time_interval)) {
      LOG_ERROR("ddl kv dump is delayed more than 2 hours", K(ddl_kv), K(freeze_scn),
          K(freeze_time_us), K(current_time_us),
          "delay time (minutes)", time_interval / (60 * 1000 * 1000L),
          "threshold time (minutes)", warn_time_interval / (60 * 1000 * 1000L));
    }
  }
  return ret;
}

int ObDDLMergeScheduler::check_inc_major_merge_delay(
    const ObTabletHandle &tablet_handle,
    const ObTransID &cur_trans_id,
    const ObTxSEQ &cur_seq_no,
    const SCN &trans_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_handle.is_valid()
               || !cur_trans_id.is_valid()
               || !cur_seq_no.is_valid()
               || !trans_version.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tablet_handle), K(cur_trans_id), K(cur_seq_no), K(trans_version));
  } else {
    const int64_t warn_time_interval = 2 * 60 * 60 * 1000 * 1000L; // 2 hours
    const int64_t commit_time_us = trans_version.convert_to_ts();
    const int64_t current_time_us = ObTimeUtility::current_time();
    const int64_t time_interval = current_time_us - commit_time_us;
    if (OB_UNLIKELY(time_interval > warn_time_interval)) {
      const ObTabletID &tablet_id = tablet_handle.get_obj()->get_tablet_id();
      LOG_ERROR("inc major merge is delayed more than 2 hours", K(tablet_id), K(cur_trans_id), K(cur_seq_no),
          K(trans_version), K(commit_time_us), K(current_time_us),
          "delay time (minutes)", time_interval / (60 * 1000 * 1000L),
          "threshold time (minutes)", warn_time_interval / (60 * 1000 * 1000L));
    }
  }
  return ret;
}



} // namespace storage
} // namespace oceanbase
