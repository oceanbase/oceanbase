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
#include "storage/direct_load/ob_direct_load_ss_update_inc_major_dag.h"
#endif
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
  if (!GCTX.is_shared_storage_mode()) {
    if (OB_FAIL(schedule_tablet_ddl_inc_major_merge_for_sn(ls, tablet_handle))) {
      LOG_WARN("fail to schedule tablet ddl inc major merge for sn", KR(ret));
    }
  }
#ifdef OB_BUILD_SHARED_STORAGE
  else {
    if (OB_FAIL(schedule_tablet_ddl_inc_major_merge_for_ss(ls, tablet_handle))) {
      LOG_WARN("fail to schedule tablet ddl inc major merge for ss", KR(ret));
    }
  }
#endif
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
  int64_t snapshot_version = 0;
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

  if (OB_FAIL(ret)) {
  } else if (need_merge && cur_trans_id.is_valid() && cur_seq_no.is_valid()) {
    ObTabletDDLCompleteMdsUserData user_data;
    if (can_read && OB_FAIL(check_inc_major_merge_delay(tablet_handle, cur_trans_id, cur_seq_no, trans_version))) {
      LOG_WARN("failed to check inc major merge delay", KR(ret),
          K(tablet_id), K(cur_trans_id), K(cur_seq_no), K(trans_version));
    } else if (OB_FAIL(tablet_handle.get_obj()->get_inc_major_direct_load_info(
        share::SCN::max_scn(), ObTabletDDLCompleteMdsUserDataKey(cur_trans_id), user_data))) {
      LOG_WARN("failed to get inc major direct load info", KR(ret), K(tablet_id), K(cur_trans_id));
    } else if (OB_UNLIKELY(user_data.data_format_version_ < DATA_VERSION_4_4_1_0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected invalid data format version", KR(ret), K(user_data.data_format_version_));
    } else if (OB_UNLIKELY(can_read && !user_data.inc_major_commit_scn_.is_valid_and_not_min())) {
      can_read = false;
      FLOG_INFO("inc_major_commit_scn of MDS is not ready, set can_read to false",
          K(tablet_id), K(cur_trans_id), K(cur_seq_no), K(can_read), K(user_data));
    }

    if (OB_SUCC(ret)) {
      snapshot_version = can_read ? trans_version.get_val_for_tx() : user_data.snapshot_version_;
      param.direct_load_type_    = DIRECT_LOAD_INCREMENTAL_MAJOR;
      param.ls_id_               = ls->get_ls_id();
      param.tablet_id_           = tablet_id;
      param.is_commit_           = can_read;
      param.start_scn_           = user_data.start_scn_;
      param.data_format_version_ = user_data.data_format_version_;
      param.snapshot_version_    = snapshot_version;
      param.trans_id_ = cur_trans_id;
      param.seq_no_ = cur_seq_no;
      param.table_type_ = table_type;

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
  if (OB_UNLIKELY(!ddl_kv.is_freezed())) {
    // do nothing
  } else {
    const SCN &freeze_scn = ddl_kv.get_freeze_scn();
    const int64_t warn_time_interval = 60 * 60 * 1000 * 1000L; // 1 hour
    const int64_t freeze_time_us = freeze_scn.convert_to_ts();
    const int64_t current_time_us = ObTimeUtility::current_time();
    const int64_t time_interval = current_time_us - freeze_time_us;
    if (OB_UNLIKELY(time_interval > warn_time_interval)) {
      LOG_ERROR("ddl kv dump is delayed more than 1 hour", K(ddl_kv), K(freeze_scn),
          K(freeze_time_us), K(current_time_us), K(time_interval), K(warn_time_interval));
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
    const int64_t warn_time_interval = 60 * 60 * 1000 * 1000L; // 1 hour
    const int64_t commit_time_us = trans_version.convert_to_ts();
    const int64_t current_time_us = ObTimeUtility::current_time();
    const int64_t time_interval = current_time_us - commit_time_us;
    if (OB_UNLIKELY(time_interval > warn_time_interval)) {
      const ObTabletID &tablet_id = tablet_handle.get_obj()->get_tablet_id();
      LOG_ERROR("inc major merge is delayed more than 1 hour", K(tablet_id), K(cur_trans_id), K(cur_seq_no),
          K(trans_version), K(commit_time_us), K(current_time_us), K(time_interval), K(warn_time_interval));
    }
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObDDLMergeScheduler::schedule_ss_update_inc_major_and_gc_inc_major(
    const ObLSID &ls_id,
    const ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  bool need_schedule = false;
  if (!tenant_config->_enable_inc_major_direct_load) {
    // do nothing
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_handle));
  } else if (OB_FAIL(tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", KR(ret));
  } else if (OB_UNLIKELY(!table_store_wrapper.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table store wrapper is invalid", KR(ret));
  } else if (table_store_wrapper.get_member()->get_inc_major_sstables().count() > 0) {
    const ObSSTableArray &inc_major_sstables = table_store_wrapper.get_member()->get_inc_major_sstables();
    for (int64_t i = 0; i < inc_major_sstables.count(); ++i) {
      if (inc_major_sstables.at(i)->get_upper_trans_version() == INT64_MAX) {
        need_schedule = true;
        break;
      }
    }
  }

  if (OB_SUCC(ret) && need_schedule) {
    storage::ObDirectLoadSSUpdateIncMajorDagParam param(ls_id, tablet_handle.get_obj()->get_tablet_id());
    if (OB_FAIL(ObScheduleDagFunc::schedule_ss_update_inc_major_dag(param))) {
      LOG_WARN("fail to schedule ss update inc major dag", KR(ret), K(param));
    }
  }
  return ret;
}

int ObDDLMergeScheduler::schedule_ss_gc_inc_major_ddl_dump(
    ObLS *ls,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator sstable_iter;

  ObSEArray<int64_t, 4> gc_inc_major_ddl_scns;
  gc_inc_major_ddl_scns.set_attr(ObMemAttr(MTL_ID(), "GCIncDDLScn"));
  UpdateUpperTransParam upper_trans_param;
  upper_trans_param.gc_inc_major_ddl_scns_ = &gc_inc_major_ddl_scns;

  if (OB_UNLIKELY(nullptr == ls || !tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ls handle or tablet handle", KR(ret));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_inc_major_ddl_sstables(sstable_iter))) {
    LOG_WARN("fail to get inc major ddl sstables", KR(ret));
  } else if (sstable_iter.count() > 0) {
    ObITable *table = nullptr;
    ObSSTable *ddl_dump = nullptr;
    int64_t trans_state = 0;
    int64_t commit_version = 0;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(sstable_iter.get_next(table))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next table", KR(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_ISNULL(ddl_dump = static_cast<ObSSTable *>(table))
                           || !ddl_dump->is_inc_major_ddl_dump_sstable()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected ddl dump", KR(ret), KP(ddl_dump));
      } else if (OB_FAIL(ObIncMajorTxHelper::get_inc_major_commit_version(
                                             *ls,
                                             *ddl_dump,
                                             SCN::max_scn(),
                                             trans_state,
                                             commit_version))) {
        LOG_WARN("fail to get inc major commit version", KR(ret));
      } else if (ObTxData::COMMIT == trans_state || ObTxData::ABORT == trans_state) {
        // need gc
        if (OB_FAIL(gc_inc_major_ddl_scns.push_back(ddl_dump->get_end_scn().get_val_for_tx()))) {
          LOG_WARN("fail to push_back", KR(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret) && gc_inc_major_ddl_scns.count() > 0) {
    ObTablet &tablet = *tablet_handle.get_obj();
    bool ls_is_migration = false;
    ObStorageSnapshotInfo snapshot_info;
    int64_t multi_version_start = 0;
    ObStorageSchema *storage_schema = nullptr;
    int64_t rebuild_seq = 0;
    share::SCN transfer_scn = tablet.get_reorganization_scn();

    ObArenaAllocator tmp_arena("SS_INC_MAJOR", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    if (OB_FAIL(ls->check_ls_migration_status(ls_is_migration, rebuild_seq))) {
      LOG_WARN("fail to check ls migration status", KR(ret));
    } else if (ls_is_migration) {
    } else if (OB_FAIL(tablet.get_kept_snapshot_info(ls->get_min_reserved_snapshot(),
                                                     snapshot_info))) {
      LOG_WARN("fail to get kept snapshot info", KR(ret));
    } else if (FALSE_IT(multi_version_start = snapshot_info.snapshot_)) {
    } else if (OB_FAIL(tablet.load_storage_schema(tmp_arena, storage_schema))) {
      LOG_WARN("fail to load storage schema", KR(ret));
    } else {
      ObUpdateTableStoreParam param(tablet.get_snapshot_version(),
                                    multi_version_start,
                                    storage_schema,
                                    ls->get_rebuild_seq());
      param.set_upper_trans_param(upper_trans_param);
      ObTabletHandle new_tablet_handle;
      if (OB_FAIL(MTL(ObSSMetaService*)->update_tablet_table_store(ls->get_ls_id(),
                                                                   tablet.get_tablet_id(),
                                                                   transfer_scn,
                                                                   ObMetaUpdateReason::TABLET_UPDATE_DATA_INC_MAJOR_SSTABLE,
                                                                   -1,
                                                                   param,
                                                                   false))) {
        LOG_WARN("fail to update tablet table store", KR(ret));
      } else if (OB_FAIL(ls->update_tablet_table_store(tablet.get_tablet_id(), param, new_tablet_handle))) {
        LOG_WARN("fail to update tablet table store", KR(ret));
      }
    }
    FLOG_INFO("[SS INC MAJOR] schedule ss gc inc major ddl dump",
              KR(ret), "tablet_id", tablet_handle.get_obj()->get_tablet_id(), "ls_id", ls->get_ls_id(), K(gc_inc_major_ddl_scns));
    ObTabletObjLoadHelper::free(tmp_arena, storage_schema);
  }
  return ret;
}

int ObDDLMergeScheduler::schedule_task_if_split_src(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  bool is_split_src_tablet = false;
  ObSEArray<ObTabletSplitTask, 1> task_array;
  const ObTabletID &tablet_id = tablet_handle.get_obj()->get_tablet_id();
  if (OB_FAIL(ObLSTabletSplitScheduler::is_split_src_tablet(tablet_handle, is_split_src_tablet))) {
    LOG_WARN("failed to check if it is split src tablet", K(ret), K(tablet_handle));
  } else if (OB_UNLIKELY(is_split_src_tablet)) {
    ObTabletSplitTask tablet_split_task(MTL_ID(), tablet_id, ObTimeUtility::current_time(), TabletSplitTaskTatus::WAITING_SPLIT_DATA_COMPLEMENT);
    if (OB_FAIL(task_array.push_back(tablet_split_task))) {
      LOG_WARN("failed ot push back into task array", K(ret), K(tablet_split_task));
    } else if (OB_FAIL(ObLSTabletSplitScheduler::get_instance().push_task(task_array))) {
      LOG_WARN("failed to push back into task array", K(ret), K(tablet_id));
    }
  }
  return ret;
}
#endif

#ifdef OB_BUILD_SHARED_STORAGE
int ObDDLMergeScheduler::schedule_tablet_ddl_inc_major_merge_for_ss(
    ObLS *ls,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  ObArray<ObDDLKVHandle> ddl_kv_handles;
  ObDDLKVQueryParam ddl_kv_query_param;
  ddl_kv_query_param.ddl_kv_type_ = ObDDLKVType::DDL_KV_INC_MAJOR;
  if (OB_UNLIKELY(ls == nullptr || !tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(ls), K(tablet_handle));
  } else if (tablet_handle.get_obj()->get_tablet_meta().has_transfer_table()) {
    if (REACH_THREAD_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
      LOG_INFO("The tablet in the transfer process does not do ddl major_merge", K(tablet_handle));
    }
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    LOG_WARN("fail to get ddl kv mgr", KR(ret));
  } else if (OB_UNLIKELY(!ddl_kv_mgr_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl kv mgr is unexpected", KR(ret));
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->get_ddl_kvs(true, ddl_kv_handles, ddl_kv_query_param))) {
    LOG_WARN("fail to get ddl kvs", KR(ret));
  } else if (ddl_kv_handles.count() > 0) {
    ObDDLKVHandle first_ddl_kv_handle = ddl_kv_handles.at(0);
    if (OB_UNLIKELY(!first_ddl_kv_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("first ddl kv handle is invalid", KR(ret));
    } else if (!first_ddl_kv_handle.get_obj()->is_freezed()
               || !first_ddl_kv_handle.get_obj()->is_inc_major_ddl_kv()) {
      // do nothing
    } else {
      ObDDLKV *ddl_kv = first_ddl_kv_handle.get_obj();
      ObDDLTableMergeDagParam param;
      param.direct_load_type_    = DIRECT_LOAD_INCREMENTAL_MAJOR;
      param.ls_id_               = ls->get_ls_id();
      param.tablet_id_           = tablet_handle.get_obj()->get_tablet_id();
      param.is_commit_           = false;
      param.start_scn_           = ddl_kv->get_start_scn();
      param.data_format_version_ = ddl_kv->get_data_format_version();
      param.snapshot_version_    = ddl_kv->get_ddl_snapshot_version();
      param.trans_id_ = ddl_kv->get_trans_id();
      param.seq_no_ = ddl_kv->get_seq_no();
      param.table_type_ = ObITable::INC_MAJOR_DDL_DUMP_SSTABLE;
      if (OB_FAIL(EN_COMPACTION_DISABLE_INC_MAJOR_SSTABLE)) {
        FLOG_INFO("EN_COMPACTION_DISABLE_INC_MAJOR_SSTABLE: stop creating inc major sstable", K(ret));
        ret = OB_NO_NEED_MERGE;
      } else if (OB_FAIL(ObScheduleDagFunc::schedule_ddl_table_merge_dag(param))) {
        if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
          LOG_WARN("schedule ddl merge dag failed", K(ret), K(param));
        }
      }
      LOG_INFO("schedule_tablet_ddl_inc_major_merge", K(ret),
          K(param), K(tablet_handle.get_obj()->get_tablet_id()), K(ls->get_ls_id()), KPC(ddl_kv));
    }
  }
  return ret;
}
#endif

} // namespace storage
} // namespace oceanbase
