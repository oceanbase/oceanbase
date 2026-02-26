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

#include "storage/ddl/ob_inc_ddl_merge_task_utils.h"
#include "share/ob_ddl_checksum.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "share/ob_ddl_sim_point.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/ob_storage_schema_util.h"
#include "storage/compaction/ob_schedule_dag_func.h"
#include "storage/ddl/ob_direct_load_struct.h"
#include "storage/ddl/ob_direct_load_mgr_utils.h"
#include "storage/ddl/ob_ddl_merge_task.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/compaction/ob_partition_merge_policy.h"
#include "storage/ddl/ob_ddl_merge_schedule.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/compaction_v2/ob_ss_compact_helper.h"
#include "storage/incremental/ob_shared_meta_service.h"
#endif

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

int ObIncDDLMergeTaskUtils::prepare_freeze_inc_major_ddl_kv(
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tablet_handle));
  } else {
    ObDDLKvMgrHandle ddl_kv_mgr_handle;

#ifdef OB_BUILD_SHARED_STORAGE
    if (GCTX.is_shared_storage_mode()) {
      // ss模式
      // do nothing
    } else {
#endif
      // sn模式
      if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle, true/*try_create*/))) {
        LOG_WARN("create ddl kv mgr failed", K(ret));
      } else {
        static const int64_t max_retry_cnt = 100;
        int64_t retry_cnt = 0;
        while (OB_SUCC(ret)) {
          if (ddl_kv_mgr_handle.get_obj()->can_freeze()) {
            break;
          } else if (retry_cnt < max_retry_cnt) {
            ++retry_cnt;
            ob_usleep(1000);
          } else {
            ret = OB_EAGAIN;
          }
        }
      }
#ifdef OB_BUILD_SHARED_STORAGE
    }
#endif

  }
  return ret;
}

ERRSIM_POINT_DEF(EN_FREEZE_EMPTY_DDL_KV);
int ObIncDDLMergeTaskUtils::freeze_inc_major_ddl_kv(
    const ObTabletHandle &tablet_handle,
    const SCN &commit_scn,
    const ObTransID &trans_id,
    const ObTxSEQ &seq_no,
    const int64_t snapshot_version,
    const uint64_t data_format_version,
    const bool is_replay)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_handle.is_valid()
               || !commit_scn.is_valid()
               || !trans_id.is_valid()
               || !seq_no.is_valid()
               || (snapshot_version <= 0)
               || !is_data_version_support_inc_major_direct_load(data_format_version))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguemnts", KR(ret), K(tablet_handle), K(commit_scn),
        K(trans_id), K(seq_no), K(snapshot_version), K(data_format_version));
  } else {
    ObDDLKvMgrHandle ddl_kv_mgr_handle;
    SCN freeze_scn;

#ifdef OB_BUILD_SHARED_STORAGE
    if (GCTX.is_shared_storage_mode()) {
      // ss模式
      freeze_scn = SCN::min_scn();
      if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
        if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
          LOG_WARN("fail to get ddl kv mgr", KR(ret), K(tablet_handle));
        } else {
          // There is no ddl kv mgr, don't need to freeze ddl kv
          LOG_INFO("ddl kv mgr not exist", K(tablet_handle));
          ret = OB_SUCCESS;
        }
      }
    } else {
#endif
      // sn模式
      freeze_scn = commit_scn;
      if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle, true/*try_create*/))) {
        LOG_WARN("create ddl kv mgr failed", K(ret));
      }
#ifdef OB_BUILD_SHARED_STORAGE
    }
#endif

    if (OB_SUCC(ret) && ddl_kv_mgr_handle.is_valid()) {
      ObITable::TableType table_type = ObITable::MAX_TABLE_TYPE;
      SCN mock_start_scn;
      mock_start_scn.convert_for_tx(SS_DDL_START_SCN_VAL);
      ObStorageSchema *storage_schema = nullptr;
      ObArenaAllocator arena("FreezeIncDdlKv", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
      if (OB_FAIL(tablet_handle.get_obj()->load_storage_schema(arena, storage_schema))) {
        LOG_WARN("load storage schema failed", K(ret), K(tablet_handle));
      } else if (OB_ISNULL(storage_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null storage schema", K(ret), K(tablet_handle));
      } else if (!is_replay) {
        // leader上根据表的属性确定
        table_type = storage_schema->is_row_store() ? ObITable::INC_MAJOR_DDL_MEM_SSTABLE : ObITable::INC_MAJOR_DDL_MEM_CO_SSTABLE;
      } else {
        // follower上根据tablet上的属性确定
        if (!storage_schema->is_user_data_table()) {
          // 非用户表只有行存
          table_type = ObITable::INC_MAJOR_DDL_MEM_SSTABLE;
        } else if (storage_schema->is_row_store() &&
                   CS_REPLICA_VISIBLE_AND_REPLAY_COLUMN !=
                     tablet_handle.get_obj()->get_tablet_meta().ddl_replay_status_) {
          table_type = ObITable::INC_MAJOR_DDL_MEM_SSTABLE;
        } else {
          table_type = ObITable::INC_MAJOR_DDL_MEM_CO_SSTABLE;
        }
      }

#ifdef ERRSIM
      if (OB_SUCC(ret)) {
        ret = EN_FREEZE_EMPTY_DDL_KV;
        if (OB_FAIL(ret)) {
          ret = OB_SUCCESS;
          if (FAILEDx(ddl_kv_mgr_handle.get_obj()->freeze_ddl_kv(mock_start_scn,
                                                                 snapshot_version,
                                                                 data_format_version,
                                                                 SCN::min_scn(),
                                                                 ObDDLKVType::DDL_KV_INC_MAJOR,
                                                                 trans_id,
                                                                 seq_no,
                                                                 table_type))) {
            LOG_WARN("failed to freeze ddl kv", KR(ret), K(freeze_scn), K(trans_id), K(seq_no), K(table_type));
          }
          FLOG_INFO("EN_FREEZE_EMPTY_DDL_KV freeze ddl kv", K(ret), K(freeze_scn), K(trans_id), K(seq_no), K(table_type), "tablet_id", tablet_handle.get_obj()->get_tablet_id());
        }
      }
#endif

      if (FAILEDx(ddl_kv_mgr_handle.get_obj()->freeze_ddl_kv(mock_start_scn,
                                                             snapshot_version,
                                                             data_format_version,
                                                             freeze_scn,
                                                             ObDDLKVType::DDL_KV_INC_MAJOR,
                                                             trans_id,
                                                             seq_no,
                                                             table_type))) {
        LOG_WARN("failed to freeze ddl kv", KR(ret), K(mock_start_scn), K(snapshot_version),
            K(data_format_version), K(freeze_scn), K(trans_id), K(seq_no), K(table_type));
      }
      if (OB_NOT_NULL(storage_schema)) {
        ObTabletObjLoadHelper::free(arena, storage_schema);
      }
    }
  }
  return ret;
}


int ObIncDDLMergeTaskUtils::update_tablet_table_store(
    ObLS *ls,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ls == nullptr || !tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(ls), K(tablet_handle));
  } else {
    ObArenaAllocator arena("INC_MAJOR_UTS", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObStorageSchema *storage_schema = nullptr;
    if (OB_FAIL(tablet_handle.get_obj()->load_storage_schema(arena, storage_schema))) {
      LOG_WARN("fail to load storage schema", KR(ret), K(tablet_handle));
    } else if (OB_ISNULL(storage_schema) || OB_UNLIKELY(!storage_schema->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected invalid storage schema", KR(ret), KPC(storage_schema));
    } else {
      ObTabletHandle new_tablet_handle;
      ObUpdateTableStoreParam param(tablet_handle.get_obj()->get_snapshot_version(),
                                    ObVersionRange::MIN_VERSION, // multi_version_start
                                    storage_schema,
                                    ls->get_rebuild_seq());
      param.ddl_info_.keep_old_ddl_sstable_ = true;
      if (OB_FAIL(ls->update_tablet_table_store(
          tablet_handle.get_obj()->get_tablet_id(), param, new_tablet_handle))) {
        LOG_WARN("failed to update tablet table store", KR(ret),
            K(ls), K(tablet_handle.get_obj()->get_tablet_id()), K(param));
      }
    }
    ObTabletObjLoadHelper::free(arena, storage_schema);
  }
  return ret;
}

int ObIncDDLMergeTaskUtils::update_tablet_table_store_with_storage_schema(
    ObLS *ls,
    ObTabletHandle &tablet_handle,
    const ObStorageSchema *storage_schema)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ls)
      || OB_UNLIKELY(!tablet_handle.is_valid()
      || OB_ISNULL(storage_schema)
      || OB_UNLIKELY(!storage_schema->is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(ls), K(tablet_handle), KPC(storage_schema));
  } else {
    ObTabletHandle new_tablet_handle;
    ObUpdateTableStoreParam param(tablet_handle.get_obj()->get_snapshot_version(),
                                  ObVersionRange::MIN_VERSION, // multi_version_start
                                  storage_schema,
                                  ls->get_rebuild_seq());
    param.ddl_info_.keep_old_ddl_sstable_ = true;
    if (OB_FAIL(ls->update_tablet_table_store(
        tablet_handle.get_obj()->get_tablet_id(), param, new_tablet_handle))) {
      LOG_WARN("failed to update tablet table store", KR(ret),
          K(ls), K(tablet_handle.get_obj()->get_tablet_id()), K(param));
    }
  }
  return ret;
}
#ifdef OB_BUILD_SHARED_STORAGE
int ObIncDDLMergeTaskUtils::link_inc_major(
    ObLS *ls,
    ObTabletHandle &tablet_handle,
    const ObTransID &trans_id,
    const ObTxSEQ &seq_no)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ls == nullptr || !tablet_handle.is_valid() || !trans_id.is_valid() ||
                  !seq_no.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(ls), K(tablet_handle), K(trans_id), K(seq_no));
  } else {
    const ObLSID &ls_id = ls->get_ls_id();
    const ObTabletID &tablet_id = tablet_handle.get_obj()->get_tablet_id();
    const int64_t snapshot_version = tablet_handle.get_obj()->get_snapshot_version();
    const share::SCN transfer_scn = tablet_handle.get_obj()->get_reorganization_scn();
    ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "LinkIncMajor"));
    ObTabletHandle ss_tablet_handle;
    share::SCN ss_tablet_version;
    bool is_exist = false;
    if (OB_FAIL(MTL(ObSSMetaService*)->get_tablet(ls_id,
                                                  tablet_id,
                                                  transfer_scn,
                                                  allocator,
                                                  ss_tablet_handle,
                                                  ss_tablet_version))) {
      LOG_WARN("fail to get ss tablet handle", KR(ret), K(ls_id), K(tablet_id), K(transfer_scn));
    } else if (OB_UNLIKELY(!ss_tablet_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ss tablet handle is invalid", KR(ret), K(ss_tablet_handle));
    } else if (OB_FAIL(ObIncMajorTxHelper::check_inc_major_exist(ss_tablet_handle,
                                                                 trans_id,
                                                                 seq_no,
                                                                 is_exist))) {
      LOG_WARN("fail to check inc major exist", KR(ret), K(ss_tablet_handle), K(trans_id), K(seq_no));
    } else if (is_exist && OB_FAIL(SSCompactHelper::link_inc_major(ls_id,
                                                                   tablet_id,
                                                                   snapshot_version,
                                                                   trans_id,
                                                                   seq_no))) {
      LOG_WARN("fail to link inc major", KR(ret), K(ls_id), K(tablet_id),
               K(snapshot_version), K(trans_id), K(seq_no));
    } else {
      FLOG_INFO("[SS INC MAJOR] link inc major succeed", K(ls_id), K(tablet_id), K(trans_id),
                K(seq_no), K(is_exist));
    }
  }
  return ret;
}

#endif

int ObIncDDLMergeTaskUtils::get_all_inc_major_ddl_sstables(
    const ObTablet *tablet,
    ObTableStoreIterator &ddl_table_iter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tablet)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), KP(tablet));
  } else if (OB_FAIL(tablet->get_inc_major_ddl_sstables(ddl_table_iter))) {
    LOG_WARN("failed to get inc major ddl sstables", K(ret));
  }
  return ret;
}

int ObIncDDLMergeTaskUtils::get_inc_major_ddl_sstables(
    const ObTransID &trans_id,
    const ObTxSEQ &seq_no,
    const ObTablet *tablet,
    ObTableStoreIterator &ddl_table_iter)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!trans_id.is_valid() || !seq_no.is_valid()) || OB_ISNULL(tablet)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(trans_id), K(seq_no), KP(tablet));
  } else if (OB_FAIL(tablet->get_inc_major_ddl_sstables(ddl_table_iter, trans_id, seq_no))) {
    LOG_WARN("failed to get inc major ddl sstables", K(ret), K(trans_id), K(seq_no));
  }
  return ret;
}

int ObIncDDLMergeTaskUtils::get_all_frozen_ddl_kvs(
    ObDDLKvMgrHandle &ddl_kv_mgr_handle,
    ObIArray<ObDDLKVHandle> &frozen_ddl_kvs)
{
  int ret = OB_SUCCESS;
  ObDDLKVQueryParam ddl_kv_query_param;
  ddl_kv_query_param.ddl_kv_type_ = ObDDLKVType::DDL_KV_INVALID;

  if (OB_UNLIKELY(!ddl_kv_mgr_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(ddl_kv_mgr_handle));
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->get_ddl_kvs(
      true/*frozen_only*/, frozen_ddl_kvs, ddl_kv_query_param))) {
    LOG_WARN("failed to get frozen ddl kvs", K(ret), K(ddl_kv_query_param));
  }
  return ret;
}

int ObIncDDLMergeTaskUtils::get_all_frozen_inc_major_ddl_kvs(
    ObDDLKvMgrHandle &ddl_kv_mgr_handle,
    ObIArray<ObDDLKVHandle> &frozen_ddl_kvs)
{
  int ret = OB_SUCCESS;
  ObDDLKVQueryParam ddl_kv_query_param;
  ddl_kv_query_param.ddl_kv_type_ = ObDDLKVType::DDL_KV_INC_MAJOR;

  if (OB_UNLIKELY(!ddl_kv_mgr_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(ddl_kv_mgr_handle));
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->get_ddl_kvs(
      true/*frozen_only*/, frozen_ddl_kvs, ddl_kv_query_param))) {
    LOG_WARN("failed to get frozen ddl kvs", K(ret), K(ddl_kv_query_param));
  }
  return ret;
}

int ObIncDDLMergeTaskUtils::get_frozen_inc_major_ddl_kvs(
    const ObTransID &trans_id,
    const ObTxSEQ &seq_no,
    ObDDLKvMgrHandle &ddl_kv_mgr_handle,
    ObIArray<ObDDLKVHandle> &frozen_ddl_kvs)
{
  int ret = OB_SUCCESS;
  ObDDLKVQueryParam ddl_kv_query_param;
  ddl_kv_query_param.ddl_kv_type_ = ObDDLKVType::DDL_KV_INC_MAJOR;
  ddl_kv_query_param.trans_id_ = trans_id;
  ddl_kv_query_param.seq_no_ = seq_no;

  if (OB_UNLIKELY(!trans_id.is_valid() || !seq_no.is_valid() || !ddl_kv_mgr_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(trans_id), K(seq_no), K(ddl_kv_mgr_handle));
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->get_ddl_kvs(
      true/*frozen_only*/, frozen_ddl_kvs, ddl_kv_query_param))) {
    LOG_WARN("failed to get frozen ddl kvs", K(ret), K(ddl_kv_query_param));
  }
  return ret;
}

int ObIncDDLMergeTaskUtils::get_all_inc_major_ddl_kvs(
  const ObDDLKvMgrHandle &ddl_kv_mgr_handle,
  ObIArray<ObDDLKVHandle> &inc_major_ddl_kvs)
{
  int ret = OB_SUCCESS;
  ObDDLKVQueryParam ddl_kv_query_param;
  ddl_kv_query_param.ddl_kv_type_ = ObDDLKVType::DDL_KV_INC_MAJOR;
  if (OB_UNLIKELY(!ddl_kv_mgr_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(ddl_kv_mgr_handle));
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->get_ddl_kvs(
      false/*frozen_only*/, inc_major_ddl_kvs, ddl_kv_query_param))) {
    LOG_WARN("fail to get ddl kvs", K(ret), K(ddl_kv_query_param));
  }
  return ret;
}

int ObIncDDLMergeTaskUtils::get_all_inc_major_ddl_kvs(
  const ObTablet *tablet,
  ObIArray<ObDDLKVHandle> &inc_major_ddl_kvs)
{
  int ret = OB_SUCCESS;
  inc_major_ddl_kvs.reset();
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  if (OB_FAIL(tablet->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
      LOG_WARN("failed to get ddl kv mgr", K(ret), KPC(tablet));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(get_all_inc_major_ddl_kvs(ddl_kv_mgr_handle, inc_major_ddl_kvs))) {
    LOG_WARN("fail to get all inc major ddl kvs", K(ret), K(ddl_kv_mgr_handle));
  }
  return ret;
}

int ObIncDDLMergeTaskUtils::check_unfinished_inc_major_before_merge(
    ObLS *ls,
    const ObTablet *tablet,
    const int64_t merge_snapshot_version,
    bool &exists_unfinished_inc_major)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator ddl_table_iter;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  ObArray<ObDDLKVHandle> frozen_ddl_kvs;
  transaction::ObTransID prev_trans_id;
  transaction::ObTransID cur_trans_id;
  transaction::ObTxSEQ prev_seq_no;
  transaction::ObTxSEQ cur_seq_no;
  ObTabletID tablet_id;
  int64_t trans_state;
  SCN trans_version;
  bool can_read = false;
  exists_unfinished_inc_major = false;

  if (GCTX.is_shared_storage_mode()) {
    // ss mode don't need to check
  } else if (OB_UNLIKELY(OB_ISNULL(ls) || OB_ISNULL(tablet) || !tablet->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ls or invalid tablet", KR(ret), KP(ls), KPC(tablet));
  } else if (FALSE_IT(tablet_id = tablet->get_tablet_id())) {
  } else if (OB_FAIL(ObIncDDLMergeTaskUtils::get_all_inc_major_ddl_sstables(tablet, ddl_table_iter))) {
    LOG_WARN("failed to get all inc major ddl sstables", KR(ret), KPC(tablet));
  } else if (OB_FAIL(const_cast<ObTablet *>(tablet)->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get ddl kv mgr", KR(ret), KPC(tablet));
    }
  } else if (OB_FAIL(ObIncDDLMergeTaskUtils::get_all_frozen_inc_major_ddl_kvs(ddl_kv_mgr_handle, frozen_ddl_kvs))) {
    LOG_WARN("failed to get all frozen inc major ddl kvs", KR(ret), K(ddl_kv_mgr_handle), KPC(tablet));
  }

  if (OB_FAIL(ret)) {
  } else if (ddl_table_iter.count() > 0) {
    prev_trans_id.reset();
    prev_seq_no.reset();
    for (int64_t i = 0; OB_SUCC(ret) && !exists_unfinished_inc_major && (i < ddl_table_iter.count()); ++i) {
      ObITable *ddl_sstable = nullptr;
      if (OB_FAIL(ddl_table_iter.get_ith_table(i, ddl_sstable))) {
        LOG_WARN("failed to get ith table", KR(ret), K(i), KPC(tablet));
      } else if (OB_FAIL(compaction::ObIncMajorTxHelper::get_trans_id_and_seq_no_from_sstable(
          static_cast<const ObSSTable *>(ddl_sstable), cur_trans_id, cur_seq_no))) {
        LOG_WARN("failed to get trans id and seq no from sstable", KR(ret), KPC(ddl_sstable));
      } else if ((cur_trans_id == prev_trans_id) && (cur_seq_no == prev_seq_no)) {
        // already checked, bypass
        continue;
      } else {
        prev_trans_id = cur_trans_id;
        prev_seq_no = cur_seq_no;
        if (OB_FAIL(ObIncMajorTxHelper::check_inc_major_trans_can_read(
            ls, cur_trans_id, cur_seq_no, SCN::max_scn()/*read_scn*/, trans_state, can_read, trans_version))) {
          LOG_WARN("failed to check inc major trans can read", KR(ret),
              KPC(tablet), K(cur_trans_id), K(cur_seq_no));
        } else if (can_read && (trans_version.get_val_for_tx() <= merge_snapshot_version)) {
          exists_unfinished_inc_major = true;
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!exists_unfinished_inc_major && !frozen_ddl_kvs.empty()) {
    for (int64_t i = 0; OB_SUCC(ret) && !exists_unfinished_inc_major && (i < frozen_ddl_kvs.count()); ++i) {
      ObDDLKVHandle &ddl_kv_handle = frozen_ddl_kvs.at(i);
      cur_trans_id = ddl_kv_handle.get_obj()->get_trans_id();
      cur_seq_no = ddl_kv_handle.get_obj()->get_seq_no();
      if ((cur_trans_id == prev_trans_id) && (cur_seq_no == prev_seq_no)) {
        // already checked, bypass
        continue;
      } else {
        prev_trans_id = cur_trans_id;
        prev_seq_no = cur_seq_no;
        if (OB_FAIL(ObIncMajorTxHelper::check_inc_major_trans_can_read(
            ls, cur_trans_id, cur_seq_no, SCN::max_scn()/*read_scn*/, trans_state, can_read, trans_version))) {
          LOG_WARN("failed to check inc major trans can read", KR(ret),
              KPC(tablet), K(cur_trans_id), K(cur_seq_no));
        } else if (can_read && (trans_version.get_val_for_tx() <= merge_snapshot_version)) {
          exists_unfinished_inc_major = true;
        }
      }
    }
  }

  FLOG_INFO("check unfinished inc major before merge", KR(ret),
      K(tablet_id), K(merge_snapshot_version), K(exists_unfinished_inc_major),
      K(cur_trans_id), K(cur_seq_no), K(ddl_table_iter.count()),
      K(frozen_ddl_kvs.count()), K(can_read), K(trans_version));

  return ret;
}

int ObIncDDLMergeTaskUtils::close_ddl_kvs(ObIArray<ObDDLKVHandle> &ddl_kvs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && (i < ddl_kvs.count()); ++i) {
    if (OB_FAIL(close_ddl_kv(ddl_kvs.at(i)))) {
      LOG_WARN("fail to close ddl kv", KR(ret), K(i), K(ddl_kvs.at(i)));
    }
  }
  return ret;
}

int ObIncDDLMergeTaskUtils::close_ddl_kv(const ObDDLKVHandle &ddl_kv_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ddl_kv_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ddl_kv_handle));
  } else {
    int max_retry_cnt = 10000;
    while(max_retry_cnt > 0) {
      max_retry_cnt--;
      if (OB_FAIL(ddl_kv_handle.get_obj()->close())) {
        if (OB_EAGAIN != ret) {
          break;
        } else {
          ob_usleep(500);
        }
      } else {
        break;
      }
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to close frozen ddl kv", K(ret), K(max_retry_cnt));
    }
  }
  return ret;
}

int ObIncDDLMergeTaskUtils::calculate_tx_data_recycle_scn(
    ObTabletMemberWrapper<ObTabletTableStore> &table_store_wrapper,
    bool &contain_uncommitted_row,
    share::SCN &recycle_end_scn)
{
  int ret = OB_SUCCESS;
  SCN inc_recycle_end_scn = SCN::max_scn();
  if (OB_UNLIKELY(!table_store_wrapper.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(table_store_wrapper));
  } else if (OB_FAIL(calculate_recycle_scn_from_inc_major(table_store_wrapper,
                                                          contain_uncommitted_row,
                                                          inc_recycle_end_scn))) {
    LOG_WARN("fail to calculate tx data recycle scn from inc major",
              KR(ret), K(table_store_wrapper), K(contain_uncommitted_row), K(inc_recycle_end_scn));
  } else if (inc_recycle_end_scn.is_max()
             && OB_FAIL(calculate_recycle_scn_from_inc_ddl_dump(table_store_wrapper,
                                                                contain_uncommitted_row,
                                                                inc_recycle_end_scn))) {
    LOG_WARN("fail to calculate tx data recycle scn from inc ddl dump",
              KR(ret), K(table_store_wrapper), K(contain_uncommitted_row), K(inc_recycle_end_scn));
  } else if (inc_recycle_end_scn.is_max()
             && OB_FAIL(calculate_recycle_scn_from_inc_ddl_kv(table_store_wrapper,
                                                              contain_uncommitted_row,
                                                              inc_recycle_end_scn))) {
    LOG_WARN("fail to calculate tx data recycle scn from inc ddl kv",
              KR(ret), K(table_store_wrapper), K(contain_uncommitted_row), K(inc_recycle_end_scn));
  } else {
    recycle_end_scn = SCN::min(recycle_end_scn, inc_recycle_end_scn);
  }
  return ret;
}

int ObIncDDLMergeTaskUtils::check_inc_major_write_stat(
    const ObTabletHandle &tablet_handle,
    const ObDDLTabletMergeDagParamV2 &merge_param,
    const int64_t cg_idx,
    const ObDDLWriteStat &write_stat)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_handle));
  } else {
    const ObLSID &ls_id = tablet_handle.get_obj()->get_ls_id();
    const ObTabletID &tablet_id = tablet_handle.get_obj()->get_tablet_id();
    ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "DdlIncCmtCb"));
    ObTabletDDLCompleteMdsUserData user_data;
    if (OB_FAIL(tablet_handle.get_obj()->get_inc_major_direct_load_info(
        SCN::max_scn(), allocator, ObTabletDDLCompleteMdsUserDataKey(merge_param.trans_id_), user_data))) {
      LOG_WARN("failed to get inc major direct load info", KR(ret), K(ls_id), K(tablet_id), K(merge_param));
    } else if (OB_LIKELY(!user_data.inc_major_commit_scn_.is_valid_and_not_min())) {
      // ignore error
      LOG_WARN("inc major has not been committed yet", KR(ret),
          K(ls_id), K(tablet_id), K(merge_param), K(user_data));
    } else {
      if (OB_UNLIKELY(user_data.write_stat_.row_count_ != write_stat.row_count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to check inc major write stat, row count not match", KR(ret),
            K(ls_id), K(tablet_id), K(merge_param), K(cg_idx), K_(user_data.write_stat), K(write_stat));
      } else {
        FLOG_INFO("succeed to check inc major write stat", KR(ret), K(ls_id), K(tablet_id),
            K(merge_param), K(cg_idx), K_(user_data.write_stat), K(write_stat));
      }
    }
  }
  return ret;
}

int ObIncDDLMergeTaskUtils::calculate_recycle_scn_from_inc_major(
    ObTabletMemberWrapper<ObTabletTableStore> &table_store_wrapper,
    bool &contain_uncommitted_row,
    share::SCN &inc_recycle_end_scn)
{
  int ret = OB_SUCCESS;
  const storage::ObSSTableArray &inc_major_sstables = table_store_wrapper.get_member()->get_inc_major_sstables();
  for (int64_t i = 0; i < inc_major_sstables.count(); i++) {
    if (INT64_MAX == inc_major_sstables.at(i)->get_upper_trans_version()) {
      contain_uncommitted_row = true;
      inc_recycle_end_scn = SCN::min(inc_recycle_end_scn, inc_major_sstables.at(i)->get_end_scn());
      break;
    }
  }
  return ret;
}

int ObIncDDLMergeTaskUtils::calculate_recycle_scn_from_inc_ddl_dump(
    ObTabletMemberWrapper<ObTabletTableStore> &table_store_wrapper,
    bool &contain_uncommitted_row,
    share::SCN &inc_recycle_end_scn)
{
  int ret = OB_SUCCESS;
  const storage::ObSSTableArray &inc_ddl_dumps = table_store_wrapper.get_member()->get_inc_major_ddl_sstables();
  if (inc_ddl_dumps.count() > 0) {
    contain_uncommitted_row = true;
    ObSSTable *first_ddl_dump = inc_ddl_dumps.at(0);
    if (OB_ISNULL(first_ddl_dump)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null first ddl dump", KR(ret), K(first_ddl_dump));
    } else {
      inc_recycle_end_scn = SCN::min(inc_recycle_end_scn, first_ddl_dump->get_end_scn());
    }
  }
  return ret;
}

int ObIncDDLMergeTaskUtils::calculate_recycle_scn_from_inc_ddl_kv(
    ObTabletMemberWrapper<ObTabletTableStore> &table_store_wrapper,
    bool &contain_uncommitted_row,
    share::SCN &inc_recycle_end_scn)
{
  int ret = OB_SUCCESS;
  const storage::ObDDLKVArray &inc_ddl_kvs = table_store_wrapper.get_member()->get_inc_ddl_mem_sstables();
  if (inc_ddl_kvs.count() > 0) {
    contain_uncommitted_row = true;
    ObDDLKV *first_ddl_kv = inc_ddl_kvs[0];
    if (OB_ISNULL(first_ddl_kv)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null first ddl mem", KR(ret), K(first_ddl_kv));
    } else if (first_ddl_kv->get_freeze_scn().is_valid_and_not_min()) {
      inc_recycle_end_scn = SCN::min(inc_recycle_end_scn, first_ddl_kv->get_freeze_scn());
    } else {
      inc_recycle_end_scn = SCN::min(inc_recycle_end_scn, first_ddl_kv->get_max_scn());
    }
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObIncDDLMergeTaskUtils::gc_ss_inc_major_ddl_dump(const ObIArray<std::pair<share::ObLSID, ObTabletID>> &ls_tablet_ids)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = MTL(ObLSService *);
  if (OB_ISNULL(ls_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service is null", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_tablet_ids.count(); ++i) {
      const ObLSID &ls_id = ls_tablet_ids.at(i).first;
      const ObTabletID &tablet_id = ls_tablet_ids.at(i).second;
      ObLSHandle ls_handle;
      ObTabletHandle tablet_handle;
      if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
        LOG_WARN("fail to get ls", KR(ret), K(ls_id));
      } else if (OB_UNLIKELY(!ls_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid ls handle", KR(ret), K(ls_id));
      } else if (OB_FAIL(ls_handle.get_ls()->get_tablet(tablet_id, tablet_handle))) {
        LOG_WARN("fail to get tablet", KR(ret), K(ls_id), K(tablet_id));
      } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tablet handle", KR(ret), K(ls_id), K(tablet_id));
      } else if (OB_FAIL(ObDDLMergeScheduler::schedule_ss_gc_inc_major_ddl_dump(ls_handle.get_ls(), tablet_handle))) {
        LOG_WARN("fail to schedule ss gc inc major ddl dump", KR(ret), K(ls_id), K(tablet_id));
      }
    }
  }
  return ret;
}
#endif

} //namespace storage
} //namespace oceanbase
