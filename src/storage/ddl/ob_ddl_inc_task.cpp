/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER

#include "storage/ddl/ob_ddl_inc_task.h"
#include "storage/ddl/ob_ddl_inc_redo_log_writer.h"
#include "storage/ddl/ob_ddl_independent_dag.h"
#include "storage/ddl/ob_ddl_tablet_context.h"
#include "storage/ob_storage_schema_util.h"
#include "storage/ddl/ob_direct_load_mgr_utils.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"
#include "storage/ddl/ob_ddl_struct.h"
#include "storage/compaction/ob_schedule_dag_func.h"
#include "lib/ob_define.h"
#include "storage/ddl/ob_inc_ddl_merge_task_utils.h"

namespace oceanbase
{
using namespace share;
using namespace common;
using namespace transaction;
using namespace observer;
using namespace lib;
using namespace compaction;
namespace storage
{
using namespace share;

/**
 * ObDDLIncStartTask
 */

ObDDLIncStartTask::ObDDLIncStartTask(const int64_t tablet_idx)
  : ObITask(TASK_TYPE_DIRECT_LOAD_INC_START_TASK), tablet_idx_(tablet_idx)
{
}

int ObDDLIncStartTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  next_task = nullptr;
  ObDDLIndependentDag *dag = static_cast<ObDDLIndependentDag *>(dag_);
  const int64_t next_tablet_idx = tablet_idx_ + 1;
  if (next_tablet_idx >= dag->get_ls_tablet_ids().count()) {
    ret = OB_ITER_END;
  } else {
    ObDDLIncStartTask *start_task = nullptr;
    if (OB_FAIL(dag->alloc_task(start_task, next_tablet_idx))) {
      LOG_WARN("fail to alloc task", KR(ret));
    } else {
      next_task = start_task;
    }
  }
  return ret;
}

int ObDDLIncStartTask::process()
{
  int ret = OB_SUCCESS;
  ObDDLIndependentDag *dag = static_cast<ObDDLIndependentDag *>(dag_);
  const std::pair<share::ObLSID, ObTabletID> &ls_tablet_id =
    dag->get_ls_tablet_ids().at(tablet_idx_);
  ObDDLTabletContext *tablet_ctx = nullptr;
  ObDDLIncRedoLogWriter redo_writer;
  SCN start_scn;
  ObStorageSchema *storage_schema = dag->get_ddl_table_schema().storage_schema_;
  if (OB_ISNULL(storage_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null storage schema", KR(ret), KP(storage_schema));
  } else if (OB_FAIL(dag->get_tablet_context(ls_tablet_id.second, tablet_ctx))) {
    LOG_WARN("fail to get tablet context", KR(ret), K(ls_tablet_id.second));
  } else if (OB_FAIL(redo_writer.init(ls_tablet_id.first,
                                      ls_tablet_id.second,
                                      dag->get_direct_load_type(),
                                      dag->get_tx_info().trans_id_,
                                      ObTxSEQ::cast_from_int(dag->get_tx_info().seq_no_),
                                      dag->is_inc_major_log()))) {
    LOG_WARN("fail to init inc redo writer", KR(ret), K(ls_tablet_id.first), K(ls_tablet_id.second),
        K(dag->get_direct_load_type()), K(dag->get_tx_info().trans_id_), K(dag->get_tx_info().seq_no_), K(dag->is_inc_major_log()));
  } else if (OB_FAIL(redo_writer.write_inc_start_log_with_retry(tablet_ctx->lob_meta_tablet_id_,
                                                                tablet_ctx->tablet_param_.with_cs_replica_,
                                                                dag->get_ddl_task_param().snapshot_version_,
                                                                dag->get_ddl_task_param().tenant_data_version_,
                                                                storage_schema,
                                                                dag->get_tx_info().tx_desc_,
                                                                start_scn))) {
    LOG_WARN("fail to write inc start log", KR(ret), KPC(tablet_ctx), K(dag->get_ddl_task_param()), K(dag->get_tx_info()));
  } else if (is_incremental_major_direct_load(dag->get_direct_load_type())) {
    ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "DdlIncStartTask"));
    if (OB_FAIL(record_inc_major_start_info_to_mds(*storage_schema,
                                                   tablet_ctx->tablet_id_,
                                                   tablet_ctx->ls_id_,
                                                   dag->get_tx_info().trans_id_,
                                                   dag->get_ddl_task_param().tenant_data_version_,
                                                   dag->get_ddl_task_param().snapshot_version_,
                                                   start_scn,
                                                   allocator))) {
      LOG_WARN("failed to record inc major start info to mds", KR(ret), KPC(storage_schema), KPC(dag));
    } else if (tablet_ctx->lob_meta_tablet_id_.is_valid()) {
      ObStorageSchema *lob_storage_schema = dag->get_ddl_table_schema().lob_meta_storage_schema_;
      if (OB_ISNULL(lob_storage_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null lob meta storage schema", KR(ret), KP(lob_storage_schema));
      } else if (OB_FAIL(record_inc_major_start_info_to_mds(*lob_storage_schema,
                                                            tablet_ctx->lob_meta_tablet_id_,
                                                            tablet_ctx->ls_id_,
                                                            dag->get_tx_info().trans_id_,
                                                            dag->get_ddl_task_param().tenant_data_version_,
                                                            dag->get_ddl_task_param().snapshot_version_,
                                                            start_scn,
                                                            allocator))) {
        LOG_WARN("failed to record inc major start info to mds", KR(ret), KPC(lob_storage_schema), KPC(dag));
      }
    }
  }
  return ret;
}

int ObDDLIncStartTask::record_inc_major_start_info_to_mds(
    const ObStorageSchema &storage_schema,
    const ObTabletID &tablet_id,
    const ObLSID &ls_id,
    const ObTransID &trans_id,
    const int64_t data_format_version,
    const int64_t snapshot_version,
    const SCN start_scn,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
                  || !storage_schema.is_valid()
                  || !tablet_id.is_valid()
                  || !ls_id.is_valid()
                  || !trans_id.is_valid()
                  || !is_data_version_support_inc_major_direct_load(data_format_version)
                  || (snapshot_version <= 0)
                  || !start_scn.is_valid_and_not_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant_id), K(storage_schema), K(tablet_id),
        K(ls_id), K(trans_id), K(data_format_version), K(snapshot_version), K(start_scn));
  } else {
    ObTabletDDLCompleteArg complete_arg;
    complete_arg.has_complete_ = true;
    complete_arg.ls_id_ = ls_id;
    complete_arg.tablet_id_ = tablet_id;
    complete_arg.direct_load_type_ = DIRECT_LOAD_INCREMENTAL_MAJOR;
    complete_arg.start_scn_ = start_scn;
    complete_arg.data_format_version_ = data_format_version;
    complete_arg.snapshot_version_ = snapshot_version;
    complete_arg.table_key_.tablet_id_ = tablet_id;
    complete_arg.trans_id_ = trans_id;

    if (OB_FAIL(complete_arg.set_storage_schema(storage_schema))) {
      LOG_WARN("failed to set storage_schema", KR(ret), K(storage_schema));
    } else if (OB_FAIL(ObTabletDDLCompleteMdsHelper::record_ddl_complete_arg_to_mds(complete_arg, allocator))) {
      LOG_WARN("failed to record ddl complete arg to mds", KR(ret), K(complete_arg));
    }
  }
  return ret;
}

/**
 * ObDDLIncCommitTask
 */

ObDDLIncCommitTask::ObDDLIncCommitTask(const int64_t tablet_idx)
  : ObITask(TASK_TYPE_DIRECT_LOAD_INC_COMMIT_TASK), tablet_idx_(tablet_idx)
{
}

ObDDLIncCommitTask::ObDDLIncCommitTask(const ObTabletID &tablet_id)
  : ObITask(TASK_TYPE_DIRECT_LOAD_INC_COMMIT_TASK), tablet_idx_(-1), tablet_id_(tablet_id)
{
}

int ObDDLIncCommitTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  next_task = nullptr;
  ObDDLIndependentDag *dag = static_cast<ObDDLIndependentDag *>(dag_);
  const int64_t next_tablet_idx = tablet_idx_ + 1;
  if (tablet_idx_ == -1) {
    ret = OB_ITER_END;
  } else if (next_tablet_idx >= dag->get_ls_tablet_ids().count()) {
    ret = OB_ITER_END;
  } else {
    ObDDLIncCommitTask *commit_task = nullptr;
    if (OB_FAIL(dag->alloc_task(commit_task, next_tablet_idx))) {
      LOG_WARN("fail to alloc task", KR(ret));
    } else {
      next_task = commit_task;
    }
  }
  return ret;
}

int ObDDLIncCommitTask::process()
{
  int ret = OB_SUCCESS;
  ObDDLIndependentDag *dag = static_cast<ObDDLIndependentDag *>(dag_);
  if (tablet_idx_ != -1) {
    const std::pair<share::ObLSID, ObTabletID> &ls_tablet_id =
      dag->get_ls_tablet_ids().at(tablet_idx_);
    tablet_id_ = ls_tablet_id.second;
  }
  ObDDLTabletContext *tablet_ctx = nullptr;
  ObDDLIncRedoLogWriter redo_writer;
  share::SCN commit_scn;
  if (OB_FAIL(dag->get_tablet_context(tablet_id_, tablet_ctx))) {
    LOG_WARN("fail to get tablet context", KR(ret), K(tablet_id_));
  } else if (OB_FAIL(redo_writer.init(tablet_ctx->ls_id_,
                                      tablet_id_,
                                      dag->get_direct_load_type(),
                                      dag->get_tx_info().trans_id_,
                                      ObTxSEQ::cast_from_int(dag->get_tx_info().seq_no_),
                                      dag->is_inc_major_log()))) {
    LOG_WARN("fail to init inc redo writer", KR(ret), K(tablet_ctx->ls_id_), K(tablet_id_),
        K(dag->get_direct_load_type()), K(dag->get_tx_info().trans_id_), K(dag->get_tx_info().seq_no_), K(dag->is_inc_major_log()));
  } else if (OB_FAIL(redo_writer.write_inc_commit_log_with_retry(true /*allow_remote_write*/,
                                                                 tablet_ctx->lob_meta_tablet_id_,
                                                                 dag->get_ddl_task_param().snapshot_version_,
                                                                 dag->get_ddl_task_param().tenant_data_version_,
                                                                 dag->get_tx_info().tx_desc_,
                                                                 commit_scn))) {
    LOG_WARN("fail to write inc commit log", KR(ret), KPC(tablet_ctx), K(dag->get_ddl_task_param()), K(dag->get_tx_info()));
  } else if (is_incremental_major_direct_load(dag->get_direct_load_type())) {
    const ObTabletID &tablet_id = tablet_id_;
    const ObTabletID &lob_meta_tablet_id = tablet_ctx->lob_meta_tablet_id_;
    const ObTransID &trans_id = dag->get_tx_info().trans_id_;
    if (OB_FAIL(record_inc_major_commit_info_to_mds(tablet_ctx->ls_id_, tablet_id, trans_id, commit_scn, tablet_ctx->write_stat_))) {
      LOG_WARN("failed to record inc major commit info to mds", KR(ret), K(tablet_id), K(trans_id), K(commit_scn));
    } else if (lob_meta_tablet_id.is_valid()
        && OB_FAIL(record_inc_major_commit_info_to_mds(tablet_ctx->ls_id_, lob_meta_tablet_id, trans_id, commit_scn, tablet_ctx->lob_write_stat_))) {
      LOG_WARN("failed to record inc major commit info to mds", KR(ret), K(lob_meta_tablet_id), K(trans_id), K(commit_scn));
    }
  }
  return ret;
}

int ObDDLIncCommitTask::record_inc_major_commit_info_to_mds(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const ObTransID &trans_id,
    const SCN &commit_scn,
    const ObDDLWriteStat &write_stat)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTabletDDLCompleteArg complete_arg;
  ObTabletDDLCompleteMdsUserData user_data;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "DdlIncCommitCb"));
  if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(ls_id, tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet handle", KR(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_inc_major_direct_load_info(
      SCN::max_scn(), ObTabletDDLCompleteMdsUserDataKey(trans_id), user_data))) {
    LOG_WARN("failed to get inc major direct load info", KR(ret), K(tablet_handle), K(trans_id));
  } else if (user_data.inc_major_commit_scn_.is_valid_and_not_min()) {
    // do nothing
    if (OB_UNLIKELY(commit_scn != user_data.inc_major_commit_scn_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mismatched commit scn", KR(ret), K(commit_scn), K(user_data));
    } else {
      LOG_INFO("no need to record inc major commit info into mds because it's already recorded",
          K(commit_scn), K(user_data));
    }
  } else if (OB_FALSE_IT(user_data.inc_major_commit_scn_ = commit_scn)) {
  } else if (OB_FAIL(complete_arg.from_mds_user_data(user_data))) {
    LOG_WARN("failed to set with merge arg", KR(ret), K(user_data));
  } else {
    complete_arg.ls_id_ = ls_id;
    complete_arg.tablet_id_ = tablet_id;

    if (OB_FAIL(complete_arg.set_write_stat(write_stat))) {
      LOG_WARN("failed to set write stat", KR(ret), K(write_stat));
    } else if (OB_FAIL(ObTabletDDLCompleteMdsHelper::record_ddl_complete_arg_to_mds(complete_arg, allocator))) {
      LOG_WARN("failed to record ddl complete arg to mds", KR(ret), K(complete_arg), K(user_data));
    } else {
      FLOG_INFO("succeed to record inc major commit info to mds", K(complete_arg));
    }
  }
  return ret;
}

/**
 * ObDDLIncWaitDumpTask
 */


ObDDLIncWaitDumpTask::ObDDLIncWaitDumpTask(const ObLSID &ls_id,
                                           const ObTabletID &tablet_id,
                                           const ObTransID &trans_id,
                                           const ObTxSEQ &seq_no)
  : ObITask(TASK_TYPE_DIRECT_LOAD_INC_WAIT_DUMP_TASK),
    ls_id_(ls_id),
    tablet_id_(tablet_id),
    trans_id_(trans_id),
    seq_no_(seq_no)
{
}

int ObDDLIncWaitDumpTask::process()
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  ObDDLKVQueryParam ddl_kv_query_param;
  ObArray<ObDDLKVHandle> ddl_kvs;

  if (!GCTX.is_shared_storage_mode()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObDDLIncWaitDumpTask should work in shared storage mode", KR(ret));
  } else if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(ls_id_, tablet_id_, tablet_handle))) {
    LOG_WARN("fail to get tablet handle", KR(ret), K(ls_id_), K(tablet_id_));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_TASK_EXPIRED;
      LOG_INFO("ddl kv mgr is not exist", KR(ret), K(tablet_id_));
    } else {
      LOG_WARN("fail to get ddl kv mgr", KR(ret), K(tablet_id_));
    }
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->get_ddl_kvs(false/*frozen_only*/,
                                                        ddl_kvs,
                                                        ddl_kv_query_param))) {
    LOG_WARN("fail to get all ddl kvs", KR(ret), K(tablet_id_));
  } else if (ddl_kvs.count() > 0) {
    ObDDLKVHandle first_ddl_kv = ddl_kvs.at(0);
    if (OB_UNLIKELY(!first_ddl_kv.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid ddl kv", KR(ret), K(first_ddl_kv));
    } else if (OB_UNLIKELY(first_ddl_kv.get_obj()->get_ddl_kv_type() != DDL_KV_INC_MAJOR)) {
      ret = OB_DAG_TASK_IS_SUSPENDED;
      if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
        LOG_INFO("unexpected ddl kv type", KR(ret), K(*first_ddl_kv.get_obj()));
      }
    } else if (first_ddl_kv.get_obj()->get_trans_id() != trans_id_ || first_ddl_kv.get_obj()->get_seq_no() != seq_no_) {
      (void) schedule_ddl_merge_dag(first_ddl_kv);
      ret = OB_DAG_TASK_IS_SUSPENDED;
      if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
        LOG_INFO("unexpected ddl kv type", KR(ret), K(*first_ddl_kv.get_obj()));
      }
    }
  }
  return ret;
}

void ObDDLIncWaitDumpTask::schedule_ddl_merge_dag(ObDDLKVHandle &first_ddl_kv)
{
  ObDDLTableMergeDagParam param;
  param.direct_load_type_    = DIRECT_LOAD_INCREMENTAL_MAJOR;
  param.ls_id_               = ls_id_;
  param.tablet_id_           = tablet_id_;
  param.rec_scn_             = first_ddl_kv.get_obj()->get_rec_scn();
  param.is_commit_           = false;
  param.start_scn_           = first_ddl_kv.get_obj()->get_start_scn();
  param.data_format_version_ = first_ddl_kv.get_obj()->get_data_format_version();
  param.snapshot_version_    = first_ddl_kv.get_obj()->get_snapshot_version();
  param.trans_id_ = first_ddl_kv.get_obj()->get_trans_id();
  param.seq_no_ = first_ddl_kv.get_obj()->get_seq_no();
  // only has ddl dump type in ss mode
  param.table_type_ = ObITable::INC_MAJOR_DDL_DUMP_SSTABLE;
  (void) compaction::ObScheduleDagFunc::schedule_ddl_table_merge_dag(param);
}

ObDDLIncPrepareTask::ObDDLIncPrepareTask()
  : ObITask(TASK_TYPE_DIRECT_LOAD_INC_PREPARE_TASK),
    tablet_idx_(0),
    is_prepared_(false)
{
}

int ObDDLIncPrepareTask::process()
{
  int ret = OB_SUCCESS;
  ObDDLIndependentDag *dag = static_cast<ObDDLIndependentDag *>(dag_);
  const ObIArray<std::pair<share::ObLSID, ObTabletID>> &ls_tablet_ids = dag->get_ls_tablet_ids();
  ObMemAttr attr(MTL_ID(), "TLD_INC_PREPARE");
  if (!trans_ids_.created() && OB_FAIL(trans_ids_.create(MAX_INC_MAJOR_SSTABLE_CNT, attr))) {
    LOG_WARN("fail to create trans ids", KR(ret));
  }
  while (OB_SUCC(ret) && tablet_idx_ < ls_tablet_ids.count()) {
    const std::pair<share::ObLSID, ObTabletID> &ls_tablet_id = ls_tablet_ids.at(tablet_idx_);
    if (OB_FAIL(process_one_tablet(ls_tablet_id.first, ls_tablet_id.second))) {
      LOG_WARN("fail to process one tablet",
               KR(ret), "ls_id", ls_tablet_id.first, "tablet_id", ls_tablet_id.second);
    } else if (is_prepared_) {
      ++tablet_idx_;
    } else {
#ifdef OB_BUILD_SHARED_STORAGE
      if (GCTX.is_shared_storage_mode()) {
        (void) ObIncDDLMergeTaskUtils::gc_ss_inc_major_ddl_dump(ls_tablet_ids);
      }
#endif
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("inc major count exceeds max limit",
               KR(ret), "ls_id", ls_tablet_id.first, "tablet_id", ls_tablet_id.second, KPC(dag));
    }
  }
  return ret;
}

int ObDDLIncPrepareTask::process_one_tablet(const ObLSID &ls_id,
                                            const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  is_prepared_ = true;
  trans_ids_.reuse();
  ObTabletHandle tablet_handle;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  ObArray<ObDDLKV *> ddl_kvs;
  if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arguments", KR(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(ObDirectLoadMgrUtil::get_tablet_handle(ls_id, tablet_id, tablet_handle))) {
    LOG_WARN("fail to get tablet handle", KR(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_INFO("ddl kv mgr is not exist", KR(ret), K(ls_id), K(tablet_id));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get ddl kv mgr", KR(ret), K(ls_id), K(tablet_id));
    }
  } else if (ddl_kv_mgr_handle.get_obj()->get_count() >= ObTabletDDLKvMgr::MAX_DDL_KV_CNT_IN_STORAGE - 1) {
    is_prepared_ = false;
    LOG_INFO("ddl kv count exceeds max limit", KR(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", KR(ret), K(ls_id), K(tablet_id));
  } else if (OB_UNLIKELY(!table_store_wrapper.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid table store wrapper", KR(ret), K(ls_id), K(tablet_id), K(table_store_wrapper));
  } else if (OB_FAIL(search_sstables(table_store_wrapper.get_member()->get_inc_major_sstables()))) {
    LOG_WARN("fail to search inc major sstables", KR(ret));
  } else if (OB_FAIL(search_sstables(table_store_wrapper.get_member()->get_inc_major_ddl_sstables()))) {
    LOG_WARN("fail to search inc major ddl sstables", KR(ret));
  } else if (!is_prepared_) {
    // skip
  } else if (OB_FAIL(tablet_handle.get_obj()->get_inc_major_ddl_kvs(ddl_kvs))) {
    LOG_WARN("fail to get inc major ddl kvs", KR(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(search_ddlkvs(ddl_kvs))) {
    LOG_WARN("fail to search ddl kv array", KR(ret), K(ls_id), K(tablet_id));
  }
  return ret;
}

int ObDDLIncPrepareTask::search_sstables(const ObSSTableArray &sstables)
{
  int ret = OB_SUCCESS;
  ObTransID trans_id;
  ObTxSEQ seq_no;
  for (int64_t i = 0; OB_SUCC(ret) && is_prepared_ && i < sstables.count(); ++i) {
    ObSSTable *sstable = sstables.at(i);
    if (OB_ISNULL(sstable)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid sstable", KR(ret), KPC(sstable));
    } else if (OB_FAIL(ObIncMajorTxHelper::get_trans_id_and_seq_no_from_sstable(sstable, trans_id, seq_no))) {
      LOG_WARN("fail to get trans id and seq no from inc major", KR(ret), KPC(sstable));
    } else if (OB_FAIL(record_trans_id(trans_id))) {
      LOG_WARN("fail to record trans id", KR(ret), K(trans_id));
    }
  }
  return ret;
}

int ObDDLIncPrepareTask::search_ddlkvs(const ObIArray<ObDDLKV *> &ddl_kvs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && is_prepared_ && i < ddl_kvs.count(); ++i) {
    ObDDLKV *ddl_kv = ddl_kvs.at(i);
    if (OB_FAIL(record_trans_id(ddl_kv->get_trans_id()))) {
      LOG_WARN("fail to record trans id", KR(ret), K(i), KPC(ddl_kv));
    }
  }
  return ret;
}

ERRSIM_POINT_DEF(REDUCE_INC_MAJOR_DIRECT_LOAD_CNT);
int ObDDLIncPrepareTask::record_trans_id(const ObTransID &trans_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!trans_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("trans id is invalid", KR(ret), K(trans_id));
  } else if (OB_FAIL(trans_ids_.set_refactored(trans_id))) {
    LOG_WARN("fail to set trans id", KR(ret), K(trans_id));
  } else if (OB_UNLIKELY(REDUCE_INC_MAJOR_DIRECT_LOAD_CNT)) {
    if (trans_ids_.size() >= 8) {
      is_prepared_ = false;
      LOG_INFO("inc major count exceeds max limit", KR(ret));
    }
  } else if (trans_ids_.size() >= MAX_INC_MAJOR_DIRECT_LOAD_CNT) {
    is_prepared_ = false;
    LOG_INFO("inc major count exceeds max limit", KR(ret));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
