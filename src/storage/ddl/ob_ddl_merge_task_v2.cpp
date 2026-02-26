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
#include "storage/ddl/ob_ddl_merge_task.h"
#include "storage/ddl/ob_ddl_merge_task_utils.h"
#include "storage/ddl/ob_ddl_merge_task_v2.h"
#include "share/ob_ddl_checksum.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "share/ob_ddl_sim_point.h"
#include "storage/compaction/ob_tenant_tablet_scheduler.h"
#include "storage/ob_storage_schema_util.h"
#include "storage/compaction/ob_schedule_dag_func.h"
#include "storage/ddl/ob_direct_load_struct.h"
#include "storage/ddl/ob_direct_load_mgr_utils.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/ddl/ob_ddl_merge_schedule.h"
using namespace oceanbase::observer;
using namespace oceanbase::share::schema;
using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::blocksstable;

namespace oceanbase
{
namespace storage
{

int ObDDLMergeBaseTask::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_process())) {
    LOG_WARN("failed to inner process", K(ret));
  }

  if (OB_FAIL(ret) && nullptr != fail_cb_) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(fail_cb_->process(ret))) {
      LOG_WARN("failed to call fail callback", K(tmp_ret));
    }
  }
  return ret;
}


int ObDDLMergeGuardTask::init(const bool for_replay, const ObTabletID &tablet_id, bool retry_uitl_get)
{
  int ret = OB_SUCCESS;
  char* buf = nullptr;
  ObDDLMergeBucketLock *mtl_bucket_lock = MTL(ObDDLMergeBucketLock*);
  if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id));
  } else if (OB_ISNULL(mtl_bucket_lock)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bucket lock should not be null", K(ret));
  }

  int64_t max_retry_times = 100;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(mtl_bucket_lock->lock(tablet_id))) {
      if (OB_EAGAIN == ret && !for_replay) {
        LOG_WARN("failed to lock tablet, but execute again", K(ret), K(tablet_id));
        if (retry_uitl_get && max_retry_times > 0) {
          ret = OB_SUCCESS;
          ob_usleep(1000 * 10); // 10ms
          max_retry_times--;
        } else {
          ret = OB_DAG_TASK_IS_SUSPENDED;
        }
      } else {
        LOG_WARN("failed to lock tablet", K(ret), K(tablet_id));
      }
    } else {
      break;
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    tablet_id_ = tablet_id;
    is_inited_ = true;
  }
  FLOG_INFO("[DDL_MERGE_TASK] success to create guard task", K(ret), K(tablet_id));
  return ret;
}

int ObDDLScheduleAnotherMergeTask::init(const share::ObLSID &ls_id, const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (!ls_id.is_valid() || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id));
  } else {
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    is_inited_ = true;
  }
  return ret;
}

int ObDDLScheduleAnotherMergeTask::process()
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = MTL(ObLSService *);
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("task is not inited", K(ret), KPC(this));
  } else {
    ObDDLTableMergeTask *merge_task = nullptr;
    ObLSHandle ls_handle;
    ObTabletHandle tablet_handle;
    if (OB_FAIL(ls_service->get_ls(ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
      LOG_WARN("get ls failed", K(ret), K(ls_id_));
    } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle,
                                               tablet_id_,
                                               tablet_handle,
                                               ObMDSGetTabletMode::READ_ALL_COMMITED))) {
      LOG_WARN("get tablet failed", K(ret), K(ls_id_), K(tablet_id_));
    } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err", K(ret), K(ls_id_), K(tablet_id_));
    } else if (OB_FAIL(ObDDLMergeScheduler::schedule_tablet_ddl_inc_major_merge(ls_handle.get_ls(), tablet_handle))) {
      LOG_WARN("failed to schedule another merge", K(ret));
    }
  }
  return ret;
}

void ObDDLScheduleAnotherMergeTask::task_debug_info_to_string(char *buf, const int64_t buf_len, int64_t &pos) const
{
  if (is_inited_) {
    BUF_PRINTF("DDL Schedule Another Merge Task: ls_id=%ld, tablet_id=%ld",
                ls_id_.id(), tablet_id_.id());
  } else {
    BUF_PRINTF("DDL Schedule Another Merge Task: is_inited=%s", is_inited_ ? "true" : "false");
  }
}


int ObDDLMergeGuardTask::process()
{
  int ret = OB_SUCCESS;
  ObDDLMergeBucketLock *mtl_bucket_lock = MTL(ObDDLMergeBucketLock*);
  if (!tablet_id_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected tablet id val", K(ret));
  } else if (OB_ISNULL(mtl_bucket_lock)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bucket lock should not be null", K(ret));
  } else if (OB_FAIL(mtl_bucket_lock->unlock(tablet_id_))) {
    LOG_WARN("[DDL_MERGE_TASK] failed to finish guard task", K(ret), K(tablet_id_));
  } else {
    FLOG_INFO("[DDL_MERGE_TASK] success to finish guard task", K(ret), K(tablet_id_));
    tablet_id_.reset();
  }
  return ret;
}

ObDDLMergeGuardTask::~ObDDLMergeGuardTask()
{
  int ret = OB_SUCCESS;
  ObDDLMergeBucketLock *mtl_bucket_lock = MTL(ObDDLMergeBucketLock*);
   if (OB_ISNULL(mtl_bucket_lock)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bucket lock should not be null", K(ret));
  } else if (tablet_id_.is_valid() && OB_FAIL(mtl_bucket_lock->unlock(tablet_id_))) {
    LOG_WARN("failed to unlock tablet", K(ret), K(tablet_id_));
  } else {
    is_inited_ = false;
  }
}

void ObDDLMergeGuardTask::task_debug_info_to_string(char *buf, const int64_t buf_len, int64_t &pos) const
{
  BUF_PRINTF("DDL Merge Guard Task: tablet_id=%ld", tablet_id_.id());
}

ObDDLMergePrepareTask::ObDDLMergePrepareTask():
  ObDDLMergeBaseTask(ObITaskType::TASK_TYPE_DDL_MERGE_PREPARE),
  merge_param_(), guard_task_(nullptr), is_inited_(false)
{}

ObDDLMergePrepareTask::~ObDDLMergePrepareTask()
{}

int ObDDLMergePrepareTask::init(const ObDDLTabletMergeDagParamV2 &merge_param, ObDDLFailCallback *fail_cb, bool retry_uitl_get)
{
  int ret = OB_SUCCESS;
  if (!merge_param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(merge_param));
  } else {
    merge_param_ = merge_param;
    guard_task_  = nullptr;
    set_fail_callback(fail_cb);
    retry_uitl_get_lock_ = retry_uitl_get;
    is_inited_   = true;
  }
  FLOG_INFO("[DDL_MERGE_TASK] success to create merge prepare task", K(ret), K(merge_param_));
  return ret;
}

/*
 * process task dependency for single tablet
 * on contrast process should process relative tablet， such as lob
*/
int ObDDLMergePrepareTask::inner_process()
{
  int ret = OB_SUCCESS;
  ObIDag *dag = get_dag();
  ObArray<ObITask*> merge_slice_tasks;
  ObDDLMergeAssembleTask *assemble_task = nullptr;

  int64_t merge_slice_idx = 0;
  ObArray<ObTuple<int64_t, int64_t, int64_t>> cg_slices; /* <cg_idx, start_slice_idx, end_slice_idx> */
  ObLSID ls_id;
  ObTabletID tablet_id;
  ObWriteTabletParam *tablet_param = nullptr;

  /* debug sync for building major in leader server*/
  if (merge_param_.for_major_) {
    DEBUG_SYNC(BEFORE_TABLET_FULL_DIRECT_LOAD_MGR_CLOSE);
  }

  /* create guard task first to avoid backend build major task */
  if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag should not be null", K(ret));
  } else if (OB_FAIL(merge_param_.get_tablet_param(ls_id, tablet_id, tablet_param))) {
    LOG_WARN("failed to get tablet param", K(ret));
  } else if (OB_ISNULL(tablet_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet param should not be nullptr", K(ret), K(merge_param_));
  } else if (OB_ISNULL(tablet_param->storage_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage schema should not be nullptr", K(ret), KPC(tablet_param));
  } else if (nullptr != guard_task_) {
    LOG_INFO("guard task has been created", K(ret), K(merge_param_));
  } else if (OB_FAIL(dag->alloc_task(guard_task_))) {
    LOG_WARN("failed to alloc task", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (guard_task_->is_inited_) {
    LOG_INFO("gaurd task already init", K(tablet_id));
  } else if (OB_FAIL(guard_task_->init(merge_param_.for_replay_, tablet_id, retry_uitl_get_lock_))) {
    LOG_WARN("failed to init merge guard task", K(ret));
  } else if (OB_FAIL(guard_task_->deep_copy_children(get_child_nodes()))) {
    LOG_WARN("fail to deep copy children", KR(ret));
  } else if (OB_FAIL(add_child(*guard_task_))) {
    LOG_WARN("failed to add child to prepare task", K(ret));
  }

  /* pre-check before merge */
  bool need_merge = true;
  ObIDDLMergeHelper *merge_helper = nullptr;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(merge_param_.get_merge_helper(merge_helper))) {
    LOG_WARN("failed to get merge helper", K(ret));
  } else if (OB_ISNULL(merge_helper)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge param is invalid", K(ret));
  } else if (OB_FAIL(merge_helper->check_need_merge(dag, merge_param_, need_merge))) {
    LOG_WARN("failed to check need merge", KR(ret));
  }

  /*
   * 1. calculate merge_cg_slice_task count
   * 2. get_rec_scn for release ddl kvs
  */
  if (OB_FAIL(ret) || !need_merge) {
  } else if (OB_FAIL(merge_helper->process_prepare_task(dag, merge_param_, cg_slices))) {
    LOG_WARN("failed to process prepare task", KR(ret), K(merge_param_));
  } else if (OB_FAIL(merge_helper->get_rec_scn(merge_param_))) {
    LOG_WARN("failed to get rec scn", K(ret));
  }

  /* generate assemble table task */
  if (OB_FAIL(ret) || !need_merge) {
  } else if (OB_FAIL(dag->alloc_task(assemble_task))) {
    LOG_WARN("failed alloc assemble task", K(ret));
  } else if (OB_ISNULL(assemble_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("assemble task should not be null", K(ret), K(merge_param_));
  } else if (OB_FAIL(assemble_task->init(merge_param_, fail_cb_))) {
    LOG_WARN("failed to init assemble", K(ret));
  } else if (OB_FAIL(assemble_task->add_child(*guard_task_))) {
    LOG_WARN("faield to add guard task as child", K(ret));
  } else if (OB_FAIL(::ObITask::add_child(*assemble_task))) {
    LOG_WARN("failed to add assemble task to prepare task", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < cg_slices.count(); i++ ) {
      ObDDLMergeCgSliceTask *merge_slice_task = nullptr;
      int64_t cg_idx          = cg_slices.at(i).element<0>();
      int64_t start_slice_idx = cg_slices.at(i).element<1>();
      int64_t end_slice_idx   = cg_slices.at(i).element<2>();
      if (OB_FAIL(dag->alloc_task(merge_slice_task))) {
        LOG_WARN("failed to alloc merge slice task", K(ret));
      } else if (OB_FAIL(merge_slice_task->init(merge_param_, cg_idx, start_slice_idx, end_slice_idx, fail_cb_))) {
        LOG_WARN("failed to init merge slice task", K(ret));
      } else if (OB_FAIL(merge_slice_task->add_child(*assemble_task))) {
        LOG_WARN("failed add child for merge slice task", K(ret));
      } else if (OB_FAIL(::ObITask::add_child(*merge_slice_task))) {
        LOG_WARN("failed to add child to prepare task", K(ret));
      } else if (OB_FAIL(merge_slice_tasks.push_back(merge_slice_task))) {
        LOG_WARN("failed to push back task", K(ret));
      }
    }
  }

  // guard_task needs to be executed no matter need_merge or not
  if (OB_FAIL(ret)) {
  } else {
    // add task in reverse order of running
    /* add merge guard task to dag */
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(dag->add_task(*guard_task_))) {
      LOG_WARN("failed to add merge guard task", K(ret));
    }

    /* generate assemble task */
    if (OB_FAIL(ret)) {
    } else if (nullptr == assemble_task) {
    } else if (OB_FAIL(dag->add_task(*assemble_task))) {
      LOG_WARN("failed to add assemble task to dag", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(dag->batch_add_task(merge_slice_tasks))) {
        LOG_WARN("batch add task failed", K(ret));
      }
    }
  }
  FLOG_INFO("[DDL_MERGE_TASK] finish merge prepare task", K(ret), K(merge_param_));
  return ret;
}

void ObDDLMergePrepareTask::task_debug_info_to_string(char *buf, const int64_t buf_len, int64_t &pos) const
{
  ObLSID ls_id;
  ObTabletID tablet_id;
  ObWriteTabletParam *tablet_param = nullptr;
  if (OB_SUCCESS == merge_param_.get_tablet_param(ls_id, tablet_id, tablet_param)) {
    BUF_PRINTF("DDL Merge Prepare Task: ls_id=%ld, tablet_id=%ld, is_inited=%s",
               ls_id.id(), tablet_id.id(), is_inited_ ? "true" : "false");
  } else {
    BUF_PRINTF("DDL Merge Prepare Task: is_inited=%s", is_inited_ ? "true" : "false");
  }
}

ObDDLMergeCgSliceTask::ObDDLMergeCgSliceTask():
ObDDLMergeBaseTask(ObITaskType::TASK_TYPE_DDL_MERGE_CG_SLICE), merge_param_(), cg_idx_(-1), start_slice_idx_(-1), end_slice_idx_(-1)
{}

int ObDDLMergeCgSliceTask::init(const ObDDLTabletMergeDagParamV2 &merge_param,
                                const int64_t cg_idx,
                                const int64_t start_slice_idx,
                                const int64_t end_slice_idx,
                                ObDDLFailCallback *fail_cb)
{
  int ret = OB_SUCCESS;
  ObLSID target_ls_id;
  ObTabletID target_tablet_id;
  if (!merge_param.is_valid() || cg_idx < 0 || start_slice_idx < 0 || end_slice_idx < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid merge param", K(ret), K(merge_param), K(cg_idx), K(start_slice_idx), K(end_slice_idx));
  } else {
    merge_param_     = merge_param;
    cg_idx_          = cg_idx;
    start_slice_idx_ = start_slice_idx;
    end_slice_idx_   = end_slice_idx;
    set_fail_callback(fail_cb);
  }
  FLOG_INFO("[DDL_MERGE_TASK]  create ddl slice merge task,", K(ret), K(cg_idx_), K(start_slice_idx_), K(end_slice_idx_), K(merge_param_.for_replay_));

  return ret;
}

int ObDDLMergeCgSliceTask::inner_process()
{
  int ret = OB_SUCCESS;
  ObIDag *dag = get_dag();
  ObArenaAllocator allocator("MergeSlice");
  ObIDDLMergeHelper *merge_helper = nullptr;

  if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag should not be null", K(ret));
  } else if (OB_FAIL(merge_param_.get_merge_helper(merge_helper))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get merge helper", K(ret), K(merge_param_));
  } else if (OB_ISNULL(merge_helper)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("merge helper should not be null", K(ret), K(merge_param_));
  } else if (OB_FAIL(merge_helper->merge_cg_slice(dag, merge_param_, cg_idx_, start_slice_idx_, end_slice_idx_))) {
    LOG_WARN("failed merge_cg_slice", K(ret));
  }

  FLOG_INFO("[DDL_MERGE_TASK] finish merge cg slice", K(ret), K(cg_idx_), K(start_slice_idx_), K(end_slice_idx_), K(merge_param_));
  return ret;
}

void ObDDLMergeCgSliceTask::task_debug_info_to_string(char *buf, const int64_t buf_len, int64_t &pos) const
{
  ObLSID ls_id;
  ObTabletID tablet_id;
  ObWriteTabletParam *tablet_param = nullptr;
  if (OB_SUCCESS == merge_param_.get_tablet_param(ls_id, tablet_id, tablet_param)) {
    BUF_PRINTF("DDL Merge CG Slice Task: ls_id=%ld, tablet_id=%ld, cg_idx=%ld, start_slice=%ld, end_slice=%ld",
               ls_id.id(), tablet_id.id(), cg_idx_, start_slice_idx_, end_slice_idx_);
  } else {
    BUF_PRINTF("DDL Merge CG Slice Task: cg_idx=%ld, start_slice=%ld, end_slice=%ld",
               cg_idx_, start_slice_idx_, end_slice_idx_);
  }
}


ObDDLMergeAssembleTask::ObDDLMergeAssembleTask():
  ObDDLMergeBaseTask(ObITaskType::TASK_TYPE_DDL_MERGE_ASSEMBLE), merge_param_(), is_inited_(false)
{}

int ObDDLMergeAssembleTask::init(const ObDDLTabletMergeDagParamV2 &ddl_merge_param, ObDDLFailCallback *fail_cb)
{
  int ret = OB_SUCCESS;
  if (!ddl_merge_param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ddl_merge_param));
  } else {
    merge_param_ = ddl_merge_param;
    set_fail_callback(fail_cb);
    is_inited_ = true;
  }
  FLOG_INFO("[DDL_MERGE_TASK] create ddl assemble merge task,", K(ret), K(ddl_merge_param));
  return ret;
}


int ObDDLMergeAssembleTask::inner_process()
{
  int ret = OB_SUCCESS;
  ObIDDLMergeHelper *merge_helper = nullptr;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(),"Ddl_Assm_Task"));
  ObLSID target_ls_id;
  ObTabletID target_tablet_id;
  ObWriteTabletParam *tablet_param = nullptr;
  ObTabletHandle tablet_handle;
  bool is_column_store_table = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("assemble task has not been init", K(ret), KPC(this));
  } else if (OB_FAIL(merge_param_.get_tablet_param(target_ls_id, target_tablet_id, tablet_param))) {
    LOG_WARN("failed to get tablet param", K(ret));
  } else if (OB_FAIL(merge_param_.get_merge_helper(merge_helper))) {
    LOG_WARN("failed to get merge helper", K(ret), K(merge_param_));
  } else if (merge_param_.for_major_) {
    DEBUG_SYNC(BEFORE_DDL_MERGE_ASSEMBLE_TASK);
  }

  if (FAILEDx(merge_helper->assemble_sstable(merge_param_))) {
    LOG_WARN("failed to assemble major sstable", K(ret));
  }
  FLOG_INFO("[DDL_MERGE_TASK]  ddl update table store finish", K(ret), K(merge_param_));
  return ret;
}

void ObDDLMergeAssembleTask::task_debug_info_to_string(char *buf, const int64_t buf_len, int64_t &pos) const
{
  ObLSID ls_id;
  ObTabletID tablet_id;
  ObWriteTabletParam *tablet_param = nullptr;
  if (OB_SUCCESS == merge_param_.get_tablet_param(ls_id, tablet_id, tablet_param)) {
    BUF_PRINTF("DDL Merge Assemble Task: ls_id=%ld, tablet_id=%ld, is_inited=%s",
               ls_id.id(), tablet_id.id(), is_inited_ ? "true" : "false");
  } else {
    BUF_PRINTF("DDL Merge Assemble Task: is_inited=%s", is_inited_ ? "true" : "false");
  }
}

} //namespcae storage
} //namespace oceanbase
