//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#include "share/compaction/ob_batch_exec_dag.h"
#include "storage/compaction/ob_sstable_merge_info_mgr.h"
namespace oceanbase
{
namespace compaction
{

void ObBatchExecCollector::add(
  const ObBatchExecInfo input_info,
  ObIDag &dag)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(lock_);
  info_.add(input_info);

  if (0 < --remain_task_cnt_) {
     // have remain task
  } else if (info_.failure_cnt_ > info_.success_cnt_) {
    dag.set_dag_ret(OB_PARTIAL_FAILED);
  } else {
    ObMergeRunningInfo &running_info = merge_history_.running_info_;
    running_info.merge_start_time_ = dag.get_start_time();
    running_info.merge_finish_time_ = ObTimeUtility::fast_current_time();
    running_info.dag_id_ = dag.get_dag_id();
    // will add into dag_warning_history if more than half tablet failed

#define ADD_COMMENT(...) \
    ADD_COMPACTION_INFO_PARAM(running_info.comment_, sizeof(running_info.comment_), __VA_ARGS__)
    ADD_COMMENT("dag_type", OB_DAG_TYPES[dag.get_type()].dag_type_str_);
    ADD_COMMENT("success_cnt", info_.success_cnt_);
    ADD_COMMENT("failure_cnt", info_.failure_cnt_);
    if (0 != info_.errno_) {
      ADD_COMMENT("errno", info_.errno_);
    }
#undef ADD_COMMENT
    if (OB_FAIL(MTL(storage::ObTenantSSTableMergeInfoMgr*)->add_sstable_merge_info(merge_history_))) {
      STORAGE_LOG(WARN, "failed to add sstable merge info", KR(ret), K_(merge_history));
    }
  }
}


} // namespace compaction
} // namespace oceanbase
