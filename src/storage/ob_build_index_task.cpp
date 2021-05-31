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
#include "ob_build_index_task.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/ob_index_status_table_operator.h"
#include "share/ob_index_checksum.h"
#include "storage/ob_partition_scheduler.h"
#include "storage/ob_partition_merge_task.h"
#include "storage/ob_long_ops_monitor.h"
#include "storage/ob_partition_storage.h"
#include "storage/ob_partition_group.h"
#include "storage/ob_pg_storage.h"
#include "ob_i_table.h"
#include "observer/ob_server_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::compaction;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::observer;
using namespace oceanbase::omt;

ObBuildIndexDag::ObBuildIndexDag()
    : ObIDag(DAG_TYPE_CREATE_INDEX, DAG_PRIO_CREATE_INDEX),
      is_inited_(false),
      pkey_(),
      param_(),
      context_(),
      partition_service_(NULL),
      guard_(),
      partition_storage_(NULL),
      compat_mode_(ObWorker::CompatMode::INVALID)
{}

ObBuildIndexDag::~ObBuildIndexDag()
{
  clean_up();
}

int ObBuildIndexDag::init(const ObPartitionKey& pkey, ObPartitionService* partition_service)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObBuildIndexDag has already been inited", K(ret));
  } else if (!pkey.is_valid() || NULL == partition_service) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey), KP(partition_service));
  } else {
    pkey_ = pkey;
    partition_service_ = partition_service;
    is_inited_ = true;
    if (OB_FAIL(get_compat_mode_with_table_id(pkey.table_id_, compat_mode_))) {
      STORAGE_LOG(WARN, "failed to get compat mode", K(ret), K(pkey));
    } else if (OB_FAIL(prepare())) {
      STORAGE_LOG(WARN, "fail to prepare building index", K(ret));
    }
  }
  return ret;
}

int64_t ObBuildIndexDag::hash() const
{
  int tmp_ret = OB_SUCCESS;
  int64_t hash_val = 0;
  if (NULL == param_.index_schema_) {
    tmp_ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "index schema must not be NULL", K(tmp_ret));
  } else {
    hash_val = pkey_.hash() + param_.index_schema_->get_table_id();
  }
  return hash_val;
}

bool ObBuildIndexDag::operator==(const ObIDag& other) const
{
  int tmp_ret = OB_SUCCESS;
  bool is_equal = false;
  if (OB_UNLIKELY(this == &other)) {
    is_equal = true;
  } else if (get_type() == other.get_type()) {
    const ObBuildIndexDag& dag = static_cast<const ObBuildIndexDag&>(other);
    if (NULL == param_.index_schema_ || NULL == dag.param_.index_schema_) {
      tmp_ret = OB_ERR_SYS;
      STORAGE_LOG(
          ERROR, "index schema must not be NULL", K(tmp_ret), KP(param_.index_schema_), KP(dag.param_.index_schema_));
    } else {
      is_equal = pkey_ == dag.pkey_ && param_.index_schema_->get_table_id() == dag.param_.index_schema_->get_table_id();
    }
  }
  return is_equal;
}

int ObBuildIndexDag::get_partition(ObPGPartition*& partition)
{
  int ret = OB_SUCCESS;
  partition = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBuildIndexDag has not been inited", K(ret));
  } else if (OB_ISNULL(partition = guard_.get_pg_partition())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, partition must not be NULL", K(ret));
  }
  return ret;
}

int ObBuildIndexDag::get_partition_group(ObIPartitionGroup*& pg)
{
  int ret = OB_SUCCESS;
  pg = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBuildIndexDag has not been inited", K(ret));
  } else if (OB_ISNULL(pg = pg_guard_.get_partition_group())) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "error sys, partition must not be null", K(ret));
  }
  return ret;
}

int ObBuildIndexDag::fill_comment(char* buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t index_id = 0;
  if (NULL != param_.index_schema_) {
    index_id = param_.index_schema_->get_table_id();
  }

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBuildIndexDag has not been inited", K(ret));
  } else if (OB_FAIL(databuff_printf(buf,
                 buf_len,
                 "build index task: key=%s index_id=%ld snapshot_version=%ld",
                 to_cstring(pkey_),
                 index_id,
                 param_.snapshot_version_))) {
    STORAGE_LOG(WARN, "failed to fill comment", K(ret), K(pkey_));
  }

  return ret;
}

int ObBuildIndexDag::get_partition_storage(ObPartitionStorage*& storage)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBuildIndexDag has not been inited", K(ret));
  } else {
    storage = partition_storage_;
  }
  return ret;
}

int ObBuildIndexDag::check_index_need_build(bool& need_build)
{
  int ret = OB_SUCCESS;
  need_build = false;
  ObPartitionStorage* storage = NULL;
  ObSpinLockGuard guard(context_.lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBuildIndexDag is not inited", K(ret));
  } else if (!context_.need_build_) {
    need_build = false;
  } else if (OB_FAIL(get_partition_storage(storage))) {
    STORAGE_LOG(WARN, "fail to get partition storage", K(ret));
  } else if (OB_ISNULL(storage)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, storage must not be NULL", K(ret));
  } else if (OB_FAIL(storage->check_index_need_build(*param_.index_schema_, context_.need_build_))) {
    STORAGE_LOG(WARN, "fail to check index need build", K(ret));
  } else {
    need_build = context_.need_build_;
  }
  return ret;
}

int ObBuildIndexDag::get_partition_service(ObPartitionService*& part_service)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBuildIndexDag has not been inited", K(ret));
  } else {
    part_service = partition_service_;
  }
  return ret;
}

int ObBuildIndexDag::get_pg(ObIPartitionGroup*& pg)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "build index dag is not inited", K(ret));
  } else {
    pg = pg_guard_.get_partition_group();
  }
  return ret;
}

int ObBuildIndexDag::prepare()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "dag is not init", K(ret));
  } else if (OB_ISNULL(partition_service_) || OB_UNLIKELY(!pkey_.is_valid())) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "invalid inner status", K(ret), KP(partition_service_), K(pkey_));
  } else if (OB_FAIL(partition_service_->get_partition(pkey_, pg_guard_)) ||
             OB_ISNULL(pg_guard_.get_partition_group())) {
    STORAGE_LOG(WARN, "fail to get partition", K_(pkey), K(ret));
  } else if (OB_FAIL(pg_guard_.get_partition_group()->get_pg_partition(pkey_, guard_)) ||
             OB_ISNULL(guard_.get_pg_partition())) {
    STORAGE_LOG(WARN, "fail to get pg partition", K_(pkey), K(ret));
  } else {
    partition_storage_ = static_cast<ObPartitionStorage*>(guard_.get_pg_partition()->get_storage());
  }
  return ret;
}

int ObBuildIndexDag::clean_up()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObBuildIndexDag has not been inited", K(ret));
  } else {
    bool need_retry = true;
    if (context_.need_build_ && !context_.is_report_succ_ && OB_FAIL(context_.build_index_ret_)) {
      if (OB_FAIL(ObReportIndexStatusTask::report_index_status(pkey_, param_, &context_, need_retry))) {
        STORAGE_LOG(ERROR, "fail to report index status", K(ret), K(pkey_), K(param_), K(context_));
      }
    }
    if (NULL != context_.output_sstable_) {
      context_.output_sstable_ = NULL;
      context_.output_sstable_handle_.reset();
    }
  }
  STORAGE_LOG(INFO, "build index finish", K(context_.build_index_ret_), K(pkey_), K(param_), K(context_));
  return ret;
}

ObIndexPrepareTask::ObIndexPrepareTask() : ObITask(TASK_TYPE_INDEX_PERPARE), is_inited_(false), param_(), context_(NULL)
{}

ObIndexPrepareTask::~ObIndexPrepareTask()
{}

int ObIndexPrepareTask::init(ObBuildIndexParam& param, ObBuildIndexContext* context)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObIndexPrepareTask has already been inited", K(ret));
  } else if (!param.is_valid() || OB_ISNULL(context)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(param), KP(context));
  } else {
    param_ = &param;
    context_ = context;
    is_inited_ = true;
  }
  return ret;
}

int ObIndexPrepareTask::process()
{
  int ret = OB_SUCCESS;
  ObIDag* tmp_dag = get_dag();
  ObBuildIndexDag* dag = NULL;
  ObIndexLocalSortTask* local_sort_task = NULL;
  ObIndexMergeTask* merge_task = NULL;
  ObCompactToLatestTask* compact_task = NULL;
  ObUniqueCheckingTask* checking_task = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObIndexPrepareTask has not been inited", K(ret));
  } else if (NULL == tmp_dag || ObIDag::DAG_TYPE_CREATE_INDEX != tmp_dag->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "dag is invalid", K(ret), KP(tmp_dag));
  } else if (FALSE_IT(dag = static_cast<ObBuildIndexDag*>(tmp_dag))) {
  } else if (OB_FAIL(generate_local_sort_tasks(dag, local_sort_task))) {
    STORAGE_LOG(WARN, "fail to generate local sort tasks", K(ret));
  } else if (OB_FAIL(generate_index_merge_task(dag, local_sort_task, merge_task))) {
    STORAGE_LOG(WARN, "fail to generate index merge task", K(ret));
  } else if (OB_FAIL(generate_compact_task(dag, merge_task, compact_task))) {
    STORAGE_LOG(WARN, "fail to generate compact task", K(ret));
  } else if (OB_FAIL(generate_unique_checking_task(dag, compact_task, checking_task))) {
    STORAGE_LOG(WARN, "fail to generate unique checking task", K(ret));
  } else if (OB_FAIL(generate_report_index_status_task(dag, checking_task))) {
    STORAGE_LOG(WARN, "fail to generate report index status task", K(ret));
  }

  if (OB_SUCC(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = add_monitor_info(dag))) {
      STORAGE_LOG(WARN, "fail to add monitor info", K(tmp_ret));
    }
  }

  if (OB_FAIL(ret)) {
    context_->build_index_ret_ = ret;
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObIndexPrepareTask::generate_report_index_status_task(ObBuildIndexDag* dag, ObUniqueCheckingTask* checking_task)
{
  int ret = OB_SUCCESS;
  ObReportIndexStatusTask* report_task = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObIndexPrepareTask has not been inited", K(ret));
  } else if (OB_ISNULL(dag)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(dag));
  } else if (OB_FAIL(dag->alloc_task(report_task))) {
    STORAGE_LOG(WARN, "fail to alloc report index status task", K(ret));
  } else if (OB_FAIL(report_task->init(dag->get_partition_key(), *param_, context_))) {
    STORAGE_LOG(WARN, "fail to init report index status task", K(ret));
  } else if (OB_FAIL(checking_task->add_child(*report_task))) {
    STORAGE_LOG(WARN, "fail to add child for report index status task", K(ret));
  } else if (OB_FAIL(dag->add_task(*report_task))) {
    STORAGE_LOG(WARN, "fail to add report index status task", K(ret));
  }
  return ret;
}

int ObIndexPrepareTask::generate_compact_task(
    ObBuildIndexDag* dag, ObIndexMergeTask* merge_task, ObCompactToLatestTask*& compact_task)
{
  int ret = OB_SUCCESS;
  compact_task = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObIndexPrepareTask has not been inited", K(ret));
  } else if (OB_ISNULL(dag) || OB_ISNULL(merge_task)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(dag), KP(merge_task));
  } else if (OB_FAIL(dag->alloc_task(compact_task))) {
    STORAGE_LOG(WARN, "fail to alloc compact task", K(ret));
  } else if (OB_FAIL(compact_task->init(dag->get_partition_key(), *param_, context_))) {
    STORAGE_LOG(WARN, "fail to init compact task", K(ret));
  } else if (OB_FAIL(merge_task->add_child(*compact_task))) {
    STORAGE_LOG(WARN, "fail to add child for index merge task", K(ret));
  } else if (OB_FAIL(dag->add_task(*compact_task))) {
    STORAGE_LOG(WARN, "fail to add compact task", K(ret));
  }
  return ret;
}

int ObIndexPrepareTask::generate_unique_checking_task(
    ObBuildIndexDag* dag, ObCompactToLatestTask* compact_task, ObUniqueCheckingTask*& checking_task)
{
  int ret = OB_SUCCESS;
  checking_task = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObIndexPrepareTask has not been inited", K(ret));
  } else if (OB_ISNULL(dag) || OB_ISNULL(compact_task)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(dag), KP(compact_task));
  } else if (OB_FAIL(dag->alloc_task(checking_task))) {
    STORAGE_LOG(WARN, "fail to alloc checking task", K(ret));
  } else if (OB_FAIL(checking_task->init(*param_, context_))) {
    STORAGE_LOG(WARN, "fail to init unique checking task", K(ret));
  } else if (OB_FAIL(compact_task->add_child(*checking_task))) {
    STORAGE_LOG(WARN, "fail to add child for compact task", K(ret));
  } else if (OB_FAIL(dag->add_task(*checking_task))) {
    STORAGE_LOG(WARN, "fail to add unique checking task", K(ret));
  }
  return ret;
}

int ObIndexPrepareTask::generate_index_merge_task(
    ObBuildIndexDag* dag, ObIndexLocalSortTask* local_sort_task, ObIndexMergeTask*& merge_task)
{
  int ret = OB_SUCCESS;
  merge_task = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObIndexPrepareTask has not been inited", K(ret));
  } else if (OB_ISNULL(dag) || OB_ISNULL(local_sort_task)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(dag), KP(local_sort_task));
  } else if (OB_FAIL(dag->alloc_task(merge_task))) {
    STORAGE_LOG(WARN, "fail to alloc index merge task", K(ret));
  } else if (OB_FAIL(merge_task->init(*param_, context_))) {
    STORAGE_LOG(WARN, "fail to init index merge task", K(ret));
  } else if (OB_FAIL(local_sort_task->add_child(*merge_task))) {
    STORAGE_LOG(WARN, "fail to add child for index local sort task", K(ret));
  } else if (OB_FAIL(dag->add_task(*merge_task))) {
    STORAGE_LOG(WARN, "fail to add index merge task", K(ret));
  }
  return ret;
}

int ObIndexPrepareTask::generate_local_sort_tasks(ObBuildIndexDag* dag, ObIndexLocalSortTask*& local_sort_task)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObIndexPrepareTask has not been inited", K(ret));
  } else if (OB_ISNULL(dag)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(dag));
  } else {
    if (OB_FAIL(dag->alloc_task(local_sort_task))) {
      STORAGE_LOG(WARN, "fail to alloc local sort task", K(ret));
    } else if (OB_FAIL(local_sort_task->init(0, *param_, context_))) {
      STORAGE_LOG(WARN, "fail to init local sort task", K(ret));
    } else if (OB_FAIL(add_child(*local_sort_task))) {
      STORAGE_LOG(WARN, "fail to add child for index prepare task", K(ret));
    } else if (OB_FAIL(dag->add_task(*local_sort_task))) {
      STORAGE_LOG(WARN, "fail to add local sort task to dag", K(ret));
    }
  }
  return ret;
}

int ObIndexPrepareTask::add_monitor_info(ObBuildIndexDag* dag)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObIndexPrepareTask has not been inited", K(ret));
  } else if (OB_ISNULL(dag)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(dag));
  } else {
    const int64_t concurrent_cnt = param_->concurrent_cnt_;
    const int64_t total_task_cnt =
        2 * concurrent_cnt + 1;  // scan(concurrent_cnt), local_sort(concurrent_cnt), merge(1)
    ObPGPartition* partition = NULL;
    if (OB_FAIL(dag->get_partition(partition))) {
      STORAGE_LOG(WARN, "fail to get partition", K(ret));
    } else {
      ObCreateIndexKey key;
      ObCreateIndexPartitionStat part_stat;
      key.index_table_id_ = param_->index_schema_->get_table_id();
      key.tenant_id_ = extract_tenant_id(key.index_table_id_);
      key.partition_id_ = dag->get_partition_key().get_partition_id();
      if (OB_FAIL(key.to_key_string())) {
        STORAGE_LOG(WARN, "fail to key string", K(ret));
      } else {
        part_stat.key_ = key;
        part_stat.common_value_.start_time_ = ObTimeUtility::current_time();
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(LONG_OPS_MONITOR_INSTANCE.add_long_ops_stat(key, part_stat))) {
        STORAGE_LOG(WARN, "fail to add partition stat", K(ret));
      } else {
        // scan task
        for (int64_t i = 0; OB_SUCC(ret) && i < concurrent_cnt; ++i) {
          ObCreateIndexScanTaskStat task_stat;
          task_stat.task_id_ = i;
          task_stat.type_ = ObILongOpsTaskStat::TaskType::SCAN;
          if (OB_FAIL(LONG_OPS_MONITOR_INSTANCE.update_task_stat(key, task_stat))) {
            STORAGE_LOG(WARN, "fail to update task stat", K(ret), K(key));
          }
        }
        // local sort task and merge task
        for (int64_t i = concurrent_cnt; OB_SUCC(ret) && i < total_task_cnt; ++i) {
          ObCreateIndexScanTaskStat task_stat;
          task_stat.task_id_ = i;
          task_stat.type_ = ObILongOpsTaskStat::TaskType::SORT;
          if (OB_FAIL(LONG_OPS_MONITOR_INSTANCE.update_task_stat(key, task_stat))) {
            STORAGE_LOG(WARN, "fail to update task stat", K(ret), K(key));
          }
        }
      }
    }
  }
  return ret;
}

ObIndexLocalSortTask::ObIndexLocalSortTask()
    : ObITask(TASK_TYPE_INDEX_LOCAL_SORT),
      is_inited_(false),
      task_id_(0),
      param_(NULL),
      context_(NULL),
      local_sorter_(NULL)
{}

ObIndexLocalSortTask::~ObIndexLocalSortTask()
{}

int ObIndexLocalSortTask::init(const int64_t task_id, ObBuildIndexParam& param, ObBuildIndexContext* context)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObIndexLocalSortTask has already been inited", K(ret));
  } else if (task_id < 0 || !param.is_valid() || OB_ISNULL(context)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(task_id), K(param), KP(context));
  } else {
    task_id_ = task_id;
    param_ = &param;
    context_ = context;
    is_inited_ = true;
    if (task_id_ >= 0 && task_id < context_->sorters_.count()) {
      local_sorter_ = context_->sorters_.at(task_id);
    }
    if (OB_ISNULL(local_sorter_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "error unexpected, local_sort must not be NULL", K(ret));
    }
  }
  return ret;
}

int ObIndexLocalSortTask::process()
{
  int ret = OB_SUCCESS;
  ObIDag* tmp_dag = get_dag();
  ObBuildIndexDag* dag = NULL;
  ObPartitionStorage* storage = NULL;
  bool need_build = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObIndexLocalSortTask has not been inited", K(ret));
  } else if (NULL == tmp_dag || ObIDag::DAG_TYPE_CREATE_INDEX != tmp_dag->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "dag is invalid", K(ret), KP(tmp_dag));
  } else if (FALSE_IT(dag = static_cast<ObBuildIndexDag*>(tmp_dag))) {
  } else if (OB_FAIL(dag->get_partition_storage(storage))) {
    STORAGE_LOG(WARN, "fail to get partition storage", K(ret));
  } else if (OB_ISNULL(storage)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, storage must not be NULL", K(ret));
  } else if (OB_SUCCESS != (context_->build_index_ret_)) {
    STORAGE_LOG(WARN, "build index has already failed", "ret", context_->build_index_ret_);
  } else if (OB_FAIL(dag->check_index_need_build(need_build))) {
    STORAGE_LOG(WARN, "fail to check index need build", K(ret), K(*param_));
  } else if (!need_build) {
    STORAGE_LOG(INFO, "index do not need build", "index_id", param_->index_schema_->get_table_id());
  } else if (OB_FAIL(storage->local_sort_index_by_range(task_id_, *param_, *context_))) {
    STORAGE_LOG(WARN, "fail to do local sort index by range", K(ret));
  } else {
    STORAGE_LOG(INFO, "finish local sort", "index_id", param_->index_schema_->get_table_id());
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_INDEX_LOCAL_SORT_TASK) OB_SUCCESS;
  }
#endif
  if (OB_FAIL(ret) && NULL != context_) {
    context_->build_index_ret_ = ret;
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObIndexLocalSortTask::generate_next_task(ObITask*& next_task)
{
  int ret = OB_SUCCESS;
  ObIDag* dag = get_dag();
  ObBuildIndexDag* build_index_dag = NULL;
  ObIndexLocalSortTask* index_local_sort_task = NULL;
  const int64_t next_task_id = task_id_ + 1;
  next_task = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObIndexLocalSortTask has not been inited", K(ret));
  } else if (next_task_id == param_->concurrent_cnt_) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, dag must not be NULL", K(ret));
  } else if (OB_UNLIKELY(ObIDag::DAG_TYPE_CREATE_INDEX != dag->get_type())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, dag type is invalid", K(ret), "dag type", dag->get_type());
  } else if (FALSE_IT(build_index_dag = static_cast<ObBuildIndexDag*>(dag))) {
  } else if (OB_FAIL(build_index_dag->alloc_task(index_local_sort_task))) {
    STORAGE_LOG(WARN, "fail to alloc task", K(ret));
  } else if (OB_FAIL(index_local_sort_task->init(next_task_id, *param_, context_))) {
    STORAGE_LOG(WARN, "fail to init index local task", K(ret));
  } else {
    next_task = index_local_sort_task;
    STORAGE_LOG(INFO, "generate next local sort task", K(ret), "index_id", param_->index_schema_->get_table_id());
  }
  if (OB_FAIL(ret) && NULL != context_) {
    if (OB_ITER_END != ret) {
      context_->build_index_ret_ = ret;
    }
  }
  return ret;
}

ObIndexMergeTask::ObIndexMergeTask()
    : ObITask(TASK_TYPE_INDEX_MERGE), is_inited_(false), param_(), context_(NULL), sorters_(NULL)
{}

ObIndexMergeTask::~ObIndexMergeTask()
{}

int ObIndexMergeTask::init(ObBuildIndexParam& param, ObBuildIndexContext* context)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObIndexMergeTask has already been inited", K(ret));
  } else if (!param.is_valid() || OB_ISNULL(context)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(param), KP(context));
  } else {
    param_ = &param;
    context_ = context;
    sorters_ = &context_->sorters_;
    is_inited_ = true;
  }
  return ret;
}

int ObIndexMergeTask::process()
{
  int ret = OB_SUCCESS;
  ObIDag* tmp_dag = get_dag();
  ObBuildIndexDag* dag = NULL;
  ObPartitionStorage* storage = NULL;
  ObIPartitionGroup* pg = nullptr;
  bool need_build = false;
  ObTableHandle new_sstable;
  ObSSTable* sstable = nullptr;
  const int64_t max_kept_major_version_number = 1;
  const bool in_slog_trans = false;
  if (OB_ISNULL(sorters_) || 0 == sorters_->count() || OB_ISNULL(tmp_dag) || OB_ISNULL(context_)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "invalid inner state", K(ret), KP(sorters_), KP(tmp_dag), KP(context_));
  } else if (OB_SUCCESS != (context_->build_index_ret_)) {
    STORAGE_LOG(WARN, "Build Index has already failed", "ret", context_->build_index_ret_);
  } else if (ObIDag::DAG_TYPE_CREATE_INDEX != tmp_dag->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "dag is invalid", K(ret), K(tmp_dag->get_type()));
  } else if (FALSE_IT(dag = static_cast<ObBuildIndexDag*>(tmp_dag))) {
  } else if (OB_FAIL(dag->get_partition_storage(storage))) {
    STORAGE_LOG(WARN, "fail to get partition storage", K(ret));
  } else if (OB_ISNULL(storage)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, storage must not be NULL", K(ret));
  } else if (OB_FAIL(dag->get_pg(pg))) {
    STORAGE_LOG(WARN, "failed to get pg", K(ret));
  } else if (OB_ISNULL(pg)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "pg is null", K(ret));
  } else if (OB_FAIL(dag->check_index_need_build(need_build))) {
    STORAGE_LOG(WARN, "fail to check index need build", K(ret), K(*param_));
  } else if (!need_build) {
    STORAGE_LOG(INFO, "index do not need build", "index_id", param_->index_schema_->get_table_id());
  } else if (OB_FAIL(merge_local_sort_index(*param_, *sorters_, merge_sorter_, context_, new_sstable))) {
    STORAGE_LOG(WARN, "fail to merge local sort index", K(ret));
  } else if (OB_FAIL(new_sstable.get_sstable(sstable))) {
    STORAGE_LOG(WARN, "failed to get sstable", K(ret));
  } else if (OB_FAIL(pg->get_pg_storage().add_sstable(
                 dag->get_partition_key(), sstable, max_kept_major_version_number, in_slog_trans))) {
    STORAGE_LOG(WARN, "failed to add sstable", K(ret), K(*param_));
  } else if (OB_FAIL(param_->report_->submit_checksum_update_task(dag->get_partition_key(),
                 param_->index_schema_->get_table_id(),
                 ObITable::MAJOR_SSTABLE,
                 ObSSTableChecksumUpdateType::UPDATE_ALL,
                 false /*do not batch report immedidately*/))) {
    STORAGE_LOG(WARN, "fail to submit checksum update task", K(ret));
  }

  STORAGE_LOG(INFO, "do clean up merge sorter");
  merge_sorter_.clean_up();
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_INDEX_MERGE_TASK) OB_SUCCESS;
  }
#endif
  if (OB_FAIL(ret) && NULL != context_) {
    context_->build_index_ret_ = ret;
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObIndexMergeTask::merge_local_sort_index(const ObBuildIndexParam& param,
    const ObIArray<ObExternalSort<ObStoreRow, ObStoreRowComparer>*>& local_sorters,
    ObExternalSort<ObStoreRow, ObStoreRowComparer>& merge_sorter, ObBuildIndexContext* context,
    ObTableHandle& new_sstable)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int comp_ret = OB_SUCCESS;
  ObArray<int64_t> sort_column_indexes;
  ObStoreRowComparer comparer(comp_ret, sort_column_indexes);
  ObCreateIndexKey key;
  ObCreateIndexSortTaskStat sort_task_stat;
  const int64_t file_buf_size = ObExternalSortConstant::DEFAULT_FILE_READ_WRITE_BUFFER;
  const int64_t expire_timestamp = 0;  // no time limited
  const int64_t buf_limit = param.DEFAULT_INDEX_SORT_MEMORY_LIMIT;
  ObPGPartition* partition = NULL;
  ObIDag* tmp_dag = get_dag();
  ObBuildIndexDag* dag = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionStorage has not been inited", K(ret));
  } else if (!param.is_valid() || 0 == local_sorters.count() || OB_ISNULL(context)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(param), K(local_sorters.count()), KP(context));
  } else if (ObIDag::DAG_TYPE_CREATE_INDEX != tmp_dag->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "dag is invalid", K(ret), K(tmp_dag->get_type()));
  } else if (FALSE_IT(dag = static_cast<ObBuildIndexDag*>(tmp_dag))) {
  } else if (OB_FAIL(dag->get_partition(partition))) {
    STORAGE_LOG(WARN, "fail to get partition", K(ret));
  } else {
    key.index_table_id_ = param.index_schema_->get_table_id();
    key.tenant_id_ = extract_tenant_id(key.index_table_id_);
    key.partition_id_ = partition->get_partition_key().get_partition_id();
    sort_task_stat.task_id_ = 2 * param.concurrent_cnt_;
    sort_task_stat.type_ = ObILongOpsTaskStat::TaskType::SORT;
    sort_task_stat.state_ = ObILongOpsTaskStat::TaskState::RUNNING;
    sort_task_stat.macro_count_ = context->index_macro_cnt_;
    sort_task_stat.run_count_ = 1;
    if (OB_SUCCESS != (tmp_ret = LONG_OPS_MONITOR_INSTANCE.update_task_stat(key, sort_task_stat))) {
      STORAGE_LOG(WARN, "fail to update task stat", K(ret), K(sort_task_stat));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < param.index_schema_->get_rowkey_column_num(); ++i) {
    if (OB_FAIL(sort_column_indexes.push_back(i))) {
      STORAGE_LOG(WARN, "Fail to push sort column indexes, ", K(ret), K(i));
    }
  }

  STORAGE_LOG(INFO, "begin merging index local sort result", K(ret), K(param));
  if (OB_SUCC(ret)) {
    const uint64_t tenant_id = extract_tenant_id(param.index_schema_->get_table_id());
    if (OB_FAIL(merge_sorter.init(buf_limit, file_buf_size, expire_timestamp, tenant_id, &comparer))) {
      STORAGE_LOG(WARN, "fail to init merge sorter", K(ret));
    } else {
      const bool is_final_merge = true;
      for (int64_t i = 0; OB_SUCC(ret) && i < local_sorters.count(); ++i) {
        ObExternalSort<ObStoreRow, ObStoreRowComparer>* local_sort = local_sorters.at(i);
        if (OB_ISNULL(local_sort)) {
          ret = OB_ERR_SYS;
          STORAGE_LOG(WARN, "local_sort must not be NULL", K(ret));
        } else if (OB_FAIL(local_sort->transfer_final_sorted_fragment_iter(merge_sorter))) {
          STORAGE_LOG(WARN, "fail to get final sorted fragment iterator", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(merge_sorter.do_sort(is_final_merge))) {
          STORAGE_LOG(WARN, "fail to do final merge", K(ret));
        } else if (OB_FAIL(add_build_index_sstable(param, merge_sorter, context, new_sstable))) {
          STORAGE_LOG(WARN, "fail to add build index sstable", K(ret));
        } else {
          STORAGE_LOG(INFO, "finish merging index local sort result", K(ret));
        }
      }
    }
  }
  sort_task_stat.state_ =
      (OB_SUCCESS == ret) ? ObILongOpsTaskStat::TaskState::SUCCESS : ObILongOpsTaskStat::TaskState::FAIL;
  if (OB_SUCCESS != (tmp_ret = key.to_key_string())) {
    STORAGE_LOG(WARN, "fail to key string", K(ret));
  } else if (OB_SUCCESS != (tmp_ret = LONG_OPS_MONITOR_INSTANCE.update_task_stat(key, sort_task_stat))) {
    STORAGE_LOG(WARN, "fail to update task stat", K(ret));
  }
  return ret;
}

int ObIndexMergeTask::add_build_index_sstable(const ObBuildIndexParam& param,
    ObExternalSort<ObStoreRow, ObStoreRowComparer>& external_sort, ObBuildIndexContext* context,
    ObTableHandle& new_sstable)
{
  int ret = OB_SUCCESS;
  ObColumnChecksumCalculator checksum;
  const bool is_major_merge = true;
  ObMacroDataSeq macro_start_seq(0);
  ObIDag* tmp_dag = get_dag();
  ObBuildIndexDag* dag = nullptr;
  ObPGPartition* partition = nullptr;
  ObIPartitionGroup* pg = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionStorage has not been inited", K(ret));
  } else if (!param.is_valid() || OB_ISNULL(context)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(param), KP(context));
  } else if (ObIDag::DAG_TYPE_CREATE_INDEX != tmp_dag->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "dag is invalid", K(ret), K(tmp_dag->get_type()));
  } else if (FALSE_IT(dag = static_cast<ObBuildIndexDag*>(tmp_dag))) {
  } else if (OB_FAIL(dag->get_partition(partition))) {
    STORAGE_LOG(WARN, "fail to get partition", K(ret));
  } else if (OB_FAIL(checksum.init(param.index_schema_->get_column_count()))) {
    STORAGE_LOG(WARN, "fail to init checksum", K(ret));
  } else if (OB_FAIL(dag->get_pg(pg))) {
    STORAGE_LOG(WARN, "fail to get pg", K(ret));
  } else if (OB_FAIL(data_desc_.init(*param.index_schema_,
                 param.version_,
                 nullptr,
                 partition->get_partition_key().get_partition_id(),
                 MAJOR_MERGE,
                 blocksstable::CCM_VALUE_ONLY == param.checksum_method_ /*need calc column checksum*/,
                 true /*store column checksum in micro block*/,
                 pg->get_partition_key(),
                 pg->get_storage_file_handle()))) {
    STORAGE_LOG(WARN, "Fail to init data store desc, ", K(ret));
  } else if (OB_FAIL(writer_.open(data_desc_, macro_start_seq))) {
    STORAGE_LOG(WARN, "Fail to open macro block writer, ", K(ret));
  } else {
    common::ObArenaAllocator allocator(ObModIds::OB_SSTABLE_CREATE_INDEX);
    const ObStoreRow* row = NULL;
    int64_t row_count = 0;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(external_sort.get_next_item(row))) {
        if (OB_ITER_END != ret) {
          STORAGE_LOG(WARN, "Fail to get next row from external sort, ", K(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(writer_.append_row(*row))) {
        if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret && param.index_schema_->is_unique_index()) {
          LOG_USER_ERROR(OB_ERR_PRIMARY_KEY_DUPLICATE, "", static_cast<int>(sizeof("UNIQUE IDX") - 1), "UNIQUE IDX");
        } else {
          STORAGE_LOG(WARN, "Fail to append row to sstable, ", K(ret));
        }
      } else if (OB_FAIL(checksum.calc_column_checksum(param.checksum_method_, row, NULL, NULL))) {
        STORAGE_LOG(WARN, "fail to calc column checksum", K(ret));
      } else {
#ifdef ERRSIM
        if (OB_SUCC(ret)) {
          ret = E(EventTable::EN_INDEX_WRITE_BLOCK) OB_SUCCESS;
        }
#endif
        ++row_count;
      }
    }
    STORAGE_LOG(INFO, "index table row count", K(row_count), "index_id", param.index_schema_->get_table_id());

    if (OB_SUCC(ret)) {
      if (OB_FAIL(writer_.close())) {
        STORAGE_LOG(WARN, "fail to close writer", K(ret));
      } else if (!param.index_schema_->is_domain_index() &&
                 OB_FAIL(
                     context->check_column_checksum(checksum.get_column_checksum(), row_count, context->column_cnt_))) {
        STORAGE_LOG(WARN, "fail to check column checksum", K(ret));
      } else if (OB_FAIL(add_new_index_sstable(param, &writer_, checksum.get_column_checksum(), new_sstable))) {
        STORAGE_LOG(WARN, "fail to update sstore", K(ret));
      }
    }
  }

  if (OB_FAIL(ret)) {
    writer_.reset();
  }
  return ret;
}

int ObIndexMergeTask::add_new_index_sstable(const ObBuildIndexParam& param, ObMacroBlockWriter* writer,
    const int64_t* column_checksum, ObTableHandle& new_sstable)
{
  int ret = OB_SUCCESS;
  ObIDag* tmp_dag = get_dag();
  ObBuildIndexDag* dag = nullptr;
  ObIPartitionGroup* pg = nullptr;
  ObPGPartition* pg_partition = nullptr;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObPartitionStorage has not been inited", K(ret));
  } else if (!param.is_valid() || OB_ISNULL(writer) || OB_ISNULL(column_checksum)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(param), KP(writer), KP(column_checksum));
  } else if (ObIDag::DAG_TYPE_CREATE_INDEX != tmp_dag->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "dag is invalid", K(ret), K(tmp_dag->get_type()));
  } else if (FALSE_IT(dag = static_cast<ObBuildIndexDag*>(tmp_dag))) {
  } else if (OB_FAIL(dag->get_partition_group(pg))) {
    STORAGE_LOG(WARN, "fail to get partition group", K(ret));
  } else if (OB_ISNULL(pg)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "error sys, invalid partition group", K(ret));
  } else if (OB_FAIL(dag->get_partition(pg_partition))) {
    STORAGE_LOG(WARN, "fail to get partition", K(ret));
  } else {
    const ObTableSchema& index_schema = *param.index_schema_;
    ObITable::TableKey table_key;
    ObPGCreateSSTableParam pg_create_sstable_param;
    ObCreateSSTableParamWithTable sstable_param;

    table_key.table_type_ = ObITable::MAJOR_SSTABLE;
    table_key.pkey_ = pg_partition->get_partition_key();
    table_key.table_id_ = index_schema.get_table_id();
    table_key.trans_version_range_.multi_version_start_ = param.snapshot_version_;
    table_key.trans_version_range_.base_version_ = ObVersionRange::MIN_VERSION;
    table_key.trans_version_range_.snapshot_version_ = param.snapshot_version_;
    table_key.version_ = param.version_;

    sstable_param.table_key_ = table_key;
    sstable_param.schema_ = &index_schema;
    sstable_param.schema_version_ = param.schema_version_;
    sstable_param.create_snapshot_version_ = param.snapshot_version_;
    sstable_param.checksum_method_ = param.checksum_method_;
    sstable_param.progressive_merge_round_ = index_schema.get_progressive_merge_round();
    sstable_param.progressive_merge_step_ = 0;
    sstable_param.logical_data_version_ = table_key.version_;
    pg_create_sstable_param.with_table_param_ = &sstable_param;

    if (OB_FAIL(pg_create_sstable_param.data_blocks_.push_back(&writer->get_macro_block_write_ctx()))) {
      LOG_WARN("fail to push back writer macro block ctx", K(ret));
    } else if (OB_FAIL(pg->create_sstable(pg_create_sstable_param, new_sstable))) {
      LOG_WARN("fail to create sstable", K(ret));
    }
  }

  return ret;
}

ObCompactToLatestTask::ObCompactToLatestTask()
    : ObITask(ObITaskType::TASK_TYPE_COMPACT_TO_LASTEST), is_inited_(false), pkey_(), param_(NULL), context_(NULL)
{}

ObCompactToLatestTask::~ObCompactToLatestTask()
{}

int ObCompactToLatestTask::init(
    const ObPartitionKey& pkey, const compaction::ObBuildIndexParam& param, compaction::ObBuildIndexContext* context)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObCompactToLatestTask has already been inited", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid() || !param.is_valid() || OB_ISNULL(context))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(pkey), K(param), KP(context));
  } else {
    pkey_ = pkey;
    param_ = &param;
    context_ = context;
    is_inited_ = true;
  }
  return ret;
}

int ObCompactToLatestTask::process()
{
  int ret = OB_SUCCESS;
  ObIDag* tmp_dag = get_dag();
  ObBuildIndexDag* dag = NULL;
  bool need_build = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCompactToLatestTask has not been inited", K(ret));
  } else if (!context_->need_build_) {
    STORAGE_LOG(INFO, "index does not need build", K(*param_));
  } else if (FALSE_IT(dag = static_cast<ObBuildIndexDag*>(tmp_dag))) {
  } else if (OB_FAIL(dag->check_index_need_build(need_build))) {
    STORAGE_LOG(WARN, "fail to check index need build", K(ret), K(*param_));
  } else if (!need_build) {
    STORAGE_LOG(INFO, "index do not need build", "index_id", param_->index_schema_->get_table_id());
  } else if (OB_SUCCESS != (context_->build_index_ret_)) {
    STORAGE_LOG(WARN, "Build Index has already failed", "ret", context_->build_index_ret_);
  } else if (OB_FAIL(wait_compact_to_latest(pkey_, param_->index_schema_->get_table_id()))) {
    STORAGE_LOG(WARN, "fail to wait compact to latest", K(ret));
  }
  if (OB_FAIL(ret) && NULL != context_) {
    context_->build_index_ret_ = ret;
    ret = OB_SUCCESS;
  }
  LOG_INFO("compact task ret", "ret", context_->build_index_ret_);
  return ret;
}

int ObCompactToLatestTask::wait_compact_to_latest(const ObPartitionKey& pkey, const uint64_t index_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!pkey.is_valid() || OB_INVALID_ID == index_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(pkey), K(index_id));
  } else {
    int tmp_ret = OB_SUCCESS;
    bool is_merged = false;
    while (OB_SUCC(ret)) {
      bool is_finished = false;
      if (OB_SUCCESS != (tmp_ret = ObPartitionScheduler::get_instance().schedule_merge(pkey, is_merged))) {
        LOG_WARN("fail to schedule merge", K(tmp_ret));
      }
      if (OB_FAIL(ObPartitionScheduler::get_instance().check_index_compact_to_latest(pkey, index_id, is_finished))) {
        LOG_WARN("fail to check index compact to latest", K(ret));
      } else if (is_finished) {
        break;
      }
      if (OB_SUCC(ret)) {
        usleep(RETRY_INTERVAL);
        dag_yield();
      }
    }
  }
  LOG_INFO("compact index to latest finish", K(ret), K(index_id));
  return ret;
}

ObUniqueIndexChecker::ObUniqueIndexChecker()
    : is_inited_(false),
      part_service_(NULL),
      pkey_(),
      index_schema_(NULL),
      data_table_schema_(NULL),
      execution_id_(0),
      snapshot_version_(0),
      part_guard_()
{}

int ObUniqueIndexChecker::init(ObPartitionService* part_service, const common::ObPartitionKey& pkey,
    const ObTableSchema* data_table_schema, const ObTableSchema* index_schema, const uint64_t execution_id,
    const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObUniqueIndexChecker has already been inited", K(ret));
  } else if (OB_UNLIKELY(
                 NULL == part_service || !pkey.is_valid() || NULL == data_table_schema || NULL == index_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(part_service), K(pkey), KP(data_table_schema), KP(index_schema));
  } else {
    is_inited_ = true;
    part_service_ = part_service;
    pkey_ = pkey;
    data_table_schema_ = data_table_schema;
    index_schema_ = index_schema;
    execution_id_ = execution_id;
    snapshot_version_ = snapshot_version;
  }
  return ret;
}

int ObUniqueIndexChecker::calc_column_checksum(const int64_t column_cnt, ObIStoreRowIterator& iterator,
    common::ObIArray<int64_t>& column_checksum, int64_t& row_count)
{
  int ret = OB_SUCCESS;
  row_count = 0;
  column_checksum.reuse();
  if (OB_UNLIKELY(column_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(column_cnt));
  } else if (OB_FAIL(column_checksum.reserve(column_cnt))) {
    STORAGE_LOG(WARN, "fail to reserve column", K(ret), K(column_cnt));
  } else {
    const ObStoreRow* row = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
      if (OB_FAIL(column_checksum.push_back(0))) {
        STORAGE_LOG(WARN, "fail to push back column checksum", K(ret));
      }
    }
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iterator.get_next_row(row))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          STORAGE_LOG(WARN, "fail to get next row", K(ret));
        }
      } else if (OB_ISNULL(row)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "store row must not be NULL", K(ret), KP(row));
      } else if (column_cnt != row->row_val_.count_) {
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(
            WARN, "column cnt not as expected", K(ret), K(column_cnt), "row_val_column_cnt", row->row_val_.count_);
      } else {
        ++row_count;
        for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
          column_checksum.at(i) += row->row_val_.cells_[i].checksum(0);
        }
        dag_yield();
      }
    }
  }
  return ret;
}

int ObUniqueIndexChecker::scan_table_with_column_checksum(
    const ObScanTableParam& param, ObIArray<int64_t>& column_checksum, int64_t& row_count)
{
  int ret = OB_SUCCESS;
  ObPartitionStorage* storage = NULL;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(param));
  } else if (OB_ISNULL(part_guard_.get_pg_partition())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, partition must not be NULL", K(ret));
  } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(part_guard_.get_pg_partition()->get_storage()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, storage must not be NULL", K(ret));
  } else {
    ObQueryFlag query_flag(ObQueryFlag::Forward,
        true,  /*is daily merge scan*/
        true,  /*is read multiple macro block*/
        true,  /*sys task scan, read one macro block in single io*/
        false, /*is full row scan?*/
        false,
        false);
    ObArray<ObColumnParam*> col_params;
    ObArray<uint64_t> output_column_ids;
    ObTableAccessParam access_param;
    ObTableAccessContext access_ctx;
    ObBlockCacheWorkingSet block_cache_ws;
    ObStoreCtx ctx;
    ObArenaAllocator allocator(ObModIds::OB_CS_BUILD_INDEX);
    ObMultipleScanMerge* scan_merge = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < param.col_ids_->count(); i++) {
      const ObColumnSchemaV2* col = param.index_schema_->get_column_schema(param.col_ids_->at(i).col_id_);
      if (NULL == col) {
        // generated column's depend column, get column schema from data table
        col = param.data_table_schema_->get_column_schema(param.col_ids_->at(i).col_id_);
      }
      if (NULL == col) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "fail to get column schema", K(ret));
      } else {
        void* buf = allocator.alloc(sizeof(ObColumnParam));
        if (NULL == buf) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "fail to allocate memory", K(ret));
        } else {
          ObColumnParam* col_param = new (buf) ObColumnParam(allocator);
          if (OB_FAIL(ObTableParam::convert_column_schema_to_param(*col, *col_param))) {
            STORAGE_LOG(WARN, "fail to convert schema column to column parameter", K(ret));
          } else if (OB_FAIL(col_params.push_back(col_param))) {
            STORAGE_LOG(WARN, "fail to push array", K(ret));
          } else if (OB_FAIL(output_column_ids.push_back(param.col_ids_->at(i).col_id_))) {
            LOG_WARN("push back output column id fail", K(ret), K(param.col_ids_->at(i)));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      transaction::ObTransService* txs = ObPartitionService::get_instance().get_trans_service();
      const ObTableSchema& query_schema = param.is_scan_index_ ? *param.index_schema_ : *param.data_table_schema_;
      if (OB_ISNULL(ctx.mem_ctx_ = txs->get_mem_ctx_factory()->alloc())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(ERROR, "fail to allocate transaction memory context", K(ret));
      } else if (OB_FAIL(ctx.mem_ctx_->trans_begin())) {
        STORAGE_LOG(WARN, "fail to begin transaction", K(ret));
      } else if (OB_FAIL(ctx.mem_ctx_->sub_trans_begin(param.snapshot_version_, BUILD_INDEX_READ_SNAPSHOT_VERSION))) {
        STORAGE_LOG(WARN, "fail to begin sub transaction", K(ret), K(param.snapshot_version_));
      } else if (OB_FAIL(block_cache_ws.init(extract_tenant_id(param.index_schema_->get_table_id())))) {
        LOG_WARN("fail to init block cache working set", K(ret));
      } else if (OB_FAIL(access_param.out_col_desc_param_.init())) {
        LOG_WARN("fail to init out cols", K(ret));
      } else if (OB_FAIL(access_param.out_col_desc_param_.assign(*param.col_ids_))) {
        LOG_WARN("fail to assign out cols", K(ret));
      } else {
        void* buf = NULL;
        ObExtStoreRange range;
        ObGetTableParam get_table_param;
        range.get_range().set_whole_range();
        access_param.iter_param_.table_id_ = query_schema.get_table_id();
        access_param.iter_param_.rowkey_cnt_ = query_schema.get_rowkey_column_num();
        access_param.iter_param_.schema_version_ = query_schema.get_schema_version();
        access_param.iter_param_.out_cols_project_ = param.output_projector_;
        access_param.iter_param_.out_cols_ = &access_param.out_col_desc_param_.get_col_descs();
        access_param.out_cols_param_ = &col_params;
        access_param.reserve_cell_cnt_ = param.output_projector_->count();

        common::ObVersionRange trans_version_range;
        trans_version_range.snapshot_version_ = param.snapshot_version_;
        trans_version_range.multi_version_start_ = param.snapshot_version_;
        trans_version_range.base_version_ = 0;

        get_table_param.partition_store_ = &storage->get_partition_store();

        ObPartitionKey pg_key;

        if (OB_FAIL(ObPartitionService::get_instance().get_pg_key(pkey_, pg_key))) {
          STORAGE_LOG(WARN, "failed to get_pg_key", K(ret), K_(pkey));
        } else if (OB_FAIL(ctx.init_trans_ctx_mgr(pg_key))) {
          STORAGE_LOG(WARN, "failed to init_ctx_mgr", K(ret), K(pg_key));
        } else if (OB_FAIL(
                       access_ctx.init(query_flag, ctx, allocator, allocator, block_cache_ws, trans_version_range))) {
          LOG_WARN("Failed to init trans_version range", K(ret));
        } else if (OB_ISNULL(buf = allocator.alloc(sizeof(ObMultipleScanMerge)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "fail to allocate memory", K(ret));
        } else if (OB_ISNULL(scan_merge = new (buf) ObMultipleScanMerge())) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "error unexpected, placement new failed", K(ret));
        } else if (OB_FAIL(scan_merge->init(access_param, access_ctx, get_table_param))) {
          STORAGE_LOG(WARN, "fail to init multiple scan merge", K(ret));
        } else if (OB_FAIL(scan_merge->open(range))) {
          STORAGE_LOG(WARN, "fail to open scan merge", K(ret));
        } else {
          const_cast<ObScanTableParam&>(param).tables_handle_->reset();
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(calc_column_checksum(
                       access_param.iter_param_.out_cols_project_->count(), *scan_merge, column_checksum, row_count))) {
          STORAGE_LOG(WARN, "fail to calc column checksum", K(ret));
        }
      }
      if (NULL != scan_merge) {
        scan_merge->~ObMultipleScanMerge();
        scan_merge = NULL;
      }
      if (NULL != ctx.mem_ctx_) {
        ctx.mem_ctx_->trans_end(true, 0);
        ctx.mem_ctx_->trans_clear();
        txs->get_mem_ctx_factory()->free(ctx.mem_ctx_);
        ctx.mem_ctx_ = NULL;
      }
    }
  }
  return ret;
}

int ObUniqueIndexChecker::scan_main_table_with_column_checksum(const ObTableSchema& data_table_schema,
    const ObTableSchema& index_schema, const int64_t snapshot_version, ObTablesHandle& tables_handle,
    ObIArray<int64_t>& column_checksum, int64_t& row_count)
{
  int ret = OB_SUCCESS;
  ObArray<share::schema::ObColDesc> col_ids;
  ObArray<share::schema::ObColDesc> org_col_ids;
  ObArray<int32_t> output_projector;
  int64_t unique_key_cnt = 0;
  if (OB_UNLIKELY(!data_table_schema.is_valid() || !index_schema.is_valid() || snapshot_version <= 0 ||
                  0 == tables_handle.get_count())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "invalid arguments",
        K(ret),
        K(data_table_schema),
        K(index_schema),
        K(snapshot_version),
        K(tables_handle));
  } else if (OB_FAIL(ObPartitionStorage::generate_index_output_param(
                 data_table_schema, index_schema, col_ids, org_col_ids, output_projector, unique_key_cnt))) {
    STORAGE_LOG(WARN, "fail to generate index output param", K(ret));
  } else {
    ObScanTableParam param;
    param.data_table_schema_ = &data_table_schema;
    param.index_schema_ = &index_schema;
    param.snapshot_version_ = snapshot_version;
    param.col_ids_ = &col_ids;
    param.output_projector_ = &output_projector;
    param.is_scan_index_ = false;
    param.tables_handle_ = &tables_handle;
    if (OB_FAIL(scan_table_with_column_checksum(param, column_checksum, row_count))) {
      STORAGE_LOG(WARN, "fail to scan table with column checksum", K(ret));
    }
  }
  return ret;
}

int ObUniqueIndexChecker::scan_index_table_with_column_checksum(const ObTableSchema& data_table_schema,
    const ObTableSchema& index_schema, const int64_t snapshot_version, ObTablesHandle& tables_handle,
    ObIArray<int64_t>& column_checksum, int64_t& row_count)
{
  int ret = OB_SUCCESS;
  ObArray<ObColDesc> column_ids;
  if (OB_UNLIKELY(!index_schema.is_valid() || snapshot_version <= 0 || 0 == tables_handle.get_count())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(index_schema), K(snapshot_version), K(tables_handle));
  } else if (OB_FAIL(index_schema.get_column_ids(column_ids))) {
    STORAGE_LOG(WARN, "fail to get column ids", K(ret), "index_id", index_schema.get_table_id());
  } else {
    ObArray<int32_t> output_projector;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
      if (OB_FAIL(output_projector.push_back(static_cast<int32_t>(i)))) {
        STORAGE_LOG(WARN, "fail to push back output projector", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      ObScanTableParam param;
      param.data_table_schema_ = &data_table_schema;
      param.index_schema_ = &index_schema;
      param.snapshot_version_ = snapshot_version;
      param.col_ids_ = &column_ids;
      param.output_projector_ = &output_projector;
      param.is_scan_index_ = true;
      param.tables_handle_ = &tables_handle;
      if (OB_FAIL(scan_table_with_column_checksum(param, column_checksum, row_count))) {
        STORAGE_LOG(WARN, "fail to scan table with column checksum", K(ret));
      }
    }
  }
  return ret;
}

int ObUniqueIndexChecker::check_global_index(ObIDag* dag, ObPartitionStorage* storage)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dag) || OB_ISNULL(storage)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(dag), KP(storage));
  } else {
    ObTablesHandle tables_handle;
    ObArray<int64_t> column_checksum;
    int64_t row_count = 0;
    if (OB_FAIL(try_get_read_tables(pkey_.get_table_id(), snapshot_version_, dag, tables_handle))) {
      LOG_WARN("fail to try get read tables", K(ret));
    }

    if (OB_SUCC(ret) && !dag->has_set_stop()) {
      if (pkey_.get_table_id() == data_table_schema_->get_table_id()) {
        if (OB_FAIL(scan_main_table_with_column_checksum(
                *data_table_schema_, *index_schema_, snapshot_version_, tables_handle, column_checksum, row_count))) {
          STORAGE_LOG(WARN, "fail to scan main table with column checksum", K(ret));
        }
      } else if (pkey_.get_table_id() == index_schema_->get_table_id()) {
        if (OB_FAIL(scan_index_table_with_column_checksum(
                *data_table_schema_, *index_schema_, snapshot_version_, tables_handle, column_checksum, row_count))) {
          STORAGE_LOG(WARN, "fail to scan index table with column checksum", K(ret));
        }
      }
    }

    if (OB_SUCC(ret) && !dag->has_set_stop()) {
      ObArray<ObColDesc> column_ids;
      if (OB_FAIL(index_schema_->get_column_ids(column_ids))) {
        STORAGE_LOG(WARN, "fail to get columns ids", K(ret));
      } else if (column_ids.count() != column_checksum.count()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(
            WARN, "error unexpected, column count mismatch", K(ret), K(column_ids.count()), K(column_checksum.count()));
      } else {
        ObArray<ObIndexChecksumItem> checksum_items;
        for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
          // generated column in index schema is not marked as generated, so we should retreive the column schema from
          // data table
          const ObColumnSchemaV2* column_schema = data_table_schema_->get_column_schema(column_ids.at(i).col_id_);
          if (NULL == column_schema) {
            column_schema = index_schema_->get_column_schema(column_ids.at(i).col_id_);
          }
          if (OB_ISNULL(column_schema)) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "error unexpected, column schema must not be NULL", K(ret), K(column_ids.at(i).col_id_));
          } else if (column_schema->is_shadow_column() || column_schema->is_generated_column() ||
                     !column_schema->is_column_stored_in_sstable()) {
            STORAGE_LOG(INFO, "column do not need to compare checksum", K(column_ids.at(i).col_id_));
          } else {
            ObIndexChecksumItem item;
            item.execution_id_ = execution_id_;
            item.tenant_id_ = extract_tenant_id(pkey_.get_table_id());
            item.table_id_ = pkey_.get_table_id();
            item.partition_id_ = pkey_.get_partition_id();
            item.column_id_ = column_ids.at(i).col_id_;
            item.task_id_ = -1;
            item.checksum_ = column_checksum.at(i);
            item.checksum_method_ = blocksstable::CCM_TYPE_AND_VALUE;
            if (OB_FAIL(checksum_items.push_back(item))) {
              LOG_WARN("fail to push back item", K(ret));
            }
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(ObIndexChecksumOperator::update_checksum(checksum_items, *GCTX.sql_proxy_))) {
            LOG_WARN("fail to update checksum", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObUniqueIndexChecker::try_get_read_tables(
    const uint64_t table_id, const int64_t snapshot_version, ObIDag* dag, ObTablesHandle& tables_handle)
{
  int ret = OB_SUCCESS;
  tables_handle.reset();
  if (OB_INVALID_ID == table_id || snapshot_version <= 0 || NULL == dag) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(table_id), K(snapshot_version), KP(dag));
  } else {
    ObIPartitionGroupGuard part_guard;
    ObPartitionStorage* storage = NULL;
    const bool allow_not_ready = false;
    const bool need_safety_check = true;
    const bool reset_handle = true;
    const bool print_dropped_alert = false;
    while (OB_SUCC(ret) && !dag->has_set_stop()) {
      if (OB_FAIL(part_service_->get_partition(pkey_, part_guard))) {
        if (OB_PARTITION_NOT_EXIST == ret) {
          STORAGE_LOG(INFO, "partition not exist, may be deleted", K(pkey_));
          ret = OB_SUCCESS;
          break;
        } else {
          STORAGE_LOG(WARN, "fail to get partition", K(ret), K_(pkey));
        }
      } else if (OB_ISNULL(part_guard_.get_pg_partition())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "The partition is NULL", K(ret), K_(pkey));
      } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(part_guard_.get_pg_partition()->get_storage()))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "error unexpected, storage must not be NULL", K(ret), K(pkey_));
      } else if (OB_FAIL(storage->get_partition_store().get_read_tables(table_id,
                     snapshot_version,
                     tables_handle,
                     allow_not_ready,
                     need_safety_check,
                     reset_handle,
                     print_dropped_alert))) {
        if (OB_REPLICA_NOT_READABLE == ret) {
          usleep(RETRY_INTERVAL);
          dag_yield();
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get read tables", K(ret));
        }
      } else {
        break;
      }
    }
  }
  return ret;
}

int ObUniqueIndexChecker::check_local_index(ObIDag* dag, ObPartitionStorage* storage, const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(storage) || snapshot_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(storage), K(snapshot_version));
  } else {
    // scan data table
    ObTablesHandle data_table_handle;
    ObArray<int64_t> data_table_column_checksum;
    ObTablesHandle index_table_handle;
    ObArray<int64_t> index_table_column_checksum;
    int64_t index_table_row_count = 0;
    int64_t main_table_row_count = 0;

    if (OB_FAIL(try_get_read_tables(data_table_schema_->get_table_id(), snapshot_version, dag, data_table_handle))) {
      LOG_WARN("fail to try get read tables", K(ret));
    }

    // read data table data and calc column checksum
    if (OB_SUCC(ret) && !dag->has_set_stop() && data_table_handle.get_count() > 0) {
      if (OB_FAIL(scan_main_table_with_column_checksum(*data_table_schema_,
              *index_schema_,
              snapshot_version,
              data_table_handle,
              data_table_column_checksum,
              main_table_row_count))) {
        STORAGE_LOG(WARN, "fail to scan main table with column checksum", K(ret));
      } else {
        data_table_handle.reset();
      }
    }

    // scan index table
    if (OB_SUCC(ret)) {
      if (OB_FAIL(try_get_read_tables(index_schema_->get_table_id(), snapshot_version, dag, index_table_handle))) {
        LOG_WARN("fail to try get read tables", K(ret));
      }

      // read index table data and calc column checksum
      if (OB_SUCC(ret) && !dag->has_set_stop() && index_table_handle.get_count() > 0) {
        if (OB_FAIL(scan_index_table_with_column_checksum(*data_table_schema_,
                *index_schema_,
                snapshot_version,
                index_table_handle,
                index_table_column_checksum,
                index_table_row_count))) {
          STORAGE_LOG(WARN, "fail to scan index table with column checksum", K(ret));
        } else {
          index_table_handle.reset();
        }
      }
    }

    // validate column checksum
    if (OB_SUCC(ret) && !dag->has_set_stop() && data_table_column_checksum.count() > 0 &&
        index_table_column_checksum.count() > 0) {
      ObArray<ObColDesc> column_ids;
      if (OB_FAIL(index_schema_->get_column_ids(column_ids))) {
        STORAGE_LOG(WARN, "fail to get columns ids", K(ret));
      } else if (main_table_row_count != index_table_row_count) {
        ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
        STORAGE_LOG(WARN,
            "check unique index failed",
            K(ret),
            K(main_table_row_count),
            K(index_table_row_count),
            "index_id",
            index_schema_->get_table_id());
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
          // generated column in index schema is not marked as generated, so we should retreive the column schema from
          // data table
          const ObColumnSchemaV2* column_schema = data_table_schema_->get_column_schema(column_ids.at(i).col_id_);
          if (NULL == column_schema) {
            column_schema = index_schema_->get_column_schema(column_ids.at(i).col_id_);
          }
          if (OB_ISNULL(column_schema)) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "error unexpected, column schema must not be NULL", K(ret));
          } else if (column_schema->is_shadow_column() || column_schema->is_generated_column() ||
                     !column_schema->is_column_stored_in_sstable()) {
            STORAGE_LOG(INFO, "column do not need to compare checksum", K(column_ids.at(i).col_id_));
            continue;
          } else if (data_table_column_checksum.at(i) != index_table_column_checksum.at(i)) {
            ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
            STORAGE_LOG(WARN,
                "check unique index failed",
                K(ret),
                K(data_table_column_checksum),
                K(index_table_column_checksum),
                K(column_ids),
                K(i),
                "index_id",
                index_schema_->get_table_id());
          }
        }
        if (OB_SUCC(ret)) {
          STORAGE_LOG(INFO,
              "build index checksum",
              K(data_table_column_checksum),
              K(index_table_column_checksum),
              K(column_ids),
              "index_id",
              index_schema_->get_table_id());
        }
      }
    }
  }
  return ret;
}

int ObUniqueIndexChecker::check_unique_index(ObIDag* dag)
{
  int ret = OB_SUCCESS;
  ObPartitionStorage* storage = NULL;
  ObTablesHandle table_handle;
  ObIPartitionGroupGuard pg_guard;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObUniqueIndexChecker has not been inited", K(ret));
  } else if (OB_ISNULL(dag)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(dag));
  } else if (OB_FAIL(part_service_->get_partition(pkey_, pg_guard)) || OB_ISNULL(pg_guard.get_partition_group())) {
    STORAGE_LOG(WARN, "fail to get partition", K(ret), K_(pkey));
  } else if (OB_FAIL(pg_guard.get_partition_group()->get_pg_partition(pkey_, part_guard_)) ||
             OB_ISNULL(part_guard_.get_pg_partition())) {
    STORAGE_LOG(WARN, "fail to get pg partition", K(ret), K_(pkey));
  } else if (OB_ISNULL(storage = static_cast<ObPartitionStorage*>(part_guard_.get_pg_partition()->get_storage()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, partition storage must not be NULL", K(ret));
  } else if (index_schema_->is_domain_index()) {
    STORAGE_LOG(INFO, "do not need to check unique for domain index", "index_id", index_schema_->get_table_id());
  } else {
    int64_t check_end_snapshot_version = 0;

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(wait_trans_end(dag, check_end_snapshot_version))) {
      LOG_WARN("fail to wait trans end", K(ret));
    } else {
      if (index_schema_->is_global_index_table()) {
        if (OB_FAIL(check_global_index(dag, storage))) {
          LOG_WARN("fail to check global index", K(ret));
        }
      } else {
        if (OB_FAIL(check_local_index(dag, storage, check_end_snapshot_version))) {
          STORAGE_LOG(WARN, "fail to check local index", K(ret), K(check_end_snapshot_version));
        }
      }
    }
  }
  if (is_inited_) {
    int tmp_ret = OB_SUCCESS;
    ObIndexStatusTableOperator::ObBuildIndexStatus status;
    const ObAddr& self_addr = GCTX.self_addr_;
    const uint64_t index_table_id = index_schema_->get_table_id();
    status.index_status_ = index_schema_->get_index_status();
    status.ret_code_ = ret;
    status.role_ = common::FOLLOWER;
    STORAGE_LOG(INFO, "begin report build index status", K(index_table_id), K(pkey_), K(status));
    while (!dag->has_set_stop()) {
      if (OB_SUCCESS != (tmp_ret = ObIndexStatusTableOperator::report_build_index_status(
                             index_table_id, pkey_.get_partition_id(), self_addr, status, *GCTX.sql_proxy_))) {
        STORAGE_LOG(WARN, "fail to report build index status", K(ret), K(pkey_));
        usleep(RETRY_INTERVAL);
        dag_yield();
      } else {
        break;
      }
    }
  }
  return ret;
}

int ObUniqueIndexChecker::wait_trans_end(ObIDag* dag, int64_t& snapshot_version)
{
  int ret = OB_SUCCESS;
  snapshot_version = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObUniqueIndexChecker has not been inited", K(ret));
  } else {
    const int64_t now = ObTimeUtility::current_time();
    while (OB_SUCC(ret) && !dag->has_set_stop() && 0 == snapshot_version) {
      if (OB_FAIL(part_service_->check_ctx_create_timestamp_elapsed(pkey_, now))) {
        if (OB_EAGAIN == ret) {
          ret = OB_SUCCESS;
          usleep(RETRY_INTERVAL);
          dag_yield();
        } else {
          LOG_WARN("fail to check ctx create timestamp elapsed", K(ret));
          break;
        }
      } else if (OB_FAIL(OB_TS_MGR.get_publish_version(pkey_.get_tenant_id(), snapshot_version))) {
        LOG_WARN("fail to get publish version", K(ret), K(pkey_));
      }
    }
  }
  return ret;
}

ObUniqueCheckingTask::ObUniqueCheckingTask()
    : ObITask(TASK_TYPE_UNIQUE_INDEX_CHECKING), is_inited_(false), param_(), context_(NULL)
{}

ObUniqueCheckingTask::~ObUniqueCheckingTask()
{}

int ObUniqueCheckingTask::init(const ObBuildIndexParam& param, ObBuildIndexContext* context)
{
  int ret = OB_SUCCESS;
  ObIDag* tmp_dag = get_dag();
  ObBuildIndexDag* dag = NULL;
  ObPartitionService* part_service = NULL;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObUniqueCheckingTask has already been inited", K(ret));
  } else if (!param.is_valid() || OB_ISNULL(context)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(param), KP(context));
  } else if (OB_ISNULL(dag = static_cast<ObBuildIndexDag*>(tmp_dag))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to cast pointer", K(ret));
  } else if (OB_FAIL(dag->get_partition_service(part_service))) {
    STORAGE_LOG(WARN, "fail to get partition service", K(ret));
  } else {
    const ObPartitionKey pkey = dag->get_partition_key();
    if (OB_FAIL(unique_checker_.init(part_service, pkey, param.table_schema_, param.index_schema_))) {
      STORAGE_LOG(WARN, "fail to init unique index checker", K(ret));
    } else {
      param_ = &param;
      context_ = context;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObUniqueCheckingTask::process()
{
  int ret = OB_SUCCESS;
  ObIDag* tmp_dag = get_dag();
  ObBuildIndexDag* dag = NULL;
  bool need_build = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObUniqueCheckingTask has not been inited", K(ret));
  } else if (!context_->need_build_) {
    STORAGE_LOG(INFO, "index does not need build", K(*param_));
  } else if (OB_ISNULL(dag = static_cast<ObBuildIndexDag*>(tmp_dag))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to cast pointer", K(ret));
  } else if (OB_FAIL(dag->check_index_need_build(need_build))) {
    STORAGE_LOG(WARN, "fail to check index need build", K(ret), K(*param_));
  } else if (!need_build) {
    STORAGE_LOG(INFO, "index do not need build", "index_id", param_->index_schema_->get_table_id());
  } else if (OB_SUCCESS != (context_->build_index_ret_)) {
    STORAGE_LOG(WARN, "Build Index has already failed", "ret", context_->build_index_ret_);
  } else if (context_->is_unique_checking_complete_) {
    // do nothing
  } else if (!param_->index_schema_->is_unique_index()) {
    context_->is_unique_checking_complete_ = true;
  } else if (OB_FAIL(unique_checker_.check_unique_index(dag))) {
    STORAGE_LOG(WARN, "fail to check unique index", K(ret));
  }
  context_->is_unique_checking_complete_ = true;
  if (OB_FAIL(ret) && NULL != context_) {
    context_->build_index_ret_ = ret;
    ret = OB_SUCCESS;
  }
  return ret;
}

ObReportIndexStatusTask::ObReportIndexStatusTask()
    : ObITask(TASK_TYPE_REPORT_INDEX_STATUS), is_inited_(false), pkey_(), param_(), context_(NULL)
{}

ObReportIndexStatusTask::~ObReportIndexStatusTask()
{}

int ObReportIndexStatusTask::init(
    const ObPartitionKey& pkey, const ObBuildIndexParam& param, ObBuildIndexContext* context)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObReportIndexStatusTask has already been inited", K(ret));
  } else if (!pkey.is_valid() || !param.is_valid() || OB_ISNULL(context)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(pkey), K(param), KP(context));
  } else {
    pkey_ = pkey;
    param_ = &param;
    context_ = context;
    is_inited_ = true;
  }
  return ret;
}

int ObReportIndexStatusTask::process()
{
  int ret = OB_SUCCESS;
  ObIDag* tmp_dag = get_dag();
  ObBuildIndexDag* dag = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObReportIndexStatusTask has not been inited", K(ret));
  } else if (OB_ISNULL(dag = static_cast<ObBuildIndexDag*>(tmp_dag))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to cast pointer", K(ret));
  } else {
    bool need_retry = true;
    bool need_build = false;
    const int64_t RETRY_INTERVAL = 5000 * 1000;  // 5s
    while (need_retry) {
      if (OB_FAIL(dag->check_index_need_build(need_build))) {
        STORAGE_LOG(WARN, "fail to check index need rebuild", K(ret));
      } else if (!need_build) {
        STORAGE_LOG(INFO, "index does not need build", K(*param_));
        need_retry = false;
      } else if (OB_FAIL(report_index_status(pkey_, *param_, context_, need_retry))) {
        STORAGE_LOG(
            WARN, "fail to report index status", K(ret), K(pkey_), "index_id", param_->index_schema_->get_table_id());
      }
      if (need_retry) {
        if (dag_->has_set_stop()) {
          STORAGE_LOG(INFO,
              "dag is stopped, cannot retry now",
              K(ret),
              K(pkey_),
              "index_id",
              param_->index_schema_->get_table_id());
          break;
        }
        usleep(RETRY_INTERVAL);
        dag_yield();
      }
    }
  }
  if (OB_FAIL(ret) && NULL != context_) {
    context_->build_index_ret_ = OB_SUCCESS;
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObReportIndexStatusTask::report_index_status(
    const ObPartitionKey& pkey, const ObBuildIndexParam& param, ObBuildIndexContext* context, bool& need_retry)
{
  int ret = OB_SUCCESS;
  need_retry = true;
  if (!pkey.is_valid() || !param.is_valid() || OB_ISNULL(context)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(pkey), K(param), KP(context));
  } else if (context->is_report_succ_ || !context->need_build_) {
    // do nothing
    need_retry = false;
  } else {
    ObIndexStatusTableOperator::ObBuildIndexStatus status;
    const ObAddr& self_addr = GCTX.self_addr_;
    const uint64_t index_table_id = param.index_schema_->get_table_id();
    status.index_status_ = param.index_schema_->get_index_status();
    status.ret_code_ = context->build_index_ret_;
    status.role_ = common::FOLLOWER;
    if (OB_FAIL(ObIndexStatusTableOperator::report_build_index_status(
            index_table_id, pkey.get_partition_id(), self_addr, status, *GCTX.sql_proxy_))) {
      STORAGE_LOG(WARN, "fail to report build index status", K(ret), K(pkey));
    } else {
      context->is_report_succ_ = true;
      need_retry = false;
    }
    STORAGE_LOG(INFO,
        "process index status report task",
        K(ret),
        K(pkey),
        "build_index_ret_code",
        status.ret_code_,
        "index_id",
        param.index_schema_->get_table_id());
  }
  return ret;
}

ObUniqueCheckingDag::ObUniqueCheckingDag()
    : ObIDag(ObIDag::DAG_TYPE_UNIQUE_CHECKING, ObIDag::DAG_PRIO_CREATE_INDEX),
      is_inited_(false),
      part_service_(NULL),
      pkey_(),
      schema_service_(NULL),
      schema_guard_(),
      index_schema_(NULL),
      data_table_schema_(NULL),
      callback_(NULL),
      execution_id_(0),
      snapshot_version_(0),
      compat_mode_(ObWorker::CompatMode::INVALID)
{}

ObUniqueCheckingDag::~ObUniqueCheckingDag()
{
  if (NULL != callback_) {
    ob_free(callback_);
    callback_ = NULL;
  }
}

int ObUniqueCheckingDag::init(const ObPartitionKey& pkey, ObPartitionService* part_service,
    ObMultiVersionSchemaService* schema_service, const uint64_t index_id, const int64_t schema_version,
    const uint64_t execution_id, const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  ObIPartitionGroupGuard pg_guard;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObUniqueCheckingDag has already been inited", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid() || NULL == part_service || NULL == schema_service ||
                         OB_INVALID_ID == index_id || schema_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN,
        "invalid arguments",
        K(ret),
        K(pkey),
        KP(part_service),
        KP(schema_service),
        K(index_id),
        K(schema_version));
  } else if (OB_FAIL(part_service->get_partition(pkey, pg_guard)) || OB_ISNULL(pg_guard.get_partition_group())) {
    STORAGE_LOG(WARN, "fail to get partition", K(ret), K(pkey));
  } else if (OB_FAIL(pg_guard.get_partition_group()->get_pg_partition(pkey, part_guard_)) ||
             OB_ISNULL(part_guard_.get_pg_partition())) {
    STORAGE_LOG(WARN, "fail to get pg partition", K(ret), K(pkey));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(
                 extract_tenant_id(pkey.get_table_id()), schema_guard_, schema_version))) {
    STORAGE_LOG(WARN, "fail to get schema guard", K(ret), K(schema_version));
  } else if (OB_FAIL(schema_guard_.check_formal_guard())) {
    LOG_WARN("schema_guard is not formal", K(ret), K(pkey));
  } else if (OB_FAIL(schema_guard_.get_table_schema(index_id, index_schema_))) {
    STORAGE_LOG(WARN, "fail to get table schema", K(ret));
  } else if (OB_ISNULL(index_schema_)) {
    ret = OB_TABLE_NOT_EXIST;
    STORAGE_LOG(WARN, "fail to get table schema", K(ret), K(index_id));
  } else if (OB_FAIL(schema_guard_.get_table_schema(index_schema_->get_data_table_id(), data_table_schema_))) {
    STORAGE_LOG(WARN, "fail to get table schema", K(ret));
  } else if (OB_ISNULL(data_table_schema_)) {
    ret = OB_TABLE_NOT_EXIST;
    STORAGE_LOG(WARN, "data table not exist", K(ret));
  } else if (OB_FAIL(get_compat_mode_with_table_id(pkey.table_id_, compat_mode_))) {
    STORAGE_LOG(WARN, "failed to get compat mode", K(ret), K(pkey));
  } else {
    is_inited_ = true;
    pkey_ = pkey;
    part_service_ = part_service;
    schema_service_ = schema_service;
    execution_id_ = execution_id;
    snapshot_version_ = snapshot_version;
  }
  return ret;
}

int ObUniqueCheckingDag::alloc_unique_checking_prepare_task(ObIUniqueCheckingCompleteCallback* callback)
{
  int ret = OB_SUCCESS;
  ObUniqueCheckingPrepareTask* prepare_task = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObUniqueCheckingDag has not been inited", K(ret));
  } else if (OB_FAIL(alloc_task(prepare_task))) {
    STORAGE_LOG(WARN, "fail to alloc task", K(ret));
  } else if (OB_FAIL(prepare_task->init(callback))) {
    STORAGE_LOG(WARN, "fail to init prepare task", K(ret));
  } else if (OB_FAIL(add_task(*prepare_task))) {
    STORAGE_LOG(WARN, "fail to add task", K(ret));
  }
  return ret;
}

int ObUniqueCheckingDag::alloc_local_index_task_callback(ObLocalUniqueIndexCallback*& callback)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  callback = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObUniqueCheckingDag has not been inited", K(ret));
  } else if (OB_ISNULL(buf = ob_malloc(sizeof(ObLocalUniqueIndexCallback), ObModIds::OB_CS_BUILD_INDEX))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to allocate memory", K(ret));
  } else if (OB_ISNULL(callback = new (buf) ObLocalUniqueIndexCallback())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to placement new local index callback", K(ret));
  } else {
    callback_ = callback;
  }

  return ret;
}

int ObUniqueCheckingDag::alloc_global_index_task_callback(
    const ObPartitionKey& pkey, const uint64_t index_id, ObGlobalUniqueIndexCallback*& callback)
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  callback = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObUniqueCheckingDag has not been inited", K(ret));
  } else if (OB_UNLIKELY(!pkey.is_valid() || OB_INVALID_ID == index_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(pkey), K(index_id));
  } else if (OB_ISNULL(buf = ob_malloc(sizeof(ObGlobalUniqueIndexCallback), ObModIds::OB_CS_BUILD_INDEX))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to allocate memory", K(ret));
  } else if (OB_ISNULL(callback = new (buf) ObGlobalUniqueIndexCallback(pkey, index_id))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "fail to placement new local index callback", K(ret));
  } else {
    callback_ = callback;
  }

  return ret;
}

int64_t ObUniqueCheckingDag::hash() const
{
  int tmp_ret = OB_SUCCESS;
  int64_t hash_val = 0;
  if (NULL == index_schema_) {
    tmp_ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "index schema must not be NULL", K(tmp_ret));
  } else {
    hash_val = pkey_.hash() + index_schema_->get_table_id();
  }
  return hash_val;
}

int ObUniqueCheckingDag::fill_comment(char* buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t index_id = 0;
  if (NULL != index_schema_) {
    index_id = index_schema_->get_table_id();
  }

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(databuff_printf(
                 buf, buf_len, "unique check task: key=%s index_id=%ld", to_cstring(pkey_), index_id))) {
    STORAGE_LOG(WARN, "failed to fill comment", K(ret), K(pkey_), K(index_id));
  }
  return ret;
}

bool ObUniqueCheckingDag::operator==(const ObIDag& other) const
{
  int tmp_ret = OB_SUCCESS;
  bool is_equal = false;
  if (OB_UNLIKELY(this == &other)) {
    is_equal = true;
  } else if (get_type() == other.get_type()) {
    const ObUniqueCheckingDag& dag = static_cast<const ObUniqueCheckingDag&>(other);
    if (NULL == index_schema_ || NULL == dag.index_schema_) {
      tmp_ret = OB_ERR_SYS;
      STORAGE_LOG(ERROR, "index schema must not be NULL", K(tmp_ret), KP(index_schema_), KP(dag.index_schema_));
    } else {
      is_equal = pkey_ == dag.pkey_ && index_schema_->get_table_id() == dag.index_schema_->get_table_id();
    }
  }
  return is_equal;
}

ObUniqueCheckingPrepareTask::ObUniqueCheckingPrepareTask()
    : ObITask(TASK_TYPE_UNIQUE_CHECKING_PREPARE),
      is_inited_(false),
      index_schema_(NULL),
      data_table_schema_(NULL),
      callback_(NULL)
{}

int ObUniqueCheckingPrepareTask::init(ObIUniqueCheckingCompleteCallback* callback)
{
  int ret = OB_SUCCESS;
  ObUniqueCheckingDag* dag = NULL;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObUniqueCheckingPrepareTask has already been inited", K(ret));
  } else if (OB_ISNULL(dag = static_cast<ObUniqueCheckingDag*>(get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, dag must not be NULL", K(ret));
  } else if (OB_ISNULL(index_schema_ = dag->get_index_schema())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(index_schema_));
  } else if (OB_ISNULL(data_table_schema_ = dag->get_data_table_schema())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid data table schema", K(ret));
  } else {
    callback_ = callback;
    is_inited_ = true;
  }
  return ret;
}

int ObUniqueCheckingPrepareTask::process()
{
  int ret = OB_SUCCESS;
  ObUniqueCheckingDag* dag = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObUniqueCheckingPrepareTask has not been inited", K(ret));
  } else if (OB_ISNULL(dag = static_cast<ObUniqueCheckingDag*>(get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, dag must not be NULL", K(ret));
  } else if (OB_FAIL(generate_unique_checking_task(dag))) {
    STORAGE_LOG(WARN, "fail to generate unique checking task", K(ret));
  }
  return ret;
}

int ObUniqueCheckingPrepareTask::generate_unique_checking_task(ObUniqueCheckingDag* dag)
{
  int ret = OB_SUCCESS;
  ObSimpleUniqueCheckingTask* checking_task = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObIndexPrepareTask has not been inited", K(ret));
  } else if (OB_ISNULL(dag)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(dag));
  } else if (OB_FAIL(dag->alloc_task(checking_task))) {
    STORAGE_LOG(WARN, "fail to alloc checking task", K(ret));
  } else if (OB_FAIL(checking_task->init(data_table_schema_, index_schema_, callback_))) {
    STORAGE_LOG(WARN, "fail to init unique checking task", K(ret));
  } else if (OB_FAIL(add_child(*checking_task))) {
    STORAGE_LOG(WARN, "fail to add child for prepare task", K(ret));
  } else if (OB_FAIL(dag->add_task(*checking_task))) {
    STORAGE_LOG(WARN, "fail to add unique checking task", K(ret));
  }
  return ret;
}

ObSimpleUniqueCheckingTask::ObSimpleUniqueCheckingTask()
    : ObITask(TASK_TYPE_SIMPLE_UNIQUE_CHECKING),
      is_inited_(false),
      unique_checker_(),
      index_schema_(NULL),
      data_table_schema_(NULL),
      pkey_(),
      callback_(NULL)
{}

int ObSimpleUniqueCheckingTask::init(const ObTableSchema* data_table_schema, const ObTableSchema* index_schema,
    ObIUniqueCheckingCompleteCallback* callback)
{
  int ret = OB_SUCCESS;
  ObUniqueCheckingDag* dag = NULL;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObSimpleUniqueCheckingTask has already been inited", K(ret));
  } else if (OB_ISNULL(data_table_schema) || OB_ISNULL(index_schema) || OB_ISNULL(callback)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(data_table_schema), KP(index_schema), KP(callback));
  } else if (OB_ISNULL(dag = static_cast<ObUniqueCheckingDag*>(get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, dag must not be NULL", K(ret));
  } else {
    ObPartitionService* part_service = dag->get_partition_service();
    pkey_ = dag->get_partition_key();
    if (OB_UNLIKELY(NULL == part_service || !pkey_.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(part_service), K(pkey_));
    } else if (OB_FAIL(unique_checker_.init(part_service,
                   pkey_,
                   data_table_schema,
                   index_schema,
                   dag->get_execution_id(),
                   dag->get_snapshot_version()))) {
      STORAGE_LOG(WARN, "fail to init unique index checker", K(ret));
    } else {
      index_schema_ = index_schema;
      data_table_schema_ = data_table_schema;
      callback_ = callback;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObSimpleUniqueCheckingTask::process()
{
  int ret = OB_SUCCESS;
  int ret_code = OB_SUCCESS;
  ObUniqueCheckingDag* dag = NULL;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObSimpleUniqueCheckingTask has not been inited", K(ret));
  } else if (OB_ISNULL(dag = static_cast<ObUniqueCheckingDag*>(get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, dag must not be NULL", K(ret));
  } else if (OB_FAIL(unique_checker_.check_unique_index(dag))) {
    STORAGE_LOG(WARN, "fail to check unique index", K(ret));
  }
  ret_code = ret;
  // overwrite ret
  if (NULL != callback_) {
    if (NULL != index_schema_) {
      STORAGE_LOG(INFO, "unique checking callback", K(pkey_), "index_id", index_schema_->get_table_id());
    }
    if (OB_FAIL(callback_->operator()(ret_code))) {
      STORAGE_LOG(WARN, "fail to check unique index response", K(ret));
    }
  }
  return ret;
}

ObGlobalUniqueIndexCallback::ObGlobalUniqueIndexCallback(const common::ObPartitionKey& pkey, const uint64_t index_id)
    : pkey_(pkey), index_id_(index_id)
{}

int ObGlobalUniqueIndexCallback::operator()(const int ret_code)
{
  int ret = OB_SUCCESS;
  obrpc::ObCalcColumnChecksumResponseArg arg;
  ObAddr rs_addr;
  arg.pkey_ = pkey_;
  arg.index_id_ = index_id_;
  arg.ret_code_ = ret_code;
  if (OB_ISNULL(GCTX.rs_rpc_proxy_) || OB_ISNULL(GCTX.rs_mgr_)) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(WARN, "innner system error, rootserver rpc proxy or rs mgr must not be NULL", K(ret), K(GCTX));
  } else if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(rs_addr))) {
    STORAGE_LOG(WARN, "fail to get rootservice address", K(ret));
  } else if (OB_FAIL(GCTX.rs_rpc_proxy_->to(rs_addr).calc_column_checksum_response(arg))) {
    STORAGE_LOG(WARN, "fail to check unique index response", K(ret), K(arg));
  }
  return ret;
}

ObLocalUniqueIndexCallback::ObLocalUniqueIndexCallback()
{}

int ObLocalUniqueIndexCallback::operator()(const int ret_code)
{
  int ret = OB_SUCCESS;
  UNUSED(ret_code);
  return ret;
}
