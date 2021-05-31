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

#include "ob_partition_split_worker.h"
#include "lib/utility/utility.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_partition_scheduler.h"

using namespace oceanbase::obsys;

namespace oceanbase {

using namespace common;
using namespace share;

namespace storage {
int ObPartitionSplitTask::init(const int64_t schema_version, const ObSplitPartitionPair& partition_pair)
{
  int ret = OB_SUCCESS;
  if (!partition_pair.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(schema_version), K(partition_pair));
  } else if (OB_FAIL(partition_pair_.assign(partition_pair))) {
    STORAGE_LOG(WARN, "assign partition pair failed", K(ret), K(partition_pair));
  } else {
    schema_version_ = schema_version;
  }
  return ret;
}

bool ObPartitionSplitTask::is_valid() const
{
  return partition_pair_.is_valid();
}

void ObPartitionSplitTask::reset()
{
  schema_version_ = 0;
  partition_pair_.reset();
  next_run_ts_ = 0;
}

ObPartitionSplitTask* ObPartitionSplitTaskFactory::alloc()
{
  return rp_alloc(ObPartitionSplitTask, ObModIds::OB_PARTITION_SPLIT_TASK);
}

void ObPartitionSplitTaskFactory::release(ObPartitionSplitTask* task)
{
  if (NULL != task) {
    rp_free(task, ObModIds::OB_PARTITION_SPLIT_TASK);
  }
}

int ObPartitionSplitWorker::init(ObPartitionService* partition_service)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "partition split worker inited twice", K(ret));
  } else if (NULL == partition_service) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(partition_service));
  } else if (OB_FAIL(queue_.init(1000000))) {
    STORAGE_LOG(WARN, "queue init failed", K(ret));
  } else {
    partition_service_ = partition_service;
    is_inited_ = true;
    STORAGE_LOG(INFO, "partition split worker inited success", KP(this));
  }
  return ret;
}

int ObPartitionSplitWorker::start()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition split worker is not inited", K(ret));
  } else if (OB_FAIL(ObThreadPool::start())) {
    STORAGE_LOG(WARN, "thread start failed", K(ret));
  } else {
    STORAGE_LOG(INFO, "partition split worker start success");
  }
  return ret;
}

void ObPartitionSplitWorker::stop()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition split worker is not inited", K(ret));
  } else {
    ObThreadPool::stop();
    STORAGE_LOG(INFO, "partition split worker stop success");
  }
}

void ObPartitionSplitWorker::wait()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "partition split worker is not inited", K(ret));
  } else {
    ObThreadPool::wait();
    STORAGE_LOG(INFO, "partition split worker wait success");
  }
}

void ObPartitionSplitWorker::destroy()
{
  if (is_inited_) {
    stop();
    wait();
    is_inited_ = false;
    STORAGE_LOG(INFO, "partition split worker destroyed");
  }
}

void ObPartitionSplitWorker::run1()
{
  (void)prctl(PR_SET_NAME, "SplitWorker", 0, 0, 0);
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (NULL == partition_service_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "partition service is null", K(ret), KP(partition_service_));
  } else {
    while (!has_set_stop()) {
      ret = OB_SUCCESS;
      for (int64_t i = 0; !has_set_stop() && OB_SUCCESS == ret && i < queue_.size(); i++) {
        void* task = NULL;
        ObPartitionSplitTask* split_task = NULL;
        if (OB_FAIL(queue_.pop(task))) {
          STORAGE_LOG(WARN, "pop task failed", K(ret));
        } else if (NULL == task) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "task is null", K(ret), KP(task));
        } else {
          split_task = reinterpret_cast<ObPartitionSplitTask*>(task);
          if (!split_task->is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "invalid split task", K(ret), K(*split_task));
          } else {
            ret = handle_(split_task);
          }
        }
      }
      usleep(20000);
    }
  }
}

int ObPartitionSplitWorker::push(ObPartitionSplitTask* task)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (NULL == task) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(task));
  } else if (OB_FAIL(push_(task))) {
    STORAGE_LOG(WARN, "push task failed", K(ret), K(*task));
  } else {
    STORAGE_LOG(INFO, "push task success", K(*task));
  }
  return ret;
}

// TODO split by PG
int ObPartitionSplitWorker::handle_(ObPartitionSplitTask* task)
{
  int ret = OB_SUCCESS;
  if (task->need_run()) {
    enum ObSplitProgress partition_progress = UNKNOWN_SPLIT_PROGRESS;
    ObIPartitionGroup* partition = NULL;
    ObIPartitionGroupGuard partition_guard;
    const ObPartitionKey& pkey = task->get_partition_pair().get_source_pkey();
    if (OB_FAIL(partition_service_->get_partition(pkey, partition_guard))) {
      STORAGE_LOG(WARN, "get partition failed", K(ret), K(pkey));
    } else if (NULL == (partition = partition_guard.get_partition_group())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "partition is null", K(ret), K(pkey));
    } else if (OB_FAIL(partition->split_source_partition(
                   task->get_schema_version(), task->get_partition_pair(), partition_progress))) {
      if (OB_FAIL(push_(task))) {
        STORAGE_LOG(WARN, "push back task failed", K(ret), K(*task));
      }
    } else if (partition_progress >= LOGICAL_SPLIT_FINISH) {
      STORAGE_LOG(INFO, "logical split partition task finished", K(*task));
      bool is_merged = false;
      // trigger source partition to split quickly
      ObPartitionScheduler::get_instance().schedule_pg(MINI_MERGE, *partition, ObVersion::MIN_VERSION, is_merged);
      ObPartitionSplitTaskFactory::release(task);
      task = NULL;
    } else {
      if (OB_FAIL(push_(task))) {
        STORAGE_LOG(WARN, "push back task failed", K(ret), K(*task));
      }
    }
    if (NULL != task) {
      STORAGE_LOG(INFO, "split worker handle task", K(ret), K(*task));
    } else {
      STORAGE_LOG(INFO, "split worker handle task finish", K(ret), K(pkey));
    }
  }
  return ret;
}

int ObPartitionSplitWorker::push_(ObPartitionSplitTask* task)
{
  return queue_.push(task);
}

}  // namespace storage
}  // namespace oceanbase
