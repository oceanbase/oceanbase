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

#include "ob_partition_log_service.h"
#include "storage/ob_partition_service.h"
#include "ob_clog_config.h"

namespace oceanbase {
using namespace common;
namespace clog {

int ObClogAggreTask::set_partition_key(const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  if (!pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    pkey_ = pkey;
  }
  return ret;
}

int ObClogAggreTask::set_run_ts(const int64_t run_ts)
{
  int ret = OB_SUCCESS;
  if (0 >= run_ts) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    run_ts_ = run_ts;
  }
  return ret;
}

void ObClogAggreTask::handle()
{
  int ret = OB_SUCCESS;
  const int64_t cur_ts = ObTimeUtility::current_time();
  const int64_t sleep_ts = run_ts_ - cur_ts;
  if (sleep_ts > 0) {
    usleep(sleep_ts);
  }
  if (NULL != ps_) {
    if (OB_FAIL(ps_->try_freeze_aggre_buffer(pkey_))) {
      CLOG_LOG(WARN, "try freeze aggre buffer failed", K(ret), K_(pkey));
    }
  }
  if (NULL != host_) {
    if (OB_FAIL(host_->push_back(this))) {
      CLOG_LOG(WARN, "push back aggre task failed", K(ret), K_(pkey));
    }
  }
}

ObClogAggreRunnable::ObClogAggreRunnable() : is_inited_(false), available_index_(-1), tg_id_(-1)
{
  for (int64_t i = 0; i < TOTAL_TASK; i++) {
    task_array_[i] = NULL;
  }
}

ObClogAggreRunnable::~ObClogAggreRunnable()
{
  destroy();
}

int ObClogAggreRunnable::init(storage::ObPartitionService* partition_service)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObClogAggreRunnable has already been inited", K(ret));
  } else if (OB_ISNULL(partition_service)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), KP(partition_service));
  } else if (OB_FAIL(TG_CREATE(lib::TGDefIDs::ClogAggre, tg_id_))) {
    TRANS_LOG(WARN, "create tg failed", K(ret));
  } else {
    ObMemAttr memattr(OB_SERVER_TENANT_ID, "ObClogAggreTask");
    for (int64_t i = 0; OB_SUCC(ret) && i < TOTAL_TASK; i++) {
      ObClogAggreTask* ptr = (ObClogAggreTask*)ob_malloc(sizeof(ObClogAggreTask), memattr);
      if (NULL == ptr) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        new (ptr) ObClogAggreTask(this, partition_service);
        task_array_[i] = ptr;
        available_index_ = i;
      }
    }
  }
  if (OB_SUCC(ret)) {
    is_inited_ = true;
  } else {
    destroy();
  }
  CLOG_LOG(INFO, "ObClogAggreRunnable init finished", K(ret));
  return ret;
}

int ObClogAggreRunnable::start()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(TG_SET_HANDLER_AND_START(tg_id_, *this))) {
    TRANS_LOG(WARN, "thread pool init error", K(ret));
  } else {
    CLOG_LOG(INFO, "clog aggre runnable start success", KP(this));
  }
  return ret;
}

void ObClogAggreRunnable::stop()
{
  TG_STOP(tg_id_);
  CLOG_LOG(INFO, "clog aggre runnable stop success", KP(this));
}

void ObClogAggreRunnable::wait()
{
  CLOG_LOG(INFO, "clog aggre runnable wait success", KP(this));
}

void ObClogAggreRunnable::destroy()
{
  if (is_inited_) {
    stop();
    wait();
    while (available_index_ >= 0) {
      task_array_[available_index_]->~ObClogAggreTask();
      ob_free(task_array_[available_index_]);
      task_array_[available_index_] = NULL;
      available_index_--;
    }
    tg_id_ = -1;
    is_inited_ = false;
    CLOG_LOG(INFO, "clog aggre runnable destroy", KP(this));
  }
}

int ObClogAggreRunnable::add_task(const ObPartitionKey& pkey, const int64_t delay_us)
{
  int ret = OB_SUCCESS;
  if (!pkey.is_valid() || 0 >= delay_us) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(pkey), K(delay_us));
  } else {
    ObClogAggreTask* task = NULL;
    {
      ObSpinLockGuard guard(lock_);
      if (available_index_ >= 0) {
        task = task_array_[available_index_];
        task_array_[available_index_] = NULL;
        available_index_--;
      } else {
        ret = OB_EAGAIN;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(task->set_partition_key(pkey))) {
        CLOG_LOG(WARN, "set partition key failed", K(ret), K(pkey));
      } else if (OB_FAIL(task->set_run_ts(ObTimeUtility::current_time() + delay_us))) {
        CLOG_LOG(WARN, "set run ts failed", K(delay_us));
      } else if (OB_FAIL(TG_PUSH_TASK(tg_id_, task))) {
        CLOG_LOG(WARN, "schedule timer task failed", K(ret), K(pkey), K(delay_us));
      }
    }
  }
  return ret;
}

void ObClogAggreRunnable::handle(void* task)
{
  if (NULL == task) {
    TRANS_LOG(ERROR, "task is null", KP(task));
  } else {
    ObClogAggreTask* aggre_task = static_cast<ObClogAggreTask*>(task);
    aggre_task->handle();
  }
}

int ObClogAggreRunnable::push_back(ObClogAggreTask* task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), KP(task));
  } else {
    ObSpinLockGuard guard(lock_);
    task_array_[++available_index_] = task;
  }
  return ret;
}

}  // namespace clog
}  // namespace oceanbase
