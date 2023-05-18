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

#include "storage/tx_storage/ob_ls_freeze_thread.h"
#include "storage/checkpoint/ob_data_checkpoint.h"
#include "share/ob_thread_mgr.h"
#include "lib/oblog/ob_log.h"
#include "lib/cpu/ob_cpu_topology.h"

namespace oceanbase
{
namespace storage
{

using namespace checkpoint;
using namespace share;

void ObLSFreezeTask::set_task(ObLSFreezeThread *host,
                              ObDataCheckpoint *data_checkpoint,
                              SCN rec_scn)
{
  host_ = host;
  rec_scn_ = rec_scn;
  data_checkpoint_ = data_checkpoint;
}

void ObLSFreezeTask::handle()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(data_checkpoint_)) {
    data_checkpoint_->road_to_flush(rec_scn_);
  }
  if (OB_NOT_NULL(host_)) {
    if (OB_FAIL(host_->push_back_(this))) {
      STORAGE_LOG(WARN, "push back ls free task failed", K(ret));
    }
  }
}

ObLSFreezeThread::ObLSFreezeThread()
    : inited_(false), tg_id_(-1), available_index_(-1), lock_(common::ObLatchIds::THREAD_POOL_LOCK)
{
  for (int64_t i = 0; i < MAX_FREE_TASK_NUM; i++) {
    task_array_[i] = NULL;
  }
}

ObLSFreezeThread::~ObLSFreezeThread()
{
  destroy();
}

void ObLSFreezeThread::destroy()
{
  if (inited_) {
    while (available_index_ >= 0) {
      task_array_[available_index_]->~ObLSFreezeTask();
      ob_free(task_array_[available_index_]);
      task_array_[available_index_] = NULL;
      available_index_--;
    }

    inited_ = false;
    tg_id_ = -1;
    STORAGE_LOG(INFO, "ls freeze thread destroy", KP(this));
  }
}

int ObLSFreezeThread::init(const int64_t tenant_id, int tg_id)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObLSFreezeThread has already been inited", K(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(tg_id, tg_id_))) {
    STORAGE_LOG(WARN, "ObSimpleThreadPool tg create", K(ret));
  } else if (OB_FAIL(TG_SET_HANDLER_AND_START(tg_id_, *this))) {
    STORAGE_LOG(WARN, "ObSimpleThreadPool inited error.", K(ret));
  } else {
    ObMemAttr memattr(tenant_id, "FreezeTask");
    for (int64_t i = 0; OB_SUCC(ret) && i < MAX_FREE_TASK_NUM; i++) {
      ObLSFreezeTask *ptr
        = (ObLSFreezeTask *)ob_malloc(sizeof(ObLSFreezeTask), memattr);
      if (NULL == ptr) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        new (ptr) ObLSFreezeTask();
        task_array_[i] = ptr;
        available_index_ = i;
        inited_ = true;
      }
    }
  }
  if (OB_SUCCESS != ret && !inited_) {
    destroy();
  }
  STORAGE_LOG(INFO, "ObLSFreezeThread init finished", K(ret));
  return ret;
}

int ObLSFreezeThread::add_task(ObDataCheckpoint *data_checkpoint,
                               SCN rec_scn)
{
  int ret = OB_SUCCESS;
  ObLSFreezeTask *task = NULL;
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
    task->set_task(this, data_checkpoint, rec_scn);
    if (OB_FAIL(TG_PUSH_TASK(tg_id_, task))) {
      STORAGE_LOG(WARN, "schedule timer task failed", K(ret));
    }
  }
  return ret;
}

void ObLSFreezeThread::handle(void *task)
{
  if (NULL == task) {
    STORAGE_LOG_RET(WARN, OB_ERR_UNEXPECTED, "task is null", KP(task));
  } else {
    ObLSFreezeTask *freeze_task = static_cast<ObLSFreezeTask *>(task);
    freeze_task->handle();
  }
}

int ObLSFreezeThread::push_back_(ObLSFreezeTask *task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(task));
  } else {
    ObSpinLockGuard guard(lock_);
    task_array_[++available_index_] = task;
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
