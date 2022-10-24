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

#define USING_LOG_PREFIX SERVER_OMT
#include "ob_worker_pool.h"

#include "share/ob_define.h"
#include "ob_th_worker.h"
#include "share/rc/ob_tenant_base.h"

using namespace oceanbase::common;
using namespace oceanbase::omt;
using namespace oceanbase::share;

ObWorkerPool::ObWorkerPool()
    : is_inited_(false),
      init_cnt_(0),
      idle_cnt_(0),
      worker_cnt_(0)
{
}

ObWorkerPool::~ObWorkerPool()
{
  destroy();
}

int ObWorkerPool::Queue::push(ObThWorker *worker)
{
  int ret = OB_SUCCESS;
  if (NULL == worker) {
    LOG_ERROR("invalid argument", K(worker));
  } else if (OB_FAIL(queue_.push(&worker->wpool_link_))) {
    LOG_ERROR("failed push queue", K(ret));
  }
  return ret;
}

int ObWorkerPool::Queue::pop(ObThWorker *&worker)
{
  int ret = OB_SUCCESS;
  ObLink *link = nullptr;
  if (OB_FAIL(queue_.pop(link))) {
    if (OB_EAGAIN != ret) {
      LOG_ERROR("failed pop queue", K(ret));
    } else {
      ret = OB_ENTRY_NOT_EXIST;
    }
  } else {
    worker = CONTAINER_OF(link, ObThWorker, wpool_link_);
  }
  return ret;
}

int ObWorkerPool::init(
    int64_t init_cnt,
    int64_t idle_cnt)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
  } else if (init_cnt < 1
             || idle_cnt < init_cnt) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid arguments",
              K(init_cnt), K(idle_cnt), K(ret));
  } else {
    ObThWorker *worker = nullptr;
    for (int64_t i = 0; i < init_cnt && OB_SUCC(ret); i++) {
      if (OB_FAIL(create_worker(worker))) {
        LOG_ERROR("create worker fail", K(ret));
      } else if (OB_FAIL(workers_.push(worker))) {
        LOG_ERROR("add worker into worker list fail", K(ret));
        destroy_worker(worker);
      }
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
    init_cnt_ = init_cnt;
    idle_cnt_ = idle_cnt;
  } else {
    destroy();
  }
  return ret;
}

void ObWorkerPool::destroy()
{
  ObThWorker *worker = NULL;
  while (OB_SUCCESS == workers_.pop(worker)) {
    destroy_worker(worker);
  }
  is_inited_ = false;
}

ObThWorker *ObWorkerPool::alloc()
{
  int ret = OB_SUCCESS;

  ObThWorker *worker = nullptr;
  if (OB_FAIL(workers_.pop(worker))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("failed to pop worker", K(ret));
    } else {
      LOG_DEBUG("no available worker right now", K(ret));
    }
  }
  if (OB_FAIL(ret) && nullptr == worker) {
    if (OB_SUCC(create_worker(worker))) {
    } else {
      worker = nullptr;
      LOG_ERROR("create worker fail", K(ret));
    }
  }
  return worker;
}

void ObWorkerPool::free(ObThWorker *worker)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(worker)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(worker), K(ret));
  } else {
    // TODO: destroy or add free list.
    if (worker_cnt_ > idle_cnt_) {
      destroy_worker(worker);
    } else  if (OB_FAIL(workers_.push(worker))) {
      LOG_ERROR("add worker to free list fail, destroy worker", K(ret));
      destroy_worker(worker);
    }
  }
}

int ObWorkerPool::create_worker(ObThWorker *&worker)
{
  int ret = OB_SUCCESS;
  worker = OB_NEW(ObThWorker, ObModIds::OMT);
  if (NULL == worker) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(worker->init())) {
    LOG_ERROR("init worker fail", K(ret));
  }
  if (OB_FAIL(ret) && nullptr != worker) {
    ob_delete(worker);
  }
  if (OB_SUCC(ret)) {
    ATOMIC_INC(&worker_cnt_);
  }
  return ret;
}

void ObWorkerPool::destroy_worker(ObThWorker *worker)
{
  if (!OB_ISNULL(worker)) {
    worker->stop();
    worker->activate();
    worker->wait();
    worker->destroy();
    ob_delete(worker);
    worker_cnt_--;
  }
}
