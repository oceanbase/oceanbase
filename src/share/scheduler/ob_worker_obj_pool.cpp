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

#include "share/scheduler/ob_worker_obj_pool.h"
#include "share/scheduler/ob_dag_worker.h"
#include "share/rc/ob_context.h"

namespace oceanbase {
using namespace lib;
using namespace common;
using namespace omt;
namespace share {

/***************************************ObWorkerObjPool impl********************************************/
ObWorkerObjPool& ObWorkerObjPool::get_instance()
{
  static ObWorkerObjPool objWorkerPool;
  return objWorkerPool;
}

ObWorkerObjPool::ObWorkerObjPool() : is_inited_(false), init_worker_cnt_(0), worker_cnt_(0)
{}

ObWorkerObjPool::~ObWorkerObjPool()
{
  destroy();
}

int ObWorkerObjPool::init(const int64_t total_mem_limit /*= TOTAL_LIMIT*/,
    const int64_t hold_mem_limit /*= HOLD_LIMIT*/, const int64_t page_size /*= PAGE_SIZE*/)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "ObWorkerObjPool init twice", K(ret));
  } else if (0 >= total_mem_limit || 0 >= hold_mem_limit || hold_mem_limit > total_mem_limit || 0 >= page_size) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN,
        "init ObTenantThreadPool with invalid arguments",
        K(ret),
        K(total_mem_limit),
        K(hold_mem_limit),
        K(page_size));
  } else if (OB_FAIL(allocator_.init(total_mem_limit, hold_mem_limit, page_size))) {
    COMMON_LOG(WARN, "failed to init allocator", K(ret), K(total_mem_limit), K(hold_mem_limit), K(page_size));
  } else {
    is_inited_ = true;
    allocator_.set_label(ObModIds::OB_SCHEDULER);
  }
  if (!is_inited_) {
    destroy();
    COMMON_LOG(WARN, "failed to init ObWorkerObjPool", K(ret));
  } else {
    COMMON_LOG(INFO, "ObWorkerObjPool is inited", K(ret));
  }
  return ret;
}

int ObWorkerObjPool::set_init_worker_cnt_inc(int32_t worker_cnt_inc)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(lock_);
  init_worker_cnt_ += worker_cnt_inc;
  return ret;
}

int ObWorkerObjPool::adjust_worker_cnt()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObMutexGuard guard(lock_);
  while (worker_cnt_ < init_worker_cnt_ * OBJ_CREATE_PERCENT && OB_SUCCESS == tmp_ret) {
    tmp_ret = create_worker();  // create worker if not enough
  }
  while (worker_cnt_ > init_worker_cnt_ * OBJ_RELEASE_PERCENT && OB_SUCCESS == tmp_ret) {
    tmp_ret = release_worker();  // release worker if there is free worker
  }
  if (OB_SUCCESS != tmp_ret && OB_ITER_END != tmp_ret) {
    ret = tmp_ret;
  }
  return ret;
}

int ObWorkerObjPool::release_worker()
{
  int ret = OB_SUCCESS;
  if (free_worker_obj_list_.is_empty()) {
    ret = OB_ITER_END;
  } else {
    ObDagWorkerNew* worker = free_worker_obj_list_.remove_last();
    --worker_cnt_;
    ob_delete(worker);
  }
  return ret;
}

int ObWorkerObjPool::create_worker()
{
  int ret = OB_SUCCESS;
  void* buf = NULL;
  ObDagWorkerNew* worker = OB_NEW(ObDagWorkerNew, ObModIds::OB_SCHEDULER);
  if (OB_ISNULL(worker)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "failed to allocate ObDagWorkerNew", K(ret));
  } else if (OB_FAIL(worker->init())) {
    COMMON_LOG(WARN, "failed to init worker", K(ret));
  } else if (!free_worker_obj_list_.add_last(worker)) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "failed to add new worker to worker list", K(ret));
  } else {
    COMMON_LOG(INFO, "create worker success", K(worker));
    ++worker_cnt_;
  }
  if (OB_FAIL(ret)) {
    if (NULL != worker) {
      worker->stop_worker();
      ob_delete(worker);
      COMMON_LOG(INFO, "ob_delete", K(worker));
    }
  }
  return ret;
}

void ObWorkerObjPool::destroy()
{
  ObDagWorkerNew* cur = free_worker_obj_list_.get_first();
  const ObDagWorkerNew* head = free_worker_obj_list_.get_header();
  ObDagWorkerNew* next = NULL;
  while (NULL != cur && head != cur) {
    next = cur->get_next();
    cur->stop_worker();
    ob_delete(cur);
    COMMON_LOG(INFO, "worker destroyed", K(cur));
    cur = next;
  }
  free_worker_obj_list_.reset();

  is_inited_ = false;
  init_worker_cnt_ = 0;
  worker_cnt_ = 0;
  allocator_.destroy();
  COMMON_LOG(INFO, "worker object pool is destroyed");
}

int ObWorkerObjPool::get_worker_obj(ObDagWorkerNew*& worker)
{
  int ret = OB_SUCCESS;
  {
    ObMutexGuard guard(lock_);
    if (worker_cnt_ >= OBJ_UP_LIMIT_PERCENT * init_worker_cnt_) {
      ret = OB_BUF_NOT_ENOUGH;
      worker = NULL;
      COMMON_LOG(INFO, "worker count is reach the upper limit", K(ret));
    } else if (free_worker_obj_list_.is_empty()) {
      create_worker();
      worker = free_worker_obj_list_.remove_first();
    } else {
      worker = free_worker_obj_list_.remove_first();
    }
  }  // end of lock
  if (OB_SUCC(ret)) {
    COMMON_LOG(INFO, "get worker object success", K(ret), K(worker));
  }
  return ret;
}

int ObWorkerObjPool::release_worker_obj(ObDagWorkerNew*& worker)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(worker)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObMutexGuard guard(lock_);
    if (!free_worker_obj_list_.add_last(worker)) {  // add into free list
      COMMON_LOG(WARN, "add into free worker list failed", K(ret), K(worker));
      --worker_cnt_;
      ob_delete(worker);
    } else {  // add into free list success
      COMMON_LOG(INFO, "release worker obj success", K(init_worker_cnt_), K(worker_cnt_), K(worker));
    }
  }
  return ret;
}

}  // namespace share
}  // namespace oceanbase
