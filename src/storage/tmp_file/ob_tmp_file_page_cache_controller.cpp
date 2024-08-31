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

#include "storage/tmp_file/ob_tmp_file_page_cache_controller.h"

namespace oceanbase
{
namespace tmp_file
{

int ObTmpFilePageCacheController::init(ObSNTenantTmpFileManager &file_mgr)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObTmpFilePageCacheController init twice");
  } else if (OB_FAIL(task_allocator_.init(lib::ObMallocAllocator::get_instance(),
                                          OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                          ObMemAttr(MTL_ID(), "TmpFileCtl", ObCtxIds::DEFAULT_CTX_ID)))) {
    STORAGE_LOG(WARN, "fail to init task allocator", KR(ret));
  } else if (OB_FAIL(flush_mgr_.init())) {
    STORAGE_LOG(WARN, "fail to init flush task mgr", KR(ret));
  } else if (OB_FAIL(flush_priority_mgr_.init())) {
    STORAGE_LOG(WARN, "fail to init flush priority mgr", KR(ret));
  } else if (OB_FAIL(write_buffer_pool_.init())) {
    STORAGE_LOG(WARN, "fail to init write buffer pool", KR(ret));
  } else if (OB_FAIL(flush_tg_.init())) {
    STORAGE_LOG(WARN, "fail to init flush thread", KR(ret));
  } else if (OB_FAIL(swap_tg_.init(file_mgr))) {
    STORAGE_LOG(WARN, "fail to init swap thread", KR(ret));
  } else {
    flush_all_data_ = false;
    is_inited_ = true;
  }
  return ret;
}

int ObTmpFilePageCacheController::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    STORAGE_LOG(WARN, "tmp file page cache controller is not inited");
  } else if (OB_FAIL(swap_tg_.start())) {
    STORAGE_LOG(WARN, "fail to start swap thread", KR(ret));
  }
  return ret;
}

void ObTmpFilePageCacheController::stop()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    STORAGE_LOG(WARN, "tmp file page cache controller is not inited");
  } else {
    // stop background threads should follow the order 'swap' -> 'flush' because 'swap' holds ref to 'flush'
    swap_tg_.stop();
  }
}

void ObTmpFilePageCacheController::wait()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    STORAGE_LOG(WARN, "tmp file page cache controller is not inited");
  } else {
    swap_tg_.wait();
  }
}

void ObTmpFilePageCacheController::destroy()
{
  swap_tg_.destroy();
  flush_tg_.destroy();
  task_allocator_.reset();
  write_buffer_pool_.destroy();
  flush_mgr_.destroy();
  evict_mgr_.destroy();
  flush_priority_mgr_.destroy();
  flush_all_data_ = false;
  is_inited_ = false;
}

int ObTmpFilePageCacheController::swap_job_enqueue_(ObTmpFileSwapJob *swap_job)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(swap_job)){
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "swap job is null", KR(ret));
  } else if (!swap_job->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "swap job is not valid", KR(ret), KPC(swap_job));
  } else if (OB_FAIL(swap_tg_.swap_job_enqueue(swap_job))) {
    STORAGE_LOG(WARN, "fail to enqueue swap job", KR(ret), KP(swap_job));
  }
  return ret;
}

int ObTmpFilePageCacheController::free_swap_job_(ObTmpFileSwapJob *swap_job)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(swap_job)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "swap job is null", KR(ret));
  } else if (swap_job->is_inited() && !swap_job->is_finished()) {
    ret = OB_EAGAIN;
    STORAGE_LOG(ERROR, "swap job is not finished", KR(ret), KPC(swap_job));
  } else {
    swap_job->~ObTmpFileSwapJob();
    task_allocator_.free(swap_job);
  }
  return ret;
}

int ObTmpFilePageCacheController::invoke_swap_and_wait(int64_t expect_swap_size, int64_t timeout_ms)
{
  int ret = OB_SUCCESS;

  int64_t mem_limit = write_buffer_pool_.get_memory_limit();
  expect_swap_size = min(expect_swap_size, static_cast<int64_t>(0.2 * mem_limit));

  void *task_buf = nullptr;
  ObTmpFileSwapJob *swap_job = nullptr;
  if (OB_ISNULL(task_buf = task_allocator_.alloc(sizeof(ObTmpFileSwapJob)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to allocate memory for swap job", KR(ret));
  } else if (FALSE_IT(swap_job = new (task_buf) ObTmpFileSwapJob())) {
  } else if (OB_FAIL(swap_job->init(expect_swap_size, timeout_ms))) {
    STORAGE_LOG(WARN, "fail to init sync swap job", KR(ret), KPC(swap_job));
  } else if (OB_FAIL(swap_job_enqueue_(swap_job))) {
    STORAGE_LOG(WARN, "fail to enqueue swap job", KR(ret), KPC(swap_job));
  } else {
    swap_tg_.notify_doing_swap();
    if (OB_FAIL(swap_job->wait_swap_complete())) {
      STORAGE_LOG(WARN, "fail to wait for swap job complete timeout", KR(ret));
    }
  }

  if (OB_NOT_NULL(swap_job)) {
    if (OB_SUCCESS != swap_job->get_ret_code()) {
      ret = swap_job->get_ret_code();
    }
    // reset swap job to set is_finished to false in case of failure to push into queue:
    // otherwise job is not finished, but it will not be executed, so it will never become finished.
    swap_job->reset();
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(free_swap_job_(swap_job))) {
      STORAGE_LOG(ERROR, "fail to free swap job", KR(ret), KR(tmp_ret));
    }
  }
  return ret;
}

}  // end namespace tmp_file
}  // end namespace oceanbase
