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
 *
 * ObLogEntryTask Pool
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_log_entry_task_pool.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace libobcdc
{

ObLogEntryTaskPool::ObLogEntryTaskPool()
  : inited_(false),
    alloc_cnt_(0),
    block_alloc_(),
    allocator_()
{
}

ObLogEntryTaskPool::~ObLogEntryTaskPool()
{
  destroy();
}

int ObLogEntryTaskPool::init(const int64_t fixed_task_count)
{
  int ret = OB_SUCCESS;
  lib::ObMemAttr mem_attr(OB_SERVER_TENANT_ID, "CDCEntryTaskPol");

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("RowDataTaskPool has been initialized");
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(fixed_task_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(fixed_task_count));
  } else if (OB_FAIL(allocator_.init(
      sizeof(ObLogEntryTask),
      common::OB_MALLOC_NORMAL_BLOCK_SIZE,
      block_alloc_,
      mem_attr))) {
    LOG_ERROR("init allocator for ObLogEntryTaskPool failed", KR(ret));
  } else {
    allocator_.set_nway(4);
    inited_ = true;
    LOG_INFO("LogEntryTaskPool init success", K(fixed_task_count));
  }

  return ret;
}

void ObLogEntryTaskPool::destroy()
{
  try_purge_pool();
  inited_ = false;
  alloc_cnt_ = 0;
  allocator_.destroy();
}

int ObLogEntryTaskPool::alloc(
    ObLogEntryTask *&log_entry_task,
    PartTransTask &host)
{
  int ret = OB_SUCCESS;
  void *ptr = nullptr;

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("RowDataTaskPool has not been initialized", KR(ret));
  } else if (OB_ISNULL(ptr = allocator_.alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc log_entry_task failed", KR(ret), K_(alloc_cnt), "memory_hold", allocator_.hold());
  } else {
    log_entry_task = new(ptr) ObLogEntryTask(host);
    ATOMIC_INC(&alloc_cnt_);
  }

  return ret;
}

void ObLogEntryTaskPool::free(ObLogEntryTask *log_entry_task)
{
  int ret = OB_SUCCESS;

  if (OB_LIKELY(inited_) && OB_NOT_NULL(log_entry_task)) {
    // Timely memory recycling
    log_entry_task->~ObLogEntryTask();
    allocator_.free(log_entry_task);
    log_entry_task = nullptr;
    ATOMIC_DEC(&alloc_cnt_);
  }
}

int64_t ObLogEntryTaskPool::get_alloc_count() const
{
  return ATOMIC_LOAD(&alloc_cnt_);
}

void ObLogEntryTaskPool::print_stat_info()
{
  LOG_INFO("[STAT] [LOG_ENTRY_TASK_POOL]",
      K_(alloc_cnt), "memory_used",
      SIZE_TO_STR(alloc_cnt_ * sizeof(ObLogEntryTask)),
      "allocated memory", SIZE_TO_STR(allocator_.hold()),
      K_(allocator));
}

void ObLogEntryTaskPool::try_purge_pool()
{
  if (inited_) {
    allocator_.try_purge();
  }
}

} // namespace libobcdc
} // namespace oceanbase
