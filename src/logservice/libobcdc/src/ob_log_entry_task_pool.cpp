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
  :inited_(false),
   pool_()
{
}

ObLogEntryTaskPool::~ObLogEntryTaskPool()
{
  destroy();
}

int ObLogEntryTaskPool::init(const int64_t fixed_task_count)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("RowDataTaskPool has been initialized");
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(fixed_task_count <= 0)) {
    LOG_ERROR("invalid argument", K(fixed_task_count));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(pool_.init(fixed_task_count, "LEntryTaskPool"))) {
    LOG_ERROR("row data task pool init fail", KR(ret), K(fixed_task_count));
  } else {
    inited_ = true;
    LOG_INFO("LogEntryTaskPool init success", K(fixed_task_count));
  }

  return ret;
}

void ObLogEntryTaskPool::destroy()
{
  inited_ = false;
  pool_.destroy();
}

int ObLogEntryTaskPool::alloc(ObLogEntryTask *&log_entry_task,
    void *host)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("RowDataTaskPool has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(pool_.alloc(log_entry_task))) {
    LOG_ERROR("alloc binlog record fail", KR(ret));
  } else if (OB_ISNULL(log_entry_task)) {
    LOG_ERROR("alloc binlog record fail", K(log_entry_task));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    log_entry_task->set_host(host);
  }

  return ret;
}

void ObLogEntryTaskPool::free(ObLogEntryTask *log_entry_task)
{
  int ret = OB_SUCCESS;

  if (OB_LIKELY(inited_) && OB_LIKELY(NULL != log_entry_task)) {
    // Timely memory recycling
    log_entry_task->reset();

    if (OB_FAIL(pool_.free(log_entry_task))) {
      LOG_ERROR("free binlog record fail", KR(ret), K(log_entry_task));
    } else {
      log_entry_task = NULL;
    }
  }
}

int64_t ObLogEntryTaskPool::get_alloc_count() const
{
  return pool_.get_alloc_count();
}

void ObLogEntryTaskPool::print_stat_info() const
{
  _LOG_INFO("[STAT] [LOG_ENTRY_TASK_POOL] TOTAL=%ld FREE=%ld FIXED=%ld",
      pool_.get_alloc_count(), pool_.get_free_count(), pool_.get_fixed_count());
}

} // namespace libobcdc
} // namespace oceanbase
