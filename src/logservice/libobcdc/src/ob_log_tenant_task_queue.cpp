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
 * tenant task queue
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_log_tenant_task_queue.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace libobcdc
{
ObLogTenantTaskQueue::ObLogTenantTaskQueue(ObLogTenant &host) :
    inited_(false),
    host_(host),
    lease_(),
    log_entry_task_queue_()
{
}

ObLogTenantTaskQueue::~ObLogTenantTaskQueue()
{
  reset();
}

int ObLogTenantTaskQueue::init(const int64_t start_seq)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("ObLogTenantTaskQueue has been initialized", K(inited_));
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(start_seq < 0)) {
    LOG_ERROR("invalid arguments", K(start_seq));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(log_entry_task_queue_.init(start_seq))) {
    LOG_ERROR("init row_data_committer queue fail", KR(ret), K(start_seq));
  } else {
    lease_.reset();
    inited_ = true;
  }

  return ret;
}

void ObLogTenantTaskQueue::reset()
{
  inited_ = false;
  lease_.reset();

  if (log_entry_task_queue_.is_inited()) {
    (void)log_entry_task_queue_.destroy();
  }
}

int ObLogTenantTaskQueue::push_log_entry_task(ObLogEntryTask *task)
{
  int ret = OB_SUCCESS;
  PartTransTask *part_trans_task = NULL;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogTenantTaskQueue has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(task)) {
    LOG_ERROR("invalid task", KPC(task));
    ret = OB_INVALID_ARGUMENT;
  } else {
    const uint64_t tenant_id = task->get_tenant_id();
    const int64_t seq = 1;
    part_trans_task = static_cast<PartTransTask *>(task->get_host());

    if (OB_FAIL(log_entry_task_queue_.set(seq, task))) {
      LOG_ERROR("push task into queue fail", KR(ret),
          K(tenant_id),
          K(seq), "task", *task,
          KPC(part_trans_task),
          "begin_sn", log_entry_task_queue_.begin_sn(),
          "end_sn", log_entry_task_queue_.end_sn());
    } else {
      LOG_DEBUG("[ROW_DATA] push task into queue succ",
          K(tenant_id),
           K(seq), "task", *task,
           KPC(part_trans_task),
          "begin_sn", log_entry_task_queue_.begin_sn(),
          "end_sn", log_entry_task_queue_.end_sn());
    }
  }

  return ret;
}

}
}
