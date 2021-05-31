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

#include "ob_trans_elr_task.h"
#include "ob_trans_define.h"
#include "ob_trans_factory.h"
#include "ob_trans_part_ctx.h"

namespace oceanbase {
namespace transaction {

void WaitTransEndTask::reset()
{
  ObTransTask::reset();
  trans_id_.reset();
  partition_.reset();
}

int WaitTransEndTask::make(const int64_t task_type, const ObPartitionKey& partition, const ObTransID& trans_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!trans_id.is_valid() || !partition.is_valid())) {
    TRANS_LOG(WARN, "invalid argument", K(trans_id), K(partition));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(ObTransTask::make(task_type))) {
    TRANS_LOG(WARN, "ObTransTask make error", K(task_type), K(partition), K(trans_id));
  } else {
    trans_id_ = trans_id;
    partition_ = partition;
  }

  return ret;
}

void CallbackTransTask::reset()
{
  ObTransTask::reset();
  partition_.reset();
  trans_id_.reset();
  prev_trans_id_.reset();
  status_ = ObTransResultState::INVALID;
}

int CallbackTransTask::make(const int64_t callback_type, const ObPartitionKey& partition, const ObTransID& trans_id,
    const ObTransID& prev_trans_id, const int status)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!partition.is_valid() || !trans_id.is_valid() || !prev_trans_id.is_valid() ||
                  !ObTransRetryTaskType::is_valid(callback_type) || !ObTransResultState::is_valid(status))) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(partition), K(trans_id), K(prev_trans_id), K(status));
  } else if (OB_FAIL(ObTransTask::make(callback_type))) {
    TRANS_LOG(WARN, "ObTransTask make error", KR(ret), K(partition), K(trans_id), K(prev_trans_id));
  } else {
    partition_ = partition;
    trans_id_ = trans_id;
    prev_trans_id_ = prev_trans_id;
    status_ = status;
  }

  return ret;
}

}  // namespace transaction

}  // namespace oceanbase
