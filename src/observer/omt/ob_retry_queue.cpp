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
#include "share/ob_define.h"
#include "lib/time/ob_time_utility.h"
#include "ob_retry_queue.h"


using namespace oceanbase::common;
using namespace oceanbase::omt;
using namespace oceanbase::rpc;


int ObRetryQueue::push(ObRequest &req, const uint64_t timestamp)
{
  uint64_t idx = max(timestamp / RETRY_QUEUE_TIMESTEP, last_timestamp_ / RETRY_QUEUE_TIMESTEP + 2);
  int queue_idx = idx & (RETRY_QUEUE_SIZE - 1);
  return queue_[queue_idx].push(&req);
}

int ObRetryQueue::pop(ObLink *&task, bool need_clear)
{
  int ret = OB_ENTRY_NOT_EXIST;
  uint64_t curr_timestamp = ObTimeUtility::current_time();
  uint64_t idx = last_timestamp_ / RETRY_QUEUE_TIMESTEP;
  if (!need_clear) {
    int queue_idx = idx & (RETRY_QUEUE_SIZE - 1);
    while (last_timestamp_ <= curr_timestamp && OB_FAIL(queue_[queue_idx].pop(task))) {
      ATOMIC_FAA(&last_timestamp_, RETRY_QUEUE_TIMESTEP);
      queue_idx = (++idx) & (RETRY_QUEUE_SIZE - 1);
    }
  } else {
    int queue_idx = 0;
    while (queue_idx < RETRY_QUEUE_SIZE && OB_FAIL(queue_[queue_idx].pop(task))) {
      queue_idx++;
    }
  }
  return ret;
}

uint64_t ObRetryQueue::get_last_timestamp() const
{
  return last_timestamp_;
}
