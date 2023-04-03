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

#ifndef OB_MULTI_LEVEL_QUEUE_H
#define OB_MULTI_LEVEL_QUEUE_H

#include "lib/queue/ob_priority_queue.h"
#include "rpc/ob_request.h"
#define MULTI_LEVEL_QUEUE_SIZE (10)
#define MULTI_LEVEL_THRESHOLD (2)

namespace oceanbase
{
namespace omt
{


class ObMultiLevelQueue {
public:
  void set_limit(const int64_t limit);
  int push(rpc::ObRequest &req, const int32_t level, const int32_t prio);
  int pop(common::ObLink *&task, const int32_t level, const int64_t timeout_us);
  int pop_timeup(common::ObLink *&task, const int32_t level, const int64_t timeout_us);
  int try_pop(common::ObLink *&task, const int32_t level);
  int64_t get_size(const int32_t level) const;
  int64_t get_total_size() const;
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    common::databuff_printf(buf, buf_len, pos, "total_size=%ld ", get_total_size());
    for(int i = 0; i < MULTI_LEVEL_QUEUE_SIZE; i++) {
      common::databuff_printf(buf, buf_len, pos, "queue[%d]=%ld ", i, queue_[i].size());
    }
    return pos;
  }
private:
  common::ObPriorityQueue<1> queue_[MULTI_LEVEL_QUEUE_SIZE];
};

}  // omt
}  // oceanbase

#endif