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

#ifndef OB_RETRY_QUEUE_H
#define OB_RETRY_QUEUE_H


#include "lib/queue/ob_link_queue.h"
#include "rpc/ob_request.h"

namespace oceanbase
{
namespace omt
{
const uint64_t RETRY_QUEUE_TIMESTEP = 10 * 1000L;
class ObRetryQueue {
public:
  ObRetryQueue()
  {
    last_timestamp_ = common::ObTimeUtility::current_time();
  }
  enum { RETRY_QUEUE_SIZE = 256 };
  int push(rpc::ObRequest &req, const uint64_t timestamp);
  int pop(common::ObLink *&task, bool need_clear = false);
  uint64_t get_last_timestamp() const;

private:

  common::ObSpLinkQueue queue_[RETRY_QUEUE_SIZE];
  uint64_t last_timestamp_;
};

}  // omt
}  // oceanbase


#endif