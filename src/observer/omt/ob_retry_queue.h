/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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