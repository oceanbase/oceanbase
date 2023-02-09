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
#include "ob_multi_level_queue.h"
#include "rpc/obrpc/ob_rpc_packet.h"


using namespace oceanbase::common;
using namespace oceanbase::omt;
using namespace oceanbase::rpc;
using namespace oceanbase::obrpc;


void ObMultiLevelQueue::set_limit(int64_t limit)
{
  for (int32_t level = 0; level < MULTI_LEVEL_QUEUE_SIZE; level++) {
    queue_[level].set_limit(limit);
  }
}

int ObMultiLevelQueue::push(ObRequest &req, const int32_t level, const int32_t prio)
{
  int ret = OB_SUCCESS;
  if (level < 0 || level >= MULTI_LEVEL_QUEUE_SIZE) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("pop queue out of bound", K(ret), K(level));
  } else {
    ret = queue_[level].push(&req, prio);
  }
  return ret;
}

int ObMultiLevelQueue::pop(ObLink *&task,
    const int32_t level, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  if (timeout_us < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pop queue invalid argument", K(ret), K(timeout_us));
  } else if (level < 0 || level >= MULTI_LEVEL_QUEUE_SIZE) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("pop queue out of bound", K(ret), K(level));
  } else {
    ret = queue_[level].pop(task, timeout_us);
  }
  return ret;
}

int ObMultiLevelQueue::pop_timeup(ObLink *&task,
    const int32_t level, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  if (timeout_us < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pop queue invalid argument", K(ret), K(timeout_us));
  } else if (level < 0 || level >= MULTI_LEVEL_QUEUE_SIZE) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("pop queue out of bound", K(ret), K(level));
  } else {
    ret = queue_[level].pop(task, timeout_us);
    if (ret == OB_SUCCESS && nullptr != task) {
      ObRequest *req = static_cast<rpc::ObRequest*>(task);
      const ObRpcPacket *pkt
          = static_cast<const ObRpcPacket*>(&(req->get_packet()));
      int64_t timeup_us = 5 * 1000 * 1000L;
      if (nullptr == pkt) {
        LOG_WARN("pop req has empty pkt", K(req));
      } else {
        timeup_us = min(timeup_us, pkt->get_timeout());
      }
      if (ObTimeUtility::current_time() - req->get_enqueue_timestamp() >= timeup_us) {
        req->set_discard_flag(true);
      } else if (OB_SUCC(queue_[level].push_front(req, 0))) {
        task = nullptr;
      } else {
        ret = OB_ERR_UNEXPECTED;
      }
    }
  }
  return ret;
}

int ObMultiLevelQueue::try_pop(ObLink *&task, const int32_t level)
{
  int ret = OB_SUCCESS;
  if (level < 0 || level >= MULTI_LEVEL_QUEUE_SIZE) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("pop queue out of bound", K(ret), K(level));
  } else if (queue_[level].size() > 0) {
    ret = queue_[level].pop(task, 0L);
  }
  return ret;
}

int64_t ObMultiLevelQueue::get_size(const int32_t level) const
{
  int64_t size = -1;
  if (level >= 0 && level < MULTI_LEVEL_QUEUE_SIZE) {
    size = queue_[level].size();
  }
  return size;
}

int64_t ObMultiLevelQueue::get_total_size() const
{
  int64_t size = 0;
  for (int level = 0; level < MULTI_LEVEL_QUEUE_SIZE; level++) {
    size += queue_[level].size();
  }
  return size;
}