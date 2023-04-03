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

#include "ob_rpc_stat.h"
#include "lib/time/ob_time_utility.h"
#include "lib/ob_errno.h"
#include "lib/lock/ob_mutex.h"

using namespace oceanbase;
using namespace oceanbase::rpc;
using namespace oceanbase::common;

////////////////////////////////////////////////////////
// RocStatItem
RpcStatItem::RpcStatItem()
    : lock_(ObLatchIds::RPC_STAT_LOCK), time_(0), size_(0), count_(0),
      max_rt_(0), min_rt_(0),
      max_sz_(0), min_sz_(0),
      failures_(0), timeouts_(0), sync_(0), async_(0),
      last_ts_(0),
      isize_(0), icount_(0),
      net_time_(0), wait_time_(0), queue_time_(0), process_time_(0),
      ilast_ts_(0), dcount_(0)
{}

void RpcStatItem::reset()
{
  time_ = 0;
  size_ = 0;
  count_ = 0;
  max_rt_ = 0;
  min_rt_ = 0;
  max_sz_ = 0;
  min_sz_ = 0;
  failures_ = 0;
  timeouts_ = 0;
  sync_ = 0;
  async_ = 0;
  last_ts_ = 0;
  isize_ = 0;
  icount_ = 0;
  net_time_ = 0;
  wait_time_ = 0;
  queue_time_ = 0;
  process_time_ = 0;
  ilast_ts_ = 0;
  dcount_ = 0;
}

void RpcStatItem::add_piece(const RpcStatPiece &piece)
{
  if (piece.reset_dcount_) {
    dcount_ = 0;
  } else if (!piece.is_server_) {
    count_++;
    time_ += piece.time_;
    max_rt_ = std::max(max_rt_, piece.time_);
    if (0 != min_rt_) {
      min_rt_ = std::min(min_rt_, piece.time_);
    } else {
      min_rt_ = piece.time_;
    }
    size_ += piece.size_;
    max_sz_ = std::max(max_sz_, piece.size_);
    if (0 != min_sz_) {
      min_sz_ = std::min(min_sz_, piece.size_);
    } else {
      min_sz_ = piece.size_;
    }
    if (piece.async_) {
      async_++;
    } else {
      sync_++;
    }
    if (piece.failed_) {
      failures_++;
      if (piece.is_timeout_) {
        timeouts_++;
      }
    }
    last_ts_ = common::ObTimeUtility::current_time();
  } else if (!piece.is_deliver_) {
    icount_++;
    isize_ += piece.size_;
    net_time_ += piece.net_time_;
    wait_time_ += piece.wait_time_;
    queue_time_ += piece.queue_time_;
    process_time_ += piece.process_time_;
    ilast_ts_ = common::ObTimeUtility::current_time();
  } else {
    dcount_++;
  }
}

////////////////////////////////////////////////////////
// RocStatEntry
void RpcStatEntry::add_piece(const RpcStatPiece &piece)
{
  bulk_.add_piece(piece);
}

void RpcStatEntry::get_item(RpcStatItem &item) const
{
  bulk_.get_item(item);
}

////////////////////////////////////////////////////////
// RocStatService
int RpcStatService::add(int64_t pidx, const RpcStatPiece &piece)
{
  int ret = OB_SUCCESS;
  if (pidx < 0 || pidx >= MAX_PCODE_COUNT) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    entries_[pidx].add_piece(piece);
  }
  return ret;
}

int RpcStatService::get(int64_t pidx, RpcStatItem &item) const
{
  int ret = OB_SUCCESS;
  if (pidx < 0 || pidx >= MAX_PCODE_COUNT) {
    ret = OB_SIZE_OVERFLOW;
  } else {
    entries_[pidx].get_item(item);
  }
  return ret;
}

namespace oceanbase
{
namespace rpc
{
RpcStatService __attribute__((weak)) *get_stat_srv_by_tenant_id(uint64_t tenant_id)
{
  UNUSED(tenant_id);
  return nullptr;
}
}
}
