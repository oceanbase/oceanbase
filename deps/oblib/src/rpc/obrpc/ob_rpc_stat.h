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

#ifndef _OCEABASE_RPC_OBRPC_OB_RPC_STAT_H_
#define _OCEABASE_RPC_OBRPC_OB_RPC_STAT_H_

#include <stdint.h>
#include "lib/lock/ob_spin_lock.h"
#include "lib/random/ob_random.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "lib/list/ob_dlist.h"
#include "lib/worker.h"

namespace oceanbase
{
namespace rpc
{
struct RpcStatPiece
{
  RpcStatPiece()
      : time_(), size_(), async_(), failed_(), is_timeout_(),
        is_server_(), net_time_(),
        wait_time_(), queue_time_(), process_time_(), is_deliver_(false), reset_dcount_(false)
  {}
  int64_t time_;
  int64_t size_;
  bool async_;
  bool failed_;
  bool is_timeout_;

  // server specific
  bool is_server_;
  int64_t net_time_;
  int64_t wait_time_;
  int64_t queue_time_;
  int64_t process_time_;
  bool is_deliver_;
  bool reset_dcount_;
};

struct RpcStatItem
{
  RpcStatItem();

  void reset();
  void add_piece(const RpcStatPiece &piece);

  inline void operator += (const RpcStatItem &item)
  {
    time_ += item.time_;
    size_ += item.size_;
    count_ += item.count_;
    if (0 != min_rt_) {
      min_rt_ = std::min(min_rt_, item.min_rt_);
    } else {
      min_rt_ = item.min_rt_;
    }
    max_rt_ = std::max(min_rt_, item.max_rt_);
    if (0 != min_sz_) {
      min_sz_ = std::min(min_sz_, item.min_sz_);
    } else {
      min_sz_ = item.min_sz_;
    }
    max_sz_ = std::max(min_sz_, item.max_sz_);
    failures_ += item.failures_;
    timeouts_ += item.timeouts_;
    sync_ += item.sync_;
    async_ += item.async_;
    last_ts_ = std::max(last_ts_, item.last_ts_);

    // server specific
    icount_ += item.icount_;
    isize_ += item.isize_;
    net_time_ += item.net_time_;
    wait_time_ += item.wait_time_;
    queue_time_ += item.queue_time_;
    process_time_ += item.process_time_;
    ilast_ts_ = std::max(ilast_ts_, item.ilast_ts_);
    dcount_ += item.dcount_;
  }

  common::ObSpinLock lock_;
  int64_t time_;
  int64_t size_;
  int64_t count_;
  int64_t max_rt_;
  int64_t min_rt_;
  int64_t max_sz_;
  int64_t min_sz_;
  int64_t failures_;
  int64_t timeouts_;
  int64_t sync_;
  int64_t async_;
  int64_t last_ts_;
  // server side
  int64_t isize_;
  int64_t icount_;
  int64_t net_time_;
  int64_t wait_time_;
  int64_t queue_time_;
  int64_t process_time_;
  int64_t ilast_ts_;
  int64_t dcount_;
};

template <int N>
class RpcStatBulk
{
public:
  void add_piece(const RpcStatPiece &piece);
  void get_item(RpcStatItem &item) const;

private:
  RpcStatItem items_[N];
  common::ObRandom rand_;
};

class RpcStatEntry
{
public:
  void add_piece(const RpcStatPiece &piece);
  void get_item(RpcStatItem &item) const;

private:
  RpcStatBulk<10> bulk_;
};

class RpcStatService
{
  static const int64_t MAX_PCODE_COUNT = obrpc::ObRpcPacketSet::THE_PCODE_COUNT;
public:
  int add(int64_t pidx, const RpcStatPiece &piece);
  int get(int64_t pidx, RpcStatItem &item) const;

private:
  RpcStatEntry entries_[MAX_PCODE_COUNT];
};

template <int N>
void RpcStatBulk<N>::add_piece(const RpcStatPiece &piece)
{
  if (piece.reset_dcount_){
    for (int64_t i = 0; i < N; i++) {
      // only reset dcount, no need lock
      items_[i].add_piece(piece);
    }
  } else {
    const int64_t start = rand_.get(0, N - 1);
    for (int64_t i = 0;; i++) {
      const int64_t idx = (i + start) % N;
      if (common::OB_SUCCESS == items_[idx].lock_.trylock()) {
        items_[idx].add_piece(piece);
        if (OB_UNLIKELY(common::OB_SUCCESS != items_[idx].lock_.unlock())) {
          RPC_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "unlock fail");
        }
        break;
      }
    }
  }
}

template <int N>
void RpcStatBulk<N>::get_item(RpcStatItem &item) const
{
  item.reset();
  for (int64_t i = 0; i < N; i++) {
    item += items_[i];
  }
}

extern RpcStatService *get_stat_srv_by_tenant_id(uint64_t tenant_id);

inline void RPC_STAT(obrpc::ObRpcPacketCode pcode, uint64_t tenant_id, const RpcStatPiece &piece)
{
  RpcStatService *srv = nullptr;
  if ((nullptr == (srv = reinterpret_cast<RpcStatService*>(lib::this_worker().get_rpc_stat_srv())))
    && (nullptr == (srv = get_stat_srv_by_tenant_id(tenant_id)))) {
    // cant find rpc_stat_srv_, so do nothing
  } else {
    const int64_t idx = obrpc::ObRpcPacketSet::instance().idx_of_pcode(pcode);
    srv->add(idx, piece);
  }
}

inline int RPC_STAT_GET(int64_t idx, uint64_t tenant_id, RpcStatItem &item)
{
  int ret = common::OB_SUCCESS;
  RpcStatService *srv = nullptr;
  if (nullptr == (srv = get_stat_srv_by_tenant_id(tenant_id))) {
    ret = common::OB_ENTRY_NOT_EXIST;
  } else {
    ret = srv->get(idx, item);
  }
  return ret;
}

} // end of namespace rpc
} // end of namespace oceanbase

#endif /* _OCEABASE_RPC_OBRPC_OB_RPC_STAT_H_ */
