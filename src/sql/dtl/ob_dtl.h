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

#ifndef OB_DTL_H
#define OB_DTL_H

#include <stdint.h>
#include "lib/hash/ob_hashmap.h"
#include "lib/allocator/ob_safe_arena.h"
#include "sql/dtl/ob_dtl_rpc_proxy.h"
#include "sql/dtl/ob_dtl_fc_server.h"

namespace oceanbase {
namespace sql {
namespace dtl {

class ObDtlChannel;
using obrpc::ObDtlRpcProxy;

class ObDtlHashTableCell
{
public:
  ObDtlHashTableCell()
  {}
  ~ObDtlHashTableCell() { chan_list_.reset(); }

  int insert_channel(uint64_t chid, ObDtlChannel *&ch);
  int remove_channel(uint64_t chid, ObDtlChannel *&ch);
  int get_channel(uint64_t chid, ObDtlChannel *&ch);

  int foreach_refactored(std::function<int(ObDtlChannel *ch)> op);
private:
  ObDList<ObDtlChannel> chan_list_;
};

class ObDtlHashTable
{
public:
  ObDtlHashTable() :
    bucket_num_(0),
    bucket_cells_(nullptr),
    allocator_()
  {}
  ~ObDtlHashTable();

  int init(int64_t bucket_num);
  int insert_channel(uint64_t hash_val, uint64_t chid, ObDtlChannel *&chan);
  int remove_channel(uint64_t hash_val, uint64_t chid, ObDtlChannel *&ch);
  int get_channel(uint64_t hash_val, uint64_t chid, ObDtlChannel *&ch);

  int foreach_refactored(int64_t nth_cell, std::function<int(ObDtlChannel *ch)> op);

  int64_t get_bucket_num() { return bucket_num_; }
private:
  int64_t bucket_num_;
  ObDtlHashTableCell* bucket_cells_;
  common::ObFIFOAllocator allocator_;
};

class ObDtlChannelManager
{
public:
  ObDtlChannelManager(int64_t idx, ObDtlHashTable &hash_table) :
    idx_(idx), spin_lock_(common::ObLatchIds::DTL_CHANNEL_MGR_LOCK), hash_table_(hash_table)
  {}
  ~ObDtlChannelManager();

  int insert_channel(uint64_t hash_val, uint64_t chid, ObDtlChannel *&chan);
  int remove_channel(uint64_t hash_val, uint64_t chid, ObDtlChannel *&chan);
  int get_channel(uint64_t hash_val, uint64_t chid, ObDtlChannel *&chan);

  int foreach_refactored(int64_t interval, std::function<int(ObDtlChannel *ch)> op);
  TO_STRING_KV(K_(idx));
private:
  int64_t idx_;
  ObSpinLock spin_lock_;
  ObDtlHashTable &hash_table_;
};

class ObDtl
{
public:
  ObDtl();
  virtual ~ObDtl();

  // Initialize DTL service.
  int init();

  ObDtlRpcProxy &get_rpc_proxy();
  const ObDtlRpcProxy &get_rpc_proxy() const;

  //// Channel Manipulations
  //
  // Create channel and register it into DTL service, so that we can
  // retrieve it back by channel ID.
  int create_channel(
      uint64_t tenant_id, uint64_t chid, const common::ObAddr &peer, ObDtlChannel *&chan, ObDtlFlowControl *dfc = nullptr);
  int create_local_channel(
      uint64_t tenant_id, uint64_t chid, const common::ObAddr &peer, ObDtlChannel *&chan, ObDtlFlowControl *dfc = nullptr);
  int create_rpc_channel(
      uint64_t tenant_id, uint64_t chid, const common::ObAddr &peer, ObDtlChannel *&chan, ObDtlFlowControl *dfc = nullptr);
  //
  // Destroy channel from DTL service.
  int destroy_channel(uint64_t chid);

  // Remove channel from DTL service but don't release channel
  int remove_channel(uint64_t chid, ObDtlChannel *&ch);

  int foreach_refactored(std::function<int(ObDtlChannel *ch)> op);

  //
  // Get channel from DTL by its channel ID.
  int get_channel(uint64_t chid, ObDtlChannel *&chan);
  //
  // Release channel which is gotten from DTL.
  int release_channel(ObDtlChannel *chan);

  OB_INLINE ObDfcServer &get_dfc_server();
  OB_INLINE const ObDfcServer &get_dfc_server() const;

public:
  // NOTE: This function doesn't have mutex protection. Make sure the
  // first call is in a single thread and after that use it as you
  // like.
  static ObDtl *instance();

  static uint64_t get_hash_value(int64_t chid)
  {
    uint64_t val = common::murmurhash(&chid, sizeof(chid), 0);
    return val & (BUCKET_NUM - 1);
  }
private:
  int new_channel(
      uint64_t tenant_id, uint64_t chid, const common::ObAddr &peer, ObDtlChannel *&chan, bool is_local);
  int init_channel(
      uint64_t tenant_id, uint64_t chid, const ObAddr &peer, ObDtlChannel *&chan,
      ObDtlFlowControl *dfc, const bool need_free_chan);
  int get_dtl_channel_manager(uint64_t hash_val, ObDtlChannelManager *&ch_mgr);
private:
  // bucket number必须是hash_cnt的整数倍，目前有依赖
  // 当前认为一个ch_mgr管理一批bucket，采用hash_cnt的倍数关系进行上锁
  // 如 ch_mgr(0) lock [0, 256, 512, ..., ]
  // 所以hash_value对于ch_mgr和hash_table必须是同一个
  static const int64_t HASH_CNT = 256;
  static const int64_t BUCKET_NUM = 131072;
  bool is_inited_;
  common::ObSafeArena allocator_;
  ObDtlRpcProxy rpc_proxy_;
  ObDfcServer dfc_server_;
  ObDtlHashTable hash_table_;
  ObDtlChannelManager *ch_mgrs_;
};

OB_INLINE ObDtlRpcProxy &ObDtl::get_rpc_proxy()
{
  return rpc_proxy_;
}

OB_INLINE const ObDtlRpcProxy &ObDtl::get_rpc_proxy() const
{
  return rpc_proxy_;
}

OB_INLINE ObDfcServer &ObDtl::get_dfc_server()
{
  return dfc_server_;
}

OB_INLINE const ObDfcServer &ObDtl::get_dfc_server() const
{
  return dfc_server_;
}

}  // dtl
}  // sql
}  // oceanbase

// We won't check instance pointer again after ensuring existence of
// the DTL instance.
#define DTL (*::oceanbase::sql::dtl::ObDtl::instance())

#endif /* OB_DTL_H */
