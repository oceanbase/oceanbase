/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_DTL_H
#define OB_DTL_H

#include <stdint.h>
#include "lib/allocator/ob_safe_arena.h"
#include "lib/lock/ob_small_spin_lock.h"
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
  ObDtlHashTableCell() : spin_lock_(common::ObLatchIds::DTL_CHANNEL_MGR_LOCK) {}
  ~ObDtlHashTableCell() { chan_list_.reset(); }

  int insert_channel(uint64_t chid, ObDtlChannel *&ch);
  int remove_channel(uint64_t chid, ObDtlChannel *&ch);
  int get_channel(uint64_t chid, ObDtlChannel *&ch);

  int foreach_refactored(const std::function<int(ObDtlChannel *ch)> &op);
private:
  common::ObByteLock spin_lock_;
  common::ObDList<ObDtlChannel> chan_list_;
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

  int foreach_refactored(const std::function<int(ObDtlChannel *ch)> &op);

private:
  inline int64_t calc_bucket_idx(uint64_t hash_val) const { return hash_val & (bucket_num_ - 1); }
  int64_t bucket_num_;
  ObDtlHashTableCell* bucket_cells_;
  common::ObFIFOAllocator allocator_;
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
  // Init and register a channel that was pre-allocated (e.g. via placement new).
  // Skips allocation; does not free chan on error.
  int register_data_channel(ObDtlChannel *chan, ObDtlFlowControl *dfc, ObTenantDfc *tenant_dfc);
  // Destroy channel from DTL service.
  int destroy_channel(uint64_t chid);

  // Remove channel from DTL service but don't release channel
  int remove_channel(uint64_t chid, ObDtlChannel *&ch);

  int foreach_refactored(const std::function<int(ObDtlChannel *ch)> &op);

  //
  // Get channel from DTL by its channel ID.
  int get_channel(uint64_t chid, ObDtlChannel *&chan);
  //
  // Release channel which is gotten from DTL.
  int release_channel(ObDtlChannel *chan);

  int get_tenant_dfc(uint64_t tenant_id, ObTenantDfc *&tenant_dfc);
  OB_INLINE ObDfcServer &get_dfc_server();
  OB_INLINE const ObDfcServer &get_dfc_server() const;

public:
  // NOTE: This function doesn't have mutex protection. Make sure the
  // first call is in a single thread and after that use it as you
  // like.
  static ObDtl *instance();

  static uint64_t get_hash_value(int64_t chid)
  {
    return common::murmurhash(&chid, sizeof(chid), 0);
  }
private:
  int new_channel(
      uint64_t tenant_id, uint64_t chid, const common::ObAddr &peer, ObDtlChannel *&chan, bool is_local);
  int init_channel(
      uint64_t tenant_id, uint64_t chid, const ObAddr &peer, ObDtlChannel *&chan,
      ObDtlFlowControl *dfc, const bool need_free_chan);
private:
  static const int64_t DTL_CELL_BUCKET_BASE = 131072;
  bool is_inited_;
  common::ObSafeArena allocator_;
  ObDtlRpcProxy rpc_proxy_;
  ObDfcServer dfc_server_;
  ObDtlHashTable hash_table_;
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
