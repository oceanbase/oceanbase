/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEABASE_STORAGE_LS_REBUILD_CB_IMPL_
#define OCEABASE_STORAGE_LS_REBUILD_CB_IMPL_

#include "share/ob_ls_id.h"
#include "common/ob_member.h"
#include "common/ob_tablet_id.h"
#include "lib/container/ob_array.h"
#include "ob_storage_ha_struct.h"
#include "logservice/palf/palf_callback.h"
#include "storage/ob_storage_rpc.h"

namespace oceanbase
{
namespace storage
{

class ObLSRebuildCbImpl : public palf::PalfRebuildCb
{
public:
  ObLSRebuildCbImpl();
  virtual ~ObLSRebuildCbImpl();
  int init(
      ObLS *ls,
      common::ObInOutBandwidthThrottle *bandwidth_throttle,
      obrpc::ObStorageRpcProxy *svr_rpc_proxy,
      storage::ObStorageRpc *storage_rpc);
  virtual int on_rebuild(const int64_t id, const palf::LSN &lsn);
  virtual bool is_rebuilding(const int64_t id) const;
  void destroy();
private:
  int check_ls_in_rebuild_status_(bool &is_ls_in_rebuild) const;
  int execute_rebuild_();
  void wakeup_rebuild_service_();
private:
  bool is_inited_;
  ObLS *ls_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  storage::ObStorageRpc *storage_rpc_;
  DISALLOW_COPY_AND_ASSIGN(ObLSRebuildCbImpl);
};


}
}
#endif
