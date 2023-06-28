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
  void destroy();
private:
  int check_ls_in_rebuild_status_(bool &is_ls_in_rebuild);
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
