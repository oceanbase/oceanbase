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

#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "rpc/obrpc/ob_rpc_result_code.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_common_rpc_proxy.h"
#include "storage/tx_storage/ob_tenant_freezer_common.h"

#ifndef OCEABASE_STORAGE_TENANT_FREEZER_RPC_
#define OCEABASE_STORAGE_TENANT_FREEZER_RPC_

namespace oceanbase
{
namespace obrpc
{
class ObRpcProxy;
class ObSrvRpcProxy;
class ObTenantFreezerRpcProxy : public ObRpcProxy
{
public:
  DEFINE_TO(ObTenantFreezerRpcProxy);
  RPC_AP(@PR5 post_freeze_request, OB_TENANT_MGR, (storage::ObTenantFreezeArg));
};


// deal with the minor/major freeze rpc
class ObTenantFreezerP : public ObRpcProcessor<
                     ObTenantFreezerRpcProxy::ObRpc<OB_TENANT_MGR> >
{
public:
  ObTenantFreezerP() {}
  virtual ~ObTenantFreezerP() {}

  const static int64_t MAX_CONCURRENT_MINOR_FREEZING = 10;
protected:
  int process();
private:
  int do_tx_data_table_freeze_();
  int do_major_freeze_();
  int do_mds_table_freeze_();
private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantFreezerP);
};

class ObTenantFreezerRpcCb
      : public ObTenantFreezerRpcProxy::AsyncCB<OB_TENANT_MGR>
{
public:
  ObTenantFreezerRpcCb() {}
  virtual ~ObTenantFreezerRpcCb() {}
public:
  int process();
  void on_timeout();
  rpc::frame::ObReqTransport::AsyncCB *clone(
      const rpc::frame::SPAlloc &alloc) const
  {
    void *buf = alloc(sizeof(*this));
    rpc::frame::ObReqTransport::AsyncCB *newcb = NULL;
    if (NULL != buf) {
      newcb = new (buf) ObTenantFreezerRpcCb();
    }
    return newcb;
  }
  void set_args(const storage::ObTenantFreezeArg &arg) { UNUSED(arg); }
private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantFreezerRpcCb);
};



} // obrpc
} // oceanbase
#endif
