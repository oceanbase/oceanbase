/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_ROOTSERVER_FREEZE_WINDOW_OB_WINDOW_COMPACTION_RPC_DEFINE_H_
#define OCEANBASE_ROOTSERVER_FREEZE_WINDOW_OB_WINDOW_COMPACTION_RPC_DEFINE_H_

#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_proxy_macros.h"
#include "observer/ob_server_struct.h"
#include "rootserver/freeze/ob_major_freeze_util.h"

namespace oceanbase
{
namespace rootserver
{
class ObPrimaryMajorFreezeService;
class ObRestoreMajorFreezeService;
}
namespace obrpc
{

struct ObWindowCompactionRequest
{
public:
  ObWindowCompactionRequest()
    : tenant_id_(OB_INVALID_TENANT_ID),
      param_()
    {}
  ~ObWindowCompactionRequest() = default;
  int init(const uint64_t tenant_id, const rootserver::ObWindowCompactionParam &param);
  bool is_valid() const { return is_user_tenant(tenant_id_) && param_.is_valid(); }
  uint64_t tenant_id() const { return tenant_id_; }
  const rootserver::ObWindowCompactionParam &param() const { return param_; }
  TO_STRING_KV(K_(tenant_id), K_(param));
  OB_UNIS_VERSION(1);
public:
  uint64_t tenant_id_;
  rootserver::ObWindowCompactionParam param_;
};

struct ObWindowCompactionResponse
{
public:
  ObWindowCompactionResponse()
    : err_code_(common::OB_SUCCESS)
    {}
  ~ObWindowCompactionResponse() = default;
  bool is_valid() const { return true; }
  TO_STRING_KV(K_(err_code));
  OB_UNIS_VERSION(1);
public:
  int err_code_;
};

class ObWindowCompactionRpcProxy : public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(ObWindowCompactionRpcProxy);
  RPC_S(PR1 window_compaction, OB_WINDOW_COMPACTION,
        (ObWindowCompactionRequest), ObWindowCompactionResponse);
};

class ObTenantWindowCompactionP : public ObWindowCompactionRpcProxy::Processor<OB_WINDOW_COMPACTION>
{
public:
  ObTenantWindowCompactionP()
    : primary_major_freeze_service_(nullptr),
      restore_major_freeze_service_(nullptr)
  {}
  virtual ~ObTenantWindowCompactionP() {}
protected:
  virtual int process() override;
private:
  rootserver::ObPrimaryMajorFreezeService *primary_major_freeze_service_;
  rootserver::ObRestoreMajorFreezeService *restore_major_freeze_service_;
};

} // end namespace obrpc
} // end namespace rootserver

#endif // OCEANBASE_ROOTSERVER_FREEZE_WINDOW_OB_WINDOW_COMPACTION_RPC_DEFINE_H_