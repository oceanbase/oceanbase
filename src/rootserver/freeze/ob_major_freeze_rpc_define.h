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

#ifndef OCEANBASE_ROOTSERVER_OB_MAJOR_FREEZE_RPC_DEFINE_H_
#define OCEANBASE_ROOTSERVER_OB_MAJOR_FREEZE_RPC_DEFINE_H_

#include "lib/utility/ob_print_utils.h"
#include "lib/net/ob_addr.h"
#include "rpc/frame/ob_req_transport.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_proxy_macros.h"
#include "observer/ob_server_struct.h"
#include "share/config/ob_server_config.h"

namespace oceanbase
{
namespace rootserver 
{
class ObPrimaryMajorFreezeService;
class ObRestoreMajorFreezeService;
}
namespace obrpc
{
class ObSrvRpcProxy;

enum class ObTenantAdminMergeType
{
  INVALID_TYPE = 0,
  START_MERGE = 1,
  SUSPEND_MERGE = 2,
  RESUME_MERGE = 3,
  CLEAR_MERGE_ERROR = 4,
};

struct ObSimpleFreezeInfo
{
public:
  uint64_t tenant_id_;

  ObSimpleFreezeInfo()
    : tenant_id_(OB_INVALID_TENANT_ID)
  {}

  ObSimpleFreezeInfo(const uint64_t tenant_id)
    : tenant_id_(tenant_id)
  {}

  bool is_valid() const { return (OB_INVALID_TENANT_ID != tenant_id_); }
  TO_STRING_KV(K_(tenant_id));
  OB_UNIS_VERSION(1);
};

struct ObMajorFreezeRequest
{
public:
  ObSimpleFreezeInfo info_;

  ObMajorFreezeRequest() {}
  ObMajorFreezeRequest(const ObSimpleFreezeInfo &info) : info_(info) {}

  bool is_valid() const { return info_.is_valid(); }
  
  uint64_t tenant_id() const { return info_.tenant_id_; }

  TO_STRING_KV(K_(info));

  OB_UNIS_VERSION(1);
};

struct ObMajorFreezeResponse
{
public:
  ObMajorFreezeResponse() : err_code_(0) {}
  ~ObMajorFreezeResponse() {}

public:
  int err_code_;

  bool is_valid() const { return true; }

  TO_STRING_KV(K_(err_code));

  OB_UNIS_VERSION(1);
};

struct ObTenantAdminMergeRequest
{
public:
  uint64_t tenant_id_;
  ObTenantAdminMergeType type_;
  ObTenantAdminMergeRequest() 
    : tenant_id_(OB_INVALID_TENANT_ID), type_(ObTenantAdminMergeType::INVALID_TYPE) {}
  ObTenantAdminMergeRequest(const uint64_t tenant_id, const ObTenantAdminMergeType &admin_type)
    : tenant_id_(tenant_id), type_(admin_type) {}
  
  bool is_valid() const { return OB_INVALID_TENANT_ID != tenant_id_; }
  
  uint64_t tenant_id() const { return tenant_id_; }
  ObTenantAdminMergeType get_type() const { return type_; }

  TO_STRING_KV(K_(tenant_id), K_(type));

  OB_UNIS_VERSION(1);
};

struct ObTenantAdminMergeResponse
{
public:
  ObTenantAdminMergeResponse() : err_code_(common::OB_SUCCESS) {}
  ~ObTenantAdminMergeResponse() {}
public:
  int err_code_;

  bool is_valid() const { return true; }

  TO_STRING_KV(K_(err_code));

  OB_UNIS_VERSION(1);
};

class ObMajorFreezeRpcProxy : public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(ObMajorFreezeRpcProxy);

  RPC_S(PR1 major_freeze, OB_TENANT_MAJOR_FREEZE,
        (ObMajorFreezeRequest), ObMajorFreezeResponse);
};

class ObTenantAdminMergeRpcProxy : public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(ObTenantAdminMergeRpcProxy);
  RPC_S(PR1 tenant_admin_merge, OB_TENANT_ADMIN_MERGE,
        (ObTenantAdminMergeRequest), ObTenantAdminMergeResponse);
};

class ObTenantMajorFreezeP : public ObMajorFreezeRpcProxy::Processor<OB_TENANT_MAJOR_FREEZE>
{
public:
  ObTenantMajorFreezeP()
    : primary_major_freeze_service_(nullptr), restore_major_freeze_service_(nullptr) {}
  virtual ~ObTenantMajorFreezeP() {}

protected:
  virtual int process() override;

private:
  rootserver::ObPrimaryMajorFreezeService *primary_major_freeze_service_;
  rootserver::ObRestoreMajorFreezeService *restore_major_freeze_service_;
};

class ObTenantAdminMergeP : public ObTenantAdminMergeRpcProxy::Processor<OB_TENANT_ADMIN_MERGE>
{
public:
  ObTenantAdminMergeP()
    : primary_major_freeze_service_(nullptr), restore_major_freeze_service_(nullptr) {}
  virtual ~ObTenantAdminMergeP() {}
protected:
  virtual int process() override;
private:
  rootserver::ObPrimaryMajorFreezeService *primary_major_freeze_service_;
  rootserver::ObRestoreMajorFreezeService *restore_major_freeze_service_;
};

} // namespace obrpc
} // namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_MAJOR_FREEZE_RPC_DEFINE_H_
