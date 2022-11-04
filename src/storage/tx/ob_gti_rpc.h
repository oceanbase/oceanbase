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

#ifndef OCEANBASE_TRANSACTION_OB_GTI_RPC_
#define OCEANBASE_TRANSACTION_OB_GTI_RPC_

#include "observer/ob_server_struct.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "storage/tx/ob_gti_source.h"

namespace oceanbase
{

namespace transaction
{

class ObGtiRequest
{
  OB_UNIS_VERSION(1);
public:
  ObGtiRequest() : tenant_id_(0), range_(0) {}
  ~ObGtiRequest() {}
  int init(const uint64_t tenant_id, const int64_t range);
  bool is_valid() const;
public:
  uint64_t get_tenant_id() const { return tenant_id_; }
  int64_t get_range() const { return range_; }
  TO_STRING_KV(K_(tenant_id), K_(range));
private:
  uint64_t tenant_id_;
  int64_t range_;
};

} //transaction

namespace obrpc
{

class ObGtiRpcResult
{
  OB_UNIS_VERSION(1);
public:
  ObGtiRpcResult() { reset(); }
  virtual ~ObGtiRpcResult() {}
  int init(const uint64_t tenant_id, const int status, const int64_t start_id, const int64_t end_id);
  uint64_t get_tenant_id() const { return tenant_id_; }
  int get_status() const { return status_; }
  int64_t get_start_id() const { return start_id_; }
  int64_t get_end_id() const { return end_id_; }
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(status), K_(start_id), K_(end_id));
public:
  static const int64_t OB_GTI_RPC_TIMEOUT = 1 * 1000 * 1000;
private:
  uint64_t tenant_id_;
  int status_;
  int64_t start_id_;
  int64_t end_id_;
};

class ObGtiRpcProxy : public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(ObGtiRpcProxy);

  RPC_AP(PR1 post, OB_GET_GTI_REQUEST, (transaction::ObGtiRequest), ObGtiRpcResult);
};

class ObGtiP : public ObRpcProcessor< obrpc::ObGtiRpcProxy::ObRpc<OB_GET_GTI_REQUEST> >
{
public:
  ObGtiP() {}
protected:
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObGtiP);
};

template<ObRpcPacketCode PC>
class ObGtiRPCCB : public ObGtiRpcProxy::AsyncCB<PC>
{
public:
  ObGtiRPCCB() : is_inited_(false), tenant_id_(0), gti_source_(NULL) {}
  ~ObGtiRPCCB() {}
  void set_args(const typename ObGtiRpcProxy::AsyncCB<PC>::Request &args)
  {
    tenant_id_ = args.get_tenant_id();
  }
  int init(transaction::ObGtiSource *gti_source)
  {
    int ret = common::OB_SUCCESS;
    if (is_inited_) {
      ret = OB_INIT_TWICE;
      TRANS_LOG(WARN, "ObGtiRPCCB inited twice", KR(ret));
    } else if (NULL == gti_source) {
      ret = common::OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", KR(ret), KP(gti_source));
    } else {
      gti_source_ = gti_source;
      is_inited_ = true;
    }
    return ret;
  }
  oceanbase::rpc::frame::ObReqTransport::AsyncCB *clone(
      const oceanbase::rpc::frame::SPAlloc &alloc) const {
    ObGtiRPCCB<PC> *newcb = NULL;
    void *buf = alloc(sizeof (*this));
    if (NULL != buf) {
      newcb = new (buf) ObGtiRPCCB<PC>();
      if (newcb) {
        newcb->is_inited_ = is_inited_;
        newcb->tenant_id_ = tenant_id_;
        newcb->gti_source_ = gti_source_;
      }
    }
    return newcb;
  }
public:
  int process()
  {
    const ObGtiRpcResult &result = ObGtiRpcProxy::AsyncCB<PC>::result_;
    const ObAddr &dst = ObGtiRpcProxy::AsyncCB<PC>::dst_;
    ObRpcResultCode &rcode = ObGtiRpcProxy::AsyncCB<PC>::rcode_;

    return process_(result, dst, rcode);
  }
  int process(const obrpc::ObGtiRpcResult &result, const common::ObAddr &dst,
              obrpc::ObRpcResultCode &rcode)
  {
    return process_(result, dst, rcode);
  }
  void set_tenant_id(uint64_t tenant_id) {tenant_id_ = tenant_id;}
  void on_timeout()
  {
    int ret = OB_SUCCESS;
    const common::ObAddr &dst = ObGtiRpcProxy::AsyncCB<PC>::dst_;
    if (!is_inited_) {
      TRANS_LOG(WARN, "ObGtiRPCCB not inited");
    } else {
      MTL_SWITCH(tenant_id_) {
        if (OB_FAIL(gti_source_->refresh_gti_location())) {
          TRANS_LOG(WARN, "refresh gti location fail", K(ret));
        }
      } else {
        TRANS_LOG(WARN, "tenant switch fail", K_(tenant_id), K(dst));
      }
    }
  }
private:
  int process_(const obrpc::ObGtiRpcResult &result, const common::ObAddr &dst,
               obrpc::ObRpcResultCode &rcode)
  {
    int ret = OB_SUCCESS;
    int status = OB_SUCCESS;
    bool update = false;

    if (!is_inited_) {
      TRANS_LOG(WARN, "ObGtiRPCCB not inited");
      ret = OB_NOT_INIT;
    } else if (!is_valid_tenant_id(tenant_id_)) {
      TRANS_LOG(WARN, "tenant_id is invalid", K_(tenant_id), K(dst));
      ret = OB_ERR_UNEXPECTED;
    } else {
      MTL_SWITCH(tenant_id_) {
        if (OB_SUCCESS != rcode.rcode_) {
          status = rcode.rcode_;
          TRANS_LOG(WARN, "gti rpc error", K(rcode), K(dst));
          if (OB_NOT_MASTER == status
              || OB_TENANT_NOT_IN_SERVER == status) {
            if (OB_FAIL(gti_source_->refresh_gti_location())) {
              TRANS_LOG(WARN, "refresh gti location fail", K(ret));
            }
          }
        } else {
          status = result.get_status();
          if (OB_SUCCESS == status) {
            if (OB_FAIL(gti_source_->update_trans_id(result.get_start_id(),
                                                            result.get_end_id()))) {
              TRANS_LOG(WARN, "update trans id fail", K(ret));
            }
          } else if (OB_NOT_MASTER == status) {
            if (OB_FAIL(gti_source_->refresh_gti_location())) {
              TRANS_LOG(WARN, "refresh gti location fail", K(ret));
            }
          }
          TRANS_LOG(INFO, "gti request callback", KR(ret), K(result), K(rcode));
        }
      } else {
        TRANS_LOG(WARN, "tenant switch fail", K_(tenant_id), K(dst));
      }
    }
    return ret;
  }
  bool is_inited_;
  uint64_t tenant_id_;
  transaction::ObGtiSource *gti_source_;
};

} // obrpc

namespace transaction
{

class ObGtiRequestRpc
{
public:
  ObGtiRequestRpc() : is_inited_(false), is_running_(false), rpc_proxy_(NULL) {}
  ~ObGtiRequestRpc() { destroy(); }
  int init(obrpc::ObGtiRpcProxy *rpc_proxy, const common::ObAddr &self, ObGtiSource *gti_source);
  int start();
  int stop();
  int wait();
  void destroy();
public:
  int post(const ObGtiRequest &msg);
private:
  bool is_inited_;
  bool is_running_;
  obrpc::ObGtiRpcProxy *rpc_proxy_;
  obrpc::ObGtiRPCCB<obrpc::OB_GET_GTI_REQUEST> gti_request_cb_;
  common::ObAddr self_;
};

} // transaction

} // oceanbase

#endif // OCEANBASE_TRANSACTION_OB_GTI_RPC_
