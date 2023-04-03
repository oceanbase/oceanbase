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

#ifndef OCEANBASE_TRANSACTION_OB_GTS_RPC_
#define OCEANBASE_TRANSACTION_OB_GTS_RPC_

#include "common/ob_queue_thread.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/utility.h"
#include "share/ob_define.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "rpc/obrpc/ob_rpc_result_code.h"
#include "share/ob_rpc_struct.h"
#include "storage/tx/ob_gts_msg.h"
#include "storage/tx/ob_ts_worker.h"
#include "storage/tx/ob_ts_response_handler.h"
#include "storage/tx/ob_ts_mgr.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{

namespace observer
{
struct ObGlobalContext;
}

namespace transaction
{
class ObTsMgr;
class ObTsWorker;
}

namespace obrpc
{
class ObGtsRpcResult
{
  OB_UNIS_VERSION(1);
public:
  ObGtsRpcResult() { reset(); }
  virtual ~ObGtsRpcResult() {}
  int init(const uint64_t tenant_id, const int status,
           const transaction::MonotonicTs srr, const int64_t gts_start, const int64_t gts_end);
  uint64_t get_tenant_id() const { return tenant_id_; }
  int get_status() const { return status_; }
  transaction::MonotonicTs get_srr() const { return srr_; }
  int64_t get_gts_start() const { return gts_start_; }
  int64_t get_gts_end() const { return gts_end_; }
  void reset();
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(status), K_(srr), K_(gts_start), K_(gts_end));
public:
  static const int64_t OB_GTS_RPC_TIMEOUT = 1 * 1000 * 1000;
private:
  uint64_t tenant_id_;
  int status_;
  transaction::MonotonicTs srr_;
  int64_t gts_start_;
  int64_t gts_end_;
};

class ObGtsRpcProxy : public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(ObGtsRpcProxy);

  RPC_AP(PR1 post, OB_GET_GTS_REQUEST, (transaction::ObGtsRequest), ObGtsRpcResult);
  RPC_AP(PR1 post, OB_GET_GTS_ERR_RESPONSE, (transaction::ObGtsErrResponse), ObGtsRpcResult);
};

class ObGtsP : public ObRpcProcessor< obrpc::ObGtsRpcProxy::ObRpc<OB_GET_GTS_REQUEST> >
{
public:
  ObGtsP() {}
protected:
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObGtsP);
};

class ObGtsErrRespP : public ObRpcProcessor< obrpc::ObGtsRpcProxy::ObRpc<OB_GET_GTS_ERR_RESPONSE> >
{
public:
  ObGtsErrRespP() {}
protected:
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObGtsErrRespP);
};

template<ObRpcPacketCode PC>
class ObGtsRPCCB : public ObGtsRpcProxy::AsyncCB<PC>
{
public:
  ObGtsRPCCB() : is_inited_(false), tenant_id_(0), ts_mgr_(NULL), ts_worker_(NULL) {}
  ~ObGtsRPCCB() {}
  void set_args(const typename ObGtsRpcProxy::AsyncCB<PC>::Request &args)
  {
    tenant_id_ = args.get_tenant_id();
  }
  int init(transaction::ObTsMgr *ts_mgr,
           transaction::ObTsWorker *ts_worker)
  {
    int ret = common::OB_SUCCESS;
    if (is_inited_) {
      ret = OB_INIT_TWICE;
      TRANS_LOG(WARN, "ObGtsRPCCB inited twice", KR(ret));
    } else if (NULL == ts_mgr || NULL == ts_worker) {
      ret = common::OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", KR(ret), KP(ts_mgr), KP(ts_worker));
    } else {
      ts_mgr_ = ts_mgr;
      ts_worker_ = ts_worker;
      is_inited_ = true;
    }
    return ret;
  }
  oceanbase::rpc::frame::ObReqTransport::AsyncCB *clone(
      const oceanbase::rpc::frame::SPAlloc &alloc) const {
    ObGtsRPCCB<PC> *newcb = NULL;
    void *buf = alloc(sizeof (*this));
    if (NULL != buf) {
      newcb = new (buf) ObGtsRPCCB<PC>();
      if (newcb) {
        newcb->is_inited_ = is_inited_;
        newcb->tenant_id_ = tenant_id_;
        newcb->ts_mgr_ = ts_mgr_;
        newcb->ts_worker_ = ts_worker_;
      }
    }
    return newcb;
  }
public:
  int process()
  {
    const ObGtsRpcResult &result = ObGtsRpcProxy::AsyncCB<PC>::result_;
    const ObAddr &dst = ObGtsRpcProxy::AsyncCB<PC>::dst_;
    ObRpcResultCode &rcode = ObGtsRpcProxy::AsyncCB<PC>::rcode_;

    return process_(result, dst, rcode);
  }
  int process(const obrpc::ObGtsRpcResult &result, const common::ObAddr &dst,
              obrpc::ObRpcResultCode &rcode)
  {
    return process_(result, dst, rcode);
  }
  void set_tenant_id(uint64_t tenant_id) {tenant_id_ = tenant_id;}
  void on_timeout()
  {
    int ret = OB_SUCCESS;
    const common::ObAddr &dst = ObGtsRpcProxy::AsyncCB<PC>::dst_;
    if (!is_inited_) {
      TRANS_LOG(WARN, "ObGtsRPCCB not inited");
    } else {
      if (EXECUTE_COUNT_PER_SEC(16)) {
        TRANS_LOG(WARN, "gts rpc timeout", K(dst), K_(tenant_id));
      }
      if (NULL == ts_mgr_) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "gts local cache mgr is NULL", K(ret));
      } else if (OB_FAIL(ts_mgr_->refresh_gts_location(tenant_id_))) {
        TRANS_LOG(WARN, "refresh gts location fail", K(ret));
      } else {
        // do nothing
      }
    }
  }
private:
  int process_(const obrpc::ObGtsRpcResult &result, const common::ObAddr &dst,
               obrpc::ObRpcResultCode &rcode)
  {
    int ret = OB_SUCCESS;
    int status = OB_SUCCESS;
    bool update = false;

    if (!is_inited_) {
      TRANS_LOG(WARN, "ObGtsRPCCB not inited");
      ret = OB_NOT_INIT;
    } else if (!is_valid_tenant_id(tenant_id_)) {
      TRANS_LOG(WARN, "tenant_id is invalid", K_(tenant_id), K(dst));
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_SUCCESS != rcode.rcode_) {
      status = rcode.rcode_;
      TRANS_LOG(WARN, "gts rpc error", K(rcode), K(dst));
      if (EXECUTE_COUNT_PER_SEC(16)) {
        TRANS_LOG(INFO, "get gts need refresh gts location", K(status), K(result));
      }
      if (NULL == ts_mgr_) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "gts local cache mgr is NULL", K(ret));
      } else if (OB_FAIL(ts_mgr_->refresh_gts_location(tenant_id_))) {
        TRANS_LOG(WARN, "refresh gts location fail", K(ret));
      } else {
        // do nothing
      }
    } else {
      status = result.get_status();
      if (OB_SUCCESS == status) {
        if (NULL == ts_mgr_) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "gts local cache mgr is NULL", KR(ret));
        } else if (NULL == ts_worker_) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "gts worker is NULL", KR(ret));
        } else if (OB_FAIL(ts_mgr_->update_gts(result.get_tenant_id(),
                                               result.get_srr(),
                                               result.get_gts_start(),
                                               transaction::TS_SOURCE_GTS,
                                               update))) {
        } else if (!update) {
          if (EXECUTE_COUNT_PER_SEC(16)) {
            TRANS_LOG(INFO, "gts local cache not updated", K(result));
          }
        } else {
          transaction::ObTsResponseTask *task = NULL;
          for (int64_t i = 0; OB_SUCC(ret) && i < transaction::ObGtsSource::TOTAL_GTS_QUEUE_COUNT; ++i) {
            if (NULL == (task = transaction::ObTsResponseTaskFactory::alloc())) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              TRANS_LOG(ERROR, "alloc memory failed", KR(ret), KP(task));
            } else {
              if (OB_FAIL(task->init(result.get_tenant_id(), i, ts_mgr_, transaction::TS_SOURCE_GTS))) {
                TRANS_LOG(WARN, "gts task init error", KR(ret), KP(task), K(i), K(result));
              } else if (OB_FAIL(ts_worker_->push_task(result.get_tenant_id(), task))) {
                TRANS_LOG(WARN, "push gts task failed", KR(ret), KP(task), K(result));
              } else {
                TRANS_LOG(DEBUG, "push gts task success", KP(task), K(result));
              }
              if (OB_SUCCESS != ret) {
                transaction::ObTsResponseTaskFactory::free(task);
                task = NULL;
              }
            }
          }
        }
      }
      TRANS_LOG(DEBUG, "gts request callback", KR(ret), K(result), K(rcode));
    }
    return ret;
  }
  bool is_inited_;
  uint64_t tenant_id_;
  transaction::ObTsMgr *ts_mgr_;
  transaction::ObTsWorker *ts_worker_;
};

} // obrpc

namespace transaction
{

class ObIGtsRequestRpc
{
public:
  ObIGtsRequestRpc() {}
  virtual ~ObIGtsRequestRpc() {}
  virtual int start() = 0;
  virtual int stop() = 0;
  virtual int wait() = 0;
  virtual void destroy() = 0;
public:
  virtual int post(const uint64_t tenant_id, const common::ObAddr &server,
      const ObGtsRequest &msg) = 0;
};

class ObGtsRequestRpc : public ObIGtsRequestRpc
{
public:
  ObGtsRequestRpc() : is_inited_(false), is_running_(false), rpc_proxy_(NULL), ts_mgr_(NULL) {}
  ~ObGtsRequestRpc() { destroy(); }
  int init(obrpc::ObGtsRpcProxy *rpc_proxy, const common::ObAddr &self,
           transaction::ObTsMgr *ts_mgr,
           transaction::ObTsWorker *ts_worker);
  int start();
  int stop();
  int wait();
  void destroy();
public:
  int post(const uint64_t tenant_id, const common::ObAddr &server, const ObGtsRequest &msg);
private:
  bool is_inited_;
  bool is_running_;
  obrpc::ObGtsRpcProxy *rpc_proxy_;
  obrpc::ObGtsRPCCB<obrpc::OB_GET_GTS_REQUEST> gts_request_cb_;
  common::ObAddr self_;
  transaction::ObTsMgr *ts_mgr_;
};

class ObIGtsResponseRpc
{
public:
  ObIGtsResponseRpc() {}
  virtual ~ObIGtsResponseRpc() {}
public:
  virtual int post(const uint64_t tenant_id, const common::ObAddr &server,
      const ObGtsErrResponse &msg) = 0;
};

class ObGtsResponseRpc : public ObIGtsResponseRpc
{
public:
  ObGtsResponseRpc() : is_inited_(false), is_running_(false) {}
  ~ObGtsResponseRpc() { destroy(); }
  int init(oceanbase::rpc::frame::ObReqTransport *req_transport, const common::ObAddr &self);
  int start();
  int stop();
  int wait();
  void destroy();
public:
  int post(const uint64_t tenant_id, const common::ObAddr &server,
      const ObGtsErrResponse &msg);
private:
  bool is_inited_;
  bool is_running_;
  obrpc::ObGtsRpcProxy rpc_proxy_;
  common::ObAddr self_;
};

} // transaction

} // oceanbase

#endif // OCEANBASE_TRANSACTION_OB_GTS_RPC_
