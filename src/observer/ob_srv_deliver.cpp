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

#define USING_LOG_PREFIX SERVER

#include "observer/ob_srv_deliver.h"

#include "lib/stat/ob_session_stat.h"
#include "rpc/ob_request.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "share/ob_thread_mgr.h"
#include "observer/ob_rpc_processor_simple.h"
#include "observer/mysql/obsm_struct.h"
#include "observer/omt/ob_tenant.h"
#include "observer/omt/ob_multi_tenant.h"

using namespace oceanbase::common;
using namespace oceanbase::rpc;
using namespace oceanbase::rpc::frame;
using namespace oceanbase::obrpc;
using namespace oceanbase::observer;
using namespace oceanbase::omt;
using namespace oceanbase::memtable;

ObSrvDeliver::ObSrvDeliver(ObiReqQHandler& qhandler, ObRpcSessionHandler& session_handler, ObGlobalContext& gctx)
    : ObReqQDeliver(qhandler),
      is_inited_(false),
      stop_(true),
      host_(),
      lease_queue_(NULL),
      ddl_queue_(NULL),
      mysql_queue_(NULL),
      diagnose_queue_(NULL),
      session_handler_(session_handler),
      gctx_(gctx)
{}

int ObSrvDeliver::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init_queue_threads())) {
    SERVER_LOG(ERROR, "init queue threads fail", K(ret));
  } else {
    SERVER_LOG(INFO, "init ObSrvDeliver done");
    is_inited_ = true;
    stop_ = false;
  }
  return ret;
}

void ObSrvDeliver::stop()
{
  stop_ = true;
  if (NULL != mysql_queue_) {
    // stop sql service first
    TG_STOP(lib::TGDefIDs::MysqlQueueTh);
    TG_WAIT(lib::TGDefIDs::MysqlQueueTh);
  }
  if (NULL != diagnose_queue_) {
    // stop sql service first
    TG_STOP(lib::TGDefIDs::DiagnoseQueueTh);
    TG_WAIT(lib::TGDefIDs::DiagnoseQueueTh);
  }
  if (NULL != lease_queue_) {
    TG_STOP(lib::TGDefIDs::LeaseQueueTh);
    TG_WAIT(lib::TGDefIDs::LeaseQueueTh);
  }
  if (NULL != ddl_queue_) {
    TG_STOP(lib::TGDefIDs::DDLQueueTh);
    TG_WAIT(lib::TGDefIDs::DDLQueueTh);
  }
}

int ObSrvDeliver::create_queue_thread(int tg_id, const char* thread_name, QueueThread*& qthread)
{
  int ret = OB_SUCCESS;
  qthread = OB_NEW(QueueThread, ObModIds::OB_RPC, thread_name);
  if (OB_ISNULL(qthread)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    qthread->queue_.set_qhandler(&qhandler_);
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(qthread)) {
    ret = TG_SET_RUNNABLE_AND_START(tg_id, qthread->thread_);
  }
  return ret;
}

int ObSrvDeliver::init_queue_threads()
{
  int ret = OB_SUCCESS;

  // TODO: , make it configurable
  if (OB_FAIL(create_queue_thread(lib::TGDefIDs::LeaseQueueTh, "LeaseQueueTh", lease_queue_))) {
  } else if (OB_FAIL(create_queue_thread(lib::TGDefIDs::DDLQueueTh, "DDLQueueTh", ddl_queue_))) {
  } else if (OB_FAIL(create_queue_thread(lib::TGDefIDs::MysqlQueueTh, "MysqlQueueTh", mysql_queue_))) {
  } else if (OB_FAIL(create_queue_thread(lib::TGDefIDs::DiagnoseQueueTh, "DiagnoseQueueTh", diagnose_queue_))) {
  } else {
    LOG_INFO("queue thread create successfully", K_(host));
  }

  return ret;
}

int ObSrvDeliver::deliver_rpc_request(ObRequest& req)
{
  int ret = OB_SUCCESS;
  ObReqQueue* queue = NULL;
  ObTenant* tenant = NULL;
  const obrpc::ObRpcPacket& pkt = reinterpret_cast<const obrpc::ObRpcPacket&>(req.get_packet());
  req.set_group_id(pkt.get_group_id());
  const int64_t now = ObTimeUtility::current_time();

  const bool need_update_stat = !req.is_retry_on_lock();

  ObTenantStatEstGuard guard(pkt.get_tenant_id());
  if (need_update_stat) {
    EVENT_INC(RPC_PACKET_IN);
    EVENT_ADD(RPC_PACKET_IN_BYTES, pkt.get_encoded_size() + OB_NET_HEADER_LENGTH);
    EVENT_ADD(RPC_NET_DELAY, req.get_receive_timestamp() - req.get_send_timestamp());
    EVENT_ADD(RPC_NET_FRAME_DELAY, now - req.get_receive_timestamp());
  }

  if (stop_ || SS_STOPPING == GCTX.status_ || SS_STOPPED == GCTX.status_) {
    ret = OB_SERVER_IS_STOPPING;
    LOG_WARN("receive request when server is stopping", K(req), K(ret));
  }

  if (!OB_SUCC(ret)) {

  } else if (pkt.is_stream()) {
    if (!session_handler_.wakeup_next_thread(req)) {
      ret = OB_SESSION_NOT_FOUND;
      LOG_WARN("receive stream rpc packet but session not found", K(pkt), K(req));
    }
  } else if (OB_RENEW_LEASE == pkt.get_pcode()) {
    queue = &lease_queue_->queue_;
  } else if (10 == pkt.get_priority()) {
    // DDL rpc
    queue = &ddl_queue_->queue_;
  } else {
    const uint64_t tenant_id = pkt.get_tenant_id();
    const uint64_t priv_tenant_id = pkt.get_priv_tenant_id();
    if (NULL != gctx_.omt_) {
      tenant = NULL;
      if (OB_FAIL(gctx_.omt_->get_tenant(tenant_id, tenant)) || NULL == tenant) {
        if (OB_FAIL(gctx_.omt_->get_tenant(priv_tenant_id, tenant)) || NULL == tenant) {
          ret = OB_TENANT_NOT_IN_SERVER;
        }
      }
    } else {
      ret = OB_NOT_INIT;
      LOG_ERROR("gctx_.omt_ is NULL", K(gctx_));
    }
  }

  if (!OB_SUCC(ret)) {

  } else if (NULL != queue) {
    SERVER_LOG(DEBUG, "deliver packet", K(queue));
    if (!queue->push(&req, MAX_QUEUE_LEN)) {
      ret = OB_QUEUE_OVERFLOW;
    }
  } else if (NULL != tenant) {
    SERVER_LOG(DEBUG, "deliver tenant packet", K(queue), K(tenant->id()));
    if (OB_FAIL(tenant->recv_request(req))) {
      if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
        LOG_WARN("tenant receive request fail", K(*tenant), K(req));
      }
    }
  } else if (!pkt.is_stream()) {
    LOG_WARN("not stream packet, should not reach here.");
    ret = OB_ERR_UNEXPECTED;
  }

  // maybe tenant hasn't synced right now.
  if (OB_TENANT_NOT_IN_SERVER == ret && SS_INIT == GCTX.status_) {
    ret = OB_SERVER_IS_INIT;
    LOG_WARN("server is initializing", K(pkt), K(ret));
  }

  if (!OB_SUCC(ret)) {
    ObErrorP p(ret);
    char buf[2048] = {};  // just a have rpc result code, 2048 is enough
    ObDataBuffer dbuf(buf, sizeof(buf));
    p.set_ob_request(req);
    p.set_io_thread_mark();
    p.run();

    EVENT_INC(RPC_DELIVER_FAIL);
    if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      SERVER_LOG(WARN, "can't deliver request", K(req), K(ret));
    }
  }

  return ret;
}

int ObSrvDeliver::deliver_mysql_request(ObRequest& req)
{
  int ret = OB_SUCCESS;
  ObTenant* tenant = NULL;

  void* sess = req.get_session();
  ObSMConnection* conn = NULL;
  if (NULL != sess) {
    conn = static_cast<ObSMConnection*>(sess);
    tenant = conn->tenant_;
    req.set_group_id(conn->group_id_);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session from request is NULL", K(req), K(ret));
  }

  if (OB_SUCC(ret)) {
    const bool need_update_stat = !req.is_retry_on_lock();
    // auth request
    if (NULL == tenant) {
      const obmysql::ObMySQLRawPacket& pkt = reinterpret_cast<const obmysql::ObMySQLRawPacket&>(req.get_packet());
      ObTenantStatEstGuard guard(OB_SERVER_TENANT_ID);
      if (need_update_stat) {
        EVENT_INC(MYSQL_PACKET_IN);
        EVENT_ADD(MYSQL_PACKET_IN_BYTES, pkt.get_clen() + OB_MYSQL_HEADER_LENGTH);
      }
      easy_request_t* ez_req = req.get_request();
      if (OB_UNLIKELY(
              NULL != diagnose_queue_ && NULL != ez_req && NULL != ez_req->ms && ez_req->ms->c->addr.port <= 0)) {
        LOG_INFO("receive login request from unix domain socket");
        if (!diagnose_queue_->queue_.push(&req, MAX_QUEUE_LEN)) {
          ret = OB_QUEUE_OVERFLOW;
          EVENT_INC(MYSQL_DELIVER_FAIL);
          LOG_ERROR("deliver request fail", K(req));
        }
      } else if (OB_NOT_NULL(mysql_queue_)) {
        if (!mysql_queue_->queue_.push(&req, MAX_QUEUE_LEN)) {
          ret = OB_QUEUE_OVERFLOW;
          EVENT_INC(MYSQL_DELIVER_FAIL);
          LOG_ERROR("deliver request fail", K(req));
        }
        // print queue length per 10s
        if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
          LOG_INFO("mysql login queue", K(mysql_queue_->queue_.size()));
        }
      }
    } else {
      const obmysql::ObMySQLRawPacket& pkt = reinterpret_cast<const obmysql::ObMySQLRawPacket&>(req.get_packet());
      ObTenantStatEstGuard guard(tenant->id());
      if (need_update_stat) {
        EVENT_INC(MYSQL_PACKET_IN);
        EVENT_ADD(MYSQL_PACKET_IN_BYTES, pkt.get_clen() + OB_MYSQL_HEADER_LENGTH);
      }
      // The tenant check has been done in the recv_request method. For performance considerations, the check here is
      // removed;
      /*
      const int64_t tenant_id = conn->tenant_id_;
      if (NULL == gctx_.omt_) {
        ret = OB_SERVER_IS_INIT;
        LOG_ERROR("gctx is not valid", K(ret));
      } else if (!gctx_.omt_->has_tenant(tenant_id)) {
        ret = OB_TENANT_NOT_IN_SERVER;
        LOG_WARN(
            "receive mysql packet with tenant not in this server",
            K(tenant_id), K(ret));
      }*/

      if (OB_SUCC(ret) && OB_FAIL(tenant->recv_request(req))) {
        EVENT_INC(MYSQL_DELIVER_FAIL);
        if (OB_IN_STOP_STATE == ret) {
          LOG_WARN("deliver request fail", K(req), K(*tenant));
        } else {
          LOG_ERROR("deliver request fail", K(req), K(*tenant));
        }
      }
    }
  }

  return ret;
}

int ObSrvDeliver::repost(void* p)
{
  rpc::ObRequest* req = CONTAINER_OF((const ObLockWaitNode*)p, rpc::ObRequest, lock_wait_node_);
  return deliver(*req);
}

int ObSrvDeliver::deliver(rpc::ObRequest& req)
{
  int ret = OB_SUCCESS;

  if (ObRequest::OB_RPC == req.get_type()) {
    if (OB_FAIL(deliver_rpc_request(req))) {
      if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
        LOG_WARN("deliver rpc request fail", K(req), K(ret));
      }
    }
    // LOG_INFO("yzfdebug deliver rpc", K(ret), "pkt", req.get_packet());
  } else if (ObRequest::OB_MYSQL == req.get_type()) {
    if (OB_FAIL(deliver_mysql_request(req))) {
      LOG_WARN("deliver mysql request fail", K(req), K(ret));
      // If it is a lock conflict repost request, if the deliver fails, the link is broken,
      // Normal requests will break the link at the upper level
      if (req.is_retry_on_lock()) {
        easy_request_t* ez_req = req.get_request();
        if (OB_ISNULL(ez_req) || OB_ISNULL(ez_req->ms)) {
          // do nothing
        } else {
          easy_connection_destroy_dispatch(ez_req->ms->c);
        }
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ignore unknown request", K(req), K(ret));
  }

  return ret;
}
