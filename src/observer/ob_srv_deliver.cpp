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

#include "util/easy_mod_stat.h"
#include "util/easy_inet.h"
#include "easy_define.h"
#include "lib/stat/ob_session_stat.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_session_handler.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "rpc/obmysql/packet/ompk_handshake_response.h"
#include "rpc/obmysql/ob_sql_nio_server.h"
#include "rpc/frame/ob_net_easy.h"
#include "share/ob_thread_mgr.h"
#include "observer/ob_rpc_processor_simple.h"
#include "rpc/obmysql/obsm_struct.h"
#include "observer/omt/ob_tenant.h"
#include "observer/omt/ob_multi_tenant.h"
#include "rpc/obmysql/ob_mysql_packet.h"

using namespace oceanbase::common;

using namespace oceanbase::rpc;
using namespace oceanbase::rpc::frame;
using namespace oceanbase::obrpc;
using namespace oceanbase::observer;
using namespace oceanbase::omt;
using namespace oceanbase::memtable;

namespace oceanbase
{
int extract_tenant_id(ObRequest &req, uint64_t &tenant_id)
{
  int ret = OB_SUCCESS;
  tenant_id = OB_INVALID_ID;
  obmysql::OMPKHandshakeResponse hsr =
      reinterpret_cast<const obmysql::OMPKHandshakeResponse &>(
          req.get_packet());
  if (OB_FAIL(hsr.decode())) {
    LOG_WARN("decode hsr fail", K(ret));
  } else {
    // resolve tenantname
    ObString in = hsr.get_username();
    const char *user_pos = in.ptr();
    const char *at_pos =
        in.find('@'); // use @ as seperator, e.g. xiaochu@tenant
    const char *tenant_pos = at_pos + 1;
    ObString tenant_name = ObString::make_empty_string();
    // sanity check
    if (NULL == at_pos) {
      tenant_id = OB_SYS_TENANT_ID; // default to sys tenant
      LOG_INFO("tenantname", K(tenant_name));
    } else {
      // Accept empty username.  Empty username is one of normal
      // usernames that we can create user with empty name.

      /* get tenant_name */
      if (at_pos - user_pos < 0) {
        ret = OB_ERR_USER_EMPTY;
        LOG_WARN("Must Provide user name to login", K(ret));
      } else {
        int64_t tenant_len = in.length() - (tenant_pos - user_pos);
        if (tenant_len > OB_MAX_TENANT_NAME_LENGTH || tenant_len <= 0) {
          ret = OB_ERR_INVALID_TENANT_NAME;
          LOG_WARN("Violate with tenant length limit", "max",
                   OB_MAX_TENANT_NAME_LENGTH, "actual", tenant_len, K(ret));
        }
        // extract
        if (OB_SUCC(ret)) {
          ObString tenantname(in.length() - (tenant_pos - user_pos),
                              tenant_pos);
          tenant_name = tenantname;
          LOG_DEBUG("get tenantname", K(tenant_name));

          /* get tenant_id */
          // OB_ASSERT(gctx_.schema_service_);
          if (OB_ISNULL(GCTX.schema_service_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("invalid schema service", K(ret),
                      K(GCTX.schema_service_));
          } else {
            share::schema::ObSchemaGetterGuard guard;
            if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                    OB_SYS_TENANT_ID, guard))) {
              LOG_WARN("get_schema_guard failed", K(ret));
            } else if (OB_FAIL(guard.get_tenant_id(tenant_name, tenant_id))) {
              LOG_WARN("get_tenant_id failed", K(ret), K(tenant_name));
            }
          }
        }
      }
    }
  }
  return ret;
}

int dispatch_req(ObRequest& req)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_ID;
  if (OB_FAIL(extract_tenant_id(req, tenant_id))) {
    LOG_WARN("extract tenant_id fail", K(ret), K(tenant_id), K(req));
    // handle all error by OB_TENANT_NOT_IN_SERVER
    ret = OB_TENANT_NOT_IN_SERVER;
  } else if (is_meta_tenant(tenant_id)) {
    // cannot login meta tenant
    ret = OB_TENANT_NOT_IN_SERVER;
    LOG_WARN("cannot login meta tenant", K(ret), K(tenant_id));
  } else if (is_sys_tenant(tenant_id) || is_user_tenant(tenant_id)) {
    MTL_SWITCH(tenant_id) {
      QueueThread *mysql_queue = MTL(QueueThread *);
      if (!mysql_queue->queue_.push(&req,
                                    10000)) { // MAX_QUEUE_LEN = 10000;
        ret = OB_QUEUE_OVERFLOW;
        EVENT_INC(MYSQL_DELIVER_FAIL);
        LOG_ERROR("deliver request fail", K(ret), K(tenant_id), K(req));
      }
      // print queue length per 10s
      if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
        LOG_INFO("mysql login queue", K(tenant_id),
                  K(mysql_queue->queue_.size()));
      }

      // if (0 != MTL(obmysql::ObSqlNioServer *)
      //              ->get_nio()
      //              ->regist_sess(req.get_server_handle_context())) {
      //   ret = OB_ERR_UNEXPECTED;
      //   LOG_ERROR("regist sess for tenant fail", K(ret), K(tenant_id), K(req));
      // }
    } else {
      LOG_WARN("cannot switch to tenant", K(ret), K(tenant_id));
    }
  }
  return ret;
}

} // namespace oceanbase

int64_t get_easy_per_src_memory_limit()
{
  return GCONF.__easy_memory_limit;
}

int check_easy_memory_limit(ObRequest &req)
{
  int ret = OB_SUCCESS;
  easy_mod_stat_t *stat = NULL;

  if (req.get_nio_protocol() == ObRequest::TRANSPORT_PROTO_POC
      || req.get_nio_protocol() == ObRequest::TRANSPORT_PROTO_RDMA) {
    // Todo:
    return ret;
  }
  easy_connection_t *c = req.get_ez_req()->ms->c;
  if (OB_UNLIKELY(NULL == (stat = c->pool->mod_stat))) {
    // it's auth request or bug
  } else {
    const int64_t easy_server_memory_limit = get_easy_per_src_memory_limit();
    if (OB_UNLIKELY(0 == easy_server_memory_limit)) {
      // do-nothing
    } else if (stat->size > easy_server_memory_limit) {
      ret = OB_EXCEED_MEM_LIMIT;
      if (REACH_TIME_INTERVAL(1000000)) {
        if (ObRequest::OB_RPC == req.get_type()) {
          char buf[64];
          easy_inet_addr_to_str(&c->addr, buf, 32);
          LOG_WARN("too many pending request received", "peer", buf, "size", stat->size,
                   "limit", easy_server_memory_limit);
        } else if (ObRequest::OB_MYSQL == req.get_type()) {
          void *sess = SQL_REQ_OP.get_sql_session(&req);
          if (NULL != sess) {
            ObSMConnection *conn = static_cast<ObSMConnection *>(sess);
            LOG_WARN("too many pending request received", "tenant", conn->tenant_id_, "size", stat->size,
                     "limit", easy_server_memory_limit);
          }
        }
      }
    }
  }
  return ret;
}

int ObSrvDeliver::get_mysql_login_thread_count_to_set(int cfg_cnt)
{
  int set_cnt = 0;
  if (0 < cfg_cnt) {
    set_cnt = cfg_cnt;
  } else {
    if (!lib::is_mini_mode()) {
      set_cnt = observer::ObSrvDeliver::MYSQL_TASK_THREAD_CNT;
    } else {
      set_cnt = observer::ObSrvDeliver::MINI_MODE_MYSQL_TASK_THREAD_CNT;
    }
  }
  return set_cnt;
}

int ObSrvDeliver::set_mysql_login_thread_count(int cnt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(mysql_queue_->set_thread_count(cnt))) {
    SERVER_LOG(WARN, "set thread count for mysql login failed", K(ret));
  } else {
    LOG_INFO("set mysql login thread count success", K(cnt));
  }
  return ret;
}

bool is_high_prio_rpc_req(const ObRequest &req)
{
  bool bool_ret = false;
  easy_request_t *r = req.get_ez_req();
  easy_io_t *eio = NULL;
  if (OB_ISNULL(r)
      || OB_ISNULL(r->ms)
      || OB_ISNULL(r->ms->c)
      || OB_ISNULL(r->ms->c->ioth)) {
  } else {
    eio = r->ms->c->ioth->eio;
    if ( ObNetEasy::HIGH_PRI_RPC_EIO_MAGIC == eio->magic) {
      bool_ret = true;
    }
  }
  return bool_ret;
}

ObSrvDeliver::ObSrvDeliver(ObiReqQHandler &qhandler,
                           ObRpcSessionHandler &session_handler,
                           ObGlobalContext &gctx)
    : ObReqQDeliver(qhandler),
      is_inited_(false),
      stop_(true),
      host_(),
      lease_queue_(NULL),
      ddl_queue_(NULL),
      ddl_parallel_queue_(NULL),
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
    mysql_queue_->stop();
    mysql_queue_->wait();
  }
  if (NULL != diagnose_queue_) {
    // stop sql service first
    diagnose_queue_->stop();
    diagnose_queue_->wait();
  }
  if (NULL != lease_queue_) {
    lease_queue_->stop();
    lease_queue_->wait();
  }
  if (NULL != ddl_queue_) {
    ddl_queue_->stop();
    ddl_queue_->wait();
  }
  if (NULL != ddl_parallel_queue_) {
    TG_STOP(lib::TGDefIDs::DDLPQueueTh);
    TG_WAIT(lib::TGDefIDs::DDLPQueueTh);
  }
}

int ObSrvDeliver::create_queue_thread(int tg_id, const char *thread_name, QueueThread *&qthread)
{
  int ret = OB_SUCCESS;
  qthread = OB_NEW(QueueThread, ObModIds::OB_RPC, thread_name);
  if (OB_ISNULL(qthread)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    qthread->queue_.set_qhandler(&qhandler_);
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(qthread)) {
    qthread->tg_id_ = tg_id;
    ret = TG_SET_RUNNABLE_AND_START(tg_id, qthread->thread_);
  }
  return ret;
}

int ObSrvDeliver::init_queue_threads()
{
  int ret = OB_SUCCESS;

  // TODO: fufeng, make it configurable
  if (OB_FAIL(create_queue_thread(lib::TGDefIDs::LeaseQueueTh, "LeaseQueueTh", lease_queue_))) {
  } else if (OB_FAIL(create_queue_thread(lib::TGDefIDs::DDLQueueTh, "DDLQueueTh", ddl_queue_))) {
  } else if (OB_FAIL(create_queue_thread(lib::TGDefIDs::DDLPQueueTh, "DDLPQueueTh", ddl_parallel_queue_))) {
  } else if (OB_FAIL(create_queue_thread(lib::TGDefIDs::MysqlQueueTh,
                                         "MysqlQueueTh", mysql_queue_))) {
  } else if (OB_FAIL(create_queue_thread(lib::TGDefIDs::DiagnoseQueueTh,
                                         "DiagnoseQueueTh", diagnose_queue_))) {
  } else {
    LOG_INFO("queue thread create successfully", K_(host));
  }

  return ret;
}

int ObSrvDeliver::deliver_rpc_request(ObRequest &req)
{
  int ret = OB_SUCCESS;
  ObReqQueue *queue = NULL;
  ObTenant *tenant = NULL;
  const obrpc::ObRpcPacket &pkt
      = reinterpret_cast<const obrpc::ObRpcPacket &>(req.get_packet());
  req.set_group_id(pkt.get_group_id());
  const int64_t now = ObTimeUtility::current_time();

  const bool need_update_stat = !req.is_retry_on_lock();
  const bool is_stream = pkt.is_stream();

  ObTenantStatEstGuard guard(pkt.get_tenant_id());
  if (need_update_stat) {
    EVENT_INC(RPC_PACKET_IN);
    EVENT_ADD(RPC_PACKET_IN_BYTES,
              pkt.get_encoded_size() + OB_NET_HEADER_LENGTH);
    EVENT_ADD(RPC_NET_DELAY,
              req.get_receive_timestamp() - req.get_send_timestamp());
    EVENT_ADD(RPC_NET_FRAME_DELAY,
              now - req.get_receive_timestamp());
  }

  if (stop_
      || SS_STOPPING == GCTX.status_
      || SS_STOPPED == GCTX.status_) {
    ret = OB_SERVER_IS_STOPPING;
    LOG_WARN("receive request when server is stopping",
             K(req),
             K(ret));
  }

  req.set_trace_point(ObRequest::OB_EASY_REQUEST_RPC_DELIVER);
  if (!OB_SUCC(ret)) {

  } else if (!is_high_prio_rpc_req(req) && OB_FAIL(check_easy_memory_limit(req))) {
  } else if (is_stream) {
    if (!session_handler_.wakeup_next_thread(req)) {
      ret = OB_SESSION_NOT_FOUND;
      LOG_WARN("receive stream rpc packet but session not found",
               K(pkt), K(req));
    }
  } else if (OB_RENEW_LEASE == pkt.get_pcode()) {
    queue = &lease_queue_->queue_;
  } else if (10 == pkt.get_priority()) {
    // for new parallel truncate table rpc
    if (OB_TRUNCATE_TABLE_V2 == pkt.get_pcode()) {
      queue = &ddl_parallel_queue_->queue_;
    } else {
      queue = &ddl_queue_->queue_;
    }
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
    RpcStatPiece piece;
    piece.is_server_ = true;
    piece.is_deliver_ = true;
    RPC_STAT(pkt.get_pcode(), tenant->id(), piece);
    if (OB_FAIL(tenant->recv_request(req))) {
      if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
        LOG_WARN("tenant receive request fail", K(*tenant), K(req));
      }
    }
  } else if (!is_stream) {
    LOG_WARN("not stream packet, should not reach here.");
    ret = OB_ERR_UNEXPECTED;
  }

  // maybe tenant hasn't synced right now.
  if (OB_TENANT_NOT_IN_SERVER == ret && SS_INIT == GCTX.status_) {
    ret = OB_SERVER_IS_INIT;
    LOG_WARN("server is initializing", K(pkt), K(ret));
  }

  if (!OB_SUCC(ret)) {
    EVENT_INC(RPC_DELIVER_FAIL);
    if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
      SERVER_LOG(WARN, "can't deliver request", K(req), K(ret));
    }
    on_translate_fail(&req, ret);
  }

  return ret;
}

int ObSrvDeliver::deliver_mysql_request(ObRequest &req)
{
  int ret = OB_SUCCESS;
  ObTenant *tenant = NULL;
  void *sess = SQL_REQ_OP.get_sql_session(&req);
  ObSMConnection *conn = NULL;
  if (NULL != sess) {
    conn = static_cast<ObSMConnection *>(sess);
    tenant = conn->tenant_;
    if (static_cast<int64_t>(share::OBCG_DEFAULT) == req.get_group_id()) {
      int64_t valid_sql_req_level = req.get_sql_request_level() ? req.get_sql_request_level() : conn->sql_req_level_;
      switch (valid_sql_req_level)
      {
      case 1:
        req.set_group_id(share::OBCG_ID_SQL_REQ_LEVEL1);
        break;
      case 2:
        req.set_group_id(share::OBCG_ID_SQL_REQ_LEVEL2);
        break;
      case 3:
        req.set_group_id(share::OBCG_ID_SQL_REQ_LEVEL3);
        break;
      default:
        req.set_group_id(conn->group_id_);
        break;
      }
    } else {
      req.set_group_id(conn->group_id_);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session from request is NULL", K(req), K(ret));
  }

  req.set_trace_point(ObRequest::OB_EASY_REQUEST_MYSQL_DELIVER);
  if (OB_FAIL(ret)) {
  } else if (rpc::ObRequest::TRANSPORT_PROTO_EASY == req.get_nio_protocol()) {
    if (OB_FAIL(check_easy_memory_limit(req))) {
      LOG_ERROR("check_easy_memory_limit failed", K(ret));
    }
  } else if (rpc::ObRequest::TRANSPORT_PROTO_POC == req.get_nio_protocol()) {
    /* TODO check memory limit for sql nio */
  }

  if (OB_SUCC(ret)) {
    const bool need_update_stat = (ObRequest::OB_MYSQL == req.get_type()) && !req.is_retry_on_lock();
    // auth request
    if (NULL == tenant) {
      const obmysql::ObMySQLRawPacket &pkt
          = reinterpret_cast<const obmysql::ObMySQLRawPacket &>(req.get_packet());
      ObTenantStatEstGuard guard(OB_SERVER_TENANT_ID);
      if (need_update_stat) {
        EVENT_INC(MYSQL_PACKET_IN);
        EVENT_ADD(MYSQL_PACKET_IN_BYTES, pkt.get_clen() + OB_MYSQL_HEADER_LENGTH);
      }
      if (OB_UNLIKELY((ObRequest::TRANSPORT_PROTO_RDMA != req.get_nio_protocol()) && NULL != diagnose_queue_ && SQL_REQ_OP.get_peer(&req).get_port() <= 0)) {
        LOG_INFO("receive login request from unix domain socket");
        if (!diagnose_queue_->queue_.push(&req, MAX_QUEUE_LEN)) {
          ret = OB_QUEUE_OVERFLOW;
          EVENT_INC(MYSQL_DELIVER_FAIL);
          LOG_ERROR("deliver request fail", K(req));
        }
      } else if (OB_NOT_NULL(mysql_queue_)) {
        if (GCONF._enable_new_sql_nio && GCONF._enable_tenant_sql_net_thread &&
            OB_SUCC(dispatch_req(req))) {
          // do nothing
        } else {
          if (OB_TENANT_NOT_IN_SERVER == ret) {
            LOG_WARN("cannot dispatch success", K(ret), K(req));
            // set OB_SUCCESS to go normal procedure
            ret = OB_SUCCESS;
          }
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
      }
    } else {
      const obmysql::ObMySQLRawPacket &pkt
          = reinterpret_cast<const obmysql::ObMySQLRawPacket &>(req.get_packet());
      ObTenantStatEstGuard guard(tenant->id());
      if (need_update_stat) {
        EVENT_INC(MYSQL_PACKET_IN);
        EVENT_ADD(MYSQL_PACKET_IN_BYTES, pkt.get_clen() + OB_MYSQL_HEADER_LENGTH);
      }
      // The tenant check has been done in the recv_request method. For performance considerations, the check here is removed;
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
  rpc::ObRequest* req = CONTAINER_OF((const ObLockWaitNode *)p, rpc::ObRequest, lock_wait_node_);
  return deliver(*req);
}

int ObSrvDeliver::deliver(rpc::ObRequest &req)
{
  int ret = OB_SUCCESS;
  LOG_DEBUG("deliver ob_request:", K(req));
  if (ObRequest::OB_RPC == req.get_type()) {
    if (OB_FAIL(deliver_rpc_request(req))) {
      if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) {
        LOG_WARN("deliver rpc request fail", KP(&req), K(ret));
      }
    }
    //LOG_INFO("yzfdebug deliver rpc", K(ret), "pkt", req.get_packet());
  } else if (ObRequest::OB_MYSQL == req.get_type()) {
    if (OB_FAIL(deliver_mysql_request(req))) {
      LOG_WARN("deliver mysql request fail", K(req), K(ret));
      //If it is a lock conflict repost request, if the deliver fails, the link is broken,
      //Normal requests will break the link at the upper level
      if (req.is_retry_on_lock()) {
        on_translate_fail(&req, ret);
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ignore unknown request", K(req), K(ret));
  }

  return ret;
}
