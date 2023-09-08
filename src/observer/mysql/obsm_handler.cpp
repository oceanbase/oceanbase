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

#define USING_LOG_PREFIX RPC_OBMYSQL

#include "observer/mysql/obsm_handler.h"
#include "lib/oblog/ob_log.h"
#include "lib/stat/ob_session_stat.h"
#include "lib/random/ob_mysql_random.h"
#include "rpc/obmysql/ob_mysql_handler.h"
#include "rpc/obmysql/packet/ompk_handshake.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "sql/session/ob_sql_session_info.h"
#include "rpc/obmysql/obsm_struct.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_srv_task.h"
#include "observer/omt/ob_tenant.h"

namespace oceanbase
{
using namespace rpc;
using namespace common;
using namespace name;
namespace obmysql {
extern uint64_t ob_calculate_tls_version_option(const ObString &tls_min_version);
}
namespace observer
{
bool enable_proto_dia()
{
  return GCONF._enable_protocol_diagnose;
}

ObSMHandler::ObSMHandler(rpc::frame::ObReqDeliver &deliver, ObGlobalContext &gctx)
    : ObMySQLHandler(deliver), gctx_(gctx)
{
  EZ_ADD_CB(on_close);
}

ObSMHandler::~ObSMHandler()
{
}

int ObSMHandler::create_scramble_string(char *scramble_buf, const int64_t buf_len,
    common::ObMysqlRandom &thread_rand)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!thread_rand.is_inited())) {
    if (OB_UNLIKELY(!gctx_.scramble_rand_->is_inited())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("global_rand has not inited, it should not happened", K(ret));
    } else {
      // Concurrent access by multiple threads maybe happened here, but we do not care
      const uint64_t tmp_seed = gctx_.scramble_rand_->get_uint64();
      thread_rand.init(tmp_seed + reinterpret_cast<uint64_t>(&thread_rand),
                       tmp_seed + static_cast<uint64_t>(ObTimeUtility::current_time()));
      LOG_INFO("init thread_rand succ", K(ret));
    }
  }

  if (FAILEDx(thread_rand.create_random_string(scramble_buf, buf_len))) {
    LOG_ERROR("fail to create_random_string", K(scramble_buf), K(buf_len), K(ret));
  }
  return ret;
}

int ObSMHandler::on_connect(easy_connection_t *c)
{
  RLOCAL(common::ObMysqlRandom, thread_scramble_rand);

  int eret = EASY_OK;
  int crt_id_ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSMConnection *conn = NULL;
  if (OB_ISNULL(c) || OB_ISNULL(gctx_.session_mgr_) || OB_ISNULL(gctx_.scramble_rand_)) {
    eret = EASY_ERROR;
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "easy_connection_t or session_mgr is null", K(eret), K(c), K(gctx_.session_mgr_));
  } else {
    uint32_t sessid = 0;
    crt_id_ret =  gctx_.session_mgr_->create_sessid(sessid);
    if (OB_UNLIKELY(OB_SUCCESS != crt_id_ret && OB_ERR_CON_COUNT_ERROR != crt_id_ret)) {
      eret = EASY_ERROR;
      LOG_WARN_RET(OB_ERROR, "fail to create sessid", K(crt_id_ret), K(sessid));
    } else {
      // send handshake
      obmysql::OMPKHandshake hsp;
      hsp.set_thread_id(sessid);
      const bool suppot_ssl = GCONF.ssl_client_authentication;
      hsp.set_ssl_cap(suppot_ssl);
      c->ssl_sm_ = (suppot_ssl ? SSM_BEFORE_FIRST_PKT : SSM_NONE);
      const int64_t BUF_LEN = sizeof(conn->scramble_buf_);
      if (OB_ISNULL(conn = reinterpret_cast<ObSMConnection*>(easy_alloc(c->pool, sizeof(ObSMConnection))))) {
        eret = EASY_ERROR;
        LOG_ERROR_RET(OB_ALLOCATE_MEMORY_FAILED, "easy alloc memory failed", K(sizeof(ObSMConnection)), K(eret));
      } else {
        conn = new (conn) ObSMConnection;
        c->user_data = conn;
        conn->sessid_ = sessid;
        conn->ret_ = crt_id_ret;
        if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = create_scramble_string(conn->scramble_buf_, BUF_LEN, thread_scramble_rand)))) {
          eret = EASY_ERROR;
          LOG_WARN_RET(tmp_ret, "create scramble string failed", K(tmp_ret), K(eret));
        } else if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = hsp.set_scramble(conn->scramble_buf_, BUF_LEN)))) {
          eret = EASY_ERROR;
          LOG_WARN_RET(tmp_ret, "set scramble failed", K(tmp_ret), K(eret));
        } else if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = send_handshake(c->fd, hsp)))) {
          eret = EASY_ERROR;
          LOG_WARN_RET(tmp_ret, "send handshake packet failed", K(tmp_ret), K(eret));
        } else {
          uint64_t tls_version_option = obmysql::ob_calculate_tls_version_option(
                                         GCONF.sql_protocol_min_tls_version.str());
          c->tls_version_option = tls_version_option;
          LOG_INFO("new mysql sessid created", K(easy_connection_str(c)), K(sessid), K(crt_id_ret));
        }
      }
    }
  }

  //如果当前function发生错误，应该在当前function中进行mark_sessid_unused
  if (EASY_OK == eret && OB_SUCCESS == crt_id_ret) {
    conn->is_need_clear_sessid_ = true;
  }
  return eret;
}

int ObSMHandler::on_disconnect(easy_connection_t *c)
{
  //
  int eret = EASY_OK;
  int tmp_ret = OB_SUCCESS;
  ObSMConnection *conn = NULL;
  if (OB_ISNULL(c) || OB_ISNULL(gctx_.session_mgr_)) {
    eret = EASY_ERROR;
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "easy_connection_t or gctx_.session_mgr_ is null", K(eret), K(c), K(gctx_.session_mgr_));
  } else if (OB_ISNULL(c->user_data)) {
    eret = EASY_ERROR;
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "connection user data is NULL", K(eret));
  } else if (OB_ISNULL(conn = reinterpret_cast<ObSMConnection*>(c->user_data))) {
    eret = EASY_ERROR;
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "conn is null", K(eret));
  } else {
    //set session shadow
    if (conn->is_sess_alloc_
        && !conn->is_sess_free_
        && ObSMConnection::INITIAL_SESSID != conn->sessid_) {
      sql::ObSQLSessionInfo *sess_info = NULL;
      sql::ObSessionGetterGuard guard(*gctx_.session_mgr_, conn->sessid_);
      if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = guard.get_session(sess_info)))) {
        LOG_WARN_RET(tmp_ret, "fail to get session", K(tmp_ret), K(conn->sessid_),
                 "proxy_sessid", conn->proxy_sessid_);
      } else if (OB_ISNULL(sess_info)) {
        tmp_ret = OB_ERR_UNEXPECTED;
        LOG_WARN_RET(tmp_ret, "session info is NULL", K(tmp_ret), K(conn->sessid_),
                 "proxy_sessid", conn->proxy_sessid_);
      } else {
        sess_info->set_session_state(sql::SESSION_KILLED);
        sess_info->set_shadow(true);
      }
    }
    LOG_INFO("kill and revert session", K(conn->sessid_),
             "proxy_sessid", conn->proxy_sessid_, "server_id", GCTX.server_id_,
             K(tmp_ret), K(eret));
  }
  return eret;
}

int ObSMHandler::on_close(easy_connection_t *c)
{
  int eret = EASY_OK; // EASY_OK will be returned finally
  int ret = OB_SUCCESS;
  bool is_need_clear = false;
  ObSMConnection *conn = NULL;
  sql::ObDisconnectState disconnect_state = sql::ObDisconnectState::DIS_INIT;
  ObCurTraceId::TraceId trace_id;
  if (OB_ISNULL(c) || OB_ISNULL(gctx_.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("easy_connection_t or session_mgr is null",
              K(ret),
              K(c),
              K(gctx_.session_mgr_));
  } else if (OB_ISNULL(conn = reinterpret_cast<ObSMConnection*>(c->user_data))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("conn is NULL", K(ret));
  } else {
    //free session
    if (conn->is_sess_alloc_) {
      if (!conn->is_sess_free_) {
        {
          int tmp_ret = OB_SUCCESS;
          sql::ObSQLSessionInfo *sess_info = NULL;
          sql::ObSessionGetterGuard guard(*gctx_.session_mgr_, conn->sessid_);
          if (OB_UNLIKELY(OB_SUCCESS != (tmp_ret = guard.get_session(sess_info)))) {
            LOG_WARN_RET(tmp_ret, "fail to get session", K(tmp_ret), K(conn->sessid_),
                    "proxy_sessid", conn->proxy_sessid_);
          } else if (OB_ISNULL(sess_info)) {
            tmp_ret = OB_ERR_UNEXPECTED;
            LOG_WARN_RET(tmp_ret, "session info is NULL", K(tmp_ret), K(conn->sessid_),
                    "proxy_sessid", conn->proxy_sessid_);
          } else {
            disconnect_state = sess_info->get_disconnect_state();
            trace_id = sess_info->get_current_trace_id();
          }
        }
        sql::ObFreeSessionCtx ctx;
        ctx.tenant_id_ = conn->tenant_id_;
        ctx.sessid_ = conn->sessid_;
        ctx.proxy_sessid_ = conn->proxy_sessid_;
        ctx.has_inc_active_num_ = conn->has_inc_active_num_;

        //free session in task
        ObSrvTask *task = OB_NEW(ObDisconnectTask,
                                ObModIds::OB_RPC,
                                ctx);
        if (OB_UNLIKELY(NULL == task)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else if (OB_UNLIKELY(NULL == conn->tenant_)) {
          ret = OB_TENANT_NOT_EXIST;
        } else if (OB_FAIL(conn->tenant_->recv_request(*task))) {
          LOG_WARN("push disconnect task fail", K(conn->sessid_),
                  "proxy_sessid", conn->proxy_sessid_, K(ret));
          ob_delete(task);
        }
        // free session locally
        if (OB_FAIL(ret)) {
          ObMPDisconnect disconnect_processor(ctx);
          rpc::frame::ObReqProcessor *processor = static_cast<rpc::frame::ObReqProcessor *>(&disconnect_processor);
          if (OB_FAIL(processor->run())) {
            LOG_WARN("free session fail and related session id can not be reused", K(ret), K(ctx), "sessid", conn->sessid_);
          }
        }
      }
    } else {
      if (OB_UNLIKELY(OB_FAIL(sql::ObSQLSessionMgr::is_need_clear_sessid(conn, is_need_clear)))) {
        LOG_ERROR("fail to judge need clear", K(ret));
      } else if (is_need_clear) {
        if (OB_UNLIKELY(OB_FAIL(gctx_.session_mgr_->mark_sessid_unused(conn->sessid_)))) {
          LOG_ERROR("fail to mark sessid unused", K(ret), K(conn->sessid_),
                    "proxy_sessid", conn->proxy_sessid_, "server_id", GCTX.server_id_);
        } else {
          LOG_INFO("mark sessid unused", K(conn->sessid_),
                  "proxy_sessid", conn->proxy_sessid_, "server_id", GCTX.server_id_);
        }
      } else {/*do nothing*/}
    }

    //unlock tenant
    if (OB_LIKELY(NULL != conn->tenant_ && conn->is_tenant_locked_)) {
      conn->tenant_->unlock(*conn->handle_);
      conn->is_tenant_locked_ = false;
      conn->tenant_ = NULL;
      LOG_INFO("unlock session of tenant", K(conn->sessid_),
               "proxy_sessid", conn->proxy_sessid_, K(conn->tenant_id_));
    }
    share::ObTaskController::get().allow_next_syslog();
    LOG_INFO("connection close",
             K(easy_connection_str(c)),
             "sessid", conn->sessid_,
             "proxy_sessid", conn->proxy_sessid_,
             "tenant_id", conn->tenant_id_,
             "server_id", gctx_.server_id_,
             "from_proxy", conn->is_proxy_,
             "from_java_client", conn->is_java_client_,
             "c/s protocol", get_cs_protocol_type_name(conn->get_cs_protocol_type()),
             "is_need_clear_sessid_", conn->is_need_clear_sessid_,
             "is_sess_alloc_", conn->is_sess_alloc_,
             K(ret),
             K(trace_id),
             K(conn->pkt_rec_wrapper_),
             K(disconnect_state));
    conn->~ObSMConnection();
    conn = nullptr;
  }
  return eret;
}

bool ObSMHandler::is_in_connected_phase(easy_connection_t *c) const
{
  bool bool_ret = false;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(c) || OB_ISNULL(c->user_data)) {
    ret = EASY_ERROR;
    LOG_ERROR("easy_connection_t is null", K(ret));
  } else {
    ObSMConnection *conn = reinterpret_cast<ObSMConnection*>(c->user_data);
    bool_ret = conn->is_in_connected_phase();
  }
  return bool_ret;
}

bool ObSMHandler::is_in_ssl_connect_phase(easy_connection_t *c) const
{
  bool bool_ret = false;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(c) || OB_ISNULL(c->user_data)) {
    ret = EASY_ERROR;
    LOG_ERROR("easy_connection_t is null", K(ret));
  } else {
    ObSMConnection *conn = reinterpret_cast<ObSMConnection*>(c->user_data);
    bool_ret = conn->is_in_ssl_connect_phase();
  }
  return bool_ret;
}

bool ObSMHandler::is_in_authed_phase(easy_connection_t *c) const
{
  bool bool_ret = false;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(c) || OB_ISNULL(c->user_data)) {
    ret = EASY_ERROR;
    LOG_ERROR("easy_connection_t is null", K(ret));
  } else {
    ObSMConnection *conn = reinterpret_cast<ObSMConnection*>(c->user_data);
    bool_ret = conn->is_in_authed_phase();
  }
  return bool_ret;
}

rpc::ConnectionPhaseEnum ObSMHandler::get_connection_phase(easy_connection_t *c) const
{
  rpc::ConnectionPhaseEnum ret_enum = rpc::ConnectionPhaseEnum::CPE_AUTHED;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(c) || OB_ISNULL(c->user_data)) {
    ret = EASY_ERROR;
    LOG_ERROR("easy_connection_t is null", K(ret));
  } else {
    ObSMConnection *conn = reinterpret_cast<ObSMConnection*>(c->user_data);
    ret_enum = conn->connection_phase_;
  }
  return ret_enum;
}

void ObSMHandler::set_ssl_connect_phase(easy_connection_t *c)
{
  if (OB_ISNULL(c) || OB_ISNULL(c->user_data)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "easy_connection_t is null", KP(c));
  } else {
    ObSMConnection *conn = reinterpret_cast<ObSMConnection*>(c->user_data);
    conn->connection_phase_ = rpc::ConnectionPhaseEnum::CPE_SSL_CONNECT;
  }
}

void ObSMHandler::set_connect_phase(easy_connection_t *c)
{
  if (OB_ISNULL(c) || OB_ISNULL(c->user_data)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "easy_connection_t is null", KP(c));
  } else {
    ObSMConnection *conn = reinterpret_cast<ObSMConnection*>(c->user_data);
    conn->connection_phase_ = rpc::ConnectionPhaseEnum::CPE_CONNECTED;
  }
}

bool ObSMHandler::is_compressed(easy_connection_t *c) const
{
  bool bool_ret = false;
  if (OB_ISNULL(c) || OB_ISNULL(c->user_data)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "easy_connection_t is null");
  } else {
    ObSMConnection *conn = reinterpret_cast<ObSMConnection*>(c->user_data);
    bool_ret = (1 == conn->cap_flags_.cap_flags_.OB_CLIENT_COMPRESS);
  }
  return bool_ret;
}

uint32_t ObSMHandler::get_sessid(easy_connection_t *c) const
{
  uint32_t ret_sessid = 0;
  if (OB_ISNULL(c) || OB_ISNULL(c->user_data)) {
  } else {
    ObSMConnection *conn = reinterpret_cast<ObSMConnection*>(c->user_data);
    ret_sessid = conn->sessid_;
  }
  return ret_sessid;
}

obmysql::ObProto20PktContext *ObSMHandler::get_proto20_pkt_context(easy_connection_t *c)
{
  obmysql::ObProto20PktContext *ret_pkt_context = NULL;
  if (OB_ISNULL(c) || OB_ISNULL(c->user_data)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "easy_connection_t is null");
  } else {
    ObSMConnection *conn = reinterpret_cast<ObSMConnection*>(c->user_data);
    ret_pkt_context = &(conn->proto20_pkt_context_);
  }
  return ret_pkt_context;
}

obmysql::ObCompressedPktContext *ObSMHandler::get_compressed_pkt_context(easy_connection_t *c)
{
  obmysql::ObCompressedPktContext *ret_pkt_context = NULL;
  if (OB_ISNULL(c) || OB_ISNULL(c->user_data)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "easy_connection_t is null");
  } else {
    ObSMConnection *conn = reinterpret_cast<ObSMConnection*>(c->user_data);
    ret_pkt_context = &(conn->compressed_pkt_context_);
  }
  return ret_pkt_context;
}

obmysql::ObMysqlPktContext *ObSMHandler::get_mysql_pkt_context(easy_connection_t *c)
{
  obmysql::ObMysqlPktContext *ret_pkt_context = NULL;
  if (OB_ISNULL(c) || OB_ISNULL(c->user_data)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "easy_connection_t is null");
  } else {
    ObSMConnection *conn = reinterpret_cast<ObSMConnection*>(c->user_data);
    ret_pkt_context = &(conn->mysql_pkt_context_);
  }
  return ret_pkt_context;
}

ObCSProtocolType ObSMHandler::get_cs_protocol_type(easy_connection_t *c) const
{
  ObCSProtocolType type = OB_INVALID_CS_TYPE;
  if (OB_ISNULL(c) || OB_ISNULL(c->user_data)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "easy_connection_t is null");
  } else {
    ObSMConnection *conn = reinterpret_cast<ObSMConnection*>(c->user_data);
    type = conn->get_cs_protocol_type();
  }
  return type;
}

} // namespace observer
} // namespace oceanbase
