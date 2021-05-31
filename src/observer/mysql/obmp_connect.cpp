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

#include <stdlib.h>
#include "observer/mysql/obmp_connect.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/string/ob_sql_string.h"
#include "lib/oblog/ob_log.h"
#include "lib/stat/ob_session_stat.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "common/object/ob_object.h"
#include "common/ob_string_buf.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/ob_cluster_version.h"
#include "share/ob_get_compat_mode.h"
#include "share/resource_manager/ob_resource_manager.h"
#include "rpc/ob_request.h"
#include "rpc/obmysql/packet/ompk_ok.h"
#include "rpc/obmysql/packet/ompk_error.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "sql/ob_sql.h"
#include "observer/ob_server.h"
#include "observer/mysql/obsm_struct.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/omt/ob_tenant.h"
#include "observer/ob_req_time_service.h"
#include "storage/transaction/ob_weak_read_util.h"  //ObWeakReadUtil
#include "sql/privilege_check/ob_privilege_check.h"

using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::obmysql;
using namespace oceanbase::observer;
using namespace oceanbase::share::schema;

ObMPConnect::ObMPConnect(const ObGlobalContext& gctx)
    : ObMPBase(gctx), user_name_(), client_ip_(), tenant_name_(), db_name_(), deser_ret_(OB_SUCCESS)
{
  client_ip_buf_[0] = '\0';
  user_name_var_[0] = '\0';
  db_name_var_[0] = '\0';
}

ObMPConnect::~ObMPConnect()
{}

int ObMPConnect::deserialize()
{
  int ret = OB_SUCCESS;

  ObSMConnection* conn = get_conn();
  // OB_ASSERT(conn);
  if (OB_ISNULL(conn)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid conn", K(ret), K(conn));
  } else if (OB_ISNULL(req_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid req_", K(ret), K(req_));
  } else {
    hsr_ = reinterpret_cast<const OMPKHandshakeResponse&>(req_->get_packet());
    if (OB_FAIL(hsr_.decode())) {
      LOG_WARN("decode hsr fail", K(ret));
    } else {
      conn->cap_flags_ = hsr_.get_capability_flags();

      if (hsr_.is_obproxy_client_mode()) {
        conn->is_proxy_ = true;
      }
      if (hsr_.is_java_client_mode()) {
        conn->is_java_client_ = true;
      }

      if (OB_FAIL(extract_user_tenant(hsr_.get_username()))) {
        LOG_WARN("parse user@tenant fail", K(ret), "str", hsr_.get_username());
      } else {
        db_name_ = hsr_.get_database();
        LOG_DEBUG("database name", K(hsr_.get_database()));
      }
    }

    deser_ret_ = ret;  // record deserialize ret code.
    ret = OB_SUCCESS;  // return OB_SUCCESS anyway.
  }
  return ret;
}

int ObMPConnect::init_process_single_stmt(
    const ObMultiStmtItem& multi_stmt_item, ObSQLSessionInfo& session, bool has_more_result) const
{
  int ret = OB_SUCCESS;
  ObSqlCtx ctx;
  const ObString& sql = multi_stmt_item.get_sql();
  ctx.exec_type_ = MpQuery;
  bool use_session_trace = false;
  ObVirtualTableIteratorFactory vt_iter_factory(*gctx_.vt_iter_creator_);
  ObSessionStatEstGuard stat_est_guard(get_conn()->tenant_->id(), session.get_sessid());
  ObSchemaGetterGuard schema_guard;
  observer::ObReqTimeGuard req_timeinfo_guard;
  if (OB_FAIL(init_process_var(ctx, multi_stmt_item, session, vt_iter_factory, use_session_trace))) {
    LOG_WARN("init process var failed.", K(ret), K(multi_stmt_item));
  } else if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(session.get_effective_tenant_id(), schema_guard))) {
    LOG_WARN("get schema guard failed.", K(ret));
  } else if (OB_FAIL(set_session_active(sql, session, ObTimeUtil::current_time()))) {
    LOG_WARN("fail to set session active", K(ret));
  } else {
    const bool enable_trace_log = lib::is_trace_log_enabled();
    if (enable_trace_log) {
      // set session log_level.Must use ObThreadLogLevelUtils::clear() in pair
      ObThreadLogLevelUtils::init(session.get_log_id_level_map());
    }
    ctx.retry_times_ = 0;
    ctx.schema_guard_ = &schema_guard;
    ObMySQLResultSet result(session);
    result.init_partition_location_cache(gctx_.location_cache_, gctx_.self_addr_, &schema_guard);
    result.set_has_more_result(has_more_result);
    result.get_exec_context().get_task_exec_ctx().set_min_cluster_version(GET_MIN_CLUSTER_VERSION());
    ctx.partition_location_cache_ = &(result.get_partition_location_cache());
    if (OB_FAIL(result.init())) {
      LOG_WARN("result set init failed");
    } else if (OB_ISNULL(gctx_.sql_engine_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid sql engine", K(ret), K(gctx_));
    } else if (OB_FAIL(gctx_.sql_engine_->stmt_query(sql, ctx, result))) {
      LOG_WARN("sql execute failed", K(multi_stmt_item), K(sql), K(ret));
    } else {
      if (OB_FAIL(result.sync_open())) {
        LOG_WARN("failed to do result set open", K(ret));
      }
      int save_ret = ret;
      if (OB_FAIL(result.close())) {
        LOG_WARN("result close failed, disconnect.", K(ret));
      }
      ret = (save_ret != OB_SUCCESS) ? save_ret : ret;
    }
    if (enable_trace_log) {
      ObThreadLogLevelUtils::clear();
    }
  }

  int tmp_ret = OB_SUCCESS;
  tmp_ret = do_after_process(session, use_session_trace, ctx, false);
  UNUSED(tmp_ret);
  return ret;
}

int ObMPConnect::init_connect_process(ObString& init_sql, ObSQLSessionInfo& session) const
{
  int ret = OB_SUCCESS;
  ObSEArray<ObString, 4> queries;
  ObArenaAllocator allocator(ObModIds::OB_SQL_PARSER);
  ObParser parser(allocator, session.get_sql_mode(), session.get_local_collation_connection());
  ObMPParseStat parse_stat;
  if (OB_SUCC(parser.split_multiple_stmt(init_sql, queries, parse_stat))) {
    if (OB_UNLIKELY(0 == queries.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("empty query!", K(ret), K(init_sql));
    }
    bool has_more;
    ARRAY_FOREACH(queries, i)
    {
      has_more = (queries.count() > i + 1);
      if (OB_FAIL(init_process_single_stmt(ObMultiStmtItem(true, i, queries[i]), session, has_more))) {
        LOG_WARN("process single stmt failed!", K(ret), K(queries[i]));
      }
    }
  } else {
    LOG_WARN("split multiple stmt failed!", K(ret));
  }
  return ret;
}

int ObMPConnect::process()
{
  int ret = deser_ret_;
  ObSMConnection* conn = NULL;
  uint64_t tenant_id = OB_INVALID_ID;
  ObSQLSessionInfo* session = NULL;
  if (THE_TRACE != nullptr) {
    THE_TRACE->reset();
  }
  if (OB_FAIL(ret)) {
    LOG_ERROR("deserialize failed", K(ret));
  } else if (OB_ISNULL(req_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("null ez_req", K(ret));
  } else if (OB_ISNULL(conn = get_conn())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("null conn", K(ret));
  } else if (OB_ISNULL(req_->get_request())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("null request", K(ret));
  } else if (OB_ISNULL(GCTX.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("session mgr is NULL", K(ret));
  } else {
    if (OB_FAIL(conn->ret_)) {
      LOG_WARN("connection fail at obsm_handle process", K(conn->ret_));
    } else if (SS_INIT == GCTX.status_ && !tenant_name_.empty() && 0 != tenant_name_.compare(OB_SYS_TENANT_NAME)) {
      // accept system tenant for bootstrap
      ret = OB_SERVER_IS_INIT;
      LOG_WARN("server is initializing", K(ret));
    } else if (SS_STOPPING == GCTX.status_) {
      ret = OB_SERVER_IS_STOPPING;
      LOG_WARN("server is stopping", K(ret));
    } else if (OB_FAIL(check_update_tenant_id(*conn, tenant_id))) {
      LOG_WARN("fail to check update tenant id", K(ret));
    } else if (OB_FAIL(check_client_property(*conn))) {
      LOG_WARN("check_client_property fail", K(ret));
    } else if (OB_FAIL(switch_sessid(*conn))) {
      LOG_WARN("switch sessid fail", K(ret));
    } else if (OB_FAIL(verify_connection(tenant_id))) {
      LOG_WARN("verify connection fail", K(ret));
    } else if (OB_FAIL(create_session(conn, session))) {
      LOG_WARN("alloc session fail", K(ret));
    } else if (OB_ISNULL(session)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("null session", K(ret), K(session));
    } else if (OB_FAIL(verify_identify(*conn, *session, tenant_id))) {
      LOG_WARN("fail to verify_identify", K(ret));
    } else if (OB_FAIL(update_transmission_checksum_flag(*session))) {
      LOG_WARN("update transmisson checksum flag failed", K(ret));
    } else if (OB_FAIL(update_proxy_sys_vars(*session))) {
      LOG_WARN("update_proxy_sys_vars failed", K(ret));
    } else if (OB_FAIL(setup_user_resource_group(*conn, tenant_id, session->get_user_id()))) {
      LOG_WARN("fail setup user resource group", K(ret));
    } else {
      LOG_TRACE("setup user resource group OK",
          "user_id",
          session->get_user_id(),
          K(tenant_id),
          K(user_name_),
          "group_id",
          conn->group_id_);
      conn->set_auth_phase();
    }

    int proc_ret = ret;
    char client_ip_buf[OB_IP_STR_BUFF] = {};
    if (!req_->get_peer().ip_to_string(client_ip_buf, OB_IP_STR_BUFF)) {
      LOG_WARN("fail to ip to string");
      snprintf(client_ip_buf, OB_IP_STR_BUFF, "xxx.xxx.xxx.xxx");
    }
    char host_name_buf[OB_IP_STR_BUFF] = {};
    if (NULL != session && !session->get_client_ip().empty()) {
      session->get_host_name().to_string(host_name_buf, OB_IP_STR_BUFF);
    } else {
      snprintf(host_name_buf, OB_IP_STR_BUFF, "xxx.xxx.xxx.xxx");
    }
    const ObString host_name(host_name_buf);
    const ObCSProtocolType protoType = conn->get_cs_protocol_type();
    const uint32_t version = conn->version_;
    const uint32_t sessid = conn->sessid_;
    const uint64_t proxy_sessid = conn->proxy_sessid_;
    const int64_t sess_create_time = conn->sess_create_time_;
    const uint32_t capability = conn->cap_flags_.capability_;
    const bool from_proxy = conn->is_proxy_;
    const bool from_java_client = conn->is_java_client_;
    const bool use_ssl = conn->cap_flags_.cap_flags_.OB_CLIENT_SSL;
    const uint64_t proxy_capability = conn->proxy_cap_flags_.capability_;

    if (OB_SUCC(proc_ret)) {
      // send packet for client
      ObOKPParam ok_param;
      ok_param.is_on_connect_ = true;
      ok_param.affected_rows_ = 0;
      if (OB_FAIL(send_ok_packet(*session, ok_param))) {
        LOG_WARN("fail to send ok packet", K(ok_param), K(ret));
      }
    } else {
      char buf[OB_MAX_ERROR_MSG_LEN];
      switch (proc_ret) {
        case OB_PASSWORD_WRONG:
        case OB_ERR_INVALID_TENANT_NAME: {
          ret = OB_PASSWORD_WRONG;
          snprintf(buf,
              OB_MAX_ERROR_MSG_LEN,
              ob_errpkt_str_user_error(ret, lib::is_oracle_mode()),
              user_name_.length(),
              user_name_.ptr(),
              host_name.length(),
              host_name.ptr(),
              (hsr_.get_auth_response().empty() ? "NO" : "YES"));
          break;
        }
        case OB_CLUSTER_NO_MATCH: {
          snprintf(
              buf, OB_MAX_ERROR_MSG_LEN, ob_errpkt_str_user_error(ret, lib::is_oracle_mode()), GCONF.cluster.str());
          break;
        }
        default: {
          buf[0] = '\0';
          break;
        }
      }
      if (OB_FAIL(send_error_packet(ret, buf))) {
        LOG_WARN("response fail packet fail", K(ret));
      }
    }

    if (NULL != session) {
      ObSqlString comment_text;
      (void)comment_text.append_fmt("LOGIN: tenant_name=%.*s, user_name=%.*s, client_ip=%.*s, "
                                    "sessid=%u, proxy_sessid=%lu, "
                                    "capability=%X, proxy_capability=%lX, use_ssl=%s, protocol=%s",
          tenant_name_.length(),
          tenant_name_.ptr(),
          user_name_.length(),
          user_name_.ptr(),
          host_name.length(),
          host_name.ptr(),
          sessid,
          proxy_sessid,
          capability,
          proxy_capability,
          use_ssl ? "true" : "false",
          get_cs_protocol_type_name(protoType));

      if (OB_SUCCESS == proc_ret) {
        proc_ret = session->drop_reused_oracle_temp_tables();
      }
      // Action!!:must revert it after no use it
      revert_session(session);
    }
    if (OB_SUCCESS != proc_ret) {
      if (NULL != session) {
        free_session();
      }
      disconnect();
    }

    LOG_INFO("MySQL LOGIN",
        "direct_client_ip",
        client_ip_buf,
        K_(client_ip),
        K_(tenant_name),
        K(tenant_id),
        K_(user_name),
        K(host_name),
        K(version),
        K(sessid),
        K(proxy_sessid),
        K(sess_create_time),
        K(from_proxy),
        K(from_java_client),
        K(capability),
        K(proxy_capability),
        K(use_ssl),
        "c/s protocol",
        get_cs_protocol_type_name(protoType),
        K(proc_ret),
        K(ret));
  }
  return ret;
}

inline bool is_inner_proxyro_user(const ObSMConnection& conn, const ObString& user_name)
{
  const static ObString PROXYRO_USERNAME(OB_PROXYRO_USERNAME);
  const static ObString PROXYRO_HOSTNAME(OB_DEFAULT_HOST_NAME);
  return (!conn.is_proxy_ && !conn.is_java_client_ && OB_SYS_TENANT_ID == conn.tenant_id_ &&
          0 == PROXYRO_USERNAME.compare(user_name));
}

inline void reset_inner_proxyro_scramble(ObSMConnection& conn, schema::ObUserLoginInfo& login_info)
{
  const ObString PROXYRO_OLD_SCRAMBLE("aaaaaaaabbbbbbbbbbbb");
  MEMCPY(conn.scramble_buf_, PROXYRO_OLD_SCRAMBLE.ptr(), PROXYRO_OLD_SCRAMBLE.length());
  login_info.scramble_str_.assign_ptr(conn.scramble_buf_, sizeof(conn.scramble_buf_));
}

int ObMPConnect::load_privilege_info(ObSQLSessionInfo& session)
{
  LOG_DEBUG("load privilege info");
  int ret = OB_SUCCESS;
  ObSMConnection* conn = get_conn();
  ObSchemaGetterGuard schema_guard;
  if (OB_ISNULL(gctx_.schema_service_) || OB_ISNULL(conn)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(gctx_.schema_service_));
  } else if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(conn->tenant_id_, schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else {
    // set client mode
    if (conn->is_java_client_) {
      session.set_client_mode(OB_JAVA_CLIENT_MODE);
    }
    if (conn->is_proxy_) {
      session.set_client_mode(OB_PROXY_CLIENT_MODE);
    }

    ObString host_name;
    if (OB_DIAG_TENANT_ID == conn->tenant_id_) {
      const ObString scramble_str(conn->scramble_buf_);
      if (OB_FAIL(GCTX.diag_->check_passwd(hsr_.get_auth_response(), scramble_str))) {
        LOG_WARN("diag password mismatch", K(ret));
      } else {
        // Attention!! must set session capability firstly
        schema::ObSessionPrivInfo session_priv;
        uint64_t db_id = OB_INVALID_ID;
        session.set_capability(hsr_.get_capability_flags());
        session.set_user_priv_set(OB_PRIV_ALL);
        session.set_db_priv_set(OB_PRIV_ALL);
        host_name.assign_ptr(OB_SYS_HOST_NAME, static_cast<int32_t>(strlen(OB_SYS_HOST_NAME)));
        if (OB_FAIL(session.init_tenant(OB_SYS_TENANT_NAME, OB_SYS_TENANT_ID))) {
          LOG_WARN("failed to init_tenant", K(ret));
        } else if (OB_FAIL(session.set_tenant(tenant_name_, OB_DIAG_TENANT_ID))) {
          LOG_WARN("failed to set_tenant", K(ret));
        } else if (OB_FAIL(session.set_user(user_name_, OB_SYS_HOST_NAME, 0))) {
          LOG_WARN("failed to set_user", K(ret));
        } else if (OB_FAIL(session.set_real_client_ip(client_ip_))) {
          LOG_WARN("failed to set_real_client_ip", K(ret));
        } else if (OB_FAIL(session.set_default_database(session_priv.db_))) {
          LOG_WARN("failed to set default database", K(ret), K(session_priv.db_));
        } else if (OB_FAIL(session.load_default_sys_variable(false, true))) {
          LOG_WARN("failed to load system variables", K(ret));
        } else if (OB_FAIL(session.update_database_variables(&schema_guard))) {
          LOG_WARN("failed to update database variables", K(ret));
        } else if (OB_FAIL(session.update_max_packet_size())) {
          LOG_WARN("failed to update max packet size", K(ret));
        } else if (OB_FAIL(schema_guard.get_database_id(
                       session.get_effective_tenant_id(), session.get_database_name(), db_id))) {
          LOG_WARN("failed to get database id", K(ret));
        } else {
          session.set_database_id(db_id);
        }
      }
    } else {
      if (tenant_name_.empty()) {
        tenant_name_ = ObString::make_string(OB_SYS_TENANT_NAME);
        OB_LOG(INFO, "no tenant name set, use default tenant name", K_(tenant_name));
      }
      if (OB_SUCC(ret)) {
        if (db_name_.length() > OB_MAX_DATABASE_NAME_LENGTH || user_name_.length() > OB_MAX_USER_NAME_LENGTH) {
          ret = OB_INVALID_ARGUMENT_FOR_LENGTH;
          LOG_WARN("invalid length for db_name or user_name", K(db_name_), K(user_name_), K(ret));
        } else {
          MEMCPY(db_name_var_, db_name_.ptr(), db_name_.length());
          db_name_var_[db_name_.length()] = '\0';
          MEMCPY(user_name_var_, user_name_.ptr(), user_name_.length());
          user_name_var_[user_name_.length()] = '\0';
          user_name_.assign_ptr(user_name_var_, user_name_.length());
          db_name_.assign_ptr(db_name_var_, db_name_.length());
        }
      }

      schema::ObSessionPrivInfo session_priv;
      const ObSysVariableSchema* sys_variable_schema = NULL;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(convert_oracle_object_name(conn->tenant_id_, user_name_))) {
        LOG_WARN("fail to convert oracle user name", K(ret));
      } else if (OB_FAIL(convert_oracle_object_name(conn->tenant_id_, db_name_))) {
        LOG_WARN("fail to convert oracle db name", K(ret));
      } else if (OB_FAIL(schema_guard.get_sys_variable_schema(conn->tenant_id_, sys_variable_schema))) {
        LOG_WARN("get sys variable schema failed", K(ret));
      } else if (OB_ISNULL(sys_variable_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sys variable schema is null", K(ret));
      } else if (OB_FAIL(session.init_tenant(tenant_name_, conn->tenant_id_))) {
        LOG_WARN("failed to init_tenant", K(ret));
      } else if (OB_FAIL(session.load_all_sys_vars(*sys_variable_schema))) {
        LOG_WARN("load system variables failed", K(ret));
      } else {
        schema::ObUserLoginInfo login_info;
        login_info.tenant_name_ = tenant_name_;
        login_info.user_name_ = user_name_;
        login_info.client_ip_ = client_ip_;
        if (OB_SUCC(ret) && ORACLE_MODE == session.get_compatibility_mode() && db_name_.empty()) {
          login_info.db_ = user_name_;
        } else {
          login_info.db_ = db_name_;
        }
        login_info.scramble_str_.assign_ptr(conn->scramble_buf_, sizeof(conn->scramble_buf_));
        login_info.passwd_ = hsr_.get_auth_response();

        SSL* ssl_st = req_->get_ssl_st();
        if (OB_FAIL(schema_guard.check_user_access(login_info, session_priv, ssl_st))) {

          int inner_ret = OB_SUCCESS;
          bool is_unlocked = false;
          if (ORACLE_MODE == session.get_compatibility_mode() && GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_3000 &&
              OB_ERR_USER_IS_LOCKED == ret) {
            if (OB_SUCCESS != (inner_ret = unlock_user_if_time_is_up(conn->tenant_id_, schema_guard, is_unlocked))) {
              LOG_WARN("fail to check user unlock", K(inner_ret));
            }
          }

          int tmp_ret = OB_SUCCESS;
          ObMultiVersionSchemaService* schema_service = gctx_.schema_service_;
          int64_t local_version = OB_INVALID_VERSION;
          int64_t global_version = OB_INVALID_VERSION;
          if (OB_SUCCESS !=
              (tmp_ret = schema_service->get_tenant_refreshed_schema_version(conn->tenant_id_, local_version))) {
            LOG_WARN("fail to get local version", K(ret), K(tmp_ret), "tenant_id", conn->tenant_id_);
          } else if (OB_SUCCESS != (tmp_ret = schema_service->get_tenant_received_broadcast_version(
                                        conn->tenant_id_, global_version))) {
            LOG_WARN("fail to get local version", K(ret), K(tmp_ret), "tenant_id", conn->tenant_id_);
          } else if (local_version < global_version || is_unlocked) {
            uint64_t tenant_id = conn->tenant_id_;
            LOG_INFO("try to refresh schema", K(tenant_id), K(is_unlocked), K(local_version), K(global_version));
            if (OB_SUCCESS != (tmp_ret = gctx_.schema_service_->async_refresh_schema(tenant_id, global_version))) {
              LOG_WARN("failed to refresh schema", K(tmp_ret), K(tenant_id), K(global_version));
            } else if (OB_SUCCESS !=
                       (tmp_ret = gctx_.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
              LOG_WARN("get schema guard failed", K(ret), K(tmp_ret), K(tenant_id));
            } else if (OB_SUCCESS == inner_ret) {
              if (OB_FAIL(schema_guard.check_user_access(login_info, session_priv, ssl_st))) {
                LOG_WARN("User access denied", K(login_info), K(ret));
              }
            }
          }

          if (OB_FAIL(ret)) {
            if (OB_PASSWORD_WRONG == ret && is_inner_proxyro_user(*conn, user_name_)) {
              reset_inner_proxyro_scramble(*conn, login_info);
              int pre_ret = ret;
              if (OB_FAIL(schema_guard.check_user_access(login_info, session_priv, ssl_st))) {
                LOG_WARN("User access denied", K(login_info), K(pre_ret), K(ret));
              }
            } else {
              LOG_WARN("User access denied", K(login_info), K(ret));
            }
          }
        }
      }

      if (ORACLE_MODE == session.get_compatibility_mode() && GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_3000 &&
          (OB_SUCC(ret) || OB_PASSWORD_WRONG == ret)) {
        int login_ret = ret;
        if (OB_FAIL(update_login_stat_in_trans(conn->tenant_id_, OB_SUCCESS == login_ret, schema_guard))) {
          LOG_WARN("fail to update login stat in trans", K(ret));
        } else {
          ret = login_ret;
        }
      }

      if ((OB_SUCC(ret) && GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2276)) {
        if (OB_FAIL(check_password_expired(conn->tenant_id_, schema_guard, session))) {
          LOG_WARN("fail to check password expired", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        // Attention!! must set session capability firstly
        session.set_capability(hsr_.get_capability_flags());
        session.set_user_priv_set(session_priv.user_priv_set_);
        session.set_db_priv_set(session_priv.db_priv_set_);
        session.set_enable_role_array(session_priv.enable_role_id_array_);
        host_name = session_priv.host_name_;
        uint64_t db_id = OB_INVALID_ID;
        const ObTenantSchema* tenant_schema = NULL;
        if (OB_FAIL(session.set_user(user_name_, session_priv.host_name_, session_priv.user_id_))) {
          LOG_WARN("failed to set_user", K(ret));
        } else if (OB_FAIL(session.set_real_client_ip(client_ip_))) {
          LOG_WARN("failed to set_real_client_ip", K(ret));
        } else if (OB_FAIL(session.set_default_database(session_priv.db_))) {
          LOG_WARN("failed to set default database", K(ret), K(session_priv.db_));
        } else if (OB_FAIL(schema_guard.get_tenant_info(session_priv.tenant_id_, tenant_schema))) {
          LOG_WARN("get tenant info failed", K(ret));
        } else if (OB_ISNULL(tenant_schema)) {
          ret = OB_TENANT_NOT_EXIST;
          LOG_WARN("tenant_schema is null", K(ret));
        } else if (tenant_schema->is_in_recyclebin()) {
          ret = OB_TENANT_NOT_EXIST;
          LOG_WARN("tenant is in recyclebin", KR(ret), K(session_priv.tenant_id_));
        } else if (tenant_schema->is_restore()) {
          ret = OB_STATE_NOT_MATCH;
          LOG_WARN("tenant is in restore", KR(ret), K(session_priv.tenant_id_));
        } else if (OB_FAIL(session.update_database_variables(&schema_guard))) {
          LOG_WARN("failed to update database variables", K(ret));
        } else if (OB_FAIL(session.update_max_packet_size())) {
          LOG_WARN("failed to update max packet size", K(ret));
        }

        if (OB_SUCC(ret) && !session.get_database_name().empty()) {
          if (OB_FAIL(schema_guard.get_database_id(
                  session.get_effective_tenant_id(), session.get_database_name(), db_id))) {
            int tmp_ret = OB_SUCCESS;
            LOG_WARN("failed to get database id", K(ret), K(session.get_database_name()));
            ObMultiVersionSchemaService* schema_service = gctx_.schema_service_;
            int64_t local_version = OB_INVALID_VERSION;
            int64_t global_version = OB_INVALID_VERSION;
            const uint64_t effective_tenant_id = session.get_effective_tenant_id();
            if (OB_SUCCESS !=
                (tmp_ret = schema_service->get_tenant_refreshed_schema_version(effective_tenant_id, local_version))) {
              LOG_WARN("fail to get local version", K(ret), K(tmp_ret), "tenant_id", effective_tenant_id);
            } else if (OB_SUCCESS != (tmp_ret = schema_service->get_tenant_received_broadcast_version(
                                          effective_tenant_id, global_version))) {
              LOG_WARN("fail to get local version", K(ret), K(tmp_ret), "tenant_id", effective_tenant_id);
            } else if (local_version < global_version) {
              LOG_INFO("try to refresh schema", K(effective_tenant_id), K(local_version), K(global_version));
              if (OB_SUCCESS !=
                  (tmp_ret = gctx_.schema_service_->async_refresh_schema(effective_tenant_id, global_version))) {
                LOG_WARN("failed to refresh schema", K(tmp_ret), K(effective_tenant_id), K(global_version));
              } else if (OB_SUCCESS != (tmp_ret = gctx_.schema_service_->get_tenant_schema_guard(
                                            effective_tenant_id, schema_guard))) {
                LOG_WARN("get schema guard failed", K(ret), K(tmp_ret));
              } else if (OB_SUCCESS != (tmp_ret = schema_guard.get_database_id(
                                            effective_tenant_id, session.get_database_name(), db_id))) {
                LOG_WARN("failed to get database id", K(ret), K(tmp_ret));
              } else {
                ret = OB_SUCCESS;
              }
            }
          }
          if (OB_SUCC(ret)) {
            session.set_database_id(db_id);
          }
        }
      }
    }
    LOG_DEBUG("obmp connect info:",
        K_(tenant_name),
        K_(user_name),
        K(host_name),
        K_(client_ip),
        "database",
        hsr_.get_database(),
        K(hsr_.get_capability_flags().capability_),
        K(session.is_client_use_lob_locator()));
  }
  return ret;
}

int ObMPConnect::switch_lock_status_for_current_login_user(const uint64_t tenant_id, bool do_lock)
{
  int ret = OB_SUCCESS;

  ObSqlString lock_user_sql;
  common::ObMySQLProxy* sql_proxy = nullptr;
  int64_t affected_rows = 0;

  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid id", K(tenant_id), K(ret));
  } else if (OB_FAIL(lock_user_sql.append_fmt("ALTER USER \"%.*s\" ACCOUNT %s",
                 user_name_.length(),
                 user_name_.ptr(),
                 do_lock ? "LOCK" : "UNLOCK"))) {
    LOG_WARN("append string failed", K(ret));
  } else if (OB_ISNULL(sql_proxy = gctx_.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", K(ret));
  } else if (OB_FAIL(
                 sql_proxy->write(tenant_id, lock_user_sql.ptr(), affected_rows, ObCompatibilityMode::ORACLE_MODE))) {
    LOG_WARN("fail to execute lock user", K(ret));
  }
  LOG_INFO("user ddl has been sent, change user lock status to ", K(do_lock));

  return ret;
}

int ObMPConnect::unlock_user_if_time_is_up(const uint64_t tenant_id, ObSchemaGetterGuard& schema_guard, bool& is_unlock)
{
  int ret = OB_SUCCESS;
  uint64_t user_id = OB_INVALID_ID;
  is_unlock = false;
  bool is_exist = false;
  int64_t failed_login_limit_num = INT64_MAX;
  int64_t failed_login_limit_time = INT64_MAX;
  int64_t current_failed_login_num = 0;
  int64_t last_failed_login_timestamp = 0;
  int64_t current_gmt = ObTimeUtil::current_time();
  ObMySQLTransaction trans;

  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid tenant", K(tenant_id), K(ret));
  } else if (OB_FAIL(schema_guard.check_user_exist(
                 tenant_id, user_name_, ObString(OB_DEFAULT_HOST_NAME), is_exist, &user_id))) {
    LOG_WARN("fail to check user exist", K(ret));
  } else if (!is_exist) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("user not exist", K(ret));
  } else if (OB_FAIL(schema_guard.get_user_profile_failed_login_limits(
                 user_id, failed_login_limit_num, failed_login_limit_time))) {
    LOG_WARN("fail to get user id and profile limit", K(ret));
  } else if (failed_login_limit_num == INT64_MAX) {
    // unlimited do nothing
  } else if (OB_FAIL(trans.start(gctx_.sql_proxy_))) {
    LOG_WARN("fail to start trans", K(ret));
  } else if (OB_FAIL(
                 get_last_failed_login_info(user_id, trans, current_failed_login_num, last_failed_login_timestamp))) {
    LOG_WARN("fail to check current login user need unlock", K(user_id), K(user_name_), K(ret));
  } else if (current_failed_login_num >= failed_login_limit_num &&
             current_gmt - last_failed_login_timestamp >= failed_login_limit_time) {
    if (OB_FAIL(switch_lock_status_for_current_login_user(tenant_id, false))) {
      LOG_WARN("fail to check lock status", K(ret));
    } else if (OB_FAIL(update_current_user_failed_login_num(user_id, trans, 0))) {
      LOG_WARN("fail to clear failed login num", K(ret));
    } else {
      is_unlock = true;
    }
  } else {
  }

  if (trans.is_started()) {
    int temp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("trans end failed", "is_commit", OB_SUCC(ret), K(ret), K(temp_ret));
      ret = OB_SUCC(ret) ? temp_ret : ret;
    }
  }

  LOG_DEBUG("user is locked, check timeout",
      K(failed_login_limit_num),
      K(current_failed_login_num),
      "need unlock",
      (current_gmt - last_failed_login_timestamp >= failed_login_limit_time));

  return ret;
}

int ObMPConnect::setup_user_resource_group(ObSMConnection& conn, const uint64_t tenant_id, const uint64_t user_id)
{
  int ret = OB_SUCCESS;
  uint64_t group_id = 0;
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid tenant", K(tenant_id), K(ret));
  } else if (OB_FAIL(G_RES_MGR.get_mapping_rule_mgr().get_group_id_by_user(tenant_id, user_id, group_id))) {
    LOG_WARN("fail get group id by user", K(user_id), K(tenant_id), K(user_name_), K(ret));
  } else {
    conn.group_id_ = group_id;
    LOG_INFO("setup user resource group OK", K(user_id), K(tenant_id), K(user_name_), K(group_id));
  }
  LOG_DEBUG("setup user resource group", K(user_name_), K(tenant_id), K(ret));
  return ret;
}

int ObMPConnect::update_login_stat_in_trans(
    const uint64_t tenant_id, const bool is_login_succ, ObSchemaGetterGuard& schema_guard)
{
  int ret = OB_SUCCESS;
  uint64_t user_id = OB_INVALID_ID;
  bool is_exist = false;
  int64_t current_failed_login_num = INT64_MAX;
  int64_t last_failed_login_timestamp = INT64_MAX;
  int64_t failed_login_limit_num = INT64_MAX;
  int64_t failed_login_limit_time = INT64_MAX;
  ObMySQLTransaction trans;
  bool commit = true;

  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid tenant", K(tenant_id), K(ret));
  } else if (OB_FAIL(schema_guard.check_user_exist(
                 tenant_id, user_name_, ObString(OB_DEFAULT_HOST_NAME), is_exist, &user_id))) {
    LOG_WARN("fail to check user exist", K(ret));
  } else if (!is_exist) {
    // do nothing
  } else if (OB_FAIL(schema_guard.get_user_profile_failed_login_limits(
                 user_id, failed_login_limit_num, failed_login_limit_time))) {
    LOG_WARN("fail to get user id and profile limit", K(ret));
  } else if (failed_login_limit_num == INT64_MAX) {
    // unlimited do nothing
  } else if (OB_FAIL(trans.start(gctx_.sql_proxy_))) {
    LOG_WARN("fail to start transaction", K(ret));
  } else if (OB_FAIL(
                 get_last_failed_login_info(user_id, trans, current_failed_login_num, last_failed_login_timestamp))) {
    LOG_WARN("fail to get current user failed login num", K(ret));
  } else if (OB_LIKELY(is_login_succ)) {
    if (OB_UNLIKELY(current_failed_login_num != 0)) {
      if (OB_FAIL(clear_current_user_failed_login_num(user_id, trans))) {
        LOG_WARN("fail to clear current user failed login", K(ret));
      }
    }
  } else {  // login failed with wrong password
    if (OB_FAIL(update_current_user_failed_login_num(user_id, trans, current_failed_login_num + 1))) {
      LOG_WARN("fail to clear current user failed login", K(ret));
    } else if (current_failed_login_num + 1 == failed_login_limit_num ||
               (current_failed_login_num + 1 > failed_login_limit_num &&
                   ObTimeUtil::current_time() - last_failed_login_timestamp > USECS_PER_SEC * 10)) {
      if (OB_FAIL(switch_lock_status_for_current_login_user(tenant_id, true))) {
        LOG_WARN("fail to lock current login user", K(ret));
      }
    }
    commit = (current_failed_login_num < failed_login_limit_num);
  }

  if (trans.is_started()) {
    int temp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCC(ret) && commit))) {
      LOG_WARN("trans end failed", "is_commit", OB_SUCC(ret) && commit, K(ret), K(temp_ret));
      ret = OB_SUCC(ret) ? temp_ret : ret;
    }
  }
  LOG_DEBUG("update_login_stat_in_trans check",
      K(commit),
      K(current_failed_login_num),
      K(last_failed_login_timestamp),
      K(failed_login_limit_num),
      K(failed_login_limit_time));
  return ret;
}

int ObMPConnect::get_last_failed_login_info(
    const uint64_t user_id, ObISQLClient& sql_client, int64_t& current_failed_login_num, int64_t& last_failed_timestamp)
{

  int ret = OB_SUCCESS;
  uint64_t tenant_id = extract_tenant_id(user_id);
  ObSqlString select_sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    if (!is_valid_tenant_id(tenant_id) || !is_valid_id(user_id)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid id", K(tenant_id), K(user_id), K(ret));
    } else if (OB_FAIL(select_sql.append_fmt("SELECT failed_login_attempts, gmt_modified FROM `%s`"
                                             " WHERE tenant_id = %lu and user_id = %lu FOR UPDATE",
                   OB_ALL_TENANT_USER_FAILED_LOGIN_STAT_TNAME,
                   tenant_id,
                   user_id))) {
      LOG_WARN("append string failed", K(ret));
    } else if (OB_FAIL(sql_client.read(res, tenant_id, select_sql.ptr()))) {
      LOG_WARN("fail to execute lock user", K(ret));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", K(ret));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        current_failed_login_num = 0;
        last_failed_timestamp = 0;
      } else {
        LOG_WARN("get result failed", K(ret));
      }
    } else if (OB_FAIL(result->get_int("failed_login_attempts", current_failed_login_num))) {
      LOG_WARN("fail to get int value", K(ret));
    } else if (OB_FAIL(result->get_timestamp("gmt_modified", NULL, last_failed_timestamp))) {
      LOG_WARN("fail get timestamp value", K(ret));
    } else if (result->next() != OB_ITER_END) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("more than one row returned", K(ret));
    }

    /*
    int temp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (temp_ret = result->close())) {
      LOG_WARN("fail to close", K(temp_ret));
    }
    ret = OB_SUCC(ret) ? temp_ret : ret;
    */
    res.~ReadResult();
  }
  return ret;
}

int ObMPConnect::clear_current_user_failed_login_num(const uint64_t user_id, ObISQLClient& sql_client)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = extract_tenant_id(user_id);
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (!is_valid_id(user_id) || !is_valid_tenant_id(tenant_id) || user_name_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid id", K(user_id), K(tenant_id), K_(user_name), K(ret));
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM `%s` "
                                    " WHERE tenant_id = %lu and user_id = %lu",
                 OB_ALL_TENANT_USER_FAILED_LOGIN_STAT_TNAME,
                 tenant_id,
                 user_id))) {
    LOG_WARN("append table name failed", K(ret));
  } else if (OB_FAIL(sql_client.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to do update", K(ret), K(sql), K(tenant_id));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", K(ret), K(affected_rows), K(sql));
  }
  return ret;
}

int ObMPConnect::update_current_user_failed_login_num(
    const uint64_t user_id, ObISQLClient& sql_client, int64_t new_failed_login_num)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = extract_tenant_id(user_id);
  ObSqlString sql;
  ObSqlString values;
  int64_t affected_rows = 0;

  if (!is_valid_id(user_id) || !is_valid_tenant_id(tenant_id) || user_name_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid id", K(user_id), K(tenant_id), K_(user_name), K(ret));
  } else if (OB_FAIL(sql.assign_fmt("INSERT INTO `%s` (", OB_ALL_TENANT_USER_FAILED_LOGIN_STAT_TNAME))) {
    LOG_WARN("append table name failed", K(ret));
  } else {
    SQL_COL_APPEND_VALUE(sql, values, tenant_id, "tenant_id", "%lu");
    SQL_COL_APPEND_VALUE(sql, values, user_id, "user_id", "%lu");
    SQL_COL_APPEND_ESCAPE_STR_VALUE(sql, values, user_name_.ptr(), user_name_.length(), "user_name");
    SQL_COL_APPEND_VALUE(sql, values, new_failed_login_num, "failed_login_attempts", "%ld");
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(sql.append_fmt(", gmt_modified) VALUES (%.*s, now(6))"
                               " ON DUPLICATE KEY UPDATE"
                               " failed_login_attempts = %ld"
                               ", last_failed_login_svr_ip = \"%.*s\"",
            static_cast<int32_t>(values.length()),
            values.ptr(),
            new_failed_login_num,
            (new_failed_login_num == 0 ? 0 : client_ip_.length()),
            client_ip_.ptr()))) {
      LOG_WARN("append sql failed", K(ret));
    } else if (OB_FAIL(sql_client.write(tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to do update", K(ret), K(sql), K(tenant_id));
    } else if (!is_single_row(affected_rows) && !is_double_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected rows", K(ret), K(affected_rows), K(sql));
    }
  }

  return ret;
}

int ObMPConnect::check_password_expired(
    const uint64_t tenant_id, ObSchemaGetterGuard& schema_guard, ObSQLSessionInfo& session)
{
  int ret = OB_SUCCESS;
  uint64_t user_id = OB_INVALID_ID;
  bool is_exist = false;
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid tenant", K(tenant_id), K(ret));
  } else if (OB_FAIL(schema_guard.check_user_exist(
                 tenant_id, user_name_, ObString(OB_DEFAULT_HOST_NAME), is_exist, &user_id))) {
    LOG_WARN("fail to check user exist", K(ret));
  } else if (!is_exist) {
    // do nothing
  } else if (OB_FAIL(ObPrivilegeCheck::check_password_expired_on_connection(user_id, schema_guard, session))) {
    LOG_WARN("fail to check password expired", K(ret));
  }
  return ret;
}

int ObMPConnect::extract_user_tenant(const ObString& in)
{
  // resolve tenantname and username
  int ret = OB_SUCCESS;
  const char* user_pos = in.ptr();
  const char* at_pos = in.find('@');  // use @ as seperator
  const char* tenant_pos = at_pos + 1;

  // sanity check
  if (NULL == at_pos) {
    user_name_ = extract_user_name(in);
    tenant_name_ = ObString::make_empty_string();  // default to sys tenant
    LOG_INFO("username and tenantname", K_(user_name), K_(tenant_name));
  } else {
    // Accept empty username.  Empty username is one of normal
    // usernames that we can create user with empty name.
    if (at_pos - user_pos < 0) {
      ret = OB_ERR_USER_EMPTY;
      LOG_WARN("Must Provide user name to login", K(ret));
    } else {
      int64_t tenant_len = in.length() - (tenant_pos - user_pos);
      if (tenant_len > OB_MAX_TENANT_NAME_LENGTH || tenant_len <= 0) {
        ret = OB_ERR_INVALID_TENANT_NAME;
        LOG_WARN("Violate with tenant length limit", "max", OB_MAX_TENANT_NAME_LENGTH, "actual", tenant_len, K(ret));
      }
      // extract
      if (OB_SUCC(ret)) {
        ObString username(at_pos - user_pos, user_pos);
        ObString tenantname(in.length() - username.length() - 1, tenant_pos);
        user_name_ = extract_user_name(username);
        tenant_name_ = tenantname;
        LOG_DEBUG("username and tenantname", K_(user_name), K_(tenant_name));
      }
    }
  }
  return ret;
}

ObString ObMPConnect::extract_user_name(const ObString& in) const
{
  ObString user_name;
  if (in.length() > 1 && '\'' == in[0] && '\'' == in[in.length() - 1]) {
    user_name.assign_ptr(in.ptr() + 1, in.length() - 2);
  } else {
    user_name = in;
  }
  return user_name;
}

int ObMPConnect::get_tenant_id(uint64_t& tenant_id)
{
  int ret = OB_SUCCESS;
  tenant_id = OB_INVALID_ID;
  if (tenant_name_.empty()) {
    tenant_id = OB_SYS_TENANT_ID;
  } else {
    // OB_ASSERT(gctx_.schema_service_);
    if (OB_ISNULL(gctx_.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid schema service", K(ret), K(gctx_.schema_service_));
    } else {
      schema::ObSchemaGetterGuard guard;
      if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
        LOG_WARN("get_schema_guard failed", K(ret));
      } else if (OB_FAIL(guard.get_tenant_id(tenant_name_, tenant_id))) {
        LOG_WARN("get_tenant_id failed", K(ret), K_(tenant_name));
      }
    }
  }
  return ret;
}

int64_t ObMPConnect::get_user_id()
{
  return OB_SYS_USER_ID;
}

int64_t ObMPConnect::get_database_id()
{
  return OB_SYS_DATABASE_ID;
}

int ObMPConnect::get_conn_id(uint32_t& conn_id) const
{
  int ret = OB_SUCCESS;
  bool is_found = false;
  ObString key_str;
  key_str.assign_ptr(OB_MYSQL_CONNECTION_ID, static_cast<int32_t>(STRLEN(OB_MYSQL_CONNECTION_ID)));
  for (int64_t i = 0; i < hsr_.get_connect_attrs().count() && OB_SUCC(ret) && !is_found; ++i) {
    ObStringKV kv = hsr_.get_connect_attrs().at(i);
    if (key_str == kv.key_) {
      ObObj value;
      value.set_varchar(kv.value_);
      ObArenaAllocator allocator(ObModIds::OB_SQL_EXPR);
      ObCastCtx cast_ctx(&allocator, NULL, CM_NONE, ObCharset::get_system_collation());
      EXPR_GET_UINT32_V2(value, conn_id);
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to cast connection id to uint32", K(kv.value_), K(ret));
      } else {
        is_found = true;
      }
    }
  }

  if (OB_SUCC(ret) && !is_found) {
    ret = OB_ENTRY_NOT_EXIST;
  }

  return ret;
}

int ObMPConnect::get_proxy_conn_id(uint64_t& proxy_conn_id) const
{
  int ret = OB_SUCCESS;
  bool is_found = false;
  ObString key_str;
  key_str.assign_ptr(OB_MYSQL_PROXY_CONNECTION_ID, static_cast<int32_t>(STRLEN(OB_MYSQL_PROXY_CONNECTION_ID)));
  for (int64_t i = 0; i < hsr_.get_connect_attrs().count() && OB_SUCC(ret) && !is_found; ++i) {
    const ObStringKV& kv = hsr_.get_connect_attrs().at(i);
    if (key_str == kv.key_) {
      ObObj value;
      value.set_varchar(kv.value_);
      ObArenaAllocator allocator(ObModIds::OB_SQL_EXPR);
      ObCastCtx cast_ctx(&allocator, NULL, CM_NONE, ObCharset::get_system_collation());
      EXPR_GET_UINT64_V2(value, proxy_conn_id);
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to cast proxy connection id to uint32", K(kv.value_), K(ret));
      } else {
        is_found = true;
      }
    }
  }

  if (OB_SUCC(ret) && !is_found) {
    // if fail to find proxy_connection_id, ignore it, compatible with old obproxyro's connection
    proxy_conn_id = 0;
  }
  return ret;
}

int ObMPConnect::get_proxy_sess_create_time(int64_t& sess_create_time) const
{
  int ret = OB_SUCCESS;
  bool is_found = false;
  ObString key_str;
  key_str.assign_ptr(
      OB_MYSQL_PROXY_SESSION_CREATE_TIME_US, static_cast<int32_t>(STRLEN(OB_MYSQL_PROXY_SESSION_CREATE_TIME_US)));
  for (int64_t i = 0; i < hsr_.get_connect_attrs().count() && OB_SUCC(ret) && !is_found; ++i) {
    const ObStringKV& kv = hsr_.get_connect_attrs().at(i);
    if (key_str == kv.key_) {
      ObObj value;
      value.set_varchar(kv.value_);
      ObArenaAllocator allocator(ObModIds::OB_SQL_EXPR);
      ObCastCtx cast_ctx(&allocator, NULL, CM_NONE, ObCharset::get_system_collation());
      EXPR_GET_INT64_V2(value, sess_create_time);
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to cast proxy session create time", K(kv.value_), K(ret));
      } else {
        is_found = true;
      }
    }
  }

  if (OB_SUCC(ret) && !is_found) {
    // if fail to find __proxy_sess_create_time, ignore it, compatible with old obproxyro's connection
    sess_create_time = 0;
  }
  return ret;
}

int ObMPConnect::get_proxy_capability(uint64_t& cap) const
{
  int ret = OB_SUCCESS;
  cap = 0;
  bool is_capability_flag_found = false;
  ObStringKV kv;
  for (int64_t i = 0; !is_capability_flag_found && i < hsr_.get_connect_attrs().count(); ++i) {
    kv = hsr_.get_connect_attrs().at(i);
    if (kv.key_ == OB_MYSQL_CAPABILITY_FLAG) {
      is_capability_flag_found = true;
    }
  }

  if (is_capability_flag_found) {
    ObObj value;
    value.set_varchar(kv.value_);
    ObArenaAllocator allocator(ObModIds::OB_SQL_EXPR);
    ObCastCtx cast_ctx(&allocator, NULL, CM_NONE, ObCharset::get_system_collation());
    EXPR_GET_UINT64_V2(value, cap);
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to cast capability flag to uint64", K_(kv.value), K(ret));
    }
  }
  return ret;
}

int ObMPConnect::check_update_proxy_capability(ObSMConnection& conn) const
{
  int ret = OB_SUCCESS;
  uint64_t client_proxy_cap = 0;
  bool is_monotonic_weak_read = transaction::ObWeakReadUtil::enable_monotonic_weak_read(conn.tenant_id_);
  if (OB_FAIL(get_proxy_capability(client_proxy_cap))) {
    LOG_WARN("get proxy capability fail", K(ret));
  } else {
    // set proxy_capability_ to tell proxy which features observer supports
    ObProxyCapabilityFlags server_proxy_cap_flag;
    server_proxy_cap_flag.cap_flags_.OB_CAP_PARTITION_TABLE = 1;
    server_proxy_cap_flag.cap_flags_.OB_CAP_CHANGE_USER = 1;
    server_proxy_cap_flag.cap_flags_.OB_CAP_READ_WEAK = 1;
    server_proxy_cap_flag.cap_flags_.OB_CAP_CHECKSUM = 1;
    if (is_monotonic_weak_read) {
      server_proxy_cap_flag.cap_flags_.OB_CAP_SAFE_WEAK_READ = 1;
    } else {
      server_proxy_cap_flag.cap_flags_.OB_CAP_SAFE_WEAK_READ = 0;
    }
    server_proxy_cap_flag.cap_flags_.OB_CAP_PRIORITY_HIT = 1;
    server_proxy_cap_flag.cap_flags_.OB_CAP_CHECKSUM_SWITCH = 1;
    server_proxy_cap_flag.cap_flags_.OB_CAP_EXTRA_OK_PACKET_FOR_OCJ = 1;
    server_proxy_cap_flag.cap_flags_.OB_CAP_OB_PROTOCOL_V2 = 1;
    server_proxy_cap_flag.cap_flags_.OB_CAP_EXTRA_OK_PACKET_FOR_STATISTICS = 1;
    server_proxy_cap_flag.cap_flags_.OB_CAP_ABUNDANT_FEEDBACK = 1;
    server_proxy_cap_flag.cap_flags_.OB_CAP_PL_ROUTE = 1;
    server_proxy_cap_flag.cap_flags_.OB_CAP_PROXY_REROUTE = 1;
    conn.proxy_cap_flags_.capability_ =
        (server_proxy_cap_flag.capability_ & client_proxy_cap);  // if old java client, set it 0

    LOG_DEBUG("Negotiated capability",
        K(conn.proxy_cap_flags_.is_proxy_reroute_support()),
        K(conn.proxy_cap_flags_.is_ob_protocol_v2_support()));
  }
  return ret;
}

int ObMPConnect::get_proxy_scramble(ObString& proxy_scramble) const
{
  int ret = OB_SUCCESS;
  bool is_found = false;
  ObString key_str;
  key_str.assign_ptr(OB_MYSQL_SCRAMBLE, static_cast<int32_t>(STRLEN(OB_MYSQL_SCRAMBLE)));
  for (int64_t i = 0; i < hsr_.get_connect_attrs().count() && OB_SUCC(ret) && !is_found; ++i) {
    const ObStringKV& kv = hsr_.get_connect_attrs().at(i);
    if (key_str == kv.key_) {
      proxy_scramble.assign_ptr(kv.value_.ptr(), kv.value_.length());
      is_found = true;
    }
  }

  if (OB_SUCC(ret) && !is_found) {
    // if fail to find proxy_scramble, ignore it, compatible with old proxy
    proxy_scramble.reset();
  }
  return ret;
}

int ObMPConnect::get_client_ip(ObString& client_ip) const
{
  int ret = OB_SUCCESS;
  bool is_found = false;
  ObString key_str;
  key_str.assign_ptr(OB_MYSQL_CLIENT_IP, static_cast<int32_t>(STRLEN(OB_MYSQL_CLIENT_IP)));
  for (int64_t i = 0; i < hsr_.get_connect_attrs().count() && OB_SUCC(ret) && !is_found; ++i) {
    const ObStringKV& kv = hsr_.get_connect_attrs().at(i);
    if (key_str == kv.key_) {
      client_ip.assign_ptr(kv.value_.ptr(), kv.value_.length());
      is_found = true;
    }
  }

  if (OB_SUCC(ret) && !is_found) {
    // if fail to find, ignore it, compatible with old proxy
    client_ip.reset();
  }
  return ret;
}

int ObMPConnect::check_user_cluster(const ObString& server_cluster, const int64_t server_cluster_id) const
{
  int ret = OB_SUCCESS;
  ObString cluster_kv(OB_MYSQL_CLUSTER_NAME);
  ObString cluster_id_key(OB_MYSQL_CLUSTER_ID);

  bool found_cluster = false;
  bool found_cluster_id = false;
  for (int64_t i = 0; OB_SUCC(ret) && (!found_cluster || !found_cluster_id) && i < hsr_.get_connect_attrs().count();
       ++i) {
    const ObStringKV& kv = hsr_.get_connect_attrs().at(i);
    if (!found_cluster && cluster_kv == kv.key_) {
      if (server_cluster != kv.value_) {
        ret = OB_CLUSTER_NO_MATCH;
        LOG_WARN(
            "user cluster is not match to server cluster", "user cluster", kv.value_, "server cluster", server_cluster);
      }
      found_cluster = true;
    } else if (!found_cluster_id && cluster_id_key == kv.key_) {
      int64_t user_cluster_id = 0;
      ObObj value;
      value.set_varchar(kv.value_);
      ObArenaAllocator allocator(ObModIds::OB_SQL_EXPR);
      ObCastCtx cast_ctx(&allocator, NULL, CM_NONE, ObCharset::get_system_collation());
      EXPR_GET_INT64_V2(value, user_cluster_id);
      if (OB_FAIL(ret)) {
        ret = OB_CLUSTER_NO_MATCH;
        LOG_WARN("fail to cast user_cluster_id to int64", K(kv.value_), K(ret));
      } else {
        if (server_cluster_id != user_cluster_id) {
          ret = OB_CLUSTER_NO_MATCH;
          LOG_WARN("user cluster id is not match to server cluster id",
              "user cluster id",
              kv.value_,
              "server cluster id",
              server_cluster_id);
        }
      }
      found_cluster_id = true;
    } else {
      // do nothing
    }
  }
  return ret;
}

// check common property for obproxy or OCJ
int ObMPConnect::check_common_property(ObSMConnection& conn, ObMySQLCapabilityFlags& client_cap)
{
  int ret = OB_SUCCESS;
  uint64_t proxy_sessid = 0;
  int64_t sess_create_time = 0;
  if (OB_FAIL(check_user_cluster(ObString::make_string(GCONF.cluster), GCONF.cluster_id))) {
    LOG_WARN("fail to check user cluster", K(ret));
  } else if (OB_FAIL(check_update_proxy_capability(conn))) {
    LOG_WARN("fail to check_update_proxy_capability", K(ret));
  } else if (OB_FAIL(get_proxy_conn_id(proxy_sessid))) {
    LOG_WARN("get proxy connection id fail", K(ret));
  } else if (OB_FAIL(get_proxy_sess_create_time(sess_create_time))) {
    LOG_WARN("get proxy session create time fail", K(ret));
  } else {
    conn.proxy_sessid_ = proxy_sessid;
    conn.sess_create_time_ = sess_create_time;
    if (conn.proxy_cap_flags_.is_ob_protocol_v2_support()) {
      // when used 2.0 protocol, do not use mysql compress
      client_cap.cap_flags_.OB_CLIENT_COMPRESS = 0;
    } else {
      if (conn.proxy_cap_flags_.is_checksum_support()) {
        client_cap.cap_flags_.OB_CLIENT_COMPRESS = 1;
      } else {
        client_cap.cap_flags_.OB_CLIENT_COMPRESS = 0;
      }
    }
  }
  return ret;
}

int ObMPConnect::check_client_property(ObSMConnection& conn)
{
  int ret = OB_SUCCESS;
  ObMySQLCapabilityFlags client_cap = hsr_.get_capability_flags();
  ObString client_ip;

  if (conn.is_java_client_) {
    // the connection is from oceanbase-connector-java(OCJ)
    if (OB_FAIL(check_common_property(conn, client_cap))) {
      LOG_WARN("fail to check common property", K(ret));
    } else {
      // if ocj enable extra_ok_packet, then track the system variables
      if (conn.proxy_cap_flags_.is_extra_ok_packet_for_ocj_support()) {
        client_cap.cap_flags_.OB_CLIENT_SESSION_TRACK = 1;
      }
    }
  } else if (conn.is_proxy_) {
    // the connection is from obproxy, set CLIENT_SESSION_TRACK flag
    client_cap.cap_flags_.OB_CLIENT_SESSION_TRACK = 1;

    ObString proxy_scramble;
    if (OB_FAIL(check_common_property(conn, client_cap))) {
      LOG_WARN("fail to check common property", K(ret));
    } else if (OB_FAIL(get_proxy_scramble(proxy_scramble))) {
      LOG_WARN("get proxy scramble fail", K(ret));
    } else if (OB_FAIL(extract_real_scramble(proxy_scramble))) {
      LOG_WARN("extract real scramble fail", K(ret));
    } else if (OB_FAIL(get_client_ip(client_ip))) {
      LOG_WARN("get client ip fail", K(ret));
    } else if (OB_FAIL(set_proxy_version(conn))) {
      LOG_WARN("get proxy version fail", K(ret));
    }
  } else {
    // login observer directly
  }

  if (client_ip.empty()) {
    const char* peer_ip = get_peer_ip_str(client_ip_buf_, common::MAX_IP_ADDR_LENGTH, req_->get_ez_req());
    client_ip_.assign_ptr(peer_ip, static_cast<int32_t>(STRLEN(peer_ip)));
  } else {
    client_ip_ = client_ip;
  }

  hsr_.set_capability_flags(client_cap);
  conn.cap_flags_ = client_cap;
  return ret;
}

int ObMPConnect::switch_sessid(ObSMConnection& conn)
{
  int ret = OB_SUCCESS;
  const uint32_t prev_sessid = conn.sessid_;
  if (OB_ISNULL(gctx_.session_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("current session mgr is NULL", K(ret), K(gctx_.session_mgr_));
  } else if (gctx_.session_mgr_->extract_server_id(prev_sessid) == 0 && !tenant_name_.empty()) {
    ret = OB_SERVER_IS_INIT;
    LOG_WARN("prev sessid is invalid, maybe server is initializing", K(prev_sessid), K(ret));
  } else if (conn.is_proxy_) {
    uint32_t sessid = 0;
    if (OB_FAIL(get_conn_id(sessid))) {
      LOG_WARN("get connection id fail", K(ret));
    } else {
      conn.sessid_ = sessid;
      LOG_DEBUG("proxy session id ", K(prev_sessid), K(sessid), "proxy_sessid", conn.proxy_sessid_);

      if (conn.sessid_ != prev_sessid) {
        // since current session use proxy sessid, clear the observer generated sessid
        if (OB_FAIL(gctx_.session_mgr_->mark_sessid_unused(prev_sessid))) {
          LOG_WARN("fail to mark sessid", K(sessid), K(prev_sessid));
        } else if ((ObSQLSessionMgr::SERVER_SESSID_TAG & sessid) &&
                   GCTX.server_id_ == gctx_.session_mgr_->extract_server_id(sessid)) {
          if (OB_FAIL(gctx_.session_mgr_->mark_sessid_used(sessid))) {
            LOG_WARN("fail to mark sessid", K(ret), K(sessid), K(GCTX.server_id_));
            conn.is_need_clear_sessid_ = false;
          }
        } else {
          conn.is_need_clear_sessid_ = false;
        }
      }
    }
  } else {
    // do nothing
  }
  return ret;
}

int ObMPConnect::extract_real_scramble(const ObString& proxy_scramble)
{
  int ret = OB_SUCCESS;
  ObSMConnection& conn = *get_conn();
  if (OB_UNLIKELY(STRLEN(conn.scramble_buf_) != ObSMConnection::SCRAMBLE_BUF_SIZE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN(
        "server orign scramble is unexpected", "length", STRLEN(conn.scramble_buf_), K(conn.scramble_buf_), K(ret));
  } else {
    if (ObSMConnection::SCRAMBLE_BUF_SIZE == proxy_scramble.length()) {
      unsigned char real_scramble_buf[ObSMConnection::SCRAMBLE_BUF_SIZE] = {0};
      // The value of '__proxy_scramble' is not real scramble of proxy
      // In fact, it __proxy_scramble = proxy's xor server's scramble, just for simple encrypt
      // Here we need get orig proxy's scramble by this -- proxy's scramble = __proxy_scramble xor server's scramble
      if (OB_FAIL(ObEncryptedHelper::my_xor(reinterpret_cast<const unsigned char*>(proxy_scramble.ptr()),
              reinterpret_cast<const unsigned char*>(conn.scramble_buf_),
              static_cast<uint32_t>(ObSMConnection::SCRAMBLE_BUF_SIZE),
              real_scramble_buf))) {
        LOG_WARN("failed to calc xor real_scramble_buf", K(ret));
      } else {
        MEMCPY(conn.scramble_buf_, real_scramble_buf, ObSMConnection::SCRAMBLE_BUF_SIZE);
      }
    } else {
      const ObString old_scramble("aaaaaaaabbbbbbbbbbbb");
      MEMCPY(conn.scramble_buf_, old_scramble.ptr(), old_scramble.length());
    }
  }
  return ret;
}

int ObMPConnect::verify_connection(const uint64_t tenant_id) const
{
  int ret = OB_SUCCESS;
  const int32_t FAKE_PORT = 324;
  const char* IPV4_LOCAL_STR = "127.0.0.1";
  const char* IPV6_LOCAL_STR = "::1";
  ObSMConnection* conn = get_conn();
  ObAddr ipv4_local, ipv6_local;

  // The monitor and the diagnose are not allowed to login by remote
  // client.  Here we check whether peer is local address(127.0.0.1 or ::1)
  // or public address for self.  If client connects to server with IP of
  // 127.0.0.1 we'll get peer IP of 127.0.0.1, it is server's public
  // address otherwise. It all depends upon which interface the client
  // is using.
  if (OB_ISNULL(conn)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("connection is NULL", K(ret));
  } else if (!ipv4_local.set_ip_addr(IPV4_LOCAL_STR, FAKE_PORT) || !ipv6_local.set_ip_addr(IPV6_LOCAL_STR, FAKE_PORT)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("set ObAddr failed", K(ret), K(IPV4_LOCAL_STR), K(IPV6_LOCAL_STR), K(FAKE_PORT));
  } else if (OB_MONITOR_TENANT_ID == conn->tenant_id_) {
    if (!(get_peer().is_equal_except_port(ipv4_local) || get_peer().is_equal_except_port(ipv6_local) ||
            get_peer().is_equal_except_port(GCTX.self_addr_))) {
      ret = OB_ERR_NO_PRIVILEGE;
      LOG_WARN("The monitor isn't allowed to login by remote client", K(get_peer()), K(ret));
    }
  } else if (OB_DIAG_TENANT_ID == conn->tenant_id_) {
    if (!(get_peer().is_equal_except_port(ipv4_local) || get_peer().is_equal_except_port(ipv6_local) ||
            get_peer().is_equal_except_port(GCTX.self_addr_))) {
      ret = OB_ERR_NO_PRIVILEGE;
      LOG_WARN("The diagnose isn't allowed to login by remote client", K(get_peer()), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    // if normal tenant can not login with error variables, sys tenant can recover the error variables
    // but if sys tenant set error variables, no one can recover it.
    // so we need leave a backdoor for root@sys from 127.0.0.1 to skip this verifing
    if (OB_SYS_TENANT_ID == tenant_id && 0 == user_name_.compare(OB_SYS_USER_NAME) &&
        (0 == client_ip_.compare(IPV4_LOCAL_STR) || 0 == client_ip_.compare(IPV6_LOCAL_STR))) {
      LOG_DEBUG("this is root@sys user from local host, no need verify_ip_white_list", K(ret));
    } else if (OB_SYS_TENANT_ID == tenant_id && (SS_INIT == GCTX.status_ || SS_STARTING == GCTX.status_)) {
      LOG_INFO("server is initializing, ignore verify_ip_white_list", "status", GCTX.status_, K(ret));
    } else if (OB_FAIL(verify_ip_white_list(tenant_id))) {
      LOG_WARN("failed to verify_ip_white_list", K(ret));
    }
  }
  return ret;
}

int ObMPConnect::check_update_tenant_id(ObSMConnection& conn, uint64_t& tenant_id)
{
  int ret = OB_SUCCESS;
  if (tenant_name_.case_compare(OB_MONITOR_TENANT_NAME) == 0) {
    tenant_name_ = ObString::make_string(OB_SYS_TENANT_NAME);
    tenant_id = OB_SYS_TENANT_ID;
    conn.tenant_id_ = tenant_id;
    conn.resource_group_id_ = OB_MONITOR_TENANT_ID;
  } else if (tenant_name_.case_compare(OB_DIAG_TENANT_NAME) == 0) {
    tenant_name_ = user_name_;
    user_name_ = ObString::make_string("root");
    if (OB_FAIL(get_tenant_id(tenant_id))) {
      LOG_WARN("get_tenant_id failed", K(ret));
    } else {
      conn.tenant_id_ = tenant_id;
      conn.resource_group_id_ = OB_DIAG_TENANT_ID;
    }
  } else {
    if (OB_FAIL(get_tenant_id(tenant_id))) {
      if (OB_ERR_TENANT_IS_LOCKED == ret) {
        LOG_WARN("tenant is locked", K(ret), K_(tenant_name));
        LOG_USER_ERROR(OB_ERR_TENANT_IS_LOCKED, tenant_name_.length(), tenant_name_.ptr());
      } else {
        LOG_WARN("get_tenant_id failed", K(ret));
      }
    } else {
      conn.tenant_id_ = tenant_id;
      conn.resource_group_id_ = tenant_id;
    }
  }
  return ret;
}

int ObMPConnect::verify_identify(ObSMConnection& conn, ObSQLSessionInfo& session, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  // at this point, tenant_id and sessid are valid
  ObSessionStatEstGuard guard(tenant_id, conn.sessid_);
  ObSQLSessionInfo::LockGuard lock_guard(session.get_query_lock());
  easy_request_t* ez_req = NULL;
  if (OB_ISNULL(req_) || OB_ISNULL(ez_req = req_->get_request())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("null request", K(ret), K(req_->get_request()));
  } else if (OB_FAIL(load_privilege_info(session))) {
    int pre_ret = ret;
    if (SS_INIT == GCTX.status_) {
      ret = OB_SERVER_IS_INIT;
    }
    LOG_WARN("load privilege info fail", K(pre_ret), K(ret), K(GCTX.status_));
  } else if (ORACLE_MODE == session.get_compatibility_mode() &&
             0 == hsr_.get_capability_flags().cap_flags_.OB_CLIENT_SUPPORT_ORACLE_MODE) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "Oracle tenant for current client driver is");
  } else {
    session.update_last_active_time();
    if (NULL != ez_req->ms) {
      session.set_conn(ez_req->ms->c);
    }
    if (NULL != gctx_.sql_engine_) {
      session.set_plan_cache_manager(gctx_.sql_engine_->get_plan_cache_manager());
    }
    session.set_peer_addr(req_->get_peer());
    session.set_trans_type(transaction::ObTransType::TRANS_USER);
    session.set_tenant(tenant_name_, tenant_id);
    session.set_proxy_cap_flags(conn.proxy_cap_flags_);
    session.set_login_tenant_id(tenant_id);
    // Check tenant after set tenant session is necessary!
    // Because if another client is deleting this tenant while the
    // session doesn't has been contructed completely, omt
    // woundn't be awared of this session. So that this session
    // maybe run normally but tenant has already deleted.
    if (NULL != gctx_.omt_) {
      if (OB_FAIL(gctx_.omt_->get_tenant(conn.resource_group_id_, conn.tenant_))) {
        LOG_WARN("can't get tenant", K_(conn.tenant_id), K(ret));
      } else if (OB_ISNULL(conn.tenant_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("null tenant", K(ret), K(conn.tenant_id_));
      } else if (conn.tenant_->has_stopped()) {
        ret = OB_TENANT_NOT_IN_SERVER;
        LOG_WARN("tenant is deleting, reject connecting", K(ret), "tenant_id", conn.tenant_id_);
      } else if (OB_FAIL(conn.tenant_->try_rdlock(conn.handle_))) {
        conn.tenant_ = NULL;
        LOG_WARN("get tenant lock fail", K(ret));
      } else {
        conn.is_tenant_locked_ = true;
      }
    }

    // at this point, conn.tenant_id_ and sessid are already set and won't be modified
    if (conn.tenant_id_ != 0 && conn.sessid_ != 0) {
      EVENT_INC(ACTIVE_SESSIONS);
      conn.has_inc_active_num_ = true;
    }

    // init_connect is not executed for users that have the super privilege
    if (OB_SUCC(ret) && !(OB_PRIV_SUPER & session.get_user_priv_set())) {
      ObString sql_str;
      if (OB_FAIL(session.get_init_connect(sql_str))) {
        LOG_WARN("get sys variable init_connect failed.", K(ret));
      } else {
        if (0 == sql_str.compare("")) {
          // do nothing
        } else {
          if (OB_FAIL(init_connect_process(sql_str, session))) {
            LOG_WARN("init connect failed.", K(sql_str), K(ret));
          }
        }
      }
      LOG_DEBUG("INIT_CONNECT", K(ret), K(sql_str));
      // a statement that has a error will causing client connections to fail
      if (OB_SUCCESS != ret) {
        disconnect();
      }
    }

    // set session state
    if (OB_SUCC(ret)) {
      if (OB_FAIL(session.set_session_state(SESSION_SLEEP))) {
        LOG_WARN("fail to set session state", K(ret));
      }
    }
  }
  return ret;
}

int ObMPConnect::verify_ip_white_list(const uint64_t tenant_id) const
{
  int ret = OB_SUCCESS;
  const ObTenantSchema* tenant_schema = NULL;
  const ObSysVariableSchema* sys_variable_schema = NULL;
  schema::ObSchemaGetterGuard schema_guard;
  ObString var_name(OB_SV_TCP_INVITED_NODES);
  const ObSysVarSchema* sysvar = NULL;
  if (OB_UNLIKELY(client_ip_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("client_ip is empty", K(ret));
  } else if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get_schema_guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_schema))) {
    LOG_WARN("get tenant info failed", K(ret));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_schema is null", K(ret));
  } else if (OB_FAIL(schema_guard.get_sys_variable_schema(tenant_id, sys_variable_schema))) {
    LOG_WARN("get sys variable schema failed", K(ret));
  } else if (OB_ISNULL(sys_variable_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys variable schema is null", K(ret));
  } else if (OB_FAIL(sys_variable_schema->get_sysvar_schema(var_name, sysvar))) {
    if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_1470) {
      LOG_INFO("maybe server is upgrading, ignore verified", "version", GET_MIN_CLUSTER_VERSION(), K(ret));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get_sysvar_schema", K(ret));
    }
  } else {
    ObString var_value = sysvar->get_value();
    if (!ObHostnameStuct::is_in_white_list(client_ip_, var_value)) {
      ret = OB_ERR_NO_PRIVILEGE;
      LOG_WARN("client is not invited into this tenant", K(ret));
    }
  }
  return ret;
}

int ObMPConnect::convert_oracle_object_name(const uint64_t tenant_id, ObString& object_name)
{
  int ret = OB_SUCCESS;
  ObWorker::CompatMode compat_mode = ObWorker::CompatMode::MYSQL;
  if (object_name.empty()) {
    LOG_DEBUG("object name is null when try to convert it");
  } else if (OB_FAIL(ObCompatModeGetter::get_tenant_mode(tenant_id, compat_mode))) {
    LOG_WARN("fail to get tenant mode in convert_oracle_object_name", K(ret));
  } else if (compat_mode == ObWorker::CompatMode::ORACLE) {
    if (object_name.length() > 1 && '\"' == object_name[0] && '\"' == object_name[object_name.length() - 1]) {
      if (2 != object_name.length()) {
        object_name.assign_ptr(object_name.ptr() + 1, object_name.length() - 2);
      } else {
        object_name.reset();
      }
    } else {
      ObCharset::caseup(CS_TYPE_UTF8MB4_BIN, object_name);
    }
  }
  return ret;
}

int ObMPConnect::set_proxy_version(ObSMConnection& conn)
{
  int ret = OB_SUCCESS;
  bool is_found = false;
  ObString key_str;
  const char* proxy_version_str = NULL;
  int64_t length = 0;
  key_str.assign_ptr(OB_MYSQL_PROXY_VEERSION, static_cast<int32_t>(STRLEN(OB_MYSQL_PROXY_VEERSION)));
  for (int64_t i = 0; !is_found && i < hsr_.get_connect_attrs().count(); ++i) {
    const ObStringKV& kv = hsr_.get_connect_attrs().at(i);
    if (key_str == kv.key_) {
      proxy_version_str = kv.value_.ptr();
      length = kv.value_.length();
      is_found = true;
    }
  }
  int64_t min_len = 5;
  if (!is_found || OB_ISNULL(proxy_version_str) || length < min_len) {
    conn.proxy_version_ = 0;
  } else {
    const int64_t VERSION_ITEM = 3;
    ObArenaAllocator allocator(ObModIds::OB_SQL_EXPR);
    char* buff = static_cast<char*>(allocator.alloc(length + 1));
    if (OB_ISNULL(buff)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("failed to alloc memory.", K(buff), K(ret));
    } else {
      memset(buff, 0, length + 1);
      int64_t cur_item = 0;
      for (int64_t i = 0; cur_item != VERSION_ITEM && i < length; ++i) {
        if (proxy_version_str[i] == '.') {
          ++cur_item;
        }
        if (cur_item != VERSION_ITEM) {
          buff[i] = proxy_version_str[i];
        }
      }
      if (OB_FAIL(ObClusterVersion::get_version(buff, conn.proxy_version_))) {
        LOG_WARN("failed to get version", K(ret));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}
