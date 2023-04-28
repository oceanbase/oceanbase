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

#include "observer/mysql/obmp_init_db.h"

#include "lib/worker.h"
#include "rpc/ob_request.h"
#include "rpc/obmysql/packet/ompk_ok.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/ob_sql_utils.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "rpc/obmysql/obsm_struct.h"
#include "observer/mysql/obmp_utils.h"
#include "observer/mysql/ob_query_retry_ctrl.h"

using namespace oceanbase::rpc;
using namespace oceanbase::obmysql;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::observer;
using namespace oceanbase::share::schema;

int ObMPInitDB::deserialize()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(req_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid packet", K(ret), K_(req));
  } else if (OB_UNLIKELY(req_->get_type() != ObRequest::OB_MYSQL)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid packet", K(ret), K_(req), K(req_->get_type()));
  } else {
    const ObMySQLRawPacket &pkt = reinterpret_cast<const ObMySQLRawPacket&>(req_->get_packet());
    db_name_.assign_ptr(const_cast<char *>(pkt.get_cdata()), pkt.get_clen()-1);
  }
  return ret;
}

int ObMPInitDB::process()
{
  LOG_INFO("init db", K_(db_name));
  int ret = OB_SUCCESS;
  bool need_disconnect = true;
  ObSQLSessionInfo *session = NULL;
  ObString tmp_db_name;
  ObDataBuffer allocator(db_name_conv_buf, sizeof(db_name_conv_buf));
  const ObMySQLRawPacket &pkt = reinterpret_cast<const ObMySQLRawPacket&>(req_->get_packet());
  int64_t query_timeout = 0;
  bool is_packet_retry = false;
  bool need_response_error = true; //temporary placeholder
  if (OB_FAIL(get_session(session))) {
    LOG_WARN("get session  fail", K(ret));
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("null pointer");
  } else if (OB_FAIL(session->get_query_timeout(query_timeout))) {
    LOG_WARN("fail to get query timeout", K(ret));
  } else if (OB_ISNULL(gctx_.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", K(ret));
  } else {
    ObCollationType old_db_coll_type = CS_TYPE_INVALID;
    ObCollationType collation_connection = CS_TYPE_INVALID;
    ObSQLSessionInfo::LockGuard lock_guard(session->get_query_lock());
    setup_wb(*session);
    tmp_db_name = session->get_database_name();
    session->update_last_active_time();
    const uint64_t effective_tenant_id = session->get_effective_tenant_id();
    int64_t global_version = OB_INVALID_VERSION;
    int64_t local_version = OB_INVALID_VERSION;
    ObQueryRetryType retry_type = RETRY_TYPE_NONE;
    int64_t retry_times = 0;
    THIS_WORKER.set_timeout_ts(get_receive_timestamp() + query_timeout);
    ObNameCaseMode mode = OB_NAME_CASE_INVALID;
    if (OB_UNLIKELY(session->is_zombie())) {
      ret = OB_ERR_SESSION_INTERRUPTED;
      LOG_WARN("session has been killed", K(ret), KPC(session));
    } else if (OB_FAIL(gctx_.schema_service_->get_tenant_received_broadcast_version(effective_tenant_id, global_version))) {
      LOG_WARN("fail to get global_version", K(ret), K(effective_tenant_id));
    } else if (OB_FAIL(gctx_.schema_service_->get_tenant_refreshed_schema_version(effective_tenant_id, local_version))) {
      LOG_WARN("fail to get local_version", K(ret), K(effective_tenant_id));
    } else if (OB_FAIL(session->get_collation_database(old_db_coll_type))) {
      LOG_WARN("fail to get collation_database", K(ret));
    } else if (OB_FAIL(session->get_collation_connection(collation_connection))) {
      LOG_WARN("fail to get collation_connection", K(ret));
    } else if (OB_FAIL(session->get_name_case_mode(mode))) {
      LOG_WARN("fail to get name case mode", K(mode), K(ret));
    } else if (OB_FAIL(update_transmission_checksum_flag(*session))) {
      LOG_WARN("update transmisson checksum flag failed", K(ret));
    } else if (FALSE_IT(session->set_txn_free_route(pkt.txn_free_route()))) {
    } else if (OB_FAIL(process_extra_info(*session, pkt, need_response_error))) {
      LOG_WARN("fail get process extra info", K(ret));
    } else if (FALSE_IT(session->post_sync_session_info())) {
    } else {
      need_disconnect = false;
      bool perserve_lettercase = lib::is_oracle_mode() ?
          true : (mode != OB_LOWERCASE_AND_INSENSITIVE);
      if (OB_FAIL(ObSQLUtils::convert_sql_text_to_schema_for_storing(allocator,
                                                                     session->get_dtc_params(),
                                                                     db_name_))) {
        LOG_WARN("fail to convert db name", K(ret), KPHEX(db_name_.ptr(), db_name_.length()));
      } else if (OB_FAIL(ObSQLUtils::check_and_convert_db_name(
                  collation_connection, perserve_lettercase, db_name_))) {
        LOG_WARN("failed to check database name", K(db_name_), K(ret));
      } else {
        bool force_local_retry = false;
        do {
          retry_type = RETRY_TYPE_NONE;
          ret = do_process(session);
          if (is_schema_error(ret)) {
            if (local_version < global_version) {
              if (!THIS_WORKER.is_timeout()) {
                if (force_local_retry
                    || retry_times < ObQueryRetryCtrl::MAX_SCHEMA_ERROR_LOCAL_RETRY_TIMES) {
                  retry_type = RETRY_TYPE_LOCAL;
                } else {
                  retry_type = RETRY_TYPE_PACKET;
                }
                retry_times++;
                if (RETRY_TYPE_LOCAL == retry_type) {
                  ob_usleep(ObQueryRetryCtrl::WAIT_LOCAL_SCHEMA_REFRESHED_US
                         * ObQueryRetryCtrl::linear_timeout_factor(retry_times));
                }
                int tmp_ret = gctx_.schema_service_->get_tenant_refreshed_schema_version(effective_tenant_id, local_version);
                if (OB_SUCCESS != tmp_ret) {
                  LOG_WARN("fail to get local_version", K(ret), K(tmp_ret), K(effective_tenant_id));
                }
              }
              LOG_WARN("schema err, need retry", K(ret),
                       K(retry_type), K(retry_times), K(force_local_retry),
                       LITERAL_K(ObQueryRetryCtrl::MAX_SCHEMA_ERROR_LOCAL_RETRY_TIMES));
            }
          }
          force_local_retry = false;
          if (RETRY_TYPE_LOCAL == retry_type) {
            // 在本线程重试
            force_local_retry = true;
          } else if (RETRY_TYPE_PACKET == retry_type) {
            // 扔回队列中重试
            if (!THIS_WORKER.can_retry()) {
              // 不允许丢回队列，在本线程重试
              // FIXME: when will we be here?
              force_local_retry = true;
              LOG_WARN("fail to set retry flag, force to do local retry");
            } else {
              THIS_WORKER.set_need_retry();
              is_packet_retry = true;
            }
          }
          if (force_local_retry) {
            clear_wb_content(*session);
          }
        } while (force_local_retry);
      }
    }
    if (OB_FAIL(ret)) {
      int set_db_ret = OB_SUCCESS;
      if (OB_SUCCESS != (set_db_ret = session->set_default_database(tmp_db_name, old_db_coll_type))) {
        LOG_WARN("failed to set default database", K(ret), K(set_db_ret), K(tmp_db_name));
      }
    }

    session->set_show_warnings_buf(ret);
    session->reset_warnings_buf();
    ob_setup_tsi_warning_buffer(NULL);
  }  // end session guard

  if (OB_FAIL(ret)) {
    if (false == is_packet_retry && need_disconnect && is_conn_valid()) {
      force_disconnect();
      LOG_WARN("disconnect connection when process query", K(ret));
    } else  if (false == is_packet_retry && OB_FAIL(send_error_packet(ret, NULL))) { // 覆盖ret, 无需继续抛出
      LOG_WARN("failed to send error packet", K(ret));
    }
  } else if (OB_LIKELY(NULL != session)) {
    ObOKPParam ok_param; // use defualt value
    if (OB_FAIL(send_ok_packet(*session, ok_param))) {
      LOG_WARN("fail to send ok packet", K(ok_param), K(ret));
    }
  }
  if (session != NULL) {
    if (OB_FAIL(revert_session(session))) {
      LOG_ERROR("failed to revert session", K(ret));
    }
  }
  return ret;
}

int ObMPInitDB::do_process(sql::ObSQLSessionInfo *session)
{
  int ret = OB_SUCCESS;
  int sret = OB_SUCCESS;
  share::schema::ObSessionPrivInfo session_priv;
  ObSchemaGetterGuard schema_guard;

  if (OB_ISNULL(session) || OB_ISNULL(gctx_.schema_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("session not init", K(ret), K(session), K(gctx_.schema_service_));
  } else if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(
                                  session->get_effective_tenant_id(), schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret));
  } else if (session->is_tenant_changed() && 0 != db_name_.case_compare(OB_SYS_DATABASE_NAME)) {
    ret = OB_ERR_NO_DB_PRIVILEGE;
    LOG_WARN("can only access oceanbase database when tenant changed", K(ret));
  } else {
    session->get_session_priv_info(session_priv);
    if (OB_FAIL(ObSQLUtils::cvt_db_name_to_org(schema_guard, session, db_name_))) {
      LOG_WARN("fail to cvt db name to orignal", K(db_name_), K(ret));
    } else if (OB_FAIL(schema_guard.check_db_access(session_priv, db_name_))) {
      LOG_WARN("fail to check db access.", K_(db_name), K(ret));
      if (OB_ERR_NO_DB_SELECTED == ret) {
        sret = OB_ERR_BAD_DATABASE;// 将错误码抛出让外层重试
      } else {
        sret = ret; // 保险起见，也抛出
      }
    } else {
      uint64_t db_id = OB_INVALID_ID;
      session->set_db_priv_set(session_priv.db_priv_set_);
      if (OB_FAIL(session->set_default_database(db_name_))) {
        LOG_WARN("failed to set default database", K(ret), K(db_name_));
      } else if (OB_FAIL(session->update_database_variables(&schema_guard))) {
        LOG_WARN("failed to update database variables", K(ret));
      } else if (OB_FAIL(schema_guard.get_database_id(session->get_effective_tenant_id(),
                                                      session->get_database_name(),
                                                      db_id))) {
        LOG_WARN("failed to get database id", K(ret));
      } else {
        session->set_database_id(db_id);
      }
    }
  }
  return (OB_SUCCESS != sret) ? sret : ret;
}
