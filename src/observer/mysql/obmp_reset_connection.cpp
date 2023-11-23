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
#include "observer/mysql/obmp_reset_connection.h"
#include "rpc/obmysql/obsm_struct.h"
#include "observer/mysql/ob_mysql_end_trans_cb.h"
#include "lib/string/ob_sql_string.h"
#include "rpc/obmysql/ob_mysql_util.h"
#include "rpc/obmysql/packet/ompk_ok.h"
//#include "share/schema/ob_schema_getter_guard.h"
//#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "sql/ob_sql.h"
#include "sql/ob_end_trans_callback.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/parser/ob_parser.h"
#include "sql/parser/ob_parser_utils.h"


using namespace oceanbase::common;
using namespace oceanbase::rpc;
using namespace oceanbase::obmysql;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace observer
{

int ObMPResetConnection::process()
{
  int ret = OB_SUCCESS;
  bool need_disconnect = false;
  ObSQLSessionInfo *session = NULL;
  ObSMConnection *conn = NULL;
  ObSchemaGetterGuard schema_guard;
  uint64_t tenant_id = OB_INVALID_ID;
  const ObSysVariableSchema *sys_variable_schema = NULL;
  if (OB_FAIL(get_session(session))) {
    LOG_ERROR("get session  fail", K(ret));
  } else if (OB_ISNULL(conn = get_conn())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("null conn", K(ret));
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fail to get session info", K(ret), K(session));
  } else {
    THIS_WORKER.set_session(session);
    ObSQLSessionInfo::LockGuard lock_guard(session->get_query_lock());
    session->update_last_active_time();
    tenant_id = session->get_effective_tenant_id();
    if (OB_FAIL(ObSqlTransControl::rollback_trans(session, need_disconnect))) {
      OB_LOG(WARN, "fail to rollback trans for change user", K(ret), K(need_disconnect));
    } else if (OB_FAIL(gctx_.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
      OB_LOG(WARN,"fail get schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_sys_variable_schema(tenant_id, sys_variable_schema))) {
        LOG_WARN("get sys variable schema failed", K(ret));
    } else if (OB_ISNULL(sys_variable_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sys variable schema is null", K(ret));
    } else if (OB_FAIL(session->load_all_sys_vars(*sys_variable_schema, true))) {
      LOG_WARN("load system variables failed", K(ret));
    } else if (OB_FAIL(session->update_database_variables(&schema_guard))) {
      OB_LOG(WARN, "failed to update database variables", K(ret));
    } else if (OB_FAIL(update_proxy_sys_vars(*session))) {
      LOG_WARN("update_proxy_sys_vars failed", K(ret));
    } else if (OB_FAIL(update_charset_sys_vars(*conn, *session))) {
        LOG_WARN("fail to update charset sys vars", K(ret));
    } else {
      session->clean_status();
    }
  }

  //send packet to client
  if (OB_SUCC(ret)) {
    ObOKPParam ok_param;
    ok_param.is_on_change_user_ = true;
    if (OB_FAIL(send_ok_packet(*session, ok_param))) {
      OB_LOG(WARN, "response ok packet fail", K(ret));
    }
  } else {
    if (OB_FAIL(send_error_packet(ret, NULL))) {
      OB_LOG(WARN,"response fail packet fail", K(ret));
    }
  }

  if (OB_UNLIKELY(need_disconnect) && is_conn_valid()) {
    if (NULL == session) {
      LOG_WARN("will disconnect connection", K(ret), K(need_disconnect));
    } else {
      LOG_WARN("will disconnect connection", K(ret), K(session->get_sessid()),
               K(need_disconnect));
    }
    force_disconnect();
  }

  THIS_WORKER.set_session(NULL);
  if (session != NULL) {
    revert_session(session);
  }
  return ret;
}

} //namespace observer
} //namespace oceanbase
