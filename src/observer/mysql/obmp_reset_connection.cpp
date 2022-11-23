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
  bool is_proxy_mod = get_conn()->is_proxy_;
  if (OB_FAIL(get_session(session))) {
    LOG_ERROR("get session  fail", K(ret));
  } else if (OB_ISNULL(session)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fail to get session info", K(ret), K(session));
  } else {
    THIS_WORKER.set_session(session);
    ObSQLSessionInfo::LockGuard lock_guard(session->get_query_lock());
    session->update_last_active_time();

    if (OB_FAIL(ObSqlTransControl::rollback_trans(session, need_disconnect))) {
      OB_LOG(WARN, "fail to rollback trans for change user", K(ret), K(need_disconnect));
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
