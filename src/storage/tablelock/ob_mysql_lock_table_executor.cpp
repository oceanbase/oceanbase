/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX TABLELOCK
#include "storage/tablelock/ob_mysql_lock_table_executor.h"

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/utility/ob_fast_convert.h"
#include "lib/alloc/alloc_assist.h"
#include "observer/ob_inner_sql_connection.h"
#include "share/ob_table_access_helper.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_end_trans_callback.h"
#include "sql/ob_sql_trans_control.h"
#include "sql/resolver/ddl/ob_lock_table_stmt.h"
#include "sql/session/ob_sql_session_info.h"
#include "storage/ob_common_id_utils.h"
#include "storage/tablelock/ob_table_lock_rpc_struct.h"
#include "storage/tablelock/ob_table_lock_service.h"
#include "storage/tablelock/ob_table_lock_live_detector.h"
#include "storage/tablelock/ob_lock_inner_connection_util.h"
#include "share/ob_table_lock_compat_versions.h"

namespace oceanbase
{
using namespace sql;
using namespace transaction;
using namespace common;
using namespace observer;

namespace transaction
{
namespace tablelock
{

int ObMySQLLockTableExecutor::execute(ObExecContext &ctx,
                                      const ObIArray<ObMySQLLockNode> &lock_node_list)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSQLSessionInfo *sess = ctx.get_my_session();
  uint32_t client_session_id = sess->get_client_sid();
  uint32_t server_session_id = sess->get_server_sid();
  uint64_t client_session_create_ts = sess->get_client_create_time();
  bool is_rollback = false;
  ObTxParam tx_param;
  int64_t timeout_us = THIS_WORKER.get_timeout_ts() - ObTimeUtility::current_time();
  OZ (ObLockContext::valid_execute_context(ctx));
  // 1. check client_session_id is valid
  // 2. modify inner table
  // 2.1 add session into CLIENT_TO_SERVER_SESSION_INFO table
  // 2.2 add lock_table into DETECT_LOCK_INFO table
  // 3. lock table
  OV (ObLockExecutor::proxy_is_support(ctx), OB_NOT_SUPPORTED);

  if (OB_SUCC(ret)) {
    SMART_VAR(ObLockContext, stack_ctx) {
      OZ (stack_ctx.init(ctx, timeout_us));
      // only when connect by proxy should check client_session
      if (sess->is_obproxy_mode()) {
        OZ (check_client_ssid(stack_ctx, client_session_id, client_session_create_ts));
        if (OB_EMPTY_RESULT == ret) {
          ret = OB_SUCCESS;  // there're no same client_session_id records, continue
        } else {
          // TODO(yangyifei.yyf): some SQL can not redo, reroute may cause errors, so skip this step temporarily
          // OZ (check_need_reroute_(stack_ctx1, sess, client_session_id, client_session_create_ts));
        }
      }
      OZ (update_session_table_(stack_ctx,
                                client_session_id,
                                client_session_create_ts,
                                server_session_id));
      OZ (ObInnerConnectionLockUtil::build_tx_param(sess, tx_param));
      OZ (lock_tables_(sess,
                      tx_param,
                      client_session_id,
                      client_session_create_ts,
                      lock_node_list,
                      timeout_us));
      OX (mark_lock_session_(sess, true));

      is_rollback = (OB_SUCCESS != ret);
      if (OB_TMP_FAIL(stack_ctx.destroy(ctx, is_rollback))) {
        LOG_WARN("stack ctx destroy failed", K(tmp_ret));
        ret = COVER_SUCC(tmp_ret);
      }
    }
  }
  return ret;
}

int ObMySQLLockTableExecutor::lock_tables_(sql::ObSQLSessionInfo *session,
                                           const ObTxParam &tx_param,
                                           const uint32_t client_session_id,
                                           const uint64_t client_session_create_ts,
                                           const ObIArray<ObMySQLLockNode> &lock_node_list,
                                           const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < lock_node_list.count(); ++i) {
    const ObMySQLLockNode &lock_node = lock_node_list.at(i);
    if (OB_UNLIKELY(!lock_node.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("lock node invalid", K(ret), K(lock_node));
    } else {
      uint64_t table_id = lock_node.table_item_->ref_id_;
      int64_t lock_mode = lock_node.lock_mode_;
      if (OB_FAIL(lock_table_(session,
                              tx_param,
                              client_session_id,
                              client_session_create_ts,
                              table_id,
                              lock_mode,
                              timeout_us))) {
        LOG_WARN("lock table failed", K(ret), K(table_id), K(lock_mode));
      }
    }
  }
  return ret;
}

int ObMySQLLockTableExecutor::lock_table_(sql::ObSQLSessionInfo *session,
                                          const ObTxParam &tx_param,
                                          const uint32_t client_session_id,
                                          const uint64_t client_session_create_ts,
                                          const uint64_t table_id,
                                          const int64_t lock_mode,
                                          const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObTableLockService *lock_service = MTL(ObTableLockService *);
  ObLockTableRequest arg;
  bool need_record_to_lock_table = true;
  ObTxDesc *tx_desc = session->get_tx_desc();

  arg.table_id_ = table_id;
  arg.lock_mode_ = lock_mode;
  arg.op_type_ = ObTableLockOpType::OUT_TRANS_LOCK;
  arg.timeout_us_ = timeout_us;
  arg.is_from_sql_ = true;
  arg.detect_func_no_ = ObTableLockDetectType::DETECT_SESSION_ALIVE;

  if (OB_FAIL(arg.owner_id_.convert_from_client_sessid(client_session_id,
                                                       client_session_create_ts))) {
    LOG_WARN("convert client_session_id to owner_id failed", K(ret), K(client_session_id));
  } else if (OB_FAIL(ObTableLockDetector::record_detect_info_to_inner_table(session,
                                                                            LOCK_TABLE,
                                                                            arg,
                                                                            /*for_dbms_lock*/ false,
                                                                            need_record_to_lock_table))) {
    LOG_WARN("record_detect_info_to_inner_table failed", K(ret), K(arg));
    // TODO: yanyuan.cxf lock twice just update the lock cnt now.
    // will unlock and lock again later just like mysql.
  } else if (need_record_to_lock_table) {
    if (OB_FAIL(lock_service->lock(*tx_desc, tx_param, arg))) {
      LOG_WARN("lock table failed", K(ret), KPC(tx_desc), K(arg));
    }
  }

  return ret;
}

int ObMySQLUnlockTableExecutor::execute(sql::ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  int64_t release_cnt = 0;
  OV (ObLockExecutor::proxy_is_support(ctx), OB_NOT_SUPPORTED);
  OZ (ObUnLockExecutor::execute(ctx, RELEASE_TABLE_LOCK, release_cnt));
  return ret;
}

} // tablelock
} // transaction
} // oceanbase
