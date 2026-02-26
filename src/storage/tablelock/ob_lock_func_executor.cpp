/**
 * Copyright (c) 2023 OceanBase
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
#include "storage/tablelock/ob_lock_func_executor.h"

#include "sql/engine/ob_exec_context.h"
#include "storage/tablelock/ob_table_lock_service.h"
#include "storage/tablelock/ob_table_lock_live_detector.h"
#include "storage/tablelock/ob_lock_inner_connection_util.h"

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
int ObGetLockExecutor::execute(ObExecContext &ctx,
                               const ObString &lock_name,
                               const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSQLSessionInfo *sess = ctx.get_my_session();
  uint32_t client_session_id = sess->get_client_sid();
  uint32_t server_session_id = sess->get_server_sid();
  uint64_t client_session_create_ts = sess->get_client_create_time();
  uint64_t lock_id = 0;
  bool is_rollback = false;
  ObTxParam tx_param;

  OV (proxy_is_support(ctx), OB_NOT_SUPPORTED);
  OZ (ObLockContext::valid_execute_context(ctx));
  // 1. generate lock_id and update DBMS_LOCK_ALLOCATED table
  // 2. check client_session_id is valid
  // 3. check whether need reroute (skip temporarily)
  // 4. modify inner table
  // 4.1 add session into CLIENT_TO_SERVER_SESSION_INFO table
  // 4.2 add lock_obj into DETECT_LOCK_INFO table
  // 5. lock obj

  if (OB_SUCC(ret)) {
    SMART_VAR(ObLockContext, stack_ctx1) {
      OZ (stack_ctx1.init(ctx, timeout_us));
      OZ (generate_lock_id_(stack_ctx1, lock_name, timeout_us, lock_id));

      is_rollback = (OB_SUCCESS != ret);
      if (OB_TMP_FAIL(stack_ctx1.destroy(ctx, is_rollback))) {
        LOG_WARN("stack ctx destroy failed", K(tmp_ret));
        ret = COVER_SUCC(tmp_ret);
      }
    }
  }
  if (OB_SUCC(ret)) {
    SMART_VAR(ObLockContext, stack_ctx2) {
      OZ (stack_ctx2.init(ctx, timeout_us));
      // only when connect by proxy should check client_session
      if (sess->is_obproxy_mode()) {
        OZ (check_client_ssid(stack_ctx2, client_session_id, client_session_create_ts));
        if (OB_EMPTY_RESULT == ret) {
          ret = OB_SUCCESS;  // there're no same client_session_id records, continue
        } else {
          // TODO(yangyifei.yyf): some SQL can not redo, reroute may cause errors, so skip this step temporarily
          // OZ (check_need_reroute_(stack_ctx1, client_session_id, client_session_create_ts));
        }
      }
      OZ (update_session_table_(stack_ctx2,
                                client_session_id,
                                client_session_create_ts,
                                server_session_id));
      OZ (ObInnerConnectionLockUtil::build_tx_param(sess, tx_param));
      OZ (lock_obj_(sess, tx_param, client_session_id, client_session_create_ts, lock_id, timeout_us));
      OX (mark_lock_session_(sess, true));

      is_rollback = (OB_SUCCESS != ret);
      if (OB_TMP_FAIL(stack_ctx2.destroy(ctx, is_rollback))) {
        LOG_WARN("stack ctx destroy failed", K(tmp_ret));
        ret = COVER_SUCC(tmp_ret);
      }
    }
  }
  return ret;
}

int ObGetLockExecutor::lock_obj_(sql::ObSQLSessionInfo *session,
                                 const ObTxParam &tx_param,
                                 const uint32_t client_session_id,
                                 const uint64_t client_session_create_ts,
                                 const int64_t obj_id,
                                 const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObTableLockService *lock_service = MTL(ObTableLockService *);
  ObLockObjsRequest arg;
  bool need_record_to_lock_table = true;
  ObTxDesc *tx_desc = session->get_tx_desc();
  ObLockID lock_id;

  arg.lock_mode_ = EXCLUSIVE;  // only X mode
  arg.op_type_ = ObTableLockOpType::OUT_TRANS_LOCK;
  arg.timeout_us_ = timeout_us;
  arg.is_from_sql_ = true;
  arg.detect_func_no_ = ObTableLockDetectType::DETECT_SESSION_ALIVE;

  if (OB_FAIL(lock_id.set(ObLockOBJType::OBJ_TYPE_MYSQL_LOCK_FUNC, obj_id))) {
    LOG_WARN("set lock_id failed", K(ret), K(obj_id));
  } else if (OB_FAIL(arg.owner_id_.convert_from_client_sessid(client_session_id, client_session_create_ts))) {
    LOG_WARN("convert client_session_id to owner_id failed", K(ret), K(client_session_id));
  } else if (OB_FAIL(arg.objs_.push_back(lock_id))) {
    LOG_WARN("push_back lock_id to arg.objs_ failed", K(ret), K(arg), K(lock_id));
  } else if (OB_FAIL(ObTableLockDetector::record_detect_info_to_inner_table(
               session, LOCK_OBJECT, arg, /*for_dbms_lock*/ false, need_record_to_lock_table))) {
    LOG_WARN("record_detect_info_to_inner_table failed", K(ret), K(arg));
  } else if (need_record_to_lock_table) {
    if (OB_FAIL(lock_service->lock(*tx_desc, tx_param, arg))) {
      LOG_WARN("lock obj failed", K(ret), KPC(tx_desc), K(arg));
    }
  }

  return ret;
}

int ObGetLockExecutor::generate_lock_id_(ObLockContext &ctx,
                                         const ObString &lock_name,
                                         const int64_t timeout_us,
                                         uint64_t &lock_id)
{
  int ret = OB_SUCCESS;
  char lock_handle_buf[MAX_LOCK_HANDLE_LEGNTH] = {0};
  OZ (query_lock_id_and_lock_handle_(lock_name, lock_id, lock_handle_buf));
  if (OB_EMPTY_RESULT == ret) {
    // there is no result, should create one
    ret = OB_SUCCESS;
    OZ (generate_lock_id_(lock_name, lock_id, lock_handle_buf));
  }
  OZ (write_lock_id_(ctx, lock_name, timeout_us, lock_id, lock_handle_buf));
  return ret;
}

int ObGetLockExecutor::generate_lock_id_(const ObString &lock_name,
                                         uint64_t &lock_id,
                                         char *lock_handle)
{
  int ret = OB_SUCCESS;
  ObCommonID unique_lock_id;
  if (OB_ISNULL(lock_handle)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lock handle can not be null", K(ret));
  } else if (OB_FAIL(ObCommonIDUtils::gen_unique_id(MTL_ID(), unique_lock_id))) {
    LOG_WARN("generate unique id for lock handle failed", K(ret));
  } else {
    uint64_t hash_val = 0;
    hash_val = murmurhash(lock_name.ptr(), lock_name.length(), hash_val);
    // the range of the lock id in lock handle is [MIN_LOCK_HANDLE_ID, MAX_LOCK_HANDLE_ID]
    lock_id = unique_lock_id.id() % (MAX_LOCK_HANDLE_ID - MIN_LOCK_HANDLE_ID + 1) + MIN_LOCK_HANDLE_ID;
    snprintf(lock_handle, MAX_LOCK_HANDLE_LEGNTH, "%" PRIu64 "%" PRIu64, lock_id, hash_val);
  }
  return ret;
}

int ObGetLockExecutor::write_lock_id_(ObLockContext &ctx,
                                      const ObString &lock_name,
                                      const int64_t timeout_us,
                                      const uint64_t &lock_id,
                                      const char *lock_handle_buf)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer insert_dml;
  ObSqlString delete_sql;
  ObSqlString insert_sql;
  int64_t affected_rows = 0;
  const int64_t now = ObTimeUtility::current_time();
  lib::CompatModeGuard guard(lib::Worker::CompatMode::MYSQL);
  char table_name[MAX_FULL_TABLE_NAME_LENGTH] = {0};
  OZ (databuff_printf(table_name, MAX_FULL_TABLE_NAME_LENGTH,
                      "%s.%s", OB_SYS_DATABASE_NAME, OB_ALL_DBMS_LOCK_ALLOCATED_TNAME));

  OZ (insert_dml.add_gmt_create(now));
  OZ (insert_dml.add_gmt_modified(now));
  OZ (insert_dml.add_pk_column("name", lock_name));
  // make sure lock_obj will be timeout or success before lock_id is expired
  OZ (insert_dml.add_time_column("expiration", now + timeout_us + DEFAULT_EXPIRATION_US));
  OZ (insert_dml.add_column("lockid", lock_id));
  OZ (insert_dml.add_column("lockhandle", lock_handle_buf));
  OZ (insert_dml.splice_insert_update_sql(table_name,
                                          insert_sql));
  OZ (ctx.execute_write(insert_sql, affected_rows));
  CK (OB_LIKELY(1 == affected_rows || 2 == affected_rows));

  return ret;
}

int ObReleaseLockExecutor::execute(ObExecContext &ctx,
                                   const ObString &lock_name,
                                   int64_t &release_cnt)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  uint32_t client_session_id = 0;
  uint64_t client_session_create_ts = 0;
  uint64_t lock_id = 0;
  bool is_rollback = false;
  bool need_remove_from_lock_table = true;
  bool lock_id_existed = false;
  ObLockID tmp_lock_id;

  release_cnt = INVALID_RELEASE_CNT;  // means not release successfully

  OV (proxy_is_support(ctx), OB_NOT_SUPPORTED);
  OZ (ObLockContext::valid_execute_context(ctx));
  if (OB_SUCC(ret)) {
    SMART_VAR(ObLockContext, stack_ctx) {
      client_session_id = ctx.get_my_session()->get_client_sid();
      client_session_create_ts = ctx.get_my_session()->get_client_create_time();
      OZ (stack_ctx.init(ctx));
      if (OB_SUCC(ret)) {
        ObSQLSessionInfo *session = GET_MY_SESSION(ctx);
        ObTxDesc *tx_desc = session->get_tx_desc();
        ObTxParam tx_param;
        ObUnLockObjsRequest arg;
        ObTableLockOwnerID lock_owner;
        // 1. get lock id from inner table
        // 2. check client_session_id is valid
        // 3. unlock obj
        // 4. modify inner table
        // 4.1 lock table: dec cnt if the cnt is greater than 1, else remove the record.
        // (this is processed internal)
        // 4.2 lock name-id table: check the lock table if there is no lock of the same
        // lock id, remove the record at lock name-id table.
        // 4.3 session table: check the lock table if there is no lock of the same
        // client session, remove the record of the same client session id.
        OZ (query_lock_id_(lock_name, lock_id));
        if (OB_EMPTY_RESULT == ret) {
          release_cnt = LOCK_NOT_EXIST_RELEASE_CNT;
        }
        if (ctx.get_my_session()->is_obproxy_mode()) {
          OZ (check_client_ssid(stack_ctx, client_session_id, client_session_create_ts));
          if (OB_EMPTY_RESULT == ret) {
            release_cnt = LOCK_NOT_OWN_RELEASE_CNT;
          }
        }
        OZ (lock_owner.convert_from_client_sessid(client_session_id, client_session_create_ts));
        OZ (ObInnerConnectionLockUtil::build_tx_param(session, tx_param));

        OZ (tmp_lock_id.set(ObLockOBJType::OBJ_TYPE_MYSQL_LOCK_FUNC, lock_id));
        OZ (arg.objs_.push_back(tmp_lock_id));
        OX (arg.lock_mode_ = EXCLUSIVE);
        OX (arg.op_type_ = ObTableLockOpType::OUT_TRANS_UNLOCK);
        OX (arg.timeout_us_ = 1000 * 1000L);  // 1s, which means is_try_lock = false
        OX (arg.is_from_sql_ = true);
        OX (arg.owner_id_ = lock_owner);

        OZ (ObTableLockDetector::remove_detect_info_from_inner_table(
               session, LOCK_OBJECT, arg, need_remove_from_lock_table));
        OX (release_cnt = 1);  // if release sucessfully, set release_cnt to 1
        if (OB_SUCC(ret) && need_remove_from_lock_table) {
          OZ (unlock_obj_(tx_desc, tx_param, arg));
          OZ (remove_session_record(stack_ctx, client_session_id, client_session_create_ts));
        } else if (OB_EMPTY_RESULT == ret) {
          if (OB_TMP_FAIL(ObTableLockDetector::check_lock_id_exist_in_inner_table(
                session, lock_id, ObLockOBJType::OBJ_TYPE_MYSQL_LOCK_FUNC, lock_id_existed))) {
            LOG_WARN("check lock_id existed failed", K(tmp_ret), K(lock_id));
          } else if (lock_id_existed) {
            release_cnt = LOCK_NOT_OWN_RELEASE_CNT;
          } else {
            release_cnt = LOCK_NOT_EXIST_RELEASE_CNT;
          }
        }
      }
      is_rollback = (OB_SUCCESS != ret);
      if (OB_TMP_FAIL(stack_ctx.destroy(ctx, is_rollback))) {
        LOG_WARN("stack ctx destroy failed", K(tmp_ret));
        ret = COVER_SUCC(tmp_ret);
      }
    }
  }
  // if release_cnt is valid, means we have tried to release,
  // and have not encountered any failures before
  if (INVALID_RELEASE_CNT != release_cnt) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObReleaseAllLockExecutor::execute(ObExecContext &ctx,
                                      int64_t &release_cnt)
{
  int ret = OB_SUCCESS;
  OV (proxy_is_support(ctx), OB_NOT_SUPPORTED);
  OZ (ObUnLockExecutor::execute(ctx, RELEASE_OBJ_LOCK, release_cnt));
  return ret;
}

int ObISFreeLockExecutor::execute(ObExecContext &ctx,
                                  const ObString &lock_name)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  uint64_t lock_id = 0;
  ObSQLSessionInfo *sess = ctx.get_my_session();
  bool exist = false;
  OZ (query_lock_id_(lock_name, lock_id));
  OZ (ObTableLockDetector::check_lock_id_exist_in_inner_table(
      sess, lock_id, ObLockOBJType::OBJ_TYPE_MYSQL_LOCK_FUNC, exist));

  if (OB_SUCC(ret) && !exist) {
    ret = OB_EMPTY_RESULT;
  }
  return ret;
}

int ObISUsedLockExecutor::execute(ObExecContext &ctx,
                                  const ObString &lock_name,
                                  uint32_t &sess_id)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  uint64_t lock_id = 0;
  ObTableLockOwnerID lock_owner;

  OZ (query_lock_id_(lock_name, lock_id));
  OZ (ObTableLockDetector::get_lock_owner_by_lock_id(lock_id, lock_owner));
  OZ (lock_owner.convert_to_sessid(sess_id));
  return ret;
}


} // tablelock
} // transaction
} // oceanbase
