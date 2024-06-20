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

#include "ob_xa_service.h"
#include "ob_xa_rpc.h"
#include "ob_xa_ctx.h"
#include "ob_trans_service.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"   // ObMySQLProxy
#include "lib/mysqlclient/ob_mysql_result.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/rc/ob_tenant_base.h"
#include "share/rc/ob_tenant_module_init_ctx.h"
#include "observer/ob_srv_network_frame.h"

namespace oceanbase
{

using namespace share;
using namespace common::sqlclient;

namespace transaction
{
int ObXAService::mtl_init(ObXAService *&xa_service)
{
  int ret = OB_SUCCESS;
  const ObAddr &self = GCTX.self_addr();
  observer::ObSrvNetworkFrame *net_frame = GCTX.net_frame_;

  return xa_service->init(self,
                          net_frame->get_req_transport());
}

int ObXAService::init(const ObAddr &self_addr,
                      rpc::frame::ObReqTransport *req_transport)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(WARN, "xa service init twice", K(ret));
  } else if (OB_ISNULL(req_transport) ||
             OB_UNLIKELY(!self_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KP(req_transport), K(self_addr));
  } else if (OB_FAIL(xa_ctx_mgr_.init())) {
    TRANS_LOG(WARN, "xa ctx mgr init failed", K(ret));
  } else if (OB_FAIL(xa_proxy_.init(req_transport, self_addr))) {
    STORAGE_LOG(WARN, "fail to init xa rpc proxy", K(ret));
  } else if (OB_FAIL(xa_rpc_.init(&xa_proxy_, self_addr))) {
    TRANS_LOG(WARN, "xa rpc init failed", K(ret));
  } else if (OB_FAIL(timer_.init("XATimeWheel"))) {
    TRANS_LOG(WARN, "xa timer init failed", K(ret));
  } else if (OB_FAIL(xa_trans_heartbeat_worker_.init(this))) {
    TRANS_LOG(ERROR, "xa trans relocate worker init error", KR(ret));
  } else if (OB_FAIL(xa_inner_table_gc_worker_.init(this))) {
    TRANS_LOG(WARN, "xa inner table gc worker init error", KR(ret));
  } else if (OB_FAIL(xa_statistics_v2_.init(MTL_ID()))) {
    TRANS_LOG(WARN, "xa statistics init error", KR(ret));
  } else if (OB_FAIL(dblink_statistics_.init(MTL_ID()))) {
    TRANS_LOG(WARN, "dblink statistics init error", KR(ret));
  } else {
    is_inited_ = true;
  }
  TRANS_LOG(INFO, "xa service init", K(ret));

  return ret;
}

void ObXAService::destroy()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    if (is_running_) {
      stop();
      wait();
    }
    xa_inner_table_gc_worker_.destroy();
    xa_trans_heartbeat_worker_.destroy();
    xa_ctx_mgr_.destroy();
    timer_.destroy();
    xa_rpc_.destroy();
    xa_proxy_.destroy();
    xa_statistics_v2_.destroy();
    dblink_statistics_.destroy();
    is_inited_ = false;
  }
  TRANS_LOG(INFO, "xa service destroy");
}

int ObXAService::start()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "xa service is not inited", K(ret));
  } else if (OB_UNLIKELY(is_running_)) {
    TRANS_LOG(WARN, "xa service is already running");
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(xa_rpc_.start())) {
    TRANS_LOG(WARN, "xa rpc start failed", K(ret));
  } else if (OB_FAIL(timer_.start())) {
    TRANS_LOG(WARN, "xa timer start failed", K(ret));
  } else if (OB_FAIL(xa_trans_heartbeat_worker_.start())) {
    TRANS_LOG(WARN, "ObXATransHeartbeatWorker start error", KR(ret));
  } else if (OB_FAIL(xa_inner_table_gc_worker_.start())) {
    TRANS_LOG(WARN, "xa inner table gc worker start error", KR(ret));
  } else {
    is_running_ = true;
  }
  TRANS_LOG(INFO, "xa service start", KR(ret));

  return ret;
}

void ObXAService::stop()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "xa service is not inited", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "xa service is not running", K(ret));
  } else if (OB_FAIL(xa_rpc_.stop())) {
    TRANS_LOG(WARN, "xa rpc stop failed", K(ret));
  } else if (OB_FAIL(timer_.stop())) {
    TRANS_LOG(WARN, "xa timer stop failed", K(ret));
  } else {
    is_running_ = false;
    xa_trans_heartbeat_worker_.stop();
    xa_inner_table_gc_worker_.stop();
  }
  TRANS_LOG(INFO, "xa service stop", KR(ret));

  return;
}

void ObXAService::wait()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "xa service is not inited", K(ret));
  } else if (OB_UNLIKELY(is_running_)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa service is running", K(ret));
  } else if (OB_FAIL(xa_rpc_.wait())) {
    TRANS_LOG(WARN, "xa rpc wait failed", K(ret));
  } else if (OB_FAIL(timer_.wait())) {
    TRANS_LOG(WARN, "xa timer wait failed", K(ret));
  } else {
    xa_trans_heartbeat_worker_.wait();
    xa_inner_table_gc_worker_.wait();
  }
  TRANS_LOG(INFO, "xa service wait", KR(ret));

  return;
}

int ObXAService::remote_one_phase_xa_commit_(const ObXATransID &xid,
                                             const ObTransID &trans_id,
                                             const uint64_t tenant_id,
                                             const ObAddr &sche_addr,
                                             const int64_t timeout_us,
                                             const int64_t request_id,
                                             bool &has_tx_level_temp_table)
{
  int ret = OB_SUCCESS;
  int result = OB_SUCCESS;
  int64_t now = ObTimeUtility::current_time();
  obrpc::ObXACommitRPCRequest req;
  ObXACommitRPCCB cb;
  ObTransCond cond;
  const int64_t wait_time = (INT64_MAX - now) / 2;

  if (OB_FAIL(cb.init(&cond, &has_tx_level_temp_table))) {
    TRANS_LOG(WARN, "ObXARPCCB init failed", KR(ret));
  } else if (OB_FAIL(req.init(trans_id, xid, timeout_us, request_id))) {
    TRANS_LOG(WARN, "init ObXACommitRPCRequest failed", KR(ret), K(trans_id), K(xid));
  } else if (OB_FAIL(xa_rpc_.xa_commit(tenant_id, sche_addr, req, cb))) {
    TRANS_LOG(WARN, "xa end trans rpc failed", KR(ret), K(req), K(sche_addr));
  } else if (OB_FAIL(cond.wait(wait_time, result))) {
    TRANS_LOG(WARN, "wait xa_end_trans rpc callback failed", KR(ret), K(req), K(sche_addr));
  } else if (OB_SUCCESS != result) {
    TRANS_LOG(WARN, "xa_end_trans rpc failed result", K(result), K(req), K(sche_addr));
  }

  #ifdef ERRSIM
  int tmp_ret = OB_E(EventTable::EN_XA_1PC_RESP_LOST) OB_SUCCESS;;
  if (OB_SUCCESS != tmp_ret) {
    TRANS_LOG(INFO, "ERRSIM, origin sche ctx not exist");
    result = OB_TRANS_CTX_NOT_EXIST;
  }
  #endif

  if (OB_SUCC(ret)) {
    ret = result;
  }

  // for statistics
  if (OB_SUCC(ret)) {
    XA_STAT_ADD_XA_COMMIT_REMOTE_COUNT();
  }

  return ret;
}

int ObXAService::local_one_phase_xa_commit_(const ObXATransID &xid,
                                            const ObTransID &trans_id,
                                            const int64_t timeout_us,
                                            const int64_t request_id,
                                            bool &has_tx_level_temp_table)
{
  int ret = OB_SUCCESS;
  ObXACtx *xa_ctx = NULL;
  bool alloc = false;
  share::ObLSID coordinator;
  int64_t end_flag = 0;
  ObTransID moke_tx_id;

  if (OB_FAIL(xa_ctx_mgr_.get_xa_ctx(trans_id, alloc, xa_ctx))) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = query_xa_coord_from_tableone(MTL_ID(), xid, coordinator,
            moke_tx_id, end_flag))) {
      if (OB_ITER_END == tmp_ret) {
        ret = OB_TRANS_XA_NOTA;
        TRANS_LOG(WARN, "xid is not valid", K(ret), K(xid));
      } else {
        TRANS_LOG(WARN, "query xa scheduler failed", K(tmp_ret), K(xid));
      }
    } else if (coordinator.is_valid()) {
      ret = OB_TRANS_XA_PROTO;
      TRANS_LOG(WARN, "xa has entered the commit phase", K(ret), K(trans_id), K(xid), K(coordinator));
    } else {
      TRANS_LOG(WARN, "get xa ctx failed", K(ret), K(trans_id));
    }
    if (OB_SUCCESS == ret) {
      TRANS_LOG(ERROR, "unexpected return code", K(ret), K(xid), K(trans_id));
    }
  } else if (OB_ISNULL(xa_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa ctx is null", K(ret), K(trans_id));
  } else {
    if (OB_FAIL(xa_ctx->one_phase_end_trans(xid, false/*is_rollback*/, timeout_us, request_id))) {
      TRANS_LOG(WARN, "one phase xa commit failed", K(ret), K(xid), K(trans_id));
    } else if (OB_FAIL(xa_ctx->wait_one_phase_end_trans(false/*is_rollback*/, timeout_us))) {
      TRANS_LOG(WARN, "fail to wait one phase xa end trans", K(ret), K(xid), K(trans_id));
    } else {
      has_tx_level_temp_table = xa_ctx->has_tx_level_temp_table();
    }
    xa_ctx_mgr_.revert_xa_ctx(xa_ctx);
  }

  return ret;
}

int ObXAService::get_xa_ctx(const ObTransID &trans_id, bool &alloc, ObXACtx *&xa_ctx)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(xa_ctx_mgr_.get_xa_ctx(trans_id, alloc, xa_ctx))) {
    TRANS_LOG(WARN, "get xa ctx from mgr failed", K(ret), K(trans_id));
  }

  return ret;
}

int ObXAService::revert_xa_ctx(ObXACtx *xa_ctx)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(xa_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FAIL(xa_ctx_mgr_.revert_xa_ctx(xa_ctx))) {
    TRANS_LOG(WARN, "rever xa ctx failed", K(ret), KP(xa_ctx));
  }

  return ret;
}

#define INSERT_XA_STANDBY_TRANS_SQL "\
  insert into %s (tenant_id, gtrid, bqual, format_id, \
  trans_id, coordinator, scheduler_ip, scheduler_port, state, flag) \
  values (%lu, x'%.*s', x'%.*s', %ld, %ld, %ld, '%s', %d, %d, %ld)"

int ObXAService::insert_record_for_standby(const uint64_t tenant_id,
                                            const ObXATransID &xid,
                                            const ObTransID &trans_id,
                                            const share::ObLSID &coordinator,
                                            const ObAddr &sche_addr)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *mysql_proxy = MTL(ObTransService *)->get_mysql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  char scheduler_ip_buf[128] = {0};
  sche_addr.ip_to_string(scheduler_ip_buf, 128);
  char gtrid_str[128] = {0};
  int64_t gtrid_len = 0;
  char bqual_str[128] = {0};
  int64_t bqual_len = 0;
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  int64_t original_timeout_us = THIS_WORKER.get_timeout_ts();
  const int64_t start_ts = ObTimeUtility::current_time();
  THIS_WORKER.set_timeout_ts(start_ts + XA_INNER_TABLE_TIMEOUT);
  ObXAInnerSqlStatGuard stat_guard(start_ts);

  if (!is_valid_tenant_id(tenant_id) || xid.empty() || !trans_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(xid), K(trans_id));
  } else if (OB_FAIL(hex_print(xid.get_gtrid_str().ptr(),
                               xid.get_gtrid_str().length(),
                               gtrid_str, 128, gtrid_len))) {
    TRANS_LOG(WARN, "fail to convert gtrid to hex", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(hex_print(xid.get_bqual_str().ptr(),
                               xid.get_bqual_str().length(),
                               bqual_str, 128, bqual_len))) {
    TRANS_LOG(WARN, "fail to convert bqual to hex", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(sql.assign_fmt(INSERT_XA_STANDBY_TRANS_SQL,
                                    OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
                                    tenant_id,
                                    (int)gtrid_len, gtrid_str,
                                    (int)bqual_len, bqual_str,
                                    xid.get_format_id(),
                                    trans_id.get_id(),
                                    coordinator.id(),
                                    scheduler_ip_buf, sche_addr.get_port(),
                                    ObXATransState::ACTIVE, (long)0))) {
    TRANS_LOG(WARN, "generate insert xa trans sql fail", K(ret), K(sql));
  } else if (OB_FAIL(mysql_proxy->write(exec_tenant_id, sql.ptr(), affected_rows))) {
    TRANS_LOG(WARN, "execute insert record sql failed", KR(ret), K(exec_tenant_id), K(tenant_id));
  } else {
    // xa_statistics_.inc_cleanup_tx_count();
    // XA_INNER_INCREMENT_COMPENSATE_COUNT
    xa_statistics_v2_.inc_compensate_record_count();
    TRANS_LOG(INFO, "execute insert record sql success", K(exec_tenant_id), K(tenant_id),
              K(sql), K(affected_rows));
  }
  THIS_WORKER.set_timeout_ts(original_timeout_us);
  return ret;
}

#define INSERT_XA_LOCK_SQL "\
  insert into %s (tenant_id, gtrid, bqual, format_id, \
  trans_id, scheduler_ip, scheduler_port, state, flag) \
  values (%lu, x'%.*s', x'%.*s', %ld, %ld, '%s', %d, %d, %ld)"

int ObXAService::insert_xa_lock(ObISQLClient &client,
                                const uint64_t tenant_id,
                                const ObXATransID &xid,
                                const ObTransID &trans_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  char scheduler_ip_buf[128] = {0};
  GCTX.self_addr().ip_to_string(scheduler_ip_buf, 128);
  char gtrid_str[128] = {0};
  int64_t gtrid_len = 0;
  char bqual_str[128] = {0};
  const int64_t bqual_len = 0;

  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  int64_t original_timeout_us = THIS_WORKER.get_timeout_ts();
  const int64_t start_ts = ObTimeUtility::current_time();
  THIS_WORKER.set_timeout_ts(start_ts + XA_INNER_TABLE_TIMEOUT);
  ObXAInnerSqlStatGuard stat_guard(start_ts);

  if (!is_valid_tenant_id(tenant_id) || xid.empty()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(xid), K(trans_id));
  } else if (OB_FAIL(hex_print(xid.get_gtrid_str().ptr(),
                               xid.get_gtrid_str().length(),
                               gtrid_str, 128, gtrid_len))) {
    TRANS_LOG(WARN, "fail to convert gtrid to hex", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(sql.assign_fmt(INSERT_XA_LOCK_SQL,
                                    OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
                                    tenant_id,
                                    (int)gtrid_len, gtrid_str,
                                    (int)bqual_len, bqual_str,
                                    LOCK_FORMAT_ID, trans_id.get_id(),
                                    scheduler_ip_buf, GCTX.self_addr().get_port(),
                                    ObXATransState::ACTIVE,
                                    (long)ObXAFlag::OBTMNOFLAGS))) {
    TRANS_LOG(WARN, "generate insert xa trans sql fail",
              KR(ret), K(exec_tenant_id), K(tenant_id), K(sql));
  } else if (OB_FAIL(client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
    if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
      ret = OB_TRANS_XA_DUPID;
      TRANS_LOG(INFO, "xa lock already hold by others", K(tenant_id), K(xid));
    } else {
      TRANS_LOG(WARN, "execute insert xa trans sql fail", K(ret), K(sql), K(affected_rows));
    }
  } else if (OB_UNLIKELY(affected_rows != 1)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected error, update xa trans sql affects multiple rows or no rows",
              K(ret), K(tenant_id), K(affected_rows), K(sql));
  } else {
    TRANS_LOG(INFO, "insert xa lock", K(tenant_id), K(xid), K(trans_id));
  }

  THIS_WORKER.set_timeout_ts(original_timeout_us);

  return ret;
}

#define UPDATE_XA_LOCK_SQL "\
  update %s set trans_id = %ld\
  where tenant_id = %lu AND gtrid = x'%.*s' AND format_id = %ld"

int ObXAService::update_xa_lock(ObISQLClient &client,
                                const uint64_t tenant_id,
                                const ObXATransID &xid,
                                const ObTransID &trans_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  char scheduler_ip_buf[128] = {0};
  GCTX.self_addr().ip_to_string(scheduler_ip_buf, 128);
  char gtrid_str[128] = {0};
  int64_t gtrid_len = 0;
  char bqual_str[128] = {0};
  const int64_t bqual_len = 0;

  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  int64_t original_timeout_us = THIS_WORKER.get_timeout_ts();
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + XA_INNER_TABLE_TIMEOUT);

  if (!is_valid_tenant_id(tenant_id) || xid.empty() || !trans_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(xid), K(trans_id));
  } else if (OB_FAIL(hex_print(xid.get_gtrid_str().ptr(),
                               xid.get_gtrid_str().length(),
                               gtrid_str, 128, gtrid_len))) {
    TRANS_LOG(WARN, "fail to convert gtrid to hex", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(sql.assign_fmt(UPDATE_XA_LOCK_SQL,
                                    OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
                                    trans_id.get_id(),
                                    tenant_id,
                                    (int)gtrid_len, gtrid_str,
                                    LOCK_FORMAT_ID))) {
    TRANS_LOG(WARN, "generate update xa lock sql fail",
              KR(ret), K(exec_tenant_id), K(tenant_id), K(sql));
  } else if (OB_FAIL(client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
    TRANS_LOG(WARN, "execute update xa lock sql fail", K(ret), K(sql), K(affected_rows));
  } else if (OB_UNLIKELY(affected_rows != 1)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected error, update xa trans sql affects multiple rows or no rows",
              K(ret), K(tenant_id), K(affected_rows), K(sql));
  } else {
    TRANS_LOG(INFO, "update xa lock", K(tenant_id), K(xid), K(trans_id));
  }

  THIS_WORKER.set_timeout_ts(original_timeout_us);

  return ret;
}

#define INSERT_XA_TRANS_SQL "\
  insert into %s (tenant_id, gtrid, bqual, format_id, \
  trans_id, scheduler_ip, scheduler_port, state, flag) \
  values (%lu, x'%.*s', x'%.*s', %ld, %ld, '%s', %d, %d, %ld)"

int ObXAService::insert_xa_record(ObISQLClient &client,
                                  const uint64_t tenant_id,
                                  const ObXATransID &xid,
                                  const ObTransID &trans_id,
                                  const ObAddr &sche_addr,
                                  const int64_t flag)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  char scheduler_ip_buf[128] = {0};
  sche_addr.ip_to_string(scheduler_ip_buf, 128);
  char gtrid_str[128] = {0};
  int64_t gtrid_len = 0;
  char bqual_str[128] = {0};
  int64_t bqual_len = 0;

  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  int64_t original_timeout_us = THIS_WORKER.get_timeout_ts();
  const int64_t start_ts = ObTimeUtility::current_time();
  THIS_WORKER.set_timeout_ts(start_ts + XA_INNER_TABLE_TIMEOUT);
  ObXAInnerSqlStatGuard stat_guard(start_ts);

  if (!is_valid_tenant_id(tenant_id) || xid.empty() || !trans_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(xid), K(trans_id));
  } else if (OB_FAIL(hex_print(xid.get_gtrid_str().ptr(),
                               xid.get_gtrid_str().length(),
                               gtrid_str, 128, gtrid_len))) {
    TRANS_LOG(WARN, "fail to convert gtrid to hex", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(hex_print(xid.get_bqual_str().ptr(),
                               xid.get_bqual_str().length(),
                               bqual_str, 128, bqual_len))) {
    TRANS_LOG(WARN, "fail to convert bqual to hex", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(sql.assign_fmt(INSERT_XA_TRANS_SQL,
                                    OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
                                    tenant_id,
                                    (int)gtrid_len, gtrid_str,
                                    (int)bqual_len, bqual_str,
                                    xid.get_format_id(), trans_id.get_id(),
                                    scheduler_ip_buf, sche_addr.get_port(),
                                    ObXATransState::ACTIVE, flag))) {
    TRANS_LOG(WARN, "generate insert xa trans sql fail", K(ret), K(sql));
  } else if (OB_FAIL(client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
    TRANS_LOG(WARN, "execute insert xa trans sql fail",
              KR(ret), K(exec_tenant_id), K(tenant_id), K(sql), K(affected_rows));
    if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
      ret = OB_TRANS_XA_DUPID;
    }
  } else if (OB_UNLIKELY(affected_rows != 1)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected error, update xa trans sql affects multiple rows or no rows",
        K(ret), K(tenant_id), K(affected_rows), K(sql));
  } else {
    TRANS_LOG(INFO, "insert xa record", K(tenant_id), K(xid), K(trans_id));
  }

  THIS_WORKER.set_timeout_ts(original_timeout_us);

  return ret;
}

#define DELETE_XA_TRANS_SQL "delete from %s where \
  tenant_id = %lu and gtrid = x'%.*s' and bqual = x'%.*s' and format_id = %ld"

int ObXAService::delete_xa_record(const uint64_t tenant_id,
                                  const ObXATransID &xid)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *mysql_proxy = NULL;
  ObSqlString sql;
  int64_t affected_rows = 0;
  char gtrid_str[128] = {0};
  int64_t gtrid_len = 0;
  char bqual_str[128] = {0};
  int64_t bqual_len = 0;
  int64_t original_timeout_us = THIS_WORKER.get_timeout_ts();
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);

  const int64_t start_ts = ObTimeUtility::current_time();
  THIS_WORKER.set_timeout_ts(start_ts + XA_INNER_TABLE_TIMEOUT);
  ObXAInnerSqlStatGuard stat_guard(start_ts);

  if (!is_valid_tenant_id(tenant_id) || xid.empty()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(xid));
  } else if (FALSE_IT(mysql_proxy = MTL(ObTransService *)->get_mysql_proxy())) {
  } else if (OB_ISNULL(mysql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "mysql_proxy is null", K(ret), KP(mysql_proxy));
  } else if (OB_FAIL(hex_print(xid.get_gtrid_str().ptr(),
                               xid.get_gtrid_str().length(),
                               gtrid_str,
                               128,
                               gtrid_len))) {
    TRANS_LOG(WARN, "fail to convert gtrid to hex", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(hex_print(xid.get_bqual_str().ptr(),
                               xid.get_bqual_str().length(),
                               bqual_str,
                               128,
                               bqual_len))) {
    TRANS_LOG(WARN, "fail to convert bqual to hex", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(sql.assign_fmt(DELETE_XA_TRANS_SQL,
                                    OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
                                    tenant_id,
                                    (int)gtrid_len, gtrid_str,
                                    (int)bqual_len, bqual_str,
                                    xid.get_format_id()))) {
    TRANS_LOG(WARN, "generate delete xa trans sql fail", K(ret), K(sql));
  } else if (OB_FAIL(mysql_proxy->write(exec_tenant_id, sql.ptr(), affected_rows))) {
    TRANS_LOG(WARN, "execute delete xa trans sql fail",
              KR(ret), K(exec_tenant_id), K(tenant_id), K(sql), K(affected_rows));
  } else if (OB_UNLIKELY(affected_rows > 1)) {
    ret = OB_ERR_UNEXPECTED; // 删除了多行
    TRANS_LOG(ERROR, "update xa trans sql affects multiple rows", K(tenant_id), K(affected_rows), K(sql));
  } else if (OB_UNLIKELY(affected_rows == 0)) {
    // 没有删除行，可能有重复xid，作为回滚成功处理
    TRANS_LOG(WARN, "update xa trans sql affects no rows", K(tenant_id), K(sql));
  } else {
    TRANS_LOG(INFO, "delete xa record", K(tenant_id), K(xid), "lbt", lbt());
  }

  THIS_WORKER.set_timeout_ts(original_timeout_us);

  TRANS_LOG(INFO, "delete xa record", K(ret), K(xid));

  return ret;
}

#define DELETE_XA_LAST_TIGHTLY_BRANCH_SQL "delete from %s where \
  tenant_id = %lu and \
  ((gtrid = x'%.*s' and bqual=x'%.*s' and format_id=%ld) or\
  (gtrid = x'%.*s' and bqual='' and format_id=%ld))"

#define DELETE_XA_ALL_TIGHTLY_BRANCH_SQL "delete from %s where \
  tenant_id = %lu and gtrid = x'%.*s' and flag & %ld = 0"

int ObXAService::delete_xa_all_tightly_branch(const uint64_t tenant_id,
                                              const ObXATransID &xid)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *mysql_proxy = NULL;
  ObSqlString sql;
  int64_t affected_rows = 0;
  char gtrid_str[128] = {0};
  int64_t gtrid_len = 0;
  int64_t original_timeout_us = THIS_WORKER.get_timeout_ts();
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);

  const int64_t start_ts = ObTimeUtility::current_time();
  THIS_WORKER.set_timeout_ts(start_ts + XA_INNER_TABLE_TIMEOUT);
  ObXAInnerSqlStatGuard stat_guard(start_ts);

  if (!is_valid_tenant_id(tenant_id) || xid.empty()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(xid));
  } else if (FALSE_IT(mysql_proxy = MTL(ObTransService *)->get_mysql_proxy())) {
  } else if (OB_ISNULL(mysql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "mysql_proxy is null", K(ret), KP(mysql_proxy));
  } else if (OB_FAIL(hex_print(xid.get_gtrid_str().ptr(),
                               xid.get_gtrid_str().length(),
                               gtrid_str,
                               128,
                               gtrid_len))) {
    TRANS_LOG(WARN, "fail to convert gtrid to hex", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(sql.assign_fmt(DELETE_XA_ALL_TIGHTLY_BRANCH_SQL,
                                    OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
                                    tenant_id,
                                    (int)gtrid_len, gtrid_str,
                                    (long)ObXAFlag::OBLOOSELY))) {
    TRANS_LOG(WARN, "generate delete xa trans sql fail", K(ret), K(xid));
  } else if (OB_FAIL(mysql_proxy->write(exec_tenant_id, sql.ptr(), affected_rows))) {
    TRANS_LOG(WARN, "execute delete xa trans sql fail",
              KR(ret), K(exec_tenant_id), K(tenant_id), K(xid), K(sql), K(affected_rows));
  } else if (OB_UNLIKELY(affected_rows > 1)) {
    TRANS_LOG(INFO, "delete xa trans sql affects multiple rows",
              K(tenant_id), K(xid), K(affected_rows), K(sql));
  } else {
    TRANS_LOG(INFO, "delete xa record", K(tenant_id), K(xid), "lbt", lbt());
  }

  THIS_WORKER.set_timeout_ts(original_timeout_us);

  TRANS_LOG(INFO, "delete xa all tightly branch", K(ret), K(xid));

  return ret;
}

int ObXAService::delete_xa_branch(const uint64_t tenant_id,
                                  const ObXATransID &xid,
                                  const bool is_tightly_coupled)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *mysql_proxy = NULL;
  ObSqlString sql;
  int64_t affected_rows = 0;
  char gtrid_str[128] = {0};
  int64_t gtrid_len = 0;
  char bqual_str[128] = {0};
  int64_t bqual_len = 0;
  int64_t original_timeout_us = THIS_WORKER.get_timeout_ts();
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  const int64_t start_ts = ObTimeUtility::current_time();
  THIS_WORKER.set_timeout_ts(start_ts + XA_INNER_TABLE_TIMEOUT);
  ObXAInnerSqlStatGuard stat_guard(start_ts);

  if (!is_valid_tenant_id(tenant_id) || xid.empty()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(xid));
  } else if (FALSE_IT(mysql_proxy = MTL(ObTransService *)->get_mysql_proxy())) {
  } else if (OB_ISNULL(mysql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "mysql_proxy is null", K(ret), KP(mysql_proxy));
  } else if (OB_FAIL(hex_print(xid.get_gtrid_str().ptr(),
                               xid.get_gtrid_str().length(),
                               gtrid_str,
                               128,
                               gtrid_len))) {
    TRANS_LOG(WARN, "fail to convert gtrid to hex", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(hex_print(xid.get_bqual_str().ptr(),
                               xid.get_bqual_str().length(),
                               bqual_str,
                               128,
                               bqual_len))) {
    TRANS_LOG(WARN, "fail to convert bqual to hex", K(ret), K(tenant_id), K(xid));
  } else if (is_tightly_coupled) {
    // TIGHTLY
    // 仅删除lock和自己xid的，不能删除所有gtrid相同的branch
    if (OB_FAIL(sql.assign_fmt(DELETE_XA_LAST_TIGHTLY_BRANCH_SQL,
                               OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
                               tenant_id,
                               (int)gtrid_len, gtrid_str,
                               (int)bqual_len, bqual_str,
                               xid.get_format_id(),
                               (int)gtrid_len, gtrid_str,
                               LOCK_FORMAT_ID))) {
      TRANS_LOG(WARN, "generate delete xa trans sql fail", K(ret), K(xid));
    } else if (OB_FAIL(mysql_proxy->write(exec_tenant_id, sql.ptr(), affected_rows))) {
      TRANS_LOG(WARN, "execute delete xa trans sql fail", KR(ret),
                K(exec_tenant_id), K(tenant_id), K(sql), K(affected_rows));
    } else if (OB_UNLIKELY(affected_rows > 1)) {
      TRANS_LOG(INFO, "delete xa trans sql affects multiple rows",
                K(tenant_id), K(affected_rows), K(sql));
    } else {
      TRANS_LOG(INFO, "delete xa record for final tightly coupled branch",
                K(tenant_id), K(xid), "lbt", lbt());
    }
  } else {
    // LOOSELY
    if (OB_FAIL(sql.assign_fmt(DELETE_XA_TRANS_SQL,
                               OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
                               tenant_id,
                               (int)gtrid_len, gtrid_str,
                               (int)bqual_len, bqual_str,
                               xid.get_format_id()))) {
      TRANS_LOG(WARN, "generate delete xa trans sql fail", K(ret), K(sql));
    } else if (OB_FAIL(mysql_proxy->write(exec_tenant_id, sql.ptr(), affected_rows))) {
      TRANS_LOG(WARN, "execute delete xa trans sql fail",
                KR(ret), K(exec_tenant_id), K(tenant_id), K(sql), K(affected_rows));
    } else if (OB_UNLIKELY(affected_rows > 1)) {
      ret = OB_ERR_UNEXPECTED; // 删除了多行
      TRANS_LOG(ERROR, "delete xa trans sql affects multiple rows",
                K(tenant_id), K(xid), K(affected_rows), K(sql));
    } else if (OB_UNLIKELY(affected_rows == 0)) {
      // 没有删除行，可能有重复xid，作为回滚成功处理
      TRANS_LOG(WARN, "delete xa trans sql affects no rows", K(tenant_id), K(xid), K(sql));
    } else {
      TRANS_LOG(INFO, "delete xa record for loosely coupled branch",
                K(tenant_id), K(xid), "lbt", lbt());
    }
  }

  THIS_WORKER.set_timeout_ts(original_timeout_us);

  TRANS_LOG(INFO, "delete xa branch", K(ret), K(xid));

  return ret;
}

#define QUERY_XA_SCHEDULER_SQL "SELECT scheduler_ip, scheduler_port, trans_id, flag \
    FROM %s WHERE tenant_id = %lu AND gtrid = x'%.*s' AND bqual = x'%.*s' AND format_id = %ld"

int ObXAService::query_xa_scheduler_trans_id(const uint64_t tenant_id,
                                             const ObXATransID &xid,
                                             ObAddr &scheduler_addr,
                                             ObTransID &trans_id,
                                             int64_t &end_flag)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *mysql_proxy = NULL;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = NULL;
    char gtrid_str[128] = {0};
    int64_t gtrid_len = 0;
    char bqual_str[128] = {0};
    int64_t bqual_len = 0;
    int64_t original_timeout_us = THIS_WORKER.get_timeout_ts();
    const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);

    const int64_t start_ts = ObTimeUtility::current_time();
    THIS_WORKER.set_timeout_ts(start_ts + XA_INNER_TABLE_TIMEOUT);
    ObXAInnerSqlStatGuard stat_guard(start_ts);

    if (!is_valid_tenant_id(tenant_id) || xid.empty()) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(xid));
    } else if (FALSE_IT(mysql_proxy = MTL(ObTransService *)->get_mysql_proxy())) {
    } else if (OB_ISNULL(mysql_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "mysql_proxy is null", K(ret), KP(mysql_proxy));
    } else if (OB_FAIL(hex_print(xid.get_gtrid_str().ptr(),
                                 xid.get_gtrid_str().length(),
                                 gtrid_str,
                                 128,
                                 gtrid_len))) {
      TRANS_LOG(WARN, "fail to convert gtrid to hex", K(ret), K(tenant_id), K(xid));
    } else if (OB_FAIL(hex_print(xid.get_bqual_str().ptr(),
                                 xid.get_bqual_str().length(),
                                 bqual_str,
                                 128,
                                 bqual_len))) {
      TRANS_LOG(WARN, "fail to convert bqual to hex", K(ret), K(tenant_id), K(xid));
    } else if (OB_FAIL(sql.assign_fmt(QUERY_XA_SCHEDULER_SQL,
                                      OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
                                      tenant_id,
                                      (int)gtrid_len, gtrid_str,
                                      (int)bqual_len, bqual_str,
                                      xid.get_format_id()))) {
      TRANS_LOG(WARN, "generate query xa scheduler sql fail", K(ret), K(sql));
    } else if (OB_FAIL(mysql_proxy->read(res, exec_tenant_id, sql.ptr()))) {
      TRANS_LOG(WARN, "execute sql read fail", KR(ret), K(exec_tenant_id), K(tenant_id), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "execute sql fail", K(ret), K(tenant_id), K(sql));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END != ret) {
        TRANS_LOG(WARN, "iterate next result fail", K(ret), K(sql));
      }
    } else {
      int64_t id = 0;
      char scheduler_ip_buf[128] = {0};
      int64_t tmp_scheduler_ip_len = 0;
      int64_t scheduler_port = 0;

      EXTRACT_STRBUF_FIELD_MYSQL(*result, "scheduler_ip", scheduler_ip_buf, 128, tmp_scheduler_ip_len);
      EXTRACT_INT_FIELD_MYSQL(*result, "scheduler_port", scheduler_port, int64_t);
      EXTRACT_INT_FIELD_MYSQL(*result, "trans_id", id, int64_t);
      EXTRACT_INT_FIELD_MYSQL(*result, "flag", end_flag, int64_t);

      (void)tmp_scheduler_ip_len;

      if (OB_FAIL(ret)) {
        TRANS_LOG(WARN, "fail to extract field from result", K(ret));
      } else if (!scheduler_addr.set_ip_addr(scheduler_ip_buf, scheduler_port)) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "set scheduler addr failed", K(scheduler_ip_buf), K(scheduler_port));
      } else {
        trans_id = ObTransID(id);
        if (!trans_id.is_valid()) {
          ret = OB_INVALID_ARGUMENT;
          TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(xid));
        }
      }
    }

    THIS_WORKER.set_timeout_ts(original_timeout_us);
  }

  return ret;
}

int ObXAService::xa_scheduler_hb_req()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    TRANS_LOG(WARN, "ObXAService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObXAService is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_FAIL(xa_ctx_mgr_.xa_scheduler_hb_req())) {
    TRANS_LOG(WARN, "xa ctx mgr scheduler hb failed", K(ret));
  }
  return ret;
}

int ObXAService::gc_invalid_xa_record(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t gc_time_threshold = GCONF._xa_gc_timeout / 1000000;//in seconds
  int64_t original_timeout_us = THIS_WORKER.get_timeout_ts();
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + XA_INNER_TABLE_TIMEOUT);

  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(tenant_id));
  } else {
    if (OB_FAIL(gc_invalid_xa_record_(tenant_id, true, gc_time_threshold))) {
      TRANS_LOG(WARN, "gc invalid xa record which is generated by self fail", K(ret), K(tenant_id));
    }
    if (OB_FAIL(gc_invalid_xa_record_(tenant_id, false, 3 * gc_time_threshold))) {
      TRANS_LOG(WARN, "gc invalid xa record which isn't generated by self fail", K(ret), K(tenant_id));
    }
  }
  THIS_WORKER.set_timeout_ts(original_timeout_us);
  return ret;
}

#define XA_INNER_TABLE_GC_SQL "delete from %s where tenant_id = %lu and gtrid = x'%.*s'"
#define XA_INNER_TABLE_CHECK_SELF_SQL "select HEX(gtrid) as gtrid, trans_id\
  from %s where tenant_id = %lu and \
  coordinator is null and \
  format_id <> -2 and \
  scheduler_ip = '%s' and \
  scheduler_port = %d and \
  unix_timestamp(now()) - %ld > unix_timestamp(gmt_modified) limit 20"
#define XA_INNER_TABLE_CHECK_NOT_SELF_SQL "select HEX(gtrid) as gtrid, scheduler_ip, scheduler_port \
  from %s where tenant_id = %lu and \
  coordinator is null and \
  format_id <> -2 and \
  (scheduler_ip != '%s' or scheduler_port != %d) and \
  unix_timestamp(now()) - %ld > unix_timestamp(gmt_modified) limit 20"

int ObXAService::gc_invalid_xa_record_(const uint64_t tenant_id,
                                       const bool check_self,
                                       const int64_t gc_time_threshold)
{
  int ret = OB_SUCCESS;
  char scheduler_ip_buf[128] = {0};
  GCTX.self_addr().ip_to_string(scheduler_ip_buf, 128);
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  ObMySQLProxy *mysql_proxy = MTL(ObTransService *)->get_mysql_proxy();
  ObSqlString sql;

  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = NULL;
    int64_t affected_rows = 0;
    if (check_self) {
      if (OB_FAIL(sql.append_fmt(XA_INNER_TABLE_CHECK_SELF_SQL,
                                OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
                                tenant_id,
                                scheduler_ip_buf,
                                GCTX.self_addr().get_port(),
                                gc_time_threshold))) {
        TRANS_LOG(WARN, "generate check xa trans sql fail", K(ret), K(sql), K(tenant_id));
      }
    } else {
      if (OB_FAIL(sql.append_fmt(XA_INNER_TABLE_CHECK_NOT_SELF_SQL,
                                OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
                                tenant_id,
                                scheduler_ip_buf,
                                GCTX.self_addr().get_port(),
                                gc_time_threshold))) {
        TRANS_LOG(WARN, "generate check xa trans sql fail", K(ret), K(sql), K(tenant_id));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(mysql_proxy->read(res, exec_tenant_id, sql.ptr()))) {
        TRANS_LOG(WARN, "execute sql read fail", KR(ret), K(exec_tenant_id), 
                  K(tenant_id), K(sql), K(tenant_id));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "execute sql fail", K(ret), K(tenant_id), K(sql));
      } else {
        char gtrid_str[128] = {0};
        int64_t gtrid_len = 0;
        int64_t tx_id_value = 0;
        ObTransID tx_id;
        char tmp_scheduler_ip_buf[128] = {0};
        int64_t tmp_scheduler_ip_len = 0;
        int64_t tmp_scheduler_port = 0;
        bool need_delete = false;
        ObAddr scheduler_addr;
        int64_t trace_time = 0;
        bool is_alive = true;
        while (OB_SUCC(ret) && OB_SUCC(result->next())) {
          need_delete = false;
          affected_rows = 0;
          EXTRACT_STRBUF_FIELD_MYSQL(*result, "gtrid", gtrid_str, 128, gtrid_len);
          if (check_self) {
            EXTRACT_INT_FIELD_MYSQL(*result, "trans_id", tx_id_value, int64_t);
            tx_id = ObTransID(tx_id_value);
            if (OB_FAIL(ret)) {
              TRANS_LOG(WARN, "fail to extract field from result", K(ret));
            } else if (OB_FAIL(xa_ctx_mgr_.check_scheduler_exist(tx_id))) {
              if (OB_ENTRY_NOT_EXIST == ret) {
                ret = OB_SUCCESS;
                need_delete = true;
              }
            }
          } else {
            is_alive = true;
            EXTRACT_STRBUF_FIELD_MYSQL(*result, "scheduler_ip", tmp_scheduler_ip_buf, 128, tmp_scheduler_ip_len);
            EXTRACT_INT_FIELD_MYSQL(*result, "scheduler_port", tmp_scheduler_port, int64_t);
            if (OB_FAIL(ret)) {
              TRANS_LOG(WARN, "fail to extract field from result", K(ret));
            } else if (!scheduler_addr.set_ip_addr(tmp_scheduler_ip_buf, tmp_scheduler_port)) {
              ret = OB_ERR_UNEXPECTED;
              TRANS_LOG(WARN, "set scheduler addr failed", K(tmp_scheduler_ip_buf), K(tmp_scheduler_port));
            } else {
              share::ObAliveServerTracer *server_tracer = GCTX.server_tracer_;
              if (OB_ISNULL(server_tracer)) {
                ret = OB_ERR_UNEXPECTED;
                TRANS_LOG(WARN, "server tracer is NULL", KR(ret));
              } else if (OB_FAIL(server_tracer->is_alive(scheduler_addr, is_alive, trace_time))) {
                TRANS_LOG(WARN, "server tracer error", KR(ret));
                // To be conservative, if the server tracer reports an error, the scheduler
                // is alive by default
                ret = OB_SUCCESS;
                is_alive = true;
              }
              if (!is_alive) {
                need_delete = true;
              }
            }
          }
          if (need_delete) {
            if (OB_FAIL(sql.assign_fmt(XA_INNER_TABLE_GC_SQL,
                                              OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
                                              tenant_id,
                                              (int)gtrid_len, gtrid_str))) {
              TRANS_LOG(WARN, "generate query xa scheduler sql fail", K(ret), K(sql));
            } else if (OB_FAIL(mysql_proxy->write(exec_tenant_id, sql.ptr(), affected_rows))) {
              TRANS_LOG(WARN, "execute gc inner table sql failed", KR(ret), K(exec_tenant_id), K(tenant_id));
            } else {
              TRANS_LOG(INFO, "execute gc inner table sql success", K(exec_tenant_id), K(tenant_id),
                        K(sql), K(affected_rows));
            }
          }
          TRANS_LOG(INFO, "execute gc inner table sql", K(exec_tenant_id), K(tenant_id), K(sql), 
                    K(affected_rows), K(need_delete), K(gtrid_str), K(gtrid_len), K(tx_id_value));
        }
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          TRANS_LOG(INFO, "iterator end", KR(ret), K(exec_tenant_id), K(tenant_id), K(sql));
        }
      }
    }
  }
  
  return ret;
}

int ObXAService::xa_start(const ObXATransID &xid,
                          const int64_t flags,
                          const int64_t timeout_seconds,
                          const uint32_t session_id,
                          const ObTxParam &tx_param,
                          ObTxDesc *&tx_desc,
                          const uint64_t data_version)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(xid.empty()) ||
      OB_UNLIKELY(!xid.is_valid()) ||
      OB_UNLIKELY(timeout_seconds < 0)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arguments", K(ret), K(xid), K(timeout_seconds));
  } else if (!ObXAFlag::is_valid(flags, ObXAReqType::XA_START)) {
    ret = OB_TRANS_XA_INVAL;
    TRANS_LOG(WARN, "invalid flags for xa start", K(ret), K(xid), K(flags));
  } else if (ObXAFlag::is_tmnoflags(flags, ObXAReqType::XA_START)) {
    if (OB_FAIL(xa_start_(xid, flags, timeout_seconds, session_id, tx_param, tx_desc, data_version))) {
      TRANS_LOG(WARN, "xa start failed", K(ret), K(flags), K(xid));
    }
  } else if (ObXAFlag::is_tmjoin(flags) || ObXAFlag::is_tmresume(flags)) {
  // } else if (ObXAFlag::contain_tmjoin(flags) || ObXAFlag::contain_tmresume(flags)) {
    if (OB_FAIL(xa_start_join_(xid, flags, timeout_seconds, session_id, tx_desc))) {
      TRANS_LOG(WARN, "xa start join failed", K(ret), K(flags), K(xid), K(tx_desc));
    }
  } else {
    ret = OB_TRANS_XA_INVAL;
    TRANS_LOG(WARN, "invalid flags for xa start", K(ret), K(xid), K(flags));
  }
  // set xa_start_addr for txn-free-route
  if (OB_SUCC(ret) && OB_NOT_NULL(tx_desc)) {
    tx_desc->set_xa_start_addr(GCONF.self_addr_);
  }
  if (OB_FAIL(ret)) {
    // xa_statistics_.inc_failure_xa_start();
    TRANS_LOG(WARN, "xa start failed", K(ret), K(xid), K(flags), K(timeout_seconds));
  } else {
    // xa_statistics_.inc_success_xa_start();
    TRANS_LOG(INFO, "xa start", K(ret), K(xid), K(flags), K(timeout_seconds),
        "tx_id", tx_desc->get_tx_id(), KPC(tx_desc));
  }

  return ret;
}

// xa start for noflags
int ObXAService::xa_start_(const ObXATransID &xid,
                           const int64_t flags,
                           const int64_t timeout_seconds,
                           const uint32_t session_id,
                           const ObTxParam &tx_param,
                           ObTxDesc *&tx_desc,
                           const uint64_t data_version)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const bool is_tightly_coupled = !ObXAFlag::contain_loosely(flags);
  ObTransID trans_id;
  const uint64_t tenant_id = MTL_ID();
  ObAddr sche_addr = GCTX.self_addr();
  ObMySQLTransaction trans;
  bool alloc = true;
  ObXACtx *xa_ctx = NULL;
  int64_t end_flag = 0;
  bool is_first_xa_start = true;

  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  // step 1: if tightly coupled, insert lock record first.
  if (OB_FAIL(MTL(transaction::ObTransService *)->gen_trans_id(trans_id))) {
    TRANS_LOG(WARN, "gen trans id fail", K(ret), K(exec_tenant_id), K(xid));
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else {
    ObXAInnerSqlStatGuard stat_guard(ObTimeUtility::current_time());
    if (OB_FAIL(trans.start(MTL(ObTransService *)->get_mysql_proxy(), exec_tenant_id))) {
      TRANS_LOG(WARN, "trans start failed", K(ret), K(exec_tenant_id), K(xid));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else {
    if (is_tightly_coupled) {
      // tightly couple
      if (OB_FAIL(insert_xa_lock(trans, tenant_id, xid, trans_id))) {
        if (OB_TRANS_XA_DUPID == ret) {
          is_first_xa_start = false;
          TRANS_LOG(INFO, "xa lock already exists", K(ret), K(trans_id), K(xid));
          // rewrite code
          ret = OB_SUCCESS;
          trans.reset_last_error();
          ObXATransID lock_xid;
          if (OB_FAIL(lock_xid.set(xid.get_gtrid_str(), "", LOCK_FORMAT_ID))) {
            TRANS_LOG(WARN, "generate lock_xid failed", K(ret), K(xid));
          } else if (OB_FAIL(query_xa_scheduler_trans_id(tenant_id,
                                                         lock_xid,
                                                         sche_addr,
                                                         trans_id,
                                                         end_flag))) {
            TRANS_LOG(WARN, "query xa trans from inner table error", K(ret), K(xid));
          }
        } else {
          TRANS_LOG(WARN, "insert xa lock record failed", K(ret), K(trans_id), K(xid));
        }
      }
    } else {
      // loosely couple
      // first xa start
    }
  }
  // step 2: if first xa start, alloc tx_desc
  if (OB_FAIL(ret)) {
    ObXAInnerSqlStatGuard stat_guard(ObTimeUtility::current_time());
    (void)trans.end(false);
    TRANS_LOG(WARN, "insert or query xa lock record failed", K(ret), K(trans_id), K(xid));
  } else {
    if (tx_desc != NULL) {
      MTL(ObTransService *)->release_tx(*tx_desc);
      tx_desc = NULL;
    }
    if (is_first_xa_start) {
      // the first xa start for xa trans with this xid
      // therefore tx_desc should be allocated
      // this code may be moved to pl sql level
      if (OB_FAIL(MTL(ObTransService *)->acquire_tx(tx_desc, session_id, data_version))) {
        TRANS_LOG(WARN, "fail acquire trans", K(ret), K(tx_param));
      } else if (OB_FAIL(MTL(ObTransService *)->start_tx(*tx_desc, tx_param, trans_id))) {
        TRANS_LOG(WARN, "fail start trans", K(ret), KPC(tx_desc));
        MTL(ObTransService *)->release_tx(*tx_desc);
        tx_desc = NULL;
      }
    } else {
      // not first xa start
    }
  }
  // step 3: insert xa record and execute xa start
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "start trans failed", K(ret), K(trans_id), K(xid));
  } else if (OB_FAIL(insert_xa_record(trans, tenant_id, xid, trans_id, sche_addr, flags))) {
    // if there exists duplicated record in inner table, return OB_TRANS_XA_DUPID
    if (OB_TRANS_XA_DUPID == ret) {
      TRANS_LOG(WARN, "xa trans already exists", K(ret), K(trans_id), K(xid));
    } else {
      TRANS_LOG(WARN, "insert xa trans into inner table error", K(ret), K(trans_id), K(xid));
    }
    { // for inner sql statistics guard
      ObXAInnerSqlStatGuard stat_guard(ObTimeUtility::current_time());
      if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
        TRANS_LOG(WARN, "rollback lock record failed", K(tmp_ret), K(xid));
      }
    }
    if (is_first_xa_start) {
      if (OB_SUCCESS != (tmp_ret = MTL(ObTransService*)->abort_tx(*tx_desc,
        ObTxAbortCause::IMPLICIT_ROLLBACK))) {
        TRANS_LOG(WARN, "fail to abort transaction", K(tmp_ret), K(trans_id), K(xid));
      }
      MTL(ObTransService *)->release_tx(*tx_desc);
      tx_desc = NULL;
    }
  } else {
    // if enter this branch, tx_desc must be valid
    if (is_first_xa_start) {
      if (OB_FAIL(xa_ctx_mgr_.get_xa_ctx(trans_id, alloc, xa_ctx))) {
        TRANS_LOG(WARN, "get xa ctx failed", K(ret), K(xid));
      } else if (!alloc) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "unexpected error", K(ret), K(xid));
      } else if (OB_FAIL(xa_ctx->init(xid,
                                      trans_id,
                                      tenant_id,
                                      GCTX.self_addr(),
                                      is_tightly_coupled,
                                      this/*xa service*/,
                                      &xa_ctx_mgr_,
                                      &xa_rpc_,
                                      &timer_))) {
        TRANS_LOG(WARN, "xa ctx init failed", K(ret), K(xid), K(trans_id));
      } else if (OB_FAIL(xa_ctx->xa_start(xid, flags, timeout_seconds, tx_desc))) {
        TRANS_LOG(WARN, "xa ctx start failed", K(ret), K(xid), K(trans_id));
      }

      if (OB_SUCC(ret)) {
        //commit record
        { // for inner sql statistics guard
          ObXAInnerSqlStatGuard stat_guard(ObTimeUtility::current_time());
          if (OB_FAIL(trans.end(true))) {
            TRANS_LOG(WARN, "commit inner table trans failed", K(ret), K(xid));
          }
        }
        if (OB_FAIL(ret)) {
          const bool need_decrease_ref = true;
          if (OB_SUCCESS != (tmp_ret = MTL(ObTransService*)->abort_tx(*tx_desc,
            ObTxAbortCause::IMPLICIT_ROLLBACK))) {
            TRANS_LOG(WARN, "fail to abort transaction", K(tmp_ret), K(trans_id), K(xid));
          }
          xa_ctx->try_exit(need_decrease_ref);
          xa_ctx_mgr_.revert_xa_ctx(xa_ctx);
          tx_desc = NULL;
        } else {
          XA_STAT_ADD_XA_TRANS_START_COUNT();
        }
      } else {
        //rollback record
        { // for inner sql statistics guard
          ObXAInnerSqlStatGuard stat_guard(ObTimeUtility::current_time());
          if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
            TRANS_LOG(WARN, "rollback inner table trans failed", K(tmp_ret), K(xid));
          }
        }
        if (OB_NOT_NULL(xa_ctx)) {
          xa_ctx_mgr_.erase_xa_ctx(trans_id);
          xa_ctx_mgr_.revert_xa_ctx(xa_ctx);
        }
        // since tx_desc is not set into xa ctx, release tx desc explicitly
        if (OB_SUCCESS != (tmp_ret = MTL(ObTransService*)->abort_tx(*tx_desc,
          ObTxAbortCause::IMPLICIT_ROLLBACK))) {
          TRANS_LOG(WARN, "fail to abort transaction", K(tmp_ret), K(trans_id), K(xid));
        }
        MTL(ObTransService *)->release_tx(*tx_desc);
        tx_desc = NULL;
      }

    } else {
      // tightly coupled mode, xa start noflags
      // this xa start is not the first for this xa trans
      // therefore the tx_id is from the inner table
      { // for inner sql statistics guard
        ObXAInnerSqlStatGuard stat_guard(ObTimeUtility::current_time());
        if (OB_FAIL(trans.end(true))) {
          TRANS_LOG(WARN, "commit inner table trans failed", K(ret), K(xid));
        }
      }
      if (OB_FAIL(ret)) {
        // do nothing
      } else {
        alloc = (GCTX.self_addr() == sche_addr) ? false : true;
        bool need_retry = false;
        do {
          need_retry = false;
          if (OB_FAIL(xa_ctx_mgr_.get_xa_ctx(trans_id, alloc, xa_ctx))) {
            TRANS_LOG(WARN, "get xa ctx failed", K(ret), K(xid));
          } else if (OB_ISNULL(xa_ctx)) {
            ret = OB_ERR_UNEXPECTED;
            TRANS_LOG(WARN, "xa ctx is null", K(ret), K(xid), K(sche_addr));
          } else {
            if (alloc) {
              if (OB_FAIL(xa_ctx->init(xid,
                                       trans_id,
                                       tenant_id,
                                       sche_addr,
                                       is_tightly_coupled,
                                       this/*xa service*/,
                                       &xa_ctx_mgr_,
                                       &xa_rpc_,
                                       &timer_))) {
                TRANS_LOG(WARN, "init xa ctx failed", K(ret), K(xid));
                // if init fails, erase xa ctx
                xa_ctx_mgr_.erase_xa_ctx(trans_id);
              }
            } else {
              if (OB_FAIL(xa_ctx->wait_xa_start_complete())) {
                TRANS_LOG(WARN, "wait xa start complete", K(ret), K(xid));
              }
            }
          }
          if (OB_FAIL(ret)) {
            // stop retry
          } else {
            if (alloc) {
              // If alloc is true, it means that the xa start noflags is executed
              // in non-orginal scheduler.
              // Therefore, tx_desc shouled be synchronized from original scheduler.
              if (OB_FAIL(xa_ctx->xa_start_remote_first(xid, flags, timeout_seconds, tx_desc))) {
                TRANS_LOG(WARN, "xa ctx start failed", K(ret), K(xid));
                // if fail, erase xa ctx
                xa_ctx_mgr_.erase_xa_ctx(trans_id);
              }
            } else {
              if (OB_FAIL(xa_ctx->xa_start_second(xid, flags, timeout_seconds, tx_desc))) {
                TRANS_LOG(WARN, "xa ctx start failed", K(ret), K(xid));
                if (is_tightly_coupled && OB_TRANS_IS_EXITING == ret) {
                  xa_ctx_mgr_.revert_xa_ctx(xa_ctx);
                  xa_ctx = NULL;
                  need_retry = true;
                  alloc = (GCTX.self_addr() == sche_addr) ? false : true;
                } else if (OB_TRANS_XA_BRANCH_FAIL == ret) {
                  const bool need_decrease_ref = false;
                  xa_ctx->try_exit(need_decrease_ref);
                }
              }
            }
          }
        } while (need_retry);

        if (OB_FAIL(ret)) {
          if (OB_NOT_NULL(xa_ctx)) {
            xa_ctx_mgr_.revert_xa_ctx(xa_ctx);
          }
          if (OB_TRANS_XA_BRANCH_FAIL == ret) {
            if (OB_SUCCESS != (tmp_ret = delete_xa_all_tightly_branch(tenant_id, xid))) {
              TRANS_LOG(WARN, "delete all xa tightly branch failed", K(tmp_ret), K(xid));
            }
          } else {
            if (OB_SUCCESS != (tmp_ret = delete_xa_record(tenant_id, xid))) {
              TRANS_LOG(WARN, "delete xa record failed", K(tmp_ret), K(xid), K(flags));
            }
          }
        } // end if fail
      }
      // xa_start on new session, adjust tx_desc.sess_id_
      if (OB_SUCC(ret)) {
        tx_desc->set_sessid(session_id);
        tx_desc->set_assoc_sessid(session_id);
      }
    }
  }

  return ret;
}

int ObXAService::xa_start_join_(const ObXATransID &xid,
                                const int64_t flags,
                                const int64_t timeout_seconds,
                                const uint32_t session_id,
                                ObTxDesc *&tx_desc)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObAddr scheduler_addr;
  ObTransID trans_id;
  const uint64_t tenant_id = MTL_ID();
  int64_t end_flag = 0;

  if (OB_FAIL(query_xa_scheduler_trans_id(tenant_id,
                                          xid,
                                          scheduler_addr,
                                          trans_id,
                                          end_flag))) {
    // xa trans does not exist
    if (OB_ITER_END == ret) {
      ret = OB_TRANS_XA_NOTA;
      TRANS_LOG(WARN, "no existing xid to join or resume", K(ret), K(xid));
    } else {
      TRANS_LOG(WARN, "query xa trans from inner table error", K(ret), K(xid));
    }
  } else if (!trans_id.is_valid() || !scheduler_addr.is_valid()) {
    ret = OB_TRANS_XA_RMFAIL;
    TRANS_LOG(WARN, "invalid arguments from inner table", K(ret), K(xid),
        K(flags), K(end_flag), K(trans_id), K(scheduler_addr));
  } else {
    if (NULL != tx_desc) {
      MTL(ObTransService *)->release_tx(*tx_desc);
      tx_desc = NULL;
    }
    const bool is_tightly_coupled = !ObXAFlag::contain_loosely(end_flag);
    ObXACtx *xa_ctx = NULL;
    bool need_retry = false;
    bool alloc = (GCTX.self_addr() == scheduler_addr) ? false : true;
    do {
      need_retry = false;
      if (OB_FAIL(xa_ctx_mgr_.get_xa_ctx(trans_id, alloc, xa_ctx))) {
        TRANS_LOG(WARN, "get xa ctx failed", K(ret), K(xid), K(trans_id));
      } else if (OB_ISNULL(xa_ctx)) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "xa ctx is null", K(ret), K(scheduler_addr), K(xid), K(trans_id));
      } else {
        if (alloc) {
          if (OB_FAIL(xa_ctx->init(xid,
                                   trans_id,
                                   tenant_id,
                                   scheduler_addr,
                                   is_tightly_coupled,
                                   this,
                                   &xa_ctx_mgr_,
                                   &xa_rpc_,
                                   &timer_))) {
            TRANS_LOG(WARN, "xa ctx init failed", K(ret), K(xid));
            // if init fails, erase xa ctx
            xa_ctx_mgr_.erase_xa_ctx(trans_id);
          }
        } else {
          if (OB_FAIL(xa_ctx->wait_xa_start_complete())) {
            TRANS_LOG(WARN, "wait xa start complete failed", K(ret), K(xid), K(trans_id));
          }
        }
      }
      if (OB_FAIL(ret)) {
        // stop retry
      } else {
        if (alloc) {
          // If alloc is true, it means that the xa start noflags is executed
          // in non-orginal scheduler.
          // Therefore, tx_desc shouled be synchronized from original scheduler.
          if (OB_FAIL(xa_ctx->xa_start_remote_first(xid, flags, timeout_seconds, tx_desc))) {
            TRANS_LOG(WARN, "xa ctx start failed", K(ret), K(xid));
            // if fail, erase xa ctx
            xa_ctx_mgr_.erase_xa_ctx(trans_id);
          }
        } else {
          if (OB_FAIL(xa_ctx->xa_start_second(xid, flags, timeout_seconds, tx_desc))) {
            TRANS_LOG(WARN, "xa ctx start failed", K(ret), K(xid));
            // must be handled here or may affect error handling
            if (is_tightly_coupled && OB_TRANS_IS_EXITING == ret) {
              xa_ctx_mgr_.revert_xa_ctx(xa_ctx);
              xa_ctx = NULL;
              need_retry = true;
              alloc = (GCTX.self_addr() == scheduler_addr) ? false : true;
            } else if (OB_TRANS_XA_BRANCH_FAIL == ret) {
              const bool need_decrease_ref = false;
              xa_ctx->try_exit(need_decrease_ref);
            }
          }
        }
      }
    } while (need_retry);
    if (OB_FAIL(ret)) {
      // ATTENTION, check here!!!
      if (OB_NOT_NULL(xa_ctx)) {
        xa_ctx_mgr_.revert_xa_ctx(xa_ctx);
      }
      if (OB_TRANS_XA_BRANCH_FAIL == ret
          && OB_SUCCESS != (tmp_ret = delete_xa_all_tightly_branch(tenant_id, xid))) {
        TRANS_LOG(WARN, "delete all xa tightly branch failed", K(tmp_ret), K(xid));
      }
    }
  }
  // xa_join/resume on new session, adjust tx_desc.sess_id_
  if (OB_SUCC(ret)) {
    tx_desc->set_sessid(session_id);
    tx_desc->set_assoc_sessid(session_id);
  }
  return ret;
}

int ObXAService::xa_end(const ObXATransID &xid,
                        const int64_t flags,
                        ObTxDesc *&tx_desc)
{
  int ret = OB_SUCCESS;
  ObXACtx *xa_ctx = NULL;

  if (!xid.is_valid() || NULL == tx_desc) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(xid));
  } else if (!ObXAFlag::is_valid(flags, ObXAReqType::XA_END)) {
    ret = OB_TRANS_XA_INVAL;
    TRANS_LOG(WARN, "invalid flags for xa end", K(ret), K(flags), K(xid));
  } else if (!tx_desc->is_xa_trans()) {
    ret = OB_TRANS_XA_PROTO;
    TRANS_LOG(WARN, "Routine invoked in an improper context", K(ret), K(xid));
  } else if (NULL == (xa_ctx = tx_desc->get_xa_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "transaction context is null", K(ret), K(xid));
  } else if (OB_FAIL(xa_ctx->xa_end(xid, flags, tx_desc))) {
    TRANS_LOG(WARN, "xa end failed", K(ret), K(xid), K(flags));
    if (OB_TRANS_XA_BRANCH_FAIL == ret) {
      tx_desc = NULL;
      xa_ctx_mgr_.revert_xa_ctx(xa_ctx);
    }
  } else {
    tx_desc = NULL;
    xa_ctx_mgr_.revert_xa_ctx(xa_ctx);
  }

  TRANS_LOG(INFO, "xa end", K(ret), K(xid), K(flags));

  return ret;
}

int ObXAService::start_stmt(const ObXATransID &xid, const uint32_t session_id, ObTxDesc &tx_desc)
{
  int ret = OB_SUCCESS;
  const ObTransID &tx_id = tx_desc.tid();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "xa service not inited", K(ret));
  } else if (tx_desc.is_xa_tightly_couple() || tx_desc.xa_start_addr() == GCONF.self_addr_) {
    ObXACtx *xa_ctx = tx_desc.get_xa_ctx();
    if (NULL == xa_ctx) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected trans descriptor", K(ret), K(tx_id), K(xid));
    } else if (OB_FAIL(xa_ctx->start_stmt(xid, session_id))) {
      TRANS_LOG(WARN, "xa trans start stmt failed", K(ret), K(tx_id), K(xid));
    } else if (OB_FAIL(xa_ctx->wait_start_stmt(session_id))) {
      TRANS_LOG(WARN, "fail to wait start stmt", K(ret), K(tx_id), K(xid));
    } else {
      TRANS_LOG(INFO, "xa trans start stmt", K(ret), K(tx_id), K(xid));
    }
  }
  return ret;
}

int ObXAService::end_stmt(const ObXATransID &xid, ObTxDesc &tx_desc)
{
  int ret = OB_SUCCESS;
  const ObTransID &tx_id = tx_desc.tid();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "xa service not inited", K(ret));
  } else if (tx_desc.is_xa_tightly_couple()) {
    ObXACtx *xa_ctx = tx_desc.get_xa_ctx();
    if (NULL == xa_ctx) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected trans descriptor", K(ret), K(tx_id), K(xid));
    } else if (OB_FAIL(xa_ctx->end_stmt(xid))) {
      TRANS_LOG(WARN, "xa trans end stmt failed", K(ret), K(tx_id), K(xid));
    } else {
      TRANS_LOG(INFO, "xa trans end stmt", K(ret), K(tx_id), K(xid));
    }
  }
  return ret;
}

int ObXAService::xa_commit(const ObXATransID &xid,
                           const int64_t flags,
                           const int64_t xa_timeout_seconds,
                           bool &has_tx_level_temp_table,
                           ObTransID &tx_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();

  if (OB_UNLIKELY(!xid.is_valid())
      || OB_UNLIKELY(!ObXAFlag::is_valid(flags, ObXAReqType::XA_COMMIT))
      || 0 > xa_timeout_seconds) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(xid), K(flags), K(xa_timeout_seconds));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "xa service not inited", K(ret), K(xid));
  } else {
    const int64_t timeout_us = xa_timeout_seconds * 1000000;
    const int64_t request_id = start_ts;
    if (ObXAFlag::is_tmnoflags(flags, ObXAReqType::XA_COMMIT)) {
      if (OB_FAIL(two_phase_xa_commit_(xid, timeout_us, request_id, has_tx_level_temp_table,
              tx_id))) {
        TRANS_LOG(WARN, "two phase xa commit failed", K(ret), K(xid));
        // xa_statistics_.inc_failure_xa_2pc_commit();
      } else {
        // xa_statistics_.inc_success_xa_2pc_commit();
      }
    } else if (ObXAFlag::is_tmonephase(flags)) {
      if (OB_FAIL(one_phase_xa_commit_(xid, timeout_us, request_id, has_tx_level_temp_table,
              tx_id))) {
        TRANS_LOG(WARN, "one phase xa commit failed", K(ret), K(xid));
        // xa_statistics_.inc_failure_xa_1pc_commit();
      } else {
        // xa_statistics_.inc_success_xa_1pc_commit();
      }
    } else {
      ret = OB_TRANS_XA_INVAL;
      TRANS_LOG(WARN, "invalid flags for xa commit", K(ret), K(xid), K(flags));
    }
  }

  TRANS_LOG(INFO, "xa commit", K(ret), K(xid), K(tx_id), K(flags), K(xa_timeout_seconds));
  return ret;
}

int ObXAService::one_phase_xa_commit_(const ObXATransID &xid,
                                      const int64_t timeout_us,
                                      const int64_t request_id,
                                      bool &has_tx_level_temp_table,
                                      ObTransID &tx_id)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  ObAddr sche_addr;
  const bool is_rollback = false;
  int64_t end_flag = 0;
  share::ObLSID coordinator;

  if (OB_FAIL(query_sche_and_coord(tenant_id,
                                   xid,
                                   sche_addr,
                                   coordinator,
                                   tx_id,
                                   end_flag))) {
    if (OB_ITER_END == ret) {
      ret = OB_TRANS_XA_NOTA;
      TRANS_LOG(WARN, "xid is not valid", K(ret), K(xid));
    } else {
      TRANS_LOG(WARN, "query xa scheduler failed", K(ret), K(xid));
    }
  } else if (OB_UNLIKELY(!sche_addr.is_valid()) || OB_UNLIKELY(!tx_id.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "invalid xa arg from inner table", K(ret), K(sche_addr), K(xid), K(tx_id));
  } else if (coordinator.is_valid()) {
    ret = OB_TRANS_XA_PROTO;
    TRANS_LOG(WARN, "xa has entered the commit phase", K(ret), K(tx_id), K(xid), K(coordinator));
  } else {
    int tmp_ret = OB_SUCCESS;
    bool need_delete = true;
    if (sche_addr == GCTX.self_addr()) {
      if (OB_FAIL(local_one_phase_xa_commit_(xid, tx_id, timeout_us, request_id, has_tx_level_temp_table))) {
        TRANS_LOG(WARN, "local one phase commit failed", K(ret), K(tx_id), K(xid));
      }
    } else {
      if (OB_FAIL(remote_one_phase_xa_commit_(xid, tx_id, tenant_id, sche_addr, timeout_us, request_id,
                                              has_tx_level_temp_table))) {
        TRANS_LOG(WARN, "remote one phase commit failed", K(ret), K(tx_id), K(xid));
      }
    }

    if (OB_TRANS_XA_PROTO == ret) {
      need_delete = false;
    } else if (OB_TRANS_CTX_NOT_EXIST == ret || OB_TRANS_IS_EXITING == ret) {
      // check xa trans state again
      if (OB_SUCCESS != (tmp_ret = query_sche_and_coord(tenant_id,
                                                        xid,
                                                        sche_addr,
                                                        coordinator,
                                                        tx_id,
                                                        end_flag))) {
        if (OB_ITER_END == tmp_ret) {
          ret = OB_TRANS_XA_NOTA;
          TRANS_LOG(WARN, "xid is not valid", K(ret), K(xid));
        } else {
          TRANS_LOG(WARN, "query xa scheduler failed", K(tmp_ret), K(xid));
        }
        need_delete = false;
      } else if (coordinator.is_valid()) {
        // xa prepare may be completed, do not delete record
        need_delete = false;
      }
    }
    // xa_proto is returned when in tightly couple mode, there are
    // still multiple branches and one phase commit is triggered.
    // Under such condition, oracle would not delete inner table record
    if (need_delete) {
      const bool is_tightly = !ObXAFlag::contain_loosely(end_flag);
      if (OB_SUCCESS != (tmp_ret = delete_xa_branch(tenant_id, xid, is_tightly))) {
        TRANS_LOG(WARN, "delete xa record failed", K(tmp_ret), K(xid), K(is_tightly));
      }
    }
  }
  // for statistics
  XA_STAT_ADD_XA_ONE_PHASE_COMMIT_TOTAL_COUNT();

  return ret;
}

int ObXAService::xa_rollback(const ObXATransID &xid,
                             const int64_t xa_timeout_seconds,
                             ObTransID &tx_id)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  int64_t end_flag = 0;
  ObAddr sche_addr;
  share::ObLSID coordinator;
  const int64_t timeout_us = xa_timeout_seconds * 1000000;
  const int64_t request_id = ObTimeUtility::current_time();

  if (OB_UNLIKELY(!xid.is_valid())
      || 0 > xa_timeout_seconds) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(xid), K(xa_timeout_seconds));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "xa service not inited", K(ret), K(xid));
  } else if (OB_FAIL(query_sche_and_coord(tenant_id,
                                          xid,
                                          sche_addr,
                                          coordinator,
                                          tx_id,
                                          end_flag))) {
    if (OB_ITER_END == ret) {
      uint64_t data_version = 0;
      if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
        TRANS_LOG(WARN, "fail to get min data version", KR(ret), K(tenant_id));
      } else if (data_version < DATA_VERSION_4_2_0_0) {
        if (OB_FAIL(query_xa_coordinator_with_xid(tenant_id, xid, tx_id, coordinator))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          } else {
            TRANS_LOG(WARN, "fail to query xa coordinator from pending transaction", K(ret), K(xid), K(tenant_id));
          }
        } else if (OB_FAIL(xa_rollback_for_pending_trans_(xid, tx_id, timeout_us, tenant_id, request_id,
                                                          !ObXAFlag::contain_loosely(end_flag), coordinator))) {
          TRANS_LOG(WARN, "fail to rollback xa trans for pending transaction", K(ret), K(xid), K(tenant_id));
        } else {
          TRANS_LOG(INFO, "rollback xa trans for pending transaction success", K(ret), K(xid), K(tenant_id));
        }
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      TRANS_LOG(WARN, "fail to query xa scheduler and trans id from global transaction", K(ret), K(xid), K(tenant_id));
    }
  } else {
    const bool is_original = (GCTX.self_addr() == sche_addr);
    if (is_original || coordinator.is_valid()) {
      if (OB_FAIL(xa_rollback_local_(xid, tx_id, timeout_us, coordinator, end_flag, request_id))) {
        TRANS_LOG(WARN, "fail to do xa rollback local", K(ret), K(tx_id), K(xid), K(timeout_us));
      }
    } else {
      if (OB_FAIL(xa_rollback_remote_(xid, tx_id, sche_addr, timeout_us, request_id))) {
        TRANS_LOG(WARN, "fail to do xa rollback remote", K(ret), K(tx_id), K(xid),
            K(timeout_us));
      }
    }
  }
  TRANS_LOG(INFO, "xa rollback", K(ret), K(xid), K(xa_timeout_seconds));
  // xa_statistics_.inc_xa_rollback();
  return ret;
}

int ObXAService::xa_rollback_for_pending_trans_(const ObXATransID &xid,
                                               const ObTransID &tx_id,
                                               const int64_t timeout_us,
                                               const uint64_t tenant_id,
                                               const int64_t request_id,
                                               const bool is_tightly_coupled,
                                               const share::ObLSID &coord)
{
  int ret = OB_SUCCESS;
  ObXACtx *xa_ctx = NULL;
  bool alloc = true;
  if (OB_FAIL(xa_ctx_mgr_.get_xa_ctx(tx_id, alloc, xa_ctx))) {
    TRANS_LOG(WARN, "get xa ctx failed", K(ret), K(tx_id), K(xid), K(alloc), KP(xa_ctx));
  } else if (OB_ISNULL(xa_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa ctx is null", K(ret), K(xid), K(tx_id));
  } else {
    if (alloc && OB_FAIL(xa_ctx->init(xid,
                                      tx_id,
                                      tenant_id,
                                      GCTX.self_addr(),
                                      is_tightly_coupled,
                                      this,
                                      &xa_ctx_mgr_,
                                      &xa_rpc_,
                                      &timer_))) {
      TRANS_LOG(WARN, "xa ctx init failed", K(ret), K(xid), K(tx_id));
    } else {
      if (OB_FAIL(xa_ctx->two_phase_end_trans(xid, coord, true/*is_rollback*/, timeout_us, request_id))) {
        if (OB_TRANS_ROLLBACKED != ret) {
          TRANS_LOG(WARN, "xa rollback failed", K(ret), K(xid), K(tx_id));
        } else {
          ret = OB_SUCCESS;
        }
      } else if (OB_FAIL(xa_ctx->wait_two_phase_end_trans(xid, true/*is_rollback*/, timeout_us))) {
        TRANS_LOG(WARN, "wait xa rollback failed", K(ret), K(xid), K(tx_id));
      } else {
        // do nothing
      }
      if (OB_SUCC(ret)) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = delete_xa_pending_record(tenant_id, tx_id))) {
          TRANS_LOG(WARN, "fail to delete xa record from pending trans", K(ret), K(xid), K(tx_id));
        }
        TRANS_LOG(INFO, "xa rollback success", K(ret), K(xid), K(tx_id));
      }
    }
  }

  return ret;
}

int ObXAService::xa_rollback_local(const ObXATransID &xid,
                                   const ObTransID &tx_id,
                                   const int64_t timeout_us,
                                   const int64_t request_id)
{
  int ret = OB_SUCCESS;
  int64_t end_flag = 0;
  share::ObLSID coordinator;
  
  if (OB_UNLIKELY(!xid.is_valid())
      || OB_UNLIKELY(!tx_id.is_valid())
      || 0 > timeout_us) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(xid), K(tx_id), K(timeout_us));
  } else if (OB_FAIL(xa_rollback_local_(xid, tx_id, timeout_us, coordinator, end_flag, request_id))) {
    TRANS_LOG(WARN, "fail to do xa rollback local", K(ret), K(tx_id), K(xid),
        K(timeout_us));
  } else {
    // do nothing
  }
  TRANS_LOG(INFO, "xa rollback local", K(ret), K(tx_id), K(xid), K(timeout_us));

  return ret;
}

int ObXAService::xa_rollback_local_(const ObXATransID &xid,
                                    const ObTransID &tx_id,
                                    const int64_t timeout_us,
                                    const share::ObLSID &coord,
                                    int64_t &end_flag,
                                    const int64_t request_id)
{
  int ret = OB_SUCCESS;
  
  if (coord.is_valid()) {
    if (OB_FAIL(two_phase_xa_rollback_(xid, tx_id, timeout_us, coord, end_flag, request_id))) {
      TRANS_LOG(WARN, "two phase xa rollback fail", K(ret), K(xid));
    }
  } else if (OB_FAIL(one_phase_xa_rollback_(xid, tx_id, timeout_us, end_flag, request_id))){
    TRANS_LOG(WARN, "one phase xa rollback fail", K(ret), K(xid));
  }
  if (OB_SUCC(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = delete_xa_branch(MTL_ID(), xid, !ObXAFlag::contain_loosely(end_flag)))) {
      TRANS_LOG(WARN, "delete xa branch failed", K(tmp_ret), K(xid), K(tx_id));
    }
    TRANS_LOG(INFO, "delete xa branch", K(tmp_ret), K(xid), K(tx_id));
  }

  return ret;
}

int ObXAService::two_phase_xa_rollback_(const ObXATransID &xid,
                                        const ObTransID &tx_id,
                                        const int64_t timeout_us,
                                        const share::ObLSID &coord,
                                        const int64_t end_flag,
                                        const int64_t request_id)
{
  int ret = OB_SUCCESS;
  ObXACtx *xa_ctx = NULL;
  bool alloc = true;
  bool is_tightly_coupled = !ObXAFlag::contain_loosely(end_flag);

  if (OB_FAIL(xa_ctx_mgr_.get_xa_ctx(tx_id, alloc, xa_ctx))) {
    TRANS_LOG(WARN, "get xa ctx failed", K(ret), K(tx_id), K(xid), K(alloc), KP(xa_ctx));
  } else if (OB_ISNULL(xa_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa ctx is null", K(ret), K(xid), K(tx_id));
  } else {
    if (alloc && OB_FAIL(xa_ctx->init(xid,
                                      tx_id,
                                      MTL_ID(),
                                      GCTX.self_addr(),
                                      is_tightly_coupled,
                                      this,
                                      &xa_ctx_mgr_,
                                      &xa_rpc_,
                                      &timer_))) {
      TRANS_LOG(WARN, "xa ctx init failed", K(ret), K(xid), K(tx_id));
    } else {
      if (OB_FAIL(xa_ctx->two_phase_end_trans(xid, coord, true/*is_rollback*/, timeout_us, request_id))) {
        if (OB_TRANS_ROLLBACKED != ret) {
          TRANS_LOG(WARN, "two phase xa rollback failed", K(ret), K(xid), K(tx_id));
        } else {
          ret = OB_SUCCESS;
          TRANS_LOG(INFO, "two phase xa rollback success", K(ret), K(xid), K(tx_id));
        }
      } else if (OB_FAIL(xa_ctx->wait_two_phase_end_trans(xid, true/*is_rollback*/, timeout_us))) {
        TRANS_LOG(WARN, "wait two phase xa rollback failed", K(ret), K(xid), K(tx_id));
      } else {
        TRANS_LOG(INFO, "two phase xa rollback success", K(ret), K(xid), K(tx_id));
      }
    }
    xa_ctx_mgr_.revert_xa_ctx(xa_ctx);
  }
  xa_cache_.clean_prepare_cache_item(xid);

  return ret;
}

// in original scheduler
int ObXAService::one_phase_xa_rollback_(const ObXATransID &xid,
                                        const ObTransID &tx_id,
                                        const int64_t timeout_us,
                                        int64_t &end_flag,
                                        const int64_t request_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool alloc = false;
  ObXACtx *xa_ctx = NULL;
  share::ObLSID coordinator;
  // mock tmp_tx_id for query global transaction second time
  ObTransID tmp_tx_id;

  if (OB_FAIL(xa_ctx_mgr_.get_xa_ctx(tx_id, alloc, xa_ctx))) {
    if (OB_TRANS_CTX_NOT_EXIST == ret) {
      if (OB_SUCCESS != (tmp_ret = query_xa_coord_from_tableone(MTL_ID(),
                                                                xid,
                                                                coordinator,
                                                                tmp_tx_id,
                                                                end_flag))) {
        if (OB_ITER_END == tmp_ret) {
          ret = OB_SUCCESS;
        } else {
          TRANS_LOG(WARN, "fail to query xa coordinator from global transaction", K(ret), K(xid),
              K(MTL_ID()));
        }
      } else if (coordinator.is_valid()) {
        if (OB_FAIL(two_phase_xa_rollback_(xid, tx_id, timeout_us, coordinator, end_flag,
                request_id))) {
          TRANS_LOG(WARN, "fail to query xa coordinator from global transaction", K(ret), K(xid),
              K(MTL_ID()));
        }
      } else {
        // the xa trans does not enter into two phase commit
        // return succcess
        ret = OB_SUCCESS;
        TRANS_LOG(INFO, "invalid coordinator, the xa trans has been finished", K(ret), K(tx_id),
            K(xid), K(alloc), KP(xa_ctx));
      }
    } else {
      TRANS_LOG(WARN, "get xa ctx failed", K(ret), K(tx_id), K(xid), K(alloc), KP(xa_ctx));
    }
  } else if (OB_ISNULL(xa_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa ctx is null", K(ret), K(tx_id), K(xid));
  } else {
    // if there exists xa ctx, one phase xa rollback is required
    if (OB_FAIL(xa_ctx->one_phase_end_trans(xid, true/*is_rollback*/, timeout_us, request_id))) {
      if (OB_TRANS_ROLLBACKED != ret) {
        TRANS_LOG(WARN, "one phase xa rollback failed", K(ret), K(tx_id));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (OB_FAIL(xa_ctx->wait_one_phase_end_trans(true/*is_rollback*/, timeout_us))) {
      TRANS_LOG(WARN, "fail to wait one phase xa end trans", K(ret), K(xid), K(tx_id));
    }
    xa_ctx_mgr_.revert_xa_ctx(xa_ctx);
  }

  return ret;
}

int ObXAService::xa_rollback_remote_(const ObXATransID &xid,
                                     const ObTransID &tx_id,
                                     const ObAddr &sche_addr,
                                     const int64_t timeout_us,
                                     const int64_t request_id)
{
  int ret = OB_SUCCESS;
  int result = OB_SUCCESS;
  int64_t now = ObTimeUtility::current_time();
  const int64_t wait_time = (INT64_MAX - now) / 2;
  const int64_t expire_ts = ObTimeUtility::current_time() + timeout_us;

  do {
    obrpc::ObXARollbackRPCRequest req;
    obrpc::ObXARPCCB<obrpc::OB_XA_ROLLBACK> cb;
    ObTransCond cond;
    result = OB_SUCCESS;
    if (OB_FAIL(cb.init(&cond))) {
      TRANS_LOG(WARN, "ObXARPCCB init failed", KR(ret));
    } else if (OB_FAIL(req.init(tx_id, xid, timeout_us, request_id))) {
      TRANS_LOG(WARN, "fail to init xa rollback rpc request", KR(ret), K(tx_id), K(xid));
    } else if (OB_FAIL(xa_rpc_.xa_rollback(MTL_ID(), sche_addr, req, &cb))) {
      TRANS_LOG(WARN, "xa rollback rpc failed", KR(ret), K(req), K(sche_addr));
    } else if (OB_FAIL(cond.wait(wait_time, result))) {
      TRANS_LOG(WARN, "wait xa rollback rpc callback failed", KR(ret), K(req), K(sche_addr));
    } else if (OB_SUCCESS != result) {
      // do nothing
    }
  } while (OB_SUCC(ret) && (OB_EAGAIN == result || OB_TIMEOUT == result) && expire_ts > ObTimeUtility::current_time());

  if (OB_SUCC(ret)) {
    switch (result) {
      case OB_SUCCESS: {
        // do nothing
        break;
      }
      case OB_TIMEOUT:
      case OB_EAGAIN: {
        ret = OB_TRANS_XA_RMFAIL;
        TRANS_LOG(WARN, "xa rollback rpc failed", KR(ret), K(result), K(sche_addr));
        break;
      }
      default: {
        ret = OB_TRANS_XA_PROTO;
        TRANS_LOG(WARN, "xa rollback rpc failed", KR(ret), K(result), K(sche_addr));
        break;
      }
    }
  }

  #ifdef ERRSIM
  int tmp_ret = OB_E(EventTable::EN_XA_1PC_RESP_LOST) OB_SUCCESS;;
  if (OB_SUCCESS != tmp_ret) {
    TRANS_LOG(INFO, "ERRSIM, origin sche ctx not exist");
    result = OB_TRANS_CTX_NOT_EXIST;
  }
  #endif
  // for statistics
  if (OB_SUCC(ret)) {
    XA_STAT_ADD_XA_ROLLBACK_REMOTE_COUNT();
  }

  return ret;
}

// this is only used for session terminate
int ObXAService::handle_terminate_for_xa_branch(const ObXATransID &xid, ObTxDesc *tx_desc, const int64_t xa_end_timeout_seconds)
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  ObAddr self = MTL(ObTransService *)->get_server();

  if (NULL == tx_desc || !tx_desc->is_valid() || !xid.is_valid() || xa_end_timeout_seconds < 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KP(tx_desc), K(xid), K(xa_end_timeout_seconds));
  } else {
    const uint64_t tenant_id = tx_desc->get_tenant_id();
    const int64_t timeout_us = xa_end_timeout_seconds * 1000 * 1000;
    ObXACtx *xa_ctx = tx_desc->get_xa_ctx();
    ObTransID tx_id = tx_desc->tid();
    int tmp_ret = OB_SUCCESS;
    bool is_first_terminate = true;
    TRANS_LOG(INFO, "start to terminate xa trans", K(xid), K(tx_id), "lbt", lbt());
    if (NULL == xa_ctx) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "xa ctx is null, may be stmt fail or rollback", K(ret), K(tx_id), K(xid));
    } else if (!xa_ctx->is_tightly_coupled()) {
      // loosely coupled mode
      if (OB_FAIL(xa_ctx->xa_rollback_session_terminate(is_first_terminate))) {
        TRANS_LOG(WARN, "rollback xa trans failed", K(ret), K(tx_id), K(xid));
      }
      // if tmp scheduler, we needs to send terminate request to the original scheduler
      // send the terminate request to the original scheduler no matter whether the rollback succeeds
      if (is_first_terminate && xa_ctx->get_original_sche_addr() != self) {
        // original scheduler is in remote
        // send terminate to original scheduler
        if (OB_SUCCESS != (tmp_ret = terminate_to_original_(xid, tx_id,
                xa_ctx->get_original_sche_addr(), timeout_us))) {
          TRANS_LOG(WARN, "terminate remote original scheduler failed", K(tmp_ret), K(xid),
              K(tx_id), K(xa_ctx->get_original_sche_addr()), K(timeout_us));
        }
      }
      if (OB_SUCCESS != (tmp_ret = xa_ctx->clear_branch_for_xa_terminate(xid))) {
        TRANS_LOG(WARN, "clear branch for xa terminate failed", K(ret), K(xid), K(tx_id));
      }
      if (OB_SUCCESS != (tmp_ret = delete_xa_branch(tenant_id, xid, false))) {
        TRANS_LOG(WARN, "delete xa record failed", K(ret), K(xid), K(tx_id));
      }
      xa_ctx_mgr_.revert_xa_ctx(xa_ctx);
      tx_desc = NULL;
      TRANS_LOG(INFO, "handle terminate for loosely coupled xa branch", K(ret), K(tx_id), K(xid));
    } else {
      // tightly coupled mode
      // regardless of the location of scheduler, try to acquire lock first
      int64_t expired_time = now + 10000000;  // 10s
      while (!xa_ctx->is_terminated()
          && OB_TRANS_STMT_NEED_RETRY == (ret = xa_ctx->stmt_lock_with_guard(xid))) {
        if (ObTimeUtility::current_time() > expired_time) {
          TRANS_LOG(INFO, "no need to wait, force to terminate", K(tx_id), K(xid));
          break;
        }
        ob_usleep(100);
      }
      if (xa_ctx->is_terminated()) {
        // avoid the terminate operations of different branches
        TRANS_LOG(INFO, "xa trans has terminated", K(tx_id), K(xid));
      } else {
        if (OB_FAIL(xa_ctx->xa_rollback_session_terminate(is_first_terminate))) {
          TRANS_LOG(WARN, "rollback xa trans failed", K(ret), K(tx_id), K(xid));
        }
        if (is_first_terminate && xa_ctx->get_original_sche_addr() != self) {
          // original scheduler is in remote
          // send terminate to original scheduler
          if (OB_SUCCESS != (tmp_ret = terminate_to_original_(xid, tx_id,
                  xa_ctx->get_original_sche_addr(), timeout_us))) {
            TRANS_LOG(WARN, "terminate remote original scheduler failed", K(tmp_ret), K(xid),
                K(tx_id), K(xa_ctx->get_original_sche_addr()), K(timeout_us));
          }
        }
      }
      if (OB_SUCCESS != (tmp_ret = xa_ctx->clear_branch_for_xa_terminate(xid))) {
        TRANS_LOG(WARN, "clear branch for xa terminate failed", K(ret), K(xid), K(tx_id));
      }
      if (OB_SUCCESS != (tmp_ret = delete_xa_all_tightly_branch(tenant_id, xid))) {
        TRANS_LOG(WARN, "delete xa tight branch failed", K(ret), K(xid));
      }
      xa_ctx_mgr_.revert_xa_ctx(xa_ctx);
      tx_desc = NULL;
      TRANS_LOG(INFO, "handle terminate for tightly coupled xa branch", K(ret), K(xid), K(tx_id));
    }
  }

  return ret;
}

// send terminate rpc to original scheduler
int ObXAService::terminate_to_original_(const ObXATransID &xid,
                                        const ObTransID &tx_id,
                                        const ObAddr &original_sche_addr,
                                        const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  int result = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const int64_t now = ObTimeUtility::current_time();
  const int64_t expire_ts = now + timeout_us;

  do {
    obrpc::ObXATerminateRPCRequest req;
    obrpc::ObXARPCCB<obrpc::OB_XA_TERMINATE> cb;
    ObTransCond cond;
    // rely on timeout of cb, therefore timeout of cond is set to max
    const int64_t wait_time = (INT64_MAX - now) / 2;
    if (OB_FAIL(cb.init(&cond))) {
      TRANS_LOG(WARN, "ObXARPCCB init failed", KR(ret));
    } else if (OB_FAIL(req.init(tx_id, xid, timeout_us))) {
      TRANS_LOG(WARN, "init ObXATerminateRPCRequest failed", KR(ret), K(xid), K(tx_id));
    } else if (OB_FAIL(xa_rpc_.xa_terminate(tenant_id, original_sche_addr, req, &cb))) {
      TRANS_LOG(WARN, "xa proxy terminate failed", KR(ret), K(original_sche_addr), K(req));
    } else if (OB_FAIL(cond.wait(wait_time, result))) {
      TRANS_LOG(WARN, "wait xa_terminate rpc callback failed", KR(ret),
          K(req), K(original_sche_addr));
    } else if (OB_SUCCESS != result) {
      TRANS_LOG(WARN, "xa_terminate rpc failed result", K(result),
          K(req), K(original_sche_addr));
    } else {
      // do nothing
    }
  } while (OB_SUCC(ret) && (OB_TIMEOUT == result && expire_ts > ObTimeUtility::current_time()));

  if (OB_SUCC(ret)) {
    if (OB_TRANS_CTX_NOT_EXIST == result || OB_TIMEOUT == result) {
      // if trans ctx does not exist or the rpc is timeout,
      // assume that the orginal scheduler has exited
      ret = OB_SUCCESS;
    } else {
      ret = result;
    }
  }
  return ret;
}

int ObXAService::xa_rollback_all_changes(const ObXATransID &xid, ObTxDesc *&tx_desc, const int64_t stmt_expired_time)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (NULL == tx_desc || !tx_desc->is_valid() || !xid.is_valid() || stmt_expired_time < 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KP(tx_desc), K(xid), K(stmt_expired_time));
  } else {
    ObTransID tx_id = tx_desc->get_tx_id();
    if (OB_FAIL(start_stmt(xid, 0/*unused session id*/, *tx_desc))) {
      TRANS_LOG(WARN, "xa start stmt fail", K(ret), K(xid), K(tx_id));
    } else {
      const transaction::ObTxSEQ savepoint = tx_desc->get_min_tx_seq();
      if (OB_FAIL(MTL(transaction::ObTransService *)->rollback_to_implicit_savepoint(*tx_desc,
              savepoint, stmt_expired_time, NULL))) {
        TRANS_LOG(WARN, "do savepoint rollback error", K(ret), K(xid), K(tx_id));
      } else {
        TRANS_LOG(INFO, "xa rollback all changes success", K(ret), K(xid), K(tx_id));
      }
      if (OB_SUCCESS != (tmp_ret = end_stmt(xid, *tx_desc))) {
        // return the error code of end stmt first
        ret = tmp_ret;
        TRANS_LOG(WARN, "xa end stmt fail", K(ret), K(xid), K(tx_id));
      }
    }
  }
  return ret;
}

// xa prepare
int ObXAService::xa_prepare(const ObXATransID &xid,
                            const int64_t timeout_seconds,
                            ObTransID &tx_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  common::ObAddr sche_addr;
  bool is_tightly_coupled = true;
  int64_t end_flag = 0;
  const uint64_t tenant_id = MTL_ID();
  share::ObLSID coordinator;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "xa service not inited", K(ret));
  } else if (OB_UNLIKELY(!xid.is_valid()) ||
             OB_UNLIKELY(0 > timeout_seconds)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(xid), K(timeout_seconds), K(tenant_id));
  } else if (OB_FAIL(query_sche_and_coord(tenant_id,
                                          xid,
                                          sche_addr,
                                          coordinator,
                                          tx_id,
                                          end_flag))) {
    if (OB_ITER_END == ret) {
      ret = OB_TRANS_XA_NOTA;
      TRANS_LOG(WARN, "invalid xid", K(ret), K(xid), K(tenant_id));
    } else {
      TRANS_LOG(WARN, "fail to query xa scheduler and trans id", K(ret), K(xid), K(tenant_id));
    }
  } else if (OB_UNLIKELY(coordinator.is_valid())) {
    ret = OB_TRANS_XA_RMERR;
    TRANS_LOG(WARN, "xa trans has been prepared", K(ret), K(xid), K(coordinator));
  } else if (OB_UNLIKELY(!sche_addr.is_valid()) || OB_UNLIKELY(!tx_id.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "invalid xa arguments from inner table", K(ret), K(xid), K(tx_id),
        K(sche_addr), K(tenant_id));
  } else {
    is_tightly_coupled = !ObXAFlag::contain_loosely(end_flag);
    const bool is_original = (sche_addr == GCTX.self_addr());
    const int64_t timeout_us = timeout_seconds * 1000 * 1000;
    if (is_original) {
      // original scheduler
      if (OB_FAIL(local_xa_prepare_(xid, tx_id, timeout_us))) {
        if (OB_TRANS_XA_RDONLY != ret) {
          TRANS_LOG(WARN, "local xa prepare failed", K(ret), K(xid), K(tx_id));
        }
      }
    } else {
      // remote scheduler
      if (OB_FAIL(remote_xa_prepare_(xid, tx_id, tenant_id, sche_addr, timeout_us))) {
        if (OB_TRANS_XA_RDONLY != ret) {
          TRANS_LOG(WARN, "remote xa prepare failed", K(ret), K(xid), K(tx_id));
        }
      }
    }
  }

  // 1. tightly coupled, OB_ERR_READ_ONLY_TRANSACTION, delete lock and record
  // 2. tightly coupled, OB_TRANS_XA_RDONLY, delete record only
  // 3. loosely coupled, OB_ERR_READ_ONLY_TRANSACTION, delete record
  if (OB_TRANS_XA_RDONLY == ret) {
    if (OB_SUCCESS != (tmp_ret = delete_xa_record(tenant_id, xid))) {
      TRANS_LOG(WARN, "delete xa record failed", K(tmp_ret), K(tenant_id), K(xid));
    }
  } else if (OB_ERR_READ_ONLY_TRANSACTION == ret) {
    // submited to scheduler, and found it is read only
    if (OB_SUCCESS != (tmp_ret = delete_xa_branch(tenant_id, xid, is_tightly_coupled))) {
      TRANS_LOG(WARN, "delete xa branch failed", K(tmp_ret), K(xid), K(tenant_id), K(tx_id));
    }
    ret = OB_TRANS_XA_RDONLY;
  }

  if (OB_FAIL(ret) && OB_TRANS_XA_RDONLY != ret) {
    // xa_statistics_.inc_failure_xa_prepare();
  } else {
    // xa_statistics_.inc_success_xa_prepare();
  }
  return ret;
}

int ObXAService::xa_prepare_for_original(const ObXATransID &xid,
                                         const ObTransID &tx_id,
                                         const int64_t timeout_seconds)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool is_tightly_coupled = true;
  const uint64_t tenant_id = MTL_ID();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "xa service not inited", K(ret));
  } else if (OB_UNLIKELY(!xid.is_valid()) ||
             OB_UNLIKELY(0 > timeout_seconds)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(xid), K(timeout_seconds), K(tenant_id));
  } else {
    const int64_t timeout_us = timeout_seconds * 1000 * 1000;
    // original scheduler
    if (OB_FAIL(local_xa_prepare_(xid, tx_id, timeout_us))) {
      if (OB_TRANS_XA_RDONLY != ret) {
        TRANS_LOG(WARN, "local xa prepare failed", K(ret), K(xid), K(tx_id));
      }
    }
  }

  if (OB_TRANS_XA_RDONLY == ret) {
    if (OB_SUCCESS != (tmp_ret = delete_xa_record(tenant_id, xid))) {
      TRANS_LOG(WARN, "delete xa record failed", K(tmp_ret), K(tenant_id), K(xid));
    }
  } else if (OB_ERR_READ_ONLY_TRANSACTION == ret) {
    // submited to scheduler, and found it is read only
    if (OB_SUCCESS != (tmp_ret = delete_xa_branch(tenant_id, xid, is_tightly_coupled))) {
      TRANS_LOG(WARN, "delete xa branch failed", K(tmp_ret), K(xid), K(tenant_id), K(tx_id));
    }
    ret = OB_TRANS_XA_RDONLY;
  }

  return ret;
}

// this interface is ONLY for original scheduler
// executte xa prepare in original scheduler
int ObXAService::local_xa_prepare(const ObXATransID &xid,
                                  const ObTransID &tx_id,
                                  const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ret = local_xa_prepare_(xid, tx_id, timeout_us);
  return ret;
}

int ObXAService::local_xa_prepare_(const ObXATransID &xid,
                                   const ObTransID &tx_id,
                                   const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  bool alloc = false;
  ObXACtx *xa_ctx = NULL;
  if (OB_FAIL(xa_ctx_mgr_.get_xa_ctx(tx_id, alloc, xa_ctx))) {
    int64_t xa_state = ObXATransState::UNKNOWN;
    if (OB_SUCCESS == xa_cache_.query_prepare_cache_item(xid, xa_state) && ObXATransState::PREPARED == xa_state) {
      TRANS_LOG(INFO, "xa_cache hit", K(xid), K(tx_id));
      ret = OB_SUCCESS;
    } else {
      TRANS_LOG(WARN, "get xa ctx failed", K(ret), K(xid), K(tx_id), KP(xa_ctx));
    }
  } else if (OB_ISNULL(xa_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa ctx is null", K(ret), K(xid), K(tx_id));
  } else {
    if (OB_FAIL(xa_ctx->xa_prepare(xid, timeout_us))) {
      if (OB_TRANS_XA_RDONLY != ret || OB_ERR_READ_ONLY_TRANSACTION != ret) {
        TRANS_LOG(WARN, "fail to execute local xa prepare", K(ret), K(xid), K(tx_id),
            K(timeout_us));
      } else {
        TRANS_LOG(INFO, "read only xa trans branch", K(ret), K(xid), K(tx_id),
            K(timeout_us));
      }
    } else if (OB_FAIL(xa_ctx->wait_xa_prepare(xid, timeout_us))) {
      TRANS_LOG(WARN, "wait drive prepare failed", K(ret), K(xid), K(tx_id),
            K(timeout_us));
    }
    xa_ctx_mgr_.revert_xa_ctx(xa_ctx);
  }

  return ret;
}

int ObXAService::remote_xa_prepare_(const ObXATransID &xid,
                                    const ObTransID &tx_id,
                                    const uint64_t tenant_id,
                                    const common::ObAddr &sche_addr,
                                    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  int result = OB_SUCCESS;
  int64_t retry_times = 5;
  bool need_retry = false;
  int64_t now = ObTimeUtility::current_time();

  do {
    obrpc::ObXAPrepareRPCRequest req;
    obrpc::ObXARPCCB<obrpc::OB_XA_PREPARE> rpc_cb;
    ObTransCond cond;
    const int64_t wait_time = (INT64_MAX / 2 ) - now;
    if (OB_FAIL(rpc_cb.init(&cond))) {
      TRANS_LOG(WARN, "ObXARPCCB init failed", KR(ret), K(xid));
    } else if (OB_FAIL(req.init(tx_id, xid, timeout_us))) {
      TRANS_LOG(WARN, "init ObXAPrepareRPCRequest failed", KR(ret), K(tx_id), K(xid));
    } else if (OB_FAIL(xa_rpc_.xa_prepare(tenant_id, sche_addr, req, rpc_cb))) {
      TRANS_LOG(WARN, "post xa prepare failed", KR(ret), K(xid), K(req), K(sche_addr));
    } else if (OB_FAIL(cond.wait(wait_time, result))) {
      TRANS_LOG(WARN, "wait xa prepare rpc callback failed", KR(ret), K(xid), K(req), K(sche_addr));
    } else if (OB_SUCCESS != result) {
      if (OB_TRANS_XA_RDONLY != result && OB_ERR_READ_ONLY_TRANSACTION != result) {
        TRANS_LOG(WARN, "xa prepare rpc failed", K(xid), K(result), K(req), K(sche_addr));
      } else {
        TRANS_LOG(INFO, "read only xa trans branch", K(xid), K(result), K(sche_addr));
      }
    }
    if (OB_TRANS_XA_RETRY == result) {
      ob_usleep(1000);//1ms
    }
    if (OB_FAIL(ret)) {
      need_retry = false;
    } else {
      need_retry = (--retry_times > 0 && OB_TIMEOUT == result) || (OB_TRANS_XA_RETRY == result);
    }
  } while (need_retry);

  #ifdef ERRSIM
  int err_switch = OB_E(EventTable::EN_XA_RPC_TIMEOUT) OB_SUCCESS;
  if (OB_SUCCESS != err_switch) {
    TRANS_LOG(INFO, "ERRSIM, rpc timeout");
    result = OB_TIMEOUT;
  }
  #endif

  if (OB_SUCC(ret)) {
    if (OB_FAIL(result)) {
      // ret = handle_remote_prepare_error_(result, tenant_id, xid);
      // just returns rmfail, TM would call xa_rollback,
      // and we expect that xa_rollback can deal with all cases
      ret = result;
    }
  }
  // for statistics
  if (OB_SUCC(ret)) {
    XA_STAT_ADD_XA_PREPARE_REMOTE_COUNT();
  }

  return ret;
}

#define DELETE_XA_PENDING_RECORD_SQL "delete from %s where \
  tenant_id = %lu and trans_id = %ld"
// delete record from pending trans (table two)
int ObXAService::delete_xa_pending_record(const uint64_t tenant_id,
                                          const ObTransID &tx_id)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *mysql_proxy = NULL;
  ObSqlString sql;
  int64_t affected_rows = 0;
  int64_t original_timeout_us = THIS_WORKER.get_timeout_ts();
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + XA_INNER_TABLE_TIMEOUT);
  if (!is_valid_tenant_id(tenant_id) || !tx_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(tx_id));
  } else if (FALSE_IT(mysql_proxy = MTL(ObTransService *)->get_mysql_proxy())) {
  } else if (OB_ISNULL(mysql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "mysql_proxy is null", K(ret), KP(mysql_proxy));
  } else if (OB_FAIL(sql.assign_fmt(DELETE_XA_PENDING_RECORD_SQL,
                                    OB_ALL_PENDING_TRANSACTION_TNAME,
                                    ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                                    tx_id.get_id()))) {
    TRANS_LOG(WARN, "generate delete xa trans sql fail", K(ret), K(sql));
  } else if (OB_FAIL(mysql_proxy->write(tenant_id, sql.ptr(), affected_rows))) {
    TRANS_LOG(WARN, "execute delete xa trans sql fail",
              KR(ret), K(tenant_id), K(sql), K(affected_rows));
  } else if (OB_UNLIKELY(affected_rows > 1)) {
    ret = OB_ERR_UNEXPECTED; // 删除了多行
    TRANS_LOG(ERROR, "update xa trans sql affects multiple rows", K(tenant_id), K(affected_rows), K(sql));
  } else if (OB_UNLIKELY(affected_rows == 0)) {
    // 没有删除行，可能有重复xid，作为回滚成功处理
    TRANS_LOG(WARN, "update xa trans sql affects no rows", K(tenant_id), K(sql));
  } else {
    TRANS_LOG(INFO, "delete xa record", K(tenant_id), "lbt", lbt());
  }
  THIS_WORKER.set_timeout_ts(original_timeout_us);
  TRANS_LOG(INFO, "delete xa record", K(ret), K(tx_id));
  return ret;
}

#define QUERY_XA_COORDINATOR_WITH_XID_SQL "\
  SELECT trans_id, coordinator FROM %s \
  WHERE gtrid = x'%.*s' AND bqual = x'%.*s' AND format_id = %ld AND tenant_id = %lu limit 1"
// query coord from pending trans (table two)
int ObXAService::query_xa_coordinator_with_xid(const uint64_t tenant_id,
                                               const ObXATransID &xid,
                                               ObTransID &trans_id,
                                               share::ObLSID &coordinator)
{
  int ret = OB_SUCCESS;
  char gtrid_str[128] = {0};
  int64_t gtrid_len = 0;
  char bqual_str[128] = {0};
  int64_t bqual_len = 0;
  ObMySQLProxy *mysql_proxy = NULL;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = NULL;
    int64_t original_timeout_us = THIS_WORKER.get_timeout_ts();
    THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + XA_INNER_TABLE_TIMEOUT);
    if (!is_valid_tenant_id(tenant_id) || !xid.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(trans_id));
    } else if (FALSE_IT(mysql_proxy = MTL(ObTransService *)->get_mysql_proxy())) {
    } else if (OB_ISNULL(mysql_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "mysql_proxy is null", K(ret), KP(mysql_proxy));
    } else if (OB_FAIL(hex_print(xid.get_gtrid_str().ptr(),
                                 xid.get_gtrid_str().length(),
                                 gtrid_str, 128, gtrid_len))) {
      TRANS_LOG(WARN, "fail to convert gtrid to hex", K(ret), K(tenant_id), K(xid));
    } else if (OB_FAIL(hex_print(xid.get_bqual_str().ptr(),
                                 xid.get_bqual_str().length(),
                                 bqual_str, 128, bqual_len))) {
      TRANS_LOG(WARN, "fail to convert bqual to hex", K(ret), K(tenant_id), K(xid));
    } else if (OB_FAIL(sql.assign_fmt(QUERY_XA_COORDINATOR_WITH_XID_SQL,
                                      OB_ALL_PENDING_TRANSACTION_TNAME,
                                      (int)gtrid_len, gtrid_str,
                                      (int)bqual_len, bqual_str,
                                      xid.get_format_id(),
                                      ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)))) {
      TRANS_LOG(WARN, "generate query coordinator sql fail", K(ret));
    } else if (OB_FAIL(mysql_proxy->read(res, tenant_id, sql.ptr()))) {
      TRANS_LOG(WARN, "execute sql read fail", KR(ret), K(tenant_id), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "execute sql fail", K(ret), K(tenant_id), K(sql));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END != ret) {
        TRANS_LOG(WARN, "iterate next result fail", K(ret), K(sql));
      }
    } else {
      int64_t ls_id_value = 0;
      int64_t tx_id_value = 0;
      EXTRACT_INT_FIELD_MYSQL(*result, "coordinator", ls_id_value, int64_t);
      EXTRACT_INT_FIELD_MYSQL(*result, "trans_id", tx_id_value, int64_t);
      coordinator = ObLSID(ls_id_value);
      trans_id = ObTransID(tx_id_value);
      if (OB_FAIL(ret)) {
        TRANS_LOG(WARN, "fail to extract field from result", K(ret));
      }
    }
    THIS_WORKER.set_timeout_ts(original_timeout_us);
  }
  TRANS_LOG(INFO, "get trans id and coordinator from pending trans", K(ret), K(trans_id), K(coordinator));
  return ret;
}

// for __all_pending_transaction
#define INSERT_XA_PENDING_RECORD_SQL "\
  insert into %s (tenant_id, gtrid, bqual, format_id, \
  trans_id, coordinator, scheduler_ip, scheduler_port) \
  values (%lu, x'%.*s', x'%.*s', %ld, %ld, %ld, '%s', %d)"

int ObXAService::insert_xa_pending_record(const uint64_t tenant_id,
                                          const ObXATransID &xid,
                                          const ObTransID &tx_id,
                                          const share::ObLSID &coordinator,
                                          const common::ObAddr &sche_addr)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  char scheduler_ip_buf[128] = {0};
  sche_addr.ip_to_string(scheduler_ip_buf, 128);
  char gtrid_str[128] = {0};
  int64_t gtrid_len = 0;
  char bqual_str[128] = {0};
  int64_t bqual_len = 0;
  ObMySQLProxy *mysql_proxy = NULL;
  ObSqlString sql;

  int64_t original_timeout_us = THIS_WORKER.get_timeout_ts();
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + XA_INNER_TABLE_TIMEOUT);

  if (!is_valid_tenant_id(tenant_id) || xid.empty() || !tx_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(xid), K(tx_id));
  } else if (FALSE_IT(mysql_proxy = MTL(ObTransService *)->get_mysql_proxy())) {
  } else if (OB_ISNULL(mysql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "mysql proxy is null", K(ret), KP(mysql_proxy));
  } else if (OB_FAIL(hex_print(xid.get_gtrid_str().ptr(),
                               xid.get_gtrid_str().length(),
                               gtrid_str, 128, gtrid_len))) {
    TRANS_LOG(WARN, "fail to convert gtrid to hex", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(hex_print(xid.get_bqual_str().ptr(),
                               xid.get_bqual_str().length(),
                               bqual_str, 128, bqual_len))) {
    TRANS_LOG(WARN, "fail to convert bqual to hex", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(sql.assign_fmt(INSERT_XA_PENDING_RECORD_SQL,
                                    OB_ALL_PENDING_TRANSACTION_TNAME,
                                    ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id),
                                    (int)gtrid_len, gtrid_str,
                                    (int)bqual_len, bqual_str,
                                    xid.get_format_id(),
                                    tx_id.get_id(), coordinator.id(),
                                    scheduler_ip_buf, sche_addr.get_port()))) {
    TRANS_LOG(WARN, "generate insert xa trans sql fail", K(ret), K(sql));
  } else if (OB_FAIL(mysql_proxy->write(tenant_id, sql.ptr(), affected_rows))) {
    TRANS_LOG(WARN, "execute insert xa trans sql fail",
              KR(ret), K(tenant_id), K(sql), K(affected_rows));
    if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
      ret = OB_TRANS_XA_DUPID;
    }
  } else if (OB_UNLIKELY(affected_rows != 1)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected error, insert sql affects multiple rows or no rows",
        K(ret), K(tenant_id), K(affected_rows), K(sql));
  } else {
    TRANS_LOG(INFO, "insert xa record", K(tenant_id), K(xid), K(tx_id));
  }

  THIS_WORKER.set_timeout_ts(original_timeout_us);

  return ret;
}

#define QUERY_XA_COORDINATOR_TRANSID_SQL "\
  SELECT coordinator, trans_id, flag FROM %s WHERE \
  tenant_id = %lu AND gtrid = x'%.*s' AND bqual = x'%.*s' AND format_id = %ld"

// query coord from global transaction (table one)
int ObXAService::query_xa_coord_from_tableone(const uint64_t tenant_id,
                                              const ObXATransID &xid,
                                              share::ObLSID &coordinator,
                                              ObTransID &trans_id,
                                              int64_t &end_flag)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *mysql_proxy = NULL;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = NULL;
    char gtrid_str[128] = {0};
    int64_t gtrid_len = 0;
    char bqual_str[128] = {0};
    int64_t bqual_len = 0;
    int64_t original_timeout_us = THIS_WORKER.get_timeout_ts();
    const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);

    const int64_t start_ts = ObTimeUtility::current_time();
    THIS_WORKER.set_timeout_ts(start_ts + XA_INNER_TABLE_TIMEOUT);
    ObXAInnerSqlStatGuard stat_guard(start_ts);

    if (!is_valid_tenant_id(tenant_id) || xid.empty()) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(xid));
    } else if (FALSE_IT(mysql_proxy = MTL(ObTransService *)->get_mysql_proxy())) {
    } else if (OB_ISNULL(mysql_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "mysql_proxy is null", K(ret), KP(mysql_proxy));
    } else if (OB_FAIL(hex_print(xid.get_gtrid_str().ptr(),
                                 xid.get_gtrid_str().length(),
                                 gtrid_str,
                                 128,
                                 gtrid_len))) {
      TRANS_LOG(WARN, "fail to convert gtrid to hex", K(ret), K(tenant_id), K(xid));
    } else if (OB_FAIL(hex_print(xid.get_bqual_str().ptr(),
                                 xid.get_bqual_str().length(),
                                 bqual_str,
                                 128,
                                 bqual_len))) {
      TRANS_LOG(WARN, "fail to convert bqual to hex", K(ret), K(tenant_id), K(xid));
    } else if (OB_FAIL(sql.assign_fmt(QUERY_XA_COORDINATOR_TRANSID_SQL,
                                      OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
                                      tenant_id,
                                      (int)gtrid_len, gtrid_str,
                                      (int)bqual_len, bqual_str,
                                      xid.get_format_id()))) {
      TRANS_LOG(WARN, "generate query coordinator sql fail", K(ret));
    } else if (OB_FAIL(mysql_proxy->read(res, exec_tenant_id, sql.ptr()))) {
      TRANS_LOG(WARN, "execute sql read fail", KR(ret), K(exec_tenant_id), K(tenant_id), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "execute sql fail", K(ret), K(tenant_id), K(sql));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END != ret) {
        TRANS_LOG(WARN, "iterate next result fail", K(ret), K(sql));
      }
    } else {
      int64_t tx_id_value = 0;

      EXTRACT_INT_FIELD_MYSQL(*result, "trans_id", tx_id_value, int64_t);
      EXTRACT_INT_FIELD_MYSQL(*result, "flag", end_flag, int64_t);

      trans_id = ObTransID(tx_id_value);

      if (OB_FAIL(ret)) {
        TRANS_LOG(WARN, "fail to extract field from result", K(ret));
      } else {
        int64_t ls_id_value = 0;
        EXTRACT_INT_FIELD_MYSQL(*result, "coordinator", ls_id_value, int64_t);
        if (OB_FAIL(ret)) {
          if (OB_ERR_NULL_VALUE == ret) {
            // rewrite code, and caller would check validity of coordinator
            ret = OB_SUCCESS;
          } else {
            TRANS_LOG(WARN, "fail to extract coordinator from result", K(ret), K(xid));
          }
        } else {
          coordinator = share::ObLSID(ls_id_value);
        }
      }
    }

    THIS_WORKER.set_timeout_ts(original_timeout_us);
  }

  return ret;
}

#define QUERY_XA_SCHE_COORD_SQL "SELECT scheduler_ip, scheduler_port, \
    coordinator, trans_id, flag FROM %s WHERE \
    tenant_id = %lu AND gtrid = x'%.*s' AND bqual = x'%.*s' AND format_id = %ld"

// query original scheduler and coordinator from global transaction (table one)
int ObXAService::query_sche_and_coord(const uint64_t tenant_id,
                                      const ObXATransID &xid,
                                      ObAddr &scheduler_addr,
                                      share::ObLSID &coordinator,
                                      ObTransID &tx_id,
                                      int64_t &end_flag)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *mysql_proxy = NULL;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = NULL;
    char gtrid_str[128] = {0};
    int64_t gtrid_len = 0;
    char bqual_str[128] = {0};
    int64_t bqual_len = 0;
    int64_t original_timeout_us = THIS_WORKER.get_timeout_ts();
    const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);

    const int64_t start_ts = ObTimeUtility::current_time();
    THIS_WORKER.set_timeout_ts(start_ts + XA_INNER_TABLE_TIMEOUT);
    ObXAInnerSqlStatGuard stat_guard(start_ts);

    if (!is_valid_tenant_id(tenant_id) || xid.empty()) {
      ret = OB_INVALID_ARGUMENT;
      TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(xid));
    } else if (FALSE_IT(mysql_proxy = MTL(ObTransService *)->get_mysql_proxy())) {
    } else if (OB_ISNULL(mysql_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "mysql_proxy is null", K(ret), KP(mysql_proxy));
    } else if (OB_FAIL(hex_print(xid.get_gtrid_str().ptr(),
                                 xid.get_gtrid_str().length(),
                                 gtrid_str,
                                 128,
                                 gtrid_len))) {
      TRANS_LOG(WARN, "fail to convert gtrid to hex", K(ret), K(tenant_id), K(xid));
    } else if (OB_FAIL(hex_print(xid.get_bqual_str().ptr(),
                                 xid.get_bqual_str().length(),
                                 bqual_str,
                                 128,
                                 bqual_len))) {
      TRANS_LOG(WARN, "fail to convert bqual to hex", K(ret), K(tenant_id), K(xid));
    } else if (OB_FAIL(sql.assign_fmt(QUERY_XA_SCHE_COORD_SQL,
                                      OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
                                      tenant_id,
                                      (int)gtrid_len, gtrid_str,
                                      (int)bqual_len, bqual_str,
                                      xid.get_format_id()))) {
      TRANS_LOG(WARN, "generate query xa scheduler sql fail", K(ret), K(sql));
    } else if (OB_FAIL(mysql_proxy->read(res, exec_tenant_id, sql.ptr()))) {
      TRANS_LOG(WARN, "execute sql read fail", KR(ret), K(exec_tenant_id), K(tenant_id), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "execute sql fail", K(ret), K(tenant_id), K(sql));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END != ret) {
        TRANS_LOG(WARN, "iterate next result fail", K(ret), K(sql));
      }
    } else {
      char scheduler_ip_buf[128] = {0};
      int64_t tmp_scheduler_ip_len = 0;
      int64_t scheduler_port = 0;
      int64_t tx_id_value = 0;

      EXTRACT_STRBUF_FIELD_MYSQL(*result, "scheduler_ip", scheduler_ip_buf, 128, tmp_scheduler_ip_len);
      EXTRACT_INT_FIELD_MYSQL(*result, "scheduler_port", scheduler_port, int64_t);
      EXTRACT_INT_FIELD_MYSQL(*result, "trans_id", tx_id_value, int64_t);
      EXTRACT_INT_FIELD_MYSQL(*result, "flag", end_flag, int64_t);

      tx_id = ObTransID(tx_id_value);

      if (OB_FAIL(ret)) {
        TRANS_LOG(WARN, "fail to extract field from result", K(ret));
      } else if (!scheduler_addr.set_ip_addr(scheduler_ip_buf, scheduler_port)) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "set scheduler addr failed", K(scheduler_ip_buf), K(scheduler_port));
      } else {
        int64_t ls_id_value = 0;
        EXTRACT_INT_FIELD_MYSQL(*result, "coordinator", ls_id_value, int64_t);
        if (OB_FAIL(ret)) {
          if (OB_ERR_NULL_VALUE == ret) {
            // rewrite code, and caller would check validity of coordinator
            ret = OB_SUCCESS;
          } else {
            TRANS_LOG(WARN, "fail to extract coordinator from result", K(ret), K(xid));
          }
        } else {
          coordinator = share::ObLSID(ls_id_value);
        }
      }
    }

    THIS_WORKER.set_timeout_ts(original_timeout_us);
  }

  return ret;
}

#define UPDATE_COORD_AND_FLAG_SQL "UPDATE %s SET coordinator = %ld, flag = flag | %ld \
  WHERE tenant_id = %lu AND gtrid = x'%.*s' AND bqual = x'%.*s' AND format_id = %ld"

// update coordinator in global transaction (table one)
int ObXAService::update_coord(const uint64_t tenant_id,
                              const ObXATransID &xid,
                              const share::ObLSID &coordinator,
                              const bool has_tx_level_temp_table,
                              int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *mysql_proxy = NULL;
  ObSqlString sql;
  char gtrid_str[128] = {0};
  int64_t gtrid_len = 0;
  char bqual_str[128] = {0};
  int64_t bqual_len = 0;
  affected_rows = 0;
  int64_t original_timeout_us = THIS_WORKER.get_timeout_ts();
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  
  int64_t mask = 0;
  if (has_tx_level_temp_table) {
    mask = ObXAFlag::OBTEMPTABLE;
  }

  const int64_t start_ts = ObTimeUtility::current_time();
  THIS_WORKER.set_timeout_ts(start_ts + XA_INNER_TABLE_TIMEOUT);
  ObXAInnerSqlStatGuard stat_guard(start_ts);

  if (!is_valid_tenant_id(tenant_id)
      || xid.empty()
      || !coordinator.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(xid), K(coordinator));
  } else if (FALSE_IT(mysql_proxy = MTL(ObTransService *)->get_mysql_proxy())) {
  } else if (OB_ISNULL(mysql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "mysql_proxy is null", K(ret), KP(mysql_proxy));
  } else if (OB_FAIL(hex_print(xid.get_gtrid_str().ptr(),
                               xid.get_gtrid_str().length(),
                               gtrid_str, 128, gtrid_len))) {
    TRANS_LOG(WARN, "fail to convert gtrid to hex", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(hex_print(xid.get_bqual_str().ptr(), xid.get_bqual_str().length(),
          bqual_str, 128, bqual_len))) {
    TRANS_LOG(WARN, "fail to convert bqual to hex", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(sql.assign_fmt(UPDATE_COORD_AND_FLAG_SQL,
                                    OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
                                    coordinator.id(),
                                    mask,
                                    tenant_id,
                                    (int)gtrid_len, gtrid_str,
                                    (int)bqual_len, bqual_str,
                                    xid.get_format_id()))) {
    TRANS_LOG(WARN, "generate update coordinator sql failed", KR(ret), K(sql));
  } else if (OB_FAIL(mysql_proxy->write(exec_tenant_id, sql.ptr(), affected_rows))) {
    TRANS_LOG(WARN, "execute update coordinator sql failed",
              KR(ret), K(exec_tenant_id), K(tenant_id));
  } else {
    TRANS_LOG(INFO, "update coordinator success", K(exec_tenant_id), K(tenant_id),
              K(xid), K(coordinator), K(affected_rows));
  }

  THIS_WORKER.set_timeout_ts(original_timeout_us);

  return ret;
}

int ObXAService::two_phase_xa_commit_(const ObXATransID &xid,
                                      const int64_t timeout_us,
                                      const int64_t request_id,
                                      bool &has_tx_level_temp_table,
                                      ObTransID &tx_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  ObXACtx *xa_ctx = NULL;
  bool alloc = true;
  share::ObLSID coordinator;
  bool record_in_tableone = true;
  int64_t end_flag = 0;
  // only used for constructor
  bool is_tightly_coupled = true;

  if (OB_FAIL(query_xa_coord_from_tableone(tenant_id, xid, coordinator, tx_id, end_flag))) {
    if (OB_ITER_END == ret) {
      TRANS_LOG(INFO, "record not exist in global transaction", K(ret), K(xid));
      uint64_t data_version = 0;
      if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
        TRANS_LOG(WARN, "fail to get min data version", KR(ret), K(tenant_id));
      } else if (data_version < DATA_VERSION_4_2_0_0) {
        ret = OB_SUCCESS;
        record_in_tableone = false;
      } else {
        ret = OB_TRANS_XA_NOTA;
      }
    } else {
      TRANS_LOG(WARN, "fail to qeery record from global transaction", K(ret), K(xid));
    }
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (!record_in_tableone && OB_FAIL(query_xa_coordinator_with_xid(tenant_id, xid,
        tx_id, coordinator))) {
    if (OB_ITER_END == ret) {
      ret = OB_TRANS_XA_NOTA;
      TRANS_LOG(WARN, "xa is not valid", K(ret), K(xid), K(tx_id), K(coordinator));
    } else {
      TRANS_LOG(WARN, "fail to query trans id and coordinator", K(ret), K(xid),
          K(tx_id), K(coordinator));
      ret = OB_TRANS_XA_RETRY;
    }
  } else if (OB_UNLIKELY(!coordinator.is_valid())) {
    ret = OB_TRANS_XA_PROTO;
    TRANS_LOG(WARN, "invalid coordinator when xa two phase commit/rollback", K(ret), K(xid), K(coordinator), K(tx_id));
  } else if (OB_UNLIKELY(!tx_id.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "invalid xa arg from inner table", K(ret), K(xid), K(tx_id), K(coordinator));
  } else if (FALSE_IT(is_tightly_coupled = !ObXAFlag::contain_loosely(end_flag))) {
  } else if (OB_FAIL(xa_ctx_mgr_.get_xa_ctx(tx_id, alloc, xa_ctx))) {
    TRANS_LOG(WARN, "get xa ctx failed", K(ret), K(tx_id), K(xid), K(alloc), KP(xa_ctx));
  } else if (OB_ISNULL(xa_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "xa ctx is null", K(ret), K(xid), K(tx_id));
  } else {
    if (false == alloc) {
      ret = OB_TRANS_STMT_NEED_RETRY;
      TRANS_LOG(WARN, "xa ctx isn't allocated", K(ret), K(xid), K(tx_id));
    } else if (OB_FAIL(xa_ctx->init(xid,
                                    tx_id,
                                    tenant_id,
                                    GCTX.self_addr(),/*unused*/
                                    is_tightly_coupled,
                                    this,
                                    &xa_ctx_mgr_,
                                    &xa_rpc_,
                                    &timer_))) {
      TRANS_LOG(WARN, "xa ctx init failed", K(ret), K(xid), K(tx_id));
    } else {
      if (OB_FAIL(xa_ctx->two_phase_end_trans(xid, coordinator, false/*is_rollback*/, timeout_us, request_id))) {
        TRANS_LOG(WARN, "xa commit failed", K(ret), K(xid), K(tx_id));
      } else if (OB_FAIL(xa_ctx->wait_two_phase_end_trans(xid, false/*is_rollback*/, timeout_us))) {
        TRANS_LOG(WARN, "wait xa commit failed", K(ret), K(xid), K(tx_id));
      } else {
        if (OB_SUCCESS != (tmp_ret = delete_xa_branch(tenant_id, xid, is_tightly_coupled))) {
          TRANS_LOG(WARN, "delete xa branch failed", K(tmp_ret), K(xid), K(tenant_id), K(tx_id));
        }
        uint64_t data_version = 0;
        if (OB_SUCCESS != (tmp_ret = GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
          TRANS_LOG(WARN, "fail to get min data version", KR(ret), K(tenant_id));
        } else if (data_version < DATA_VERSION_4_2_0_0) {
          if (OB_SUCCESS != (tmp_ret = delete_xa_pending_record(tenant_id, tx_id))) {
            TRANS_LOG(WARN, "fail to delete xa record from pending trans", K(ret), K(xid), K(tx_id));
          }
        }
      }
    }
    xa_ctx_mgr_.revert_xa_ctx(xa_ctx);
    has_tx_level_temp_table = ObXAFlag::contain_temp_table(end_flag);
  }

  xa_cache_.clean_prepare_cache_item(xid);

  TRANS_LOG(INFO, "two phase xa commit", K(ret), K(xid), K(tx_id), K(coordinator));
  return ret;
}

void ObXAService::clear_xa_branch(const ObXATransID &xid, ObTxDesc *&tx_desc)
{
  const ObTransID tx_id = tx_desc->tid();
  if (OB_UNLIKELY(!is_inited_)) {
    TRANS_LOG_RET(WARN, OB_NOT_INIT, "xa service not inited");
  } else {
    ObXACtx *xa_ctx = tx_desc->get_xa_ctx();
    if (NULL == xa_ctx) {
      TRANS_LOG_RET(WARN, OB_ERR_UNEXPECTED, "xa ctx is null", K(tx_id), K(xid));
    } else {
      const bool need_decrease_ref = true;
      xa_ctx->try_exit(need_decrease_ref);
      xa_ctx_mgr_.revert_xa_ctx(xa_ctx);
    }
  }
  tx_desc = NULL;
  TRANS_LOG(INFO, "clear xa branch", K(xid), K(tx_id));
}

// XACache
int ObXACache::query_prepare_cache_item(const ObXATransID &xid, int64_t &state)
{
  int ret = OB_SUCCESS;
  int idx = xid.get_hash() % XA_PREPARE_CACHE_COUNT;
  ObXACacheItem &item = xa_prepare_cache_[idx];
  ObSpinLockGuard guard(item.lock_);
  if (item.is_valid_to_query(xid)) {
    state = item.state_;
  } else {
    ret = OB_HASH_NOT_EXIST;
  }
  return ret;
}

void ObXACache::insert_prepare_cache_item(const ObXATransID &xid, int64_t state)
{
  int idx = xid.get_hash() % XA_PREPARE_CACHE_COUNT;
  ObXACacheItem &item = xa_prepare_cache_[idx];
  ObSpinLockGuard guard(item.lock_);
  if (item.is_valid_to_set()) {
    clean_prepare_cache_item_(item);
    item.state_ = state;
    item.xid_ = xid;
    item.create_timestamp_ = ObTimeUtility::current_time();
  }
}

void ObXACache::clean_prepare_cache_item(const ObXATransID &xid) {
  int idx = xid.get_hash() % XA_PREPARE_CACHE_COUNT;
  ObXACacheItem &item = xa_prepare_cache_[idx];
  ObSpinLockGuard guard(item.lock_);
  if (item.is_valid_to_query(xid)) {
    clean_prepare_cache_item_(item);
  }
}

void ObXACache::clean_prepare_cache_item_(ObXACacheItem &item) {
  // should get lock in upper layer
  item.reset();
}

void ObXAService::try_print_statistics()
{
  xa_statistics_v2_.try_print_xa_statistics();
  dblink_statistics_.try_print_dblink_statistics();
}

}//transaction


}//oceanbase
