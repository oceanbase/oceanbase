// Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
//
#include "storage/tx/ob_xa_service.h"
#include "storage/tx/ob_xa_ctx.h"
#include "storage/tx/ob_trans_service.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"   // ObMySQLProxy
#include "lib/mysqlclient/ob_mysql_result.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/rc/ob_tenant_base.h"
#include "share/rc/ob_tenant_module_init_ctx.h"
#include "observer/ob_srv_network_frame.h"
#include "storage/tx/ob_xa_inner_sql_client.h"

namespace oceanbase
{

using namespace share;
using namespace common;
using namespace common::sqlclient;

namespace transaction
{
int ObXAService::xa_start_for_mysql(const ObXATransID &xid,
                                    const int64_t flags,
                                    const uint32_t session_id,
                                    const ObTxParam &tx_param,
                                    ObTxDesc *&tx_desc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(xid.empty()) ||
      OB_UNLIKELY(!xid.is_valid()) ||
      OB_UNLIKELY(0 > xid.get_format_id())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arguments", K(ret), K(xid));
  } else if (ObXAFlag::OBTMNOFLAGS != flags) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid flags for mysql xa start", K(ret), K(xid));
  } else if (OB_FAIL(xa_start_for_mysql_(xid, session_id, tx_param, tx_desc))) {
    TRANS_LOG(WARN, "mysql xa start failed", K(ret), K(xid));
  } else {
    // do nothing
  }
  // set xa_start_addr for txn-free-route
  if (OB_SUCC(ret) && OB_NOT_NULL(tx_desc)) {
    tx_desc->set_xa_start_addr(GCONF.self_addr_);
  }
  TRANS_LOG(INFO, "mysql xa start", K(ret), K(xid));
  return ret;
}

int ObXAService::xa_start_for_mysql_(const ObXATransID &xid,
                                     const uint32_t session_id,
                                     const ObTxParam &tx_param,
                                     ObTxDesc *&tx_desc)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  ObAddr sche_addr = GCTX.self_addr();
  bool alloc = true;
  ObXACtx *xa_ctx = NULL;
  if (tx_desc != NULL) {
    MTL(ObTransService *)->release_tx(*tx_desc);
    tx_desc = NULL;
  }
  if (OB_FAIL(MTL(ObTransService *)->acquire_tx(tx_desc, session_id))) {
    TRANS_LOG(WARN, "acquire trans failed", K(ret), K(tx_param));
  } else if (OB_FAIL(MTL(ObTransService *)->start_tx(*tx_desc, tx_param))) {
    TRANS_LOG(WARN, "start trans failed", K(ret), KPC(tx_desc));
    MTL(ObTransService *)->release_tx(*tx_desc);
    tx_desc = NULL;
  } else {
    const bool is_tightly_coupled = false;
    const ObTransID trans_id = tx_desc->tid();
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
    } else if (OB_FAIL(xa_ctx->xa_start_for_mysql(xid, tx_desc))) {
      TRANS_LOG(WARN, "xa ctx start failed", K(ret), K(xid), K(trans_id));
    }
    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(xa_ctx)) {
        xa_ctx_mgr_.erase_xa_ctx(trans_id);
        xa_ctx_mgr_.revert_xa_ctx(xa_ctx);
      }
      // since tx_desc is not set into xa ctx, release tx desc explicitly
      if (OB_SUCCESS != (tmp_ret = MTL(ObTransService*)->abort_tx(*tx_desc,
        ObTxAbortCause::IMPLICIT_ROLLBACK))) {
        TRANS_LOG(WARN, "abort transaction failed", K(tmp_ret), K(trans_id), K(xid));
      }
      MTL(ObTransService *)->release_tx(*tx_desc);
      tx_desc = NULL;
    } else {
      // for statistics
      XA_STAT_ADD_XA_TRANS_START_COUNT();
    }
  }
  return ret;
}

int ObXAService::xa_end_for_mysql(const ObXATransID &xid,
                                  ObTxDesc *&tx_desc)
{
  int ret = OB_SUCCESS;
  ObXACtx *xa_ctx = NULL;
  if (OB_UNLIKELY(xid.empty()) ||
      OB_UNLIKELY(!xid.is_valid()) ||
      OB_UNLIKELY(0 > xid.get_format_id()) ||
      OB_UNLIKELY(NULL == tx_desc)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arguments", K(ret), K(xid), KP(tx_desc));
  } else if (!tx_desc->is_xa_trans()) {
    ret = OB_TRANS_XA_PROTO;
    TRANS_LOG(WARN, "Routine invoked in an improper context", K(ret), K(xid));
  } else if (NULL == (xa_ctx = tx_desc->get_xa_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "transaction context is null", K(ret), K(xid));
  } else if (!xid.all_equal_to(tx_desc->get_xid())) {
    ret = OB_TRANS_XA_NOTA;
    TRANS_LOG(WARN, "unknown xid", K(ret), K(xid));
  } else if (OB_FAIL(xa_ctx->xa_end_for_mysql(xid, tx_desc))) {
    TRANS_LOG(WARN, "xa end failed", K(ret), K(xid));
  } else {
    // do nothing
  }
  TRANS_LOG(INFO, "mysql xa end", K(ret), K(xid));
  return ret;
}

int ObXAService::xa_prepare_for_mysql(const ObXATransID &xid,
                                      const int64_t timeout_us,
                                      ObTxDesc *&tx_desc,
                                      bool &need_exit)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool is_read_only = false;
  ObXACtx *xa_ctx = NULL;
  share::ObLSID coord_id;
  need_exit = false;
  ObTransID tx_id;
  // 1. check basic first, if fail, do not exit
  if (OB_UNLIKELY(xid.empty()) ||
      OB_UNLIKELY(!xid.is_valid()) ||
      OB_UNLIKELY(0 > xid.get_format_id()) ||
      OB_UNLIKELY(NULL == tx_desc) ||
      OB_UNLIKELY(0 >= timeout_us)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arguments", K(ret), K(xid), K(timeout_us), KP(tx_desc));
  } else if (!tx_desc->is_xa_trans()) {
    ret = OB_TRANS_XA_PROTO;
    TRANS_LOG(WARN, "Routine invoked in an improper context", K(ret), K(xid));
  } else if (NULL == (xa_ctx = tx_desc->get_xa_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "transaction context is null", K(ret), K(xid));
  } else {
    tx_id = tx_desc->tid();
  }
  // 2. pre xa prepare, persist xa record
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(xa_ctx->pre_xa_prepare_for_mysql(xid, tx_desc, need_exit,
          is_read_only, coord_id))) {
    TRANS_LOG(WARN, "pre xa prepare for mysql failed", K(ret), K(xid));
  } else if (OB_FAIL(MTL(ObXAService*)->insert_record_for_mysql(MTL_ID(), xid, tx_id,
          coord_id, GCTX.self_addr(), is_read_only))) {
    // if persist failed, abort this trans
    TRANS_LOG(WARN, "insert xa record failed", K(ret), K(xid), K(coord_id));
    need_exit = true;
    if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
      // if duplicate, return DUPID and abort this trans
      ret = OB_TRANS_XA_DUPID;
    }
  } else {
    // do nothing
  }
  // 3. send sub prepare to coordinator
  if (is_read_only) {
    // read-only trans
    // since ctx has exited, do nothing
  } else if (OB_FAIL(ret)) {
    // if fail and need exit (include dupid), abort the trans
    if (need_exit) {
      if (OB_SUCCESS != (tmp_ret = xa_ctx->handle_abort_for_mysql(
              ObTxAbortCause::IMPLICIT_ROLLBACK))) {
        TRANS_LOG(WARN, "handle abort failed", K(tmp_ret), K(xid));
      }
    }
  } else {
    need_exit = true;
    if (OB_FAIL(xa_ctx->xa_prepare_for_mysql(xid, timeout_us))) {
      TRANS_LOG(WARN, "xa prepare for mysql failed", K(ret), K(xid), K(timeout_us));
    } else if (OB_FAIL(xa_ctx->wait_xa_prepare_for_mysql(xid, timeout_us))) {
      TRANS_LOG(WARN, "wait xa prepare for mysql failed", K(ret), K(xid), K(timeout_us));
    }
  }
  if (NULL != xa_ctx && need_exit) {
    xa_ctx_mgr_.revert_xa_ctx(xa_ctx);
    tx_desc = NULL;
    xa_ctx = NULL;
  }
  TRANS_LOG(INFO, "mysql xa prepare", K(ret), K(xid), K(need_exit));
  return ret;
}

int ObXAService::xa_commit_onephase_for_mysql(const ObXATransID &xid,
                                              const int64_t timeout_us,
                                              ObTxDesc *&tx_desc,
                                              bool &need_exit)
{
  int ret = OB_SUCCESS;
  bool is_read_only = false;
  ObXACtx *xa_ctx = NULL;
  need_exit = false;
  if (OB_UNLIKELY(xid.empty()) ||
      OB_UNLIKELY(!xid.is_valid()) ||
      OB_UNLIKELY(0 > xid.get_format_id()) ||
      OB_UNLIKELY(NULL == tx_desc) ||
      OB_UNLIKELY(0 >= timeout_us)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arguments", K(ret), K(xid), K(timeout_us), KP(tx_desc));
  } else if (!tx_desc->is_xa_trans()) {
    ret = OB_TRANS_XA_PROTO;
    TRANS_LOG(WARN, "Routine invoked in an improper context", K(ret), K(xid));
  } else if (!xid.all_equal_to(tx_desc->get_xid())) {
    ret = OB_TRANS_XA_NOTA;
    TRANS_LOG(WARN, "unknown xid", K(ret), K(xid));
  } else if (NULL == (xa_ctx = tx_desc->get_xa_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "transaction context is null", K(ret), K(xid));
  } else {
    const bool is_rollback = false;
    ObTransID tx_id = tx_desc->tid();
    if (OB_FAIL(xa_ctx->one_phase_end_trans_for_mysql(xid, is_rollback, timeout_us,
            tx_desc, need_exit))) {
      TRANS_LOG(WARN, "one phase xa commit failed", K(ret), K(xid));
    } else {
      need_exit = true;
      if (OB_FAIL(xa_ctx->wait_one_phase_end_trans_for_mysql(is_rollback, timeout_us))) {
        TRANS_LOG(WARN, "fail to wait one phase xa end trans", K(ret), K(xid));
      } else {
        // do nothing
      }
    }
    if (need_exit) {
      xa_ctx_mgr_.revert_xa_ctx(xa_ctx);
      xa_ctx = NULL;
      tx_desc = NULL;
    }
  }
  // for statistics
  XA_STAT_ADD_XA_ONE_PHASE_COMMIT_TOTAL_COUNT();
  TRANS_LOG(INFO, "mysql one phase xa commit", K(ret), K(xid));
  return ret;
}

int ObXAService::xa_second_phase_twophase_for_mysql(const ObXATransID &xid,
                                                    const int64_t timeout_us,
                                                    const bool is_rollback,
                                                    ObTransID &tx_id)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  const uint64_t tenant_id = MTL_ID();
  ObXACtx *xa_ctx = NULL;
  bool alloc = true;
  bool is_read_only = false;
  share::ObLSID coord_id;
  ObXAInnerSQLClient inner_sql_client;
  if (OB_UNLIKELY(xid.empty()) ||
      OB_UNLIKELY(!xid.is_valid()) ||
      OB_UNLIKELY(0 > xid.get_format_id()) ||
      OB_UNLIKELY(0 >= timeout_us)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arguments", K(ret), K(xid), K(timeout_us));
  } else if (OB_FAIL(inner_sql_client.start(MTL(ObTransService *)->get_mysql_proxy()))) {
    TRANS_LOG(WARN, "xa inner sql client start failed", K(ret), K(xid));
  } else if (OB_FAIL(inner_sql_client.query_xa_coord_for_mysql(xid, coord_id, tx_id,
          is_read_only))) {
    if (OB_ITER_END == ret) {
      ret = OB_TRANS_XA_NOTA;
      TRANS_LOG(WARN, "unkonwn xid", K(tmp_ret), K(xid));
    } else {
      TRANS_LOG(WARN, "query xa record for mysql failed", K(ret), K(xid), K(coord_id),
          K(tx_id), K(is_read_only));
    }
  } else if (is_read_only) {
    // read only trans
    if (OB_SUCCESS != (tmp_ret = inner_sql_client.delete_xa_branch_for_mysql(xid))) {
      TRANS_LOG(WARN, "delete xa branch failed", K(tmp_ret), K(xid), K(tenant_id), K(tx_id));
    } else {
      // do nothing
    }
  } else {
    if (OB_UNLIKELY(!coord_id.is_valid()) ||
        OB_UNLIKELY(!tx_id.is_valid())) {
      ret = OB_TRANS_XA_PROTO;
      TRANS_LOG(WARN, "invalid record for mysql two phase xa commit", K(ret), K(xid),
          K(coord_id), K(tx_id));
    } else if (OB_FAIL(xa_ctx_mgr_.get_xa_ctx(tx_id, alloc, xa_ctx))) {
      TRANS_LOG(WARN, "get xa ctx failed", K(ret), K(tx_id), K(xid), K(alloc), KP(xa_ctx));
    } else if (OB_ISNULL(xa_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "xa ctx is null", K(ret), K(xid), K(tx_id));
    } else if (false == alloc) {
      ret = OB_TRANS_STMT_NEED_RETRY;
      TRANS_LOG(WARN, "xa ctx already exist", K(ret), K(xid), K(tx_id));
    } else {
      const int64_t request_id = start_ts;
      const bool is_tightly_coupled = false;
      if (OB_FAIL(xa_ctx->init(xid,
                               tx_id,
                               tenant_id,
                               GCTX.self_addr(),
                               is_tightly_coupled,
                               this,
                               &xa_ctx_mgr_,
                               &xa_rpc_,
                               &timer_))) {
        TRANS_LOG(WARN, "xa ctx init failed", K(ret), K(xid), K(tx_id));
      } else if (OB_FAIL(xa_ctx->two_phase_end_trans(xid, coord_id, is_rollback,
              timeout_us, request_id))) {
        TRANS_LOG(WARN, "xa commit failed", K(ret), K(xid), K(tx_id));
      } else if (OB_FAIL(xa_ctx->wait_two_phase_end_trans(xid, is_rollback, timeout_us))) {
        TRANS_LOG(WARN, "wait xa commit failed", K(ret), K(xid), K(tx_id));
      } else if (OB_SUCCESS != (tmp_ret = inner_sql_client.delete_xa_branch_for_mysql(xid))) {
        TRANS_LOG(WARN, "delete xa branch failed", K(tmp_ret), K(xid), K(tx_id));
      } else {
        // do nothing
      }
      xa_ctx_mgr_.revert_xa_ctx(xa_ctx);
    }
  }
  if (inner_sql_client.has_started()) {
    (void)inner_sql_client.end();
  }
  if (is_rollback) {
    TRANS_LOG(INFO, "mysql two phase xa rollback", K(ret), K(xid));
  } else {
    TRANS_LOG(INFO, "mysql two phase xa commit", K(ret), K(xid));
  }
  return ret;
}

int ObXAService::xa_rollback_onephase_for_mysql(const ObXATransID &xid,
                                                const int64_t timeout_us,
                                                ObTxDesc *&tx_desc,
                                                bool &need_exit)
{
  int ret = OB_SUCCESS;
  bool is_read_only = false;
  ObXACtx *xa_ctx = NULL;
  need_exit = false;
  if (OB_UNLIKELY(xid.empty()) ||
      OB_UNLIKELY(!xid.is_valid()) ||
      OB_UNLIKELY(0 > xid.get_format_id()) ||
      OB_UNLIKELY(NULL == tx_desc) ||
      OB_UNLIKELY(0 >= timeout_us)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arguments", K(ret), K(xid), K(timeout_us), KP(tx_desc));
  } else if (!tx_desc->is_xa_trans()) {
    ret = OB_TRANS_XA_PROTO;
    TRANS_LOG(WARN, "Routine invoked in an improper context", K(ret), K(xid));
  } else if (!xid.all_equal_to(tx_desc->get_xid())) {
    ret = OB_TRANS_XA_NOTA;
    TRANS_LOG(WARN, "unknown xid", K(ret), K(xid));
  } else if (NULL == (xa_ctx = tx_desc->get_xa_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "transaction context is null", K(ret), K(xid));
  } else {
    const bool is_rollback = true;
    ObTransID tx_id = tx_desc->tid();
    if (OB_FAIL(xa_ctx->one_phase_end_trans_for_mysql(xid, is_rollback, timeout_us,
            tx_desc, need_exit))) {
      TRANS_LOG(WARN, "one phase xa commit failed", K(ret), K(xid));
    } else {
      need_exit = true;
      if (OB_FAIL(xa_ctx->wait_one_phase_end_trans_for_mysql(is_rollback, timeout_us))) {
        TRANS_LOG(WARN, "fail to wait one phase xa end trans", K(ret), K(xid));
      } else {
        // do nothing
      }
    }
    if (need_exit) {
      // if exit for xa rollback, return success
      ret = OB_SUCCESS;
      xa_ctx_mgr_.revert_xa_ctx(xa_ctx);
      xa_ctx = NULL;
      tx_desc = NULL;
    }
  }
  TRANS_LOG(INFO, "mysql one phase xa rollback", K(ret), K(xid));
  return ret;
}

#define INSERT_MYSQLXA_SQL "\
  insert into %s (tenant_id, gtrid, bqual, format_id, \
  trans_id, coordinator, scheduler_ip, scheduler_port, state, flag, is_readonly) \
  values (%lu, x'%.*s', x'%.*s', %ld, %ld, %ld, '%s', %d, %d, %ld, %d)"

int ObXAService::insert_record_for_mysql(const uint64_t tenant_id,
                                         const ObXATransID &xid,
                                         const ObTransID &trans_id,
                                         const share::ObLSID &coordinator,
                                         const ObAddr &sche_addr,
                                         const bool is_readonly)
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
  } else if (!is_readonly && !coordinator.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(is_readonly), K(coordinator));
  } else if (OB_FAIL(hex_print(xid.get_gtrid_str().ptr(),
                               xid.get_gtrid_str().length(),
                               gtrid_str, 128, gtrid_len))) {
    TRANS_LOG(WARN, "fail to convert gtrid to hex", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(hex_print(xid.get_bqual_str().ptr(),
                               xid.get_bqual_str().length(),
                               bqual_str, 128, bqual_len))) {
    TRANS_LOG(WARN, "fail to convert bqual to hex", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(sql.assign_fmt(INSERT_MYSQLXA_SQL,
                                    OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
                                    tenant_id,
                                    (int)gtrid_len, gtrid_str,
                                    (int)bqual_len, bqual_str,
                                    xid.get_format_id(),
                                    trans_id.get_id(),
                                    coordinator.id(),
                                    scheduler_ip_buf, sche_addr.get_port(),
                                    ObXATransState::PREPARED, (long)0,
                                    is_readonly))) {
    TRANS_LOG(WARN, "generate insert xa trans sql fail", K(ret), K(sql));
  } else if (OB_FAIL(mysql_proxy->write(exec_tenant_id, sql.ptr(), affected_rows))) {
    TRANS_LOG(WARN, "execute insert record sql failed", KR(ret), K(exec_tenant_id), K(tenant_id));
  } else {
    TRANS_LOG(INFO, "execute insert record sql success", K(exec_tenant_id), K(tenant_id),
              K(sql), K(affected_rows));
  }
  THIS_WORKER.set_timeout_ts(original_timeout_us);
  return ret;
}

int ObXAService::handle_terminate_for_mysql(const ObXATransID &xid, ObTxDesc *tx_desc)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (NULL == tx_desc || !tx_desc->is_valid() || !xid.is_valid() || 0 > xid.get_format_id()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KP(tx_desc), K(xid));
  } else if (!xid.all_equal_to(tx_desc->get_xid())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected trans descriptor", K(ret), K(xid));
  } else {
    ObXACtx *xa_ctx = tx_desc->get_xa_ctx();
    ObTransID tx_id = tx_desc->tid();
    TRANS_LOG(INFO, "start to terminate mysql xa trans", K(xid), K(tx_id));
    if (NULL == xa_ctx) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected xa ctx", K(ret), K(xid), K(tx_id));
    } else if (OB_FAIL(xa_ctx->handle_abort_for_mysql(ObTxAbortCause::SESSION_DISCONNECT))) {
      TRANS_LOG(WARN, "handle terminate for mysql failed", K(ret), K(xid), K(tx_id));
    }
    xa_ctx_mgr_.revert_xa_ctx(xa_ctx);
    tx_desc = NULL;
  }
  XA_INNER_INCREMENT_TERMINATE_COUNT();
  return ret;
}

#define GC_MYSQL_RECORD_SQL "delete from %s where tenant_id = %lu and \
  gtrid = x'%.*s' and \
  bqual = x'%.*s'"

#define QUERY_MYSQL_LONG_PENDING_SQL "select HEX(gtrid) as gtrid, HEX(bqual) as bqual, \
  trans_id, is_readonly \
  from %s where tenant_id = %lu and \
  unix_timestamp(now()) - %ld > unix_timestamp(gmt_modified) limit 20"

// 1. for read-only xa trans, if exceed gc threadhold, do not check trans ctx
//    and clean record directly
// 2. for read-write xa trans, if exceed gc threadhold, check trans ctx.
//    if trans ctx exists, do not clean record
int ObXAService::gc_record_for_mysql()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t gc_threshold_second = GCONF._xa_gc_timeout / 1000000;  // in seconds
  const uint64_t tenant_id = MTL_ID();
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  ObMySQLProxy *mysql_proxy = MTL(ObTransService *)->get_mysql_proxy();
  ObSqlString sql;

  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + XA_INNER_TABLE_TIMEOUT);
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = NULL;
    if (OB_FAIL(sql.append_fmt(QUERY_MYSQL_LONG_PENDING_SQL,
                               OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
                               tenant_id,
                               gc_threshold_second))) {
      TRANS_LOG(WARN, "generate sql fail", K(ret), K(sql), K(tenant_id));
    } else if (OB_FAIL(mysql_proxy->read(res, exec_tenant_id, sql.ptr()))) {
      TRANS_LOG(WARN, "execute sql read fail", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "execute sql fail", K(ret), K(tenant_id), K(sql));
    } else {
      int64_t count = 0;
      while (OB_SUCC(ret) && OB_SUCC(result->next())) {
        int64_t affected_rows = 0;
        bool is_exist = false;
        bool need_delete = false;
        char gtrid_str[128] = {0};
        int64_t gtrid_len = 0;
        char bqual_str[128] = {0};
        int64_t bqual_len = 0;
        int64_t tx_id_value = 0;
        bool is_readonly = false;
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "gtrid", gtrid_str, 128, gtrid_len);
        EXTRACT_STRBUF_FIELD_MYSQL(*result, "bqual", bqual_str, 128, bqual_len);
        EXTRACT_INT_FIELD_MYSQL(*result, "trans_id", tx_id_value, int64_t);
        EXTRACT_BOOL_FIELD_MYSQL(*result, "is_readonly", is_readonly);
        if (OB_FAIL(ret)) {
          TRANS_LOG(WARN, "extract field failed", K(ret));
        } else if (is_readonly) {
          need_delete = true;
        } else {
          if (OB_FAIL(check_trans_ctx(tx_id_value, is_exist))) {
            // if fail, do not gc record
            TRANS_LOG(WARN, "check trans ctx failed", K(ret), K(tx_id_value));
          } else if (!is_exist) {
            need_delete = true;
          }
        }
        if (need_delete) {
          ObSqlString delete_sql;
          if (OB_FAIL(delete_sql.assign_fmt(GC_MYSQL_RECORD_SQL,
                                            OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
                                            tenant_id,
                                            (int)gtrid_len, gtrid_str,
                                            (int)bqual_len, bqual_str))) {
            TRANS_LOG(WARN, "generate query xa scheduler sql fail", K(ret), K(delete_sql));
          } else if (OB_SUCCESS != (tmp_ret = mysql_proxy->write(exec_tenant_id,
                                                                 delete_sql.ptr(),
                                                                 affected_rows))) {
            TRANS_LOG(WARN, "execute gc record sql failed", KR(ret), K(delete_sql));
          } else {
            TRANS_LOG(INFO, "gc record success", K(ret), K(tx_id_value), K(is_readonly),
                K(affected_rows));
            count++;
          }
        }
      } // end while
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
      TRANS_LOG(INFO, "gc record for mysql", K(ret), K(count));
    }
  }

  return ret;
}

#define CHECK_TRANS_CTX_SQL "SELECT trans_id \
  FROM %s WHERE \
  trans_id = %ld"

int ObXAService::check_trans_ctx(const int64_t tx_id_value, bool &is_exist)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *mysql_proxy = NULL;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = NULL;
    if (FALSE_IT(mysql_proxy = MTL(ObTransService *)->get_mysql_proxy())) {
    } else if (OB_ISNULL(mysql_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "mysql_proxy is null", K(ret), KP(mysql_proxy));
    } else if (OB_FAIL(sql.assign_fmt(CHECK_TRANS_CTX_SQL,
                                      OB_ALL_VIRTUAL_TRANS_STAT_TNAME,
                                      tx_id_value))) {
      TRANS_LOG(WARN, "generate sql fail", K(ret), K(sql));
    } else if (OB_FAIL(mysql_proxy->read(res, MTL_ID() , sql.ptr()))) {
      TRANS_LOG(WARN, "execute sql read fail", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "execute sql fail", K(ret), K(sql));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END == ret) {
        is_exist = false;
        // return success
        ret = OB_SUCCESS;
      } else {
        TRANS_LOG(WARN, "iterate next result fail", K(ret), K(sql));
      }
    } else {
      is_exist = true;
    }
    TRANS_LOG(INFO, "check trans ctx from virtual table", K(ret), K(tx_id_value), K(is_exist));
  }
  return ret;
}

} // transaction
} // oceanbase
