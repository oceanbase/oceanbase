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
#include "ob_xa_service.h"
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
using namespace common;
using namespace common::sqlclient;

namespace transaction
{
// generate a xid with new gtrid
// xid content:
//  gtrid:
//    base_version: "$tenant_id.$trans_id.$timestamp"
//    using_ipv4: "$tenant_id.$trans_id.$timestamp,$ipv4"
//  bqual: "10000"
// @param[out] new_xid
int ObXAService::generate_xid(const ObTransID &tx_id, ObXATransID &new_xid)
{
  int ret = OB_SUCCESS;
  // bqual
  static const ObString BQUAL_STRING = ObString("10000");
  // gtrid
  int64_t txid_value = tx_id.get_id();
  uint64_t tenant_id = MTL_ID();
  char gtrid_base_str[ObXATransID::MAX_GTRID_LENGTH] = {0};
  int64_t timestamp = ObTimeUtility::current_time() / 1000000; // second level
  const char *gtrid_base_format = "%llu.%lld.%lld";
  int base_len = snprintf(gtrid_base_str, ObXATransID::MAX_GTRID_LENGTH, gtrid_base_format,
                          tenant_id, txid_value, timestamp);
  if (ObXATransID::MAX_GTRID_LENGTH <= base_len || 0 > base_len) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected gtrid length", K(ret), K(base_len));
  } else {
    ObString gtrid_string;
    if (GCONF.self_addr_.using_ipv4()) {
      char ip_port[MAX_IP_PORT_LENGTH];
      int ip_str_length = 0;
      memset(ip_port, 0, MAX_IP_PORT_LENGTH);
      if (OB_FAIL(GCONF.self_addr_.addr_to_buffer(ip_port, MAX_IP_PORT_LENGTH, ip_str_length))) {
        TRANS_LOG(WARN, "convert server to string failed", K(ret), K(tx_id));
      } else {
        const char *gtrid_full_format = "%s,%s";
        char gtrid_full_str[ObXATransID::MAX_GTRID_LENGTH] = {0};
        int full_len = snprintf(gtrid_full_str, ObXATransID::MAX_GTRID_LENGTH, gtrid_full_format,
                                gtrid_base_str, ip_port);
        if (ObXATransID::MAX_GTRID_LENGTH <= full_len || 0 > full_len) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "unexpected gtrid length", K(ret), K(full_len));
        } else {
          gtrid_string = ObString(full_len, gtrid_full_str);
        }
      }
    } else {
      gtrid_string = ObString(base_len, gtrid_base_str);
    }
    if (OB_SUCC(ret)) {
      ret = new_xid.set(gtrid_string, BQUAL_STRING, DBLINK_FORMAT_ID);
    }
  }

  return ret;
}

// generate a xid with new bqual according to base xid
// @param[in] base_xid
// @param[in] seed
// @param[out] generated_xid
int ObXAService::generate_xid_with_new_bqual(const ObXATransID &base_xid,
                                             const int64_t seed,
                                             ObXATransID &new_xid)
{
  int ret = OB_SUCCESS;
  // TODO,
  int64_t target = 11000 + seed;
  char target_bqual[32] = {0};
  int length = sprintf(target_bqual, "%ld", target);
  ObString bqual_string = ObString(length, target_bqual);
  ret = new_xid.set(base_xid.get_gtrid_str(), bqual_string, DBLINK_FORMAT_ID);

  return ret;
}

// promote normal trans to dblink trans
// NOTE that the input xid should be empty
// @param [out] xid
int ObXAService::xa_start_for_tm_promotion(const int64_t flags,
                                           const int64_t timeout_seconds,
                                           ObTxDesc *&tx_desc,
                                           ObXATransID &xid)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!xid.empty()) ||
      OB_UNLIKELY(!xid.is_valid()) ||
      OB_UNLIKELY(timeout_seconds < 0)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arguments", K(ret), K(xid), K(timeout_seconds));
  } else if (NULL == tx_desc || !tx_desc->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected trans desc", K(ret), K(xid));
  } else if (!ObXAFlag::is_valid(flags, ObXAReqType::XA_START)) {
    ret = OB_TRANS_XA_INVAL;
    TRANS_LOG(WARN, "invalid flags for xa start", K(ret), K(xid), K(flags));
  } else {
    if (ObXAFlag::is_tmnoflags(flags, ObXAReqType::XA_START)) {
      if (OB_FAIL(xa_start_for_tm_promotion_(flags, timeout_seconds, tx_desc, xid))) {
        TRANS_LOG(WARN, "xa start promotion failed", K(ret), K(flags), K(xid));
      }
    } else {
      ret = OB_TRANS_XA_INVAL;
      TRANS_LOG(WARN, "invalid flags for xa start promotion", K(ret), K(xid), K(flags));
    }
  }

  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "xa start for dblink promotion failed", K(ret), K(xid), K(flags));
    // xa_statistics_.inc_failure_dblink_promotion();
  } else {
    TRANS_LOG(INFO, "xa start for dblink promtion", K(ret), K(xid), K(flags));
    // xa_statistics_.inc_success_dblink_promotion();
  }

  return ret;
}

int ObXAService::xa_start_for_tm_promotion_(const int64_t flags,
                                            const int64_t timeout_seconds,
                                            ObTxDesc *&tx_desc,
                                            ObXATransID &xid)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  // dblink trans must be tightly coupled
  const bool is_tightly_coupled = true;
  ObTransID tx_id = tx_desc->get_tx_id();
  const uint64_t tenant_id = MTL_ID();
  ObAddr sche_addr = GCTX.self_addr();
  ObMySQLTransaction trans;
  ObXACtx *xa_ctx = NULL;
  int64_t end_flag = 0;
  bool alloc = true;

  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  if (OB_FAIL(ObXAService::generate_xid(tx_id, xid))) {
    TRANS_LOG(WARN, "fail to generate xid", K(ret), K(tx_id), K(xid));
  } else if (!xid.is_valid() || xid.empty()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unexpected xid", K(ret), K(tx_id), K(xid));
  } else if (OB_FAIL(trans.start(MTL(ObTransService *)->get_mysql_proxy(), exec_tenant_id))) {
    TRANS_LOG(WARN, "trans start failed", K(ret), K(exec_tenant_id), K(xid));
  } else if (OB_FAIL(insert_xa_lock(trans, tenant_id, xid, tx_id))) {
    if (OB_TRANS_XA_DUPID == ret) {
      TRANS_LOG(WARN, "xa lock already exists", K(ret), K(tx_id), K(xid));
      // trans.reset_last_error();
    } else {
      TRANS_LOG(WARN, "insert xa lock record failed", K(ret), K(tx_id), K(xid));
    }
  }

  if (OB_FAIL(ret)) {
    (void)trans.end(false);
  } else if (OB_FAIL(insert_xa_record(trans, tenant_id, xid, tx_id, sche_addr, flags))) {
    // if there exists duplicated record in inner table, return OB_TRANS_XA_DUPID
    if (OB_TRANS_XA_DUPID == ret) {
      TRANS_LOG(WARN, "xa trans already exists", K(ret), K(tx_id), K(xid));
    } else {
      TRANS_LOG(WARN, "insert xa trans into inner table error", K(ret), K(tx_id), K(xid));
    }
    if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
      TRANS_LOG(WARN, "rollback lock record failed", K(tmp_ret), K(xid));
    }
  } else {
    const bool need_promote = true;
    if (OB_FAIL(xa_ctx_mgr_.get_xa_ctx(tx_id, alloc, xa_ctx))) {
      TRANS_LOG(WARN, "get xa ctx failed", K(ret), K(xid));
    } else if (!alloc) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected error", K(ret), K(xid));
    } else if (OB_FAIL(xa_ctx->init(xid,
                                    tx_id,
                                    tenant_id,
                                    GCTX.self_addr(),
                                    is_tightly_coupled,
                                    this/*xa service*/,
                                    &xa_ctx_mgr_,
                                    &xa_rpc_,
                                    &timer_))) {
      TRANS_LOG(WARN, "fail to init xa ctx", K(ret), K(xid), K(tx_id));
    } else if (OB_FAIL(xa_ctx->xa_start_for_dblink(xid, flags, timeout_seconds,
            need_promote, tx_desc))) {
      TRANS_LOG(WARN, "xa start promotion failed", K(ret), K(xid), K(tx_id));
    }

    if (OB_SUCC(ret)) {
      //commit record
      if (OB_FAIL(trans.end(true))) {
        // NOTE that if fail, do not release tx desc
        TRANS_LOG(WARN, "commit inner table trans failed", K(ret), K(xid));
        xa_ctx_mgr_.erase_xa_ctx(tx_id);
        xa_ctx_mgr_.revert_xa_ctx(xa_ctx);
      }
    } else {
      //rollback record
      if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
        TRANS_LOG(WARN, "rollback inner table trans failed", K(tmp_ret), K(xid));
      }
      if (OB_NOT_NULL(xa_ctx)) {
        xa_ctx_mgr_.erase_xa_ctx(tx_id);
        xa_ctx_mgr_.revert_xa_ctx(xa_ctx);
      }
    }
  }

  return ret;
}

// for 4.0 dblink
// start a dblink trans
// NOTE that the input xid should be empty
// @param [out] xid
int ObXAService::xa_start_for_tm(const int64_t flags,
                                 const int64_t timeout_seconds,
                                 const uint32_t session_id,
                                 const ObTxParam &tx_param,
                                 ObTxDesc *&tx_desc,
                                 ObXATransID &xid,
                                 const uint64_t data_version)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!xid.empty()) ||
      OB_UNLIKELY(!xid.is_valid()) ||
      OB_UNLIKELY(timeout_seconds < 0)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arguments", K(ret), K(xid), K(timeout_seconds));
  } else if (!ObXAFlag::is_valid(flags, ObXAReqType::XA_START)) {
    ret = OB_TRANS_XA_INVAL;
    TRANS_LOG(WARN, "invalid flags for xa start", K(ret), K(xid), K(flags));
  } else {
    if (ObXAFlag::is_tmnoflags(flags, ObXAReqType::XA_START)) {
      if (OB_FAIL(xa_start_for_tm_(flags, timeout_seconds, session_id, tx_param, tx_desc, xid, data_version))) {
        TRANS_LOG(WARN, "xa start promotion failed", K(ret), K(flags), K(xid));
      }
    } else {
      ret = OB_TRANS_XA_INVAL;
      TRANS_LOG(WARN, "invalid flags for xa start promotion", K(ret), K(xid), K(flags));
    }
  }

  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "xa start for dblink failed", K(ret), K(xid), K(flags));
    // xa_statistics_.inc_failure_dblink();
  } else {
    TRANS_LOG(INFO, "xa start for dblink", K(ret), K(xid), K(flags));
    // xa_statistics_.inc_success_dblink();
  }

  return ret;
}

int ObXAService::xa_start_for_tm_(const int64_t flags,
                                  const int64_t timeout_seconds,
                                  const uint32_t session_id,
                                  const ObTxParam &tx_param,
                                  ObTxDesc *&tx_desc,
                                  ObXATransID &xid,
                                  const uint64_t data_version)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  // dblink trans must be tightly coupled
  const bool is_tightly_coupled = true;
  ObTransID tx_id;
  const uint64_t tenant_id = MTL_ID();
  ObAddr sche_addr = GCTX.self_addr();
  ObMySQLTransaction trans;
  bool alloc = true;
  ObXACtx *xa_ctx = NULL;
  int64_t end_flag = 0;

  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  // start trans to generate tx desc
  {
    if (tx_desc != NULL) {
      MTL(ObTransService *)->release_tx(*tx_desc);
      tx_desc = NULL;
    }
    // the first xa start for xa trans with this xid
    // therefore tx_desc should be allocated
    if (OB_FAIL(MTL(ObTransService *)->acquire_tx(tx_desc, session_id, data_version))) {
      TRANS_LOG(WARN, "fail acquire trans", K(ret), K(tx_param));
    } else if (OB_FAIL(MTL(ObTransService *)->start_tx(*tx_desc, tx_param))) {
      TRANS_LOG(WARN, "fail start trans", K(ret), KPC(tx_desc));
      MTL(ObTransService *)->release_tx(*tx_desc);
      tx_desc = NULL;
    } else {
      tx_id = tx_desc->get_tx_id();
    }
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "fail to start trans", K(ret), K(tx_id), K(xid));
  } else {
    if (!tx_id.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected trans id", K(ret), K(tx_id), K(xid));
    } else if (OB_FAIL(generate_xid(tx_id, xid))) {
      TRANS_LOG(WARN, "fail to generate xid", K(ret), K(tx_id), K(xid));
    } else if (!xid.is_valid() || xid.empty()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected xid", K(ret), K(tx_id), K(xid));
    } else {
      if (OB_FAIL(trans.start(MTL(ObTransService *)->get_mysql_proxy(), exec_tenant_id))) {
        TRANS_LOG(WARN, "trans start failed", K(ret), K(exec_tenant_id), K(xid));
      } else {
        if (OB_FAIL(insert_xa_lock(trans, tenant_id, xid, tx_id))) {
          if (OB_TRANS_XA_DUPID == ret) {
            TRANS_LOG(WARN, "xa lock already exists", K(ret), K(tx_id), K(xid));
            // trans.reset_last_error();
          } else {
            TRANS_LOG(WARN, "insert xa lock record failed", K(ret), K(tx_id), K(xid));
          }
        }
      }
      if (OB_FAIL(ret)) {
        (void)trans.end(false);
        TRANS_LOG(WARN, "insert or query xa lock record failed", K(ret), K(tx_id), K(xid));
      } else {
        // do nothing
      }
    }
    if (OB_FAIL(ret)) {
      TRANS_LOG(WARN, "start trans failed", K(ret), K(tx_id), K(xid));
      if (OB_SUCCESS != (tmp_ret = MTL(ObTransService*)->abort_tx(*tx_desc,
        ObTxAbortCause::IMPLICIT_ROLLBACK))) {
        TRANS_LOG(WARN, "fail to abort transaction", K(tmp_ret), K(tx_id), K(xid));
      }
      MTL(ObTransService *)->release_tx(*tx_desc);
      tx_desc = NULL;
    } else if (OB_FAIL(insert_xa_record(trans, tenant_id, xid, tx_id, sche_addr, flags))) {
      // if there exists duplicated record in inner table, return OB_TRANS_XA_DUPID
      if (OB_TRANS_XA_DUPID == ret) {
        TRANS_LOG(WARN, "xa trans already exists", K(ret), K(tx_id), K(xid));
      } else {
        TRANS_LOG(WARN, "insert xa trans into inner table error", K(ret), K(tx_id), K(xid));
      }
      if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
        TRANS_LOG(WARN, "rollback lock record failed", K(tmp_ret), K(xid));
      }
      // txs_->remove_tx(*tx_desc);
      if (OB_SUCCESS != (tmp_ret = MTL(ObTransService*)->abort_tx(*tx_desc,
        ObTxAbortCause::IMPLICIT_ROLLBACK))) {
        TRANS_LOG(WARN, "fail to abort transaction", K(tmp_ret), K(tx_id), K(xid));
      }
      MTL(ObTransService *)->release_tx(*tx_desc);
      tx_desc = NULL;
    } else {
      const bool need_promote = false;
      //if (OB_FAIL(update_xa_lock(trans, tenant_id, xid, tx_id))) {
      //  TRANS_LOG(WARN, "update xa lock record failed", K(ret), K(tx_id), K(xid));
      if (OB_FAIL(xa_ctx_mgr_.get_xa_ctx(tx_id, alloc, xa_ctx))) {
        TRANS_LOG(WARN, "get xa ctx failed", K(ret), K(xid));
      } else if (!alloc) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "unexpected error", K(ret), K(xid));
      } else if (OB_FAIL(xa_ctx->init(xid,
                                      tx_id,
                                      tenant_id,
                                      GCTX.self_addr(),
                                      is_tightly_coupled,
                                      this/*xa service*/,
                                      &xa_ctx_mgr_,
                                      &xa_rpc_,
                                      &timer_))) {
        TRANS_LOG(WARN, "xa ctx init failed", K(ret), K(xid), K(tx_id));
      } else if (OB_FAIL(xa_ctx->xa_start_for_dblink(xid, flags, timeout_seconds,
              need_promote, tx_desc))) {
        TRANS_LOG(WARN, "xa ctx start failed", K(ret), K(xid), K(tx_id));
      }

      if (OB_SUCC(ret)) {
        //commit record
        if (OB_FAIL(trans.end(true))) {
          TRANS_LOG(WARN, "commit inner table trans failed", K(ret), K(xid));
          const bool need_decrease_ref = true;
          if (OB_SUCCESS != (tmp_ret = MTL(ObTransService*)->abort_tx(*tx_desc,
            ObTxAbortCause::IMPLICIT_ROLLBACK))) {
            TRANS_LOG(WARN, "fail to abort transaction", K(tmp_ret), K(tx_id), K(xid));
          }
          xa_ctx->try_exit(need_decrease_ref);
          xa_ctx_mgr_.revert_xa_ctx(xa_ctx);
          tx_desc = NULL;
        }
      } else {
        //rollback record
        if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
          TRANS_LOG(WARN, "rollback inner table trans failed", K(tmp_ret), K(xid));
        }
        if (OB_NOT_NULL(xa_ctx)) {
          xa_ctx_mgr_.erase_xa_ctx(tx_id);
          xa_ctx_mgr_.revert_xa_ctx(xa_ctx);
        }
        // since tx_desc is not set into xa ctx, release tx desc explicitly
        if (OB_SUCCESS != (tmp_ret = MTL(ObTransService*)->abort_tx(*tx_desc,
          ObTxAbortCause::IMPLICIT_ROLLBACK))) {
          TRANS_LOG(WARN, "fail to abort transaction", K(tmp_ret), K(tx_id), K(xid));
        }
        MTL(ObTransService *)->release_tx(*tx_desc);
        tx_desc = NULL;
      }
    }
  }

  return ret;
}

int ObXAService::xa_start_for_dblink_client(const DblinkDriverProto dblink_type,
                                            ObISQLConnection *dblink_conn,
                                            ObTxDesc *&tx_desc,
                                            ObXATransID &remote_xid)
{
  int ret = OB_SUCCESS;
  if (NULL == tx_desc || !tx_desc->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid trans desc", K(ret));
  } else if (NULL == dblink_conn || !ObDBLinkClient::is_valid_dblink_type(dblink_type)) {
    ret =  OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid dblink connection", K(ret), KP(dblink_conn), K(dblink_type));
  } else {
    ObXACtx *xa_ctx = tx_desc->get_xa_ctx();
    ObDBLinkClient *client = NULL;
    bool alloc = false;
    const ObTransID tx_id = tx_desc->get_tx_id();
    const ObXATransID xid = tx_desc->get_xid();
    if (NULL == xa_ctx) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected xa context", K(ret), K(xid), K(tx_id));
    } else if (OB_FAIL(xa_ctx->get_dblink_client(dblink_type, dblink_conn, &dblink_statistics_, client))) {
      TRANS_LOG(WARN, "fail to preapre xa start for dblink client", K(ret), K(xid), K(tx_id));
    } else if (NULL == client) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected dblink client", K(ret), K(xid), K(tx_id));
    } else {
      if (client->is_started(xid)) {
        // return success
        remote_xid = client->get_xid();
      } else if (OB_FAIL(ObXAService::generate_xid_with_new_bqual(xid,
              client->get_index(), remote_xid))) {
        TRANS_LOG(WARN, "fail to generate xid", K(ret), K(xid), K(tx_id), K(remote_xid));
      } else if (OB_FAIL(client->rm_xa_start(remote_xid, tx_desc->get_isolation_level()))) {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(xa_ctx->remove_dblink_client(client))) {
          ret = tmp_ret;
          TRANS_LOG(WARN, "fail to remove dblink client", K(ret), K(xid), K(tx_id),
            KP(client), K(remote_xid));
        } else {
          client = NULL;
        }
        TRANS_LOG(WARN, "fail to execute xa start for dblink connection, remove dblink client", K(ret), K(xid), K(tx_id),
            KPC(client), K(remote_xid));
      } else {
        // do nothing
      }
    }
  }
  return ret;
}

// for 4.0 dblink
// commit dblink trans
// @param[in] tx_desc
int ObXAService::commit_for_dblink_trans(ObTxDesc *&tx_desc)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool need_rollback = false;
  if (NULL == tx_desc || !tx_desc->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid trans desc", K(ret));
  } else {
    ObXACtx *xa_ctx = NULL;
    bool alloc = false;
    const ObTransID tx_id = tx_desc->get_tx_id();
    const ObXATransID xid = tx_desc->get_xid();
    if (OB_FAIL(xa_ctx_mgr_.get_xa_ctx(tx_id, alloc, xa_ctx))) {
      TRANS_LOG(WARN, "get xa ctx failed", K(ret), K(xid), K(tx_id));
    } else if (NULL == xa_ctx) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected xa context", K(ret), K(xid), K(tx_id));
    } else {
      bool is_readonly_local_branch = false;
      const int64_t timeout_seconds = 60;
      ObDBLinkClientArray &client_array = xa_ctx->get_dblink_client_array();
      // step 1, xa end for each participant
      // step 1.1, xa end for each dblink branch
      for (int i = 0; i < client_array.count(); i++) {
        ObDBLinkClient *client = client_array.at(i);
        if (NULL == client) {
          tmp_ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "invalid client", K(tmp_ret), KPC(client), K(tx_id));
          need_rollback = true;
        } else if (OB_SUCCESS != (tmp_ret = client->rm_xa_end())) {
          TRANS_LOG(WARN, "xa end failed", K(tmp_ret), KPC(client), K(tx_id));
          need_rollback = true;
        }
      }
      // step 1.2, xa end for local branch
      if (OB_SUCCESS != (tmp_ret = xa_end(xid, ObXAFlag::OBTMSUCCESS, tx_desc))) {
        TRANS_LOG(WARN, "xa end failed", K(tmp_ret), K(xid), K(tx_id));
        need_rollback = true;
      }
      // step 2, xa prepare for each participant
      // step 2.1, xa prepare for each dblink branch
      if (need_rollback) {
        // do nothing
      } else {
        for (int i = 0; i < client_array.count(); i++) {
          ObDBLinkClient *client = client_array.at(i);
          if (NULL == client) {
            tmp_ret = OB_ERR_UNEXPECTED;
            TRANS_LOG(WARN, "invalid client", K(tmp_ret), KPC(client), K(tx_id));
            need_rollback = true;
          } else if (OB_SUCCESS != (tmp_ret = client->rm_xa_prepare())) {
            if (OB_TRANS_XA_RDONLY != tmp_ret) {
              TRANS_LOG(WARN, "xa prepare failed", K(tmp_ret), KPC(client), K(tx_id));
              need_rollback = true;
              break;
            }
          }
        }
      }
      // step 2.2, xa prepare for local branch
      if (!need_rollback) {
        if (OB_SUCCESS != (tmp_ret = xa_prepare_for_original(xid, tx_id, timeout_seconds))) {
          if (OB_TRANS_XA_RDONLY != tmp_ret) {
            TRANS_LOG(WARN, "xa prepare for local failed", K(tmp_ret), K(xid), K(tx_id));
            need_rollback = true;
          } else {
            is_readonly_local_branch = true;
            TRANS_LOG(INFO, "read only local branch of dblink trans", K(tmp_ret), K(xid), K(tx_id));
          }
        }
      }
      // step 3, two phase xa commit/rollback
      // if an error is returned in this phase, set this error to ret
      // step 3.1, two phase xa commit/rollback for local branch
      if (need_rollback) {
        ObTransID unused_tx_id;
        if (OB_SUCCESS != (tmp_ret = xa_rollback(xid, timeout_seconds, unused_tx_id))) {
          TRANS_LOG(WARN, "xa rollback for local failed", K(tmp_ret), K(xid), K(tx_id));
          ret = tmp_ret;
        } else {
          TRANS_LOG(INFO, "xa rollback for local", K(tmp_ret), K(xid), K(tx_id));
        }
      } else {
        // TODO, temp table in dblink trans
        bool has_tx_level_temp_table = false;
        if (is_readonly_local_branch) {
          // do nothing
        } else {
          ObTransID unused_tx_id;
          if (OB_SUCCESS != (tmp_ret = xa_commit(xid, ObXAFlag::OBTMNOFLAGS, timeout_seconds,
                  has_tx_level_temp_table, unused_tx_id))) {
            TRANS_LOG(WARN, "xa rollback for local failed", K(tmp_ret), K(xid), K(tx_id),
                K(has_tx_level_temp_table));
            ret = tmp_ret;
          }
        }
      }
      // step 3.2, two phase xa commit/rollback for dblink branch
      for (int i = 0; i < client_array.count(); i++) {
        ObDBLinkClient *client = client_array.at(i);
        if (NULL == client) {
          tmp_ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "invalid client", K(tmp_ret), KPC(client), K(tx_id));
          ret = tmp_ret;
        } else {
          if (need_rollback) {
            if (OB_SUCCESS != (tmp_ret = client->rm_xa_rollback())) {
              TRANS_LOG(WARN, "xa rollback failed", K(tmp_ret), K(client), K(tx_id));
              ret = tmp_ret;
            } else {
              TRANS_LOG(INFO, "xa rollback", K(tmp_ret), K(client), K(tx_id));
            }
          } else {
            if (OB_SUCCESS != (tmp_ret = client->rm_xa_commit())) {
              TRANS_LOG(WARN, "xa commit failed", K(tmp_ret), K(client), K(tx_id));
              ret = tmp_ret;
            }
          }
        }
      }
      // step 4, rewrite ret and finish
      if (need_rollback) {
        if (OB_SUCCESS == ret) {
          ret = OB_TRANS_XA_RBROLLBACK;
        }
      }
      xa_ctx_mgr_.revert_xa_ctx(xa_ctx);
    }
    TRANS_LOG(INFO, "commit for dblink trans", K(ret), K(xid), K(tx_id), K(need_rollback));
  }
  return ret;
}

// for 4.0 dblink
// rollback dblink trans
// @param[in] tx_desc
int ObXAService::rollback_for_dblink_trans(ObTxDesc *&tx_desc)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (NULL == tx_desc || !tx_desc->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid trans desc", K(ret));
  } else {
    ObXACtx *xa_ctx = NULL;
    bool alloc = false;
    const ObTransID tx_id = tx_desc->get_tx_id();
    const ObXATransID xid = tx_desc->get_xid();
    if (OB_FAIL(xa_ctx_mgr_.get_xa_ctx(tx_id, alloc, xa_ctx))) {
      TRANS_LOG(WARN, "get xa ctx failed", K(ret), K(xid), K(tx_id));
    } else if (NULL == xa_ctx) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected xa context", K(ret), K(xid), K(tx_id));
    } else {
      const int64_t timeout_seconds = 60;
      ObDBLinkClientArray &client_array = xa_ctx->get_dblink_client_array();
      ObTransID unused_tx_id;
      // step 1, xa end for each participant
      // step 1.1, xa end for each dblink branch
      for (int i = 0; i < client_array.count(); i++) {
        ObDBLinkClient *client = client_array.at(i);
        if (NULL == client) {
          tmp_ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "invalid client", K(tmp_ret), KPC(client), K(tx_id));
        } else if (OB_SUCCESS != (tmp_ret = client->rm_xa_end())) {
          TRANS_LOG(WARN, "xa end failed", K(tmp_ret), KPC(client), K(tx_id));
        }
      }
      // step 1.2, xa end for local branch
      if (OB_SUCCESS != (tmp_ret = xa_end(xid, ObXAFlag::OBTMSUCCESS, tx_desc))) {
        TRANS_LOG(WARN, "xa end failed", K(tmp_ret), K(xid), K(tx_id));
      }
      // step 2, xa rollback for each participant
      // step 2.1, xa rollback for local branch
      if (OB_SUCCESS != (tmp_ret = xa_rollback(xid, timeout_seconds, unused_tx_id))) {
        TRANS_LOG(WARN,"xa rollback for local failed", K(tmp_ret), K(xid), K(tx_id));
        ret = tmp_ret;
      }
      // step 2.2, xa rollback for each dblink branch
      for (int i = 0; i < client_array.count(); i++) {
        ObDBLinkClient *client = client_array.at(i);
        if (NULL == client) {
          tmp_ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(WARN, "invalid client", K(tmp_ret), KPC(client), K(tx_id));
          ret = tmp_ret;
        } else {
          if (OB_SUCCESS != (tmp_ret = client->rm_xa_rollback())) {
            TRANS_LOG(WARN,"xa rollback failed", K(tmp_ret), K(client), K(tx_id));
            ret = tmp_ret;
          }
        }
      }
      // step 3, finish
      xa_ctx_mgr_.revert_xa_ctx(xa_ctx);
    }
    TRANS_LOG(INFO,"rollback for dblink trans", K(ret), K(xid), K(tx_id));
  }
  return ret;
}

int ObXAService::recover_tx_for_dblink_callback(const ObTransID &tx_id,
                                                ObTxDesc *&tx_desc)
{
  int ret = OB_SUCCESS;
  if (!tx_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arguments", K(ret), K(tx_id));
  } else {
    bool alloc = false;
    ObXACtx *xa_ctx = NULL;
    if (tx_desc != NULL) {
      MTL(ObTransService *)->release_tx(*tx_desc);
      tx_desc = NULL;
    }
    if (OB_FAIL(xa_ctx_mgr_.get_xa_ctx(tx_id, alloc, xa_ctx))) {
      TRANS_LOG(WARN, "get xa ctx failed", K(ret), K(tx_id), KP(xa_ctx));
    } else if (OB_ISNULL(xa_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "xa ctx is null", K(ret), K(tx_id));
    } else {
      if (OB_FAIL(xa_ctx->recover_tx_for_dblink_callback(tx_desc))) {
        TRANS_LOG(WARN, "fail to recover tx for dblink callback", K(ret), K(tx_id));
      } else {
        // do nothing
      }
      if (OB_SUCCESS != ret) {
        xa_ctx_mgr_.revert_xa_ctx(xa_ctx);
      }
    }
  }
  TRANS_LOG(INFO, "recover tx for dblink callback", K(ret), K(tx_id));
  return ret;
}

int ObXAService::revert_tx_for_dblink_callback(ObTxDesc *&tx_desc)
{
  int ret = OB_SUCCESS;
  ObTransID tx_id;
  if (NULL == tx_desc || !tx_desc->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arguments", K(ret), KP(tx_desc));
  } else {
    tx_id = tx_desc->tid();
    ObXACtx *xa_ctx = tx_desc->get_xa_ctx();
    if (NULL == xa_ctx) {
      ret = ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "xa transaction context is null", K(ret), K(tx_id));
    } else {
      if (OB_FAIL(xa_ctx->revert_tx_for_dblink_callback(tx_desc))) {
        TRANS_LOG(WARN, "fail to revert tx for dblink callback", K(ret), K(tx_id));
      } else {
        // NOTE that tx_desc is null currently
        // do nothing
      }
      xa_ctx_mgr_.revert_xa_ctx(xa_ctx);
    }
  }
  TRANS_LOG(INFO, "revert tx for dblink callback", K(ret), K(tx_id));
  return ret;
}

} // transaction
} // oceanbase
