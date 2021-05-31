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

#include "lib/mysqlclient/ob_mysql_proxy.h"   // ObMySQLProxy
#include "lib/mysqlclient/ob_mysql_result.h"  // ObMySQLResult
#include "lib/utility/ob_print_utils.h"       // ObMySQLResult
#include "observer/ob_server_struct.h"
#include "ob_trans_service.h"
#include "ob_trans_define.h"
#include "ob_trans_sche_ctx.h"
#include "ob_xa_rpc.h"

namespace oceanbase {
namespace transaction {
using namespace share;
using namespace common;
using namespace common::sqlclient;

// const static int64_t LONGEST_HANGOVER_TIME = 24 * 60 * 60; //24 hours
const static int64_t LOCK_FORMAT_ID = -2;
const static int64_t XA_INNER_TABLE_TIMEOUT = 10 * 1000 * 1000;

int ObTransService::xa_start_v2(
    const ObXATransID& xid, const int64_t flags, const int64_t xa_end_timeout_seconds, ObTransDesc& trans_desc)
{
  int ret = OB_SUCCESS;
  ObTransCtx* ctx = NULL;
  ObScheTransCtx* sche_ctx = NULL;
  const uint64_t tenant_id = trans_desc.get_tenant_id();
  bool reuse_transaction = false;
  bool is_tightly_coupled = true;
  int tmp_ret = OB_SUCCESS;

  // call start_trans first, and then cll xa_start
  if (xid.empty() || !xid.is_valid() || !trans_desc.is_valid() || xa_end_timeout_seconds <= 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arguments", K(ret), K(xid), K(trans_desc), K(xa_end_timeout_seconds));
  } else if (!ObXAFlag::is_valid(flags, ObXAReqType::XA_START)) {
    ret = OB_TRANS_XA_INVAL;
    TRANS_LOG(WARN, "invalid flags for xa start", K(ret), K(xid), K(flags));
  } else {
    trans_desc.set_xa_end_timeout_seconds(xa_end_timeout_seconds);
    if (ObXAFlag::is_tmnoflags(flags, ObXAReqType::XA_START)) {
      // TMNOFLAGS
      // start xa trans for the first time
      bool alloc = true;
      const bool for_replay = false;
      const bool is_readonly = ObXAFlag::contain_tmreadonly(flags);
      const bool is_serializable = ObXAFlag::contain_tmserializable(flags);
      ObTransID trans_id = trans_desc.get_trans_id();
      ObAddr sche_addr = self_;
      ObTransCtx* ctx = NULL;
      ObScheTransCtx* sche_ctx = NULL;
      ObMySQLTransaction trans;
      is_tightly_coupled = !ObXAFlag::contain_loosely(flags);
      trans_desc.set_xa_tightly_coupled(is_tightly_coupled);

      if (is_tightly_coupled) {
        if (OB_FAIL(trans.start(get_mysql_proxy()))) {
          TRANS_LOG(WARN, "trans start failed", K(ret), K(xid));
        } else if (OB_FAIL(insert_xa_lock(trans, tenant_id, xid, trans_id))) {
          if (OB_TRANS_XA_DUPID == ret) {
            TRANS_LOG(INFO, "xa lock already exists", K(ret), K(trans_id), K(xid));
            // to help understand easily
            ret = OB_SUCCESS;
            reuse_transaction = true;
            // usleep(10000);//10ms
            int64_t end_flag = 0;
            int64_t state = 0;
            ObXATransID lock_xid;
            if (OB_FAIL(lock_xid.set(xid.get_gtrid_str(), "", LOCK_FORMAT_ID))) {
              TRANS_LOG(WARN, "generate lock_xid failed", K(ret), K(xid));
            } else if (OB_FAIL(
                           query_xa_scheduler_trans_id(tenant_id, lock_xid, sche_addr, trans_id, state, end_flag))) {
              TRANS_LOG(WARN, "query xa trans from inner table error", K(ret), K(xid));
            }
          } else {
            TRANS_LOG(WARN, "insert xa lock record failed", K(ret), K(trans_id), K(xid));
          }
        }
      }
      if (OB_FAIL(ret)) {
        (void)trans.end(false);
        TRANS_LOG(WARN, "insert or query xa lock record failed", K(ret), K(trans_id), K(xid));
      } else if (OB_FAIL(insert_xa_record(tenant_id, xid, trans_id, sche_addr, flags))) {
        // if the xa record with the same xid exists in the inner table,
        // OB_TRANS_XA_DUPID is returned
        if (OB_TRANS_XA_DUPID == ret) {
          TRANS_LOG(WARN, "xa trans already exists", K(ret), K(trans_id), K(xid));
        } else {
          TRANS_LOG(WARN, "insert xa trans into inner table error", K(ret), K(trans_id), K(xid));
        }
        // rollback the trans regardless of reuse_transaction
        if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
          TRANS_LOG(WARN, "rollback lock record failed", K(tmp_ret), K(xid));
        }
      } else if (!reuse_transaction) {
        if (OB_FAIL(
                sche_trans_ctx_mgr_.get_trans_ctx(SCHE_PARTITION_ID, trans_id, for_replay, is_readonly, alloc, ctx))) {
          TRANS_LOG(WARN, "get transaction context error", K(ret), K(trans_id), K(xid));
        } else {
          sche_ctx = static_cast<ObScheTransCtx*>(ctx);
          ObStartTransParam& trans_param = trans_desc.get_trans_param();
          if (is_readonly) {
            trans_param.set_access_mode(ObTransAccessMode::READ_ONLY);
          }
          if (is_serializable && !trans_param.is_serializable_isolation()) {
            if (OB_FAIL(set_trans_snapshot_version_for_serializable_(
                    trans_desc, 0 /*stmt_snapshot_version*/, false /*is_stmt_snapshot_version_valid*/))) {
              TRANS_LOG(WARN,
                  "set trans snapshot version for serializable isolation",
                  K(ret),
                  K(xid),
                  K(trans_desc),
                  K(flags));
            }
            trans_param.set_isolation(ObTransIsolation::SERIALIZABLE);
            trans_param.set_read_snapshot_type(ObTransReadSnapshotType::TRANSACTION_SNAPSHOT);
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(trans_desc.set_xid(xid))) {
              TRANS_LOG(WARN, "set xid failed", K(ret), K(xid), K(trans_desc));
            } else if (OB_FAIL(trans_desc.set_orig_scheduler(self_))) {
              TRANS_LOG(WARN, "set orig scheduler failed", K(ret), K(self_));
            } else if (OB_FAIL(xa_init_sche_ctx_(trans_desc, sche_ctx))) {
              TRANS_LOG(WARN, "xa init sche ctx failed", K(ret), K(xid), K(trans_id));
            } else if (OB_FAIL(sche_ctx->add_xa_branch_count())) {
              TRANS_LOG(WARN, "add total branch count failed", K(ret), K(xid));
            } else if (OB_FAIL(sche_ctx->update_xa_branch_info(
                           xid, ObXATransState::ACTIVE, self_, xa_end_timeout_seconds))) {
              TRANS_LOG(WARN, "update xa branch info failed", K(ret), K(xid), K(trans_desc));
            } else if (OB_FAIL(sche_ctx->register_xa_timeout_task())) {
              TRANS_LOG(WARN, "register xa timeout task failed", K(ret), K(xid), K(trans_desc));
            } else {
              sche_ctx->add_and_get_xa_ref_count();
              sche_ctx->set_xa_tightly_coupled(is_tightly_coupled);
              // trans_desc.set_xa_end_timeout_seconds(xa_end_timeout_seconds);
              TRANS_LOG(INFO, "orig scheduler start transaction success", K(xid), K(trans_desc));
            }
            sche_ctx->notify_xa_start_complete(ret);
          }
        }
        if (OB_SUCC(ret)) {
          // commit lock record
          if (OB_FAIL(trans.end(true))) {
            TRANS_LOG(WARN, "commit lock record failed", K(ret), K(xid));
          }
        } else {
          // rollback lock record
          if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
            TRANS_LOG(WARN, "rollback lock record failed", K(tmp_ret), K(xid));
          }
        }
        if (OB_FAIL(ret)) {
          TRANS_LOG(WARN, "fail to execute xa start", K(ret), K(xid), K(sche_ctx), K(trans_desc));
          trans_desc.set_sche_ctx(NULL);
          if (OB_NOT_NULL(sche_ctx)) {
            sche_ctx->set_exiting();
            (void)sche_trans_ctx_mgr_.revert_trans_ctx(sche_ctx);
          }
          if (OB_SUCCESS != (tmp_ret = delete_xa_record(tenant_id, xid))) {
            TRANS_LOG(WARN, "delete xa record failed", K(tmp_ret), K(xid), K(flags));
          }
        }
      } else {
        // tightly coupled, reuse transaction
        (void)trans.end(false);
        if (OB_FAIL(prepare_scheduler_for_xa_start_resume_(
                xid, trans_id, sche_addr, trans_desc, reuse_transaction, true /*tightly coupled*/))) {
          TRANS_LOG(WARN,
              "prepare scheduler and trans desc for xa start resume error",
              K(ret),
              K(xid),
              K(trans_id),
              K(trans_desc));
          // clear the record in the inner table
          if (OB_SUCCESS != (tmp_ret = delete_xa_record(tenant_id, xid))) {
            TRANS_LOG(WARN, "delete xa record failed", K(tmp_ret), K(xid), K(flags));
          }
        }
        TRANS_LOG(INFO, "reuse transaction for a new branch", K(xid), K(ret));
      }
      TRANS_LOG(INFO, "xa start with tmnoflags", K(ret), K(xid), K(is_tightly_coupled));
    } else if (ObXAFlag::is_tmjoin(flags) || ObXAFlag::is_tmresume(flags)) {
      // xa start with TMJOIN/TMRESUME
      ObAddr scheduler_addr;
      ObTransID trans_id;
      int64_t end_flag = 0;
      int64_t state = 0;
      int64_t affected_rows = 0;

      if (OB_FAIL(update_xa_state(tenant_id, ObXATransState::ACTIVE, xid, false /*not one phase*/, affected_rows))) {
        // update the inner table first
        TRANS_LOG(WARN, "fail to update xa state to active", K(ret), K(xid));
      } else if (OB_FAIL(query_xa_scheduler_trans_id(tenant_id, xid, scheduler_addr, trans_id, state, end_flag))) {
        // xa trans does not exist
        if (OB_ITER_END == ret) {
          ret = OB_TRANS_XA_NOTA;
          TRANS_LOG(WARN, "no existing xid to join or resume", K(ret), K(xid));
        } else {
          TRANS_LOG(WARN, "query xa trans from inner table error", K(ret), K(xid));
        }
      } else if (ObXATransState::ACTIVE != state || 0 == affected_rows) {
        ret = OB_TRANS_XA_PROTO;
        TRANS_LOG(WARN, "xa trans state should be IDLE", K(ret), K(xid), K(flags), K(state), K(end_flag));
      } else if (!ObXAFlag::is_valid_inner_flag(end_flag)) {
        ret = OB_TRANS_XA_PROTO;
        TRANS_LOG(WARN, "unexpected xa trans end flag", K(ret), K(xid), K(flags), K(state), K(end_flag));
      } else if (!trans_id.is_valid() || !scheduler_addr.is_valid()) {
        ret = OB_TRANS_XA_RMFAIL;
        TRANS_LOG(WARN,
            "invalid arguments from inner table",
            K(ret),
            K(xid),
            K(flags),
            K(state),
            K(end_flag),
            K(trans_id),
            K(scheduler_addr));
      } else {
        const bool is_tightly_coupled = (end_flag & ObXAFlag::LOOSELY) ? false : true;
        if (OB_FAIL(prepare_scheduler_for_xa_start_resume_(
                xid, trans_id, scheduler_addr, trans_desc, reuse_transaction, is_tightly_coupled))) {
          TRANS_LOG(WARN,
              "prepare scheduler and trans desc for xa start resume error",
              K(ret),
              K(xid),
              K(trans_id),
              K(trans_desc));
        }
      }
      if (OB_FAIL(ret) && OB_ITER_END != ret) {
        // reset the xa trans state to idle if failure encounters
        if (OB_SUCCESS !=
            (tmp_ret = update_xa_state(tenant_id, ObXATransState::IDLE, xid, false /*not one phase*/, affected_rows))) {
          TRANS_LOG(WARN, "update xa state failed", K(tmp_ret), K(xid), K(affected_rows));
        }
      }
    } else {
      ret = OB_TRANS_XA_INVAL;
      TRANS_LOG(WARN, "invalid flags for xa start", K(ret), K(xid), K(flags));
    }
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "xa start failed", K(ret), K(xid), K(trans_desc), K(flags), K(reuse_transaction));
    trans_desc.reset();
  } else {
    TRANS_LOG(INFO, "xa start", K(ret), K(xid), K(trans_desc), K(flags), K(reuse_transaction));
  }
  return ret;
}

int ObTransService::xa_start_local_resume_(const ObXATransID& xid, const ObTransID& trans_id,
    const ObAddr& scheduler_addr, const bool is_new_branch, const bool is_tightly_coupled, ObTransDesc& trans_desc)
{
  int ret = OB_SUCCESS;
  ObTransCtx* ctx = NULL;
  ObScheTransCtx* sche_ctx = NULL;
  bool alloc = false;
  const bool is_readonly = false;
  const bool for_replay = false;
  bool xa_ref_count_added = false;
  trans_desc.set_trans_id(trans_id);

  if (OB_FAIL(sche_trans_ctx_mgr_.get_trans_ctx(SCHE_PARTITION_ID, trans_id, for_replay, is_readonly, alloc, ctx))) {
    TRANS_LOG(WARN, "get transaction context error", K(ret), K(xid), K(trans_id));
  } else if (OB_ISNULL(ctx) || (NULL == (sche_ctx = static_cast<ObScheTransCtx*>(ctx)))) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "sche ctx is null", K(ret), K(xid), K(trans_id));
  } else {
    (void)sche_ctx->add_and_get_xa_ref_count();
    xa_ref_count_added = true;
    if (sche_ctx->is_xa_tightly_coupled() != is_tightly_coupled) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "unexpected tight couple flag", K(ret), K(is_tightly_coupled));
    } else if (OB_FAIL(sche_ctx->check_for_xa_execution(is_new_branch, xid))) {
      if (OB_TRANS_XA_BRANCH_FAIL == ret) {
        TRANS_LOG(INFO, "original scheduler has terminated", K(ret), K(trans_desc), K(xid));
      } else {
        TRANS_LOG(WARN, "unexpected original scheduler for xa execution", K(ret), K(trans_desc), K(xid));
      }
    } else if (OB_FAIL(sche_ctx->wait_xa_start_complete())) {
      TRANS_LOG(WARN, "wait xa start failed", K(ret), K(xid));
    } else if (OB_FAIL(sche_ctx->set_xa_trans_state(ObXATransState::ACTIVE))) {
      TRANS_LOG(WARN, "fail to set xa trans state for scheduler", K(ret), K(trans_desc));
    } else if (OB_FAIL(sche_ctx->trans_deep_copy(trans_desc))) {
      TRANS_LOG(WARN, "copy original trans desc error", K(ret), K(xid));
    } else if (OB_FAIL(sche_ctx->update_xa_branch_info(
                   xid, ObXATransState::ACTIVE, self_, trans_desc.get_xa_end_timeout_seconds()))) {
      TRANS_LOG(WARN, "update xa branch info failed", K(ret), K(xid), K(trans_desc));
    } else if (OB_FAIL(sche_ctx->register_xa_timeout_task())) {
      TRANS_LOG(WARN, "register xa timeout task failed", K(ret), K(trans_desc));
    } else if (OB_FAIL(trans_desc.set_xid(xid))) {
      TRANS_LOG(WARN, "set xid failed", K(ret), K(xid));
    } else if (OB_FAIL(trans_desc.set_sche_ctx(sche_ctx))) {
      TRANS_LOG(WARN, "set sche ctx failed", K(ret), K(xid));
    } else if (OB_FAIL(trans_desc.set_orig_scheduler(scheduler_addr))) {
      TRANS_LOG(WARN, "set orig scheduler failed", K(ret), K(scheduler_addr), K(xid));
    } else {
      trans_desc.set_trans_type(TransType::DIST_TRANS);
      if (is_new_branch) {
        (void)sche_ctx->add_xa_branch_count();
      }
    }
  }
  // the case OB_TRANS_XA_BRANCH_FAIL has been handled
  if (OB_FAIL(ret)) {
    if (OB_TRANS_XA_BRANCH_FAIL == ret) {
      // do not rewrite ret
      (void)clear_branch_for_xa_terminate_(trans_desc, sche_ctx, false /*need_delelte_xa_record*/);
    } else {
      trans_desc.set_sche_ctx(NULL);
      if (OB_NOT_NULL(sche_ctx)) {
        if (xa_ref_count_added) {
          sche_ctx->dec_and_get_xa_ref_count();
        }
        (void)sche_trans_ctx_mgr_.revert_trans_ctx(sche_ctx);
      }
    }
  }
  return ret;
}

int ObTransService::xa_start_remote_resume_(const ObXATransID& xid, const ObTransID& trans_id,
    const ObAddr& scheduler_addr, const bool is_new_branch, const bool is_tightly_coupled, ObTransDesc& trans_desc)
{
  int ret = OB_SUCCESS;
  static const int64_t RETRY_TIMEOUT_US = 1000 * 1000;
  const int64_t start_us = ObTimeUtility::current_time();
  ObTransCtx* ctx = NULL;
  ObScheTransCtx* sche_ctx = NULL;
  bool alloc = true;
  const bool is_readonly = false;
  const bool for_replay = false;
  bool xa_ref_count_added = false;
  const uint64_t tenant_id = trans_desc.get_tenant_id();
  bool need_delete_xa_all_tightly_branch = false;
  trans_desc.set_trans_id(trans_id);

  // resolve conflict with xa end of the same xa trans
  while (OB_SUCC(ret) && (ObTimeUtility::current_time() - start_us < RETRY_TIMEOUT_US)) {
    if (OB_FAIL(sche_trans_ctx_mgr_.get_trans_ctx(SCHE_PARTITION_ID, trans_id, for_replay, is_readonly, alloc, ctx))) {
      TRANS_LOG(WARN, "get transaction context error", K(ret), K(trans_id));
    } else if (OB_ISNULL(ctx) || (NULL == (sche_ctx = static_cast<ObScheTransCtx*>(ctx)))) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "sche ctx is null", K(ret), K(xid), K(trans_id));
    } else if (!is_tightly_coupled) {
      if (!alloc) {
        sche_trans_ctx_mgr_.revert_trans_ctx(ctx);
        sche_ctx = NULL;
        ctx = NULL;
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "loose couple, trans already exists", K(xid), K(trans_id));
      } else {
        sche_ctx->add_and_get_xa_ref_count();
        xa_ref_count_added = true;
      }
      TRANS_LOG(INFO, "loose couple sche ctx", K(xid), K(trans_id));
      break;
    } else {
      if (alloc) {
        sche_ctx->add_and_get_xa_ref_count();
        xa_ref_count_added = true;
        TRANS_LOG(INFO, "alloc sche ctx", K(trans_id), K(xid));
        break;
      } else {
        if (1 < sche_ctx->add_and_get_xa_ref_count()) {
          TRANS_LOG(INFO, "reuse sche ctx", K(trans_id), K(xid));
          xa_ref_count_added = true;
          break;
        } else {
          // NOTE that alloc is required to set to true
          alloc = true;
          sche_ctx->dec_and_get_xa_ref_count();
          sche_trans_ctx_mgr_.revert_trans_ctx(ctx);
          sche_ctx = NULL;
          ctx = NULL;
          usleep(100);  // 100us
        }
      }
    }
  }
  if (OB_ISNULL(sche_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "get ctx failed", K(ret), K(start_us), K(xid), K(trans_id));
  } else if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "prepare scheduler for xa start failed", K(ret), K(xid), K(trans_id));
  } else {
    const int64_t timeout_seconds = trans_desc.get_xa_end_timeout_seconds();
    if (alloc) {
      int tmp_ret = OB_SUCCESS;
      // use the tmp_trans_desc to store trans_desc
      // after receiving the sync response, trans_desc can be updated
      sche_ctx->set_tmp_trans_desc(trans_desc);
      // when the tmp scheduler is created sucessfully, do not revert it immediately
      int result = OB_SUCCESS;
      ObXASyncStatusRPCRequest sync_request;
      obrpc::ObXARPCCB<obrpc::OB_XA_SYNC_STATUS> cb;
      ObTransCond cond;
      // wait_time is required to be greater than rpc timeout
      const int64_t wait_time = OB_XA_RPC_TIMEOUT + 1000 * 1000;
      if (OB_FAIL(sync_request.init(
              trans_id, xid, self_, is_new_branch, false /*not stmt pull*/, is_tightly_coupled, timeout_seconds))) {
        TRANS_LOG(WARN, "sync request init failed", K(ret), K(xid), K(trans_id));
      } else if (OB_FAIL(cb.init(&cond))) {
        TRANS_LOG(WARN, "init cb failed", K(ret), K(xid));
      } else if (OB_FAIL(xa_rpc_.xa_sync_status(tenant_id, scheduler_addr, sync_request, &cb))) {
        TRANS_LOG(WARN, "post xa sync status failed", K(ret), K(sync_request), K(scheduler_addr));
        // FIXME. replace expired time
      } else if (OB_FAIL(cond.wait(wait_time, result)) || OB_FAIL(result)) {
        if (is_tightly_coupled && (OB_TRANS_XA_BRANCH_FAIL == ret || OB_TRANS_CTX_NOT_EXIST == ret)) {
          TRANS_LOG(INFO, "xa trans has terminated", K(ret), K(trans_desc), K(sync_request), K(scheduler_addr));
          sche_ctx->set_terminated();
          need_delete_xa_all_tightly_branch = true;
          ret = OB_TRANS_XA_BRANCH_FAIL;
        } else {
          TRANS_LOG(WARN, "wait cond failed", K(ret), K(xid), K(sync_request), K(scheduler_addr));
        }
      } else if (OB_FAIL(sche_ctx->wait_xa_sync_status(wait_time + 500000))) {
        TRANS_LOG(WARN, "wait xa sync status failed", K(ret), K(xid));
      } else if (trans_desc.get_xid().get_gtrid_str() != xid.get_gtrid_str()) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "wrong xid", K(ret), K(xid), K(trans_desc));
      } else if (OB_FAIL(trans_desc.set_xid(xid))) {
        TRANS_LOG(WARN, "set xid failed", K(ret), K(xid), K(trans_desc));
      } else if (OB_FAIL(trans_desc.set_orig_scheduler(scheduler_addr))) {
        TRANS_LOG(WARN, "set orig scheduler failed", K(ret), K(scheduler_addr), K(trans_desc));
      }
      // init the sche if we can not get the info from the original scheduler
      if (OB_SUCCESS != (tmp_ret = xa_init_sche_ctx_(trans_desc, sche_ctx))) {
        TRANS_LOG(WARN, "xa init sche ctx failed", K(tmp_ret), K(xid), K(trans_id));
        if (OB_SUCC(ret)) {
          ret = tmp_ret;
        }
      } else {
        sche_ctx->set_xa_tightly_coupled(is_tightly_coupled);
        TRANS_LOG(INFO, "save trans desc success", K(trans_desc));
      }
      sche_ctx->notify_xa_start_complete(ret);
    } else {
      // not alloc, wait on cond
      if (sche_ctx->is_xa_tightly_coupled() != is_tightly_coupled) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "unexpected tight couple flag", K(ret), K(is_tightly_coupled));
      } else if (sche_ctx->is_terminated()) {
        ret = OB_TRANS_XA_BRANCH_FAIL;
        TRANS_LOG(INFO, "original scheduler has terminated", K(ret), K(trans_desc), K(xid));
      } else if (OB_FAIL(sche_ctx->wait_xa_start_complete())) {
        TRANS_LOG(WARN, "wait xa start complete failed", K(ret), K(xid));
      } else if (OB_FAIL(sche_ctx->trans_deep_copy(trans_desc))) {
        TRANS_LOG(WARN, "trans deep copy failed", K(ret), K(xid), K(trans_desc));
      } else if (OB_FAIL(trans_desc.set_xid(xid))) {
        TRANS_LOG(WARN, "set xid failed", K(ret), K(xid));
      } else if (OB_FAIL(trans_desc.set_sche_ctx(sche_ctx))) {
        TRANS_LOG(WARN, "set sche ctx failed", K(ret), K(xid));
      } else if (OB_FAIL(trans_desc.set_orig_scheduler(scheduler_addr))) {
        TRANS_LOG(WARN, "set orig scheduler failed", K(ret), K(scheduler_addr), K(trans_desc));
      } else {
        // send rpc to add branch count and register timeout task
        int result = OB_SUCCESS;
        ObXASyncStatusRPCRequest sync_request;
        obrpc::ObXARPCCB<obrpc::OB_XA_SYNC_STATUS> cb;
        ObTransCond cond;
        // wait_time is required to be greater than rpc timeout
        const int64_t wait_time = OB_XA_RPC_TIMEOUT + 1000 * 1000;
        if (OB_FAIL(sync_request.init(
                trans_id, xid, self_, is_new_branch, false /*not stmt pull*/, is_tightly_coupled, timeout_seconds))) {
          TRANS_LOG(WARN, "sync request init failed", K(ret), K(xid), K(trans_id));
        } else {
          sync_request.set_pull_trans_desc(false);
          if (OB_FAIL(cb.init(&cond))) {
            TRANS_LOG(WARN, "init cb failed", K(ret), K(xid));
          } else if (OB_FAIL(xa_rpc_.xa_sync_status(tenant_id, scheduler_addr, sync_request, &cb))) {
            TRANS_LOG(WARN, "post xa sync status failed", K(ret), K(sync_request), K(scheduler_addr));
            // FIXME. replace expired time
          } else if (OB_FAIL(cond.wait(wait_time, result)) || OB_FAIL(result)) {
            if (is_tightly_coupled && (OB_TRANS_XA_BRANCH_FAIL == ret || OB_TRANS_CTX_NOT_EXIST == ret)) {
              TRANS_LOG(INFO, "xa trans has terminated", K(ret), K(trans_desc), K(sync_request), K(scheduler_addr));
              sche_ctx->set_terminated();
              need_delete_xa_all_tightly_branch = true;
              ret = OB_TRANS_XA_BRANCH_FAIL;
            } else {
              TRANS_LOG(WARN, "wait cond failed", K(ret), K(result), K(sync_request), K(scheduler_addr));
            }
          }
        }
        TRANS_LOG(INFO, "xa start reuse sche ctx", K(ret), K(trans_desc));
      }
    }
  }
  // the case OB_TRANS_XA_BRANCH_FAIL has been handled
  if (OB_FAIL(ret)) {
    if (OB_TRANS_XA_BRANCH_FAIL == ret) {
      // do not rewrit ret
      (void)clear_branch_for_xa_terminate_(trans_desc, sche_ctx, need_delete_xa_all_tightly_branch);
    } else {
      trans_desc.set_sche_ctx(NULL);
      if (OB_NOT_NULL(sche_ctx)) {
        if (xa_ref_count_added) {
          if (0 == sche_ctx->dec_and_get_xa_ref_count()) {
            sche_ctx->set_exiting();
          }
        }
      }
      (void)sche_trans_ctx_mgr_.revert_trans_ctx(sche_ctx);
    }
  }
  return ret;
}

int ObTransService::prepare_scheduler_for_xa_start_resume_(const ObXATransID& xid, const ObTransID& trans_id,
    const ObAddr& scheduler_addr, ObTransDesc& trans_desc, const bool is_new_branch, const bool is_tightly_coupled)
{
  int ret = OB_SUCCESS;
  uint32_t session_id = trans_desc.get_session_id();
  uint64_t proxy_session_id = trans_desc.get_proxy_session_id();

  if (!xid.is_valid() || !scheduler_addr.is_valid() || !trans_desc.is_valid() || !trans_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arguments", K(ret), K(xid), K(scheduler_addr), K(trans_id), K(trans_desc));
  } else if (scheduler_addr == self_) {
    // original scheduler is in local
    // therefore we can get it directly
    if (OB_FAIL(xa_start_local_resume_(xid, trans_id, scheduler_addr, is_new_branch, is_tightly_coupled, trans_desc))) {
      TRANS_LOG(WARN, "xa start local resume failed", K(ret), K(xid), K(trans_id));
    }
  } else {
    // original scheduler is in remote
    // therefore a tmp scheduler is created first, and recover it accoring to orginal scheduler
    if (OB_FAIL(
            xa_start_remote_resume_(xid, trans_id, scheduler_addr, is_new_branch, is_tightly_coupled, trans_desc))) {
      TRANS_LOG(WARN, "xa start remote resume failed", K(ret), K(xid), K(trans_id));
    }
  }
  TRANS_LOG(INFO,
      "prepare xa scheduler",
      K(ret),
      K(xid),
      K(trans_id),
      K(is_tightly_coupled),
      K(session_id),
      K(proxy_session_id));

  return ret;
}

int ObTransService::xa_end_v2(const ObXATransID& xid, const int64_t flags, ObTransDesc& trans_desc)
{
  int ret = OB_SUCCESS;
  ObScheTransCtx* sche_ctx = NULL;
  ObTransTraceLog& tlog = trans_desc.get_tlog();
  int32_t state = ObXATransState::NON_EXISTING;
  const ObAddr& scheduler_addr = trans_desc.get_orig_scheduler();
  const ObTransID& trans_id = trans_desc.get_trans_id();
  const uint64_t tenant_id = trans_desc.get_tenant_id();
  const int64_t timeout_seconds = trans_desc.get_xa_end_timeout_seconds();
  bool need_delete_xa_all_tightly_branch = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTransService not inited", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObTransService is not running", K(ret));
  } else if (!ObXAFlag::is_valid(flags, ObXAReqType::XA_END)) {
    ret = OB_TRANS_XA_INVAL;
    TRANS_LOG(WARN, "invalid flags for xa end", K(ret), K(xid), K(flags));
  } else if (OB_UNLIKELY(!trans_desc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(trans_desc), K(xid));
  } else if (trans_desc.get_xid().empty()) {
    ret = OB_TRANS_XA_PROTO;
    TRANS_LOG(WARN, "Routine invoked in an improper context", K(ret), K(trans_desc));
  } else if (trans_desc.get_xid() != xid) {
    // in this case, 0 is returned in oracle
    ret = OB_TRANS_XA_NOTA;
    LOG_USER_ERROR(OB_TRANS_XA_RMFAIL, ObXATransState::to_string(state));
    TRANS_LOG(WARN, "xid not match", K(ret), K(trans_desc), K(xid), K(state));
  } else if (NULL == (sche_ctx = trans_desc.get_sche_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "transaction context is null", K(ret), K(trans_desc), K(xid));
  } else {
    const bool is_tightly_coupled = sche_ctx->is_xa_tightly_coupled();
    int64_t end_flag;
    if (!is_tightly_coupled) {
      end_flag = flags | ObXAFlag::LOOSELY;
      if (scheduler_addr == self_) {
        const int64_t fake_timeout = timeout_seconds;
        ObAddr fake_addr;
        if (OB_FAIL(sche_ctx->save_trans_desc(trans_desc))) {
          TRANS_LOG(WARN, "save trans desc failed", K(ret), K(trans_desc));
        } else if (OB_FAIL(sche_ctx->update_xa_branch_info(
                       xid, ObXATransState::IDLE, fake_addr /*not used*/, fake_timeout /*not used*/))) {
          TRANS_LOG(WARN, "update xa branch info failed", K(ret), K(xid), K(trans_desc));
        } else if (OB_FAIL(sche_ctx->register_xa_timeout_task())) {
          TRANS_LOG(WARN, "register xa timeout task failed", K(ret), K(trans_desc));
        } else if (OB_FAIL(sche_ctx->set_xa_trans_state(ObXATransState::IDLE))) {
          TRANS_LOG(WARN, "set xa trans state for scheduler failed", K(ret), K(trans_desc));
        }
      } else {
        int result = OB_SUCCESS;
        ObXAMergeStatusRPCRequest req;
        obrpc::ObXARPCCB<obrpc::OB_XA_MERGE_STATUS> cb;
        ObTransCond cond;
        // wait_time is required to be greater than rpc timeout
        const int64_t wait_time = OB_XA_RPC_TIMEOUT + 1000 * 1000;
        const int64_t seq_no = trans_desc.get_sql_no();
        if (OB_FAIL(req.init(trans_desc, false, is_tightly_coupled, xid, seq_no))) {
          TRANS_LOG(WARN, "init merge status request failed", K(ret), K(xid));
        } else if (OB_FAIL(cb.init(&cond))) {
          TRANS_LOG(WARN, "init cb failed", K(ret), K(xid));
        } else if (OB_FAIL(xa_rpc_.xa_merge_status(tenant_id, scheduler_addr, req, &cb))) {
          TRANS_LOG(WARN, "post xa merge status failed", K(ret), K(req), K(scheduler_addr));
        } else if (OB_FAIL(cond.wait(wait_time, result)) || OB_FAIL(result)) {
          TRANS_LOG(WARN, "wait cond failed", K(ret), K(result), K(req), K(scheduler_addr));
        } else {
          TRANS_LOG(INFO, "xa merge status success", K(xid), K(trans_id), K(scheduler_addr));
        }
      }
    } else {
      // tightly
      end_flag = flags;
      if (scheduler_addr == self_) {
        const int64_t fake_timeout = timeout_seconds;
        ObAddr fake_addr;
        if (OB_FAIL(sche_ctx->check_for_xa_execution(false /*is_new_branch*/, xid))) {
          if (OB_TRANS_XA_BRANCH_FAIL == ret) {
            TRANS_LOG(INFO, "xa trans has terminated", K(ret), K(trans_desc), K(xid));
          } else {
            TRANS_LOG(WARN, "unexpected original scheduler for xa execution", K(ret), K(trans_desc), K(xid));
          }
        } else if (OB_FAIL(sche_ctx->update_xa_branch_info(xid, ObXATransState::IDLE, fake_addr, fake_timeout))) {
          TRANS_LOG(WARN, "update xa branch info failed", K(ret), K(xid), K(trans_desc));
        } else if (OB_FAIL(sche_ctx->register_xa_timeout_task())) {
          TRANS_LOG(WARN, "register xa timeout task failed", K(ret), K(xid), K(trans_desc));
        }
      } else {
        // send rpc to register timeout task
        int result = OB_SUCCESS;
        ObXAMergeStatusRPCRequest req;
        obrpc::ObXARPCCB<obrpc::OB_XA_MERGE_STATUS> cb;
        ObTransCond cond;
        // wait_time is required to be greater than rpc timeout
        const int64_t wait_time = OB_XA_RPC_TIMEOUT + 1000 * 1000;
        const int64_t seq_no = trans_desc.get_sql_no();
        if (sche_ctx->is_terminated()) {
          ret = OB_TRANS_XA_BRANCH_FAIL;
          TRANS_LOG(INFO, "xa trans has terminated", K(ret), K(trans_desc));
        } else if (OB_FAIL(req.init(trans_desc, false /*not stmt push*/, is_tightly_coupled, xid, seq_no))) {
          TRANS_LOG(WARN, "init merge status request failed", K(ret), K(xid));
        } else if (OB_FAIL(cb.init(&cond))) {
          TRANS_LOG(WARN, "init cb failed", K(ret), K(xid));
        } else if (OB_FAIL(xa_rpc_.xa_merge_status(tenant_id, scheduler_addr, req, &cb))) {
          TRANS_LOG(WARN, "post xa merge status failed", K(ret), K(req), K(scheduler_addr));
        } else if (OB_FAIL(cond.wait(wait_time, result)) || OB_FAIL(result)) {
          if (OB_TRANS_XA_BRANCH_FAIL == ret || OB_TRANS_CTX_NOT_EXIST == ret) {
            TRANS_LOG(INFO, "another xa branch has failed", K(ret), K(xid), K(trans_desc), K(req), K(scheduler_addr));
            need_delete_xa_all_tightly_branch = true;
            sche_ctx->set_terminated();
            ret = OB_TRANS_XA_BRANCH_FAIL;
          } else {
            TRANS_LOG(WARN, "wait cond failed", K(ret), K(result), K(req), K(scheduler_addr));
          }
        } else {
          TRANS_LOG(INFO, "xa merge status success", K(xid), K(trans_id), K(scheduler_addr));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(update_xa_state_and_flag(tenant_id, ObXATransState::IDLE, end_flag, xid))) {
        // ret = OB_TRANS_XA_RMFAIL;
        TRANS_LOG(WARN, "update xa trans state and end flag for xa end error", K(ret), K(xid), K(flags), K(trans_desc));
      } else {
        trans_desc.set_trans_end();
        TRANS_LOG(DEBUG, "update xa trans state and end flag for xa end success", K(xid), K(flags), K(trans_desc));
      }
    } else {
      // TODO, the case of end failure of scheduler should be handled
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(sche_ctx)) {
    trans_desc.set_sche_ctx(NULL);
    // release the trans ctx due to the tmp scheduler
    if (0 == sche_ctx->dec_and_get_xa_ref_count()) {
      if (scheduler_addr != self_) {
        TRANS_LOG(INFO, "tmp sche_ctx going to exit", K(xid));
        sche_ctx->set_exiting();
      }
    }
    (void)sche_trans_ctx_mgr_.revert_trans_ctx(sche_ctx);
  }

  REC_TRACE_EXT(tlog, xa_end, Y(ret));
  if (OB_FAIL(ret)) {
    trans_desc.set_need_print_trace_log();
    // trans_desc.set_need_rollback();
    if (OB_TRANS_XA_BRANCH_FAIL == ret) {
      (void)clear_branch_for_xa_terminate_(trans_desc, trans_desc.get_sche_ctx(), need_delete_xa_all_tightly_branch);
    }
  }
  TRANS_LOG(INFO, "xa end", K(ret), K(xid), K(trans_desc), K(flags));

  return ret;
}

int ObTransService::xa_prepare_v2(const ObXATransID& xid, const uint64_t tenant_id, const int64_t stmt_expired_time)
{
  int ret = OB_SUCCESS;
  int64_t now = ObTimeUtility::current_time();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTransService not inited", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObTransService is not running", K(ret));
  } else {
    ObAddr scheduler_addr;
    int64_t state;
    ObTransID trans_id;
    int64_t end_flag;
    if (OB_FAIL(query_xa_scheduler_trans_id(tenant_id, xid, scheduler_addr, trans_id, state, end_flag))) {
      // ret = OB_TRANS_XA_RMFAIL;
      if (OB_ITER_END == ret) {
        ret = OB_TRANS_XA_NOTA;
        TRANS_LOG(WARN, "xid is not valid", K(ret), K(xid));
      } else {
        TRANS_LOG(WARN, "query xa trans from inner table error", K(ret), K(xid), K(tenant_id));
      }
    } else if (!trans_id.is_valid() || !scheduler_addr.is_valid()) {
      ret = OB_TRANS_XA_RMFAIL;
      TRANS_LOG(WARN, "invalid xa trans arguments from inner table", K(ret), K(xid), K(trans_id), K(scheduler_addr));
    } else if (ObXATransState::PREPARED == state) {
      // ret = OB_TRANS_XA_PROTO;
      ret = OB_TRANS_XA_RMFAIL;
      TRANS_LOG(WARN, "xa trans state has been prepared", K(ret), K(xid));
    } else if (ObXATransState::IDLE != state) {
      ret = OB_TRANS_XA_PROTO;
      TRANS_LOG(WARN, "unexpected xa trans state", K(ret), K(xid), K(state), K(end_flag));
    } else if (!ObXAFlag::is_valid_inner_flag(end_flag)) {
      ret = OB_TRANS_XA_RMFAIL;
      TRANS_LOG(WARN, "unexpected xa trans end_flag", K(ret), K(xid), K(state), K(end_flag));
    } else {
      TRANS_LOG(INFO, "scheduler addr", K(xid), K(state), K(end_flag), K(scheduler_addr), K_(self));
      if (scheduler_addr == self_) {
        // original scheduler is in local
        bool alloc = false;
        const bool is_readonly = false;
        const bool for_replay = false;
        ObTransCtx* ctx = NULL;
        if (OB_FAIL(
                sche_trans_ctx_mgr_.get_trans_ctx(SCHE_PARTITION_ID, trans_id, for_replay, is_readonly, alloc, ctx))) {
          TRANS_LOG(WARN, "get transaction context error", K(ret), K(xid), K(trans_id));
        } else if (OB_ISNULL(ctx)) {
          ret = OB_ERR_UNEXPECTED;
          TRANS_LOG(ERROR, "transaction context is null", K(ret));
        } else {
          ObScheTransCtx* sche_ctx = static_cast<ObScheTransCtx*>(ctx);
          if (OB_FAIL(local_xa_prepare(xid, stmt_expired_time, sche_ctx))) {
            if (OB_TRANS_XA_RDONLY != ret) {
              TRANS_LOG(WARN, "execute local xa prepare error", K(ret), K(xid), K(trans_id));
            }
          }
          (void)sche_trans_ctx_mgr_.revert_trans_ctx(ctx);
          if (OB_TRANS_XA_RBROLLBACK == ret) {
            int tmp_ret;
            if (OB_SUCCESS != (tmp_ret = delete_xa_branch(tenant_id, xid, !ObXAFlag::contain_loosely(end_flag)))) {
              TRANS_LOG(WARN, "delete xa record failed", K(tmp_ret), K(xid));
            }
          }
        }
        if (OB_FAIL(ret) && OB_TRANS_XA_RDONLY != ret) {
          TRANS_LOG(WARN, "xa prepare error", K(ret), K(tenant_id), K(xid));
        }
      } else {
        // forward the request to the node creating the xa trans (original scheduler)
        // therefore the tmp scheduler is not required to be created
        int result = OB_SUCCESS;
        int64_t retry_times = 5;
        do {
          obrpc::ObXAPrepareRPCRequest req;
          obrpc::ObXARPCCB<obrpc::OB_XA_PREPARE> rpc_cb;
          ObTransCond cond;
          // rely on the cb timeout, therefore cond timeout is set to max
          const int64_t wait_time = (INT64_MAX / 2) - now;
          const int64_t stmt_timeout = stmt_expired_time - ObClockGenerator::getClock();
          if (OB_FAIL(rpc_cb.init(&cond))) {
            TRANS_LOG(WARN, "ObXARPCCB init failed", KR(ret), K(xid));
          } else if (OB_FAIL(req.init(trans_id, xid, stmt_timeout))) {
            TRANS_LOG(WARN, "init ObXAEndTransRPCRequest failed", KR(ret), K(trans_id), K(xid));
          } else if (OB_FAIL(xa_rpc_.xa_prepare(tenant_id, scheduler_addr, req, rpc_cb))) {
            TRANS_LOG(WARN, "post xa prepare failed", KR(ret), K(xid), K(req), K(scheduler_addr));
          } else if (OB_FAIL(cond.wait(wait_time, result))) {
            TRANS_LOG(WARN, "wait xa prepare rpc callback failed", KR(ret), K(xid), K(req), K(scheduler_addr));
          } else if (OB_SUCCESS != result) {
            if (OB_TRANS_XA_RDONLY != result) {
              TRANS_LOG(WARN, "xa prepare rpc failed", K(xid), K(result), K(req), K(scheduler_addr));
            } else {
              TRANS_LOG(INFO, "read only xa branch", K(xid), K(result), K(scheduler_addr));
            }
          }
          if (OB_TRANS_XA_RETRY == result) {
            usleep(10000);  // 10ms
          }
        } while (OB_SUCC(ret) && ((--retry_times > 0 && OB_TIMEOUT == result) || OB_TRANS_XA_RETRY == result));
#ifdef ERRSIM
        int err_switch = E(EventTable::EN_XA_RPC_TIMEOUT) OB_SUCCESS;
        if (OB_SUCCESS != err_switch) {
          TRANS_LOG(INFO, "ERRSIM, rpc timeout");
          result = OB_TIMEOUT;
        }
#endif
        if (OB_SUCC(ret)) {
          const bool is_tightly = !ObXAFlag::contain_loosely(end_flag);
          if (OB_TRANS_CTX_NOT_EXIST == result || OB_TIMEOUT == result) {
            TRANS_LOG(INFO, "origin scheduler maybe not exist", K(ret), K(xid), K(state));
            // retry some times if failure occurs
            retry_times = 5;
            while (retry_times-- > 0 && OB_FAIL(query_xa_state_and_flag(tenant_id, xid, state, end_flag))) {
              TRANS_LOG(WARN, "query xa state failed", K(ret), K(xid));
              usleep(100000);  // 100ms
            }
            if (OB_UNLIKELY(OB_SUCCESS != ret)) {
              ret = OB_TRANS_XA_RMFAIL;
            } else if (ObXATransState::ROLLBACKED == state) {
              // delete failure, therefore we need to collect the record according to gc
              if (OB_FAIL(delete_xa_branch(tenant_id, xid, is_tightly))) {
                TRANS_LOG(WARN, "delete xa record failed", K(ret), K(xid));
              }
              ret = OB_TRANS_XA_RBROLLBACK;
            } else if (ObXATransState::PREPARED == state) {
              ret = OB_SUCCESS;
            } else {
              ret = OB_TRANS_XA_RMFAIL;
            }
          } else if (OB_TRANS_XA_RBROLLBACK == result) {
            if (OB_FAIL(delete_xa_branch(tenant_id, xid, is_tightly))) {
              TRANS_LOG(WARN, "delete xa record failed", K(ret), K(xid));
            }
            ret = result;
          } else {
            ret = result;
          }
        }
      }
    }
  }
  TRANS_LOG(INFO, "xa prepare", K(ret), K(xid), K(tenant_id));

  return ret;
}

int ObTransService::local_xa_prepare(const ObXATransID& xid, const int64_t stmt_expired_time, ObScheTransCtx* sche_ctx)
{
  int ret = OB_SUCCESS;
  const bool is_rollback = false;
  const MonotonicTs commit_time = MonotonicTs::current_time();

  if (OB_ISNULL(sche_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret));
  } else if (OB_FAIL(sche_ctx->pre_xa_prepare(xid))) {
    if (OB_TRANS_HAS_DECIDED == ret) {
      // rewrite ret
      ret = OB_SUCCESS;
    } else if (OB_TRANS_XA_RDONLY != ret && OB_TRANS_XA_RETRY != ret) {
      TRANS_LOG(WARN, "pre xa prepare failed", K(ret), K(xid));
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = sche_ctx->register_xa_timeout_task())) {
      TRANS_LOG(WARN, "register xa timeout task failed", K(tmp_ret), K(ret), K(xid));
    }
  } else if (OB_FAIL(sche_ctx->register_xa_timeout_task())) {
    TRANS_LOG(WARN, "register xa timeout task failed", K(ret), K(xid));
  } else {
    ObTransDesc* trans_desc = sche_ctx->get_trans_desc();
    if (!trans_desc->is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "invalid trans dessc", K(ret), K(xid), K(trans_desc));
    } else if (OB_FAIL(trans_desc->set_sche_ctx(sche_ctx))) {
      TRANS_LOG(WARN, "set trans_desc failed", K(ret), K(xid), K(trans_desc));
      // TODO, refer to the remote xa prepare to handle the case that ctx does not exist
    } else {
      sql::ObEndTransSyncCallback callback;
      if (OB_FAIL(callback.init(trans_desc, NULL))) {
        TRANS_LOG(WARN, "end trans callback init failed", K(ret), K(*trans_desc));
      } else {
        callback.set_is_need_rollback(true);
        callback.set_end_trans_type(sql::ObExclusiveEndTransCallback::END_TRANS_TYPE_IMPLICIT);
        callback.handout();
        if (OB_FAIL(end_trans_(is_rollback, *trans_desc, callback, stmt_expired_time, commit_time))) {
          if (OB_TRANS_HAS_DECIDED == ret) {
            // rewrite ret
            ret = OB_SUCCESS;
          } else {
            TRANS_LOG(WARN, "end_trans_ error", K(ret), K(is_rollback), K(trans_desc), K(xid));
            if (OB_TRANS_IS_EXITING == ret) {
              // the trans has been rollbacked
              ret = OB_TRANS_XA_NOTA;
            }
          }
        } else if (OB_FAIL(callback.wait())) {
          TRANS_LOG(WARN, "sync xa prepare callback return an error!", K(ret), K(*trans_desc));
        }
      }
    }
    if (OB_NOT_NULL(trans_desc)) {
      trans_desc->set_sche_ctx(NULL);
    }
  }
  return ret;
}

#define QUERY_XA_TRANS_SQL \
  "\
  select table_id, partition_id, partition_cnt, trans_id, is_readonly \
  from %s where gtrid = '%.*s' AND bqual = '%.*s' AND format_id = %ld\
"

int ObTransService::query_xa_trans_(const ObXATransID& xid, const uint64_t tenant_id, ObPartitionKey& coordinator,
    ObTransID& trans_id, bool& is_xa_readonly)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy* mysql_proxy = get_mysql_proxy();
  ObSqlString sql;
  ObMySQLProxy::MySQLResult res;
  ObMySQLResult* result = NULL;

  // query the xa trans with prepare state from the inner table
  if (OB_FAIL(sql.assign_fmt(QUERY_XA_TRANS_SQL,
          OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
          xid.get_gtrid_str().length(),
          xid.get_gtrid_str().ptr(),
          xid.get_bqual_str().length(),
          xid.get_bqual_str().ptr(),
          xid.get_format_id()))) {
    TRANS_LOG(WARN, "generate QUERY_XA_TRANS_SQL fail", K(ret));
  } else if (OB_FAIL(mysql_proxy->read(res, tenant_id, sql.ptr()))) {
    TRANS_LOG(WARN, "execute sql read fail", K(ret), K(tenant_id), K(sql));
  } else if (OB_ISNULL(result = res.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "execute sql fail", K(ret), K(tenant_id), K(sql));
  } else if (OB_FAIL(result->next())) {
    if (OB_ITER_END != ret) {
      TRANS_LOG(WARN, "iterate next result fail", K(ret), K(sql));
    }
  } else {
    uint64_t table_id = OB_INVALID_ID;
    int64_t partition_id = OB_INVALID_INDEX;
    int64_t partition_cnt = OB_INVALID_INDEX;
    char trans_id_buf[256];
    int64_t tmp_trans_id_len = 0;

    EXTRACT_INT_FIELD_MYSQL(*result, "table_id", table_id, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "partition_id", partition_id, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "partition_cnt", partition_cnt, int64_t);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "trans_id", trans_id_buf, 256, tmp_trans_id_len);
    EXTRACT_BOOL_FIELD_MYSQL(*result, "is_readonly", is_xa_readonly);
    (void)tmp_trans_id_len;  // make compiler happy

    if (OB_FAIL(ret)) {
      TRANS_LOG(WARN, "fail to extract field from result", K(ret));
    } else if (!is_xa_readonly &&
               OB_FAIL(coordinator.init(combine_id(tenant_id, table_id), partition_id, partition_cnt))) {
      TRANS_LOG(WARN, "fail to init coordinator", K(ret));
    } else if (OB_FAIL(trans_id.parse(trans_id_buf))) {
      TRANS_LOG(WARN, "fail to parse trans_id", K(ret));
    } else {
      // do nothing
    }
  }

  return ret;
}

// the info in trans_desc is invalid
// scheduler is not required to be recoverd for read only trans
// because there is no record in inner table, error is returned directly
int ObTransService::xa_recover_scheduler_v2_(const ObXATransID& xid, const ObPartitionKey& coordinator,
    const ObTransID& trans_id, const ObTransDesc& trans_desc, ObScheTransCtx*& sche_ctx)
{
  int ret = OB_SUCCESS;

  ObTransCtx* ctx = NULL;
  const bool for_replay = false;
  const bool is_readonly = false;
  const uint64_t tenant_id = trans_desc.get_tenant_id();
  bool alloc = true;

  if (OB_FAIL(sche_trans_ctx_mgr_.get_trans_ctx(SCHE_PARTITION_ID, trans_id, for_replay, is_readonly, alloc, ctx))) {
    TRANS_LOG(DEBUG, "get transaction context error", K(ret), K(trans_id), K(xid));
  } else if (OB_ISNULL(ctx)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "transaction context is null", K(ret), K(trans_id), K(xid));
  } else {
    ObScheTransCtx* tmp_sche_ctx = static_cast<ObScheTransCtx*>(ctx);
    if (alloc) {
      if (OB_FAIL(tmp_sche_ctx->init(tenant_id,
              trans_id,
              trans_desc.get_trans_expired_time(),
              SCHE_PARTITION_ID,
              &sche_trans_ctx_mgr_,
              trans_desc.get_trans_param(),
              trans_desc.get_cluster_version(),
              this,
              trans_desc.is_can_elr()))) {
        TRANS_LOG(WARN, "transaction context init error", K(ret), K(trans_id), K(xid));
      } else {
        if (OB_FAIL(tmp_sche_ctx->set_scheduler(self_))) {
          TRANS_LOG(WARN, "set scheduler error", K(ret), K_(self), K(trans_id), K(xid));
        } else if (OB_FAIL(tmp_sche_ctx->set_sql_no(trans_desc.get_sql_no()))) {
          TRANS_LOG(WARN, "scheduler set sql no error", K(ret), K(trans_id), K(xid));
        } else if (OB_FAIL(tmp_sche_ctx->set_session_id(trans_desc.get_session_id()))) {
          TRANS_LOG(WARN,
              "scheduler set session id error",
              K(ret),
              K(trans_id),
              "session_id",
              trans_desc.get_session_id(),
              K(xid));
        } else if (OB_FAIL(tmp_sche_ctx->set_proxy_session_id(trans_desc.get_proxy_session_id()))) {
          TRANS_LOG(WARN,
              "scheduler set proxy session id error",
              K(ret),
              K(trans_id),
              "proxy_session_id",
              trans_desc.get_proxy_session_id(),
              K(xid));
        } else if (OB_FAIL(tmp_sche_ctx->set_xid(xid))) {
          TRANS_LOG(WARN,
              "scheduler set xa local trans error",
              K(ret),
              K(trans_id),
              "proxy_session_id",
              trans_desc.get_proxy_session_id(),
              K(xid));
        } else if (OB_FAIL(tmp_sche_ctx->set_coordinator(coordinator))) {
          TRANS_LOG(WARN, "sche set coordinator error", K(ret), K(trans_id), K(xid));
        } else if (OB_FAIL(tmp_sche_ctx->set_xa_trans_state(ObXATransState::PREPARED, false))) {
          TRANS_LOG(WARN, "set xa trans state error", K(ret), K(trans_desc), K(xid));
        } else if (OB_FAIL(tmp_sche_ctx->start_trans(trans_desc))) {
          TRANS_LOG(WARN, "start transaction error", K(ret), K(trans_id), K(xid));
        } else {
          TRANS_LOG(INFO, "tmp sche ctx restore transaction success", K(trans_desc));
        }
      }
      const int64_t expire_renew_time = 0;
      if (OB_FAIL(location_adapter_->nonblock_renew(coordinator, expire_renew_time))) {
        TRANS_LOG(WARN, "nonblock renew location error", K(ret), "partition", coordinator);
      } else {
        TRANS_LOG(INFO, "refresh location cache success", "partition", coordinator);
      }
    } else {
      TRANS_LOG(INFO, "doesn't alloc sche_ctx", K(trans_desc));
    }

    if (OB_FAIL(ret)) {
      // return error
      if (alloc) {
        tmp_sche_ctx->set_exiting();
      }
      (void)sche_trans_ctx_mgr_.revert_trans_ctx(ctx);
    } else {
      sche_ctx = tmp_sche_ctx;
    }
  }
  TRANS_LOG(INFO, "xa recover scheduler", K(ret), K(xid), K(trans_desc));
  return ret;
}

int ObTransService::xa_end_trans_v2(
    const ObXATransID& xid, const bool is_rollback, const int64_t flags, ObTransDesc& trans_desc)
{
  int ret = OB_SUCCESS;
  // int32_t state = ObXATransState::NON_EXISTING;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTransService not inited", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObTransService is not running", K(ret));
  } else {
    trans_desc.set_trans_end();
    if (!is_rollback) {
      if (OB_FAIL(xa_commit_(xid, flags, trans_desc))) {
        TRANS_LOG(WARN, "[XA] execute xa commit failed", K(ret), K(xid));
      }
    } else {
      if (OB_FAIL(xa_rollback_(xid, flags, trans_desc))) {
        TRANS_LOG(WARN, "[XA] execute xa rollback failed", K(ret), K(xid));
      }
    }
  }
  TRANS_LOG(INFO, "xa end trans", K(ret), K(xid), K(is_rollback), K(flags), K(trans_desc));

  return ret;
}

int ObTransService::xa_commit_(const ObXATransID& xid, const int64_t flags, ObTransDesc& trans_desc)
{
  int ret = OB_SUCCESS;
  ObScheTransCtx* sche_ctx = NULL;
  int64_t now = ObTimeUtility::current_time();
  // int64_t state = NON-EXISTING;
  // int64_t end_flag = TMNOFLAGS;
  // ObTransID trans_id;

  if (!ObXAFlag::is_valid(flags, ObXAReqType::XA_COMMIT)) {
    ret = OB_TRANS_XA_INVAL;
    TRANS_LOG(WARN, "invalid flags for xa commit", K(ret), K(xid), K(flags));
  } else if (ObXAFlag::is_tmnoflags(flags, ObXAReqType::XA_COMMIT)) {
    // xa commit with TMNOFLAGS
    const uint64_t tenant_id = trans_desc.get_tenant_id();
    ObPartitionKey coordinator;
    ObTransID trans_id;
    int64_t state;
    bool is_readonly;
    int64_t end_flag;
    if (OB_FAIL(query_xa_coordinator_trans_id(tenant_id, xid, coordinator, trans_id, state, is_readonly, end_flag))) {
      if (OB_ITER_END == ret) {
        ret = OB_TRANS_XA_NOTA;
        TRANS_LOG(WARN, "xa trans not exist in inner table", K(ret), K(xid), K(coordinator), K(trans_id), K(state));
      } else {
        TRANS_LOG(WARN, "query xa trans from inner table error", K(ret), K(xid), K(coordinator), K(trans_id), K(state));
        // TM retries the xa commit according to xa protocol
        ret = OB_TRANS_XA_RETRY;
      }
      // error no is returned if the xa trans state is not prepared
    } else if (ObXATransState::PREPARED != state) {
      ret = OB_TRANS_XA_PROTO;
      TRANS_LOG(WARN, "unexpected xa trans state", K(ret), K(xid), K(coordinator), K(trans_id), K(state));
    } else if (is_readonly) {
      // delete the record from inner table directly for read-only xa trans
      if (OB_FAIL(delete_xa_branch(tenant_id, xid, !ObXAFlag::contain_loosely(end_flag)))) {
        TRANS_LOG(WARN, "delete XA inner table record failed", K(ret), K(xid), K(trans_id));
      }
    } else {
      // create the tmp scheduelr for read-write xa trans
      if (OB_FAIL(xa_recover_scheduler_v2_(xid, coordinator, trans_id, trans_desc, sche_ctx))) {
        TRANS_LOG(WARN, "recover scheduler for xa commit error", K(ret), K(xid), K(coordinator), K(trans_id), K(state));
      } else if (NULL == sche_ctx) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "sche_ctx is null", K(ret), K(trans_desc), K(xid));
      } else {
        sche_ctx->set_xa_tightly_coupled(!ObXAFlag::contain_loosely(end_flag));
        TRANS_LOG(DEBUG, "xa recover scheduler ctx success", K(xid), K(trans_desc));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(sche_ctx->xa_end_trans(false))) {
          TRANS_LOG(WARN, "xa end trans error", K(ret), K(xid));
        } else if (OB_FAIL(sche_ctx->wait_xa_end_trans())) {
          TRANS_LOG(WARN, "wait xa end trans error", K(ret), K(xid), K(trans_desc));
        } else {
          TRANS_LOG(DEBUG, "xa end trans success", K(ret), K(xid), K(trans_desc));
        }
        (void)sche_trans_ctx_mgr_.revert_trans_ctx(sche_ctx);
      }
    }
  } else if (ObXAFlag::is_tmonephase(flags)) {
    // xa commit with TMONEPHASE
    // one phase commit
    const uint64_t tenant_id = trans_desc.get_tenant_id();
    ObAddr scheduler_addr;
    ObTransID trans_id;
    int64_t state;
    int64_t end_flag;
    int tmp_ret = OB_SUCCESS;
    const bool is_rollback = false;
    if (OB_FAIL(query_xa_scheduler_trans_id(tenant_id, xid, scheduler_addr, trans_id, state, end_flag))) {
      if (OB_ITER_END == ret) {
        ret = OB_TRANS_XA_NOTA;
        TRANS_LOG(WARN, "xid is not valid", K(ret), K(xid));
      } else {
        TRANS_LOG(WARN, "query xa scheduler failed", KR(ret));
      }
    } else if (ObXATransState::IDLE != state) {
      ret = OB_TRANS_XA_PROTO;
      TRANS_LOG(WARN, "xa trans one phase commit invoked in improper state", KR(ret), K(state));
    } else if (scheduler_addr != self_) {
      // execute one phse commit by forwarding the request
      int result = OB_SUCCESS;
      int64_t retry_times = 5;
      do {
        obrpc::ObXAEndTransRPCRequest req;
        obrpc::ObXARPCCB<obrpc::OB_XA_END_TRANS> cb;
        ObTransCond cond;
        // rely on the cb timeout, therefore cond timeout is set to max
        const int64_t wait_time = (INT64_MAX - now) / 2;
        if (OB_FAIL(cb.init(&cond))) {
          TRANS_LOG(WARN, "ObXARPCCB init failed", KR(ret));
        } else if (OB_FAIL(req.init(trans_id, xid, is_rollback))) {
          TRANS_LOG(WARN, "init ObXAEndTransRPCRequest failed", KR(ret), K(trans_id), K(xid), K(is_rollback));
        } else if (OB_FAIL(xa_rpc_.xa_end_trans(tenant_id, scheduler_addr, req, cb))) {
          TRANS_LOG(WARN, "xa end trans rpc failed", KR(ret), K(req), K(scheduler_addr));
        } else if (OB_FAIL(cond.wait(wait_time, result))) {
          TRANS_LOG(WARN, "wait xa_end_trans rpc callback failed", KR(ret), K(req), K(scheduler_addr));
        } else if (OB_SUCCESS != result) {
          TRANS_LOG(WARN, "xa_end_trans rpc failed result", K(result), K(req), K(scheduler_addr));
        }
        if (OB_TRANS_XA_RETRY == result) {
          usleep(10000);  // 10ms
        }
      } while (OB_SUCC(ret) && ((OB_TIMEOUT == result && --retry_times > 0) || OB_TRANS_XA_RETRY == result));
#ifdef ERRSIM
      tmp_ret = E(EventTable::EN_XA_1PC_RESP_LOST) OB_SUCCESS;
      ;
      if (OB_SUCCESS != tmp_ret) {
        TRANS_LOG(INFO, "ERRSIM, origin sche ctx not exist");
        result = OB_TRANS_CTX_NOT_EXIST;
      }
#endif
      if (OB_SUCC(ret)) {
        if (OB_TRANS_CTX_NOT_EXIST == result || OB_TIMEOUT == result || OB_TRANS_IS_EXITING == result) {
          if (OB_FAIL(query_xa_state_and_flag(tenant_id, xid, state, end_flag))) {
            TRANS_LOG(WARN, "query xa state failed", K(ret), K(xid));
            ret = OB_TRANS_XA_RMFAIL;
          } else {
            TRANS_LOG(INFO, "query xa state success", K(xid), K(state), K(end_flag));
            if (ObXATransState::COMMITTED == state) {
              ret = OB_SUCCESS;
            } else if (ObXATransState::ROLLBACKED == state) {
              ret = OB_TRANS_XA_RBROLLBACK;
            } else {
              ret = OB_TRANS_XA_RMFAIL;
            }
          }
        } else {
          // include the case OB_TRANS_XA_RBROLLBACK
          ret = result;
        }
        // the case that TM sends one phase commit to RM when there are many branches due to tightly coupled mode
        // according to oracle, we do not delete the records from inner table at this point
        if (OB_TRANS_XA_PROTO != ret) {
          if (OB_SUCCESS != (tmp_ret = delete_xa_branch(tenant_id, xid, !ObXAFlag::contain_loosely(end_flag)))) {
            TRANS_LOG(WARN, "delete xa record failed", K(ret), K(xid));
          }
        }
      }
    } else {
      // local one phase commit
      ObTransCtx* ctx = NULL;
      bool alloc = false;
      const bool for_replay = false;
      const bool is_readonly = false;
      if (OB_FAIL(
              sche_trans_ctx_mgr_.get_trans_ctx(SCHE_PARTITION_ID, trans_id, for_replay, is_readonly, alloc, ctx))) {
        TRANS_LOG(WARN, "get transaction context error", K(ret), K(trans_id), K(xid));
      } else if (OB_ISNULL(ctx)) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "scheduler context not found", KR(ret));
      } else {
        sche_ctx = static_cast<ObScheTransCtx*>(ctx);
        const bool is_tightly = sche_ctx->is_xa_tightly_coupled();
        if (OB_FAIL(sche_ctx->xa_one_phase_end_trans(is_rollback))) {
          TRANS_LOG(WARN, "sche_ctx one phase commit failed", KR(ret), K(*sche_ctx));
        } else {
          if (OB_FAIL(sche_ctx->wait_xa_end_trans())) {
            TRANS_LOG(WARN, "wait xa end trans error", K(ret), K(xid), K(trans_desc));
          }
          if (OB_SUCCESS != (tmp_ret = delete_xa_branch(tenant_id, xid, is_tightly))) {
            TRANS_LOG(WARN, "delete xa record failed", K(tmp_ret), K(xid));
          }
        }
        (void)sche_trans_ctx_mgr_.revert_trans_ctx(ctx);
      }
    }
  } else {
    ret = OB_TRANS_XA_INVAL;
    TRANS_LOG(WARN, "invalid flags for xa commit", K(ret), K(xid), K(trans_desc), K(flags));
  }
  TRANS_LOG(INFO, "xa commit", K(ret), K(xid), K(flags), K(trans_desc));

  return ret;
}

int ObTransService::two_phase_rollback_(
    const uint64_t tenant_id, const ObXATransID& xid, const ObTransID& trans_id, const ObTransDesc& trans_desc)
{
  int ret = OB_SUCCESS;
  ObPartitionKey coordinator;
  ObScheTransCtx* sche_ctx = NULL;
  bool is_readonly = false;
  int64_t end_flag = 0;

  if (OB_FAIL(query_xa_coordinator(tenant_id, xid, coordinator, is_readonly, end_flag))) {
    if (OB_UNLIKELY(OB_ITER_END == ret)) {
      ret = OB_TRANS_XA_NOTA;
      TRANS_LOG(WARN, "xid is not valid", K(ret), K(xid));
    } else {
      TRANS_LOG(ERROR, "query xa coordinator failed", KR(ret), K(xid));
    }
  } else if (is_readonly) {
    // read only xa trans
    // delete the record from the inner table directly
    if (OB_FAIL(delete_xa_branch(tenant_id, xid, !ObXAFlag::contain_loosely(end_flag)))) {
      TRANS_LOG(WARN, "delete XA inner table record failed", K(ret), K(xid), K(trans_id), K(end_flag));
    }
  } else {
    if (OB_FAIL(xa_recover_scheduler_v2_(xid, coordinator, trans_id, trans_desc, sche_ctx))) {
      TRANS_LOG(WARN, "xa recover scheduler ctx failed", K(ret), K(xid), K(trans_desc));
    } else if (NULL == sche_ctx) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "sche_ctx is null", K(ret), K(trans_desc), K(xid));
    } else {
      sche_ctx->set_xa_tightly_coupled(!ObXAFlag::contain_loosely(end_flag));
      TRANS_LOG(DEBUG, "xa recover scheduler ctx success", K(ret), K(xid), K(trans_desc));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(sche_ctx->xa_end_trans(true))) {
        TRANS_LOG(WARN, "xa end trans error", K(ret), K(xid));
      } else if (OB_FAIL(sche_ctx->wait_xa_end_trans())) {
        TRANS_LOG(WARN, "wait xa end trans error", K(ret), K(xid), K(trans_desc));
      } else {
        TRANS_LOG(DEBUG, "xa end trans success", K(ret), K(xid), K(trans_desc));
      }
      (void)sche_trans_ctx_mgr_.revert_trans_ctx(sche_ctx);
    }
  }
  return ret;
}

int ObTransService::xa_rollback_(const ObXATransID& xid, const int64_t flags, ObTransDesc& trans_desc)
{
  int ret = OB_SUCCESS;
  ObScheTransCtx* sche_ctx = NULL;
  int64_t state = ObXATransState::PREPARED;
  int64_t now = ObTimeUtility::current_time();
  UNUSED(flags);

  // query inner table to get the xa trans state
  const uint64_t tenant_id = trans_desc.get_tenant_id();
  ObAddr scheduler_addr;
  ObTransID trans_id;
  int64_t end_flag;
  if (OB_FAIL(query_xa_scheduler_trans_id(tenant_id, xid, scheduler_addr, trans_id, state, end_flag))) {
    if (OB_ITER_END == ret) {
      // ret = OB_TRANS_XA_NOTA;
      ret = OB_SUCCESS;
      TRANS_LOG(INFO, "xa trans not exist in inner table", K(ret), K(xid), K(trans_id), K(state));
    } else {
      TRANS_LOG(ERROR, "query xa trans from inner table error", K(ret), K(xid), K(trans_id), K(state));
    }
  } else if (!scheduler_addr.is_valid() || !trans_id.is_valid()) {
    ret = OB_TRANS_XA_RMFAIL;
    TRANS_LOG(WARN, "invalid argumemnts from inner table", K(ret), K(xid), K(trans_id), K(scheduler_addr), K(trans_id));
  } else if (ObXATransState::ACTIVE == state) {
    // error no is returned
    ret = OB_TRANS_XA_PROTO;
    TRANS_LOG(WARN, "invalid xa state", KR(ret), K(state), K(trans_desc));
  } else if (ObXATransState::PREPARED == state) {
    // two phase rollback
    // query the inner table again, coordinator info can be got only if the trans state is set to prepared
    // TODO, if NULL coordinator can be executed safely, we only need to query the inner table once
    if (OB_FAIL(two_phase_rollback_(tenant_id, xid, trans_id, trans_desc))) {
      TRANS_LOG(WARN, "two phase xa rollback failed", K(ret), K(xid));
    }
  } else if (ObXATransState::IDLE == state) {
    // one phase rollback
    const bool is_rollback = true;
    if (scheduler_addr != self_) {
      int result = OB_SUCCESS;
      int64_t retry_times = 5;
      do {
        obrpc::ObXAEndTransRPCRequest req;
        obrpc::ObXARPCCB<obrpc::OB_XA_END_TRANS> cb;
        ObTransCond cond;
        // rely on the cb timeout, therefore cond timeout is set to max
        const int64_t wait_time = (INT64_MAX - now) / 2;
        if (OB_FAIL(cb.init(&cond))) {
          TRANS_LOG(WARN, "ObXARPCCB init failed", KR(ret));
        } else if (OB_FAIL(req.init(trans_id, xid, is_rollback))) {
          TRANS_LOG(WARN, "init ObXAEndTransRPCRequest failed", KR(ret), K(trans_id), K(xid), K(is_rollback));
        } else if (OB_FAIL(xa_rpc_.xa_end_trans(tenant_id, scheduler_addr, req, cb))) {
          TRANS_LOG(WARN, "xa end trans rpc failed", KR(ret), K(req), K(scheduler_addr));
        } else if (OB_FAIL(cond.wait(wait_time, result))) {
          TRANS_LOG(WARN, "wait xa end trans rpc callback failed", KR(ret), K(req), K(scheduler_addr));
        } else if (OB_SUCCESS != result) {
          TRANS_LOG(WARN, "xa end trans rpc failed result", K(result), K(req), K(scheduler_addr));
        }
        if (OB_TRANS_XA_RETRY == result) {
          usleep(10000);  // 10ms
        }
      } while (OB_SUCC(ret) && ((OB_TIMEOUT == result && --retry_times > 0) || OB_TRANS_XA_RETRY == result));
      if (OB_SUCC(ret)) {
        if (OB_TRANS_CTX_NOT_EXIST == result || OB_TIMEOUT == result) {
          int64_t affected_rows = 0;
          int64_t retry_times = 5;
          while (retry_times-- > 0 &&
                 OB_FAIL(delete_xa_record_state(tenant_id, xid, ObXATransState::IDLE, affected_rows))) {
            TRANS_LOG(WARN, "delete idle xa record failed", K(ret), K(xid));
            usleep(100000);  // 100 ms
          }
          if (OB_UNLIKELY(OB_SUCCESS != ret)) {
            TRANS_LOG(ERROR, "check stuck xa trans", K(ret), K(xid));
            ret = OB_TRANS_XA_RMFAIL;
          } else if (1 == affected_rows) {
            // do nothing
          } else if (0 == affected_rows) {
            if (OB_FAIL(query_xa_state_and_flag(tenant_id, xid, state, end_flag))) {
              if (OB_ITER_END == ret) {
                // there are two possible cases as follows
                // 1. xa prepare failure occurs, therefore the record has been deleted
                // 2. the record has been deleted by one phase rollback
                ret = OB_SUCCESS;
              } else {
                TRANS_LOG(ERROR, "query xa state failed", K(ret), K(xid));
              }
            } else {
              TRANS_LOG(INFO, "query xa state", K(xid), K(state), K(end_flag));
              if (ObXATransState::PREPARED == state) {
                // the trasn state may be set to prepared by coordinator
                // try to execute the two phase rollback
                if (OB_FAIL(two_phase_rollback_(tenant_id, xid, trans_id, trans_desc))) {
                  TRANS_LOG(WARN, "two phase rollback failed", K(ret), K(xid));
                  if (OB_TRANS_XA_NOTA == ret) {
                    ret = OB_SUCCESS;
                  }
                }
              } else if (ObXATransState::ROLLBACKED == state) {
                // the trans state may be set to rollbacked by coordinator
                // delete the record from the inner table
                if (OB_FAIL(delete_xa_branch(tenant_id, xid, !ObXAFlag::contain_loosely(end_flag)))) {
                  TRANS_LOG(WARN, "delete xa record failed", K(ret), K(xid));
                }
                ret = OB_SUCCESS;
              } else {
                ret = OB_ERR_UNEXPECTED;
                TRANS_LOG(WARN, "unexpected xa state", K(ret), K(xid), K(state));
              }
            }
          }
        } else {
          ret = result;
        }
      }
    } else {
      // local one phse rollback
      ObTransCtx* ctx = NULL;
      bool alloc = false;
      const bool for_replay = false;
      const bool is_readonly = false;
      if (OB_FAIL(
              sche_trans_ctx_mgr_.get_trans_ctx(SCHE_PARTITION_ID, trans_id, for_replay, is_readonly, alloc, ctx))) {
        TRANS_LOG(WARN, "get transaction context error", K(ret), K(trans_id), K(xid));
        if (OB_TRANS_CTX_NOT_EXIST == ret) {
          int64_t affected_rows = 0;
          int64_t retry_times = 5;
          while (retry_times-- > 0 &&
                 OB_FAIL(delete_xa_record_state(tenant_id, xid, ObXATransState::IDLE, affected_rows))) {
            TRANS_LOG(WARN, "delete idle xa record failed", K(ret), K(xid));
            usleep(100000);  // 100 ms
          }
          if (OB_UNLIKELY(OB_SUCCESS != ret)) {
            TRANS_LOG(ERROR, "check stuck xa trans", K(ret), K(xid));
            ret = OB_TRANS_XA_RMFAIL;
          } else if (1 == affected_rows) {
            ret = OB_SUCCESS;
          } else {
            if (OB_FAIL(query_xa_state_and_flag(tenant_id, xid, state, end_flag))) {
              if (OB_ITER_END == ret) {
                // there are two possible cases as follows
                // 1. xa prepare failure occurs, therefore the record has been deleted
                // 2. the record has been deleted by one phase rollback
                ret = OB_SUCCESS;
              } else {
                TRANS_LOG(ERROR, "query xa state failed", K(ret), K(xid));
              }
            } else {
              TRANS_LOG(INFO, "query xa state success", K(xid), K(state), K(end_flag));
              if (ObXATransState::PREPARED == state) {
                // the trasn state may be set to prepared by coordinator
                // try to execute the two phase rollback
                if (OB_FAIL(two_phase_rollback_(tenant_id, xid, trans_id, trans_desc))) {
                  TRANS_LOG(WARN, "two phase rollback failed", K(ret), K(xid));
                  if (OB_TRANS_XA_NOTA == ret) {
                    ret = OB_SUCCESS;
                  }
                }
              } else if (ObXATransState::ROLLBACKED == state) {
                // the trans state may be set to rollbacked by coordinator
                // delete the record from the inner table
                if (OB_FAIL(delete_xa_branch(tenant_id, xid, !ObXAFlag::contain_loosely(end_flag)))) {
                  TRANS_LOG(WARN, "delete xa record failed", K(ret), K(xid));
                }
                ret = OB_SUCCESS;
              } else {
                ret = OB_ERR_UNEXPECTED;
                TRANS_LOG(WARN, "unexpected state", K(ret), K(xid), K(state));
              }
            }
          }
        }
      } else if (OB_ISNULL(ctx)) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(ERROR, "scheduler context not found", KR(ret));
      } else {
        sche_ctx = static_cast<ObScheTransCtx*>(ctx);
        if (OB_FAIL(sche_ctx->xa_one_phase_end_trans(is_rollback))) {
          TRANS_LOG(WARN, "sche_ctx one phase commit failed", KR(ret));
        } else if (OB_FAIL(sche_ctx->wait_xa_end_trans())) {
          TRANS_LOG(WARN, "wait xa end trans error", K(ret), K(xid), K(trans_desc));
        }
        sche_ctx->set_terminated();
        (void)sche_trans_ctx_mgr_.revert_trans_ctx(ctx);
      }
    }
  } else if (ObXATransState::ROLLBACKED == state) {
    TRANS_LOG(INFO, "xa trans already rollbacked", K(ret), K(xid));
    if (OB_FAIL(delete_xa_branch(tenant_id, xid, !ObXAFlag::contain_loosely(end_flag)))) {
      TRANS_LOG(WARN, "delete xa record failed", K(ret), K(xid));
    }
    ret = OB_SUCCESS;
  } else {
    // error no is returned
    ret = OB_TRANS_XA_PROTO;
    TRANS_LOG(WARN, "invalid xa state", KR(ret), K(state), K(trans_desc));
  }
  TRANS_LOG(INFO, "xa rollback", K(ret), K(xid), K(flags), K(trans_desc));

  return ret;
}

int ObTransService::get_xa_trans_state(int32_t& state, ObTransDesc& trans_desc)
{
  int ret = OB_SUCCESS;
  ObScheTransCtx* sche_ctx = NULL;
  int32_t tmp_state = ObXATransState::NON_EXISTING;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "ObTransService not inited", K(ret));
  } else if (OB_UNLIKELY(!is_running_)) {
    ret = OB_NOT_RUNNING;
    TRANS_LOG(WARN, "ObTransService is not running", K(ret));
  } else if (trans_desc.is_valid() && NULL != (sche_ctx = trans_desc.get_sche_ctx())) {
    if (OB_FAIL(sche_ctx->get_xa_trans_state(tmp_state))) {
      TRANS_LOG(WARN, "get xa state error", K(ret), K(trans_desc));
      // rewrite ret if fail to get xa trans state
      ret = OB_SUCCESS;
    }
  } else {
    // do nothing
  }

  state = tmp_state;
  return ret;
}

int ObTransService::xa_init_sche_ctx_(ObTransDesc& trans_desc, ObScheTransCtx* sche_ctx)
{
  int ret = OB_SUCCESS;
  const ObTransID& trans_id = trans_desc.get_trans_id();
  const ObXATransID& xid = trans_desc.get_xid();

  if (OB_FAIL(sche_ctx->init(trans_desc.get_tenant_id(),
          trans_id,
          trans_desc.get_trans_expired_time(),
          SCHE_PARTITION_ID,
          &sche_trans_ctx_mgr_,
          trans_desc.get_trans_param(),
          trans_desc.get_cluster_version(),
          this,
          trans_desc.is_can_elr()))) {
    TRANS_LOG(WARN, "transaction context init error", K(ret), K(trans_id));
  } else {
    //(void)sche_ctx->set_trans_app_trace_id_str(trans_desc.get_trans_app_trace_id_str());
    // sche_ctx->set_need_record_rollback_trans_log(trans_desc.need_record_rollback_trans_log());
    if (OB_FAIL(sche_ctx->set_xid(xid))) {
      TRANS_LOG(WARN, "sche ctx set xid failed", K(ret), K(xid));
    } else if (OB_FAIL(sche_ctx->set_xa_trans_state(ObXATransState::ACTIVE))) {
      TRANS_LOG(WARN, "set xa trans state error", K(ret), K(xid), K(trans_id));
    } else if (OB_FAIL(sche_ctx->set_scheduler(self_))) {
      TRANS_LOG(WARN, "set scheduler error", K(ret), K_(self), K(trans_id));
      // when scheduler is being created, if no quires are executed, then the sql_no is 0;
      // if a query has been executed, then this indicates that all are rollbacked, scheduler needs to record this value
    } else if (OB_FAIL(sche_ctx->set_sql_no(trans_desc.get_sql_no()))) {
      TRANS_LOG(WARN, "scheduler set sql no error", K(ret), K(trans_id));
    } else if (OB_FAIL(sche_ctx->set_session_id(trans_desc.get_session_id()))) {
      TRANS_LOG(WARN, "scheduler set session id error", K(ret), K(trans_id), "session_id", trans_desc.get_session_id());
    } else if (OB_FAIL(sche_ctx->set_proxy_session_id(trans_desc.get_proxy_session_id()))) {
      TRANS_LOG(WARN,
          "scheduler set proxy session id error",
          KR(ret),
          K(trans_id),
          "proxy_session_id",
          trans_desc.get_proxy_session_id());
    } else if (OB_FAIL(sche_ctx->start_trans(trans_desc))) {
      TRANS_LOG(WARN, "start transaction error", KR(ret), K(trans_id));
    } else if (OB_FAIL(sche_ctx->save_trans_desc(trans_desc))) {
      TRANS_LOG(WARN, "save trans desc failed", K(ret), K(trans_desc));
    } else if (OB_FAIL(trans_desc.set_sche_ctx(sche_ctx))) {
      TRANS_LOG(WARN, "set sche ctx failed", K(ret), K(trans_desc));
    }
  }
  return ret;
}

int ObTransService::xa_scheduler_hb_req()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    TRANS_LOG(WARN, "ObTransService not inited");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(!is_running_)) {
    TRANS_LOG(WARN, "ObTransService is not running");
    ret = OB_NOT_RUNNING;
  } else if (OB_FAIL(sche_trans_ctx_mgr_.xa_scheduler_hb_req())) {
    TRANS_LOG(WARN, "sche ctx mgr scheduler hb failed", K(ret));
  }
  return ret;
}

// OB_SUCCESS is returned when the func is executed successfully
int ObTransService::handle_terminate_for_xa_branch_(ObTransDesc& trans_desc)
{
  int ret = OB_SUCCESS;
  ObScheTransCtx* sche_ctx = trans_desc.get_sche_ctx();
  const uint64_t tenant_id = trans_desc.get_tenant_id();
  const bool is_rollback = true;
  const bool is_terminated = true;
  const int64_t now = ObTimeUtility::current_time();
  ObAddr original_scheduler_addr = trans_desc.get_orig_scheduler();
  ObTransID trans_id = trans_desc.get_trans_id();
  ObXATransID xid = trans_desc.get_xid();
  TRANS_LOG(INFO, "start to terminate xa trans", K(xid), K(trans_id), "lbt", lbt());
  if (NULL == sche_ctx) {
    TRANS_LOG(INFO, "transaction ctx is null, may be stmt fail or rollback", K(trans_desc));
  } else if (!sche_ctx->is_xa_tightly_coupled()) {
    // use original logic for loosely coupled mode
    if (OB_FAIL(sche_ctx->xa_rollback_session_terminate())) {
      TRANS_LOG(WARN, "rollback xa trans failed", K(ret), K(trans_desc));
    }
    // if tmp scheduler, we needs to send terminate request to the original scheduler
    // send the terminate request to the original scheduler no matter whether the rollback succeeds
    if (original_scheduler_addr != self_) {
      int result = OB_SUCCESS;
      int64_t retry_times = 5;
      do {
        obrpc::ObXAEndTransRPCRequest req;
        obrpc::ObXARPCCB<obrpc::OB_XA_END_TRANS> cb;
        ObTransCond cond;
        // rely on timeout of cb, therefore timeout of cond is set to max
        const int64_t wait_time = (INT64_MAX - now) / 2;
        if (OB_FAIL(cb.init(&cond))) {
          TRANS_LOG(WARN, "ObXARPCCB init failed", KR(ret));
        } else if (OB_FAIL(req.init(trans_id, xid, is_rollback /*true*/, is_terminated /*true*/))) {
          TRANS_LOG(WARN,
              "init ObXAEndTransRPCRequest failed",
              KR(ret),
              K(xid),
              K(trans_id),
              K(is_rollback),
              K(is_terminated));
        } else if (OB_FAIL(xa_rpc_.xa_end_trans(tenant_id, original_scheduler_addr, req, cb))) {
          TRANS_LOG(WARN, "xa proxy end trans failed", KR(ret), K(req), K(original_scheduler_addr));
        } else if (OB_FAIL(cond.wait(wait_time, result))) {
          TRANS_LOG(WARN, "wait xa_end_trans rpc callback failed", KR(ret), K(req), K(original_scheduler_addr));
        } else if (OB_SUCCESS != result) {
          TRANS_LOG(WARN, "xa_end_trans rpc failed result", K(result), K(req), K(original_scheduler_addr));
        }
      } while (OB_SUCC(ret) && (OB_TIMEOUT == result && --retry_times > 0));
      if (OB_SUCC(ret)) {
        if (OB_TRANS_CTX_NOT_EXIST == result || OB_TIMEOUT == result) {
          // if trans ctx does not exist or the rpc is timeout, assume that the orginal scheduler has been exited
        } else {
          ret = result;
        }
      }
      if (OB_FAIL(ret)) {
        TRANS_LOG(WARN,
            "terminate remote original scheduler failed",
            K(ret),
            K(original_scheduler_addr),
            K(trans_id),
            K(xid));
      }
    }
    trans_desc.set_sche_ctx(NULL);
    (void)sche_trans_ctx_mgr_.revert_trans_ctx(sche_ctx);
    TRANS_LOG(INFO, "handle terminate for loosely coupled xa branch", K(ret), K(trans_desc));
  } else {
    // tightly
    bool need_delete_xa_all_tightly_branch = false;
    // regardless of the location of scheduler, try to acquire lock first
    int64_t expired_time = now + 10000000;  // 10s
    while (!sche_ctx->is_terminated() && OB_TRANS_STMT_NEED_RETRY == (ret = sche_ctx->xa_try_global_lock(xid))) {
      if (ObTimeUtility::current_time() > expired_time) {
        TRANS_LOG(INFO, "no need to wait, force to terminate", K(trans_desc));
        break;
      }
      usleep(100);
    }
    if (sche_ctx->is_terminated()) {
      // avoid the terminate operations of different branches
      TRANS_LOG(INFO, "xa trans has terminated", K(trans_desc));
    } else if (original_scheduler_addr == self_) {
      // original scheduler is in local
      if (OB_FAIL(sche_ctx->xa_rollback_session_terminate())) {
        TRANS_LOG(WARN, "rollback xa trans failed", K(ret), K(trans_desc));
      } else {
        TRANS_LOG(INFO, "rollback xa trans success", K(trans_desc));
      }
    } else {
      // original scheduler is in remote
      int result = OB_SUCCESS;
      int64_t retry_times = 5;
      sche_ctx->set_terminated();
      do {
        obrpc::ObXAEndTransRPCRequest req;
        obrpc::ObXARPCCB<obrpc::OB_XA_END_TRANS> cb;
        ObTransCond cond;
        // rely on timeout of cb, therefore timeout of cond is set to max
        const int64_t wait_time = (INT64_MAX - now) / 2;
        if (OB_FAIL(cb.init(&cond))) {
          TRANS_LOG(WARN, "ObXARPCCB init failed", KR(ret));
        } else if (OB_FAIL(req.init(trans_id, xid, is_rollback /*true*/, is_terminated /*true*/))) {
          TRANS_LOG(WARN,
              "init ObXAEndTransRPCRequest failed",
              KR(ret),
              K(xid),
              K(trans_id),
              K(is_rollback),
              K(is_terminated));
        } else if (OB_FAIL(xa_rpc_.xa_end_trans(tenant_id, original_scheduler_addr, req, cb))) {
          TRANS_LOG(WARN, "xa proxy end trans failed", KR(ret), K(original_scheduler_addr), K(req));
        } else if (OB_FAIL(cond.wait(wait_time, result))) {
          TRANS_LOG(WARN, "wait xa_end_trans rpc callback failed", KR(ret), K(req), K(original_scheduler_addr));
        } else if (OB_SUCCESS != result) {
          TRANS_LOG(WARN, "xa_end_trans rpc failed result", K(result), K(req), K(original_scheduler_addr));
        } else {
          // do nothing
        }
      } while (OB_SUCC(ret) && (OB_TIMEOUT == result && --retry_times > 0));
      if (OB_SUCC(ret)) {
        if (OB_TRANS_CTX_NOT_EXIST == result || OB_TIMEOUT == result) {
          // delete all branches of the global transaction
          // repeated delete may occur
          ret = OB_SUCCESS;
          need_delete_xa_all_tightly_branch = true;
        } else {
          ret = result;
        }
      }
    }
    clear_branch_for_xa_terminate_(trans_desc, sche_ctx, need_delete_xa_all_tightly_branch);
    TRANS_LOG(INFO, "handle terminate for tightly coupled xa branch", K(ret), K(trans_desc));
  }

  return ret;
}

int ObTransService::xa_try_remote_lock_(ObTransDesc& trans_desc)
{
  int ret = OB_SUCCESS;
  ObScheTransCtx* sche_ctx = trans_desc.get_sche_ctx();
  ObXATransID xid = trans_desc.get_xid();
  const uint64_t tenant_id = trans_desc.get_tenant_id();
  const ObTransID& trans_id = trans_desc.get_trans_id();
  const ObAddr& original_scheduler_addr = trans_desc.get_orig_scheduler();
  bool locked = false;

  if (OB_ISNULL(sche_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "sche ctx is null", K(ret), K(trans_desc));
  } else if (OB_SUCCESS != (ret = sche_ctx->xa_try_local_lock(xid))) {
    TRANS_LOG(INFO, "xa trans try get local lock failed, retry stmt", K(ret), K(trans_desc));
  } else {
    sche_ctx->set_tmp_trans_desc(trans_desc);
    int result = OB_SUCCESS;
    ObXASyncStatusRPCRequest sync_request;
    obrpc::ObXARPCCB<obrpc::OB_XA_SYNC_STATUS> cb;
    ObTransCond cond;
    // the wait time is required to be greater than rpc timeout
    const int64_t wait_time = OB_XA_RPC_TIMEOUT + 1000 * 1000;
    if (OB_FAIL(sync_request.init(trans_id,
            xid,
            self_,
            false /*not new branch*/,
            true /*stmt pull*/,
            sche_ctx->is_xa_tightly_coupled(),
            trans_desc.get_xa_end_timeout_seconds()))) {
      TRANS_LOG(WARN, "sync request init failed", K(ret), K(xid), K(trans_id));
    } else if (OB_FAIL(cb.init(&cond))) {
      TRANS_LOG(WARN, "init cb failed", K(ret), K(trans_desc));
    } else if (OB_FAIL(xa_rpc_.xa_sync_status(tenant_id, original_scheduler_addr, sync_request, &cb))) {
      TRANS_LOG(WARN,
          "post xa sync status failed",
          K(ret),
          K(xid),
          K(original_scheduler_addr),
          K(sync_request),
          K(sync_request));
      // FIXME. replace expired time
    } else if (OB_FAIL(cond.wait(wait_time, result)) || OB_FAIL(result)) {
      if (OB_TRANS_STMT_NEED_RETRY == ret) {
        if (REACH_TIME_INTERVAL(1000 * 1000)) {
          TRANS_LOG(INFO, "xa trans get remote global lock failed", K(ret), K(trans_desc));
        }
      } else if (OB_TIMEOUT == ret) {
        TRANS_LOG(WARN, "xa trans get remote global lock timeout, need retry", K(ret), K(trans_desc));
        ret = OB_TRANS_STMT_NEED_RETRY;
      } else {
        TRANS_LOG(WARN, "wait cond failed", K(ret), K(result), K(sync_request), K(trans_desc));
      }
    } else if (OB_FAIL(sche_ctx->wait_xa_sync_status(wait_time + 500000))) {
      if (OB_TIMEOUT == ret) {
        TRANS_LOG(WARN, "wait xa sync status timeout, need retry", K(ret), K(trans_desc));
        ret = OB_TRANS_STMT_NEED_RETRY;
      } else {
        TRANS_LOG(WARN, "wait xa sync status failed", K(ret), K(trans_desc));
      }
    } else {
      TRANS_LOG(INFO, "xa sync status success", K(trans_desc));
      trans_desc.set_sche_ctx(sche_ctx);
      trans_desc.set_xid(xid);
      sche_ctx->set_xid(xid);
      locked = true;
    }
    if (!locked) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = sche_ctx->xa_release_local_lock(xid))) {
        TRANS_LOG(WARN, "xa trans release local lock failed", K(tmp_ret), K(trans_desc));
      }
    }
  }
  return ret;
}

int ObTransService::xa_release_remote_lock_(ObTransDesc& trans_desc)
{
  int ret = OB_SUCCESS;
  int result = OB_SUCCESS;
  const ObAddr& original_scheduler_addr = trans_desc.get_orig_scheduler();
  const ObTransID& trans_id = trans_desc.get_trans_id();
  // the wait time is required to be greater than rpc timeout
  const int64_t wait_time = OB_XA_RPC_TIMEOUT + 1000 * 1000;
  ObScheTransCtx* sche_ctx = trans_desc.get_sche_ctx();
  ObXATransID xid = trans_desc.get_xid();

  if (OB_ISNULL(sche_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "sche ctx is null", K(ret), K(xid), K(trans_desc));
  } else {
    int64_t retry_times = 5;
    ObXAMergeStatusRPCRequest req;
    const int64_t seq_no = trans_desc.get_sql_no();
    if (OB_FAIL(req.init(trans_desc, true /*is stmt push*/, sche_ctx->is_xa_tightly_coupled(), xid, seq_no))) {
      TRANS_LOG(WARN, "init merge status request failed", K(ret), K(xid), K(trans_desc), K(seq_no));
    } else {
      do {
        obrpc::ObXARPCCB<obrpc::OB_XA_MERGE_STATUS> cb;
        ObTransCond cond;
        if (OB_FAIL(cb.init(&cond))) {
          TRANS_LOG(WARN, "init cb failed", K(ret), K(xid), K(trans_desc));
        } else if (OB_FAIL(xa_rpc_.xa_merge_status(trans_desc.get_tenant_id(), original_scheduler_addr, req, &cb))) {
          TRANS_LOG(
              WARN, "post xa merge status failed", K(ret), K(xid), K(trans_desc), K(original_scheduler_addr), K(req));
        } else if (OB_FAIL(cond.wait(wait_time, result))) {
          TRANS_LOG(WARN, "wait cond failed", K(ret), K(result), K(trans_desc), K(original_scheduler_addr), K(req));
        } else {
          // do nothing
        }
      } while (OB_SUCC(ret) && (--retry_times > 0 && OB_TIMEOUT == result));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(result)) {
        TRANS_LOG(WARN, "merge status rpc failed", K(ret), K(xid), K(trans_desc), K(original_scheduler_addr), K(req));
      } else if (OB_FAIL(sche_ctx->xa_release_local_lock(xid))) {
        TRANS_LOG(WARN, "xa release local lock failed", K(ret), K(xid), K(trans_desc));
      } else {
        TRANS_LOG(INFO, "xa release remote lock success", K(xid), K(trans_id), K(seq_no));
      }
    } else {
      TRANS_LOG(WARN, "xa release remote lock failed", K(ret), K(xid), K(trans_id));
    }
  }
  return ret;
}

int ObTransService::xa_release_lock_(ObTransDesc& trans_desc)
{
  int ret = OB_SUCCESS;
  ObScheTransCtx* sche_ctx = trans_desc.get_sche_ctx();
  ObXATransID xid = trans_desc.get_xid();
  if (OB_ISNULL(sche_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected, sche_ctx is null", K(ret), K(trans_desc));
  } else if (!sche_ctx->is_xa_tightly_coupled()) {
    // do nothing
  } else {
    // tightly
    if (self_ == trans_desc.get_orig_scheduler()) {
      if (OB_FAIL(sche_ctx->check_for_xa_execution(false /*is_new_branch*/, xid))) {
        if (OB_TRANS_XA_BRANCH_FAIL == ret) {
          TRANS_LOG(INFO, "xa trans has terminated", K(ret), K(trans_desc), K(xid));
        } else {
          TRANS_LOG(WARN, "unexpected original scheduler for xa execution", K(ret), K(trans_desc), K(xid));
        }
      } else if (OB_FAIL(sche_ctx->xa_end_stmt(xid, trans_desc, false /*from_remote*/, trans_desc.get_sql_no()))) {
        TRANS_LOG(WARN, "xa end stmt failed", K(ret), K(xid), K(trans_desc));
      } else {
        TRANS_LOG(INFO, "xa end stmt success", K(xid), K(trans_desc));
      }
    } else {
      if (OB_FAIL(xa_release_remote_lock_(trans_desc))) {
        TRANS_LOG(WARN, "xa release remote lock failed", K(ret), K(trans_desc));
      }
      TRANS_LOG(INFO, "xa release remote lock", K(ret), K(trans_desc), K(trans_desc.get_orig_scheduler()));
    }
  }
  return ret;
}

int ObTransService::clear_branch_for_xa_terminate_(
    ObTransDesc& trans_desc, ObScheTransCtx* sche_ctx, const bool need_delete_xa_record)
{
  int ret = OB_SUCCESS;
  const int64_t tenant_id = trans_desc.get_tenant_id();
  const ObXATransID xid = trans_desc.get_xid();
  trans_desc.set_sche_ctx(NULL);
  if (need_delete_xa_record && OB_FAIL(delete_xa_all_tightly_branch(tenant_id, xid))) {
    TRANS_LOG(WARN, "delete all tightly branch from inner table failed", K(ret), K(trans_desc));
  }
  if (0 == sche_ctx->dec_and_get_xa_ref_count()) {
    // this may be repeated exit
    TRANS_LOG(INFO, "sche ctx going to exit", K(xid));
    sche_ctx->set_exiting();
  }
  (void)sche_trans_ctx_mgr_.revert_trans_ctx(sche_ctx);
  TRANS_LOG(INFO, "clear branch for xa trans when terminate", K(xid), K(trans_desc));
  return ret;
}

#define QUERY_XA_STATE_FLAG_SQL \
  "\
  SELECT state, end_flag FROM %s WHERE \
  gtrid = x'%.*s' AND bqual = x'%.*s' AND format_id = %ld"

int ObTransService::query_xa_state_and_flag(
    const uint64_t tenant_id, const ObXATransID& xid, int64_t& state, int64_t& end_flag)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy* mysql_proxy = get_mysql_proxy();
  ObSqlString sql;
  ObMySQLProxy::MySQLResult res;
  ObMySQLResult* result = NULL;
  char gtrid_str[128];
  int64_t gtrid_len = 0;
  char bqual_str[128];
  int64_t bqual_len = 0;
  int64_t original_timeout_us = THIS_WORKER.get_timeout_ts();
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + XA_INNER_TABLE_TIMEOUT);

  // query the xa trans state from inner table
  if (!is_valid_tenant_id(tenant_id) || xid.empty()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(hex_print(xid.get_gtrid_str().ptr(), xid.get_gtrid_str().length(), gtrid_str, 128, gtrid_len))) {
    TRANS_LOG(WARN, "fail to convert gtrid to hex", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(hex_print(xid.get_bqual_str().ptr(), xid.get_bqual_str().length(), bqual_str, 128, bqual_len))) {
    TRANS_LOG(WARN, "fail to convert bqual to hex", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(sql.assign_fmt(QUERY_XA_STATE_FLAG_SQL,
                 OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
                 (int)gtrid_len,
                 gtrid_str,
                 (int)bqual_len,
                 bqual_str,
                 xid.get_format_id()))) {
    TRANS_LOG(WARN, "generate query xa state flag fail", K(ret));
  } else if (OB_FAIL(mysql_proxy->read(res, tenant_id, sql.ptr()))) {
    TRANS_LOG(WARN, "execute sql read fail", K(ret), K(tenant_id), K(sql));
  } else if (OB_ISNULL(result = res.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "execute sql fail", K(ret), K(tenant_id), K(sql));
  } else if (OB_FAIL(result->next())) {
    if (OB_ITER_END != ret) {
      TRANS_LOG(WARN, "iterate next result fail", K(ret), K(sql));
    }
  } else {
    EXTRACT_INT_FIELD_MYSQL(*result, "state", state, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "end_flag", end_flag, int64_t);
  }
  THIS_WORKER.set_timeout_ts(original_timeout_us);

  return ret;
}

#define UPDATE_XA_STATE_SQL \
  "UPDATE %s SET state = %ld WHERE \
  gtrid = x'%.*s' AND bqual = x'%.*s' AND format_id = %ld AND state = %ld"

#define UPDATE_XA_STATE_SQL_2 \
  "UPDATE %s SET state = %ld WHERE \
  gtrid = x'%.*s' AND bqual = x'%.*s' AND format_id = %ld"

int ObTransService::update_xa_state(
    const uint64_t tenant_id, const int64_t state, const ObXATransID& xid, const bool one_phase, int64_t& affected_rows)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy* mysql_proxy = get_mysql_proxy();
  ObSqlString sql;
  char gtrid_str[128];
  int64_t gtrid_len = 0;
  char bqual_str[128];
  int64_t bqual_len = 0;
  int64_t prev_state;
  affected_rows = 0;
  int64_t original_timeout_us = THIS_WORKER.get_timeout_ts();
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + XA_INNER_TABLE_TIMEOUT);

  if (!ObXATransState::is_valid(state) || !is_valid_tenant_id(tenant_id) || xid.empty()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(state), K(tenant_id), K(xid));
  } else if (OB_FAIL(hex_print(xid.get_gtrid_str().ptr(), xid.get_gtrid_str().length(), gtrid_str, 128, gtrid_len))) {
    TRANS_LOG(WARN, "fail to convert gtrid to hex", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(hex_print(xid.get_bqual_str().ptr(), xid.get_bqual_str().length(), bqual_str, 128, bqual_len))) {
    TRANS_LOG(WARN, "fail to convert bqual to hex", K(ret), K(tenant_id), K(xid));
  } else if (ObXATransState::ACTIVE == state || ObXATransState::COMMITTED == state || ObXATransState::IDLE == state) {
    if (ObXATransState::ACTIVE == state) {
      prev_state = ObXATransState::IDLE;
    } else if (ObXATransState::IDLE == state) {
      prev_state = ObXATransState::ACTIVE;
    } else {
      prev_state = one_phase ? ObXATransState::IDLE : ObXATransState::PREPARED;
    }
    if (OB_FAIL(sql.assign_fmt(UPDATE_XA_STATE_SQL,
            OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
            state,
            (int)gtrid_len,
            gtrid_str,
            (int)bqual_len,
            bqual_str,
            xid.get_format_id(),
            prev_state))) {
      TRANS_LOG(WARN, "generate update xa state sql failed", KR(ret));
    }
  } else if (ObXATransState::ROLLBACKED == state) {
    if (OB_FAIL(sql.assign_fmt(UPDATE_XA_STATE_SQL_2,
            OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
            state,
            (int)gtrid_len,
            gtrid_str,
            (int)bqual_len,
            bqual_str,
            xid.get_format_id()))) {
      TRANS_LOG(WARN, "generate update xa state sql failed", KR(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "unsupport target xa state", K(ret), K(tenant_id), K(state), K(xid));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(mysql_proxy->write(tenant_id, sql.ptr(), affected_rows))) {
      TRANS_LOG(WARN, "execute update xa state sql failed", KR(ret));
    } else {
      TRANS_LOG(INFO, "update xa state", K(tenant_id), K(state), K(xid), K(affected_rows));
    }
  }
  THIS_WORKER.set_timeout_ts(original_timeout_us);
  return ret;
}

#define UPDATE_XA_STATE_FLAG_SQL \
  "UPDATE %s SET state = %ld, end_flag = %ld WHERE \
  gtrid = x'%.*s' AND bqual = x'%.*s' AND format_id = %ld AND state = %ld"

int ObTransService::update_xa_state_and_flag(
    const uint64_t tenant_id, const int64_t state, const int64_t end_flag, const ObXATransID& xid)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy* mysql_proxy = get_mysql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  char gtrid_str[128];
  int64_t gtrid_len = 0;
  char bqual_str[128];
  int64_t bqual_len = 0;
  // Currently, this interface is only called by xa end.
  // Therefore, the previous xa trans state must be active.
  int64_t prev_state = ObXATransState::ACTIVE;
  int64_t original_timeout_us = THIS_WORKER.get_timeout_ts();
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + XA_INNER_TABLE_TIMEOUT);

  if (!ObXATransState::is_valid(state) ||
      !is_valid_tenant_id(tenant_id)
      //|| (!(ObXAEndFlag::TMSUSPEND & end_flag) && !(ObXAEndFlag::TMSUCCESS & end_flag))
      || !ObXAFlag::is_valid_inner_flag(end_flag) || xid.empty()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(state), K(end_flag), K(xid), K(tenant_id));
  } else if (OB_FAIL(hex_print(xid.get_gtrid_str().ptr(), xid.get_gtrid_str().length(), gtrid_str, 128, gtrid_len))) {
    TRANS_LOG(WARN, "fail to convert gtrid to hex", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(hex_print(xid.get_bqual_str().ptr(), xid.get_bqual_str().length(), bqual_str, 128, bqual_len))) {
    TRANS_LOG(WARN, "fail to convert bqual to hex", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(sql.assign_fmt(UPDATE_XA_STATE_FLAG_SQL,
                 OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
                 state,
                 end_flag,
                 (int)gtrid_len,
                 gtrid_str,
                 (int)bqual_len,
                 bqual_str,
                 xid.get_format_id(),
                 prev_state))) {
    TRANS_LOG(WARN, "generate update xa state sql failed", KR(ret));
  } else if (OB_FAIL(mysql_proxy->write(tenant_id, sql.ptr(), affected_rows))) {
    TRANS_LOG(WARN, "execute update xa state sql failed", KR(ret));
  } else if (OB_UNLIKELY(affected_rows != 1)) {
    TRANS_LOG(WARN, "wrong updated rows", K(affected_rows));
  } else {
    TRANS_LOG(INFO, "update xa state and flag", K(tenant_id), K(state), K(end_flag), K(xid));
  }
  THIS_WORKER.set_timeout_ts(original_timeout_us);
  return ret;
}

#define QUERY_XA_COORDINATOR_TRANSID_SQL \
  "\
  SELECT coordinator, trans_id, state, is_readonly, end_flag FROM %s WHERE \
  gtrid = x'%.*s' AND bqual = x'%.*s' AND format_id = %ld"

int ObTransService::query_xa_coordinator_trans_id(const uint64_t tenant_id, const ObXATransID& xid,
    ObPartitionKey& coordinator, ObTransID& trans_id, int64_t& state, bool& is_readonly, int64_t& end_flag)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy* mysql_proxy = get_mysql_proxy();
  ObSqlString sql;
  ObMySQLProxy::MySQLResult res;
  ObMySQLResult* result = NULL;
  char gtrid_str[128];
  int64_t gtrid_len = 0;
  char bqual_str[128];
  int64_t bqual_len = 0;
  int64_t original_timeout_us = THIS_WORKER.get_timeout_ts();
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + XA_INNER_TABLE_TIMEOUT);

  if (!is_valid_tenant_id(tenant_id) || xid.empty()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(hex_print(xid.get_gtrid_str().ptr(), xid.get_gtrid_str().length(), gtrid_str, 128, gtrid_len))) {
    TRANS_LOG(WARN, "fail to convert gtrid to hex", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(hex_print(xid.get_bqual_str().ptr(), xid.get_bqual_str().length(), bqual_str, 128, bqual_len))) {
    TRANS_LOG(WARN, "fail to convert bqual to hex", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(sql.assign_fmt(QUERY_XA_COORDINATOR_TRANSID_SQL,
                 OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
                 (int)gtrid_len,
                 gtrid_str,
                 (int)bqual_len,
                 bqual_str,
                 xid.get_format_id()))) {
    TRANS_LOG(WARN, "generate query coordinator sql fail", K(ret));
  } else if (OB_FAIL(mysql_proxy->read(res, tenant_id, sql.ptr()))) {
    TRANS_LOG(WARN, "execute sql read fail", K(ret), K(tenant_id), K(sql));
  } else if (OB_ISNULL(result = res.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "execute sql fail", K(ret), K(tenant_id), K(sql));
  } else if (OB_FAIL(result->next())) {
    if (OB_ITER_END != ret) {
      TRANS_LOG(WARN, "iterate next result fail", K(ret), K(sql));
    }
  } else {
    char trans_id_buf[512];
    int64_t tmp_trans_id_len = 0;
    char coordinator_buf[128];
    int64_t tmp_coordinator_len = 0;

    EXTRACT_STRBUF_FIELD_MYSQL(*result, "coordinator", coordinator_buf, 128, tmp_coordinator_len);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "trans_id", trans_id_buf, 512, tmp_trans_id_len);
    EXTRACT_INT_FIELD_MYSQL(*result, "state", state, int64_t);
    EXTRACT_BOOL_FIELD_MYSQL(*result, "is_readonly", is_readonly);
    EXTRACT_INT_FIELD_MYSQL(*result, "end_flag", end_flag, int64_t);

    (void)tmp_trans_id_len;  // make compiler happy
    (void)tmp_coordinator_len;

    if (OB_FAIL(ret)) {
      TRANS_LOG(WARN, "fail to extract field from result", K(ret));
    } else if (OB_FAIL(trans_id.parse(trans_id_buf))) {
      TRANS_LOG(WARN, "fail to parse trans_id", K(ret));
    } else if (!is_readonly && OB_FAIL(coordinator.parse_from_cstring(coordinator_buf))) {
      TRANS_LOG(WARN, "fail to init coordinator", K(ret));
    } else {
      // do nothing
    }
  }
  THIS_WORKER.set_timeout_ts(original_timeout_us);

  return ret;
}

#define QUERY_XA_COORDINATOR_SQL \
  "\
  SELECT coordinator, trans_id, state, end_flag, is_readonly FROM %s WHERE \
  gtrid = x'%.*s' AND bqual = x'%.*s' AND format_id = %ld"

int ObTransService::query_xa_coordinator(
    const uint64_t tenant_id, const ObXATransID& xid, ObPartitionKey& coordinator, bool& is_readonly, int64_t& end_flag)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy* mysql_proxy = get_mysql_proxy();
  ObSqlString sql;
  ObMySQLProxy::MySQLResult res;
  ObMySQLResult* result = NULL;
  char gtrid_str[128];
  int64_t gtrid_len = 0;
  char bqual_str[128];
  int64_t bqual_len = 0;
  int64_t original_timeout_us = THIS_WORKER.get_timeout_ts();
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + XA_INNER_TABLE_TIMEOUT);

  if (!is_valid_tenant_id(tenant_id) || xid.empty()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(hex_print(xid.get_gtrid_str().ptr(), xid.get_gtrid_str().length(), gtrid_str, 128, gtrid_len))) {
    TRANS_LOG(WARN, "fail to convert gtrid to hex", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(hex_print(xid.get_bqual_str().ptr(), xid.get_bqual_str().length(), bqual_str, 128, bqual_len))) {
    TRANS_LOG(WARN, "fail to convert bqual to hex", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(sql.assign_fmt(QUERY_XA_COORDINATOR_SQL,
                 OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
                 (int)gtrid_len,
                 gtrid_str,
                 (int)bqual_len,
                 bqual_str,
                 xid.get_format_id()))) {
    TRANS_LOG(WARN, "generate query coordinator sql fail", K(ret));
  } else if (OB_FAIL(mysql_proxy->read(res, tenant_id, sql.ptr()))) {
    TRANS_LOG(WARN, "execute sql read fail", K(ret), K(tenant_id), K(sql));
  } else if (OB_ISNULL(result = res.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "execute sql fail", K(ret), K(tenant_id), K(sql));
  } else if (OB_FAIL(result->next())) {
    if (OB_ITER_END != ret) {
      TRANS_LOG(WARN, "iterate next result fail", K(ret), K(sql));
    }
  } else {
    char coordinator_buf[128];
    int64_t tmp_coordinator_len = 0;

    EXTRACT_BOOL_FIELD_MYSQL(*result, "is_readonly", is_readonly);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "coordinator", coordinator_buf, 128, tmp_coordinator_len);
    EXTRACT_INT_FIELD_MYSQL(*result, "end_flag", end_flag, int64_t);
    (void)tmp_coordinator_len;

    if (OB_FAIL(ret)) {
      TRANS_LOG(WARN, "fail to extract field from result", K(ret));
    } else if (!is_readonly && OB_FAIL(coordinator.parse_from_cstring(coordinator_buf))) {
      TRANS_LOG(WARN, "fail to init coordinator", K(ret), K(is_readonly));
    } else {
      // do nothing
    }
  }
  THIS_WORKER.set_timeout_ts(original_timeout_us);

  return ret;
}

#define UPDATE_COORDINATOR_SQL \
  "UPDATE %s SET coordinator = '%s',\
  state = %ld, is_readonly = %d WHERE \
  gtrid = x'%.*s' AND bqual = x'%.*s' AND format_id = %ld"

int ObTransService::update_coordinator_and_state(const uint64_t tenant_id, const ObXATransID& xid,
    const ObPartitionKey& coordinator, const int64_t state, const bool is_readonly, int64_t& affected_rows)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy* mysql_proxy = get_mysql_proxy();
  ObSqlString sql;
  char coordinator_buf[128];
  coordinator.to_string(coordinator_buf, 128);
  char gtrid_str[128];
  int64_t gtrid_len = 0;
  char bqual_str[128];
  int64_t bqual_len = 0;
  affected_rows = 0;
  int64_t original_timeout_us = THIS_WORKER.get_timeout_ts();
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + XA_INNER_TABLE_TIMEOUT);

  if (!is_valid_tenant_id(tenant_id) || xid.empty() || (!is_readonly && !coordinator.is_valid()) ||
      !ObXATransState::is_valid(state)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(xid), K(coordinator), K(state), K(is_readonly));
  } else if (OB_FAIL(hex_print(xid.get_gtrid_str().ptr(), xid.get_gtrid_str().length(), gtrid_str, 128, gtrid_len))) {
    TRANS_LOG(WARN, "fail to convert gtrid to hex", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(hex_print(xid.get_bqual_str().ptr(), xid.get_bqual_str().length(), bqual_str, 128, bqual_len))) {
    TRANS_LOG(WARN, "fail to convert bqual to hex", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(sql.assign_fmt(UPDATE_COORDINATOR_SQL,
                 OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
                 coordinator_buf,
                 state,
                 is_readonly,
                 (int)gtrid_len,
                 gtrid_str,
                 (int)bqual_len,
                 bqual_str,
                 xid.get_format_id()))) {
    TRANS_LOG(WARN, "generate update coordinator sql failed", KR(ret), K(sql));
  } else if (OB_FAIL(mysql_proxy->write(tenant_id, sql.ptr(), affected_rows))) {
    TRANS_LOG(WARN, "execute update coordinator sql failed", KR(ret));
  } else {
    TRANS_LOG(INFO,
        "update coordinator and state success",
        K(tenant_id),
        K(xid),
        K(coordinator),
        K(state),
        K(is_readonly),
        K(affected_rows));
  }
  THIS_WORKER.set_timeout_ts(original_timeout_us);
  return ret;
}

#define QUERY_XA_SCHEDULER_SQL \
  "SELECT scheduler_ip, scheduler_port,\
    trans_id, state, end_flag FROM %s WHERE \
    gtrid = x'%.*s' AND bqual = x'%.*s' AND format_id = %ld"
// select scheduler, trans id, state and end_flag
int ObTransService::query_xa_scheduler_trans_id(const uint64_t tenant_id, const ObXATransID& xid,
    ObAddr& scheduler_addr, ObTransID& trans_id, int64_t& state, int64_t& end_flag)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy* mysql_proxy = get_mysql_proxy();
  ObSqlString sql;
  ObMySQLProxy::MySQLResult res;
  ObMySQLResult* result = NULL;
  char gtrid_str[128];
  int64_t gtrid_len = 0;
  char bqual_str[128];
  int64_t bqual_len = 0;
  int64_t original_timeout_us = THIS_WORKER.get_timeout_ts();
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + XA_INNER_TABLE_TIMEOUT);

  if (!is_valid_tenant_id(tenant_id) || xid.empty()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(hex_print(xid.get_gtrid_str().ptr(), xid.get_gtrid_str().length(), gtrid_str, 128, gtrid_len))) {
    TRANS_LOG(WARN, "fail to convert gtrid to hex", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(hex_print(xid.get_bqual_str().ptr(), xid.get_bqual_str().length(), bqual_str, 128, bqual_len))) {
    TRANS_LOG(WARN, "fail to convert bqual to hex", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(sql.assign_fmt(QUERY_XA_SCHEDULER_SQL,
                 OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
                 (int)gtrid_len,
                 gtrid_str,
                 (int)bqual_len,
                 bqual_str,
                 xid.get_format_id()))) {
    TRANS_LOG(WARN, "generate query xa scheduler sql fail", K(ret), K(sql));
  } else if (OB_FAIL(mysql_proxy->read(res, tenant_id, sql.ptr()))) {
    TRANS_LOG(WARN, "execute sql read fail", K(ret), K(tenant_id), K(sql));
  } else if (OB_ISNULL(result = res.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "execute sql fail", K(ret), K(tenant_id), K(sql));
  } else if (OB_FAIL(result->next())) {
    if (OB_ITER_END != ret) {
      TRANS_LOG(WARN, "iterate next result fail", K(ret), K(sql));
    }
  } else {
    char trans_id_buf[512];
    int64_t tmp_trans_id_len = 0;
    char scheduler_ip_buf[128];
    int64_t tmp_scheduler_ip_len = 0;
    int64_t scheduler_port = 0;

    EXTRACT_STRBUF_FIELD_MYSQL(*result, "scheduler_ip", scheduler_ip_buf, 128, tmp_scheduler_ip_len);
    EXTRACT_INT_FIELD_MYSQL(*result, "scheduler_port", scheduler_port, int64_t);
    EXTRACT_STRBUF_FIELD_MYSQL(*result, "trans_id", trans_id_buf, 512, tmp_trans_id_len);
    EXTRACT_INT_FIELD_MYSQL(*result, "state", state, int64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "end_flag", end_flag, int64_t);

    (void)tmp_trans_id_len;  // make compiler happy
    (void)tmp_scheduler_ip_len;

    // TODO parse addr
    if (OB_FAIL(ret)) {
      TRANS_LOG(WARN, "fail to extract field from result", K(ret));
    } else if (OB_FAIL(trans_id.parse(trans_id_buf))) {
      TRANS_LOG(WARN, "fail to parse trans_id", K(ret));
    } else if (!scheduler_addr.set_ip_addr(scheduler_ip_buf, scheduler_port)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "set scheduler addr failed", K(scheduler_ip_buf), K(scheduler_port));
    }
  }
  THIS_WORKER.set_timeout_ts(original_timeout_us);
  return ret;
}

#define INSERT_XA_LOCK_SQL \
  "\
  insert into %s (gtrid, bqual, format_id, \
  trans_id, scheduler_ip, scheduler_port, state, end_flag) \
  values (x'%.*s', x'%.*s', %ld, '%s', '%s', %d, %d, %ld)"

int ObTransService::insert_xa_lock(
    ObISQLClient& client, const uint64_t tenant_id, const ObXATransID& xid, const ObTransID& trans_id)
{
  int ret = OB_SUCCESS;
  // ObMySQLProxy *mysql_proxy = get_mysql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  char trans_id_buf[512];
  trans_id.to_string(trans_id_buf, 512);
  char scheduler_ip_buf[128];
  self_.ip_to_string(scheduler_ip_buf, 128);
  char gtrid_str[128];
  int64_t gtrid_len = 0;
  char bqual_str[128];
  const int64_t bqual_len = 0;
  int64_t original_timeout_us = THIS_WORKER.get_timeout_ts();
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + XA_INNER_TABLE_TIMEOUT);

  if (!is_valid_tenant_id(tenant_id) || xid.empty() || !trans_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(xid), K(trans_id));
  } else if (OB_FAIL(hex_print(xid.get_gtrid_str().ptr(), xid.get_gtrid_str().length(), gtrid_str, 128, gtrid_len))) {
    TRANS_LOG(WARN, "fail to convert gtrid to hex", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(sql.assign_fmt(INSERT_XA_LOCK_SQL,
                 OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
                 (int)gtrid_len,
                 gtrid_str,
                 (int)bqual_len,
                 bqual_str,
                 LOCK_FORMAT_ID,
                 trans_id_buf,
                 scheduler_ip_buf,
                 self_.get_port(),
                 ObXATransState::ACTIVE,
                 (long)ObXAFlag::TMNOFLAGS))) {
    TRANS_LOG(WARN, "generate insert xa trans sql fail", K(ret), K(sql));
  } else if (OB_FAIL(client.write(tenant_id, sql.ptr(), affected_rows))) {
    if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
      ret = OB_TRANS_XA_DUPID;
      TRANS_LOG(INFO, "xa lock already hold by others", K(tenant_id), K(xid));
    } else {
      TRANS_LOG(WARN, "execute insert xa trans sql fail", K(ret), K(sql), K(affected_rows));
    }
  } else if (OB_UNLIKELY(affected_rows != 1)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR,
        "unexpected error, update xa trans sql affects multiple rows or no rows",
        K(ret),
        K(tenant_id),
        K(affected_rows),
        K(sql));
  } else {
    TRANS_LOG(INFO, "insert xa lock", K(tenant_id), K(xid), K(trans_id));
  }
  THIS_WORKER.set_timeout_ts(original_timeout_us);
  return ret;
}

#define INSERT_XA_TRANS_SQL \
  "\
  insert into %s (gtrid, bqual, format_id, \
  trans_id, scheduler_ip, scheduler_port, state, end_flag) \
  values (x'%.*s', x'%.*s', %ld, '%s', '%s', %d, %d, %ld)"

int ObTransService::insert_xa_record(const uint64_t tenant_id, const ObXATransID& xid, const ObTransID& trans_id,
    const ObAddr& sche_addr, const int64_t flag)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy* mysql_proxy = get_mysql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  char trans_id_buf[512];
  trans_id.to_string(trans_id_buf, 512);
  char scheduler_ip_buf[128];
  sche_addr.ip_to_string(scheduler_ip_buf, 128);
  char gtrid_str[128];
  int64_t gtrid_len = 0;
  char bqual_str[128];
  int64_t bqual_len = 0;
  int64_t original_timeout_us = THIS_WORKER.get_timeout_ts();
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + XA_INNER_TABLE_TIMEOUT);

  if (!is_valid_tenant_id(tenant_id) || xid.empty() || !trans_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(xid), K(trans_id));
  } else if (OB_FAIL(hex_print(xid.get_gtrid_str().ptr(), xid.get_gtrid_str().length(), gtrid_str, 128, gtrid_len))) {
    TRANS_LOG(WARN, "fail to convert gtrid to hex", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(hex_print(xid.get_bqual_str().ptr(), xid.get_bqual_str().length(), bqual_str, 128, bqual_len))) {
    TRANS_LOG(WARN, "fail to convert bqual to hex", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(sql.assign_fmt(INSERT_XA_TRANS_SQL,
                 OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
                 (int)gtrid_len,
                 gtrid_str,
                 (int)bqual_len,
                 bqual_str,
                 xid.get_format_id(),
                 trans_id_buf,
                 scheduler_ip_buf,
                 sche_addr.get_port(),
                 ObXATransState::ACTIVE,
                 flag))) {
    TRANS_LOG(WARN, "generate insert xa trans sql fail", K(ret), K(sql));
  } else if (OB_FAIL(mysql_proxy->write(tenant_id, sql.ptr(), affected_rows))) {
    TRANS_LOG(WARN, "execute insert xa trans sql fail", K(ret), K(sql), K(affected_rows));
    if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
      ret = OB_TRANS_XA_DUPID;
    }
  } else if (OB_UNLIKELY(affected_rows != 1)) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR,
        "unexpected error, update xa trans sql affects multiple rows or no rows",
        K(ret),
        K(tenant_id),
        K(affected_rows),
        K(sql));
  } else {
    TRANS_LOG(INFO, "insert xa record", K(tenant_id), K(xid), K(trans_id));
  }
  THIS_WORKER.set_timeout_ts(original_timeout_us);
  return ret;
}

#define DELETE_XA_TRANS_SQL \
  "delete from %s where \
  gtrid = x'%.*s' and bqual = x'%.*s' and format_id = %ld"

int ObTransService::delete_xa_record(const uint64_t tenant_id, const ObXATransID& xid)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy* mysql_proxy = get_mysql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  char gtrid_str[128];
  int64_t gtrid_len = 0;
  char bqual_str[128];
  int64_t bqual_len = 0;
  int64_t original_timeout_us = THIS_WORKER.get_timeout_ts();
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + XA_INNER_TABLE_TIMEOUT);

  if (!is_valid_tenant_id(tenant_id) || xid.empty()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(hex_print(xid.get_gtrid_str().ptr(), xid.get_gtrid_str().length(), gtrid_str, 128, gtrid_len))) {
    TRANS_LOG(WARN, "fail to convert gtrid to hex", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(hex_print(xid.get_bqual_str().ptr(), xid.get_bqual_str().length(), bqual_str, 128, bqual_len))) {
    TRANS_LOG(WARN, "fail to convert bqual to hex", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(sql.assign_fmt(DELETE_XA_TRANS_SQL,
                 OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
                 (int)gtrid_len,
                 gtrid_str,
                 (int)bqual_len,
                 bqual_str,
                 xid.get_format_id()))) {
    TRANS_LOG(WARN, "generate delete xa trans sql fail", K(ret), K(sql));
  } else if (OB_FAIL(mysql_proxy->write(tenant_id, sql.ptr(), affected_rows))) {
    TRANS_LOG(WARN, "execute delete xa trans sql fail", K(ret), K(sql), K(affected_rows));
  } else if (OB_UNLIKELY(affected_rows > 1)) {
    // delete multiple rows, unexpected
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "update xa trans sql affects multiple rows", K(tenant_id), K(affected_rows), K(sql));
  } else if (OB_UNLIKELY(affected_rows == 0)) {
    // no rows deleted, since a duplicate xid may exist
    // in rollback case, success is returned
    TRANS_LOG(WARN, "update xa trans sql affects no rows", K(tenant_id), K(sql));
  } else {
    TRANS_LOG(INFO, "delete xa record", K(tenant_id), K(xid), "lbt", lbt());
  }
  THIS_WORKER.set_timeout_ts(original_timeout_us);
  return ret;
}

#define DELETE_XA_LAST_TIGHTLY_BRANCH_SQL \
  "delete from %s where \
  (gtrid = x'%.*s' and bqual=x'%.*s' and format_id=%ld) or\
  (gtrid = x'%.*s' and bqual='' and format_id=%ld)"

#define DELETE_XA_ALL_TIGHTLY_BRANCH_SQL \
  "delete from %s where \
  gtrid = x'%.*s' and end_flag & %ld = 0"

int ObTransService::delete_xa_branch(const uint64_t tenant_id, const ObXATransID& xid, const bool is_tightly_coupled)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy* mysql_proxy = get_mysql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  char gtrid_str[128];
  int64_t gtrid_len = 0;
  char bqual_str[128];
  int64_t bqual_len = 0;
  int64_t original_timeout_us = THIS_WORKER.get_timeout_ts();
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + XA_INNER_TABLE_TIMEOUT);

  if (!is_valid_tenant_id(tenant_id) || xid.empty()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(hex_print(xid.get_gtrid_str().ptr(), xid.get_gtrid_str().length(), gtrid_str, 128, gtrid_len))) {
    TRANS_LOG(WARN, "fail to convert gtrid to hex", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(hex_print(xid.get_bqual_str().ptr(), xid.get_bqual_str().length(), bqual_str, 128, bqual_len))) {
    TRANS_LOG(WARN, "fail to convert bqual to hex", K(ret), K(tenant_id), K(xid));
  } else if (is_tightly_coupled) {
    // TIGHTLY
    // delete the lock and the branch only
    // do not delete all branches with the same gtrid
    if (OB_FAIL(sql.assign_fmt(DELETE_XA_LAST_TIGHTLY_BRANCH_SQL,
            OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
            (int)gtrid_len,
            gtrid_str,
            (int)bqual_len,
            bqual_str,
            xid.get_format_id(),
            (int)gtrid_len,
            gtrid_str,
            LOCK_FORMAT_ID))) {
      TRANS_LOG(WARN, "generate delete xa trans sql fail", K(ret), K(xid));
    } else if (OB_FAIL(mysql_proxy->write(tenant_id, sql.ptr(), affected_rows))) {
      TRANS_LOG(WARN, "execute delete xa trans sql fail", K(ret), K(sql), K(affected_rows));
    } else if (OB_UNLIKELY(affected_rows > 1)) {
      TRANS_LOG(INFO, "delete xa trans sql affects multiple rows", K(tenant_id), K(affected_rows), K(sql));
    } else {
      TRANS_LOG(INFO, "delete xa record for final tightly coupled branch", K(tenant_id), K(xid), "lbt", lbt());
    }
  } else {
    // LOOSELY
    if (OB_FAIL(sql.assign_fmt(DELETE_XA_TRANS_SQL,
            OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
            (int)gtrid_len,
            gtrid_str,
            (int)bqual_len,
            bqual_str,
            xid.get_format_id()))) {
      TRANS_LOG(WARN, "generate delete xa trans sql fail", K(ret), K(sql));
    } else if (OB_FAIL(mysql_proxy->write(tenant_id, sql.ptr(), affected_rows))) {
      TRANS_LOG(WARN, "execute delete xa trans sql fail", K(ret), K(sql), K(affected_rows));
    } else if (OB_UNLIKELY(affected_rows > 1)) {
      // delete multiple rows, unexpected
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "delete xa trans sql affects multiple rows", K(tenant_id), K(xid), K(affected_rows), K(sql));
    } else if (OB_UNLIKELY(affected_rows == 0)) {
      // no rows deleted, since a duplicate xid may exist
      // in rollback case, success is returned
      TRANS_LOG(WARN, "delete xa trans sql affects no rows", K(tenant_id), K(xid), K(sql));
    } else {
      TRANS_LOG(INFO, "delete xa record for loosely coupled branch", K(tenant_id), K(xid), "lbt", lbt());
    }
  }
  THIS_WORKER.set_timeout_ts(original_timeout_us);

  return ret;
}

int ObTransService::delete_xa_all_tightly_branch(const uint64_t tenant_id, const ObXATransID& xid)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy* mysql_proxy = get_mysql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  char gtrid_str[128];
  int64_t gtrid_len = 0;
  int64_t original_timeout_us = THIS_WORKER.get_timeout_ts();
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + XA_INNER_TABLE_TIMEOUT);

  if (!is_valid_tenant_id(tenant_id) || xid.empty()) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(hex_print(xid.get_gtrid_str().ptr(), xid.get_gtrid_str().length(), gtrid_str, 128, gtrid_len))) {
    TRANS_LOG(WARN, "fail to convert gtrid to hex", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(sql.assign_fmt(DELETE_XA_ALL_TIGHTLY_BRANCH_SQL,
                 OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
                 (int)gtrid_len,
                 gtrid_str,
                 (long)ObXAFlag::LOOSELY))) {
    TRANS_LOG(WARN, "generate delete xa trans sql fail", K(ret), K(xid));
  } else if (OB_FAIL(mysql_proxy->write(tenant_id, sql.ptr(), affected_rows))) {
    TRANS_LOG(WARN, "execute delete xa trans sql fail", K(ret), K(xid), K(sql), K(affected_rows));
  } else if (OB_UNLIKELY(affected_rows > 1)) {
    TRANS_LOG(INFO, "delete xa trans sql affects multiple rows", K(tenant_id), K(xid), K(affected_rows), K(sql));
  } else {
    TRANS_LOG(INFO, "delete xa record", K(tenant_id), K(xid), "lbt", lbt());
  }
  THIS_WORKER.set_timeout_ts(original_timeout_us);

  return ret;
}

#define DELETE_XA_RECORD_STATE \
  "delete from %s where gtrid = x'%.*s' \
  and bqual = x'%.*s' and format_id = %ld and state = %d"

int ObTransService::delete_xa_record_state(
    const uint64_t tenant_id, const ObXATransID& xid, const int32_t state, int64_t& affected_rows)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy* mysql_proxy = get_mysql_proxy();
  ObSqlString sql;
  char gtrid_str[128];
  int64_t gtrid_len = 0;
  char bqual_str[128];
  int64_t bqual_len = 0;
  int64_t original_timeout_us = THIS_WORKER.get_timeout_ts();
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + XA_INNER_TABLE_TIMEOUT);

  if (!is_valid_tenant_id(tenant_id) || xid.empty() || !ObXATransState::is_valid(state)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(xid), K(state));
  } else if (OB_FAIL(hex_print(xid.get_gtrid_str().ptr(), xid.get_gtrid_str().length(), gtrid_str, 128, gtrid_len))) {
    TRANS_LOG(WARN, "fail to convert gtrid to hex", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(hex_print(xid.get_bqual_str().ptr(), xid.get_bqual_str().length(), bqual_str, 128, bqual_len))) {
    TRANS_LOG(WARN, "fail to convert bqual to hex", K(ret), K(tenant_id), K(xid));
  } else if (OB_FAIL(sql.assign_fmt(DELETE_XA_RECORD_STATE,
                 OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
                 (int)gtrid_len,
                 gtrid_str,
                 (int)bqual_len,
                 bqual_str,
                 xid.get_format_id(),
                 state))) {
    TRANS_LOG(WARN, "generate delete xa trans sql fail", K(ret), K(xid), K(state));
  } else if (OB_FAIL(mysql_proxy->write(tenant_id, sql.ptr(), affected_rows))) {
    TRANS_LOG(WARN, "execute delete xa trans sql fail", K(ret), K(xid), K(sql), K(affected_rows));
  } else if (OB_UNLIKELY(affected_rows > 1)) {
    // delete multiple rows, unexpected
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "update xa trans sql affects multiple rows", K(tenant_id), K(xid), K(affected_rows), K(sql));
  } else {
    TRANS_LOG(INFO, "delete xa record with specific state", K(tenant_id), K(xid), K(state));
  }
  THIS_WORKER.set_timeout_ts(original_timeout_us);
  return ret;
}

#define XA_INNER_TABLE_GC_SQL \
  "delete from %s where state != %d and \
  unix_timestamp(now()) - %ld > unix_timestamp(gmt_modified)"

int ObTransService::gc_invalid_xa_record(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy* mysql_proxy = get_mysql_proxy();
  ObSqlString sql;
  int64_t affected_rows = 0;
  int64_t gc_time_threshold = GCONF._xa_gc_timeout / 1000000;  // in seconds
  int64_t original_timeout_us = THIS_WORKER.get_timeout_ts();
  THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + XA_INNER_TABLE_TIMEOUT);

  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(tenant_id));
  } else if (OB_FAIL(sql.append_fmt(XA_INNER_TABLE_GC_SQL,
                 OB_ALL_TENANT_GLOBAL_TRANSACTION_TNAME,
                 ObXATransState::PREPARED,
                 gc_time_threshold))) {
    TRANS_LOG(WARN, "generate delete xa trans sql fail", K(ret), K(sql), K(tenant_id));
  } else if (OB_FAIL(mysql_proxy->write(tenant_id, sql.ptr(), affected_rows))) {
    TRANS_LOG(WARN, "execute gc xa record sql fail", K(ret), K(sql), K(affected_rows), K(tenant_id));
  } else if (affected_rows > 0) {
    TRANS_LOG(INFO, "gc suspicious records", K(affected_rows), K(tenant_id));
  }
  THIS_WORKER.set_timeout_ts(original_timeout_us);
  return ret;
}

}  // namespace transaction
}  // namespace oceanbase
