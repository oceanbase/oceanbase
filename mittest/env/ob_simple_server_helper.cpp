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

#define USING_LOG_PREFIX STORAGE
#define private public
#define protected public
#include "ob_simple_server_helper.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "logservice/ob_log_service.h"
#include "unittest/storage/init_basic_struct.h"
#include "lib/profile/ob_trace_id.h"


namespace oceanbase
{

int SimpleServerHelper::create_ls(uint64_t tenant_id, ObAddr addr)
{
  #define FR(x) \
    if (FAILEDx(x)) {   \
      return ret;       \
    }
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  static int64_t start_ls_id = 1001;
  ObLSID ls_id(ATOMIC_AAF(&start_ls_id,1));
  if (OB_FAIL(GCTX.sql_proxy_->write(tenant_id, "alter system set enable_rebalance=false", affected_rows))) {
  }
  if (OB_SUCC(ret)) {
    ObSqlString sql;
    sql.assign_fmt("insert into __all_ls (ls_id, ls_group_id, status, flag, create_scn) values(%ld, 1001,'NORMAL', '',0)", ls_id.id());
    if (FAILEDx(GCTX.sql_proxy_->write(tenant_id, sql.ptr(), affected_rows))) {
    }
    sql.assign_fmt("insert into __all_ls_status (tenant_id, ls_id, status, ls_group_id, unit_group_id, primary_zone) values(%ld, %ld,'NORMAL', 1001, 1001, 'zone1')", tenant_id, ls_id.id());
    if (FAILEDx(GCTX.sql_proxy_->write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    }
  }
  if (OB_FAIL(ret)) {
    return ret;
  }
  MTL_SWITCH(tenant_id) {
    ObCreateLSArg arg;
    ObLSService* ls_svr = MTL(ObLSService*);
    FR(gen_create_ls_arg(tenant_id, ls_id, arg));
    FR(ls_svr->create_ls(arg));
    LOG_INFO("set member list");
    ObLSHandle handle;
    ObLS *ls = nullptr;
    FR(ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
    ls = handle.get_ls();
    ObMemberList member_list;
    int64_t paxos_replica_num = 1;
    (void) member_list.add_server(addr);
    GlobalLearnerList learner_list;
    FR(ls->set_initial_member_list(member_list,
                                                      paxos_replica_num,
                                                      learner_list));

    // check leader
    LOG_INFO("check leader");
    for (int i = 0; i < 15; i++) {
      ObRole role;
      int64_t leader_epoch = 0;
      ls->get_log_handler()->get_role(role, leader_epoch);
      if (role == ObRole::LEADER) {
        break;
      }
      ::sleep(1);
    }
  }
  return ret;
}

// select with sql_proxy
int SimpleServerHelper::select_int64(common::ObMySQLProxy &sql_proxy, const char *sql, int64_t &val)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    if (OB_FAIL(sql_proxy.read(res, sql))) {
    } else {
      sqlclient::ObMySQLResult *result = res.get_result();
      if (result == nullptr) {
        ret = OB_ENTRY_NOT_EXIST;
      } else if (OB_FAIL(result->next())) {
      } else if (OB_FAIL(result->get_int("val", val))) {
      }
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("select failed", KR(ret), K(sql));
  }
  return ret;
}

// select with sql_proxy
int SimpleServerHelper::g_select_int64(uint64_t tenant_id, const char *sql, int64_t &val)
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy &sql_proxy = *GCTX.sql_proxy_;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    if (OB_FAIL(sql_proxy.read(res, tenant_id, sql))) {
    } else {
      sqlclient::ObMySQLResult *result = res.get_result();
      if (result == nullptr) {
        ret = OB_ENTRY_NOT_EXIST;
      } else if (OB_FAIL(result->next())) {
      } else if (OB_FAIL(result->get_int("val", val))) {
      }
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("select failed", KR(ret), K(sql));
  }
  return ret;
}

int SimpleServerHelper::select_uint64(common::ObMySQLProxy &sql_proxy, const char *sql, uint64_t &val)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    if (OB_FAIL(sql_proxy.read(res, sql))) {
    } else {
      sqlclient::ObMySQLResult *result = res.get_result();
      if (result == nullptr) {
        ret = OB_ENTRY_NOT_EXIST;
      } else if (OB_FAIL(result->next())) {
      } else if (OB_FAIL(result->get_uint("val", val))) {
      }
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("select failed", KR(ret), K(sql));
  }
  return ret;
}

// select with sql_proxy
int SimpleServerHelper::g_select_uint64(uint64_t tenant_id, const char *sql, uint64_t &val)
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy &sql_proxy = *GCTX.sql_proxy_;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    if (OB_FAIL(sql_proxy.read(res, tenant_id, sql))) {
    } else {
      sqlclient::ObMySQLResult *result = res.get_result();
      if (result == nullptr) {
        ret = OB_ENTRY_NOT_EXIST;
      } else if (OB_FAIL(result->next())) {
      } else if (OB_FAIL(result->get_uint("val", val))) {
      }
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("select failed", KR(ret), K(sql));
  }
  return ret;
}

int SimpleServerHelper::select_int64(sqlclient::ObISQLConnection *conn, const char *sql, int64_t &val)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    if (OB_FAIL(conn->execute_read(OB_SYS_TENANT_ID, sql, res))) {
    } else {
      sqlclient::ObMySQLResult *result = res.get_result();
      if (result == nullptr) {
        ret = OB_ENTRY_NOT_EXIST;
      } else if (OB_FAIL(result->next())) {
      } else if (OB_FAIL(result->get_int("val", val))) {
      }
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("select failed", KR(ret), K(sql));
  }
  return ret;
}

int SimpleServerHelper::select_int64(ObMySQLTransaction &trans, uint64_t tenant_id, const char *sql, int64_t &val)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    if (OB_FAIL(trans.read(res, tenant_id, sql))) {
    } else {
      sqlclient::ObMySQLResult *result = res.get_result();
      if (result == nullptr) {
        ret = OB_ENTRY_NOT_EXIST;
      } else if (OB_FAIL(result->next())) {
      } else if (OB_FAIL(result->get_int("val", val))) {
      }
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("select failed", KR(ret), K(sql));
  }
  return ret;
}
int SimpleServerHelper::g_select_varchar(uint64_t tenant_id, const char *sql, ObString &val)
{
  int ret = OB_SUCCESS;
  common::ObMySQLProxy &sql_proxy = *GCTX.sql_proxy_;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    if (OB_FAIL(sql_proxy.read(res, tenant_id, sql))) {
    } else {
      sqlclient::ObMySQLResult *result = res.get_result();
      if (result == nullptr) {
        ret = OB_ENTRY_NOT_EXIST;
      } else if (OB_FAIL(result->next())) {
      } else {
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "val", val);
      }
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("select failed", KR(ret), K(sql));
  }
  return ret;
}

int SimpleServerHelper::select_varchar(sqlclient::ObISQLConnection *conn, const char *sql, ObString &val)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    if (OB_FAIL(conn->execute_read(OB_SYS_TENANT_ID, sql, res))) {
    } else {
      sqlclient::ObMySQLResult *result = res.get_result();
      if (result == nullptr) {
        ret = OB_ENTRY_NOT_EXIST;
      } else if (OB_FAIL(result->next())) {
      } else {
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "val", val);
      }
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("select failed", KR(ret), K(sql));
  }
  return ret;
}

int SimpleServerHelper::select_table_loc(uint64_t tenant_id, const char* table_name, ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t val = 0;
  sql.assign_fmt("select a.ls_id as val from __all_tablet_to_ls a join __all_table b where a.table_id=b.table_id and b.table_name='%s'", table_name);
  if (OB_FAIL(g_select_int64(tenant_id, sql.ptr(), val))) {
  } else {
    ls_id = ObLSID(val);
  }
  return ret;
}

int SimpleServerHelper::select_table_tablet(uint64_t tenant_id, const char* table_name, ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t val = 0;
  sql.assign_fmt("select tablet_id as val from __all_table b where table_name='%s'", table_name);
  if (OB_FAIL(g_select_int64(tenant_id, sql.ptr(), val))) {
  } else {
    tablet_id = ObTabletID(val);
  }
  return ret;
}

int SimpleServerHelper::submit_redo(uint64_t tenant_id, ObLSID ls_id)
{
  int ret = OB_SUCCESS;
  ObTransID failed_tx_id;
  MTL_SWITCH(tenant_id) {
    ObLSHandle ls_handle;
    if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    } else if (OB_FAIL(ls_handle.get_ls()->get_tx_svr()->traverse_trans_to_submit_redo_log(failed_tx_id))) {
    }
  }
  return ret;
}

int SimpleServerHelper::wait_checkpoint_newest(uint64_t tenant_id, ObLSID ls_id)
{
  LOG_INFO("wait_checkpoint_newest", K(tenant_id), K(ls_id));
  int ret = OB_SUCCESS;
  ObTransID failed_tx_id;
  SCN end_scn;
  MTL_SWITCH(tenant_id) {
    ObLSHandle ls_handle;
    if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    } else if (OB_FAIL(ls_handle.get_ls()->get_tx_svr()->traverse_trans_to_submit_redo_log(failed_tx_id))) {
    } else if (OB_FAIL(ls_handle.get_ls()->get_end_scn(end_scn))) {
    } else {
      SCN checkpoint_scn;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(ls_handle.get_ls()->advance_checkpoint_by_flush(SCN::max_scn()))) {
        } else if (FALSE_IT(checkpoint_scn = ls_handle.get_ls()->get_ls_meta().get_clog_checkpoint_scn())) {
        } else if (checkpoint_scn < end_scn) {
          LOG_INFO("wait ls checkpoint advance", K(tenant_id), K(ls_id), K(checkpoint_scn), K(end_scn));
          ob_usleep(500 * 1000);
        } else {
          LOG_INFO("wait ls checkpoint advance", K(tenant_id), K(ls_id), K(checkpoint_scn), K(end_scn));
          break;
        }
      }
    }
  }
  LOG_INFO("wait_checkpoint_newest finish", K(tenant_id), K(ls_id));
  return ret;
}

int SimpleServerHelper::freeze(uint64_t tenant_id, ObLSID ls_id, ObTabletID tablet_id)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id) {
    ObLSHandle ls_handle;
    const bool need_rewrite_meta = false;
    const bool is_sync = true;
    const bool abs_timeout_ts = ObClockGenerator::getClock() + 60LL * 1000LL * 1000LL;
    if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    } else if (OB_FAIL(ls_handle.get_ls()->tablet_freeze(tablet_id, need_rewrite_meta, is_sync, abs_timeout_ts))) {
    }
  }
  return ret;
}

int SimpleServerHelper::freeze_tx_data(uint64_t tenant_id, ObLSID ls_id)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id) {
    ObLSHandle ls_handle;
    if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    } else {
      storage::checkpoint::ObCheckpointExecutor *checkpoint_executor = ls_handle.get_ls()->get_checkpoint_executor();
      ObTxDataMemtableMgr *tx_data_memtable_mgr
        = dynamic_cast<ObTxDataMemtableMgr *>(
          dynamic_cast<ObLSTxService *>(
            checkpoint_executor->handlers_[logservice::TRANS_SERVICE_LOG_BASE_TYPE])
          ->common_checkpoints_[storage::checkpoint::ObCommonCheckpointType::TX_DATA_MEMTABLE_TYPE]);
      if (OB_ISNULL(tx_data_memtable_mgr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("checkpoint obj is null", KR(ret));
      } else if (OB_FAIL(tx_data_memtable_mgr->flush(share::SCN::max_scn(),
              checkpoint::INVALID_TRACE_ID))) {
      } else {
        usleep(10 * 1000 * 1000);
      }
    }
  }
  return ret;
}

int SimpleServerHelper::freeze_tx_ctx(uint64_t tenant_id, ObLSID ls_id)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id) {
    ObLSHandle ls_handle;
    if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    } else {
      storage::checkpoint::ObCheckpointExecutor *checkpoint_executor = ls_handle.get_ls()->get_checkpoint_executor();
      ObTxCtxMemtable *tx_ctx_memtable
        = dynamic_cast<ObTxCtxMemtable *>(
          dynamic_cast<ObLSTxService *>(
            checkpoint_executor->handlers_[logservice::TRANS_SERVICE_LOG_BASE_TYPE])
          ->common_checkpoints_[storage::checkpoint::ObCommonCheckpointType::TX_CTX_MEMTABLE_TYPE]);
      if (OB_ISNULL(tx_ctx_memtable)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("checkpoint obj is null", KR(ret));
      } else if (OB_FAIL(tx_ctx_memtable->flush(share::SCN::max_scn(), 0))) {
      } else {
        usleep(10 * 1000 * 1000);
      }
    }
  }
  return ret;
}

int SimpleServerHelper::wait_flush_finish(uint64_t tenant_id, ObLSID ls_id, ObTabletID tablet_id)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id) {
    ObLSHandle ls_handle;
    if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    } else {
      while (OB_SUCC(ret)) {
        ObTabletHandle handle;
        ObTablet *tablet = NULL;
        common::ObSEArray<storage::ObITable *, 1> memtables;
        if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->direct_get_tablet(tablet_id, handle))) {
          LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
        } else if (FALSE_IT(tablet = handle.get_obj())) {
        } else if (OB_FAIL(tablet->get_memtables(memtables))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            break;
          }
        } else {
          bool flush_finish = true;
          for (int64_t idx = 0; idx < memtables.count();idx++) {
            memtable::ObMemtable *mt = dynamic_cast<memtable::ObMemtable*>(memtables.at(idx));
            if (mt->get_mt_stat().release_time_ == 0) {
              flush_finish = false;
              break;
            }
          }
          if (flush_finish) {
            break;
          }
          ob_usleep(100 * 1000);
        }
      }
    }
  }
  return ret;
}

int SimpleServerHelper::remove_tx(uint64_t tenant_id, ObLSID ls_id, ObTransID tx_id)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id) {
    ObLSHandle ls_handle;
    if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("get ls failed", KR(ret), K(ls_id));
    } else {
      auto &m = ls_handle.get_ls()->ls_tx_svr_.mgr_->ls_tx_ctx_map_;
      ObPartTransCtx *ctx = nullptr;
      if (OB_FAIL(ls_handle.get_ls()->get_tx_ctx(tx_id, false, ctx))) {
      } else {
        ls_handle.get_ls()->revert_tx_ctx(ctx);
        CtxLockGuard ctx_lock_guard;
        ctx->get_ctx_guard(ctx_lock_guard);
        m.del(tx_id, ctx);
      }
    }
  }
  return ret;
}

int SimpleServerHelper::get_tx_ctx(uint64_t tenant_id,
                                   ObLSID ls_id,
                                   ObTransID tx_id,
                                   ObPartTransCtx *&ctx)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id) {
    ObLSHandle ls_handle;
    if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id,
                                          ls_handle,
                                          ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("get ls failed", KR(ret), K(ls_id));
    } else if (OB_FAIL(ls_handle.get_ls()->get_tx_ctx(tx_id, false, ctx))) {
      LOG_WARN("fail to get tx ctx", KR(ret), K(ls_id));
    }
  }
  return ret;
}

int SimpleServerHelper::revert_tx_ctx(uint64_t tenant_id,
                                      ObLSID ls_id,
                                      ObPartTransCtx *ctx)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id) {
    ObLSHandle ls_handle;
    if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id,
                                          ls_handle,
                                          ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("get ls failed", KR(ret), K(ls_id));
    } else if (OB_FAIL(ls_handle.get_ls()->revert_tx_ctx(ctx))) {
      LOG_WARN("fail to revert tx ctx", KR(ret), K(ls_id));
    }
  }
  return ret;
}

int SimpleServerHelper::abort_tx(uint64_t tenant_id, ObLSID ls_id, ObTransID tx_id)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id) {
    ObLSHandle ls_handle;
    ObPartTransCtx *ctx = nullptr;
    if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("get ls failed", KR(ret), K(ls_id));
    } else if (OB_FAIL(ls_handle.get_ls()->get_tx_ctx(tx_id, false, ctx))) {
    } else {
      ls_handle.get_ls()->revert_tx_ctx(ctx);
      {
        CtxLockGuard ctx_lock_guard;
        ctx->get_ctx_guard(ctx_lock_guard);
        if (OB_FAIL(ctx->do_local_abort_tx_())) {
        }
      }
      /*
      if (OB_SUCC(ret)) {
        while (true) {
          ret = ls_handle.get_ls()->get_tx_ctx(tx_id, false, ctx);
          if (OB_SUCCESS == ret) {
            ob_usleep(200* 1000);
            continue;
          } else if (OB_TRANS_CTX_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            break;
          }
        }
      }
      */
    }
  }
  return ret;
}

int SimpleServerHelper::find_session(sqlclient::ObISQLConnection *conn,
                                             int64_t &session_id)
{
  return select_int64(conn, "select connection_id() as val", session_id);
}

int SimpleServerHelper::find_tx(sqlclient::ObISQLConnection *conn, ObTransID &tx_id)
{
  int ret = OB_SUCCESS;
  int64_t session_id = 0;
  if (OB_FAIL(find_session(conn, session_id))) {
  } else {
    ObSqlString sql;
    uint64_t val = 0;
    sql.assign_fmt("select trans_id as val from __all_virtual_session_info where id=%ld", session_id);
    if (OB_FAIL(g_select_uint64(OB_SYS_TENANT_ID, sql.ptr(), val))) {
      LOG_WARN("find tx", KR(ret), K(sql));
    } else {
      tx_id = ObTransID(val);
    }
  }
  return ret;
}

int SimpleServerHelper::find_trace_id(sqlclient::ObISQLConnection *conn, ObString &trace_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(select_varchar(conn, "select last_trace_id() as val", trace_id))) {
  }
  return ret;
}

int SimpleServerHelper::find_request(uint64_t tenant_id, int64_t session_id ,
    int64_t &request_id, ObTransID &tx_id, ObString &trace_id, int64_t &retry_cnt)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  sql.assign_fmt("select request_id,transaction_id,trace_id,retry_cnt from __all_virtual_sql_audit where tenant_id=%ld and session_id=%ld order by request_id desc limit 1",
      tenant_id, session_id);
  common::ObMySQLProxy &sql_proxy = *GCTX.sql_proxy_;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    if (OB_FAIL(sql_proxy.read(res, tenant_id, sql.ptr()))) {
    } else {
      sqlclient::ObMySQLResult *result = res.get_result();
      if (result == nullptr) {
        ret = OB_ENTRY_NOT_EXIST;
      } else if (OB_FAIL(result->next())) {
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "request_id", request_id, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "transaction_id", tx_id.tx_id_, int64_t);
        EXTRACT_VARCHAR_FIELD_MYSQL(*result, "trace_id", trace_id);
        EXTRACT_INT_FIELD_MYSQL(*result, "retry_cnt", retry_cnt, int64_t);
      }
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("select failed", KR(ret), K(sql));
  }
  return ret;
}

int SimpleServerHelper::ls_resume(uint64_t tenant_id, ObLSID ls_id)
{
  int ret = OB_SUCCESS;

  MTL_SWITCH(tenant_id) {
    ObLSHandle ls_handle;
    if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    } else if (OB_FAIL(ls_handle.get_ls()->ls_tx_svr_.switch_to_follower_gracefully())) {
    } else if (OB_FAIL(ls_handle.get_ls()->ls_tx_svr_.switch_to_leader())) {
    }
  }
  return ret;
}

int SimpleServerHelper::find_tx_info(uint64_t tenant_id, ObLSID ls_id, ObTransID tx_id, ObPartTransCtx &ctx_info)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id) {
    ObLSHandle ls_handle;
    if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("get ls failed", KR(ret), K(ls_id));
    } else {
      ObPartTransCtx *ctx = nullptr;
      if (OB_FAIL(ls_handle.get_ls()->get_tx_ctx(tx_id, true, ctx))) {
      } else {
        LOGI("find_tx_info tenant_id:%ld ls_id:%ld txid:%ld epoch:%ld state:%hhu ptr:%p", tenant_id,
            ls_id.id(), tx_id.get_id(), ctx->epoch_, ctx->exec_info_.state_, ctx);
        ctx_info.trans_id_ = ctx->trans_id_;
        ctx_info.ls_id_ = ctx->ls_id_;
        ctx_info.epoch_ = ctx->epoch_;
        ctx_info.exec_info_.assign(ctx->exec_info_);
        ls_handle.get_ls()->revert_tx_ctx(ctx);
      }
    }
  }
  return ret;
}

int SimpleServerHelper::wait_tx(uint64_t tenant_id, ObLSID ls_id, ObTransID tx_id, ObTxState tx_state)
{
  LOG_INFO("wait_tx", K(tenant_id), K(ls_id), K(tx_id));
  int ret = OB_SUCCESS;
  int wait_end = false;
  while (OB_SUCC(ret) && !wait_end) {
    MTL_SWITCH(tenant_id) {
      ObLSHandle ls_handle;
      if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
        LOG_WARN("get ls failed", KR(ret), K(ls_id));
      } else {
        ObPartTransCtx *ctx = nullptr;
        if (OB_FAIL(ls_handle.get_ls()->get_tx_ctx(tx_id, true, ctx))) {
        } else {
          if (ctx->exec_info_.state_ >= tx_state) {
            wait_end = true;
          }
          if (wait_end || REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
            LOG_INFO("wait_tx", K(tx_state), K(*ctx), KP(ctx), K(ctx->exec_info_.state_), K(ls_id));
          }
          ls_handle.get_ls()->revert_tx_ctx(ctx);
        }
      }
    }
    ob_usleep(50 * 1000);
  }
  LOG_INFO("wait_tx finish", K(tenant_id), K(ls_id), K(tx_id));
  return ret;
}

int SimpleServerHelper::wait_tx_exit(uint64_t tenant_id, ObLSID ls_id, ObTransID tx_id)
{
  LOG_INFO("wait_tx_end", K(tenant_id), K(ls_id), K(tx_id));
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    MTL_SWITCH(tenant_id) {
      ObLSHandle ls_handle;
      if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
        LOG_WARN("get ls failed", KR(ret), K(ls_id));
      } else {
        ObPartTransCtx *ctx = nullptr;
        if (OB_FAIL(ls_handle.get_ls()->get_tx_ctx(tx_id, true, ctx))) {
        } else {
          if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
            LOG_INFO("wait_tx", K(*ctx), KP(ctx), K(ctx->exec_info_.state_));
          }
          ls_handle.get_ls()->revert_tx_ctx(ctx);
        }
      }
    }
    ob_usleep(50 * 1000);
  }
  LOG_INFO("wait_tx_end finish", K(ret), K(tenant_id), K(ls_id), K(tx_id));
  return ret;
}


int SimpleServerHelper::get_ls_end_scn(uint64_t tenant_id, ObLSID ls_id, SCN &end_scn)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id) {
    ObLSHandle ls_handle;
    if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("get ls failed", KR(ret), K(ls_id));
    } else if (OB_FAIL(ls_handle.get_ls()->get_end_scn(end_scn))) {
    }
  }
  return ret;
}

int SimpleServerHelper::wait_replay_advance(uint64_t tenant_id, ObLSID ls_id, SCN end_scn)
{
  int ret = OB_SUCCESS;
  bool advance = false;
  while (OB_SUCC(ret) && !advance) {
    MTL_SWITCH(tenant_id) {
      SCN replayed_scn;
      if (OB_FAIL(MTL(logservice::ObLogService*)->get_log_replay_service()->get_max_replayed_scn(ls_id, replayed_scn))) {
      } else if (replayed_scn >= end_scn) {
        advance = true;
      } else {
        ob_usleep(200 * 1000);
      }
    }
  }
  return ret;
}

int SimpleServerHelper::enable_wrs(uint64_t tenant_id, ObLSID ls_id, bool enable)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id) {
    ObLSHandle ls_handle;
    if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    } else {
      ls_handle.get_ls()->get_ls_wrs_handler()->is_enabled_ = enable;
    }
  }
  return ret;
}

int SimpleServerHelper::wait_weak_read_ts_advance(uint64_t tenant_id, ObLSID ls_id1, ObLSID ls_id2)
{
  int ret = OB_SUCCESS;
  LOG_INFO("wait_weak_read_ts_advance", K(tenant_id), K(ls_id1), K(ls_id2));
  bool advance = false;
  SCN ts1,ts2;
  while (OB_SUCC(ret) && !advance) {
    MTL_SWITCH(tenant_id) {
      ObLSHandle ls_handle1;
      ObLSHandle ls_handle2;
      if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id1, ls_handle1, ObLSGetMod::STORAGE_MOD))) {
      } else if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id2, ls_handle2, ObLSGetMod::STORAGE_MOD))) {
      } else if (FALSE_IT(ts1 = ls_handle1.get_ls()->get_ls_wrs_handler()->ls_weak_read_ts_)) {
      } else if (FALSE_IT(ts2 = ls_handle2.get_ls()->get_ls_wrs_handler()->ls_weak_read_ts_)) {
      } else if (ts1 > ts2) {
        advance = true;
      } else {
        ob_usleep(200 * 1000);
      }
    }
  }
  LOG_INFO("wait_weak_read_ts_advance finish", K(tenant_id), K(ls_id1), K(ts1), K(ls_id2), K(ts2));
  return ret;
}

int SimpleServerHelper::modify_wrs(uint64_t tenant_id, ObLSID ls_id, int64_t add_ns)
{
  LOG_INFO("modify_wrs", K(tenant_id), K(ls_id), K(add_ns));
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id) {
    ObLSHandle ls_handle;
    if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    } else {
      SCN &wrs_scn = ls_handle.get_ls()->get_ls_wrs_handler()->ls_weak_read_ts_;
      SCN old_scn = wrs_scn;
      wrs_scn = SCN::plus(old_scn, add_ns);
      LOG_INFO("modify_wrs finish", K(tenant_id), K(ls_id), K(add_ns), K(old_scn), K(wrs_scn));
    }
  }
  return ret;

}

int SimpleServerHelper::ls_reboot(uint64_t tenant_id, ObLSID ls_id)
{
  LOG_INFO("ls_reboot", K(tenant_id), K(ls_id));
  int ret = OB_SUCCESS;
  auto print_mgr_state = [](ObLS *ls) {
    const ObTxLSStateMgr &state_mgr = ls->ls_tx_svr_.mgr_->tx_ls_state_mgr_;
    LOG_INFO("print ls ctx mgr state:", K(ls->get_ls_id()),
                                        K(state_mgr));
  };
  auto func = [tenant_id, ls_id, print_mgr_state] () {
    int ret = OB_SUCCESS;
    MTL_SWITCH(tenant_id) {
      ObLSHandle ls_handle;
      SCN end_scn;
      if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
        LOG_WARN("get ls failed", KR(ret), K(tenant_id), K(ls_id));
      } else if (OB_FAIL(ls_handle.get_ls()->ls_tx_svr_.switch_to_follower_gracefully())) {
        LOG_WARN("switch to follower failed", KR(ret));
      } else if (OB_FAIL(ls_handle.get_ls()->get_end_scn(end_scn))) {
      } else if (OB_FAIL(ls_handle.get_ls()->offline())) {
        LOG_WARN("ls offline failed", KR(ret), K(tenant_id), K(ls_id));
      } else if (FALSE_IT(print_mgr_state(ls_handle.get_ls()))) {
      } else if (OB_FAIL(ls_handle.get_ls()->online())) {
        LOG_WARN("ls online failed", KR(ret), K(tenant_id), K(ls_id));
      } else if (OB_FAIL(wait_replay_advance(tenant_id, ls_id, end_scn))) {
        LOG_WARN("wait replay advance failed", KR(ret), K(tenant_id), K(ls_id), K(end_scn));
      }
      LOG_INFO("ls_reboot", KR(ret), K(tenant_id), K(ls_id));
    }
    return ret;
  };

  for (int i = 0; i < 10; i++) {
    if (OB_FAIL(func())) {
      ::sleep(2);
    } else {
      break;
    }
  }
  LOG_INFO("ls_reboot finish", K(tenant_id), K(ls_id));
  return ret;
}

int SimpleServerHelper::write(sqlclient::ObISQLConnection *conn, const char *sql)
{
  int64_t affected_rows = 0;
  return conn->execute_write(OB_SYS_TENANT_ID, sql, affected_rows);
}

int SimpleServerHelper::write(sqlclient::ObISQLConnection *conn, const char *sql, int64_t &affected_rows)
{
  return conn->execute_write(OB_SYS_TENANT_ID, sql, affected_rows);
}

int InjectTxFaultHelper::submit_log(const char *buf,
                                    const int64_t size,
                                    const share::SCN &base_ts,
                                    ObTxBaseLogCb *cb,
                                    const bool need_nonblock,
                                    const int64_t retry_timeout_us)
{

  int ret = OB_SUCCESS;
  ObSEArray<ObTxLogType, 1> log_list;
  ObTxLogBlock log_block;
  int64_t replay_hint = 0;
  ObTxLogBlockHeader &log_block_header = log_block.get_header();
  if (OB_ISNULL(mgr_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(log_block.init_for_replay(buf, size))) {
    LOG_WARN("log_block init failed", K(ret), KP(buf), K(size));
  } else {
    while (OB_SUCC(ret)) {
      ObTxLogHeader header;
      if (OB_FAIL(log_block.get_next_log(header))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("log_block get_next failed", K(ret), K(log_block_header));
        }
      } else if (OB_FAIL(log_list.push_back(header.get_tx_log_type()))) {
      }
    }
  }
  ObLSID ls_id;
  if (OB_NOT_NULL(mgr_)) {
    ls_id = mgr_->ls_id_;
  }
  LOG_INFO("submit_log", K(ret), K(log_block_header), K(log_list), K(ls_id));
  ObTxLogType *inject_tx_log_type = nullptr;
  if (FALSE_IT(inject_tx_log_type = tx_injects_.get(log_block_header.tx_id_))) {
  } else if (OB_ISNULL(inject_tx_log_type)) {
  } else if (*inject_tx_log_type == ObTxLogType::UNKNOWN) {
    ret = OB_EAGAIN;
    LOG_WARN("submit log tx inject fault", K(ret), K(log_block_header.tx_id_));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < log_list.count(); i++) {
      if (log_list.at(i) == *inject_tx_log_type) {
        ret = OB_EAGAIN;
        LOG_WARN("submit log tx inject fault", K(ret), K(log_block_header.tx_id_));
      }
    }
  }
  if (FAILEDx(mgr_->log_adapter_def_.submit_log(buf,
                                                size,
                                                base_ts,
                                                cb,
                                                need_nonblock))) {
  }
  return ret;
}

int InjectTxFaultHelper::inject_tx_block(uint64_t tenant_id, ObLSID ls_id, ObTransID tx_id, ObTxLogType log_type)
{
  LOG_INFO("inject_tx_block", K(tenant_id), K(ls_id), K(tx_id), K(log_type));
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id) {
    ObLSHandle ls_handle;
    if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    } else if (OB_FAIL(tx_injects_.set_refactored(tx_id, log_type))) {
    } else if (OB_ISNULL(mgr_)) {
      // replace log_adapter
      ObLSTxCtxMgr *mgr = ls_handle.get_ls()->ls_tx_svr_.mgr_;
      log_handler_ = mgr->log_adapter_def_.log_handler_;
      dup_table_ls_handler_ = mgr->log_adapter_def_.dup_table_ls_handler_;
      tx_table_ = mgr->log_adapter_def_.tx_table_;
      mgr->tx_log_adapter_ = this;
      mgr_ = mgr;
    }
  }
  LOG_INFO("inject_tx_block finish", K(ret), K(tenant_id), K(ls_id), K(tx_id), K(log_type));
  return ret;
}

void InjectTxFaultHelper::release()
{
  if (OB_NOT_NULL(mgr_)) {
    mgr_->tx_log_adapter_ = &mgr_->log_adapter_def_;
  }
  mgr_ = NULL;
  tx_injects_.clear();
}

}
