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

#define USING_LOG_PREFIX TABLELOCK
#include "common/ob_tablet_id.h"
#include "storage/tablelock/ob_table_lock_service.h"

#include "observer/ob_server_struct.h"
#include "observer/ob_srv_network_frame.h"
#include "share/ob_define.h"
#include "share/ob_errno.h"
#include "share/schema/ob_part_mgr_util.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_tenant_schema_service.h"
#include "storage/tx/ob_trans_deadlock_adapter.h"
#include "storage/tx/ob_trans_service.h"

namespace oceanbase
{

using namespace common;
using namespace share;
using namespace share::schema;

namespace transaction
{

namespace tablelock
{

ObTableLockService::ObTableLockCtx::ObTableLockCtx(const uint64_t table_id,
                                                   const int64_t origin_timeout_us,
                                                   const int64_t timeout_us)
  : is_in_trans_(false),
    table_id_(table_id),
    origin_timeout_us_(origin_timeout_us),
    timeout_us_(timeout_us),
    abs_timeout_ts_(),
    trans_state_(),
    tx_desc_(nullptr),
    current_savepoint_(-1),
    tablet_list_(),
    schema_version_(-1),
    tx_is_killed_(false),
    stmt_savepoint_(-1)
{
  tenant_id_ = MTL_ID();
  abs_timeout_ts_ = (0 == timeout_us)
    ? ObTimeUtility::current_time() + DEFAULT_TIMEOUT_US
    : ObTimeUtility::current_time() + timeout_us;
}

ObTableLockService::ObTableLockCtx::ObTableLockCtx(const uint64_t table_id,
                                                   const ObTabletID &tablet_id,
                                                   const int64_t origin_timeout_us,
                                                   const int64_t timeout_us)
  : ObTableLockCtx(table_id, origin_timeout_us, timeout_us)
{
  tablet_id_ = tablet_id;
}

bool ObTableLockService::ObTableLockCtx::is_timeout() const
{
  return ObTimeUtility::current_time() >= abs_timeout_ts_;
}

int64_t ObTableLockService::ObTableLockCtx::remain_timeoutus() const
{
  int64_t remain_us = abs_timeout_ts_ - ObTimeUtility::current_time();
  return remain_us > 0 ? remain_us : 0;
}

int64_t ObTableLockService::ObTableLockCtx::get_rpc_timeoutus() const
{
  // rpc timeout should larger than stmt remain timeout us.
  // we add 2 second now.
  return (remain_timeoutus() + DEFAULT_RPC_TIMEOUT_US);
}

int64_t ObTableLockService::ObTableLockCtx::get_tablet_cnt() const
{
  return tablet_list_.count();
}

const ObTabletID &ObTableLockService::ObTableLockCtx::get_tablet_id(const int64_t index) const
{
  return tablet_list_.at(index);
}

int ObTableLockService::ObTableLockCtx::add_touched_ls(const ObLSID &lsid)
{
  int ret = OB_SUCCESS;
  bool exist = false;
  // check if the touched ls exist.
  for (int64_t i = 0; OB_SUCC(ret) && i < need_rollback_ls_.count(); i++) {
    ObLSID &curr = need_rollback_ls_.at(i);
    if (curr == lsid) {
      exist = true;
      break;
    }
  }
  // add if the touched ls not exist.
  if (!exist && OB_FAIL(need_rollback_ls_.push_back(lsid))) {
    LOG_ERROR("add touche ls failed", K(ret), K(lsid));
  }
  return ret;
}

void ObTableLockService::ObTableLockCtx::clean_touched_ls()
{
  need_rollback_ls_.reuse();
}

bool ObTableLockService::ObTableLockCtx::is_deadlock_avoid_enabled() const
{
  return tablelock::is_deadlock_avoid_enabled(origin_timeout_us_);
}

int ObTableLockService::mtl_init(ObTableLockService* &lock_service)
{
  return lock_service->init();
}

int ObTableLockService::init()
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("lock service init twice.", K(ret));
  } else if (OB_UNLIKELY(!GCTX.self_addr().is_valid()) ||
             OB_ISNULL(GCTX.net_frame_) ||
             OB_ISNULL(GCTX.location_service_) ||
             OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(GCTX.self_addr()),
             KP(GCTX.net_frame_), KP(GCTX.location_service_), KP(GCTX.sql_proxy_));
  } else {
    location_service_ = GCTX.location_service_;
    sql_proxy_ = GCTX.sql_proxy_;
    is_inited_ = true;
  }

  if (OB_FAIL(ret)) {
    destroy();
  }

  return ret;
}

int ObTableLockService::start()
{
  return OB_SUCCESS;
}

void ObTableLockService::stop()
{
}

void ObTableLockService::wait()
{
}

void ObTableLockService::destroy()
{
  location_service_ = nullptr;
  sql_proxy_ = nullptr;
  is_inited_ = false;
}

static inline
bool need_retry_partitions(const int64_t ret)
{
  // TODO: check return code
  UNUSED(ret);
  return false;
}

int ObTableLockService::lock_table(ObTxDesc &tx_desc,
                                   const ObTxParam &tx_param,
                                   const uint64_t table_id,
                                   const ObTableLockMode lock_mode,
                                   const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObTransService *txs = NULL;
  const ObTableLockOwnerID lock_owner = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock service is not inited", K(ret), K(table_id), K(lock_mode));
  } else if (OB_UNLIKELY(!tx_desc.is_valid()) ||
             OB_UNLIKELY(!is_valid_id(table_id)) ||
             OB_UNLIKELY(!is_lock_mode_valid(lock_mode))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tx_desc), K(table_id), K(lock_mode));
  } else {
    ObTableLockCtx ctx(table_id, timeout_us, timeout_us);
    ctx.is_in_trans_ = true;
    ctx.tx_desc_ = &tx_desc;
    ctx.tx_param_ = tx_param;
    ret = process_lock_task_(ctx, LOCK_TABLE, lock_mode, lock_owner);
  }
  return ret;
}

int ObTableLockService::lock_tablet(ObTxDesc &tx_desc,
                                    const ObTxParam &tx_param,
                                    const uint64_t table_id,
                                    const ObTabletID &tablet_id,
                                    const ObTableLockMode lock_mode,
                                    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObTransService *txs = NULL;
  const ObTableLockOwnerID lock_owner = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock service is not inited", K(ret), K(table_id), K(lock_mode));
  } else if (OB_UNLIKELY(!tx_desc.is_valid()) ||
             OB_UNLIKELY(!is_valid_id(table_id)) ||
             OB_UNLIKELY(!tablet_id.is_valid()) ||
             OB_UNLIKELY(!is_lock_mode_valid(lock_mode))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tx_desc), K(table_id), K(tablet_id), K(lock_mode));
  } else {
    ObTableLockCtx ctx(table_id, tablet_id, timeout_us, timeout_us);
    ctx.is_in_trans_ = true;
    ctx.tx_desc_ = &tx_desc;
    ctx.tx_param_ = tx_param;
    ret = process_lock_task_(ctx, LOCK_TABLET, lock_mode, lock_owner);
  }
  return ret;
}

int ObTableLockService::lock_table(const uint64_t table_id,
                                   const ObTableLockMode lock_mode,
                                   const ObTableLockOwnerID lock_owner,
                                   const int64_t timeout_us)
{
  LOG_INFO("ObTableLockService::lock_table",
            K(table_id), K(lock_mode), K(lock_owner), K(timeout_us));
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock service is not inited", K(ret), K(table_id), K(lock_mode),
             K(lock_owner));
  } else if (OB_UNLIKELY(!is_valid_id(table_id)) ||
             OB_UNLIKELY(!is_lock_mode_valid(lock_mode))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_id), K(lock_mode), K(lock_owner));
  } else {
    // avoid deadlock when ddl conflict with dml
    // by restart ddl table lock trans
    int64_t retry_timeout_us = timeout_us;
    bool need_retry = false;
    int64_t abs_timeout_ts = (0 == timeout_us)
      ? ObTimeUtility::current_time() + DEFAULT_TIMEOUT_US
      : ObTimeUtility::current_time() + timeout_us;
    do {
      if (timeout_us != 0) {
        retry_timeout_us = abs_timeout_ts - ObTimeUtility::current_time();
      }
      ObTableLockCtx ctx(table_id, timeout_us, retry_timeout_us);
      ret = process_lock_task_(ctx, LOCK_TABLE, lock_mode, lock_owner);
      need_retry = (ctx.tx_is_killed_ &&
                    !ctx.is_try_lock() &&
                    !ctx.is_timeout());
    } while (need_retry);
  }
  ret = rewrite_return_code_(ret);
  return ret;
}

int ObTableLockService::unlock_table(const uint64_t table_id,
                                     const ObTableLockMode lock_mode,
                                     const ObTableLockOwnerID lock_owner,
                                     const int64_t timeout_us)
{
  LOG_INFO("ObTableLockService::unlock_table",
            K(table_id), K(lock_mode), K(lock_owner), K(timeout_us));
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_LOCK_SERVICE_UNLOCK);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock service is not inited", K(ret), K(table_id), K(lock_mode),
             K(lock_owner));
  } else if (OB_UNLIKELY(!is_valid_id(table_id)) ||
             OB_UNLIKELY(!is_lock_mode_valid(lock_mode))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_id), K(lock_mode), K(lock_owner));
  } else {
    ObTableLockCtx ctx(table_id, timeout_us, timeout_us);
    ret = process_lock_task_(ctx, UNLOCK_TABLE, lock_mode, lock_owner);
  }
  ret = rewrite_return_code_(ret);
  return ret;
}

int ObTableLockService::lock_tablet(const uint64_t table_id,
                                    const ObTabletID &tablet_id,
                                    const ObTableLockMode lock_mode,
                                    const ObTableLockOwnerID lock_owner,
                                    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock service is not inited", K(ret), K(table_id), K(tablet_id),
             K(lock_mode), K(lock_owner));
  } else if (OB_UNLIKELY(!is_valid_id(table_id)) ||
             OB_UNLIKELY(!tablet_id.is_valid()) ||
             OB_UNLIKELY(!is_lock_mode_valid(lock_mode))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_id), K(tablet_id), K(lock_mode),
             K(lock_owner));
  } else {
    // avoid deadlock when ddl conflict with dml
    // by restart ddl table lock trans
    int64_t retry_timeout_us = timeout_us;
    bool need_retry = false;
    int64_t abs_timeout_ts = (0 == timeout_us)
      ? ObTimeUtility::current_time() + DEFAULT_TIMEOUT_US
      : ObTimeUtility::current_time() + timeout_us;
    do {
      if (timeout_us != 0) {
        retry_timeout_us = abs_timeout_ts - ObTimeUtility::current_time();
      }
      ObTableLockCtx ctx(table_id, tablet_id, timeout_us, retry_timeout_us);
      ret = process_lock_task_(ctx, LOCK_TABLET, lock_mode, lock_owner);
      need_retry = (ctx.tx_is_killed_ &&
                    !ctx.is_try_lock() &&
                    !ctx.is_timeout());
    } while (need_retry);
  }
  ret = rewrite_return_code_(ret);
  return ret;
}

int ObTableLockService::unlock_tablet(const uint64_t table_id,
                                      const ObTabletID &tablet_id,
                                      const ObTableLockMode lock_mode,
                                      const ObTableLockOwnerID lock_owner,
                                      const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_LOCK_SERVICE_UNLOCK);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("lock service is not inited", K(ret), K(table_id), K(tablet_id),
             K(lock_mode), K(lock_owner));
  } else if (OB_UNLIKELY(!is_valid_id(table_id)) ||
             OB_UNLIKELY(!tablet_id.is_valid()) ||
             OB_UNLIKELY(!is_lock_mode_valid(lock_mode))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_id), K(tablet_id), K(lock_mode),
             K(lock_owner));
  } else {
    ObTableLockCtx ctx(table_id, tablet_id, timeout_us, timeout_us);
    ret = process_lock_task_(ctx, UNLOCK_TABLET, lock_mode, lock_owner);
  }
  ret = rewrite_return_code_(ret);
  return ret;
}

int ObTableLockService::process_lock_task_(ObTableLockCtx &ctx,
                                           const ObTableLockTaskType task_type,
                                           const ObTableLockMode lock_mode,
                                           const ObTableLockOwnerID lock_owner)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  const uint64_t tenant_id = MTL_ID();
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = NULL;
  ObMultiVersionSchemaService *schema_service = MTL(ObTenantSchemaService*)->get_schema_service();

  bool is_allowed = false;

  if (OB_FAIL(refresh_schema_if_needed_(ctx))) {
    LOG_WARN("failed to refresh schema", K(ret), K(ctx));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id,
                                                             schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, ctx.table_id_, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_INFO("table not exist, check whether it meets expectations", K(ret), K(ctx));
  } else if (FALSE_IT(ctx.schema_version_ = table_schema->get_schema_version())) {
  } else if (OB_FAIL(check_op_allowed_(ctx.table_id_, table_schema, is_allowed))) {
    LOG_WARN("failed to check op allowed", K(ret), K(ctx));
  } else if (!is_allowed) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("lock table not allowed now", K(ret), K(ctx));
  } else if (!ctx.is_in_trans_ && OB_FAIL(start_tx_(ctx))) {
    LOG_WARN("failed to start trans", K(ret));
  } else if (ctx.is_in_trans_ && OB_FAIL(start_stmt_(ctx))) {
    LOG_WARN("start stmt failed", K(ret), K(ctx));
  } else if (OB_FAIL(process_table_lock_(ctx, task_type, lock_mode, lock_owner))) {
    LOG_WARN("lock table failed", K(ret), K(ctx), K(task_type), K(lock_mode), K(lock_owner));
  } else if (OB_FAIL(get_process_tablets_(task_type, table_schema, ctx))) {
    LOG_WARN("failed to get parts", K(ret), K(ctx));
  } else if (OB_FAIL(pre_check_lock_(ctx,
                                     task_type,
                                     lock_mode,
                                     lock_owner))) {
    LOG_WARN("failed to pre_check_lock_", K(ret), K(ctx), K(task_type), K(lock_mode), K(lock_owner));
  } else if (OB_FAIL(process_table_tablet_lock_(ctx,
                                                task_type,
                                                lock_mode,
                                                lock_owner))) {
    LOG_WARN("failed to lock table tablet", K(ret), K(ctx));
  }
  if (ctx.is_in_trans_ && OB_UNLIKELY(OB_SUCCESS != (tmp_ret = end_stmt_(ctx, OB_SUCCESS != ret)))) {
    LOG_WARN("failed to end stmt", K(ret), K(tmp_ret), K(ctx));
    ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
  } else if (!ctx.is_in_trans_ && OB_UNLIKELY(OB_SUCCESS != (tmp_ret = end_tx_(ctx, OB_SUCCESS != ret)))) {
    LOG_WARN("failed to end trans", K(ret), K(tmp_ret), K(ctx));
    ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
  }
  if (ctx.is_in_trans_ && ctx.tx_is_killed_) {
    // kill the in trans lock trans.
    // kill the whole trans if it is mysql mode.
    // kill the current stmt if it is oracle mode.
    if (OB_SUCCESS != (tmp_ret = deal_with_deadlock_(ctx))) {
      LOG_WARN("deal with deadlock failed.", K(tmp_ret), K(ctx));
    }
  }

  LOG_INFO("[table lock] lock_table", K(ret), K(ctx));

  return ret;
}

int ObTableLockService::get_table_lock_mode_(const ObTableLockTaskType task_type,
                                             const ObTableLockMode tablet_lock_mode,
                                             ObTableLockMode &table_lock_mode)
{
  int ret = OB_SUCCESS;
  if (task_type == LOCK_TABLET ||
      task_type == UNLOCK_TABLET) {
    // lock tablet.
    if (EXCLUSIVE == tablet_lock_mode ||
        ROW_EXCLUSIVE == tablet_lock_mode) {
      table_lock_mode = ROW_EXCLUSIVE;
    } else if (SHARE == tablet_lock_mode ||
               ROW_SHARE == tablet_lock_mode) {
      table_lock_mode = ROW_SHARE;
    } else {
      ret = OB_ERR_UNEXPECTED;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

template<class RpcProxy>
int ObTableLockService::handle_parallel_rpc_response_(int rpc_call_ret,
                                                      int64_t rpc_count,
                                                      RpcProxy &proxy_batch,
                                                      ObTableLockCtx &ctx,
                                                      ObArray<share::ObLSID> &ls_array)
{
  int ret = rpc_call_ret;
  int tmp_ret = OB_SUCCESS;
  ObTransService *txs = MTL(ObTransService*);

  if (OB_FAIL(ret)) {
    // need wait rpcs that sent finish
    // otherwise proxy reused or destructored will cause flying rpc core
    ObArray<int> return_code_array;
    proxy_batch.wait_all(return_code_array);

    // all rpcs treated as failed
    for (int i = 0; i < ls_array.count(); i++) {
      if (OB_SUCCESS != (tmp_ret = ctx.add_touched_ls(ls_array.at(i)))) {
        LOG_ERROR("add touched ls failed.", K(tmp_ret), K(ls_array.at(i)));
      }
    }
  } else {
    // handle result
    ObArray<int> return_code_array;
    if (OB_SUCCESS != (tmp_ret = proxy_batch.wait_all(return_code_array))
        || rpc_count != return_code_array.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rpc failed", KR(ret), K(tmp_ret),
                K(rpc_count), K(return_code_array.count()));
      // we need add the ls into touched to make rollback.
      for (int i = 0; i < ls_array.count(); i++) {
        if (OB_SUCCESS != (tmp_ret = ctx.add_touched_ls(ls_array.at(i)))) {
          LOG_ERROR("add touched ls failed.", K(tmp_ret), K(ls_array.at(i)));
        }
      }
    } else {
      //check each ret of every rpc
      for (int64_t i = 0; i < return_code_array.count(); ++i) {
        const ObTableLockTaskResult *result = proxy_batch.get_results().at(i);
        if (OB_SUCCESS != (tmp_ret = return_code_array.at(i))) {
          LOG_WARN("lock rpc failed", KR(tmp_ret), K(i), K(ls_array.at(i)));
        } else if (OB_ISNULL(result)) {
          tmp_ret = OB_ERR_UNEXPECTED;
          LOG_WARN("result is null", KR(tmp_ret), K(i), K(ls_array.at(i)));
        } else if (OB_SUCCESS != (tmp_ret = result->get_tx_result_code())) {
          LOG_WARN("get tx exec result failed", KR(tmp_ret), K(i), K(ls_array.at(i)));
        }

        // rpc failed or we get tx exec result failed,
        // we need add the ls into touched to make rollback.
        if (OB_SUCCESS != tmp_ret) {
          ret = tmp_ret;
          share::ObLSID ls_id = ls_array.at(i);
          if (OB_SUCCESS != (tmp_ret = ctx.add_touched_ls(ls_id))) {
            LOG_ERROR("add touched ls failed.", K(tmp_ret), K(ls_id));
          }
        } else if (OB_SUCCESS != (tmp_ret = (txs->add_tx_exec_result(*ctx.tx_desc_,
                                        proxy_batch.get_results().at(i)->tx_result_)))) {
          ret = tmp_ret;
          // must rollback the whole tx.
          LOG_WARN("failed to add exec result", K(tmp_ret), K(ctx),
                                      K(proxy_batch.get_results().at(i)->tx_result_));
          if (OB_SUCCESS != (tmp_ret = ctx.add_touched_ls(ls_array.at(i)))) {
            LOG_ERROR("add touched ls failed.", K(tmp_ret), K(ls_array.at(i)));
          }
        } else {
          // if error codes are noly OB_TRY_LOCK_ROW_CONFLICT, will retry
          tmp_ret = result->get_ret_code();
          if (OB_TRANS_KILLED == tmp_ret) {
            // the trans need kill.
            ctx.tx_is_killed_ = true;
          }
          if (OB_FAIL(tmp_ret)) {
            LOG_WARN("lock rpc wrong", K(tmp_ret), K(ls_array.at(i)));
            if (OB_SUCC(ret) || ret != OB_TRY_LOCK_ROW_CONFLICT) {
              ret = tmp_ret;
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObTableLockService::pre_check_lock_(ObTableLockCtx &ctx,
                                        const ObTableLockTaskType task_type,
                                        const ObTableLockMode lock_mode,
                                        const ObTableLockOwnerID lock_owner)
{
  int ret = OB_SUCCESS;
  int last_ret = OB_SUCCESS;
  int64_t USLEEP_TIME = 100; // 0.1 ms
  bool need_retry = false;
  // only used in LOCK_TABLE
  if (task_type == LOCK_TABLE) {
    do {
      need_retry = false;
      ObArray<share::ObLSID> ls_array;
      int64_t rpc_count = 0;
      ObTransService *txs = MTL(ObTransService*);
      ObTableLockProxy proxy_batch(*GCTX.srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::lock_table);
      int64_t timeout_us = 0;
      proxy_batch.reuse();

      // send async rpc parallel
      for (int64_t i = 0; i < ctx.get_tablet_cnt() && OB_SUCC(ret); ++i) {
        share::ObLSID ls_id;
        ObLockID lock_id;
        ObTabletID &tablet_id = ctx.tablet_list_.at(i);
        if (OB_FAIL(get_tablet_ls_(tablet_id,
                                    ls_id))) {
          LOG_WARN("failed to get tablet ls", K(ret), K(tablet_id));
        } else if (OB_FAIL(get_lock_id(tablet_id,
                                        lock_id))) {
          LOG_WARN("get lock id failed", K(ret), K(ctx));
        } else {
          ObAddr addr;
          ObTableLockTaskRequest request;
          ObTableLockTaskResult result;
          ObTableLockOpType lock_op_type = OUT_TRANS_LOCK;
          if (ctx.is_in_trans_ &&
            lock_op_type == OUT_TRANS_LOCK) {
            lock_op_type = IN_TRANS_LOCK_TABLE_LOCK;
          }

          if (OB_FAIL(ls_array.push_back(ls_id))) {
            LOG_WARN("push_back lsid failed", K(ret), K(ls_id));
          } else if (OB_FAIL(pack_request_(ctx, PRE_CHECK_TABLET, lock_op_type, lock_mode, lock_owner,
                                           lock_id, ls_id, addr, request))) {
            LOG_WARN("pack_request_ failed", K(ret), K(ls_id), K(lock_id));
            // the rpc timeout must larger than stmt timeout.
          } else if (FALSE_IT(timeout_us = ctx.get_rpc_timeoutus())) {
          } else if (ctx.is_timeout()) {
            ret = (last_ret == OB_TRY_LOCK_ROW_CONFLICT) ? OB_ERR_EXCLUSIVE_LOCK_CONFLICT : OB_TIMEOUT;
            LOG_WARN("process obj lock timeout", K(ret), K(ctx));
          } else if (OB_FAIL(proxy_batch.call(addr,
                                              timeout_us,
                                              ctx.tx_desc_->get_tenant_id(),
                                              request))) {
            LOG_WARN("failed to all async rpc", KR(ret), K(addr),
                                                K(ctx.abs_timeout_ts_), K(request));
          } else {
            rpc_count++;
            ALLOW_NEXT_LOG();
            LOG_INFO("send table pre_check rpc", KR(ret), K(addr), "request", request);
          }
        }
      }

      ret = handle_parallel_rpc_response_(ret, rpc_count, proxy_batch, ctx, ls_array);
      if (!ctx.is_try_lock() &&
          ctx.is_deadlock_avoid_enabled() &&
          OB_TRY_LOCK_ROW_CONFLICT == ret) {
        ret = OB_TRANS_KILLED;
        ctx.tx_is_killed_ = true;
      }
      if (ret == OB_TRY_LOCK_ROW_CONFLICT) {
        if (ctx.is_try_lock()) {
          // do nothing
        } else if (OB_UNLIKELY(ctx.is_timeout())) {
          ret = OB_ERR_EXCLUSIVE_LOCK_CONFLICT;
          LOG_WARN("lock table timeout", K(ret), K(ctx));
        } else {
          need_retry = true;
          last_ret = ret;
          ret = OB_SUCCESS;
          ob_usleep(USLEEP_TIME);
        }
      }
    } while (need_retry);
    LOG_DEBUG("ObTableLockService::pre_check_lock_", K(ret), K(ctx),
              K(task_type), K(lock_mode), K(lock_owner));
  }
  return ret;
}

int ObTableLockService::deal_with_deadlock_(ObTableLockCtx &ctx)
{
  int ret = OB_SUCCESS;
  SessionGuard session_guard;
  const uint32_t sess_id = ctx.tx_desc_->get_session_id();
  if (OB_FAIL(ObTransDeadlockDetectorAdapter::get_session_info(sess_id, session_guard))) {
    LOG_WARN("get session info failed", K(ret), K(sess_id));
  } else if (!session_guard.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session guard invalid", K(ret), K(sess_id));
  } else if (ObCompatibilityMode::MYSQL_MODE == session_guard->get_compatibility_mode()) {
    ret = ObTransDeadlockDetectorAdapter::kill_tx(sess_id);
  } else if (ObCompatibilityMode::ORACLE_MODE == session_guard->get_compatibility_mode()) {
    ret = ObTransDeadlockDetectorAdapter::kill_stmt(sess_id);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unknown mode", K(ret), K(session_guard->get_compatibility_mode()));
  }
  if (!OB_SUCC(ret)) {
    LOG_WARN("kill trans or stmt failed", K(ret), K(sess_id));
  }
  LOG_DEBUG("ObTableLockService::deal_with_deadlock_", K(ret), K(sess_id));
  return ret;
}

int ObTableLockService::pack_request_(ObTableLockCtx &ctx,
                                      const ObTableLockTaskType &task_type,
                                      ObTableLockOpType &lock_op_type,
                                      const ObTableLockMode &lock_mode,
                                      const ObTableLockOwnerID &lock_owner,
                                      const ObLockID &lock_id,
                                      const share::ObLSID &ls_id,
                                      ObAddr &addr,
                                      ObTableLockTaskRequest &request)
{
  int ret = OB_SUCCESS;
  ObLockParam lock_param;
  if (OB_FAIL(lock_param.set(lock_id,
                             lock_mode,
                             lock_owner,
                             lock_op_type,
                             ctx.schema_version_,
                             ctx.is_deadlock_avoid_enabled(),
                             ctx.is_try_lock(),
                             ctx.abs_timeout_ts_))) {
    LOG_WARN("get lock param failed", K(ret));
  } else if (OB_FAIL(request.set(task_type,
                                ls_id,
                                lock_param,
                                ctx.tx_desc_))) {
    LOG_WARN("get lock request failed", K(ret));
  } else if (OB_FAIL(get_ls_leader_(ctx.tx_desc_->get_cluster_id(),
                                    ctx.tx_desc_->get_tenant_id(),
                                    ls_id,
                                    ctx.abs_timeout_ts_,
                                    addr))) {
    LOG_WARN("failed to get ls leader", K(ret), K(ctx), K(ls_id));
  }
  return ret;
}

template<class RpcProxy>
int ObTableLockService::parallel_rpc_handle_(RpcProxy &proxy_batch,
                                             ObTableLockCtx &ctx,
                                             LockMap &lock_map,
                                             const ObTableLockTaskType task_type,
                                             const ObTableLockMode lock_mode,
                                             const ObTableLockOwnerID lock_owner)
{
  int ret = OB_SUCCESS;
  int64_t timeout_us = 0;
  FOREACH_X(lock, lock_map, OB_SUCC(ret)) {
    proxy_batch.reuse();
    ObArray<share::ObLSID> ls_array;
    int64_t rpc_count = 0;
    ObLockID lock_id = lock->first;
    share::ObLSID ls_id = lock->second;
    ObAddr addr;
    ObTableLockTaskRequest request;
    ObTableLockTaskResult result;
    ObTableLockOpType lock_op_type = ((task_type == LOCK_TABLET || task_type == LOCK_TABLE) ?
                                      OUT_TRANS_LOCK : OUT_TRANS_UNLOCK);
    if (ctx.is_in_trans_ &&
        lock_op_type == OUT_TRANS_LOCK) {
      lock_op_type = IN_TRANS_LOCK_TABLE_LOCK;
    }

    if (OB_FAIL(ls_array.push_back(ls_id))) {
      LOG_WARN("push_back lsid failed", K(ret), K(ls_id));
    } else if (OB_FAIL(pack_request_(ctx, task_type, lock_op_type, lock_mode, lock_owner,
                                     lock_id, ls_id, addr, request))) {
      LOG_WARN("pack_request_ failed", K(ret), K(ls_id), K(lock_id));
    } else if (FALSE_IT(timeout_us = ctx.get_rpc_timeoutus())) {
    } else if (ctx.is_timeout()) {
      ret = OB_TIMEOUT;
      LOG_WARN("process obj lock timeout", K(ret), K(ctx));
    } else if (OB_FAIL(proxy_batch.call(addr,
                                        timeout_us,
                                        ctx.tx_desc_->get_tenant_id(),
                                        request))) {
      LOG_WARN("failed to all async rpc", KR(ret), K(addr),
                                          K(ctx.abs_timeout_ts_), K(request));
    } else {
      rpc_count++;
      ALLOW_NEXT_LOG();
      LOG_INFO("send table lock rpc", KR(ret), K(rpc_count), K(addr), "request", request);
    }
    ret = handle_parallel_rpc_response_(ret, rpc_count, proxy_batch, ctx, ls_array);
  }
  return ret;
}

int ObTableLockService::inner_process_obj_lock_(ObTableLockCtx &ctx,
                                                LockMap &lock_map,
                                                const ObTableLockTaskType task_type,
                                                const ObTableLockMode lock_mode,
                                                const ObTableLockOwnerID lock_owner)
{
  int ret = OB_SUCCESS;
  // TODO: yanyuan.cxf we process the rpc one by one and do parallel later.
  if (task_type == UNLOCK_TABLE || task_type == UNLOCK_TABLET) {
    ObHighPriorityTableLockProxy proxy_batch(*GCTX.srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::unlock_table);
    ret = parallel_rpc_handle_(proxy_batch, ctx, lock_map, task_type, lock_mode, lock_owner);
  } else {
    ObTableLockProxy proxy_batch(*GCTX.srv_rpc_proxy_, &obrpc::ObSrvRpcProxy::lock_table);
    ret = parallel_rpc_handle_(proxy_batch, ctx, lock_map, task_type, lock_mode, lock_owner);
  }

  return ret;
}

int ObTableLockService::process_obj_lock_(ObTableLockCtx &ctx,
                                          const ObLSID &ls_id,
                                          const ObLockID &lock_id,
                                          const ObTableLockTaskType task_type,
                                          const ObTableLockMode lock_mode,
                                          const ObTableLockOwnerID lock_owner)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool need_retry = false;
  do {
    need_retry = false;

    if (ctx.is_timeout()) {
      ret = OB_TIMEOUT;
      LOG_WARN("lock table timeout", K(ret), K(ctx));
    } else if (OB_FAIL(start_sub_tx_(ctx))) {
      LOG_WARN("failed to start sub tx", K(ret), K(ctx));
    } else {
      LockMap lock_map;
      if (OB_FAIL(lock_map.create(1, lib::ObLabel("TableLockMap")))) {
        LOG_WARN("lock_map create failed");
      } else if (OB_FAIL(lock_map.set_refactored(lock_id, ls_id))) {
        LOG_WARN("fail to set lock_map", K(ret), K(lock_id), K(ls_id));
      } else if (OB_FAIL(inner_process_obj_lock_(ctx,
                                                 lock_map,
                                                 task_type,
                                                 lock_mode,
                                                 lock_owner))) {
        LOG_WARN("process object lock failed", K(ret), K(lock_id), K(ls_id));
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(end_sub_tx_(ctx, false/*not rollback*/))) {
          LOG_WARN("failed to end sub tx", K(ret), K(ctx));
        }
      } else {
        // rollback the sub tx.
        need_retry = need_retry_single_task_(ctx, ret);
        // overwrite the ret code.
        if (need_retry &&
            OB_FAIL(end_sub_tx_(ctx, true/*rollback*/))) {
          LOG_WARN("failed to rollback sub tx", K(ret), K(ctx));
        }
      }
    }
  } while (need_retry && OB_SUCC(ret));
  LOG_DEBUG("ObTableLockService::process_obj_lock_", K(ret), K(ctx), K(ls_id),
            K(lock_id), K(task_type), K(lock_mode), K(lock_owner));

  return ret;
}

int ObTableLockService::process_table_lock_(ObTableLockCtx &ctx,
                                            const ObTableLockTaskType task_type,
                                            const ObTableLockMode lock_mode,
                                            const ObTableLockOwnerID lock_owner)
{
  int ret = OB_SUCCESS;
  ObLockID lock_id;
  ObTableLockMode table_lock_mode = lock_mode;
  if (OB_FAIL(get_lock_id(ctx.table_id_,
                          lock_id))) {
    LOG_WARN("get lock id failed", K(ret), K(ctx));
  } else if ((task_type == LOCK_TABLET || task_type == UNLOCK_TABLET) &&
             OB_FAIL(get_table_lock_mode_(task_type,
                                          lock_mode,
                                          table_lock_mode))) {
    LOG_WARN("get table lock mode failed", K(ret), K(ctx), K(task_type), K(lock_mode));
  } else if (OB_FAIL(process_obj_lock_(ctx,
                                       LOCK_SERVICE_LS,
                                       lock_id,
                                       task_type,
                                       table_lock_mode,
                                       lock_owner))) {
    LOG_WARN("lock obj failed", K(ret), K(ctx), K(LOCK_SERVICE_LS), K(lock_id), K(task_type),
             K(lock_mode));
  }

  LOG_DEBUG("ObTableLockService::process_table_lock_", K(ret), K(ctx), K(task_type), K(lock_mode), K(lock_owner));
  return ret;
}

int ObTableLockService::process_table_tablet_lock_(ObTableLockCtx &ctx,
                                                   const ObTableLockTaskType task_type,
                                                   const ObTableLockMode lock_mode,
                                                   const ObTableLockOwnerID lock_owner)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool need_retry = false;
  ObLockID lock_id;
  ObLSID ls_id;
  LockMap lock_map;
  if (OB_FAIL(lock_map.create(ctx.get_tablet_cnt(), lib::ObLabel("TableLockMap")))) {
    LOG_WARN("fail to create lock_map", K(ret), K(ctx.get_tablet_cnt()));
  } else {
    // parallel
    for (int64_t i = 0; i < ctx.get_tablet_cnt() && OB_SUCC(ret); ++i) {
      ObTabletID &tablet_id = ctx.tablet_list_.at(i);
      if (OB_FAIL(get_tablet_ls_(tablet_id,
                                ls_id))) {
        LOG_WARN("failed to get tablet ls", K(ret), K(tablet_id));
      } else if (OB_FAIL(get_lock_id(tablet_id,
                                    lock_id))) {
        LOG_WARN("get lock id failed", K(ret), K(ctx));
      } else if (OB_FAIL(lock_map.set_refactored(lock_id, ls_id))) {
        LOG_WARN("fail to set lock_map", K(ret), K(lock_id),
                                          K(ls_id), K(tablet_id));
      }
      LOG_DEBUG("tablet add to lock map", K(lock_id), K(tablet_id), K(i));
    }

    if (OB_FAIL(ret)) {
      LOG_WARN("add to lock map failed", K(ret));
    } else {
      do {
        need_retry = false;

        if (ctx.is_timeout()) {
          ret = OB_TIMEOUT;
          LOG_WARN("lock table timeout", K(ret), K(ctx));
        } else if (OB_FAIL(start_sub_tx_(ctx))) {
          LOG_WARN("failed to start sub tx", K(ret), K(ctx));
        } else if (OB_FAIL(inner_process_obj_lock_(ctx,
                                                    lock_map,
                                                    task_type,
                                                    lock_mode,
                                                    lock_owner))) {
          LOG_WARN("fail to lock tablets", K(ret));
          // rollback the sub tx.
          need_retry = need_retry_single_task_(ctx, ret);
          // overwrite the ret code.
          if (need_retry &&
              OB_FAIL(end_sub_tx_(ctx, true/*rollback*/))) {
            LOG_WARN("failed to rollback sub tx", K(ret), K(ctx));
          }
        } else if (OB_FAIL(end_sub_tx_(ctx, false/*not rollback*/))) {
          LOG_WARN("failed to end sub tx", K(ret), K(ctx));
        }
      } while (need_retry && OB_SUCC(ret));
    }
  }
  LOG_DEBUG("ObTableLockService::process_table_tablet_lock_", K(ret), K(ctx),
            K(task_type), K(lock_mode), K(lock_owner));

  return ret;
}

int ObTableLockService::check_op_allowed_(const uint64_t table_id,
                                          const ObTableSchema *table_schema,
                                          bool &is_allowed)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();

  is_allowed = true;

  if (is_inner_table(table_id)) {
    is_allowed = false;
  } else if (!table_schema->is_user_table()) {
    // table lock not support virtual table/tmp table /sys table  etc.
    is_allowed = false;
  } else if (GCTX.is_standby_cluster() && OB_SYS_TENANT_ID != tenant_id) {
    is_allowed = false;
  } else if (!GCTX.is_standby_cluster()) {
    bool is_restore = false;
    ObMultiVersionSchemaService *schema_service = MTL(ObTenantSchemaService*)->get_schema_service();
    if (OB_FAIL(schema_service->check_tenant_is_restore(NULL,
                                                        tenant_id,
                                                        is_restore))) {
      LOG_WARN("failed to check tenant restore", K(ret), K(table_id));
    } else if (is_restore) {
      is_allowed = false;
    }
  }

  return ret;
}

int ObTableLockService::get_tablet_ls_(
    const ObTabletID &tablet_id,
    ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  bool unused_cache_hit;
  const uint64_t tenant_id = MTL_ID();
  if (OB_FAIL(location_service_->nonblock_get(tenant_id,
                                              tablet_id,
                                              ls_id))) {
    if (OB_MAPPING_BETWEEN_TABLET_AND_LS_NOT_EXIST == ret &&
        OB_FAIL(location_service_->get(tenant_id,
                                       tablet_id,
                                       INT64_MAX,
                                       unused_cache_hit,
                                       ls_id))) {
      LOG_WARN("failed to sync get ls by tablet failed.",
               K(ret), K(tenant_id), K(tablet_id));
    } else if (OB_FAIL(ret)) {
      LOG_WARN("failed to get ls by tablet", K(ret), K(tenant_id),
               K(tablet_id));
    }
  }
  LOG_DEBUG("get tablet ls", K(ret), K(tenant_id), K(ls_id));

  return ret;
}

int ObTableLockService::get_process_tablets_(const ObTableLockTaskType task_type,
                                             const ObTableSchema *table_schema,
                                             ObTableLockCtx &ctx)
{
  int ret = OB_SUCCESS;

  ctx.tablet_list_.reuse();
  if ((task_type == LOCK_TABLET || task_type == UNLOCK_TABLET)) {
    // case 1: lock/unlock tablet
    // just push the specified tablet into the tablet list.
    if (OB_FAIL(ctx.tablet_list_.push_back(ctx.tablet_id_))) {
      LOG_WARN("failed to push back tablet id", K(ret));
    }
  } else {
    // case 2: lock/unlock table
    // get all the tablet of this table.
    if (PARTITION_LEVEL_ZERO == table_schema->get_part_level()) {
      if (OB_FAIL(ctx.tablet_list_.push_back(table_schema->get_tablet_id()))) {
        LOG_WARN("failed to push back tablet id", K(ret));
      }
    } else {
      ObCheckPartitionMode check_partition_mode = CHECK_PARTITION_MODE_NORMAL;
      ObPartitionSchemaIter partition_iter(*table_schema, check_partition_mode);
      ObTabletID tablet_id;
      while (OB_SUCC(ret) && OB_SUCC(partition_iter.next_tablet_id(tablet_id))) {
        if (ctx.tablet_id_.is_valid() && tablet_id != ctx.tablet_id_) {
          // search for specified partition
        } else if (OB_FAIL(ctx.tablet_list_.push_back(tablet_id))) {
          LOG_WARN("failed to push back tablet id", K(ret));
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  LOG_DEBUG("ObTableLockService::get_process_tablets_", K(ret), K(task_type), K(ctx));

  return ret;
}

int ObTableLockService::refresh_schema_if_needed_(ObTableLockCtx &ctx)
{
  int ret = OB_SUCCESS;

  int64_t latest_schema_version = OB_INVALID_VERSION;
  ObRefreshSchemaStatus schema_status;
  ObMultiVersionSchemaService *schema_service = MTL(ObTenantSchemaService*)->get_schema_service();
  schema_status.tenant_id_ = ctx.get_tenant_id();

  int64_t tmp_timout_ts = THIS_WORKER.get_timeout_ts();
  THIS_WORKER.set_timeout_ts(ctx.abs_timeout_ts_);

  if (OB_FAIL(schema_service->get_schema_version_in_inner_table(*sql_proxy_,
                                                                schema_status,
                                                                latest_schema_version))) {
    LOG_WARN("failed to get latest schema version", K(ret), K(ctx));
  } else if (OB_FAIL(schema_service->async_refresh_schema(ctx.get_tenant_id(),
                                                          latest_schema_version))) {
    LOG_WARN("failed to refresh schema", K(ret), K(latest_schema_version), K(ctx));
  }

  THIS_WORKER.set_timeout_ts(tmp_timout_ts);

  return ret;
}

bool ObTableLockService::need_retry_single_task_(const ObTableLockCtx &ctx,
                                                 const int64_t ret) const
{
  bool need_retry = false;
  if (ctx.is_in_trans_) {
    need_retry = (OB_NOT_MASTER == ret ||
                  OB_LS_NOT_EXIST == ret);
  } else {
    // TODO: yanyuan.cxf multi data source can not rollback, so we can not retry.
  }
  return need_retry;
}

int ObTableLockService::rewrite_return_code_(const int ret) const
{
  int rewrite_rcode = ret;
  // rewrite to OB_EAGAIN, to make sure the ddl process will retry again.
  if (OB_TRANS_KILLED == ret) {
    rewrite_rcode = OB_EAGAIN;
  }
  return rewrite_rcode;
}

int ObTableLockService::get_ls_leader_(
    const int64_t cluster_id,
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const int64_t abs_timeout_ts,
    ObAddr &addr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(location_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("table lock service not inited", K(ret));
  } else if (OB_FAIL(location_service_->get_leader_with_retry_until_timeout(
      cluster_id,
      tenant_id,
      ls_id,
      addr,
      abs_timeout_ts))) {
    LOG_WARN("failed to get ls leader with retry until timeout",
        K(ret), K(cluster_id), K(tenant_id), K(ls_id), K(addr), K(abs_timeout_ts));
  } else {
    LOG_DEBUG("get ls leader from location_service",
        K(ret), K(cluster_id), K(tenant_id), K(ls_id), K(addr), K(abs_timeout_ts));
  }
  LOG_DEBUG("ObTableLockService::process_obj_lock_", K(ret), K(tenant_id), K(ls_id), K(addr));

  return ret;
}

int ObTableLockService::start_tx_(ObTableLockCtx &ctx)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTxParam &tx_param = ctx.tx_param_;
  tx_param.access_mode_ = ObTxAccessMode::RW;
  tx_param.isolation_ = ObTxIsolationLevel::RC;
  tx_param.timeout_us_ = common::max(0l, ctx.abs_timeout_ts_ - ObTimeUtility::current_time());
  tx_param.lock_timeout_us_ = -1; // use abs_timeout_ts as lock wait timeout
  tx_param.cluster_id_ = GCONF.cluster_id;
  // no session id here

  ObTransService *txs = MTL(ObTransService*);
  if (ctx.trans_state_.is_start_trans_executed()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("start_trans is executed", K(ret));
  } else if (OB_FAIL(txs->acquire_tx(ctx.tx_desc_))) {
    LOG_WARN("fail acquire txDesc", K(ret), K(tx_param));
  } else {
    if (OB_FAIL(txs->start_tx(*ctx.tx_desc_, tx_param))) {
      LOG_WARN("fail start trans", K(ret), K(tx_param));
    } else {
      ctx.trans_state_.set_start_trans_executed(true);
    }
    // start tx failed, release the txDesc I just created.
    if (OB_FAIL(ret)) {
      if (OB_TMP_FAIL(txs->release_tx(*ctx.tx_desc_))) {
        LOG_ERROR("release tx failed", K(tmp_ret), KPC(ctx.tx_desc_));
      }
    }
  }

  LOG_DEBUG("ObTableLockService::start_tx_", K(ret), K(ctx), K(tx_param));
  return ret;
}

int ObTableLockService::end_tx_(ObTableLockCtx &ctx, const bool is_rollback)
{
  int ret = OB_SUCCESS;

  if (!ctx.trans_state_.is_start_trans_executed()
      || !ctx.trans_state_.is_start_trans_success()) {
    LOG_INFO("end_trans skip", K(ret), K(ctx));
  } else {
    ObTransService *txs = MTL(ObTransService*);
    const int64_t stmt_timeout_ts = ctx.abs_timeout_ts_;
    if (is_rollback) {
      if (OB_FAIL(txs->rollback_tx(*ctx.tx_desc_))) {
        LOG_WARN("fail rollback tx when session terminate",
                 K(ret), KPC(ctx.tx_desc_), K(stmt_timeout_ts));
      }
    } else if (OB_FAIL(txs->commit_tx(*ctx.tx_desc_, stmt_timeout_ts))) {
      LOG_WARN("fail end trans when session terminate",
               K(ret), KPC(ctx.tx_desc_), K(stmt_timeout_ts));
    }
    if (OB_FAIL(txs->release_tx(*ctx.tx_desc_))) {
      LOG_ERROR("release tx failed", K(ret), KPC(ctx.tx_desc_));
    }
    ctx.tx_desc_ = NULL;
    ctx.trans_state_.clear_start_trans_executed();
  }

  ctx.trans_state_.reset();
  LOG_DEBUG("ObTableLockService::end_tx_", K(ret), K(ctx), K(is_rollback));

  return ret;
}

int ObTableLockService::start_sub_tx_(ObTableLockCtx &ctx)
{
  int ret = OB_SUCCESS;

  if (ctx.is_savepoint_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("start_sub_tx is executed", K(ret));
  } else {
    ObTransService *txs = MTL(ObTransService*);
    const ObTxParam &tx_param = ctx.tx_param_;
    const ObTxIsolationLevel &isolation_level = tx_param.isolation_;
    const int64_t expire_ts = ctx.abs_timeout_ts_;
    int64_t &savepoint = ctx.current_savepoint_;
    if (OB_FAIL(txs->create_implicit_savepoint(*ctx.tx_desc_,
                                               tx_param,
                                               savepoint))) {
      ctx.reset_savepoint();
      LOG_WARN("create implicit savepoint failed", K(ret), KPC(ctx.tx_desc_), K(tx_param));
    }
  }
  LOG_DEBUG("ObTableLockService::start_sub_tx_", K(ret), K(ctx));

  return ret;
}

int ObTableLockService::end_sub_tx_(ObTableLockCtx &ctx, const bool is_rollback)
{
  int ret = OB_SUCCESS;

  if (!ctx.is_savepoint_valid()) {
    LOG_INFO("end_sub_tx_ skip", K(ret), K(ctx));
  } else {
    const int64_t &savepoint = ctx.current_savepoint_;
    const int64_t expire_ts = OB_MAX(ctx.abs_timeout_ts_, DEFAULT_TIMEOUT_US + ObTimeUtility::current_time());
    ObTransService *txs = MTL(ObTransService*);
    if (is_rollback &&
        OB_FAIL(txs->rollback_to_implicit_savepoint(*ctx.tx_desc_,
                                                    savepoint,
                                                    expire_ts,
                                                    &ctx.need_rollback_ls_))) {
      LOG_WARN("fail to rollback sub tx", K(ret), K(ctx.tx_desc_),
               K(ctx.need_rollback_ls_));
    }

    ctx.clean_touched_ls();
    ctx.reset_savepoint();
  }
  LOG_DEBUG("ObTableLockService::end_sub_tx_", K(ret), K(ctx));

  return ret;
}

int ObTableLockService::start_stmt_(ObTableLockCtx &ctx)
{
  int ret = OB_SUCCESS;

  if (ctx.is_stmt_savepoint_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("start_stmt_ is executed", K(ret));
  } else {
    ObTransService *txs = MTL(ObTransService*);
    const ObTxParam &tx_param = ctx.tx_param_;
    const ObTxIsolationLevel &isolation_level = tx_param.isolation_;
    const int64_t expire_ts = ctx.abs_timeout_ts_;
    int64_t &savepoint = ctx.stmt_savepoint_;
    if (OB_FAIL(txs->create_implicit_savepoint(*ctx.tx_desc_,
                                               tx_param,
                                               savepoint))) {
      ctx.reset_stmt_savepoint();
      LOG_WARN("create implicit savepoint failed", K(ret), KPC(ctx.tx_desc_), K(tx_param));
    }
  }
  LOG_DEBUG("ObTableLockService::start_stmt_", K(ret), K(ctx));

  return ret;
}

int ObTableLockService::end_stmt_(ObTableLockCtx &ctx, const bool is_rollback)
{
  int ret = OB_SUCCESS;

  if (!ctx.is_stmt_savepoint_valid()) {
    LOG_INFO("end_stmt_ skip", K(ret), K(ctx));
  } else {
    const int64_t &savepoint = ctx.stmt_savepoint_;
    const int64_t expire_ts = OB_MAX(ctx.abs_timeout_ts_, DEFAULT_TIMEOUT_US + ObTimeUtility::current_time());
    ObTransService *txs = MTL(ObTransService*);
    // just rollback the whole stmt, if it is needed.
    if (is_rollback &&
        OB_FAIL(txs->rollback_to_implicit_savepoint(*ctx.tx_desc_,
                                                    savepoint,
                                                    expire_ts,
                                                    &ctx.need_rollback_ls_))) {
      LOG_WARN("fail to rollback stmt", K(ret), K(ctx.tx_desc_),
               K(ctx.need_rollback_ls_));
    }

    ctx.clean_touched_ls();
    ctx.reset_stmt_savepoint();
  }
  LOG_DEBUG("ObTableLockService::end_stmt_", K(ret), K(ctx));

  return ret;
}

} // tablelock
} // transaction
} // oceanbase
