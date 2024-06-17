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

#include "observer/mysql/ob_query_retry_ctrl.h"
#include "sql/ob_sql_context.h"
#include "sql/resolver/ob_stmt.h"
#include "pl/ob_pl.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/memtable/ob_lock_wait_mgr.h"
#include "observer/mysql/ob_mysql_result_set.h"
#include "observer/ob_server_struct.h"
#include "observer/mysql/obmp_query.h"

namespace oceanbase
{
using namespace common;
using namespace sql;
using namespace share::schema;
using namespace oceanbase::transaction;


namespace observer
{

common::hash::ObHashMap<int, ObQueryRetryCtrl::RetryFuncs, common::hash::NoPthreadDefendMode> ObQueryRetryCtrl::map_;

void ObRetryPolicy::try_packet_retry(ObRetryParam &v) const
{
  const ObMultiStmtItem &multi_stmt_item = v.ctx_.multi_stmt_item_;
  if (v.force_local_retry_) {
    v.retry_type_ = RETRY_TYPE_LOCAL;
  } else if (multi_stmt_item.is_batched_multi_stmt()) {
    // in batch optimization, can't do packet retry
    v.retry_type_ = RETRY_TYPE_LOCAL;
  } else if (multi_stmt_item.is_part_of_multi_stmt() && multi_stmt_item.get_seq_num() > 0) {
    // muti stmt，并且不是第一句，不能扔回队列重试，因为前面的无法回滚
    v.retry_type_ = RETRY_TYPE_LOCAL;
  } else if (!THIS_WORKER.can_retry()) {
    // false == THIS_WORKER.can_retry() means throw back to queue disabled by SOME logic
    v.retry_type_ = RETRY_TYPE_LOCAL;
  } else {
    v.retry_type_ = RETRY_TYPE_PACKET;
    THIS_WORKER.set_need_retry();
  }
}

void ObRetryPolicy::sleep_before_local_retry(ObRetryParam &v,
                                             RetrySleepType retry_sleep_type,
                                             int64_t base_sleep_us,
                                             int64_t timeout_timestamp) const
{
  int ret = OB_SUCCESS;
  int64_t sleep_us = 0;
  switch(retry_sleep_type) {
    case RETRY_SLEEP_TYPE_LINEAR: {
      sleep_us = base_sleep_us * linear_timeout_factor(v.stmt_retry_times_);
      break;
    }
    case RETRY_SLEEP_TYPE_INDEX: {
      sleep_us = base_sleep_us * index_timeout_factor(v.stmt_retry_times_);
      break;
    }
    case RETRY_SLEEP_TYPE_NONE: {
      sleep_us = 0;
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("unexpected retry sleep type", K(ret), K(base_sleep_us),
                K(retry_sleep_type), K(v.stmt_retry_times_), K(timeout_timestamp));
      break;
    }
  }
  if (RETRY_SLEEP_TYPE_NONE != retry_sleep_type && OB_SUCC(ret)) {
    int64_t remain_us = timeout_timestamp - ObTimeUtility::current_time();
    if (sleep_us > remain_us) {
      sleep_us = remain_us;
    }
    if (sleep_us > 0) {
      LOG_INFO("will sleep", K(sleep_us), K(remain_us), K(base_sleep_us),
               K(retry_sleep_type), K(v.stmt_retry_times_), K(v.err_), K(timeout_timestamp));
      THIS_WORKER.sched_wait();
      ob_usleep(static_cast<uint32_t>(sleep_us));
      THIS_WORKER.sched_run();
      if (THIS_WORKER.is_timeout()) {
        v.client_ret_ = OB_TIMEOUT;
        v.retry_type_ = RETRY_TYPE_NONE;
        v.no_more_test_ = true;
        LOG_WARN("this worker is timeout after retry sleep. no more retry", K(v));
      }
    } else {
      LOG_INFO("already timeout, do not need sleep", K(sleep_us), K(remain_us), K(base_sleep_us),
               K(retry_sleep_type), K(v.stmt_retry_times_), K(timeout_timestamp));
    }
  }
}

template<bool is_async>
class ObRefreshLocationCachePolicy : public ObRetryPolicy
{
public:
  ObRefreshLocationCachePolicy() = default;
  ~ObRefreshLocationCachePolicy() = default;
  virtual void test(ObRetryParam &v) const override
  {
    v.result_.force_refresh_location_cache(is_async, v.err_);
  }
};

typedef ObRefreshLocationCachePolicy<true> ObRefreshLocationCacheNonblockPolicy;
typedef ObRefreshLocationCachePolicy<false> ObRefreshLocationCacheBlockPolicy;


template<RetrySleepType SleepType, int64_t WaitUs>
class ObCommonRetryPolicy : public ObRetryPolicy
{
public:
  ObCommonRetryPolicy() = default;
  ~ObCommonRetryPolicy() = default;
  virtual void test(ObRetryParam &v) const override
  {
    try_packet_retry(v);
    if (RETRY_TYPE_LOCAL == v.retry_type_) {
      sleep_before_local_retry(v,
                               SleepType,
                               WaitUs,
                               THIS_WORKER.get_timeout_ts());
    }
  }
};

typedef ObCommonRetryPolicy<RETRY_SLEEP_TYPE_NONE, 0>                                    ObCommonRetryNoWaitPolicy;
typedef ObCommonRetryPolicy<RETRY_SLEEP_TYPE_LINEAR, ObRetryPolicy::WAIT_RETRY_LONG_US>  ObCommonRetryLinearLongWaitPolicy;
typedef ObCommonRetryPolicy<RETRY_SLEEP_TYPE_LINEAR, ObRetryPolicy::WAIT_RETRY_SHORT_US> ObCommonRetryLinearShortWaitPolicy;
typedef ObCommonRetryPolicy<RETRY_SLEEP_TYPE_INDEX, ObRetryPolicy::WAIT_RETRY_LONG_US>   ObCommonRetryIndexLongWaitPolicy;
typedef ObCommonRetryPolicy<RETRY_SLEEP_TYPE_INDEX, ObRetryPolicy::WAIT_RETRY_SHORT_US>  ObCommonRetryIndexShortWaitPolicy;


class ObFastFailRetryPolicy : public ObRetryPolicy
{
public:
  ObFastFailRetryPolicy() = default;
  ~ObFastFailRetryPolicy() = default;
  virtual void test(ObRetryParam &v) const override
  {
    if (v.session_.get_retry_info_for_update()
        .should_fast_fail(v.session_.get_effective_tenant_id())) {
      v.client_ret_ = v.err_;
      v.retry_type_ = RETRY_TYPE_NONE;
      v.no_more_test_ = true;
      LOG_WARN_RET(v.err_, "server down error, fast fail", K(v));
    }
  }
};

class ObForceLocalRetryPolicy : public ObRetryPolicy
{
public:
  ObForceLocalRetryPolicy() = default;
  ~ObForceLocalRetryPolicy() = default;
  virtual void test(ObRetryParam &v) const override
  {
    v.retry_type_ = RETRY_TYPE_LOCAL;
  }
};

class ObBatchExecOptRetryPolicy : public ObRetryPolicy
{
public:
  ObBatchExecOptRetryPolicy() = default;
  ~ObBatchExecOptRetryPolicy() = default;
  virtual void test(ObRetryParam &v) const override
  {
    if (v.ctx_.is_do_insert_batch_opt()) {
      v.retry_type_ = RETRY_TYPE_LOCAL;
    } else {
      v.retry_type_ = RETRY_TYPE_NONE;
    }
  }
};

class ObSwitchConsumerGroupRetryPolicy : public ObRetryPolicy
{
public:
  ObSwitchConsumerGroupRetryPolicy() = default;
  ~ObSwitchConsumerGroupRetryPolicy() = default;
  virtual void test(ObRetryParam &v) const override
  {
    try_packet_retry(v);
    if (RETRY_TYPE_LOCAL == v.retry_type_) {
      LOG_WARN_RET(v.err_, "set retry packet failed, retry at local",
        K(v.ctx_.multi_stmt_item_.is_part_of_multi_stmt()),
        K(v.ctx_.multi_stmt_item_.get_seq_num()));
      v.session_.set_group_id_not_expected(true);
      v.result_.get_exec_context().set_need_disconnect(false);
    }
  }
};

class ObBeforeRetryCheckPolicy : public ObRetryPolicy
{
public:
  ObBeforeRetryCheckPolicy() = default;
  ~ObBeforeRetryCheckPolicy() = default;
  virtual void test(ObRetryParam &v) const override
  {
    int ret = OB_SUCCESS;
    if (v.session_.is_terminate(ret)) {
      v.no_more_test_ = true;
      v.retry_type_ = RETRY_TYPE_NONE;
      // In the kill client session scenario, the server session will be marked
      // with the SESSION_KILLED mark. In the retry scenario, there will be an error
      // code covering 5066, so the judgment logic is added here.
      if (ret == OB_ERR_SESSION_INTERRUPTED && v.err_ == OB_ERR_KILL_CLIENT_SESSION) {
        v.client_ret_ = v.err_;
      } else{
        v.client_ret_ = ret; // session terminated
      }
      LOG_WARN("execution was terminated", K(ret), K(v.client_ret_), K(v.err_));
    } else if (THIS_WORKER.is_timeout()) {
      v.no_more_test_ = true;
      v.retry_type_ = RETRY_TYPE_NONE;
      if (OB_ERR_INSUFFICIENT_PX_WORKER == v.err_ ||
          OB_ERR_EXCLUSIVE_LOCK_CONFLICT == v.err_ ||
          OB_ERR_EXCLUSIVE_LOCK_CONFLICT_NOWAIT == v.err_ ||
          OB_ERR_QUERY_INTERRUPTED == v.err_) {
        v.client_ret_ = v.err_;
      } else if (is_try_lock_row_err(v.session_.get_retry_info().get_last_query_retry_err())) {
        // timeout caused by locking, should return OB_ERR_EXCLUSIVE_LOCK_CONFLICT
        v.client_ret_ = OB_ERR_EXCLUSIVE_LOCK_CONFLICT;
      } else {
        v.client_ret_ = OB_TIMEOUT;
      }
      LOG_WARN("this worker is timeout, do not need retry", K(v),
               K(THIS_WORKER.get_timeout_ts()), K(v.result_.get_stmt_type()),
               K(v.session_.get_retry_info().get_last_query_retry_err()));
      if (v.session_.get_retry_info().is_rpc_timeout() || is_transaction_rpc_timeout_err(v.err_)) {
        // rpc超时了，可能是location cache不对，异步刷新location cache
        v.result_.force_refresh_location_cache(true, v.err_); // 非阻塞
        LOG_WARN("sql rpc timeout, or trans rpc timeout, maybe location is changed, "
                 "refresh location cache non blockly", K(v),
                 K(v.session_.get_retry_info().is_rpc_timeout()));
      }
    }
  }
};

class ObStmtTypeRetryPolicy : public ObRetryPolicy
{
public:
  ObStmtTypeRetryPolicy() = default;
  ~ObStmtTypeRetryPolicy() = default;

  bool is_direct_load(ObRetryParam &v) const
  {
    ObExecContext &exec_ctx = v.result_.get_exec_context();
    return exec_ctx.get_table_direct_insert_ctx().get_is_direct();
  }

  bool is_load_local(ObRetryParam &v) const
  {
    bool bret = false;
    const ObICmd *cmd = v.result_.get_cmd();
    if (OB_NOT_NULL(cmd) && cmd->get_cmd_type() == stmt::T_LOAD_DATA) {
      const ObLoadDataStmt *load_data_stmt = static_cast<const ObLoadDataStmt *>(cmd);
      bret = load_data_stmt->get_load_arguments().load_file_storage_ == ObLoadFileLocation::CLIENT_DISK;
    }
    return bret;
  }

  virtual void test(ObRetryParam &v) const override
  {
    int err = v.err_;
    if (v.result_.is_pl_stmt(v.result_.get_stmt_type()) && !v.session_.get_pl_can_retry()) {
      LOG_WARN_RET(err, "current pl can not retry, commit may have occurred",
               K(v), K(v.result_.get_stmt_type()));
      v.client_ret_ = err;
      v.retry_type_ = RETRY_TYPE_NONE;
      v.no_more_test_ = true;
    } else if (ObStmt::force_skip_retry_stmt(v.result_.get_stmt_type())) {
      v.client_ret_ = err;
      v.retry_type_ = RETRY_TYPE_NONE;
      v.no_more_test_ = true;
    } else if (ObStmt::is_ddl_stmt(v.result_.get_stmt_type(), v.result_.has_global_variable())) {
      if (is_ddl_stmt_packet_retry_err(err)) {
        try_packet_retry(v);
      } else {
        v.client_ret_ = err;
        v.retry_type_ = RETRY_TYPE_NONE;
      }
      v.no_more_test_ = true;
    } else if (is_direct_load(v) && !is_load_local(v)) {
      if (is_direct_load_retry_err(err)) {
        try_packet_retry(v);
      } else {
        v.client_ret_ = err;
        v.retry_type_ = RETRY_TYPE_NONE;
      }
      v.no_more_test_ = true;
    } else if (stmt::T_LOAD_DATA == v.result_.get_stmt_type()) {
      v.client_ret_ = err;
      v.retry_type_ = RETRY_TYPE_NONE;
      v.no_more_test_ = true;
    }
  }
};


class ObCheckSchemaUpdatePolicy : public ObRetryPolicy
{
public:
  ObCheckSchemaUpdatePolicy() = default;
  ~ObCheckSchemaUpdatePolicy() = default;
  virtual void test(ObRetryParam &v) const override
  {
    int ret = OB_SUCCESS;
    // 设计讨论参考：
    if (NULL == GCTX.schema_service_) {
      v.client_ret_ = OB_INVALID_ARGUMENT;
      v.retry_type_ = RETRY_TYPE_NONE;
      v.no_more_test_ = true;
      LOG_WARN("invalid argument", K(v));
    } else {
      ObSchemaGetterGuard schema_guard;
      int64_t local_tenant_version_latest = 0;
      int64_t local_sys_version_latest = 0;
      if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                  v.session_.get_effective_tenant_id(), schema_guard))) {
        // 不需要重试了，同时让它返回get_schema_guard出错的错误码，因为是由它引起不重试的
        LOG_WARN("get schema guard failed", K(v), K(ret));
        v.client_ret_ = ret;
        v.retry_type_ = RETRY_TYPE_NONE;
        v.no_more_test_ = true;
      } else if (OB_FAIL(schema_guard.get_schema_version(
                  v.session_.get_effective_tenant_id(), local_tenant_version_latest))) {
        LOG_WARN("fail get tenant schema version", K(v), K(ret));
        v.client_ret_ = ret;
        v.retry_type_ = RETRY_TYPE_NONE;
        v.no_more_test_ = true;
      } else if (OB_FAIL(schema_guard.get_schema_version(
                  OB_SYS_TENANT_ID, local_sys_version_latest))) {
        LOG_WARN("fail get sys schema version", K(v), K(ret));
        v.client_ret_ = ret;
        v.retry_type_ = RETRY_TYPE_NONE;
        v.no_more_test_ = true;
      } else {
        bool local_schema_not_full = GCTX.schema_service_->is_schema_error_need_retry(
                                     &schema_guard, v.session_.get_effective_tenant_id());
        int64_t local_tenant_version_start = v.curr_query_tenant_local_schema_version_;
        int64_t global_tenant_version_start = v.curr_query_tenant_global_schema_version_;
        int64_t local_sys_version_start = v.curr_query_sys_local_schema_version_;
        int64_t global_sys_version_start = v.curr_query_sys_global_schema_version_;
        // (c1) 需要考虑远端机器的Schema比本地落后，远端机器抛出Schema错误的情景
        //      当远端抛出Schema错误的时候，强行将所有Schema错误转化成OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH
        //      权限不足也会触发该重试规则，因为远端schema刷新不及时可能误报权限不足，此时是需要重试的
        // (c4) 弱一致性读场景，会校验schema版本是否大于等于数据的schema版本，
        //      如果schema版本旧，则要求重试；
        //      目的是保证：始终采用新schema解析老数据
        // (c5) 梳理了OB_SCHEMA_EAGAIN使用的地方，主路径上出现了该错误码的地方需要触发SQL重试
        // (c2) 表存在或不存在/数据库存在或不存在/用户存在或不存在，并且local和global版本不等时重试
        // (c3) 其它任何sql开始执行时local version比当前local version小导致schema错误的情况
        // (c6) For local server, related tenant's schema maybe not refreshed yet when observer restarts or create tenant.
        // (c7) For remote server, related tenant's schema maybe not refreshed yet when observer restarts or create tenant.
        if ((OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH == v.err_) || // (c1)
            (OB_SCHEMA_NOT_UPTODATE == v.err_) || // (c4)
            (OB_SCHEMA_EAGAIN == v.err_) || // (c5)
            (global_tenant_version_start > local_tenant_version_start) || // (c2)
            (global_sys_version_start > local_sys_version_start) || // (c2)
            (local_tenant_version_latest > local_tenant_version_start) || // (c3)
            (local_sys_version_latest > local_sys_version_start) || // (c3)
            (local_schema_not_full) || // (c6)
            (OB_ERR_REMOTE_SCHEMA_NOT_FULL == v.err_) // (c7)
           ) {
          if (v.stmt_retry_times_ < ObQueryRetryCtrl::MAX_SCHEMA_ERROR_LOCAL_RETRY_TIMES) {
            v.retry_type_ = RETRY_TYPE_LOCAL;
          } else {
            try_packet_retry(v);
          }
          if (RETRY_TYPE_LOCAL == v.retry_type_) {
            // 线性重试响应更快
            sleep_before_local_retry(v,
                                     RETRY_SLEEP_TYPE_LINEAR,
                                     WAIT_RETRY_SHORT_US,
                                     THIS_WORKER.get_timeout_ts());
          }
        } else {
          // 这里的client_ret不好决定，让它依然返回err
          v.client_ret_ = v.err_;
          v.retry_type_ = RETRY_TYPE_NONE;
          v.no_more_test_ = true;
        }
      }
    }
  }
};

// if tenant status is abnormal, do not retry sql
class ObCheckTenantStatusPolicy : public ObRetryPolicy
{
public:
  ObCheckTenantStatusPolicy() = default;
  ~ObCheckTenantStatusPolicy() = default;
  virtual void test(ObRetryParam &v) const override
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(GCTX.schema_service_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_TRACE("invalid schema_service", KR(ret), K(v));
    } else {
      ObSchemaGetterGuard schema_guard;
      const ObTenantSchema *tenant_schema = NULL;
      if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
          OB_SYS_TENANT_ID,
          schema_guard))) {
        LOG_TRACE("get sys tenant schema guard failed", KR(ret), K(v));
      } else if (OB_FAIL(schema_guard.get_tenant_info(
          v.session_.get_effective_tenant_id(),
          tenant_schema))) {
        LOG_TRACE("fail get tenant info", KR(ret),
            "tenant_id", v.session_.get_effective_tenant_id(), K(v));
      } else if (OB_ISNULL(tenant_schema) || !tenant_schema->is_normal()) {
        // use LOG_TRACE to prevent too much warning during creating tenant
        LOG_TRACE("tenant status is abnormal, do not retry",
            "tenant_id", v.session_.get_effective_tenant_id(), KPC(tenant_schema), K(v));
        // tenant status is abnormal, do not retry and return v.err_
        v.client_ret_ = v.err_;
        v.retry_type_ = RETRY_TYPE_NONE;
        v.no_more_test_ = true;
      } else {
        // tenant status is normal, check passed
      }
    }
  }
};

class ObDMLPeerServerStateUncertainPolicy : public ObRetryPolicy
{
public:
  ObDMLPeerServerStateUncertainPolicy() = default;
  ~ObDMLPeerServerStateUncertainPolicy() = default;
  virtual void test(ObRetryParam &v) const override
  {
    if (OB_ISNULL(v.result_.get_physical_plan())) {
      // issue#43741246, plan not generated, won't be a remote trans
      // safe to continue with other retry test
    } else if (ObStmt::is_dml_write_stmt(v.result_.get_stmt_type())) {
      // bugfix:
      // bugfix:
      bool autocommit = v.session_.get_local_autocommit();
      ObPhyPlanType plan_type = v.result_.get_physical_plan()->get_plan_type();
      bool in_transaction = v.session_.is_in_transaction();
      if (ObSqlTransUtil::is_remote_trans(autocommit, in_transaction, plan_type)) {
        // 当前observer内部无法进行重试
        // err是OB_RPC_CONNECT_ERROR
        v.client_ret_ = v.err_;
        v.retry_type_ = RETRY_TYPE_NONE;
        v.no_more_test_ = true;
        LOG_WARN_RET(v.err_, "server down error, the write dml is remote, don't retry",
                 K(autocommit), K(plan_type), K(in_transaction), K(v));
      }
    }
  }
};


class ObLockRowConflictRetryPolicy : public ObRetryPolicy
{
public:
  ObLockRowConflictRetryPolicy() = default;
  ~ObLockRowConflictRetryPolicy() = default;
  virtual void test(ObRetryParam &v) const override
  {
    // sql which in pl will local retry first. see ObInnerSQLConnection::process_retry.
    // sql which not in pl use the same strategy to avoid never getting the lock.
    if (v.force_local_retry_ || (v.local_retry_times_ <= 1 && !v.result_.is_pl_stmt(v.result_.get_stmt_type()))) {
      v.retry_type_ = RETRY_TYPE_LOCAL;
    } else {
      const ObMultiStmtItem &multi_stmr_item = v.ctx_.multi_stmt_item_;
      try_packet_retry(v);
    }
  }
};


class ObTrxSetViolationRetryPolicy : public ObRetryPolicy
{
public:
  ObTrxSetViolationRetryPolicy() = default;
  ~ObTrxSetViolationRetryPolicy() = default;
  virtual void test(ObRetryParam &v) const override
  {
    if (ObQueryRetryCtrl::is_isolation_RR_or_SE(v.session_.get_tx_isolation())) {
      v.client_ret_ = OB_TRANS_CANNOT_SERIALIZE;
      v.retry_type_ = RETRY_TYPE_NONE;
      v.no_more_test_ = true;
      LOG_WARN_RET(v.client_ret_, "transaction cannot serialize", K(v));
    }
  }
};

class ObTrxCannotSerializeRetryPolicy : public ObRetryPolicy
{
public:
  ObTrxCannotSerializeRetryPolicy() = default;
  ~ObTrxCannotSerializeRetryPolicy() = default;
  virtual void test(ObRetryParam &v) const override
  {
    v.client_ret_ = OB_TRANS_CANNOT_SERIALIZE;
    v.retry_type_ = RETRY_TYPE_NONE;
    v.no_more_test_ = true;
    LOG_WARN_RET(v.client_ret_, "transaction cannot serialize", K(v));
  }
};

class ObPxThreadNotEnoughRetryPolicy : public ObRetryPolicy
{
public:
  ObPxThreadNotEnoughRetryPolicy() = default;
  ~ObPxThreadNotEnoughRetryPolicy() = default;
  virtual void test(ObRetryParam &v) const override
  {
    if (v.force_local_retry_) {
      v.retry_type_ = RETRY_TYPE_LOCAL;
    } else {
      try_packet_retry(v);
      if (RETRY_TYPE_LOCAL == v.retry_type_) {
        v.client_ret_ = v.err_;
        v.retry_type_ = RETRY_TYPE_NONE;
        v.no_more_test_ = true;
        LOG_WARN_RET(v.client_ret_, "can not retry local. need to terminate to prevent thread resouce deadlock", K(v));
      }
    }
  }
};

////////// special inner retry policy for inner connection ////////////
//
class ObInnerCommonCheckSchemaPolicy : public ObRetryPolicy
{
public:
  ObInnerCommonCheckSchemaPolicy() = default;
  ~ObInnerCommonCheckSchemaPolicy() = default;
  virtual void test(ObRetryParam &v) const override
  {
    int ret = OB_SUCCESS;
    bool local_schema_not_full = GSCHEMASERVICE.is_schema_error_need_retry(
                                 NULL, v.session_.get_effective_tenant_id());
    if (local_schema_not_full || OB_ERR_REMOTE_SCHEMA_NOT_FULL == v.err_) {
      v.no_more_test_ = true;
      v.retry_type_ = RETRY_TYPE_LOCAL;
      sleep_before_local_retry(v,
                          RETRY_SLEEP_TYPE_LINEAR,
                          WAIT_RETRY_SHORT_US,
                          THIS_WORKER.get_timeout_ts());
    }
  }
};

class ObInnerCheckSchemaPolicy : public ObRetryPolicy
{
public:
  ObInnerCheckSchemaPolicy() = default;
  ~ObInnerCheckSchemaPolicy() = default;
  virtual void test(ObRetryParam &v) const override
  {
    int ret = OB_SUCCESS;
    // As DDL is not reentranable in OceanBase, we have to retry those SQL issued by DDL in place
    // is_user_session=true: create table t1 as select ...
    // is_ddl=true: create index idx1 on ...
    if (v.session_.is_user_session() || v.session_.get_ddl_info().is_ddl()) {
      v.no_more_test_ = true;
      v.retry_type_ = RETRY_TYPE_LOCAL;
    } else {
      v.no_more_test_ = true;
      v.retry_type_ = RETRY_TYPE_NONE;
      v.client_ret_ = v.err_;
    }
  }
};

class ObInnerLockRowConflictRetryPolicy : public ObRetryPolicy
{
public:
  ObInnerLockRowConflictRetryPolicy() = default;
  ~ObInnerLockRowConflictRetryPolicy() = default;
  virtual void test(ObRetryParam &v) const override
  {
    // sql which in pl will local retry first. see ObInnerSQLConnection::process_retry.
    // sql which not in pl use the same strategy to avoid never getting the lock.
    if (v.is_from_pl_) {
      if (v.local_retry_times_ <= 1 ||
          !v.session_.get_pl_can_retry() ||
          ObSQLUtils::is_in_autonomous_block(v.session_.get_cur_exec_ctx())) {
        v.no_more_test_ = true;
        v.retry_type_ = RETRY_TYPE_LOCAL;
      } else {
        v.no_more_test_ = true;
        v.retry_type_ = RETRY_TYPE_NONE;
        v.client_ret_ = v.err_;
      }
    } else {
      // for DDL etc
      v.no_more_test_ = true;
      v.retry_type_ = RETRY_TYPE_LOCAL;
    }
  }
};

class ObInnerBeforeRetryCheckPolicy: public ObRetryPolicy
{
public:
  ObInnerBeforeRetryCheckPolicy() = default;
  ~ObInnerBeforeRetryCheckPolicy() = default;
  virtual void test(ObRetryParam &v) const override
  {
    int ret = OB_SUCCESS;
    if (v.session_.get_ddl_info().is_ddl() && !v.session_.get_ddl_info().is_retryable_ddl()) {
      v.client_ret_ = v.err_;
      v.retry_type_ = RETRY_TYPE_NONE;
      v.no_more_test_ = true;
    }
    // nested transaction already supported In 32x and can only rollback nested sql.
    // for forigen key, we keep old logic and do not retry. for pl will retry current nested sql.
    else if (is_nested_conn(v) && !is_static_engine_retry(v.err_) && !v.is_from_pl_) {
      // right now, top session will retry, bug we can do something here like refresh XXX cache.
      // in future, nested session can retry if nested transaction is supported.
      v.no_more_test_ = true;
      v.retry_type_ = RETRY_TYPE_NONE;
      v.client_ret_ = v.err_;
    } else if (v.session_.is_terminate(ret)) {
      v.no_more_test_ = true;
      v.retry_type_ = RETRY_TYPE_NONE;
      // In the kill client session scenario, the server session will be marked
      // with the SESSION_KILLED mark. In the retry scenario, there will be an error
      // code covering 5066, so the judgment logic is added here.
      if (ret == OB_ERR_SESSION_INTERRUPTED && v.err_ == OB_ERR_KILL_CLIENT_SESSION) {
        v.client_ret_ = v.err_;
      } else{
        v.client_ret_ = ret; // session terminated
      }
      LOG_WARN("execution was terminated", K(ret), K(v.client_ret_), K(v.err_));
    } else if (THIS_WORKER.is_timeout()) {
      v.no_more_test_ = true;
      v.retry_type_ = RETRY_TYPE_NONE;
      v.client_ret_ = OB_TIMEOUT;
    }
  }
private:
  //is_nested_conn means this connection is triggered by a foreign key or PL object
  //because session and ObExecContext are linked in SQL engine,
  //in the inner sql connection stage,
  //the ObExecContext belonging to the current inner connection has not been linked to session.
  //for nested SQL, the ObExecContext on the current session belongs to the parent statement
  //so session.cur_exec_ctx_ is the parent ctx of the nested SQL
  bool is_nested_conn(ObRetryParam &v) const
  {
    ObExecContext *parent_ctx = v.session_.get_cur_exec_ctx();
    bool is_pl_nested = (parent_ctx != nullptr
                         && ObStmt::is_dml_stmt(parent_ctx->get_sql_ctx()->stmt_type_)
                         && parent_ctx->get_pl_stack_ctx() != nullptr
                         && !parent_ctx->get_pl_stack_ctx()->in_autonomous());
    bool is_fk_nested = (parent_ctx != nullptr && parent_ctx->get_das_ctx().is_fk_cascading_);
    bool is_online_stat_gathering_nested = (parent_ctx != nullptr && parent_ctx->is_online_stats_gathering());
    return is_pl_nested || is_fk_nested || is_online_stat_gathering_nested;
  }
};

class ObAutoincCacheNotEqualRetryPolicy: public ObRetryPolicy
{
public:
  ObAutoincCacheNotEqualRetryPolicy() = default;
  ~ObAutoincCacheNotEqualRetryPolicy() = default;
  virtual void test(ObRetryParam &v) const override
  {
    if (v.stmt_retry_times_ < ObQueryRetryCtrl::MAX_SCHEMA_ERROR_LOCAL_RETRY_TIMES) {
      v.retry_type_ = RETRY_TYPE_LOCAL;
    } else {
      try_packet_retry(v);
    }
  }
};


////////// end of policies ////////////



void ObQueryRetryCtrl::px_thread_not_enough_proc(ObRetryParam &v)
{
  ObRetryObject retry_obj(v);
  ObPxThreadNotEnoughRetryPolicy thread_not_enough;
  retry_obj.test(thread_not_enough);
}

void ObQueryRetryCtrl::trx_set_violation_proc(ObRetryParam &v)
{
  ObRetryObject retry_obj(v);
  ObTrxSetViolationRetryPolicy trx_violation;
  ObCommonRetryLinearShortWaitPolicy retry_short_wait;
  retry_obj.test(trx_violation).test(retry_short_wait);
}

void ObQueryRetryCtrl::trx_can_not_serialize_proc(ObRetryParam &v)
{
  ObRetryObject retry_obj(v);
  ObTrxCannotSerializeRetryPolicy trx_cannot_serialize;
  retry_obj.test(trx_cannot_serialize);
}

void ObQueryRetryCtrl::try_lock_row_conflict_proc(ObRetryParam &v)
{
  ObRetryObject retry_obj(v);
  ObLockRowConflictRetryPolicy lock_conflict;
  retry_obj.test(lock_conflict);
}


void ObQueryRetryCtrl::location_error_proc(ObRetryParam &v)
{
  ObRetryObject retry_obj(v);
  ObFastFailRetryPolicy fast_fail;
  ObCommonRetryIndexLongWaitPolicy retry_long_wait;
  retry_obj.test(fast_fail).test(retry_long_wait);

  if (RETRY_TYPE_LOCAL == v.retry_type_) {
    ObRefreshLocationCacheBlockPolicy block_refresh; // FIXME: why block?
    retry_obj.test(block_refresh);
  } else {
    ObRefreshLocationCacheNonblockPolicy nonblock_refresh;
    retry_obj.test(nonblock_refresh);
  }
}

void ObQueryRetryCtrl::nonblock_location_error_proc(ObRetryParam &v)
{
  ObRetryObject retry_obj(v);
  ObFastFailRetryPolicy fast_fail;
  ObCommonRetryIndexLongWaitPolicy retry_long_wait;
  ObRefreshLocationCacheNonblockPolicy nonblock_refresh;
  retry_obj.test(fast_fail).test(retry_long_wait).test(nonblock_refresh);
}

void ObQueryRetryCtrl::location_error_nothing_readable_proc(ObRetryParam &v)
{
  // 强一致性读的情况，主不可读了，有可能是invalid servers将主过滤掉了。
  // 弱一致性读的情况，没有副本可以选择了，有可能是invalid servers将所有副本都过滤掉了。
  // 为了更好地处理主短暂地断网的情况，将retry info清空（主要是invalid servers清空，
  // 但是还是要保持inited的状态以便通过防御性检查，所以不能调reset，而是要调clear），然后再重试。
  v.session_.get_retry_info_for_update().clear();
  location_error_proc(v);
}

void ObQueryRetryCtrl::peer_server_status_uncertain_proc(ObRetryParam &v)
{
  ObRetryObject retry_obj(v);
  ObFastFailRetryPolicy fast_fail;
  ObRefreshLocationCacheNonblockPolicy nonblock_refresh;
  ObDMLPeerServerStateUncertainPolicy check_dml; // will abort check if dml has remote trans
  ObCommonRetryIndexLongWaitPolicy retry_long_wait;
  retry_obj.test(fast_fail).test(nonblock_refresh).test(check_dml).test(retry_long_wait);
}

void ObQueryRetryCtrl::schema_error_proc(ObRetryParam &v)
{
  ObRetryObject retry_obj(v);
  ObCheckSchemaUpdatePolicy schema_update_policy;
  retry_obj.test(schema_update_policy);
}

void ObQueryRetryCtrl::autoinc_cache_not_equal_retry_proc(ObRetryParam &v)
{
  ObRetryObject retry_obj(v);
  ObAutoincCacheNotEqualRetryPolicy autoinc_retry_policy;
  ObCommonRetryLinearShortWaitPolicy retry_short_wait;
  retry_obj.test(autoinc_retry_policy).test(retry_short_wait);
}

void ObQueryRetryCtrl::snapshot_discard_proc(ObRetryParam &v)
{
  if (ObQueryRetryCtrl::is_isolation_RR_or_SE(v.session_.get_tx_isolation())) {
    // see:
    v.client_ret_ = v.err_;
    v.retry_type_ = RETRY_TYPE_NONE;
    LOG_WARN_RET(v.client_ret_, "snapshot discarded in serializable isolation should not retry", K(v));
  } else {
    // 读到落后太多的备机或者正在回放日志的副本了
    // 副本不可读类型的错误最多在本线程重试1次。
    const int64_t MAX_DATA_NOT_READABLE_ERROR_LOCAL_RETRY_TIMES = 1;
    if (v.stmt_retry_times_ < MAX_DATA_NOT_READABLE_ERROR_LOCAL_RETRY_TIMES) {
      v.retry_type_ = RETRY_TYPE_LOCAL;
    } else {
      ObRetryObject retry_obj(v);
      ObCommonRetryNoWaitPolicy no_wait_retry;
      retry_obj.test(no_wait_retry);
    }
  }
}

void ObQueryRetryCtrl::long_wait_retry_proc(ObRetryParam &v)
{
  ObRetryObject retry_obj(v);
  ObCommonRetryIndexLongWaitPolicy long_wait_retry;
  retry_obj.test(long_wait_retry);
}

void ObQueryRetryCtrl::short_wait_retry_proc(ObRetryParam &v)
{
  ObRetryObject retry_obj(v);
  ObCommonRetryLinearShortWaitPolicy short_wait_retry;
  retry_obj.test(short_wait_retry);
}

void ObQueryRetryCtrl::force_local_retry_proc(ObRetryParam &v)
{
  ObRetryObject retry_obj(v);
  ObForceLocalRetryPolicy force_local_retry;
  retry_obj.test(force_local_retry);
}

void ObQueryRetryCtrl::batch_execute_opt_retry_proc(ObRetryParam &v)
{
  ObRetryObject retry_obj(v);
  ObBatchExecOptRetryPolicy batch_opt_retry;
  retry_obj.test(batch_opt_retry);
}

void ObQueryRetryCtrl::switch_consumer_group_retry_proc(ObRetryParam &v)
{
  ObRetryObject retry_obj(v);
  ObSwitchConsumerGroupRetryPolicy switch_group_retry;
  retry_obj.test(switch_group_retry);
}

void ObQueryRetryCtrl::timeout_proc(ObRetryParam &v)
{
#ifdef OB_BUILD_SPM
  if (OB_UNLIKELY(v.err_ == OB_TIMEOUT &&
                  ObSpmCacheCtx::STAT_FIRST_EXECUTE_PLAN == v.ctx_.spm_ctx_.spm_stat_ &&
                  v.ctx_.spm_ctx_.need_spm_timeout_)) {
    const_cast<ObSqlCtx &>(v.ctx_).spm_ctx_.spm_stat_ = ObSpmCacheCtx::STAT_FALLBACK_EXECUTE_PLAN;
    const_cast<ObSqlCtx &>(v.ctx_).spm_ctx_.need_spm_timeout_ = false;
    ObRetryObject retry_obj(v);
    ObForceLocalRetryPolicy force_local_retry;
    retry_obj.test(force_local_retry);
  } else if (is_try_lock_row_err(v.session_.get_retry_info().get_last_query_retry_err())) {
    v.client_ret_ = OB_ERR_EXCLUSIVE_LOCK_CONFLICT;
    v.retry_type_ = RETRY_TYPE_NONE;
  }
#else
  if (is_try_lock_row_err(v.session_.get_retry_info().get_last_query_retry_err())) {
    v.client_ret_ = OB_ERR_EXCLUSIVE_LOCK_CONFLICT;
    v.retry_type_ = RETRY_TYPE_NONE;
  }
#endif
}

/////// For inner SQL only ///////////////
void ObQueryRetryCtrl::inner_try_lock_row_conflict_proc(ObRetryParam &v)
{
  ObRetryObject retry_obj(v);
  ObInnerLockRowConflictRetryPolicy lock_conflict;
  retry_obj.test(lock_conflict);
}

void ObQueryRetryCtrl::inner_table_location_error_proc(ObRetryParam &v)
{
  // 这种情况一般是内部sql执行的时候获取不到location，可能是宕机，
  // 这里涉及到的是内部表，刷新本sql查询的表的location cache没有意义，因此不刷新。
  ObRetryObject retry_obj(v);
  ObCommonRetryIndexLongWaitPolicy retry_long_wait;
  retry_obj.test(retry_long_wait);
}

void ObQueryRetryCtrl::inner_location_error_proc(ObRetryParam &v)
{
  const uint64_t *trace_id = ObCurTraceId::get();
  bool sql_trigger_by_user_req = (NULL != trace_id && 0 != trace_id[0] && 0 != trace_id[1]);
  ObRetryObject retry_obj(v);
  ObCheckTenantStatusPolicy check_tenant;
  ObRefreshLocationCacheBlockPolicy block_refresh;
  retry_obj.test(check_tenant);
  if (true == v.no_more_test_) {
    // case1: tenant status is abnormal, do not retry
  } else if (v.session_.get_ddl_info().is_ddl()) {
    // case2: inner sql ddl need retry (add by shuangcan.yjw)
    ObCommonRetryIndexLongWaitPolicy retry_long_wait;
    retry_obj.test(retry_long_wait).test(block_refresh);
  } else if (sql_trigger_by_user_req) {
    // case3: sql trigger by user request, e.g. PL
    ObFastFailRetryPolicy fast_fail; // only enable fast fail for user triggered req
    ObCommonRetryLinearShortWaitPolicy short_wait_retry;
    retry_obj.test(fast_fail).test(short_wait_retry).test(block_refresh);
  } else {
    // case 4: do nothing for other inner sql
    empty_proc(v);
  }
}

void ObQueryRetryCtrl::inner_location_error_nothing_readable_proc(ObRetryParam &v)
{
  // 强一致性读的情况，主不可读了，有可能是invalid servers将主过滤掉了。
  // 弱一致性读的情况，没有副本可以选择了，有可能是invalid servers将所有副本都过滤掉了。
  // 为了更好地处理主短暂地断网的情况，将retry info清空（主要是invalid servers清空，
  // 但是还是要保持inited的状态以便通过防御性检查，所以不能调reset，而是要调clear），然后再重试。
  v.session_.get_retry_info_for_update().clear();
  inner_location_error_proc(v);
}

void ObQueryRetryCtrl::inner_common_schema_error_proc(ObRetryParam &v)
{
  ObRetryObject retry_obj(v);
  ObInnerCommonCheckSchemaPolicy common_schema_policy;
  retry_obj.test(common_schema_policy);
}


void ObQueryRetryCtrl::inner_schema_error_proc(ObRetryParam &v)
{
  ObRetryObject retry_obj(v);
  ObInnerCommonCheckSchemaPolicy common_schema_policy;
  ObInnerCheckSchemaPolicy schema_policy;
  retry_obj.test(common_schema_policy).test(schema_policy);
}

void ObQueryRetryCtrl::inner_peer_server_status_uncertain_proc(ObRetryParam &v)
{
  ObRetryObject retry_obj(v);
  if (v.session_.get_ddl_info().is_ddl()) {
    ObFastFailRetryPolicy fast_fail;
    ObCommonRetryLinearShortWaitPolicy short_wait_retry;
    retry_obj.test(fast_fail).test(short_wait_retry);
  } else {
    empty_proc(v);
  }
}

/////// system defined common func /////////////

void ObQueryRetryCtrl::empty_proc(ObRetryParam &v)
{
  // 根据"给用户返回导致不重试的最后一个错误码"的原则，
  // 这里是err不在重试错误码列表中的情况，需要将client_ret设置为相应的值
  v.client_ret_ = v.err_;
  v.retry_type_ = RETRY_TYPE_NONE;
  if (OB_ERR_PROXY_REROUTE != v.client_ret_) {
    LOG_DEBUG("no retry handler for this err code, no need retry", K(v),
             K(THIS_WORKER.get_timeout_ts()), K(v.result_.get_stmt_type()),
             K(v.session_.get_retry_info().get_last_query_retry_err()));
  }
}

void ObQueryRetryCtrl::before_func(ObRetryParam &v)
{
  if (OB_UNLIKELY(v.is_inner_sql_)) {
    ObRetryObject retry_obj(v);
    ObInnerBeforeRetryCheckPolicy before_retry;
    retry_obj.test(before_retry);
  } else {
    ObRetryObject retry_obj(v);
    ObBeforeRetryCheckPolicy before_retry;
    ObStmtTypeRetryPolicy check_stmt_type;
    retry_obj.test(before_retry).test(check_stmt_type);
  }
}

void ObQueryRetryCtrl::after_func(ObRetryParam &v)
{
  if (OB_TRY_LOCK_ROW_CONFLICT == v.client_ret_
        || OB_ERR_PROXY_REROUTE == v.client_ret_
        || (v.is_from_pl_ && OB_READ_NOTHING == v.client_ret_)) {
    //锁冲突不打印了，避免日志刷屏
    // 二次路由不打印
    // PL 里面的 OB_READ_NOTHING 不打印日志
  } else {
    LOG_WARN_RET(v.client_ret_, "[RETRY] check if need retry", K(v), "need_retry", RETRY_TYPE_NONE != v.retry_type_);
  }
  if (RETRY_TYPE_NONE != v.retry_type_) {
    v.session_.get_retry_info_for_update().set_last_query_retry_err(v.err_);
    v.session_.get_retry_info_for_update().inc_retry_cnt();
    if (OB_UNLIKELY(v.err_ != v.client_ret_)) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "when need retry, v.client_ret_ must be equal to err", K(v));
    }
  }
  if (OB_UNLIKELY(OB_SUCCESS == v.client_ret_)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "no matter need retry or not, v.client_ret_ should not be OB_SUCCESS", K(v));
  }
  // bug fix, reset lock_wait_mgr node before doing local retry
  if (RETRY_TYPE_LOCAL == v.retry_type_) {
    rpc::ObLockWaitNode* node = MTL(memtable::ObLockWaitMgr*)->get_thread_node();
    if (NULL != node) {
      node->reset_need_wait();
    }
  }
}

int ObQueryRetryCtrl::init()
{
  int ret = OB_SUCCESS;

  OX(map_.create(8192, "RetryCtrl", "RetryCtrl"));

  // Macro parameters:
  //  tag: unused, just for organization
  //  r: error code
  //  func: processor for obmp* query
  //  inner_func: processor for inner connection query
  //  das_func: processor for DAS task retry
#ifndef ERR_RETRY_FUNC
#define ERR_RETRY_FUNC(tag, r, func, inner_func, das_func) \
  if (OB_SUCC(ret)) { \
    if (OB_SUCCESS != (ret = map_.set_refactored(r, RetryFuncs(func, inner_func, das_func)))) { \
      LOG_ERROR("Duplicated error code registered", "code", #r, KR(ret)); \
    } \
  }
#endif

  // register your error code retry handler here, no order required
  /* schema */
  ERR_RETRY_FUNC("SCHEMA",   OB_SCHEMA_ERROR,                    schema_error_proc,          empty_proc,                                           nullptr);
  ERR_RETRY_FUNC("SCHEMA",   OB_TENANT_EXIST,                    schema_error_proc,          empty_proc,                                           nullptr);
  ERR_RETRY_FUNC("SCHEMA",   OB_TENANT_NOT_EXIST,                schema_error_proc,          empty_proc,                                           nullptr);
  ERR_RETRY_FUNC("SCHEMA",   OB_ERR_BAD_DATABASE,                schema_error_proc,          empty_proc,                                           nullptr);
  ERR_RETRY_FUNC("SCHEMA",   OB_DATABASE_EXIST,                  schema_error_proc,          empty_proc,                                           nullptr);
  ERR_RETRY_FUNC("SCHEMA",   OB_TABLEGROUP_NOT_EXIST,            schema_error_proc,          empty_proc,                                           nullptr);
  ERR_RETRY_FUNC("SCHEMA",   OB_TABLEGROUP_EXIST,                schema_error_proc,          empty_proc,                                           nullptr);
  ERR_RETRY_FUNC("SCHEMA",   OB_TABLE_NOT_EXIST,                 schema_error_proc,          inner_common_schema_error_proc,                       nullptr);
  ERR_RETRY_FUNC("SCHEMA",   OB_ERR_TABLE_EXIST,                 schema_error_proc,          empty_proc,                                           nullptr);
  ERR_RETRY_FUNC("SCHEMA",   OB_ERR_BAD_FIELD_ERROR,             schema_error_proc,          empty_proc,                                           nullptr);
  ERR_RETRY_FUNC("SCHEMA",   OB_ERR_COLUMN_DUPLICATE,            schema_error_proc,          empty_proc,                                           nullptr);
  ERR_RETRY_FUNC("SCHEMA",   OB_ERR_USER_EXIST,                  schema_error_proc,          empty_proc,                                           nullptr);
  ERR_RETRY_FUNC("SCHEMA",   OB_ERR_USER_NOT_EXIST,              schema_error_proc,          empty_proc,                                           nullptr);
  ERR_RETRY_FUNC("SCHEMA",   OB_ERR_NO_PRIVILEGE,                schema_error_proc,          empty_proc,                                           nullptr);
  ERR_RETRY_FUNC("SCHEMA",   OB_ERR_NO_DB_PRIVILEGE,             schema_error_proc,          empty_proc,                                           nullptr);
  ERR_RETRY_FUNC("SCHEMA",   OB_ERR_NO_TABLE_PRIVILEGE,          schema_error_proc,          empty_proc,                                           nullptr);
  ERR_RETRY_FUNC("SCHEMA",   OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH,  schema_error_proc,          inner_schema_error_proc,                              nullptr);
  ERR_RETRY_FUNC("SCHEMA",   OB_ERR_REMOTE_SCHEMA_NOT_FULL,      schema_error_proc,          inner_schema_error_proc,                              nullptr);
  ERR_RETRY_FUNC("SCHEMA",   OB_ERR_SP_ALREADY_EXISTS,           schema_error_proc,          empty_proc,                                           nullptr);
  ERR_RETRY_FUNC("SCHEMA",   OB_ERR_SP_DOES_NOT_EXIST,           schema_error_proc,          empty_proc,                                           nullptr);
  ERR_RETRY_FUNC("SCHEMA",   OB_OBJECT_NAME_NOT_EXIST,           schema_error_proc,          empty_proc,                                           nullptr);
  ERR_RETRY_FUNC("SCHEMA",   OB_OBJECT_NAME_EXIST,               schema_error_proc,          empty_proc,                                           nullptr);
  ERR_RETRY_FUNC("SCHEMA",   OB_SCHEMA_EAGAIN,                   schema_error_proc,          inner_schema_error_proc,                              nullptr);
  ERR_RETRY_FUNC("SCHEMA",   OB_SCHEMA_NOT_UPTODATE,             schema_error_proc,          inner_schema_error_proc,                              nullptr);
  ERR_RETRY_FUNC("SCHEMA",   OB_ERR_PARALLEL_DDL_CONFLICT,       schema_error_proc,          inner_schema_error_proc,                              nullptr);
  ERR_RETRY_FUNC("SCHEMA",   OB_AUTOINC_CACHE_NOT_EQUAL,         autoinc_cache_not_equal_retry_proc, autoinc_cache_not_equal_retry_proc, nullptr);

  /* location */
  ERR_RETRY_FUNC("LOCATION", OB_LOCATION_LEADER_NOT_EXIST,       location_error_nothing_readable_proc, inner_location_error_nothing_readable_proc, ObDASRetryCtrl::tablet_location_retry_proc);
  ERR_RETRY_FUNC("LOCATION", OB_LS_LOCATION_LEADER_NOT_EXIST,    location_error_nothing_readable_proc, inner_location_error_nothing_readable_proc, ObDASRetryCtrl::tablet_location_retry_proc);
  ERR_RETRY_FUNC("LOCATION", OB_NO_READABLE_REPLICA,             location_error_nothing_readable_proc, inner_location_error_nothing_readable_proc, ObDASRetryCtrl::tablet_location_retry_proc);
  ERR_RETRY_FUNC("LOCATION", OB_NOT_MASTER,                      location_error_proc,        inner_location_error_proc,                            ObDASRetryCtrl::tablet_location_retry_proc);
  ERR_RETRY_FUNC("LOCATION", OB_RS_NOT_MASTER,                   location_error_proc,        inner_location_error_proc,                            ObDASRetryCtrl::tablet_location_retry_proc);
  ERR_RETRY_FUNC("LOCATION", OB_RS_SHUTDOWN,                     location_error_proc,        inner_location_error_proc,                            ObDASRetryCtrl::tablet_location_retry_proc);
  ERR_RETRY_FUNC("LOCATION", OB_PARTITION_NOT_EXIST,             location_error_proc,        inner_location_error_proc,                            ObDASRetryCtrl::tablet_location_retry_proc);
  ERR_RETRY_FUNC("LOCATION", OB_LOCATION_NOT_EXIST,              location_error_proc,        inner_location_error_proc,                            ObDASRetryCtrl::tablet_location_retry_proc);
  ERR_RETRY_FUNC("LOCATION", OB_PARTITION_IS_STOPPED,            location_error_proc,        inner_location_error_proc,                            ObDASRetryCtrl::tablet_location_retry_proc);
  ERR_RETRY_FUNC("LOCATION", OB_SERVER_IS_INIT,                  location_error_proc,        inner_location_error_proc,                            ObDASRetryCtrl::tablet_location_retry_proc);
  ERR_RETRY_FUNC("LOCATION", OB_SERVER_IS_STOPPING,              location_error_proc,        inner_location_error_proc,                            ObDASRetryCtrl::tablet_location_retry_proc);
  ERR_RETRY_FUNC("LOCATION", OB_TENANT_NOT_IN_SERVER,            location_error_proc,        inner_location_error_proc,                            ObDASRetryCtrl::tablet_location_retry_proc);
  ERR_RETRY_FUNC("LOCATION", OB_TRANS_RPC_TIMEOUT,               location_error_proc,        inner_location_error_proc,                            nullptr);
  ERR_RETRY_FUNC("LOCATION", OB_USE_DUP_FOLLOW_AFTER_DML,        location_error_proc,        inner_location_error_proc,                            ObDASRetryCtrl::tablet_location_retry_proc);
  ERR_RETRY_FUNC("LOCATION", OB_TRANS_STMT_NEED_RETRY,           location_error_proc,        inner_location_error_proc,                            nullptr);
  ERR_RETRY_FUNC("LOCATION", OB_LS_NOT_EXIST,                    location_error_proc,        inner_location_error_proc,                            ObDASRetryCtrl::tablet_location_retry_proc);
  // OB_TABLET_NOT_EXIST may be caused by old version schema or incorrect location.
  // Just use location_error_proc to retry sql and a new schema guard will be obtained during the retry process.
  ERR_RETRY_FUNC("LOCATION", OB_TABLET_NOT_EXIST,                location_error_proc,        inner_location_error_proc,                            ObDASRetryCtrl::tablet_location_retry_proc);
  ERR_RETRY_FUNC("LOCATION", OB_LS_LOCATION_NOT_EXIST,           location_error_proc,        inner_location_error_proc,                            ObDASRetryCtrl::tablet_location_retry_proc);
  ERR_RETRY_FUNC("LOCATION", OB_PARTITION_IS_BLOCKED,            location_error_proc,        inner_location_error_proc,                            ObDASRetryCtrl::tablet_location_retry_proc);
  ERR_RETRY_FUNC("LOCATION", OB_MAPPING_BETWEEN_TABLET_AND_LS_NOT_EXIST, location_error_proc,inner_location_error_proc,                            ObDASRetryCtrl::tablet_location_retry_proc);

  ERR_RETRY_FUNC("LOCATION", OB_GET_LOCATION_TIME_OUT,           location_error_proc,        inner_table_location_error_proc,                      ObDASRetryCtrl::tablet_location_retry_proc);



  /* network */
  ERR_RETRY_FUNC("NETWORK",  OB_RPC_CONNECT_ERROR,               peer_server_status_uncertain_proc, inner_peer_server_status_uncertain_proc,       ObDASRetryCtrl::task_network_retry_proc);
  ERR_RETRY_FUNC("NETWORK",  OB_RPC_SEND_ERROR,                  peer_server_status_uncertain_proc, inner_peer_server_status_uncertain_proc,       ObDASRetryCtrl::task_network_retry_proc);
  ERR_RETRY_FUNC("NETWORK",  OB_RPC_POST_ERROR,                  peer_server_status_uncertain_proc, inner_peer_server_status_uncertain_proc,       ObDASRetryCtrl::task_network_retry_proc);

  /* storage */
  ERR_RETRY_FUNC("STORAGE",  OB_SNAPSHOT_DISCARDED,              snapshot_discard_proc,         short_wait_retry_proc,                             nullptr);
  ERR_RETRY_FUNC("STORAGE",  OB_DATA_NOT_UPTODATE,               long_wait_retry_proc,          short_wait_retry_proc,                             nullptr);
  ERR_RETRY_FUNC("STORAGE",  OB_REPLICA_NOT_READABLE,            long_wait_retry_proc,          short_wait_retry_proc,                             ObDASRetryCtrl::tablet_nothing_readable_proc);
  ERR_RETRY_FUNC("STORAGE",  OB_PARTITION_IS_SPLITTING,          short_wait_retry_proc,         short_wait_retry_proc,                             nullptr);
  ERR_RETRY_FUNC("STORAGE",  OB_DISK_HUNG,                       nonblock_location_error_proc,  empty_proc,                                        nullptr);

  /* trx */
  ERR_RETRY_FUNC("TRX",      OB_TRY_LOCK_ROW_CONFLICT,           try_lock_row_conflict_proc, inner_try_lock_row_conflict_proc,                     nullptr);
  ERR_RETRY_FUNC("TRX",      OB_TRANSACTION_SET_VIOLATION,       trx_set_violation_proc,     trx_set_violation_proc,                               nullptr);
  ERR_RETRY_FUNC("TRX",      OB_TRANS_CANNOT_SERIALIZE,          trx_can_not_serialize_proc, trx_can_not_serialize_proc,                           nullptr);
  ERR_RETRY_FUNC("TRX",      OB_GTS_NOT_READY,                   short_wait_retry_proc,      short_wait_retry_proc,                                nullptr);
  ERR_RETRY_FUNC("TRX",      OB_GTI_NOT_READY,                   short_wait_retry_proc,      short_wait_retry_proc,                                nullptr);
  ERR_RETRY_FUNC("TRX",      OB_TRANS_WEAK_READ_VERSION_NOT_READY, short_wait_retry_proc,    short_wait_retry_proc,                                nullptr);
  ERR_RETRY_FUNC("TRX",      OB_SEQ_NO_REORDER_UNDER_PDML,       short_wait_retry_proc,      short_wait_retry_proc,                                nullptr);

  /* sql */
  ERR_RETRY_FUNC("SQL",      OB_ERR_INSUFFICIENT_PX_WORKER,      px_thread_not_enough_proc,  short_wait_retry_proc,                                nullptr);
  // create a new interval part when inserting a row which has no matched part,
  // wait and retry, will see new part
  ERR_RETRY_FUNC("SQL",      OB_NO_PARTITION_FOR_INTERVAL_PART,  short_wait_retry_proc,             short_wait_retry_proc,                         nullptr);
  ERR_RETRY_FUNC("SQL",      OB_BATCHED_MULTI_STMT_ROLLBACK,     batch_execute_opt_retry_proc,      batch_execute_opt_retry_proc,                  nullptr);
  ERR_RETRY_FUNC("SQL",      OB_SQL_RETRY_SPM,                   force_local_retry_proc,            force_local_retry_proc,                        nullptr);
  ERR_RETRY_FUNC("SQL",      OB_NEED_SWITCH_CONSUMER_GROUP,      switch_consumer_group_retry_proc,  empty_proc,                                    nullptr);

  /* timeout */
  ERR_RETRY_FUNC("SQL",      OB_TIMEOUT,                         timeout_proc,                timeout_proc,                                        nullptr);
  ERR_RETRY_FUNC("SQL",      OB_TRANS_TIMEOUT,                   timeout_proc,                timeout_proc,                                        nullptr);
  ERR_RETRY_FUNC("SQL",      OB_TRANS_STMT_TIMEOUT,              timeout_proc,                timeout_proc,                                        nullptr);

  /* ddl */


#undef ERR_RETRY_FUNC
  return ret;
}

void ObQueryRetryCtrl::destroy()
{
  // don't want to add a lock here
  // must ensure calling destroy after all threads exit
  map_.destroy();
}

ObQueryRetryCtrl::ObQueryRetryCtrl()
  : curr_query_tenant_local_schema_version_(0),
    curr_query_tenant_global_schema_version_(0),
    curr_query_sys_local_schema_version_(0),
    curr_query_sys_global_schema_version_(0),
    retry_times_(0),
    retry_type_(RETRY_TYPE_NONE),
    retry_err_code_(OB_SUCCESS)
{
}

ObQueryRetryCtrl::~ObQueryRetryCtrl()
{
}

int ObQueryRetryCtrl::get_das_retry_func(int err, ObDASRetryCtrl::retry_func &retry_func)
{
  int ret = OB_SUCCESS;
  retry_func = nullptr;
  RetryFuncs funcs;
  if (OB_FAIL(map_.get_refactored(err, funcs))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
  } else {
    retry_func = funcs.element<2>();
  }
  return ret;
}

int ObQueryRetryCtrl::get_func(int err, bool is_inner_sql, retry_func &func)
{
  int ret = OB_SUCCESS;
  RetryFuncs funcs;
  if (OB_FAIL(map_.get_refactored(err, funcs))) {
    if (OB_HASH_NOT_EXIST == ret) {
      func = empty_proc;
      ret = OB_SUCCESS;
    }
  } else {
    func = is_inner_sql ? funcs.element<1>() : funcs.element<0>();
  }
  return ret;
}

void ObQueryRetryCtrl::test_and_save_retry_state(const ObGlobalContext &gctx,
                                                 const ObSqlCtx &ctx,
                                                 ObResultSet &result,
                                                 int err,
                                                 int &client_ret,
                                                 bool force_local_retry,
                                                 bool is_inner_sql,
                                                 bool is_part_of_pl_sql)
{
  int ret = OB_SUCCESS;
  client_ret = err;
  retry_type_ = RETRY_TYPE_NONE;
  retry_err_code_ = OB_SUCCESS;
  retry_func func = nullptr;
  ObSQLSessionInfo *session = result.get_exec_context().get_my_session();
  if (OB_ISNULL(session)) {
    // ignore ret
    // this is possible. #issue/43953721
    LOG_WARN("session is null in exec_context. maybe OOM. don't retry", K(err));
  } else if (OB_FAIL(get_func(err, is_inner_sql, func))) {
    // note: if no err proc registered, a default handler
    // 'empty_proc' is used as processor func
    LOG_WARN("fail get retry func", K(err), K(ret));
  } else if (OB_ISNULL(func)) {
    client_ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid retry processor, no retry", K(err));
  } else {
    // you can't tell exact stmt retry times for a SQL in PL as PL may do whole block retry
    // so we use retry_times_ as stmt_retry_times for any stmt in PL
    // if pl + stmt_retry_times == 0 scene, will cause timeout early.
    // So the number of retry times here is at least 1
    const int64_t stmt_retry_times =
        is_part_of_pl_sql ? (retry_times_ == 0 ? 1 : retry_times_):
        session->get_retry_info().get_retry_cnt();
    ObRetryParam retry_param(ctx, result, *session,
                             curr_query_tenant_local_schema_version_,
                             curr_query_tenant_global_schema_version_,
                             curr_query_sys_local_schema_version_,
                             curr_query_sys_global_schema_version_,
                             force_local_retry,
                             is_inner_sql,
                             is_part_of_pl_sql,
                             stmt_retry_times,
                             retry_times_,
                             err,
                             retry_type_,
                             client_ret);
    // do some common checks in this hook, which is not bond to certain error code
    ObQueryRetryCtrl::before_func(retry_param);
    // this 'if' check is necessary, as direct call to func may override
    // the decision made in 'before_func', which is not what you want.
    if (!retry_param.no_more_test_) {
      func(retry_param);
    }
    // always execute after func hook to set some states
    ObQueryRetryCtrl::after_func(retry_param);
  }
  if (RETRY_TYPE_NONE != retry_type_) {
    // this retry times only apply to current thread retry.
    // reset to 0 after each packet retry
    retry_times_++;
  }
  // xiaochu: I don't like the idea 'retry_err_code_', remove it later
  if (RETRY_TYPE_NONE != retry_type_) {
    retry_err_code_ = client_ret;
  }
  if (RETRY_TYPE_NONE != retry_type_) {
    struct CloseFailFunctor {
      ObQueryRetryCtrl* retry_ctl_;
      CloseFailFunctor(ObQueryRetryCtrl* retry_ctl): retry_ctl_(retry_ctl) {}
      void operator()(const int err, int &client_ret) {
        retry_ctl_->on_close_resultset_fail_(err, client_ret);
      }
    } callback_functor(this);
    result.set_close_fail_callback(callback_functor);
  }
}

void ObQueryRetryCtrl::on_close_resultset_fail_(const int err, int &client_ret)
{
  // some unretryable error happened in close result set phase
  if (OB_SUCCESS != err && RETRY_TYPE_NONE != retry_type_) {
    // the txn relative error in close stmt
    // thses error will cause the txn must to be rollbacked
    // and can not accept new request any more, so if retry
    // current stmt, it must be failed, hence we cancel retry
    if (OB_TRANS_NEED_ROLLBACK == err ||
        OB_TRANS_INVALID_STATE == err ||
        OB_TRANS_HAS_DECIDED == err) {
      retry_type_ = RETRY_TYPE_NONE;
      // also clear the packet retry
      THIS_WORKER.unset_need_retry();
      // rewrite the client error code
      // when decide to cancel the retry, return an unretryable error
      // is better, because it won't leak the internal error to user
      client_ret = err;
    }
  }
}
}/* ns observer*/
}/* ns oceanbase */
