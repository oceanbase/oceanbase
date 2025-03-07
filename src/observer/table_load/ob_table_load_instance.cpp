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

#include "observer/table_load/ob_table_load_instance.h"
#include "observer/table_load/ob_table_load_coordinator.h"
#include "observer/table_load/ob_table_load_index_long_wait.h"
#include "observer/table_load/ob_table_load_redef_table.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "storage/tablelock/ob_table_lock_service.h"

namespace oceanbase
{
namespace observer
{
using namespace sql;
using namespace storage;
using namespace table;
using namespace transaction;
using namespace transaction::tablelock;
using namespace common;

ObTableLoadInstance::ObTableLoadInstance()
  : execute_ctx_(nullptr),
    allocator_(nullptr),
    table_ctx_(nullptr),
    job_stat_(nullptr),
    is_inited_(false)
{
}

ObTableLoadInstance::~ObTableLoadInstance() { destroy(); }

void ObTableLoadInstance::destroy()
{
  int ret = OB_SUCCESS;
  if (nullptr != table_ctx_) {
    if (OB_FAIL(end_direct_load(false /*commit*/))) {
      LOG_WARN("fail to end direct load", KR(ret));
    }
  }
  if (stmt_ctx_.is_started()) {
    if (OB_FAIL(end_stmt(false /*commit*/))) {
      LOG_WARN("fail to end stmt", KR(ret));
    }
  }
}

int ObTableLoadInstance::init(ObTableLoadParam &param,
                              const ObIArray<uint64_t> &column_ids,
                              ObTableLoadExecCtx *execute_ctx)
{
  int ret = OB_SUCCESS;
  ObArray<ObTabletID> tablet_ids;
  tablet_ids.reset();
  return init(param, column_ids, tablet_ids, execute_ctx);
}

int ObTableLoadInstance::init(ObTableLoadParam &param,
                              const ObIArray<uint64_t> &column_ids,
                              const ObIArray<ObTabletID> &tablet_ids,
                              ObTableLoadExecCtx *execute_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadInstance init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!param.is_valid() || column_ids.empty() ||
                         column_ids.count() != param.column_count_ || !execute_ctx->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(param), K(column_ids), KPC(execute_ctx));
  } else if ((ObDirectLoadLevel::PARTITION == param.load_level_)
              && OB_UNLIKELY(!is_valid_tablet_ids(tablet_ids))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet ids", KR(ret), K(param.load_level_), K(tablet_ids));
  } else {
    DISABLE_SQL_MEMLEAK_GUARD;
    execute_ctx_ = execute_ctx;
    allocator_ = execute_ctx->get_allocator();
    if (OB_FAIL(param.normalize())) {
      LOG_WARN("fail to normalize param", KR(ret));
    }
    // check tenant
    else if (OB_FAIL(ObTableLoadService::check_tenant())) {
      LOG_WARN("fail to check tenant", KR(ret), K(param.tenant_id_));
    }
    // start stmt
    else if (OB_FAIL(start_stmt(param, tablet_ids))) {
      LOG_WARN("fail to start stmt", KR(ret), K(param), K(tablet_ids));
    }
    // double check support for concurrency of direct load and ddl
    else if (OB_FAIL(ObTableLoadService::check_support_direct_load(param.table_id_,
                                                                   param.method_,
                                                                   param.insert_mode_,
                                                                   param.load_mode_,
                                                                   param.load_level_,
                                                                   column_ids))) {
      LOG_WARN("fail to check support direct load", KR(ret), K(param));
    }
    // start direct load
    else if (OB_FAIL(start_direct_load(param, column_ids, tablet_ids))) {
      LOG_WARN("fail to start direct load", KR(ret));
    }
    // init succ
    else {
      job_stat_ = table_ctx_->job_stat_;
      is_inited_ = true;
    }
  }
  return ret;
}

// commit() = px_commit_data() + px_commit_ddl()
// used in non px_mode
int ObTableLoadInstance::commit()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadInstance not init", KR(ret), KP(this));
  } else {
    // end direct load
    if (OB_FAIL(end_direct_load(true /*commit*/))) {
      LOG_WARN("fail to end direct load", KR(ret));
    }
    // end stmt
    else if (OB_FAIL(end_stmt(true /*commit*/))) {
      LOG_WARN("fail to end stmt", KR(ret));
    }
  }
  return ret;
}

// used in insert /*+ append */ into select clause
int ObTableLoadInstance::px_commit_data()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadInstance not init", KR(ret), KP(this));
  } else {
    if (OB_FAIL(end_direct_load(true /*commit*/))) {
      LOG_WARN("fail to end direct load", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadInstance::px_commit_ddl()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadInstance not init", KR(ret), KP(this));
  } else {
    if (OB_FAIL(end_stmt(true /*commit*/))) {
      LOG_WARN("fail to end stmt", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadInstance::start_stmt(
    const ObTableLoadParam &param,
    const ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  stmt_ctx_.reset();
  stmt_ctx_.tenant_id_ = param.tenant_id_;
  stmt_ctx_.table_id_ = param.table_id_;
  stmt_ctx_.session_info_ = execute_ctx_->get_session_info();
  stmt_ctx_.is_incremental_ = ObDirectLoadMethod::is_incremental(param.method_);
  stmt_ctx_.use_insert_into_select_tx_ = param.px_mode_;
  if (stmt_ctx_.is_incremental_) { // incremental direct-load
    if (OB_FAIL(build_tx_param())) {
      LOG_WARN("fail to build tx param", KR(ret), K(stmt_ctx_));
    } else if (OB_FAIL(start_sql_tx())) {
      LOG_WARN("fail to start sql tx", KR(ret), K(stmt_ctx_));
    } else if (OB_FAIL(lock_for_inc_load(tablet_ids))) {
      LOG_WARN("fail to lock table in tx", KR(ret), K(stmt_ctx_), K(tablet_ids));
    } else if (OB_FAIL(init_ddl_param_for_inc_direct_load())) {
      LOG_WARN("fail to init ddl param for inc direct load", KR(ret), K(stmt_ctx_));
    }
    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(end_sql_tx(false /*commit*/))) {
        LOG_WARN("fail to end sql tx", KR(tmp_ret));
      }
    }
  } else {
    if (OB_FAIL(start_redef_table(param, tablet_ids))) {
      LOG_WARN("fail to start redef table", KR(ret), K(param), K(tablet_ids));
    }
  }
  if (OB_SUCC(ret)) {
    stmt_ctx_.is_started_ = true;
    LOG_INFO("start stmt succeed", KR(ret), K(stmt_ctx_), K(param));
  }
  return ret;
}

int ObTableLoadInstance::end_stmt(const bool commit)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (stmt_ctx_.is_incremental_) {
    if (OB_FAIL(end_sql_tx(commit))) {
      LOG_WARN("fail to end sql tx", KR(ret));
    }
  } else {
    if (commit && OB_FAIL(commit_redef_table())) {
      LOG_WARN("fail to commit redef table", KR(ret));
    }
    if (OB_FAIL(ret) || !commit) {
      if (OB_TMP_FAIL(abort_redef_table())) {
        LOG_WARN("fail to abort redef table", KR(tmp_ret));
        ret = COVER_SUCC(tmp_ret);
      }
    }
  }
  stmt_ctx_.is_started_ = false;
  LOG_INFO("end stmt succeed", KR(ret));
  return ret;
}

int64_t ObTableLoadInstance::get_stmt_expire_ts(ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  const int64_t default_query_timeout = 1000 * 1000; // default timeout 1s
  if (OB_ISNULL(session_info)) {
    return ObTimeUtil::current_time() + default_query_timeout;
  } else {
    int64_t query_timeout = 0;
    if (OB_FAIL(session_info->get_query_timeout(query_timeout))) {
      LOG_WARN("fail to get query timeout", KR(ret));
      query_timeout = default_query_timeout;
    }
    return session_info->get_query_start_time() + query_timeout;
  }
}

int ObTableLoadInstance::build_tx_param()
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = stmt_ctx_.session_info_;
  ObTxParam &tx_param = stmt_ctx_.tx_param_;
  int64_t timeout_us = 0;
  if (OB_FAIL(session_info->get_tx_timeout(timeout_us))) {
    LOG_WARN("failed to get tx timeout", KR(ret));
  } else {
    tx_param.timeout_us_ = timeout_us;
    tx_param.lock_timeout_us_ = session_info->get_trx_lock_timeout();
    tx_param.access_mode_ = ObTxAccessMode::RW;
    tx_param.isolation_ = session_info->get_tx_isolation();
    tx_param.cluster_id_ = GCONF.cluster_id;
  }
  return ret;
}

int ObTableLoadInstance::start_sql_tx()
{
  int ret = OB_SUCCESS;
  ObTransService *txs = MTL(ObTransService *);
  ObSQLSessionInfo *session_info = stmt_ctx_.session_info_;
  ObTxDesc *&tx_desc = session_info->get_tx_desc();
  if (stmt_ctx_.use_insert_into_select_tx_) { // insert into select path, tx_desc should be available
    if (OB_ISNULL(tx_desc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tx_desc of insert into select should not be null", KR(ret), KP(tx_desc));
    } else if (OB_UNLIKELY(!tx_desc->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tx_desc of insert into select should be valid", KR(ret), KPC(tx_desc));
    } else {
      stmt_ctx_.tx_desc_ = tx_desc;
      execute_ctx_->tx_desc_ = tx_desc;
      LOG_INFO("use insert into select tx", KPC(tx_desc));
    }
  } else { // other path, tx_desc could be null, tx_param needs to be set manually
    if (OB_UNLIKELY(nullptr != tx_desc && tx_desc->is_in_tx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("trans already exist", KR(ret), KPC(tx_desc));
    } else if (OB_ISNULL(tx_desc) && OB_FAIL(txs->acquire_tx(tx_desc,
                                                             session_info->get_sessid(),
                                                             session_info->get_data_version()))) {
      LOG_WARN("failed to acquire tx", KR(ret), K(session_info->get_sessid()),
               K(session_info->get_data_version()));
    } else if (OB_FAIL(txs->start_tx(*tx_desc, stmt_ctx_.tx_param_))) {
      LOG_WARN("failed to start tx", KR(ret), K(stmt_ctx_));
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(txs->release_tx(*tx_desc))) {
        LOG_WARN("failed to release tx", KR(tmp_ret), KPC(tx_desc));
      } else {
        tx_desc = nullptr;
      }
    } else {
      stmt_ctx_.tx_desc_ = tx_desc;
      execute_ctx_->tx_desc_ = tx_desc;
      LOG_INFO("start tx succeed", KPC(tx_desc));
    }
  }
  return ret;
}

int ObTableLoadInstance::end_sql_tx(const bool commit)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTransService *txs = MTL(ObTransService *);
  ObSQLSessionInfo *session_info = stmt_ctx_.session_info_;
  ObTxDesc *tx_desc = stmt_ctx_.tx_desc_;
  execute_ctx_->tx_desc_ = nullptr;
  if (stmt_ctx_.use_insert_into_select_tx_) {
    // do nothing
  } else {
    if (nullptr == tx_desc) {
      // do nothing
    } else {
      if (commit) {
        const int64_t stmt_timeout_ts = get_stmt_expire_ts(session_info);
        if (OB_FAIL(txs->commit_tx(*tx_desc, stmt_timeout_ts))) {
          LOG_WARN("failed to commit tx", KR(ret), KPC(tx_desc));
        } else {
          LOG_INFO("commit tx succeed", KPC(tx_desc));
        }
      } else {
        if (OB_FAIL(txs->rollback_tx(*tx_desc))) {
          LOG_WARN("failed to rollback tx", KR(ret), KPC(tx_desc));
        } else {
          LOG_INFO("rollback tx succeed", KPC(tx_desc));
        }
      }
      if (OB_TMP_FAIL(txs->release_tx(*tx_desc))) {
        LOG_ERROR("failed to release tx", KR(tmp_ret), KPC(tx_desc));
      }
      // reset session tx_desc
      session_info->get_tx_desc() = nullptr;
    }
  }
  stmt_ctx_.tx_desc_ = nullptr;
  return ret;
}

int ObTableLoadInstance::lock_for_inc_load(const ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  if (tablet_ids.empty()) {
    if (OB_FAIL(lock_table_in_tx())) {
      LOG_WARN("fail to lock table in tx", KR(ret));
    }
  } else {
    if (OB_FAIL(lock_tablets_in_tx(tablet_ids))) {
      LOG_WARN("fail to lock tablets in tx", KR(ret), K(tablet_ids));
    }
  }
  return ret;
}

int ObTableLoadInstance::build_base_lock_arg(ObLockTableRequest &lock_arg)
{
  int ret = OB_SUCCESS;
  const uint64_t table_id = stmt_ctx_.table_id_;
  lock_arg.owner_id_.set_default();
  lock_arg.lock_mode_ = tablelock::EXCLUSIVE;
  lock_arg.op_type_ = ObTableLockOpType::IN_TRANS_DML_LOCK;
  lock_arg.timeout_us_ = 0; // try lock
  lock_arg.table_id_ = table_id;
  return ret;
}

int ObTableLoadInstance::lock_table_in_tx()
{
  int ret = OB_SUCCESS;
  ObLockTableRequest lock_table_arg;
  if (OB_FAIL(build_base_lock_arg(lock_table_arg))) {
    LOG_WARN("fail to build lock arg", KR(ret));
  } else if (OB_FAIL(try_lock_in_tx(lock_table_arg))) {
    LOG_WARN("fail to try lock in tx", KR(ret), K(lock_table_arg));
  } else {
    LOG_INFO("lock table in tx succeed", K(lock_table_arg));
  }
  return ret;
}

int ObTableLoadInstance::lock_tablets_in_tx(const ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  ObLockTabletsRequest lock_tablets_arg;
  ObArray<ObTabletID> tmp_tablet_ids;
  if (OB_FAIL(tmp_tablet_ids.assign(tablet_ids))) {
    LOG_WARN("fail to assign tablet ids", KR(ret), K(tablet_ids));
  } else {
    lib::ob_sort(tmp_tablet_ids.begin(), tmp_tablet_ids.end());
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(build_base_lock_arg(lock_tablets_arg))) {
    LOG_WARN("fail to build lock arg", KR(ret), K(tablet_ids));
  } else if (OB_FAIL(lock_tablets_arg.tablet_ids_.assign(tmp_tablet_ids))) {
    LOG_WARN("fail to assign tablet ids", KR(ret), K(tmp_tablet_ids));
  } else if (OB_FAIL(try_lock_in_tx(lock_tablets_arg))) {
    LOG_WARN("fail to try lock in tx", KR(ret), K(lock_tablets_arg));
  } else {
    LOG_INFO("lock tablets in tx succeed", K(lock_tablets_arg), K(tablet_ids));
  }
  return ret;
}

int ObTableLoadInstance::try_lock_in_tx(const ObLockRequest &lock_arg)
{
  int ret = OB_SUCCESS;
  ObTableLockService *table_lock_service = MTL(ObTableLockService *);
  ObTxDesc *tx_desc = stmt_ctx_.tx_desc_;
  bool lock_succeed = false;
  int64_t sleep_time = 100 * 1000L; // 100ms
  if (OB_ISNULL(table_lock_service) || OB_ISNULL(tx_desc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_lock_service or tx_desc is nullptr", KR(ret),
                                                         KP(table_lock_service),
                                                         KP(tx_desc));
  } else {
    while (OB_SUCC(ret) && !lock_succeed) {
      if (OB_FAIL(execute_ctx_->check_status())) {
        LOG_WARN("failed to check status", KR(ret));
      } else if (OB_FAIL(table_lock_service->lock(*tx_desc, stmt_ctx_.tx_param_, lock_arg))) {
        if (OB_EAGAIN == ret) {
          ob_usleep(sleep_time);
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to lock table", KR(ret), K(lock_arg));
        }
      } else {
        lock_succeed = true;
      }
    }
  }
  return ret;
}

int ObTableLoadInstance::init_ddl_param_for_inc_direct_load()
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObCommonID raw_id;
  share::SCN current_scn;
  int64_t schema_version = 0;
  uint64_t tenant_data_version = 0;
  const uint64_t tenant_id = stmt_ctx_.tenant_id_;
  const uint64_t table_id = stmt_ctx_.table_id_;
  ObTableLoadDDLParam &ddl_param = stmt_ctx_.ddl_param_;
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id,
                                                                                  schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id, schema_version))) {
    LOG_WARN("failed to get tenant schema version", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObCommonIDUtils::gen_unique_id_by_rpc(tenant_id, raw_id))) {
    LOG_WARN("failed to gen unique id by rpc", KR(ret), K(tenant_id));
  } else if (OB_FAIL(share::ObLSAttrOperator::get_tenant_gts(tenant_id, current_scn))) {
    LOG_WARN("failed to get gts", KR(ret), K(tenant_id));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
    LOG_WARN("failed to get min data version", KR(ret), K(tenant_id));
  } else {
    ddl_param.schema_version_ = schema_version;
    ddl_param.task_id_ = raw_id.id();
    ddl_param.snapshot_version_ = current_scn.convert_to_ts();
    ddl_param.data_version_ = tenant_data_version;
    ddl_param.dest_table_id_ = table_id;
    ddl_param.cluster_version_ = GET_MIN_CLUSTER_VERSION();
    LOG_INFO("init ddl param for inc direct load succeed", K(ddl_param));
  }
  return ret;
}

int ObTableLoadInstance::start_redef_table(
    const ObTableLoadParam &param,
    const ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  ObTableLoadDDLParam &ddl_param = stmt_ctx_.ddl_param_;
  ObTableLoadRedefTableStartArg start_arg;
  ObTableLoadRedefTableStartRes start_res;
  start_arg.tenant_id_ = param.tenant_id_;
  start_arg.table_id_ = param.table_id_;
  start_arg.parallelism_ = param.parallel_;
  start_arg.is_load_data_ = !param.px_mode_;
  start_arg.is_insert_overwrite_ = ObDirectLoadMode::is_insert_overwrite(param.load_mode_);
  if ((ObDirectLoadLevel::PARTITION == param.load_level_)
      && OB_FAIL(start_arg.tablet_ids_.assign(tablet_ids))) {
    LOG_WARN("failed to assign tablet ids", KR(ret), K(param.load_level_), K(tablet_ids));
  } else if (OB_FAIL(ObTableLoadRedefTable::start(start_arg, start_res, *stmt_ctx_.session_info_))) {
    LOG_WARN("fail to start redef table", KR(ret), K(start_arg));
    // rewrite error code for concurrency of direct load and offline ddl
    if (OB_TABLE_NOT_EXIST == ret) {
      ret = OB_SCHEMA_NOT_UPTODATE;
    }
  } else {
    ddl_param.dest_table_id_ = start_res.dest_table_id_;
    ddl_param.task_id_ = start_res.task_id_;
    ddl_param.schema_version_ = start_res.schema_version_;
    ddl_param.snapshot_version_ = start_res.snapshot_version_;
    ddl_param.data_version_ = start_res.data_format_version_;
    ddl_param.cluster_version_ = GET_MIN_CLUSTER_VERSION();
    ddl_param.is_no_logging_ = start_res.is_no_logging_;
    LOG_INFO("start redef table succeed", K(ddl_param));
  }
  return ret;
}

int ObTableLoadInstance::commit_redef_table()
{
  int ret = OB_SUCCESS;
  ObTableLoadRedefTableFinishArg arg;
  arg.tenant_id_ = stmt_ctx_.tenant_id_;
  arg.table_id_ = stmt_ctx_.table_id_;
  arg.dest_table_id_ = stmt_ctx_.ddl_param_.dest_table_id_;
  arg.task_id_ = stmt_ctx_.ddl_param_.task_id_;
  arg.schema_version_ = stmt_ctx_.ddl_param_.schema_version_;
  if (OB_FAIL(ObTableLoadRedefTable::finish(arg, *stmt_ctx_.session_info_))) {
    LOG_WARN("fail to finish redef table", KR(ret), K(arg));
  } else {
    LOG_INFO("commit redef table succeed");
  }
  return ret;
}

int ObTableLoadInstance::abort_redef_table()
{
  int ret = OB_SUCCESS;
  ObTableLoadRedefTableAbortArg arg;
  arg.tenant_id_ = stmt_ctx_.tenant_id_;
  arg.task_id_ = stmt_ctx_.ddl_param_.task_id_;
  if (OB_FAIL(ObTableLoadRedefTable::abort(arg, *stmt_ctx_.session_info_))) {
    LOG_WARN("fail to abort redef table", KR(ret), K(arg));
  } else {
    LOG_INFO("abort redef table succeed");
  }
  return ret;
}

int ObTableLoadInstance::start_direct_load(const ObTableLoadParam &param,
                                           const ObIArray<uint64_t> &column_ids,
                                           const ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  ObTableLoadTableCtx *table_ctx = nullptr;
  ObSQLSessionInfo *session_info = execute_ctx_->get_session_info();
  if (OB_UNLIKELY(nullptr != table_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table ctx is not null", KR(ret));
  } else if (OB_FAIL(ObTableLoadService::alloc_ctx(table_ctx))) {
    LOG_WARN("fail to alloc table ctx", KR(ret), K(param));
  } else if (OB_FAIL(table_ctx->init(param, stmt_ctx_.ddl_param_, session_info, ObString::make_string(""), execute_ctx_->exec_ctx_))) {
    LOG_WARN("fail to init table ctx", KR(ret));
  } else if (OB_FAIL(ObTableLoadCoordinator::init_ctx(table_ctx, column_ids, tablet_ids, execute_ctx_))) {
    LOG_WARN("fail to coordinator init ctx", KR(ret));
  } else if (OB_FAIL(ObTableLoadService::add_ctx(table_ctx))) {
    LOG_WARN("fail to add ctx", KR(ret));
  } else {
    table_ctx_ = table_ctx;
  }
  if (OB_FAIL(ret)) {
    // table_ctx没有初始化成功不能赋值给table_ctx_
    if (nullptr != table_ctx) {
      ObTableLoadService::put_ctx(table_ctx);
      table_ctx = nullptr;
    }
  }
  if (OB_SUCC(ret)) {
    ObTableLoadCoordinator coordinator(table_ctx_);
    if (OB_FAIL(coordinator.init())) {
      LOG_WARN("fail to init coordinator", KR(ret));
    } else if (OB_FAIL(coordinator.begin())) {
      LOG_WARN("fail to coodrinator begin", KR(ret));
    } else if (OB_FAIL(wait_begin_finish())) {
      LOG_WARN("fail to wait begin finish", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadInstance::wait_begin_finish()
{
  int ret = OB_SUCCESS;
  ObTableLoadCoordinator coordinator(table_ctx_);

  if (OB_FAIL(coordinator.init())) {
    LOG_WARN("fail to init coordinator", KR(ret));
  } else {
    ObTableLoadStatusType status = ObTableLoadStatusType::NONE;
    int error_code = OB_SUCCESS;
    ObTableLoadIndexLongWait wait_obj(10 * 1000, WAIT_INTERVAL_US);
    while (OB_SUCC(ret) && ObTableLoadStatusType::LOADING != status && OB_SUCC(execute_ctx_->check_status())) {
      if (OB_FAIL(coordinator.get_status(status, error_code))) {
        LOG_WARN("fail to coordinator get status", KR(ret));
      } else {
        switch (status) {
          case ObTableLoadStatusType::INITED:
            wait_obj.wait();
            break;
          case ObTableLoadStatusType::LOADING:
            break;
          case ObTableLoadStatusType::ERROR:
            ret = error_code;
            LOG_WARN("table load has error", KR(ret));
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected status", KR(ret), K(status));
            break;
        }
      }
    }
  }

  return ret;
}

int ObTableLoadInstance::end_direct_load(const bool commit)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == table_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table ctx is null", KR(ret));
  } else {
    bool need_abort = !commit;
    if (commit) {
      ObTableLoadStatusType status = ObTableLoadStatusType::NONE;
      int error_code = OB_SUCCESS;
      ObTableLoadCoordinator coordinator(table_ctx_);
      if (OB_FAIL(coordinator.init())) {
        LOG_WARN("fail to init coordinator", KR(ret));
      }
      // finish
      else if (OB_FAIL(coordinator.finish())) {
        LOG_WARN("fail to finish", KR(ret));
      }
      ObTableLoadIndexLongWait wait_obj(10 * 1000, WAIT_INTERVAL_US);
      while (OB_SUCC(ret) && ObTableLoadStatusType::MERGED != status &&
             OB_SUCC(execute_ctx_->check_status())) {
        if (OB_FAIL(coordinator.get_status(status, error_code))) {
          LOG_WARN("fail to coordinator get status", KR(ret));
        } else {
          switch (status) {
            case ObTableLoadStatusType::FROZEN:
            case ObTableLoadStatusType::MERGING:
              wait_obj.wait();
              break;
            case ObTableLoadStatusType::MERGED:
              break;
            case ObTableLoadStatusType::ERROR:
              ret = error_code;
              LOG_WARN("table load has error", KR(ret));
              break;
            default:
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected status", KR(ret), K(status));
              break;
          }
        }
      }
      if (OB_SUCC(ret)) {
        // commit
        if (OB_FAIL(coordinator.commit(result_info_))) {
          LOG_WARN("fail to commit", KR(ret));
        }
      }
      if (OB_FAIL(ret)) {
        need_abort = true;
      }
    }
    if (need_abort) {
      // must abort here, abort redef table need exec_ctx session_info
      ObTableLoadCoordinator::abort_ctx(table_ctx_);
    }
    if (OB_TMP_FAIL(add_tx_result_to_user_session())) {
      LOG_WARN("fail to add tx result to user session", KR(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
    if (OB_TMP_FAIL(ObTableLoadService::remove_ctx(table_ctx_))) {
      LOG_WARN("table ctx may remove by service", KR(tmp_ret), KP(table_ctx_));
    }
    ObTableLoadService::put_ctx(table_ctx_);
    table_ctx_ = nullptr;
    job_stat_ = nullptr;
  }
  return ret;
}

int ObTableLoadInstance::add_tx_result_to_user_session()
{
  int ret = OB_SUCCESS;
  ObTransService *txs = MTL(ObTransService *);
  ObSQLSessionInfo *session_info = stmt_ctx_.session_info_;
  ObTxDesc *tx_desc = session_info->get_tx_desc();
  if (OB_ISNULL(table_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table ctx is null", KR(ret));
  } else if (stmt_ctx_.is_incremental_ && !stmt_ctx_.has_added_tx_result_ && OB_NOT_NULL(tx_desc)) {
    ObTxExecResult exec_result;
    if (OB_FAIL(txs->get_tx_exec_result(*table_ctx_->session_info_->get_tx_desc(), exec_result))) {
      LOG_WARN("failed to get tx exec result", KR(ret));
    } else if (OB_FAIL(txs->add_tx_exec_result(*tx_desc, exec_result))) {
      LOG_WARN("failed to add tx exec result", KR(ret), K(exec_result));
    } else {
      stmt_ctx_.has_added_tx_result_ = true;
      LOG_INFO("add tx result to user session succeed");
    }
  }
  return ret;
}

int ObTableLoadInstance::check_status()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadInstance not init", KR(ret), KP(this));
  } else {
    ObTableLoadCoordinator coordinator(table_ctx_);
    ObTableLoadStatusType status = ObTableLoadStatusType::NONE;
    int error_code = OB_SUCCESS;
    if (OB_FAIL(coordinator.init())) {
      LOG_WARN("fail to init coordinator", KR(ret));
    } else if (OB_FAIL(coordinator.get_status(status, error_code))) {
      LOG_WARN("fail to coordinator get status", KR(ret));
    } else if (OB_UNLIKELY(ObTableLoadStatusType::ERROR == status ||
                           ObTableLoadStatusType::ABORT == status)) {
      ret = (OB_SUCCESS != error_code ? error_code : OB_CANCELED);
      LOG_WARN("coordinator status error", KR(ret), K(status), K(error_code));
    }
  }
  return ret;
}

int ObTableLoadInstance::start_trans(TransCtx &trans_ctx, int64_t segment_id,
                                     ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObTableLoadCoordinator coordinator(table_ctx_);
  if (OB_FAIL(coordinator.init())) {
    LOG_WARN("fail to init coordinator", KR(ret));
  } else if (OB_FAIL(
               coordinator.start_trans(ObTableLoadSegmentID(segment_id), trans_ctx.trans_id_))) {
    LOG_WARN("fail to coordinator start trans", KR(ret));
  } else if (OB_FAIL(trans_ctx.next_sequence_no_array_.create(table_ctx_->param_.session_count_,
                                                              allocator))) {
    LOG_WARN("fail to create next sequence no array", KR(ret));
  } else {
    for (int64_t i = 0; i < table_ctx_->param_.session_count_; ++i) {
      trans_ctx.next_sequence_no_array_[i] = 1;
    }
  }
  return ret;
}

int ObTableLoadInstance::commit_trans(TransCtx &trans_ctx)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadInstance not init", KR(ret), KP(this));
  } else {
    ObTableLoadCoordinator coordinator(table_ctx_);
    if (OB_FAIL(coordinator.init())) {
      LOG_WARN("fail to init coordinator", KR(ret));
    }
    // finish trans
    else if (OB_FAIL(coordinator.finish_trans(trans_ctx.trans_id_))) {
      LOG_WARN("fail to finish trans", KR(ret));
    }
    // wait trans commit
    else if (OB_FAIL(check_trans_committed(trans_ctx))) {
      LOG_WARN("fail to check trans committed", KR(ret));
    }
  }
  return ret;
}

int ObTableLoadInstance::check_trans_committed(TransCtx &trans_ctx)
{
  int ret = OB_SUCCESS;
  const ObTableLoadTransId &trans_id = trans_ctx.trans_id_;
  ObTableLoadTransStatusType trans_status = ObTableLoadTransStatusType::NONE;
  int error_code = OB_SUCCESS;
  ObTableLoadCoordinator coordinator(table_ctx_);
  if (OB_FAIL(coordinator.init())) {
    LOG_WARN("fail to init coordinator", KR(ret));
  }
  ObTableLoadIndexLongWait wait_obj(10 * 1000, WAIT_INTERVAL_US);
  while (OB_SUCC(ret) && ObTableLoadTransStatusType::COMMIT != trans_status &&
         OB_SUCC(execute_ctx_->check_status())) {
    if (OB_FAIL(coordinator.get_trans_status(trans_id, trans_status, error_code))) {
      LOG_WARN("fail to coordinator get trans status", KR(ret));
    } else {
      switch (trans_status) {
        case ObTableLoadTransStatusType::FROZEN:
          wait_obj.wait();
          break;
        case ObTableLoadTransStatusType::COMMIT:
          break;
        case ObTableLoadTransStatusType::ERROR:
          ret = error_code;
          LOG_WARN("trans has error", KR(ret));
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected trans status", KR(ret), K(trans_status));
          break;
      }
    }
  }
  return ret;
}

int ObTableLoadInstance::write_trans(TransCtx &trans_ctx, int32_t session_id,
                                     const table::ObTableLoadObjRowArray &obj_rows)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadInstance not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(session_id < 0 || session_id > table_ctx_->param_.session_count_ ||
                         obj_rows.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(session_id), K(obj_rows.count()));
  } else {
    // TODO(suzhi.yt): java客户端调用的时候, 对于相同session_id可能会并发
    uint64_t &next_sequence_no = trans_ctx.next_sequence_no_array_[session_id - 1];
    ObTableLoadCoordinator coordinator(table_ctx_);
    if (OB_FAIL(coordinator.init())) {
      LOG_WARN("fail to init coordinator", KR(ret));
    } else if (OB_FAIL(coordinator.write(trans_ctx.trans_id_, session_id, next_sequence_no++,
                                         obj_rows))) {
      LOG_WARN("fail to write coordinator", KR(ret));
    }
  }
  return ret;
}

bool ObTableLoadInstance::is_valid_tablet_ids(const ObIArray<ObTabletID> &tablet_ids)
{
  bool is_valid = true;
  if (tablet_ids.empty()) {
    is_valid = false;
  } else {
    for (int64_t i = 0; is_valid && (i < tablet_ids.count()); ++i) {
      const ObTabletID &tablet_id = tablet_ids.at(i);
      if (OB_UNLIKELY(!tablet_id.is_valid())) {
        is_valid = false;
      }
    }
  }
  return is_valid;
}

} // namespace observer
} // namespace oceanbase
