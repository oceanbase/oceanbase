// Copyright (c) 2018-present Alibaba Inc. All Rights Reserved.
// Author:
//   Junquan Chen <>

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_begin_processor.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/omt/ob_tenant.h"
#include "observer/table_load/ob_table_load_coordinator.h"
#include "observer/table_load/ob_table_load_redef_table.h"
#include "observer/table_load/ob_table_load_schema.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/ob_table_load_store.h"
#include "observer/table_load/ob_table_load_table_ctx.h"

namespace oceanbase
{
namespace observer
{
using namespace table;
using namespace sql;
using namespace omt;

/**
 * ObTableLoadBeginP
 */

ObTableLoadBeginP::ObTableLoadBeginP(const ObGlobalContext &gctx) : gctx_(gctx), table_ctx_(nullptr)
{
}

ObTableLoadBeginP::~ObTableLoadBeginP()
{
  if (OB_NOT_NULL(table_ctx_)) {
    ObTableLoadService::put_ctx(table_ctx_);
    table_ctx_ = nullptr;
  }
}

int ObTableLoadBeginP::process()
{
  int ret = OB_SUCCESS;
  uint64_t table_id = OB_INVALID_ID;
  ObTableLoadArray<ObString> column_names;
  bool is_new = false;

  if (OB_FAIL(check_user_access(arg_.credential_))) {
    LOG_WARN("fail to check_user_access", KR(ret), K_(arg));
  }

  if (OB_SUCC(ret)) {
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *table_schema = nullptr;
    if (OB_FAIL(ObTableLoadSchema::get_table_schema(credential_.tenant_id_,
                                                    credential_.database_id_, arg_.table_name_,
                                                    schema_guard, table_schema))) {
      LOG_WARN("fail to get table schema", KR(ret), K_(arg));
    } else {
      table_id = table_schema->get_table_id();
      if (OB_FAIL(ObTableLoadSchema::get_column_names(table_schema, allocator_, column_names))) {
        LOG_WARN("fail to get column name", KR(ret), K_(arg));
      } else if (OB_UNLIKELY(column_names.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected empty column names", KR(ret), K_(arg));
      } else if (OB_FAIL(init_idx_array(table_schema))) {
        LOG_WARN("failed to init idx array", K(ret));
      }
    }
  }

  // get the existing table ctx if it exists
  if (OB_SUCC(ret)) {
    ObTableLoadKey key(credential_.tenant_id_, table_id);
    if (OB_FAIL(ObTableLoadService::get_ctx(key, table_ctx_))) {
      if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
        LOG_WARN("fail to get ctx", KR(ret), K(key));
      } else {
        ret = OB_SUCCESS;
        table_ctx_ = nullptr;
      }
    } else if (OB_UNLIKELY(!ObTableLoadCoordinator::is_ctx_inited(table_ctx_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected uninited coordinator ctx", KR(ret));
    }
  }

  // create new table ctx if it does not exist
  if (OB_SUCC(ret) && nullptr == table_ctx_) {
    ObTenant *tenant = nullptr;
    sql::ObSQLSessionInfo *session_info = nullptr;
    sql::ObFreeSessionCtx free_session_ctx;
    free_session_ctx.sessid_ = ObSQLSessionInfo::INVALID_SESSID;
    if (arg_.config_.flag_.data_type_ >=
        static_cast<uint64_t>(ObTableLoadDataType::MAX_DATA_TYPE)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("data type is error", KR(ret), K(arg_.config_.flag_.data_type_));
    } else if (OB_FAIL(GCTX.omt_->get_tenant(credential_.tenant_id_, tenant))) {
      LOG_WARN("fail to get tenant handle", KR(ret), K(credential_.tenant_id_));
    } else if (OB_FAIL(ObTableLoadUtils::create_session_info(credential_.user_id_, session_info, free_session_ctx))) {
      LOG_WARN("fail to init session info", KR(ret));
    } else {
      ObTableLoadParam param;
      param.tenant_id_ = credential_.tenant_id_;
      param.table_id_ = table_id;
      param.batch_size_ = arg_.config_.batch_size_;
      param.parallel_ = arg_.config_.session_count_;
      param.session_count_ = MIN(arg_.config_.session_count_, (int32_t)tenant->unit_max_cpu());
      param.max_error_row_count_ = arg_.config_.max_error_row_count_;
      param.column_count_ = column_names.count();
      param.need_sort_ = arg_.config_.flag_.is_need_sort_;
      param.px_mode_ = false;
      param.online_opt_stat_gather_ = false;
      param.data_type_ = static_cast<ObTableLoadDataType>(arg_.config_.flag_.data_type_);
      param.dup_action_ = static_cast<ObLoadDupActionType>(arg_.config_.flag_.dup_action_);
      if (OB_FAIL(param.normalize())) {
        LOG_WARN("fail to normalize param", KR(ret));
      }
      // check support
      else if (OB_FAIL(ObTableLoadService::check_support_direct_load(table_id))) {
        LOG_WARN("fail to check support direct load", KR(ret), K(table_id));
      }
      // create table ctx
      else if (OB_FAIL(create_table_ctx(param, idx_array_, *session_info, table_ctx_))) {
        LOG_WARN("fail to create table ctx", KR(ret));
      } else {
        is_new = true;
      }
    }
    if (session_info != nullptr) {
      ObTableLoadUtils::free_session_info(session_info, free_session_ctx);
      session_info = nullptr;
    }
  }

  if (OB_SUCC(ret)) {
    ObTableLoadCoordinator coordinator(table_ctx_);
    if (OB_FAIL(coordinator.init())) {
      LOG_WARN("fail to init coordinator", KR(ret));
    } else if (is_new && OB_FAIL(coordinator.begin())) { // 新建的ctx需要begin
      LOG_WARN("fail to coordinator begin", KR(ret));
    } else if (OB_FAIL(coordinator.get_status(result_.status_, result_.error_code_))) {
      LOG_WARN("fail to coordinator get status", KR(ret));
    }
  }

  if (OB_SUCC(ret)) {
    result_.table_id_ = table_id;
    result_.task_id_ = table_ctx_->ddl_param_.task_id_;
    result_.column_names_ = column_names;
  }

  if (OB_FAIL(ret)) {
    if (nullptr != table_ctx_) {
      ObTableLoadService::remove_ctx(table_ctx_);
      ObTableLoadCoordinator::abort_ctx(table_ctx_);
    }
  }

  return ret;
}

int ObTableLoadBeginP::check_user_access(const ObString &credential_str)
{
  return ObTableLoadUtils::check_user_access(credential_str, gctx_, credential_);
}

int ObTableLoadBeginP::init_idx_array(const ObTableSchema *table_schema)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObColDesc, 64> column_descs;
  if (OB_FAIL(table_schema->get_column_ids(column_descs, false))) {
    LOG_WARN("fail to get column ids", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && (i < column_descs.count()); ++i) {
      ObColDesc &col_desc = column_descs.at(i);
      const ObColumnSchemaV2 *column_schema = table_schema->get_column_schema(col_desc.col_id_);
      if (!column_schema->is_hidden()) {
        int64_t idx = col_desc.col_id_ - OB_APP_MIN_COLUMN_ID;
        if (OB_FAIL(idx_array_.push_back(idx))) {
          LOG_WARN("failed to push back idx to array", K(ret), K(idx));
        }
      }
    }
  }

  return ret;
}

int ObTableLoadBeginP::create_table_ctx(const ObTableLoadParam &param,
                                        const ObIArray<int64_t> &idx_array,
                                        ObSQLSessionInfo &session_info,
                                        ObTableLoadTableCtx *&table_ctx)
{
  int ret = OB_SUCCESS;
  table_ctx = nullptr;
  ObTableLoadDDLParam ddl_param;
  // start redef table
  ObTableLoadRedefTableStartArg start_arg;
  ObTableLoadRedefTableStartRes start_res;
  uint64_t data_version = 0;
  start_arg.tenant_id_ = param.tenant_id_;
  start_arg.table_id_ = param.table_id_;
  start_arg.parallelism_ = param.parallel_;
  if (OB_FAIL(GET_MIN_DATA_VERSION(param.tenant_id_, data_version))) {
    LOG_WARN("fail to get tenant data version", KR(ret));
  } else if (OB_FAIL(ObTableLoadRedefTable::start(start_arg, start_res, session_info))) {
    LOG_WARN("fail to start redef table", KR(ret), K(start_arg));
  } else {
    ddl_param.dest_table_id_ = start_res.dest_table_id_;
    ddl_param.task_id_ = start_res.task_id_;
    ddl_param.schema_version_ = start_res.schema_version_;
    ddl_param.snapshot_version_ = start_res.snapshot_version_;
    ddl_param.data_version_ = data_version;
  }
  if (OB_SUCC(ret)) {
    const int64_t origin_timeout_ts = THIS_WORKER.get_timeout_ts();
    THIS_WORKER.set_timeout_ts(ObTimeUtil::current_time() + arg_.timeout_);
    if (OB_ISNULL(table_ctx = ObTableLoadService::alloc_ctx())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc table ctx", KR(ret), K(param));
    } else if (OB_FAIL(table_ctx->init(param, ddl_param, &session_info))) {
      LOG_WARN("fail to init table ctx", KR(ret));
    } else if (OB_FAIL(table_ctx->init_client_exec_ctx())) {
      LOG_WARN("fail to init client exec ctx", KR(ret));
    } else if (OB_FAIL(ObTableLoadCoordinator::init_ctx(table_ctx, idx_array_,
                                                        session_info.get_priv_user_id(),
                                                        table_ctx->client_exec_ctx_))) {
      LOG_WARN("fail to coordinator init ctx", KR(ret));
    } else if (OB_FAIL(ObTableLoadService::add_ctx(table_ctx))) {
      LOG_WARN("fail to add ctx", KR(ret));
    }
    THIS_WORKER.set_timeout_ts(origin_timeout_ts);
  }
  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (ddl_param.is_valid()) {
      ObTableLoadRedefTableAbortArg abort_arg;
      abort_arg.tenant_id_ = param.tenant_id_;
      abort_arg.task_id_ = ddl_param.task_id_;
      if (OB_TMP_FAIL(ObTableLoadRedefTable::abort(abort_arg, session_info))) {
        LOG_WARN("fail to abort redef table", KR(tmp_ret), K(abort_arg));
      }
    }
    if (nullptr != table_ctx) {
      ObTableLoadService::free_ctx(table_ctx);
      table_ctx = nullptr;
    }
  }
  return ret;
}

/**
 * ObTableLoadPreBeginPeerP
 */

int ObTableLoadPreBeginPeerP::deserialize()
{
  arg_.partition_id_array_.set_allocator(allocator_);
  arg_.target_partition_id_array_.set_allocator(allocator_);
  return ParentType::deserialize();
}

int ObTableLoadPreBeginPeerP::process()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_user_access(arg_.credential_))) {
    LOG_WARN("fail to check_user_access", KR(ret), K_(arg));
  }

  if (OB_SUCC(ret)) {
    ObTableLoadTableCtx *table_ctx = nullptr;
    ObTableLoadParam param;
    param.tenant_id_ = credential_.tenant_id_;
    param.table_id_ = arg_.table_id_;
    param.batch_size_ = arg_.config_.batch_size_;
    param.parallel_ = arg_.config_.session_count_;
    param.session_count_ = arg_.config_.session_count_;
    param.max_error_row_count_ = arg_.config_.max_error_row_count_;
    param.column_count_ = arg_.column_count_;
    param.need_sort_ = arg_.config_.flag_.is_need_sort_;
    param.px_mode_ = arg_.px_mode_;
    param.online_opt_stat_gather_ = arg_.online_opt_stat_gather_;
    param.dup_action_ = arg_.dup_action_;
    ObTableLoadDDLParam ddl_param;
    uint64_t data_version = 0;
    ddl_param.dest_table_id_ = arg_.dest_table_id_;
    ddl_param.task_id_ = arg_.task_id_;
    ddl_param.schema_version_ = arg_.schema_version_;
    ddl_param.snapshot_version_ = arg_.snapshot_version_;
    ddl_param.data_version_ = arg_.data_version_;
    if (OB_FAIL(create_table_ctx(param, ddl_param, table_ctx))) {
      LOG_WARN("fail to create table ctx", KR(ret));
    }
    if (OB_SUCC(ret)) {
      ObTableLoadStore store(table_ctx);
      if (OB_FAIL(store.init())) {
        LOG_WARN("fail to init store", KR(ret));
      } else if (OB_FAIL(store.pre_begin())) {
        LOG_WARN("fail to store pre begin", KR(ret));
      }
    }
    if (OB_NOT_NULL(table_ctx)) {
      ObTableLoadService::put_ctx(table_ctx);
      table_ctx = nullptr;
    }
  }

  return ret;
}

int ObTableLoadPreBeginPeerP::check_user_access(const ObString &credential_str)
{
  return ObTableLoadUtils::check_user_access(credential_str, gctx_, credential_);
}

int ObTableLoadPreBeginPeerP::create_table_ctx(const ObTableLoadParam &param,
                                               const ObTableLoadDDLParam &ddl_param,
                                               ObTableLoadTableCtx *&table_ctx)
{
  int ret = OB_SUCCESS;
  table_ctx = nullptr;
  if (OB_ISNULL(table_ctx = ObTableLoadService::alloc_ctx())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc table ctx", KR(ret), K(param));
  } else if (OB_FAIL(table_ctx->init(param, ddl_param, arg_.session_info_))) {
    LOG_WARN("fail to init table ctx", KR(ret));
  } else if (OB_FAIL(ObTableLoadStore::init_ctx(table_ctx,
                                                arg_.partition_id_array_,
                                                arg_.target_partition_id_array_))) {
    LOG_WARN("fail to store init ctx", KR(ret));
  } else if (OB_FAIL(ObTableLoadService::add_ctx(table_ctx))) {
    LOG_WARN("fail to add ctx", KR(ret));
  }
  if (OB_FAIL(ret)) {
    if (nullptr != table_ctx) {
      ObTableLoadService::free_ctx(table_ctx);
      table_ctx = nullptr;
    }
  }
  return ret;
}

/**
 * ObTableLoadConfirmBeginPeerP
 */

int ObTableLoadConfirmBeginPeerP::process()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_user_access(arg_.credential_))) {
    LOG_WARN("fail to check_user_access", KR(ret), K_(arg));
  }

  if (OB_SUCC(ret)) {
    ObTableLoadTableCtx *table_ctx = nullptr;
    ObTableLoadUniqueKey key(arg_.table_id_, arg_.task_id_);
    if (OB_FAIL(ObTableLoadService::get_ctx(key, table_ctx))) {
      LOG_WARN("fail to get table ctx", KR(ret), K(key));
    } else {
      ObTableLoadStore store(table_ctx);
      if (OB_FAIL(store.init())) {
        LOG_WARN("fail to init store", KR(ret));
      } else if (OB_FAIL(store.confirm_begin())) {
        LOG_WARN("fail to store confirm begin", KR(ret));
      }
    }
    if (OB_NOT_NULL(table_ctx)) {
      ObTableLoadService::put_ctx(table_ctx);
      table_ctx = nullptr;
    }
  }

  return ret;
}

int ObTableLoadConfirmBeginPeerP::check_user_access(const ObString &credential_str)
{
  return ObTableLoadUtils::check_user_access(credential_str, gctx_, credential_);
}

} // namespace observer
} // namespace oceanbase
