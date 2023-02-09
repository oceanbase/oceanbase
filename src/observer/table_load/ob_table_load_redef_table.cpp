// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   yuya.yu <yuya.yu@oceanbase.com>

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/ob_table_load_redef_table.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/ob_table_load_struct.h"
#include "share/ob_common_rpc_proxy.h"
#include "sql/engine/cmd/ob_ddl_executor_util.h"
#include "storage/ddl/ob_ddl_server_client.h"

namespace oceanbase
{
using namespace common;
using namespace sql;
using namespace obrpc;
namespace observer
{
ObTableLoadRedefTable::ObTableLoadRedefTable()
  : ctx_(nullptr),
    session_info_(nullptr),
    ddl_task_id_(0),
    schema_version_(0),
    is_finish_or_abort_called_(false),
    is_inited_(false)
{
}

ObTableLoadRedefTable::~ObTableLoadRedefTable()
{
  reset();
}

void ObTableLoadRedefTable::reset()
{
  if (!is_finish_or_abort_called_) {
    abort(); // 这个必须执行
  }
}

int ObTableLoadRedefTable::init(ObTableLoadTableCtx *ctx, ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTableLoadRedefTable init twice", KR(ret), KP(this));
  } else if (OB_ISNULL(ctx) || OB_ISNULL(session_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_ctx or session_info cannot be null", KR(ret));
  } else {
    ctx_ = ctx;
    session_info_ = session_info;
    is_inited_ = true;
  }
  return ret;
}

int ObTableLoadRedefTable::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadRedefTable not init", KR(ret));
  } else {
    const int64_t origin_timeout_ts = THIS_WORKER.get_timeout_ts();
    ObCommonRpcProxy *common_rpc_proxy = GCTX.rs_rpc_proxy_;
    ObCreateHiddenTableArg create_table_arg;
    ObCreateHiddenTableRes create_table_res;
    create_table_arg.reset();
    create_table_arg.tenant_id_ = ctx_->param_.tenant_id_;
    create_table_arg.table_id_ = ctx_->param_.table_id_;
    create_table_arg.dest_tenant_id_ = ctx_->param_.tenant_id_;
    create_table_arg.parallelism_ = ctx_->param_.session_count_;
    create_table_arg.ddl_type_ = share::DDL_DIRECT_LOAD;
    create_table_arg.session_id_ = session_info_->get_sessid_for_table();
    create_table_arg.sql_mode_ = session_info_->get_sql_mode();
    create_table_arg.tz_info_ = session_info_->get_tz_info_wrap().get_tz_info_offset();
    create_table_arg.nls_formats_[ObNLSFormatEnum::NLS_DATE] = session_info_->get_local_nls_date_format();
    create_table_arg.nls_formats_[ObNLSFormatEnum::NLS_TIMESTAMP] = session_info_->get_local_nls_timestamp_format();
    create_table_arg.nls_formats_[ObNLSFormatEnum::NLS_TIMESTAMP_TZ] = session_info_->get_local_nls_timestamp_tz_format();
    if (OB_FAIL(create_table_arg.tz_info_wrap_.deep_copy(session_info_->get_tz_info_wrap()))) {
      LOG_WARN("failed to deep copy tz_info_wrap", KR(ret));
    } else if (OB_FAIL(ObDDLServerClient::create_hidden_table(create_table_arg, create_table_res, *session_info_))) {
      LOG_WARN("failed to create hidden table", KR(ret), K(create_table_arg));
    } else {
      ddl_task_id_ = create_table_res.task_id_;
      schema_version_ = create_table_res.schema_version_;
      const_cast<ObTableLoadParam &>(ctx_->param_).target_table_id_ = create_table_res.dest_table_id_;
      LOG_INFO("succeed to create hidden table", K(create_table_res), K(create_table_res));
    }
    THIS_WORKER.set_timeout_ts(origin_timeout_ts);
  }


  return ret;
}

int ObTableLoadRedefTable::finish()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadRedefTable not init", KR(ret));
  } else {
    const int64_t origin_timeout_ts = THIS_WORKER.get_timeout_ts();
    obrpc::ObCommonRpcProxy *common_rpc_proxy = GCTX.rs_rpc_proxy_;
    obrpc::ObCopyTableDependentsArg copy_table_dependents_arg;
    copy_table_dependents_arg.task_id_ = ddl_task_id_;
    copy_table_dependents_arg.tenant_id_ = ctx_->param_.tenant_id_;
    copy_table_dependents_arg.copy_indexes_ = true;
    copy_table_dependents_arg.copy_constraints_ = true;
    copy_table_dependents_arg.copy_triggers_ = false;
    copy_table_dependents_arg.ignore_errors_ = false;
    if (OB_FAIL(ObDDLServerClient::copy_table_dependents(copy_table_dependents_arg))) {
      LOG_WARN("failed to copy table dependents", KR(ret), K(copy_table_dependents_arg));
    } else {
      LOG_INFO("succeed to copy table dependents", K(copy_table_dependents_arg));
      obrpc::ObFinishRedefTableArg finish_redef_table_arg;
      finish_redef_table_arg.task_id_ = ddl_task_id_;
      finish_redef_table_arg.tenant_id_ = ctx_->param_.tenant_id_;

      ObDDLBuildSingleReplicaResponseArg build_single_replica_response_arg;
      build_single_replica_response_arg.task_id_ = ddl_task_id_;
      build_single_replica_response_arg.tenant_id_ = ctx_->param_.tenant_id_;
      build_single_replica_response_arg.ls_id_ = share::ObLSID(1);
      build_single_replica_response_arg.tablet_id_ = ObTableID(-1);
      build_single_replica_response_arg.source_table_id_ = ctx_->param_.table_id_;
      build_single_replica_response_arg.dest_schema_id_ = ctx_->param_.target_table_id_;
      build_single_replica_response_arg.ret_code_ = ret;
      build_single_replica_response_arg.task_id_ = ddl_task_id_;
      build_single_replica_response_arg.snapshot_version_ = 1;
      build_single_replica_response_arg.schema_version_ = schema_version_;
      build_single_replica_response_arg.execution_id_ = 1;
      if (OB_FAIL(ObDDLServerClient::finish_redef_table(finish_redef_table_arg, build_single_replica_response_arg, *session_info_))) {
        LOG_WARN("failed to finish redef table", KR(ret), K(finish_redef_table_arg));
      } else {
        is_finish_or_abort_called_ = true;
        LOG_INFO("succeed to finish redef table", KR(ret), K(finish_redef_table_arg));
      }
    }
    THIS_WORKER.set_timeout_ts(origin_timeout_ts);
  }
  return ret;
}

int ObTableLoadRedefTable::abort()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadRedefTable not init", KR(ret));
  } else {
    const int64_t origin_timeout_ts = THIS_WORKER.get_timeout_ts();
    is_finish_or_abort_called_ = true;
    obrpc::ObCommonRpcProxy *common_rpc_proxy = GCTX.rs_rpc_proxy_;
    obrpc::ObAbortRedefTableArg abort_redef_table_arg;
    abort_redef_table_arg.task_id_ = ddl_task_id_;
    abort_redef_table_arg.tenant_id_ = ctx_->param_.tenant_id_;
    if (OB_FAIL(ObDDLServerClient::abort_redef_table(abort_redef_table_arg, *session_info_))) {
      LOG_WARN("failed to abort redef table", KR(ret), K(abort_redef_table_arg));
    } else {
      LOG_INFO("abort_redef_table success", KR(ret));
    }
    THIS_WORKER.set_timeout_ts(origin_timeout_ts);
  }
  return ret;
}
}  // namespace observer
}  // namespace oceanbase
