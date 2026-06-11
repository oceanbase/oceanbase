/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/mview/cmd/ob_mview_explain_refresh_executor.h"
#include "sql/engine/ob_exec_context.h"
#include "storage/mview/cmd/ob_mview_executor_util.h"
#include "storage/mview/ob_mview_refresh_helper.h"
#include "storage/mview/ob_mview_refresh.h"
#include "sql/resolver/mv/ob_mv_provider.h"
#include "sql/ob_sql_utils.h"
#include "sql/session/ob_local_session_var.h"
#include "rootserver/mview/ob_mview_maintenance_service.h"
#include "rootserver/mview/ob_mview_utils.h"

namespace oceanbase
{
namespace storage
{
using namespace share;
using namespace share::schema;
using namespace sql;

namespace {
// explain_refresh generates the refresh SQL by resolving the mview definition through the same
// path as the real refresh (ObMVProvider). That resolution enforces that the session's sys vars
// (e.g. sql_mode) match the values solidified when the mview was created, otherwise it raises
// OB_ERR_SESSION_VAR_CHANGED. The real refresh satisfies this by applying the mview's solidified
// vars onto the (inner) refresh session; explain runs in the user session, whose vars may differ.
// This guard temporarily applies the mview's solidified vars to the session and restores the
// original values on destruction, mirroring ObMViewTransaction::ObSessionParamSaved.
class ObExplainSessionVarGuard
{
public:
  explicit ObExplainSessionVarGuard(ObSQLSessionInfo *session)
    : session_(session), allocator_(ObMemAttr(MTL_ID(), "MViewExpVar")), saved_vars_() {}
  ~ObExplainSessionVarGuard() { (void)restore(); }
  int apply(const ObLocalSessionVar &solidified_vars)
  {
    int ret = OB_SUCCESS;
    ObSEArray<const ObSessionSysVar *, 8> diff_vars;
    ObSEArray<ObObj, 8> cur_vals;
    if (OB_ISNULL(session_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is null", KR(ret));
    } else if (OB_FAIL(solidified_vars.get_different_vars_from_session(session_, diff_vars, cur_vals))) {
      LOG_WARN("fail to get different vars from session", KR(ret));
    } else if (OB_UNLIKELY(diff_vars.count() != cur_vals.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected array size", KR(ret), K(diff_vars.count()), K(cur_vals.count()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < diff_vars.count(); ++i) {
        const ObSessionSysVar *var = diff_vars.at(i);
        if (OB_ISNULL(var)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("var is null", KR(ret), K(i));
        } else {
          ObSessionSysVar saved;
          saved.type_ = var->type_;
          if (OB_FAIL(saved_vars_.push_back(saved))) {
            LOG_WARN("fail to push back saved var", KR(ret));
          } else {
            // deep copy the original value so it stays valid until restore
            ObSessionSysVar &pushed = saved_vars_.at(saved_vars_.count() - 1);
            if (OB_FAIL(deep_copy_obj(allocator_, cur_vals.at(i), pushed.val_))) {
              LOG_WARN("fail to deep copy old var val", KR(ret));
              saved_vars_.pop_back();
            } else if (OB_FAIL(session_->update_sys_variable(var->type_, var->val_))) {
              LOG_WARN("fail to update sys var", KR(ret), K(var->type_));
            }
          }
        }
      }
    }
    return ret;
  }
private:
  int restore()
  {
    int ret = OB_SUCCESS;
    if (OB_NOT_NULL(session_)) {
      ARRAY_FOREACH(saved_vars_, i) {
        const ObSessionSysVar &var = saved_vars_.at(i);
        if (OB_FAIL(session_->update_sys_variable(var.type_, var.val_))) {
          LOG_WARN("fail to restore sys var", KR(ret), K(var.type_));
        }
      }
      saved_vars_.reuse();
    }
    return ret;
  }
  ObSQLSessionInfo *session_;
  ObArenaAllocator allocator_;
  ObSEArray<ObSessionSysVar, 8> saved_vars_;
};
} // anonymous namespace

ObMViewExplainRefreshExecutor::ObMViewExplainRefreshExecutor()
  : ctx_(nullptr), arg_(nullptr), session_info_(nullptr), schema_checker_(),
    tenant_id_(OB_INVALID_TENANT_ID)
{
}

ObMViewExplainRefreshExecutor::~ObMViewExplainRefreshExecutor() {}

int ObMViewExplainRefreshExecutor::execute(ObExecContext &ctx,
                                           const ObMViewExplainRefreshArg &arg,
                                           ObIAllocator &alloc,
                                           ObString &result)
{
  int ret = OB_SUCCESS;
  ctx_ = &ctx;
  arg_ = &arg;
  if (OB_ISNULL(session_info_ = ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", KR(ret));
  } else if (OB_ISNULL(ctx.get_sql_ctx()) || OB_ISNULL(ctx.get_sql_ctx()->schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema guard is null", KR(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(arg));
  } else if (OB_FAIL(resolve_tenant(arg))) {
    LOG_WARN("fail to resolve tenant", KR(ret), K(arg));
  } else if (OB_FAIL(resolve_arg(arg))) {
    LOG_WARN("fail to resolve arg", KR(ret));
  } else if (mview_ids_.empty()) {
    // do nothing
  } else if (mview_ids_.count() > 1) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported explain_refresh for multiple mviews", KR(ret), K(arg));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "explain_refresh for multiple materialized views");
  } else {
    ObSqlString output;
    // when explaining another tenant (only allowed for the sys tenant), switch both the
    // MTL context and the session's effective tenant so that mview sql resolution and
    // sql_id computation run against the target tenant's schema.
    const uint64_t saved_eff_tenant_id = session_info_->get_effective_tenant_id();
    const bool need_switch = (tenant_id_ != saved_eff_tenant_id);
    MTL_SWITCH(tenant_id_) {
      if (need_switch && OB_FAIL(session_info_->switch_tenant(tenant_id_))) {
        LOG_WARN("fail to switch session tenant", KR(ret), K(tenant_id_));
      } else if (!arg_->nested_) {
        if (OB_FAIL(do_explain_refresh(output))) {
          LOG_WARN("fail to do explain refresh", KR(ret));
        }
      } else {
        if (OB_FAIL(do_explain_nested_refresh(output))) {
          LOG_WARN("fail to do explain nested refresh", KR(ret));
        }
      }
      if (need_switch) {
        const int tmp_ret = session_info_->switch_tenant(saved_eff_tenant_id);
        if (OB_SUCCESS != tmp_ret) {
          LOG_ERROR("fail to restore session tenant", K(tmp_ret), K(saved_eff_tenant_id));
          ret = OB_SUCCESS == ret ? tmp_ret : ret;
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObString tmp_result(output.length(), output.ptr());
      if (OB_FAIL(ob_write_string(alloc, tmp_result, result))) {
        LOG_WARN("fail to write string", KR(ret));
      }
    }
  }
  return ret;
}

int ObMViewExplainRefreshExecutor::resolve_tenant(const ObMViewExplainRefreshArg &arg)
{
  int ret = OB_SUCCESS;
  const uint64_t cur_tenant_id = session_info_->get_effective_tenant_id();
  tenant_id_ = (0 == arg.tenant_id_) ? cur_tenant_id : arg.tenant_id_;
  if (tenant_id_ != cur_tenant_id && OB_SYS_TENANT_ID != cur_tenant_id) {
    // only the sys tenant may explain another tenant's mview refresh plan
    ret = OB_ERR_NO_PRIVILEGE;
    LOG_WARN("non-sys tenant can not explain refresh for other tenant", KR(ret),
             K(cur_tenant_id), K(tenant_id_));
    LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, "SUPER");
  } else if (tenant_id_ == cur_tenant_id) {
    // same tenant: reuse the session's schema guard
    if (OB_FAIL(schema_checker_.init(*ctx_->get_sql_ctx()->schema_guard_,
                                     session_info_->get_server_sid()))) {
      LOG_WARN("fail to init schema checker", KR(ret));
    }
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("schema service is null", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_,
                                                                   tenant_schema_guard_))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_checker_.init(tenant_schema_guard_,
                                          session_info_->get_server_sid()))) {
    LOG_WARN("fail to init schema checker", KR(ret), K(tenant_id_));
  }
  return ret;
}

int ObMViewExplainRefreshExecutor::resolve_arg(const ObMViewExplainRefreshArg &arg)
{
  int ret = OB_SUCCESS;
  ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
  ObCollationType cs_type = CS_TYPE_INVALID;
  ObMVRefreshMethod refresh_method = ObMVRefreshMethod::MAX;
  ObArray<ObString> mview_names;
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", K(ret), KP(session_info_));
  } else if (OB_FAIL(session_info_->get_name_case_mode(case_mode))) {
    LOG_WARN("fail to get name case mode", KR(ret));
  } else if (OB_FAIL(session_info_->get_collation_connection(cs_type))) {
    LOG_WARN("fail to get collation_connection", KR(ret));
  } else if (OB_FAIL(ObMViewExecutorUtil::split_table_list(arg.list_, mview_names))) {
    LOG_WARN("fail to split table list", KR(ret), K(arg.list_));
  }
  // resolve list
  for (int64_t i = 0; OB_SUCC(ret) && i < mview_names.count(); ++i) {
    const ObString &mview_name = mview_names.at(i);
    ObString database_name, table_name;
    bool has_synonym = false;
    ObString new_db_name, new_tbl_name;
    const ObTableSchema *table_schema = nullptr;
    if (OB_FAIL(ObMViewExecutorUtil::resolve_table_name(cs_type, case_mode, lib::is_oracle_mode(),
                                                        mview_name, database_name, table_name))) {
      LOG_WARN("fail to resolve table name", KR(ret), K(cs_type), K(case_mode), K(mview_name));
      LOG_USER_ERROR(OB_WRONG_TABLE_NAME, static_cast<int>(mview_name.length()), mview_name.ptr());
    } else if (database_name.empty() &&
                FALSE_IT(database_name = session_info_->get_database_name())) {
    } else if (OB_UNLIKELY(database_name.empty())) {
      ret = OB_ERR_NO_DB_SELECTED;
      LOG_WARN("No database selected", KR(ret));
    } else if (OB_FAIL(schema_checker_.get_table_schema_with_synonym(
                  tenant_id_, database_name, table_name, false /*is_index_table*/, has_synonym,
                  new_db_name, new_tbl_name, table_schema))) {
      if (OB_TABLE_NOT_EXIST == ret) {
        ret = OB_ERR_MVIEW_NOT_EXIST;
        ObSqlString db_buf;
        ObSqlString tbl_buf;
        if (OB_SUCCESS == db_buf.append(database_name)
            && OB_SUCCESS == tbl_buf.append(table_name)) {
          LOG_USER_ERROR(OB_ERR_MVIEW_NOT_EXIST, db_buf.ptr(), tbl_buf.ptr());
        }
      }
      LOG_WARN("fail to get table schema with synonym", KR(ret), K(database_name), K(table_name));
    } else if (OB_ISNULL(table_schema) || OB_UNLIKELY(!table_schema->is_materialized_view())) {
      ret = OB_ERR_MVIEW_NOT_EXIST;
      LOG_WARN("mview not exist", KR(ret), K(database_name), K(table_name), KPC(table_schema));
    } else if (OB_FAIL(mview_ids_.push_back(table_schema->get_table_id()))) {
      LOG_WARN("fail to push back", KR(ret));
    }
  }
  // resolve method
  for (int64_t i = 0; OB_SUCC(ret) && i < arg.method_.length(); ++i) {
    const char c = arg.method_.ptr()[i];
    if (OB_FAIL(ObMViewExecutorUtil::to_refresh_method(c, refresh_method))) {
      LOG_WARN("fail to parse refresh method", KR(ret));
    } else if (OB_FAIL(refresh_methods_.push_back(refresh_method))) {
      LOG_WARN("fail to push back", KR(ret));
    }
  }
  return ret;
}

int ObMViewExplainRefreshExecutor::do_explain_refresh(ObSqlString &output)
{
  int ret = OB_SUCCESS;
  int64_t step_no = 1;
  for (int64_t i = 0; OB_SUCC(ret) && i < mview_ids_.count(); ++i) {
    const uint64_t mview_id = mview_ids_.at(i);
    const ObMVRefreshMethod refresh_method =
      refresh_methods_.count() > i ? refresh_methods_.at(i) : ObMVRefreshMethod::MAX;
    if (OB_FAIL(explain_single_mview(mview_id, refresh_method, step_no, output))) {
      LOG_WARN("fail to explain single mview", KR(ret), K(mview_id));
    }
  }
  return ret;
}

int ObMViewExplainRefreshExecutor::do_explain_nested_refresh(ObSqlString &output)
{
  int ret = OB_SUCCESS;
  using namespace rootserver;
  const uint64_t target_mview_id = mview_ids_.at(0);
  ObSEArray<uint64_t, 4> nested_mview_ids;
  MViewDeps target_mview_deps;
  MViewDeps mview_reverse_deps;
  const uint64_t bucket_num = 16;
  ObMemAttr attr(tenant_id_, "MViewExplain");
  ObMViewMaintenanceService *mview_maintenance_service = MTL(ObMViewMaintenanceService*);
  int64_t step_no = 1;
  const ObMVRefreshMethod refresh_method =
    refresh_methods_.count() > 0 ? refresh_methods_.at(0) : ObMVRefreshMethod::MAX;
  if (OB_ISNULL(mview_maintenance_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mview maintenance service is nullptr", KR(ret));
  } else if (OB_FAIL(target_mview_deps.create(bucket_num, attr))) {
    LOG_WARN("fail to create mview deps", KR(ret));
  } else if (OB_FAIL(mview_reverse_deps.create(bucket_num, attr))) {
    LOG_WARN("fail to create mview reverse deps", KR(ret));
  } else if (OB_FAIL(mview_maintenance_service->get_target_nested_mview_deps(
                      target_mview_id, target_mview_deps))) {
    LOG_WARN("fail to get target nested mview deps", KR(ret));
  } else if (OB_FAIL(mview_maintenance_service->gen_target_nested_mview_topo_order(
                      target_mview_deps, mview_reverse_deps, nested_mview_ids))) {
    LOG_WARN("fail to gen target nested mview topo order", KR(ret));
  } else if (OB_FAIL(output.append("Nested hierarchy (refresh order):\n"))) {
    LOG_WARN("fail to append output", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < nested_mview_ids.count(); ++i) {
    ObSqlString full_name;
    if (OB_FAIL(get_mview_full_name(nested_mview_ids.at(i), full_name))) {
      LOG_WARN("fail to get mview full name", KR(ret));
    } else if (OB_FAIL(output.append_fmt("  %ld. %s\n", i + 1, full_name.ptr()))) {
      LOG_WARN("fail to append output", KR(ret));
    }
  }
  if (FAILEDx(output.append("\n"))) {
    LOG_WARN("fail to append output", KR(ret));
  }
  // explain each mview in topo order
  for (int64_t i = 0; OB_SUCC(ret) && i < nested_mview_ids.count(); ++i) {
    if (OB_FAIL(explain_single_mview(nested_mview_ids.at(i), refresh_method, step_no, output))) {
      LOG_WARN("fail to explain single mview", KR(ret), K(nested_mview_ids.at(i)));
    }
  }
  return ret;
}

int ObMViewExplainRefreshExecutor::explain_single_mview(const uint64_t mview_id,
                                                        const ObMVRefreshMethod refresh_method,
                                                        int64_t &step_no,
                                                        ObSqlString &output)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *mview_table_schema = nullptr;
  ObMViewInfo mview_info;
  ObSchemaGetterGuard schema_guard;
  SCN current_scn;
  ObArray<ObDependencyInfo> dependency_infos;
  ObArray<uint64_t> tables_need_mlog;
  ObScnRange mview_refresh_scn_range;
  ObScnRange base_table_scn_range;
  ObMVRefreshMethod final_method = refresh_method;
  ObMVProvider mv_provider(tenant_id_, mview_id);
  bool can_fast_refresh = false;
  const ObIArray<ObString> *operators = nullptr;
  ObMVRefreshType refresh_type = ObMVRefreshType::MAX;
  // temporarily aligns the session's sys vars with the mview's solidified vars while resolving
  // the refresh SQL; restored when this function returns
  ObExplainSessionVarGuard session_var_guard(session_info_);

  // 1. get schema and mview info
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("schema service is null", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, mview_id, mview_table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id_), K(mview_id));
  } else if (OB_ISNULL(mview_table_schema)) {
    ret = OB_ERR_MVIEW_NOT_EXIST;
    LOG_WARN("mview not exist", KR(ret), K(tenant_id_), K(mview_id));
  } else if (OB_UNLIKELY(!mview_table_schema->is_materialized_view())) {
    ret = OB_ERR_MVIEW_NOT_EXIST;
    LOG_WARN("table is not mview", KR(ret), K(tenant_id_), K(mview_id));
  } else if (OB_FAIL(session_var_guard.apply(mview_table_schema->get_local_session_var()))) {
    LOG_WARN("fail to apply mview solidified session vars", KR(ret), K(tenant_id_), K(mview_id));
  } else if (OB_FAIL(ObMViewRefreshHelper::get_current_scn(current_scn))) {
    LOG_WARN("fail to get current scn", KR(ret));
    // 1. get mview info
  } else if (OB_FAIL(ObMViewInfo::fetch_mview_info(*ctx_->get_sql_proxy(), tenant_id_,
                                              mview_id, mview_info))) {
    LOG_WARN("fail to fetch mview info", KR(ret), K(tenant_id_), K(mview_id));
    // 2. calc scn range
  } else if (OB_INVALID_SCN_VAL == mview_info.get_last_refresh_scn()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("last refresh scn is invalid", KR(ret), K(mview_info));
  } else if (OB_FAIL(mview_refresh_scn_range.start_scn_.convert_for_inner_table_field(
                      mview_info.get_last_refresh_scn()))) {
    LOG_WARN("fail to convert for inner table field", KR(ret), K(mview_info));
  } else {
    mview_refresh_scn_range.end_scn_ = current_scn;
    base_table_scn_range.start_scn_ = mview_refresh_scn_range.start_scn_;
    base_table_scn_range.end_scn_ = mview_refresh_scn_range.end_scn_;
    if (ObMVRefreshMethod::MAX == final_method) {
      final_method = mview_info.get_refresh_method();
    }
  }

  // 3. determine refresh type and generate sql
  if (OB_FAIL(ret)) {
  } else if (ObMVRefreshMode::NEVER == mview_info.get_refresh_mode()) {
    ret = OB_ERR_MVIEW_NEVER_REFRESH;
    LOG_WARN("mview never refresh", KR(ret), K(mview_info));
  } else if (OB_FAIL(mv_provider.get_mlog_mv_refresh_infos(session_info_,
                                                            &schema_guard,
                                                            base_table_scn_range.start_scn_,
                                                            base_table_scn_range.end_scn_,
                                                            &mview_refresh_scn_range.start_scn_,
                                                            &mview_refresh_scn_range.end_scn_,
                                                            dependency_infos,
                                                            tables_need_mlog,
                                                            can_fast_refresh,
                                                            operators))) {
    LOG_WARN("fail to get mlog mv refresh infos", KR(ret));
    // determine final refresh type
  } else if (ObMVRefreshMethod::COMPLETE == final_method ||
             (!can_fast_refresh && ObMVRefreshMethod::FORCE == final_method)) {
    refresh_type = ObMVRefreshType::COMPLETE;
  } else if (!can_fast_refresh && ObMVRefreshMethod::FAST == final_method) {
    ret = OB_ERR_MVIEW_CAN_NOT_FAST_REFRESH;
    LOG_WARN("mv can not fast refresh", KR(ret));
    LOG_USER_ERROR(OB_ERR_MVIEW_CAN_NOT_FAST_REFRESH, mview_table_schema->get_table_name(),
                   mv_provider.get_error_str().ptr());
  } else {
    refresh_type = can_fast_refresh ? ObMVRefreshType::FAST : ObMVRefreshType::COMPLETE;
  }

  // 4. collect and output sql
  if (OB_FAIL(ret)) {
  } else {
    ObSqlString full_name;
    const char *refresh_type_str = ObMVRefreshType::FAST == refresh_type ? "FAST" : "COMPLETE";
    if (OB_FAIL(get_mview_full_name(mview_id, full_name))) {
      LOG_WARN("fail to get mview full name", KR(ret), K(mview_id));
    } else if (ObMVRefreshType::COMPLETE == refresh_type) {
      // generate complete refresh sql using the same logic as actual refresh
      const uint64_t container_table_id = mview_table_schema->get_data_table_id();
      uint64_t data_version = 0;
      int64_t parallelism = 1;
      ObSEArray<ObBasedSchemaObjectInfo, 4> based_schema_object_infos;
      ObSqlString complete_sql;
      if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, data_version))) {
        LOG_WARN("fail to get data version", KR(ret), K(tenant_id_));
      } else if (OB_FAIL(ObMViewRefresher::collect_based_schema_object_infos(tenant_id_, 
                                                                             data_version, 
                                                                             schema_guard, 
                                                                             dependency_infos,
                                                                             based_schema_object_infos))) {
        LOG_WARN("fail to collect based schema object infos", KR(ret));
      } else if (OB_FAIL(ObMViewRefresher::calc_mv_refresh_parallelism(
                     mview_info.get_refresh_dop(), session_info_, parallelism))) {
        LOG_WARN("fail to calc mv refresh parallelism", KR(ret));
      } else if (OB_FAIL(rootserver::ObMViewUtils::generate_mview_complete_refresh_sql(
                     tenant_id_,
                     mview_id,
                     container_table_id,
                     schema_guard,
                     current_scn.get_val_for_inner_table_field(),
                     OB_INVALID_SCN_VAL,
                     1 /*execution_id*/,
                     1 /*task_id*/,
                     parallelism,
                     data_version >= DATA_VERSION_4_3_5_1,
                     based_schema_object_infos,
                     ObString(),
                     complete_sql))) {
        LOG_WARN("fail to generate mview complete refresh sql", KR(ret));
      } else {
        char sql_id_buf[OB_MAX_SQL_ID_LENGTH + 1] = {0};
        ObString sql_text(complete_sql.length(), complete_sql.ptr());
        if (OB_FAIL(ObSQLUtils::gen_sql_id_from_sql_string(*session_info_, sql_text,
                                                           sql_id_buf, sizeof(sql_id_buf)))) {
          LOG_WARN("fail to calc sql id", KR(ret));
        } else if (OB_FAIL(output.append_fmt("Step %ld:\n"
                             "  MView: %s\n"
                             "  Refresh Method: %s\n"
                             "  SQL_ID: %s\n"
                             "  SQL: %.*s\n\n",
                             step_no++, full_name.ptr(),
                             refresh_type_str,
                             sql_id_buf,
                             static_cast<int>(complete_sql.length()), complete_sql.ptr()))) {
          LOG_WARN("fail to append output", KR(ret));
        }
      }
    } else if (ObMVRefreshType::FAST == refresh_type) {
      if (OB_ISNULL(operators) || OB_UNLIKELY(operators->empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected empty refresh operators", KR(ret), KPC(operators));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < operators->count(); ++i) {
        const ObString &op_sql = operators->at(i);
        char sql_id_buf[OB_MAX_SQL_ID_LENGTH + 1] = {0};
        if (OB_FAIL(ObSQLUtils::gen_sql_id_from_sql_string(*session_info_, op_sql,
                                                           sql_id_buf, sizeof(sql_id_buf)))) {
          LOG_WARN("fail to calc sql id", KR(ret));
        } else if (OB_FAIL(output.append_fmt("Step %ld:\n"
                             "  MView: %s\n"
                             "  Refresh Method: %s\n"
                             "  SQL_ID: %s\n"
                             "  SQL: %.*s\n\n",
                             step_no++, full_name.ptr(),
                             refresh_type_str,
                             sql_id_buf,
                             static_cast<int>(op_sql.length()), op_sql.ptr()))) {
          LOG_WARN("fail to append output", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObMViewExplainRefreshExecutor::get_mview_full_name(const uint64_t mview_id,
                                                       ObSqlString &full_name)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  const ObDatabaseSchema *database_schema = nullptr;
  if (OB_FAIL(schema_checker_.get_table_schema(tenant_id_, mview_id, table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(mview_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_MVIEW_NOT_EXIST;
    LOG_WARN("table schema is null", KR(ret), K(mview_id));
  } else if (OB_FAIL(schema_checker_.get_database_schema(tenant_id_,
                     table_schema->get_database_id(), database_schema))) {
    LOG_WARN("fail to get database schema", KR(ret));
  } else if (OB_ISNULL(database_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database schema is null", KR(ret));
  } else if (OB_FAIL(full_name.assign_fmt("%.*s.%.*s",
                     static_cast<int>(database_schema->get_database_name_str().length()),
                     database_schema->get_database_name_str().ptr(),
                     static_cast<int>(table_schema->get_table_name_str().length()),
                     table_schema->get_table_name_str().ptr()))) {
    LOG_WARN("fail to assign full name", KR(ret));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
