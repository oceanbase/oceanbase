/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_ENG

#include "sql/optimizer/ob_direct_load_optimizer.h"
#include "sql/optimizer/ob_optimizer.h"
#include "sql/optimizer/ob_optimizer_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/dml/ob_insert_stmt.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/ob_table_load_schema.h"

namespace oceanbase
{
namespace sql
{
using namespace common;
using namespace observer;
using namespace storage;
using namespace share;

DEFINE_ENUM_FUNC(ObDefaultLoadMode::Type, type, OB_DEFAULT_LOAD_MODE_DEF, ObDefaultLoadMode::);

ObDirectLoadOptimizerCtx::ObDirectLoadOptimizerCtx()
  : table_id_(OB_INVALID_ID),
    load_method_(ObDirectLoadMethod::INVALID_METHOD),
    insert_mode_(ObDirectLoadInsertMode::INVALID_INSERT_MODE),
    load_mode_(ObDirectLoadMode::INVALID_MODE),
    dup_action_(ObLoadDupActionType::LOAD_INVALID_MODE),
    max_error_row_count_(0),
    need_sort_(false),
    can_use_direct_load_(false),
    use_direct_load_(false),
    is_optimized_(false)
{
}

void ObDirectLoadOptimizerCtx::reset()
{
  table_id_ = OB_INVALID_ID;
  load_method_ = ObDirectLoadMethod::INVALID_METHOD;
  insert_mode_ = ObDirectLoadInsertMode::INVALID_INSERT_MODE;
  load_mode_ = ObDirectLoadMode::INVALID_MODE;
  dup_action_ = ObLoadDupActionType::LOAD_INVALID_MODE;
  max_error_row_count_ = 0;
  need_sort_ = false;
  can_use_direct_load_ = false;
  use_direct_load_ = false;
  is_optimized_ = false;
}

int ObDirectLoadOptimizer::optimize(ObExecContext *exec_ctx, ObLoadDataStmt &stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(exec_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("exec_ctx cannot be null", K(ret));
  } else if (GCONF._ob_enable_direct_load) {
    direct_load_optimizer_ctx_.reset();
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    ObLoadDataHint &load_data_hint = stmt.get_hints();
    ObDirectLoadHint &direct_load_hint = load_data_hint.get_direct_load_hint();
    int64_t append = 0;
    ObSQLSessionInfo *session_info = nullptr;
    if (OB_ISNULL(session_info = exec_ctx->get_my_session())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected session info is null", K(ret));
    } else if (direct_load_hint.has_no_direct()) {
      // do nothing
    } else if (direct_load_hint.has_direct()) {
      enable_by_direct_load_hint(direct_load_hint);
    } else if (OB_FAIL(load_data_hint.get_value(ObLoadDataHint::APPEND, append))) {
      LOG_WARN("fail to get APPEND", K(ret));
    } else if (append != 0) {
      enable_by_append_hint();
    } else if (!session_info->is_inner()) {
      enable_by_config();
    }
    if (OB_SUCC(ret)) {
      if (direct_load_optimizer_ctx_.load_method_ != ObDirectLoadMethod::INVALID_METHOD) {
        direct_load_optimizer_ctx_.table_id_ = stmt.get_load_arguments().table_id_;
        direct_load_optimizer_ctx_.load_mode_ = ObDirectLoadMode::LOAD_DATA;
        direct_load_optimizer_ctx_.dup_action_ = stmt.get_load_arguments().dupl_action_;
        if (OB_FAIL(check_semantics())) {
          LOG_WARN("fail to check semantics", K(ret));
        } else if (OB_FAIL(check_support_direct_load(exec_ctx, ObDirectLoadLevel::TABLE))) {
          LOG_WARN("fail to check support direct load", K(ret));
        } else {
          direct_load_optimizer_ctx_.dup_action_ = direct_load_optimizer_ctx_.insert_mode_ == ObDirectLoadInsertMode::INC_REPLACE ?
              ObLoadDupActionType::LOAD_REPLACE : stmt.get_load_arguments().dupl_action_;   //改写成replace语义
          direct_load_optimizer_ctx_.set_use_direct_load();
        }
        if (ret == OB_NOT_SUPPORTED) {
          bool allow_fallback = false;
          int tmp_ret = OB_SUCCESS;
          if (OB_TMP_FAIL(check_direct_load_allow_fallback(exec_ctx, allow_fallback))) {
            LOG_WARN("fail to check support direct load allow fallback", K(tmp_ret));
          } else if (allow_fallback) {
            LOG_INFO("direct load has been allowed fallback");
            ret = OB_SUCCESS;
          }
        }
      }
    }
    LOG_INFO("direct load optimize result", K(ret), K(direct_load_hint), K(direct_load_optimizer_ctx_));
  }
  return ret;
}

int ObDirectLoadOptimizer::optimize(
    const ObInsertStmt &stmt,
    ObOptimizerContext &optimizer_ctx,
    const int64_t dml_parallel)
{
  int ret = OB_SUCCESS;
  ObExecContext *exec_ctx = nullptr;
  if (OB_ISNULL(exec_ctx = optimizer_ctx.get_exec_ctx())) {
    ret = ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec_ctx cannot be null", K(ret));
  } else if (GCONF._ob_enable_direct_load) {
    direct_load_optimizer_ctx_.reset();
    uint64_t table_id = stmt.get_table_item(0) != nullptr ? stmt.get_table_item(0)->ref_id_ : 0;
    const ObGlobalHint &global_hint = optimizer_ctx.get_global_hint();
    const ObDirectLoadHint &direct_load_hint = global_hint.direct_load_hint_;
    ObSQLSessionInfo *session_info = nullptr;
    if (OB_ISNULL(session_info = exec_ctx->get_my_session())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected session info is null", K(ret));
    } else if (0 != table_id && (stmt.is_overwrite() || stmt.value_from_select())) {
      if (stmt.is_overwrite()) {
        if (OB_FAIL(check_support_insert_overwrite(global_hint))) {
          LOG_WARN("fail to check support insert overwrite", K(ret), K(global_hint));
        } else {
          enable_by_overwrite();
        }
      } else if (direct_load_hint.has_no_direct()) {
        // do nothing
      } else if (direct_load_hint.has_direct()) {
        enable_by_direct_load_hint(direct_load_hint);
      } else if (global_hint.has_append()) {
        enable_by_append_hint();
      } else if (!session_info->is_inner()) {
        if (stmt.get_query_ctx()->optimizer_features_enable_version_ >= COMPAT_VERSION_4_3_4) {
          enable_by_config();
        }
      }
      if (OB_FAIL(ret)) {
      } else if (dml_parallel <= ObGlobalHint::DEFAULT_PARALLEL && !direct_load_optimizer_ctx_.is_optimized()) {
        // 通过hint而不是默认配置项的方式，不会修改并行度，当并行度小于2时不满足pdml条件，无需走旁路导入检查
        // do nothing
      } else if (direct_load_optimizer_ctx_.load_method_ != ObDirectLoadMethod::INVALID_METHOD) {
        ObDirectLoadLevel::Type load_level = ObDirectLoadLevel::TABLE;
        if (session_info->get_ddl_info().is_mview_complete_refresh()) {
          if (direct_load_optimizer_ctx_.insert_mode_ == ObDirectLoadInsertMode::INC_REPLACE) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected mview complete refresh enable inc replace", K(ret));
          } else {
            direct_load_optimizer_ctx_.insert_mode_ = ObDirectLoadInsertMode::OVERWRITE;
          }
        }
        if (OB_SUCC(ret)) {
          direct_load_optimizer_ctx_.table_id_ = table_id;
          direct_load_optimizer_ctx_.load_mode_ = ObDirectLoadMode::INSERT_INTO;
          direct_load_optimizer_ctx_.dup_action_ = direct_load_optimizer_ctx_.insert_mode_ == ObDirectLoadInsertMode::INC_REPLACE ?
              ObLoadDupActionType::LOAD_REPLACE : ObLoadDupActionType::LOAD_STOP_ON_DUP;
          ObIArray<ObTablePartitionInfo *> & table_partition_infos = optimizer_ctx.get_table_partition_info();
          for (int64_t i = 0; OB_SUCC(ret) && i < table_partition_infos.count(); ++i) {
            const ObTablePartitionInfo *info = table_partition_infos.at(i);
            if (OB_ISNULL(info)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpect null table partition info", K(ret));
            } else if (info->get_ref_table_id() == table_id) {
              if (info->get_table_location().get_part_hint_ids().count() > 0) {
                load_level = ObDirectLoadLevel::PARTITION;
              }
              break;
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(check_support_direct_load(exec_ctx, load_level))) {
            LOG_WARN("fail to check support direct load", K(ret));
            bool allow_fallback = false;
            if (ret == OB_NOT_SUPPORTED && stmt.get_query_ctx()->optimizer_features_enable_version_ >= COMPAT_VERSION_4_3_4) {
              int tmp_ret = OB_SUCCESS;
              if (OB_TMP_FAIL(check_direct_load_allow_fallback(exec_ctx, allow_fallback))) {
                LOG_WARN("fail to check support direct load allow fallback", K(tmp_ret));
              } else if (allow_fallback) {
                LOG_INFO("direct load has been allowed fallback");
                ret = OB_SUCCESS;
              }
            }
          } else {
            direct_load_optimizer_ctx_.set_can_use_direct_load();
          }
        }
      }
    }
    LOG_INFO("direct load optimize result", K(ret), K(direct_load_hint), K(direct_load_optimizer_ctx_));
  }
  return ret;
}

void ObDirectLoadOptimizer::enable_by_direct_load_hint(const ObDirectLoadHint &hint)
{
  direct_load_optimizer_ctx_.need_sort_ = hint.need_sort_;
  direct_load_optimizer_ctx_.insert_mode_ = ObDirectLoadInsertMode::NORMAL;
  direct_load_optimizer_ctx_.max_error_row_count_ = hint.get_max_error_row_count();
  if (hint.is_full_direct_load()) {
    direct_load_optimizer_ctx_.load_method_ = ObDirectLoadMethod::FULL;
  } else if (hint.is_inc_direct_load()) {
    direct_load_optimizer_ctx_.load_method_ = ObDirectLoadMethod::INCREMENTAL;
    if (hint.is_inc_replace_load_method()) {
      direct_load_optimizer_ctx_.insert_mode_ = ObDirectLoadInsertMode::INC_REPLACE;
    }
  }
}

void ObDirectLoadOptimizer::enable_by_append_hint()
{
  direct_load_optimizer_ctx_.need_sort_ = true;
  direct_load_optimizer_ctx_.insert_mode_ = ObDirectLoadInsertMode::NORMAL;
  direct_load_optimizer_ctx_.load_method_ = ObDirectLoadMethod::FULL;
}

void ObDirectLoadOptimizer::enable_by_config()
{
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  const ObString &config_str = tenant_config->default_load_mode.get_value_string();
  direct_load_optimizer_ctx_.need_sort_ = true;
  direct_load_optimizer_ctx_.insert_mode_ = ObDirectLoadInsertMode::NORMAL;
  if (tenant_config.is_valid()) {
    if (0 == config_str.case_compare("FULL_DIRECT_WRITE")) {
      direct_load_optimizer_ctx_.load_method_ = ObDirectLoadMethod::FULL;
    } else if (0 == config_str.case_compare("INC_DIRECT_WRITE")) {
      direct_load_optimizer_ctx_.load_method_ = ObDirectLoadMethod::INCREMENTAL;
    } else if (0 == config_str.case_compare("INC_REPLACE_DIRECT_WRITE")) {
      direct_load_optimizer_ctx_.load_method_ = ObDirectLoadMethod::INCREMENTAL;
      direct_load_optimizer_ctx_.insert_mode_ = ObDirectLoadInsertMode::INC_REPLACE;
    }
    if (direct_load_optimizer_ctx_.load_method_ != ObDirectLoadMethod::INVALID_METHOD) {
      direct_load_optimizer_ctx_.is_optimized_ = true;
    }
  }
}

void ObDirectLoadOptimizer::enable_by_overwrite()
{
  direct_load_optimizer_ctx_.need_sort_ = true;
  direct_load_optimizer_ctx_.load_method_ =  ObDirectLoadMethod::FULL;
  direct_load_optimizer_ctx_.insert_mode_ = ObDirectLoadInsertMode::OVERWRITE;
  direct_load_optimizer_ctx_.load_mode_ = ObDirectLoadMode::INSERT_OVERWRITE;
  direct_load_optimizer_ctx_.dup_action_ = ObLoadDupActionType::LOAD_STOP_ON_DUP;
}

int ObDirectLoadOptimizer::check_semantics()
{
  int ret = OB_SUCCESS;
  if (direct_load_optimizer_ctx_.is_inc_direct_load()) {
    if (direct_load_optimizer_ctx_.dup_action_ == ObLoadDupActionType::LOAD_REPLACE) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "replace for inc load method in direct load is");
    }
  } else if (direct_load_optimizer_ctx_.is_inc_replace_direct_load()) {
    if (direct_load_optimizer_ctx_.dup_action_ != ObLoadDupActionType::LOAD_STOP_ON_DUP) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "replace or ignore for inc_replace load method in direct load is");
    }
  }
  return ret;
}

// For insert overwrite select
// 1. not allow add no_direct hint
// 2. not allow add direct load hint
// 3. not allow add append hint
int ObDirectLoadOptimizer::check_support_insert_overwrite(const ObGlobalHint &global_hint)
{
  int ret = OB_SUCCESS;
  const ObDirectLoadHint &direct_load_hint = global_hint.direct_load_hint_;
  if (direct_load_hint.has_no_direct()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "insert overwrite stmt with no_direct hint");
  } else if (direct_load_hint.has_direct()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "insert overwrite stmt with direct load hint");
  } else if (global_hint.has_append()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "insert overwrite stmt with append hint");
  }
  return ret;
}

int ObDirectLoadOptimizer::check_support_direct_load(
    ObExecContext *exec_ctx,
    storage::ObDirectLoadLevel::Type load_level)
{
  int ret = OB_SUCCESS;
  ObSqlCtx *sql_ctx = nullptr;
  ObSchemaGetterGuard *schema_guard = nullptr;
  const ObTableSchema *table_schema = nullptr;
  ObArray<uint64_t> column_ids;
  if (OB_ISNULL(sql_ctx = exec_ctx->get_sql_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get sql ctx", K(ret), KP(exec_ctx));
  } else if (OB_ISNULL(schema_guard = sql_ctx->schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get schema guard", K(ret), KP(sql_ctx));
  } else {
    // insert overwrite和insert into select全量不支持autocommit=false和session在事务内
    if (direct_load_optimizer_ctx_.is_insert_overwrite() || (direct_load_optimizer_ctx_.is_insert_into() && direct_load_optimizer_ctx_.is_full_direct_load())) {
      bool auto_commit = false;
      ObSQLSessionInfo *session_info = nullptr;
      if (OB_ISNULL(session_info = exec_ctx->get_my_session())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected session info is null", K(ret));
      } else if (OB_FAIL(session_info->get_autocommit(auto_commit))) {
        LOG_WARN("failed to get auto commit", K(ret));
      } else if (!auto_commit || session_info->is_in_transaction()) {
        if (direct_load_optimizer_ctx_.is_insert_overwrite()) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "using insert overwrite within a transaction is");
          LOG_WARN("insert overwrite within a transaction is not support", KR(ret), K(auto_commit), K(session_info->is_in_transaction()));
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "using full direct-insert within a transaction is");
          LOG_WARN("using full direct-insert within a transaction is not support", KR(ret), K(auto_commit), K(session_info->is_in_transaction()));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObTableLoadSchema::get_table_schema(
        *schema_guard, MTL_ID(), direct_load_optimizer_ctx_.table_id_, table_schema))) {
      LOG_WARN("fail to get table schema", KR(ret));
    } else if (OB_FAIL(ObTableLoadSchema::get_column_ids(table_schema, column_ids))) {
      LOG_WARN("fail to get column ids", KR(ret));
    } else if (OB_FAIL(ObTableLoadService::check_support_direct_load(
        *schema_guard,
        direct_load_optimizer_ctx_.table_id_,
        direct_load_optimizer_ctx_.load_method_,
        direct_load_optimizer_ctx_.insert_mode_,
        direct_load_optimizer_ctx_.load_mode_,
        load_level,
        column_ids))) {
      LOG_WARN("fail to check support direct load", K(ret), K(direct_load_optimizer_ctx_));
    }
  }
  return ret;
}

int ObDirectLoadOptimizer::check_direct_load_allow_fallback(
    ObExecContext *exec_ctx,
    bool &allow_fallback)
{
  int ret = OB_SUCCESS;
  allow_fallback = true;
  ObSQLSessionInfo *session_info = nullptr;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (OB_ISNULL(session_info = exec_ctx->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected session info is null", K(ret));
  } else if (session_info->get_ddl_info().is_mview_complete_refresh()) {
    allow_fallback = false;
  } else if (direct_load_optimizer_ctx_.is_insert_overwrite()) {
    allow_fallback = false;
  } else if (tenant_config.is_valid()) {
    allow_fallback = tenant_config->direct_load_allow_fallback;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
