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

#include "sql/optimizer/ob_direct_load_optimizer_ctx.h"
#include "sql/optimizer/ob_optimizer.h"
#include "sql/optimizer/ob_optimizer_context.h"
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
    load_level_(ObDirectLoadLevel::INVALID_LEVEL),
    dup_action_(ObLoadDupActionType::LOAD_INVALID_MODE),
    max_error_row_count_(0),
    need_sort_(false),
    is_backup_(false),
    is_mview_complete_refresh_(false),
    is_insert_overwrite_(false),
    is_enabled_(false),
    enable_inc_major_(false),
    is_optimized_by_default_load_mode_(false),
    can_use_direct_load_(false),
    use_direct_load_(false),
    disabled_by_transaction_(false),
    is_online_gather_statistics_(false),
    online_sample_percent_(1.0)
{
  column_ids_.set_tenant_id(MTL_ID());
}

void ObDirectLoadOptimizerCtx::reset()
{
  table_id_ = OB_INVALID_ID;
  load_method_ = ObDirectLoadMethod::INVALID_METHOD;
  insert_mode_ = ObDirectLoadInsertMode::INVALID_INSERT_MODE;
  load_mode_ = ObDirectLoadMode::INVALID_MODE;
  load_level_ = ObDirectLoadLevel::INVALID_LEVEL;
  dup_action_ = ObLoadDupActionType::LOAD_INVALID_MODE;
  max_error_row_count_ = 0;
  need_sort_ = false;
  is_backup_ = false;
  is_mview_complete_refresh_ = false;
  is_insert_overwrite_ = false;
  is_enabled_ = false;
  enable_inc_major_ = false;
  is_optimized_by_default_load_mode_ = false;
  can_use_direct_load_ = false;
  use_direct_load_ = false;
  disabled_by_transaction_ = false;
  is_online_gather_statistics_ = false;
  online_sample_percent_ = 1.0;
  column_ids_.reset();
}

int ObDirectLoadOptimizerCtx::check_exec_ctx(ObExecContext *exec_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(exec_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("exec_ctx cannot be null", K(ret));
  } else if (OB_ISNULL(exec_ctx->get_sql_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql ctx cannot be null", K(ret));
  } else if (OB_ISNULL(exec_ctx->get_sql_ctx()->schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema guard cannot be null", K(ret));
  } else if (OB_ISNULL(exec_ctx->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info cannot be null", K(ret));
  }
  return ret;
}

int ObDirectLoadOptimizerCtx::init_direct_load_ctx(ObExecContext *exec_ctx, ObLoadDataStmt &stmt)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  if (OB_FAIL(check_exec_ctx(exec_ctx))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to check exect ctx", KR(ret), KP(exec_ctx));
  } else {
    ObSQLSessionInfo *session_info = exec_ctx->get_my_session();
    ObLoadDataHint &load_data_hint = stmt.get_hints();
    ObDirectLoadHint &direct_load_hint = load_data_hint.get_direct_load_hint();
    int64_t append = 0;

    is_backup_ = ObLoadDataFormat::is_backup(stmt.get_load_arguments().access_info_.get_load_data_format());
    is_enabled_ = GCONF._ob_enable_direct_load;
    {
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
      enable_inc_major_ = tenant_config->_enable_inc_major_direct_load;
    }

    if (direct_load_hint.has_no_direct()) {
      LOG_TRACE("has no direct hint", K(direct_load_hint));
    } else if (direct_load_hint.has_direct()) {
      enable_by_direct_load_hint(direct_load_hint);
    } else if (OB_FAIL(load_data_hint.get_value(ObLoadDataHint::APPEND, append))) {
      LOG_WARN("fail to get APPEND", K(ret));
    } else if (append != 0) {
      enable_by_append_hint();
    } else if (!is_enabled_) {
      // do nothing
    } else if (!session_info->is_inner()) {
      enable_by_config(exec_ctx);
    }

    if (OB_FAIL(ret)) {
    } else if (!is_enabled_) {
      if (is_backup_) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "backup load with direct load disabled is");
        LOG_WARN("backup load with direct load disabled is not supported", KR(ret));
      }
    } else if (ObDirectLoadMethod::INVALID_METHOD == load_method_) {
      if (is_backup_) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "backup load using non-direct insert is");
        LOG_WARN("backup load using non-direct insert is not supported", KR(ret));
      }
    } else {
      table_id_ = stmt.get_load_arguments().table_id_;
      load_mode_ = ObDirectLoadMode::LOAD_DATA;
      load_level_ = stmt.get_part_ids().empty() ? ObDirectLoadLevel::TABLE
                                                : ObDirectLoadLevel::PARTITION;
      dup_action_ = stmt.get_load_arguments().dupl_action_;
      if (is_backup_) {
        need_sort_ = false; // 备份导入need_sort改写为false
      }
      // init column_ids_
      const ObTableSchema *table_schema = nullptr;
      if (OB_FAIL(ObTableLoadSchema::get_table_schema(*exec_ctx->get_sql_ctx()->schema_guard_,
                                                      tenant_id, table_id_, table_schema))) {
        LOG_WARN("fail to get table schema", KR(ret));
      } else if (is_backup_) {
        if (OB_FAIL(ObTableLoadSchema::get_column_ids(table_schema, column_ids_))) {
          LOG_WARN("fail to get column ids for backup", KR(ret));
        }
      } else if (stmt.get_default_table_columns()) {
        if (OB_FAIL(ObTableLoadSchema::get_user_column_ids(table_schema, column_ids_))) {
          LOG_WARN("fail to get user column ids", KR(ret));
        }
      } else {
        const ObIArray<ObLoadDataStmt::FieldOrVarStruct> &field_or_var_list =
          stmt.get_field_or_var_list();
        for (int64_t i = 0; i < field_or_var_list.count(); ++i) {
          const ObLoadDataStmt::FieldOrVarStruct &field_or_var_struct = field_or_var_list.at(i);
          if (OB_UNLIKELY(!field_or_var_struct.is_table_column_)) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("var is not supported", KR(ret), K(field_or_var_struct), K(i),
                      K(field_or_var_list));
          } else if (OB_FAIL(column_ids_.push_back(field_or_var_struct.column_id_))) {
            LOG_WARN("fail to push back column id", KR(ret));
          }
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(check_semantics())) {
        LOG_WARN("fail to check semantics", K(ret));
      } else if (OB_FAIL(ObTableLoadService::check_support_direct_load(
                    *exec_ctx->get_sql_ctx()->schema_guard_,
                    table_id_,
                    load_method_,
                    insert_mode_,
                    load_mode_,
                    load_level_,
                    column_ids_,
                    enable_inc_major_))) {
        LOG_WARN("fail to check support direct load", K(ret));
      } else {
        can_use_direct_load_ = true;
      }
    }

    if (ret == OB_NOT_SUPPORTED) {
      bool allow_fallback = false;
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(check_direct_load_allow_fallback(*this, exec_ctx, allow_fallback))) {
        LOG_WARN("fail to check support direct load allow fallback", K(tmp_ret));
      } else if (allow_fallback) {
        LOG_INFO("direct load has been allowed fallback");
        ret = OB_SUCCESS;
        ObWarningBuffer *buf = common::ob_get_tsi_warning_buffer();
        if (NULL != buf) {
          buf->reset_err();
        }
      }
    }

    LOG_INFO("init direct load optimizer ctx result", K(ret), K(direct_load_hint), K(append), KPC(this));
  }
  return ret;
}

int ObDirectLoadOptimizerCtx::init_direct_load_ctx(
    const ObInsertStmt &stmt,
    ObOptimizerContext &optimizer_ctx)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  ObExecContext *exec_ctx = optimizer_ctx.get_exec_ctx();
  if (OB_UNLIKELY(!stmt.value_from_select() || stmt.is_external_table_overwrite() || stmt.is_ignore() || stmt.is_insert_up())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected init direct load", KR(ret), K(stmt));
  } else if (OB_ISNULL(stmt.get_table_item(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table item is null", KR(ret), K(stmt));
  } else if (OB_FAIL(check_exec_ctx(exec_ctx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to check exec ctx", K(ret), KP(exec_ctx));
  } else {
    ObSQLSessionInfo *session_info = exec_ctx->get_my_session();
    const ObGlobalHint &global_hint = optimizer_ctx.get_global_hint();
    const ObDirectLoadHint &direct_load_hint = global_hint.direct_load_hint_;
    const bool enable_optimizer = stmt.get_query_ctx()->optimizer_features_enable_version_ >= COMPAT_VERSION_4_3_4;

    is_mview_complete_refresh_ = session_info->get_ddl_info().is_mview_complete_refresh();
    is_insert_overwrite_ = stmt.is_normal_table_overwrite();
    is_enabled_ = GCONF._ob_enable_direct_load;
    {
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
      enable_inc_major_ = tenant_config->_enable_inc_major_direct_load;
    }

    if (is_insert_overwrite_) {
      if (OB_FAIL(check_support_insert_overwrite(global_hint))) {
        LOG_WARN("fail to check support insert overwrite", K(ret), K(global_hint));
      } else {
        enable_by_overwrite();
      }
    } else if (direct_load_hint.has_no_direct()) {
      LOG_TRACE("has no direct hint", K(direct_load_hint));
    } else if (direct_load_hint.has_direct()) {
      enable_by_direct_load_hint(direct_load_hint);
    } else if (global_hint.has_append()) {
      enable_by_append_hint();
    } else if (!is_enabled_) {
      // do nothing
    } else if (!session_info->is_inner()) {
      if (enable_optimizer) {
        enable_by_config(exec_ctx);
      }
    }

    if (OB_FAIL(ret)) {
    } else if (!is_enabled_) {
      if (is_mview_complete_refresh_) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "mview complete refresh with direct load disabled is");
        LOG_WARN("mview complete refresh with direct load disabled is not support", KR(ret));
      }
    } else if (ObDirectLoadMethod::INVALID_METHOD == load_method_) {
      if (is_mview_complete_refresh_) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "mview complete refresh using non-direct insert is");
        LOG_WARN("mview complete refresh using non-direct insert is not support", KR(ret));
      }
    } else {
      if (is_mview_complete_refresh_) {
        insert_mode_ = ObDirectLoadInsertMode::OVERWRITE;
      }
      if (insert_mode_ == ObDirectLoadInsertMode::INC_REPLACE) {
        dup_action_ = ObLoadDupActionType::LOAD_REPLACE; // 改写成replace语义
      }

      table_id_ = stmt.get_table_item(0)->ref_id_;
      load_mode_ = is_insert_overwrite_ ? ObDirectLoadMode::INSERT_OVERWRITE : ObDirectLoadMode::INSERT_INTO;
      load_level_ = stmt.get_table_item(0)->part_ids_.empty() ? ObDirectLoadLevel::TABLE : ObDirectLoadLevel::PARTITION;
      dup_action_ = ObLoadDupActionType::LOAD_STOP_ON_DUP;
      // init column_ids_
      const ObTableSchema *table_schema = nullptr;
      if (OB_FAIL(ObTableLoadSchema::get_table_schema(*exec_ctx->get_sql_ctx()->schema_guard_,
                                                      tenant_id,
                                                      table_id_,
                                                      table_schema))) {
        LOG_WARN("fail to get table schema", KR(ret));
      } else if (OB_FAIL(ObTableLoadSchema::get_column_ids(table_schema, column_ids_))) {
        LOG_WARN("fail to get column ids", KR(ret));
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(check_transaction(session_info))) {
        LOG_WARN("fail to get auto commit", KR(ret));
      } else if (OB_FAIL(ObTableLoadService::check_support_direct_load(
                                                    *exec_ctx->get_sql_ctx()->schema_guard_,
                                                    table_id_,
                                                    load_method_,
                                                    insert_mode_,
                                                    load_mode_,
                                                    load_level_,
                                                    column_ids_,
                                                    enable_inc_major_))) {
        LOG_WARN("fail to check support direct load", K(ret));
      } else {
        can_use_direct_load_ = true;
      }

      if (ret == OB_NOT_SUPPORTED) {
        bool allow_fallback = false;
        int tmp_ret = OB_SUCCESS;
        if (enable_optimizer && OB_TMP_FAIL(check_direct_load_allow_fallback(*this, exec_ctx, allow_fallback))) {
          LOG_WARN("fail to check support direct load allow fallback", K(tmp_ret));
        } else if (allow_fallback) {
          LOG_INFO("direct load has been allowed fallback");
          ret = OB_SUCCESS;
          ObWarningBuffer *buf = common::ob_get_tsi_warning_buffer();
          if (NULL != buf) {
            buf->reset_err();
          }
        }
      }
    }

    LOG_INFO("init direct load optimizer ctx result", K(ret), K(direct_load_hint), K(global_hint.has_append()), K(enable_optimizer), KPC(this));

    if (can_use_direct_load_) {
      // Only for direct load
      DEBUG_SYNC(AFTER_DIRECT_LOAD_FIRST_CHECK_IS_SUPPORT);
    }

  }
  return ret;
}

void ObDirectLoadOptimizerCtx::enable_by_direct_load_hint(const ObDirectLoadHint &hint)
{
  need_sort_ = hint.need_sort_;
  insert_mode_ = ObDirectLoadInsertMode::NORMAL;
  max_error_row_count_ = hint.get_max_error_row_count();
  if (hint.is_full_direct_load()) {
    load_method_ = ObDirectLoadMethod::FULL;
  } else if (hint.is_inc_direct_load()) {
    load_method_ = ObDirectLoadMethod::INCREMENTAL;
    if (hint.is_inc_replace_load_method()) {
      insert_mode_ = ObDirectLoadInsertMode::INC_REPLACE;
    }
  }
}

void ObDirectLoadOptimizerCtx::enable_by_append_hint()
{
  need_sort_ = true;
  insert_mode_ = ObDirectLoadInsertMode::NORMAL;
  load_method_ = ObDirectLoadMethod::FULL;
}

void ObDirectLoadOptimizerCtx::enable_by_config(ObExecContext *exec_ctx)
{
  if (OB_UNLIKELY(exec_ctx->get_table_direct_insert_ctx().get_force_inc_direct_write())) {
    insert_mode_ = ObDirectLoadInsertMode::NORMAL;
    need_sort_ = true;
    load_method_ = ObDirectLoadMethod::INCREMENTAL;
    is_optimized_by_default_load_mode_ = true;
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    const ObString &config_str = tenant_config->default_load_mode.get_value_string();
    need_sort_ = true;
    insert_mode_ = ObDirectLoadInsertMode::NORMAL;
    if (tenant_config.is_valid()) {
      if (0 == config_str.case_compare("FULL_DIRECT_WRITE")) {
        load_method_ = ObDirectLoadMethod::FULL;
      } else if (0 == config_str.case_compare("INC_DIRECT_WRITE")) {
        load_method_ = ObDirectLoadMethod::INCREMENTAL;
      } else if (0 == config_str.case_compare("INC_REPLACE_DIRECT_WRITE")) {
        load_method_ = ObDirectLoadMethod::INCREMENTAL;
        insert_mode_ = ObDirectLoadInsertMode::INC_REPLACE;
      }
      if (load_method_ != ObDirectLoadMethod::INVALID_METHOD) {
        is_optimized_by_default_load_mode_ = true;
      }
    }
  }
}

void ObDirectLoadOptimizerCtx::enable_by_overwrite()
{
  need_sort_ = true;
  load_method_ =  ObDirectLoadMethod::FULL;
  insert_mode_ = ObDirectLoadInsertMode::OVERWRITE;
  dup_action_ = ObLoadDupActionType::LOAD_STOP_ON_DUP;
}

int ObDirectLoadOptimizerCtx::check_semantics()
{
  int ret = OB_SUCCESS;
  if (is_inc_direct_load()) {
    if (dup_action_ == ObLoadDupActionType::LOAD_REPLACE) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "replace for inc load method in direct load is");
    }
  } else if (is_inc_replace_direct_load()) {
    if (dup_action_ != ObLoadDupActionType::LOAD_STOP_ON_DUP) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "replace or ignore for inc_replace load method in direct load is");
    } else {
      dup_action_ = ObLoadDupActionType::LOAD_REPLACE; // 改写成replace语义
    }
  }
  return ret;
}

int ObDirectLoadOptimizerCtx::check_transaction(ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  bool auto_commit = false;
  if (OB_FAIL(session_info->get_autocommit(auto_commit))) {
    LOG_WARN("failed to get auto commit", K(ret));
  } else if (!auto_commit || session_info->is_in_transaction()) {
    disabled_by_transaction_ = true;
    if (is_insert_overwrite()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "using insert overwrite within a transaction is");
      LOG_WARN("insert overwrite within a transaction is not support", KR(ret), K(auto_commit), K(session_info->is_in_transaction()));
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "using full or inc direct-insert within a transaction is");
      LOG_WARN("using full or inc direct-insert within a transaction is not support", KR(ret), K(auto_commit), K(session_info->is_in_transaction()));
    }
  }
  return ret;
}

// For insert overwrite select
// 1. not allow add no_direct hint
// 2. not allow add direct load hint
// 3. not allow add append hint
int ObDirectLoadOptimizerCtx::check_support_insert_overwrite(const ObGlobalHint &global_hint)
{
  int ret = OB_SUCCESS;
  const ObDirectLoadHint &direct_load_hint = global_hint.direct_load_hint_;
  if (!is_enabled_) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "insert overwrite stmt with direct load disabled is");
    LOG_WARN("insert overwrite stmt with direct load disabled is not support", KR(ret));
  } else if (direct_load_hint.has_no_direct()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "insert overwrite stmt with no_direct hint");
    LOG_WARN("insert overwrite stmt with no_direct hint is not support", KR(ret));
  } else if (direct_load_hint.has_direct()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "insert overwrite stmt with direct load hint");
    LOG_WARN("insert overwrite stmt with direct load hint is not support", KR(ret));
  } else if (global_hint.has_append()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "insert overwrite stmt with append hint");
    LOG_WARN("insert overwrite stmt with append hint is not support", KR(ret));
  }
  return ret;
}

int ObDirectLoadOptimizerCtx::check_direct_load_allow_fallback(
    const ObDirectLoadOptimizerCtx &optimize_ctx,
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
  } else if (optimize_ctx.is_backup()) {
    allow_fallback = false;
  } else if (session_info->get_ddl_info().is_mview_complete_refresh()) {
    allow_fallback = false;
  } else if (optimize_ctx.is_insert_overwrite()) {
    allow_fallback = false;
  } else if (tenant_config.is_valid()) {
    allow_fallback = tenant_config->direct_load_allow_fallback;
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
