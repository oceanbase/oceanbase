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

#define USING_LOG_PREFIX SQL_SESSION
#include "sql/privilege_check/ob_privilege_check.h"

#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/ob_sql_context.h"
#include "sql/resolver/ddl/ob_explain_stmt.h"
#include "sql/resolver/ddl/ob_create_table_stmt.h"
#include "sql/resolver/ddl/ob_create_index_stmt.h"
#include "sql/resolver/ddl/ob_create_mlog_stmt.h"
#include "sql/resolver/ddl/ob_drop_mlog_stmt.h"
#include "sql/resolver/ddl/ob_create_database_stmt.h"
#include "sql/resolver/ddl/ob_alter_table_stmt.h"
#include "sql/resolver/ddl/ob_sequence_stmt.h"
#include "sql/resolver/ddl/ob_create_outline_stmt.h"
#include "sql/resolver/ddl/ob_alter_outline_stmt.h"
#include "sql/resolver/ddl/ob_drop_outline_stmt.h"
#include "sql/resolver/ddl/ob_drop_database_stmt.h"
#include "sql/resolver/ddl/ob_drop_index_stmt.h"
#include "sql/resolver/ddl/ob_lock_tenant_stmt.h"
#include "sql/resolver/ddl/ob_drop_tenant_stmt.h"
#include "sql/resolver/dcl/ob_create_user_stmt.h"
#include "sql/resolver/ddl/ob_drop_table_stmt.h"
#include "sql/resolver/dcl/ob_drop_user_stmt.h"
#include "sql/resolver/dcl/ob_lock_user_stmt.h"
#include "sql/resolver/dcl/ob_rename_user_stmt.h"
#include "sql/resolver/dcl/ob_revoke_stmt.h"
#include "sql/resolver/dcl/ob_set_password_stmt.h"
#include "sql/resolver/dml/ob_delete_stmt.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/dml/ob_update_stmt.h"
#include "sql/resolver/dml/ob_merge_stmt.h"
#include "sql/resolver/dml/ob_insert_stmt.h"
#include "sql/resolver/dcl/ob_grant_stmt.h"
#include "sql/resolver/dcl/ob_revoke_stmt.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/resolver/cmd/ob_variable_set_stmt.h"
#include "sql/resolver/ddl/ob_modify_tenant_stmt.h"
#include "sql/resolver/ddl/ob_alter_database_stmt.h"
#include "sql/resolver/ddl/ob_truncate_table_stmt.h"
#include "sql/resolver/ddl/ob_rename_table_stmt.h"
#include "sql/resolver/ddl/ob_create_table_like_stmt.h"
#include "sql/resolver/cmd/ob_set_names_stmt.h"
#include "sql/resolver/ddl/ob_create_tablegroup_stmt.h"
#include "sql/resolver/ddl/ob_drop_tablegroup_stmt.h"
#include "sql/resolver/ddl/ob_alter_tablegroup_stmt.h"
#include "sql/resolver/ddl/ob_flashback_stmt.h"
#include "sql/resolver/ddl/ob_purge_stmt.h"
#include "sql/resolver/ddl/ob_create_synonym_stmt.h"
#include "sql/resolver/ddl/ob_drop_synonym_stmt.h"
#include "sql/resolver/cmd/ob_call_procedure_stmt.h"
#include "sql/resolver/cmd/ob_load_data_stmt.h"
#include "sql/resolver/ddl/ob_create_routine_stmt.h"
#include "sql/resolver/ddl/ob_alter_routine_stmt.h"
#include "sql/resolver/ddl/ob_drop_routine_stmt.h"
#include "rootserver/ob_ddl_service.h"
#include "sql/resolver/dml/ob_merge_stmt.h"
#include "sql/privilege_check/ob_ora_priv_check.h"
#include "sql/resolver/dcl/ob_alter_user_profile_stmt.h"
#include "pl/ob_pl_stmt.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/optimizer/ob_optimizer_util.h"

namespace oceanbase {
using namespace share;
using namespace share::schema;
using namespace common;
namespace sql {
#define ADD_NEED_PRIV(need_priv)                                              \
    if (OB_SUCC(ret) && OB_FAIL(need_privs.push_back(need_priv))) {           \
      LOG_WARN("Fail to add need_priv", K(need_priv), K(ret));                \
    }

int err_stmt_type_priv(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  UNUSED(session_priv);
  UNUSED(need_privs);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should not be NULL", K(ret));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Stmt type should not be here", K(ret), "stmt type", basic_stmt->get_stmt_type());
  }
  return ret;
}

///@brief if you sure no priv needed for the stmt
int no_priv_needed(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  UNUSED(session_priv);
  UNUSED(basic_stmt);
  UNUSED(need_privs);
  return OB_SUCCESS;
}

/* 判断expr是否含指定表的列，表的表级由expr level 和rel ids标记 */
int expr_has_col_in_tab(
    const ObRawExpr *expr,
    const ObRelIds &rel_ids,
    bool& exists)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is null", K(ret));
  } else if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (expr->is_column_ref_expr()) {
    if (rel_ids.is_superset(expr->get_relation_ids())) {
      exists = true;
    }
  } else if (expr->has_flag(CNT_COLUMN)) {
    for (int64_t i = 0; OB_SUCC(ret) && !exists && i < expr->get_param_count(); ++i) {
      OZ (expr_has_col_in_tab(expr->get_param_expr(i), rel_ids, exists));
    }
  }
  return ret;
}

/* 对update，delete语句增加必要的select权限 */
int add_nec_sel_priv_in_dml(
    const ObDMLStmt* dml_stmt,
    ObPackedObjPriv& packed_privs)
{
  int ret = OB_SUCCESS;
  bool exists = false;
  if (OB_ISNULL(dml_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Dml stmt should not be NULL", K(ret));
  } else if (stmt::T_UPDATE == dml_stmt->get_stmt_type() ||
             stmt::T_DELETE == dml_stmt->get_stmt_type()) {
    /* 基表的table id*/
    ObRelIds table_ids;
    for (int i = 0; i < dml_stmt->get_table_size() && OB_SUCC(ret); i++) {
      const TableItem *table_item = dml_stmt->get_table_item(i);
      if (OB_ISNULL(table_item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table item is null");
      } else {
        OZ (table_ids.add_member(dml_stmt->get_table_bit_index(table_item->table_id_)));
      }
    }
    for (int i = 0; OB_SUCC(ret) && (!exists) && i < dml_stmt->get_condition_size(); i++) {
      const ObRawExpr *raw_expr = dml_stmt->get_condition_expr(i);
      OZ (expr_has_col_in_tab(raw_expr, table_ids, exists));
    }
    if (OB_SUCC(ret) && exists) {
      OZ (ObPrivPacker::append_raw_obj_priv(NO_OPTION, OBJ_PRIV_ID_SELECT, packed_privs));
    }
  }
  return ret;
}

int add_need_priv(
    ObIArray<ObOraNeedPriv> &need_privs,
    ObOraNeedPriv& need_priv)
{
  int ret = OB_SUCCESS;
  bool found = false;
  for (int i = 0; OB_SUCC(ret) && !found && i < need_privs.count(); i++) {
    ObOraNeedPriv &tmp_priv = need_privs.at(i);
    if (need_priv.same_obj(tmp_priv)) {
      found = true;
      tmp_priv.obj_privs_ |= need_priv.obj_privs_;
    }
  }
  if (OB_SUCC(ret) && !found) {
    OZ (need_privs.push_back(need_priv));
    if (need_priv.owner_id_ == OB_INVALID_ID) {
      LOG_INFO("obj owner id -1", K(need_priv), K(lbt()));
    }
  }
  return ret;
}

/* 根据dbname，找到同名的用户信息，返回用户id */
int get_owner_id_by_db_name(
  const ObSqlCtx &ctx,
  const ObString &db_name,
  uint64_t& owner_id)
{
  int ret = OB_SUCCESS;
  ObString host_name(OB_DEFAULT_HOST_NAME);
  const ObUserInfo* user_info = NULL;

  CK (ctx.session_info_ != NULL);
  if (db_name == OB_SYS_DATABASE_NAME) {
    owner_id = OB_ORA_SYS_USER_ID;
  } else {
    CK (ctx.schema_guard_ != NULL);
    OZ (ctx.schema_guard_->get_user_info(ctx.session_info_->get_login_tenant_id(),
                                        db_name,
                                        host_name,
                                        user_info));
    CK (user_info != NULL);
    OX (owner_id = user_info->get_user_id());
  }
  return ret;
}

int set_need_priv_owner_id(
  const ObSqlCtx &ctx,
  ObOraNeedPriv &need_priv)
{
  int ret = OB_SUCCESS;
  uint64_t owner_id = OB_INVALID_ID;

  if (!need_priv.db_name_.empty()) {
    OZ (get_owner_id_by_db_name(ctx, need_priv.db_name_, owner_id), need_priv.db_name_);
    OX (need_priv.owner_id_ = owner_id);
  }
  return ret;
}


int get_view_owner_id(
    const ObSqlCtx &ctx,
    const TableItem* table_item,
    uint64_t& user_id)
{
  int ret = OB_SUCCESS;
  ObString host_name(OB_DEFAULT_HOST_NAME);
  const ObUserInfo* user_info = NULL;

  CK (table_item != NULL);
  CK (table_item->is_view_table_ == true);
  CK( table_item->ref_query_ != NULL);
  CK (ctx.schema_guard_ != NULL);
  CK (ctx.session_info_ != NULL);
  OZ (ctx.schema_guard_->get_user_info(ctx.session_info_->get_login_tenant_id(),
                                       table_item->database_name_,
                                       host_name,
                                       user_info));
  CK (user_info != NULL);
  OX (user_id = user_info->get_user_id());
  return ret;
}

int mock_table_item(
    const ObSqlCtx &ctx,
    const TableItem* table_item,
    TableItem &new_table_item)
{
  int ret = OB_SUCCESS;
  uint64_t view_id;
  CK (table_item != NULL);
  CK (ctx.schema_guard_ != NULL);
  CK (ctx.session_info_ != NULL);

  new_table_item = *table_item;
  new_table_item.alias_name_ = ObString("");
  OZ (ctx.schema_guard_->get_table_id(ctx.session_info_->get_login_tenant_id(),
                                      table_item->database_name_,
                                      table_item->table_name_,
                                      false,
                                      share::schema::ObSchemaGetterGuard::ALL_NON_HIDDEN_TYPES,
                                      view_id));
  OX (new_table_item.table_id_ = view_id);
  return ret;
}
/** add_col_id_array_to_need_priv
 * 从 basic_stmt 中解析出sql语句作用的列，并加入到 need_priv 中
 * 修改col_id_并不会影响到obj权限的检查，因为进行表级权限检查时，无需使用col_id_
 * @param  {const ObStmt*} basic_stmt  : basic_stmt
 * @param  {const uint64_t} table_id   : 当前处理的 table_id
 * @param  {ObOraNeedPriv &} need_priv : 视情况将need_priv由表级改为列级
 * @return {int}                       : ret
 */
int add_col_id_array_to_need_priv(
    const ObStmt *basic_stmt,
    const uint64_t table_id,
    ObOraNeedPriv &need_priv)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("basic_stmt is NULL", K(ret));
  } else {
    stmt::StmtType stmt_type = basic_stmt->get_stmt_type();
    switch (stmt_type) {
      case stmt::T_INSERT: {
        need_priv.col_id_array_.reset();
        const ObInsertStmt *insert_stmt = NULL;
        insert_stmt = dynamic_cast<const ObInsertStmt*>(basic_stmt);
        if (OB_ISNULL(insert_stmt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("insert_stmt is NULL", K(ret));
        } else {
          ObColumnRefRawExpr *value_desc = NULL;
          for (int i = 0; OB_SUCC(ret) && i < insert_stmt->get_values_desc().count(); ++i) {
            if (OB_ISNULL(value_desc = insert_stmt->get_values_desc().at(i))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("value_desc is null", K(ret));
            } else if (table_id == value_desc->get_table_id()) {
              OZ (need_priv.col_id_array_.push_back(value_desc->get_column_id()));
            }
          }
        }
        if (need_priv.col_id_array_.count() > 0) {
          need_priv.obj_level_ = OBJ_LEVEL_FOR_COL_PRIV;
        }
        break;
      }
      case stmt::T_UPDATE:{
        need_priv.col_id_array_.reset();
        const ObUpdateStmt *update_stmt = NULL;
        update_stmt = dynamic_cast<const ObUpdateStmt*>(basic_stmt);
        if (OB_ISNULL(update_stmt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("update_stmt is null", K(ret));
        } else {
          for (int i = 0; OB_SUCC(ret) && i < update_stmt->get_update_table_info().count(); ++i) {
            ObUpdateTableInfo* table_info = update_stmt->get_update_table_info().at(i);
            if (OB_ISNULL(table_info)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get null table info", K(ret));
            } else if (table_info->table_id_ == table_id) {
              const ObAssignments &assigns = table_info->assignments_;
              for (int j = 0; OB_SUCC(ret) && j < assigns.count(); ++j) {
                const ObColumnRefRawExpr *col_expr = assigns.at(i).column_expr_;
                if (OB_ISNULL(col_expr)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("(col_expr is null");
                } else if (col_expr->get_table_id() == table_id){
                  OZ (need_priv.col_id_array_.push_back(col_expr->get_column_id()));
                }
              }
            }
          }
        }
        if (need_priv.col_id_array_.count() > 0) {
          need_priv.obj_level_ = OBJ_LEVEL_FOR_COL_PRIV;
        }
        break;
      }
      default : {
        break;
      }
    }
  }
  return ret;
}

int add_col_priv_to_need_priv(
    const ObStmt *basic_stmt,
    const TableItem &table_item,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  ObStmtExprGetter visitor;
  ObNeedPriv need_priv;
  need_priv.db_ = table_item.database_name_;
  need_priv.table_ = table_item.table_name_;
  need_priv.is_sys_table_ = table_item.is_system_table_;
  need_priv.is_for_update_ = table_item.for_update_;
  need_priv.priv_level_ = OB_PRIV_TABLE_LEVEL;
  const uint64_t table_id = table_item.table_id_;
  visitor.set_relation_scope();
  visitor.remove_scope(SCOPE_DML_COLUMN);
  visitor.remove_scope(SCOPE_DML_CONSTRAINT);
  visitor.remove_scope(SCOPE_DMLINFOS);
  ObSEArray<ObRawExpr *, 4> col_exprs;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("basic_stmt is NULL", K(ret));
  } else {
    stmt::StmtType stmt_type = basic_stmt->get_stmt_type();
    switch (stmt_type) {
      case stmt::T_DELETE: {
        need_priv.priv_set_ = OB_PRIV_DELETE;
        ADD_NEED_PRIV(need_priv);
        break;
      }
      case stmt::T_REPLACE:
      case stmt::T_INSERT: {
        visitor.remove_scope(SCOPE_INSERT_DESC);
        visitor.remove_scope(SCOPE_DML_VALUE);
        const ObInsertStmt *insert_stmt = NULL;
        insert_stmt = static_cast<const ObInsertStmt*>(basic_stmt);
        if (OB_ISNULL(insert_stmt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("insert_stmt is NULL", K(ret));
        } else {
          ObColumnRefRawExpr *value_desc = NULL;
          if (insert_stmt->is_replace()) {
            need_priv.priv_set_ = OB_PRIV_DELETE;
            ADD_NEED_PRIV(need_priv);
            need_priv.columns_.reuse();
          }
          need_priv.priv_set_ = OB_PRIV_INSERT;
          for (int i = 0; OB_SUCC(ret) && i < insert_stmt->get_values_desc().count(); ++i) {
            if (OB_ISNULL(value_desc = insert_stmt->get_values_desc().at(i))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("value_desc is null", K(ret));
            } else if (table_id == value_desc->get_table_id()
                      && value_desc->get_column_id() >= OB_APP_MIN_COLUMN_ID) {
              OZ (need_priv.columns_.push_back(value_desc->get_column_name()));
            }
          }
          if (OB_SUCC(ret)) {
            ADD_NEED_PRIV(need_priv);
            need_priv.columns_.reuse();
          }

          if (OB_FAIL(ret)) {
          } else if (insert_stmt->is_insert_up()) {
            const ObIArray<ObAssignment> &assigns = insert_stmt->get_table_assignments();
            need_priv.priv_set_ = OB_PRIV_UPDATE;
            for (int j = 0; OB_SUCC(ret) && j < assigns.count(); ++j) {
              if (!assigns.at(j).is_implicit_) {
                const ObColumnRefRawExpr *col_expr = assigns.at(j).column_expr_;
                const ObRawExpr *expr = assigns.at(j).expr_;
                if (OB_ISNULL(col_expr) || OB_ISNULL(expr)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("col_expr is null");
                } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(expr, col_exprs))) {
                  LOG_WARN("extract column exprs failed", K(ret));
                } else if (col_expr->get_table_id() == table_id
                        && col_expr->get_column_id() >= OB_APP_MIN_COLUMN_ID) {
                  OZ (need_priv.columns_.push_back(col_expr->get_column_name()));
                }
              }
            }
            if (OB_SUCC(ret)) {
              ADD_NEED_PRIV(need_priv);
              need_priv.columns_.reuse();
            }
          }
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(append(col_exprs, insert_stmt->get_insert_table_info().column_in_values_vector_))) {
            LOG_WARN("append failed", K(ret));
          }
        }

        break;
      }
      case stmt::T_UPDATE: {
        need_priv.priv_set_ = OB_PRIV_UPDATE;
        const ObUpdateStmt *update_stmt = NULL;
        update_stmt = static_cast<const ObUpdateStmt*>(basic_stmt);
        if (OB_ISNULL(update_stmt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("update_stmt is null", K(ret));
        } else {
          for (int i = 0; OB_SUCC(ret) && i < update_stmt->get_update_table_info().count(); ++i) {
            ObUpdateTableInfo* table_info = update_stmt->get_update_table_info().at(i);
            if (OB_ISNULL(table_info)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get null table info", K(ret));
            } else if (table_info->table_id_ == table_id) {
              const ObAssignments &assigns = table_info->assignments_;
              for (int j = 0; OB_SUCC(ret) && j < assigns.count(); ++j) {
                if (!assigns.at(j).is_implicit_) {
                  const ObColumnRefRawExpr *col_expr = assigns.at(j).column_expr_;
                  const ObRawExpr *value_expr = assigns.at(j).expr_;
                  if (OB_ISNULL(col_expr)) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("col_expr is null");
                  } else if (col_expr->get_table_id() == table_id
                          && col_expr->get_column_id() >= OB_APP_MIN_COLUMN_ID) {
                    OZ (need_priv.columns_.push_back(col_expr->get_column_name()));
                  } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(value_expr, col_exprs))) {
                    LOG_WARN("extract column exprs failed", K(ret));
                  }
                }
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          ADD_NEED_PRIV(need_priv);
          need_priv.columns_.reuse();
        }
        break;
      }
      default : {
        break;
      }
    }
    if (OB_SUCC(ret)) {
      ObSEArray<ObRawExpr *, 4> rel_exprs;
      need_priv.priv_set_ = OB_PRIV_SELECT;
      if (OB_FAIL(static_cast<const ObDMLStmt *>(basic_stmt)->get_relation_exprs(rel_exprs, visitor))) {
        LOG_WARN("get rel exprs failed", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::extract_column_exprs(rel_exprs, col_exprs))) {
        LOG_WARN("extract column exprs failed", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < col_exprs.count(); i++) {
          if (OB_ISNULL(col_exprs.at(i)) || OB_UNLIKELY(!col_exprs.at(i)->is_column_ref_expr())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected error", K(ret));
          } else {
            ObColumnRefRawExpr *col_expr = static_cast<ObColumnRefRawExpr *>(col_exprs.at(i));
            if (OB_ISNULL(col_expr)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected error", K(ret));
            } else if (col_expr->get_table_id() == table_id && col_expr->get_column_id() >= OB_APP_MIN_COLUMN_ID) {
              OZ (need_priv.columns_.push_back(col_expr->get_column_name()));
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (need_priv.columns_.empty()) {
            if (basic_stmt->is_select_stmt()) {
              need_priv.check_any_column_priv_ = true;
              ADD_NEED_PRIV(need_priv);
              need_priv.check_any_column_priv_ = false;
            }
          } else {
            ADD_NEED_PRIV(need_priv);
            need_priv.columns_.reuse();
          }
        }
      }
    }
  }
  return ret;
}

int set_privs_by_table_item_recursively(
    uint64_t user_id,
    const ObSqlCtx &ctx,
    const TableItem* table_item,
    ObPackedObjPriv &packed_privs,
    ObIArray<ObOraNeedPriv> &need_privs,
    uint64_t check_flag,
    const ObStmt *basic_stmt)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  ObOraNeedPriv need_priv;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  }
  CK (table_item != NULL);
  CK (ctx.schema_guard_ != NULL);
  if (OB_SUCC(ret)) {
    if (TableItem::BASE_TABLE == table_item->type_
        || TableItem::ALIAS_TABLE == table_item->type_) {
      if (!is_ora_virtual_table(table_item->ref_id_)) {
        need_priv.db_name_ = table_item->database_name_;
        need_priv.grantee_id_ = user_id;
        need_priv.obj_id_ = table_item->ref_id_;
        need_priv.obj_level_ = OBJ_LEVEL_FOR_TAB_PRIV;
        need_priv.obj_type_ = static_cast<uint64_t>(ObObjectType::TABLE);
        need_priv.obj_privs_ = packed_privs;
        need_priv.check_flag_ = check_flag;
        // Add inserted column id
        OZ (add_col_id_array_to_need_priv(basic_stmt, table_item->table_id_, need_priv));
        OZ (set_need_priv_owner_id(ctx, need_priv));
        OZ (add_need_priv(need_privs, need_priv));
      }
    } else if (table_item->is_view_table_) {
      if (!table_item->alias_name_.empty()) {
        TableItem new_table_item;
        OZ (mock_table_item(ctx, table_item, new_table_item));
        OZ (set_privs_by_table_item_recursively(user_id, ctx, &new_table_item,
            packed_privs, need_privs, check_flag, basic_stmt));
      } else {
        bool need_check = true;
        if (is_sys_view(table_item->ref_id_)) {
          /* oracle的字典视图(dba_*)需要做检查，其他系统视图(all_*, user_*, 性能视图(v$)不做权限检查 */
          need_check = table_item->is_oracle_dba_sys_view() || (OB_PROXY_USERS_TID == table_item->ref_id_);
        } else {
          need_check = true;
        }
        if (OB_SUCC(ret) && need_check) {
          uint64_t view_owner_id;

          /* 1. 对于视图，先记录视图id */
          need_priv.db_name_ = table_item->database_name_;
          need_priv.grantee_id_ = user_id;
          need_priv.obj_id_ = table_item->ref_id_;
          need_priv.obj_level_ = OBJ_LEVEL_FOR_TAB_PRIV;
          need_priv.obj_type_ = static_cast<uint64_t>(ObObjectType::VIEW);
          need_priv.obj_privs_ = packed_privs;
          need_priv.check_flag_ = check_flag;

          OZ (get_view_owner_id(ctx, table_item, view_owner_id));
          OX (need_priv.owner_id_ = view_owner_id);
          OZ (add_need_priv(need_privs, need_priv));

          /* 2.1 对于insert，update，delete视图，
                需要记录view的owner对基表的权限 */
          if (table_item->view_base_item_ != NULL) {
            OZ (set_privs_by_table_item_recursively(view_owner_id,
                                                    ctx,
                                                    table_item->view_base_item_,
                                                    packed_privs,
                                                    need_privs,
                                                    check_flag,
                                                    basic_stmt));
          }
          /* 2.2 对于select语句，
                需要记录view的owner对视图定义里面的所有表，视图的权限 */
          if (table_item->ref_query_ != NULL && basic_stmt != NULL 
              && stmt::T_SELECT == basic_stmt->get_stmt_type()) {
            OZ (ObPrivilegeCheck::get_stmt_ora_need_privs(view_owner_id,
                                                          ctx,
                                                          table_item->ref_query_,
                                                          need_privs,
                                                          check_flag));
          }
        }
      }
    }
  }
  return ret;
}

int get_proc_db_name(
    const ObSqlCtx &ctx,
    ObOraNeedPriv &need_priv)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ctx.session_info_) || OB_ISNULL(ctx.schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else {
    uint64_t tenant_id = ctx.session_info_->get_login_tenant_id();
    ObSchemaGetterGuard &schema_guard = *ctx.schema_guard_;
    uint64_t db_id = OB_INVALID_ID;
    const ObDatabaseSchema *db_schema = NULL;
    if (static_cast<uint64_t>(ObObjectType::FUNCTION) == need_priv.obj_type_) {
      const ObRoutineInfo *routine_schema = NULL;
      OZ (schema_guard.get_routine_info(tenant_id, need_priv.obj_id_, routine_schema));
      CK (routine_schema != NULL);
      OX (db_id = routine_schema->get_database_id());
    } else if (static_cast<uint64_t>(ObObjectType::PACKAGE) == need_priv.obj_type_) {
      const ObSimplePackageSchema *pkg_schema = NULL;
      OZ (schema_guard.get_simple_package_info(tenant_id, need_priv.obj_id_, pkg_schema));
      CK (pkg_schema != NULL);
      OX (db_id = pkg_schema->get_database_id());
    } else if (static_cast<uint64_t>(ObObjectType::TYPE) == need_priv.obj_type_) {
      const ObUDTTypeInfo *type_schema = NULL;
      OZ (schema_guard.get_udt_info(tenant_id, need_priv.obj_id_, type_schema));
      CK (OB_NOT_NULL(type_schema));
      OX (db_id = type_schema->get_database_id());
    }
    OZ (schema_guard.get_database_schema(tenant_id,
                                       db_id,
                                       db_schema));
    CK (db_schema != NULL);
    OX (need_priv.db_name_ = db_schema->get_database_name());
  }
  return ret;
}

int get_seq_db_name(
    uint64_t tenant_id,
    const ObSqlCtx &ctx,
    uint64_t obj_id,
    ObString &db_name)
{
  int ret = OB_SUCCESS;
  uint64_t db_id = OB_INVALID_ID;
  const ObSequenceSchema *seq_schema = NULL;
  const ObDatabaseSchema *db_schema = NULL;
  CK(ctx.schema_guard_);
  CK(ctx.session_info_);
  OZ(ctx.session_info_->get_dblink_sequence_schema(obj_id, seq_schema));
  if (OB_SUCC(ret) && NULL == seq_schema) {
    OZ (ctx.schema_guard_->get_sequence_schema(tenant_id, obj_id, seq_schema));
    CK (seq_schema != NULL);
  }
  OX (db_id = seq_schema->get_database_id());
  OZ (ctx.schema_guard_->get_database_schema(tenant_id,
                                       db_id,
                                       db_schema));
  CK (db_schema != NULL);
  OX (db_name = db_schema->get_database_name());
  return ret;
}

int add_udf_expr_priv(
    const ObSqlCtx &ctx,
    ObRawExpr *expr,
    uint64_t user_id,
    ObIArray<ObOraNeedPriv> &need_privs,
    uint64_t check_flag)
{
  int ret = OB_SUCCESS;
  bool is_sys_udf = false;
  ObOraNeedPriv need_priv;
  ObString db_name;
  ObPackedObjPriv packed_privs = 0;
  ObUDFRawExpr *udf_expr = NULL;
  if (OB_ISNULL(expr) || OB_ISNULL(udf_expr = static_cast<ObUDFRawExpr *>(expr))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", KPC(expr), K(ret));
  } else if (0 == udf_expr->get_database_name().case_compare(OB_SYS_DATABASE_NAME)) {
    is_sys_udf = true;
  } else if (common::OB_INVALID_ID != udf_expr->get_type_id()) {
    need_priv.obj_id_ = udf_expr->get_type_id();
    need_priv.obj_type_ = static_cast<uint64_t>(ObObjectType::TYPE);
  } else if (common::OB_INVALID_ID != udf_expr->get_pkg_id()) {
    need_priv.obj_id_ = udf_expr->get_pkg_id();
    need_priv.obj_type_ = static_cast<uint64_t>(ObObjectType::PACKAGE);
  } else if (common::OB_INVALID_ID != udf_expr->get_udf_id()) {
    need_priv.obj_id_ = udf_expr->get_udf_id();
    need_priv.obj_type_ = static_cast<uint64_t>(ObObjectType::FUNCTION);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid udf expr", KPC(udf_expr), K(ret));
  }
  if (OB_SUCC(ret) && !is_sys_udf) {
    // todo: check sys udf privilege after grant privs to public role
    need_priv.grantee_id_ = user_id;
    need_priv.obj_level_ = OBJ_LEVEL_FOR_TAB_PRIV;
    need_priv.check_flag_ = check_flag;
    need_priv.db_name_ = udf_expr->get_database_name();
    OZ (set_need_priv_owner_id(ctx, need_priv));
    OZ (ObPrivPacker::pack_raw_obj_priv(NO_OPTION, OBJ_PRIV_ID_EXECUTE, packed_privs));
    OX (need_priv.obj_privs_ = packed_privs);
    OZ (add_need_priv(need_privs, need_priv));
  }
  return ret;
}

int add_proc_priv_in_expr(
    const ObSqlCtx &ctx,
    ObRawExpr *expr,
    uint64_t user_id,
    ObIArray<ObOraNeedPriv> &need_privs,
    uint64_t check_flag)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null expr", K(ret));
  } else if (!expr->has_flag(CNT_PL_UDF)) {
    // do nothing
  } else if (expr->has_flag(IS_PL_UDF)) {
    if (OB_FAIL(add_udf_expr_priv(ctx, expr, user_id, need_privs, check_flag))) {
      LOG_WARN("failed to add udf expr priv", K(ret));
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < expr->get_param_count(); ++i) {
      ObRawExpr *param_expr = expr->get_param_expr(i);
      if (OB_FAIL(add_proc_priv_in_expr(ctx, param_expr, user_id, need_privs, check_flag))) {
        LOG_WARN("failed to add proc priv in expr", K(ret));
      }
    }
  }
  return ret;
}

int add_procs_priv_in_dml(
    uint64_t user_id,
    const ObSqlCtx &ctx,
    const ObDMLStmt *dml_stmt,
    ObIArray<ObOraNeedPriv> &need_privs,
    uint64_t check_flag)
{
  int ret = OB_SUCCESS;
  ObOraNeedPriv need_priv;
  ObString db_name;
  ObPackedObjPriv packed_privs = 0;
  CK (ctx.schema_guard_ != NULL);
  CK (ctx.session_info_ != NULL);
  CK (dml_stmt != NULL);
  ObSEArray<ObRawExpr *, 8> relation_exprs;
  if (OB_ISNULL(dml_stmt) || OB_ISNULL(dml_stmt->get_query_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (OB_FAIL(dml_stmt->get_relation_exprs(relation_exprs))) {
    LOG_WARN("failed to get relation exprs", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < relation_exprs.count(); ++i) {
    ObRawExpr *expr = relation_exprs.at(i);
    if (OB_FAIL(add_proc_priv_in_expr(ctx, expr, user_id, need_privs, check_flag))) {
      LOG_WARN("failed to add udf priv", K(ret));
    }
  }
  return ret;
}

int add_seqs_priv_in_dml_inner(
    uint64_t user_id,
    const ObSqlCtx &ctx,
    const ObIArray<uint64_t> &seq_ids,
    uint64_t priv,
    ObIArray<ObOraNeedPriv> &need_privs,
    uint64_t check_flag)
{
  int ret = OB_SUCCESS;
  ObOraNeedPriv need_priv;
  ObString db_name;
  ObPackedObjPriv packed_privs = 0;
  for (int i = 0; OB_SUCC(ret) && i < seq_ids.count(); i++) {
    const uint64_t obj_id = seq_ids.at(i);
    need_priv.grantee_id_ = user_id;
    need_priv.obj_id_ = obj_id;
    need_priv.obj_level_ = OBJ_LEVEL_FOR_TAB_PRIV;
    need_priv.obj_type_ = static_cast<uint64_t>(ObObjectType::SEQUENCE);
    need_priv.check_flag_ = check_flag;
    OZ (get_seq_db_name(ctx.session_info_->get_login_tenant_id(),
                          ctx,
                          obj_id, db_name));
    OX (need_priv.db_name_ = db_name);
    OZ (set_need_priv_owner_id(ctx, need_priv));
    OZ (ObPrivPacker::pack_raw_obj_priv(NO_OPTION, priv, packed_privs));
    OX (need_priv.obj_privs_ = packed_privs);
    OZ (add_need_priv(need_privs, need_priv));
  }
  return ret;
}

int add_seqs_priv_in_dml(
    uint64_t user_id,
    const ObSqlCtx &ctx,
    const ObDMLStmt *dml_stmt,
    ObIArray<ObOraNeedPriv> &need_privs,
    uint64_t check_flag)
{
  int ret = OB_SUCCESS;
  CK (ctx.schema_guard_ != NULL);
  CK (ctx.session_info_ != NULL);
  CK (dml_stmt != NULL);
  common::ObArray<uint64_t> nextval_sequence_ids;
  common::ObArray<uint64_t> currval_sequence_ids;
  ObArray<const ObRawExpr *> exprs;
  if (dml_stmt->is_update_stmt()) {
    const ObUpdateStmt *stmt = static_cast<const ObUpdateStmt *>(dml_stmt);
    for (int64_t k = 0; k < dml_stmt->get_column_items().count() && OB_SUCC(ret); k++) {
      for (int i = 0; OB_SUCC(ret) && i < stmt->get_update_table_info().count(); i++) {
        CK (stmt->get_update_table_info().at(i) != NULL);
        for (int j = 0; OB_SUCC(ret) && j < stmt->get_update_table_info().at(i)->assignments_.count(); j++) {
          if (stmt->get_update_table_info().at(i)->assignments_.at(j).column_expr_ == dml_stmt->get_column_items().at(k).get_expr()) {
            const ObRawExpr *default_expr = NULL;
            if (NULL != (default_expr = dml_stmt->get_column_items().at(k).default_value_expr_)
                && default_expr->has_flag(CNT_SEQ_EXPR)) {
              OZ (exprs.push_back(default_expr));
            }
          }
        }
      }
    }
    while(OB_SUCC(ret) && !exprs.empty()) {
      const ObRawExpr *expr = NULL;
      OZ (exprs.pop_back(expr));
      CK (expr != NULL);
      if (OB_SUCC(ret)) {
        if (expr->has_flag(IS_SEQ_EXPR)) {
          const ObSequenceRawExpr *seq_raw_expr = static_cast<const ObSequenceRawExpr *>(expr);
          uint64_t sequence_id = seq_raw_expr->get_sequence_id();
          const ObString &action = seq_raw_expr->get_action();
          if (sequence_id == OB_INVALID_ID) {
          } else if (action.case_compare("CURRVAL")) {
            OZ (currval_sequence_ids.push_back(sequence_id));
          } else {
            OZ (nextval_sequence_ids.push_back(sequence_id));
          }
        } else {
          for (int i = 0; OB_SUCC(ret) && i < expr->get_param_count(); i++) {
            const ObRawExpr *child_expr = expr->get_param_expr(i);
            if (child_expr->has_flag(CNT_SEQ_EXPR)) {
              OZ (exprs.push_back(child_expr));
            }
          }
        }
      }
    }
  } else {
    OZ (append(nextval_sequence_ids, dml_stmt->get_nextval_sequence_ids()));
    OZ (append(currval_sequence_ids, dml_stmt->get_currval_sequence_ids()));
  }
  if (OB_SUCC(ret)) {
    OZ (add_seqs_priv_in_dml_inner(user_id, ctx, nextval_sequence_ids, OBJ_PRIV_ID_SELECT,
                               need_privs, check_flag));
    OZ (add_seqs_priv_in_dml_inner(user_id, ctx, currval_sequence_ids, OBJ_PRIV_ID_SELECT,
                               need_privs, check_flag));
  }
  return ret;
}

/* You must have the INSERT and UPDATE object privileges on the
  target table and the SELECT object privilege on the source table.
  To specify the DELETE clause of the merge_update_clause,
  you must also have the DELETE object privilege on the target table
  **************************************************************
  * Oracle的文档有误，目标表的select权限是无条件需要的。(12c上验证过)  *
  **************************************************************
  target table: INSERT + UPDATE + SELECT
                DELETE  if has delete clause (note: 这里，oracle也没有
                          按照delete语句仔细处理，当where 1 > 0，也粗糙的增加了select权限。
                目前，我们兼容oracle的做法。 )
  source table: SELECT */
int get_merge_stmt_ora_need_privs(
    uint64_t user_id,
    const ObSqlCtx &ctx,
    const ObStmt *basic_stmt,
    ObIArray<ObOraNeedPriv> &need_privs,
    uint64_t check_flag)
{
  int ret = OB_SUCCESS;
  ObPackedObjPriv packed_privs = 0;
  const TableItem *target_table = NULL;
  const TableItem *source_table = NULL;

  const ObMergeStmt *merge_stmt = static_cast<const ObMergeStmt*>(basic_stmt);
  OZ (ObPrivPacker::pack_raw_obj_priv(NO_OPTION, OBJ_PRIV_ID_UPDATE, packed_privs));
  OZ (ObPrivPacker::append_raw_obj_priv(NO_OPTION, OBJ_PRIV_ID_INSERT, packed_privs));
  OZ (ObPrivPacker::append_raw_obj_priv(NO_OPTION, OBJ_PRIV_ID_SELECT, packed_privs));
  CK (merge_stmt != NULL);
  /* add delete+select priv if has 'delete where clause' */
  if (!merge_stmt->get_delete_condition_exprs().empty()) {
    OZ (ObPrivPacker::append_raw_obj_priv(NO_OPTION, OBJ_PRIV_ID_DELETE, packed_privs));
  }

  OX (target_table = merge_stmt->get_table_item_by_id(merge_stmt->get_target_table_id()));
  OX (source_table = merge_stmt->get_table_item_by_id(merge_stmt->get_source_table_id()));
  CK (target_table != NULL && source_table != NULL);
  /* first add target table */
  if (OB_SUCC(ret) && (TableItem::BASE_TABLE == target_table->type_
      || TableItem::ALIAS_TABLE == target_table->type_
      || target_table->is_view_table_)) {
      OZ (set_privs_by_table_item_recursively(user_id,
        ctx, target_table, packed_privs, need_privs, check_flag, basic_stmt));
  }
  /* then add source table */
  OZ (ObPrivPacker::pack_raw_obj_priv(NO_OPTION, OBJ_PRIV_ID_SELECT, packed_privs));
  if (OB_SUCC(ret) && (TableItem::BASE_TABLE == source_table->type_
    || TableItem::ALIAS_TABLE == source_table->type_
    || source_table->is_view_table_)) {
    OZ (set_privs_by_table_item_recursively(user_id,
        ctx, source_table, packed_privs, need_privs, check_flag, basic_stmt));
  }
  return ret;
}

int get_dml_stmt_ora_need_privs(
    uint64_t user_id,
    const ObSqlCtx &ctx,
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObOraNeedPriv> &need_privs,
    uint64_t check_flag)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should not be NULL", K(ret));
  } else {
    ObOraNeedPriv need_priv;
    stmt::StmtType stmt_type = basic_stmt->get_stmt_type();
    switch (stmt_type) {
      case stmt::T_SELECT : {
        const ObSelectStmt *select_stmt = static_cast<const ObSelectStmt*>(basic_stmt);
        if (select_stmt->is_from_show_stmt()) {
          //do not check priv for show stmt.
          //现在大部分show 语句都是在执行时过滤掉无权限看到的信息
          break;
        }
      }//fall through for non-show select
      case stmt::T_INSERT :
      case stmt::T_REPLACE :
      case stmt::T_DELETE :
      case stmt::T_UPDATE :
      case stmt::T_MERGE : {
        ObPackedObjPriv packed_privs = 0;
        ObString op_literal;
        if (stmt::T_SELECT == stmt_type) {
          OZ (ObPrivPacker::pack_raw_obj_priv(NO_OPTION, OBJ_PRIV_ID_SELECT, packed_privs));
          if (check_flag == CHECK_FLAG_DIRECT) {
            OZ (ObPrivPacker::append_raw_obj_priv(NO_OPTION, OBJ_PRIV_ID_UPDATE, packed_privs));
            OZ (ObPrivPacker::append_raw_obj_priv(NO_OPTION, OBJ_PRIV_ID_INSERT, packed_privs));
            OZ (ObPrivPacker::append_raw_obj_priv(NO_OPTION, OBJ_PRIV_ID_DELETE, packed_privs));
          }
          op_literal = ObString::make_string("SELECT");
        } else if (stmt::T_INSERT == stmt_type) {
          OZ (ObPrivPacker::pack_raw_obj_priv(NO_OPTION, OBJ_PRIV_ID_INSERT, packed_privs));
          op_literal = ObString::make_string("INSERT");
          if (static_cast<const ObInsertStmt*>(basic_stmt)->is_insert_up()) {
            OZ (ObPrivPacker::append_raw_obj_priv(NO_OPTION, OBJ_PRIV_ID_UPDATE, packed_privs));
            OZ (ObPrivPacker::append_raw_obj_priv(NO_OPTION, OBJ_PRIV_ID_SELECT, packed_privs));
            op_literal = ObString::make_string("INSERT, UPDATE");
          }
          op_literal = ObString::make_string("INSERT");
        } else if (stmt::T_REPLACE == stmt_type) {
          OZ (ObPrivPacker::pack_raw_obj_priv(NO_OPTION, OBJ_PRIV_ID_DELETE, packed_privs));
          op_literal = ObString::make_string("INSERT, DELETE");
        } else if (stmt::T_DELETE == stmt_type) {
          OZ (ObPrivPacker::pack_raw_obj_priv(NO_OPTION, OBJ_PRIV_ID_DELETE, packed_privs));
          op_literal = ObString::make_string("DELETE");
        } else if (stmt::T_UPDATE == stmt_type) {
          OZ (ObPrivPacker::pack_raw_obj_priv(NO_OPTION, OBJ_PRIV_ID_UPDATE, packed_privs));
          op_literal = ObString::make_string("UPDATE");
        } else if (stmt::T_MERGE == stmt_type) {
          OZ (get_merge_stmt_ora_need_privs(user_id, ctx, basic_stmt, need_privs, check_flag));
          OX (op_literal = ObString::make_string("MERGE"));
        } else { } //do nothing
        const ObDMLStmt *dml_stmt = static_cast<const ObDMLStmt*>(basic_stmt);
        OZ (add_nec_sel_priv_in_dml(dml_stmt, packed_privs));
        OZ (add_procs_priv_in_dml(user_id, ctx, dml_stmt, need_privs, check_flag));
        OZ (add_seqs_priv_in_dml(user_id, ctx, dml_stmt, need_privs, check_flag));
        int64_t table_size = dml_stmt->get_table_size();
        for (int64_t i = 0; OB_SUCC(ret) && i < table_size; i++) {
          const TableItem *table_item = dml_stmt->get_table_item(i);
          if (OB_ISNULL(table_item)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("table item is null");
          } else if (stmt::T_MERGE != stmt_type
                    && (TableItem::BASE_TABLE == table_item->type_
                        || TableItem::ALIAS_TABLE == table_item->type_
                        || table_item->is_view_table_)) {
            OZ (set_privs_by_table_item_recursively(user_id,
                ctx, table_item, packed_privs, need_privs, check_flag, basic_stmt));
            //need_priv.is_sys_table_ = table_item->is_system_table_;
            //need_priv.priv_level_ = OB_PRIV_TABLE_LEVEL;
            //no check for information_schema select
            if (stmt::T_SELECT != dml_stmt->get_stmt_type()) {
              if (OB_FAIL(ObPrivilegeCheck::can_do_operation_on_db(session_priv,
                                                                  table_item->database_name_))) {
                LOG_WARN("Can not do this operation on the database", K(session_priv),
                         K(ret), "stmt_type", dml_stmt->get_stmt_type());
              } else if (ObPrivilegeCheck::is_mysql_org_table(table_item->database_name_,
                                                              table_item->table_name_)) {
                ret = OB_ERR_NO_TABLE_PRIVILEGE;
                LOG_USER_ERROR(OB_ERR_NO_TABLE_PRIVILEGE, op_literal.length(), op_literal.ptr(),
                               session_priv.user_name_.length(), session_priv.user_name_.ptr(),
                               session_priv.host_name_.length(),session_priv.host_name_.ptr(),
                               table_item->table_name_.length(), table_item->table_name_.ptr());
              }
            }
            if (OB_SUCC(ret)) {
              if (session_priv.is_tenant_changed()
                  && 0 != table_item->database_name_.case_compare(OB_SYS_DATABASE_NAME)) {
                ret = OB_ERR_NO_DB_PRIVILEGE;
                LOG_USER_ERROR(OB_ERR_NO_DB_PRIVILEGE, session_priv.user_name_.length(),
                               session_priv.user_name_.ptr(),
                               session_priv.host_name_.length(),session_priv.host_name_.ptr(),
                               table_item->database_name_.length(),
                               table_item->database_name_.ptr());
              }
            }
            /*if (OB_SUCC(ret)) {
              if (table_item->is_oracle_all_or_user_sys_view()) {
                need_priv.priv_set_ &= ~OB_PRIV_SELECT;
              }
            }*/
            //OZ (add_need_priv(need_privs, need_priv));
          }
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Stmt type error, should be DML stmt", K(ret), K(stmt_type));
      }
    }
  }
  return ret;
}

int get_dml_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should not be NULL", K(ret));
  } else {
    ObNeedPriv need_priv;
    stmt::StmtType stmt_type = basic_stmt->get_stmt_type();
    switch (stmt_type) {
      case stmt::T_SELECT : {
        const ObSelectStmt *select_stmt = static_cast<const ObSelectStmt*>(basic_stmt);
        if (select_stmt->is_from_show_stmt()) {
          //do not check priv for show stmt.
          //现在大部分show 语句都是在执行时过滤掉无权限看到的信息
          break;
        }
      }//fall through for non-show select
      case stmt::T_INSERT_ALL :
      case stmt::T_INSERT :
      case stmt::T_REPLACE :
      case stmt::T_DELETE :
      case stmt::T_UPDATE :
      case stmt::T_MERGE : {
        ObPrivSet priv_set = 0;
        ObString op_literal;
        if (stmt::T_SELECT == stmt_type) {
          priv_set = OB_PRIV_SELECT;
          op_literal = ObString::make_string("SELECT");
          if (static_cast<const ObSelectStmt*>(basic_stmt)->is_select_into_outfile()) {
            ObNeedPriv need_priv;
            need_priv.priv_set_ = OB_PRIV_FILE;
            need_priv.priv_level_ = OB_PRIV_USER_LEVEL;
            ADD_NEED_PRIV(need_priv);
          }
        } else if (stmt::T_INSERT_ALL == stmt_type) {
          priv_set = OB_PRIV_INSERT;
          op_literal = ObString::make_string("INSERT ALL");
        } else if (stmt::T_INSERT == stmt_type) {
          priv_set = OB_PRIV_INSERT;
          op_literal = ObString::make_string("INSERT");
          if (static_cast<const ObInsertStmt*>(basic_stmt)->is_insert_up()) {
            priv_set |= (OB_PRIV_UPDATE | OB_PRIV_SELECT);
            op_literal = ObString::make_string("INSERT, UPDATE");
          }
          op_literal = ObString::make_string("INSERT");
        } else if (stmt::T_REPLACE == stmt_type) {
          priv_set = OB_PRIV_INSERT | OB_PRIV_DELETE;
          op_literal = ObString::make_string("INSERT, DELETE");
        } else if (stmt::T_DELETE == stmt_type) {
          priv_set = OB_PRIV_DELETE | OB_PRIV_SELECT;
          op_literal = ObString::make_string("DELETE");
        } else if (stmt::T_UPDATE == stmt_type) {
          priv_set = OB_PRIV_UPDATE | OB_PRIV_SELECT;
          op_literal = ObString::make_string("UPDATE");
        } else if (stmt::T_MERGE == stmt_type) {
          priv_set = OB_PRIV_INSERT | OB_PRIV_DELETE | OB_PRIV_UPDATE | OB_PRIV_SELECT;
          op_literal = ObString::make_string("MERGE");
        } else { } //do nothing
        const ObDMLStmt *dml_stmt = static_cast<const ObDMLStmt*>(basic_stmt);
        int64_t table_size = dml_stmt->get_table_size();
        if (stmt::T_SELECT != stmt_type) {
          ObSEArray<const ObDmlTableInfo*, 4> table_infos;
          if (OB_FAIL(static_cast<const ObDelUpdStmt*>(basic_stmt)->get_dml_table_infos(table_infos))) {
            LOG_WARN("failed to get dml table infos", K(ret));
          } else if (OB_FAIL(ObPrivilegeCheck::can_do_operation_on_db(session_priv, table_infos, op_literal))) {
            LOG_WARN("cann't do this operation on this database", K(ret), K(stmt_type));
          }
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < table_size; i++) {
          const TableItem *table_item = dml_stmt->get_table_item(i);
          if (OB_ISNULL(table_item)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("table item is null");
          } else if (table_item->is_link_table()) {
            // skip link table
          } else if (TableItem::BASE_TABLE == table_item->type_
            || TableItem::ALIAS_TABLE == table_item->type_
            || table_item->is_view_table_) {
            need_priv.db_ = table_item->database_name_;
            need_priv.table_ = table_item->table_name_;
            need_priv.priv_set_ = priv_set;
            need_priv.is_sys_table_ = table_item->is_system_table_;
            need_priv.is_for_update_ = table_item->for_update_;
            need_priv.priv_level_ = OB_PRIV_TABLE_LEVEL;
            //no check for information_schema select
            if (stmt::T_SELECT != dml_stmt->get_stmt_type()) {
              if (OB_FAIL(ObPrivilegeCheck::can_do_operation_on_db(session_priv, table_item->database_name_))) {
                LOG_WARN("Can not do this operation on the database", K(session_priv),
                         K(ret), "stmt_type", dml_stmt->get_stmt_type());
              } else if (ObPrivilegeCheck::is_mysql_org_table(table_item->database_name_, table_item->table_name_)) {
                ret = OB_ERR_NO_TABLE_PRIVILEGE;
                LOG_USER_ERROR(OB_ERR_NO_TABLE_PRIVILEGE, op_literal.length(), op_literal.ptr(),
                               session_priv.user_name_.length(), session_priv.user_name_.ptr(),
                               session_priv.host_name_.length(),session_priv.host_name_.ptr(),
                               table_item->table_name_.length(), table_item->table_name_.ptr());
              }
            }
            if (OB_SUCC(ret)) {
              if (session_priv.is_tenant_changed()
                  && 0 != table_item->database_name_.case_compare(OB_SYS_DATABASE_NAME)) {
                ret = OB_ERR_NO_DB_PRIVILEGE;
                LOG_USER_ERROR(OB_ERR_NO_DB_PRIVILEGE, session_priv.user_name_.length(), session_priv.user_name_.ptr(),
                               session_priv.host_name_.length(),session_priv.host_name_.ptr(),
                               table_item->database_name_.length(), table_item->database_name_.ptr());
              }
            }
            if (OB_SUCC(ret)) {
              if (table_item->is_view_table_ && !table_item->alias_name_.empty()) {
                if (table_item->is_oracle_all_or_user_sys_view_for_alias()) {
                  need_priv.priv_set_ &= ~OB_PRIV_SELECT;
                }
              } else if (table_item->is_oracle_all_or_user_sys_view()) {
                need_priv.priv_set_ &= ~OB_PRIV_SELECT;
              }
            }

            if (OB_SUCC(ret)) {
              if (lib::is_mysql_mode()) {
                if (OB_FAIL(add_col_priv_to_need_priv(basic_stmt, *table_item, need_privs))) {
                  LOG_WARN("add col id array to need priv failed", K(ret));
                }
              } else {
                ADD_NEED_PRIV(need_priv);
              }
            }
          }
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Stmt type error, should be DML stmt", K(ret), K(stmt_type));
      }
    }
  }
  return ret;
}

int get_alter_table_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_UNLIKELY(stmt::T_ALTER_TABLE != basic_stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be T_ALTER_TABLE",
             K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else {
    ObNeedPriv need_priv;
    const ObAlterTableStmt *stmt = static_cast<const ObAlterTableStmt*>(basic_stmt);
    if (OB_FAIL(ObPrivilegeCheck::can_do_operation_on_db(session_priv, stmt->get_org_database_name()))) {
      LOG_WARN("Can not alter table in the database", K(session_priv), K(ret),
               "database_name", stmt->get_org_database_name());
    } else if (ObPrivilegeCheck::is_mysql_org_table(stmt->get_org_database_name(),
                                  stmt->get_org_table_name())
               && session_priv.tenant_id_ != OB_SYS_TENANT_ID) {
      ret = OB_ERR_NO_TABLE_PRIVILEGE;
      LOG_USER_ERROR(OB_ERR_NO_TABLE_PRIVILEGE, (int)strlen("ALTER"), "ALTER",
                     session_priv.user_name_.length(),session_priv.user_name_.ptr(),
                     session_priv.host_name_.length(),session_priv.host_name_.ptr(),
                     stmt->get_org_table_name().length(), stmt->get_org_table_name().ptr());
    } else if (stmt->has_rename_action()) {
      if (OB_FAIL(ObPrivilegeCheck::can_do_operation_on_db(session_priv, stmt->get_database_name()))) {
        LOG_WARN("Can not alter table in the database", K(session_priv), K(ret),
                 "database_name", stmt->get_database_name());
      } else {
        need_priv.db_ = stmt->get_org_database_name();
        need_priv.table_ = stmt->get_org_table_name();
        need_priv.priv_set_ = OB_PRIV_ALTER | OB_PRIV_DROP;
        need_priv.priv_level_ = OB_PRIV_TABLE_LEVEL;
        ADD_NEED_PRIV(need_priv);
        need_priv.db_ = stmt->get_database_name();
        need_priv.table_ = stmt->get_table_name();
        need_priv.priv_set_ = OB_PRIV_CREATE | OB_PRIV_INSERT;
        need_priv.priv_level_ = OB_PRIV_TABLE_LEVEL;
        ADD_NEED_PRIV(need_priv);
      }
    } else {
      need_priv.db_ = stmt->get_org_database_name();
      need_priv.table_ = stmt->get_org_table_name();
      need_priv.priv_set_ = OB_PRIV_ALTER;
      need_priv.priv_level_ = OB_PRIV_TABLE_LEVEL;
      ADD_NEED_PRIV(need_priv);

      const AlterTableSchema &alter_schema = const_cast<ObAlterTableStmt*>(stmt)->get_alter_table_arg().alter_table_schema_;
      if (alter_schema.alter_option_bitset_.has_member(obrpc::ObAlterTableArg::SESSION_ACTIVE_TIME)
          && session_priv.tenant_id_ != OB_SYS_TENANT_ID) {
        ret = OB_ERR_NO_PRIVILEGE;
        LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, "SUPER");
      }
    }
  }
  return ret;
}

int get_create_database_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_UNLIKELY(stmt::T_CREATE_DATABASE != basic_stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be T_CREATE_DATABASE",
             K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else {
    ObNeedPriv need_priv;
    const ObCreateDatabaseStmt *stmt = static_cast<const ObCreateDatabaseStmt*>(basic_stmt);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObPrivilegeCheck::can_do_operation_on_db(session_priv, stmt->get_database_name()))) {
      LOG_WARN("Can not create in the database", K(session_priv), "database_name", stmt->get_database_name(), K(ret));
    } else {
      need_priv.db_ = stmt->get_database_name();
      need_priv.priv_set_ = OB_PRIV_CREATE;
      need_priv.priv_level_ = OB_PRIV_DB_LEVEL;
      ADD_NEED_PRIV(need_priv);
    }
  }
  return ret;
}

int get_alter_database_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_UNLIKELY(stmt::T_ALTER_DATABASE != basic_stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be T_ALTER_DATABASE",
             K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else {
    ObNeedPriv need_priv;
    const ObAlterDatabaseStmt *stmt = static_cast<const ObAlterDatabaseStmt*>(basic_stmt);
    const ObString &db_name = stmt->get_database_name();
    if (0 == db_name.case_compare(OB_INFORMATION_SCHEMA_NAME)
        || (0 == db_name.case_compare(OB_SYS_DATABASE_NAME)
            && !stmt->only_alter_primary_zone())) {
      ret = OB_ERR_NO_DB_PRIVILEGE;
      LOG_USER_ERROR(OB_ERR_NO_DB_PRIVILEGE, session_priv.user_name_.length(), session_priv.user_name_.ptr(),
                     session_priv.host_name_.length(), session_priv.host_name_.ptr(),
                     db_name.length(), db_name.ptr());
    } else {
      need_priv.db_ = stmt->get_database_name();
      need_priv.priv_set_ = OB_PRIV_ALTER;
      need_priv.priv_level_ = OB_PRIV_DB_LEVEL;
      ADD_NEED_PRIV(need_priv);
    }
  }
  return ret;
}

int get_drop_database_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_UNLIKELY(stmt::T_DROP_DATABASE != basic_stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be T_DROP_DATABASE",
             K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else {
    ObNeedPriv need_priv;
    const ObDropDatabaseStmt *stmt = static_cast<const ObDropDatabaseStmt*>(basic_stmt);
    if (OB_FAIL(ObPrivilegeCheck::can_do_drop_operation_on_db(session_priv, stmt->get_database_name()))) {
      LOG_WARN("Can not drop information_schema database", K(ret));
    } else {
      need_priv.db_ = stmt->get_database_name();
      need_priv.priv_set_ = OB_PRIV_DROP;
      need_priv.priv_level_ = OB_PRIV_DB_LEVEL;
      ADD_NEED_PRIV(need_priv);
    }
  }
  return ret;
}

int get_create_table_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_UNLIKELY(stmt::T_CREATE_TABLE != basic_stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be T_CREATE_TABLE", K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else {
    ObNeedPriv need_priv;
    const ObCreateTableStmt *stmt = static_cast<const ObCreateTableStmt*>(basic_stmt);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObPrivilegeCheck::can_do_operation_on_db(session_priv, stmt->get_database_name()))) {
      LOG_WARN("Can not create table in information_schema database", K(session_priv), K(ret));
    } else if (stmt->is_view_table()){
      for (int64_t i = 0; i < stmt->get_view_need_privs().count(); i++) {
        LOG_INFO("output need privs", K(stmt->get_view_need_privs().at(i)), K(i));
      }
      if (OB_FAIL(need_privs.assign(stmt->get_view_need_privs()))) {
        LOG_WARN("fail to assign need_privs", K(ret));
      }
    } else {
      const ObSelectStmt *select_stmt = stmt->get_sub_select();
      if (NULL != select_stmt) {
        need_priv.priv_set_ = OB_PRIV_CREATE | OB_PRIV_INSERT;
      } else {
        need_priv.priv_set_ = OB_PRIV_CREATE;
      }
      need_priv.db_ = stmt->get_database_name();
      need_priv.table_ = stmt->get_table_name();
      need_priv.priv_level_ = OB_PRIV_TABLE_LEVEL;
      ADD_NEED_PRIV(need_priv);
      if (OB_SUCC(ret) && NULL != select_stmt) {
        OZ (ObPrivilegeCheck::get_stmt_need_privs(session_priv, select_stmt, need_privs));
      }
    }
  }
  return ret;
}

int get_drop_table_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_UNLIKELY(stmt::T_DROP_TABLE != basic_stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be T_DROP_TABLE",
             K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else {
    ObNeedPriv need_priv;
    const ObDropTableStmt *stmt = static_cast<const ObDropTableStmt*>(basic_stmt);
    const ObIArray<obrpc::ObTableItem> &tables = stmt->get_drop_table_arg().tables_;
    for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); i++) {
      const obrpc::ObTableItem &table_item = tables.at(i);
      if (OB_FAIL(ObPrivilegeCheck::can_do_operation_on_db(session_priv, table_item.database_name_))) {
        LOG_WARN("Can not drop table in information_schema database", K(session_priv), K(ret));
      } else if (ObPrivilegeCheck::is_mysql_org_table(table_item.database_name_, table_item.table_name_)
                 && session_priv.tenant_id_ != OB_SYS_TENANT_ID) {
        ret = OB_ERR_NO_TABLE_PRIVILEGE;
        LOG_USER_ERROR(OB_ERR_NO_TABLE_PRIVILEGE, (int)strlen("DROP"), "DROP",
                       session_priv.user_name_.length(), session_priv.user_name_.ptr(),
                       session_priv.host_name_.length(),session_priv.host_name_.ptr(),
                       table_item.table_name_.length(), table_item.table_name_.ptr());
      } else {
        need_priv.db_ = table_item.database_name_;
        need_priv.table_ = table_item.table_name_;
        need_priv.priv_set_ = OB_PRIV_DROP;
        need_priv.priv_level_ = OB_PRIV_TABLE_LEVEL;
        ADD_NEED_PRIV(need_priv);
      }
    }
    if ((share::schema::TMP_TABLE == stmt->get_drop_table_arg().table_type_
        || share::schema::TMP_TABLE_ALL == stmt->get_drop_table_arg().table_type_)
        && common::OB_INVALID_ID != stmt->get_drop_table_arg().session_id_
        && session_priv.tenant_id_ != OB_SYS_TENANT_ID) {
        ret = OB_ERR_NO_PRIVILEGE;
        LOG_USER_ERROR(OB_ERR_NO_PRIVILEGE, "SUPER");
      }
  }
  return ret;
}

int get_create_sequence_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  ObNeedPriv need_priv;
  bool need_check = false;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_UNLIKELY(stmt::T_CREATE_SEQUENCE != basic_stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be T_CREATE_SEQUENCE", K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else if (OB_FAIL(ObPrivilegeCheck::get_priv_need_check(session_priv,
                                          ObCompatFeatureType::MYSQL_PRIV_ENHANCE, need_check))) {
    LOG_WARN("failed to get priv need check", K(ret));
  } else if (lib::is_mysql_mode() && need_check) {
    const ObCreateSequenceStmt *stmt = static_cast<const ObCreateSequenceStmt*>(basic_stmt);
    if (OB_FAIL(ObPrivilegeCheck::can_do_operation_on_db(session_priv,
                                                         stmt->get_arg().get_database_name()))) {
      LOG_WARN("Can not create sequence in current database", K(session_priv), K(ret));
    } else {
      need_priv.db_ = stmt->get_arg().get_database_name();
      need_priv.priv_set_ = OB_PRIV_CREATE;
      need_priv.priv_level_ = OB_PRIV_DB_LEVEL;
      ADD_NEED_PRIV(need_priv);
    }
  }
  return ret;
}

int get_alter_sequence_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  ObNeedPriv need_priv;
  bool need_check = false;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_UNLIKELY(stmt::T_ALTER_SEQUENCE != basic_stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be T_ALTER_SEQUENCE", K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else if (OB_FAIL(ObPrivilegeCheck::get_priv_need_check(session_priv,
                                          ObCompatFeatureType::MYSQL_PRIV_ENHANCE, need_check))) {
    LOG_WARN("failed to get priv need check", K(ret));
  } else if (lib::is_mysql_mode() && need_check) {
    const ObAlterSequenceStmt *stmt = static_cast<const ObAlterSequenceStmt*>(basic_stmt);
    if (OB_FAIL(ObPrivilegeCheck::can_do_operation_on_db(session_priv,
                                                         stmt->get_arg().get_database_name()))) {
      LOG_WARN("Can not alter sequence in current database", K(session_priv), K(ret));
    } else {
      need_priv.db_ = stmt->get_arg().get_database_name();
      need_priv.priv_set_ = OB_PRIV_ALTER;
      need_priv.priv_level_ = OB_PRIV_DB_LEVEL;
      ADD_NEED_PRIV(need_priv);
    }
  }
  return ret;
}

int get_drop_sequence_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  ObNeedPriv need_priv;
  bool need_check = false;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_UNLIKELY(stmt::T_DROP_SEQUENCE != basic_stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be T_DROP_SEQUENCE", K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else if (OB_FAIL(ObPrivilegeCheck::get_priv_need_check(session_priv,
                                          ObCompatFeatureType::MYSQL_PRIV_ENHANCE, need_check))) {
    LOG_WARN("failed to get priv need check", K(ret));
  } else if (lib::is_mysql_mode() && need_check) {
    const ObDropSequenceStmt *stmt = static_cast<const ObDropSequenceStmt*>(basic_stmt);
    if (OB_FAIL(ObPrivilegeCheck::can_do_operation_on_db(session_priv,
                                                         stmt->get_arg().get_database_name()))) {
      LOG_WARN("Can not drop sequence in current database", K(session_priv), K(ret));
    } else {
      need_priv.db_ = stmt->get_arg().get_database_name();
      need_priv.priv_set_ = OB_PRIV_DROP;
      need_priv.priv_level_ = OB_PRIV_DB_LEVEL;
      ADD_NEED_PRIV(need_priv);
    }
  }
  return ret;
}

int get_create_outline_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  ObNeedPriv need_priv;
  bool need_check = false;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_UNLIKELY(stmt::T_CREATE_OUTLINE != basic_stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be T_CREATE_OUTLINE", K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else if (OB_FAIL(ObPrivilegeCheck::get_priv_need_check(session_priv,
                                          ObCompatFeatureType::MYSQL_PRIV_ENHANCE, need_check))) {
    LOG_WARN("failed to get priv need check", K(ret));
  } else if (lib::is_mysql_mode() && need_check) {
    const ObCreateOutlineStmt *stmt = static_cast<const ObCreateOutlineStmt*>(basic_stmt);
    if (OB_FAIL(ObPrivilegeCheck::can_do_operation_on_db(session_priv,
                                                         stmt->get_create_outline_arg().db_name_))) {
      LOG_WARN("Can not create outline in current database", K(session_priv), K(ret));
    } else {
      need_priv.db_ = stmt->get_create_outline_arg().db_name_;
      need_priv.priv_set_ = OB_PRIV_CREATE;
      need_priv.priv_level_ = OB_PRIV_DB_LEVEL;
      ADD_NEED_PRIV(need_priv);
    }
  }
  return ret;
}

int get_alter_outline_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  ObNeedPriv need_priv;
  bool need_check = false;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_UNLIKELY(stmt::T_ALTER_OUTLINE != basic_stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be T_ALTER_OUTLINE", K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else if (OB_FAIL(ObPrivilegeCheck::get_priv_need_check(session_priv,
                                          ObCompatFeatureType::MYSQL_PRIV_ENHANCE, need_check))) {
    LOG_WARN("failed to get priv need check", K(ret));
  } else if (lib::is_mysql_mode() && need_check) {
    const ObAlterOutlineStmt *stmt = static_cast<const ObAlterOutlineStmt*>(basic_stmt);
    if (OB_FAIL(ObPrivilegeCheck::can_do_operation_on_db(session_priv,
                                                         stmt->get_alter_outline_arg().db_name_))) {
      LOG_WARN("Can not alter outline in current database", K(session_priv), K(ret));
    } else {
      need_priv.db_ = stmt->get_alter_outline_arg().db_name_;
      need_priv.priv_set_ = OB_PRIV_ALTER;
      need_priv.priv_level_ = OB_PRIV_DB_LEVEL;
      ADD_NEED_PRIV(need_priv);
    }
  }
  return ret;
}

int get_drop_outline_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  ObNeedPriv need_priv;
  bool need_check = false;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_UNLIKELY(stmt::T_DROP_OUTLINE != basic_stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be T_DROP_OUTLINE", K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else if (OB_FAIL(ObPrivilegeCheck::get_priv_need_check(session_priv,
                                          ObCompatFeatureType::MYSQL_PRIV_ENHANCE, need_check))) {
    LOG_WARN("failed to get priv need check", K(ret));
  } else if (lib::is_mysql_mode() && need_check) {
    const ObDropOutlineStmt *stmt = static_cast<const ObDropOutlineStmt*>(basic_stmt);
    if (OB_FAIL(ObPrivilegeCheck::can_do_operation_on_db(session_priv,
                                                         stmt->get_drop_outline_arg().db_name_))) {
      LOG_WARN("Can not drop outline in current database", K(session_priv), K(ret));
    } else {
      need_priv.db_ = stmt->get_drop_outline_arg().db_name_;
      need_priv.priv_set_ = OB_PRIV_DROP;
      need_priv.priv_level_ = OB_PRIV_DB_LEVEL;
      ADD_NEED_PRIV(need_priv);
    }
  }
  return ret;
}

int get_create_synonym_priv(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  UNUSED(session_priv);
  ObNeedPriv need_priv;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (stmt::T_CREATE_SYNONYM != basic_stmt->get_stmt_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected stmt type", K(basic_stmt->get_stmt_type()), K(ret));
  } else if (OB_FAIL(ObPrivilegeCheck::can_do_operation_on_db(
                         session_priv, static_cast<const ObCreateSynonymStmt *>(basic_stmt)->get_database_name()))) {
    LOG_WARN("Can not do this operation on the database", KPC(basic_stmt), K(ret));
  } else {
    need_priv.priv_set_ = OB_PRIV_CREATE_SYNONYM;
    need_priv.priv_level_ = OB_PRIV_USER_LEVEL;
    ADD_NEED_PRIV(need_priv);
  }
  return ret;
}

int get_create_tablespace_priv(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  ObNeedPriv need_priv;
  bool need_check = false;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_UNLIKELY(stmt::T_CREATE_TABLESPACE != basic_stmt->get_stmt_type()
                         && stmt::T_DROP_TABLESPACE != basic_stmt->get_stmt_type()
                         && stmt::T_ALTER_TABLESPACE != basic_stmt->get_stmt_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected stmt type", K(basic_stmt->get_stmt_type()), K(ret));
  } else if (OB_FAIL(ObPrivilegeCheck::get_priv_need_check(session_priv,
                                          ObCompatFeatureType::MYSQL_PRIV_ENHANCE, need_check))) {
    LOG_WARN("failed to get priv need check", K(ret));
  } else if (lib::is_mysql_mode() && need_check) {
    need_priv.priv_set_ = OB_PRIV_CREATE_TABLESPACE;
    need_priv.priv_level_ = OB_PRIV_USER_LEVEL;
    ADD_NEED_PRIV(need_priv);
  }
  return ret;
}

int get_create_dblink_priv(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  UNUSED(session_priv);
  ObNeedPriv need_priv;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (stmt::T_CREATE_DBLINK != basic_stmt->get_stmt_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected stmt type", K(basic_stmt->get_stmt_type()), K(ret));
  } else {
    need_priv.priv_set_ = OB_PRIV_CREATE_DATABASE_LINK;
    need_priv.priv_level_ = OB_PRIV_USER_LEVEL;
    ADD_NEED_PRIV(need_priv);
  }
  return ret;
}

int get_drop_dblink_priv(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  UNUSED(session_priv);
  ObNeedPriv need_priv;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (stmt::T_DROP_DBLINK != basic_stmt->get_stmt_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected stmt type", K(basic_stmt->get_stmt_type()), K(ret));
  } else {
    need_priv.priv_set_ = OB_PRIV_DROP_DATABASE_LINK;
    need_priv.priv_level_ = OB_PRIV_USER_LEVEL;
    ADD_NEED_PRIV(need_priv);
  }
  return ret;
}

int get_drop_synonym_priv(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  UNUSED(session_priv);
  ObNeedPriv need_priv;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (stmt::T_DROP_SYNONYM != basic_stmt->get_stmt_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected stmt type", K(basic_stmt->get_stmt_type()), K(ret));
  } else if (OB_FAIL(ObPrivilegeCheck::can_do_operation_on_db(
                         session_priv, static_cast<const ObDropSynonymStmt *>(basic_stmt)->get_database_name()))) {
    LOG_WARN("Can not do this operation on the database", KPC(basic_stmt), K(ret));
  } else {
    need_priv.priv_set_ = OB_PRIV_DROP;
    need_priv.priv_level_ = OB_PRIV_USER_LEVEL;
    ADD_NEED_PRIV(need_priv);
  }
  return ret;
}

int get_create_index_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_UNLIKELY(stmt::T_CREATE_INDEX != basic_stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be T_CREATE_INDEX",
             K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else {
    ObNeedPriv need_priv;
    const ObCreateIndexStmt *stmt = static_cast<const ObCreateIndexStmt*>(basic_stmt);
    if (OB_FAIL(ObPrivilegeCheck::can_do_operation_on_db(session_priv, stmt->get_database_name()))) {
      LOG_WARN("Can not create index in information_schema database", K(session_priv), K(ret));
    } else {
      need_priv.db_ = stmt->get_database_name();
      need_priv.table_ = stmt->get_table_name();
      need_priv.priv_set_ = OB_PRIV_INDEX;
      need_priv.priv_level_ = OB_PRIV_TABLE_LEVEL;
      ADD_NEED_PRIV(need_priv);
    }
  }
  return ret;
}

int get_drop_index_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_UNLIKELY(stmt::T_DROP_INDEX != basic_stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be T_DROP_INDEX",
             K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else {
    ObNeedPriv need_priv;
    const ObDropIndexStmt *stmt = static_cast<const ObDropIndexStmt*>(basic_stmt);
    if (OB_FAIL(ObPrivilegeCheck::can_do_operation_on_db(session_priv, stmt->get_database_name()))) {
      LOG_WARN("Can not drop index in information_schema database", K(session_priv), K(ret));
    } else {
      need_priv.db_ = stmt->get_database_name();
      need_priv.table_ = stmt->get_table_name();
      need_priv.priv_set_ = OB_PRIV_INDEX;
      need_priv.priv_level_ = OB_PRIV_TABLE_LEVEL;
      ADD_NEED_PRIV(need_priv);
    }
  }
  return ret;
}

int get_create_mlog_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", KR(ret));
  } else if (OB_UNLIKELY(stmt::T_CREATE_MLOG != basic_stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be T_CREATE_MLOG",
        KR(ret), "stmt type", basic_stmt->get_stmt_type());
  } else {
    ObNeedPriv need_priv;
    const ObCreateMLogStmt *stmt = static_cast<const ObCreateMLogStmt*>(basic_stmt);
    if (OB_FAIL(ObPrivilegeCheck::can_do_operation_on_db(session_priv, stmt->get_database_name()))) {
      LOG_WARN("Can not create materialized view log in information_schema database",
          KR(ret), K(session_priv));
    } else {
      // create mlog requires select privilege on base table
      need_priv.db_ = stmt->get_database_name();
      need_priv.table_ = stmt->get_table_name();
      need_priv.priv_set_ = OB_PRIV_SELECT;
      need_priv.priv_level_ = OB_PRIV_TABLE_LEVEL;
      ADD_NEED_PRIV(need_priv);

      need_priv.db_ = stmt->get_database_name();
      need_priv.table_ = stmt->get_mlog_name();
      need_priv.priv_set_ = OB_PRIV_CREATE;
      need_priv.priv_level_ = OB_PRIV_TABLE_LEVEL;
      ADD_NEED_PRIV(need_priv);
    }
  }
  return ret;
}

int get_drop_mlog_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", KR(ret));
  } else if (OB_UNLIKELY(stmt::T_DROP_MLOG != basic_stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be T_DROP_MLOG",
        KR(ret), "stmt type", basic_stmt->get_stmt_type());
  } else {
    ObNeedPriv need_priv;
    const ObDropMLogStmt *stmt = static_cast<const ObDropMLogStmt*>(basic_stmt);
    if (OB_FAIL(ObPrivilegeCheck::can_do_operation_on_db(session_priv, stmt->get_database_name()))) {
      LOG_WARN("Can not drop materialized view log in information_schema database",
          KR(ret), K(session_priv));
    } else {
      need_priv.db_ = stmt->get_database_name();
      need_priv.table_ = stmt->get_mlog_name();
      need_priv.priv_set_ = OB_PRIV_DROP;
      need_priv.priv_level_ = OB_PRIV_TABLE_LEVEL;
      ADD_NEED_PRIV(need_priv);
    }
  }
  return ret;
}

int get_grant_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_UNLIKELY(stmt::T_GRANT != basic_stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be T_GRANT",
             K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else {
    ObNeedPriv need_priv;
    const ObGrantStmt *stmt = static_cast<const ObGrantStmt *>(basic_stmt);
    if (OB_FAIL(ObPrivilegeCheck::can_do_grant_on_db_table(session_priv, stmt->get_priv_set(),
                                         stmt->get_database_name(),
                                         stmt->get_table_name()))) {
      LOG_WARN("Can not grant information_schema database", K(ret));
    } else if (stmt->need_create_user_priv() &&
               !(session_priv.user_priv_set_ & OB_PRIV_CREATE_USER)) {
      ret = OB_ERR_CREATE_USER_WITH_GRANT;
      LOG_WARN("Need create user priv", K(ret), "user priv", ObPrintPrivSet(session_priv.user_priv_set_));
    } else if (is_root_user(session_priv.user_id_)) {
      //not neccessary
    } else {
      need_priv.db_ = stmt->get_database_name();
      need_priv.table_ = stmt->get_table_name();
      need_priv.priv_set_ = stmt->get_priv_set() | OB_PRIV_GRANT;
      need_priv.priv_level_ = stmt->get_grant_level();
      need_priv.obj_type_ = stmt->get_object_type();
      ADD_NEED_PRIV(need_priv);
      #define DEF_COLUM_NEED_PRIV(priv_prefix, priv_type) \
        ObNeedPriv priv_prefix##_need_priv;  \
        priv_prefix##_need_priv.db_ = stmt->get_database_name(); \
        priv_prefix##_need_priv.table_ = stmt->get_table_name(); \
        priv_prefix##_need_priv.priv_set_ = priv_type | OB_PRIV_GRANT; \
        priv_prefix##_need_priv.priv_level_ = OB_PRIV_TABLE_LEVEL;

      DEF_COLUM_NEED_PRIV(sel, OB_PRIV_SELECT);
      DEF_COLUM_NEED_PRIV(ins, OB_PRIV_INSERT);
      DEF_COLUM_NEED_PRIV(upd, OB_PRIV_UPDATE);
      DEF_COLUM_NEED_PRIV(ref, OB_PRIV_REFERENCES);

      #undef DEF_COLUM_NEED_PRIV

      for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_column_privs().count(); i++) {
        const ObString &column_name = stmt->get_column_privs().at(i).first;
        ObPrivSet priv_set = stmt->get_column_privs().at(i).second;
        if ((stmt->get_priv_set() & priv_set) != 0) {
          //contains in table priv already.
        } else if (0 != (priv_set & OB_PRIV_SELECT)) {
          ret = sel_need_priv.columns_.push_back(column_name);
        } else if (0 != (priv_set & OB_PRIV_INSERT)) {
          ret = ins_need_priv.columns_.push_back(column_name);
        } else if (0 != (priv_set & OB_PRIV_UPDATE)) {
          ret = upd_need_priv.columns_.push_back(column_name);
        } else if (0 != (priv_set & OB_PRIV_REFERENCES)) {
          ret = ref_need_priv.columns_.push_back(column_name);
        }
      }

      #define ADD_COLUMN_NEED_PRIV(priv_prefix) \
        if (OB_FAIL(ret)) { \
        } else if (priv_prefix##_need_priv.columns_.count() != 0) { \
          ADD_NEED_PRIV(priv_prefix##_need_priv); \
        }

      ADD_COLUMN_NEED_PRIV(sel);
      ADD_COLUMN_NEED_PRIV(ins);
      ADD_COLUMN_NEED_PRIV(upd);
      ADD_COLUMN_NEED_PRIV(ref);
      #undef ADD_COLUMN_NEED_PRIV
    }
  }
  return ret;
}

int get_revoke_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  bool check_revoke_all_user_create_user = false;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_UNLIKELY(stmt::T_REVOKE != basic_stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be T_REVOKE",
             K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else if (OB_FAIL(ObPrivilegeCheck::get_priv_need_check(session_priv,
                                          ObCompatFeatureType::MYSQL_USER_REVOKE_ALL_ENHANCE, check_revoke_all_user_create_user))) {
        LOG_WARN("failed to get priv need check", K(ret));
  } else {
    ObNeedPriv need_priv;
    const ObRevokeStmt *stmt = static_cast<const ObRevokeStmt *>(basic_stmt);
    if (check_revoke_all_user_create_user &&
      stmt->get_grant_level() == OB_PRIV_USER_LEVEL && stmt->get_priv_set() == OB_PRIV_ALL) {
      need_priv.db_ = stmt->get_database_name();
      need_priv.table_ = stmt->get_table_name();
      need_priv.priv_set_ = OB_PRIV_CREATE_USER;
      need_priv.priv_level_ = stmt->get_grant_level();
      need_priv.obj_type_ = stmt->get_object_type();
      ADD_NEED_PRIV(need_priv);

      ObSchemaGetterGuard schema_guard;
      bool need_add = false;
      CK (GCTX.schema_service_ != NULL);
      OZ(GCTX.schema_service_->get_tenant_schema_guard(session_priv.tenant_id_, schema_guard));
      for (int i = 0; OB_SUCC(ret) && i < stmt->get_users().count(); i++) {
        const ObUserInfo *user_info = NULL;
        OZ(schema_guard.get_user_info(session_priv.tenant_id_, stmt->get_users().at(i), user_info));
        CK (user_info != NULL);
        need_add = (0 != (user_info->get_priv_set() & OB_PRIV_SUPER));
      }
      if (OB_FAIL(ret)) {
      } else if (need_add) { //mysql8.0 if exists dynamic privs, then need SYSTEM_USER dynamic privilge to revoke all, now use SUPER to do so.
        need_priv.db_ = stmt->get_database_name();
        need_priv.table_ = stmt->get_table_name();
        need_priv.priv_set_ = OB_PRIV_SUPER;
        need_priv.priv_level_ = stmt->get_grant_level();
        need_priv.obj_type_ = stmt->get_object_type();
        ADD_NEED_PRIV(need_priv);
      }
    } else if (OB_FAIL(ObPrivilegeCheck::can_do_grant_on_db_table(session_priv, stmt->get_priv_set(),
                                         stmt->get_database_name(),
                                         stmt->get_table_name()))) {
      LOG_WARN("Can not grant information_schema database", K(ret));
    } else if (lib::is_mysql_mode() && stmt->get_revoke_all()) {
      //check privs at resolver
    } else {
      need_priv.db_ = stmt->get_database_name();
      need_priv.table_ = stmt->get_table_name();
      need_priv.priv_set_ = stmt->get_priv_set() | OB_PRIV_GRANT;
      need_priv.priv_level_ = stmt->get_grant_level();
      need_priv.obj_type_ = stmt->get_object_type();
      bool check_revoke_all_with_pl_priv = false;
      if (OB_FAIL(ObPrivilegeCheck::get_priv_need_check(session_priv,
                          ObCompatFeatureType::MYSQL_USER_REVOKE_ALL_WITH_PL_PRIV_CHECK, check_revoke_all_with_pl_priv))) {
        LOG_WARN("failed to get priv need check", K(ret));
      } else if (check_revoke_all_with_pl_priv) {
        //do nothing
      } else {
        need_priv.priv_set_ &= ~(OB_PRIV_EXECUTE | OB_PRIV_ALTER_ROUTINE | OB_PRIV_CREATE_ROUTINE);
      }

      ADD_NEED_PRIV(need_priv);
      #define DEF_COLUM_NEED_PRIV(priv_prefix, priv_type) \
        ObNeedPriv priv_prefix##_need_priv;  \
        priv_prefix##_need_priv.db_ = stmt->get_database_name(); \
        priv_prefix##_need_priv.table_ = stmt->get_table_name(); \
        priv_prefix##_need_priv.priv_set_ = priv_type | OB_PRIV_GRANT; \
        priv_prefix##_need_priv.priv_level_ = OB_PRIV_TABLE_LEVEL;

      DEF_COLUM_NEED_PRIV(sel, OB_PRIV_SELECT);
      DEF_COLUM_NEED_PRIV(ins, OB_PRIV_INSERT);
      DEF_COLUM_NEED_PRIV(upd, OB_PRIV_UPDATE);
      DEF_COLUM_NEED_PRIV(ref, OB_PRIV_REFERENCES);

      #undef DEF_COLUM_NEED_PRIV

      for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_column_privs().count(); i++) {
        const ObString &column_name = stmt->get_column_privs().at(i).first;
        ObPrivSet priv_set = stmt->get_column_privs().at(i).second;
        if ((stmt->get_priv_set() & priv_set) != 0) {
          //contains in table priv already.
        } else if (0 != (priv_set & OB_PRIV_SELECT)) {
          ret = sel_need_priv.columns_.push_back(column_name);
        } else if (0 != (priv_set & OB_PRIV_INSERT)) {
          ret = ins_need_priv.columns_.push_back(column_name);
        } else if (0 != (priv_set & OB_PRIV_UPDATE)) {
          ret = upd_need_priv.columns_.push_back(column_name);
        } else if (0 != (priv_set & OB_PRIV_REFERENCES)) {
          ret = ref_need_priv.columns_.push_back(column_name);
        }
      }

      #define ADD_COLUMN_NEED_PRIV(priv_prefix) \
        if (OB_FAIL(ret)) { \
        } else if (priv_prefix##_need_priv.columns_.count() != 0) { \
          ADD_NEED_PRIV(priv_prefix##_need_priv); \
        }

      ADD_COLUMN_NEED_PRIV(sel);
      ADD_COLUMN_NEED_PRIV(ins);
      ADD_COLUMN_NEED_PRIV(upd);
      ADD_COLUMN_NEED_PRIV(ref);
      #undef ADD_COLUMN_NEED_PRIV
    }
  }
  return ret;
}

int get_create_user_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  UNUSED(session_priv);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else {
    ObNeedPriv need_priv;
    stmt::StmtType stmt_type = basic_stmt->get_stmt_type();
    switch (stmt_type) {//TODO deleted switch
      case stmt::T_LOCK_USER :
      case stmt::T_ALTER_USER_PROFILE :
      case stmt::T_ALTER_USER_PROXY :
      case stmt::T_ALTER_USER_PRIMARY_ZONE:
      case stmt::T_ALTER_USER:
      case stmt::T_SET_PASSWORD :
      case stmt::T_RENAME_USER :
      case stmt::T_DROP_USER :
      case stmt::T_CREATE_USER : {
        if (stmt::T_SET_PASSWORD == stmt_type
            && static_cast<const ObSetPasswordStmt*>(basic_stmt)->get_for_current_user()) {
        } else if (stmt::T_ALTER_USER_PROFILE == stmt_type
                   && lib::is_mysql_mode()
                   && !!static_cast<const ObAlterUserProfileStmt*>(basic_stmt)->get_set_role_flag()) {
        } else {
          need_priv.priv_set_ = OB_PRIV_CREATE_USER;
          need_priv.priv_level_ = OB_PRIV_USER_LEVEL;
          ADD_NEED_PRIV(need_priv);
        }
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Stmt type not in types dealt in this function", K(ret), K(stmt_type));
        break;
      }
    }
  }
  return ret;
}

int get_role_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  UNUSED(session_priv);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (lib::is_oracle_mode()) {
    ret = no_priv_needed(session_priv, basic_stmt, need_privs);
  } else {
    ObNeedPriv need_priv;
    stmt::StmtType stmt_type = basic_stmt->get_stmt_type();
    switch (stmt_type) {
      case stmt::T_CREATE_ROLE: {
        need_priv.priv_set_ = OB_PRIV_CREATE_USER; //[TODO ROLE]
        need_priv.priv_level_ = OB_PRIV_USER_LEVEL;
        ADD_NEED_PRIV(need_priv);
        break;
      }
      case stmt::T_DROP_ROLE: {
        need_priv.priv_set_ = OB_PRIV_CREATE_USER;
        need_priv.priv_level_ = OB_PRIV_USER_LEVEL;
        ADD_NEED_PRIV(need_priv);
        break;
      }
      case stmt::T_GRANT_ROLE: {
        need_priv.priv_set_ = OB_PRIV_SUPER;
        need_priv.priv_level_ = OB_PRIV_USER_LEVEL;
        ADD_NEED_PRIV(need_priv);
        break;
      }
      case stmt::T_REVOKE_ROLE: {
        need_priv.priv_set_ = OB_PRIV_SUPER;
        need_priv.priv_level_ = OB_PRIV_USER_LEVEL;
        ADD_NEED_PRIV(need_priv);
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Stmt type not in types dealt in this function", K(ret), K(stmt_type));
        break;
      }
    }
  }
  return ret;
}

int get_variable_set_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  UNUSED(session_priv);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_UNLIKELY(stmt::T_VARIABLE_SET != basic_stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be T_VARIABLE_SET",
             K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else {
    ObNeedPriv need_priv;
    const ObVariableSetStmt *stmt = static_cast<const ObVariableSetStmt *>(basic_stmt);
    if (stmt->has_global_variable()) {
      // Return alter system instead of super.
      need_priv.priv_set_ = OB_PRIV_ALTER_SYSTEM;
      need_priv.priv_level_ = OB_PRIV_USER_LEVEL;
      ADD_NEED_PRIV(need_priv);
    }
  }
  return ret;
}

int get_modify_tenant_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_UNLIKELY(stmt::T_MODIFY_TENANT != basic_stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be T_MODIFY_TENANT",
             K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else {
    ObNeedPriv need_priv;
    const ObModifyTenantStmt *stmt = static_cast<const ObModifyTenantStmt *>(basic_stmt);
    if (OB_SYS_TENANT_ID != session_priv.tenant_id_) {
      if (!stmt->is_for_current_tenant()) {
        ret = OB_ERR_NO_PRIVILEGE;
        LOG_WARN("Only sys tenant can do this operation for other tenant", K(ret));
      } else {
        bool normal_tenant_can_do = false;
        if (OB_FAIL(stmt->check_normal_tenant_can_do(normal_tenant_can_do))) {
          LOG_WARN("Failed to check normal tenant can do the job", K(ret));
        } else if (!normal_tenant_can_do) {
          ret = OB_ERR_NO_PRIVILEGE;
          LOG_WARN("Include operation normal tenant can not do", K(ret));
        } else { }
      }
    }
    if (OB_SUCC(ret)) {
      need_priv.priv_set_ = OB_PRIV_ALTER_TENANT;
      need_priv.priv_level_ = OB_PRIV_USER_LEVEL;
      ADD_NEED_PRIV(need_priv);
    }
  }
  return ret;
}

int get_lock_tenant_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_UNLIKELY(stmt::T_LOCK_TENANT != basic_stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be T_LOCK_TENANT",
             K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else {
    ObNeedPriv need_priv;
    const ObLockTenantStmt *stmt = static_cast<const ObLockTenantStmt *>(basic_stmt);
    if (OB_SYS_TENANT_ID != session_priv.tenant_id_) {
      ret = OB_ERR_NO_PRIVILEGE;
      LOG_WARN("Only sys tenant can do this operation", K(ret));
    } else if (stmt->get_tenant_name() == OB_SYS_TENANT_NAME) {
      ret = OB_ERR_NO_PRIVILEGE;
      LOG_WARN("Can not lock sys tenant", K(ret));
    } else {
      need_priv.priv_set_ = OB_PRIV_SUPER;
      need_priv.priv_level_ = OB_PRIV_USER_LEVEL;
      ADD_NEED_PRIV(need_priv);
    }
  }
  return ret;
}

int get_routine_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_UNLIKELY(stmt::T_CREATE_ROUTINE != basic_stmt->get_stmt_type()
                        && stmt::T_DROP_ROUTINE != basic_stmt->get_stmt_type()
                        && stmt::T_ALTER_ROUTINE != basic_stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be routine stmt",
             K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else if (lib::is_oracle_mode()) {
    //do nothing
  } else if (stmt::T_CREATE_ROUTINE == basic_stmt->get_stmt_type()) {
    const ObCreateRoutineStmt *stmt = static_cast<const ObCreateRoutineStmt*>(basic_stmt);
    if (stmt->get_routine_arg().routine_info_.get_routine_type() == ObRoutineType::ROUTINE_PROCEDURE_TYPE
        || stmt->get_routine_arg().routine_info_.get_routine_type() == ObRoutineType::ROUTINE_FUNCTION_TYPE) {
      ObNeedPriv need_priv;
      need_priv.table_ = stmt->get_routine_arg().routine_info_.get_routine_name();
      need_priv.db_ = stmt->get_routine_arg().db_name_;
      need_priv.obj_type_ = stmt->get_routine_arg().routine_info_.get_routine_type() == ObRoutineType::ROUTINE_PROCEDURE_TYPE ? ObObjectType::PROCEDURE : ObObjectType::FUNCTION;
      need_priv.priv_level_ = OB_PRIV_ROUTINE_LEVEL;
      need_priv.priv_set_ = OB_PRIV_CREATE_ROUTINE;
      ADD_NEED_PRIV(need_priv);
    }
  } else if (stmt::T_ALTER_ROUTINE == basic_stmt->get_stmt_type()) {
    const ObAlterRoutineStmt *stmt = static_cast<const ObAlterRoutineStmt*>(basic_stmt);
    if (stmt->get_routine_arg().routine_info_.get_routine_type() == ObRoutineType::ROUTINE_PROCEDURE_TYPE
        || stmt->get_routine_arg().routine_info_.get_routine_type() == ObRoutineType::ROUTINE_FUNCTION_TYPE) {
      ObNeedPriv need_priv;
      need_priv.table_ = stmt->get_routine_arg().routine_info_.get_routine_name();
      need_priv.db_ = stmt->get_routine_arg().db_name_;
      need_priv.obj_type_ = stmt->get_routine_arg().routine_info_.get_routine_type() == ObRoutineType::ROUTINE_PROCEDURE_TYPE ? ObObjectType::PROCEDURE : ObObjectType::FUNCTION;
      need_priv.priv_level_ = OB_PRIV_ROUTINE_LEVEL;
      need_priv.priv_set_ = OB_PRIV_ALTER_ROUTINE;
      ADD_NEED_PRIV(need_priv);
    }
  } else if (stmt::T_DROP_ROUTINE == basic_stmt->get_stmt_type()) {
    const ObDropRoutineStmt *stmt = static_cast<const ObDropRoutineStmt*>(basic_stmt);
    if (stmt->get_routine_arg().routine_type_ == ObRoutineType::ROUTINE_PROCEDURE_TYPE
        || stmt->get_routine_arg().routine_type_ == ObRoutineType::ROUTINE_FUNCTION_TYPE) {
      ObNeedPriv need_priv;
      need_priv.table_ = stmt->get_routine_arg().routine_name_;
      need_priv.db_ = stmt->get_routine_arg().db_name_;
      need_priv.obj_type_ = stmt->get_routine_arg().routine_type_ == ObRoutineType::ROUTINE_PROCEDURE_TYPE ? ObObjectType::PROCEDURE : ObObjectType::FUNCTION;
      need_priv.priv_level_ = OB_PRIV_ROUTINE_LEVEL;
      need_priv.priv_set_ = OB_PRIV_ALTER_ROUTINE;
      ADD_NEED_PRIV(need_priv);
    }
  }
  return ret;
}

int get_drop_tenant_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_UNLIKELY(stmt::T_DROP_TENANT != basic_stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be T_DROP_TENANT",
             K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else {
    ObNeedPriv need_priv;
    const ObDropTenantStmt *stmt = static_cast<const ObDropTenantStmt *>(basic_stmt);
    if (OB_SYS_TENANT_ID != session_priv.tenant_id_) {
      ret = OB_ERR_NO_PRIVILEGE;
      LOG_WARN("Only sys tenant can do this operation", K(ret));
    } else if (stmt->get_tenant_name() == OB_SYS_TENANT_NAME
        || stmt->get_tenant_name() == OB_GTS_TENANT_NAME) {
      ret = OB_ERR_NO_PRIVILEGE;
      LOG_WARN("Can not drop sys or gts tenant", K(ret));
    } else {
      need_priv.priv_set_ = OB_PRIV_SUPER;
      need_priv.priv_level_ = OB_PRIV_USER_LEVEL;
      ADD_NEED_PRIV(need_priv);
    }
  }
  return ret;
}

int get_truncate_table_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_UNLIKELY(stmt::T_TRUNCATE_TABLE != basic_stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be T_TRUNCATE_TABLE",
             K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else {
    ObNeedPriv need_priv;
    const ObTruncateTableStmt *stmt = static_cast<const ObTruncateTableStmt *>(basic_stmt);
    //as there is tenant id is truncate_table_arg, so I check this.
    //And not allow truncate other tenant's table. Even sys tenant.
    if (session_priv.tenant_id_ != stmt->get_tenant_id()) {
      ret = OB_ERR_NO_PRIVILEGE;
      LOG_WARN("Can not truncate other tenant's table. Should not be here except change"
               "tenant which not suggested", K(ret));
    } else if (OB_FAIL(ObPrivilegeCheck::can_do_operation_on_db(session_priv, stmt->get_database_name()))) {
      LOG_WARN("Can not do this operation on the database", K(session_priv), K(ret));
    } else {
      need_priv.db_ = stmt->get_database_name();
      need_priv.table_ = stmt->get_table_name();
      need_priv.priv_set_ = OB_PRIV_DROP;
      need_priv.priv_level_ = OB_PRIV_TABLE_LEVEL;
      ADD_NEED_PRIV(need_priv);
    }
  }
  return ret;
}

int get_rename_table_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_UNLIKELY(stmt::T_RENAME_TABLE != basic_stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be T_RENAME_TABLE",
             K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else {
    ObNeedPriv need_priv;
    const ObRenameTableStmt *stmt = static_cast<const ObRenameTableStmt *>(basic_stmt);
    if (session_priv.tenant_id_ != stmt->get_tenant_id()) {
      ret = OB_ERR_NO_PRIVILEGE;
      LOG_WARN("Can not rename other tenant's table. Should not be here except change"
               "tenant which not suggested", K(ret));
    } else {
      const obrpc::ObRenameTableArg &arg = stmt->get_rename_table_arg();
      for (int64_t idx = 0; OB_SUCC(ret) && idx < arg.rename_table_items_.count(); ++idx) {
        const obrpc::ObRenameTableItem &table_item = arg.rename_table_items_.at(idx);
        if (OB_FAIL(ObPrivilegeCheck::can_do_operation_on_db(session_priv, table_item.origin_db_name_))) {
          LOG_WARN("Can not do this operation on the database", K(session_priv), K(ret));
        } else if (OB_FAIL(ObPrivilegeCheck::can_do_operation_on_db(session_priv, table_item.new_db_name_))) {
          LOG_WARN("Can not do this operation on the database", K(session_priv), K(ret));
        } else {
          need_priv.db_ = table_item.origin_db_name_;
          need_priv.table_ = table_item.origin_table_name_;
          need_priv.priv_set_ = OB_PRIV_DROP | OB_PRIV_ALTER;
          need_priv.priv_level_ = OB_PRIV_TABLE_LEVEL;
          ADD_NEED_PRIV(need_priv);
          need_priv.db_ = table_item.new_db_name_;
          need_priv.table_ = table_item.new_table_name_;
          need_priv.priv_set_ = OB_PRIV_CREATE | OB_PRIV_INSERT;
          need_priv.priv_level_ = OB_PRIV_TABLE_LEVEL;
          ADD_NEED_PRIV(need_priv);
        }
      }
    }
  }
  return ret;
}

int get_create_table_like_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_UNLIKELY(stmt::T_CREATE_TABLE_LIKE != basic_stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be T_CREATE_TABLE_LIKE",
             K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else {
    ObNeedPriv need_priv;
    const ObCreateTableLikeStmt *stmt = static_cast<const ObCreateTableLikeStmt *>(basic_stmt);
    if (OB_FAIL(ret)) {
    } else if (session_priv.tenant_id_ != stmt->get_tenant_id()) {
      ret = OB_ERR_NO_PRIVILEGE;
      LOG_WARN("Can not create other tenant's table. Should not be here except change"
               "tenant which not suggested", K(ret));
    } else if (OB_FAIL(ObPrivilegeCheck::can_do_operation_on_db(session_priv, stmt->get_new_db_name()))) {
      LOG_WARN("Can not do this operation on the database", K(session_priv), K(ret));
    } else {
      need_priv.db_ = stmt->get_origin_db_name();
      need_priv.table_ = stmt->get_origin_table_name();
      need_priv.priv_set_ = OB_PRIV_SELECT;
      need_priv.priv_level_ = OB_PRIV_TABLE_LEVEL;
      ADD_NEED_PRIV(need_priv);
      need_priv.db_ = stmt->get_new_db_name();
      need_priv.table_ = stmt->get_new_table_name();
      need_priv.priv_set_ = OB_PRIV_CREATE;
      need_priv.priv_level_ = OB_PRIV_TABLE_LEVEL;
      ADD_NEED_PRIV(need_priv);
    }
  }
  return ret;
}

int get_sys_tenant_super_priv(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_SYS_TENANT_ID != session_priv.tenant_id_ &&
             stmt::T_ALTER_SYSTEM_SET_PARAMETER != basic_stmt->get_stmt_type() &&
             stmt::T_REFRESH_TIME_ZONE_INFO != basic_stmt->get_stmt_type() &&
             stmt::T_SWITCHOVER != basic_stmt->get_stmt_type()) {
    ret = OB_ERR_NO_PRIVILEGE;
    LOG_WARN("Only sys tenant can do this operation",
             K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else {
    ObNeedPriv need_priv;
    need_priv.priv_set_ = OB_PRIV_SUPER;
    need_priv.priv_level_ = OB_PRIV_USER_LEVEL;
    ADD_NEED_PRIV(need_priv);
  }
  return ret;
}

int get_sys_tenant_alter_system_priv(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_SYS_TENANT_ID != session_priv.tenant_id_ &&
             stmt::T_FLUSH_CACHE != basic_stmt->get_stmt_type() &&
             stmt::T_ALTER_SYSTEM_SET_PARAMETER != basic_stmt->get_stmt_type() &&
             stmt::T_FREEZE != basic_stmt->get_stmt_type() &&
             stmt::T_CLEAR_MERGE_ERROR != basic_stmt->get_stmt_type() &&
             stmt::T_ADMIN_MERGE != basic_stmt->get_stmt_type() &&
             stmt::T_ARCHIVE_LOG != basic_stmt->get_stmt_type() &&
             stmt::T_BACKUP_DATABASE != basic_stmt->get_stmt_type() && 
             stmt::T_BACKUP_MANAGE != basic_stmt->get_stmt_type() &&
             stmt::T_BACKUP_CLEAN != basic_stmt->get_stmt_type() &&
             stmt::T_DELETE_POLICY != basic_stmt->get_stmt_type() &&
             stmt::T_BACKUP_KEY != basic_stmt->get_stmt_type() &&
             stmt::T_RECOVER != basic_stmt->get_stmt_type() &&
             stmt::T_TABLE_TTL != basic_stmt->get_stmt_type() &&
             stmt::T_ALTER_SYSTEM_RESET_PARAMETER != basic_stmt->get_stmt_type() &&
             stmt::T_TRANSFER_PARTITION != basic_stmt->get_stmt_type()) {
    ret = OB_ERR_NO_PRIVILEGE;
    LOG_WARN("Only sys tenant can do this operation",
             K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else {
    ObNeedPriv need_priv;
    need_priv.priv_set_ = OB_PRIV_ALTER_SYSTEM;
    need_priv.priv_level_ = OB_PRIV_USER_LEVEL;
    ADD_NEED_PRIV(need_priv);
  }
  return ret;
}

int get_sys_tenant_create_resource_pool_priv(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_SYS_TENANT_ID != session_priv.tenant_id_) {
    ret = OB_ERR_NO_PRIVILEGE;
    LOG_WARN("Only sys tenant can do this operation",
             K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else if (stmt::T_CREATE_RESOURCE_POOL != basic_stmt->get_stmt_type()
             && stmt::T_DROP_RESOURCE_POOL  != basic_stmt->get_stmt_type()
             && stmt::T_ALTER_RESOURCE_POOL != basic_stmt->get_stmt_type()
             && stmt::T_SPLIT_RESOURCE_POOL != basic_stmt->get_stmt_type()
             && stmt::T_ALTER_RESOURCE_TENANT != basic_stmt->get_stmt_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be create/alter/drop/split resource pool",
             K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else {
    ObNeedPriv need_priv;
    need_priv.priv_set_ = OB_PRIV_CREATE_RESOURCE_POOL;
    need_priv.priv_level_ = OB_PRIV_USER_LEVEL;
    ADD_NEED_PRIV(need_priv);
  }
  return ret;
}

int get_sys_tenant_create_resource_unit_priv(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_SYS_TENANT_ID != session_priv.tenant_id_) {
    ret = OB_ERR_NO_PRIVILEGE;
    LOG_WARN("Only sys tenant can do this operation",
             K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else if (stmt::T_CREATE_RESOURCE_UNIT != basic_stmt->get_stmt_type()
             && stmt::T_ALTER_RESOURCE_UNIT  != basic_stmt->get_stmt_type()
             && stmt::T_DROP_RESOURCE_UNIT != basic_stmt->get_stmt_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be create/alter/drop resource",
             K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else {
    ObNeedPriv need_priv;
    need_priv.priv_set_ = OB_PRIV_CREATE_RESOURCE_UNIT;
    need_priv.priv_level_ = OB_PRIV_USER_LEVEL;
    ADD_NEED_PRIV(need_priv);
  }
  return ret;
}

int get_change_tenant_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  UNUSED(need_privs);
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_UNLIKELY(stmt::T_CHANGE_TENANT != basic_stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be T_CHANGE_TENANT",
             K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else if (OB_SYS_TENANT_ID != session_priv.tenant_id_) {
    ret = OB_ERR_NO_PRIVILEGE;
    LOG_WARN("Only sys tenant can do this operation",
             K(ret), "stmt type", basic_stmt->get_stmt_type());
  }
  return ret;
}

int get_boot_strap_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_UNLIKELY(stmt::T_BOOTSTRAP != basic_stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be T_BOOTSTRAP",
             K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else if (OB_SYS_TENANT_ID != session_priv.tenant_id_) {
    ret = OB_ERR_NO_PRIVILEGE;
    LOG_WARN("Only sys tenant can do this operation", K(ret));
  } else {
    ObNeedPriv need_priv;
    need_priv.priv_set_ = OB_PRIV_BOOTSTRAP;
    need_priv.priv_level_ = OB_PRIV_USER_LEVEL;
    ADD_NEED_PRIV(need_priv);
  }
  return ret;
}

int get_load_data_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  UNUSED(session_priv);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_UNLIKELY(stmt::T_LOAD_DATA != basic_stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be T_LOAD_DATA",
             K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else {
    const ObLoadDataStmt *load_data_stmt = static_cast<const ObLoadDataStmt *>(basic_stmt);
    if (OB_SUCC(ret)) {
      ObNeedPriv need_priv;
      need_priv.db_ = load_data_stmt->get_load_arguments().database_name_;
      need_priv.table_ = load_data_stmt->get_load_arguments().table_name_;
      need_priv.priv_set_ = OB_PRIV_INSERT;
      need_priv.is_sys_table_ = false;
      need_priv.is_for_update_ = false;
      need_priv.priv_level_ = OB_PRIV_TABLE_LEVEL;
      ADD_NEED_PRIV(need_priv);
    }
    if (OB_SUCC(ret)) {
      ObNeedPriv need_priv;
      need_priv.priv_set_ = OB_PRIV_FILE;
      need_priv.priv_level_ = OB_PRIV_USER_LEVEL;
      ADD_NEED_PRIV(need_priv);
    }
  }

  return ret;
}

int get_create_tablegroup_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  UNUSED(session_priv);
  int ret = OB_SUCCESS;
  UNUSED(session_priv);
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_UNLIKELY(stmt::T_CREATE_TABLEGROUP != basic_stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be T_CREATE_TABLEGROUP",
             K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else {
    if (OB_SUCC(ret)) {
      ObNeedPriv need_priv;
      need_priv.priv_set_ = OB_PRIV_CREATE;
      need_priv.priv_level_ = OB_PRIV_USER_LEVEL;
      ADD_NEED_PRIV(need_priv);
    }
  }
  return ret;
}

int get_drop_tablegroup_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_UNLIKELY(stmt::T_DROP_TABLEGROUP != basic_stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be T_DROP_TABLEGROUP",
             K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else {
    const ObDropTablegroupStmt *stmt = static_cast<const ObDropTablegroupStmt *>(basic_stmt);
    if (OB_SYS_TENANT_ID != session_priv.tenant_id_
        && 0 == stmt->get_tablegroup_name().compare(OB_SYS_TABLEGROUP_NAME)) { //tablegroup case sensetitive
      ret = OB_ERR_NO_PRIVILEGE;
      LOG_WARN("Only sys tenant can do drop sys tablegroup",
               K(ret), "stmt type", basic_stmt->get_stmt_type());
    } else {
      ObNeedPriv need_priv;
      need_priv.priv_set_ = OB_PRIV_DROP;
      need_priv.priv_level_ = OB_PRIV_USER_LEVEL;
      ADD_NEED_PRIV(need_priv);
    }
  }
  return ret;
}

int get_alter_tablegroup_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  UNUSED(session_priv);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_UNLIKELY(stmt::T_ALTER_TABLEGROUP != basic_stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be T_ALTER_TABLEGROUP",
             K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else {
    ObNeedPriv need_priv;
    need_priv.priv_set_ = OB_PRIV_ALTER;
    need_priv.priv_level_ = OB_PRIV_USER_LEVEL;
    ADD_NEED_PRIV(need_priv);
  }
  return ret;
}

int get_flashback_table_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  UNUSED(session_priv);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (stmt::T_FLASHBACK_TABLE_FROM_RECYCLEBIN != basic_stmt->get_stmt_type()
      && stmt::T_FLASHBACK_TABLE_TO_SCN != basic_stmt->get_stmt_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be T_FLASHBACK_TABLE",
             K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else {
    ObNeedPriv need_priv;
    need_priv.priv_set_ = OB_PRIV_SUPER;
    need_priv.priv_level_ = OB_PRIV_USER_LEVEL;
    ADD_NEED_PRIV(need_priv);
  }
  return ret;
}

int get_purge_recyclebin_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  UNUSED(session_priv);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (stmt::T_PURGE_RECYCLEBIN != basic_stmt->get_stmt_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be T_PURGE_RECYCLEBIN",
             K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else {
    ObNeedPriv need_priv;
    need_priv.priv_set_ = OB_PRIV_DROP;
    need_priv.priv_level_ = OB_PRIV_USER_LEVEL;
    ADD_NEED_PRIV(need_priv);
  }
  return ret;
}

int get_flashback_index_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  UNUSED(session_priv);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should not be NULL", K(ret));
  } else if (OB_UNLIKELY(stmt::T_FLASHBACK_INDEX != basic_stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be T_FLASHBACK_TABLE",
             K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else {
    ObNeedPriv need_priv;
    need_priv.priv_set_ = OB_PRIV_SUPER;
    need_priv.priv_level_ = OB_PRIV_USER_LEVEL;
    ADD_NEED_PRIV(need_priv);
  }

  return ret;
}

int get_flashback_database_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  UNUSED(session_priv);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_UNLIKELY(stmt::T_FLASHBACK_DATABASE != basic_stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be T_FLASHBACK_DATABASE",
             K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else {
    ObNeedPriv need_priv;
    need_priv.priv_set_ = OB_PRIV_SUPER;
    need_priv.priv_level_ = OB_PRIV_USER_LEVEL;
    ADD_NEED_PRIV(need_priv);
  }
  return ret;
}

int get_purge_table_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  UNUSED(session_priv);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_UNLIKELY(stmt::T_PURGE_TABLE != stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be T_PURGE_TABLE",
             K(ret), "stmt type", stmt->get_stmt_type());
  } else {
    ObNeedPriv need_priv;
    need_priv.priv_set_ = OB_PRIV_SUPER;
    need_priv.priv_level_ = OB_PRIV_USER_LEVEL;
    ADD_NEED_PRIV(need_priv);
  }
  return ret;
}

int get_purge_index_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  UNUSED(session_priv);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should not be NULL", K(ret));
  } else if (OB_UNLIKELY(stmt::T_PURGE_INDEX != stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be T_PURGE_INDEX",
             K(ret), "stmt type", stmt->get_stmt_type());
  } else {
    ObNeedPriv need_priv;
    need_priv.priv_set_ = OB_PRIV_SUPER;
    need_priv.priv_level_ = OB_PRIV_USER_LEVEL;
    ADD_NEED_PRIV(need_priv);
  }
  return ret;
}

int get_purge_database_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  UNUSED(session_priv);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_UNLIKELY(stmt::T_PURGE_DATABASE != stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be PURGE_DATABASE",
             K(ret), "stmt type", stmt->get_stmt_type());
  } else {
    ObNeedPriv need_priv;
    need_priv.priv_set_ = OB_PRIV_SUPER;
    need_priv.priv_level_ = OB_PRIV_USER_LEVEL;
    ADD_NEED_PRIV(need_priv);
  }
  return ret;

}

int get_flashback_tenant_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  UNUSED(session_priv);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_UNLIKELY(stmt::T_FLASHBACK_TENANT != basic_stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be T_FLASHBACK_TENANT",
             K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else {
    ObNeedPriv need_priv;
    if (OB_SYS_TENANT_ID != session_priv.tenant_id_) {
      ret = OB_ERR_NO_PRIVILEGE;
      LOG_WARN("Only sys tenant can do this operation", K(ret));
    } else {
      need_priv.priv_set_ = OB_PRIV_SUPER;
      need_priv.priv_level_ = OB_PRIV_USER_LEVEL;
      ADD_NEED_PRIV(need_priv);
    }
  }
  return ret;
}


int get_purge_tenant_stmt_need_privs(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  UNUSED(session_priv);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_UNLIKELY(stmt::T_PURGE_TENANT != stmt->get_stmt_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt type should be PURGE_TENANT",
             K(ret), "stmt type", stmt->get_stmt_type());
  } else {
    ObNeedPriv need_priv;
    if (OB_SYS_TENANT_ID != session_priv.tenant_id_) {
      ret = OB_ERR_NO_PRIVILEGE;
      LOG_WARN("Only sys tenant can do this operation", K(ret));
    } else {
      need_priv.priv_set_ = OB_PRIV_SUPER;
      need_priv.priv_level_ = OB_PRIV_USER_LEVEL;
      ADD_NEED_PRIV(need_priv);
    }
  }
  return ret;
}

int get_restore_point_priv(
    const ObSessionPrivInfo &session_priv,
    const ObStmt *basic_stmt,
    ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Basic stmt should be not be NULL", K(ret));
  } else if (OB_SYS_TENANT_ID == session_priv.tenant_id_) {
    ret = OB_ERR_NO_PRIVILEGE;
    LOG_WARN("Only non sys tenant can do this operation",
             K(ret), "stmt type", basic_stmt->get_stmt_type());
  } else {
    ObNeedPriv need_priv;
    need_priv.priv_set_ = OB_PRIV_SELECT;
    need_priv.priv_level_ = OB_PRIV_USER_LEVEL;
    ADD_NEED_PRIV(need_priv);
  }
  return ret;
}

const ObGetStmtNeedPrivsFunc ObPrivilegeCheck::priv_check_funcs_[] =
{
#define OB_STMT_TYPE_DEF(stmt_type, priv_check_func, id, action_type) priv_check_func,
#include "sql/resolver/ob_stmt_type.h"
#undef OB_STMT_TYPE_DEF
};

int ObPrivilegeCheck::check_read_only(const ObSqlCtx &ctx,
                                      const stmt::StmtType stmt_type,
                                      const bool has_global_variable,
                                      const ObStmtNeedPrivs &stmt_need_privs)
{
  int ret = OB_SUCCESS;
  const bool is_mysql_mode = lib::is_mysql_mode();
  if (OB_ISNULL(ctx.session_info_) || OB_ISNULL(ctx.schema_guard_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Session is NULL");
  } else if (ctx.session_info_->has_user_super_privilege()) {
    // super priv is only supported in mysql mode on design firstly. But some customer may use it in oracle mode to avoid this check in later time.
    // for upgrade compatibility, we still retain the oracle mode super priv checking here
  } else if (is_mysql_mode) {
    if (ObStmt::is_write_stmt(stmt_type, has_global_variable) &&
        OB_FAIL(ctx.schema_guard_->verify_read_only(ctx.session_info_->get_effective_tenant_id(),
                                                    stmt_need_privs))) {
      LOG_WARN("database or table is read only, cannot execute this stmt", K(ret));
    }
  } else if (!is_mysql_mode) {
    if (ObStmt::is_dml_write_stmt(stmt_type) &&
        OB_FAIL(ctx.schema_guard_->verify_read_only(ctx.session_info_->get_effective_tenant_id(),
                                                    stmt_need_privs))) {
      LOG_WARN("database or table is read only, cannot execute this stmt", K(ret));
    }
  }
  return ret;
}

/* 新的检查权限总入口。
  mysql mode, 或者 oracle mode的_enable_priv_check = false，沿用老的权限check逻辑，
  oracle mode并且_enable_priv_check = true，用新的权限check逻辑 */
int ObPrivilegeCheck::check_privilege_new(
    const ObSqlCtx &ctx,
    const ObStmt *basic_stmt,
    ObStmtNeedPrivs &stmt_need_privs,
    ObStmtOraNeedPrivs &stmt_ora_need_privs)
{
  int ret = OB_SUCCESS;
  if (!ObSchemaChecker::is_ora_priv_check()) {
    OZ (check_privilege(ctx, basic_stmt, stmt_need_privs));
  } else {
    if (ctx.disable_privilege_check_ == PRIV_CHECK_FLAG_DISABLE || ctx.is_remote_sql_) {
      //do not check privilege
    } else {
      if (OB_ISNULL(basic_stmt)
          || OB_ISNULL(ctx.session_info_)
          || OB_ISNULL(ctx.schema_guard_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("basic_stmt, ctx.session_info or ctx.schema_manager is NULL",
            K(ret), K(basic_stmt), "session_info", ctx.session_info_,
            "schema manager", ctx.schema_guard_);
      } else if (basic_stmt->is_explain_stmt()) {
        basic_stmt = static_cast<const ObExplainStmt*>(basic_stmt)->get_explain_query_stmt();
        if (OB_ISNULL(basic_stmt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Explain query stmt is NULL", K(ret));
        }
      } else {
        //do nothing
      }

      if (OB_SUCC(ret)) {
        common::ObSEArray<ObNeedPriv, 4> tmp_need_privs;
        common::ObSEArray<ObOraNeedPriv, 4> tmp_ora_need_privs;
        ObSessionPrivInfo session_priv;
        bool has_global_variable = false;
        OZ (ctx.session_info_->get_session_priv_info(session_priv));
        OX (session_priv.set_effective_tenant_id(ctx.session_info_->get_effective_tenant_id()));
        OZ (get_stmt_need_privs(session_priv, basic_stmt, tmp_need_privs));
        OZ (stmt_need_privs.need_privs_.assign(tmp_need_privs));
        /* set user id=-1， means: use current user executing sql and can change
           user to be checked should be -1.
           reason: normal sql stmt and inner sql stmt in pl can share plan cache
           how to change user to be checked:
              1. when execute pl, change priv_user_id in session
              2. when check priv, use priv_user_id in session  */
        OZ (get_stmt_ora_need_privs(common::OB_INVALID_ID,
                                    ctx,
                                    basic_stmt,
                                    tmp_ora_need_privs,
                                    CHECK_FLAG_NORMAL));
        OZ (stmt_ora_need_privs.need_privs_.assign(tmp_ora_need_privs));
        if (OB_SUCC(ret) && basic_stmt->get_stmt_type() == stmt::T_VARIABLE_SET) {
          has_global_variable =
                           static_cast<const ObVariableSetStmt*>(basic_stmt)->has_global_variable();
        }
        OZ (check_read_only(ctx, basic_stmt->get_stmt_type(), has_global_variable, stmt_need_privs));
        OZ (check_ora_privilege(ctx, stmt_ora_need_privs));
      }
    }
  }
  return ret;
}

int ObPrivilegeCheck::check_ora_privilege(
    const ObSqlCtx &ctx,
    const ObStmtOraNeedPrivs &stmt_ora_need_priv)
{
  int ret = OB_SUCCESS;
  if (ctx.disable_privilege_check_ == PRIV_CHECK_FLAG_DISABLE) {
    //do not check privilege
  } else if (OB_ISNULL(ctx.session_info_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Session is NULL");
  } else {
    ObIArray<uint64_t> &role_id_array = ctx.session_info_->get_enable_role_array();
    OZ (const_cast<ObSchemaGetterGuard *>(ctx.schema_guard_)->check_ora_priv(
              ctx.session_info_->get_login_tenant_id(),
              ctx.session_info_->get_priv_user_id(), stmt_ora_need_priv,
              role_id_array),
              K(ctx.session_info_->get_in_definer_named_proc()),
              K(ctx.session_info_->get_user_id()),
              ctx.session_info_->get_priv_user_id());
  }
  return ret;
}

int ObPrivilegeCheck::check_privilege(
    const ObSqlCtx &ctx,
    const ObStmt *basic_stmt,
    ObStmtNeedPrivs &stmt_need_privs)
{
  int ret = OB_SUCCESS;
  if (ctx.disable_privilege_check_ == PRIV_CHECK_FLAG_DISABLE || ctx.is_remote_sql_) {
    //do not check privilege
    //远端SQL生成执行计划不需要check权限，权限检查都在控制端完成，统一流程
  } else {
    if (OB_ISNULL(basic_stmt)
        || OB_ISNULL(ctx.session_info_)
        || OB_ISNULL(ctx.schema_guard_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("basic_stmt, ctx.session_info or ctx.schema_manager is NULL",
          K(ret), K(basic_stmt), "session_info", ctx.session_info_,
          "schema manager", ctx.schema_guard_);
    } else if (basic_stmt->is_explain_stmt()) {
      basic_stmt = static_cast<const ObExplainStmt*>(basic_stmt)->get_explain_query_stmt();
      if (OB_ISNULL(basic_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Explain query stmt is NULL", K(ret));
      }
    } else {
      //do nothing
    }

    if (OB_SUCC(ret)) {
      common::ObSEArray<ObNeedPriv, 4> tmp_need_privs;
      ObSessionPrivInfo session_priv;
      bool has_global_variable = false;
      if (OB_FAIL(ctx.session_info_->get_session_priv_info(session_priv))) {
        LOG_WARN("fail to get session priv info", K(ret));
      } else if (FALSE_IT(session_priv.set_effective_tenant_id(
                                          ctx.session_info_->get_effective_tenant_id()))) {
      } else if (OB_FAIL(get_stmt_need_privs(session_priv, basic_stmt, tmp_need_privs))) {
        LOG_WARN("Get stmt need privs error", K(ret));
      } else if (OB_FAIL(stmt_need_privs.need_privs_.assign(tmp_need_privs))) {
        LOG_WARN("fail to assign need_privs", K(ret));
      } else if (basic_stmt->get_stmt_type() == stmt::T_VARIABLE_SET) {
        has_global_variable =
                           static_cast<const ObVariableSetStmt*>(basic_stmt)->has_global_variable();
      }
      OZ (check_read_only(ctx, basic_stmt->get_stmt_type(), has_global_variable, stmt_need_privs));
      if (OB_SUCC(ret) && OB_FAIL(check_privilege(ctx, stmt_need_privs))) {
        LOG_WARN("privilege check not passed", K(ret));
      }
    }
  }
  return ret;
}

int adjust_session_priv(ObSchemaGetterGuard &schema_guard,
                        ObSessionPrivInfo &session_priv) {
  int ret = OB_SUCCESS;
  const ObUserInfo *user_info = NULL;
  if (OB_ISNULL(user_info = schema_guard.get_user_info(session_priv.tenant_id_, session_priv.user_id_))) {
    ret = OB_USER_NOT_EXIST;
    LOG_WARN("fail to get user_info", K(ret));
  } else {
    session_priv.user_name_ = user_info->get_user_name_str();
    session_priv.host_name_ = user_info->get_host_name_str();
  }
  return ret;
}

int ObPrivilegeCheck::check_privilege(
    const ObSqlCtx &ctx,
    const ObStmtNeedPrivs &stmt_need_priv)
{
  int ret = OB_SUCCESS;
  if (ctx.disable_privilege_check_ == PRIV_CHECK_FLAG_DISABLE) {
    //do not check privilege
  } else {
    ObSessionPrivInfo session_priv;
    if (OB_ISNULL(ctx.session_info_) || OB_ISNULL(ctx.schema_guard_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Session is NULL");
    } else {
      if (OB_FAIL(ctx.session_info_->get_session_priv_info(session_priv))) {
        LOG_WARN("fail to get session priv info", K(ret));
      } else if (ctx.session_info_->get_user_id() != ctx.session_info_->get_priv_user_id()
          && OB_FAIL(adjust_session_priv(*ctx.schema_guard_, session_priv))) {
        LOG_WARN("fail to assign enable role id array", K(ret));
      } else if (OB_UNLIKELY(!session_priv.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("Session priv is invalid", "tenant_id", session_priv.tenant_id_,
                 "user_id", session_priv.user_id_, K(ret));
      } else if (OB_FAIL(const_cast<ObSchemaGetterGuard *>(ctx.schema_guard_)->check_priv(
               session_priv, stmt_need_priv))) {
        LOG_WARN("No privilege", K(session_priv),
                 "disable check", ctx.disable_privilege_check_, K(ret));
      } else {
        //do nothing
      }
      LOG_DEBUG("check priv",
                K(session_priv),
                "enable_roles", ctx.session_info_->get_enable_role_array());
    }
  }
  return ret;
}

/* xinqi to do: 获取每个stmt需要的权限 */
int ObPrivilegeCheck::get_stmt_ora_need_privs(
    uint64_t user_id,
    const ObSqlCtx &ctx,
    const ObStmt *basic_stmt,
    ObIArray<ObOraNeedPriv> &need_privs,
    uint64_t check_flag)
{
  int ret = OB_SUCCESS;

  const ObDMLStmt *dml_stmt = NULL;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt is NULL", K(ret));
  } else if (OB_FAIL(one_level_stmt_ora_need_priv(user_id, ctx, basic_stmt,
     need_privs, check_flag))) {
    LOG_WARN("Failed to get one level stmt need priv", K(ret));
  } else if (basic_stmt->is_show_stmt()
             || (stmt::T_SELECT == basic_stmt->get_stmt_type()
                 && static_cast<const ObSelectStmt*>(basic_stmt)->is_from_show_stmt())) {
    //do not check sub-stmt of show_stmt
  } else if ((dml_stmt = dynamic_cast<const ObDMLStmt*>(basic_stmt))) {
    ObArray<ObSelectStmt*> child_stmts;
    if (OB_FAIL(dml_stmt->get_child_stmts(child_stmts))) {
      LOG_WARN("get child stmt failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
      const ObSelectStmt *sub_stmt = child_stmts.at(i);
      if (OB_ISNULL(sub_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Sub-stmt is NULL", K(ret));
      } else if (sub_stmt->is_view_stmt()
                 && (stmt::T_SELECT == basic_stmt->get_stmt_type()
                   || stmt::T_INSERT == basic_stmt->get_stmt_type()
                   || stmt::T_UPDATE == basic_stmt->get_stmt_type()
                   || stmt::T_DELETE == basic_stmt->get_stmt_type())) {
        //do not check privilege of view stmt, one level 已经递归处理了视图的情况
      } else if (OB_FAIL(get_stmt_ora_need_privs(user_id,
                                                 ctx,
                                                 sub_stmt,
                                                 need_privs,
                                                 check_flag))) {
        LOG_WARN("Failed to extract priv info of shild stmts", K(i), K(dml_stmt), K(ret));
      } else {
        //do nothing
      }
    }
  }
  return ret;
}

int ObPrivilegeCheck::get_stmt_need_privs(const ObSessionPrivInfo &session_priv,
                                          const ObStmt *basic_stmt,
                                          ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  const ObDMLStmt *dml_stmt = NULL;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt is NULL", K(ret));
  } else if (OB_FAIL(one_level_stmt_need_priv(session_priv, basic_stmt, need_privs))) {
    LOG_WARN("Failed to get one level stmt need priv", K(ret));
  } else if (basic_stmt->is_show_stmt()
             || (stmt::T_SELECT == basic_stmt->get_stmt_type()
                 && static_cast<const ObSelectStmt*>(basic_stmt)->is_from_show_stmt())) {
    //do not check sub-stmt of show_stmt
  } else if ((dml_stmt = dynamic_cast<const ObDMLStmt*>(basic_stmt))) {
    ObArray<ObSelectStmt*> child_stmts;
    if (OB_FAIL(dml_stmt->get_child_stmts(child_stmts))) {
      LOG_WARN("get child stmt failed", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < child_stmts.count(); ++i) {
      const ObSelectStmt *sub_stmt = child_stmts.at(i);
      if (OB_ISNULL(sub_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Sub-stmt is NULL", K(ret));
      } else if (sub_stmt->is_view_stmt() && ObStmt::is_dml_stmt(basic_stmt->get_stmt_type())) {
        //do not check privilege of view stmt
      } else if (OB_FAIL(get_stmt_need_privs(session_priv, sub_stmt, need_privs))) {
        LOG_WARN("Failed to extract priv info of shild stmts", K(i), K(dml_stmt), K(ret));
      } else {
        //do nothing
      }
    }
  }
  return ret;
}

int adjust_objpriv_for_grant_obj(
    const ObGrantStmt * grant_stmt,
    int org_n_need_privs,
    ObIArray<ObOraNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  bool need_adjust = true;
  CK (grant_stmt != NULL);
  const ObRawObjPrivArray &raw_obj_privs = grant_stmt->get_obj_priv_array();
  if (OB_UNLIKELY(raw_obj_privs.count() == 0)) {
    need_adjust = false;
  } else if (raw_obj_privs.count() == 1 && raw_obj_privs.at(0) == OBJ_PRIV_ID_SELECT) {
    need_adjust = false;
  } else {
    need_adjust = true;
  }
  if (need_adjust) {
    ObPackedObjPriv packed_obj_privs;
    OZ (ObPrivPacker::pack_raw_obj_priv_list(GRANT_OPTION,
        raw_obj_privs, packed_obj_privs));
    for (int i = org_n_need_privs; i < need_privs.count(); i ++) {
      OX (need_privs.at(i).obj_privs_ = packed_obj_privs);
    }
  }
  return ret;
}

/* 检查references权限时，检查的是create table的owner。
例如： z1用户：
 create table z3.t2(c1 int, c2 int, foreign key (c2) references z1.t1(c1));
 如果z3没有z1.t1的引用权限，是无法建立表的。
 */
int get_reference_ora_need_privs(
    const ObSqlCtx &ctx,
    const ObString &user_name,
    const common::ObSArray<obrpc::ObCreateForeignKeyArg> fk_array,
    ObIArray<ObOraNeedPriv> &need_privs,
    uint64_t check_flag)
{
  int ret = OB_SUCCESS;
  uint64_t user_id = OB_INVALID_ID;
  ObOraNeedPriv need_priv;
  ObPackedObjPriv packed_privs = 0;
  uint64_t tenant_id = ctx.session_info_->get_login_tenant_id();
  uint64_t table_id = OB_INVALID_ID;

  for (int i = 0; OB_SUCC(ret) && i < fk_array.count(); i++) {
    const obrpc::ObCreateForeignKeyArg &fk_arg = fk_array.at(i);
    if (!fk_arg.parent_database_.empty() && !fk_arg.parent_table_.empty()) {
      OZ (ObPrivPacker::pack_raw_obj_priv(NO_OPTION, OBJ_PRIV_ID_REFERENCES, packed_privs));
      OX (need_priv.db_name_ = fk_arg.parent_database_);
      OZ (ctx.schema_guard_->get_user_id(tenant_id, user_name, ObString(""), user_id));
      OX (need_priv.grantee_id_ = user_id);
      OZ (ctx.schema_guard_->get_table_id(tenant_id,
                                          fk_arg.parent_database_,
                                          fk_arg.parent_table_,
                                          false /* is_index */,
                                          share::schema::ObSchemaGetterGuard::NON_TEMP_WITH_NON_HIDDEN_TABLE_TYPE,
                                          table_id));
      OX (need_priv.obj_id_ = table_id);
      OX (need_priv.obj_level_ = OBJ_LEVEL_FOR_TAB_PRIV);
      OX (need_priv.obj_type_ = static_cast<uint64_t>(ObObjectType::TABLE));
      OX (need_priv.obj_privs_ = packed_privs);
      OX (need_priv.check_flag_ = check_flag);
      OZ (add_need_priv(need_privs, need_priv));
    }
  }
  return ret;
}

int get_create_table_ora_need_privs(
    uint64_t user_id,
    const ObSqlCtx &ctx,
    const ObStmt *basic_stmt,
    ObIArray<ObOraNeedPriv> &need_privs,
    uint64_t check_flag)
{
  int ret = OB_SUCCESS;
  const ObCreateTableStmt * ddl_stmt = dynamic_cast<const ObCreateTableStmt*>(basic_stmt);
  const ObAlterTableStmt * alter_stmt = dynamic_cast<const ObAlterTableStmt*>(basic_stmt);
  if (ddl_stmt != NULL) {
    if (ddl_stmt->get_sub_select() != NULL) {
      OZ (ObPrivilegeCheck::get_stmt_ora_need_privs(user_id,
                                                    ctx,
                                                    ddl_stmt->get_sub_select(),
                                                    need_privs,
                                                    check_flag));
    }
    /* 对于create view语句，还需要check view的owner是否有依赖的table, view等的任何直接权限*/
    if (ddl_stmt->is_view_stmt() && ddl_stmt->get_view_define() != NULL) {
      ObString host_name(OB_DEFAULT_HOST_NAME);
      const ObUserInfo* user_info = NULL;

      CK (ctx.session_info_ != NULL);
      OZ (ctx.schema_guard_->get_user_info(ctx.session_info_->get_login_tenant_id(),
                                            ddl_stmt->get_database_name(),
                                            host_name,
                                            user_info));
      CK (user_info != NULL);
      OZ (ObPrivilegeCheck::get_stmt_ora_need_privs(user_info->get_user_id(),
                                                    ctx,
                                                    ddl_stmt->get_view_define(),
                                                    need_privs,
                                                    CHECK_FLAG_DIRECT));

    }
    /* add reference obj priv if necessary */
    OZ (get_reference_ora_need_privs(ctx,
                                     ddl_stmt->get_database_name(),
                                     ddl_stmt->get_read_only_foreign_key_arg_list(),
                                     need_privs,
                                     check_flag));
  } else if (alter_stmt != NULL) {
    /* add reference obj priv if necessary */
    OZ (get_reference_ora_need_privs(ctx,
                                     alter_stmt->get_database_name(),
                                     alter_stmt->get_read_only_foreign_key_arg_list(),
                                     need_privs,
                                     check_flag));

  }
  return ret;
}

int get_grant_ora_need_privs(
    const ObSqlCtx &ctx,
    const ObStmt *basic_stmt,
    ObIArray<ObOraNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  const ObGrantStmt * grant_stmt = dynamic_cast<const ObGrantStmt*>(basic_stmt);
  if (grant_stmt != NULL && grant_stmt->get_ref_query() != NULL) {
    ObString host_name(OB_DEFAULT_HOST_NAME);
    const ObUserInfo* user_info = NULL;
    int org_n_need_privs = 0;

    /* ref query是视图定义的展开，视图的owner记录在grant stmt的database_name中*/
    CK (ctx.schema_guard_ != NULL);
    CK (ctx.session_info_ != NULL);
    OZ (ctx.schema_guard_->get_user_info(ctx.session_info_->get_login_tenant_id(),
                                          grant_stmt->get_database_name(),
                                          host_name,
                                          user_info));
    CK (user_info != NULL);
    OX (org_n_need_privs = need_privs.count());
    OZ (ObPrivilegeCheck::get_stmt_ora_need_privs(user_info->get_user_id(),
                                                  ctx,
                                                  grant_stmt->get_ref_query(),
                                                  need_privs,
                                                  CHECK_FLAG_WITH_GRANT_OPTION));
    OZ (adjust_objpriv_for_grant_obj(grant_stmt, org_n_need_privs, need_privs));
  }
  return ret;
}

int sys_pkg_need_priv_check(uint64_t pkg_id, ObSchemaGetterGuard *schema_guard,
                            bool &need_check, uint64_t &pkg_spec_id,
                            bool &need_only_obj_check)
{
  static const char *pkg_name_need_priv[] = {
    /* add package's name here, who need to be check priv, for example */
    "dbms_plan_cache",
    "dbms_resource_manager",
  };
  static const char *pkg_name_only_need_obj_priv[] = {
    /* add package's name here, who need to be check priv, for example */
    "dbms_plan_cache",
  };
  int ret = OB_SUCCESS;
  int64_t compatible_mode = lib::is_oracle_mode() ? COMPATIBLE_ORACLE_MODE
                                                  : COMPATIBLE_MYSQL_MODE;
  need_check = false;
  need_only_obj_check = false;
  pkg_spec_id = OB_INVALID_ID;
  const uint64_t tenant_id = pl::get_tenant_id_by_object_id(pkg_id);
  if (OB_NOT_NULL(schema_guard) && OB_INVALID_ID != pkg_id && COMPATIBLE_ORACLE_MODE == compatible_mode) {
    const ObSimplePackageSchema *pkg_schema = NULL;
    if (OB_FAIL(schema_guard->get_simple_package_info(tenant_id, pkg_id, pkg_schema))) {
      LOG_WARN("failed to get pkg schema", K(ret), K(tenant_id), K(pkg_id));
    } else if (OB_ISNULL(pkg_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null pkg schema, unexpected", K(ret), K(pkg_id));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(pkg_name_need_priv); ++i) {
        if (0 == pkg_schema->get_package_name().case_compare(pkg_name_need_priv[i])) {
          need_check = true;
          for (int64_t j = 0; OB_SUCC(ret) && j < ARRAYSIZEOF(pkg_name_only_need_obj_priv); ++j) {
            if (0 == pkg_schema->get_package_name().case_compare(pkg_name_only_need_obj_priv[j])) {
              need_only_obj_check = true;
              break;
            }
          }
          break;
        }
      }
      if (OB_SUCC(ret) && need_check) {
        OZ (schema_guard->get_package_id(pkg_schema->get_tenant_id(),
                                         pkg_schema->get_database_id(),
                                         pkg_schema->get_package_name(),
                                         PACKAGE_TYPE,
                                         compatible_mode,
                                         pkg_spec_id));
      }
    }
  }
  return ret;
}

int get_call_ora_need_privs(
    const ObSqlCtx &ctx,
    uint64_t user_id,
    const ObStmt *basic_stmt,
    ObIArray<ObOraNeedPriv> &need_privs,
    uint64_t check_flag)
{
  int ret = OB_SUCCESS;
  ObOraNeedPriv need_priv;
  ObPackedObjPriv packed_privs = 0;
  ObCallProcedureStmt * call_stmt = const_cast<ObCallProcedureStmt *>(dynamic_cast<const ObCallProcedureStmt*>(basic_stmt));
  if (call_stmt != NULL && call_stmt->get_call_proc_info() != NULL) {
    need_priv.db_name_ = call_stmt->get_call_proc_info()->get_db_name();
    uint64_t pkg_id = call_stmt->get_call_proc_info()->get_package_id();
    /* 对于sys库的package，不需要权限 */
    if (need_priv.db_name_ == OB_SYS_DATABASE_NAME
       && (OB_INVALID_ID == pkg_id
        || pl::get_tenant_id_by_object_id(pkg_id) == OB_SYS_TENANT_ID)) {
      bool need_check_priv = false;
      bool need_only_obj_check = false;
      ObSQLSessionInfo *session_info = ctx.session_info_;
      CK (OB_NOT_NULL(session_info));
      uint64_t spec_id = OB_INVALID_ID;
      OZ (sys_pkg_need_priv_check(pkg_id, ctx.schema_guard_, need_check_priv, spec_id,
                                  need_only_obj_check));
      LOG_DEBUG("check sys package priv", K(pkg_id), K(need_check_priv), K(ret));
      if (need_check_priv) {
        OX (need_priv.grantee_id_ = user_id);
        /* mock user tenent schema id */
        OX (need_priv.obj_id_ = pkg_id);
        OX (need_priv.obj_type_ = need_only_obj_check ?
                                  static_cast<uint64_t>(ObObjectType::SYS_PACKAGE_ONLY_OBJ_PRIV)
                                : static_cast<uint64_t>(ObObjectType::SYS_PACKAGE));
        OX (need_priv.obj_level_ = OBJ_LEVEL_FOR_TAB_PRIV);
        OX (need_priv.check_flag_ = check_flag);
        OZ (set_need_priv_owner_id(ctx, need_priv));
        OZ (ObPrivPacker::pack_raw_obj_priv(NO_OPTION, OBJ_PRIV_ID_EXECUTE, packed_privs));
        OX (need_priv.obj_privs_ = packed_privs);
        OZ (add_need_priv(need_privs, need_priv));
      }
    } else {
      need_priv.grantee_id_ = user_id;
      if (call_stmt->get_call_proc_info()->get_package_id() != OB_INVALID_ID) {
        need_priv.obj_id_ = call_stmt->get_call_proc_info()->get_package_id();
        need_priv.obj_type_ = static_cast<uint64_t>(ObObjectType::PACKAGE);
      } else {
        need_priv.obj_id_ = call_stmt->get_call_proc_info()->get_routine_id();
        need_priv.obj_type_ = static_cast<uint64_t>(ObObjectType::PROCEDURE);
      }
      need_priv.obj_level_ = OBJ_LEVEL_FOR_TAB_PRIV;
      need_priv.check_flag_ = check_flag;
      OZ (set_need_priv_owner_id(ctx, need_priv));
      OZ (ObPrivPacker::pack_raw_obj_priv(NO_OPTION, OBJ_PRIV_ID_EXECUTE, packed_privs));
      OX (need_priv.obj_privs_ = packed_privs);
      OZ (add_need_priv(need_privs, need_priv));
    }
  }
  return ret;
}

/* flashback table to scn
To flash back a table to an earlier SCN or timestamp,
1. you must have either the FLASHBACK object privilege on the table or the
  FLASHBACK ANY TABLE system privilege.
2. In addition, you must have the READ or SELECT object privilege on the table,
and you must have the INSERT, DELETE, and ALTER object privileges on the table.

在resovle阶段，检查ddl相关的权限：flashback权限和alter 权限。
本阶段，检查dml相关权限：select，insert，delete */
int get_fb_tbl_scn_ora_need_privs(
    const ObSqlCtx &ctx,
    uint64_t user_id,
    const ObStmt *basic_stmt,
    ObIArray<ObOraNeedPriv> &need_privs,
    uint64_t check_flag)
{
  int ret = OB_SUCCESS;
  ObOraNeedPriv need_priv;
  ObPackedObjPriv packed_privs = 0;
  const ObFlashBackTableToScnStmt * ddl_stmt =
        dynamic_cast<const ObFlashBackTableToScnStmt*>(basic_stmt);

  if (ddl_stmt != NULL) {
    CK (ctx.session_info_ != NULL);
    CK (ctx.schema_guard_ != NULL);
    const obrpc::ObFlashBackTableToScnArg &arg = ddl_stmt->flashback_table_to_scn_arg_;
    const ObSimpleTableSchemaV2 *simple_table_schema = NULL;
    uint64_t db_id = OB_INVALID_ID;
    uint64_t tenant_id = ctx.session_info_->get_login_tenant_id();
    OZ (ObPrivPacker::pack_raw_obj_priv(NO_OPTION, OBJ_PRIV_ID_SELECT, packed_privs));
    OZ (ObPrivPacker::append_raw_obj_priv(NO_OPTION, OBJ_PRIV_ID_INSERT, packed_privs));
    OZ (ObPrivPacker::append_raw_obj_priv(NO_OPTION, OBJ_PRIV_ID_DELETE, packed_privs));
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.tables_.count(); ++i) {
      need_priv.db_name_ = arg.tables_.at(i).database_name_;
      need_priv.grantee_id_ = user_id;
      OZ (ctx.schema_guard_->get_database_id(tenant_id,
                                             arg.tables_.at(i).database_name_,
                                             db_id));
      if (OB_SUCC(ret) && db_id == OB_INVALID_ID) {
        ret = OB_ERR_BAD_DATABASE;
        LOG_USER_ERROR(OB_ERR_BAD_DATABASE, arg.tables_.at(i).database_name_.length(),
                       arg.tables_.at(i).database_name_.ptr());
        LOG_WARN("database not exists", K(ret), K(i), K(tenant_id),
                 K(arg.tables_.at(i).database_name_));
      }
      OZ (ctx.schema_guard_->get_simple_table_schema(tenant_id,
                                                     db_id,
                                                     arg.tables_.at(i).table_name_,
                                                     false,/*is index*/
                                                     simple_table_schema));
      if (OB_SUCC(ret) && simple_table_schema == NULL) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("table not exists", K(ret), K(i), K(tenant_id),
                  K(arg.tables_.at(i).table_name_), K(arg.tables_.at(i).database_name_));
      }
      OX (need_priv.obj_id_ = simple_table_schema->get_table_id());
      OX (need_priv.obj_level_ = OBJ_LEVEL_FOR_TAB_PRIV);
      OX (need_priv.obj_type_ = static_cast<uint64_t>(ObObjectType::TABLE));
      OX (need_priv.obj_privs_ = packed_privs);
      OX (need_priv.check_flag_ = check_flag);
      OZ (set_need_priv_owner_id(ctx, need_priv));
      OZ (add_need_priv(need_privs, need_priv));
    }
  }
  return ret;
}

int get_load_data_ora_need_privs(
    const ObSqlCtx &ctx,
    uint64_t user_id,
    const ObStmt *basic_stmt,
    ObIArray<ObOraNeedPriv> &need_privs,
    uint64_t check_flag)
{
  int ret = OB_SUCCESS;
  ObOraNeedPriv need_priv;
  ObPackedObjPriv packed_privs = 0;
  const ObLoadDataStmt *cmd_stmt =
        dynamic_cast<const ObLoadDataStmt*>(basic_stmt);
  const ObDirectorySchema *directory_schema = NULL;
  uint64_t tenant_id = ctx.session_info_->get_login_tenant_id();
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(basic_stmt));
  } else if (OB_FAIL(ctx.schema_guard_->get_directory_schema_by_name(
        tenant_id, "DEFAULT", directory_schema))) {
    LOG_WARN("fail to get directory schema", K(ret), K(tenant_id));
  } else if (OB_ISNULL(directory_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("directory schema is null", K(ret), KP(directory_schema));
  } else {
    UNUSED(ctx);
    OZ (ObPrivPacker::pack_raw_obj_priv(NO_OPTION, OBJ_PRIV_ID_INSERT, packed_privs));
    OZ (ObPrivPacker::append_raw_obj_priv(NO_OPTION, OBJ_PRIV_ID_UPDATE, packed_privs));
    need_priv.db_name_ = cmd_stmt->get_load_arguments().database_name_;
    need_priv.grantee_id_ = user_id;
    need_priv.obj_id_ = cmd_stmt->get_load_arguments().table_id_;
    need_priv.obj_level_ = OBJ_LEVEL_FOR_TAB_PRIV;
    need_priv.obj_type_ = static_cast<uint64_t>(ObObjectType::TABLE);
    need_priv.obj_privs_ = packed_privs;
    need_priv.check_flag_ = check_flag;
    OZ (set_need_priv_owner_id(ctx, need_priv));
    OZ (add_need_priv(need_privs, need_priv));
  }

  return ret;
}

/* 根据 stmt里面的数据库对象信息和stmt type，确定需要什么类型的权限 */
int ObPrivilegeCheck::one_level_stmt_ora_need_priv(
    uint64_t user_id,
    const ObSqlCtx &ctx,
    const ObStmt *basic_stmt,
    ObIArray<ObOraNeedPriv> &need_privs,
    uint64_t check_flag)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = ctx.session_info_;
  ObSessionPrivInfo session_priv;
  if (OB_ISNULL(session_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Session info is invalid", K(session_info), K(ret));
  } else if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt is NULL", K(basic_stmt), K(ret));
  }
  if (OB_SUCC(ret)) {
    stmt::StmtType stmt_type = basic_stmt->get_stmt_type();
    if (stmt_type < 0
        || stmt::get_stmt_type_idx(stmt_type) >=
          static_cast<int64_t>(sizeof(priv_check_funcs_) / sizeof(ObGetStmtNeedPrivsFunc))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Stmt type is error", K(ret), K(stmt_type));
    } else if (session_info->is_tenant_changed()
               && !ObStmt::check_change_tenant_stmt(stmt_type)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("stmt invalid", K(ret), K(stmt_type));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant changed, statement");
    } else {
      switch (stmt_type) {
        case stmt::T_SELECT:
        case stmt::T_REPLACE:
        case stmt::T_DELETE:
        case stmt::T_UPDATE:
        case stmt::T_MERGE:
        case stmt::T_INSERT:
          OZ (get_dml_stmt_ora_need_privs(user_id, ctx, session_priv,
             basic_stmt, need_privs, check_flag));
          break;
        case stmt::T_CREATE_TABLE:
        case stmt::T_ALTER_TABLE:
          OZ (get_create_table_ora_need_privs(user_id, ctx, basic_stmt, need_privs, check_flag));
          break;
        case stmt::T_GRANT:
          OZ (get_grant_ora_need_privs(ctx, basic_stmt, need_privs));
          break;
        case stmt::T_CALL_PROCEDURE:
          OZ (get_call_ora_need_privs(ctx, user_id, basic_stmt, need_privs, check_flag));
          break;
        case stmt::T_FLASHBACK_TABLE_TO_SCN:
          OZ (get_fb_tbl_scn_ora_need_privs(ctx, user_id, basic_stmt, need_privs, check_flag));
          break;
        default:
          break;
      }
    }
  }
  return ret;
}

int ObPrivilegeCheck::one_level_stmt_need_priv(const ObSessionPrivInfo &session_priv,
                                               const ObStmt *basic_stmt,
                                               ObIArray<ObNeedPriv> &need_privs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(basic_stmt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Stmt is NULL", K(basic_stmt), K(ret));
  } else if (OB_UNLIKELY(!session_priv.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Session priv is invalid", K(ret), K(session_priv));
  } else {
    stmt::StmtType stmt_type = basic_stmt->get_stmt_type();
    if (stmt_type < 0
        || stmt::get_stmt_type_idx(stmt_type) >= static_cast<int64_t>(sizeof(priv_check_funcs_) / sizeof(ObGetStmtNeedPrivsFunc))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Stmt type is error", K(ret), K(stmt_type));
    } else if (session_priv.is_tenant_changed()
               && !ObStmt::check_change_tenant_stmt(stmt_type)
               && stmt_type != stmt::T_SYSTEM_GRANT
               && stmt_type != stmt::T_REFRESH_TIME_ZONE_INFO) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("stmt invalid", K(ret), K(stmt_type), K(session_priv));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant changed, statement");
    } else if (OB_ISNULL(priv_check_funcs_[stmt::get_stmt_type_idx(stmt_type)])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("No stmt privilege check function", K(ret), K(stmt_type));
    } else if (OB_FAIL(priv_check_funcs_[stmt::get_stmt_type_idx(stmt_type)](session_priv, basic_stmt, need_privs))) {
      LOG_WARN("Failed to check priv", K(ret), K(stmt_type));
    } else { }//do nothing
  }
  return ret;
}

int ObPrivilegeCheck::can_do_operation_on_db(
    const ObSessionPrivInfo &session_priv,
    const ObString &db_name)
{
  int ret = OB_SUCCESS;
  if (session_priv.is_tenant_changed()
      && 0 != db_name.case_compare(OB_SYS_DATABASE_NAME)) {
    ret = OB_ERR_NO_DB_PRIVILEGE;
    LOG_USER_ERROR(OB_ERR_NO_DB_PRIVILEGE, session_priv.user_name_.length(), session_priv.user_name_.ptr(),
                   session_priv.host_name_.length(),session_priv.host_name_.ptr(),
                   db_name.length(), db_name.ptr());
  } else if (0 == db_name.case_compare(OB_INFORMATION_SCHEMA_NAME)
             || 0 == db_name.case_compare(OB_RECYCLEBIN_SCHEMA_NAME)
             //|| 0 == db_name.case_compare(OB_MYSQL_SCHEMA_NAME)
             || 0 == db_name.case_compare(OB_SYS_DATABASE_NAME)) {
    if (session_priv.tenant_id_ != OB_SYS_TENANT_ID) {
      if ((0 == db_name.case_compare(OB_RECYCLEBIN_SCHEMA_NAME))
          && ((0 == session_priv.user_name_.compare(OB_RESTORE_USER_NAME))
              || (0 == session_priv.user_name_.compare(OB_DRC_USER_NAME)))) {
        // do nothing，仅仅允许sync ddl user用户来operate recyclebin
      } else {
				ret = OB_ERR_NO_DB_PRIVILEGE;
				LOG_USER_ERROR(OB_ERR_NO_DB_PRIVILEGE, session_priv.user_name_.length(), session_priv.user_name_.ptr(),
											 session_priv.host_name_.length(),session_priv.host_name_.ptr(),
											 db_name.length(), db_name.ptr());
			}
		} else {
      //do nothing
		}
  } else {
		//do nothing
  }
  return ret;
}

int ObPrivilegeCheck::can_do_operation_on_db(const ObSessionPrivInfo &session_priv,
                                             const ObIArray<const ObDmlTableInfo*> &table_infos,
                                             const ObString &op_literal)
{
  int ret = OB_SUCCESS;
  if (is_sys_tenant(session_priv.tenant_id_)) {
    /* system tenant, no checking */
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_infos.count(); i++) {
      const ObDmlTableInfo *table_info = table_infos.at(i);
      if (OB_ISNULL(table_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table info is null");
      } else if (table_info->is_link_table_) {
        // skip link table
      } else if (is_inner_table(table_info->ref_table_id_)) {
        ret = OB_ERR_NO_TABLE_PRIVILEGE;
        LOG_USER_ERROR(OB_ERR_NO_TABLE_PRIVILEGE, op_literal.length(), op_literal.ptr(),
                      session_priv.user_name_.length(), session_priv.user_name_.ptr(),
                      session_priv.host_name_.length(),session_priv.host_name_.ptr(),
                      table_info->table_name_.length(), table_info->table_name_.ptr());
      }
    }
  }
  return ret;
}

int ObPrivilegeCheck::can_do_grant_on_db_table(
    const ObSessionPrivInfo &session_priv,
    const ObPrivSet priv_set,
    const ObString &db_name,
    const ObString &table_name)
{
  int ret = OB_SUCCESS;
  if (session_priv.is_tenant_changed()) {
    ret = OB_ERR_NO_DB_PRIVILEGE;
    LOG_USER_ERROR(OB_ERR_NO_DB_PRIVILEGE, session_priv.user_name_.length(), session_priv.user_name_.ptr(),
                   session_priv.host_name_.length(),session_priv.host_name_.ptr(),
                   db_name.length(), db_name.ptr());
  } else if (session_priv.tenant_id_ != OB_SYS_TENANT_ID) {
    if (0 == db_name.case_compare(OB_INFORMATION_SCHEMA_NAME)
      || 0 == db_name.case_compare(OB_RECYCLEBIN_SCHEMA_NAME)
      || 0 == db_name.case_compare(OB_PUBLIC_SCHEMA_NAME)
      || 0 == db_name.case_compare(OB_SYS_DATABASE_NAME)) {
      if (0 == db_name.case_compare(OB_INFORMATION_SCHEMA_NAME)
        || OB_PRIV_HAS_OTHER(priv_set, OB_PRIV_SELECT)) {
        ret = OB_ERR_NO_DB_PRIVILEGE;
        LOG_USER_ERROR(OB_ERR_NO_DB_PRIVILEGE, session_priv.user_name_.length(), session_priv.user_name_.ptr(),
                       session_priv.host_name_.length(),session_priv.host_name_.ptr(),
                       db_name.length(), db_name.ptr());
      }
    } else if (ObPrivilegeCheck::is_mysql_org_table(db_name, table_name)) {
      if (OB_PRIV_HAS_OTHER(priv_set, OB_PRIV_SELECT)) {
        ret = OB_ERR_NO_TABLE_PRIVILEGE;
        const char *command = "NOT-SELECT";
        LOG_USER_ERROR(OB_ERR_NO_TABLE_PRIVILEGE, (int)strlen(command), command,
                       session_priv.user_name_.length(), session_priv.user_name_.ptr(),
                       session_priv.host_name_.length(),session_priv.host_name_.ptr(),
                       table_name.length(), table_name.ptr());
      }
    } else { }//do nothing
  } else {
    if (0 == db_name.case_compare(OB_INFORMATION_SCHEMA_NAME)) {
      ret = OB_ERR_NO_DB_PRIVILEGE;
      LOG_USER_ERROR(OB_ERR_NO_DB_PRIVILEGE, session_priv.user_name_.length(), session_priv.user_name_.ptr(),
                     session_priv.host_name_.length(),session_priv.host_name_.ptr(),
                     db_name.length(), db_name.ptr());
    }
  }
  return ret;
}

int ObPrivilegeCheck::can_do_drop_operation_on_db(
    const ObSessionPrivInfo &session_priv,
    const ObString &db_name)
{
  int ret = OB_SUCCESS;
  if (session_priv.is_tenant_changed()) {
    ret = OB_ERR_NO_DB_PRIVILEGE;
    LOG_USER_ERROR(OB_ERR_NO_DB_PRIVILEGE, session_priv.user_name_.length(), session_priv.user_name_.ptr(),
                   session_priv.host_name_.length(),session_priv.host_name_.ptr(),
                   db_name.length(), db_name.ptr());
  } else if (0 == db_name.case_compare(OB_INFORMATION_SCHEMA_NAME)
      || 0 == db_name.case_compare(OB_SYS_DATABASE_NAME)
      || 0 == db_name.case_compare(OB_RECYCLEBIN_SCHEMA_NAME)
      || 0 == db_name.case_compare(OB_PUBLIC_SCHEMA_NAME)
      || 0 == db_name.case_compare(OB_MYSQL_SCHEMA_NAME)) {
      ret = OB_ERR_NO_DB_PRIVILEGE;
      LOG_USER_ERROR(OB_ERR_NO_DB_PRIVILEGE, session_priv.user_name_.length(), session_priv.user_name_.ptr(),
                     session_priv.host_name_.length(),session_priv.host_name_.ptr(),
                     db_name.length(), db_name.ptr());
  }
  return ret;
}

bool ObPrivilegeCheck::is_mysql_org_table(const ObString &db_name, const ObString &table_name)
{
  bool bret = false;
  if (0 == db_name.case_compare(OB_MYSQL_SCHEMA_NAME)) {
     if (0 == table_name.case_compare("user") || 0 == table_name.case_compare("db")){
       bret = true;
     }
  }
  return bret;
}

int ObPrivilegeCheck::check_password_expired(const ObSqlCtx &ctx, const stmt::StmtType stmt_type)
{
  int ret = OB_SUCCESS;
  if (stmt::T_SET_PASSWORD == stmt_type) {
    // do nothing
  } else if (OB_ISNULL(ctx.session_info_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("params's session info is null", K(ret));
  } else if (ctx.session_info_->is_password_expired()) {
    ret = OB_ERR_MUST_CHANGE_PASSWORD;
    LOG_WARN("the password is out of date, please change the password", K(ret));
  }
  return ret;
}

int ObPrivilegeCheck::check_password_expired_on_connection(
    const uint64_t tenant_id,
    const uint64_t user_id,
    ObSchemaGetterGuard &schema_guard,
    ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  if (ObOraSysChecker::is_super_user(user_id)) {
    //do nothing
  } else if (MYSQL_MODE == session.get_compatibility_mode()
             && OB_FAIL(check_password_life_time_mysql(tenant_id, user_id, schema_guard, session))) {
    LOG_WARN("The current user's password may be out of date", K(ret), K(tenant_id), K(user_id));
  } else if (ORACLE_MODE == session.get_compatibility_mode()
             && OB_FAIL(check_password_life_time_oracle(tenant_id, user_id, schema_guard, session))) {
    LOG_WARN("The current user's password may be out of date", K(ret), K(tenant_id), K(user_id));
  }
  return ret;
}

int ObPrivilegeCheck::check_password_life_time_mysql(
    const uint64_t tenant_id,
    const uint64_t user_id,
    ObSchemaGetterGuard &schema_guard,
    ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  const share::schema::ObUserInfo *user_info = NULL;
  uint64_t password_life_time = 0;
  if (OB_FAIL(session.get_sys_variable(share::SYS_VAR_DEFAULT_PASSWORD_LIFETIME,
                                              password_life_time))) {
    LOG_WARN("fail to get default_password_lifetime variable", K(ret));
  } else if (password_life_time != ObPasswordLifeTime::FOREVER) {
    if (OB_FAIL(schema_guard.get_user_info(tenant_id, user_id, user_info))) {
      LOG_WARN("fail to get user info", K(ret), K(tenant_id), K(user_id));
    } else if (NULL == user_info) {
      ret = OB_USER_NOT_EXIST;
      LOG_WARN("user is not exist", K(user_id), K(ret));
    } else if (check_password_expired_time(user_info->get_password_last_changed(),
                                                  password_life_time)) {
      session.set_password_expired(true);
      LOG_WARN("the password may have expired", K(ret));
    }
  }
  return ret;
}

bool ObPrivilegeCheck::check_password_expired_time(int64_t password_last_changed_ts,
                                                  int64_t password_life_time)
{
  bool is_expired = false;
  int64_t now = ObClockGenerator::getClock();
  int64_t timeline = now - password_life_time * 24 * 60 * 60 * 1000 * 1000UL;
  if (OB_UNLIKELY(timeline < 0)) {
    /*do nothing*/
  } else if ((uint64_t)(timeline) > (uint64_t)password_last_changed_ts) {
    is_expired = true;
    LOG_WARN_RET(OB_SUCCESS, "the password is out of date, please change the password");
  }
  return is_expired;
}

int ObPrivilegeCheck::check_password_life_time_oracle(
    const uint64_t tenant_id,
    const uint64_t user_id,
    ObSchemaGetterGuard &schema_guard,
    ObSQLSessionInfo &session)
{
  int ret = OB_SUCCESS;
  int64_t password_last_changed = INT64_MAX;
  int64_t password_life_time = INT64_MAX;
  int64_t password_grace_time = INT64_MAX;
  if (OB_FAIL(schema_guard.get_user_password_expire_times(tenant_id,
                                                          user_id,
                                                          password_last_changed,
                                                          password_life_time,
                                                          password_grace_time))) {
    LOG_WARN("fail to get user id and profile limit", K(ret));
  } else {
    int64_t now = ObTimeUtility::current_time();
    if (password_life_time == INT64_MAX || password_last_changed + password_life_time > now) {
      //do nothing
    } else if (password_grace_time == INT64_MAX ||
               password_last_changed + password_life_time + password_grace_time > now) {
      int64_t expire_in_days = (now - password_last_changed + password_life_time) / USECS_PER_DAY;
      LOG_WARN("the password will expire", K(expire_in_days));
    } else {
      // setting password expired flag on session allows user login to set password theirself
      session.set_password_expired(true);
      LOG_WARN("the password is expired", K(password_last_changed), K(password_life_time),
          K(password_grace_time), K(ret));
    }
  }
  return ret;
}

int ObPrivilegeCheck::check_priv_in_roles(
    const uint64_t tenant_id,
    const uint64_t user_id,
    ObSchemaGetterGuard &schema_guard,
    const ObIArray<uint64_t> &role_ids,
    const ObStmtNeedPrivs &stmt_need_priv)
{
  int ret = OB_SUCCESS;
  bool check_succ = false;
  ObArray<uint64_t> role_ids_queue;
  const ObUserInfo *user_info = NULL;
  if (OB_ISNULL(user_info = schema_guard.get_user_info(tenant_id, user_id))) {
    ret = OB_USER_NOT_EXIST;
    LOG_WARN("fail to get user_info", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < role_ids.count(); ++i) {
    uint64_t cur_role_id = role_ids.at(i);
    if (has_exist_in_array(user_info->get_role_id_array(), cur_role_id)) {
      //enabled role can be revoked from the current user by other session
      //only check the granted roles
      if (OB_FAIL(role_ids_queue.push_back(cur_role_id))) {
        LOG_WARN("fail to push back", K(ret));
      }
    }
  }

  for (int i = 0; OB_SUCC(ret) && i < role_ids_queue.count() && !check_succ; i++) {
    ObSessionPrivInfo session_priv;
    //for print correct error info
    session_priv.user_name_ = user_info->get_user_name_str();
    session_priv.host_name_ = user_info->get_host_name_str();
    OZ (schema_guard.get_session_priv_info(tenant_id, role_ids_queue.at(i), "", session_priv));

    if (OB_SUCC(ret)) {
      if (OB_SUCCESS == schema_guard.check_priv(session_priv, stmt_need_priv)) {
        check_succ = true;
      }
    }
    if (OB_SUCC(ret) && !check_succ) {
      const ObUserInfo *user_info = NULL;
      OZ (schema_guard.get_user_info(tenant_id, role_ids_queue.at(i), user_info));
      if (OB_SUCC(ret) && OB_ISNULL(user_info)) {
        ret = OB_USER_NOT_EXIST;
        LOG_WARN("user not exist", K(ret));
      }
      for (int j = 0; OB_SUCC(ret) && j < user_info->get_role_id_array().count(); j++) {
        OZ (role_ids_queue.push_back(user_info->get_role_id_array().at(j)));
      }
    }
  }

  if (OB_SUCC(ret) && !check_succ) {
    ret = OB_ERR_NO_PRIVILEGE;
  }
  return ret;
}

int ObPrivilegeCheck::get_priv_need_check(const ObSessionPrivInfo &session_priv,
                                          const ObCompatFeatureType feature_type,
                                          bool &need_check)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObCompatControl::check_feature_enable(session_priv.security_version_,
                                                    feature_type, need_check))) {
    LOG_WARN("failed to check feature enable", K(ret), K(feature_type));
  }
  return ret;
}

#undef ADD_NEED_PRIV

}
}
