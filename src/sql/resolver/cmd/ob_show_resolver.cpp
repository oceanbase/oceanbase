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

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/cmd/ob_show_resolver.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/schema/ob_priv_type.h"
#include "share/schema/ob_schema_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_sql_context.h"
#include "sql/parser/ob_parser.h"
#include "lib/charset/ob_charset.h"
#include "observer/ob_server_struct.h"
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
namespace oceanbase {
namespace sql {

ObShowResolver::ObShowResolver(ObResolverParams& params) : ObSelectResolver(params)
{
  params_.is_from_show_resolver_ = true;
}

ObShowResolver::~ObShowResolver()
{}

#define GEN_SQL_STEP_1(SHOW_STMT_TYPE, args...)                                                                     \
  do {                                                                                                              \
    if (OB_SUCC(ret)) {                                                                                             \
      show_resv_ctx.like_column_ =                                                                                  \
          SHOW_STMT_TYPE##_LIKE == NULL ? ObString::make_string("") : ObString::make_string(SHOW_STMT_TYPE##_LIKE); \
      if (OB_SUCC(ret) && OB_FAIL(sql_gen.init(params_.allocator_))) {                                              \
        LOG_WARN("fail to init sql string generator", K(ret));                                                      \
      } else if (OB_FAIL(sql_gen.gen_select_str(SHOW_STMT_TYPE##_SELECT, ##args))) {                                \
        LOG_WARN("fail to generate select string", K(ret));                                                         \
      } else { /*do nothing*/                                                                                       \
      }                                                                                                             \
    }                                                                                                               \
  } while (0)

#define GEN_SQL_STEP_2(SHOW_STMT_TYPE, args...)                                                         \
  do {                                                                                                  \
    if (OB_SUCC(ret)) {                                                                                 \
      if (is_oracle_mode && OB_FAIL(sql_gen.gen_from_str(SHOW_STMT_TYPE##_ORA_SUBQUERY, ##args))) {     \
        LOG_WARN("fail to generate from string in oracle mode", K(ret));                                \
      } else if (!is_oracle_mode && OB_FAIL(sql_gen.gen_from_str(SHOW_STMT_TYPE##_SUBQUERY, ##args))) { \
        LOG_WARN("fail to generate from string", K(ret));                                               \
      } else {                                                                                          \
        sql_gen.assign_sql_str(select_sql);                                                             \
      }                                                                                                 \
    }                                                                                                   \
  } while (0)

#define REAL_NAME(a, b) ((!is_oracle_mode) ? (a) : (b))

int ObShowResolver::resolve(const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  uint64_t real_tenant_id = OB_INVALID_ID;
  uint64_t sql_tenant_id = OB_INVALID_TENANT_ID;
  ObString database_name;
  ObSessionPrivInfo session_priv;
  ObString select_sql;
  ObString user_name;
  ObString host_name;
  ObSynonymChecker synonym_checker;
  uint64_t user_id = OB_INVALID_ID;
  ObShowResolverContext show_resv_ctx;
  if (OB_UNLIKELY(NULL == session_info_ || NULL == params_.allocator_ || NULL == schema_checker_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("data member is not init", K(ret), K(session_info_), K(params_.allocator_), K(schema_checker_));
  } else if (OB_UNLIKELY(parse_tree.type_ < T_SHOW_TABLES || parse_tree.type_ > T_SHOW_QUERY_RESPONSE_TIME) &&
             (parse_tree.type_ != T_SHOW_TRIGGERS)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected parse tree type", K(ret), K(parse_tree.type_));
  } else {
    real_tenant_id = session_info_->get_effective_tenant_id();
    sql_tenant_id = ObSchemaUtils::get_extract_tenant_id(real_tenant_id, real_tenant_id);
    database_name.assign_ptr(session_info_->get_database_name().ptr(), session_info_->get_database_name().length());
    user_name.assign_ptr(session_info_->get_user_name().ptr(), session_info_->get_user_name().length());
    host_name.assign_ptr(session_info_->get_host_name().ptr(), session_info_->get_host_name().length());
    user_id = session_info_->get_user_id();
    session_info_->get_session_priv_info(session_priv);
  }

  if (OB_SUCC(ret)) {
    show_resv_ctx.cur_tenant_id_ = real_tenant_id;
    show_resv_ctx.actual_tenant_id_ = real_tenant_id;
    show_resv_ctx.database_name_ = ObString("oceanbase");
    show_resv_ctx.parse_tree_ = &parse_tree;
  }

  if (OB_SUCC(ret)) {
    common::ObSEArray<int64_t, 4> subquery_params;
    ObArenaAllocator alloc;
    ObStmtNeedPrivs stmt_need_privs;
    stmt_need_privs.need_privs_.set_allocator(&alloc);
    ObSqlStrGenerator sql_gen;
    bool is_oracle_mode = share::is_oracle_mode();
    switch (parse_tree.type_) {
      case T_SHOW_TABLES: {
        if (is_oracle_mode) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support show table in oracle mode", K(ret));
        } else if (OB_UNLIKELY(parse_tree.num_child_ != 3 || NULL == parse_tree.children_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parse tree is wrong", K(ret), K(parse_tree.num_child_), K(parse_tree.children_));
        } else if (OB_UNLIKELY(NULL == parse_tree.children_[2])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parser tree child is NULL", K(ret), K(parse_tree.children_[2]));
        } else {
          show_resv_ctx.condition_node_ = parse_tree.children_[1];
          show_resv_ctx.stmt_type_ = stmt::T_SHOW_TABLES;
          ParseNode* condition_node = show_resv_ctx.condition_node_;
          uint64_t show_db_id = OB_INVALID_ID;
          if (OB_FAIL(get_database_info(
                  parse_tree.children_[0], database_name, real_tenant_id, show_resv_ctx, show_db_id))) {
            LOG_WARN("fail to get database info", K(ret));
          } else if (OB_UNLIKELY(OB_INVALID_ID == show_db_id)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("database id is invalid", K(ret), K(show_db_id));
          } else {
            if (0 == parse_tree.children_[2]->value_) {
              if (NULL != condition_node && T_LIKE_CLAUSE == condition_node->type_) {
                if (OB_UNLIKELY(condition_node->num_child_ != 2 || NULL == condition_node->children_)) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN(
                      "invalid like parse node", K(ret), K(condition_node->num_child_), K(condition_node->children_));
                } else if (OB_UNLIKELY(NULL == condition_node->children_[0] || NULL == condition_node->children_[1])) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("invalid like parse node",
                      K(ret),
                      K(condition_node->num_child_),
                      K(condition_node->children_[0]),
                      K(condition_node->children_[1]));

                } else {
                  GEN_SQL_STEP_1(ObShowSqlSet::SHOW_TABLES_LIKE,
                      show_resv_ctx.show_database_name_.length(),
                      show_resv_ctx.show_database_name_.ptr(),
                      static_cast<ObString::obstr_size_t>(
                          condition_node->children_[0]->str_len_),  // cast int64_t to obstr_size_t
                      condition_node->children_[0]->str_value_);
                  GEN_SQL_STEP_2(ObShowSqlSet::SHOW_TABLES_LIKE,
                      OB_SYS_DATABASE_NAME,
                      OB_TENANT_VIRTUAL_SHOW_TABLES_TNAME,
                      show_db_id);
                }
              } else {
                GEN_SQL_STEP_1(ObShowSqlSet::SHOW_TABLES,
                    show_resv_ctx.show_database_name_.length(),
                    show_resv_ctx.show_database_name_.ptr());
                GEN_SQL_STEP_2(
                    ObShowSqlSet::SHOW_TABLES, OB_SYS_DATABASE_NAME, OB_TENANT_VIRTUAL_SHOW_TABLES_TNAME, show_db_id);
              }
            } else if (1 == parse_tree.children_[2]->value_) {
              if (NULL != condition_node && T_LIKE_CLAUSE == condition_node->type_) {
                if (OB_UNLIKELY(condition_node->num_child_ != 2 || NULL == condition_node->children_[0] ||
                                NULL == condition_node->children_[1])) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("invalid like parse node",
                      K(ret),
                      K(condition_node->num_child_),
                      K(condition_node->children_[0]),
                      K(condition_node->children_[1]));
                } else {
                  GEN_SQL_STEP_1(ObShowSqlSet::SHOW_FULL_TABLES_LIKE,
                      show_resv_ctx.show_database_name_.length(),
                      show_resv_ctx.show_database_name_.ptr(),
                      static_cast<ObString::obstr_size_t>(
                          condition_node->children_[0]->str_len_),  // cast int64_t to obstr_size_t
                      condition_node->children_[0]->str_value_);
                  GEN_SQL_STEP_2(ObShowSqlSet::SHOW_FULL_TABLES_LIKE,
                      OB_SYS_DATABASE_NAME,
                      OB_TENANT_VIRTUAL_SHOW_TABLES_TNAME,
                      show_db_id);
                }
              } else {
                GEN_SQL_STEP_1(ObShowSqlSet::SHOW_FULL_TABLES,
                    show_resv_ctx.show_database_name_.length(),
                    show_resv_ctx.show_database_name_.ptr());
                GEN_SQL_STEP_2(ObShowSqlSet::SHOW_FULL_TABLES,
                    OB_SYS_DATABASE_NAME,
                    OB_TENANT_VIRTUAL_SHOW_TABLES_TNAME,
                    show_db_id);
              }
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("node value unexpected", K(parse_tree.value_));
              break;
            }

            // change where condition :Tables_in_xxx=>table_name
            if (OB_SUCCESS == ret && NULL != condition_node && T_WHERE_CLAUSE == condition_node->type_) {
              char* column_name = NULL;
              int64_t tmp_pos = 0;
              if (OB_FAIL(NULL == (column_name = static_cast<char*>(
                                       params_.allocator_->alloc(OB_MAX_COLUMN_NAME_BUF_LENGTH))))) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_ERROR("failed to alloc column name buf", K(column_name));
              } else if (OB_FAIL(databuff_printf(column_name,
                             OB_MAX_COLUMN_NAME_BUF_LENGTH,
                             tmp_pos,
                             "tables_in_%.*s",
                             show_resv_ctx.show_database_name_.length(),
                             show_resv_ctx.show_database_name_.ptr()))) {
                LOG_WARN("fail to add database name", K(show_resv_ctx.show_database_name_.ptr()));
                break;
              } else if (FALSE_IT(show_resv_ctx.column_name_ = ObString::make_string(column_name))) {
                // won't be here
              } else if (OB_FAIL(replace_where_clause(condition_node->children_[0], show_resv_ctx))) {
                LOG_WARN("fail to replace where clause", K(condition_node->children_[0]));
                break;
              }
            }
          }
        }
        break;
      }
      case T_SHOW_DATABASES: {
        if (is_oracle_mode) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support show database in oracle mode", K(ret));
        } else if (OB_UNLIKELY(parse_tree.num_child_ != 2 || NULL == parse_tree.children_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parse tree is wrong", K(ret), K(parse_tree.num_child_), K(parse_tree.children_));
        } else {
          show_resv_ctx.condition_node_ = parse_tree.children_[0];
          ParseNode* condition_node = show_resv_ctx.condition_node_;
          show_resv_ctx.stmt_type_ = stmt::T_SHOW_DATABASES;
          bool show_db_status = parse_tree.children_[1] != NULL ? true : false;
          if (NULL != show_resv_ctx.condition_node_ && T_LIKE_CLAUSE == show_resv_ctx.condition_node_->type_) {
            if (OB_UNLIKELY(show_resv_ctx.condition_node_->num_child_ != 2 ||
                            NULL == show_resv_ctx.condition_node_->children_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid like parse node",
                  K(ret),
                  K(show_resv_ctx.condition_node_->num_child_),
                  K(show_resv_ctx.condition_node_->children_));
            } else if (OB_UNLIKELY(NULL == condition_node->children_[0] || NULL == condition_node->children_[1])) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid like parse node",
                  K(ret),
                  K(condition_node->num_child_),
                  K(condition_node->children_[0]),
                  K(condition_node->children_[1]));
            } else {
              if (show_db_status) {
                GEN_SQL_STEP_1(ObShowSqlSet::SHOW_DATABASES_STATUS_LIKE,
                    static_cast<ObString::obstr_size_t>(
                        show_resv_ctx.condition_node_->children_[0]->str_len_),  // cast int64_t to obstr_size_t
                    show_resv_ctx.condition_node_->children_[0]->str_value_);
                GEN_SQL_STEP_2(ObShowSqlSet::SHOW_DATABASES_STATUS_LIKE,
                    OB_SYS_DATABASE_NAME,
                    OB_TENANT_VIRTUAL_DATABASE_STATUS_TNAME);
              } else {
                GEN_SQL_STEP_1(ObShowSqlSet::SHOW_DATABASES_LIKE,
                    static_cast<ObString::obstr_size_t>(
                        show_resv_ctx.condition_node_->children_[0]->str_len_),  // cast int64_t to obstr_size_t
                    show_resv_ctx.condition_node_->children_[0]->str_value_);
                if (is_oracle_mode) {
                  GEN_SQL_STEP_2(ObShowSqlSet::SHOW_DATABASES_LIKE,
                      OB_SYS_DATABASE_NAME,
                      OB_ALL_DATABASE_TNAME,
                      sql_tenant_id,
                      OB_RECYCLEBIN_SCHEMA_NAME,
                      OB_ORA_PUBLIC_SCHEMA_NAME,
                      OB_SYS_DATABASE_NAME);
                } else {
                  GEN_SQL_STEP_2(ObShowSqlSet::SHOW_DATABASES_LIKE,
                      OB_SYS_DATABASE_NAME,
                      OB_ALL_DATABASE_TNAME,
                      sql_tenant_id,
                      OB_RECYCLEBIN_SCHEMA_NAME,
                      OB_PUBLIC_SCHEMA_NAME,
                      OB_PUBLIC_SCHEMA_NAME);
                }
              }
            }
          } else {
            if (show_db_status) {
              GEN_SQL_STEP_1(ObShowSqlSet::SHOW_DATABASES_STATUS);
              GEN_SQL_STEP_2(
                  ObShowSqlSet::SHOW_DATABASES_STATUS, OB_SYS_DATABASE_NAME, OB_TENANT_VIRTUAL_DATABASE_STATUS_TNAME);
            } else {
              GEN_SQL_STEP_1(ObShowSqlSet::SHOW_DATABASES);
              if (is_oracle_mode) {
                GEN_SQL_STEP_2(ObShowSqlSet::SHOW_DATABASES,
                    OB_SYS_DATABASE_NAME,
                    OB_ALL_DATABASE_TNAME,
                    sql_tenant_id,
                    OB_RECYCLEBIN_SCHEMA_NAME,
                    OB_ORA_PUBLIC_SCHEMA_NAME,
                    OB_SYS_DATABASE_NAME);
              } else {
                GEN_SQL_STEP_2(ObShowSqlSet::SHOW_DATABASES,
                    OB_SYS_DATABASE_NAME,
                    OB_ALL_DATABASE_TNAME,
                    sql_tenant_id,
                    OB_RECYCLEBIN_SCHEMA_NAME,
                    OB_PUBLIC_SCHEMA_NAME,
                    OB_PUBLIC_SCHEMA_NAME);
              }
            }
          }
        }
        break;
      }
      case T_SHOW_VARIABLES: {
        if (OB_UNLIKELY(parse_tree.num_child_ != 2 || NULL == parse_tree.children_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parse tree is wrong", K(ret), K(parse_tree.num_child_), K(parse_tree.children_));
        } else if (OB_UNLIKELY(NULL == parse_tree.children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parser tree child is NULL", K(ret), K(parse_tree.children_[0]));
        } else {
          show_resv_ctx.condition_node_ = parse_tree.children_[1];
          show_resv_ctx.stmt_type_ = stmt::T_SHOW_VARIABLES;
          show_resv_ctx.global_scope_ = 1 == parse_tree.children_[0]->value_ ? true : false;
          if (true == show_resv_ctx.global_scope_) {
            GEN_SQL_STEP_1(ObShowSqlSet::SHOW_GLOBAL_VARIABLES);
            GEN_SQL_STEP_2(ObShowSqlSet::SHOW_GLOBAL_VARIABLES,
                REAL_NAME(OB_SYS_DATABASE_NAME, OB_ORA_SYS_SCHEMA_NAME),
                REAL_NAME(OB_TENANT_VIRTUAL_GLOBAL_VARIABLE_TNAME, OB_TENANT_VIRTUAL_GLOBAL_VARIABLE_ORA_TNAME));
          } else {
            GEN_SQL_STEP_1(ObShowSqlSet::SHOW_VARIABLES);
            GEN_SQL_STEP_2(ObShowSqlSet::SHOW_VARIABLES,
                REAL_NAME(OB_SYS_DATABASE_NAME, OB_ORA_SYS_SCHEMA_NAME),
                REAL_NAME(OB_TENANT_VIRTUAL_SESSION_VARIABLE_TNAME, OB_TENANT_VIRTUAL_SESSION_VARIABLE_ORA_TNAME));
          }
        }
        break;
      }
      case T_SHOW_COLUMNS: {
        // desc table
        if (OB_UNLIKELY(parse_tree.num_child_ != 4 || NULL == parse_tree.children_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parse tree is wrong", K(ret), K(parse_tree.num_child_), K(parse_tree.children_));
        } else if (OB_UNLIKELY(NULL == parse_tree.children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parser tree child is NULL", K(ret), K(parse_tree.children_[0]));
        } else {
          ObString show_db_name;
          uint64_t show_db_id = OB_INVALID_ID;
          ObString show_table_name;
          uint64_t show_table_id = OB_INVALID_ID;
          bool is_view;
          show_resv_ctx.condition_node_ = parse_tree.children_[3];
          show_resv_ctx.stmt_type_ = stmt::T_SHOW_COLUMNS;
          if (OB_FAIL(resolve_show_from_table(parse_tree.children_[1],
                  parse_tree.children_[2],
                  database_name.empty(),
                  T_SHOW_COLUMNS,
                  real_tenant_id,
                  show_db_name,
                  show_db_id,
                  show_table_name,
                  show_table_id,
                  is_view,
                  synonym_checker))) {
            LOG_WARN("fail to resolve show from table", K(ret));
          } else {
            if (1 == parse_tree.children_[0]->value_) {
              GEN_SQL_STEP_1(ObShowSqlSet::SHOW_FULL_COLUMNS);
              GEN_SQL_STEP_2(ObShowSqlSet::SHOW_FULL_COLUMNS,
                  REAL_NAME(OB_SYS_DATABASE_NAME, OB_ORA_SYS_SCHEMA_NAME),
                  REAL_NAME(OB_TENANT_VIRTUAL_TABLE_COLUMN_TNAME, OB_TENANT_VIRTUAL_TABLE_COLUMN_ORA_TNAME),
                  show_table_id);
            } else {
              GEN_SQL_STEP_1(ObShowSqlSet::SHOW_COLUMNS);
              GEN_SQL_STEP_2(ObShowSqlSet::SHOW_COLUMNS,
                  REAL_NAME(OB_SYS_DATABASE_NAME, OB_ORA_SYS_SCHEMA_NAME),
                  REAL_NAME(OB_TENANT_VIRTUAL_TABLE_COLUMN_TNAME, OB_TENANT_VIRTUAL_TABLE_COLUMN_ORA_TNAME),
                  show_table_id);
            }
          }
        }
        break;
      }
      case T_SHOW_CREATE_DATABASE: {
        if (is_oracle_mode) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support show create database in oracle mode", K(ret));
        } else if (OB_UNLIKELY(parse_tree.num_child_ != 2 || NULL == parse_tree.children_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parse tree is wrong", K(ret), K(parse_tree.num_child_), K(parse_tree.children_));
        } else if (OB_UNLIKELY(NULL == parse_tree.children_[1])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parser tree child is NULL", K(ret), K(parse_tree.children_[1]));
        } else {
          ObString show_db_name;
          uint64_t show_db_id = OB_INVALID_ID;
          show_resv_ctx.stmt_type_ = stmt::T_SHOW_CREATE_DATABASE;
          if (OB_FAIL(resolve_show_from_database(*parse_tree.children_[1], real_tenant_id, show_db_id, show_db_name))) {
            LOG_WARN("fail to resolve show database", K(ret), K(real_tenant_id));
          } else {
            if (NULL != parse_tree.children_[0]) {
              GEN_SQL_STEP_1(ObShowSqlSet::SHOW_CREATE_DATABASE_EXISTS);
              GEN_SQL_STEP_2(ObShowSqlSet::SHOW_CREATE_DATABASE_EXISTS,
                  OB_SYS_DATABASE_NAME,
                  OB_TENANT_VIRTUAL_SHOW_CREATE_DATABASE_TNAME,
                  show_db_id);
            } else {
              GEN_SQL_STEP_1(ObShowSqlSet::SHOW_CREATE_DATABASE);
              GEN_SQL_STEP_2(ObShowSqlSet::SHOW_CREATE_DATABASE,
                  OB_SYS_DATABASE_NAME,
                  OB_TENANT_VIRTUAL_SHOW_CREATE_DATABASE_TNAME,
                  show_db_id);
            }
          }
        }
        break;
      }
      case T_SHOW_CREATE_VIEW:
      case T_SHOW_CREATE_TABLE: {
        if (OB_UNLIKELY(parse_tree.num_child_ != 1 || NULL == parse_tree.children_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parse tree is wrong", K(ret), K(parse_tree.num_child_), K(parse_tree.children_));
        } else {
          ObString show_db_name;
          uint64_t show_db_id = OB_INVALID_ID;
          ObString show_table_name;
          uint64_t show_table_id = OB_INVALID_ID;
          bool is_view = false;
          bool allow_show = false;
          if (OB_FAIL(resolve_show_from_table(parse_tree.children_[0],
                  NULL,
                  database_name.empty(),
                  parse_tree.type_,
                  real_tenant_id,
                  show_db_name,
                  show_db_id,
                  show_table_name,
                  show_table_id,
                  is_view,
                  synonym_checker))) {
            LOG_WARN("fail to resolve show from table", K(ret));
          }
          if (OB_FAIL(ret)) {
          } else if (T_SHOW_CREATE_VIEW == parse_tree.type_) {
            if (ObSchemaChecker::is_ora_priv_check()) {
            } else {
              ObNeedPriv need_priv;
              need_priv.db_ = show_db_name;
              need_priv.table_ = show_table_name;
              need_priv.priv_set_ = OB_PRIV_SHOW_VIEW | OB_PRIV_SELECT;
              need_priv.priv_level_ = OB_PRIV_TABLE_LEVEL;
              if (OB_FAIL(stmt_need_privs.need_privs_.init(1))) {
                LOG_WARN("Failed to init stmt need priv", K(ret));
              } else if (OB_FAIL(stmt_need_privs.need_privs_.push_back(need_priv))) {
                LOG_WARN("Failed to add need priv", K(ret));
              } else if (OB_FAIL(schema_checker_->check_priv(session_priv, stmt_need_privs))) {
                LOG_WARN("Failed to check acc", K(ret));
              } else {
              }  // do nothing
            }
          } else if (OB_FAIL(
                         schema_checker_->check_table_show(session_priv, show_db_name, show_table_name, allow_show))) {
            LOG_WARN("Check table show error", K(ret));
          } else if (!allow_show) {
            ret = OB_ERR_NO_TABLE_PRIVILEGE;
            LOG_USER_ERROR(OB_ERR_NO_TABLE_PRIVILEGE,
                static_cast<int>(strlen("SHOW")),
                "SHOW",
                session_priv.user_name_.length(),
                session_priv.user_name_.ptr(),
                session_priv.host_name_.length(),
                session_priv.host_name_.ptr(),
                show_table_name.length(),
                show_table_name.ptr());
          } else {
          }  // do nothing
          if (OB_SUCC(ret)) {
            show_resv_ctx.stmt_type_ =
                (parse_tree.type_ == T_SHOW_CREATE_TABLE) ? stmt::T_SHOW_CREATE_TABLE : stmt::T_SHOW_CREATE_VIEW;
            if (parse_tree.type_ == T_SHOW_CREATE_VIEW || is_view) {
              GEN_SQL_STEP_1(ObShowSqlSet::SHOW_CREATE_VIEW);
              GEN_SQL_STEP_2(ObShowSqlSet::SHOW_CREATE_VIEW,
                  REAL_NAME(OB_SYS_DATABASE_NAME, OB_ORA_SYS_SCHEMA_NAME),
                  REAL_NAME(OB_TENANT_VIRTUAL_SHOW_CREATE_TABLE_TNAME, OB_TENANT_VIRTUAL_SHOW_CREATE_TABLE_ORA_TNAME),
                  show_table_id);
            } else {
              GEN_SQL_STEP_1(ObShowSqlSet::SHOW_CREATE_TABLE);
              GEN_SQL_STEP_2(ObShowSqlSet::SHOW_CREATE_TABLE,
                  REAL_NAME(OB_SYS_DATABASE_NAME, OB_ORA_SYS_SCHEMA_NAME),
                  REAL_NAME(OB_TENANT_VIRTUAL_SHOW_CREATE_TABLE_TNAME, OB_TENANT_VIRTUAL_SHOW_CREATE_TABLE_ORA_TNAME),
                  show_table_id);
            }
          }
        }
        break;
      }
      case T_SHOW_CREATE_PROCEDURE:
      case T_SHOW_CREATE_FUNCTION: {
        ret = OB_NOT_SUPPORTED;
        break;
      }
      case T_SHOW_INDEXES: {
        if (is_oracle_mode) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support show indexes in oracle mode", K(ret));
        } else if (OB_UNLIKELY(parse_tree.num_child_ != 3 || NULL == parse_tree.children_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parse tree is wrong", K(ret), K(parse_tree.num_child_), K(parse_tree.children_));
        } else {
          ObString show_db_name;
          uint64_t show_db_id = OB_INVALID_ID;
          ObString show_table_name;
          uint64_t show_table_id = OB_INVALID_ID;
          ObObj show_table_id_obj;
          bool is_view;
          show_resv_ctx.condition_node_ = parse_tree.children_[2];
          show_resv_ctx.stmt_type_ = stmt::T_SHOW_INDEXES;
          if (OB_FAIL(resolve_show_from_table(parse_tree.children_[0],
                  parse_tree.children_[1],
                  database_name.empty(),
                  T_SHOW_INDEXES,
                  real_tenant_id,
                  show_db_name,
                  show_db_id,
                  show_table_name,
                  show_table_id,
                  is_view,
                  synonym_checker))) {
            LOG_WARN("fail to resolve show from table", K(ret));
          } else {
            GEN_SQL_STEP_1(ObShowSqlSet::SHOW_INDEXES);
            GEN_SQL_STEP_2(
                ObShowSqlSet::SHOW_INDEXES, OB_SYS_DATABASE_NAME, OB_TENANT_VIRTUAL_TABLE_INDEX_TNAME, show_table_id);
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(stmt_need_privs.need_privs_.init(3))) {
              LOG_WARN("fail to init need privs array", K(ret));
            } else {
              ObNeedPriv need_priv;
              // Priv check: global select || db select || table acc
              need_priv.priv_level_ = OB_PRIV_USER_LEVEL;
              need_priv.priv_set_ = OB_PRIV_SELECT;
              stmt_need_privs.need_privs_.push_back(need_priv);

              need_priv.priv_level_ = OB_PRIV_DB_LEVEL;
              stmt_need_privs.need_privs_.push_back(need_priv);

              need_priv.priv_level_ = OB_PRIV_TABLE_LEVEL;
              need_priv.priv_set_ = OB_PRIV_TABLE_ACC;
              need_priv.db_ = show_db_name;
              need_priv.table_ = show_table_name;
              stmt_need_privs.need_privs_.push_back(need_priv);

              if (OB_FAIL(schema_checker_->check_priv_or(session_priv, stmt_need_privs))) {
                ret = OB_ERR_NO_TABLE_PRIVILEGE;
                LOG_USER_ERROR(OB_ERR_NO_TABLE_PRIVILEGE,
                    (int)strlen("SELECT"),
                    "SELECT",
                    session_priv.user_name_.length(),
                    session_priv.user_name_.ptr(),
                    session_priv.host_name_.length(),
                    session_priv.host_name_.ptr(),
                    show_table_name.length(),
                    show_table_name.ptr());
              }
            }
          }
        }
        break;
      }
      case T_SHOW_CHARSET: {
        if (OB_UNLIKELY(parse_tree.num_child_ != 1 || NULL == parse_tree.children_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parse tree is wrong", K(ret), K(parse_tree.num_child_), K(parse_tree.children_));
        } else {
          ObSqlStrGenerator sql_gen;
          show_resv_ctx.condition_node_ = parse_tree.children_[0];
          show_resv_ctx.stmt_type_ = stmt::T_SHOW_CHARSET;
          GEN_SQL_STEP_1(ObShowSqlSet::SHOW_CHARSET);
          GEN_SQL_STEP_2(ObShowSqlSet::SHOW_CHARSET,
              REAL_NAME(OB_SYS_DATABASE_NAME, OB_ORA_SYS_SCHEMA_NAME),
              REAL_NAME(OB_TENANT_VIRTUAL_CHARSET_TNAME, OB_TENANT_VIRTUAL_CHARSET_AGENT_TNAME));
          break;
        }
      }
      case T_SHOW_COLLATION:
        if (OB_UNLIKELY(parse_tree.num_child_ != 1 || NULL == parse_tree.children_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parse tree is wrong", K(ret), K(parse_tree.num_child_), K(parse_tree.children_));
        } else {
          show_resv_ctx.condition_node_ = parse_tree.children_[0];
          show_resv_ctx.stmt_type_ = stmt::T_SHOW_COLLATION;
          GEN_SQL_STEP_1(ObShowSqlSet::SHOW_COLLATION);
          if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2000) {
            GEN_SQL_STEP_2(
                ObShowSqlSet::SHOW_COLLATION, OB_SYS_DATABASE_NAME, OB_SYS_DATABASE_NAME, OB_ALL_COLLATION_TNAME);
          } else {
            GEN_SQL_STEP_2(ObShowSqlSet::SHOW_COLLATION,
                REAL_NAME(OB_SYS_DATABASE_NAME, OB_ORA_SYS_SCHEMA_NAME),
                REAL_NAME(OB_TENANT_VIRTUAL_COLLATION_TNAME, OB_TENANT_VIRTUAL_COLLATION_AGENT_TNAME));
          }
        }
        break;
      case T_SHOW_TRACE: {
        if (OB_UNLIKELY(parse_tree.num_child_ != 1 || NULL == parse_tree.children_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parse tree is wrong", K(ret), K(parse_tree.num_child_), K(parse_tree.children_));
        } else {
          show_resv_ctx.condition_node_ = parse_tree.children_[0];
          show_resv_ctx.stmt_type_ = stmt::T_SHOW_TRACE;
          GEN_SQL_STEP_1(ObShowSqlSet::SHOW_TRACE);
          GEN_SQL_STEP_2(ObShowSqlSet::SHOW_TRACE,
              REAL_NAME(OB_SYS_DATABASE_NAME, OB_ORA_SYS_SCHEMA_NAME),
              REAL_NAME(OB_ALL_VIRTUAL_TRACE_LOG_TNAME, OB_ALL_VIRTUAL_TRACE_LOG_ORA_TNAME));
        }
        break;
      }
      case T_SHOW_GRANTS: {
        if (OB_UNLIKELY(parse_tree.num_child_ != 1 || NULL == parse_tree.children_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parse tree is wrong", K(ret), K(parse_tree.num_child_), K(parse_tree.children_));
        } else {
          uint64_t show_user_id = OB_INVALID_ID;
          ObString show_user_name;
          ObString show_host_name;
          show_resv_ctx.stmt_type_ = stmt::T_SHOW_GRANTS;
          if (NULL == parse_tree.children_[0]) {
            show_user_id = user_id;
            show_user_name = user_name;
            show_host_name = host_name;
          } else if (2 != parse_tree.children_[0]->num_child_) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("sql_parser parse user error", K(ret));
            // 0: user name; 1: host name
          } else if (OB_ISNULL(parse_tree.children_[0])) {
            ret = OB_ERR_PARSE_SQL;
            LOG_WARN("The child of user node should not be NULL", K(ret));
          } else {
            const ParseNode* user_hostname_node = parse_tree.children_[0];
            ObString user_name(
                user_hostname_node->children_[0]->str_len_, user_hostname_node->children_[0]->str_value_);
            ObString host_name;
            if (NULL == user_hostname_node->children_[1]) {
              host_name.assign_ptr(OB_DEFAULT_HOST_NAME, static_cast<int32_t>(STRLEN(OB_DEFAULT_HOST_NAME)));
            } else {
              host_name.assign_ptr(user_hostname_node->children_[1]->str_value_,
                  static_cast<int32_t>(user_hostname_node->children_[1]->str_len_));
            }
            show_user_name = user_name;
            show_host_name = host_name;
            if (OB_FAIL(schema_checker_->get_user_id(real_tenant_id, user_name, host_name, show_user_id))) {
              LOG_WARN("Get user id error", "tenant_id", real_tenant_id, K(user_name), K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            GEN_SQL_STEP_1(ObShowSqlSet::SHOW_GRANTS,
                show_user_name.length(),
                show_user_name.ptr(),
                show_host_name.length(),
                show_host_name.ptr());
            GEN_SQL_STEP_2(ObShowSqlSet::SHOW_GRANTS,
                REAL_NAME(OB_SYS_DATABASE_NAME, OB_ORA_SYS_SCHEMA_NAME),
                REAL_NAME(OB_TENANT_VIRTUAL_PRIVILEGE_GRANT_TNAME, OB_TENANT_VIRTUAL_PRIVILEGE_GRANT_ORA_TNAME),
                show_user_id);
          }
        }
        break;
      }
      case T_SHOW_PROCESSLIST: {
        if (OB_UNLIKELY(parse_tree.num_child_ != 1 || NULL == parse_tree.children_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parse tree is wrong", K(ret), K(parse_tree.num_child_), K(parse_tree.children_));
        } else if (OB_UNLIKELY(NULL == parse_tree.children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parser tree child is NULL", K(ret), K(parse_tree.children_[0]));
        } else {
          ObString database_name;
          ObString table_name;
          uint64_t priv_tenant_id = session_info_->get_priv_tenant_id();
          show_resv_ctx.database_name_ = database_name;
          show_resv_ctx.stmt_type_ = stmt::T_SHOW_PROCESSLIST;
          if (0 == parse_tree.children_[0]->value_) {
            if (OB_SYS_TENANT_ID == priv_tenant_id) {
              GEN_SQL_STEP_1(ObShowSqlSet::SHOW_SYS_PROCESSLIST);
              GEN_SQL_STEP_2(ObShowSqlSet::SHOW_SYS_PROCESSLIST,
                  REAL_NAME(OB_SYS_DATABASE_NAME, OB_ORA_SYS_SCHEMA_NAME),
                  REAL_NAME(OB_ALL_VIRTUAL_PROCESSLIST_TNAME, OB_ALL_VIRTUAL_PROCESSLIST_ORA_TNAME));
            } else {
              GEN_SQL_STEP_1(ObShowSqlSet::SHOW_PROCESSLIST);
              GEN_SQL_STEP_2(ObShowSqlSet::SHOW_PROCESSLIST,
                  REAL_NAME(OB_SYS_DATABASE_NAME, OB_ORA_SYS_SCHEMA_NAME),
                  REAL_NAME(OB_ALL_VIRTUAL_PROCESSLIST_TNAME, OB_ALL_VIRTUAL_PROCESSLIST_ORA_TNAME),
                  real_tenant_id);
            }
          } else if (1 == parse_tree.children_[0]->value_) {
            if (OB_SYS_TENANT_ID == priv_tenant_id) {
              GEN_SQL_STEP_1(ObShowSqlSet::SHOW_SYS_FULL_PROCESSLIST);
              GEN_SQL_STEP_2(ObShowSqlSet::SHOW_SYS_FULL_PROCESSLIST,
                  REAL_NAME(OB_SYS_DATABASE_NAME, OB_ORA_SYS_SCHEMA_NAME),
                  REAL_NAME(OB_ALL_VIRTUAL_PROCESSLIST_TNAME, OB_ALL_VIRTUAL_PROCESSLIST_ORA_TNAME));
            } else {
              GEN_SQL_STEP_1(ObShowSqlSet::SHOW_FULL_PROCESSLIST);
              GEN_SQL_STEP_2(ObShowSqlSet::SHOW_FULL_PROCESSLIST,
                  REAL_NAME(OB_SYS_DATABASE_NAME, OB_ORA_SYS_SCHEMA_NAME),
                  REAL_NAME(OB_ALL_VIRTUAL_PROCESSLIST_TNAME, OB_ALL_VIRTUAL_PROCESSLIST_ORA_TNAME),
                  real_tenant_id);
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected node value", K(parse_tree.value_));
            break;
          }
        }
        break;
      }
      case T_SHOW_TABLE_STATUS: {
        if (OB_UNLIKELY(parse_tree.num_child_ != 2 || NULL == parse_tree.children_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parse tree is wrong", K(ret), K(parse_tree.num_child_), K(parse_tree.children_));
        } else {
          show_resv_ctx.condition_node_ = parse_tree.children_[1];
          uint64_t show_db_id;
          if (OB_FAIL(get_database_info(
                  parse_tree.children_[0], database_name, real_tenant_id, show_resv_ctx, show_db_id))) {
            LOG_WARN("fail to get database info", K(ret));
          } else if (OB_UNLIKELY(OB_INVALID_ID == show_db_id)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("database id is invalid", K(ret), K(show_db_id));
          } else {
            show_resv_ctx.stmt_type_ = stmt::T_SHOW_TABLE_STATUS;
            GEN_SQL_STEP_1(ObShowSqlSet::SHOW_TABLE_STATUS);
            GEN_SQL_STEP_2(ObShowSqlSet::SHOW_TABLE_STATUS,
                REAL_NAME(OB_SYS_DATABASE_NAME, OB_ORA_SYS_SCHEMA_NAME),
                REAL_NAME(OB_TENANT_VIRTUAL_ALL_TABLE_TNAME, OB_TENANT_VIRTUAL_ALL_TABLE_AGENT_TNAME),
                show_db_id);
          }
        }
        break;
      }
      case T_SHOW_PROCEDURE_STATUS:  // fallthrough
      case T_SHOW_FUNCTION_STATUS: {
        ret = OB_NOT_SUPPORTED;
        break;
      }
      case T_SHOW_TRIGGERS: {
        // Trigger is not supported in mysql mode and this is only used for mysqldump tool now.
        if (is_oracle_mode) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support show triggers in oracle mode", K(ret));
        } else if (OB_UNLIKELY(parse_tree.num_child_ != 2 || NULL == parse_tree.children_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parse tree is wrong",
              K(ret),
              K(parse_tree.num_child_),
              K(parse_tree.children_));
        } else {
          if (OB_FAIL(sql_gen.init(params_.allocator_))) {
            LOG_WARN("fail to init sql string generator", K(ret));
          } else if (OB_FAIL(sql_gen.gen_select_str(ObShowSqlSet::SHOW_TRIGGERS_SELECT))) {
            LOG_WARN("fail to generate select string", K(ret));
          } else if (OB_FAIL(sql_gen.gen_from_str(ObShowSqlSet::SHOW_TRIGGERS_SUBQUERY))) {
            LOG_WARN("fail to generate from string", K(ret));
          } else {
            sql_gen.assign_sql_str(select_sql);
          }
        }
        break;
      }
      case T_SHOW_WARNINGS:
      case T_SHOW_ERRORS: {
        int64_t offset = 0;
        int64_t row_count = 0;
        ParseNode* offset_node = NULL;
        ParseNode* row_count_node = NULL;
        if (is_oracle_mode) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support show warnings/errors in oracle mode", K(ret));
        } else if (OB_UNLIKELY(parse_tree.num_child_ != 1 || NULL == parse_tree.children_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parse tree is wrong", K(ret), K(parse_tree.num_child_), K(parse_tree.children_));
        } else {
          show_resv_ctx.stmt_type_ = parse_tree.type_ == T_SHOW_WARNINGS ? stmt::T_SHOW_WARNINGS : stmt::T_SHOW_ERRORS;
          if (NULL == parse_tree.children_[0]) {  // show  warnings|errors
            if (parse_tree.type_ == T_SHOW_WARNINGS) {
              GEN_SQL_STEP_1(ObShowSqlSet::SHOW_WARNINGS);
              GEN_SQL_STEP_2(ObShowSqlSet::SHOW_WARNINGS, OB_SYS_DATABASE_NAME, OB_TENANT_VIRTUAL_WARNING_TNAME);
            } else if (parse_tree.type_ == T_SHOW_ERRORS) {
              GEN_SQL_STEP_1(ObShowSqlSet::SHOW_ERRORS);
              GEN_SQL_STEP_2(ObShowSqlSet::SHOW_ERRORS, OB_SYS_DATABASE_NAME, OB_TENANT_VIRTUAL_WARNING_TNAME);
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected node type", K(parse_tree.type_));
            }
          } else if (NULL != parse_tree.children_[0] &&  // show warnings|errors limit [offset,] row_count
                     T_SHOW_LIMIT == parse_tree.children_[0]->type_) {
            if (OB_UNLIKELY(parse_tree.children_[0]->num_child_ != 2 || NULL == parse_tree.children_[0]->children_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("parse tree is wrong",
                  K(ret),
                  K(parse_tree.children_[0]->num_child_),
                  K(parse_tree.children_[0]->children_));
            } else if (OB_UNLIKELY(NULL == parse_tree.children_[0]->children_[1])) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("parser tree child is NULL", K(ret), K(parse_tree.children_[0]->children_[0]));
            } else {
              offset_node = parse_tree.children_[0]->children_[0];
              row_count_node = parse_tree.children_[0]->children_[1];
              row_count = row_count_node->value_;
              offset = NULL != offset_node ? offset_node->value_ : 0;
              if (parse_tree.type_ == T_SHOW_WARNINGS) {
                if (OB_FAIL(sql_gen.init(params_.allocator_))) {
                  LOG_WARN("fail to init sql string generator", K(ret));
                } else if (OB_FAIL(sql_gen.gen_select_str(ObShowSqlSet::SHOW_WARNINGS_SELECT))) {
                  LOG_WARN("fail to generate select string", K(ret));
                } else if (OB_FAIL(sql_gen.gen_from_str(ObShowSqlSet::SHOW_WARNINGS_SUBQUERY,
                               OB_SYS_DATABASE_NAME,
                               OB_TENANT_VIRTUAL_WARNING_TNAME))) {
                  LOG_WARN("fail to generate from string", K(ret));
                } else if (OB_FAIL(sql_gen.gen_limit_str(offset, row_count))) {
                  LOG_WARN("fail to generate limit string", K(ret));
                } else {
                  sql_gen.assign_sql_str(select_sql);
                }
              } else if (parse_tree.type_ == T_SHOW_ERRORS) {
                if (OB_FAIL(sql_gen.init(params_.allocator_))) {
                  LOG_WARN("fail to init sql string generator", K(ret));
                } else if (OB_FAIL(sql_gen.gen_select_str(ObShowSqlSet::SHOW_ERRORS_SELECT))) {
                  LOG_WARN("fail to generate select string", K(ret));
                } else if (OB_FAIL(sql_gen.gen_from_str(ObShowSqlSet::SHOW_ERRORS_SUBQUERY,
                               OB_SYS_DATABASE_NAME,
                               OB_TENANT_VIRTUAL_WARNING_TNAME))) {
                  LOG_WARN("fail to generate from string", K(ret));
                } else if (OB_FAIL(sql_gen.gen_limit_str(offset, row_count))) {
                  LOG_WARN("fail to generate limit string", K(ret));
                } else {
                  sql_gen.assign_sql_str(select_sql);
                }
              } else {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected node type", K(parse_tree.type_));
              }
            }
          } else if (NULL != parse_tree.children_[0] &&  // show count(*) warnings|errors
                     T_FUN_COUNT == parse_tree.children_[0]->type_) {
            if (parse_tree.type_ == T_SHOW_WARNINGS) {
              GEN_SQL_STEP_1(ObShowSqlSet::SHOW_COUNT_WARNINGS);
              GEN_SQL_STEP_2(ObShowSqlSet::SHOW_COUNT_WARNINGS, OB_SYS_DATABASE_NAME, OB_TENANT_VIRTUAL_WARNING_TNAME);
            } else if (parse_tree.type_ == T_SHOW_ERRORS) {
              GEN_SQL_STEP_1(ObShowSqlSet::SHOW_COUNT_ERRORS);
              GEN_SQL_STEP_2(ObShowSqlSet::SHOW_COUNT_ERRORS, OB_SYS_DATABASE_NAME, OB_TENANT_VIRTUAL_WARNING_TNAME);
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected node type", K(parse_tree.type_));
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected child type", K(parse_tree.children_[0]->type_));
          }
        }
        break;
      }
      case T_SHOW_PARAMETERS: {
        uint64_t show_tenant_id = real_tenant_id;
        if (OB_UNLIKELY(parse_tree.num_child_ != 2 || nullptr == parse_tree.children_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parse tree is wrong", K(ret), K(parse_tree.num_child_), K(parse_tree.children_));
        } else {
          show_resv_ctx.stmt_type_ = stmt::T_SHOW_PARAMETERS;
          show_resv_ctx.condition_node_ = parse_tree.children_[0];
          // tenant=
          if (nullptr != parse_tree.children_[1]) {
            if (OB_SYS_TENANT_ID != real_tenant_id) {
              ret = OB_ERR_NO_PRIVILEGE;
              LOG_WARN("non sys tenant", K(real_tenant_id), K(ret));
            } else if (OB_UNLIKELY(T_TENANT_NAME != parse_tree.children_[1]->type_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("type is not T_TENANT_NAME", "type", get_type_name(parse_tree.type_));
            } else {
              const ParseNode* tenant_node = parse_tree.children_[1];
              if (OB_ISNULL(tenant_node->children_)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("children should not be null");
              } else {
                ObString show_tenant_name(tenant_node->children_[0]->str_len_, tenant_node->children_[0]->str_value_);
                if (ObString::make_string("seed") == show_tenant_name) {
                  params_.show_seed_ = true;
                } else if (OB_FAIL(schema_checker_->get_tenant_id(show_tenant_name, show_tenant_id)) ||
                           OB_INVALID_ID == show_tenant_id) {
                  ret = OB_ERR_INVALID_TENANT_NAME;
                  LOG_WARN("fail to get tenant id", K(show_tenant_name), K(ret));
                } else {
                  params_.show_tenant_id_ = show_tenant_id;
                }
              }
            }
            if (OB_FAIL(ret)) {
              break;
            }
          }  // if
          if (params_.show_seed_) {
            char local_ip[OB_MAX_SERVER_ADDR_SIZE] = "";
            if (OB_UNLIKELY(true != GCONF.self_addr_.ip_to_string(local_ip, sizeof(local_ip)))) {
              ret = OB_CONVERT_ERROR;
            } else {
              GEN_SQL_STEP_1(ObShowSqlSet::SHOW_PARAMETERS_SEED);
              GEN_SQL_STEP_2(ObShowSqlSet::SHOW_PARAMETERS_SEED,
                  REAL_NAME(OB_SYS_DATABASE_NAME, OB_ORA_SYS_SCHEMA_NAME),
                  REAL_NAME(OB_ALL_VIRTUAL_TENANT_PARAMETER_STAT_TNAME, OB_ALL_VIRTUAL_TENANT_PARAMETER_STAT_ORA_TNAME),
                  local_ip,
                  GCONF.self_addr_.get_port());
            }
          } else if (OB_SYS_TENANT_ID == show_tenant_id) {
            if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_2200) {
              GEN_SQL_STEP_1(ObShowSqlSet::SHOW_PARAMETERS);
              GEN_SQL_STEP_2(ObShowSqlSet::SHOW_PARAMETERS,
                  REAL_NAME(OB_SYS_DATABASE_NAME, OB_ORA_SYS_SCHEMA_NAME),
                  REAL_NAME(OB_ALL_VIRTUAL_SYS_PARAMETER_STAT_TNAME, OB_ALL_VIRTUAL_SYS_PARAMETER_STAT_AGENT_TNAME));
            } else {
              GEN_SQL_STEP_1(ObShowSqlSet::SHOW_PARAMETERS_COMPAT);
              GEN_SQL_STEP_2(ObShowSqlSet::SHOW_PARAMETERS_COMPAT,
                  REAL_NAME(OB_SYS_DATABASE_NAME, OB_ORA_SYS_SCHEMA_NAME),
                  REAL_NAME(OB_ALL_VIRTUAL_SYS_PARAMETER_STAT_TNAME, OB_ALL_VIRTUAL_SYS_PARAMETER_STAT_AGENT_TNAME));
            }
          } else {
            GEN_SQL_STEP_1(ObShowSqlSet::SHOW_PARAMETERS);
            GEN_SQL_STEP_2(ObShowSqlSet::SHOW_PARAMETERS,
                REAL_NAME(OB_SYS_DATABASE_NAME, OB_ORA_SYS_SCHEMA_NAME),
                REAL_NAME(OB_ALL_VIRTUAL_TENANT_PARAMETER_STAT_TNAME, OB_ALL_VIRTUAL_TENANT_PARAMETER_STAT_ORA_TNAME));
          }
        }
        break;
      }
      case T_SHOW_TABLEGROUPS: {
        const char* table_name = NULL;
        if (OB_UNLIKELY(parse_tree.num_child_ != 1 || NULL == parse_tree.children_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parse tree is wrong", K(ret), K(parse_tree.num_child_), K(parse_tree.children_));
        } else if (OB_FAIL(ObSchemaUtils::get_all_table_name(real_tenant_id, table_name))) {
          LOG_WARN("fail to get all table name", K(ret), K(real_tenant_id));
        } else {
          ObSqlStrGenerator sql_gen;
          show_resv_ctx.condition_node_ = parse_tree.children_[0];
          show_resv_ctx.stmt_type_ = stmt::T_SHOW_TABLEGROUPS;
          GEN_SQL_STEP_1(ObShowSqlSet::SHOW_TABLEGROUPS);
          GEN_SQL_STEP_2(ObShowSqlSet::SHOW_TABLEGROUPS,
              REAL_NAME(OB_SYS_DATABASE_NAME, OB_ORA_SYS_SCHEMA_NAME),
              REAL_NAME(OB_ALL_TABLEGROUP_TNAME, OB_ALL_VIRTUAL_TABLEGROUP_AGENT_TNAME),
              REAL_NAME(OB_SYS_DATABASE_NAME, OB_ORA_SYS_SCHEMA_NAME),
              REAL_NAME(table_name, OB_ALL_VIRTUAL_TABLE_AGENT_TNAME),
              is_oracle_mode ? real_tenant_id : sql_tenant_id,
              REAL_NAME(OB_SYS_DATABASE_NAME, OB_ORA_SYS_SCHEMA_NAME),
              REAL_NAME(OB_ALL_DATABASE_TNAME, OB_ALL_VIRTUAL_DATABASE_AGENT_TNAME),
              is_oracle_mode ? real_tenant_id : sql_tenant_id,
              is_oracle_mode ? real_tenant_id : sql_tenant_id);
        }
        break;
      }
      case T_SHOW_STATUS: {
        if (is_oracle_mode) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support show status in oracle mode", K(ret));
        } else if (OB_UNLIKELY(parse_tree.num_child_ != 2 || NULL == parse_tree.children_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parse tree is wrong", K(ret), K(parse_tree.num_child_), K(parse_tree.children_));
        } else if (OB_UNLIKELY(NULL == parse_tree.children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parser tree child is NULL", K(ret), K(parse_tree.children_[0]));
        } else {
          show_resv_ctx.condition_node_ = parse_tree.children_[1];
          show_resv_ctx.stmt_type_ = stmt::T_SHOW_STATUS;
          show_resv_ctx.global_scope_ = 1 == parse_tree.children_[0]->value_ ? true : false;
          if (true == show_resv_ctx.global_scope_) {
            GEN_SQL_STEP_1(ObShowSqlSet::SHOW_GLOBAL_STATUS);
            GEN_SQL_STEP_2(ObShowSqlSet::SHOW_GLOBAL_STATUS, OB_INFORMATION_SCHEMA_NAME, OB_SESSION_STATUS_TNAME);
          } else {
            GEN_SQL_STEP_1(ObShowSqlSet::SHOW_SESSION_STATUS);
            GEN_SQL_STEP_2(ObShowSqlSet::SHOW_SESSION_STATUS, OB_INFORMATION_SCHEMA_NAME, OB_SESSION_STATUS_TNAME);
          }
        }
        break;
      }
      case T_SHOW_TENANT: {
        if (is_oracle_mode) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support show tenant in oracle mode", K(ret));
        } else if (OB_UNLIKELY(parse_tree.num_child_ != 1 || NULL == parse_tree.children_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parse tree is wrong", K(ret), K(parse_tree.num_child_), K(parse_tree.children_));
        } else {
          show_resv_ctx.stmt_type_ = stmt::T_SHOW_TENANT;
          if (parse_tree.children_[0] != NULL) {
            GEN_SQL_STEP_1(ObShowSqlSet::SHOW_TENANT_STATUS);
            GEN_SQL_STEP_2(
                ObShowSqlSet::SHOW_TENANT_STATUS, OB_SYS_DATABASE_NAME, OB_TENANT_VIRTUAL_TENANT_STATUS_TNAME);
          } else {
            GEN_SQL_STEP_1(ObShowSqlSet::SHOW_TENANT);
            GEN_SQL_STEP_2(ObShowSqlSet::SHOW_TENANT,
                OB_SYS_DATABASE_NAME,
                OB_TENANT_VIRTUAL_CURRENT_TENANT_TNAME,
                real_tenant_id);
          }
        }
        break;
      }
      case T_SHOW_CREATE_TENANT: {
        if (is_oracle_mode) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support show create tenant in oracle mode", K(ret));
        } else if (OB_UNLIKELY(parse_tree.num_child_ != 1 || NULL == parse_tree.children_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parse tree is wrong", K(ret), K(parse_tree.num_child_), K(parse_tree.children_));
        } else if (OB_UNLIKELY(NULL == parse_tree.children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parser tree child is NULL", K(ret), K(parse_tree.children_[0]));
        } else {
          show_resv_ctx.stmt_type_ = stmt::T_SHOW_CREATE_TENANT;
          ObString show_tenant_name;
          show_tenant_name.assign_ptr(parse_tree.children_[0]->str_value_,
              static_cast<ObString::obstr_size_t>(parse_tree.children_[0]->str_len_));

          uint64_t show_tenant_id = OB_INVALID_ID;
          if (OB_FAIL(schema_checker_->get_tenant_id(show_tenant_name, show_tenant_id))) {
            LOG_WARN("fail to get_tenant_id", K(ret));
          } else if ((real_tenant_id != OB_SYS_TENANT_ID && real_tenant_id != show_tenant_id) ||
                     OB_INVALID_ID == show_tenant_id) {
            ret = OB_TENANT_NOT_EXIST;
            LOG_USER_ERROR(
                OB_TENANT_NOT_EXIST, (int)parse_tree.children_[0]->str_len_, parse_tree.children_[0]->str_value_);
          } else {
            GEN_SQL_STEP_1(ObShowSqlSet::SHOW_CREATE_TENANT);
            GEN_SQL_STEP_2(ObShowSqlSet::SHOW_CREATE_TENANT,
                OB_SYS_DATABASE_NAME,
                OB_TENANT_VIRTUAL_CURRENT_TENANT_TNAME,
                show_tenant_id);
          }
        }
        break;
      }
      case T_SHOW_CREATE_TABLEGROUP: {
        if (OB_UNLIKELY(parse_tree.num_child_ != 1 || NULL == parse_tree.children_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parse tree is wrong", K(ret), K(parse_tree.num_child_), K(parse_tree.children_));
        } else if (OB_UNLIKELY(NULL == parse_tree.children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parser tree child is NULL", K(ret), K(parse_tree.children_[1]));
        } else {
          ObString show_tablegroup_name;
          uint64_t show_tablegroup_id = OB_INVALID_ID;
          show_resv_ctx.stmt_type_ = stmt::T_SHOW_CREATE_TABLEGROUP;
          show_tablegroup_name.assign_ptr(parse_tree.children_[0]->str_value_,
              static_cast<ObString::obstr_size_t>(parse_tree.children_[0]->str_len_));
          const ObTablegroupSchema* tablegroup_schema = NULL;
          if (OB_FAIL(
                  schema_checker_->get_tablegroup_schema(real_tenant_id, show_tablegroup_name, tablegroup_schema))) {
            if (OB_ISNULL(tablegroup_schema) || OB_INVALID_ID == tablegroup_schema->get_tablegroup_id()) {
              ret = OB_TABLEGROUP_NOT_EXIST;
              LOG_WARN("tablegroup not exist", K(ret), K(show_tablegroup_name));
            } else {
              LOG_WARN("fail to get tablegroup_schema", K(ret));
            }
          } else {
            show_tablegroup_id = tablegroup_schema->get_tablegroup_id();
            GEN_SQL_STEP_1(ObShowSqlSet::SHOW_CREATE_TABLEGROUP);
            GEN_SQL_STEP_2(ObShowSqlSet::SHOW_CREATE_TABLEGROUP,
                REAL_NAME(OB_SYS_DATABASE_NAME, OB_ORA_SYS_SCHEMA_NAME),
                REAL_NAME(
                    OB_TENANT_VIRTUAL_SHOW_CREATE_TABLEGROUP_TNAME, OB_TENANT_VIRTUAL_SHOW_CREATE_TABLEGROUP_ORA_TNAME),
                show_tablegroup_id);
          }
        }
        break;
      }
      case T_SHOW_ENGINES: {
        if (is_oracle_mode) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support show engines in oracle mode", K(ret));
        } else if (OB_UNLIKELY(parse_tree.num_child_ != 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parse tree is wrong", K(ret), K(parse_tree.num_child_));
        } else {
          show_resv_ctx.stmt_type_ = stmt::T_SHOW_ENGINES;
          GEN_SQL_STEP_1(ObShowSqlSet::SHOW_ENGINES);
          GEN_SQL_STEP_2(ObShowSqlSet::SHOW_ENGINES, OB_SYS_DATABASE_NAME, OB_ALL_VIRTUAL_ENGINE_TNAME);
        }
        break;
      }
      case T_SHOW_PRIVILEGES: {
        if (OB_UNLIKELY(parse_tree.num_child_ != 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parse tree is wrong", K(ret), K(parse_tree.num_child_));
        } else {
          show_resv_ctx.stmt_type_ = stmt::T_SHOW_PRIVILEGES;
          GEN_SQL_STEP_1(ObShowSqlSet::SHOW_PRIVILEGES);
          GEN_SQL_STEP_2(ObShowSqlSet::SHOW_PRIVILEGES,
              REAL_NAME(OB_SYS_DATABASE_NAME, OB_ORA_SYS_SCHEMA_NAME),
              REAL_NAME(OB_ALL_PRIVILEGE_TNAME, OB_ALL_VIRTUAL_PRIVILEGE_AGENT_TNAME));
        }
        break;
      }
      case T_SHOW_QUERY_RESPONSE_TIME: {
        if (is_oracle_mode) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support show engines in oracle mode", K(ret));
        } else if (OB_UNLIKELY(parse_tree.num_child_ != 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parse tree is wrong", K(ret), K(parse_tree.num_child_));
        } else {
          show_resv_ctx.stmt_type_ = stmt::T_SHOW_QUERY_RESPONSE_TIME;
          GEN_SQL_STEP_1(ObShowSqlSet::SHOW_QUERY_RESPONSE_TIME);
          GEN_SQL_STEP_2(ObShowSqlSet::SHOW_QUERY_RESPONSE_TIME, 
              OB_SYS_DATABASE_NAME, OB_ALL_VIRTUAL_QUERY_RESPONSE_TIME_TNAME, real_tenant_id);
        }
        break;
      }
      case T_SHOW_RECYCLEBIN: {
        if (OB_UNLIKELY(parse_tree.num_child_ != 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parse tree is wrong", K(ret), K(parse_tree.num_child_));
        } else {
          show_resv_ctx.stmt_type_ = stmt::T_SHOW_RECYCLEBIN;
          if (real_tenant_id == OB_SYS_TENANT_ID) {
            GEN_SQL_STEP_1(ObShowSqlSet::SHOW_SYS_RECYCLEBIN);
            GEN_SQL_STEP_2(
                ObShowSqlSet::SHOW_SYS_RECYCLEBIN, OB_SYS_DATABASE_NAME, OB_ALL_RECYCLEBIN_TNAME, OB_SYS_TENANT_ID);
          } else {
            GEN_SQL_STEP_1(ObShowSqlSet::SHOW_RECYCLEBIN);
            GEN_SQL_STEP_2(ObShowSqlSet::SHOW_RECYCLEBIN,
                REAL_NAME(OB_SYS_DATABASE_NAME, OB_ORA_SYS_SCHEMA_NAME),
                REAL_NAME(OB_ALL_RECYCLEBIN_TNAME, OB_ALL_VIRTUAL_RECYCLEBIN_AGENT_TNAME),
                is_oracle_mode ? real_tenant_id : sql_tenant_id);
          }
        }
        break;
      }
      case T_SHOW_RESTORE_PREVIEW: {
        if (OB_UNLIKELY(parse_tree.num_child_ != 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parse tree is wrong", K(ret), K(parse_tree.num_child_));
        } else {
          show_resv_ctx.stmt_type_ = stmt::T_SHOW_RESTORE_PREVIEW;
          GEN_SQL_STEP_1(ObShowSqlSet::SHOW_RESTORE_PREVIEW);
          GEN_SQL_STEP_2(
              ObShowSqlSet::SHOW_RESTORE_PREVIEW, OB_SYS_DATABASE_NAME, OB_VIRTUAL_SHOW_RESTORE_PREVIEW_TNAME);
        }
        break;
      }
      default:
        /* won't be here */
        ret = OB_NOT_IMPLEMENT;
        LOG_WARN("not implement type", K(parse_tree.type_));
        break;
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("show select sql", K(ret), K(select_sql));
      if (OB_FAIL(parse_and_resolve_select_sql(select_sql))) {
        LOG_WARN("fail to parse and resolve select sql", K(ret), K(select_sql));
      } else if (OB_FAIL(resolve_like_or_where_clause(show_resv_ctx))) {
        LOG_WARN("fail to resolve like or where clause", K(ret), K(show_resv_ctx), K(select_sql));
      }
    }
  }

  if (OB_SUCC(ret) && synonym_checker.has_synonym()) {
    if (OB_FAIL(add_synonym_obj_id(synonym_checker, false /* error_with_exist */))) {
      LOG_WARN("add_synonym_obj_id failed", K(ret), K(synonym_checker.get_synonym_ids()));
    }
  }

  if (OB_LIKELY(OB_SUCCESS == ret && NULL != stmt_)) {
    if (OB_UNLIKELY(stmt::T_SELECT != stmt_->get_stmt_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected stmt type", K(stmt_->get_stmt_type()));
    } else {
      ObSelectStmt* select_stmt = static_cast<ObSelectStmt*>(stmt_);
      select_stmt->set_is_from_show_stmt(true);
      select_stmt->set_literal_stmt_type(show_resv_ctx.stmt_type_);
      if (OB_FAIL(process_select_type(select_stmt, show_resv_ctx.stmt_type_, parse_tree))) {
        LOG_WARN("fail to process select type", K(ret), K(show_resv_ctx));
      } else if (OB_FAIL(select_stmt->formalize_stmt(session_info_))) {
        LOG_WARN("pull select stmt all expr relation ids failed", K(ret));
      }
    }
  }
  return ret;
}

int ObShowResolver::get_database_info(const ParseNode* database_node, const ObString& database_name,
    uint64_t real_tenant_id, ObShowResolverContext& show_resv_ctx, uint64_t& show_db_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_checker_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("some data member is not init", K(ret), K(schema_checker_));
  } else {
    if (NULL == database_node) {
      if (OB_UNLIKELY(database_name.empty())) {
        ret = OB_ERR_NO_DB_SELECTED;
        LOG_WARN("no database selected");
      } else {
        show_resv_ctx.show_database_name_ = database_name;
        if (OB_FAIL(schema_checker_->get_database_id(real_tenant_id, database_name, show_db_id))) {
          LOG_WARN("fail to get database_id", K(ret), K(database_name), K(real_tenant_id));
        }
      }
    } else {
      ObString show_db_name;
      if (OB_UNLIKELY(database_node->num_child_ != 1 || NULL == database_node->children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("parse tree is wrong", K(ret), K(database_node->num_child_), K(database_node->children_));
      } else if (OB_UNLIKELY(NULL == database_node->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("show from database node is NULL", K(ret));
      } else if (OB_FAIL(resolve_show_from_database(
                     *database_node->children_[0], real_tenant_id, show_db_id, show_resv_ctx.show_database_name_))) {
        LOG_WARN("fail to resolve show from database", K(ret), K(real_tenant_id));
      } else { /*do nothing*/
      }
    }
  }
  return ret;
}
int ObShowResolver::process_select_type(
    ObSelectStmt* select_stmt, stmt::StmtType stmt_type, const ParseNode& parse_tree)
{
  int ret = OB_SUCCESS;
  if ((stmt_type == stmt::T_SHOW_ERRORS || stmt_type == stmt::T_SHOW_WARNINGS) && parse_tree.children_[0] != NULL &&
      T_FUN_COUNT == parse_tree.children_[0]->type_) {
    // handling select count(*) error/warn
    select_stmt->set_select_type(AFFECT_FOUND_ROWS);
  } else if (stmt_type == stmt::T_SHOW_CREATE_TABLE
             //|| stmt_type == stmt::T_SHOW_CREATE_TRIGGER
             || stmt_type == stmt::T_SHOW_CREATE_DATABASE ||
             stmt_type == stmt::T_SHOW_CREATE_TABLEGROUP
             //|| stmt_type == stmt::T_SHOW_CREATE_EVENT
             //|| stmt_type == stmt::T_SHOW_CREATE_FUNCIONT
             || stmt_type == stmt::T_SHOW_CREATE_VIEW || stmt_type == stmt::T_SHOW_ERRORS ||
             stmt_type == stmt::T_SHOW_GRANTS
             //|| stmt_type == stmt::T_SHOW_PRIVILEGES
             || stmt_type == stmt::T_SHOW_PROCESSLIST
             //|| stmt_type == stmt::T_SHOW_PROFILES
             || stmt_type == stmt::T_SHOW_WARNINGS) {
    select_stmt->set_select_type(NOT_AFFECT_FOUND_ROWS);
  } else {
    select_stmt->set_select_type(AFFECT_FOUND_ROWS);
  }
  return ret;
}

int ObShowResolver::resolve_show_from_table(const ParseNode* from_table_node,
    const ParseNode* from_database_clause_node, bool is_database_unselected, ObItemType node_type,
    uint64_t real_tenant_id, ObString& show_database_name, uint64_t& show_database_id, ObString& show_table_name,
    uint64_t& show_table_id, bool& is_view, ObSynonymChecker& synonym_checker)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == schema_checker_)) {
    ret = OB_ERR_SCHEMA_UNSET;
    LOG_WARN("some data member is not init", K(ret), K(schema_checker_));
  } else if (OB_UNLIKELY(T_RELATION_FACTOR != from_table_node->type_ || from_table_node->num_child_ < 2 ||
                         NULL == from_table_node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parse tree is invalid",
        K(ret),
        K(from_table_node->type_),
        K(from_table_node->num_child_),
        K(from_table_node->children_));
  } else if (from_table_node->num_child_ > 2 && OB_NOT_NULL(from_table_node->children_[2])) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("desc link table is not supported", K(ret));
  } else if (OB_UNLIKELY(NULL == from_table_node->children_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parser tree child is NULL", K(ret), K(from_table_node->children_[1]));
  } else {
    const ObTableSchema* table_schema = NULL;
    if (NULL == from_database_clause_node) {
      if (OB_UNLIKELY(is_database_unselected && NULL == from_table_node->children_[0])) {
        ret = OB_ERR_NO_DB_SELECTED;
        LOG_WARN("no database selected");
      } else {
        ObString synonym_name;
        ObString synonym_db_name;
        if (OB_FAIL(resolve_table_relation_factor_normal(from_table_node,
                real_tenant_id,
                show_database_id,
                show_table_name,
                synonym_name,
                synonym_db_name,
                show_database_name,
                synonym_checker))) {
          if (OB_TABLE_NOT_EXIST == ret) {
            // check inner sys view
            bool use_sys_tenant = false;
            int tmp_ret = OB_SUCCESS;
            if (OB_SUCCESS !=
                (tmp_ret = inner_resolve_sys_view(
                     from_table_node, show_database_id, show_table_name, show_database_name, use_sys_tenant))) {
              LOG_WARN("fail to resolve sys view", K(tmp_ret));
            } else {
              ret = OB_SUCCESS;
            }
            if (OB_SUCC(ret)) {
              // resolve success
            } else if (OB_INFORMATION_SCHEMA_ID == extract_pure_id(show_database_id)) {
              ret = OB_ERR_UNKNOWN_TABLE;
              LOG_USER_ERROR(OB_ERR_UNKNOWN_TABLE,
                  show_table_name.length(),
                  show_table_name.ptr(),
                  show_database_name.length(),
                  show_database_name.ptr());
            } else {
              LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(show_database_name), to_cstring(show_table_name));
            }
          } else {
            LOG_WARN("fail to resolve table name", K(ret));
          }
        }
      }
    } else if (NULL == from_database_clause_node->children_[0]) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("from_database_clause_node->children_[0] is NULL", K(ret));
    } else {
      if (OB_UNLIKELY(T_FROM_LIST != from_database_clause_node->type_ || from_database_clause_node->num_child_ != 1 ||
                      NULL == from_table_node->children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("parse tree is invalid",
            K(ret),
            K(from_database_clause_node->type_),
            K(from_database_clause_node->num_child_),
            K(from_database_clause_node->children_));
      } else if (OB_UNLIKELY(NULL == from_database_clause_node->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("parser tree child is NULL", K(ret), K(from_database_clause_node->children_[0]));
      } else {
        ParseNode* relation_node = from_table_node->children_[1];
        show_table_name.assign_ptr(
            const_cast<char*>(relation_node->str_value_), static_cast<int32_t>(relation_node->str_len_));
        if (show_table_name.empty()) {
          ret = OB_WRONG_TABLE_NAME;
          LOG_WARN("table name is empty", K(ret));
        } else if (OB_FAIL(resolve_show_from_database(*from_database_clause_node->children_[0],
                       real_tenant_id,
                       show_database_id,
                       show_database_name))) {
          LOG_WARN("fail to resolve show from database", K(ret), K(real_tenant_id));
        }
      }
    }
    const bool is_index = false;
    uint64_t org_session_id = OB_INVALID_ID;
    // bug16913178, make session id = 0 to return view, otherwise return tmptable
    if (T_SHOW_CREATE_VIEW == node_type && OB_NOT_NULL(schema_checker_->get_schema_mgr())) {
      org_session_id = schema_checker_->get_schema_mgr()->get_session_id();
      schema_checker_->get_schema_mgr()->set_session_id(0);
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(schema_checker_->get_table_schema(real_tenant_id,
                   show_database_id,
                   show_table_name,
                   is_index,
                   false, /*cte_table_fisrt false*/
                   table_schema))) {
      LOG_WARN("get table schema failed", K(ret), K(real_tenant_id), K(show_database_id), K(show_table_name));
    } else if (OB_UNLIKELY(NULL == table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema from schema checker is NULL", K(ret), K(table_schema));
    } else if (T_SHOW_CREATE_VIEW == node_type && !table_schema->is_view_table()) {
      ret = OB_ERR_WRONG_OBJECT;
      LOG_USER_ERROR(OB_ERR_WRONG_OBJECT, to_cstring(show_database_name), to_cstring(show_table_name), "VIEW");
    } else {
      show_table_id = table_schema->get_table_id();
      is_view = table_schema->is_view_table() ? true : false;
    }
    if (OB_INVALID_ID != org_session_id) {
      schema_checker_->get_schema_mgr()->set_session_id(org_session_id);
    }
  }
  return ret;
}

int ObShowResolver::resolve_show_from_database(
    const ParseNode& from_db_node, uint64_t real_tenant_id, uint64_t& show_database_id, ObString& show_database_name)
{
  // resolve clause for database name
  int ret = OB_SUCCESS;
  if (OB_FAIL(resolve_database_factor(&from_db_node, real_tenant_id, show_database_id, show_database_name))) {
    if (OB_ERR_BAD_DATABASE == ret) {
      LOG_USER_ERROR(OB_ERR_BAD_DATABASE, show_database_name.length(), show_database_name.ptr());
    } else {
      LOG_WARN("fail to resolve database name", K(ret));
    }
  }
  return ret;
}

int ObShowResolver::parse_and_resolve_select_sql(const ObString& select_sql)
{
  int ret = OB_SUCCESS;
  // 1. parse and resolve view defination
  if (OB_ISNULL(session_info_) || OB_ISNULL(params_.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data member is not init", K(ret), K(session_info_), K(params_.allocator_));
  } else {
    ParseResult select_result;
    ObParser parser(*params_.allocator_, session_info_->get_sql_mode());
    if (OB_FAIL(parser.parse(select_sql, select_result))) {
      LOG_WARN("parse select sql failed", K(select_sql), K(ret));
    } else {
      // use alias to make all columns number continued
      if (OB_ISNULL(select_result.result_tree_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result tree is NULL", K(ret));
      } else if (OB_UNLIKELY(
                     select_result.result_tree_->num_child_ != 1 || NULL == select_result.result_tree_->children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result tree is invalid",
            K(ret),
            K(select_result.result_tree_->num_child_),
            K(select_result.result_tree_->children_));
      } else if (OB_UNLIKELY(NULL == select_result.result_tree_->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result tree is invalid", K(ret), "child ptr", select_result.result_tree_->children_[0]);
      } else {
        ParseNode* select_stmt_node = select_result.result_tree_->children_[0];
        if (OB_FAIL(ObSelectResolver::resolve(*select_stmt_node))) {
          LOG_WARN("resolve select in view definition failed", K(ret), K(select_stmt_node));
        }
      }
    }
  }

  return ret;
}

int ObShowResolver::resolve_like_or_where_clause(ObShowResolverContext& ctx)
{
  int ret = OB_SUCCESS;
  const ParseNode* parse_tree = ctx.parse_tree_;
  ParseNode* condition_node = ctx.condition_node_;
  ObDMLStmt* stmt = get_stmt();
  if (OB_ISNULL(ctx.parse_tree_) || OB_ISNULL(stmt) || OB_ISNULL(allocator_) || OB_ISNULL(params_.expr_factory_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("data member is not init", K(ret), K(ctx.parse_tree_), K(stmt), K_(params_.expr_factory));
  } else if (NULL == condition_node ||
             (parse_tree->type_ != T_SHOW_TABLES && parse_tree->type_ != T_SHOW_DATABASES &&
                 parse_tree->type_ != T_SHOW_VARIABLES && parse_tree->type_ != T_SHOW_CHARSET &&
                 parse_tree->type_ != T_SHOW_COLLATION && parse_tree->type_ != T_SHOW_TRACE &&
                 parse_tree->type_ != T_SHOW_COLUMNS && parse_tree->type_ != T_SHOW_TABLE_STATUS &&
                 parse_tree->type_ != T_SHOW_SERVER_STATUS && parse_tree->type_ != T_SHOW_INDEXES &&
                 parse_tree->type_ != T_SHOW_PARAMETERS && parse_tree->type_ != T_SHOW_STATUS &&
                 parse_tree->type_ != T_SHOW_TABLEGROUPS && parse_tree->type_ != T_SHOW_PROCEDURE_STATUS)) {
    // do nothing
  } else {
    // Like or Where clause
    if (T_LIKE_CLAUSE == condition_node->type_) {
      // like clause
      if (OB_UNLIKELY(condition_node->num_child_ != 2 || NULL == condition_node->children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("parse tree is wrong", K(ret), K(condition_node->num_child_), K(condition_node->children_));
      } else if (OB_UNLIKELY(NULL == condition_node->children_[0] || NULL == condition_node->children_[1])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("parse tree child is NULL", K(ret), K(condition_node->children_[0]), K(condition_node->children_[1]));
      } else if (OB_UNLIKELY(T_VARCHAR != condition_node->children_[0]->type_ ||
                             T_VARCHAR != condition_node->children_[1]->type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("parse node type is unexpected",
            K(ret),
            K(condition_node->children_[0]->type_),
            K(condition_node->children_[1]->type_));
      } else {
        ObString like_pattern;
        ObString like_escape;
        like_pattern.assign_ptr(static_cast<const char*>(condition_node->children_[0]->str_value_),
            static_cast<int32_t>((condition_node->children_[0]->str_len_)));
        like_escape.assign_ptr(static_cast<const char*>(condition_node->children_[1]->str_value_),
            static_cast<int32_t>(condition_node->children_[1]->str_len_));

        ObRawExpr* ref_expr = NULL;
        ObConstRawExpr* like_pat_expr = NULL;
        ObConstRawExpr* like_es_expr = NULL;
        ObOpRawExpr* op_expr = NULL;
        if (like_pattern.length() <= 0) {
          // stmt has no like pattern, do nothing
        } else {
          ObString col_name;
          ObString alias_name;
          ObQualifiedName q_name;
          q_name.database_name_ = ObString::make_string("");
          if (share::is_oracle_mode()) {
            // alias name and column name must be upper
            size_t size = 0;
            if (OB_FAIL(ob_write_string(
                    *params_.allocator_, ObString::make_string(ObShowSqlSet::SUBQERY_ALIAS), alias_name))) {
              LOG_WARN("write alias name failed", K(ret));
            } else if (OB_FAIL(ob_write_string(*params_.allocator_, ctx.like_column_, col_name))) {
              LOG_WARN("write column name failed", K(ret));
            } else {
              if (!col_name.empty()) {
                size = ObCharset::caseup(ObCollationType::CS_TYPE_UTF8MB4_BIN,
                    col_name.ptr(),
                    col_name.length(),
                    col_name.ptr(),
                    col_name.length());
                col_name.set_length(static_cast<int32_t>(size));
              }
              if (!alias_name.empty()) {
                size = ObCharset::caseup(ObCollationType::CS_TYPE_UTF8MB4_BIN,
                    alias_name.ptr(),
                    alias_name.length(),
                    alias_name.ptr(),
                    alias_name.length());
                alias_name.set_length(static_cast<int32_t>(size));
              }
            }
          } else {
            alias_name = ObString::make_string(ObShowSqlSet::SUBQERY_ALIAS);
            col_name = ctx.like_column_;
          }
          q_name.tbl_name_ = alias_name;
          q_name.col_name_ = col_name;
          if (OB_FAIL(resolve_column_ref_expr(q_name, ref_expr))) {
            LOG_WARN("resolve column ref expr failed", K(q_name));
          } else if (OB_ISNULL(ref_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column expr is null");
          } else if (OB_FAIL(ObRawExprUtils::build_const_string_expr(*params_.expr_factory_,
                         ObVarcharType,
                         like_pattern,
                         ObCharset::get_default_collation(ObCharset::get_default_charset()),
                         like_pat_expr))) {
            LOG_WARN("fail to create string raw expr", K(ret), K(like_pattern));
          } else if (OB_FAIL(ObRawExprUtils::build_const_string_expr(*params_.expr_factory_,
                         ObVarcharType,
                         like_escape,
                         ObCharset::get_default_collation(ObCharset::get_default_charset()),
                         like_es_expr))) {
            LOG_WARN("fail to create string raw expr", K(ret), K(like_escape));
          } else if (OB_FAIL(params_.expr_factory_->create_raw_expr(T_OP_LIKE, op_expr))) {
            LOG_WARN("create raw expr failed", K(ret));
          } else if (OB_ISNULL(op_expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("op expr is null");
          } else {
            op_expr->set_param_exprs(ref_expr, like_pat_expr, like_es_expr);
            if (OB_FAIL(op_expr->formalize(session_info_))) {
              LOG_WARN("fail to formalize expression", K(ret), K(op_expr));
            } else if (OB_FAIL(stmt->add_condition_expr(op_expr))) {
              LOG_WARN("fail to add condition expression", K(ret), K(op_expr));
            }
          }
        }
      }
    } else if (T_WHERE_CLAUSE == condition_node->type_) {
      // where clause
      if (OB_FAIL(ObDMLResolver::resolve_where_clause(condition_node))) {
        LOG_WARN("resolve where clause failed", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid condition type", K(ret), K(get_type_name(condition_node->type_)));
    }
  }
  return ret;
}

int ObShowResolver::replace_where_clause(ParseNode* node, const ObShowResolverContext& show_resv_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("expr node is null");
  } else {
    switch (node->type_) {
      case T_NULL:
      case T_INT:
      case T_FLOAT:
      case T_DOUBLE:
      case T_YEAR:
      case T_DATE:
      case T_TIME:
      case T_DATETIME:
      case T_TIMESTAMP:
      case T_TIMESTAMP_TZ:
      case T_TIMESTAMP_LTZ:
      case T_TIMESTAMP_NANO:
      case T_HEX_STRING:
      case T_BOOL:
      case T_NUMBER:
      case T_NUMBER_FLOAT:
      case T_QUESTIONMARK:
      case T_SYSTEM_VARIABLE:
      case T_DEFAULT_NULL:
      case T_VARCHAR:
      case T_RAW:
      case T_INTERVAL_YM:
      case T_INTERVAL_DS:
      case T_NVARCHAR2:
      case T_NCHAR:
      case T_UROWID:
      case T_LOB: 
      case T_JSON:{
        break;  // do nothing
      }
      case T_COLUMN_REF: {
        // expr has column == tables_in_xxx,must be changed to the real column name :table_name
        if (ObCharset::case_insensitive_equal(ObString(node->str_len_, node->str_value_), show_resv_ctx.column_name_)) {
          node->str_value_ = "table_name";
          node->str_len_ = strlen("table_name");
        }
        if (OB_UNLIKELY(node->num_child_ != 3 || NULL == node->children_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parse tree is wrong", K(ret), K(node->num_child_), K(node->children_));
        } else if (OB_UNLIKELY(NULL == node->children_[2])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parser tree child is NULL", K(ret), K(node->children_[2]));
        } else {
          if (ObCharset::case_insensitive_equal(
                  ObString(node->children_[2]->str_len_, node->children_[2]->str_value_), show_resv_ctx.column_name_)) {
            node->children_[2]->str_value_ = "table_name";
            node->children_[2]->str_len_ = strlen("table_name");
          }
        }
        break;
      }
      case T_OBJ_ACCESS_REF: {
        if (OB_UNLIKELY(node->num_child_ != 2 || NULL == node->children_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parse tree is wrong", K(ret), K(node->num_child_), K(node->children_));
        } else if (OB_UNLIKELY(NULL == node->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parser tree child is NULL", K(ret), K(node->children_[2]));
        } else {
          if (T_IDENT == node->children_[0]->type_) {
            if (ObCharset::case_insensitive_equal(
                    ObString(node->children_[0]->str_len_, node->children_[0]->str_value_),
                    show_resv_ctx.column_name_)) {
              node->children_[0]->str_value_ = "table_name";
              node->children_[0]->str_len_ = strlen("table_name");
            }
          } else if (OB_FAIL(replace_where_clause(node->children_[0], show_resv_ctx))) {
            LOG_WARN("failed replace expr", K(ret));
          }
          if (OB_SUCC(ret) && OB_NOT_NULL(node->children_[1])) {
            if (OB_FAIL(replace_where_clause(node->children_[1], show_resv_ctx))) {
              LOG_WARN("failed replace expr", K(ret));
            }
          }
        }
        break;
      }
      case T_OP_EXISTS:
      case T_ANY:
      case T_ALL: {
        if (OB_UNLIKELY(node->num_child_ < 1 || NULL == node->children_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parse tree is wrong", K(ret), K(node->num_child_), K(node->children_));
        } else if (OB_UNLIKELY(NULL == node->children_[0] || T_SELECT != node->children_[0]->type_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parser tree child is NULL", K(ret), K(node->children_[0]));
        } else {
          if (OB_FAIL(replace_where_clause(node->children_[0], show_resv_ctx))) {
            LOG_WARN("failed replace expr", K(ret));
          }
        }
        break;
      }
      case T_OP_NOT: {
        ParseNode* cur_expr = node;
        while (OB_SUCCESS == ret && cur_expr && cur_expr->type_ == T_OP_NOT) {
          if (OB_UNLIKELY(cur_expr->num_child_ < 1 || NULL == cur_expr->children_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("parse tree is wrong", K(ret), K(cur_expr->num_child_), K(cur_expr->children_));
          } else {
            cur_expr = cur_expr->children_[0];
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(replace_where_clause(cur_expr, show_resv_ctx))) {
            LOG_WARN("failed replace expr", K(ret));
          }
        }
        break;
      }
      case T_OP_POS:
      case T_OP_NEG: {
        ParseNode* cur_expr = node;
        while (
            OB_SUCCESS == ret && cur_expr && cur_expr && (cur_expr->type_ == T_OP_POS || cur_expr->type_ == T_OP_NEG)) {
          if (OB_UNLIKELY(cur_expr->num_child_ < 1 || NULL == cur_expr->children_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("parse tree is wrong", K(ret), K(cur_expr->num_child_), K(cur_expr->children_));
          } else {
            cur_expr = cur_expr->children_[0];
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(replace_where_clause(cur_expr, show_resv_ctx))) {
            LOG_WARN("failed replace expr", K(ret));
          }
        }
        break;
      }
      case T_OP_AND:
      case T_OP_OR:
      case T_OP_ADD:
      case T_OP_MINUS:
      case T_OP_MUL:
      case T_OP_DIV:
      case T_OP_REM:
      case T_OP_BIT_AND:
      case T_OP_BIT_OR:
      case T_OP_BIT_XOR:
      case T_OP_BIT_LEFT_SHIFT:
      case T_OP_BIT_RIGHT_SHIFT:
      case T_OP_POW:
      case T_OP_MOD:
      case T_OP_INT_DIV:
      case T_OP_LE:
      case T_OP_LT:
      case T_OP_EQ:
      case T_OP_GE:
      case T_OP_GT:
      case T_OP_NE:
      case T_OP_IS:
      case T_OP_IS_NOT:
      case T_OP_CNN:
      case T_OP_REGEXP:
      case T_OP_NOT_REGEXP:
      case T_OP_IN:
      case T_OP_NOT_IN: {
        if (OB_UNLIKELY(node->num_child_ != 2 || NULL == node->children_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parse tree is wrong", K(ret), K(node->num_child_), K(node->children_));
        } else {
          if (OB_FAIL(replace_where_clause(node->children_[0], show_resv_ctx))) {
            LOG_WARN("failed replace expr", K(ret));
          } else if (OB_FAIL(replace_where_clause(node->children_[1], show_resv_ctx))) {
            LOG_WARN("failed replace expr", K(ret));
          }
        }
        break;
      }
      case T_OP_LIKE:
      case T_OP_NOT_LIKE: {
        if (OB_UNLIKELY(!(node->num_child_ == 3 || node->num_child_ == 2) || NULL == node->children_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parse tree is wrong", K(ret), K(node->num_child_), K(node->children_));
        } else if (node->num_child_ == 3) {
          if (OB_FAIL(replace_where_clause(node->children_[0], show_resv_ctx))) {
            LOG_WARN("failed replace expr", K(ret));
          } else if (OB_FAIL(replace_where_clause(node->children_[1], show_resv_ctx))) {
            LOG_WARN("failed replace expr", K(ret));
          } else if (OB_FAIL(replace_where_clause(node->children_[2], show_resv_ctx))) {
            LOG_WARN("failed replace expr", K(ret));
          }
        } else if (node->num_child_ == 2) {
          if (OB_FAIL(replace_where_clause(node->children_[0], show_resv_ctx))) {
            LOG_WARN("failed replace expr", K(ret));
          } else if (OB_FAIL(replace_where_clause(node->children_[1], show_resv_ctx))) {
            LOG_WARN("failed replace expr", K(ret));
          }
        }
        break;
      }
      case T_OP_BTW:
      case T_OP_NOT_BTW: {
        if (OB_UNLIKELY(node->num_child_ != 3 || NULL == node->children_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parse tree is wrong", K(ret), K(node->num_child_), K(node->children_));
        } else {
          if (OB_FAIL(replace_where_clause(node->children_[0], show_resv_ctx))) {
            LOG_WARN("failed replace expr", K(ret));
          } else if (OB_FAIL(replace_where_clause(node->children_[1], show_resv_ctx))) {
            LOG_WARN("failed replace expr", K(ret));
          } else if (OB_FAIL(replace_where_clause(node->children_[2], show_resv_ctx))) {
            LOG_WARN("failed replace expr", K(ret));
          }
        }
        break;
      }
      case T_CASE: {
        if (OB_UNLIKELY(node->num_child_ != 3 || NULL == node->children_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parse tree is wrong", K(ret), K(node->num_child_), K(node->children_));
        } else if (OB_UNLIKELY(NULL == node->children_[1] || T_WHEN_LIST != node->children_[1]->type_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parser tree child is NULL", K(ret), K(node->children_[1]));
        } else {
          if (node->children_[0]) {
            if (OB_FAIL(replace_where_clause(node->children_[0], show_resv_ctx))) {
              LOG_WARN("failed replace expr", K(ret));
              break;
            }
          }
          ParseNode* when_node;
          for (int32_t i = 0; OB_SUCC(ret) && i < node->children_[1]->num_child_; i++) {
            if (OB_UNLIKELY(NULL == node->children_[1]->children_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("parser tree child is NULL", K(ret));
            } else {
              when_node = node->children_[1]->children_[i];
              if (OB_UNLIKELY(when_node->num_child_ != 2 || NULL == when_node->children_)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("parse tree is wrong", K(ret), K(when_node->num_child_), K(when_node->children_));
              } else {
                if (OB_FAIL(replace_where_clause(when_node->children_[0], show_resv_ctx))) {
                  LOG_WARN("failed replace expr", K(ret));
                  break;
                } else if (OB_FAIL(replace_where_clause(when_node->children_[1], show_resv_ctx))) {
                  LOG_WARN("failed replace expr", K(ret));
                  break;
                } else { /*do nothing*/
                }
              }
            }
          }

          if (node->children_[2]) {
            if (OB_FAIL(replace_where_clause(node->children_[2], show_resv_ctx))) {
              LOG_WARN("failed replace expr", K(ret));
              break;
            }
          }
        }
        break;
      }
      case T_EXPR_LIST: {
        if (OB_UNLIKELY(NULL == node->children_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("parse tree is wrong", K(ret), K(node->num_child_), K(node->children_));
        } else {
          for (int32_t i = 0; OB_SUCC(ret) && i < node->num_child_; i++) {
            if (OB_FAIL(replace_where_clause(node->children_[i], show_resv_ctx))) {
              LOG_WARN("failed replace expr", K(ret));
            }
          }
        }
        break;
      }
      case T_FUN_SYS: {
        if (node->num_child_ > 1) {
          if (OB_UNLIKELY(NULL == node->children_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("parse tree is wrong", K(ret), K(node->num_child_), K(node->children_));
          } else if (OB_UNLIKELY(NULL == node->children_[1] || T_EXPR_LIST != node->children_[1]->type_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("parser tree child is NULL", K(ret), K(node->children_[1]));
          } else {
            int32_t num = node->children_[1]->num_child_;
            for (int32_t i = 0; OB_SUCC(ret) && i < num; i++) {
              if (OB_FAIL(replace_where_clause(node->children_[1]->children_[i], show_resv_ctx))) {
                LOG_WARN("failed replace expr", K(ret));
                break;
              }
            }
          }
        }
        break;
      }
      default:
        ret = OB_ERR_PARSER_SYNTAX;
        LOG_WARN("wrong type in expression", K(node->type_));
        break;
    }
  }
  return ret;
}

int ObShowResolver::resolve_column_ref_expr(const ObQualifiedName& q_name, ObRawExpr*& real_ref_expr)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(resolve_table_column_ref(q_name, real_ref_expr))) {
    LOG_WARN("fail to resolve table column_ref", K(ret));
  }
  return ret;
}

int ObShowResolver::ObSqlStrGenerator::init(common::ObIAllocator* allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == (sql_buf_ = static_cast<char*>(allocator->alloc(OB_MAX_SQL_LENGTH))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to alloc sql buf", K(ret), K(OB_MAX_SQL_LENGTH));
  }
  return ret;
}

int ObShowResolver::ObSqlStrGenerator::gen_select_str(const char* select_str, ...)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sql_buf_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("sql buffer is not init", K(ret));
  } else {
    if (NULL == select_str) {
      if (OB_FAIL(databuff_printf(sql_buf_, OB_MAX_SQL_LENGTH, sql_buf_pos_, "SELECT * "))) {
        LOG_WARN("fail to add select sql string", K(ret));
      }
    } else {
      va_list select_args;
      va_start(select_args, select_str);
      if (share::is_oracle_mode()) {
        // show xxx will be banned on oracle mode, so here just replace ` to "
        char select_str_o[OB_SHORT_SQL_LENGTH];
        int64_t len = strlen(select_str);
        int64_t new_len = 0;
        for (int i = 0; i < len; i++) {
          if ('`' != select_str[i]) {
            select_str_o[new_len++] = select_str[i];
          } else {
            select_str_o[new_len++] = '"';
          }
        }
        select_str_o[new_len] = '\0';
        if (OB_FAIL(databuff_vprintf(sql_buf_, OB_MAX_SQL_LENGTH, sql_buf_pos_, select_str_o, select_args))) {
          LOG_WARN("fail to add select sql string", K(ret));
        }
      } else {
        if (OB_FAIL(databuff_vprintf(sql_buf_, OB_MAX_SQL_LENGTH, sql_buf_pos_, select_str, select_args))) {
          LOG_WARN("fail to add select sql string", K(ret));
        }
      }
      va_end(select_args);
    }
  }
  return ret;
}

int ObShowResolver::ObSqlStrGenerator::gen_from_str(const char* subquery_str, ...)
{
  int ret = OB_SUCCESS;
  HEAP_VAR(char[OB_MAX_SQL_LENGTH], tmp_buf)
  {
    int64_t pos = 0;
    if (OB_ISNULL(sql_buf_) || OB_ISNULL(subquery_str)) {
      ret = OB_NOT_INIT;
      LOG_WARN("sql buffer or subquery_str is not init", K(ret), K(sql_buf_), K(subquery_str));
    }
    if (share::is_oracle_mode()) {
      // show xxx will be banned on oracle mode, so here just replace ` to "
      char new_str[OB_SHORT_SQL_LENGTH];
      int64_t len = strlen(subquery_str);
      int64_t new_len = 0;
      for (int i = 0; i < len; i++) {
        if ('`' != subquery_str[i]) {
          new_str[new_len++] = subquery_str[i];
        } else {
          new_str[new_len++] = '"';
        }
      }
      new_str[new_len] = '\0';
      if (OB_FAIL(databuff_printf(
              tmp_buf, OB_MAX_SQL_LENGTH, pos, " FROM (%s) %s", new_str, ObShowSqlSet::SUBQERY_ALIAS))) {
        LOG_WARN("fail to add subquery sql string", K(ret));
      }
    } else {
      if (OB_FAIL(databuff_printf(
              tmp_buf, OB_MAX_SQL_LENGTH, pos, " FROM (%s) %s", subquery_str, ObShowSqlSet::SUBQERY_ALIAS))) {
        LOG_WARN("fail to add subquery sql string", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      va_list subquery_args;
      va_start(subquery_args, subquery_str);
      if (OB_FAIL(databuff_vprintf(sql_buf_, OB_MAX_SQL_LENGTH, sql_buf_pos_, tmp_buf, subquery_args))) {
        LOG_WARN("fail to add subquery args sql string", K(ret));
      }
      va_end(subquery_args);
    }
  }
  return ret;
}

int ObShowResolver::ObSqlStrGenerator::gen_limit_str(int64_t offset, int64_t row_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(sql_buf_, OB_MAX_SQL_LENGTH, sql_buf_pos_, " LIMIT %ld, %ld ", offset, row_cnt))) {
    LOG_WARN("fail to gen limit string", K(ret));
  }
  return ret;
}
void ObShowResolver::ObSqlStrGenerator::assign_sql_str(ObString& sql_str)
{
  sql_str.assign_ptr(sql_buf_, static_cast<uint32_t>(sql_buf_pos_));
}

}  // namespace sql
}  // namespace oceanbase

namespace oceanbase {
namespace sql {
#define DEFINE_SHOW_CLAUSE(clause_type, string) const char* ObShowResolver::ObShowSqlSet::clause_type = string

#define DEFINE_SHOW_CLAUSE_SET(SHOW_STMT_TYPE, select_str, subquery_str, ora_subquery_str, like_str) \
  DEFINE_SHOW_CLAUSE(SHOW_STMT_TYPE##_SELECT, select_str);                                           \
  DEFINE_SHOW_CLAUSE(SHOW_STMT_TYPE##_SUBQUERY, subquery_str);                                       \
  DEFINE_SHOW_CLAUSE(SHOW_STMT_TYPE##_ORA_SUBQUERY, ora_subquery_str);                               \
  DEFINE_SHOW_CLAUSE(SHOW_STMT_TYPE##_LIKE, like_str)

DEFINE_SHOW_CLAUSE(SUBQERY_ALIAS, "subquery_alias");
DEFINE_SHOW_CLAUSE_SET(SHOW_TABLES, "SELECT table_name AS `Tables_in_%.*s` ",
    "SELECT table_name FROM %s.%s WHERE database_id = %ld ORDER BY table_name COLLATE utf8mb4_bin ASC", NULL,
    "table_name");
DEFINE_SHOW_CLAUSE_SET(SHOW_TABLES_LIKE, "SELECT table_name AS `Tables_in_%.*s (%.*s)` ",
    "SELECT table_name FROM %s.%s WHERE database_id = %ld ORDER BY table_name COLLATE utf8mb4_bin ASC", NULL,
    "table_name");
DEFINE_SHOW_CLAUSE_SET(SHOW_FULL_TABLES, "SELECT table_name AS `Tables_in_%.*s`, table_type AS `Table_type` ",
    "SELECT table_name, table_type FROM %s.%s WHERE database_id = %ld ORDER BY table_name COLLATE utf8mb4_bin ASC",
    NULL, "table_name");
DEFINE_SHOW_CLAUSE_SET(SHOW_FULL_TABLES_LIKE,
    "SELECT table_name AS `Tables_in_%.*s (%.*s)`, table_type AS `Table_type` ",
    "SELECT table_name, table_type FROM %s.%s WHERE database_id = %ld ORDER BY table_name COLLATE utf8mb4_bin ASC",
    NULL, "table_name");
DEFINE_SHOW_CLAUSE_SET(SHOW_CHARSET, NULL,
    "SELECT charset AS Charset, description AS `Description`, default_collation AS `Default collation`, max_length AS "
    "`Maxlen` FROM %s.%s",
    R"(SELECT "CHARSET" AS "CHARSET", "DESCRIPTION" AS "DESCRIPTION", "DEFAULT_COLLATION" AS "DEFAULT COLLATION", "MAX_LENGTH" AS "MAXLEN" FROM %s.%s)",
    "Charset");
DEFINE_SHOW_CLAUSE_SET(SHOW_TABLEGROUPS, NULL,
    "SELECT /*+read_consistency(WEAK)*/ t1.Tablegroup_name AS Tablegroup_name, t2.Table_name AS Table_name, t3.Database_name AS Database_name \
                        FROM %s.%s t1 LEFT JOIN %s.%s  t2 ON (t1.tablegroup_id = t2.tablegroup_id and t2.tenant_id = %lu) \
                        LEFT JOIN %s.%s  t3 ON (t2.database_id = t3.database_id and t3.tenant_id = %lu) \
                        WHERE t1.tenant_id = %lu and t1.drop_schema_version = -1\
                        ORDER BY t1.tablegroup_name, t2.table_name",
    "SELECT /*+READ_CONSISTENCY(WEAK)*/ T1.TABLEGROUP_NAME AS \"TABLEGROUP_NAME\", T2.TABLE_NAME AS \"TABLE_NAME\", T3.DATABASE_NAME AS \"DATABASE_NAME\" \
                        FROM %s.%s T1 LEFT JOIN %s.%s  T2 ON (T1.TABLEGROUP_ID = T2.TABLEGROUP_ID AND T2.TENANT_ID = %lu) \
                        LEFT JOIN %s.%s  T3 ON (T2.DATABASE_ID = T3.DATABASE_ID AND T3.TENANT_ID = %lu) \
                        WHERE T1.TENANT_ID = %lu and T1.DROP_SCHEMA_VERSION = -1\
                        ORDER BY T1.TABLEGROUP_NAME, T2.TABLE_NAME",
    "Tablegroup_name");
DEFINE_SHOW_CLAUSE_SET(SHOW_VARIABLES, NULL,
    "SELECT variable_name AS `Variable_name`, value AS `Value` FROM %s.%s ORDER BY variable_name ASC",
    R"(SELECT "VARIABLE_NAME" AS "VARIABLE_NAME", "VALUE" AS "VALUE" FROM %s.%s ORDER BY VARIABLE_NAME ASC)",
    "Variable_name");
DEFINE_SHOW_CLAUSE_SET(SHOW_GLOBAL_VARIABLES, NULL,
    "SELECT variable_name AS `Variable_name`, value AS `Value` FROM %s.%s ORDER BY variable_name ASC",
    R"(SELECT "VARIABLE_NAME" AS "VARIABLE_NAME", "VALUE" AS "VALUE" FROM %s.%s ORDER BY VARIABLE_NAME ASC)",
    "Variable_name");
DEFINE_SHOW_CLAUSE_SET(SHOW_COLUMNS, NULL,
    "SELECT field AS `Field`, type AS `Type`, `NULL` AS `Null`, `KEY` AS `Key`, `DEFAULT` AS `Default`, extra AS `Extra` \
                        FROM %s.%s  where table_id = %ld",
    R"(SELECT "FIELD" AS "FIELD", "TYPE" AS "TYPE", "NULL" AS "NULL", "KEY" AS "KEY", "DEFAULT" AS "DEFAULT", "EXTRA" AS "EXTRA" )"
    R"(FROM %s.%s  WHERE TABLE_ID = %ld)",
    "Field");
DEFINE_SHOW_CLAUSE_SET(SHOW_FULL_COLUMNS, NULL,
    "SELECT field AS `Field`, type AS `Type`, collation AS `Collation`, `NULL` AS `Null`, `KEY` AS `Key`, `DEFAULT` AS `Default`, extra AS `Extra`, `PRIVILEGES` AS `Privileges`, `COMMENT` AS `Comment` \
                       FROM %s.%s  where table_id = %ld",
    R"(SELECT "FIELD" AS "FIELD", "TYPE" AS "TYPE", "COLLATION" AS "COLLATION", "NULL" AS "NULL", KEY AS "KEY", "DEFAULT" AS "DEFAULT", EXTRA AS "EXTRA", "PRIVILEGES" AS "PRIVILEGES", "COMMENT" AS "COMMENT" )"
    R"(FROM %s.%s  WHERE TABLE_ID = %ld)",
    "Field");
DEFINE_SHOW_CLAUSE_SET(SHOW_CREATE_DATABASE, NULL,
    "SELECT `database_name` AS `Database`, create_database AS `Create Database` FROM %s.%s  WHERE database_id = %ld",
    NULL, NULL);
DEFINE_SHOW_CLAUSE_SET(SHOW_CREATE_DATABASE_EXISTS, NULL,
    "SELECT `database_name` AS `Database`, create_database_with_if_not_exists AS `Create Database` FROM %s.%s WHERE "
    "database_id = %ld",
    NULL, NULL);
DEFINE_SHOW_CLAUSE_SET(SHOW_CREATE_TABLEGROUP, NULL,
    "SELECT tablegroup_name AS `Tablegroup`, create_tablegroup AS `Create Tablegroup` FROM %s.%s  WHERE tablegroup_id "
    "= %ld",
    R"(SELECT "TABLEGROUP_NAME" AS "TABLEGROUP", "CREATE_TABLEGROUP" AS "CREATE TABLEGROUP" FROM %s.%s  WHERE TABLEGROUP_ID = %ld)",
    NULL);
DEFINE_SHOW_CLAUSE_SET(SHOW_INDEXES, NULL,
    "SELECT `TABLE` AS `Table`, NON_UNIQUE AS Non_unique, KEY_NAME AS Key_name, SEQ_IN_INDEX AS Seq_in_index, "
    "COLUMN_NAME AS Column_name, COLLATION AS Collation, CARDINALITY AS Cardinality, SUB_PART AS Sub_part, PACKED AS "
    "Packed, `NULL` AS `Null`, INDEX_TYPE AS Index_type, `COMMENT` AS `Comment`, INDEX_COMMENT AS Index_comment, "
    "IS_VISIBLE AS Visible FROM %s.%s  where table_id = %ld",
    R"(SELECT "TABLE" AS "TABLE", "NON_UNIQUE" AS "NON_UNIQUE", "KEY_NAME" AS "KEY_NAME", "SEQ_IN_INDEX" AS "SEQ_IN_INDEX", "COLUMN_NAME" AS "COLUMN_NAME", "COLLATION" AS "COLLATION", "CARDINALITY" AS "CARDINALITY", "SUB_PART" AS "SUB_PART", "PACKED" AS "PACKED", "NULL" AS "NULL", "INDEX_TYPE" AS "INDEX_TYPE", "COMMENT" AS "COMMENT", )"
    R"(INDEX_COMMENT" AS "INDEX_COMMENT", "IS_VISIBLE" AS "VISIBLE" FROM %s.%s  WHERE TABLE_ID = %ld")",
    NULL);

DEFINE_SHOW_CLAUSE_SET(SHOW_TRACE, NULL, "SELECT title AS `Title`, key_value AS `KeyValue`, time AS `Time` FROM %s.%s ",
    R"(SELECT "TITLE" AS "TITLE", "KEY_VALUE" AS "KEYVALUE", "TIME" AS "TIME" FROM %s.%s )", "Title");

DEFINE_SHOW_CLAUSE_SET(SHOW_ENGINES, NULL, "SELECT * FROM %s.%s ", NULL, NULL);

DEFINE_SHOW_CLAUSE_SET(SHOW_PRIVILEGES, NULL, "SELECT * FROM %s.%s ", "SELECT * FROM %s.%s ", NULL);

DEFINE_SHOW_CLAUSE_SET(SHOW_QUERY_RESPONSE_TIME, NULL, 
    "SELECT response_time as RESPONSE_TIME, count as COUNT, total as TOTAL FROM %s.%s where tenant_id = %lu", NULL, NULL);

DEFINE_SHOW_CLAUSE_SET(SHOW_COLLATION, NULL,
    "SELECT collation AS `Collation`, charset AS `Charset`, id AS `Id`, is_default AS `Default`, is_compiled AS "
    "`Compiled`, sortlen AS `Sortlen` FROM %s.%s ",
    R"(SELECT "COLLATION" AS "COLLATION", "CHARSET" AS "CHARSET", "ID" AS "ID", "IS_DEFAULT" AS "DEFAULT", "IS_COMPILED" AS "COMPILED", "SORTLEN" AS "SORTLEN" FROM %s.%s )",
    "Collation");
DEFINE_SHOW_CLAUSE_SET(SHOW_GRANTS, "SELECT grants AS `Grants for %.*s@%.*s` ",
    "SELECT grants FROM %s.%s WHERE user_id = %ld", R"(SELECT "GRANTS" AS "GRANTS" FROM %s.%s WHERE USER_ID = %ld)",
    NULL);
DEFINE_SHOW_CLAUSE_SET(SHOW_PROCESSLIST, NULL,
    "SELECT id AS `Id`, user AS `User`, host AS `Host`, db AS `db`, command AS `Command`, time AS `Time`, state AS "
    "`State`, info AS `Info` FROM %s.%s WHERE is_serving_tenant(svr_ip, svr_port, %ld)=1",
    R"(SELECT "ID" AS "ID", "USER" AS "USER", "HOST" AS "HOST", "DB" AS "DB", "COMMAND" AS "COMMAND", "TIME" AS "TIME", "STATE" AS "STATE", "INFO" AS "INFO" FROM %s.%s WHERE IS_SERVING_TENANT(SVR_IP, SVR_PORT, %ld)=1)",
    NULL);
DEFINE_SHOW_CLAUSE_SET(SHOW_FULL_PROCESSLIST, NULL,
    "SELECT id AS `Id`, user as `User`, tenant as `Tenant`, host AS `Host`, db AS `db`, command AS `Command`, time AS "
    "`Time`, state AS `State`, info AS `Info` , svr_ip AS `Ip`, sql_port AS `Port` FROM %s.%s WHERE "
    "is_serving_tenant(svr_ip, svr_port, %ld)=1",
    R"(SELECT "ID" AS "ID", "USER" AS "USER", "TENANT" AS "TENANT", "HOST" AS "HOST", "DB" AS "DB", "COMMAND" AS "COMMAND", "TIME" AS "TIME", "STATE" AS "STATE", "INFO" AS "INFO" , "SVR_IP" AS "IP", "SQL_PORT" AS "PORT" FROM %s.%s WHERE IS_SERVING_TENANT(SVR_IP, SVR_PORT, %ld)=1)",
    NULL);
DEFINE_SHOW_CLAUSE_SET(SHOW_SYS_PROCESSLIST, NULL,
    "SELECT id AS `Id`, user AS `User`, host AS `Host`, db AS `db`, command AS `Command`, time AS `Time`, state AS "
    "`State`, info AS `Info` FROM %s.%s",
    R"(SELECT "ID" AS "ID", "USER" AS "USER", "HOST" AS "HOST", "DB" AS "DB", "COMMAND" AS "COMMAND", "TIME" AS "TIME", "STATE" AS "STATE", "INFO" AS "INFO" FROM %s.%s)",
    NULL);
DEFINE_SHOW_CLAUSE_SET(SHOW_SYS_FULL_PROCESSLIST, NULL,
    "SELECT id AS `Id`, user as `User`, tenant as `Tenant`, host AS `Host`, db AS `db`, command AS `Command`, time AS "
    "`Time`, state AS `State`, info AS `Info` , svr_ip AS `Ip`, sql_port AS `Port`, proxy_sessid AS `Proxy_sessid` "
    "FROM %s.%s",
    R"(SELECT "ID" AS "ID", "USER" AS "USER", "TENANT" AS "TENANT", "HOST" AS "HOST", "DB" AS "DB", "COMMAND" AS "COMMAND", "TIME" AS "TIME", "STATE" AS "STATE", "INFO" AS "INFO" , "SVR_IP" AS "IP", "SQL_PORT" AS "PORT", "PROXY_SESSID" AS "PROXY_SESSID" FROM %s.%s)",
    NULL);
DEFINE_SHOW_CLAUSE_SET(SHOW_TABLE_STATUS, NULL,
    "SELECT table_name AS `Name`, engine as `Engine`, version as `Version`, row_format as `Row_format`, `ROWS` as "
    "`Rows`, avg_row_length as `Avg_row_length`, data_length as `Data_length`, max_data_length as `Max_data_length`, "
    "index_length as `Index_length`, data_free as `Data_free`, auto_increment as `Auto_increment`, create_time as "
    "`Create_time`, update_time as `Update_time`, check_time as `Check_time`, collation as `Collation`, checksum as "
    "`Checksum`, create_options as `Create_options`, `COMMENT` as `Comment` FROM %s.%s WHERE database_id = %ld ORDER "
    "BY name COLLATE utf8mb4_bin ASC",
    R"(SELECT "TABLE_NAME" AS "NAME", "ENGINE", "VERSION", "ROW_FORMAT", "ROWS" AS "ROWS", "AVG_ROW_LENGTH", "DATA_LENGTH", "MAX_DATA_LENGTH", "INDEX_LENGTH", "DATA_FREE", "AUTO_INCREMENT", "CREATE_TIME", "UPDATE_TIME", "CHECK_TIME", "COLLATION", "CHECKSUM", "CREATE_OPTIONS", "COMMENT" AS "COMMENT" FROM %s.%s WHERE DATABASE_ID = %ld ORDER BY NAME COLLATE UTF8MB4_BIN ASC)",
    "name");
DEFINE_SHOW_CLAUSE_SET(SHOW_PROCEDURE_STATUS, NULL,
    "select database_name AS `Db`, routine_name AS `Name`, c.type AS `Type`, user_name AS `Definer`, p.gmt_modified AS "
    "`Modified`, p.gmt_create AS `Created`,'DEFINER' AS `Security_type`, p.comment AS `Comment`, character_set_client, "
    "collation_connection, collation_database AS `Database Collation`from %s.%s p, %s.%s d, %s.%s u, %s.%s c where "
    "p.tenant_id = d.tenant_id and p.database_id = d.database_id and p.tenant_id = u.tenant_id and p.owner_id = "
    "u.user_id and d.database_name = c.db and p.routine_name = c.name and (case c.type when 'PROCEDURE' then 1 else 0 "
    "end) = p.routine_type and d.database_id = %ld and p.routine_type = %ld ORDER BY name COLLATE utf8mb4_bin ASC",
    NULL, "name");
DEFINE_SHOW_CLAUSE_SET(SHOW_TRIGGERS, NULL, "SELECT NULL FROM dual WHERE 1 = 2 ", NULL, NULL);
DEFINE_SHOW_CLAUSE_SET(
    SHOW_WARNINGS, NULL, "SELECT `level` AS `Level`, `code` AS `Code`, `message` AS `Message` FROM %s.%s ", NULL, NULL);
DEFINE_SHOW_CLAUSE_SET(
    SHOW_COUNT_WARNINGS, NULL, "SELECT count(*) AS `@@session.warning_count` FROM %s.%s ", NULL, NULL);
DEFINE_SHOW_CLAUSE_SET(SHOW_ERRORS, NULL,
    "SELECT `level` AS `Level`, `code` AS `Code`, `message` AS `Message` FROM %s.%s  WHERE `level` = \'Error\'", NULL,
    NULL);
DEFINE_SHOW_CLAUSE_SET(SHOW_COUNT_ERRORS, NULL,
    "SELECT count(*) AS `@@session.error_count` FROM %s.%s  WHERE `level` = \'Error\'", NULL, NULL);
DEFINE_SHOW_CLAUSE_SET(SHOW_PARAMETERS, NULL,
    "SELECT zone, svr_type, svr_ip, svr_port, name, data_type, value, info, section, scope, source, edit_level from "
    "%s.%s where name not like '\\_%'",
    R"(SELECT "ZONE", "SVR_TYPE", "SVR_IP", "SVR_PORT", "NAME", "DATA_TYPE", "VALUE", "INFO", "SECTION", "SCOPE", "SOURCE", "EDIT_LEVEL" FROM %s.%s WHERE NAME NOT LIKE '\\_%')",
    "name");
DEFINE_SHOW_CLAUSE_SET(SHOW_PARAMETERS_UNSYS, NULL,
    "SELECT 1 `gmt_create`, 1 `gmt_modified`, 1 `zone`, 1 `svr_type`, 1 `svr_ip`, 1 `svr_port`, 1 `name`, 1 "
    "`data_type`, 1 `value`, 1 `info`, 1 `section`, 1 `scope`, 1 `source`, 1 `edit_level` FROM (SELECT 1 FROM DUAL) "
    "tmp_table WHERE 1 != 1",
    R"(SELECT 1 "GMT_CREATE", 1 "GMT_MODIFIED", 1 "ZONE", 1 "SVR_TYPE", 1 "SVR_IP", 1 "SVR_PORT", 1 "NAME", 1 "DATA_TYPE", 1 "VALUE", 1 "INFO", 1 "SECTION", 1 "SCOPE" , 1 "SOURCE", 1 "EDIT_LEVEL" FROM (SELECT 1 FROM DUAL) TMP_TABLE WHERE 1 != 1)",
    "name");
DEFINE_SHOW_CLAUSE_SET(SHOW_PARAMETERS_COMPAT, NULL,
    "SELECT zone, svr_type, svr_ip, svr_port, name, data_type, value, info, section from %s.%s where name not like "
    "'\\_%'",
    R"(SELECT "ZONE", "SVR_TYPE", "SVR_IP", "SVR_PORT", "NAME", "DATA_TYPE", "VALUE", "INFO", "SECTION" FROM %s.%s WHERE NAME NOE LIKE '\\_%')",
    "name");
DEFINE_SHOW_CLAUSE_SET(SHOW_PARAMETERS_SEED, NULL,
    "SELECT zone, svr_type, svr_ip, svr_port, name, data_type, value, info, section from %s.%s where svr_ip = '%s' and "
    "svr_port = %ld ",
    R"(SELECT "ZONE", "SVR_TYPE", "SVR_IP", "SVR_PORT", "NAME", "DATA_TYPE", "VALUE", "INFO", "SECTION" FROM %s.%s WHERE "SVR_IP" = '%s' and "SVR_PORT" = %ld )",
    "name");
DEFINE_SHOW_CLAUSE_SET(SHOW_SESSION_STATUS, NULL,
    "select variable_name as Variable_name, variable_value as Value from %s.%s", NULL, "Variable_name");
DEFINE_SHOW_CLAUSE_SET(SHOW_GLOBAL_STATUS, NULL,
    "select variable_name as Variable_name, variable_value as Value from %s.%s", NULL, "Variable_name");
DEFINE_SHOW_CLAUSE_SET(
    SHOW_TENANT, NULL, "select  `tenant_name` as `Current_tenant_name` from %s.%s  where tenant_id = %ld", NULL, NULL);
DEFINE_SHOW_CLAUSE_SET(SHOW_TENANT_STATUS, NULL,
    "select tenant as `Tenant`, case when sum(read_only) = 0 then \'read write\' when sum(read_only) < "
    "count(read_only) then \'partially read only\' else \'read only\' end as `Status` from %s.%s  group by tenant",
    NULL, NULL);
DEFINE_SHOW_CLAUSE_SET(SHOW_CREATE_TENANT, NULL,
    "select  `tenant_name` as `Tenant`, `create_stmt` as `Create Tenant` from %s.%s  where tenant_id = %ld", NULL,
    NULL);
DEFINE_SHOW_CLAUSE_SET(SHOW_DATABASES, NULL,
    "SELECT `database_name` AS `Database` FROM %s.%s  WHERE tenant_id = %ld and in_recyclebin = 0 and database_name "
    "not in('%s', '%s', '%s') and 0 = sys_privilege_check(\'db_acc\', `tenant_id`, `database_name`, \'\')",
    NULL, "Database");
DEFINE_SHOW_CLAUSE_SET(SHOW_DATABASES_LIKE, "SELECT `Database` AS `Database (%.*s)` ",
    "SELECT `database_name` AS `Database` FROM %s.%s  WHERE tenant_id = %ld and in_recyclebin = 0 and database_name "
    "not in ('%s', '%s', '%s') and 0 = sys_privilege_check(\'db_acc\', `tenant_id`, `database_name`, \'\')",
    NULL, "Database");
DEFINE_SHOW_CLAUSE_SET(SHOW_DATABASES_STATUS, NULL,
    "select db as `Database`, case when sum(read_only) = 0 then \'read write\' when sum(read_only) < count(read_only) "
    "then \'partially read only\' else \'read only\' end as `Status` from %s.%s  group by db",
    NULL, "Database");
DEFINE_SHOW_CLAUSE_SET(SHOW_DATABASES_STATUS_LIKE, "SELECT `Database` AS `Database (%.*s)`, `Status` ",
    "select db as `Database`, case when sum(read_only) = 0 then \'read write\' when sum(read_only) < count(read_only) "
    "then \'partially read only\' else \'read only\' end as `Status` from %s.%s  group by db",
    NULL, "Database");
DEFINE_SHOW_CLAUSE_SET(SHOW_CREATE_TABLE, NULL,
    "SELECT table_name AS `Table`, create_table AS `Create Table` FROM %s.%s  WHERE table_id = %ld",
    R"(SELECT "TABLE_NAME" AS "TABLE", "CREATE_TABLE" AS "CREATE TABLE" FROM %s.%s  WHERE TABLE_ID = %ld)", NULL);
DEFINE_SHOW_CLAUSE_SET(SHOW_CREATE_VIEW, NULL,
    "SELECT table_name AS `View`, create_table AS `Create View` , character_set_client, collation_connection FROM "
    "%s.%s WHERE table_id = %ld",
    R"(SELECT "TABLE_NAME" AS "VIEW", "CREATE_TABLE" AS "CREATE VIEW" , "CHARACTER_SET_CLIENT", "COLLATION_CONNECTION" FROM %s.%s WHERE TABLE_ID = %ld)",
    NULL);
DEFINE_SHOW_CLAUSE_SET(SHOW_CREATE_PROCEDURE, NULL,
    "SELECT routine_name AS `Procedure`, sql_mode, create_routine AS `Create Procedure`, character_set_client, "
    "collation_connection, collation_database AS `Database Collation` FROM %s.%s  WHERE routine_id = %ld and proc_type "
    "= %ld",
    R"(SELECT "ROUTINE_NAME" AS "PROCEDURE", "SQL_MODE", "CREATE_ROUTINE" AS "CREATE PROCEDURE", "CHARACTER_SET_CLIENT", "COLLATION_CONNECTION", "COLLATION_DATABASE" AS "DATABASE COLLATION" FROM %s.%s  WHERE ROUTINE_ID = %ld AND PROC_TYPE = %ld)",
    NULL);
DEFINE_SHOW_CLAUSE_SET(SHOW_CREATE_FUNCTION, NULL,
    "SELECT routine_name AS `Function`, sql_mode, create_routine AS `Create Function`, character_set_client, "
    "collation_connection, collation_database AS `Database Collation` FROM %s.%s  WHERE routine_id = %ld and proc_type "
    "= %ld",
    R"(SELECT "ROUTINE_NAME" AS "FUNCTION", "SQL_MODE", "CREATE_ROUTINE" AS "CREATE FUNCTION", "CHARACTER_SET_CLIENT", "COLLATION_CONNECTION", "COLLATION_DATABASE" AS "DATABASE COLLATION" FROM %s.%s  WHERE ROUTINE_ID = %ld AND PROC_TYPE = %ld)",
    NULL);
DEFINE_SHOW_CLAUSE_SET(SHOW_RECYCLEBIN, "SELECT OBJECT_NAME, ORIGINAL_NAME, TYPE, CREATETIME",
    "SELECT OBJECT_NAME, ORIGINAL_NAME, case TYPE when 1 then 'TABLE' when 2 then 'INDEX' when 3 then 'VIEW' when 4 "
    "then 'DATABASE' when 5 then 'AUX_VP' when 6 then 'TRIGGER' when 7 then 'TENANT' else 'INVALID' end as TYPE, "
    "gmt_create as CREATETIME FROM %s.%s WHERE tenant_id = %lu AND TYPE != 7",
    R"(SELECT "OBJECT_NAME", "ORIGINAL_NAME", CASE "TYPE" WHEN 1 THEN 'TABLE' WHEN 2 THEN 'INDEX' WHEN 3 THEN 'VIEW' WHEN 4 THEN 'DATABASE' when 5 then 'AUX_VP' when 6 then 'TRIGGER' WHEN 7 THEN 'TENANT' ELSE 'INVALID' END AS "TYPE", "GMT_CREATE" AS "CREATETIME" FROM %s.%s WHERE TENANT_ID = %lu AND TYPE != 7)",
    NULL);
DEFINE_SHOW_CLAUSE_SET(SHOW_SYS_RECYCLEBIN, "SELECT OBJECT_NAME, ORIGINAL_NAME, TYPE, CREATETIME",
    "SELECT OBJECT_NAME, ORIGINAL_NAME, case TYPE when 1 then 'TABLE' when 2 then 'INDEX' when 3 then 'VIEW' when 4 "
    "then 'DATABASE' when 5 then 'AUX_VP' when 6 then 'TRIGGER' when 7 then 'TENANT' else 'INVALID' end as TYPE, "
    "gmt_create as CREATETIME FROM %s.%s where tenant_id = %lu OR TYPE = 7",
    NULL, NULL);
DEFINE_SHOW_CLAUSE_SET(SHOW_RESTORE_PREVIEW, NULL, "SELECT * FROM %s.%s", NULL, NULL);

}  // namespace sql
}  // namespace oceanbase
