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

#include "sql/resolver/ddl/ob_create_mlog_resolver.h"
#include "sql/resolver/ddl/ob_create_mlog_stmt.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/parser/ob_parser_utils.h"
#include "lib/json/ob_json_print_utils.h"
#include "storage/mview/ob_mview_sched_job_utils.h"
#include "sql/ob_sql_utils.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share::schema;
using namespace storage;
using namespace obrpc;
namespace sql
{
/*
CREATE MATERIALIZED VIEW LOG ON [ schema. ] table
  [ parallel_clause ]
  [ WITH [ { PRIMARY KEY
         | ROWID
         | SEQUENCE
         }
           [ { , PRIMARY KEY
             | , ROWID
             | , SEQUENCE
             }
           ]... ]
    (column [, column ]...)
    [ new_values_clause ]
  ] [ mv_log_purge_clause ]
;
*/
ObCreateMLogResolver::ObCreateMLogResolver(ObResolverParams &params)
    : ObDDLResolver(params),
      is_heap_table_(false)
{
}

int ObCreateMLogResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode &parse_node = const_cast<ParseNode &>(parse_tree);
  ObCreateMLogStmt *create_mlog_stmt = nullptr;
  uint64_t compat_version = 0;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;

  if (OB_UNLIKELY(T_CREATE_MLOG != parse_node.type_)
      || OB_UNLIKELY(ENUM_TOTAL_COUNT != parse_node.num_child_)
      || OB_ISNULL(parse_node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", KR(ret));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null session info", KR(ret), KP_(session_info));
  } else if (OB_FALSE_IT(tenant_id = session_info_->get_effective_tenant_id())) {
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("failed to get min data version", KR(ret), K(tenant_id));
  } else if (compat_version < DATA_VERSION_4_3_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("materialized view log before version 4.3 is not supported", KR(ret), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "materialized view log before version 4.3 is");
  } else if (OB_ISNULL(create_mlog_stmt = create_stmt<ObCreateMLogStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create create_mlog_stmt", KR(ret));
  } else {
    stmt_ = create_mlog_stmt;
  }

  if (OB_SUCC(ret)) {
    // resolve table name
    if (OB_ISNULL(parse_node.children_[ENUM_TABLE_NAME])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parse tree", KR(ret));
    } else if (OB_FAIL(resolve_table_name_node(parse_tree.children_[ENUM_TABLE_NAME],
                                               *create_mlog_stmt))) {
      LOG_WARN("failed to resolve table name", KR(ret));
    }
  }

  if (OB_SUCC(ret)) {
    // resolve table options
    ParseNode *table_options_node = parse_node.children_[ENUM_OPT_TABLE_OPTIONS];
    if (OB_NOT_NULL(table_options_node)) {
      if (OB_FAIL(resolve_table_option_node(table_options_node, *create_mlog_stmt))) {
        LOG_WARN("failed to resolve table options", KR(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    // resolve with clause
    ParseNode *with_options_node = parse_node.children_[ENUM_OPT_WITH];
    create_mlog_stmt->set_with_sequence(true);
    if (is_heap_table_) {
      create_mlog_stmt->set_with_primary_key(false);
      create_mlog_stmt->set_with_rowid(true);
    } else {
      create_mlog_stmt->set_with_primary_key(true);
      create_mlog_stmt->set_with_rowid(false);
    }
    if (OB_NOT_NULL(with_options_node)) {
      if (OB_FAIL(resolve_with_option_node(with_options_node, *create_mlog_stmt))) {
        LOG_WARN("failed to resolve with option node", KR(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    // resolve including / excluding new values
    ParseNode *new_values_node = parse_node.children_[ENUM_OPT_NEW_VALUES];
    create_mlog_stmt->set_include_new_values(true);
    if (OB_NOT_NULL(new_values_node)) {
      if (OB_FAIL(resolve_new_values_node(new_values_node, *create_mlog_stmt))) {
        LOG_WARN("failed to resolve new values node", KR(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    // resolve purge options
    ObCreateMLogArg &create_mlog_arg = create_mlog_stmt->get_create_mlog_arg();
    create_mlog_arg.purge_options_.purge_mode_ = ObMLogPurgeMode::IMMEDIATE_SYNC; // default
    ParseNode *purge_node = parse_node.children_[ENUM_OPT_PURGE];
    if (OB_NOT_NULL(purge_node)) {
      if (OB_FAIL(resolve_purge_node(purge_node, *create_mlog_stmt))) {
        LOG_WARN("failed to resolve purge node", KR(ret));
      }
    }
  }

  if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
    bool accessible = false;
    // check create table privilege
    OZ (schema_checker_->check_ora_ddl_priv(session_info_->get_effective_tenant_id(),
                                            session_info_->get_priv_user_id(),
                                            create_mlog_stmt->get_database_name(),
                                            stmt::T_CREATE_MLOG,
                                            session_info_->get_enable_role_array()),
        session_info_->get_effective_tenant_id(), session_info_->get_user_id(),
        create_mlog_stmt->get_database_name());
    // check access privileges to base table
    if (OB_SUCC(ret)) {
      OZ (schema_checker_->check_access_to_obj(session_info_->get_effective_tenant_id(),
                                              session_info_->get_priv_user_id(),
                                              create_mlog_stmt->get_data_table_id(),
                                              create_mlog_stmt->get_database_name(),
                                              stmt::T_CREATE_MLOG,
                                              session_info_->get_enable_role_array(),
                                              accessible));
      if (OB_SUCC(ret) && !accessible) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(create_mlog_stmt->get_database_name()),
                                          to_cstring(create_mlog_stmt->get_table_name()));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(fill_session_info_into_arg(*session_info_, *create_mlog_stmt))) {
      LOG_WARN("failed to fill session info into arg", KR(ret));
    }
  }

  return ret;
}

int ObCreateMLogResolver::fill_session_info_into_arg(
    const sql::ObSQLSessionInfo &session,
    ObCreateMLogStmt &create_mlog_stmt)
{
  int ret = OB_SUCCESS;
  ObCreateMLogArg &create_mlog_arg = create_mlog_stmt.get_create_mlog_arg();
  create_mlog_arg.nls_date_format_ = session.get_local_nls_date_format();
  create_mlog_arg.nls_timestamp_format_ = session.get_local_nls_timestamp_format();
  create_mlog_arg.nls_timestamp_tz_format_ = session.get_local_nls_timestamp_tz_format();

  char buf[OB_MAX_PROC_ENV_LENGTH];
  int64_t pos = 0;
  if (allocator_ == nullptr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator_ is null", KR(ret));
  }
  OZ (ObExecEnv::gen_exec_env(session, buf, OB_MAX_PROC_ENV_LENGTH, pos));
  OX (create_mlog_arg.purge_options_.exec_env_.assign(buf, pos));
  OZ (ob_write_string(*allocator_, create_mlog_arg.purge_options_.exec_env_, create_mlog_arg.purge_options_.exec_env_));

  return ret;
}

// simple checking
bool ObCreateMLogResolver::is_column_exists(
    ObIArray<ObString> &column_name_array,
    const ObString &column_name)
{
  bool bret = false;
  for (int64_t i = 0; !bret && (i < column_name_array.count()); ++i) {
    if (0 == column_name_array.at(i).compare(column_name)) {
      bret = true;
    }
  }
  return bret;
}

int ObCreateMLogResolver::resolve_table_name_node(
    ParseNode *table_name_node,
    ObCreateMLogStmt &create_mlog_stmt)
{
  int ret = OB_SUCCESS;
  bool has_synonym = false;
  ObString database_name;
  ObString data_table_name;
  ObString new_db_name;
  ObString new_tbl_name;
  ObString mlog_table_name;
  bool table_exist = false;
  const ObTableSchema *data_table_schema = nullptr;
  uint64_t tenant_id = session_info_->get_effective_tenant_id();
  ObNameCaseMode mode = OB_NAME_CASE_INVALID;
  ObCollationType cs_type = CS_TYPE_INVALID;
  if (OB_ISNULL(table_name_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid table option node", KR(ret), KP(table_name_node));
  } else if (OB_FAIL(resolve_table_relation_node(table_name_node, data_table_name, database_name))) {
    LOG_WARN("failed to resolve table name",
        KR(ret), K(data_table_name), K(database_name));
  } else if (OB_FAIL(set_database_name(database_name))) {
    LOG_WARN("failed to set database name", KR(ret), K(database_name));
  } else if (OB_FAIL(schema_checker_->get_table_schema_with_synonym(
      session_info_->get_effective_tenant_id(),
      database_name,
      data_table_name,
      false/*is index table*/,
      has_synonym,
      new_db_name,
      new_tbl_name,
      data_table_schema))) {
    if (OB_TABLE_NOT_EXIST == ret) {
      LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(database_name), to_cstring(data_table_name));
      LOG_WARN("table not exist", KR(ret), K(database_name), K(data_table_name));
    } else {
      LOG_WARN("failed to get table schema", KR(ret));
    }
  } else if (OB_ISNULL(data_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("data table schema is null", KR(ret));
  } else if(!data_table_schema->is_user_table()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("create materialized view log on a non-user table is not supported",
        KR(ret), K(data_table_schema->get_table_type()));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "create materialized view log on a non-user table is");
  } else if (data_table_schema->has_mlog_table()) {
    ret = OB_ERR_MLOG_EXIST;
    LOG_WARN("a materialized view log already exists on table",
        K(data_table_name), K(data_table_schema->get_mlog_tid()));
    LOG_USER_ERROR(OB_ERR_MLOG_EXIST, to_cstring(data_table_name));
  } else if (OB_FAIL(ObTableSchema::build_mlog_table_name(
      *allocator_, data_table_name, mlog_table_name, lib::is_oracle_mode()))) {
    LOG_WARN("failed to build mlog table name", KR(ret), K(data_table_name));
  } else if (OB_FAIL(session_info_->get_name_case_mode(mode))) {
    LOG_WARN("failed to get name case mode", KR(ret));
  } else if (OB_FAIL(session_info_->get_collation_connection(cs_type))) {
    LOG_WARN("failed to get collation connection", KR(ret));
  } else if (OB_FAIL(ObSQLUtils::check_and_convert_table_name(
      cs_type,
      lib::is_oracle_mode() ? true : (OB_LOWERCASE_AND_INSENSITIVE != mode)/*perserve_lettercase*/,
      mlog_table_name))) {
    LOG_WARN("failed to check and convert table name",
        KR(ret), K(cs_type), K(mode), K(mlog_table_name));
  } else if (OB_FAIL(schema_checker_->check_table_exists(tenant_id,
                                                         database_name,
                                                         mlog_table_name,
                                                         false /*is_index*/,
                                                         false /*is_hidden*/,
                                                         table_exist))) {
    LOG_WARN("failed to check table exists", KR(ret), K(database_name), K(mlog_table_name));
  } else if (table_exist) {
    ret = OB_ERR_TABLE_EXIST;
    LOG_WARN("table already exist", KR(ret), K(mlog_table_name), K(table_exist));
    LOG_USER_ERROR(OB_ERR_TABLE_EXIST, mlog_table_name.length(), mlog_table_name.ptr());
  }

  if (OB_SUCC(ret)) {
    ObString tmp_new_db_name;
    ObString tmp_new_tbl_name;
    if (!has_synonym) {
      new_db_name.assign_ptr(database_name.ptr(), database_name.length());
      new_tbl_name.assign_ptr(data_table_name.ptr(), data_table_name.length());
    }
    if (OB_FAIL(deep_copy_str(new_db_name, tmp_new_db_name))) {
      LOG_WARN("failed to deep copy new_db_name", KR(ret));
    } else if (OB_FAIL(deep_copy_str(new_tbl_name, tmp_new_tbl_name))) {
      LOG_WARN("failed to deep copy new_tbl_name", KR(ret));
    } else {
      create_mlog_stmt.set_database_name(tmp_new_db_name);
      create_mlog_stmt.set_table_name(tmp_new_tbl_name);
      create_mlog_stmt.set_mlog_name(mlog_table_name);
      create_mlog_stmt.set_tenant_id(tenant_id);
      create_mlog_stmt.set_data_table_id(data_table_schema->get_table_id());
      create_mlog_stmt.set_name_generated_type(GENERATED_TYPE_SYSTEM);
      is_heap_table_ = data_table_schema->is_heap_table();
    }
  }

  if (OB_SUCC(ret)) {
    ObBasedSchemaObjectInfo based_info;
    based_info.schema_id_ = data_table_schema->get_table_id();
    based_info.schema_type_ = ObSchemaType::TABLE_SCHEMA;
    based_info.schema_version_ = data_table_schema->get_schema_version();
    based_info.schema_tenant_id_ = tenant_id;
    if (OB_FAIL(create_mlog_stmt.get_create_mlog_arg().based_schema_object_infos_.push_back(based_info))) {
      LOG_WARN("fail to push back base info", KR(ret));
    }
  }
  return ret;
}

int ObCreateMLogResolver::resolve_table_option_node(
    ParseNode *table_option_node,
    ObCreateMLogStmt &create_mlog_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_option_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid table option node", KR(ret), K(table_option_node));
  } else if (OB_FAIL(resolve_table_options(table_option_node, true/*is_index_option*/))) {
    LOG_WARN("failed to resolve table options", KR(ret));
  } else {
    ObCreateMLogArg &create_mlog_arg = create_mlog_stmt.get_create_mlog_arg();
    create_mlog_arg.mlog_schema_.set_block_size(block_size_);
    create_mlog_arg.mlog_schema_.set_pctfree(pctfree_);
    create_mlog_arg.mlog_schema_.set_dop(table_dop_);
    create_mlog_arg.mlog_schema_.set_tablespace_id(tablespace_id_);
    create_mlog_arg.mlog_schema_.set_comment(comment_);
  }
  return ret;
}

int ObCreateMLogResolver::resolve_with_option_node(
    ParseNode *with_options_node,
    ObCreateMLogStmt &create_mlog_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(with_options_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid with option node", KR(ret), K(with_options_node));
  } else if (T_MLOG_WITH != with_options_node->type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to resolve with clause", KR(ret), K(with_options_node));
  } else if (1 != with_options_node->num_child_
              || OB_ISNULL(with_options_node->children_)
              || OB_ISNULL(with_options_node->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid child node", KR(ret), K(with_options_node));
  } else {
    ParseNode *with_column_node = with_options_node->children_[0];
    if (T_MLOG_WITH_VALUES != with_column_node->type_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to resolve with clause", KR(ret), K(with_column_node));
    } else {
      if ((2 != with_column_node->num_child_)
          || OB_ISNULL(with_column_node->children_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid child node", KR(ret), K(with_column_node));
      } else {
        ParseNode *special_column_list_node = with_column_node->children_[0];
        ParseNode *table_column_list_node = with_column_node->children_[1];
        if (OB_NOT_NULL(special_column_list_node)
            && OB_FAIL(resolve_special_columns_node(special_column_list_node, create_mlog_stmt))) {
          LOG_WARN("failed to resolve special columns node", KR(ret));
        } else if (OB_NOT_NULL(table_column_list_node)
            && OB_FAIL(resolve_reference_columns_node(table_column_list_node, create_mlog_stmt))) {
          LOG_WARN("failed to resolve reference columns node", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObCreateMLogResolver::resolve_special_columns_node(
    ParseNode *special_column_list_node,
    ObCreateMLogStmt &create_mlog_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(special_column_list_node)
      || (T_MLOG_WITH_SPECIAL_COLUMN_LIST != special_column_list_node->type_)
      || (1 != special_column_list_node->num_child_)
      || OB_ISNULL(special_column_list_node->children_)
      || OB_ISNULL(special_column_list_node->children_[0])){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid special column list node", KR(ret), K(special_column_list_node));
  } else {
    ParseNode *special_columns_node = special_column_list_node->children_[0];
    while (OB_SUCC(ret)
           && T_MLOG_WITH_SPECIAL_COLUMN == special_columns_node->type_) {
      if (2 != special_columns_node->num_child_
          || OB_ISNULL(special_columns_node->children_)
          || OB_ISNULL(special_columns_node->children_[0])
          || OB_ISNULL(special_columns_node->children_[1])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid child node", KR(ret), K(special_columns_node));
      } else {
        ParseNode *special_column_node = special_columns_node->children_[0];
        if (OB_FAIL(resolve_special_column_node(special_column_node, create_mlog_stmt))) {
          LOG_WARN("failed to resolve special column node", KR(ret));
        } else {
          special_columns_node = special_columns_node->children_[1];
        }
      }
    } // end while
    if (OB_SUCC(ret) && OB_FAIL(resolve_special_column_node(
        special_columns_node, create_mlog_stmt))) {
      LOG_WARN("failed to resolve special column node", KR(ret));
    }
  }
  return ret;
}

int ObCreateMLogResolver::resolve_special_column_node(
    ParseNode *special_column_node,
    ObCreateMLogStmt &create_mlog_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(special_column_node)
      || (0 != special_column_node->num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid special column node", KR(ret), K(special_column_node));
  } else {
    switch (special_column_node->type_) {
      case T_MLOG_WITH_PRIMARY_KEY:
        if (!is_heap_table_) {
          create_mlog_stmt.set_with_primary_key(true);
        }
        break;
      case T_MLOG_WITH_ROWID:
        if (is_heap_table_) {
          create_mlog_stmt.set_with_rowid(true);
        }
        break;
      case T_MLOG_WITH_SEQUENCE:
        create_mlog_stmt.set_with_sequence(true);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid child node", KR(ret), K(special_column_node->type_));
        break;
    }
  }
  return ret;
}

int ObCreateMLogResolver::resolve_reference_columns_node(
    ParseNode *ref_column_list_node,
    ObCreateMLogStmt &create_mlog_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ref_column_list_node)
      || (T_COLUMN_LIST != ref_column_list_node->type_)
      || (1 != ref_column_list_node->num_child_)
      || OB_ISNULL(ref_column_list_node->children_)
      || OB_ISNULL(ref_column_list_node->children_[0])){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ref column list node", KR(ret), K(ref_column_list_node));
  } else {
    ParseNode *ref_columns_node = ref_column_list_node->children_[0];
    while (OB_SUCC(ret)
           && T_MLOG_WITH_REFERENCE_COLUMN == ref_columns_node->type_) {
      if (2 != ref_columns_node->num_child_
          || OB_ISNULL(ref_columns_node->children_)
          || OB_ISNULL(ref_columns_node->children_[0])
          || OB_ISNULL(ref_columns_node->children_[1])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid child node", KR(ret), K(ref_columns_node));
      } else {
        ParseNode *ref_column_node = ref_columns_node->children_[0];
        if (OB_FAIL(resolve_reference_column_node(ref_column_node, create_mlog_stmt))) {
          LOG_WARN("failed to resolve reference column node", KR(ret));
        } else {
          ref_columns_node = ref_columns_node->children_[1];
        }
      }
    } // end while
    if (OB_SUCC(ret) && OB_FAIL(resolve_reference_column_node(
        ref_columns_node, create_mlog_stmt))) {
      LOG_WARN("failed to resolve reference column node", KR(ret));
    }
  }
  return ret;
}

int ObCreateMLogResolver::resolve_reference_column_node(
    ParseNode *ref_column_node,
    ObCreateMLogStmt &create_mlog_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ref_column_node)
      || (0 != ref_column_node->num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ref column node", KR(ret), K(ref_column_node));
  } else {
    ObCreateMLogArg &create_mlog_arg = create_mlog_stmt.get_create_mlog_arg();
    ObString column_name(ref_column_node->str_len_, ref_column_node->str_value_);
    if (is_column_exists(create_mlog_arg.store_columns_, column_name)) {
      ret = OB_ERR_COLUMN_DUPLICATE;
      LOG_USER_ERROR(OB_ERR_COLUMN_DUPLICATE, column_name.length(), column_name.ptr());
    } else if (OB_FAIL(create_mlog_arg.store_columns_.push_back(column_name))) {
      LOG_WARN("failed to push back column name to store columns", KR(ret));
    }
  }
  return ret;
}

int ObCreateMLogResolver::resolve_new_values_node(
    ParseNode *new_values_node,
    ObCreateMLogStmt &create_mlog_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(new_values_node)
      || (T_MLOG_NEW_VALUES != new_values_node->type_)
      || (1 != new_values_node->num_child_)
      || OB_ISNULL(new_values_node->children_)
      || OB_ISNULL(new_values_node->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid new values node", KR(ret), K(new_values_node));
  } else {
    ParseNode *value_node = new_values_node->children_[0];
    if (T_MLOG_INCLUDING_NEW_VALUES == value_node->type_) {
      create_mlog_stmt.set_include_new_values(true);
    } else if (T_MLOG_EXCLUDING_NEW_VALUES == value_node->type_) {
      create_mlog_stmt.set_include_new_values(false);
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("create materialized view log excluding new values is not supported", KR(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "create materialized view log excluding new values is");
    }
  }
  return ret;
}

int ObCreateMLogResolver::resolve_purge_node(
    ParseNode *purge_node,
    ObCreateMLogStmt &create_mlog_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(purge_node)
      || (T_MLOG_PURGE != purge_node->type_)
      || (1 != purge_node->num_child_)
      || OB_ISNULL(purge_node->children_)
      || OB_ISNULL(purge_node->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid purge node", KR(ret), K(purge_node));
  } else {
    ParseNode *purge_type_node = purge_node->children_[0];
    switch (purge_type_node->type_) {
      case T_MLOG_PURGE_IMMEDIATE:
        if (1 != purge_type_node->num_child_
            || OB_ISNULL(purge_type_node->children_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid child node", KR(ret), K(purge_type_node));
        } else {
          if (OB_ISNULL(purge_type_node->children_[0])) {
            // use default value
            create_mlog_stmt.set_purge_mode(ObMLogPurgeMode::IMMEDIATE_SYNC);
          } else {
            ParseNode *purge_immediate_node = purge_type_node->children_[0];
            if (T_MLOG_PURGE_IMMEDIATE_SYNC != purge_immediate_node->type_
                && T_MLOG_PURGE_IMMEDIATE_ASYNC != purge_immediate_node->type_) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid child node", KR(ret), K(purge_immediate_node->type_));
            } else {
              if (T_MLOG_PURGE_IMMEDIATE_SYNC == purge_immediate_node->type_) {
                create_mlog_stmt.set_purge_mode(ObMLogPurgeMode::IMMEDIATE_SYNC);
              } else {
                ret = OB_NOT_SUPPORTED;
                LOG_WARN("create materialized view log purge immediate asynchronous is not supported", KR(ret));
                LOG_USER_ERROR(OB_NOT_SUPPORTED,
                    "create materialized view log purge immediate asynchronous is");
              }
            }
          }
        }
        break;
      case T_MLOG_PURGE_START_NEXT:
        if (2 != purge_type_node->num_child_
            || OB_ISNULL(purge_type_node->children_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid child node", KR(ret), K(purge_type_node));
        } else {
          ParseNode *purge_start_node = purge_type_node->children_[0];
          ParseNode *purge_next_node = purge_type_node->children_[1];
          if (OB_FAIL(resolve_purge_start_next_node(purge_start_node, purge_next_node, create_mlog_stmt))) {
            LOG_WARN("failed to resolve purge start next node", KR(ret));
          }
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid child type", KR(ret), K(purge_type_node->type_));
        break;
    }
  }
  return ret;
}

int ObCreateMLogResolver::resolve_purge_start_next_node(
    ParseNode *purge_start_node,
    ParseNode *purge_next_node,
    ObCreateMLogStmt &create_mlog_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(purge_start_node) || OB_NOT_NULL(purge_next_node)) {
    int64_t current_time = ObTimeUtility::current_time() / 1000000L * 1000000L; // ignore micro seconds
    int64_t start_time = OB_INVALID_TIMESTAMP;
    ObCreateMLogArg &create_mlog_arg = create_mlog_stmt.get_create_mlog_arg();

    if (OB_NOT_NULL(purge_start_node)
        && (T_MLOG_PURGE_START_TIME_EXPR == purge_start_node->type_)
        && (1 == purge_start_node->num_child_)
        && OB_NOT_NULL(purge_start_node->children_)
        && OB_NOT_NULL(purge_start_node->children_[0])) {
      if (OB_FAIL(ObMViewSchedJobUtils::resolve_date_expr_to_timestamp(params_,
          *session_info_, *(purge_start_node->children_[0]), *allocator_, start_time))) {
        LOG_WARN("failed to resolve date expr to timestamp", KR(ret));
      } else if (start_time < current_time) {
        ret = OB_ERR_TIME_EARLIER_THAN_SYSDATE;
        LOG_WARN("the parameter start date must evaluate to a time in the future",
            KR(ret), K(current_time), K(start_time));
        LOG_USER_ERROR(OB_ERR_TIME_EARLIER_THAN_SYSDATE, "start date");
      }
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(purge_next_node)) {
      int64_t next_time = OB_INVALID_TIMESTAMP;
      if (OB_FAIL(ObMViewSchedJobUtils::resolve_date_expr_to_timestamp(params_,
          *session_info_, *purge_next_node, *allocator_, next_time))) {
        LOG_WARN("failed to resolve date expr to timestamp", KR(ret));
      } else if (next_time < current_time) {
        ret = OB_ERR_TIME_EARLIER_THAN_SYSDATE;
        LOG_WARN("the parameter next date must evaluate to a time in the future",
            KR(ret), K(current_time), K(next_time));
        LOG_USER_ERROR(OB_ERR_TIME_EARLIER_THAN_SYSDATE, "next date");
      } else if (OB_INVALID_TIMESTAMP == start_time) {
        start_time = next_time;
      }

      if (OB_SUCC(ret)) {
        ObString next_date_str(purge_next_node->str_len_, purge_next_node->str_value_);
        if (OB_FAIL(ob_write_string(*allocator_, next_date_str,
            create_mlog_arg.purge_options_.next_datetime_expr_))) {
          LOG_WARN("fail to write string", KR(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      create_mlog_arg.purge_options_.start_datetime_expr_.set_timestamp(start_time);
      create_mlog_stmt.set_purge_mode(ObMLogPurgeMode::DEFERRED);
    }
  }
  return ret;
}
} // sql
} // oceanbase
