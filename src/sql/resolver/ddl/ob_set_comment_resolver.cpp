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
#include "sql/resolver/ddl/ob_set_comment_resolver.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_service.h"

namespace oceanbase
{
using namespace common;

namespace sql
{
ObSetCommentResolver::ObSetCommentResolver(ObResolverParams &params)
    : ObDDLResolver(params),
      table_schema_(NULL),
      collation_type_(CS_TYPE_INVALID),
      charset_type_(CHARSET_INVALID)
{
}

ObSetCommentResolver::~ObSetCommentResolver()
{
}

int ObSetCommentResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  CHECK_COMPATIBILITY_MODE(session_info_);
  if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "session_info should not be null", K(ret));
  } else if (OB_ISNULL(parse_tree.children_)
      || ((T_SET_TABLE_COMMENT != parse_tree.type_)
         && (T_SET_COLUMN_COMMENT != parse_tree.type_))) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
  } else if (!lib::is_oracle_mode()) {
    // do-nothing for non-oracle mode
  } else {
    const uint64_t tenant_id = session_info_->get_effective_tenant_id();
    ObString database_name;
    ObString table_name;
    ObString col_name;
    ObAlterTableStmt *alter_table_stmt = NULL;
    const share::schema::ObTableSchema *table_schema = NULL;
    if (NULL == (alter_table_stmt = create_stmt<ObAlterTableStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SQL_RESV_LOG(ERROR, "failed to create alter table stmt", K(ret));
    } else if (OB_FAIL(alter_table_stmt->set_tz_info_wrap(session_info_->get_tz_info_wrap()))) {
      LOG_WARN("failed to set_tz_info_wrap", "tz_info_wrap", session_info_->get_tz_info_wrap(), K(ret));
    } else if (OB_FAIL(alter_table_stmt->set_nls_formats(session_info_->get_local_nls_date_format(),
        session_info_->get_local_nls_timestamp_format(),
        session_info_->get_local_nls_timestamp_tz_format()))) {
      SQL_RESV_LOG(WARN, "failed to set_nls_formats", K(ret));
    } else {
      stmt_ = alter_table_stmt;
      alter_table_stmt->set_tenant_id(tenant_id);
      alter_table_stmt->set_is_comment_table(true);
    }
    
    // resolve string_value
    if (OB_FAIL(ret)) {
      // do-nothing
    } else if (OB_ISNULL(parse_tree.children_[1])) {
      ret = OB_ERR_UNEXPECTED;
      SQL_RESV_LOG(WARN, "invalid parse tree for comment string", K(ret));
    } else {
      int64_t comment_length = 0;
      const char *comment_ptr = NULL;
      comment_length = parse_tree.children_[1]->str_len_;
      comment_ptr = parse_tree.children_[1]->str_value_;
      if (T_SET_TABLE_COMMENT == parse_tree.type_ && comment_length <= MAX_ORACLE_COMMENT_LENGTH) {
        comment_.assign_ptr((char *)(comment_ptr), static_cast<int32_t>(comment_length));
      } else if (T_SET_COLUMN_COMMENT == parse_tree.type_ && comment_length <= MAX_ORACLE_COMMENT_LENGTH) {
        comment_.assign_ptr((char *)(comment_ptr), static_cast<int32_t>(comment_length));
      } else {
        comment_ = "";
        if (T_SET_TABLE_COMMENT == parse_tree.type_) {
          ret = OB_ERR_TOO_LONG_TABLE_COMMENT;
          LOG_USER_ERROR(OB_ERR_TOO_LONG_TABLE_COMMENT, MAX_ORACLE_COMMENT_LENGTH);
        } else {
          ret = OB_ERR_TOO_LONG_FIELD_COMMENT;
          LOG_USER_ERROR(OB_ERR_TOO_LONG_FIELD_COMMENT, MAX_ORACLE_COMMENT_LENGTH);
        }
      }
      OZ (ObSQLUtils::convert_sql_text_to_schema_for_storing(
            *allocator_, session_info_->get_dtc_params(), comment_));
    }
    
    if (OB_SUCC(ret)) {
      if (T_SET_TABLE_COMMENT == parse_tree.type_) {
        // COMMENT ON TABLE 
        bool is_exists = false;
        uint64_t compat_version = OB_INVALID_VERSION;
        if (2 != parse_tree.num_child_) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "invalid parse tree num", K(ret), K(parse_tree.num_child_));
        } else if (OB_ISNULL(parse_tree.children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
        } else if (OB_FAIL(resolve_table_relation_node(parse_tree.children_[0],
                                                       table_name,
                                                       database_name))) {
          SQL_RESV_LOG(WARN, "failed to resolve table name.", K(table_name), K(database_name), K(ret));
        } else if (OB_FAIL(get_table_schema(parse_tree.children_[0]->children_[0],
                                            tenant_id,
                                            database_name,
                                            table_name,
                                            table_schema))) {
          SQL_RESV_LOG(WARN, "failed to get table schema", K(table_name), K(database_name), K(ret));
        } else if (OB_ISNULL(table_schema)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "table schema is null", K(ret), K(database_name));
        } else if (OB_FAIL(GET_MIN_DATA_VERSION(session_info_->get_effective_tenant_id(),
                                                compat_version))) {
          LOG_WARN("get min data_version failed", K(ret), K(session_info_->get_effective_tenant_id()));
        } else if (!sql::ObSQLUtils::is_data_version_ge_422_or_431(compat_version) && table_schema->is_view_table()) {
          ret = OB_ERR_WRONG_OBJECT;
          LOG_USER_ERROR(OB_ERR_WRONG_OBJECT, to_cstring(database_name), to_cstring(table_name),
                         "BASE TABLE");
          LOG_WARN("version before 4.3.1 or 4.2.2 not support comment on view", K(ret));
        } else {
          alter_table_stmt->set_table_id(table_schema->get_table_id());
        }
      } else if (T_SET_COLUMN_COMMENT == parse_tree.type_) {
        // COMMENT ON COLUMN
        // resolve dabase_name
        if (2 != parse_tree.num_child_) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "invalid parse tree num", K(ret), K(parse_tree.num_child_));
        } else if (OB_ISNULL(parse_tree.children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "invalid parse tree", K(ret));
        } else {
          bool is_exists = false;
          ObQualifiedName column_ref;
          ObNameCaseMode case_mode = OB_NAME_CASE_INVALID;
          ParseNode *column_ref_node = parse_tree.children_[0];
          uint64_t compat_version = OB_INVALID_VERSION;
          if (OB_UNLIKELY(T_COLUMN_REF != column_ref_node->type_)) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "node type is not T_COLUMN_LIST", K(ret), K(column_ref_node->type_));
          } else if (OB_FAIL(session_info_->get_name_case_mode(case_mode))) {
            SQL_RESV_LOG(WARN, "fail to get name case mode", K(ret));
          } else if (OB_FAIL(ObResolverUtils::resolve_column_ref(column_ref_node, case_mode, column_ref))) {
            SQL_RESV_LOG(WARN, "failed to resolve column def", K(ret));
          } else {
            table_name = column_ref.tbl_name_;
            col_name = column_ref.col_name_;
            if (column_ref.tbl_name_.empty() || column_ref.col_name_.empty()) {
              ret = OB_ERR_PARSER_SYNTAX;
              SQL_RESV_LOG(WARN, "syntax error", K(ret));
            } else if (column_ref.database_name_.empty()) {
              if (session_info_->get_database_name().empty()) {
                ret = OB_ERR_NO_DB_SELECTED;
                SQL_RESV_LOG(WARN, "No database selected");
              } else {
                database_name = session_info_->get_database_name();
              }
            } else {
              database_name = column_ref.database_name_;
            }
          }

          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(get_table_schema(column_ref_node->children_[0],
                                              tenant_id,
                                              database_name,
                                              table_name,
                                              table_schema))) {
            SQL_RESV_LOG(WARN, "failed to get table schema", K(table_name), K(database_name), K(ret));
          } else if (OB_ISNULL(table_schema)) {
            ret = OB_ERR_UNEXPECTED;
            SQL_RESV_LOG(WARN, "table schema is null", K(ret), K(database_name));
          } else if (OB_FAIL(GET_MIN_DATA_VERSION(session_info_->get_effective_tenant_id(),
                                                  compat_version))) {
            LOG_WARN("get min data_version failed", K(ret), K(session_info_->get_effective_tenant_id()));
          } else if (!sql::ObSQLUtils::is_data_version_ge_422_or_431(compat_version) && table_schema->is_view_table()) {
            ret = OB_ERR_WRONG_OBJECT;
            LOG_USER_ERROR(OB_ERR_WRONG_OBJECT, to_cstring(database_name), to_cstring(table_name),
                           "BASE TABLE");
            LOG_WARN("version before 4.3.1 or 4.2.2 not support comment on column of view", K(ret));
          } else if (OB_FAIL(schema_checker_->check_column_exists(tenant_id,
                                                                  table_schema->get_table_id(),
                                                                  col_name,
                                                                  is_exists))) {
            SQL_RESV_LOG(WARN, "failed to check_column_exists", K(ret), K(table_schema), K(col_name));
          } else if (!is_exists) {
            ret = OB_ERR_COLUMN_NOT_FOUND;
            SQL_RESV_LOG(WARN, "column doesn't exist", K(ret), K(col_name));
          } else {
            alter_table_stmt->set_table_id(table_schema->get_table_id());
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "invalid parse tree", K(ret), K(parse_tree.type_));
      }
    }
    if (OB_SUCC(ret)) {
      if (T_SET_TABLE_COMMENT == parse_tree.type_) {
        share::schema::AlterTableSchema &alter_table_schema = 
          alter_table_stmt->get_alter_table_arg().alter_table_schema_;
        if (OB_FAIL(alter_table_schema.set_comment(comment_))) {
          SQL_RESV_LOG(WARN, "Write comment_ to alter_table_schema failed!", K(ret));
        } else if (OB_FAIL(alter_table_bitset_.add_member(obrpc::ObAlterTableArg::COMMENT))) {
          SQL_RESV_LOG(WARN, "failed to add member to bitset!", K(ret));
        } else {
          alter_table_schema.alter_option_bitset_ = alter_table_bitset_;
        }

        if (OB_FAIL(ret)) {
          alter_table_schema.reset();
          SQL_RESV_LOG(WARN, "Set table options error!", K(ret));
        }
      } else if (T_SET_COLUMN_COMMENT == parse_tree.type_) {
        const share::schema::ObColumnSchemaV2 *column_schema = NULL;
        if (OB_FAIL(schema_checker_->get_column_schema(
            table_schema->get_tenant_id(),
            table_schema->get_table_id(),
            col_name,
            column_schema,
            false))) {
          SQL_RESV_LOG(WARN, "fail to get origin column schema", K(ret));
        } else if (OB_ISNULL(column_schema)) {
          ret = OB_ERR_UNEXPECTED;
          SQL_RESV_LOG(WARN, "column schema is NULL", K(ret));
        } else {
          share::schema::AlterColumnSchema alter_column_schema;
          alter_column_schema.assign(*column_schema);
          alter_column_schema.set_comment(comment_);
          alter_table_stmt->set_alter_table_column();
          alter_column_schema.alter_type_ = share::schema::OB_DDL_MODIFY_COLUMN;
          alter_column_schema.is_set_comment_ = true;
          if (OB_FAIL(alter_column_schema.set_origin_column_name(col_name))) {
            SQL_RESV_LOG(WARN, "failed to set origin column name", K(col_name), K(ret));
          } else if (OB_FAIL(alter_table_stmt->add_column(alter_column_schema))) {
            SQL_RESV_LOG(WARN, "Add alter column schema failed!", K(ret));
          }
          
          if (OB_FAIL(ret)) {
            alter_column_schema.reset();
            SQL_RESV_LOG(WARN, "Set column options error!", K(ret));
          }
        } 
      } else {
        ret = OB_ERR_UNEXPECTED;
        SQL_RESV_LOG(WARN, "Unkonwn operation type", K(ret), K(parse_tree.type_));
      }
        
      if (OB_SUCC(ret)) {
        if (OB_FAIL(alter_table_stmt->set_origin_database_name(database_name))) {
          SQL_RESV_LOG(WARN, "failed to set origin database name", K(ret));
        } else if (OB_FAIL(alter_table_stmt->set_origin_table_name(table_name))) {
          SQL_RESV_LOG(WARN, "failed to set origin table name", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSetCommentResolver::get_table_schema(const ParseNode *db_node,
                                           const uint64_t tenant_id,
                                           ObString &database_name,
                                           ObString &table_name,
                                           const ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool has_synonym = false;
  ObString new_db_name;
  ObString new_tbl_name;
  if (OB_FAIL(schema_checker_->get_table_schema_with_synonym(tenant_id,
                                                             database_name,
                                                             table_name,
                                                             false/*not index table*/,
                                                             has_synonym,
                                                             new_db_name,
                                                             new_tbl_name,
                                                             table_schema))) {
    if (OB_ERR_BAD_DATABASE == ret) {
      ret = OB_TABLE_NOT_EXIST; // oracle cmpt
      LOG_WARN("database not exist", K(ret), K(database_name), K(table_name));
    } else if (OB_TABLE_NOT_EXIST != ret) {
      LOG_WARN("failed to get table schema", K(ret), K(database_name), K(table_name));
    } else if (NULL == db_node && ObSQLUtils::is_oracle_sys_view(table_name)) {
      if (OB_SUCCESS != (tmp_ret = ob_write_string(*allocator_,
                                                   OB_ORA_SYS_SCHEMA_NAME,
                                                   database_name))) {
        LOG_WARN("fail to write db name", K(ret), K(tmp_ret));
      } else if (OB_FAIL(schema_checker_->get_table_schema_with_synonym(tenant_id,
                                                                        database_name,
                                                                        table_name,
                                                                        false/*not index table*/,
                                                                        has_synonym,
                                                                        new_db_name,
                                                                        new_tbl_name,
                                                                        table_schema))) {
        LOG_WARN("failed to get sys view schema", K(ret), K(database_name), K(table_name));
      }
    }
  }
  if (OB_SUCC(ret) && has_synonym) {
    database_name = new_db_name;
    table_name = new_tbl_name;
  }
  return ret;
}

} //namespace sql
} //namespace oceanbase
