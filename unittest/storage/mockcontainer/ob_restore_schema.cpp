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

#include <fstream>
#include <iterator>
#include "lib/stat/ob_session_stat.h"
#define private public
#include "lib/utility/ob_test_util.h"
#include "sql/parser/ob_parser.h"
#include "sql/resolver/ob_resolver.h"
#include "lib/allocator/page_arena.h"
#include "lib/json/ob_json_print_utils.h"  // for SJ
#include "sql/resolver/ob_schema_checker.h"
#include "sql/resolver/ddl/ob_create_table_stmt.h"
#include "sql/resolver/ddl/ob_create_index_stmt.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/system_variable/ob_system_variable.h"
#include "ob_restore_schema.h"
#include "../../share/schema/mock_schema_service.h"
//#include <iostream>
//#include <string>

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

ObRestoreSchema::ObRestoreSchema()
    : schema_service_(nullptr), table_id_(0), tenant_id_(0), database_id_(0)
{}

//ObSchemaManager *ObRestoreSchema::get_schema_manager()
//{
//  return &schema_manager_;
//}

int ObRestoreSchema::init()
{
  int ret = OB_SUCCESS;

  schema_service_ = new MockSchemaService();
  if (OB_FAIL(schema_service_->init())) {
    STORAGE_LOG(WARN, "schema_service init fail", K(ret));
  } else if (OB_FAIL(schema_service_->get_schema_guard(schema_guard_, INT64_MAX))) {
    STORAGE_LOG(WARN, "schema_guard init fail", K(ret));
  } else {
    table_id_ = 3001;
    tenant_id_ = OB_SYS_TENANT_ID;
    database_id_ = OB_SYS_TABLEGROUP_ID;
    ObDatabaseSchema db_schema;
    //ObString tenant;
    db_schema.set_tenant_id(tenant_id_);
    db_schema.set_database_id(database_id_);
    db_schema.set_charset_type(CHARSET_UTF8MB4);
    db_schema.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    db_schema.set_database_name("default_database");
    if (OB_FAIL(add_database_schema(db_schema))) {
      STORAGE_LOG(WARN, "fail add database schema", K(ret));
    }
  //ObArray<ObDatabaseSchema> db_array;
  //if (OB_SUCCESS != (ret = schema_manager_.init())) {
  //  STORAGE_LOG(WARN, "fail to initialize schema manager", K(ret));
  //} else if (OB_SUCCESS != (ret = db_array.push_back(db_schema))) {
  //  STORAGE_LOG(WARN, "fail to add database", K(ret));
  //} else if (OB_SUCCESS != (ret = schema_manager_.add_new_database_schema_array(db_array))) {
  //  STORAGE_LOG(WARN, "fail to add default database", K(ret));
  //}
  }
  if (OB_SUCC(ret)) {
    if (OB_SUCCESS != (ret = ObPreProcessSysVars::init_sys_var())) {
      STORAGE_LOG(ERROR, "fail to init sys variables", K(ret));
    }
  }
  return ret;
}

int ObRestoreSchema::add_database_schema(ObDatabaseSchema &database_schema)
{
  int ret = OB_SUCCESS;
  const ObTenantSchema *tenant_schema = NULL;
  const ObSysVariableSchema *sys_variable= NULL;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(schema_guard_.get_tenant_info(database_schema.get_tenant_id(), tenant_schema))) {
      STORAGE_LOG(WARN, "get tenant info failed", K(database_schema), K(ret));
    } else if (OB_ISNULL(tenant_schema)) {
      ret = OB_TENANT_NOT_EXIST;
      STORAGE_LOG(WARN, "tenant schema is null", K(ret));
    } else if (OB_FAIL(schema_guard_.get_sys_variable_schema(database_schema.get_tenant_id(), sys_variable))) {
      OB_LOG(WARN, "get sys variable failed", K(sys_variable), K(ret));
    } else if (OB_ISNULL(sys_variable)) {
      ret = OB_TENANT_NOT_EXIST;
      OB_LOG(WARN, "sys variable schema is null", K(ret));
    } else {
      ObNameCaseMode local_mode = sys_variable->get_name_case_mode();
      if (local_mode <= OB_NAME_CASE_INVALID || local_mode >= OB_NAME_CASE_MAX)   {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "invalid tenant mod", K(ret));
      } else {
        database_schema.set_name_case_mode(local_mode);
        database_schema.set_schema_version(RESTORE_SCHEMA_VERSION);
        if (OB_FAIL(schema_service_->add_database_schema(database_schema,
           RESTORE_SCHEMA_VERSION))) {
          STORAGE_LOG(WARN, "put schema fail", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObRestoreSchema::add_table_schema(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  const ObTenantSchema *tenant_schema = NULL;
  const ObSysVariableSchema *sys_variable= NULL;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(schema_guard_.get_tenant_info(table_schema.get_tenant_id(), tenant_schema))) {
      STORAGE_LOG(WARN, "get tenant info failed", K(table_schema), K(ret));
    } else if (OB_ISNULL(tenant_schema)) {
      ret = OB_TENANT_NOT_EXIST;
      STORAGE_LOG(WARN, "tenant schema is null", K(ret));
    } else if (OB_FAIL(schema_guard_.get_sys_variable_schema(table_schema.get_tenant_id(), sys_variable))) {
      OB_LOG(WARN, "get sys variable failed", K(sys_variable), K(ret));
    } else if (OB_ISNULL(sys_variable)) {
      ret = OB_TENANT_NOT_EXIST;
      OB_LOG(WARN, "sys variable schema is null", K(ret));
    } else {
      ObNameCaseMode local_mode = sys_variable->get_name_case_mode();
      if (local_mode <= OB_NAME_CASE_INVALID || local_mode >= OB_NAME_CASE_MAX)   {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "invalid tenant mode", K(ret));
      } else {
        table_schema.set_name_case_mode(local_mode);
        table_schema.set_schema_version(RESTORE_SCHEMA_VERSION);
        if (OB_FAIL(schema_service_->add_table_schema(table_schema,
          RESTORE_SCHEMA_VERSION))) {
          STORAGE_LOG(WARN, "add table schema fail", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObRestoreSchema::do_create_table(ObStmt *stmt)
{
  int ret = OB_SUCCESS;
  ObCreateTableStmt *create_table_stmt = dynamic_cast<ObCreateTableStmt *>(stmt);
  if (NULL == create_table_stmt) {
    STORAGE_LOG(WARN, "create_table_stmt should not be NULL", K(ret));
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObTableSchema &table_schema = create_table_stmt->get_create_table_arg().schema_;
    ObSEArray<ObColDesc, 512> col_descs;
    table_schema.set_tenant_id(tenant_id_);
    table_schema.set_database_id(database_id_);
    table_schema.set_tablegroup_id(0);
    table_schema.set_table_id(table_id_++);
    table_schema.set_compress_func_name("none");
    ret = table_schema.get_column_ids(col_descs);
    for (int64_t i = 0; OB_SUCC(ret) && i < col_descs.count(); ++i) {
      const ObColumnSchemaV2 *col = table_schema.get_column_schema(col_descs.at(i).col_id_);
      const_cast<ObColumnSchemaV2 *>(col)->set_table_id(table_schema.get_table_id());
    }
    if (OB_SUCC(ret)) {
      //ret = schema_manager_.add_new_table_schema(table_schema);
      ret = add_table_schema(table_schema);
    }
  }
  return ret;
}

int ObRestoreSchema::gen_columns(ObCreateIndexStmt &stmt,
                                 ObTableSchema &index_schema)
{
  int ret = OB_SUCCESS;
  int64_t index_rowkey_num = 0;
  uint64_t max_column_id = 0;
  const ObTableSchema *table_schema = NULL;
  obrpc::ObCreateIndexArg &index_arg = stmt.get_create_index_arg();

  //bool is_index = false;
  //table_schema = schema_manager_.get_table_schema(index_arg.tenant_id_,
  //                                                index_arg.database_name_,
  //                                                index_arg.table_name_,
  //                                                is_index);
  if (OB_FAIL(schema_guard_.get_table_schema(index_arg.tenant_id_,
                                             index_arg.database_name_,
                                             index_arg.table_name_,
                                             false,
                                             table_schema))) {
    STORAGE_LOG(WARN, "get table schema fail", K(ret));
  } else if (NULL == table_schema) {
    ret = OB_ERR_ILLEGAL_ID;
    STORAGE_LOG(WARN, "invalid table id", K(index_arg));
  } else {
    const ObColumnSchemaV2 *col = NULL;
    // add index column(s)
    for (int64_t i = 0; OB_SUCC(ret) && i < index_arg.index_columns_.count(); ++i) {
      ObColumnSchemaV2 index_column;
      if (NULL == (col = table_schema->get_column_schema(
                             index_arg.index_columns_[i].column_name_))) {
        ret = OB_ERR_ILLEGAL_NAME;
        STORAGE_LOG(WARN, "invalid column name",
                    K_(index_arg.index_columns_[i].column_name));
      } else {
        index_column = *col;
        index_column.set_rowkey_position(++index_rowkey_num);
        if (col->get_column_id() > max_column_id) {
          max_column_id = col->get_column_id();
        }
        if (OB_SUCCESS != (ret = index_schema.add_column(index_column))) {
          STORAGE_LOG(WARN, "add column schema error", K(ret));
        }
      }
    }
    //add primary key
    ObSEArray<const ObColumnSchemaV2*, 64> rk_cols;
    const ObRowkeyInfo &rowkey_info = table_schema->get_rowkey_info();
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
      uint64_t column_id = OB_INVALID_ID;
      ObColumnSchemaV2 index_column;
      if (OB_SUCCESS != (ret = rowkey_info.get_column_id(i, column_id))) {
        STORAGE_LOG(WARN, "get column id error", K(i), K(ret));
      } else if (NULL == index_schema.get_column_schema(column_id)) {
        if (NULL == (col = table_schema->get_column_schema(column_id))) {
          ret = OB_ERR_ILLEGAL_ID;
          STORAGE_LOG(WARN, "invalid column id", K(column_id));
        } else {
          index_column = *col;
          index_column.set_rowkey_position(++index_rowkey_num);
          // we do not have index of other type now
          OB_ASSERT(INDEX_TYPE_UNIQUE_LOCAL == index_arg.index_type_
              || INDEX_TYPE_NORMAL_LOCAL == index_arg.index_type_
              || INDEX_TYPE_UNIQUE_GLOBAL == index_arg.index_type_
              || INDEX_TYPE_NORMAL_GLOBAL == index_arg.index_type_);
          if (INDEX_TYPE_UNIQUE_LOCAL == index_arg.index_type_
              || INDEX_TYPE_UNIQUE_GLOBAL == index_arg.index_type_) {
            index_column.set_column_id(OB_MIN_SHADOW_COLUMN_ID + col->get_column_id());
            int32_t shadow_name_len = col->get_column_name_str().length() + 8;
            char shadow_name[shadow_name_len];
            int64_t len = 0;
            if (shadow_name_len - 1 != (len = snprintf(shadow_name,
                                                       shadow_name_len,
                                                       "%s_%s",
                                                       "shadow",
                                                       col->get_column_name()))) {
              STORAGE_LOG(WARN, "failed to generate shadow name",
                  K(col->get_column_name_str()));
            } else {
              shadow_name[shadow_name_len - 1] = '\0';
              if (OB_SUCCESS != (ret = index_column.set_column_name(shadow_name))) {
                STORAGE_LOG(WARN, "failed to set shadow name", K(*shadow_name));
              } else if (OB_SUCCESS != (ret = rk_cols.push_back(col))) {
                STORAGE_LOG(WARN, "failed to remember rowkey column", K(ret));
              }
            }
          }
          if (OB_SUCC(ret)) {
            if (index_column.get_column_id() > max_column_id) {
              max_column_id = index_column.get_column_id();
            }
            if (OB_SUCCESS != (ret = index_schema.add_column(index_column))) {
              STORAGE_LOG(WARN, "add column schema error", K(ret));
            }
          }
        }
      }
    }
    //add primary key of unique index
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < rk_cols.count(); ++i) {
        ObColumnSchemaV2 index_column = *rk_cols.at(i);
        index_column.set_rowkey_position(0);
        if (OB_SUCCESS != (ret = index_schema.add_column(index_column))) {
          STORAGE_LOG(WARN, "add column schema error", K(ret));
        }
      }
    }
    //add storing column
    for (int64_t i = 0; OB_SUCC(ret) && i < index_arg.store_columns_.count(); ++i) {
      if (NULL == (col = table_schema->get_column_schema(index_arg.store_columns_[i]))) {
        ret = OB_ERR_ILLEGAL_NAME;
        STORAGE_LOG(WARN, "invalid column name", K(index_arg.store_columns_[i]));
      } else if (OB_SUCCESS != (ret = index_schema.add_column(*col))) {
        STORAGE_LOG(WARN, "add column schema error", K(ret));
      } else {
        if (col->get_column_id() > max_column_id) {
          max_column_id = col->get_column_id();
        }
      }
    }
    if (OB_SUCC(ret)) {
      index_schema.set_rowkey_column_num(index_rowkey_num);
      index_schema.set_max_used_column_id(max_column_id);
    }
  }
  return ret;
}

int ObRestoreSchema::do_create_index(ObStmt *stmt)
{
  int ret = OB_SUCCESS;
  ObCreateIndexStmt *crt_idx_stmt = dynamic_cast<ObCreateIndexStmt *>(stmt);
  if (NULL == crt_idx_stmt) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "not create statement", K(ret));
  } else {
    obrpc::ObCreateIndexArg &index_arg = crt_idx_stmt->get_create_index_arg();
    //const bool is_index = false;
    const ObTableSchema *data_schema = NULL;
    //const ObTableSchema *data_schema = schema_manager_.get_table_schema(index_arg.tenant_id_,
    //                                                                     index_arg.database_name_,
    //                                                                     index_arg.table_name_,
    //                                                                     is_index);
    if (OB_FAIL(schema_guard_.get_table_schema(index_arg.tenant_id_,
                                               index_arg.database_name_,
                                               index_arg.table_name_,
                                               false,
                                               data_schema))) {
      STORAGE_LOG(WARN, "get table schema fail", K(ret));
    } else if (data_schema == NULL)  {
      ret = OB_TABLE_NOT_EXIST;
      STORAGE_LOG(WARN, "table schema should not be null!", K(index_arg), K(ret));
    } else {
      ObTableSchema index_schema;
      if (OB_SUCCESS != (ret = gen_columns(*crt_idx_stmt, index_schema))) {
        STORAGE_LOG(WARN, "get index column(s) error", K(ret));
      } else if (OB_SUCCESS != (ret = index_schema.set_compress_func_name(
                                        index_arg.index_option_.compress_method_))) {
        STORAGE_LOG(WARN, "set compress func error", K(ret));
      } else if (OB_SUCCESS != (ret = index_schema.set_comment(
                                        index_arg.index_option_.comment_))) {
        STORAGE_LOG(WARN, "set comment error", K(ret));
      } else {
        index_schema.set_block_size(index_arg.index_option_.block_size_);
        index_schema.set_is_use_bloomfilter(index_arg.index_option_.use_bloom_filter_);
        index_schema.set_progressive_merge_num(index_arg.index_option_.progressive_merge_num_);
        index_schema.set_data_table_id(data_schema->get_table_id());
        index_schema.set_table_type(USER_INDEX);
        index_schema.set_index_type(index_arg.index_type_);
        index_schema.set_tenant_id(tenant_id_);
        index_schema.set_database_id(database_id_);
        index_schema.set_tablegroup_id(0);
        index_schema.set_table_id(table_id_++);
        index_schema.set_table_name(index_arg.index_name_);
        index_schema.set_compress_func_name("none");
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(add_table_schema(index_schema))) {
          STORAGE_LOG(WARN, "add table schema fail", K(ret));
        } else {
          ObTableSchema *table_schema = const_cast<ObTableSchema *>(data_schema);
          if (OB_FAIL(table_schema->add_simple_index_info(
              ObAuxTableMetaInfo(index_schema.get_table_id(), USER_INDEX, INDEX_TYPE_NORMAL_LOCAL)))) {
            STORAGE_LOG(WARN, "add simple_index_info fail", K(ret));
          } else if (OB_FAIL(add_table_schema(*table_schema))) {
            STORAGE_LOG(WARN, "add table schema fail", K(ret));
          }
        }
        //ret = schema_manager_.add_new_table_schema(index_schema);
      }
    }

  }
  return ret;
}

int ObRestoreSchema::do_resolve_single_stmt(ParseNode *node,
                                            ObResolverParams &ctx)
{
  int ret = OB_SUCCESS;
  if (NULL == node) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "parse node should not be NULL", K(ret));
  } else {
    ObResolver resolver(ctx);
    ObStmt *stmt = NULL;
    if (OB_SUCCESS != (ret = resolver.resolve(
                                 ObResolver::IS_NOT_PREPARED_STMT,
                                 *node, stmt))) {
      STORAGE_LOG(WARN, "resolver.resolve() failed", K(ret));
    } else {
      switch (stmt->get_stmt_type()) {
        case stmt::T_CREATE_TABLE:
          ret = do_create_table(stmt);
          break;
        case stmt::T_CREATE_INDEX:
          ret = do_create_index(stmt);
          break;
        default:
          STORAGE_LOG(WARN, "unknown stmt type", K(stmt->get_stmt_type()));
          break;
      }
    }
  }
  return ret;
}

int ObRestoreSchema::do_parse_line(ObArenaAllocator &allocator, const char *query_str)
{
  int ret = OB_SUCCESS;
  ObSQLMode mode = SMO_DEFAULT;
  ObParser parser(allocator, mode);
  ObString query = ObString::make_string(query_str);
  ParseResult parse_result;
  if (OB_SUCCESS != (ret = parser.parse(query, parse_result))) {
    STORAGE_LOG(WARN, "parser.parse() failed", K(ret));
  }
  if (OB_SUCC(ret)) {
    ObSchemaChecker schema_checker;
    schema_checker.init(schema_guard_);
    ObSQLSessionInfo session_info;
    char db_name_str[] = "default_database";
    int32_t db_name_len = static_cast<int32_t>(strlen(db_name_str));
    ObString db_name(db_name_len, db_name_len, db_name_str);
    ObString tenant("storage_test");
    ObResolverParams resolver_ctx;
    uint32_t version = 0;
    ObRawExprFactory expr_factory(allocator);
    ObStmtFactory stmt_factory(allocator);
    resolver_ctx.allocator_  = &allocator;
    resolver_ctx.schema_checker_ = &schema_checker;
    resolver_ctx.session_info_ = &session_info;
    resolver_ctx.expr_factory_ = &expr_factory;
    resolver_ctx.stmt_factory_ = &stmt_factory;
    resolver_ctx.query_ctx_ = stmt_factory.get_query_ctx();
    if (OB_SUCCESS != (ret = session_info.init_tenant(tenant, tenant_id_))) {
      STORAGE_LOG(WARN, "fail to init sql session info", K(ret), K(tenant_id_));
    } else if (OB_SUCCESS != (ret = session_info.set_default_database(db_name))) {
      STORAGE_LOG(WARN, "fail to set default database", K(ret));
    } else if (OB_SUCCESS != (ret = session_info.test_init(version, 0, 0, &allocator))) {
      STORAGE_LOG(WARN, "fail to init session info", K(ret));
    } else if (OB_SUCCESS != (ret= session_info.load_default_sys_variable(false, true))) {
      STORAGE_LOG(WARN, "fail to load default sys variable");
    } else if (T_STMT_LIST == parse_result.result_tree_->type_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < parse_result.result_tree_->num_child_; ++i) {
        if (OB_SUCCESS != (ret = do_resolve_single_stmt(parse_result.result_tree_->children_[i],
                                                        resolver_ctx))) {
          STORAGE_LOG(WARN, "resolve single stmt failed", K(ret));
        }
      }
    } else {
      if (OB_SUCCESS != (ret = do_resolve_single_stmt(parse_result.result_tree_,
                                                      resolver_ctx))) {
        STORAGE_LOG(WARN, "resolve single stmt failed", K(ret));
      }
    }
    parser.free_result(parse_result);
  }
  return ret;
}

int ObRestoreSchema::parse_from_file(const char *filename,
                                     ObSchemaGetterGuard *&schema_guard)
{
  int ret = OB_SUCCESS;
  std::ifstream if_file(filename);
  std::string line;
  ObArenaAllocator allocator(ObModIds::TEST);
  while (OB_SUCCESS == ret && std::getline(if_file, line)) {
    ret = do_parse_line(allocator, line.c_str());
    allocator.reuse();
  }
  schema_guard = &schema_guard_;
 // if (OB_SUCC(ret)) {
 //   if (OB_SUCCESS != (ret = schema_manager_.cons_table_to_index_relation())) {
 //     STORAGE_LOG(WARN, "construct table to index relation error", K(ret));
 //   } else {
 //     schema_manager = &schema_manager_;
 //   }
 // } else {
 //   STORAGE_LOG(WARN, "fail to construct schema manager", K(ret));
 //   exit(1);
 // }
  return ret;
}
