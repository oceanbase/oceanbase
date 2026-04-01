/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
// Allow access to private members of ObTenantBase for test setup
#define private public
#define protected public

#define USING_LOG_PREFIX SQL

#include "unittest/sql/engine/op_test/ob_op_test_engine.h"
#include "share/config/ob_server_config.h"  // for GCONF
#include "observer/omt/ob_tenant_config_mgr.h"  // for ObTenantConfigGuard
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/resolver/ob_stmt.h"
#include "sql/code_generator/ob_static_engine_expr_cg.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/ob_batch_rows.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_struct.h"
#include "common/object/ob_object.h"
#include "lib/number/ob_number_v2.h"
#include "lib/worker.h"  // for CompatMode, set_compat_mode
#include "storage/tmp_file/ob_tmp_file_manager.h"  // for ObTenantTmpFileManager

namespace oceanbase
{
namespace sql
{

OpTestEngine::OpTestEngine()
  : inited_(false)
{
}

OpTestEngine::~OpTestEngine()
{
  destroy();
}

void OpTestEngine::init()
{
  if (inited_) {
    return;
  }
  // Call parent init
  test::TestSqlUtils::init();

  // Save current GCONF state for later restore in destroy()
  saved_gconf_dump_enabled_ = GCONF.is_sql_operator_dump_enabled();

  // Configure SQL operator dump if requested
  if (enable_sql_operator_dump_) {
    GCONF.enable_sql_operator_dump.set_value("True");
    LOG_INFO("Enabled SQL operator dump", K(enable_sql_operator_dump_));
  }

  // Configure hash_area_size if dump is enabled
  if (enable_sql_operator_dump_ && hash_area_size_ > 0) {
    uint64_t tenant_id = ObTenantEnv::get_tenant_local()->id_;
    if (tenant_id != OB_INVALID_TENANT_ID) {
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
      if (tenant_config.is_valid()) {
        tenant_config->_hash_area_size = hash_area_size_;
        LOG_INFO("Set hash_area_size for tenant", K(tenant_id), K(hash_area_size_));
      }
    }
  }

  // Set compatibility mode in thread-local variable
  // This is required for lib::is_oracle_mode() / lib::is_mysql_mode() to work correctly
  if (sql_mode_ == SqlMode::ORACLE) {
    lib::set_compat_mode(lib::Worker::CompatMode::ORACLE);
    session_info_.set_compatibility_mode(ObCompatibilityMode::ORACLE_MODE);
  } else {
    lib::set_compat_mode(lib::Worker::CompatMode::MYSQL);
    session_info_.set_compatibility_mode(ObCompatibilityMode::MYSQL_MODE);
  }

  // Link execution context to session so that session->get_cur_exec_ctx() works
  // This is required for implicit cast creation during expression type deduction
  session_info_.set_cur_exec_ctx(&exec_ctx_);

  // Force enable rich format for vector execution
  session_info_.set_force_rich_format(
      oceanbase::sql::ObBasicSessionInfo::ForceRichFormatStatus::FORCE_ON);

  // Initialize ObTenantTmpFileManager for scalar aggregate operations
  // This is required by ObChunkStoreUtil::alloc_dir_id() which is called in
  // ObScalarAggregateVecOp::inner_open()
  //
  // IMPORTANT: The alloc_dir() function checks if tenant_id == MTL_ID().
  // MTL_ID() returns get_tenant_local()->id(), which defaults to OB_INVALID_TENANT_ID.
  // We need to set the tenant ID on get_tenant_local() to match session's tenant ID.
  int ret = OB_SUCCESS;

  // Step 1: Set tenant ID on get_tenant_local() so MTL_ID() returns correct value
  // The get_tenant_local() returns a static thread-local ObTenantBase object with id_ = OB_INVALID_TENANT_ID
  // We directly set the id_ member (accessed via #define private public hack at top of file)
  ObTenantBase *tenant_ctx = ObTenantEnv::get_tenant();
  ObTenantBase *local_ctx = ObTenantEnv::get_tenant_local();
  if (OB_NOT_NULL(tenant_ctx) && OB_NOT_NULL(local_ctx)) {
    // Directly set id_ to match the tenant context
    local_ctx->id_ = tenant_ctx->id_;
    LOG_INFO("Set tenant local id", "tenant_id", local_ctx->id_,
             "local_ctx_ptr", (void*)local_ctx,
             "tenant_ctx_ptr", (void*)tenant_ctx);
  }

  // Step 2: Create and set ObTenantTmpFileManager if not exists
  tmp_file::ObTenantTmpFileManager *tmp_file_mgr = nullptr;
  if (OB_ISNULL(tmp_file_mgr = MTL(tmp_file::ObTenantTmpFileManager*))) {
    // Create and initialize ObTenantTmpFileManager
    void *mem = allocator_.alloc(sizeof(tmp_file::ObTenantTmpFileManager));
    if (OB_ISNULL(mem)) {
      LOG_WARN("failed to allocate ObTenantTmpFileManager");
    } else {
      tmp_file_mgr = new (mem) tmp_file::ObTenantTmpFileManager();
      if (OB_FAIL(tmp_file_mgr->init())) {
        LOG_WARN("failed to init ObTenantTmpFileManager", K(ret));
      } else if (OB_FAIL(tmp_file_mgr->start())) {
        // IMPORTANT: Must call start() to start ObSNTenantTmpFileManager
        // Otherwise alloc_dir() will fail with OB_NOT_RUNNING
        LOG_WARN("failed to start ObTenantTmpFileManager", K(ret));
      } else {
        // Set the manager in tenant context
        // IMPORTANT: Use get_tenant_local() not get_tenant() because MTL() macro uses get_tenant_local()
        // See src/share/rc/ob_tenant_base.h: mtl() calls get_tenant_local()->get<T>()
        ObTenantEnv::get_tenant_local()->set(tmp_file_mgr);
        // Verify the set was successful
        tmp_file::ObTenantTmpFileManager *verify_mgr = MTL(tmp_file::ObTenantTmpFileManager*);
        LOG_INFO("ObTenantTmpFileManager initialized and started successfully",
                 "set_ptr", (void*)tmp_file_mgr,
                 "verify_ptr", (void*)verify_mgr,
                 "local_ctx_ptr", (void*)ObTenantEnv::get_tenant_local(),
                 "is_null", OB_ISNULL(verify_mgr));
      }
    }
  } else {
    LOG_INFO("ObTenantTmpFileManager already exists", "ptr", (void*)tmp_file_mgr);
  }

  inited_ = true;
}

void OpTestEngine::destroy()
{
  if (!inited_) {
    return;
  }
  tmp_file::ObTenantTmpFileManager* tmp_file_mgr = nullptr;
  if (OB_NOT_NULL(tmp_file_mgr = MTL(tmp_file::ObTenantTmpFileManager*))) {
    tmp_file_mgr->stop();
    tmp_file_mgr->destroy();
    allocator_.free(tmp_file_mgr);
  }

  // Restore GCONF.enable_sql_operator_dump to original value
  if (enable_sql_operator_dump_) {
    if (saved_gconf_dump_enabled_) {
      GCONF.enable_sql_operator_dump.set_value("True");
    } else {
      GCONF.enable_sql_operator_dump.set_value("False");
    }
    LOG_INFO("Restored GCONF.enable_sql_operator_dump", K(saved_gconf_dump_enabled_));
  }

  test::TestSqlUtils::destroy();
  inited_ = false;
}

// Unit tests for register_table are in test_op_test_engine.cpp
int OpTestEngine::register_table(const char *table_name, const char *col_defs)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(table_name) || OB_ISNULL(col_defs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(table_name), KP(col_defs), K(ret));
    return ret;
  }

  // Use database_id = 1024 to match resolver_ctx.database_id_ in do_resolve
  // This is the database ID the resolver uses for table lookups
  const uint64_t test_database_id = 1024;
  const char *database_name = "test_db";

  // Step 1: Create database if not exists
  uint64_t database_id = OB_INVALID_ID;
  if (OB_FAIL(schema_guard_.get_database_id(sys_tenant_id_, ObString::make_string(database_name), database_id))) {
    LOG_WARN("get database id failed", K(ret));
    return ret;
  }

  // get_database_id returns OB_SUCCESS with database_id=OB_INVALID_ID if not found
  if (OB_INVALID_ID == database_id) {
    // Database doesn't exist, create it
    share::schema::ObDatabaseSchema database_schema;
    database_schema.set_tenant_id(sys_tenant_id_);
    database_schema.set_database_id(test_database_id);
    database_schema.set_database_name(ObString::make_string(database_name));
    database_schema.set_charset_type(CHARSET_UTF8MB4);
    database_schema.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    ret = add_database_schema(database_schema);
    if (OB_FAIL(ret)) {
      LOG_WARN("add database schema failed", K(ret));
      return ret;
    }
    database_id = test_database_id;

    // Refresh schema guard to see newly added database
    ret = schema_service_->get_schema_guard(schema_guard_, schema_version_);
    if (OB_FAIL(ret)) {
      LOG_WARN("refresh schema guard after adding database failed", K(ret));
      return ret;
    }
  }

  // Step 2: Check if table already exists
  uint64_t table_id = OB_INVALID_ID;
  if (OB_FAIL(schema_guard_.get_table_id(sys_tenant_id_, database_id,
                                          ObString::make_string(table_name),
                                          false,
                                          share::schema::ObSchemaGetterGuard::ALL_NON_HIDDEN_TYPES,
                                          table_id))) {
    LOG_WARN("get table id failed", K(ret));
    return ret;
  }

  // get_table_id returns OB_SUCCESS with table_id=OB_INVALID_ID if not found
  if (OB_INVALID_ID != table_id) {
    // Table already exists, return success
    return OB_SUCCESS;
  }

  // Step 3: Create table schema directly
  share::schema::ObTableSchema table_schema;
  table_schema.set_tenant_id(sys_tenant_id_);
  table_schema.set_database_id(database_id);
  table_schema.set_table_name(ObString::make_string(table_name));
  table_schema.set_table_type(share::schema::USER_TABLE);
  table_schema.set_tablegroup_id(OB_SYS_TABLEGROUP_ID);
  table_schema.set_charset_type(CHARSET_UTF8MB4);
  table_schema.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);

  // Step 4: Parse column definitions and add columns
  // Format: "col1 type1, col2 type2, ..."
  // Simple parser for common types: int, varchar(n), number, double
  std::string col_defs_str(col_defs);
  size_t pos = 0;
  int64_t column_id = 1;  // Column IDs start from 1

  while (pos < col_defs_str.size() && OB_SUCC(ret)) {
    // Skip whitespace
    while (pos < col_defs_str.size() && isspace(col_defs_str[pos])) pos++;
    if (pos >= col_defs_str.size()) break;

    // Find column name
    size_t name_start = pos;
    while (pos < col_defs_str.size() && !isspace(col_defs_str[pos]) && col_defs_str[pos] != ',') pos++;
    std::string col_name = col_defs_str.substr(name_start, pos - name_start);

    // Skip whitespace
    while (pos < col_defs_str.size() && isspace(col_defs_str[pos])) pos++;
    if (pos >= col_defs_str.size()) break;

    // Find type (until comma or end)
    size_t type_start = pos;
    int paren_depth = 0;
    while (pos < col_defs_str.size() && (col_defs_str[pos] != ',' || paren_depth > 0)) {
      if (col_defs_str[pos] == '(') paren_depth++;
      else if (col_defs_str[pos] == ')') paren_depth--;
      pos++;
    }
    std::string col_type = col_defs_str.substr(type_start, pos - type_start);

    // Trim trailing whitespace from type
    while (!col_type.empty() && isspace(col_type.back())) col_type.pop_back();

    // Create column schema
    share::schema::ObColumnSchemaV2 column_schema;
    column_schema.set_column_id(column_id);
    column_schema.set_column_name(ObString::make_string(col_name.c_str()));
    column_schema.set_tenant_id(sys_tenant_id_);
    column_schema.set_table_id(table_schema.get_table_id());
    column_schema.set_charset_type(CHARSET_UTF8MB4);
    column_schema.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);

    // Parse type
    if (col_type.find("int") != std::string::npos || col_type.find("INT") != std::string::npos) {
      column_schema.set_data_type(ObIntType);
      column_schema.set_nullable(true);
    } else if (col_type.find("varchar") != std::string::npos || col_type.find("VARCHAR") != std::string::npos) {
      column_schema.set_data_type(ObVarcharType);
      column_schema.set_nullable(true);
      // Extract length if specified
      size_t paren_pos = col_type.find('(');
      if (paren_pos != std::string::npos) {
        size_t end_paren = col_type.find(')', paren_pos);
        if (end_paren != std::string::npos) {
          int len = std::stoi(col_type.substr(paren_pos + 1, end_paren - paren_pos - 1));
          column_schema.set_data_length(len);
        }
      }
    } else if (col_type.find("number") != std::string::npos || col_type.find("NUMBER") != std::string::npos) {
      // Parse number(P, S) if present
      size_t paren_pos = col_type.find('(');
      if (paren_pos != std::string::npos) {
        size_t end_paren = col_type.find(')', paren_pos);
        if (end_paren != std::string::npos) {
          std::string args = col_type.substr(paren_pos + 1, end_paren - paren_pos - 1);
          size_t comma_pos = args.find(',');
          if (comma_pos != std::string::npos) {
            int precision = std::stoi(args.substr(0, comma_pos));
            int scale = std::stoi(args.substr(comma_pos + 1));
            column_schema.set_data_type(ObDecimalIntType);
            column_schema.set_data_precision(precision);
            column_schema.set_data_scale(scale);
          } else {
            // number(P) without scale
            int precision = std::stoi(args);
            column_schema.set_data_type(ObDecimalIntType);
            column_schema.set_data_precision(precision);
            column_schema.set_data_scale(0);
          }
        }
      } else {
        // number without precision/scale
        // MySQL mode: use ObDecimalIntType
        // Oracle mode: use ObNumberType
        if (sql_mode_ == SqlMode::MYSQL) {
          column_schema.set_data_type(ObDecimalIntType);
          column_schema.set_data_precision(10);
          column_schema.set_data_scale(0);
        } else {
          column_schema.set_data_type(ObNumberType);
        }
      }
      column_schema.set_nullable(true);
    } else if (col_type.find("decimal") != std::string::npos || col_type.find("DECIMAL") != std::string::npos) {
      // Parse decimal(P, S) if present
      size_t paren_pos = col_type.find('(');
      if (paren_pos != std::string::npos) {
        size_t end_paren = col_type.find(')', paren_pos);
        if (end_paren != std::string::npos) {
          std::string args = col_type.substr(paren_pos + 1, end_paren - paren_pos - 1);
          size_t comma_pos = args.find(',');
          if (comma_pos != std::string::npos) {
            int precision = std::stoi(args.substr(0, comma_pos));
            int scale = std::stoi(args.substr(comma_pos + 1));
            column_schema.set_data_type(ObDecimalIntType);
            column_schema.set_data_precision(precision);
            column_schema.set_data_scale(scale);
          } else {
            // decimal(P) without scale
            int precision = std::stoi(args);
            column_schema.set_data_type(ObDecimalIntType);
            column_schema.set_data_precision(precision);
            column_schema.set_data_scale(0);
          }
        }
      } else {
        // decimal without precision/scale
        column_schema.set_data_type(ObDecimalIntType);
      }
      column_schema.set_nullable(true);
    } else if (col_type.find("double") != std::string::npos || col_type.find("DOUBLE") != std::string::npos) {
      column_schema.set_data_type(ObDoubleType);
      // Parse double(M, S) if present
      size_t paren_pos = col_type.find('(');
      if (paren_pos != std::string::npos) {
        size_t end_paren = col_type.find(')', paren_pos);
        if (end_paren != std::string::npos) {
          std::string args = col_type.substr(paren_pos + 1, end_paren - paren_pos - 1);
          size_t comma_pos = args.find(',');
          if (comma_pos != std::string::npos) {
            int m = std::stoi(args.substr(0, comma_pos));
            int s = std::stoi(args.substr(comma_pos + 1));
            column_schema.set_data_length(m);
            column_schema.set_data_scale(s);
          } else {
            // double(M) without scale
            column_schema.set_data_length(std::stoi(args));
          }
        }
      }
      column_schema.set_nullable(true);
    } else if (col_type.find("float") != std::string::npos || col_type.find("FLOAT") != std::string::npos) {
      column_schema.set_data_type(ObFloatType);
      column_schema.set_nullable(true);
    } else {
      // Default to varchar
      column_schema.set_data_type(ObVarcharType);
      column_schema.set_nullable(true);
    }

    table_schema.add_column(column_schema);
    column_id++;

    // Skip comma
    if (pos < col_defs_str.size() && col_defs_str[pos] == ',') pos++;
  }

  // Step 5: Set rowkey column count (0 for non-primary key tables)
  table_schema.set_rowkey_column_num(0);

  // Step 6: Generate table ID using parent's method
  uint64_t new_table_id = get_next_table_id(sys_tenant_id_);
  table_schema.set_table_id(new_table_id);

  // Step 7: Add table schema
  ret = add_table_schema(table_schema);
  if (OB_FAIL(ret)) {
    LOG_WARN("add table schema failed", K(ret), K(table_name));
    return ret;
  }

  // Step 8: Refresh schema guard to see newly added table
  ret = schema_service_->get_schema_guard(schema_guard_, schema_version_);
  if (OB_FAIL(ret)) {
    LOG_WARN("refresh schema guard failed", K(ret));
    return ret;
  }

  LOG_INFO("register table success", K(table_name), K(database_id), "table_id", table_schema.get_table_id());

  // Step 9: Set session's default database to test_db so resolver can find the table
  ret = session_info_.set_default_database(ObString::make_string(database_name), CS_TYPE_UTF8MB4_GENERAL_CI);
  if (OB_FAIL(ret)) {
    LOG_WARN("set default database failed", K(ret), K(database_name));
    return ret;
  }
  // Also set database_id which is used by resolver's session_info_->get_database_id()
  session_info_.set_database_id(database_id);

  return ret;
}

// Unit tests for resolve_sql are in test_op_test_engine.cpp
//       check stmt->to_string()
int OpTestEngine::resolve_sql(const std::string &sql, ObDMLStmt *&stmt, int expect_error)
{
  int ret = OB_SUCCESS;
  stmt = nullptr;

  // Check stmt_factory_ and query_ctx
  ObQueryCtx *query_ctx = stmt_factory_.get_query_ctx();

  // Check if schema_guard can find our tables
  uint64_t check_db_id = OB_INVALID_ID;
  ret = schema_guard_.get_database_id(sys_tenant_id_, ObString::make_string("test_db"), check_db_id);

  uint64_t check_table_id = OB_INVALID_ID;
  if (OB_SUCCESS == ret && OB_INVALID_ID != check_db_id) {
    ret = schema_guard_.get_table_id(sys_tenant_id_, check_db_id, ObString::make_string("t"),
                                      false, share::schema::ObSchemaGetterGuard::ALL_NON_HIDDEN_TYPES,
                                      check_table_id);
  }

  // Parse and resolve SQL
  ObString ob_sql(sql.length(), sql.c_str());
  ObStmt *base_stmt = nullptr;

  // IMPORTANT: Pass parameterized=false to keep string literals as T_VARCHAR
  // instead of converting them to T_QUESTIONMARK (parameter placeholders)
  // This is required for proper expression CG without param store setup
  // Pass expect_error to do_resolve so it can handle expected errors properly
  do_resolve(ob_sql, base_stmt, test::JSON_FORMAT, expect_error, false, true);

  if (OB_NOT_NULL(base_stmt)) {
    stmt = static_cast<ObDMLStmt *>(base_stmt);
  } else {
    // If we expected an error and got null stmt, that's expected
    // Only log warning if we expected success
    if (OB_SUCCESS == expect_error) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("resolve failed, stmt is null", K(ret));
    }
  }

  return ret;
}

// Private function - tested indirectly through generate_exprs and execute
int OpTestEngine::collect_raw_exprs(ObDMLStmt &stmt,
                                    ObIArray<ObRawExpr *> &exprs)
{
  int ret = OB_SUCCESS;

  // Try to cast to ObSelectStmt for select items
  ObSelectStmt *select_stmt = nullptr;
  if (stmt.is_select_stmt()) {
    select_stmt = static_cast<ObSelectStmt *>(&stmt);
  }

  // Collect select items (only for SELECT statements)
  if (OB_NOT_NULL(select_stmt)) {
    const common::ObIArray<SelectItem> &select_items = select_stmt->get_select_items();
    for (int64_t i = 0; i < select_items.count() && OB_SUCC(ret); ++i) {
      if (OB_NOT_NULL(select_items.at(i).expr_)) {
        if (OB_FAIL(exprs.push_back(select_items.at(i).expr_))) {
          LOG_WARN("failed to push select item expr", K(ret));
        }
      }
    }
  }

  // Collect group by expressions (only for SELECT statements)
  if (OB_SUCC(ret) && OB_NOT_NULL(select_stmt)) {
    const common::ObIArray<ObRawExpr *> &group_exprs = select_stmt->get_group_exprs();
    for (int64_t i = 0; i < group_exprs.count() && OB_SUCC(ret); ++i) {
      if (OB_NOT_NULL(group_exprs.at(i))) {
        if (OB_FAIL(exprs.push_back(group_exprs.at(i)))) {
          LOG_WARN("failed to push group expr", K(ret));
        }
      }
    }
  }

  // Collect aggregate expressions
  if (OB_SUCC(ret) && OB_NOT_NULL(select_stmt)) {
    const common::ObIArray<ObAggFunRawExpr *> &aggr_items = select_stmt->get_aggr_items();
    for (int64_t i = 0; i < aggr_items.count() && OB_SUCC(ret); ++i) {
      if (OB_NOT_NULL(aggr_items.at(i))) {
        if (OB_FAIL(exprs.push_back(aggr_items.at(i)))) {
          LOG_WARN("failed to push aggr item", K(ret));
        }
      }
    }
  }

  // Collect window function expressions
  if (OB_SUCC(ret) && OB_NOT_NULL(select_stmt)) {
    const common::ObIArray<ObWinFunRawExpr *> &win_func_exprs = select_stmt->get_window_func_exprs();
    for (int64_t i = 0; i < win_func_exprs.count() && OB_SUCC(ret); ++i) {
      if (OB_NOT_NULL(win_func_exprs.at(i))) {
        if (OB_FAIL(exprs.push_back(win_func_exprs.at(i)))) {
          LOG_WARN("failed to push win func expr", K(ret));
        }
      }
    }
  }

  // Collect order items
  if (OB_SUCC(ret) && OB_NOT_NULL(select_stmt)) {
    const common::ObIArray<OrderItem> &order_items = select_stmt->get_order_items();
    for (int64_t i = 0; i < order_items.count() && OB_SUCC(ret); ++i) {
      if (OB_NOT_NULL(order_items.at(i).expr_)) {
        if (OB_FAIL(exprs.push_back(order_items.at(i).expr_))) {
          LOG_WARN("failed to push order item expr", K(ret));
        }
      }
    }
  }

  // Collect WHERE condition expressions for filter injection
  for (int64_t i = 0; i < stmt.get_condition_size() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(exprs.push_back(stmt.get_condition_expr(i)))) {
      LOG_WARN("failed to push condition expr", K(ret));
    }
  }

  // Collect limit expressions
  if (OB_SUCC(ret) && stmt.has_limit()) {
    if (OB_NOT_NULL(stmt.get_limit_expr())) {
      if (OB_FAIL(exprs.push_back(stmt.get_limit_expr()))) {
        LOG_WARN("failed to push limit expr", K(ret));
      }
    }
    if (OB_NOT_NULL(stmt.get_offset_expr())) {
      if (OB_FAIL(exprs.push_back(stmt.get_offset_expr()))) {
        LOG_WARN("failed to push offset expr", K(ret));
      }
    }
    if (OB_NOT_NULL(stmt.get_limit_percent_expr())) {
      if (OB_FAIL(exprs.push_back(stmt.get_limit_percent_expr()))) {
        LOG_WARN("failed to push limit percent expr", K(ret));
      }
    }
  }

  return ret;
}

int OpTestEngine::generate_exprs(ObDMLStmt &stmt)
{
  int ret = OB_SUCCESS;

  // CRITICAL: Clear expression frame info from previous runs
  // The rt_exprs_ array is reused across runs, and prepare_allocate() only expands it.
  // Without clearing, expressions from previous runs remain, causing frame layout corruption.
  // Use reuse() instead of reset() to keep the underlying memory valid.
  phy_plan_.get_expr_frame_info().rt_exprs_.reuse();
  phy_plan_.get_expr_frame_info().const_frame_ptrs_.reuse();
  phy_plan_.get_expr_frame_info().const_frame_.reuse();
  phy_plan_.get_expr_frame_info().param_frame_.reuse();
  phy_plan_.get_expr_frame_info().dynamic_frame_.reuse();
  phy_plan_.get_expr_frame_info().datum_frame_.reuse();
  phy_plan_.get_expr_frame_info().ser_expr_marks_.reuse();
  phy_plan_.get_expr_frame_info().need_ctx_cnt_ = 0;

  // CRITICAL: Reset exec_ctx_ expr_op_size to allow re-initialization
  // init_expr_op() checks expr_op_size_ > 0 and returns OB_INIT_TWICE if already initialized.
  // We need to reset it to 0 so that init_expr_op() can be called again for the new expressions.
  exec_ctx_.reset_expr_op();

  // Step 1: Collect top-level raw expressions into ObRawExprUniqueSet
  // Do NOT pre-flatten! Let generate() handle the flattening.
  // Pre-flattening sets IS_MARKED flag which never gets cleared,
  // causing generate()'s internal flatten to skip all expressions.
  ObRawExprUniqueSet raw_exprs(true);  // need_unique=true required by generate()
  ObSEArray<ObRawExpr *, 16> top_level_exprs;
  ret = collect_raw_exprs(stmt, top_level_exprs);
  if (OB_FAIL(ret)) {
    LOG_WARN("collect raw exprs failed", K(ret));
    return ret;
  }

  // Just add top-level expressions without flattening
  // generate() will internally call flatten_and_add_raw_exprs
  for (int64_t i = 0; i < top_level_exprs.count() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(raw_exprs.append(top_level_exprs.at(i)))) {
      LOG_WARN("append raw expr failed", K(ret));
      return ret;
    }
  }

  // CRITICAL: Clear IS_MARKED flag from expressions before calling generate()
  // The append() calls above set IS_MARKED on each expression for deduplication.
  // But generate() internally calls flatten_and_add_raw_exprs() which checks IS_MARKED
  // to skip already-marked expressions. Without clearing, all expressions are skipped!
  if (OB_FAIL(ObRawExprUtils::clear_exprs_flag(raw_exprs.get_expr_array(), IS_MARKED))) {
    LOG_WARN("clear IS_MARKED flag failed", K(ret));
    return ret;
  }

  // Create expression CG with correct constructor parameters
  ObStaticEngineExprCG expr_cg(allocator_,
                                &session_info_,
                                &schema_guard_,
                                0,  // original_param_cnt
                                0,  // param_cnt
                                GET_MIN_CLUSTER_VERSION());

  // Set batch size for vectorized execution - this is critical for proper frame layout!
  // Without batch_size_ > 0, the CG uses non-vector frame layout which doesn't set
  // null_bitmap_off_ and other vector-specific offsets.
  expr_cg.set_batch_size(batch_size_);

  // Also set batch_size on the physical plan so is_vectorized() returns true
  // This is required for init_skip_vector() to be called during operator open()
  phy_plan_.set_batch_size(batch_size_);

  // Generate expressions
  ret = expr_cg.generate(raw_exprs, phy_plan_.get_expr_frame_info());
  if (OB_FAIL(ret)) {
    LOG_WARN("generate exprs failed", K(ret));
    return ret;
  }

  // Pre-allocate execution memory
  ret = phy_plan_.get_expr_frame_info().pre_alloc_exec_memory(exec_ctx_);
  if (OB_FAIL(ret)) {
    LOG_WARN("pre alloc exec memory failed", K(ret));
    return ret;
  }

  // Initialize expression operator context store
  // This is required for expressions like concat that need context (e.g., ObExprConcatContext)
  ret = exec_ctx_.init_expr_op(phy_plan_.get_expr_frame_info().need_ctx_cnt_);
  if (OB_FAIL(ret)) {
    LOG_WARN("init expr op failed", K(ret));
    return ret;
  }

  return ret;
}

MockDataSourceSpec *OpTestEngine::create_mock_data_source_spec(const ExprFixedArray &output_exprs,
                                                                  const ExprFixedArray *filter_exprs)
{
  int ret = OB_SUCCESS;

  // Allocate spec
  void *spec_mem = allocator_.alloc(sizeof(MockDataSourceSpec));
  if (OB_ISNULL(spec_mem)) {
    LOG_WARN("alloc spec failed");
    return nullptr;
  }

  MockDataSourceSpec *spec = new (spec_mem) MockDataSourceSpec(allocator_, PHY_TABLE_SCAN);
  spec->output_ = output_exprs;
  spec->plan_ = &phy_plan_;  // Set plan pointer to enable open() to work
  spec->max_batch_size_ = batch_size_;  // Use configured batch size
  spec->use_rich_format_ = session_info_.use_rich_format();  // Use session setting

  // Set filters if provided - enables filter_rows() in ObOperator::get_next_batch()
  if (OB_NOT_NULL(filter_exprs) && filter_exprs->count() > 0) {
    spec->filters_.init(filter_exprs->count());
    spec->filters_.prepare_allocate(filter_exprs->count());
    for (int64_t i = 0; i < filter_exprs->count(); ++i) {
      spec->filters_.at(i) = filter_exprs->at(i);
    }
    LOG_INFO("set filter expressions on mock spec", "count", filter_exprs->count());
  }

  return spec;
}

MockDataSourceOp *OpTestEngine::create_mock_data_source_op(MockDataSourceSpec &spec)
{
  int ret = OB_SUCCESS;
  // Allocate op
  void *op_mem = allocator_.alloc(sizeof(MockDataSourceOp));
  if (OB_ISNULL(op_mem)) {
    LOG_WARN("alloc op failed", K(ret));
    return nullptr;
  }

  MockDataSourceOp *op = new (op_mem) MockDataSourceOp(exec_ctx_, spec, nullptr);
  return op;
}

MockDataSourceOp *OpTestEngine::create_mock_data_source(const ExprFixedArray &output_exprs,
                                                          const ExprFixedArray *filter_exprs)
{
  int ret = OB_SUCCESS;
  MockDataSourceSpec *spec = create_mock_data_source_spec(output_exprs, filter_exprs);
  if (OB_ISNULL(spec)) {
    LOG_WARN("create mock data source spec failed", K(ret));
    return nullptr;
  }
  return create_mock_data_source_op(*spec);
}

OpTestResult OpTestEngine::collect_batch_results(ObOperator *op, const ExprFixedArray *output_exprs)
{
  OpTestResult result;
  int ret = OB_SUCCESS;

  if (OB_ISNULL(op)) {
    LOG_WARN("op is null");
    return result;
  }

  // Determine which expressions to use for output
  // If output_exprs is provided, use those (for SELECT expression evaluation)
  // Otherwise use operator's output expressions (for direct column output)
  const ExprFixedArray *result_exprs = output_exprs ? output_exprs : &op->get_spec().output_;

  // Get next batch in a loop
  const ObOpSpec &spec = op->get_spec();
  const ObBatchRows *batch_rows = nullptr;

  // For CHECKSUM mode, we accumulate checksum incrementally without storing all rows
  uint64_t running_checksum = 0;
  int64_t total_row_count = 0;

  while (OB_SUCC(ret)) {
    ret = op->get_next_batch(INT64_MAX, batch_rows);
    if (OB_FAIL(ret)) {
      if (ret != OB_ITER_END) {
        LOG_WARN("get next batch failed", K(ret));
      }
      break;
    }

    if (OB_ISNULL(batch_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("batch_rows is null", K(ret));
      break;
    }

    LOG_DEBUG("batch_rows info", "size", batch_rows->size_, "end", batch_rows->end_,
             "all_rows_active", batch_rows->all_rows_active_);

    // Collect rows from output expressions
    // Note: Do this BEFORE checking end_ flag, because operators like ScalarAggregate
    // return both data (size_ > 0) and end_ flag in the same batch.
    if (batch_rows->size_ > 0) {
      ObEvalCtx &eval_ctx = op->get_eval_ctx();

      // Clear calc_exprs_ evaluated flags so expressions are re-evaluated with new batch data.
      // Must be done after get_next_batch() fills child data and before eval_batch().
      const ExprFixedArray &calc_exprs = spec.calc_exprs_;
      for (int64_t i = 0; i < calc_exprs.count(); ++i) {
        ObExpr *e = calc_exprs.at(i);
        e->get_eval_info(eval_ctx).clear_evaluated_flag();
        // if (OB_NOT_NULL(e)) {
        //   if (e->is_batch_result()) {
        //     e->get_evaluated_flags(eval_ctx).reset(batch_rows->size_);
        //   } else {
        //     e->get_eval_info(eval_ctx).clear_evaluated_flag();
        //   }
        // }
      }

      // Evaluate output expressions if they're different from mock output
      // This is needed for cases like "SELECT a + b" where mock outputs a,b but we need a+b evaluated
      if (output_exprs != nullptr) {
        if (use_row_by_row_eval_) {
          // Row-by-row evaluation for custom eval_func testing
          // eval_batch() uses eval_vector_func_, not eval_func_, so we need eval() for custom eval_func
          // CRITICAL: For row-by-row eval with custom eval_func, we must clear evaluated flags per row
          for (int64_t row = 0; row < batch_rows->size_ && OB_SUCC(ret); ++row) {
            if (batch_rows->skip_->exist(row)) {
              continue;
            }
            eval_ctx.set_batch_idx(row);
            // Clear evaluated flag for each row to force re-evaluation
            // When is_batch_result() is true, we must clear the batch flag at this row index
            for (int64_t col = 0; col < result_exprs->count() && OB_SUCC(ret); ++col) {
              ObExpr *expr = result_exprs->at(col);
              if (OB_NOT_NULL(expr)) {
                if (expr->is_batch_result()) {
                  expr->get_evaluated_flags(eval_ctx).unset(row);
                } else {
                  expr->get_eval_info(eval_ctx).clear_evaluated_flag();
                }
              }
            }
            for (int64_t col = 0; col < result_exprs->count() && OB_SUCC(ret); ++col) {
              ObExpr *expr = result_exprs->at(col);
              if (OB_NOT_NULL(expr)) {
                // Call eval_func_ directly to bypass cached result checks
                // This ensures custom eval_func is actually invoked
                ObDatum &datum = expr->locate_datum_for_write(eval_ctx);
                ret = expr->eval_func_(*expr, eval_ctx, datum);
                if (OB_FAIL(ret)) {
                  LOG_WARN("evaluate expression failed", K(ret), K(col), K(row));
                }
              }
            }
          }
        } else {
          // Batch evaluation (default path)
          for (int64_t col = 0; col < result_exprs->count() && OB_SUCC(ret); ++col) {
            ObExpr *expr = result_exprs->at(col);
            if (OB_NOT_NULL(expr)) {
              // Evaluate the expression for the batch using eval_batch
              ret = expr->eval_batch(eval_ctx, *batch_rows->skip_, batch_rows->size_);
              if (OB_FAIL(ret)) {
                LOG_WARN("evaluate expression failed", K(ret), K(col));
              }
            }
          }
        }
      }

      for (int64_t row = 0; row < batch_rows->size_ && OB_SUCC(ret); ++row) {
        if (batch_rows->skip_->exist(row)) {
          continue;
        }
        // TODO: add decimal int int32/int64/int128/int256/int512 support
        std::vector<std::string> row_values;
        for (int64_t col = 0; col < result_exprs->count(); ++col) {
          ObExpr *expr = result_exprs->at(col);
          if (OB_ISNULL(expr)) {
            row_values.push_back("NULL");
            continue;
          }

          // Use direct buffer access to avoid alignment issues with vector virtual methods
          // Note: expr->get_format() may return VEC_INVALID in some cases,
          // but expr->get_vector()->get_format() returns the actual vector format.
          // We need to use the vector's format for correct data access.
          ObIVector *vec = expr->get_vector(eval_ctx);
          VectorFormat fmt = vec ? vec->get_format() : VEC_INVALID;
          // TODO: fmt should not be VEC_INVALID

          // Check NULL using vector's is_null() method which correctly checks the null bitmap
          if (vec && vec->is_null(row)) {
            row_values.push_back("NULL");
            continue;
          }

          // If format is still invalid, skip
          if (fmt == VEC_INVALID) {
            row_values.push_back("");
            continue;
          }

          // Get value as string using direct buffer access
          const char *payload = nullptr;
          ObLength length = 0;
          char tmp_buf[64];

          if (fmt == VEC_FIXED) {
            // For fixed-length types, data is in res_buf
            char *data = expr->get_res_buf(eval_ctx);
            const ObObjType obj_type = expr->datum_meta_.get_type();
            switch (obj_type) {
            case ObIntType:
            case ObInt32Type: {
              int64_t val = reinterpret_cast<const int64_t *>(data)[row];
              snprintf(tmp_buf, sizeof(tmp_buf), "%ld", val);
              payload = tmp_buf;
              length = strlen(tmp_buf);
              break;
            }
            case ObUInt64Type:
            case ObUInt32Type: {
              uint64_t val = reinterpret_cast<const uint64_t *>(data)[row];
              snprintf(tmp_buf, sizeof(tmp_buf), "%lu", val);
              payload = tmp_buf;
              length = strlen(tmp_buf);
              break;
            }
            case ObDoubleType: {
              double val = reinterpret_cast<const double *>(data)[row];
              snprintf(tmp_buf, sizeof(tmp_buf), "%g", val);
              payload = tmp_buf;
              length = strlen(tmp_buf);
              break;
            }
            case ObFloatType: {
              float val = reinterpret_cast<const float *>(data)[row];
              snprintf(tmp_buf, sizeof(tmp_buf), "%g", val);
              payload = tmp_buf;
              length = strlen(tmp_buf);
              break;
            }
            case ObDecimalIntType: {
              // For decimal int, get precision to calculate int_bytes
              const int16_t precision = expr->datum_meta_.precision_;
              const int32_t int_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(precision);
              const ObDecimalInt *decint = reinterpret_cast<const ObDecimalInt *>(data + row * int_bytes);
              int64_t pos = 0;
              char dec_buf[128];  // Larger buffer for decimal strings
              if (OB_SUCCESS == wide::to_string(decint, int_bytes, expr->datum_meta_.scale_,
                                                  dec_buf, sizeof(dec_buf), pos)) {
                payload = dec_buf;
                length = pos;
              } else {
                payload = "?";
                length = 1;
              }
              break;
            }
            default:
              // For other types, try to interpret as int64
              int64_t val = reinterpret_cast<const int64_t *>(data)[row];
              snprintf(tmp_buf, sizeof(tmp_buf), "%ld", val);
              payload = tmp_buf;
              length = strlen(tmp_buf);
              break;
            }
          } else if (fmt == VEC_CONTINUOUS) {
            // Continuous format: offsets[] + contiguous data buffer
            // offsets[row] points to the start of row's data
            // offsets[row + 1] - offsets[row] is the length
            char *cont_data = expr->get_continuous_vector_data(eval_ctx);
            uint32_t *offsets = expr->get_continuous_vector_offsets(eval_ctx);
            const ObObjType obj_type = expr->datum_meta_.get_type();

            if (obj_type == ObNumberType) {
              const number::ObCompactNumber *cnum =
                reinterpret_cast<const number::ObCompactNumber *>(cont_data + offsets[row]);
              number::ObNumber num(*cnum);
              int64_t pos = 0;
              int fmt_ret = num.format(tmp_buf, sizeof(tmp_buf), pos, -1);
              if (OB_SUCCESS == fmt_ret && pos > 0) {
                tmp_buf[pos] = '\0';
                payload = tmp_buf;
                length = pos;
              } else {
                payload = "?";
                length = 1;
              }
            } else {
              payload = cont_data + offsets[row];
              length = offsets[row + 1] - offsets[row];
            }
          } else if (fmt == VEC_DISCRETE) {
            // For discrete format, ptrs and lens are in separate arrays
            char **ptrs = expr->get_discrete_vector_ptrs(eval_ctx);
            int32_t *lens = expr->get_discrete_vector_lens(eval_ctx);
            const ObObjType obj_type = expr->datum_meta_.get_type();
            // For number type, need to decode ObCompactNumber to string
            if (obj_type == ObNumberType) {
              const number::ObCompactNumber *cnum =
                reinterpret_cast<const number::ObCompactNumber *>(ptrs[row]);
              number::ObNumber num(*cnum);
              int64_t pos = 0;
              int fmt_ret = num.format(tmp_buf, sizeof(tmp_buf), pos, -1);
              if (OB_SUCCESS == fmt_ret && pos > 0) {
                tmp_buf[pos] = '\0';
                payload = tmp_buf;
                length = pos;
              } else {
                payload = "?";
                length = 1;
              }
            } else {
              payload = ptrs[row];
              length = lens[row];
            }
          } else if (fmt == VEC_UNIFORM) {
            // For uniform format, use ObDatum
            ObDatum *datums = expr->locate_batch_datums(eval_ctx);
            const ObObjType obj_type = expr->datum_meta_.get_type();
            // For numeric types, need to convert binary to string
            switch (obj_type) {
            case ObIntType:
            case ObInt32Type: {
              int64_t val = datums[row].get_int();
              snprintf(tmp_buf, sizeof(tmp_buf), "%ld", val);
              payload = tmp_buf;
              length = strlen(tmp_buf);
              break;
            }
            case ObUInt64Type:
            case ObUInt32Type: {
              uint64_t val = datums[row].get_uint();
              snprintf(tmp_buf, sizeof(tmp_buf), "%lu", val);
              payload = tmp_buf;
              length = strlen(tmp_buf);
              break;
            }
            case ObDoubleType: {
              double val = datums[row].get_double();
              snprintf(tmp_buf, sizeof(tmp_buf), "%g", val);
              payload = tmp_buf;
              length = strlen(tmp_buf);
              break;
            }
            case ObFloatType: {
              float val = datums[row].get_float();
              snprintf(tmp_buf, sizeof(tmp_buf), "%g", val);
              payload = tmp_buf;
              length = strlen(tmp_buf);
              break;
            }
            case ObNumberType: {
              // For number type, datum.ptr_ points to ObCompactNumber
              const number::ObCompactNumber *cnum =
                reinterpret_cast<const number::ObCompactNumber *>(datums[row].ptr_);
              // Use ObNumber constructor that takes ObCompactNumber
              number::ObNumber num(*cnum);
              // Use format() method to get actual number string, not debug format
              // format() writes to buffer and returns success/failure
              int64_t pos = 0;
              int fmt_ret = num.format(tmp_buf, sizeof(tmp_buf), pos, -1);  // -1 = auto scale
              if (OB_SUCCESS == fmt_ret && pos > 0) {
                tmp_buf[pos] = '\0';
                payload = tmp_buf;
                length = pos;
              } else {
                payload = "?";
                length = 1;
              }
              break;
            }
            case ObDecimalIntType: {
              // For decimal int, datum.ptr_ points to the decimal int data
              const int16_t precision = expr->datum_meta_.precision_;
              const int32_t int_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(precision);
              const ObDecimalInt *decint = reinterpret_cast<const ObDecimalInt *>(datums[row].ptr_);
              int64_t pos = 0;
              char dec_buf[128];
              if (OB_SUCCESS == wide::to_string(decint, int_bytes, expr->datum_meta_.scale_,
                                                 dec_buf, sizeof(dec_buf), pos)) {
                payload = dec_buf;
                length = pos;
              } else {
                payload = "?";
                length = 1;
              }
              break;
            }
            default:
              // For string types, use the raw pointer
              payload = datums[row].ptr_;
              length = datums[row].len_;
              break;
            }
          } else {
            // VEC_INVALID or flat format - use ObDatum
            // When enable_rich_format() is false, expressions use flat format (ObDatum)
            // The format will be VEC_INVALID in this case
            ObDatum *datums = expr->locate_batch_datums(eval_ctx);
            const ObObjType obj_type = expr->datum_meta_.get_type();
            switch (obj_type) {
            case ObIntType:
            case ObInt32Type: {
              int64_t val = datums[row].get_int();
              snprintf(tmp_buf, sizeof(tmp_buf), "%ld", val);
              payload = tmp_buf;
              length = strlen(tmp_buf);
              break;
            }
            case ObUInt64Type:
            case ObUInt32Type: {
              uint64_t val = datums[row].get_uint();
              snprintf(tmp_buf, sizeof(tmp_buf), "%lu", val);
              payload = tmp_buf;
              length = strlen(tmp_buf);
              break;
            }
            case ObDoubleType: {
              double val = datums[row].get_double();
              snprintf(tmp_buf, sizeof(tmp_buf), "%g", val);
              payload = tmp_buf;
              length = strlen(tmp_buf);
              break;
            }
            case ObFloatType: {
              float val = datums[row].get_float();
              snprintf(tmp_buf, sizeof(tmp_buf), "%g", val);
              payload = tmp_buf;
              length = strlen(tmp_buf);
              break;
            }
            case ObNumberType: {
              // For number type, datum.ptr_ points to ObCompactNumber
              const number::ObCompactNumber *cnum =
                reinterpret_cast<const number::ObCompactNumber *>(datums[row].ptr_);
              number::ObNumber num(*cnum);
              int64_t pos = 0;
              int fmt_ret = num.format(tmp_buf, sizeof(tmp_buf), pos, -1);
              if (OB_SUCCESS == fmt_ret && pos > 0) {
                tmp_buf[pos] = '\0';
                payload = tmp_buf;
                length = pos;
              } else {
                payload = "?";
                length = 1;
              }
              break;
            }
            case ObDecimalIntType: {
              // For decimal int, datum.ptr_ points to the decimal int data
              const int16_t precision = expr->datum_meta_.precision_;
              const int32_t int_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(precision);
              const ObDecimalInt *decint = reinterpret_cast<const ObDecimalInt *>(datums[row].ptr_);
              int64_t pos = 0;
              char dec_buf[128];
              if (OB_SUCCESS == wide::to_string(decint, int_bytes, expr->datum_meta_.scale_,
                                                 dec_buf, sizeof(dec_buf), pos)) {
                payload = dec_buf;
                length = pos;
              } else {
                payload = "?";
                length = 1;
              }
              break;
            }
            case ObVarcharType:
            case ObTextType:
            case ObLongTextType:
            case ObCharType: {
              // For string types, use the raw pointer
              payload = datums[row].ptr_;
              length = datums[row].len_;
              break;
            }
            default:
              // For unknown types, try to use raw data
              payload = datums[row].ptr_;
              length = datums[row].len_;
              if (nullptr == payload || 0 == length) {
                // Fallback: try as int
                int64_t val = datums[row].get_int();
                snprintf(tmp_buf, sizeof(tmp_buf), "%ld", val);
                payload = tmp_buf;
                length = strlen(tmp_buf);
              }
              break;
            }
          }

          row_values.push_back(std::string(payload, length));
        }
        // Handle based on dump_verify_mode_
        if (dump_verify_mode_ == DumpVerifyMode::FULL_ROWS) {
          // Default: collect all rows in memory
          result.add_row(row_values);
        } else if (dump_verify_mode_ == DumpVerifyMode::CHECKSUM) {
          // For large data: calculate checksum incrementally
          for (const auto &cell : row_values) {
            uint64_t h = 0;
            for (char c : cell) {
              h = h * 31 + static_cast<uint64_t>(c);
            }
            running_checksum ^= h + 0x9e3779b9 + (running_checksum << 6) + (running_checksum >> 2);
          }
          ++total_row_count;
        }
        // DumpVerifyMode::NONE: just iterate, no storage
      }
    }

    // Check end_ flag after collecting data
    if (batch_rows->end_) {
      break;
    }
  }

  // For CHECKSUM mode, set the computed checksum and row count in result
  if (dump_verify_mode_ == DumpVerifyMode::CHECKSUM) {
    result.set_checksum_info(running_checksum, total_row_count);
  } else if (dump_verify_mode_ == DumpVerifyMode::NONE) {
  }

  return result;
}

int64_t OpTestEngine::get_memory_hold_bytes() const
{
  int64_t memory_hold = 0;
  // Get tenant ID from the tenant context
  ObTenantBase *tenant_ctx = ObTenantEnv::get_tenant_local();
  if (OB_NOT_NULL(tenant_ctx) && tenant_ctx->id_ != OB_INVALID_TENANT_ID) {
    memory_hold = lib::get_tenant_memory_hold(tenant_ctx->id_);
  }
  return memory_hold;
}

OpTestResult OpTestEngine::execute(ObOperator *op, const ExprFixedArray *output_exprs,
                                   int64_t rescan_times)
{
  OpTestResult result;
  int ret = OB_SUCCESS;

  if (OB_ISNULL(op)) {
    LOG_WARN("op is null");
    return result;
  }

  // Open operator
  ret = op->open();
  if (OB_FAIL(ret)) {
    LOG_WARN("open operator failed", K(ret));
    return result;
  }

  // Collect first result
  OpTestResult first_result = collect_batch_results(op, output_exprs);

  // Rescan memory tracking
  RescanMemoryInfo memory_info;
  if (rescan_memory_check_ && rescan_times > 0) {
    // Record memory after first scan
    memory_info.memory_after_first_scan_ = get_memory_hold_bytes();
  }

  // Rescan loop if requested
  for (int64_t i = 0; i < rescan_times && OB_SUCC(ret); ++i) {
    ret = op->rescan();
    if (OB_FAIL(ret)) {
      LOG_WARN("rescan operator failed", K(ret), K(i));
      break;
    }

    OpTestResult rescan_result = collect_batch_results(op, output_exprs);

    // Verify rescan produces the same results
    if (!first_result.equals(rescan_result)) {
      LOG_WARN("rescan result mismatch", K(i),
               "first_result", first_result.to_string().c_str(),
               "rescan_result", rescan_result.to_string().c_str());
      // Continue with other rescans even if this one failed
    }

    // Track memory after each rescan
    if (rescan_memory_check_ && rescan_times > 0) {
      int64_t memory_after_rescan = get_memory_hold_bytes();

      // Update min/max
      if (memory_info.rescan_count_ == 0) {
        memory_info.min_memory_after_rescan_ = memory_after_rescan;
        memory_info.max_memory_after_rescan_ = memory_after_rescan;
      } else {
        memory_info.min_memory_after_rescan_ = std::min(memory_info.min_memory_after_rescan_, memory_after_rescan);
        memory_info.max_memory_after_rescan_ = std::max(memory_info.max_memory_after_rescan_, memory_after_rescan);
      }
      memory_info.rescan_count_++;

      // Check memory consistency
      // Tolerance: max(first_scan * 10%, 64KB) if not explicitly set
      // Tolarance should be a fixed size, not percentage
      int64_t tolerance = rescan_memory_tolerance_bytes_;
      if (tolerance == 0) {
        tolerance = std::max(memory_info.memory_after_first_scan_ / 10, 64 * 1024L);
      }
      if (memory_after_rescan > memory_info.memory_after_first_scan_ + tolerance) {
        memory_info.memory_consistent_ = false;
        LOG_WARN("Rescan memory inconsistency detected",
                 K(i),
                 "memory_after_first_scan", memory_info.memory_after_first_scan_,
                 "memory_after_rescan", memory_after_rescan,
                 K(tolerance),
                 "excess_bytes", memory_after_rescan - memory_info.memory_after_first_scan_);
      }
    }
  }

  // Set memory info in result
  if (rescan_memory_check_ && rescan_times > 0) {
    first_result.set_rescan_memory_info(memory_info);
    LOG_INFO("Rescan memory check completed",
             "memory_after_first_scan", memory_info.memory_after_first_scan_,
             "min_memory_after_rescan", memory_info.min_memory_after_rescan_,
             "max_memory_after_rescan", memory_info.max_memory_after_rescan_,
             "rescan_count", memory_info.rescan_count_,
             "memory_consistent", memory_info.memory_consistent_);
  }

  // Close operator
  int close_ret = op->close();
  if (OB_SUCCESS != close_ret) {
    LOG_WARN("close operator failed", K(close_ret));
  }

  return first_result;
}

}  // namespace sql
}  // namespace oceanbase