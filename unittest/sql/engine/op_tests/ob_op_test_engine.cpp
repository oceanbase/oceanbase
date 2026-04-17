/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
// Allow access to private members of ObTenantBase for test setup
#include "lib/ob_errno.h"
#define private public
#define protected public

#define USING_LOG_PREFIX SQL

#include "unittest/sql/engine/op_tests/ob_op_test_engine.h"
#include <algorithm>  // for std::transform
#include <cctype>     // for ::tolower
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
#include "share/vector/ob_vector_define.h"  // for VecValueTypeClass, get_vec_value_tc
#include "lib/number/ob_number_v2.h"
#include "lib/json_type/ob_json_base.h"  // for ObJsonBaseFactory, ObIJsonBase, ObJsonBuffer
#include "lib/worker.h"  // for CompatMode, set_compat_mode
#include "storage/tmp_file/ob_tmp_file_manager.h"  // for ObTenantTmpFileManager
#include "sql/rewrite/ob_expand_aggregate_utils.h"  // for ObExpandAggregateUtils
#include "unittest/sql/engine/op_tests/ob_op_test_base.h"

namespace oceanbase
{
namespace sql
{

// ===== TestDefaultParameterConf Implementation =====

TestDefaultParameterConf &TestDefaultParameterConf::instance()
{
  static thread_local TestDefaultParameterConf inst;
  return inst;
}

int TestDefaultParameterConf::set(const std::string &name, const std::string &value)
{
  int ret = OB_SUCCESS;  // Required by LOG_WARN macro
  FatalErrorChecker ret_checker(ret);
  // Step 1: Get sys tenant config for validation
  omt::ObTenantConfigGuard tc(TENANT_CONF(OB_SYS_TENANT_ID));
  if (!tc.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Tenant config not available. Call set() after engine.init()");
    return ret;
  }

  // Step 2: Validate name exists
  ObConfigStringKey key(name.c_str());
  ObConfigItem *item = nullptr;
  ret = tc->container_.get_refactored(key, item);
  if (OB_SUCCESS != ret || OB_ISNULL(item)) {
    if (OB_SUCC(ret)) { ret = OB_ERR_UNEXPECTED; }
    LOG_WARN("Unknown tenant config item", "name", name.c_str(), K(ret));
    return ret;
  }

  // Step 3: Validate value by trial set then restore
  // Save current value (value_ptr() is protected virtual, accessible via #define private public)
  char saved_buf[OB_MAX_CONFIG_VALUE_LEN];
  const char *cur_val = item->value_ptr();
  STRNCPY(saved_buf, cur_val, sizeof(saved_buf));
  saved_buf[sizeof(saved_buf) - 1] = '\0';

  bool valid = item->set_value(value.c_str());
  // Restore original value regardless of validation result
  item->set_value(saved_buf);

  if (!valid) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Invalid value for tenant config", "name", name.c_str(), "value", value.c_str());
    return ret;
  }

  // Step 4: Validation passed, store override
  overrides_[name] = value;
  return ret;
}

// ===== OpTestEngine Implementation =====

OpTestEngine::OpTestEngine()
  : inited_(false),
    phy_plan_ctx_(nullptr)
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

  // Set my_session on exec_ctx so that ctx_.get_my_session() works for hash join
  // and other operators that need session access during open()
  exec_ctx_.set_my_session(&session_info_);

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
  FatalErrorChecker error_checker(ret);

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

  // Step 1.5: Add tenant config for the session's effective tenant ID.
  // ObHashJoinVecOp::inner_open() calls TENANT_CONF(session->get_effective_tenant_id()).
  // The test framework sets effective_tenant_id_ = 1 in init_schema(), but the
  // constructor only adds config for sys_tenant_id_ (OB_SYS_TENANT_ID).
  // We must ensure a config exists for tenant_id=1.
  {
    uint64_t effective_tenant_id = session_info_.get_effective_tenant_id();
    if (effective_tenant_id != OB_INVALID_TENANT_ID) {
      // Check if config already exists
      omt::ObTenantConfigGuard tc(TENANT_CONF(effective_tenant_id));
      if (!tc.is_valid()) {
        int add_ret = OB_SUCCESS;
        if (OB_FAIL(omt::ObTenantConfigMgr::get_instance().add_tenant_config(effective_tenant_id))) {
          LOG_WARN("failed to add tenant config for effective tenant", K(add_ret), K(effective_tenant_id));
        } else {
          LOG_INFO("Added tenant config for effective tenant", K(effective_tenant_id));
        }
      }
      // Apply TLS tenant config overrides (e.g., _hash_area_size, _enable_hash_join_processor)
      // _enable_hash_join_processor default is "7" from parameter_seed.ipp, no need to hardcode
      apply_tenant_config_overrides();
    }
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

void OpTestEngine::apply_tenant_config_overrides()
{
  const auto &overrides = TestDefaultParameterConf::instance().overrides_;
  if (overrides.empty()) return;

  uint64_t effective_tenant_id = session_info_.get_effective_tenant_id();
  if (effective_tenant_id == OB_INVALID_TENANT_ID) return;

  omt::ObTenantConfigGuard tc(TENANT_CONF(effective_tenant_id));
  if (!tc.is_valid()) return;

  for (const auto &kv : overrides) {
    ObConfigStringKey key(kv.first.c_str());
    ObConfigItem *item = nullptr;
    if (OB_SUCCESS == tc->container_.get_refactored(key, item)
        && OB_NOT_NULL(item)) {
      item->set_value(kv.second.c_str());
      LOG_INFO("Applied tenant config override",
               "name", kv.first.c_str(), "value", kv.second.c_str());
    }
  }
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
  FatalErrorChecker error_checker(ret);

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

    // Parse type: lowercase for matching, keep original for parameter extraction
    std::string col_type_lower = col_type;
    std::transform(col_type_lower.begin(), col_type_lower.end(), col_type_lower.begin(), ::tolower);

    // Check for unsigned suffix
    bool is_unsigned = false;
    {
      const std::string unsigned_kw = "unsigned";
      size_t us_pos = col_type_lower.rfind(unsigned_kw);
      if (us_pos != std::string::npos) {
        // Ensure "unsigned" is a trailing token (not part of another word)
        size_t after_us = us_pos + unsigned_kw.size();
        if (after_us == col_type_lower.size()) {
          is_unsigned = true;
          col_type_lower = col_type_lower.substr(0, us_pos);
          // Trim trailing whitespace
          while (!col_type_lower.empty() && isspace(col_type_lower.back())) col_type_lower.pop_back();
          // Also trim from original col_type for parameter extraction
          col_type = col_type.substr(0, us_pos);
          while (!col_type.empty() && isspace(col_type.back())) col_type.pop_back();
        }
      }
    }

    // Helper: extract parenthesized args from original col_type
    // Returns the content inside the first pair of parentheses, or empty string
    auto extract_paren_args = [&col_type]() -> std::string {
      size_t paren_pos = col_type.find('(');
      if (paren_pos == std::string::npos) return "";
      size_t end_paren = col_type.find(')', paren_pos);
      if (end_paren == std::string::npos) return "";
      return col_type.substr(paren_pos + 1, end_paren - paren_pos - 1);
    };

    // Helper: extract first integer from parenthesized args
    auto extract_paren_int = [&col_type]() -> int {
      size_t paren_pos = col_type.find('(');
      if (paren_pos == std::string::npos) return -1;
      size_t end_paren = col_type.find(')', paren_pos);
      if (end_paren == std::string::npos) return -1;
      try {
        return std::stoi(col_type.substr(paren_pos + 1, end_paren - paren_pos - 1));
      } catch (...) { return -1; }
    };

    // Match types from most specific to least specific
    // Integer types (ordered by length/specificity to avoid substring mis-matches)
    if (col_type_lower.find("mediumint") != std::string::npos) {
      column_schema.set_data_type(is_unsigned ? ObUMediumIntType : ObMediumIntType);
      column_schema.set_nullable(true);
    } else if (col_type_lower.find("smallint") != std::string::npos) {
      column_schema.set_data_type(is_unsigned ? ObUSmallIntType : ObSmallIntType);
      column_schema.set_nullable(true);
    } else if (col_type_lower.find("tinyint") != std::string::npos) {
      column_schema.set_data_type(is_unsigned ? ObUTinyIntType : ObTinyIntType);
      column_schema.set_nullable(true);
    } else if (col_type_lower.find("int32") != std::string::npos) {
      column_schema.set_data_type(is_unsigned ? ObUInt32Type : ObInt32Type);
      column_schema.set_nullable(true);
    } else if (col_type_lower.find("bigint") != std::string::npos) {
      column_schema.set_data_type(is_unsigned ? ObUInt64Type : ObIntType);
      column_schema.set_nullable(true);
    } else if (col_type_lower.find("int") != std::string::npos) {
      column_schema.set_data_type(is_unsigned ? ObUInt64Type : ObIntType);
      column_schema.set_nullable(true);
    // Float/Double types
    } else if (col_type_lower.find("ufloat") != std::string::npos) {
      column_schema.set_data_type(ObUFloatType);
      column_schema.set_nullable(true);
    } else if (col_type_lower.find("udouble") != std::string::npos) {
      column_schema.set_data_type(ObUDoubleType);
      column_schema.set_nullable(true);
    } else if (col_type_lower.find("float") != std::string::npos) {
      column_schema.set_data_type(is_unsigned ? ObUFloatType : ObFloatType);
      column_schema.set_nullable(true);
    } else if (col_type_lower.find("double") != std::string::npos) {
      column_schema.set_data_type(is_unsigned ? ObUDoubleType : ObDoubleType);
      // Parse double(M, S) if present
      std::string args = extract_paren_args();
      if (!args.empty()) {
        size_t comma_pos = args.find(',');
        if (comma_pos != std::string::npos) {
          column_schema.set_data_length(std::stoi(args.substr(0, comma_pos)));
          column_schema.set_data_scale(std::stoi(args.substr(comma_pos + 1)));
        } else {
          column_schema.set_data_length(std::stoi(args));
        }
      }
      column_schema.set_nullable(true);
    // Number types
    } else if (col_type_lower.find("number_float") != std::string::npos) {
      column_schema.set_data_type(ObNumberFloatType);
      column_schema.set_nullable(true);
    } else if (col_type_lower.find("unumber") != std::string::npos) {
      column_schema.set_data_type(ObUNumberType);
      column_schema.set_nullable(true);
    } else if (col_type_lower.find("number") != std::string::npos) {
      // Parse number(P, S) if present
      std::string args = extract_paren_args();
      if (!args.empty()) {
        size_t comma_pos = args.find(',');
        if (comma_pos != std::string::npos) {
          int precision = std::stoi(args.substr(0, comma_pos));
          int scale = std::stoi(args.substr(comma_pos + 1));
          column_schema.set_data_type(ObDecimalIntType);
          column_schema.set_data_precision(precision);
          column_schema.set_data_scale(scale);
        } else {
          int precision = std::stoi(args);
          column_schema.set_data_type(ObDecimalIntType);
          column_schema.set_data_precision(precision);
          column_schema.set_data_scale(0);
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
          column_schema.set_data_type(is_unsigned ? ObUNumberType : ObNumberType);
        }
      }
      column_schema.set_nullable(true);
    } else if (col_type_lower.find("decimal") != std::string::npos) {
      // Parse decimal(P, S) if present
      std::string args = extract_paren_args();
      if (!args.empty()) {
        size_t comma_pos = args.find(',');
        if (comma_pos != std::string::npos) {
          int precision = std::stoi(args.substr(0, comma_pos));
          int scale = std::stoi(args.substr(comma_pos + 1));
          column_schema.set_data_type(ObDecimalIntType);
          column_schema.set_data_precision(precision);
          column_schema.set_data_scale(scale);
        } else {
          int precision = std::stoi(args);
          column_schema.set_data_type(ObDecimalIntType);
          column_schema.set_data_precision(precision);
          column_schema.set_data_scale(0);
        }
      } else {
        // decimal without precision/scale
        column_schema.set_data_type(ObDecimalIntType);
      }
      column_schema.set_nullable(true);
    // Timestamp types (ordered by specificity)
    } else if (col_type_lower.find("timestamp_tz") != std::string::npos) {
      column_schema.set_data_type(ObTimestampTZType);
      column_schema.set_data_scale(9);  // nanosecond precision for Oracle timestamp
      column_schema.set_nullable(true);
    } else if (col_type_lower.find("timestamp_ltz") != std::string::npos) {
      column_schema.set_data_type(ObTimestampLTZType);
      column_schema.set_data_scale(9);
      column_schema.set_nullable(true);
    } else if (col_type_lower.find("timestamp_nano") != std::string::npos) {
      column_schema.set_data_type(ObTimestampNanoType);
      column_schema.set_data_scale(9);
      column_schema.set_nullable(true);
    } else if (col_type_lower.find("timestamp") != std::string::npos) {
      column_schema.set_data_type(ObTimestampType);
      column_schema.set_data_scale(6);  // microsecond precision
      column_schema.set_nullable(true);
    // Datetime/Date/Time/Year types
    } else if (col_type_lower.find("mysql_datetime") != std::string::npos) {
      column_schema.set_data_type(ObMySQLDateTimeType);
      column_schema.set_data_scale(6);
      column_schema.set_nullable(true);
    } else if (col_type_lower.find("mysql_date") != std::string::npos) {
      column_schema.set_data_type(ObMySQLDateType);
      column_schema.set_nullable(true);
    } else if (col_type_lower.find("datetime") != std::string::npos) {
      column_schema.set_data_type(ObDateTimeType);
      column_schema.set_data_scale(6);  // microsecond precision
      column_schema.set_nullable(true);
    } else if (col_type_lower.find("date") != std::string::npos) {
      column_schema.set_data_type(ObDateType);
      column_schema.set_nullable(true);
    } else if (col_type_lower.find("time") != std::string::npos) {
      column_schema.set_data_type(ObTimeType);
      column_schema.set_data_scale(6);  // microsecond precision
      column_schema.set_nullable(true);
    } else if (col_type_lower.find("year") != std::string::npos) {
      column_schema.set_data_type(ObYearType);
      column_schema.set_nullable(true);
    // Blob types (must come before text types, and more specific before less specific)
    } else if (col_type_lower.find("longblob") != std::string::npos) {
      column_schema.set_data_type(ObLongTextType);
      column_schema.set_collation_type(CS_TYPE_BINARY);
      column_schema.set_nullable(true);
    } else if (col_type_lower.find("mediumblob") != std::string::npos) {
      column_schema.set_data_type(ObMediumTextType);
      column_schema.set_collation_type(CS_TYPE_BINARY);
      column_schema.set_nullable(true);
    } else if (col_type_lower.find("tinyblob") != std::string::npos) {
      column_schema.set_data_type(ObTinyTextType);
      column_schema.set_collation_type(CS_TYPE_BINARY);
      column_schema.set_nullable(true);
    } else if (col_type_lower.find("blob") != std::string::npos) {
      column_schema.set_data_type(ObTextType);
      int len = extract_paren_int();
      column_schema.set_data_length(len > 0 ? len : 65535);
      column_schema.set_collation_type(CS_TYPE_BINARY);
      column_schema.set_nullable(true);
    // Text types (ordered by specificity)
    } else if (col_type_lower.find("longtext") != std::string::npos) {
      column_schema.set_data_type(ObLongTextType);
      column_schema.set_data_length(65535);
      column_schema.set_nullable(true);
    } else if (col_type_lower.find("mediumtext") != std::string::npos) {
      column_schema.set_data_type(ObMediumTextType);
      column_schema.set_data_length(65535);
      column_schema.set_nullable(true);
    } else if (col_type_lower.find("tinytext") != std::string::npos) {
      column_schema.set_data_type(ObTinyTextType);
      int len = extract_paren_int();
      column_schema.set_data_length(len > 0 ? len : 65535);
      column_schema.set_nullable(true);
    } else if (col_type_lower.find("text") != std::string::npos) {
      column_schema.set_data_type(ObTextType);
      int len = extract_paren_int();
      column_schema.set_data_length(len > 0 ? len : 65535);
      column_schema.set_nullable(true);
    // Character string types (ordered by specificity)
    } else if (col_type_lower.find("nvarchar2") != std::string::npos) {
      column_schema.set_data_type(ObNVarchar2Type);
      int len = extract_paren_int();
      if (len > 0) column_schema.set_data_length(len);
      column_schema.set_nullable(true);
    } else if (col_type_lower.find("nchar") != std::string::npos) {
      column_schema.set_data_type(ObNCharType);
      int len = extract_paren_int();
      if (len > 0) column_schema.set_data_length(len);
      column_schema.set_nullable(true);
    } else if (col_type_lower.find("varchar") != std::string::npos) {
      column_schema.set_data_type(ObVarcharType);
      int len = extract_paren_int();
      if (len > 0) column_schema.set_data_length(len);
      column_schema.set_nullable(true);
    } else if (col_type_lower.find("char") != std::string::npos) {
      column_schema.set_data_type(ObCharType);
      int len = extract_paren_int();
      if (len > 0) column_schema.set_data_length(len);
      column_schema.set_nullable(true);
    // Special types
    } else if (col_type_lower.find("hex_string") != std::string::npos) {
      column_schema.set_data_type(ObHexStringType);
      column_schema.set_nullable(true);
    } else if (col_type_lower.find("raw") != std::string::npos) {
      column_schema.set_data_type(ObRawType);
      int len = extract_paren_int();
      if (len > 0) column_schema.set_data_length(len);
      column_schema.set_nullable(true);
    // Interval types
    } else if (col_type_lower.find("interval_ym") != std::string::npos) {
      column_schema.set_data_type(ObIntervalYMType);
      column_schema.set_nullable(true);
    } else if (col_type_lower.find("interval_ds") != std::string::npos) {
      column_schema.set_data_type(ObIntervalDSType);
      column_schema.set_nullable(true);
    // JSON/Geometry/RoaringBitmap types (before bit/set to avoid substring matches)
    } else if (col_type_lower.find("roaringbitmap") != std::string::npos) {
      column_schema.set_data_type(ObRoaringBitmapType);
      column_schema.set_nullable(true);
    } else if (col_type_lower.find("json") != std::string::npos) {
      column_schema.set_data_type(ObJsonType);
      column_schema.set_nullable(true);
    } else if (col_type_lower.find("geometry") != std::string::npos) {
      column_schema.set_data_type(ObGeometryType);
      column_schema.set_nullable(true);
    // Bit type: parse bit(N), default N=1 (after roaringbitmap to avoid substring match)
    } else if (col_type_lower.find("bit") != std::string::npos) {
      column_schema.set_data_type(ObBitType);
      int len = extract_paren_int();
      column_schema.set_data_length(len > 0 ? len : 1);
      column_schema.set_nullable(true);
    // Enum/Set types
    } else if (col_type_lower.find("enum") != std::string::npos) {
      column_schema.set_data_type(ObEnumType);
      column_schema.set_data_length(1);
      column_schema.set_nullable(true);
    } else if (col_type_lower.find("set") != std::string::npos) {
      column_schema.set_data_type(ObSetType);
      column_schema.set_data_length(1);
      column_schema.set_nullable(true);
    // Lob/URowID/UDT/Collection types
    } else if (col_type_lower.find("lob") != std::string::npos) {
      column_schema.set_data_type(ObLobType);
      column_schema.set_nullable(true);
    } else if (col_type_lower.find("urowid") != std::string::npos) {
      column_schema.set_data_type(ObURowIDType);
      column_schema.set_nullable(true);
    } else if (col_type_lower.find("udt") != std::string::npos) {
      column_schema.set_data_type(ObUserDefinedSQLType);
      column_schema.set_nullable(true);
    } else if (col_type_lower.find("collection") != std::string::npos) {
      column_schema.set_data_type(ObCollectionSQLType);
      column_schema.set_nullable(true);
    } else {
      // Default to varchar
      column_schema.set_data_type(ObVarcharType);
      int len = extract_paren_int();
      if (len > 0) column_schema.set_data_length(len);
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
  FatalErrorChecker error_checker(ret);
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
  FatalErrorChecker error_checker(ret);

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
        // Also collect aggr params - needed when aggr is expanded (e.g., AVG -> SUM/COUNT)
        // The expanded aggr params may be new expressions not in select items
        const ObIArray<ObRawExpr *> &params = aggr_items.at(i)->get_real_param_exprs();
        for (int64_t j = 0; OB_SUCC(ret) && j < params.count(); ++j) {
          if (OB_NOT_NULL(params.at(j))) {
            if (OB_FAIL(exprs.push_back(params.at(j)))) {
              LOG_WARN("failed to push aggr param", K(ret), K(i), K(j));
            }
          }
        }
        // Also collect ORDER BY expressions inside aggregate (for GROUP_CONCAT, etc.)
        const ObIArray<OrderItem> &aggr_order_items = aggr_items.at(i)->get_order_items();
        for (int64_t j = 0; OB_SUCC(ret) && j < aggr_order_items.count(); ++j) {
          if (OB_NOT_NULL(aggr_order_items.at(j).expr_)) {
            if (OB_FAIL(exprs.push_back(aggr_order_items.at(j).expr_))) {
              LOG_WARN("failed to push aggr order item", K(ret), K(i), K(j));
            }
          }
        }
        // Also collect separator expression for GROUP_CONCAT
        ObRawExpr *separator_expr = aggr_items.at(i)->get_separator_param_expr();
        if (OB_SUCC(ret) && OB_NOT_NULL(separator_expr)) {
          if (OB_FAIL(exprs.push_back(separator_expr))) {
            LOG_WARN("failed to push aggr separator expr", K(ret), K(i));
          }
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
  FatalErrorChecker error_checker(ret);

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

  // Step 0: Expand aggregate expressions (AVG -> SUM/COUNT, STDDEV, VAR_POP, etc.)
  // This is required for complex aggregate functions that are transformed into simpler ones.
  // Without this step, the aggregate processor will reject T_FUN_AVG and other complex types.
  bool trans_happened = false;
  ObExpandAggregateUtils expand_utils(expr_factory_, &session_info_);
  if (OB_FAIL(expand_utils.expand_aggr_expr(&stmt, trans_happened))) {
    LOG_WARN("failed to expand aggregate expressions", K(ret));
    return ret;
  }

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

  // Add pending raw exprs (e.g., grouping_id pseudo column from Expand operator)
  for (size_t i = 0; i < pending_raw_exprs_.size() && OB_SUCC(ret); ++i) {
    if (OB_FAIL(raw_exprs.append(pending_raw_exprs_.at(i)))) {
      LOG_WARN("append pending raw expr failed", K(ret));
      return ret;
    }
  }
  pending_raw_exprs_.clear();

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

  // Create and set ObPhysicalPlanCtx for operators that need it (e.g., GROUP_CONCAT with ORDER BY)
  // This must be set before operator execution
  if (OB_ISNULL(phy_plan_ctx_)) {
    void *ctx_mem = allocator_.alloc(sizeof(ObPhysicalPlanCtx));
    if (OB_ISNULL(ctx_mem)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc phy_plan_ctx failed", K(ret));
      return ret;
    }
    phy_plan_ctx_ = new (ctx_mem) ObPhysicalPlanCtx(allocator_);
  }
  exec_ctx_.set_physical_plan_ctx(phy_plan_ctx_);
  exec_ctx_.reference_my_plan(&phy_plan_);
  // Set phy_plan on phy_plan_ctx so operators (e.g., ObHashJoinVecOp::init_join_table_ctx)
  // can access it via get_phy_plan()
  phy_plan_ctx_->set_phy_plan(&phy_plan_);
  // Set min_cluster_version so operators can query cluster capabilities
  // (e.g., ObHashJoinVecOp::init_join_table_ctx checks >= CLUSTER_VERSION_4_3_5_0)
  phy_plan_.set_min_cluster_version(CLUSTER_CURRENT_VERSION);

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

namespace {

int json_bin_to_text(common::ObIAllocator &allocator, const char *bin_data, int32_t bin_len,
                     char *out_buf, int64_t out_buf_size, int32_t &out_len)
{
  int ret = OB_SUCCESS;
  common::ObString json_bin(bin_len, bin_data);
  common::ObIJsonBase *j_base = nullptr;
  if (OB_FAIL(common::ObJsonBaseFactory::get_json_base(
        &allocator, json_bin, common::ObJsonInType::JSON_BIN,
        common::ObJsonInType::JSON_TREE, j_base))) {
    return ret;
  }
  common::ObJsonBuffer j_buf(&allocator);
  if (OB_FAIL(j_base->print(j_buf, true))) {
    return ret;
  }
  out_len = static_cast<int32_t>(std::min(j_buf.length(), static_cast<uint64_t>(out_buf_size - 1)));
  MEMCPY(out_buf, j_buf.ptr(), out_len);
  out_buf[out_len] = '\0';
  return ret;
}

}  // anonymous namespace

OpTestResult OpTestEngine::collect_batch_results(ObOperator *op, const ExprFixedArray *output_exprs)
{
  OpTestResult result;
  int ret = OB_SUCCESS;
  FatalErrorChecker error_checker(ret);

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

      // In 1.0 mode (no rich format), operators fill datum store but do not
      // initialize vector headers. We must call init_vector(VEC_UNIFORM) so
      // that get_vector() returns a valid VEC_UNIFORM vector rather than VEC_INVALID.
      if (!op->get_spec().use_rich_format_) {
        for (int64_t col = 0; col < result_exprs->count() && OB_SUCC(ret); ++col) {
          ObExpr *expr = result_exprs->at(col);
          if (OB_NOT_NULL(expr)) {
            VectorFormat fmt = expr->is_batch_result() ? VEC_UNIFORM : VEC_UNIFORM_CONST;
            if (OB_FAIL(expr->init_vector(eval_ctx, fmt, batch_rows->size_))) {
              LOG_WARN("init vector failed", K(ret), K(col));
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
          EXPECT_NE(fmt, VEC_INVALID);

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
            const VecValueTypeClass vec_tc = get_vec_value_tc(obj_type,
                expr->datum_meta_.scale_, expr->datum_meta_.precision_);

            switch (vec_tc) {
            case VEC_TC_INTEGER: {
              int64_t val = reinterpret_cast<const int64_t *>(data)[row];
              snprintf(tmp_buf, sizeof(tmp_buf), "%ld", val);
              payload = tmp_buf;
              length = strlen(tmp_buf);
              break;
            }
            case VEC_TC_UINTEGER:
            case VEC_TC_BIT:
            case VEC_TC_ENUM_SET: {
              uint64_t val = reinterpret_cast<const uint64_t *>(data)[row];
              snprintf(tmp_buf, sizeof(tmp_buf), "%lu", val);
              payload = tmp_buf;
              length = strlen(tmp_buf);
              break;
            }
            case VEC_TC_FLOAT: {
              float val = reinterpret_cast<const float *>(data)[row];
              snprintf(tmp_buf, sizeof(tmp_buf), "%g", val);
              payload = tmp_buf;
              length = strlen(tmp_buf);
              break;
            }
            case VEC_TC_DOUBLE:
            case VEC_TC_FIXED_DOUBLE: {
              double val = reinterpret_cast<const double *>(data)[row];
              snprintf(tmp_buf, sizeof(tmp_buf), "%g", val);
              payload = tmp_buf;
              length = strlen(tmp_buf);
              break;
            }
            case VEC_TC_DATETIME:
            case VEC_TC_TIME:
            case VEC_TC_INTERVAL_YM:
            case VEC_TC_MYSQL_DATETIME: {
              int64_t val = reinterpret_cast<const int64_t *>(data)[row];
              snprintf(tmp_buf, sizeof(tmp_buf), "%ld", val);
              payload = tmp_buf;
              length = strlen(tmp_buf);
              break;
            }
            case VEC_TC_DATE:
            case VEC_TC_MYSQL_DATE: {
              int32_t val = reinterpret_cast<const int32_t *>(data)[row];
              snprintf(tmp_buf, sizeof(tmp_buf), "%d", val);
              payload = tmp_buf;
              length = strlen(tmp_buf);
              break;
            }
            case VEC_TC_YEAR: {
              uint8_t val = reinterpret_cast<const uint8_t *>(data)[row];
              snprintf(tmp_buf, sizeof(tmp_buf), "%u", val);
              payload = tmp_buf;
              length = strlen(tmp_buf);
              break;
            }
            case VEC_TC_TIMESTAMP_TZ: {
              const ObOTimestampData *ts = reinterpret_cast<const ObOTimestampData *>(
                  data + row * sizeof(ObOTimestampData));
              snprintf(tmp_buf, sizeof(tmp_buf), "%ld", ts->time_us_);
              payload = tmp_buf;
              length = strlen(tmp_buf);
              break;
            }
            case VEC_TC_TIMESTAMP_TINY: {
              const ObOTimestampTinyData *ts = reinterpret_cast<const ObOTimestampTinyData *>(
                  data + row * sizeof(ObOTimestampTinyData));
              snprintf(tmp_buf, sizeof(tmp_buf), "%ld", ts->time_us_);
              payload = tmp_buf;
              length = strlen(tmp_buf);
              break;
            }
            case VEC_TC_INTERVAL_DS: {
              const ObIntervalDSValue *ds = reinterpret_cast<const ObIntervalDSValue *>(
                  data + row * sizeof(ObIntervalDSValue));
              snprintf(tmp_buf, sizeof(tmp_buf), "%ld", ds->nsecond_);
              payload = tmp_buf;
              length = strlen(tmp_buf);
              break;
            }
            case VEC_TC_DEC_INT32:
            case VEC_TC_DEC_INT64:
            case VEC_TC_DEC_INT128:
            case VEC_TC_DEC_INT256:
            case VEC_TC_DEC_INT512: {
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
            case VEC_TC_UNKNOWN: {
              int64_t val = reinterpret_cast<const int64_t *>(data)[row];
              snprintf(tmp_buf, sizeof(tmp_buf), "%ld", val);
              payload = tmp_buf;
              length = strlen(tmp_buf);
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
            const VecValueTypeClass vec_tc = get_vec_value_tc(obj_type,
                expr->datum_meta_.scale_, expr->datum_meta_.precision_);

            switch (vec_tc) {
            case VEC_TC_NUMBER: {
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
              break;
            }
            default:
              payload = cont_data + offsets[row];
              length = offsets[row + 1] - offsets[row];
              break;
            }
          } else if (fmt == VEC_DISCRETE) {
            // For discrete format, ptrs and lens are in separate arrays
            char **ptrs = expr->get_discrete_vector_ptrs(eval_ctx);
            int32_t *lens = expr->get_discrete_vector_lens(eval_ctx);
            const ObObjType obj_type = expr->datum_meta_.get_type();
            const VecValueTypeClass vec_tc = get_vec_value_tc(obj_type,
                expr->datum_meta_.scale_, expr->datum_meta_.precision_);

            // Helper lambda to strip ObLobCommon header if present
            auto strip_lob_header = [&](const char *&payload_ref, int32_t &length_ref) {
              if (expr->obj_meta_.has_lob_header() && length_ref > 0) {
                const ObLobCommon *lob = reinterpret_cast<const ObLobCommon *>(payload_ref);
                if (lob->in_row_) {
                  payload_ref = lob->get_inrow_data_ptr();
                  length_ref = static_cast<int32_t>(lob->get_byte_size(length_ref));
                }
              }
            };

            switch (vec_tc) {
            case VEC_TC_NUMBER: {
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
              break;
            }
            case VEC_TC_STRING:
            case VEC_TC_RAW:
            case VEC_TC_ROWID:
            case VEC_TC_ENUM_SET_INNER: {
              payload = ptrs[row];
              length = lens[row];
              break;
            }
            case VEC_TC_LOB:
            case VEC_TC_UDT:
            case VEC_TC_COLLECTION: {
              payload = ptrs[row];
              length = lens[row];
              strip_lob_header(payload, length);
              break;
            }
            case VEC_TC_JSON: {
              payload = ptrs[row];
              length = lens[row];
              strip_lob_header(payload, length);
              if (OB_SUCC(json_bin_to_text(allocator_, payload, length,
                                           tmp_buf, sizeof(tmp_buf), length))) {
                payload = tmp_buf;
              }
              break;
            }
            case VEC_TC_GEO:
            case VEC_TC_ROARINGBITMAP: {
              payload = ptrs[row];
              length = lens[row];
              strip_lob_header(payload, length);
              break;
            }
            default: {
              payload = ptrs[row];
              length = lens[row];
              break;
            }
            }
          } else if (fmt == VEC_UNIFORM) {
            // For uniform format, use ObDatum
            ObDatum *datums = expr->locate_batch_datums(eval_ctx);
            const ObObjType obj_type = expr->datum_meta_.get_type();
            const VecValueTypeClass vec_tc = get_vec_value_tc(obj_type,
                expr->datum_meta_.scale_, expr->datum_meta_.precision_);

            switch (vec_tc) {
            case VEC_TC_INTEGER: {
              int64_t val = datums[row].get_int();
              snprintf(tmp_buf, sizeof(tmp_buf), "%ld", val);
              payload = tmp_buf;
              length = strlen(tmp_buf);
              break;
            }
            case VEC_TC_UINTEGER:
            case VEC_TC_BIT:
            case VEC_TC_ENUM_SET: {
              uint64_t val = datums[row].get_uint();
              snprintf(tmp_buf, sizeof(tmp_buf), "%lu", val);
              payload = tmp_buf;
              length = strlen(tmp_buf);
              break;
            }
            case VEC_TC_FLOAT: {
              float val = datums[row].get_float();
              snprintf(tmp_buf, sizeof(tmp_buf), "%g", val);
              payload = tmp_buf;
              length = strlen(tmp_buf);
              break;
            }
            case VEC_TC_DOUBLE:
            case VEC_TC_FIXED_DOUBLE: {
              double val = datums[row].get_double();
              snprintf(tmp_buf, sizeof(tmp_buf), "%g", val);
              payload = tmp_buf;
              length = strlen(tmp_buf);
              break;
            }
            case VEC_TC_DATETIME:
            case VEC_TC_TIME:
            case VEC_TC_INTERVAL_YM:
            case VEC_TC_MYSQL_DATETIME:
            case VEC_TC_TIMESTAMP_TZ:
            case VEC_TC_TIMESTAMP_TINY:
            case VEC_TC_INTERVAL_DS:
            case VEC_TC_YEAR:
            case VEC_TC_UNKNOWN: {
              int64_t val = datums[row].get_int();
              snprintf(tmp_buf, sizeof(tmp_buf), "%ld", val);
              payload = tmp_buf;
              length = strlen(tmp_buf);
              break;
            }
            case VEC_TC_DATE:
            case VEC_TC_MYSQL_DATE: {
              int32_t val = datums[row].get_int();
              snprintf(tmp_buf, sizeof(tmp_buf), "%d", val);
              payload = tmp_buf;
              length = strlen(tmp_buf);
              break;
            }
            case VEC_TC_NUMBER: {
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
            case VEC_TC_DEC_INT32:
            case VEC_TC_DEC_INT64:
            case VEC_TC_DEC_INT128:
            case VEC_TC_DEC_INT256:
            case VEC_TC_DEC_INT512: {
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
            case VEC_TC_STRING:
            case VEC_TC_RAW:
            case VEC_TC_ROWID:
            case VEC_TC_ENUM_SET_INNER: {
              payload = datums[row].ptr_;
              length = datums[row].len_;
              break;
            }
            case VEC_TC_LOB:
            case VEC_TC_UDT:
            case VEC_TC_COLLECTION: {
              payload = datums[row].ptr_;
              length = datums[row].len_;
              if (expr->obj_meta_.has_lob_header() && length > 0) {
                const ObLobCommon *lob = reinterpret_cast<const ObLobCommon *>(payload);
                if (lob->in_row_) {
                  payload = lob->get_inrow_data_ptr();
                  length = static_cast<int32_t>(lob->get_byte_size(length));
                }
              }
              break;
            }
            case VEC_TC_JSON: {
              payload = datums[row].ptr_;
              length = datums[row].len_;
              if (expr->obj_meta_.has_lob_header() && length > 0) {
                const ObLobCommon *lob = reinterpret_cast<const ObLobCommon *>(payload);
                if (lob->in_row_) {
                  payload = lob->get_inrow_data_ptr();
                  length = static_cast<int32_t>(lob->get_byte_size(length));
                }
              }
              if (OB_SUCC(json_bin_to_text(allocator_, payload, length,
                                           tmp_buf, sizeof(tmp_buf), length))) {
                payload = tmp_buf;
              }
              break;
            }
            case VEC_TC_GEO:
            case VEC_TC_ROARINGBITMAP: {
              payload = datums[row].ptr_;
              length = datums[row].len_;
              if (expr->obj_meta_.has_lob_header() && length > 0) {
                const ObLobCommon *lob = reinterpret_cast<const ObLobCommon *>(payload);
                if (lob->in_row_) {
                  payload = lob->get_inrow_data_ptr();
                  length = static_cast<int32_t>(lob->get_byte_size(length));
                }
              }
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
          } else {
            // VEC_INVALID or flat format - use ObDatum
            // When enable_rich_format() is false, expressions use flat format (ObDatum)
            // The format will be VEC_INVALID in this case
            ObDatum *datums = expr->locate_batch_datums(eval_ctx);
            const ObObjType obj_type = expr->datum_meta_.get_type();
            const VecValueTypeClass vec_tc = get_vec_value_tc(obj_type,
                expr->datum_meta_.scale_, expr->datum_meta_.precision_);

            switch (vec_tc) {
            case VEC_TC_INTEGER: {
              int64_t val = datums[row].get_int();
              snprintf(tmp_buf, sizeof(tmp_buf), "%ld", val);
              payload = tmp_buf;
              length = strlen(tmp_buf);
              break;
            }
            case VEC_TC_UINTEGER:
            case VEC_TC_BIT:
            case VEC_TC_ENUM_SET: {
              uint64_t val = datums[row].get_uint();
              snprintf(tmp_buf, sizeof(tmp_buf), "%lu", val);
              payload = tmp_buf;
              length = strlen(tmp_buf);
              break;
            }
            case VEC_TC_FLOAT: {
              float val = datums[row].get_float();
              snprintf(tmp_buf, sizeof(tmp_buf), "%g", val);
              payload = tmp_buf;
              length = strlen(tmp_buf);
              break;
            }
            case VEC_TC_DOUBLE:
            case VEC_TC_FIXED_DOUBLE: {
              double val = datums[row].get_double();
              snprintf(tmp_buf, sizeof(tmp_buf), "%g", val);
              payload = tmp_buf;
              length = strlen(tmp_buf);
              break;
            }
            case VEC_TC_DATETIME:
            case VEC_TC_TIME:
            case VEC_TC_INTERVAL_YM:
            case VEC_TC_MYSQL_DATETIME:
            case VEC_TC_TIMESTAMP_TZ:
            case VEC_TC_TIMESTAMP_TINY:
            case VEC_TC_INTERVAL_DS:
            case VEC_TC_YEAR:
            case VEC_TC_UNKNOWN: {
              int64_t val = datums[row].get_int();
              snprintf(tmp_buf, sizeof(tmp_buf), "%ld", val);
              payload = tmp_buf;
              length = strlen(tmp_buf);
              break;
            }
            case VEC_TC_DATE:
            case VEC_TC_MYSQL_DATE: {
              int32_t val = datums[row].get_int();
              snprintf(tmp_buf, sizeof(tmp_buf), "%d", val);
              payload = tmp_buf;
              length = strlen(tmp_buf);
              break;
            }
            case VEC_TC_NUMBER: {
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
            case VEC_TC_DEC_INT32:
            case VEC_TC_DEC_INT64:
            case VEC_TC_DEC_INT128:
            case VEC_TC_DEC_INT256:
            case VEC_TC_DEC_INT512: {
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
            case VEC_TC_STRING:
            case VEC_TC_RAW:
            case VEC_TC_ROWID:
            case VEC_TC_ENUM_SET_INNER: {
              payload = datums[row].ptr_;
              length = datums[row].len_;
              break;
            }
            case VEC_TC_LOB:
            case VEC_TC_UDT:
            case VEC_TC_COLLECTION: {
              payload = datums[row].ptr_;
              length = datums[row].len_;
              if (expr->obj_meta_.has_lob_header() && length > 0) {
                const ObLobCommon *lob = reinterpret_cast<const ObLobCommon *>(payload);
                if (lob->in_row_) {
                  payload = lob->get_inrow_data_ptr();
                  length = static_cast<int32_t>(lob->get_byte_size(length));
                }
              }
              break;
            }
            case VEC_TC_JSON: {
              payload = datums[row].ptr_;
              length = datums[row].len_;
              if (expr->obj_meta_.has_lob_header() && length > 0) {
                const ObLobCommon *lob = reinterpret_cast<const ObLobCommon *>(payload);
                if (lob->in_row_) {
                  payload = lob->get_inrow_data_ptr();
                  length = static_cast<int32_t>(lob->get_byte_size(length));
                }
              }
              if (OB_SUCC(json_bin_to_text(allocator_, payload, length,
                                           tmp_buf, sizeof(tmp_buf), length))) {
                payload = tmp_buf;
              }
              break;
            }
            case VEC_TC_GEO:
            case VEC_TC_ROARINGBITMAP: {
              payload = datums[row].ptr_;
              length = datums[row].len_;
              if (expr->obj_meta_.has_lob_header() && length > 0) {
                const ObLobCommon *lob = reinterpret_cast<const ObLobCommon *>(payload);
                if (lob->in_row_) {
                  payload = lob->get_inrow_data_ptr();
                  length = static_cast<int32_t>(lob->get_byte_size(length));
                }
              }
              break;
            }
            default:
              // For unknown types, try to use raw data
              payload = datums[row].ptr_;
              length = datums[row].len_;
              if (nullptr == payload || 0 == length) {
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
  FatalErrorChecker error_checker(ret);

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