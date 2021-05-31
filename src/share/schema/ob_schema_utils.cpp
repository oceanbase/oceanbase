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

#define USING_LOG_PREFIX SHARE_SCHEMA
#include "share/schema/ob_schema_utils.h"

#include "lib/oblog/ob_log.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_server_schema_service.h"
#include "share/ob_cluster_type.h"
#include "share/ob_get_compat_mode.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "observer/ob_server_struct.h"
namespace oceanbase {
using namespace common;
using namespace sql;
namespace share {
namespace schema {

uint64_t ObSchemaUtils::get_exec_tenant_id(const uint64_t tenant_id)
{
  uint64_t exec_tenant_id = OB_SYS_TENANT_ID;
  if (OB_INVALID_TENANT_ID != tenant_id) {
    exec_tenant_id = tenant_id;
  }
  return exec_tenant_id;
}

uint64_t ObSchemaUtils::get_extract_tenant_id(const uint64_t exec_tenant_id, const uint64_t tenant_id)
{
  uint64_t new_tenant_id = tenant_id;
  if (OB_SYS_TENANT_ID != exec_tenant_id) {
    new_tenant_id = OB_INVALID_TENANT_ID;
  }
  return new_tenant_id;
}
uint64_t ObSchemaUtils::get_extract_schema_id(const uint64_t exec_tenant_id, const uint64_t schema_id)
{
  uint64_t new_schema_id = schema_id;
  if (OB_SYS_TENANT_ID != exec_tenant_id && static_cast<int64_t>(schema_id) > 0) {
    new_schema_id = extract_pure_id(schema_id);
  }
  return new_schema_id;
}

uint64_t ObSchemaUtils::get_real_table_mappings_tid(const uint64_t ref_table_id)
{
  int ret = OB_SUCCESS;
  uint64_t base_table_id = share::get_real_table_mappings_tid(ref_table_id);
  if (common::OB_INVALID_ID != base_table_id) {
    uint64_t tenant_id = extract_tenant_id(ref_table_id);
    if (OB_ALL_TABLE_TID == base_table_id && OB_FAIL(get_all_table_id(tenant_id, base_table_id))) {
      LOG_WARN("fail to get all table tid", K(ret), K(base_table_id));
    } else if (OB_ALL_TABLE_HISTORY_TID == base_table_id &&
               OB_FAIL(get_all_table_history_id(tenant_id, base_table_id))) {
      LOG_WARN("fail to get all table tid", K(ret), K(base_table_id));
    } else {
      base_table_id = combine_id(tenant_id, base_table_id);
      LOG_DEBUG("debug get real table id", K(ret), K(ref_table_id), K(base_table_id));
    }
  }
  return base_table_id;
}

int ObSchemaUtils::get_all_table_id(
    const uint64_t tenant_id, uint64_t& table_id, const ObServerSchemaService* schema_service /*=NULL*/)
{
  int ret = OB_SUCCESS;
  bool new_mode = true;
  schema_service = OB_ISNULL(schema_service) ? &GSCHEMASERVICE : schema_service;
  if (OB_FAIL(schema_service->check_tenant_can_use_new_table(tenant_id, new_mode))) {
    LOG_WARN("fail to check schema mode", K(ret), K(tenant_id));
  } else if (new_mode) {
    table_id = combine_id(tenant_id, OB_ALL_TABLE_V2_TID);
  } else {
    table_id = combine_id(tenant_id, OB_ALL_TABLE_TID);
  }
  return ret;
}

int ObSchemaUtils::get_all_table_history_id(
    const uint64_t tenant_id, uint64_t& table_id, const ObServerSchemaService* schema_service /*=NULL*/)
{
  int ret = OB_SUCCESS;
  bool new_mode = true;
  schema_service = OB_ISNULL(schema_service) ? &GSCHEMASERVICE : schema_service;
  if (OB_FAIL(schema_service->check_tenant_can_use_new_table(tenant_id, new_mode))) {
    LOG_WARN("fail to check schema mode", K(ret), K(tenant_id));
  } else if (new_mode) {
    table_id = combine_id(tenant_id, OB_ALL_TABLE_V2_HISTORY_TID);
  } else {
    table_id = combine_id(tenant_id, OB_ALL_TABLE_HISTORY_TID);
  }
  return ret;
}

int ObSchemaUtils::get_all_table_name(
    const uint64_t tenant_id, const char*& table_name, const ObServerSchemaService* schema_service /*=NULL*/)
{
  int ret = OB_SUCCESS;
  bool new_mode = true;
  schema_service = OB_ISNULL(schema_service) ? &GSCHEMASERVICE : schema_service;
  if (OB_FAIL(schema_service->check_tenant_can_use_new_table(tenant_id, new_mode))) {
    LOG_WARN("fail to check schema mode", K(ret), K(tenant_id));
  } else if (new_mode) {
    table_name = OB_ALL_TABLE_V2_TNAME;
  } else {
    table_name = OB_ALL_TABLE_TNAME;
  }
  return ret;
}

int ObSchemaUtils::get_all_table_history_name(
    const uint64_t tenant_id, const char*& table_name, const ObServerSchemaService* schema_service /*=NULL*/)
{
  int ret = OB_SUCCESS;
  bool new_mode = true;
  schema_service = OB_ISNULL(schema_service) ? &GSCHEMASERVICE : schema_service;
  if (OB_FAIL(schema_service->check_tenant_can_use_new_table(tenant_id, new_mode))) {
    LOG_WARN("fail to check schema mode", K(ret), K(tenant_id));
  } else if (new_mode) {
    table_name = OB_ALL_TABLE_V2_HISTORY_TNAME;
  } else {
    table_name = OB_ALL_TABLE_HISTORY_TNAME;
  }
  return ret;
}

int ObSchemaUtils::cascaded_generated_column(ObTableSchema& table_schema, ObColumnSchemaV2& column)
{
  int ret = OB_SUCCESS;
  ObString col_def;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  ObRawExprFactory expr_factory(allocator);
  ObRawExpr* expr = NULL;
  ObArray<ObQualifiedName> columns;
  ObColumnSchemaV2* col_schema = NULL;
  bool is_oracle_mode = false;
  if (column.is_generated_column()) {
    // This is the mock session, so test_init should be used, otherwise it cannot be initialized for tz_mgr
    ObSQLSessionInfo default_session;
    if (OB_FAIL(default_session.test_init(0, 0, 0, &allocator))) {
      LOG_WARN("init empty session failed", K(ret));
    } else if (OB_FAIL(default_session.load_default_sys_variable(false, false))) {
      LOG_WARN("session load default system variable failed", K(ret));
    } else {
      if (ObSchemaService::g_liboblog_mode_ && GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_1471) {
        // when 2.0liboblog fetch <1471 cluster, Parsing the column schema only needs to read orig_default_value
        // Can not judge cur_default_value.is_null(), because the dependent column may have a default value
        // cur_default_value is is_not_null, misjudgment
        if (OB_FAIL(column.get_orig_default_value().get_string(col_def))) {
          LOG_WARN("get orig default value failed", K(ret));
        }
      } else {
        // If the dependent column of the generated column has a change column, the current default value
        // should be used instead of orig vaule
        if (column.get_cur_default_value().is_null()) {
          if (OB_FAIL(column.get_orig_default_value().get_string(col_def))) {
            LOG_WARN("get orig default value failed", K(ret));
          }
        } else {
          if (OB_FAIL(column.get_cur_default_value().get_string(col_def))) {
            LOG_WARN("get cur default value failed", K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObRawExprUtils::build_generated_column_expr(col_def, expr_factory, default_session, expr, columns))) {
        LOG_WARN("get generated column expr failed", K(ret));
      } else if (OB_ISNULL(expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr is null");
      } else if (T_FUN_SYS_WORD_SEGMENT == expr->get_expr_type()) {
        column.add_column_flag(GENERATED_CTXCAT_CASCADE_FLAG);
      } else {
        LOG_DEBUG("succ to build_generated_column_expr", K(col_def), KPC(expr), K(columns), K(table_schema));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(
              ObCompatModeGetter::check_is_oracle_mode_with_tenant_id(table_schema.get_tenant_id(), is_oracle_mode))) {
        LOG_WARN("fail to check is oracle mode", K(ret));
      }
    }

    // TODO: materialized view
    if (table_schema.is_table() || table_schema.is_tmp_table()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); ++i) {
        // alter table t add  b char(10) as(concat(a, '1')); oracle mode
        // The pl implementation causes concat to be parsed into T_OBJ_ACCESS_REF, so column_name may be empty
        if (is_oracle_mode && columns.at(i).access_idents_.count() > 0 &&
            columns.at(i).access_idents_[0].type_ != UNKNOWN) {
          continue;
        } else if (!columns.at(i).database_name_.empty() || !columns.at(i).tbl_name_.empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column is invalid", K(columns.at(i)));
        } else if (OB_ISNULL(col_schema = table_schema.get_column_schema(columns.at(i).col_name_))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get column schema failed", K(columns.at(i)));
        } else if (OB_FAIL(column.add_cascaded_column_id(col_schema->get_column_id()))) {
          LOG_WARN("add cascaded column id failed", K(ret));
        } else {
          col_schema->add_column_flag(GENERATED_DEPS_CASCADE_FLAG);
        }
      }
    }
  }
  return ret;
}

bool ObSchemaUtils::is_virtual_generated_column(uint64_t flag)
{
  return flag & VIRTUAL_GENERATED_COLUMN_FLAG;
}

bool ObSchemaUtils::is_stored_generated_column(uint64_t flag)
{
  return flag & STORED_GENERATED_COLUMN_FLAG;
}

bool ObSchemaUtils::is_invisible_column(uint64_t flag)
{
  return flag & INVISIBLE_COLUMN_FLAG;
}

bool ObSchemaUtils::is_cte_generated_column(uint64_t flag)
{
  return flag & CTE_GENERATED_COLUMN_FLAG;
}

bool ObSchemaUtils::is_default_expr_v2_column(uint64_t flag)
{
  return flag & DEFAULT_EXPR_V2_COLUMN_FLAG;
}

bool ObSchemaUtils::is_fulltext_column(uint64_t flag)
{
  return flag & GENERATED_CTXCAT_CASCADE_FLAG;
}

int ObSchemaUtils::add_column_to_table_schema(ObColumnSchemaV2& column, ObTableSchema& table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cascaded_generated_column(table_schema, column))) {
    LOG_WARN("cascaded generated column failed", K(ret));
  } else if (OB_FAIL(table_schema.add_column(column))) {
    LOG_WARN("add column to table schema failed", K(ret));
  }
  return ret;
}

int ObSchemaUtils::convert_sys_param_to_sysvar_schema(const ObSysParam& sysparam, ObSysVarSchema& sysvar_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sysvar_schema.set_name(ObString::make_string(sysparam.name_)))) {
    LOG_WARN("set sysvar schema name failed", K(ret));
  } else if (OB_FAIL(sysvar_schema.set_value(ObString::make_string(sysparam.value_)))) {
    LOG_WARN("set sysvar schema value failed", K(ret));
  } else if (OB_FAIL(sysvar_schema.set_min_val(ObString::make_string(sysparam.min_val_)))) {
    LOG_WARN("set sysvar schema min val failed", K(ret));
  } else if (OB_FAIL(sysvar_schema.set_max_val(ObString::make_string(sysparam.max_val_)))) {
    LOG_WARN("set sysvar schema max val failed", K(ret));
  } else if (OB_FAIL(sysvar_schema.set_info(ObString::make_string(sysparam.info_)))) {
    LOG_WARN("set sysvar schema info failed", K(ret));
  } else if (OB_FAIL(sysvar_schema.set_zone(sysparam.zone_))) {
    LOG_WARN("set sysvar schema zone failed", K(ret));
  } else {
    sysvar_schema.set_flags(sysparam.flags_);
    sysvar_schema.set_tenant_id(sysparam.tenant_id_);
    sysvar_schema.set_data_type(static_cast<ObObjType>(sysparam.data_type_));
  }
  return ret;
}

int ObSchemaUtils::get_primary_zone_array(common::PageArena<>& alloc, const ObTablegroupSchema& tg_schema,
    ObSchemaGetterGuard& schema_guard, common::ObIArray<ObZoneScore>& primary_zone_array)
{

  int ret = OB_SUCCESS;
  primary_zone_array.reset();
  const ObTenantSchema* tenant_info = NULL;
  uint64_t tg_id = tg_schema.get_tablegroup_id();
  uint64_t tenant_id = tg_schema.get_tenant_id();
  bool use_tenant_primary_zone = GCTX.is_standby_cluster() && OB_SYS_TENANT_ID != tenant_id;
  bool is_restore = false;
  if (use_tenant_primary_zone) {
    // skip
  } else if (OB_FAIL(schema_guard.check_tenant_is_restore(tenant_id, is_restore))) {
    LOG_WARN("fail to check tenant is restore", K(ret), K(tenant_id));
  } else if (is_restore) {
    // skip
  } else if (OB_FAIL(get_schema_primary_zone_array(alloc, &tg_schema, primary_zone_array))) {
    LOG_WARN("fail to get primary zone array", K(ret), K(tg_id));
  } else if (primary_zone_array.count() > 0) {
    // got it
  }

  if (OB_FAIL(ret)) {
  } else if (primary_zone_array.count() > 0) {
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_info))) {
    LOG_WARN("fail to get tenant info", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_schema_primary_zone_array(alloc, tenant_info, primary_zone_array))) {
    LOG_WARN("fail to get primary zone array", K(ret), K(tg_id), K(tenant_id));
  }
  return ret;
}

int ObSchemaUtils::get_primary_zone_array(common::PageArena<>& alloc, const ObTableSchema& table_schema,
    ObSchemaGetterGuard& schema_guard, common::ObIArray<ObZoneScore>& primary_zone_array)
{
  int ret = OB_SUCCESS;
  primary_zone_array.reset();
  const ObTenantSchema* tenant_info = NULL;
  uint64_t table_id = table_schema.get_table_id();
  uint64_t database_id = table_schema.get_database_id();
  uint64_t tablegroup_id = table_schema.get_tablegroup_id();
  uint64_t tenant_id = table_schema.get_tenant_id();
  bool use_tenant_primary_zone = GCTX.is_standby_cluster() && OB_SYS_TENANT_ID != tenant_id;
  bool is_restore = false;
  if (use_tenant_primary_zone) {
    // skip
  } else if (OB_FAIL(schema_guard.check_tenant_is_restore(tenant_id, is_restore))) {
    LOG_WARN("fail to check tenant is restore", K(ret), K(tenant_id));
  } else if (is_restore) {
    // skip
  } else if (OB_FAIL(get_schema_primary_zone_array(alloc, &table_schema, primary_zone_array))) {
    LOG_WARN("fail to get primary zone array", K(ret), K(table_id));
  } else if (primary_zone_array.count() > 0) {
    // got it
  } else if (OB_INVALID_ID != tablegroup_id && is_new_tablegroup_id(tablegroup_id)) {
    const ObTablegroupSchema* tg_schema = nullptr;
    if (OB_FAIL(schema_guard.get_tablegroup_schema(tablegroup_id, tg_schema))) {
      LOG_WARN("fail to get tablegroup schema", K(ret));
    } else if (OB_FAIL(get_schema_primary_zone_array(alloc, tg_schema, primary_zone_array))) {
      LOG_WARN("fail to get primary zone array", K(ret), K(table_id), K(tablegroup_id));
    }
  } else {
    const ObDatabaseSchema* database_schema = NULL;
    if (OB_FAIL(schema_guard.get_database_schema(database_id, database_schema))) {
      LOG_WARN("fail to get database schema", K(ret), K(table_id), K(database_id));
    } else if (OB_FAIL(get_schema_primary_zone_array(alloc, database_schema, primary_zone_array))) {
      LOG_WARN("fail to get primary zone array", K(ret), K(table_id), K(database_id));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (primary_zone_array.count() > 0) {
    // already got
  } else if (OB_FAIL(schema_guard.get_tenant_info(tenant_id, tenant_info))) {
    LOG_WARN("fail to get tenant info", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_schema_primary_zone_array(alloc, tenant_info, primary_zone_array))) {
    LOG_WARN("fail to get primary zone array", K(ret), K(table_id), K(database_id));
  }
  return ret;
}

int ObSchemaUtils::get_tenant_int_variable(uint64_t tenant_id, ObSysVarClassType var_id, int64_t& v)
{
  int ret = OB_SUCCESS;
  schema::ObSchemaGetterGuard schema_guard;
  ObObj value;
  if (OB_FAIL(get_tenant_variable(schema_guard, tenant_id, var_id, value))) {
    LOG_WARN("fail get tenant variable", K(value), K(var_id), K(tenant_id), K(ret));
  } else if (OB_FAIL(value.get_int(v))) {
    LOG_WARN("get int from value failed", K(ret), K(value));
  }
  return ret;
}

int ObSchemaUtils::get_tenant_varchar_variable(
    uint64_t tenant_id, ObSysVarClassType var_id, ObIAllocator& allocator, ObString& v)
{
  int ret = OB_SUCCESS;
  schema::ObSchemaGetterGuard schema_guard;
  ObObj value;
  ObString tmp;
  if (OB_FAIL(get_tenant_variable(schema_guard, tenant_id, var_id, value))) {
    LOG_WARN("fail get tenant variable", K(value), K(var_id), K(tenant_id), K(ret));
  } else if (OB_FAIL(value.get_varchar(tmp))) {
    LOG_WARN("get varchar from value failed", K(ret), K(value));
  } else if (OB_FAIL(ob_write_string(allocator, tmp, v))) {
    // must be deep copy, otherwise very low probability v will reference illegal memory
    LOG_WARN("fail deep copy string", K(ret));
  }
  return ret;
}

int ObSchemaUtils::get_tenant_variable(
    schema::ObSchemaGetterGuard& schema_guard, uint64_t tenant_id, ObSysVarClassType var_id, ObObj& value)
{
  int ret = OB_SUCCESS;
  const schema::ObSysVarSchema* var_schema = NULL;
  share::schema::ObMultiVersionSchemaService& schema_service =
      share::schema::ObMultiVersionSchemaService::get_instance();
  if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_system_variable(tenant_id, var_id, var_schema))) {
    LOG_WARN("fail to get system variable", K(ret), K(tenant_id), K(var_id));
  } else if (OB_ISNULL(var_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("var_schema is null");
  } else if (OB_FAIL(var_schema->get_value(NULL, NULL, value))) {
    LOG_WARN("get value from var_schema failed", K(ret), K(*var_schema));
  }
  return ret;
}

int ObSchemaUtils::str_to_int(const ObString& str, int64_t& value)
{
  int ret = OB_SUCCESS;
  char buf[OB_MAX_BIT_LENGTH];
  value = OB_INVALID_ID;
  if (str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(str));
  } else {
    int n = snprintf(buf, OB_MAX_BIT_LENGTH, "%.*s", str.length(), str.ptr());
    if (n < 0 || n >= OB_MAX_BIT_LENGTH) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("id_buf is not long enough", K(ret), K(n), LITERAL_K(OB_MAX_BIT_LENGTH));
    } else {
      const int64_t base = 10;
      value = strtol(buf, NULL, base);
    }
  }
  return ret;
}

bool ObSchemaUtils::is_public_database(const ObString& db_name, bool is_oracle_mode)
{
  // mysql mode: __public
  // oracle mode: PUBLIC
  bool bret =
      is_oracle_mode ? (ObString(OB_ORA_PUBLIC_SCHEMA_NAME) == db_name) : (ObString(OB_PUBLIC_SCHEMA_NAME) == db_name);
  return bret;
}

bool ObSchemaUtils::is_public_database_case_cmp(const ObString& db_name, bool is_oracle_mode)
{
  // mysql mode: __public
  // oracle mode: PUBLIC
  bool bret = is_oracle_mode ? (0 == db_name.case_compare(OB_ORA_PUBLIC_SCHEMA_NAME))
                             : (0 == db_name.case_compare(OB_PUBLIC_SCHEMA_NAME));
  return bret;
}

}  // namespace schema
}  // namespace share
}  // namespace oceanbase
