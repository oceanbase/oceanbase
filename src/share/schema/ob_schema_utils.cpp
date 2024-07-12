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
#include "share/ob_cluster_role.h"
#include "share/ob_get_compat_mode.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "sql/session/ob_sql_session_info.h"
#include "observer/ob_server_struct.h"
#include "sql/engine/cmd/ob_ddl_executor_util.h"
namespace oceanbase
{
using namespace common;
using namespace sql;
namespace share
{
namespace schema
{

uint64_t ObSchemaUtils::get_exec_tenant_id(const uint64_t tenant_id)
{
  uint64_t exec_tenant_id = OB_SYS_TENANT_ID;
  if (OB_INVALID_TENANT_ID !=  tenant_id) {
    exec_tenant_id = tenant_id;
  }
  return exec_tenant_id;
}

uint64_t ObSchemaUtils::get_extract_tenant_id(const uint64_t exec_tenant_id, const uint64_t tenant_id)
{
  UNUSEDx(exec_tenant_id, tenant_id);
  return OB_INVALID_TENANT_ID;
}
uint64_t ObSchemaUtils::get_extract_schema_id(const uint64_t exec_tenant_id, const uint64_t schema_id)
{
  UNUSED(exec_tenant_id);
  return schema_id;
}

uint64_t ObSchemaUtils::get_real_table_mappings_tid(const uint64_t ref_table_id)
{
  int ret = OB_SUCCESS;
  uint64_t base_table_id = share::get_real_table_mappings_tid(ref_table_id);
  if (common::OB_INVALID_ID != base_table_id) {
    LOG_DEBUG("debug get real table id", K(ret), K(ref_table_id), K(base_table_id));
  }
  return base_table_id;
}

int ObSchemaUtils::get_all_table_name(
    const uint64_t tenant_id,
    const char* &table_name,
    const ObServerSchemaService *schema_service /*=NULL*/)
{
  int ret = OB_SUCCESS;
  UNUSEDx(tenant_id, schema_service);
  table_name = OB_ALL_TABLE_TNAME;
  return ret;
}

int ObSchemaUtils::get_all_table_history_name(
    const uint64_t tenant_id,
    const char* &table_name,
    const ObServerSchemaService *schema_service /*=NULL*/)
{
  int ret = OB_SUCCESS;
  UNUSEDx(tenant_id, schema_service);
  table_name = OB_ALL_TABLE_HISTORY_TNAME;
  return ret;
}

int ObSchemaUtils::cascaded_generated_column(ObTableSchema &table_schema,
                                             ObColumnSchemaV2 &column,
                                             const bool resolve_dependencies)
{
  int ret = OB_SUCCESS;
  ObString col_def;
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  ObItemType root_expr_type = T_INVALID;
  ObArray<ObString> columns_names;
  ObColumnSchemaV2 *col_schema = NULL;
  bool is_oracle_mode = false;
  if (column.is_generated_column()) {
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

    if (OB_SUCC(ret)) {
      if (OB_FAIL(table_schema.check_if_oracle_compat_mode(is_oracle_mode))) {
        SHARE_SCHEMA_LOG(WARN, "failed to check if oracle mode", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      lib::Worker::CompatMode compat_mode = is_oracle_mode ? lib::Worker::CompatMode::ORACLE : lib::Worker::CompatMode::MYSQL;
      lib::CompatModeGuard guard(compat_mode);
      if (OB_FAIL(ObResolverUtils::resolve_generated_column_info(col_def, allocator,
          root_expr_type, columns_names))) {
        LOG_WARN("get generated column expr failed", K(ret));
      } else if (T_FUN_SYS_WORD_SEGMENT == root_expr_type) {
        column.add_column_flag(GENERATED_FTS_WORD_SEGMENT_COLUMN_FLAG);
      } else if (T_FUN_SYS_WORD_COUNT == root_expr_type) {
        column.add_column_flag(GENERATED_FTS_WORD_COUNT_COLUMN_FLAG);
      } else if (T_FUN_SYS_DOC_LENGTH == root_expr_type) {
        column.add_column_flag(GENERATED_FTS_DOC_LENGTH_COLUMN_FLAG);
      } else if (T_FUN_SYS_SPATIAL_CELLID == root_expr_type || T_FUN_SYS_SPATIAL_MBR == root_expr_type) {
        column.add_column_flag(SPATIAL_INDEX_GENERATED_COLUMN_FLAG);
      } else if (T_FUN_SYS_JSON_QUERY == root_expr_type) {
        if (strstr(col_def.ptr(), "multivalue)")) {
          column.add_column_flag(MULTIVALUE_INDEX_GENERATED_ARRAY_COLUMN_FLAG);
        } else {
          column.add_column_flag(MULTIVALUE_INDEX_GENERATED_COLUMN_FLAG);
        }
      } else {
        LOG_DEBUG("succ to resolve_generated_column_info", K(col_def), K(root_expr_type), K(columns_names), K(table_schema));
      }
    }

    // TODO: materialized view
    if (OB_SUCC(ret) && resolve_dependencies && !column.is_doc_id_column() && (table_schema.is_table()
                                                || table_schema.is_tmp_table())) {
      for (int64_t i = 0; OB_SUCC(ret) && i < columns_names.count(); ++i) {
        if (OB_ISNULL(col_schema = table_schema.get_column_schema(columns_names.at(i)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get column schema failed", K(columns_names.at(i)));
        } else if (OB_FAIL(column.add_cascaded_column_id(col_schema->get_column_id()))) {
          LOG_WARN("add cascaded column id failed", K(ret));
        } else if (col_schema->get_udt_set_id() > 0) {
          ObSEArray<ObColumnSchemaV2 *, 1> hidden_cols;
          if (OB_FAIL(table_schema.get_column_schema_in_same_col_group(col_schema->get_column_id(), col_schema->get_udt_set_id(), hidden_cols))) {
            LOG_WARN("get column schema in same col group failed", K(ret), K(col_schema->get_udt_set_id()));
          } else {
            for (int i = 0; i < hidden_cols.count() && OB_SUCC(ret); i++) {
              uint64_t cascaded_column_id = hidden_cols.at(i)->get_column_id();
              if (OB_FAIL(column.add_cascaded_column_id(cascaded_column_id))) {
                LOG_WARN("add cascaded column id to generated column failed", K(ret), K(cascaded_column_id));
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (column.is_tbl_part_key_column()) {
            col_schema->add_column_flag(TABLE_PART_KEY_COLUMN_ORG_FLAG);
          }
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

bool ObSchemaUtils::is_multivalue_generated_column(uint64_t flag)
{
  return flag & MULTIVALUE_INDEX_GENERATED_COLUMN_FLAG;
}

bool ObSchemaUtils::is_multivalue_generated_array_column(uint64_t flag)
{
  return flag & MULTIVALUE_INDEX_GENERATED_ARRAY_COLUMN_FLAG;
}

bool ObSchemaUtils::is_stored_generated_column(uint64_t flag)
{
  return flag & STORED_GENERATED_COLUMN_FLAG;
}

bool ObSchemaUtils::is_always_identity_column(uint64_t flag)
{
  return flag & ALWAYS_IDENTITY_COLUMN_FLAG;
}

bool ObSchemaUtils::is_default_identity_column(uint64_t flag)
{
  return flag & DEFAULT_IDENTITY_COLUMN_FLAG;
}

bool ObSchemaUtils::is_default_on_null_identity_column(uint64_t flag)
{
  return flag & DEFAULT_ON_NULL_IDENTITY_COLUMN_FLAG;
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

bool ObSchemaUtils::is_fulltext_column(const uint64_t flag)
{
  return is_doc_id_column(flag)
      || is_word_segment_column(flag)
      || is_word_count_column(flag)
      || is_doc_length_column(flag);
}

bool ObSchemaUtils::is_doc_id_column(const uint64_t flag)
{
  return flag & GENERATED_DOC_ID_COLUMN_FLAG;
}

bool ObSchemaUtils::is_word_segment_column(const uint64_t flag)
{
  return flag & GENERATED_FTS_WORD_SEGMENT_COLUMN_FLAG;
}

bool ObSchemaUtils::is_word_count_column(const uint64_t flag)
{
  return flag & GENERATED_FTS_WORD_COUNT_COLUMN_FLAG;
}

bool ObSchemaUtils::is_doc_length_column(const uint64_t flag)
{
  return flag & GENERATED_FTS_DOC_LENGTH_COLUMN_FLAG;
}

bool ObSchemaUtils::is_spatial_generated_column(uint64_t flag)
{
  return flag & SPATIAL_INDEX_GENERATED_COLUMN_FLAG;
}

bool ObSchemaUtils::is_label_se_column(uint64_t flag)
{
  return (flag & LABEL_SE_COLUMN_FLAG) != 0;
}

int ObSchemaUtils::add_column_to_table_schema(ObColumnSchemaV2 &column, ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cascaded_generated_column(table_schema, column, false))) {
    LOG_WARN("cascaded generated column failed", K(ret));
  } else if (OB_FAIL(table_schema.add_column(column))) {
    LOG_WARN("add column to table schema failed", K(ret));
  }
  return ret;
}

int ObSchemaUtils::convert_sys_param_to_sysvar_schema(const ObSysParam &sysparam, ObSysVarSchema &sysvar_schema)
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

int ObSchemaUtils::get_tenant_int_variable(uint64_t tenant_id,
                                           ObSysVarClassType var_id,
                                           int64_t &v)
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

int ObSchemaUtils::get_tenant_varchar_variable(uint64_t tenant_id,
                                               ObSysVarClassType var_id,
                                               ObIAllocator &allocator,
                                               ObString &v)
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

int ObSchemaUtils::get_tenant_variable(schema::ObSchemaGetterGuard &schema_guard,
                                       uint64_t tenant_id,
                                       ObSysVarClassType var_id,
                                       ObObj &value)
{
  int ret = OB_SUCCESS;
  const schema::ObSysVarSchema *var_schema = NULL;
  share::schema::ObMultiVersionSchemaService &schema_service =
      share::schema::ObMultiVersionSchemaService::get_instance();
  if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_system_variable(
              tenant_id, var_id, var_schema))) {
    LOG_WARN("fail to get system variable", K(ret), K(tenant_id), K(var_id));
  } else if (OB_ISNULL(var_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("var_schema is null");
  } else if (OB_FAIL(var_schema->get_value(NULL, NULL, value))) {
    LOG_WARN("get value from var_schema failed", K(ret), K(*var_schema));
  }
  return ret;
}

int ObSchemaUtils::str_to_int(const ObString &str, int64_t &value)
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

int ObSchemaUtils::str_to_uint(const ObString &str, uint64_t &value)
{
  int ret = OB_SUCCESS;
  int64_t int_value = OB_INVALID_ID;
  if (OB_FAIL(str_to_int(str, int_value))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to translate str to int", K(str), K(ret));
  } else {
    value = static_cast<uint64_t>(int_value);
  }
  return ret;
}

int ObSchemaUtils::construct_tenant_space_simple_table(
    const uint64_t tenant_id,
    ObSimpleTableSchemaV2 &table)
{
  int ret = OB_SUCCESS;
  table.set_tenant_id(tenant_id);
  // for distributed virtual table in tenant space
  int64_t part_num = table.get_partition_num();
  for (int64_t i = 0; OB_SUCC(ret) && i < part_num; i++) {
    ObPartition *part = table.get_part_array()[i];
    if (OB_ISNULL(part)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part is null", KR(ret), K(i));
    } else {
      part->set_tenant_id(tenant_id);
    }
  } // end for
  return ret;
}

int ObSchemaUtils::construct_tenant_space_full_table(
    const uint64_t tenant_id,
    ObTableSchema &table)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(construct_tenant_space_simple_table(tenant_id, table))) {
    LOG_WARN("fail to construct tenant space simple table", KR(ret), K(tenant_id), K(table));
  } else {
    // index
    const int64_t table_id = table.get_table_id();
    if (OB_FAIL(ObSysTableChecker::fill_sys_index_infos(table))) {
      LOG_WARN("fail to fill sys indexes", KR(ret), K(tenant_id), K(table_id));
    }
    // lob aux
    if (OB_SUCC(ret) && is_system_table(table_id)) {
      uint64_t lob_meta_table_id = 0;
      uint64_t lob_piece_table_id = 0;
      if (OB_ALL_CORE_TABLE_TID == table_id) {
        // do nothing
      } else if (!get_sys_table_lob_aux_table_id(table_id, lob_meta_table_id, lob_piece_table_id)) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("fail to get lob aux table id", KR(ret), K(table_id));
      } else if (lob_meta_table_id == 0 || lob_piece_table_id == 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get lob aux table id", KR(ret), K(table_id), K(lob_meta_table_id), K(lob_piece_table_id));
      } else {
        table.set_aux_lob_meta_tid(lob_meta_table_id);
        table.set_aux_lob_piece_tid(lob_piece_table_id);
      }
    }
    // column
    int64_t column_count = table.get_column_count();
    for (int64_t i = 0; OB_SUCC(ret) && i < column_count; ++i) {
      ObColumnSchemaV2 *column = NULL;
      if (NULL == (column = table.get_column_schema_by_idx(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column is null", K(ret));
      } else {
        column->set_tenant_id(tenant_id);
        column->set_table_id(table.get_table_id());
      }
    }
  }
  return ret;
}

int ObSchemaUtils::add_sys_table_lob_aux_table(
    uint64_t tenant_id,
    uint64_t data_table_id,
    ObIArray<ObTableSchema> &table_schemas)
{
  int ret = OB_SUCCESS;
  if (is_system_table(data_table_id)) {
    HEAP_VARS_2((ObTableSchema, lob_meta_schema), (ObTableSchema, lob_piece_schema)) {
      if (OB_ALL_CORE_TABLE_TID == data_table_id) {
        // do nothing
      } else if (OB_FAIL(get_sys_table_lob_aux_schema(data_table_id, lob_meta_schema, lob_piece_schema))) {
        LOG_WARN("fail to get sys table lob aux schema", KR(ret), K(data_table_id));
      } else if (OB_FAIL(ObSchemaUtils::construct_tenant_space_full_table(
                  tenant_id, lob_meta_schema))) {
        LOG_WARN("fail to construct tenant space table", KR(ret), K(tenant_id));
      } else if (OB_FAIL(ObSchemaUtils::construct_tenant_space_full_table(
                  tenant_id, lob_piece_schema))) {
        LOG_WARN("fail to construct tenant space table", KR(ret), K(tenant_id));
      } else if (OB_FAIL(table_schemas.push_back(lob_meta_schema))) {
        LOG_WARN("fail to push back table schema", KR(ret), K(lob_meta_schema));
      } else if (OB_FAIL(table_schemas.push_back(lob_piece_schema))) {
        LOG_WARN("fail to push back table schema", KR(ret), K(lob_piece_schema));
      }
    }
  }
  return ret;
}

// construct inner table schemas in tenant space
int ObSchemaUtils::construct_inner_table_schemas(
    const uint64_t tenant_id,
    ObSArray<ObTableSchema> &tables,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (is_sys_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else {
    const schema_create_func *creator_ptr_arrays[] = {
      all_core_table_schema_creator,
      core_table_schema_creators,
      sys_table_schema_creators,
      virtual_table_schema_creators,
      sys_view_schema_creators
    };
    int64_t capacity = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(creator_ptr_arrays); ++i) {
      for (const schema_create_func *creator_ptr = creator_ptr_arrays[i];
           OB_SUCC(ret) && OB_NOT_NULL(*creator_ptr); ++creator_ptr) {
        ++capacity;
      }
    }
    if (FAILEDx(tables.prepare_allocate_and_keep_count(capacity, &allocator))) {
      LOG_WARN("fail to prepare allocate table schemas", KR(ret), K(tenant_id), K(capacity));
    }
    HEAP_VARS_2((ObTableSchema, table_schema), (ObTableSchema, data_schema)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(creator_ptr_arrays); ++i) {
        for (const schema_create_func *creator_ptr = creator_ptr_arrays[i];
             OB_SUCC(ret) && OB_NOT_NULL(*creator_ptr); ++creator_ptr) {
          table_schema.reset();
          bool exist = false;
          if (OB_FAIL((*creator_ptr)(table_schema))) {
            LOG_WARN("fail to gen sys table schema", KR(ret));
          } else if (OB_FAIL(ObSchemaUtils::construct_tenant_space_full_table(
                     tenant_id, table_schema))) {
            LOG_WARN("fail to construct tenant space table", KR(ret), K(tenant_id));
          } else if (OB_FAIL(ObSysTableChecker::is_inner_table_exist(
                     tenant_id, table_schema, exist))) {
            LOG_WARN("fail to check inner table exist",
                     KR(ret), K(tenant_id), K(table_schema));
          } else if (!exist) {
            // skip
          } else if (OB_FAIL(tables.push_back(table_schema))) {
            LOG_WARN("fail to push back table schema", KR(ret), K(table_schema));
          } else if (OB_FAIL(ObSysTableChecker::append_sys_table_index_schemas(
                     tenant_id, table_schema.get_table_id(), tables))) {
            LOG_WARN("fail to append sys table index schemas",
                     KR(ret), K(tenant_id), "table_id", table_schema.get_table_id());
          }
          const int64_t data_table_id = table_schema.get_table_id();
          if (OB_SUCC(ret) && exist) {
            if (OB_FAIL(add_sys_table_lob_aux_table(tenant_id, data_table_id, tables))) {
              LOG_WARN("fail to add lob table to sys table", KR(ret), K(data_table_id));
            }
          } // end lob aux table
        }
      }
    }
  }
  return ret;
}

int ObSchemaUtils::try_check_parallel_ddl_schema_in_sync(
    const ObTimeoutCtx &ctx,
    sql::ObSQLSessionInfo *session,
    const uint64_t tenant_id,
    const int64_t schema_version,
    const bool skip_consensus)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  ObMultiVersionSchemaService *schema_service = NULL;
  int64_t consensus_timeout = 30 * 1000 * 1000L; // 30s
  omt::ObTenantConfigGuard tenant_config(OTC_MGR.get_tenant_config_with_lock(tenant_id));
  if (tenant_config.is_valid()) {
    consensus_timeout = tenant_config->_wait_interval_after_parallel_ddl;
  }
  if (OB_ISNULL(session) || OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
      || schema_version <= 0
      || consensus_timeout < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), KP(session), K(tenant_id), K(schema_version), K(consensus_timeout));
  } else if (OB_ISNULL(schema_service = GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", KR(ret));
  }
  bool is_dropped = false;
  while (OB_SUCC(ret) && ctx.get_timeout() > 0) {
    int64_t refreshed_schema_version = OB_INVALID_VERSION;
    int64_t consensus_schema_version = OB_INVALID_VERSION;
    if (OB_FAIL(schema_service->check_if_tenant_has_been_dropped(tenant_id, is_dropped))) {
      LOG_WARN("fail to check if tenant has been dropped", KR(ret), K(tenant_id));
    } else if (OB_UNLIKELY(is_dropped)) {
      ret = OB_TENANT_HAS_BEEN_DROPPED;
      LOG_WARN("tenant has been dropped", KR(ret), K(tenant_id));
    } else if (OB_FAIL(ObDDLExecutorUtil::handle_session_exception(*session))) {
      LOG_WARN("fail to handle session exception", KR(ret));
    } else if (OB_FAIL(schema_service->get_tenant_refreshed_schema_version(tenant_id, refreshed_schema_version))) {
      LOG_WARN("get refreshed schema_version fail", KR(ret), K(tenant_id));
    } else if (OB_FAIL(schema_service->get_tenant_broadcast_consensus_version(tenant_id, consensus_schema_version))) {
      LOG_WARN("get consensus schema_version fail", KR(ret), K(tenant_id));
    } else if (refreshed_schema_version >= schema_version
                && consensus_schema_version >= schema_version) {
      break;
    } else if (refreshed_schema_version >= schema_version
                && (skip_consensus
                    || ObTimeUtility::current_time() - start_time >= consensus_timeout)) {
      break;
    } else {
      if (REACH_TIME_INTERVAL(1000 * 1000L)) { // 1s
        LOG_WARN("schema version not sync", K(tenant_id), K(consensus_timeout),
                 K(refreshed_schema_version), K(consensus_schema_version), K(schema_version));
      }
      ob_usleep(10 * 1000L); // 10ms
    }
  }
  return ret;
}

int ObSchemaUtils::build_column_group(
    const ObTableSchema &table_schema,
    const uint64_t tenant_id,
    const ObColumnGroupType &cg_type,
    const ObString &cg_name,
    const ObIArray<uint64_t> &column_ids,
    const uint64_t cg_id,
    ObColumnGroupSchema &column_group)
{
  int ret = OB_SUCCESS;
  lib::Worker::CompatMode mode = lib::Worker::CompatMode::INVALID;
  column_group.reset();
  if (!is_valid_tenant_id(tenant_id) || /*table_schema may be not valid*/
      cg_name.empty() || (cg_type >= ObColumnGroupType::MAX_COLUMN_GROUP)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",K(table_schema), KR(ret), K(cg_name), K(cg_type), "column_id_cnt", column_ids.count());
  } else if(OB_FAIL(ObCompatModeGetter::get_tenant_mode(tenant_id, mode))) {
    LOG_WARN("fail to check if oralce mode", K(ret), K(table_schema));
  } else {
    column_group.set_column_group_id(cg_id);
    column_group.set_column_group_type(cg_type);
    column_group.set_block_size(table_schema.get_block_size());
    column_group.set_compressor_type(table_schema.get_compressor_type());
    const ObStoreFormatType store_format = table_schema.get_store_format();
    bool is_flat = lib::Worker::CompatMode::ORACLE == mode ? ((OB_STORE_FORMAT_NOCOMPRESS_ORACLE == store_format)
                                            || (OB_STORE_FORMAT_BASIC_ORACLE == store_format)
                                            || (OB_STORE_FORMAT_OLTP_ORACLE == store_format))
                                         : ((OB_STORE_FORMAT_REDUNDANT_MYSQL == store_format)
                                            || (OB_STORE_FORMAT_COMPACT_MYSQL == store_format));
    if (is_flat) {
      // use the encoding type according to the row format
      column_group.set_row_store_type(table_schema.get_row_store_type());
    } else {
      // row_store encoding type according to the row format; column_store uses cs_encoding
      if ((cg_type == ObColumnGroupType::DEFAULT_COLUMN_GROUP) || (cg_type == ObColumnGroupType::ALL_COLUMN_GROUP)) {
        column_group.set_row_store_type(table_schema.get_row_store_type());
      } else {
        column_group.set_row_store_type(ObRowStoreType::CS_ENCODING_ROW_STORE);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(column_group.set_column_group_name(cg_name))) {
        LOG_WARN("fail to set column group name", KR(ret), K(cg_name));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && (i < column_ids.count()); ++i) {
      if (OB_FAIL(column_group.add_column_id(column_ids.at(i)))) {
        LOG_WARN("fail to add column_id into column_group", KR(ret), K(i), "column_id", column_ids.at(i));
      }
    }
  }
  return ret;
}

int ObSchemaUtils::batch_get_latest_table_schemas(
    common::ObISQLClient &sql_client,
    common::ObIAllocator &allocator,
    const uint64_t tenant_id,
    const common::ObIArray<ObObjectID> &table_ids,
    common::ObIArray<ObSimpleTableSchemaV2 *> &table_schemas)
{
  int ret = OB_SUCCESS;
  table_schemas.reset();
  ObSchemaService *schema_service = NULL;
  ObArray<ObTableLatestSchemaVersion> table_schema_versions;
  ObArray<SchemaKey> need_refresh_table_schema_keys;
  ObArray<ObSimpleTableSchemaV2 *> table_schemas_from_inner_table;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || table_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(table_ids));
  } else if (OB_ISNULL(GCTX.schema_service_)
      || OB_ISNULL(schema_service = GCTX.schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("multiversion_schema_service or schema_service is null", KR(ret));
  } else if (OB_FAIL(schema_service->get_table_latest_schema_versions(
      sql_client,
      tenant_id,
      table_ids,
      table_schema_versions))) {
    LOG_WARN("get table latest schema versions failed", KR(ret), K(tenant_id), K(table_ids));
  } else if (OB_FAIL(batch_get_table_schemas_from_cache_(
      allocator,
      tenant_id,
      table_schema_versions,
      need_refresh_table_schema_keys,
      table_schemas))) {
    LOG_WARN("batch get table schemas from cache failed", KR(ret), K(table_schema_versions));
  } else if (OB_FAIL(batch_get_table_schemas_from_inner_table_(
      sql_client,
      allocator,
      tenant_id,
      need_refresh_table_schema_keys,
      table_schemas_from_inner_table))) {
    LOG_WARN("batch get table_schemas from inner table failed", KR(ret), K(need_refresh_table_schema_keys));
  } else if (OB_FAIL(common::append(table_schemas, table_schemas_from_inner_table))) {
    LOG_WARN("append failed", KR(ret), "table_schemas count", table_schemas.count(),
        "table_schemas_from_inner_table count", table_schemas_from_inner_table.count());
  } else if (table_ids.count() != table_schemas.count()) {
    LOG_INFO("get less table_schemas, some tables have been deleted", K(tenant_id),
        "table_ids count", table_ids.count(), "table_schemas count", table_schemas.count(),
        K(table_ids), K(table_schema_versions), K(need_refresh_table_schema_keys));
  }
  // check table schema ptr
  ARRAY_FOREACH(table_schemas, idx) {
    const ObSimpleTableSchemaV2 *table_schema = table_schemas.at(idx);
    if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table_schema can't be null", KR(ret), K(idx), K(table_ids), K(table_schemas));
    }
  }
  return ret;
}

int ObSchemaUtils::alter_rowkey_column_group(share::schema::ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  bool is_each_cg_exist = false;
  bool is_all_cg_exist = false;
  ObColumnGroupSchema *rowkey_cg = nullptr;
  /* scan all column column group*/

  /*get rowkey_cg*/
  if (OB_FAIL(table_schema.get_column_group_by_name(OB_ROWKEY_COLUMN_GROUP_NAME, rowkey_cg))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      rowkey_cg = nullptr;
    } else {
      LOG_WARN("Fail to get rowkey column group", K(ret), K(table_schema));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(table_schema.is_column_group_exist(OB_ALL_COLUMN_GROUP_NAME, is_all_cg_exist))) {
      LOG_WARN("Fail to check whetehre all column group exist", K(ret));
    } else if (OB_FAIL(table_schema.is_column_group_exist(OB_EACH_COLUMN_GROUP_NAME, is_each_cg_exist))) {
      LOG_WARN("Fail to check whether each column group exist", K(ret));
    }
  }


  if (OB_SUCC(ret)) {
    /* only when only each exist, rowkey_cg is needed*/
    if (is_each_cg_exist && (!is_all_cg_exist)) {
      if (OB_ISNULL(rowkey_cg)) {
        ObColumnGroupSchema new_rowkey_cg;
        ObArray<uint64_t> rowkey_ids;
        uint64_t rowkey_cg_id =  table_schema.get_max_used_column_group_id() + 1;
        ObTableSchema::const_column_iterator iter_begin = table_schema.column_begin();
        ObTableSchema::const_column_iterator iter_end = table_schema.column_end();
        for (; OB_SUCC(ret) && iter_begin != iter_end; ++iter_begin) {
          ObColumnSchemaV2 *column = (*iter_begin);
          if (OB_ISNULL(column)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column schema should not be null", K(ret));
          } else if (column->is_rowkey_column()) {
            if (OB_FAIL(rowkey_ids.push_back(column->get_column_id()))) {
              LOG_WARN("fail to push back value", K(ret));
            }
          }
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ObSchemaUtils::build_column_group(
                               table_schema, table_schema.get_tenant_id(),ObColumnGroupType::ROWKEY_COLUMN_GROUP,
                               OB_ROWKEY_COLUMN_GROUP_NAME, rowkey_ids, rowkey_cg_id, new_rowkey_cg))) {
          LOG_WARN("fail to build rowkey column group", K(ret));
        } else if (OB_FAIL(table_schema.add_column_group(new_rowkey_cg))) {
          LOG_WARN("fail to add rowkey column group to table_schema", K(ret));
        }
      } else {
        /*rowkey cg exist skip*/
      }
    } else {
      /*other situation, rowkey column group should not exist*/
      if (OB_NOT_NULL(rowkey_cg)) {
        if (OB_FAIL(table_schema.remove_column_group(rowkey_cg->get_column_group_id()))){
          LOG_WARN("fail to remove rowkey cg", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSchemaUtils::check_sys_table_exist_by_sql(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObObjectID &table_id,
    bool &exist)
{
  int ret = OB_SUCCESS;
  exist = false;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || OB_INVALID_ID == table_id
      || !is_sys_table(table_id)
      || is_core_table(table_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(table_id));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      ObSqlString sql;
      common::sqlclient::ObMySQLResult *res = NULL;
      // in __all_table, tenant_id is primary key and it's value is 0
      if (OB_FAIL(sql.append_fmt(
          "SELECT count(*) = 1 AS exist FROM %s WHERE tenant_id = 0 and table_id = %lu",
          OB_ALL_TABLE_TNAME, table_id))) {
        LOG_WARN("fail to assign sql", KR(ret));
      } else if (OB_FAIL(sql_client.read(result, tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(sql));
      } else if (OB_ISNULL(res = result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(tenant_id), K(sql));
      } else if (OB_FAIL(res->next())) {
        LOG_WARN("next failed", KR(ret), K(sql));
      } else if (OB_FAIL(res->get_bool("exist", exist))) {
        LOG_WARN("get bool value failed", KR(ret), K(sql));
      }
    }
  }
  return ret;
}

/* check all column group add add not exi*/
int ObSchemaUtils::alter_default_column_group(share::schema::ObTableSchema &new_table_schema)
{
  int ret = OB_SUCCESS;
  ObColumnGroupSchema* default_cg = nullptr;
  hash::ObHashSet<uint64_t> cg_column_ids;
  ObArray<ObColumnGroupSchema*> column_groups;
  bool is_all_cg_exist = false;
  ObColumnGroupSchema* all_column_group = nullptr;
  column_groups.reset();
  if (!new_table_schema.is_column_store_supported()) {
    /* skip */
  } else {
    if (new_table_schema.get_column_group_count() < 1) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("there is no column group in the table schema", K(ret), K(new_table_schema));
    } else if (OB_FAIL(cg_column_ids.create(new_table_schema.get_column_count()))) {
      LOG_WARN("fail to create hashmap", K(ret));
    }

    ObTableSchema::const_column_group_iterator iter_begin = new_table_schema.column_group_begin();
    ObTableSchema::const_column_group_iterator iter_end = new_table_schema.column_group_end();

    for (; OB_SUCC(ret) && iter_begin != iter_end; iter_begin++) {
      const ObColumnGroupSchema *column_group = *iter_begin;
      if (OB_ISNULL(column_group)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column group should not be null", K(ret), K(new_table_schema));
      } else if (column_group->get_column_group_type() != ObColumnGroupType::DEFAULT_COLUMN_GROUP) {
        for (int64_t col_index = 0;
          OB_SUCC(ret) && col_index < column_group->get_column_id_count();
          col_index++) {
          if (OB_FAIL(cg_column_ids.set_refactored((column_group->get_column_ids())[col_index],
                                                1 /*overwrite*/))) {
            LOG_WARN("fail to add column id", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(new_table_schema.get_column_group_by_name(OB_DEFAULT_COLUMN_GROUP_NAME, default_cg))) {
        LOG_WARN("fail to get all columns in column groups", K(ret), K(new_table_schema));
      } else {
        default_cg->remove_all_cols();
        ObArray<int64_t> col_ids;
        /*schema may not be ready not use default func*/
        ObTableSchema::const_column_iterator iter_begin = new_table_schema.column_begin();
        ObTableSchema::const_column_iterator iter_end = new_table_schema.column_end();
        for (;OB_SUCC(ret) && iter_begin != iter_end; ++iter_begin) {
          ObColumnSchemaV2 *column = (*iter_begin);
          if (OB_ISNULL(column)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column schema should not be null", K(ret));
          } else if (column->is_virtual_generated_column()) {
            /* skip virtual column*/
          } else if (OB_FAIL(col_ids.push_back(column->get_column_id()))) {
            LOG_WARN("fail to push back value", K(ret));
          }
        }

        for (int64_t i = 0; OB_SUCC(ret) && i < col_ids.count(); i++) {
          int hash_ret = cg_column_ids.exist_refactored(col_ids[i]);
          if (hash_ret != OB_HASH_EXIST && hash_ret != OB_HASH_NOT_EXIST) {
            ret = hash_ret;
            LOG_WARN("fail to check key exist", K(ret));
          } else if (OB_HASH_EXIST == hash_ret) {
            /*skip, column exist in other column group don't need to be added*/
          } else if (OB_HASH_NOT_EXIST == hash_ret) {
            if (OB_FAIL(default_cg->add_column_id(col_ids[i]))) {
              LOG_WARN("fail to add column to default cg", KPC(default_cg), K(col_ids[i]), K(new_table_schema));
            }
          }
        }

        /*default cg check, used when only support all/each column group*/
        if (OB_SUCC(ret) && default_cg->get_column_id_count() != 0 && default_cg->get_column_id_count() != col_ids.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("default column group have invalid column id count", K(ret), KPC(default_cg));
        }
      }
    }
  }

  return ret;
}


/*build and add each column group*/
int ObSchemaUtils::build_add_each_column_group(const share::schema::ObTableSchema &table_schema,
                                                 share::schema::ObTableSchema &dst_table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  HEAP_VAR(share::schema::ObTableSchema, tmp_table_schema) {
    if (OB_FAIL(tmp_table_schema.assign(table_schema))) {
      LOG_WARN("failed to assign table schema", K(ret), K(table_schema));
    } else if (OB_FAIL(GET_MIN_DATA_VERSION(table_schema.get_tenant_id(), compat_version))) {
      LOG_WARN("fail to get data version", K(ret), K(table_schema.get_tenant_id()));
    } else if (compat_version < DATA_VERSION_4_3_1_0) {
    } else if (OB_FAIL(tmp_table_schema.sort_column_array_by_column_id())) {
      LOG_WARN("failed to sort column", K(ret), K(tmp_table_schema));
    }
    if (OB_SUCC(ret)) {
      ObColumnGroupSchema column_group_schema;
      ObTableSchema::const_column_iterator iter_begin = tmp_table_schema.column_begin();
      ObTableSchema::const_column_iterator iter_end = tmp_table_schema.column_end();
      for (;OB_SUCC(ret) && iter_begin != iter_end; ++iter_begin) {
        column_group_schema.reset();
        ObColumnSchemaV2 *column = (*iter_begin);
        uint64_t cg_id = dst_table_schema.get_max_used_column_group_id() + 1;
        if (OB_ISNULL(column)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column schema should not be null", K(ret));
        } else if (column->is_virtual_generated_column()) {
            /* skip virtual column*/
        } else if (OB_FAIL(ObSchemaUtils::build_single_column_group(
                                      table_schema, column, dst_table_schema.get_tenant_id(),
                                      cg_id, column_group_schema))) {
            LOG_WARN("fail to build single column group", K(ret));
        } else if (column_group_schema.is_valid()) {
          if (OB_FAIL(dst_table_schema.add_column_group(column_group_schema))) {
            LOG_WARN("fail to add single column group to table schema", K(ret), K(column_group_schema));
          }
        }
      }
    }
  }
  return ret;
}

int ObSchemaUtils::build_single_column_group(
    const share::schema::ObTableSchema &table_schema,
    share::schema::ObColumnSchemaV2 *column_schema,
    const uint64_t tenant_id,
    const uint64_t column_group_id,
    share::schema::ObColumnGroupSchema &column_group_schema)
{
  /* check and build single_column_group for the column*/
  /* table schema may be not fully constructed and do not check here*/
  int ret = OB_SUCCESS;
  column_group_schema.reset();
  if (OB_ISNULL(column_schema) || column_group_id < COLUMN_GROUP_START_ID
      || column_schema->is_virtual_generated_column()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, column is null or virtual column or invalid column group id",
             K(ret), KPC(column_schema), K(column_group_id));
  } else {
    ObArray<uint64_t> column_ids;
    char cg_name_ptr[OB_MAX_COLUMN_GROUP_NAME_LENGTH] = {'\0'};
    ObString cg_name(sizeof(cg_name_ptr), 0 /*length*/, cg_name_ptr);
    column_ids.reset();
    if (OB_FAIL(column_ids.push_back(column_schema->get_column_id()))) {
      LOG_WARN("fail to add column group", K(ret));
    } else if (OB_FAIL(column_schema->get_each_column_group_name(cg_name))){
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to format column group_name", K(ret));
    } else if (OB_FAIL(build_column_group(table_schema, tenant_id,
                                          ObColumnGroupType::SINGLE_COLUMN_GROUP,
                                          cg_name, column_ids, column_group_id, column_group_schema))) {
      LOG_WARN("fail to build column group for single column", K(ret));
    }
  }
  return ret;
}

int ObSchemaUtils::build_all_column_group(
    const share::schema::ObTableSchema &table_schema,
    const uint64_t tenant_id,
    const uint64_t column_group_id,
    share::schema::ObColumnGroupSchema &column_group_schema)
{
  int ret = OB_SUCCESS;
  column_group_schema.reset();
  /* table_schema_ is constructed by resolver and is not valid*/
  if (column_group_id < COLUMN_GROUP_START_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, table schema or column group_id is invalid",
             K(ret), K(table_schema), K(column_group_id));
  } else{
    ObArray<uint64_t> column_ids;

    ObTableSchema::const_column_iterator iter_begin = table_schema.column_begin();
    ObTableSchema::const_column_iterator iter_end = table_schema.column_end();
    for (; OB_SUCC(ret) && iter_begin != iter_end; ++iter_begin) {
      column_group_schema.reset();
      ObColumnSchemaV2 *column = (*iter_begin);
      if (OB_ISNULL(column)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column schema should not be null", K(ret));
      } else if (column->is_virtual_generated_column()) {
          /* skip virtual column*/
      } else if (OB_FAIL(column_ids.push_back(column->get_column_id()))) {
        LOG_WARN("fail to push back value", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else {
      const ObString cg_name = OB_ALL_COLUMN_GROUP_NAME;
      if (column_ids.count() <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("number of available columns should not be zeror", K(ret));

      } else if (OB_FAIL(build_column_group(table_schema, tenant_id,
                                            ObColumnGroupType::ALL_COLUMN_GROUP, cg_name,
                                            column_ids, column_group_id, column_group_schema))) {
        LOG_WARN("fail to build column group", K(ret));
      }
    }
  }
  return ret;
}

int ObSchemaUtils::mock_default_cg(
    const uint64_t tenant_id,
    share::schema::ObTableSchema &new_table_schema)
{
  int ret = OB_SUCCESS;
  ObColumnGroupSchema* default_cg = nullptr;
  /* for table update from less than 4.2, default cg may not exist
   *  mock a default cg
  */
  uint64_t compat_version = 0;

  if (OB_INVALID == tenant_id || new_table_schema.get_table_id() == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(new_table_schema), K(tenant_id));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get tenant, data_version", K(ret));
  } else if (compat_version < DATA_VERSION_4_3_0_0) {
    /* skip, do nothing*/
  } else if (!(new_table_schema.is_user_table() || new_table_schema.is_tmp_table() || new_table_schema.is_index_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("should not mock inner table", K(ret), K(new_table_schema));
  } else {
    new_table_schema.set_column_store(true);
    if (OB_FAIL(new_table_schema.get_column_group_by_name(OB_DEFAULT_COLUMN_GROUP_NAME, default_cg))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        default_cg = nullptr;
      } else {
        LOG_WARN("fail to get default column group", K(ret), K(new_table_schema));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(default_cg)) {
      ObColumnGroupSchema tmp_cg;
      ObArray<uint64_t> column_ids;
      if (OB_FAIL(new_table_schema.get_all_column_ids(column_ids))) {
        LOG_WARN("fail to get column ids", K(ret), K(new_table_schema));
      } else if (OB_FAIL(ObSchemaUtils::build_column_group(new_table_schema, new_table_schema.get_tenant_id(),
                                        ObColumnGroupType::DEFAULT_COLUMN_GROUP,
                                        OB_DEFAULT_COLUMN_GROUP_NAME, column_ids,
                                        DEFAULT_TYPE_COLUMN_GROUP_ID, tmp_cg))) {
        LOG_WARN("fail to build column group", K(ret));
      } else if (OB_FAIL(new_table_schema.add_column_group(tmp_cg))) {
        LOG_WARN("failt to add default column group", K(ret));
      }
    }
  }
  return ret;
}

int ObSchemaUtils::get_latest_table_schema(
    common::ObISQLClient &sql_client,
    common::ObIAllocator &allocator,
    const uint64_t tenant_id,
    const ObObjectID &table_id,
    ObSimpleTableSchemaV2 *&table_schema)
{
  int ret = OB_SUCCESS;
  table_schema = NULL;
  ObSEArray<ObObjectID, 1> table_ids;
  ObSEArray<ObSimpleTableSchemaV2 *, 1> table_schemas;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || OB_INVALID_ID == table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_FAIL(table_ids.push_back(table_id))) {
    LOG_WARN("push back failed", KR(ret), K(table_id), K(table_ids));
  } else if (OB_FAIL(batch_get_latest_table_schemas(
      sql_client,
      allocator,
      tenant_id,
      table_ids,
      table_schemas))) {
    LOG_WARN("batch get latest table schema failed", KR(ret), K(table_id));
  } else if (table_schemas.empty()) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist when get latest table schema", KR(ret), K(table_id));
  } else if (OB_ISNULL(table_schemas.at(0))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema can not be null", KR(ret), K(table_id), K(table_schemas));
  } else {
    table_schema = table_schemas.at(0);
  }
  return ret;
}

int ObSchemaUtils::batch_get_table_schemas_from_cache_(
    common::ObIAllocator &allocator,
    const uint64_t tenant_id,
    const ObIArray<ObTableLatestSchemaVersion> &table_schema_versions,
    common::ObIArray<SchemaKey> &need_refresh_table_schema_keys,
    common::ObIArray<ObSimpleTableSchemaV2 *> &table_schemas)
{
  int ret = OB_SUCCESS;
  need_refresh_table_schema_keys.reset();
  table_schemas.reset();
  ObSchemaGetterGuard schema_guard;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("multiversion_schema_service is null", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
      tenant_id,
      schema_guard))) {
    LOG_WARN("get schema guard failed", KR(ret), K(tenant_id));
  } else {
    ARRAY_FOREACH(table_schema_versions, idx) {
      const ObSimpleTableSchemaV2 *cached_table_schema = NULL;
      ObSimpleTableSchemaV2 *new_table_schema = NULL;
      const ObTableLatestSchemaVersion &table_schema_version = table_schema_versions.at(idx);
      if (table_schema_version.is_deleted()) {
        LOG_INFO("table has been deleted", K(tenant_id), K(table_schema_version));
        // skip
      } else if (OB_FAIL(schema_guard.get_simple_table_schema(
          tenant_id,
          table_schema_version.get_table_id(),
          cached_table_schema))) {
        LOG_WARN("get simple table schema failed", KR(ret), K(tenant_id), K(table_schema_version));
      } else if (OB_ISNULL(cached_table_schema)
          || (cached_table_schema->get_schema_version() < table_schema_version.get_schema_version())) {
        // need fetch new table schema
        SchemaKey table_schema_key;
        table_schema_key.tenant_id_ = tenant_id;
        table_schema_key.table_id_ = table_schema_version.get_table_id();
        if (OB_FAIL(need_refresh_table_schema_keys.push_back(table_schema_key))) {
          LOG_WARN("push back failed", KR(ret), K(table_schema_version));
        }
      } else if (OB_FAIL(alloc_schema(allocator, *cached_table_schema, new_table_schema))) {
        LOG_WARN("fail to alloc schema", KR(ret), K(tenant_id), KPC(cached_table_schema));
      } else if (OB_FAIL(table_schemas.push_back(new_table_schema))) {
        LOG_WARN("push back failed", KR(ret), KP(new_table_schema));
      }
    } // end ARRAY_FOREACH
  }
  return ret;
}

int ObSchemaUtils::batch_get_table_schemas_from_inner_table_(
    common::ObISQLClient &sql_client,
    common::ObIAllocator &allocator,
    const uint64_t tenant_id,
    common::ObArray<SchemaKey> &need_refresh_table_schema_keys,
    common::ObIArray<ObSimpleTableSchemaV2 *> &table_schemas)
{
  int ret = OB_SUCCESS;
  table_schemas.reset();
  ObSchemaService *schema_service = NULL;
  ObRefreshSchemaStatus schema_status;
  schema_status.tenant_id_ = tenant_id;
  int64_t schema_version = INT64_MAX - 1; // get latest schema
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.schema_service_)
      || OB_ISNULL(schema_service = GCTX.schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("multiversion_schema_service or schema_service is null", KR(ret));
  } else if (need_refresh_table_schema_keys.empty()) {
    // skip
  } else if (OB_FAIL(schema_service->get_batch_tables(
      schema_status,
      sql_client,
      allocator,
      schema_version,
      need_refresh_table_schema_keys,
      table_schemas))) {
    LOG_WARN("get batch tables failed", KR(ret),
        K(schema_status), K(schema_version), K(need_refresh_table_schema_keys));
  }
  return ret;
}

int ObSchemaUtils::check_whether_column_exist(
    const uint64_t tenant_id,
    const ObObjectID &table_id,
    const ObString &column_name,
    bool &exist)
{
  int ret = OB_SUCCESS;
  exist = false;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || OB_INVALID_ID == table_id
      || column_name.empty()
      || !is_sys_table(table_id)
      || is_core_table(table_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(tenant_id), K(table_id), K(column_name));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      ObSqlString sql;
      common::sqlclient::ObMySQLResult *res = NULL;
      // in __all_column, tenant_id is primary key and it's value is 0
      if (OB_FAIL(sql.append_fmt(
          "SELECT count(*) = 1 AS exist FROM %s WHERE tenant_id = 0 and table_id = %lu and column_name = '%.*s'",
          OB_ALL_COLUMN_TNAME, table_id, column_name.length(), column_name.ptr()))) {
        LOG_WARN("fail to assign sql", KR(ret));
      } else if (OB_FAIL(GCTX.sql_proxy_->read(result, tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(sql));
      } else if (OB_ISNULL(res = result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(tenant_id), K(sql));
      } else if (OB_FAIL(res->next())) {
        LOG_WARN("next failed", KR(ret), K(sql));
      } else if (OB_FAIL(res->get_bool("exist", exist))) {
        LOG_WARN("get max task id failed", KR(ret), K(sql));
      }
    }
  }
  return ret;
}

int ObSchemaUtils::is_drop_column_only(const AlterTableSchema &alter_table_schema, bool &is_drop_col_only)
{
  int ret = OB_SUCCESS;
  is_drop_col_only = true;
  ObTableSchema::const_column_iterator it_begin = alter_table_schema.column_begin();
  ObTableSchema::const_column_iterator it_end = alter_table_schema.column_end();
  for (; OB_SUCC(ret) && is_drop_col_only && it_begin != it_end; it_begin++) {
    const AlterColumnSchema *alter_col = static_cast<AlterColumnSchema*>(*it_begin);
    if (OB_ISNULL(alter_col)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("alter col should not be null", K(ret), K(alter_table_schema));
    } else if (OB_DDL_DROP_COLUMN != alter_col->alter_type_) {
      is_drop_col_only = false;
    }
  }
  return ret;
}
const char* DDLType[]
{
  "TRUNCATE_TABLE",
  "SET_COMMENT",
  "CREATE_INDEX",
  "CREATE_VIEW"
};

int ObParallelDDLControlMode::string_to_ddl_type(const ObString &ddl_string, ObParallelDDLType &ddl_type)
{
  int ret = OB_SUCCESS;
  ddl_type = MAX_TYPE;
  STATIC_ASSERT((ARRAYSIZEOF(DDLType)) == MAX_TYPE, "size count not match");
  bool find = false;
  for (uint64_t i = 0; !find && i < ARRAYSIZEOF(DDLType); i++) {
    if (ddl_string.case_compare(DDLType[i]) == 0) {
      find = true;
      ddl_type = static_cast<ObParallelDDLType>(i);
    }
  }
  if (OB_UNLIKELY(!find)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "unknown ddl_type", KR(ret), K(ddl_string));
  }
  return ret;
}

int ObParallelDDLControlMode::set_value(const ObConfigModeItem &mode_item)
{
  int ret = OB_SUCCESS;
  const uint8_t* values = mode_item.get_value();
  if (OB_ISNULL(values)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "mode item's value_ is null ptr", KR(ret));
  } else {
    STATIC_ASSERT(((sizeof(value_)/sizeof(uint8_t) <= ObConfigModeItem::MAX_MODE_BYTES)),
                  "value_ size overflow");
    STATIC_ASSERT( (MAX_TYPE * 2) <= (sizeof(value_) * 8), "type size overflow");
    value_ = 0;
    for (uint64_t i = 0; i < 8; ++i) {
      value_ = (value_ | static_cast<uint64_t>(values[i]) << (8 * i));
    }
  }
  return ret;
}

int ObParallelDDLControlMode::set_parallel_ddl_mode(const ObParallelDDLType type, const uint8_t mode)
{
  int ret = OB_SUCCESS;
  if ((TRUNCATE_TABLE <= type) && (type < MAX_TYPE)) {
    uint64_t shift = static_cast<uint64_t>(type);
    if (!check_mode_valid_(mode)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "mode invalid", KR(ret), K(mode));
    } else {
      uint64_t mask = MASK << (shift * MASK_SIZE);
      value_ = (value_ & ~mask) | (static_cast<uint64_t>(mode) << (shift * MASK_SIZE));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "type invalid", KR(ret), K(type));
  }
  return ret;
}

int ObParallelDDLControlMode::is_parallel_ddl(const ObParallelDDLType type, bool &is_parallel)
{
  int ret = OB_SUCCESS;
  is_parallel = true;
  if ((TRUNCATE_TABLE <= type) && (type < MAX_TYPE)) {
    uint64_t shift = static_cast<uint64_t>(type);
    uint8_t value = static_cast<uint8_t>((value_ >> (shift * MASK_SIZE)) & MASK);
    if (value == ObParallelDDLControlParser::MODE_OFF) {
      is_parallel = false;
    } else if (value == ObParallelDDLControlParser::MODE_ON) {
      is_parallel = true;
    } else if (value == ObParallelDDLControlParser::MODE_DEFAULT) {
      if (TRUNCATE_TABLE == type) {
        is_parallel = true;
      } else {
        is_parallel = false;
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "invalid value unexpected", KR(ret), K(value));
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "type invalid", KR(ret), K(type));
  }
  return ret;
}

int ObParallelDDLControlMode::is_parallel_ddl_enable(const ObParallelDDLType ddl_type, const uint64_t tenant_id, bool &is_parallel)
{
  int ret = OB_SUCCESS;
  is_parallel = true;
  ObParallelDDLControlMode cfg;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (OB_UNLIKELY(!tenant_config.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "invalid tenant config", KR(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_config->_parallel_ddl_control.init_mode(cfg))) {
    LOG_WARN("init mode failed", KR(ret));
  } else if (OB_FAIL(cfg.is_parallel_ddl(ddl_type, is_parallel))) {
    LOG_WARN("fail to check is parallel ddl", KR(ret), K(ddl_type));
  }
  return ret;
}

bool ObSchemaUtils::can_add_column_group(const ObTableSchema &table_schema)
{
  bool can_add_cg = false;
  if (table_schema.is_user_table()
      || table_schema.is_tmp_table()
      || table_schema.is_index_table()) {
    can_add_cg = true;
  }
  return can_add_cg;
}

int ObParallelDDLControlMode::generate_parallel_ddl_control_config_for_create_tenant(ObSqlString &config_value)
{
  int ret = OB_SUCCESS;
  int ddl_type_size = ARRAYSIZEOF(DDLType);
  for (int i = 0; OB_SUCC(ret) && i < (ddl_type_size - 1); ++i) {
    if (OB_FAIL(config_value.append_fmt("%s:ON, ", DDLType[i]))) {
      LOG_WARN("fail to append fmt", KR(ret), K(i));
    }
  }
  if ((ddl_type_size > 0)
      && FAILEDx(config_value.append_fmt("%s:ON", DDLType[ddl_type_size - 1]))) {
    LOG_WARN("fail to append fmt", KR(ret), K(ddl_type_size));
  }
  return ret;
}

} // end schema
} // end share
} // end oceanbase
