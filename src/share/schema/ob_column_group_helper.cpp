/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE_SCHEMA
#include "share/schema/ob_column_group_helper.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_table_sql_service.h"
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_table_param.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_tenant_id_schema_version.h"
#include "storage/column_store/ob_column_store_util.h"

namespace oceanbase
{
using namespace common;
namespace share
{
namespace schema
{

int ObColumnGroupHelper::batch_add_column_groups_for_create_table(ObISQLClient &sql_client,
                                                                  const ObIArray<ObTableSchema> &tables)
{
  int ret = OB_SUCCESS;
  if (tables.empty()) {
  } else {
    common::ObTimeGuard time_guard("batch_add_column_groups_for_create_table", 1_ms);
    const uint64_t tenant_id = tables.at(0).get_tenant_id();
    ObDMLSqlSplicer cg_dml;
    ObDMLSqlSplicer cg_history_dml;
    ObDMLSqlSplicer mapping_dml;
    ObDMLSqlSplicer mapping_history_dml;
    int64_t column_group_cnt = 0;
    int64_t mapping_cnt = 0;
    uint64_t data_version = 0;
    if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
      LOG_WARN("fail to get min data version", KR(ret), K(tenant_id));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < tables.count(); i++) {
      const ObTableSchema &table = tables.at(i);
      if (table.get_tenant_id() != tenant_id) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant id not same", KR(ret), K(table), K(tenant_id));
      } else if (OB_FAIL(ObColumnGroupHelper::append_column_group_dml_for_create_table(table,
                  data_version, cg_dml, cg_history_dml, mapping_dml, mapping_history_dml,
                  column_group_cnt, mapping_cnt))) {
        ObCStringHelper helper;
        LOG_WARN("append column group dml failed", KR(ret), "table", helper.convert(table));
      }
    }
    if (OB_SUCC(ret)) {
      time_guard.click("generate_dml");
      if (FAILEDx(ObTableSqlService::exec_dml(sql_client, tenant_id, OB_ALL_COLUMN_GROUP_TNAME, cg_dml, column_group_cnt))) {
        LOG_WARN("failed to insert all_column_group", KR(ret), K(tenant_id), K(column_group_cnt));
      } else if (FALSE_IT(time_guard.click("insert_all_column_group"))) {
      } else if (FAILEDx(ObTableSqlService::exec_dml(sql_client, tenant_id, OB_ALL_COLUMN_GROUP_HISTORY_TNAME, cg_history_dml, column_group_cnt))) {
        LOG_WARN("failed to insert all_column_group_history", KR(ret), K(tenant_id), K(column_group_cnt));
      } else if (FALSE_IT(time_guard.click("insert_all_column_group_history"))) {
      } else if (FAILEDx(ObTableSqlService::exec_dml(sql_client, tenant_id, OB_ALL_COLUMN_GROUP_MAPPING_TNAME, mapping_dml, mapping_cnt))) {
        LOG_WARN("failed to insert all_column_group_mapping", KR(ret), K(tenant_id), K(mapping_cnt));
      } else if (FALSE_IT(time_guard.click("insert_all_column_group_mapping"))) {
      } else if (FAILEDx(ObTableSqlService::exec_dml(sql_client, tenant_id, OB_ALL_COLUMN_GROUP_MAPPING_HISTORY_TNAME, mapping_history_dml, mapping_cnt))) {
        LOG_WARN("failed to insert all_column_group_mapping_history", KR(ret), K(tenant_id), K(mapping_cnt));
      } else if (FALSE_IT(time_guard.click("insert_all_column_group_mapping_history"))) {
      }
    }
  }
  return ret;
}

int ObColumnGroupHelper::generate_dml_for_create_table(
    const ObTableSchema &table,
    const uint64_t data_version,
    const ObColumnGroupSchema &column_group,
    ObDMLSqlSplicer &cg_dml,
    ObDMLSqlSplicer &cg_history_dml,
    ObDMLSqlSplicer &mapping_dml,
    ObDMLSqlSplicer &mapping_history_dml,
    int64_t &column_group_cnt,
    int64_t &mapping_cnt)
{
  UNUSED(data_version);
  int ret = OB_SUCCESS;
  const int64_t schema_version = table.get_schema_version();
  if (OB_FAIL(ObColumnGroupHelper::gen_column_group_dml(table, column_group, false /*not history*/,
                                   false /*not deleted*/, schema_version, cg_dml))) {
    LOG_WARN("fail to gen column_group_dml", KR(ret), K(table), K(column_group));
  } else if (OB_FAIL(ObColumnGroupHelper::gen_column_group_dml(table, column_group, true /*history*/,
                                          false /*not deleted*/, schema_version, cg_history_dml))) {
    LOG_WARN("fail to gen column_group_history_dml", KR(ret), K(table), K(column_group));
  } else {
    ++column_group_cnt;
    const int64_t column_id_cnt = column_group.get_column_id_count();
    if (column_id_cnt > 0) {
      for (int64_t idx = 0; OB_SUCC(ret) && idx < column_id_cnt; ++idx) {
        uint64_t tmp_column_id = UINT64_MAX;
        if (OB_FAIL(column_group.get_column_id(idx, tmp_column_id))) {
          LOG_WARN("fail to get column id", KR(ret), K(idx), K(column_group));
        } else {
          const int64_t column_id = static_cast<int64_t>(tmp_column_id);
          if (OB_FAIL(ObColumnGroupHelper::gen_column_group_mapping_dml(table, column_group,
                                                   column_id, false /*not history*/,
                                                   false /*not deleted*/, schema_version, mapping_dml))) {
            LOG_WARN("fail to gen column_group_mapping_dml", KR(ret), K(table), K(column_id));
          } else if (OB_FAIL(ObColumnGroupHelper::gen_column_group_mapping_dml(table, column_group,
                                                          column_id, true /*history*/,
                                                          false /*not deleted*/, schema_version, mapping_history_dml))) {
            LOG_WARN("fail to gen column_group_mapping_history_dml", KR(ret), K(table), K(column_id));
          } else {
            ++mapping_cnt;
          }
        }
      }
    } else if (column_group.get_column_group_type() != ObColumnGroupType::DEFAULT_COLUMN_GROUP) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid column group type without column ids", KR(ret), K(column_group));
    }
  }
  return ret;
}

int ObColumnGroupHelper::append_column_group_dml_for_create_table(const ObTableSchema &table,
                                                                  const uint64_t data_version,
                                                                  ObDMLSqlSplicer &cg_dml,
                                                                  ObDMLSqlSplicer &cg_history_dml,
                                                                  ObDMLSqlSplicer &mapping_dml,
                                                                  ObDMLSqlSplicer &mapping_history_dml,
                                                                  int64_t &column_group_cnt,
                                                                  int64_t &mapping_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObColumnGroupHelper::check_column_store_valid(table, data_version))) {
    LOG_WARN("fail to check column store valid", KR(ret), K(table), K(data_version));
  } else if ((data_version < DATA_VERSION_4_3_0_0) || (table.get_column_group_count() < 1)) {
    // skip, no need to persist column_group
  } else if (!table.is_column_store_supported()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("we can persist column_group info only when column_store=true", KR(ret), K(table));
  } else {
    ObTableSchema::const_column_group_iterator it_begin = table.column_group_begin();
    ObTableSchema::const_column_group_iterator it_end = table.column_group_end();
    for (; OB_SUCC(ret) && it_begin != it_end; ++it_begin) {
      const ObColumnGroupSchema *column_group = *it_begin;
      if (OB_ISNULL(column_group)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("column_group schema should not be null", KR(ret), K(table));
      } else if (OB_FAIL(ObColumnGroupHelper::generate_dml_for_create_table(table,
                                                       data_version,
                                                       *column_group,
                                                       cg_dml,
                                                       cg_history_dml,
                                                       mapping_dml,
                                                       mapping_history_dml,
                                                       column_group_cnt,
                                                       mapping_cnt))) {
        LOG_WARN("fail to generate dml for create table", KR(ret), K(table), KPC(column_group));
      }
    }

    if (OB_SUCC(ret)) {
      const ObColumnGroupSchema *hidden_cg = table.get_hidden_rowkey_column_group();
      if (OB_ISNULL(hidden_cg)) {
      } else if (data_version < DATA_VERSION_4_6_1_0) {
        ret = OB_NOT_SUPPORTED;
        LOG_ERROR("hidden rowkey column group should be null", K(ret), K(data_version), K(table));
      } else if (OB_FAIL(ObColumnGroupHelper::generate_dml_for_create_table(table,
                                                       data_version,
                                                       *hidden_cg,
                                                       cg_dml,
                                                       cg_history_dml,
                                                       mapping_dml,
                                                       mapping_history_dml,
                                                       column_group_cnt,
                                                       mapping_cnt))) {
        LOG_WARN("fail to generate dml for create table", KR(ret), K(table), KPC(hidden_cg));
      }
    }
  }
  return ret;
}

int ObColumnGroupHelper::add_column_groups(
    ObISQLClient &sql_client,
    const ObTableSchema &table,
    const int64_t schema_version,
    const bool only_history)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(table.get_tenant_id(), data_version))) {
    LOG_WARN("fail to get min data version", KR(ret), K(table));
  } else if (OB_FAIL(ObColumnGroupHelper::check_column_store_valid(table, data_version))) {
    LOG_WARN("fail to check column store valid", KR(ret));
  } else if ((data_version < DATA_VERSION_4_3_0_0) || (table.get_column_group_count() < 1)) {
  } else if (!table.is_column_store_supported()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("we can persist column_group info only when column_store=true", KR(ret), K(table));
  } else if ((!only_history) && OB_FAIL(ObColumnGroupHelper::exec_insert_column_group(sql_client, table, schema_version, false))) {
    LOG_WARN("fail to insert column group", KR(ret), K(table));
  } else if (OB_FAIL(ObColumnGroupHelper::exec_insert_column_group(sql_client, table, schema_version, true))) {
    LOG_WARN("fail to insert column group history", KR(ret), K(table));
  } else if ((!only_history) && OB_FAIL(ObColumnGroupHelper::exec_insert_column_group_mapping(sql_client, table, schema_version, false))) {
    LOG_WARN("fail to insert column group mapping", KR(ret), K(table));
  } else if (OB_FAIL(ObColumnGroupHelper::exec_insert_column_group_mapping(sql_client, table, schema_version, true))) {
    LOG_WARN("fail to insert column group mapping", KR(ret), K(table));
  }
  return ret;
}

int ObColumnGroupHelper::insert_column_ids_into_column_group(
    ObISQLClient &sql_client,
    const ObTableSchema &table,
    const int64_t schema_version,
    const ObIArray<uint64_t> &column_ids,
    const ObColumnGroupSchema &column_group,
    const bool only_history)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  if (!table.is_column_store_supported()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table not support column store", K(ret));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(table.get_tenant_id(), data_version))) {
    LOG_WARN("fail to get min data version", KR(ret), K(table));
  } else if (OB_FAIL(ObColumnGroupHelper::check_column_store_valid(table, data_version))) {
    LOG_WARN("fail to check column store valid", KR(ret));
  } else if (!only_history && OB_FAIL(ObColumnGroupHelper::exec_insert_column_group_mapping(sql_client, table, schema_version, column_group, column_ids, true))) {
    LOG_WARN("fail to exec_insert_column_group_mapping", K(ret), K(column_ids));
  } else if (OB_FAIL(ObColumnGroupHelper::exec_insert_column_group_mapping(sql_client, table, schema_version, column_group, column_ids, false))) {
    LOG_WARN("fail to exec_insert_column_group_mapping", K(ret), K(column_ids));
  }
  return ret;
}

int ObColumnGroupHelper::check_column_store_valid(const ObTableSchema &table, const uint64_t data_version)
{
  int ret = OB_SUCCESS;
  if (data_version < DATA_VERSION_4_3_0_0) {
    if (table.is_column_store_supported()
        || table.get_max_used_column_group_id() > COLUMN_GROUP_START_ID
        || table.get_column_group_count() > 1) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("can't set column_store or add column_group in current version", KR(ret), K(table));
    } else if (table.get_column_group_count() == 1) {
      const ObColumnGroupSchema *cg = *(table.column_group_begin());
      if (OB_NOT_NULL(cg) && (cg->get_column_group_type() != ObColumnGroupType::DEFAULT_COLUMN_GROUP)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("can't support other type column_group in current_version", KR(ret), K(table));
      }
    }
  }
  return ret;
}

int ObColumnGroupHelper::exec_insert_column_group(
    ObISQLClient &sql_client,
    const ObTableSchema &table,
    const int64_t schema_version,
    bool is_history)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;

  const int64_t cg_cnt = table.get_column_group_count();
  if (cg_cnt > 0) {
    ObDMLSqlSplicer dml;
    ObSqlString sql;
    const char* tname = is_history ? OB_ALL_COLUMN_GROUP_HISTORY_TNAME : OB_ALL_COLUMN_GROUP_TNAME;

    const uint64_t tenant_id = table.get_tenant_id();
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    ObTableSchema::const_column_group_iterator it_begin = table.column_group_begin();
    ObTableSchema::const_column_group_iterator it_end = table.column_group_end();
    const ObColumnGroupSchema *column_group = NULL;

    if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
      LOG_WARN("failed to get data version", K(ret));
    }

    for (; OB_SUCC(ret) && (it_begin != it_end); ++it_begin) {
      column_group = *it_begin;
      if (OB_ISNULL(column_group)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("column_group schema should not be null", KR(ret));
      } else if (OB_FAIL(ObColumnGroupHelper::gen_column_group_dml(table, *column_group, is_history,
                                              false /*not deleted*/, schema_version, dml))){
        LOG_WARN("fail to gen column_group_dml", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      const ObColumnGroupSchema *hidden_cg = table.get_hidden_rowkey_column_group();
      if (OB_ISNULL(hidden_cg)) {
      } else if (data_version < DATA_VERSION_4_6_1_0) {
        ret = OB_NOT_SUPPORTED;
        LOG_ERROR("hidden rowkey column group should be null", K(ret), K(data_version), K(table));
      } else if (OB_FAIL(ObColumnGroupHelper::gen_column_group_dml(table, *hidden_cg, is_history, false /*not deleted*/, schema_version, dml))){
        LOG_WARN("fail to gen column_group_dml", K(ret));
      }
    }

    int64_t affected_rows = 0;
    if (FAILEDx(dml.splice_batch_insert_sql(tname, sql))) {
      LOG_WARN("fail to splice batch update sql", KR(ret), K(sql));
    } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(exec_tenant_id), K(sql));
    } else if (affected_rows != cg_cnt) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows), K(cg_cnt));
    }
  }
  return ret;
}

int ObColumnGroupHelper::exec_insert_column_group_mapping(
    ObISQLClient &sql_client,
    const ObTableSchema &table,
    const int64_t schema_version,
    const ObColumnGroupSchema &column_group,
    const ObIArray<uint64_t> &column_ids,
    const bool is_history)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObDMLSqlSplicer dml;
  const char* tname = is_history ? OB_ALL_COLUMN_GROUP_MAPPING_HISTORY_TNAME : OB_ALL_COLUMN_GROUP_MAPPING_TNAME;
  const uint64_t tenant_id = table.get_tenant_id();
  const uint64_t table_id = table.get_table_id();
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

  const uint64_t tmp_tenant_id = ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id);
  const uint64_t tmp_table_id = ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id);
  const uint64_t tmp_cg_id = ObSchemaUtils::get_extract_schema_id(exec_tenant_id, column_group.get_column_group_id());
  UNUSED(tmp_tenant_id);
  UNUSED(tmp_table_id);
  UNUSED(tmp_cg_id);

  for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); i++) {
    const int64_t IS_DELETED = 0;
    uint64_t tmp_column_id = column_ids.at(i);
    UNUSED(IS_DELETED);
    if (OB_FAIL(ObColumnGroupHelper::gen_column_group_mapping_dml(table, column_group, tmp_column_id, is_history,
                                             false /*not delete*/, schema_version, dml))) {
      LOG_WARN("fail to finish row", K(ret), K(i), K(column_group));
    }
  }

  int64_t affected_rows = 0;
  if (FAILEDx(dml.splice_batch_insert_sql(tname, sql))) {
    LOG_WARN("fail to splice batch update sql", KR(ret), K(sql));
  } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(exec_tenant_id), K(sql));
  } else if (affected_rows != column_ids.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows), K(column_ids.count()));
  }

  return ret;
}

int ObColumnGroupHelper::exec_insert_column_group_mapping(
    ObISQLClient &sql_client,
    const ObTableSchema &table,
    const int64_t schema_version,
    bool is_history)
{
  int ret = OB_SUCCESS;

  ObTableSchema::const_column_group_iterator it_begin = table.column_group_begin();
  ObTableSchema::const_column_group_iterator it_end = table.column_group_end();
  const ObColumnGroupSchema *column_group = NULL;
  for (; OB_SUCC(ret) && (it_begin != it_end); ++it_begin) {
    column_group = *it_begin;
    if (OB_ISNULL(column_group)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("column_group schema should not be null", KR(ret));
    } else {
      const ObColumnGroupType cg_type = column_group->get_column_group_type();
      const int64_t column_id_cnt = column_group->get_column_id_count();
      if (column_id_cnt > 0) {
        if (OB_FAIL(ObColumnGroupHelper::exec_insert_single_column_group_mapping(sql_client, table, schema_version, *column_group, is_history))) {
          LOG_WARN("fail to insert single column group mapping", KR(ret), K(table), KPC(column_group));
        }
      } else if (cg_type != ObColumnGroupType::DEFAULT_COLUMN_GROUP) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(column_id_cnt), KPC(column_group));
      }
    }
  }

  if (OB_FAIL(ret) || nullptr == (column_group = table.get_hidden_rowkey_column_group())) {
  } else if (OB_FAIL(ObColumnGroupHelper::exec_insert_single_column_group_mapping(sql_client, table, schema_version, *column_group, is_history))) {
    LOG_WARN("fail to insert single column group mapping", KR(ret), K(table), KPC(column_group));
  }
  return ret;
}

int ObColumnGroupHelper::exec_insert_single_column_group_mapping(
    ObISQLClient &sql_client,
    const ObTableSchema &table,
    const int64_t schema_version,
    const ObColumnGroupSchema &column_group,
    const bool is_history)
{
  int ret = OB_SUCCESS;
  const int64_t column_id_cnt = column_group.get_column_id_count();
  ObSEArray<uint64_t, 16> column_ids;
  for (int64_t i = 0; OB_SUCC(ret) && (i < column_id_cnt); ++i) {
    uint64_t tmp_column_id = UINT64_MAX;
    if (OB_FAIL(column_group.get_column_id(i, tmp_column_id))) {
      LOG_WARN("fail to get column id", KR(ret), K(i), K(column_id_cnt));
    } else if (OB_FAIL(column_ids.push_back(tmp_column_id))) {
      LOG_WARN("fail to push back column id", KR(ret), K(i), K(column_ids));
    }
  }

  if (OB_FAIL(ret) || column_ids.empty()) {
  } else if (OB_FAIL(ObColumnGroupHelper::exec_insert_column_group_mapping(sql_client, table, schema_version, column_group, column_ids, is_history))) {
    LOG_WARN("fail to exec_insert_column_group_mapping", KR(ret), K(table), K(column_group), K(column_ids));
  }
  return ret;
}

int ObColumnGroupHelper::gen_column_group_dml(const ObTableSchema &table_schema,
                                              const ObColumnGroupSchema &column_group_schema,
                                              const bool is_history,
                                              const bool is_deleted,
                                              const int64_t schema_version,
                                              ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  if (!table_schema.is_valid() || !column_group_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_schema), K(column_group_schema));
  } else {
    const uint64_t tenant_id = table_schema.get_tenant_id();
    const uint64_t table_id = table_schema.get_table_id();
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    const uint64_t tmp_tenant_id = ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id);
    const uint64_t tmp_table_id = ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id);
    const uint64_t tmp_cg_id = ObSchemaUtils::get_extract_schema_id(exec_tenant_id, column_group_schema.get_column_group_id());

    if (OB_FAIL(dml.add_pk_column("tenant_id", tmp_tenant_id))
      || OB_FAIL(dml.add_pk_column("table_id", tmp_table_id))
      || OB_FAIL(dml.add_pk_column("column_group_id", tmp_cg_id))
      || (!(is_history && is_deleted) && OB_FAIL(dml.add_column("column_group_name", ObHexEscapeSqlStr(column_group_schema.get_column_group_name().ptr()))))
      || OB_FAIL(dml.add_column("column_group_type", column_group_schema.get_column_group_type()))
      || OB_FAIL(dml.add_column("block_size", column_group_schema.get_block_size()))
      || OB_FAIL(dml.add_column("compressor_type", column_group_schema.get_compressor_type()))
      || OB_FAIL(dml.add_column("row_store_type", column_group_schema.get_row_store_type()))
      || (is_history && OB_FAIL(dml.add_column("is_deleted", is_deleted)))
      || (is_history && OB_FAIL(dml.add_column("schema_version", schema_version)))) {
      LOG_WARN("fail to build column column dml", K(ret));
    } else if (OB_FAIL(dml.finish_row())) {
      LOG_WARN("fail to finish column group dml row", K(ret));
    }
  }
  return ret;
}

int ObColumnGroupHelper::gen_column_group_mapping_dml(const ObTableSchema &table_schema,
                                                      const ObColumnGroupSchema &column_group_schema,
                                                      const int64_t column_id,
                                                      const bool is_history,
                                                      const bool is_deleted,
                                                      const int64_t schema_version,
                                                      ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  if (!table_schema.is_valid() || !column_group_schema.is_valid() || schema_version == OB_INVALID_SCHEMA_VERSION) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_schema), K(column_group_schema), K(schema_version));
  } else {
    const uint64_t tenant_id = table_schema.get_tenant_id();
    const uint64_t table_id = table_schema.get_table_id();
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

    const uint64_t tmp_tenant_id = ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id);
    const uint64_t tmp_table_id = ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id);
    const uint64_t tmp_cg_id = ObSchemaUtils::get_extract_schema_id(exec_tenant_id, column_group_schema.get_column_group_id());
    if (OB_FAIL(dml.add_pk_column("tenant_id", tmp_tenant_id))
        || OB_FAIL(dml.add_pk_column("table_id", tmp_table_id))
        || OB_FAIL(dml.add_pk_column("column_group_id", tmp_cg_id))
        || OB_FAIL(dml.add_pk_column("column_id", column_id))
        || (is_history && OB_FAIL(dml.add_column("is_deleted", is_deleted ? 1: 0)))
        || (is_history && OB_FAIL(dml.add_column("schema_version", schema_version)))) {
      LOG_WARN("fail to add info to column group mapping dml", K(ret), K(table_schema), K(column_group_schema));
    } else if (OB_FAIL(dml.finish_row())) {
      LOG_WARN("dml splicer fail to finish row", K(ret));
    }
  }
  return ret;
}

int ObColumnGroupHelper::delete_from_column_group(ObISQLClient &sql_client,
                                                  const ObTableSchema &table_schema,
                                                  const int64_t new_schema_version,
                                                  const bool is_history)
{
  int ret = OB_SUCCESS;
  if (!table_schema.is_valid() || new_schema_version == OB_INVALID_SCHEMA_VERSION) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table schema", K(ret), K(table_schema), K(new_schema_version));
  } else {
    ObDMLSqlSplicer dml;
    int64_t affect_rows = 0;
    ObSqlString sql;
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(table_schema.get_tenant_id());
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    ObTableSchema::const_column_group_iterator iter_begin = table_schema.column_group_begin();
    ObTableSchema::const_column_group_iterator iter_end = table_schema.column_group_end();

    if (table_schema.get_column_group_count() == 0) {
      /* skip table has not column*/
    } else if (is_history) { /* remove from __all_column_group_history*/
      for (; OB_SUCC(ret) && iter_begin != iter_end; iter_begin++) {
        const ObColumnGroupSchema *column_group = *iter_begin;
        if (OB_ISNULL(column_group)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column group should not be null", K(ret), K(table_schema));
        } else if (OB_FAIL(ObColumnGroupHelper::gen_column_group_dml(table_schema, *column_group, is_history,
                                              true /* is_delete */, new_schema_version, dml))) {
          LOG_WARN("fail to write dml for __all_column_group_history", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        const ObColumnGroupSchema *hidden_cg = table_schema.get_hidden_rowkey_column_group();
        if (OB_ISNULL(hidden_cg)) {
        } else if (OB_FAIL(ObColumnGroupHelper::gen_column_group_dml(table_schema, *hidden_cg, is_history, true /*is_delete*/, new_schema_version, dml))){
          LOG_WARN("fail to write dml for __all_column_group_history", K(ret));
        }
      }

      if (FAILEDx(dml.splice_batch_insert_sql(OB_ALL_COLUMN_GROUP_HISTORY_TNAME, sql))) {
        LOG_WARN("fail to splice batch insert sql", K(ret), K(sql), K(table_schema));
      } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affect_rows))) {
        LOG_WARN("fail to insert deleted record to all column group history", K(ret));
      } else if (table_schema.get_column_group_count() != affect_rows){
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to drop all column group columns", K(ret), K(affect_rows), K(table_schema));
      }
    } else {
      if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                               exec_tenant_id, table_schema.get_tenant_id())))
          || OB_FAIL(dml.add_pk_column("table_id", table_schema.get_table_id()))) {
        LOG_WARN("fail to gen dml to delete from __all_column_group", K(ret));
      } else if (OB_FAIL(exec.exec_delete(OB_ALL_COLUMN_GROUP_TNAME, dml, affect_rows))) {
        LOG_WARN("fail to insert deleted record to all column group history", K(ret));
      } else if (table_schema.get_column_group_count() != affect_rows && affect_rows != 0) {
        ret = OB_ERR_UNDEFINED;
        LOG_WARN("fail to drop all column group columns", K(ret), K(affect_rows), K(table_schema));
      }
    }
  }
  return ret;
}

int ObColumnGroupHelper::delete_from_column_group_mapping(ObISQLClient &sql_client,
                                                          const ObTableSchema &table_schema,
                                                          const int64_t schema_version,
                                                          const bool is_history)
{
  int ret = OB_SUCCESS;
  if (!table_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table schema", K(ret), K(table_schema));
  } else {
    ObDMLSqlSplicer dml;
    int64_t cg_mapping_cnt = 0;
    int64_t affect_rows = 0;
    ObSqlString sql;
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(table_schema.get_tenant_id());
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    ObTableSchema::const_column_group_iterator iter_begin = table_schema.column_group_begin();
    ObTableSchema::const_column_group_iterator iter_end = table_schema.column_group_end();
    for (; OB_SUCC(ret) && iter_begin != iter_end; iter_begin++) {
      const ObColumnGroupSchema *column_group = *iter_begin;
      if (OB_ISNULL(column_group)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column group should not be null", K(ret), K(table_schema));
      } else {
        cg_mapping_cnt += column_group->get_column_id_count();
        for (int64_t i = 0; OB_SUCC(ret) && is_history && i < column_group->get_column_id_count(); i++) {
          if (OB_FAIL(ObColumnGroupHelper::gen_column_group_mapping_dml(table_schema, *column_group, column_group->get_column_ids()[i],
                                                  is_history, true /* is_deleted*/, schema_version, dml))) {
            LOG_WARN("fail to write column group mapping dml", K(ret));
          }
        }
      }
    }

    const ObColumnGroupSchema *hidden_cg = table_schema.get_hidden_rowkey_column_group();
    if (OB_SUCC(ret) && nullptr != hidden_cg) {
      cg_mapping_cnt += hidden_cg->get_column_id_count();
      for (int64_t i = 0; OB_SUCC(ret) && is_history && i < hidden_cg->get_column_id_count(); i++) {
        if (OB_FAIL(ObColumnGroupHelper::gen_column_group_mapping_dml(table_schema, *hidden_cg, hidden_cg->get_column_ids()[i],
                                                is_history, true /* is_deleted*/, schema_version, dml))) {
          LOG_WARN("fail to write column group mapping dml", K(ret));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (cg_mapping_cnt == 0) {
    } else if (is_history) {
      if (OB_FAIL(dml.splice_batch_insert_sql(OB_ALL_COLUMN_GROUP_MAPPING_HISTORY_TNAME, sql))) {
        LOG_WARN("fail to splice batch insert_sql", K(ret), K(sql), K(table_schema));
      } else if (OB_FAIL(sql_client.write(exec_tenant_id, sql.ptr(), affect_rows))) {
        LOG_WARN("fail to wirte rows into __all_column_group_mapping_history", K(ret), K(sql));
      } else if (cg_mapping_cnt != affect_rows){
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to drop in __all_column_group_mapping_history", K(ret), K(affect_rows), K(table_schema));
      }
    } else {
      if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(
                                                 exec_tenant_id, table_schema.get_tenant_id())))
          || OB_FAIL(dml.add_pk_column("table_id", table_schema.get_table_id()))) {
        LOG_WARN("fail to gen dml to delete from __all_column_group", K(ret));
      } else if (OB_FAIL(exec.exec_delete(OB_ALL_COLUMN_GROUP_MAPPING_TNAME, dml, affect_rows))) {
        LOG_WARN("fail to insert deleted record to all column group", K(ret));
      } else if (cg_mapping_cnt != affect_rows  && 0 != affect_rows) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to drop in __all_column_group_mapping", K(ret), K(affect_rows), K(table_schema));
      }
    }
  }
  return ret;
}

int ObColumnGroupHelper::delete_column_group(ObISQLClient &sql_client,
                                             const ObTableSchema &table_schema,
                                             const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  if (!table_schema.is_valid() || !table_schema.is_column_store_supported()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid table", K(ret), K(table_schema));
  } else if (OB_FAIL(ObColumnGroupHelper::delete_from_column_group(sql_client, table_schema, schema_version))) {
    LOG_WARN("fail to delete from table __all_column_group", K(ret));
  } else if (OB_FAIL(ObColumnGroupHelper::delete_from_column_group(sql_client, table_schema, schema_version, true /*history table*/))) {
    LOG_WARN("fail to delete from table __all_column_group_history", K(ret));
  } else if (OB_FAIL(ObColumnGroupHelper::delete_from_column_group_mapping(sql_client, table_schema, schema_version))) {
    LOG_WARN("fail to delete from table __all_column_group_mapping", K(ret));
  } else if (OB_FAIL(ObColumnGroupHelper::delete_from_column_group_mapping(sql_client, table_schema, schema_version, true /*history*/))) {
    LOG_WARN("fail to delete from talbe __all_column_group_mapping_history", K(ret));
  }
  return ret;
}

int ObColumnGroupHelper::update_single_column_group(ObISQLClient &sql_client,
                                                    const ObTableSchema &new_table_schema,
                                                    const ObColumnGroupSchema &ori_cg_schema,
                                                    const ObColumnGroupSchema &new_cg_schema)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  int64_t affect_rows = 0;
  uint64_t compat_version = 0;
  if (!sql_client.is_active() || !new_table_schema.is_valid() ||
      !ori_cg_schema.is_valid() || !new_cg_schema.is_valid() || !new_table_schema.is_column_store_supported()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(new_table_schema), K(ori_cg_schema), K(new_cg_schema));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(new_table_schema.get_tenant_id(), compat_version))) {
    LOG_WARN("fail to check min data_version", K(ret), K(new_table_schema));
  } else if (OB_FAIL(ObColumnGroupHelper::check_column_store_valid(new_table_schema, compat_version))) {
    LOG_WARN("fail to check column store valid", KR(ret), K(new_table_schema), K(compat_version));
  } else {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(new_table_schema.get_tenant_id());
    ObDMLExecHelper exec(sql_client, exec_tenant_id);
    if (OB_FAIL(ObColumnGroupHelper::gen_column_group_dml(new_table_schema, new_cg_schema, false, false,
                                     new_cg_schema.get_schema_version(), dml))) {
      LOG_WARN("fail to gen column group dml", K(ret));
    } else if (OB_FAIL(exec.exec_update(OB_ALL_COLUMN_GROUP_TNAME, dml, affect_rows))) {
      LOG_WARN("fail to update all column group", K(ret));
    } else if (affect_rows != (ori_cg_schema.get_column_group_name() != new_cg_schema.get_column_group_name())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to update single row in all column group ", K(ret), K(affect_rows), K(ori_cg_schema), K(new_cg_schema));
    }

    dml.reset();
    affect_rows = 0;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObColumnGroupHelper::gen_column_group_dml(new_table_schema, new_cg_schema, true,
                                            false, new_cg_schema.get_schema_version(), dml))) {
      LOG_WARN("fail to gen column group dml", K(ret));
    } else if (OB_FAIL(exec.exec_insert(OB_ALL_COLUMN_GROUP_HISTORY_TNAME, dml, affect_rows))) {
      LOG_WARN("fail to exec dml on history table", K(ret));
    } else if (1 != affect_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affect row not equal to 1", K(ret), K(affect_rows), K(ori_cg_schema), K(new_cg_schema));
    }
  }
  return ret;
}

int ObColumnGroupHelper::update_origin_column_group_with_new_schema(ObISQLClient &sql_client,
                                                                    const int64_t delete_schema_version,
                                                                    const int64_t insert_schema_version,
                                                                    const ObTableSchema &origin_table_schema,
                                                                    const ObTableSchema &new_table_schema)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = OB_INVALID_VERSION;
  uint64_t origin_tenant_id = origin_table_schema.get_tenant_id();
  uint64_t new_tenant_id = new_table_schema.get_tenant_id();
  if (OB_UNLIKELY(!sql_client.is_active()
                  || !origin_table_schema.is_valid()
                  || !new_table_schema.is_valid()
                  || origin_tenant_id != new_tenant_id
                  || !new_table_schema.is_column_store_supported())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(origin_table_schema), K(new_table_schema));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(origin_tenant_id, data_version))) {
    LOG_WARN("fail to get min data version", KR(ret), K(origin_table_schema));
  } else if (OB_FAIL(ObColumnGroupHelper::check_column_store_valid(origin_table_schema, data_version))) {
    LOG_WARN("fail to check column store valid for origin table schema", KR(ret), K(origin_table_schema));
  } else if (OB_FAIL(ObColumnGroupHelper::check_column_store_valid(new_table_schema, data_version))) {
    LOG_WARN("fail to check column store valid for new table schema", KR(ret), K(new_table_schema));
  } else if (origin_table_schema.is_column_store_supported() && OB_FAIL(ObColumnGroupHelper::delete_column_group(sql_client, origin_table_schema, delete_schema_version))) {
    LOG_WARN("fail to delete column group for origin table schema", KR(ret), K(origin_table_schema));
  } else if (OB_FAIL(ObColumnGroupHelper::add_column_groups(sql_client, new_table_schema, insert_schema_version, false/*only_history*/))) {
    LOG_WARN("fail to add column groups from new table schema", KR(ret), K(new_table_schema), K(insert_schema_version));
  }
  return ret;
}

int ObColumnGroupHelper::check_column_group_valid(
    const ObTableSchema &table,
    ObColumnGroupSchema *const *cg_arr,
    int64_t cg_cnt,
    uint64_t max_used_cg_id,
    const ObColumnGroupSchema *hidden_rowkey_cg)
{
  UNUSED(table);
  int ret = OB_SUCCESS;
  ObColumnGroupSchema *column_group = nullptr;
  bool arr_exist_all_cg = false;
  bool arr_exist_each_cg = false;
  bool arr_exist_rowkey_cg = false;

  for (int64_t i = 0; OB_SUCC(ret) && (i < cg_cnt); ++i) {
    if (OB_ISNULL(column_group = cg_arr[i])) {
      ret = OB_INVALID_ERROR;
      LOG_WARN_RET(OB_INVALID_ERROR, "column_group should not be null", K(i), "column_group_cnt", cg_cnt);
    } else if (!column_group->is_valid()) {
      ret = OB_INVALID_ERROR;
      LOG_WARN_RET(OB_INVALID_ERROR, "column_group is invalid", K(i), KPC(column_group));
    } else if (column_group->get_column_group_id() > max_used_cg_id) {
      ret = OB_INVALID_ERROR;
      LOG_WARN_RET(OB_INVALID_ERROR, "column_group id should not be greater than max_used_column_group_id",
                                     "cg_id", column_group->get_column_group_id(), "max_used_column_group_id", max_used_cg_id);
    } else if (ROWKEY_COLUMN_GROUP == column_group->get_column_group_type()) {
      arr_exist_rowkey_cg = true;
      if (OB_UNLIKELY(nullptr != hidden_rowkey_cg)) {
        ret = OB_INVALID_ERROR;
        LOG_WARN_RET(OB_INVALID_ERROR, "rowkey cg and hidden rowkey cg cannot exist at the same time",
                     KPC(column_group), KPC(hidden_rowkey_cg));
      }
    } else if (ALL_COLUMN_GROUP == column_group->get_column_group_type()) {
      arr_exist_all_cg = true;
    } else if (column_group->is_normal_column_group()) {
      arr_exist_each_cg = true;
    }
  }

  if (OB_FAIL(ret) || nullptr == hidden_rowkey_cg) {
  } else if (!hidden_rowkey_cg->is_valid()) {
    ret = OB_INVALID_ERROR;
    LOG_WARN_RET(OB_INVALID_ERROR, "hidden rowkey column group is invalid", KPC(hidden_rowkey_cg));
  } else if (hidden_rowkey_cg->get_column_group_id() > max_used_cg_id) {
    ret = OB_INVALID_ERROR;
    LOG_WARN_RET(OB_INVALID_ERROR, "hidden rowkey column group id should not be greater than max_used_column_group_id",
                                    "cg_id", hidden_rowkey_cg->get_column_group_id(), "max_used_column_group_id", max_used_cg_id);
  } else if (arr_exist_all_cg && arr_exist_each_cg && !arr_exist_rowkey_cg) {
  } else {
    ret = OB_INVALID_ERROR;
    LOG_WARN_RET(OB_INVALID_ERROR, "hidden rowkey column group should not exist", KPC(hidden_rowkey_cg),
                 K(arr_exist_all_cg), K(arr_exist_each_cg), K(arr_exist_rowkey_cg), K(cg_arr));
  }
  return ret;
}

int ObColumnGroupHelper::get_store_column_group_count(
    const ObTableSchema &table,
    ObColumnGroupSchema *const *cg_arr,
    int64_t cg_cnt,
    int64_t &column_group_cnt,
    const bool filter_empty_cg)
{
  UNUSED(table);
  int ret = OB_SUCCESS;
  int64_t tmp_cnt = 0;
  if (cg_cnt > 0) {
    if (OB_ISNULL(cg_arr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column_group_array should not be null", KR(ret), "column_group_cnt", cg_cnt);
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && (i < cg_cnt); ++i) {
        if (OB_ISNULL(cg_arr[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column_group should not be null", KR(ret), K(i), "column_group_cnt", cg_cnt);
        } else if (filter_empty_cg && (0 == cg_arr[i]->get_column_id_count())) {
        } else {
          ++tmp_cnt;
        }
      }
    }
  }
  column_group_cnt = tmp_cnt;
  return ret;
}

int ObColumnGroupHelper::get_store_column_groups(
    const ObTableSchema &table,
    ObColumnGroupSchema *const *cg_arr,
    int64_t cg_cnt,
    ObIArray<const ObColumnGroupSchema *> &column_groups,
    const bool filter_empty_cg)
{
  UNUSED(table);
  int ret = OB_SUCCESS;
  column_groups.reset();
  if (cg_cnt > 0) {
    if (OB_ISNULL(cg_arr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column_group_array should not be null", KR(ret), "column_group_cnt", cg_cnt);
    } else {
      if (OB_FAIL(column_groups.reserve(cg_cnt))) {
        LOG_WARN("fail to reserve", KR(ret), "column_group_cnt", cg_cnt);
      }
      for (int64_t i = 0; OB_SUCC(ret) && (i < cg_cnt); ++i) {
        if (OB_ISNULL(cg_arr[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column_group should not be null", KR(ret), K(i), "column_group_cnt", cg_cnt);
        } else if (filter_empty_cg && (0 == cg_arr[i]->get_column_id_count())) {
        } else if (OB_FAIL(column_groups.push_back(cg_arr[i]))) {
          LOG_WARN("fail to push back", KR(ret), K(i), "column_group_cnt", cg_cnt);
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    column_groups.reset();
  }
  return ret;
}

int ObColumnGroupHelper::has_all_column_group(const ObTableSchema &table,
                                              ObColumnGroupSchema *const *cg_arr,
                                              int64_t cg_cnt,
                                              bool &has_all_column_group)
{
  int ret = OB_SUCCESS;
  int64_t column_group_cnt = 0;
  has_all_column_group = false;
  ObSEArray<const ObColumnGroupSchema *, 8> column_group_metas;
  if (OB_FAIL(ObColumnGroupHelper::get_store_column_group_count(table, cg_arr, cg_cnt, column_group_cnt))) {
    LOG_WARN("Failed to get column group count", K(ret), KPC(&table));
  } else if (column_group_cnt <= 1) {
    has_all_column_group = true;
  } else if (OB_FAIL(ObColumnGroupHelper::get_store_column_groups(table, cg_arr, cg_cnt, column_group_metas))) {
    LOG_WARN("Failed to get column group metas", K(ret), KPC(&table));
  } else {
    const ObColumnGroupSchema *cg_schema = nullptr;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < column_group_metas.count(); ++idx) {
      if (OB_ISNULL(cg_schema = column_group_metas.at(idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null cg_schema", K(ret));
      } else if (cg_schema->get_column_group_type() == ALL_COLUMN_GROUP) {
        has_all_column_group = true;
        break;
      }
    }
  }
  return ret;
}

int ObColumnGroupHelper::has_column_group(
    const ObTableSchema &table,
    ObColumnGroupSchema *const *cg_arr,
    int64_t cg_cnt,
    const ObColumnGroupType &cg_type,
    bool &has_column_group)
{
  int ret = OB_SUCCESS;
  has_column_group = false;
  int64_t no_empty_cg_cnt = 0;
  ObSEArray<const ObColumnGroupSchema *, 8> no_empty_cg_metas;

  if (OB_UNLIKELY(cg_type >= MAX_COLUMN_GROUP)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(cg_type));
  } else if (DEFAULT_COLUMN_GROUP == cg_type) {
    has_column_group = true;
  } else if (OB_FAIL(ObColumnGroupHelper::get_store_column_group_count(table, cg_arr, cg_cnt, no_empty_cg_cnt))) {
    LOG_WARN("Failed to get column group count", K(ret), KPC(&table));
  } else if (no_empty_cg_cnt <= 1) {
    has_column_group = ALL_COLUMN_GROUP == cg_type;
  } else if (OB_FAIL(ObColumnGroupHelper::get_store_column_groups(table, cg_arr, cg_cnt, no_empty_cg_metas))) {
    LOG_WARN("Failed to get column group metas", K(ret), KPC(&table));
  } else {
    const ObColumnGroupSchema *cg_schema = nullptr;
    for (int64_t idx = 0; OB_SUCC(ret) && idx < no_empty_cg_metas.count(); ++idx) {
      if (OB_ISNULL(cg_schema = no_empty_cg_metas.at(idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected null cg_schema", K(ret));
      } else if (cg_schema->get_column_group_type() == cg_type) {
        has_column_group = true;
        break;
      }
    }
  }
  return ret;
}

int ObColumnGroupHelper::get_column_group_by_id(const ObTableSchema &table,
                                                const CgIdHashArray *cg_id_hash_arr,
                                                const uint64_t column_group_id,
                                                ObColumnGroupSchema *&column_group)
{
  UNUSED(table);
  int ret = OB_SUCCESS;
  column_group = NULL;
  if (OB_NOT_NULL(cg_id_hash_arr)) {
    if (OB_FAIL(cg_id_hash_arr->get_refactored(ObColumnGroupIdKey(column_group_id), column_group))) {
      column_group = NULL;
      LOG_WARN("fail to get column_group from hash array", KR(ret), K(column_group_id));
    } else if (OB_ISNULL(column_group)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column_group should not be null", KR(ret), K(column_group_id));
    }
  }
  return ret;
}

int ObColumnGroupHelper::get_column_group_by_name(const ObTableSchema &table,
                                                  const CgNameHashArray *cg_name_hash_arr,
                                                  const ObString &cg_name,
                                                  ObColumnGroupSchema *&column_group)
{
  UNUSED(table);
  int ret = OB_SUCCESS;
  column_group = nullptr;
  if (cg_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(cg_name));
  } else if (OB_NOT_NULL(cg_name_hash_arr)) {
    if (OB_FAIL(cg_name_hash_arr->get_refactored(ObColumnGroupSchemaHashWrapper(cg_name), column_group))) {
      column_group = nullptr;
      if (OB_HASH_NOT_EXIST == ret) {
      } else {
        LOG_WARN("fail to get column_group from hash array", K(ret), K(cg_name));
      }
    }
  }
  return ret;
}

int ObColumnGroupHelper::is_column_group_exist(const ObTableSchema &table,
                                               ObColumnGroupSchema *const *cg_arr,
                                               int64_t cg_cnt,
                                               const CgNameHashArray *cg_name_hash_arr,
                                               const ObString &cg_name,
                                               bool &exist)
{
  UNUSED(cg_arr);
  int ret = OB_SUCCESS;
  exist = false;
  if (cg_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(cg_name));
  } else if (cg_cnt > 0) {
    if (OB_ISNULL(cg_name_hash_arr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cg_name_hash_array should not be null", KR(ret));
    } else if (cg_name == OB_EACH_COLUMN_GROUP_NAME) {
      ObTableSchema::const_column_group_iterator iter_begin = table.column_group_begin();
      ObTableSchema::const_column_group_iterator iter_end = table.column_group_end();
      for (; OB_SUCC(ret) && iter_begin != iter_end; iter_begin++) {
        const ObColumnGroupSchema *cg = *iter_begin;
        if (ObColumnGroupType::SINGLE_COLUMN_GROUP == cg->get_column_group_type()) {
          exist = true;
          break;
        }
      }
    } else {
      ObColumnGroupSchema *column_group = NULL;
      if (OB_FAIL(cg_name_hash_arr->get_refactored(ObColumnGroupSchemaHashWrapper(cg_name), column_group))) {
        exist = false;
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get refactored from cg_name_hash_arr ", K(ret));
        }
      } else{
        exist = true;
      }
    }
  }
  return ret;
}

int ObColumnGroupHelper::get_column_group_index(const ObTableSchema &table,
                                                ObColumnGroupSchema *const *cg_arr,
                                                int64_t cg_cnt,
                                                ObColumnSchemaV2 *const *col_arr,
                                                int64_t col_cnt,
                                                uint64_t max_used_cg_id,
                                                const share::schema::ObColumnParam &param,
                                                const bool need_calculate_cg_idx,
                                                int32_t &cg_idx)
{
  int ret = OB_SUCCESS;
  const uint64_t column_id = param.get_column_id();
  cg_idx = -1;
  if (OB_UNLIKELY(1 >= cg_cnt && !need_calculate_cg_idx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("No column group exist", K(ret), K(need_calculate_cg_idx), "is_column_store_supported", table.is_column_store_supported(), "column_group_cnt", cg_cnt);
  } else if (param.is_virtual_gen_col()) {
    cg_idx = -1;
  } else if ((column_id < OB_END_RESERVED_COLUMN_ID_NUM || common::OB_MAJOR_REFRESH_MVIEW_OLD_NEW_COLUMN_ID == column_id) &&
      common::OB_HIDDEN_SESS_CREATE_TIME_COLUMN_ID != column_id &&
      common::OB_HIDDEN_SESSION_ID_COLUMN_ID != column_id &&
      common::OB_HIDDEN_PK_INCREMENT_COLUMN_ID != column_id) {
    if (common::OB_HIDDEN_TRANS_VERSION_COLUMN_ID == column_id ||
        common::OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID == column_id) {
      if (need_calculate_cg_idx) {
        cg_idx = storage::OB_CS_COLUMN_REPLICA_ROWKEY_CG_IDX;
      } else if (OB_FAIL(ObColumnGroupHelper::get_base_rowkey_column_group_index(table, cg_arr, cg_cnt, cg_idx))) {
        LOG_WARN("Fail to get base/rowkey column group index", K(ret), K(column_id));
      }
    } else {
      cg_idx = -1;
    }
  } else if (need_calculate_cg_idx) {
    if (OB_FAIL(ObColumnGroupHelper::calc_column_group_index(table, col_arr, col_cnt, column_id, cg_idx))) {
      LOG_WARN("Fail to calc_column_group_index", K(ret), K(column_id));
    }
  } else {
    bool found = false;
    int64_t cg_column_cnt = 0;
    int32_t iter_cg_idx = 0;
    uint64_t *cg_column_ids = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && !found && i < cg_cnt; i++) {
      if (OB_ISNULL(cg_arr[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column_group should not be null", K(ret), K(i), "column_group_cnt", cg_cnt);
      } else if (FALSE_IT(cg_column_cnt = cg_arr[i]->get_column_id_count())) {
      } else if (0 == cg_column_cnt) {
        if (cg_arr[i]->get_column_group_type() != DEFAULT_COLUMN_GROUP) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected column group type", K(ret), KPC(cg_arr[i]));
        }
      } else if (1 < cg_column_cnt || cg_arr[i]->get_column_group_type() != ObColumnGroupType::SINGLE_COLUMN_GROUP) {
        iter_cg_idx++;
      } else if (OB_ISNULL(cg_column_ids = cg_arr[i]->get_column_ids())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Unexpected error for null column ids", K(ret), KPC(cg_arr[i]));
      } else if (cg_column_ids[0] != column_id) {
        iter_cg_idx++;
      } else {
        cg_idx = iter_cg_idx;
        found = true;
      }
    }

    if (OB_SUCC(ret) && !found) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected, can not find cg idx", K(ret), K(column_id), "max_used_column_group_id", max_used_cg_id);
    }
  }
  LOG_TRACE("[CS-Replica] get column group index", K(ret), K(need_calculate_cg_idx), K(param), K(cg_idx), KPC(&table));
  return ret;
}

int ObColumnGroupHelper::calc_column_group_index(const ObTableSchema &table,
                                                 ObColumnSchemaV2 *const *col_arr,
                                                 int64_t col_cnt,
                                                 const uint64_t column_id,
                                                 int32_t &cg_idx)
{
  UNUSED(table);
  int ret = OB_SUCCESS;
  cg_idx = -1;
  int64_t virtual_column_cnt = 0;
  int64_t nullptr_column_cnt = 0;
  for (int64_t i = 0; i < col_cnt; i++) {
    ObColumnSchemaV2 *column = col_arr[i];
    if (OB_ISNULL(column)) {
      nullptr_column_cnt++;
    } else if (column->is_virtual_generated_column()) {
      virtual_column_cnt++;
    } else if (column->get_column_id() == column_id) {
      cg_idx = i + 1 - virtual_column_cnt - nullptr_column_cnt;
      break;
    }
  }

  if (OB_UNLIKELY(-1 == cg_idx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Unexpected cg idx", K(ret));
  }

  return ret;
}

int ObColumnGroupHelper::get_base_rowkey_column_group_index(const ObTableSchema &table,
                                                            ObColumnGroupSchema *const *cg_arr,
                                                            int64_t cg_cnt,
                                                            int32_t &cg_idx)
{
  int ret = OB_SUCCESS;
  cg_idx = -1;
  if (OB_UNLIKELY(1 >= cg_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("No column group exist", K(ret), "is_column_store_supported", table.is_column_store_supported(), "column_group_cnt", cg_cnt);
  } else {
    bool found = false;
    int32_t iter_cg_idx = 0;
    for (int32_t i = 0; OB_SUCC(ret) && !found && i < cg_cnt; i++) {
      if (OB_ISNULL(cg_arr[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column_group should not be null", K(ret), K(i), "column_group_cnt", cg_cnt);
      } else if (0 == cg_arr[i]->get_column_id_count()) {
        if (cg_arr[i]->get_column_group_type() != DEFAULT_COLUMN_GROUP) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected column group type", K(ret), KPC(cg_arr[i]));
        }
      } else if (ALL_COLUMN_GROUP == cg_arr[i]->get_column_group_type() ||
                 ROWKEY_COLUMN_GROUP == cg_arr[i]->get_column_group_type()) {
        cg_idx = iter_cg_idx;
        found = true;
      } else {
        iter_cg_idx++;
      }
    }
    if (OB_SUCC(ret) && !found) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected not found base/rowkey column group", K(ret));
    }
  }
  return ret;
}

int ObColumnGroupHelper::check_is_normal_cgs_at_the_end(const ObTableSchema &table,
                                                        ObColumnGroupSchema *const *cg_arr,
                                                        int64_t cg_cnt,
                                                        bool &is_normal_cgs_at_the_end)
{
  UNUSED(table);
  int ret = OB_SUCCESS;
  is_normal_cgs_at_the_end = false;
  if (0 < cg_cnt) {
    bool found_normal_cg = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < cg_cnt; i++) {
      if (OB_ISNULL(cg_arr[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column_group should not be null", K(ret), K(i), "column_group_cnt", cg_cnt);
      } else if (0 == cg_arr[i]->get_column_id_count()) {
        if (cg_arr[i]->get_column_group_type() != DEFAULT_COLUMN_GROUP) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected column group type", K(ret), KPC(cg_arr[i]));
        }
      } else if (SINGLE_COLUMN_GROUP == cg_arr[i]->get_column_group_type() ||
                 NORMAL_COLUMN_GROUP == cg_arr[i]->get_column_group_type()) {
        found_normal_cg = true;
      } else {
        if (!found_normal_cg) {
          is_normal_cgs_at_the_end = true;
        }
        break;
      }
    }
  }
  return ret;
}

int ObColumnGroupHelper::get_each_column_group(const ObTableSchema &table,
                                               ObColumnGroupSchema *const *cg_arr,
                                               int64_t cg_cnt,
                                               ObIArray<ObColumnGroupSchema*> &each_cgs)
{
  UNUSED(cg_arr);
  UNUSED(cg_cnt);
  int ret = OB_SUCCESS;
  each_cgs.reset();
  ObTableSchema::const_column_group_iterator iter_begin = table.column_group_begin();
  ObTableSchema::const_column_group_iterator iter_end = table.column_group_end();

  for (;OB_SUCC(ret) && iter_begin != iter_end; iter_begin++ ) {
    ObColumnGroupSchema *cg = *iter_begin;
    if (OB_ISNULL(cg)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column group should not be null", K(ret), KP(cg));
    } else if (cg->get_column_group_type() == ObColumnGroupType::SINGLE_COLUMN_GROUP) {
      if (OB_FAIL(each_cgs.push_back(cg))) {
        LOG_WARN("fail to add column group pointer to the array", K(ret));
      }
    }
  }
  return ret;
}

} // namespace schema
} // namespace share
} // namespace oceanbase
