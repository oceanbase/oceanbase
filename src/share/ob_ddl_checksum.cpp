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

#define USING_LOG_PREFIX SERVER

#include "ob_ddl_checksum.h"
#include "lib/string/ob_sql_string.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "observer/ob_server_struct.h"

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

int ObDDLChecksumOperator::fill_one_item(const ObDDLChecksumItem &item,
    ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = item.tenant_id_;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(item));
  } else if (OB_FAIL(dml.add_pk_column("execution_id", item.execution_id_))
      || OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id)))
      || OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(exec_tenant_id, item.table_id_)))
      // currently tablet id is not necessary, so instead we save task id in this column to distinguish different DDL
      // task_id is the primary key in __all_ddl_task_status, so it can uniquely identify a DDL.
      || OB_FAIL(dml.add_pk_column("ddl_task_id", item.ddl_task_id_))
      || OB_FAIL(dml.add_pk_column("column_id", item.column_id_))
      || OB_FAIL(dml.add_pk_column("task_id", item.task_id_))
      || OB_FAIL(dml.add_column("checksum", item.checksum_))) {
    LOG_WARN("fail to add column", K(ret));
  }
  return ret;
}

int ObDDLChecksumOperator::update_checksum(
    const uint64_t tenant_id,
    const int64_t table_id,
    const int64_t ddl_task_id,
    const common::ObIArray<int64_t> &main_table_checksum,
    const common::ObIArray<int64_t> &col_ids,
    const int64_t schema_version,
    const int64_t task_idx,
    common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == table_id || OB_INVALID_ID == ddl_task_id
      || main_table_checksum.count() <= 0 || col_ids.count() <= 0 || schema_version <= 0 || task_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(table_id), K(task_idx),
        K(main_table_checksum.count()), K(col_ids.count()), K(schema_version), K(task_idx));
  } else {
    const int64_t column_cnt = col_ids.count();
    ObArray<ObDDLChecksumItem> checksum_items;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; ++i) {
      ObDDLChecksumItem item;
      item.execution_id_ = schema_version;
      item.tenant_id_ = tenant_id;
      item.table_id_ = table_id;
      item.ddl_task_id_ = ddl_task_id;
      item.column_id_ = col_ids.at(i);
      item.task_id_ = task_idx;
      item.checksum_ = main_table_checksum.at(i);
      if (item.column_id_ == OB_HIDDEN_TRANS_VERSION_COLUMN_ID ||
          item.column_id_ == OB_HIDDEN_SQL_SEQUENCE_COLUMN_ID) {
        continue;
      } else if (OB_FAIL(checksum_items.push_back(item))) {
        LOG_WARN("fail to push back checksum item", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObDDLChecksumOperator::update_checksum(checksum_items, sql_proxy))) {
        LOG_WARN("fail to update checksum items", K(ret));
      }
    }
  }
  return ret;
}

int ObDDLChecksumOperator::update_checksum(const ObIArray<ObDDLChecksumItem> &checksum_items,
    ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString columns;
  ObSqlString values;
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  uint64_t tenant_id = OB_INVALID_ID;
  if (0 == checksum_items.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(checksum_items.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < checksum_items.count(); ++i) {
      const ObDDLChecksumItem &item = checksum_items.at(i);
      dml.reuse();
      if (OB_FAIL(fill_one_item(item, dml))) {
        LOG_WARN("fail to fill one item", K(ret), K(item));
      } else {
        if (0 == i) {
          if (OB_FAIL(dml.splice_column_names(columns))) {
            LOG_WARN("fail to splice column names", K(ret));
          } else if (OB_FAIL(sql.assign_fmt("INSERT /*+ use_plan_cache(none) */ INTO %s (%s) VALUES",
              OB_ALL_DDL_CHECKSUM_TNAME, columns.ptr()))) {
            LOG_WARN("fail to assign sql string", K(ret));
          } else {
            tenant_id = item.tenant_id_;
          }
        }

        if (OB_SUCC(ret)) {
          values.reset();
          if (OB_FAIL(dml.splice_values(values))) {
            LOG_WARN("fail to splicer values", K(ret));
          } else if (OB_FAIL(sql.append_fmt("%s(%s)",
              0 == i ? " " : " , ", values.ptr()))) {
            LOG_WARN("fail to assign sql string", K(ret));
          }
        }
      }
    }

    if (OB_SUCC(ret) && checksum_items.count() > 0) {
      if (OB_FAIL(sql.append(" ON DUPLICATE KEY UPDATE "))) {
        LOG_WARN("fail to append sql string", K(ret));
      } else if (OB_FAIL(sql.append(" checksum = values(checksum)"))) {
        LOG_WARN("fail to append sql string", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(sql_proxy.write(tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("fail to execute sql", K(ret));
      } else if (OB_UNLIKELY(affected_rows > 2 * checksum_items.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, invalid affected rows", K(ret), K(affected_rows),
            K(checksum_items.count()));
      }
    }
  }
  return ret;
}

int ObDDLChecksumOperator::get_column_checksum(const ObSqlString &sql, const uint64_t tenant_id,
    ObHashMap<int64_t, int64_t> &column_checksum_map, common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    sqlclient::ObMySQLResult *result = NULL;
    if (!sql.is_valid() || !column_checksum_map.created()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(sql), K(column_checksum_map.created()));
    } else if (OB_FAIL(sql_proxy.read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("fail to execute sql", K(ret));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, query result must not be NULL", K(ret));
    } else {
      int64_t column_id = 0;
      int64_t column_checksum = 0;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            if (0 != column_id && OB_FAIL(column_checksum_map.set_refactored(column_id, column_checksum))) {
              LOG_WARN("fail to set column checksum to map", K(ret));
            }
            break;
          } else {
            LOG_WARN("fail to get next row", K(ret));
          }
        } else {
          int64_t curr_column_id = 0;
          int64_t curr_column_checksum = 0;
          EXTRACT_INT_FIELD_MYSQL(*result, "column_id", curr_column_id, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "checksum", curr_column_checksum, int64_t);
          if (0 == column_id) {
            column_id = curr_column_id;
          }
          if (curr_column_id != column_id) {
            if (OB_FAIL(column_checksum_map.set_refactored(column_id, column_checksum))) {
              LOG_WARN("fail to set column checksum to map", K(ret));
            } else {
              column_id = curr_column_id;
              column_checksum = curr_column_checksum;
            }
          } else {
            column_checksum += curr_column_checksum;
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLChecksumOperator::get_tablet_checksum_status(
    const ObSqlString &sql,
    const uint64_t tenant_id,
    ObIArray<uint64_t> &batch_tablet_array,
    common::ObMySQLProxy &sql_proxy,
    common::hash::ObHashMap<uint64_t, bool> &tablet_checksum_status_map)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    sqlclient::ObMySQLResult *result = NULL;
    if (!sql.is_valid() || !tablet_checksum_status_map.created()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(sql), K(tablet_checksum_status_map.created()));
    } else if (OB_FAIL(sql_proxy.read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("fail to execute sql", K(ret));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, query result must not be NULL", K(ret));
    } else {
      bool force_update = true;
      // 1. get tablet column checksums from sql result
      while (OB_SUCC(ret)) {
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("fail to get next row", K(ret));
          }
        } else {
          // int64_t column_id = 0;
          int64_t task_id = 0;
          // EXTRACT_INT_FIELD_MYSQL(*result, "column_id", column_id, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "task_id", task_id, int64_t);
          if (OB_SUCC(ret)
              && OB_FAIL(tablet_checksum_status_map.set_refactored(task_id, true, force_update))) {
            LOG_WARN("fail to set tablet column map", K(ret), K(task_id));
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLChecksumOperator::get_tablet_checksum_record(
  const uint64_t tenant_id,
  const uint64_t execution_id,
  const uint64_t table_id,
  const int64_t ddl_task_id,
  ObIArray<ObTabletID> &tablet_ids,
  ObMySQLProxy &sql_proxy,
  common::hash::ObHashMap<uint64_t, bool> &tablet_checksum_status_map)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);

  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == execution_id ||
                  OB_INVALID_ID == table_id || OB_INVALID_ID == ddl_task_id ||
                  tablet_ids.count() <= 0 || !tablet_checksum_status_map.created())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
      K(ret), K(tenant_id), K(execution_id), K(table_id), K(ddl_task_id),
      K(tablet_checksum_status_map.created()));
  } else {
    int64_t batch_size = 100;
    ObArray<uint64_t> batch_tablet_array;

    // check every tablet column checksum, task_id is equal to tablet_id
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
      uint64_t last_tablet_id_id = tablet_ids.at(i).id();
      if (OB_FAIL(batch_tablet_array.push_back(last_tablet_id_id))) {
        LOG_WARN("fail to push back tablet_id_id", K(ret), K(tenant_id), K(execution_id), K(ddl_task_id));
      } else {
        if ((i != 0 && i % batch_size == 0) /* reach batch size */ || i == tablet_ids.count() - 1 /* reach end */) {
          if (OB_FAIL(sql.assign_fmt(
              "SELECT task_id FROM %s "
              "WHERE execution_id = %ld AND tenant_id = %ld AND table_id = %ld AND ddl_task_id = %ld AND task_id >= %ld and task_id <= %ld "
              "GROUP BY task_id",
              OB_ALL_DDL_CHECKSUM_TNAME,
              execution_id,
              ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
              ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id),
              ddl_task_id,
              batch_tablet_array.at(0), // first tablet_id in one batch
              last_tablet_id_id))) {    // last  tablet id in one batch
            LOG_WARN("fail to assign fmt", K(ret), K(tenant_id), K(execution_id), K(ddl_task_id));
          } else if (OB_FAIL(get_tablet_checksum_status(
              sql, tenant_id, batch_tablet_array, sql_proxy, tablet_checksum_status_map))) {
            LOG_WARN("fail to get column checksum", K(ret), K(sql));
          } else {
            batch_tablet_array.reset();
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLChecksumOperator::get_table_column_checksum(
    const uint64_t tenant_id,
    const int64_t execution_id,
    const uint64_t table_id,
    const int64_t ddl_task_id,
    const bool is_unique_index_checking,
    common::hash::ObHashMap<int64_t, int64_t> &column_checksum_map,
    ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || execution_id < 0 || OB_INVALID_ID == table_id
        || OB_INVALID_ID == ddl_task_id || !column_checksum_map.created())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(execution_id), K(table_id), K(ddl_task_id),
        K(column_checksum_map.created()));
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT column_id, checksum FROM %s "
      "WHERE execution_id = %ld AND tenant_id = %ld AND table_id = %ld AND ddl_task_id = %ld AND task_id %s "
      "ORDER BY column_id", OB_ALL_DDL_CHECKSUM_TNAME,
      execution_id, ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
      ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id), ddl_task_id, is_unique_index_checking ? "< 0" : ">= 0"))) {
    LOG_WARN("fail to assign fmt", K(ret));
  } else if (OB_FAIL(get_column_checksum(sql, tenant_id, column_checksum_map, sql_proxy))) {
    LOG_WARN("fail to get column checksum", K(ret), K(sql));
  }
  return ret;
}

int ObDDLChecksumOperator::check_column_checksum(
    const uint64_t tenant_id,
    const int64_t execution_id,
    const uint64_t data_table_id,
    const uint64_t index_table_id,
    const int64_t ddl_task_id,
    const bool is_unique_index_checking,
    bool &is_equal,
    common::ObMySQLProxy &sql_proxy)
{
  int ret = OB_SUCCESS;
  hash::ObHashMap<int64_t, int64_t> data_table_column_checksums;
  hash::ObHashMap<int64_t, int64_t> index_table_column_checksums;
  is_equal = true;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || execution_id < 0 || OB_INVALID_ID == data_table_id
        || OB_INVALID_ID == index_table_id || OB_INVALID_ID == ddl_task_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(execution_id), K(data_table_id), K(index_table_id), K(ddl_task_id));
  } else if (OB_FAIL(data_table_column_checksums.create(OB_MAX_COLUMN_NUMBER / 2, ObModIds::OB_SSTABLE_CREATE_INDEX))) {
    LOG_WARN("fail to create column checksum map", K(ret));
  } else if (OB_FAIL(index_table_column_checksums.create(OB_MAX_COLUMN_NUMBER / 2, ObModIds::OB_SSTABLE_CREATE_INDEX))) {
    LOG_WARN("fail to create column checksum map", K(ret));
  } else if (OB_FAIL(get_table_column_checksum(tenant_id, execution_id, data_table_id,
      ddl_task_id, is_unique_index_checking, data_table_column_checksums, sql_proxy))) {
    LOG_WARN("fail to get table column checksum", K(ret), K(execution_id), K(data_table_id), K(ddl_task_id));
  } else if (OB_FAIL(get_table_column_checksum(tenant_id, execution_id, index_table_id,
      ddl_task_id, is_unique_index_checking, index_table_column_checksums, sql_proxy))) {
    LOG_WARN("fail to get table column checksum", K(ret), K(execution_id), K(index_table_id), K(ddl_task_id));
  } else {
    for (hash::ObHashMap<int64_t, int64_t>::const_iterator iter = index_table_column_checksums.begin();
        OB_SUCC(ret) && iter != index_table_column_checksums.end(); ++iter) {
      int64_t data_table_column_checksum = 0;
      if (OB_FAIL(data_table_column_checksums.get_refactored(iter->first, data_table_column_checksum))) {
        LOG_WARN("fail to get data table column checksum", K(ret), "column_id", iter->first);
      } else if (data_table_column_checksum != iter->second) {
        ret = OB_CHECKSUM_ERROR;
        // In most cases, this checksum error is caused by unique constraint violation in user data, so we do not print error here.
        LOG_WARN("column checksum is not equal", K(ret), K(data_table_id), K(index_table_id),
            "column_id", iter->first, K(data_table_column_checksum),
            "index_table_column_checksum", iter->second);
      }
    }
  }
  if (data_table_column_checksums.created()) {
    data_table_column_checksums.destroy();
  }
  if (index_table_column_checksums.created()) {
    index_table_column_checksums.destroy();
  }
  return ret;
}

/**
 * The request of complement data task is to clean up the scan checksum record of the specified tablet.
 * The request of insert into select is to clean up all tablets' checksum record.
 * And the input argument (tablet_task_id) is to classify the above two scenarios, default value (OB_INVALID_INDEX)
 * means to clear all checksum records.
*/
int ObDDLChecksumOperator::delete_checksum(
    const uint64_t tenant_id,
    const int64_t execution_id,
    const uint64_t source_table_id,
    const uint64_t dest_table_id,
    const int64_t ddl_task_id,
    common::ObMySQLProxy &sql_proxy,
    const int64_t tablet_task_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString remove_tablet_chksum_sql;
  int64_t affected_rows = 0;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || execution_id < 0 || OB_INVALID_ID == ddl_task_id
                  || OB_INVALID_ID == source_table_id || OB_INVALID_ID == dest_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(execution_id), K(source_table_id), K(dest_table_id));
  } else if (OB_INVALID_INDEX != tablet_task_id
    && OB_FAIL(remove_tablet_chksum_sql.assign_fmt("AND (task_id >> %ld) = %ld ", ObDDLChecksumItem::PX_SQC_ID_OFFSET, tablet_task_id))) {
    LOG_WARN("assign fmt failed", K(ret), K(tablet_task_id), K(remove_tablet_chksum_sql));
  } else if (OB_FAIL(sql.assign_fmt(
      "DELETE /*+ use_plan_cache(none) */ FROM %s "
      "WHERE tenant_id = 0 AND execution_id = %ld AND ddl_task_id = %ld AND table_id IN (%ld, %ld) %.*s",
      OB_ALL_DDL_CHECKSUM_TNAME, execution_id, ddl_task_id, source_table_id, dest_table_id,
      static_cast<int>(remove_tablet_chksum_sql.length()), remove_tablet_chksum_sql.ptr()))) {
    LOG_WARN("fail to assign fmt", K(ret), K(remove_tablet_chksum_sql));
  } else if (OB_FAIL(sql_proxy.write(tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", KR(ret), K(sql));
  } else if (OB_UNLIKELY(affected_rows < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected_rows is unexpected", KR(ret), K(affected_rows));
  }
  return ret;
}
