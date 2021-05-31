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

#include "ob_index_checksum.h"
#include "lib/string/ob_sql_string.h"
#include "share/inner_table/ob_inner_table_schema.h"

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::share;

int ObIndexChecksumOperator::fill_one_item(const ObIndexChecksumItem& item, ObDMLSqlSplicer& dml)
{
  int ret = OB_SUCCESS;
  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(item));
  } else if (OB_FAIL(dml.add_pk_column("execution_id", item.execution_id_)) ||
             OB_FAIL(dml.add_pk_column("tenant_id", extract_tenant_id(item.table_id_))) ||
             OB_FAIL(dml.add_pk_column("table_id", item.table_id_)) ||
             OB_FAIL(dml.add_pk_column("partition_id", item.partition_id_)) ||
             OB_FAIL(dml.add_pk_column("column_id", item.column_id_)) ||
             OB_FAIL(dml.add_pk_column("task_id", item.task_id_)) ||
             OB_FAIL(dml.add_column("checksum", item.checksum_)) ||
             OB_FAIL(dml.add_column("checksum_method", item.checksum_method_))) {
    LOG_WARN("fail to add column", K(ret));
  }
  return ret;
}

int ObIndexChecksumOperator::update_checksum(
    const ObIArray<ObIndexChecksumItem>& checksum_items, ObMySQLProxy& sql_proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString columns;
  ObSqlString values;
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  if (0 == checksum_items.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(checksum_items.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < checksum_items.count(); ++i) {
      const ObIndexChecksumItem& item = checksum_items.at(i);
      dml.reuse();
      if (OB_FAIL(fill_one_item(item, dml))) {
        LOG_WARN("fail to fill one item", K(ret), K(item));
      } else {
        if (0 == i) {
          if (OB_FAIL(dml.splice_column_names(columns))) {
            LOG_WARN("fail to splice column names", K(ret));
          } else if (OB_FAIL(sql.assign_fmt("INSERT /*+ use_plan_cache(none) */ INTO %s (%s) VALUES",
                         OB_ALL_INDEX_CHECKSUM_TNAME,
                         columns.ptr()))) {
            LOG_WARN("fail to assign sql string", K(ret));
          }
        }

        if (OB_SUCC(ret)) {
          values.reset();
          if (OB_FAIL(dml.splice_values(values))) {
            LOG_WARN("fail to splicer values", K(ret));
          } else if (OB_FAIL(sql.append_fmt("%s(%s)", 0 == i ? " " : " , ", values.ptr()))) {
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
      if (OB_FAIL(sql_proxy.write(sql.ptr(), affected_rows))) {
        LOG_WARN("fail to execute sql", K(ret));
      } else if (OB_UNLIKELY(affected_rows > 2 * checksum_items.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, invalid affected rows", K(ret), K(affected_rows), K(checksum_items.count()));
      }
    }
  }
  return ret;
}

int ObIndexChecksumOperator::get_column_checksum(
    const ObSqlString& sql, ObHashMap<int64_t, int64_t>& column_checksum_map, common::ObMySQLProxy& sql_proxy)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    if (!sql.is_valid() || !column_checksum_map.created()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(sql), K(column_checksum_map.created()));
    } else if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
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

int ObIndexChecksumOperator::get_partition_column_checksum(const uint64_t execution_id, const uint64_t table_id,
    const int64_t partition_id, ObHashMap<int64_t, int64_t>& column_checksum_map, ObMySQLProxy& sql_proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_UNLIKELY(OB_INVALID_ID == execution_id || OB_INVALID_ID == table_id || partition_id < 0 ||
                  !column_checksum_map.created())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
        "invalid argument", K(ret), K(execution_id), K(table_id), K(partition_id), K(column_checksum_map.created()));
  } else if (OB_FAIL(sql.assign_fmt("SELECT column_id, checksum FROM %s "
                                    "WHERE execution_id = %ld AND tenant_id = %ld AND table_id = %ld "
                                    "AND partition_id=%ld ORDER BY column_id",
                 OB_ALL_INDEX_CHECKSUM_TNAME,
                 execution_id,
                 extract_tenant_id(table_id),
                 table_id,
                 partition_id))) {
    LOG_WARN("fail to assign fmt", K(ret));
  } else if (OB_FAIL(get_column_checksum(sql, column_checksum_map, sql_proxy))) {
    LOG_WARN("fail to get column checksum", K(ret), K(sql));
  }
  return ret;
}

int ObIndexChecksumOperator::get_table_column_checksum(const uint64_t execution_id, const uint64_t table_id,
    common::hash::ObHashMap<int64_t, int64_t>& column_checksum_map, ObMySQLProxy& sql_proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_UNLIKELY(OB_INVALID_ID == execution_id || OB_INVALID_ID == table_id || !column_checksum_map.created())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(execution_id), K(table_id), K(column_checksum_map.created()));
  } else if (OB_FAIL(sql.assign_fmt("SELECT column_id, checksum FROM %s "
                                    "WHERE execution_id = %ld AND tenant_id = %ld AND table_id = %ld "
                                    "ORDER BY column_id",
                 OB_ALL_INDEX_CHECKSUM_TNAME,
                 execution_id,
                 extract_tenant_id(table_id),
                 table_id))) {
    LOG_WARN("fail to assign fmt", K(ret));
  } else if (OB_FAIL(get_column_checksum(sql, column_checksum_map, sql_proxy))) {
    LOG_WARN("fail to get column checksum", K(ret), K(sql));
  }
  return ret;
}

int ObIndexChecksumOperator::check_column_checksum(const uint64_t execution_id, const uint64_t data_table_id,
    const uint64_t index_table_id, bool& is_equal, common::ObMySQLProxy& sql_proxy)
{
  int ret = OB_SUCCESS;
  hash::ObHashMap<int64_t, int64_t> data_table_column_checksums;
  hash::ObHashMap<int64_t, int64_t> index_table_column_checksums;
  is_equal = true;
  if (OB_UNLIKELY(OB_INVALID_ID == execution_id || OB_INVALID_ID == data_table_id || OB_INVALID_ID == index_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(execution_id), K(data_table_id), K(index_table_id));
  } else if (OB_FAIL(data_table_column_checksums.create(OB_MAX_COLUMN_NUMBER / 2, ObModIds::OB_SSTABLE_CREATE_INDEX))) {
    LOG_WARN("fail to create column checksum map", K(ret));
  } else if (OB_FAIL(
                 index_table_column_checksums.create(OB_MAX_COLUMN_NUMBER / 2, ObModIds::OB_SSTABLE_CREATE_INDEX))) {
    LOG_WARN("fail to create column checksum map", K(ret));
  } else if (OB_FAIL(get_table_column_checksum(execution_id, data_table_id, data_table_column_checksums, sql_proxy))) {
    LOG_WARN("fail to get table column checksum", K(ret), K(execution_id), K(data_table_id));
  } else if (OB_FAIL(
                 get_table_column_checksum(execution_id, index_table_id, index_table_column_checksums, sql_proxy))) {
    LOG_WARN("fail to get table column checksum", K(ret), K(execution_id), K(index_table_id));
  } else {
    for (hash::ObHashMap<int64_t, int64_t>::const_iterator iter = index_table_column_checksums.begin();
         OB_SUCC(ret) && iter != index_table_column_checksums.end();
         ++iter) {
      int64_t data_table_column_checksum = 0;
      if (OB_FAIL(data_table_column_checksums.get_refactored(iter->first, data_table_column_checksum))) {
        LOG_WARN("fail to get data table column checksum", K(ret), "column_id", iter->first);
      } else if (data_table_column_checksum != iter->second) {
        ret = OB_CHECKSUM_ERROR;
        // In most cases, this checksum error is caused by unique constraint violation in user data, so we do not print
        // error here.
        LOG_WARN("column checksum is not equal",
            K(ret),
            K(data_table_id),
            K(index_table_id),
            "column_id",
            iter->first,
            K(data_table_column_checksum),
            "index_table_column_checksum",
            iter->second);
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

int ObIndexChecksumOperator::get_checksum_method(
    const uint64_t execution_id, const uint64_t table_id, int64_t& checksum_method, common::ObMySQLProxy& sql_proxy)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    ObSqlString sql;
    checksum_method = 0;
    if (OB_INVALID_ID == execution_id || OB_INVALID_ID == table_id) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), K(execution_id), K(table_id));
    } else if (OB_FAIL(sql.assign_fmt("SELECT checksum_method FROM %s "
                                      "WHERE execution_id = %ld AND tenant_id = %ld AND table_id = %ld ",
                   OB_ALL_INDEX_CHECKSUM_TNAME,
                   execution_id,
                   extract_tenant_id(table_id),
                   table_id))) {
      LOG_WARN("fail to assign fmt", K(ret));
    } else if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
      LOG_WARN("fail to execute sql", K(ret));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, query result must not be NULL", K(ret));
    } else {
      while (OB_SUCC(ret)) {
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("fail to get next row", K(ret));
          }
        } else {
          int64_t tmp_checksum_method = 0;
          EXTRACT_INT_FIELD_MYSQL(*result, "checksum_method", tmp_checksum_method, int64_t);
          if (0 == checksum_method) {
            checksum_method = tmp_checksum_method;
          } else if (checksum_method != tmp_checksum_method) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("error unexpected, checksum method not equal", K(checksum_method), K(tmp_checksum_method));
          }
        }
      }
    }
  }
  return ret;
}
