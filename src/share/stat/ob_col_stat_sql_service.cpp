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

#define USING_LOG_PREFIX COMMON
#include "share/stat/ob_col_stat_sql_service.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_connection.h"
#include "lib/mysqlclient/ob_mysql_statement.h"
#include "lib/mysqlclient/ob_mysql_connection_pool.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/compress/ob_compressor_pool.h"
#include "common/ob_partition_key.h"
#include "common/ob_timeout_ctx.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/config/ob_server_config.h"
#include "share/partition_table/ob_ipartition_table.h"
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/stat/ob_table_stat.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_sql_client_decorator.h"

#define MY_ALL_COL_STAT_COLUMN_NAME \
  "tenant_id, "                     \
  "table_id, "                      \
  "partition_id, "                  \
  "column_id, "                     \
  "version, "                       \
  "last_rebuild_version,"           \
  "num_distinct, "                  \
  "num_null,"                       \
  "min_value, "                     \
  "max_value, "                     \
  "llc_bitmap,"                     \
  "llc_bitmap_size "
#define USER_TAB_COL_STATISTICS "__all_column_statistic"
#define MY_FETCH_COL_STAT_SQL "SELECT " MY_ALL_COL_STAT_COLUMN_NAME " FROM " USER_TAB_COL_STATISTICS
#define REPLACE_COL_STAT_SQL "REPLACE INTO " USER_TAB_COL_STATISTICS "(" MY_ALL_COL_STAT_COLUMN_NAME ") "

#define MY_ALL_TABLE_STAT_COLUMN_NAME \
  "tenant_id, "                       \
  "table_id",                         \
      "partition_id, "                \
      "partition_cnt, "               \
      "row_count, "                   \
      "data_size,"                    \
      "data_version "

#define DEFINE_SQL_CLIENT_RETRY_WEAK_FOR_STAT(sql_client)  \
  const bool did_use_weak = false;                         \
  const bool did_use_retry = false;                        \
  const int64_t snapshot_timestamp = OB_INVALID_TIMESTAMP; \
  const bool check_sys_variable = false;                   \
  ObSQLClientRetryWeak sql_client_retry_weak(              \
      sql_client, did_use_weak, did_use_retry, snapshot_timestamp, check_sys_variable);

namespace oceanbase {
using namespace share;
using namespace share::schema;
namespace share {
class ObIPartitiontable;
}
namespace common {

const char* ObTableColStatSqlService::bitmap_compress_lib_name = "zlib_1.0";

ObTableColStatSqlService::ObTableColStatSqlService() : inited_(false), mysql_proxy_(NULL), config_(NULL)
{}

ObTableColStatSqlService::~ObTableColStatSqlService()
{}

int ObTableColStatSqlService::init(ObMySQLProxy* proxy, ObServerConfig* config)
{
  int ret = OB_SUCCESS;
  lib::ObMutexGuard guard(mutex_);
  if (NULL == proxy) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "client proxy is null", K(ret));
  } else if (inited_) {
    ret = OB_INIT_TWICE;
    COMMON_LOG(WARN, "sql service have already been initialized.", K(ret));
  } else {
    mysql_proxy_ = proxy;
    config_ = config;
    inited_ = true;
  }
  return ret;
}

// init virtual table converter
int ObTableColStatSqlService::init_vt_converter(
    uint64_t tenant_id, uint64_t ref_table_id, uint64_t column_id, ObStatConverterInfo& stat_converter_info)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 1> column_ids;
  if (OB_FAIL(column_ids.push_back(column_id))) {
    LOG_WARN("failed to push back column ids", K(ret));
  } else if (OB_FAIL(init_vt_converter(tenant_id, ref_table_id, column_ids, stat_converter_info))) {
    LOG_WARN("failed to init vt convert", K(ret));
  }
  return ret;
}

int ObTableColStatSqlService::init_vt_converter(
    uint64_t tenant_id, uint64_t ref_table_id, ObStatConverterInfo& stat_converter_info)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> column_ids;
  if (OB_FAIL(init_vt_converter(tenant_id, ref_table_id, column_ids, stat_converter_info))) {
    LOG_WARN("failed to init vt convert", K(ret));
  }
  return ret;
}

int ObTableColStatSqlService::init_vt_converter(uint64_t tenant_id, uint64_t ref_table_id,
    const ObIArray<uint64_t>& column_ids, ObStatConverterInfo& stat_converter_info)
{
  int ret = OB_SUCCESS;
  const uint64_t real_table_id = ObSchemaUtils::get_real_table_mappings_tid(ref_table_id);
  if (OB_INVALID_ID == real_table_id) {
  } else {
    share::schema::ObSchemaGetterGuard schema_guard;
    const ObTableSchema* real_table_schema = NULL;
    const ObTableSchema* vt_table_schema = NULL;
    stat_converter_info.is_vt_mapping_ = true;
    stat_converter_info.real_table_id_ = real_table_id;
    bool has_column = (0 < column_ids.count());
    if (!has_column) {
    } else if (OB_ISNULL(GCTX.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_service is null", K(ret));
    } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, stat_converter_info.schema_guard_))) {
      LOG_WARN("fail to get schema guard", K(ret));
    } else if (OB_FAIL(stat_converter_info.schema_guard_.get_table_schema(
                   stat_converter_info.real_table_id_, real_table_schema))) {
      LOG_WARN("get table schema failed", K(stat_converter_info.real_table_id_), K(ret));
    } else if (OB_FAIL(stat_converter_info.schema_guard_.get_table_schema(ref_table_id, vt_table_schema))) {
      LOG_WARN("get table schema failed", K(ref_table_id), K(ret));
    } else if (OB_ISNULL(real_table_schema) || OB_ISNULL(vt_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema is null", KP(real_table_schema), KP(vt_table_schema), K(ret));
    } else if (OB_FAIL(get_mapping_columns_info(vt_table_schema, real_table_schema, column_ids, stat_converter_info))) {
      LOG_WARN("failed to get mapping columns info", K(ret));
    } else if (OB_FAIL(stat_converter_info.vt_result_converter_.init_without_allocator(stat_converter_info.col_schemas_,
                   &stat_converter_info.output_column_with_tenant_ids_,
                   tenant_id,
                   stat_converter_info.tenant_id_col_id_,
                   stat_converter_info.use_real_tenant_id_))) {
      LOG_WARN("failed to init converter", K(ret));
    }
    if (OB_FAIL(ret)) {
    } else {
      LOG_DEBUG("debug virtual table stat",
          K(stat_converter_info.real_table_id_),
          K(tenant_id),
          K(column_ids),
          K(stat_converter_info.real_table_id_));
    }
  }
  return ret;
}

int ObTableColStatSqlService::get_mapping_columns_info(const ObTableSchema* vt_table_schema,
    const ObTableSchema* real_table_schema, const ObIArray<uint64_t>& column_ids,
    ObStatConverterInfo& stat_converter_info)
{
  int ret = OB_SUCCESS;
  ObIArray<bool>& key_with_tenant_ids = stat_converter_info.output_column_with_tenant_ids_;
  OZ(key_with_tenant_ids.reserve(column_ids.count()));
  OZ(stat_converter_info.col_schemas_.reserve(column_ids.count()));
  OZ(stat_converter_info.real_column_ids_.reserve(column_ids.count()));
  OZ(stat_converter_info.column_ids_.assign(column_ids));
  VTMapping* vt_mapping = nullptr;
  get_real_table_vt_mapping(vt_table_schema->get_table_id(), vt_mapping);
  if (OB_ISNULL(vt_mapping)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected status: vt mapping is null", K(ret), "table id", vt_table_schema->get_table_id());
  }
  for (int64_t k = 0; k < column_ids.count() && OB_SUCC(ret); ++k) {
    uint64_t real_column_id = UINT64_MAX;
    uint64_t vt_column_id = column_ids.at(k);
    const ObColumnSchemaV2* vt_col_schema = vt_table_schema->get_column_schema(vt_column_id);
    if (OB_ISNULL(vt_col_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: column schema is null", K(vt_column_id), K(ret));
    }
    for (int64_t nth_col = 0; nth_col < real_table_schema->get_column_count() && OB_SUCC(ret); ++nth_col) {
      const ObColumnSchemaV2* real_col_schema = real_table_schema->get_column_schema_by_idx(nth_col);
      if (OB_ISNULL(real_col_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column schema is null", K(ret), K(nth_col));
      } else if (0 == real_col_schema->get_column_name_str().case_compare(vt_col_schema->get_column_name_str())) {
        real_column_id = real_col_schema->get_column_id();
        bool extra_tenant_id = false;
        for (int64_t nth_i = vt_mapping->start_pos_; !extra_tenant_id && nth_i < vt_mapping->end_pos_ && OB_SUCC(ret);
             ++nth_i) {
          if (0 == ObString(with_tenant_id_columns[nth_i]).case_compare(vt_col_schema->get_column_name_str())) {
            extra_tenant_id = true;
            break;
          }
        }
        OZ(key_with_tenant_ids.push_back(extra_tenant_id));
        OZ(stat_converter_info.col_schemas_.push_back(vt_col_schema));
        OZ(stat_converter_info.real_column_ids_.push_back(real_col_schema->get_column_id()));
        if (0 == real_col_schema->get_column_name_str().case_compare(ObString("TENANT_ID"))) {
          stat_converter_info.tenant_id_col_id_ = vt_column_id;
        }
        break;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (UINT64_MAX == real_column_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected status: column not found", K(ret), K(vt_column_id), K(k));
    }
  }
  if (OB_SUCC(ret)) {
    stat_converter_info.use_real_tenant_id_ = vt_mapping->use_real_tenant_id_;
  }
  return ret;
}

int ObTableColStatSqlService::fetch_column_stat(const ObColumnStat::Key& key, ObColumnStat& stat)
{
  int ret = OB_SUCCESS;
  DEFINE_SQL_CLIENT_RETRY_WEAK_FOR_STAT(mysql_proxy_);
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    ObSqlString sql;
    ObStatConverterInfo stat_converter_info;
    const uint64_t tenant_id = extract_tenant_id(key.table_id_);
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    if (!inited_) {
      ret = OB_NOT_INIT;
      COMMON_LOG(WARN, "sql service has not initialized.", K(ret));
    } else if (!key.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid arguments.", K(key), K(ret));
    } else if (OB_FAIL(init_vt_converter(tenant_id, key.table_id_, key.column_id_, stat_converter_info))) {
      COMMON_LOG(WARN, "fail to init virtual table converter.", K(key), K(ret));
    } else if (OB_FAIL(sql.append(MY_FETCH_COL_STAT_SQL))) {
      COMMON_LOG(WARN, "fail to append SQL stmt string.", K(MY_FETCH_COL_STAT_SQL), K(ret));
    } else if (!stat_converter_info.is_vt_mapping_ &&
               OB_FAIL(sql.append_fmt(" WHERE ((TENANT_ID=%lu AND TABLE_ID=%ld) OR (TENANT_ID=%lu AND TABLE_ID=%ld))"
                                      " AND PARTITION_ID=%ld AND COLUMN_ID=%ld ORDER BY TENANT_ID",
                   ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                   ObSchemaUtils::get_extract_schema_id(exec_tenant_id, key.table_id_),
                   tenant_id,
                   key.table_id_,
                   key.partition_id_,
                   key.column_id_))) {
      COMMON_LOG(WARN, "fail to append SQL where string.", K(MY_FETCH_COL_STAT_SQL), K(ret));
    } else if (stat_converter_info.is_vt_mapping_ &&
               OB_FAIL(sql.append_fmt(" WHERE ((TENANT_ID=%lu AND TABLE_ID=%ld) OR (TENANT_ID=%lu AND TABLE_ID=%ld))"
                                      " AND PARTITION_ID=%ld AND COLUMN_ID=%ld ORDER BY TENANT_ID",
                   ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                   ObSchemaUtils::get_extract_schema_id(exec_tenant_id, stat_converter_info.real_table_id_),
                   tenant_id,
                   stat_converter_info.real_table_id_,
                   key.partition_id_,
                   stat_converter_info.real_column_ids_.at(0)))) {
      COMMON_LOG(WARN, "fail to append SQL where string.", K(MY_FETCH_COL_STAT_SQL), K(ret));
    } else if (OB_FAIL(sql_client_retry_weak.read(res, exec_tenant_id, sql.ptr()))) {
      COMMON_LOG(WARN, "execute sql failed", "sql", sql.ptr(), K(ret));
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "fail to execute ", "sql", sql.ptr(), K(ret));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END != ret) {
        COMMON_LOG(WARN, "result next failed, ", K(key), K(ret));
      } else {
        ret = OB_ENTRY_NOT_EXIST;
      }
    } else if (OB_FAIL(fill_column_stat(key.table_id_, *result, stat, 0, stat_converter_info))) {
      COMMON_LOG(WARN, "read stat from result failed. ", K(key), K(ret));
    } else {
      COMMON_LOG(INFO, "succeed to get column stat", K(stat));
    }
  }
  return ret;
}

int ObTableColStatSqlService::fetch_table_stat(
    const common::ObPartitionKey& key, ObIArray<std::pair<ObPartitionKey, ObTableStat>>& all_part_stats)
{
  int ret = OB_SUCCESS;
  DEFINE_SQL_CLIENT_RETRY_WEAK_FOR_STAT(mysql_proxy_);
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    ObSqlString sql;
    const char* table_name = NULL;
    uint64_t exec_tenant_id = OB_SYS_TENANT_ID;
    all_part_stats.reset();
    ObStatConverterInfo stat_converter_info;
    if (!inited_) {
      ret = OB_NOT_INIT;
      COMMON_LOG(WARN, "sql service has not initialized.", K(ret));
    } else if (!key.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid arguments.", K(key), K(ret));
    } else {
      if (is_sys_table(key.get_table_id())) {
        table_name = oceanbase::share::OB_ALL_ROOT_TABLE_TNAME;
        exec_tenant_id = OB_SYS_TENANT_ID;
      } else {
        table_name = oceanbase::share::OB_ALL_TENANT_PARTITION_META_TABLE_TNAME;
        exec_tenant_id = extract_tenant_id(key.get_table_id());
      }
      if (OB_FAIL(init_vt_converter(exec_tenant_id, key.table_id_, stat_converter_info))) {
        COMMON_LOG(WARN, "fail to init virtual table converter.", K(key), K(ret));
      } else if (OB_FAIL(
                     sql.append_fmt("SELECT partition_id, data_version,row_count,data_size FROM %s ", table_name))) {
        COMMON_LOG(WARN, "fail to append SQL stmt string.", K(sql), K(ret));
      } else if (!stat_converter_info.is_vt_mapping_ &&
                 OB_FAIL(sql.append_fmt(" WHERE TENANT_ID = %ld AND TABLE_ID=%ld AND REPLICA_TYPE = 0 AND STATUS = "
                                        "'REPLICA_STATUS_NORMAL' ORDER BY data_version DESC",
                     common::extract_tenant_id(key.get_table_id()),
                     key.get_table_id()))) {
        COMMON_LOG(WARN, "fail to append SQL where string.", K(MY_FETCH_COL_STAT_SQL), K(ret));
      } else if (stat_converter_info.is_vt_mapping_ &&
                 OB_FAIL(sql.append_fmt(" WHERE TENANT_ID = %ld AND TABLE_ID=%ld AND REPLICA_TYPE = 0 AND STATUS = "
                                        "'REPLICA_STATUS_NORMAL' ORDER BY data_version DESC",
                     exec_tenant_id,
                     stat_converter_info.real_table_id_))) {
        COMMON_LOG(WARN, "fail to append SQL where string.", K(MY_FETCH_COL_STAT_SQL), K(ret));
      } else if (OB_FAIL(sql_client_retry_weak.read(res, exec_tenant_id, sql.ptr()))) {
        COMMON_LOG(WARN, "execute sql failed", "sql", sql.ptr(), K(ret));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "fail to execute ", "sql", sql.ptr(), K(ret));
      }
      while (OB_SUCC(ret)) {
        int64_t partition_id = OB_INVALID_ID;
        std::pair<ObPartitionKey, ObTableStat> part_stat;
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END != ret) {
            COMMON_LOG(WARN, "result next failed.", K(ret));
          } else if (all_part_stats.empty()) {
            ret = OB_ENTRY_NOT_EXIST;
          } else {
            ret = OB_SUCCESS;
            break;
          }
        } else if (OB_FAIL(fill_table_stat(*result, partition_id, part_stat.second))) {
          COMMON_LOG(WARN, "read stat from result failed. ", K(ret));
        } else if (OB_FAIL(part_stat.first.init(key.get_table_id(), partition_id, key.get_partition_cnt()))) {
          LOG_WARN("failed to init partition key", K(ret));
        } else if (OB_FAIL(all_part_stats.push_back(part_stat))) {
          LOG_WARN("failed to push back partition stats", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObTableColStatSqlService::fetch_batch_stat(const uint64_t table_id, const ObIArray<uint64_t>& partition_id,
    const ObIArray<uint64_t>& column_id, ObIAllocator& allocator, common::ObIArray<ObColumnStat*>& stats,
    const bool use_pure_id)
{
  int ret = OB_SUCCESS;
  DEFINE_SQL_CLIENT_RETRY_WEAK_FOR_STAT(mysql_proxy_);
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    ObSqlString sql;
    stats.reuse();
    ObStatConverterInfo stat_converter_info;
    const uint64_t tenant_id = extract_tenant_id(table_id);
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    if (!inited_) {
      ret = OB_NOT_INIT;
      COMMON_LOG(WARN, "sql service has not initialized.", K(ret));
    } else if (OB_INVALID_ID == table_id) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid argument.", K(ret), K(table_id), K(partition_id));
    } else if (OB_UNLIKELY(partition_id.empty() || column_id.empty())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("column stat entry size is invalid", K(ret), K(partition_id.count()), K(column_id.count()));
    } else if (OB_FAIL(init_vt_converter(tenant_id, table_id, column_id, stat_converter_info))) {
      COMMON_LOG(WARN, "fail to init virtual table converter.", K(ret));
    } else if (OB_FAIL(sql.append(MY_FETCH_COL_STAT_SQL))) {
      COMMON_LOG(WARN, "fail to append SQL stmt string.", K(ret), K(MY_FETCH_COL_STAT_SQL));
    } else if (!stat_converter_info.is_vt_mapping_ &&
               OB_FAIL(sql.append_fmt(" WHERE TENANT_ID=%lu AND TABLE_ID = %ld AND PARTITION_ID IN ",
                   use_pure_id ? ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id) : tenant_id,
                   use_pure_id ? ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id) : table_id))) {
      COMMON_LOG(WARN, "fail to append SQL where string.", K(ret), K(MY_FETCH_COL_STAT_SQL));
    } else if (stat_converter_info.is_vt_mapping_ &&
               OB_FAIL(sql.append_fmt(" WHERE TENANT_ID=%lu AND TABLE_ID = %ld AND PARTITION_ID IN ",
                   use_pure_id ? ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id) : tenant_id,
                   use_pure_id
                       ? ObSchemaUtils::get_extract_schema_id(exec_tenant_id, stat_converter_info.real_table_id_)
                       : stat_converter_info.real_table_id_))) {
      COMMON_LOG(WARN, "fail to append SQL where string.", K(ret), K(MY_FETCH_COL_STAT_SQL));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_id.count(); ++i) {
      if (OB_FAIL(sql.append_fmt(
              "%c%lu%c", (i == 0 ? '(' : ' '), partition_id.at(i), (i < partition_id.count() - 1 ? ',' : ')')))) {
        COMMON_LOG(WARN, "fail to append SQL in string. ", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(sql.append_fmt(" AND COLUMN_ID IN "))) {
        COMMON_LOG(WARN, "fail to append column id filter.", K(ret), K(MY_FETCH_COL_STAT_SQL));
      }
    }
    if (!stat_converter_info.is_vt_mapping_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < column_id.count(); ++i) {
        if (OB_FAIL(sql.append_fmt(
                "%c%lu%c", (i == 0 ? '(' : ' '), column_id.at(i), (i < column_id.count() - 1 ? ',' : ')')))) {
          COMMON_LOG(WARN, "fail to append SQL in string", K(ret), K(i));
        }
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < stat_converter_info.real_column_ids_.count(); ++i) {
        if (OB_FAIL(sql.append_fmt("%c%lu%c",
                (i == 0 ? '(' : ' '),
                stat_converter_info.real_column_ids_.at(i),
                (i < stat_converter_info.real_column_ids_.count() - 1 ? ',' : ')')))) {
          COMMON_LOG(WARN, "fail to append SQL in string", K(ret), K(i));
        }
      }
    }
    if (OB_SUCC(ret)) {
      void* tmp_ptr = NULL;
      ObColumnStat* column_stat = NULL;
      if (OB_FAIL(sql_client_retry_weak.read(res, exec_tenant_id, sql.ptr()))) {
        COMMON_LOG(WARN, "execute sql failed", "sql", sql.ptr(), K(ret));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "fail to execute ", "sql", sql.ptr(), K(ret));
      } else {
        int64_t idx = 0;
        while (OB_SUCC(ret)) {
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END != ret) {
              COMMON_LOG(WARN, "result next failed.", K(ret));
            } else if (stats.empty()) {
              ret = OB_ENTRY_NOT_EXIST;
            } else {
              ret = OB_SUCCESS;
              break;
            }
          } else if (OB_ISNULL(tmp_ptr = allocator.alloc(sizeof(ObColumnStat))) ||
                     OB_ISNULL(column_stat = new (tmp_ptr) ObColumnStat(allocator)) ||
                     OB_UNLIKELY(!column_stat->is_writable())) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            COMMON_LOG(ERROR, "failed to allocate memory for ObColumnStat", K(ret), "size", sizeof(ObColumnStat));
          } else if (OB_FAIL(fill_column_stat(table_id, *result, *column_stat, idx, stat_converter_info))) {
            COMMON_LOG(WARN, "read stat from result failed. ", K(ret), K(table_id), K(partition_id));
          } else if (OB_FAIL(stats.push_back(column_stat))) {
            LOG_WARN("failed to push back column stat", K(ret));
          } else {
#if !defined(NDEBUG)
            COMMON_LOG(INFO, "succeed to get column stat", K(*column_stat));
#endif
            tmp_ptr = NULL;
            column_stat = NULL;
            ++idx;
          }
        }
      }
    }
  }
  return ret;
}

int ObTableColStatSqlService::insert_or_update_column_stats(const ObIArray<ObColumnStat*>& stats)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  ObSqlString sql;
  ObSqlString values;
  share::ObDMLSqlSplicer dml;
  ObArenaAllocator arean(ObModIds::OB_BUFFER);
  int64_t affected_rows = 0;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;

  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "sql service has not initialized.", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < stats.count(); ++i) {
      dml.reuse();
      if (NULL == stats.at(i)) {
        ret = OB_ERR_UNEXPECTED;
        COMMON_LOG(WARN, "item in stats must not NULL, ", K(ret), K(i));
      } else if (OB_INVALID_TENANT_ID != tenant_id && tenant_id != extract_tenant_id(stats.at(i)->get_table_id())) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("update column stats with multi tenant not supported", K(ret), K(tenant_id), KPC(stats.at(i)));
      } else if (OB_FAIL(fill_dml_splicer(*stats.at(i), dml))) {
        COMMON_LOG(WARN, "fill add ObColumnStat stat.", K(ret), K(i), K(*stats.at(i)));
      } else {
        tenant_id = extract_tenant_id(stats.at(i)->get_table_id());
        if (0 == i) {
          if (OB_FAIL(dml.splice_column_names(values))) {
            COMMON_LOG(WARN, "fail to splice column names.", K(ret));
          } else if (OB_FAIL(sql.assign_fmt("INSERT /*+ use_plan_cache(none) */ INTO %s (%s) VALUES",
                         USER_TAB_COL_STATISTICS,
                         values.ptr()))) {
            COMMON_LOG(WARN, "fail to assign sql string.", K(ret));
          }
        }
      }

      if (OB_SUCC(ret)) {
        values.reset();
        if (OB_FAIL(dml.splice_values(values))) {
          COMMON_LOG(WARN, "fail to splice values.", K(ret));
        } else if (OB_FAIL(sql.append_fmt("%s(%s)", 0 == i ? " " : " , ", values.ptr()))) {
          COMMON_LOG(WARN, "fail to assign sql string.", K(ret));
        } else {
          if ((stats.count() - 1) == i) {
            if (OB_FAIL(sql.append(" ON DUPLICATE KEY UPDATE "))) {
              COMMON_LOG(WARN, "fail to append sql string.", K(ret));
            } else if (OB_FAIL(sql.append(" version = values(version)")) ||
                       OB_FAIL(sql.append(", last_rebuild_version = values(last_rebuild_version)")) ||
                       OB_FAIL(sql.append(", num_distinct = values(num_distinct)")) ||
                       OB_FAIL(sql.append(", num_null = values(num_null)")) ||
                       OB_FAIL(sql.append(", min_value = values(min_value)")) ||
                       OB_FAIL(sql.append(", max_value = values(max_value)")) ||
                       OB_FAIL(sql.append(", llc_bitmap = values(llc_bitmap)")) ||
                       OB_FAIL(sql.append(", llc_bitmap_size = values(llc_bitmap_size)"))) {
              COMMON_LOG(WARN, "fail to append sql string.", K(ret));
            }
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    if (OB_FAIL(trans.start(mysql_proxy_))) {
      COMMON_LOG(WARN, "fail to start transaction. ", K(ret));
    } else if (OB_FAIL(mysql_proxy_->write(exec_tenant_id, sql.ptr(), affected_rows))) {
      COMMON_LOG(WARN, "fail to exec sql. ", K(ret), K(tenant_id));
    } else if (affected_rows > 2 * stats.count()) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN,
          "unexpected affected rows.",
          K(ret),
          "expect_affect_rows",
          stats.count(),
          "actual_affect_rows",
          affected_rows);
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(trans.end(true))) {
      COMMON_LOG(WARN, "fail to commit transacrion.", K(ret));
    }
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
      COMMON_LOG(WARN, "fail to rollback transaction. ", K(tmp_ret));
    }
  }

  return ret;
}

// replace into statement is not supported for now, use dml splice (insert_or_update) as substitution.
int ObTableColStatSqlService::insert_or_update_column_stat(const ObColumnStat& stat)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = extract_tenant_id(stat.get_table_id());
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  ObMySQLTransaction trans;
  share::ObDMLSqlSplicer dml;
  share::ObDMLExecHelper dml_exec_helper(*mysql_proxy_, exec_tenant_id);
  // TODO min,max value could be very large??
  ObArenaAllocator arena(ObModIds::OB_BUFFER);

  const int64_t total_buffer_size = stat.get_min_value().get_serialize_size() * 3 +
                                    stat.get_min_value().get_serialize_size() * 3 + stat.get_llc_bitmap_size() * 2 +
                                    3;  // (2 bytes for c string end \0);

  int64_t pos = 0;
  int64_t min_value_str_pos = 0;
  int64_t max_value_str_pos = 0;
  int64_t llc_str_pos = 0;
  int64_t affected_rows = 0;
  int64_t llc_comp_size = 0;

  char* all_field_buf = NULL;
  char* llc_comp_buf = NULL;

  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "sql service has not initialized.", K(ret));
  } else if (!stat.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.", K(stat), K(ret));
  } else if (NULL == (all_field_buf = static_cast<char*>(arena.alloc(total_buffer_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(ERROR, "cannot allocate memory.", K(total_buffer_size), K(ret));
  } else if (OB_FAIL(serialize_to_hex_cstr(
                 stat.get_min_value(), all_field_buf, total_buffer_size, pos, min_value_str_pos))) {
    COMMON_LOG(WARN, "serialize min obj value failed.", K(ret));
  } else if (OB_FAIL(serialize_to_hex_cstr(
                 stat.get_max_value(), all_field_buf, total_buffer_size, pos, max_value_str_pos))) {
    COMMON_LOG(WARN, "serialize max obj value failed.", K(ret));
  } else if (OB_FAIL(get_compressed_llc_bitmap(
                 arena, stat.get_llc_bitmap(), stat.get_llc_bitmap_size(), llc_comp_buf, llc_comp_size))) {
    COMMON_LOG(WARN, "get_compressed_llc_bitmap failed.", K(ret));
  } else if (OB_FAIL(common::to_hex_cstr(
                 llc_comp_buf, llc_comp_size, all_field_buf, total_buffer_size, pos, llc_str_pos))) {
    COMMON_LOG(WARN, "serialize llc bitmap failed.", K(llc_comp_size), K(ret));
  } else {
    if (OB_FAIL(dml.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id))) ||
        OB_FAIL(dml.add_pk_column("table_id", ObSchemaUtils::get_extract_schema_id(tenant_id, stat.get_table_id()))) ||
        OB_FAIL(dml.add_pk_column("partition_id", stat.get_partition_id())) ||
        OB_FAIL(dml.add_pk_column("column_id", stat.get_column_id())) ||
        OB_FAIL(dml.add_column("version", stat.get_version())) ||
        OB_FAIL(dml.add_column("last_rebuild_version", stat.get_last_rebuild_version())) ||
        OB_FAIL(dml.add_column("num_distinct", stat.get_num_distinct())) ||
        OB_FAIL(dml.add_column("num_null", stat.get_num_null())) ||
        OB_FAIL(dml.add_column("min_value", all_field_buf + min_value_str_pos)) ||
        OB_FAIL(dml.add_column("max_value", all_field_buf + max_value_str_pos)) ||
        OB_FAIL(dml.add_column("llc_bitmap", all_field_buf + llc_str_pos)) ||
        OB_FAIL(dml.add_column("llc_bitmap_size", llc_comp_size * 2))) {
      COMMON_LOG(WARN, "add column failed", K(ret));
    } else if (OB_FAIL(trans.start(mysql_proxy_))) {
      COMMON_LOG(WARN, "fail to to start transaction. ", K(ret));
    } else if (OB_FAIL(dml_exec_helper.exec_insert_update(USER_TAB_COL_STATISTICS, dml, affected_rows))) {
      COMMON_LOG(WARN, "fail to to write transaction. ", K(ret));
    } else if (affected_rows > 2) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN, "replace into return grate than 2", K(affected_rows), K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(trans.end(true))) {
      COMMON_LOG(WARN, "fail to commit transaction.", K(ret));
    }
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
      COMMON_LOG(WARN, "fail to rollback transaction. ", K(tmp_ret));
    }
  }

  return ret;
}

int ObTableColStatSqlService::fill_column_stat(const uint64_t table_id, common::sqlclient::ObMySQLResult& result,
    ObColumnStat& stat, int64_t idx, ObStatConverterInfo& stat_converter_info)
{
  int ret = OB_SUCCESS;
  uint64_t dummy = 0;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "sql service has not initialized.", K(ret));
  }
  // after 2.2.4 __all_column_statistic stores pure table id (without tenant_id)
  // the stat cache stores full table id (with tenant_id)
  stat.set_table_id(table_id);
  EXTRACT_INT_FIELD_MYSQL(result, "tenant_id", dummy, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "table_id", dummy, uint64_t);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, partition_id, stat, uint64_t);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, column_id, stat, uint64_t);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, version, stat, int64_t);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, last_rebuild_version, stat, int64_t);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, num_distinct, stat, int64_t);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, num_null, stat, int64_t);

  ObArenaAllocator arena(ObModIds::OB_BUFFER);
  char* bitmap_buf = NULL;
  ObString str_field;
  EXTRACT_VARCHAR_FIELD_MYSQL(result, "llc_bitmap", str_field);
  if (OB_SUCC(ret)) {
    if (NULL == (bitmap_buf = static_cast<char*>(arena.alloc(str_field.length())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(ERROR, "allocate memory for llc_bitmap failed.", K(str_field.length()), K(ret));
    } else {
      common::str_to_hex(str_field.ptr(), str_field.length(), bitmap_buf, str_field.length());
    }
  }

  if (OB_SUCC(ret)) {
    // decompress llc bitmap;
    char* decomp_buf = NULL;
    int64_t decomp_size = ObColumnStat::NUM_LLC_BUCKET;
    const int64_t bitmap_size = str_field.length() / 2;

    if (OB_FAIL(get_decompressed_llc_bitmap(arena, bitmap_buf, bitmap_size, decomp_buf, decomp_size))) {
      COMMON_LOG(WARN, "decompress bitmap buffer failed.", K(ret));
    } else if (OB_FAIL(stat.store_llc_bitmap(decomp_buf, decomp_size))) {
      COMMON_LOG(
          WARN, "set llc bitmap failed.", KP(bitmap_buf), K(bitmap_size), KP(decomp_buf), K(decomp_size), K(ret));
    }
  }

  common::ObObj obj;

  EXTRACT_VARCHAR_FIELD_MYSQL(result, "min_value", str_field);
  if (OB_SUCC(ret)) {
    if (stat_converter_info.is_vt_mapping_ && idx >= stat_converter_info.column_ids_.count()) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN,
          "unexpected status: column idx is invalid",
          K(ret),
          "column count",
          stat_converter_info.column_ids_.count(),
          "column idx",
          idx);
    } else if (OB_FAIL(deserialize_hex_cstr(str_field.ptr(), str_field.length(), arena, obj))) {
      COMMON_LOG(WARN, "deserialize_hex_cstr min value failed.", K(stat), K(ret));
    } else if (stat_converter_info.is_vt_mapping_ && OB_FAIL(stat_converter_info.vt_result_converter_.convert_column(
                                                         obj, stat_converter_info.column_ids_.at(idx), idx))) {
    } else if (OB_FAIL(stat.store_min_value(obj))) {
      COMMON_LOG(WARN, "store min value failed.", K(stat), K(ret));
    }
  }

  EXTRACT_VARCHAR_FIELD_MYSQL(result, "max_value", str_field);
  if (OB_SUCC(ret)) {
    if (stat_converter_info.is_vt_mapping_ && idx >= stat_converter_info.column_ids_.count()) {
      ret = OB_ERR_UNEXPECTED;
      COMMON_LOG(WARN,
          "unexpected status: column idx is invalid",
          K(ret),
          "column count",
          stat_converter_info.column_ids_.count(),
          "column idx",
          idx);
    } else if (OB_FAIL(deserialize_hex_cstr(str_field.ptr(), str_field.length(), arena, obj))) {
      COMMON_LOG(WARN, "deserialize_hex_cstr max value failed.", K(stat), K(ret));
    } else if (stat_converter_info.is_vt_mapping_ && OB_FAIL(stat_converter_info.vt_result_converter_.convert_column(
                                                         obj, stat_converter_info.column_ids_.at(idx), idx))) {
    } else if (OB_FAIL(stat.store_max_value(obj))) {
      COMMON_LOG(WARN, "store max value failed.", K(stat), K(ret));
    }
  }

  UNUSED(dummy);
  return ret;
}

int ObTableColStatSqlService::fill_table_stat(
    common::sqlclient::ObMySQLResult& result, int64_t& part_id, ObTableStat& stat)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "sql service has not initialized.", K(ret));
  }
  EXTRACT_INT_FIELD_MYSQL(result, "partition_id", part_id, int64_t);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, data_version, stat, int64_t);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, row_count, stat, int64_t);
  EXTRACT_INT_FIELD_TO_CLASS_MYSQL(result, data_size, stat, int64_t);
  if (OB_SUCC(ret)) {
    if (stat.get_row_count() > 0) {
      stat.set_average_row_size(stat.get_data_size() / stat.get_row_count());
    }
  }
  return ret;
}

int ObTableColStatSqlService::serialize_to_hex_cstr(
    const common::ObObj& obj, char* buf, int64_t buf_len, int64_t& pos, int64_t& cstr_pos)
{
  int ret = OB_SUCCESS;
  int64_t hex_len = 0;
  int64_t start_pos = pos;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "sql service has not initialized.", K(ret));
  } else if (NULL == buf || buf_len < 0 || pos < 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.", KP(buf), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(obj.serialize(buf, buf_len, pos))) {
    COMMON_LOG(WARN, "serialize obj failed.", KP(buf), K(buf_len), K(pos), K(ret));
  } else {
    hex_len = pos - start_pos;
    if (OB_FAIL(common::to_hex_cstr(buf + start_pos, hex_len, buf, buf_len, pos, cstr_pos))) {
      COMMON_LOG(WARN, "Failed to transform to hex cstr", K(ret));
    }
  }
  return ret;
}

int ObTableColStatSqlService::deserialize_hex_cstr(char* buf, int64_t buf_len, common::ObObj& obj)
{
  int ret = OB_SUCCESS;

  int64_t pos = 0;
  int64_t ret_len = 0;

  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "sql service has not initialized.", K(ret));
  } else if (NULL == buf || buf_len < 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.", KP(buf), K(buf_len), K(ret));
  } else if (buf_len !=
             (ret_len = common::str_to_hex(buf, static_cast<int32_t>(buf_len), buf, static_cast<int32_t>(buf_len)))) {
    // str_to_hex can use input as output, convert char in placement.
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "transfer str to hex failed.", K(buf), K(buf_len), K(ret_len), K(ret));
  } else if (OB_FAIL(obj.deserialize(buf, ret_len / 2, pos))) {
    COMMON_LOG(WARN, "deserialize obj failed.", K(buf), K(buf_len), K(pos), K(ret));
  }
  return ret;
}

int ObTableColStatSqlService::deserialize_hex_cstr(
    char* buf, int64_t buf_len, ObIAllocator& allocator, common::ObObj& obj)
{

  int ret = OB_SUCCESS;

  int64_t pos = 0;
  int64_t ret_len = 0;
  char* resbuf = NULL;
  if (!inited_) {
    ret = OB_NOT_INIT;
    COMMON_LOG(WARN, "sql service has not initialized.", K(ret));
  } else if (NULL == buf || buf_len < 0) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.", KP(buf), K(buf_len), K(ret));
  } else if (NULL == (resbuf = static_cast<char*>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(ERROR, "cannot allocate memory for deserialize obj.", K(buf_len), K(ret));
  } else if (buf_len != (ret_len = common::str_to_hex(
                             buf, static_cast<int32_t>(buf_len), resbuf, static_cast<int32_t>(buf_len)))) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "transfer str to hex failed.", K(buf), K(buf_len), K(ret_len), K(ret));
  } else if (OB_FAIL(obj.deserialize(resbuf, ret_len, pos))) {
    COMMON_LOG(WARN, "deserialize obj failed.", K(buf), K(buf_len), K(pos), K(ret));
  }
  return ret;
}

int ObTableColStatSqlService::get_compressed_llc_bitmap(
    ObIAllocator& allocator, const char* bitmap_buf, int64_t bitmap_size, char*& comp_buf, int64_t& comp_size)
{
  int ret = OB_SUCCESS;
  ObCompressor* compressor = NULL;
  int64_t max_comp_size = 0;
  if (NULL == bitmap_buf || bitmap_size <= 0) {
    ret = common::OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid arguments.", KP(bitmap_buf), K(bitmap_size), K(ret));
  } else if (OB_FAIL(ObCompressorPool::get_instance().get_compressor(bitmap_compress_lib_name, compressor))) {
    COMMON_LOG(WARN, "cannot create compressor, do not compress data.", K(bitmap_compress_lib_name), K(ret));
  } else if (NULL == compressor) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "compressor is NULL, do not compress data.", K(bitmap_compress_lib_name), K(ret));
  } else if (OB_FAIL(compressor->get_max_overflow_size(bitmap_size, max_comp_size))) {
    COMMON_LOG(WARN, "get max overflow size failed.", K(bitmap_compress_lib_name), K(bitmap_size), K(ret));
  } else {
    max_comp_size += bitmap_size;
    if (NULL == (comp_buf = static_cast<char*>(allocator.alloc(max_comp_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(ERROR, "cannot allocate compressed buffer.", K(max_comp_size), K(ret));
    } else if (OB_FAIL(compressor->compress(bitmap_buf, bitmap_size, comp_buf, max_comp_size, comp_size))) {
      COMMON_LOG(WARN, "compress llc bitmap failed.", K(ret));
    } else if (comp_size >= bitmap_size) {
      // compress is not work, just use original data.
      comp_buf = const_cast<char*>(bitmap_buf);
      comp_size = bitmap_size;
    }
  }
  return ret;
}

int ObTableColStatSqlService::get_decompressed_llc_bitmap(
    ObIAllocator& allocator, const char* comp_buf, int64_t comp_size, char*& bitmap_buf, int64_t& bitmap_size)
{
  int ret = OB_SUCCESS;
  const int64_t max_bitmap_size = ObColumnStat::NUM_LLC_BUCKET;  // max size of uncompressed buffer.
  ObCompressor* compressor = NULL;

  if (comp_size >= ObColumnStat::NUM_LLC_BUCKET) {
    // not compressed bitmap, use directly;
    bitmap_buf = const_cast<char*>(comp_buf);
    bitmap_size = comp_size;
  } else if (NULL == (bitmap_buf = static_cast<char*>(allocator.alloc(max_bitmap_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(ERROR, "allocate memory for uncompressed data failed.", K(max_bitmap_size), K(ret));
  } else if (OB_FAIL(ObCompressorPool::get_instance().get_compressor(bitmap_compress_lib_name, compressor))) {
    COMMON_LOG(WARN, "cannot create compressor, do not uncompress data.", K(bitmap_compress_lib_name), K(ret));
  } else if (NULL == compressor) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "compressor is NULL, do not compress data.", K(bitmap_compress_lib_name), K(ret));
  } else if (OB_FAIL(compressor->decompress(comp_buf, comp_size, bitmap_buf, max_bitmap_size, bitmap_size))) {
    COMMON_LOG(WARN,
        "decompress bitmap buffer failed.",
        KP(comp_buf),
        K(comp_size),
        KP(bitmap_buf),
        K(max_bitmap_size),
        K(bitmap_size),
        K(ret));
  }
  return ret;
}

int ObTableColStatSqlService::fill_dml_splicer(const ObColumnStat& stat, share::ObDMLSqlSplicer& dml_splicer)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator arena(ObModIds::OB_BUFFER);
  const int64_t total_buffer_size = stat.get_min_value().get_serialize_size() * 3 +
                                    stat.get_min_value().get_serialize_size() * 3 + stat.get_llc_bitmap_size() * 2 + 3;
  int64_t pos = 0;
  int64_t min_value_str_pos = 0;
  int64_t max_value_str_pos = 0;
  int64_t llc_str_pos = 0;
  int64_t llc_comp_size = 0;
  char* all_field_buf = NULL;
  char* llc_comp_buf = NULL;

  if (!stat.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument, ", K(ret), K(stat));
  } else if (NULL == (all_field_buf = static_cast<char*>(arena.alloc(total_buffer_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(ERROR, "cannot allocate memory.", K(ret), K(total_buffer_size));
  } else if (OB_FAIL(serialize_to_hex_cstr(
                 stat.get_min_value(), all_field_buf, total_buffer_size, pos, min_value_str_pos))) {
    COMMON_LOG(WARN, "serialize min obj value failed.", K(ret));
  } else if (OB_FAIL(serialize_to_hex_cstr(
                 stat.get_max_value(), all_field_buf, total_buffer_size, pos, max_value_str_pos))) {
    COMMON_LOG(WARN, "serialize max obj value failed.", K(ret));
  } else if (OB_FAIL(get_compressed_llc_bitmap(
                 arena, stat.get_llc_bitmap(), stat.get_llc_bitmap_size(), llc_comp_buf, llc_comp_size))) {
    COMMON_LOG(WARN, "get_compressed_llc_bitmap failed.", K(ret));
  } else if (OB_FAIL(common::to_hex_cstr(
                 llc_comp_buf, llc_comp_size, all_field_buf, total_buffer_size, pos, llc_str_pos))) {
    COMMON_LOG(WARN, "serialize llc bitmap failed.", K(ret), K(llc_comp_size));
  } else {
    const uint64_t tenant_id = extract_tenant_id(stat.get_table_id());
    if (OB_FAIL(dml_splicer.add_pk_column("tenant_id", ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id))) ||
        OB_FAIL(dml_splicer.add_pk_column(
            "table_id", ObSchemaUtils::get_extract_schema_id(tenant_id, stat.get_table_id()))) ||
        OB_FAIL(dml_splicer.add_pk_column("partition_id", stat.get_partition_id())) ||
        OB_FAIL(dml_splicer.add_pk_column("column_id", stat.get_column_id())) ||
        OB_FAIL(dml_splicer.add_column("version", stat.get_version())) ||
        OB_FAIL(dml_splicer.add_column("last_rebuild_version", stat.get_last_rebuild_version())) ||
        OB_FAIL(dml_splicer.add_column("num_distinct", stat.get_num_distinct())) ||
        OB_FAIL(dml_splicer.add_column("num_null", stat.get_num_null())) ||
        OB_FAIL(dml_splicer.add_column("min_value", all_field_buf + min_value_str_pos)) ||
        OB_FAIL(dml_splicer.add_column("max_value", all_field_buf + max_value_str_pos)) ||
        OB_FAIL(dml_splicer.add_column("llc_bitmap", all_field_buf + llc_str_pos)) ||
        OB_FAIL(dml_splicer.add_column("llc_bitmap_size", llc_comp_size * 2))) {
      COMMON_LOG(WARN, "add column failed.", K(ret));
    }
  }

  return ret;
}

}  // end of namespace common
}  // end of namespace oceanbase
