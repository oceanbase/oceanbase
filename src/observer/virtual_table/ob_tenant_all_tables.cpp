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
#include <algorithm>
#include "observer/virtual_table/ob_tenant_all_tables.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_printer.h"
#include "share/ob_autoincrement_service.h"
#include "sql/session/ob_sql_session_info.h"
#include "observer/ob_server_struct.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "common/ob_store_format.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
namespace oceanbase {
namespace share {
class ObPartitionTableOperator;
}
namespace observer {

ObTenantAllTables::ObTenantAllTables()
    : ObVirtualTableIterator(),
      sql_proxy_(NULL),
      tenant_id_(OB_INVALID_ID),
      database_id_(OB_INVALID_ID),
      table_schemas_(),
      table_schema_idx_(0),
      seq_values_(),
      tables_statistics_(),
      option_buf_(NULL)
{}

ObTenantAllTables::~ObTenantAllTables()
{}

int ObTenantAllTables::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == schema_guard_ || OB_INVALID_ID == tenant_id_ || NULL == allocator_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "data member doesn't init", K(ret), K(schema_guard_), K(tenant_id_), K(allocator_));
  } else if (OB_UNLIKELY(NULL == (option_buf_ = static_cast<char*>(
                                      allocator_->alloc(MAX_TABLE_STATUS_CREATE_OPTION_LENGTH))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(ERROR, "fail to alloc memory", K(ret));
  } else {
    // get database_id
    ObRowkey start_key;
    ObRowkey end_key;
    for (int64_t i = 0; OB_SUCC(ret) && !is_valid_id(database_id_) && i < key_ranges_.count(); ++i) {
      start_key.reset();
      end_key.reset();
      start_key = key_ranges_.at(i).start_key_;
      end_key = key_ranges_.at(i).end_key_;
      const ObObj* start_key_obj_ptr = start_key.get_obj_ptr();
      const ObObj* end_key_obj_ptr = end_key.get_obj_ptr();
      if (start_key.get_obj_cnt() > 0 && start_key.get_obj_cnt() == end_key.get_obj_cnt()) {
        if (OB_UNLIKELY(NULL == start_key_obj_ptr || NULL == end_key_obj_ptr)) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(ERROR, "key obj ptr is NULL", K(ret), K(start_key_obj_ptr), K(end_key_obj_ptr));
        } else if (start_key_obj_ptr[0] == end_key_obj_ptr[0] && ObIntType == start_key_obj_ptr[0].get_type()) {
          database_id_ = start_key_obj_ptr[0].get_int();
        }
      }
    }  // for
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(!is_valid_id(database_id_))) {
        // FIXME: we should have a better way to handle this in the future
        ret = OB_ERR_UNEXPECTED;
        LOG_USER_ERROR(OB_ERR_UNEXPECTED, "this table is used for show clause, can't be selected");
      } else {
        if (OB_FAIL(schema_guard_->get_table_schemas_in_database(tenant_id_, database_id_, table_schemas_))) {
          SERVER_LOG(WARN, "fail to get table schemas in database", K(ret), K(tenant_id_), K(database_id_));
        } else if (OB_FAIL(seq_values_.create(
                       FETCH_SEQ_NUM_ONCE, ObModIds::OB_AUTOINCREMENT, ObModIds::OB_AUTOINCREMENT))) {
          SERVER_LOG(WARN, "failed to create seq values ObHashMap", K(ret), LITERAL_K(FETCH_SEQ_NUM_ONCE));
        } else {
          bool fetch_inc = false;
          bool fetch_stat = false;
          for (int64_t i = 0; OB_SUCC(ret) && (!fetch_inc || !fetch_stat) && i < output_column_ids_.count(); ++i) {
            uint64_t col_id = output_column_ids_.at(i);
            switch (col_id) {
              case AUTO_INCREMENT: {
                if (!fetch_inc) {
                  if (OB_FAIL(get_sequence_value())) {
                    SERVER_LOG(WARN, "fail to get sequence value", K(ret));
                  } else {
                    fetch_inc = true;
                  }
                }
                break;
              }
              case TABLE_VERSION:
              case ROWS:
              case AVG_ROW_LENGTH:
              case DATA_LENGTH:
              case CREATE_TIME:
              case UPDATE_TIME:
              case CHECKSUM: {
                if (!fetch_stat) {
                  if (OB_FAIL(get_tables_stat())) {
                    SERVER_LOG(WARN, "fail to get tables stat", K(ret));
                  } else {
                    fetch_stat = true;
                  }
                }
                break;
              }
              default: {
                break;  // do nothing
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObTenantAllTables::get_sequence_value()
{
  int ret = OB_SUCCESS;
  ObArray<AutoincKey> autoinc_keys;
  AutoincKey key;
  for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas_.count(); ++i) {
    const ObTableSchema* table_schema = table_schemas_.at(i);
    if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "table schema is NULL", K(ret), K(i), K(tenant_id_), K(database_id_));
    } else if (table_schema->get_autoinc_column_id() != 0) {
      key.reset();
      key.tenant_id_ = table_schema->get_tenant_id();  // always same as tenant_id_
      key.table_id_ = table_schema->get_table_id();
      key.column_id_ = table_schema->get_autoinc_column_id();
      if (OB_FAIL(autoinc_keys.push_back(key))) {
        SERVER_LOG(WARN, "failed to push back AutoincKey", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(share::ObAutoincrementService::get_instance().get_sequence_value(
                          tenant_id_, autoinc_keys, seq_values_))) {
    SERVER_LOG(WARN, "failed to get sequence value", K(ret));
  }
  return ret;
}
int ObTenantAllTables::get_tables_stat()
{
  int ret = OB_SUCCESS;
  const char* sql_str = "select t1.table_id as table_id, row_count, data_size, data_version, \
                         data_checksum, create_time, time_to_usec(t1.gmt_modified) as update_time \
                         from oceanbase.%s t1 inner join oceanbase.%s t2 on (t1.tenant_id = %lu and \
                         t1.tenant_id = t2.tenant_id and t1.table_id  = t2.table_id) \
                         where database_id = %lu and partition_id = 0 and role = %d";
  ObSqlString sql;  // for get sys table status
  const uint64_t BUCKET_NUM = 1000;
  if (OB_ISNULL(GCTX.pt_operator_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "pt_operator is null", K(ret));
  } else if (OB_UNLIKELY(!is_valid_id(database_id_))) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "database id is invalid", K(ret), K(database_id_));
  } else if (tables_statistics_.create(
                 BUCKET_NUM, ObModIds::OB_HASH_BUCKET_TABLE_STATISTICS, ObModIds::OB_HASH_BUCKET_TABLE_STATISTICS)) {
    SERVER_LOG(WARN, "fail to create hash map", K(ret), K(BUCKET_NUM));
  } else if (OB_FAIL(sql.append_fmt(sql_str,
                 OB_ALL_VIRTUAL_TABLE_TNAME,
                 OB_ALL_ROOT_TABLE_TNAME,
                 extract_tenant_id(database_id_),
                 database_id_,
                 common::LEADER))) {
    SERVER_LOG(WARN, "fail to append SQL stmt string.", K(ret), K(database_id_), K(OB_ALL_ROOT_TABLE_TNAME), K(sql));
  } else if (OB_FAIL(fill_table_statistics(sql, OB_SYS_TENANT_ID))) {
    SERVER_LOG(WARN, "fail to fill table statistics", K(ret));
  } else {  // get user table status
    const char* meta_table_name = NULL;
    uint64_t sql_tenant_id = OB_SYS_TENANT_ID;
    ObArray<uint64_t> all_user_table_ids;
    meta_table_name = OB_ALL_TENANT_META_TABLE_TNAME;
    sql_tenant_id = extract_tenant_id(database_id_);
    if (OB_FAIL(get_all_table_ids(all_user_table_ids))) {
      LOG_WARN("fail to get all table ids", K(ret));
    } else if (OB_FAIL(fill_user_table_statistics(all_user_table_ids, meta_table_name, sql_tenant_id))) {
      LOG_WARN("fail to fill user table statictics", K(ret));
    } else { /*do nothing*/
    }
  }
  return ret;
}

int ObTenantAllTables::fill_user_table_statistics(
    const ObIArray<uint64_t>& table_ids, const char* meta_table_name, uint64_t sql_tenant_id)
{
  int ret = OB_SUCCESS;
  const int64_t BATCH_SIZE = 1000;  // do not allow oversized user_sql
  if (table_ids.count() == 0) {
    // do nothing
  } else {
    ObSqlString user_sql;  // for get user table status;
    for (int64_t i = 0; OB_SUCC(ret) && i < table_ids.count(); i += BATCH_SIZE) {
      user_sql.reset();
      int64_t begin_idx = i;
      int64_t end_idx = std::min(begin_idx + BATCH_SIZE, table_ids.count());
      if (OB_FAIL(construct_fill_user_table_sql(table_ids, begin_idx, end_idx, meta_table_name, user_sql))) {
        SERVER_LOG(WARN, "fail to construct sql", K(i), K(ret));
      } else if (OB_FAIL(fill_table_statistics(user_sql, sql_tenant_id))) {
        SERVER_LOG(WARN, "fail to fill table statistics", K(user_sql), K(sql_tenant_id), K(ret));
      }
    }
  }
  return ret;
}

int ObTenantAllTables::construct_fill_user_table_sql(const ObIArray<uint64_t>& table_ids, int64_t begin_idx,
    int64_t end_idx, const char* meta_table_name, ObSqlString& sql)
{
  int ret = OB_SUCCESS;
  const char* sql_str = "select table_id as table_id, row_count, data_size, data_version, \
                         data_checksum, create_time, time_to_usec(gmt_modified) as update_time \
                         from oceanbase.%s \
                         where partition_id = 0 and role = %d";
  if (OB_FAIL(sql.append_fmt(sql_str, meta_table_name, common::LEADER))) {
    LOG_WARN("fail to append fmt", K(sql_str), K(meta_table_name), K(ret));
  } else if (OB_FAIL(sql.append(" and table_id in ("))) {
    LOG_WARN("fail to append str", K(table_ids), K(ret));
  }
  for (int64_t i = begin_idx; OB_SUCC(ret) && i < end_idx; ++i) {
    if (i == begin_idx) {
      if (OB_FAIL(sql.append_fmt("%lu", table_ids.at(i)))) {
        LOG_WARN("fail to append table id", K(table_ids), K(ret));
      }
    } else {
      if (OB_FAIL(sql.append_fmt(" ,%lu", table_ids.at(i)))) {
        LOG_WARN("fail to append table id", K(table_ids), K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql.append(")"))) {
    LOG_WARN("fail to append str", K(ret));
  }
  return ret;
}

int ObTenantAllTables::get_all_table_ids(ObIArray<uint64_t>& table_ids)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    common::sqlclient::ObMySQLResult* result = NULL;
    uint64_t table_id = OB_INVALID_ID;
    const uint64_t exec_tenant_id = OB_SYS_TENANT_ID;
    const char* sql_str = "select table_id from %s\
                           where tenant_id = %lu and database_id = %lu";
    ObSqlString sql;
    if (OB_FAIL(sql.append_fmt(sql_str, OB_ALL_VIRTUAL_TABLE_TNAME, extract_tenant_id(database_id_), database_id_))) {
      LOG_WARN("fail to append sql", K(sql_str), K(database_id_), K(ret));
    } else if (OB_ISNULL(sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "data member is not init", K(ret));
    } else if (OB_FAIL(sql_proxy_->read(res, exec_tenant_id, sql.ptr()))) {
      SERVER_LOG(WARN, "fail to read result", K(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "result set from read is NULL", K(ret));
    } else { /*do nothing*/
    }

    while (OB_SUCC(ret) && OB_SUCC(result->next())) {
      EXTRACT_INT_FIELD_MYSQL(*result, "table_id", table_id, uint64_t);
      if (OB_FAIL(table_ids.push_back(table_id))) {
        LOG_WARN("fail to push back table_id", K(table_id), K(ret));
      }
    }

    if (OB_LIKELY(OB_ITER_END == ret)) {
      ret = OB_SUCCESS;
    } else {
      SERVER_LOG(WARN, "fail to fill table statstistics", K(ret));
    }
  }
  return ret;
}

int ObTenantAllTables::fill_table_statistics(const ObSqlString& sql, uint64_t sql_tenant_id)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    common::sqlclient::ObMySQLResult* result = NULL;
    if (OB_ISNULL(sql_proxy_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "data member is not init", K(ret));
    } else if (OB_FAIL(sql_proxy_->read(res, sql_tenant_id, sql.ptr()))) {
      SERVER_LOG(WARN, "fail to read result", K(ret), K(sql), K(sql_tenant_id));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "result set from read is NULL", K(ret));
    } else { /*do nothing*/
    }

    uint64_t table_id = OB_INVALID_ID;
    TableStatistics table_stst;
    while (OB_SUCC(ret) && OB_SUCC(result->next())) {
      table_stst.reset();
      EXTRACT_INT_FIELD_MYSQL(*result, "table_id", table_id, uint64_t);
      EXTRACT_INT_FIELD_MYSQL(*result, "row_count", table_stst.row_count_, int64_t);
      EXTRACT_INT_FIELD_MYSQL(*result, "data_size", table_stst.data_size_, int64_t);
      EXTRACT_INT_FIELD_MYSQL(*result, "data_version", table_stst.data_version_, int64_t);
      EXTRACT_INT_FIELD_MYSQL(*result, "data_checksum", table_stst.data_checksum_, int64_t);
      EXTRACT_INT_FIELD_MYSQL(*result, "create_time", table_stst.create_time_, int64_t);
      EXTRACT_INT_FIELD_MYSQL(*result, "update_time", table_stst.update_time_, int64_t);
      if (OB_FAIL(ret)) {
        SERVER_LOG(WARN, "fail to extract field from result", K(ret));
      } else {
        int hash_ret = OB_SUCCESS;
        if (OB_FAIL(tables_statistics_.set_refactored(table_id, table_stst))) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "fail to set hashmap", K(ret), K(hash_ret));
        }
      }
    }
    if (OB_LIKELY(OB_ITER_END == ret)) {
      ret = OB_SUCCESS;
    } else {
      SERVER_LOG(WARN, "fail to fill table statstistics", K(ret));
    }
  }
  return ret;
}
void ObTenantAllTables::reset()
{
  session_ = NULL;
  sql_proxy_ = NULL;
  tenant_id_ = OB_INVALID_ID;
  database_id_ = OB_INVALID_ID;
  table_schemas_.reset();
  table_schema_idx_ = 0;
  seq_values_.reuse();
  tables_statistics_.reuse();
  option_buf_ = NULL;
  ObVirtualTableIterator::reset();
}

int ObTenantAllTables::inner_get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inner_get_next_row())) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "fail to get next row", K(ret), K(cur_row_));
    }
  } else {
    row = &cur_row_;
  }
  return ret;
}

int ObTenantAllTables::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  const ObTableSchema* table_schema = NULL;
  ObObj* cells = NULL;
  const int64_t col_count = output_column_ids_.count();
  ObString database_name;
  if (OB_UNLIKELY(NULL == allocator_ || NULL == schema_guard_ || NULL == session_ ||
                  NULL == (cells = cur_row_.cells_) || NULL == option_buf_ || !is_valid_id(tenant_id_) ||
                  !is_valid_id(database_id_))) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN,
        "data member dosen't init",
        K(ret),
        K(allocator_),
        K(schema_guard_),
        K(session_),
        K(cells),
        K(option_buf_),
        K(tenant_id_),
        K(database_id_));
  } else {
    ObSchemaPrinter schema_printer(*schema_guard_);
    const ObDatabaseSchema* db_schema = NULL;
    if (OB_FAIL(schema_guard_->get_database_schema(database_id_, db_schema))) {
      SERVER_LOG(WARN, "Failed to get database schema", K(ret));
    } else if (OB_ISNULL(db_schema)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "db_schema should not be null", K(ret), K_(database_id));
    } else {
      database_name = db_schema->get_database_name_str();
      TableStatistics tstat;
      bool is_allow = false;
      AutoincKey key;
      ObSessionPrivInfo priv_info;
      do {
        is_allow = true;
        if (OB_UNLIKELY(table_schema_idx_ < 0)) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid table_schema_idx_", K(ret), K(table_schema_idx_));
        } else if (table_schema_idx_ >= table_schemas_.count()) {
          ret = OB_ITER_END;
          table_schema_idx_ = 0;
        } else {
          if (OB_ISNULL(table_schema = table_schemas_.at(table_schema_idx_))) {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "table schema is NULL", K(ret), K(table_schema_idx_), K(tenant_id_), K(database_id_));
          } else {
            uint64_t cell_idx = 0;
            int stat_ret = OB_SUCCESS;
            tstat.reset();
            if (tables_statistics_.created()) {
              stat_ret = tables_statistics_.get_refactored(table_schema->get_table_id(), tstat);
              if (OB_UNLIKELY(OB_SUCCESS != stat_ret && OB_HASH_NOT_EXIST != stat_ret)) {
                ret = stat_ret;
                SERVER_LOG(WARN, "fail to get table stat", K(stat_ret), K(ret), K(table_schema->get_table_id()));
              }
            }
            for (int64_t j = 0; OB_SUCC(ret) && j < col_count; ++j) {
              uint64_t col_id = output_column_ids_.at(j);
              switch (col_id) {
                case DATABASE_ID: {
                  cells[cell_idx].set_int(database_id_);
                  break;
                }
                case TABLE_NAME: {
                  cells[cell_idx].set_varchar(table_schema->get_table_name_str());
                  cells[cell_idx].set_collation_type(
                      ObCharset::get_default_collation(ObCharset::get_default_charset()));
                  break;
                }
                case TABLE_TYPE: {
                  if (OB_MYSQL_SCHEMA_ID == extract_pure_id(table_schema->get_database_id())) {
                    cells[cell_idx].set_varchar(ObString::make_string("BASE TABLE"));
                  } else {
                    cells[cell_idx].set_varchar(
                        ObString::make_string(ob_mysql_table_type_str(table_schema->get_table_type())));
                  }
                  cells[cell_idx].set_collation_type(
                      ObCharset::get_default_collation(ObCharset::get_default_charset()));
                  break;
                }
                case ENGINE: {
                  if (table_schema->is_user_view()) {
                    cells[cell_idx].set_null();
                  } else {
                    cells[cell_idx].set_varchar(ObString::make_string("oceanbase"));
                    cells[cell_idx].set_collation_type(
                        ObCharset::get_default_collation(ObCharset::get_default_charset()));
                  }
                  break;
                }
                case TABLE_VERSION: {
                  if (table_schema->is_user_view()) {
                    cells[cell_idx].set_null();
                  } else {
                    if (OB_SUCCESS == stat_ret) {
                      cells[cell_idx].set_int(tstat.data_version_);
                    } else {
                      cells[cell_idx].set_null();
                    }
                  }
                  break;
                }
                case ROW_FORMAT: {  // use compress function instead
                  if (table_schema->is_user_view()) {
                    cells[cell_idx].set_null();
                  } else {
                    if (ObStoreFormat::is_store_format_valid(table_schema->get_store_format())) {
                      cells[cell_idx].set_varchar(ObString::make_string(
                          ObStoreFormat::get_store_format_name(table_schema->get_store_format())));
                      cells[cell_idx].set_collation_type(
                          ObCharset::get_default_collation(ObCharset::get_default_charset()));
                    } else {
                      cells[cell_idx].set_null();
                    }
                  }
                  break;
                }
                case ROWS: {
                  if (table_schema->is_user_view()) {
                    cells[cell_idx].set_null();
                  } else {
                    if (OB_SUCCESS == stat_ret) {
                      cells[cell_idx].set_int(tstat.row_count_);
                    } else {
                      cells[cell_idx].set_null();
                    }
                  }
                  break;
                }
                case AVG_ROW_LENGTH: {
                  if (table_schema->is_user_view()) {
                    cells[cell_idx].set_null();
                  } else {
                    if (OB_SUCCESS == stat_ret) {
                      if (0 == tstat.row_count_) {
                        cells[cell_idx].set_int(0);
                      } else {
                        cells[cell_idx].set_int(tstat.data_size_ / tstat.row_count_);
                      }
                    } else {
                      cells[cell_idx].set_null();
                    }
                  }
                  break;
                }
                case DATA_LENGTH: {
                  if (table_schema->is_user_view()) {
                    cells[cell_idx].set_null();
                  } else {
                    if (OB_SUCCESS == stat_ret) {
                      cells[cell_idx].set_int(tstat.data_size_);
                    } else {
                      cells[cell_idx].set_null();
                    }
                  }
                  break;
                }
                case MAX_DATA_LENGTH: {  // TODO
                  cells[cell_idx].set_null();
                  break;
                }
                case INDEX_LENGTH: {  // TODO
                  cells[cell_idx].set_null();
                  break;
                }
                case DATA_FREE: {  // TODO
                  cells[cell_idx].set_null();
                  break;
                }
                case AUTO_INCREMENT: {
                  if (table_schema->is_user_view()) {
                    cells[cell_idx].set_null();
                  } else {
                    uint64_t auto_increment = 0;
                    int err = OB_SUCCESS;
                    key.reset();
                    key.tenant_id_ = table_schema->get_tenant_id();
                    key.table_id_ = table_schema->get_table_id();
                    key.column_id_ = table_schema->get_autoinc_column_id();
                    err = seq_values_.get_refactored(key, auto_increment);
                    if (OB_UNLIKELY(OB_SUCCESS != err && OB_HASH_NOT_EXIST != err)) {
                      ret = OB_ERR_UNEXPECTED;
                      SERVER_LOG(WARN, "failed to get seq value", K(ret));
                    } else {
                      if (OB_HASH_NOT_EXIST == err) {
                        cells[cell_idx].set_null();
                      } else {
                        cells[cell_idx].set_uint64(auto_increment);
                      }
                    }
                  }
                  break;
                }
                case CREATE_TIME: {
                  if (table_schema->is_user_view()) {
                    cells[cell_idx].set_null();
                  } else {
                    if (OB_SUCCESS == stat_ret) {
                      cells[cell_idx].set_timestamp(tstat.create_time_);
                    } else {
                      cells[cell_idx].set_null();
                    }
                  }
                  break;
                }
                case UPDATE_TIME: {
                  if (table_schema->is_user_view()) {
                    cells[cell_idx].set_null();
                  } else {
                    if (OB_SUCCESS == stat_ret) {
                      cells[cell_idx].set_timestamp(tstat.update_time_);
                    } else {
                      cells[cell_idx].set_null();
                    }
                  }
                  break;
                }
                case CHECK_TIME: {
                  cells[cell_idx].set_null();
                  break;
                }
                case COLLATION: {
                  if (table_schema->is_user_view()) {
                    cells[cell_idx].set_null();
                  } else {
                    if (CS_TYPE_INVALID != table_schema->get_collation_type()) {
                      cells[cell_idx].set_varchar(
                          ObString::make_string(ObCharset::collation_name(table_schema->get_collation_type())));
                      cells[cell_idx].set_collation_type(
                          ObCharset::get_default_collation(ObCharset::get_default_charset()));
                    } else {
                      cells[cell_idx].set_null();
                    }
                  }
                  break;
                }
                case CHECKSUM: {
                  if (table_schema->is_user_view()) {
                    cells[cell_idx].set_null();
                  } else {
                    if (OB_SUCCESS == stat_ret) {
                      cells[cell_idx].set_int(tstat.data_checksum_);
                    } else {
                      cells[cell_idx].set_null();
                    }
                  }
                  break;
                }
                case CREATE_OPTIONS: {
                  int64_t pos = 0;
                  if (table_schema->is_user_view()) {
                    cells[cell_idx].set_null();
                  } else {
                    if (OB_FAIL(schema_printer.print_table_definition_table_options(
                            *table_schema, option_buf_, MAX_TABLE_STATUS_CREATE_OPTION_LENGTH, pos, true))) {
                      SERVER_LOG(WARN,
                          "print table definition table options failed",
                          K(option_buf_),
                          K(MAX_TABLE_STATUS_CREATE_OPTION_LENGTH),
                          K(pos));
                    } else if (strlen(option_buf_) > 0) {
                      cells[cell_idx].set_varchar(ObString::make_string(option_buf_));
                      cells[cell_idx].set_collation_type(
                          ObCharset::get_default_collation(ObCharset::get_default_charset()));
                    } else {
                      cells[cell_idx].set_null();
                    }
                  }
                  break;
                }
                case COMMENT: {
                  // system view and base table show comments as normal, for user views, it only shows "VIEW"
                  if (table_schema->is_user_view()) {
                    cells[cell_idx].set_varchar(ObString::make_string("VIEW"));
                    cells[cell_idx].set_collation_type(
                        ObCharset::get_default_collation(ObCharset::get_default_charset()));
                  } else {
                    if (table_schema->get_comment_str().length() > 0) {
                      cells[cell_idx].set_varchar(table_schema->get_comment_str());
                      cells[cell_idx].set_collation_type(
                          ObCharset::get_default_collation(ObCharset::get_default_charset()));
                    } else {
                      cells[cell_idx].set_varchar(ObString::make_string(""));
                      cells[cell_idx].set_collation_type(
                          ObCharset::get_default_collation(ObCharset::get_default_charset()));
                    }
                  }
                  break;
                }
                default: {
                  ret = OB_ERR_UNEXPECTED;
                  SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx), K(j), K(output_column_ids_), K(col_id));
                  break;
                }
              }
              if (OB_SUCC(ret)) {
                cell_idx++;
              }
            }
            if (OB_SUCC(ret)) {
              // skip table that is neither normal table, system table nor view table
              if (!table_schema->is_user_table() && !table_schema->is_sys_table() && !table_schema->is_view_table() &&
                  OB_INFORMATION_SCHEMA_ID != extract_pure_id(table_schema->get_database_id()) &&
                  OB_MYSQL_SCHEMA_ID != extract_pure_id(table_schema->get_database_id())) {
                is_allow = false;
              } else {
                priv_info.reset();
                session_->get_session_priv_info(priv_info);
                if (OB_FAIL(schema_guard_->check_table_show(
                        priv_info, database_name, table_schema->get_table_name_str(), is_allow))) {
                  SERVER_LOG(WARN, "check show table priv failed", K(ret));
                }
              }
            }
          }
          table_schema_idx_++;
        }
      } while (!is_allow && OB_SUCCESS == ret);
    }
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase
