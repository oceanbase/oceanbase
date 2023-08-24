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
#include "common/ob_store_format.h"
#include "observer/ob_sql_client_decorator.h"
#include "deps/oblib/src/lib/mysqlclient/ob_mysql_result.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace observer
{

#define TABLE_STATUS_SQL  "select /*+ leading(a) no_use_nl(ts)*/" \
                    "cast( coalesce(ts.row_cnt,0) as unsigned) as table_rows," \
                    "cast( coalesce(ts.data_size,0) as unsigned) as data_length," \
                    "cast(a.gmt_create as datetime) as create_time," \
                    "cast(a.gmt_modified as datetime) as update_time " \
                    "from " \
                    "(" \
                    "select tenant_id," \
                           "database_id," \
                           "table_id," \
                           "table_name," \
                           "table_type," \
                           "gmt_create," \
                           "gmt_modified " \
                    "from oceanbase.__all_table) a " \
                    "join oceanbase.__all_database b " \
                    "on a.database_id = b.database_id " \
                    "and a.tenant_id = b.tenant_id " \
                    "left join (" \
                      "select tenant_id," \
                             "table_id," \
                             "row_cnt," \
                             "avg_row_len," \
                             "row_cnt * avg_row_len as data_size " \
                      "from oceanbase.__all_table_stat " \
                      "where partition_id = -1 or partition_id = table_id) ts " \
                    "on a.table_id = ts.table_id " \
                    "and a.tenant_id = ts.tenant_id " \
                    "and a.table_type in (0, 1, 2, 3, 4, 14) " \
                    "and b.database_name != '__recyclebin' " \
                    "and b.in_recyclebin = 0 " \
                    "and 0 = sys_privilege_check('table_acc', effective_tenant_id(), b.database_name, a.table_name) " \
                    "where a.table_id = %ld "

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
{
}

ObTenantAllTables::~ObTenantAllTables()
{
}

int ObTenantAllTables::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == schema_guard_
                  || OB_INVALID_ID == tenant_id_
                  || NULL == allocator_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "data member doesn't init", K(ret), K(schema_guard_), K(tenant_id_), K(allocator_));
  } else if (OB_UNLIKELY(NULL == (option_buf_ = static_cast<char *>(allocator_->alloc(MAX_TABLE_STATUS_CREATE_OPTION_LENGTH))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    SERVER_LOG(ERROR, "fail to alloc memory", K(ret));
  } else {
    //get database_id
    ObRowkey start_key;
    ObRowkey end_key;
    for (int64_t i = 0; OB_SUCC(ret) && !is_valid_id(database_id_) && i < key_ranges_.count(); ++i) {
      start_key.reset();
      end_key.reset();
      start_key = key_ranges_.at(i).start_key_;
      end_key = key_ranges_.at(i).end_key_;
      const ObObj *start_key_obj_ptr = start_key.get_obj_ptr();
      const ObObj *end_key_obj_ptr = end_key.get_obj_ptr();
      if (start_key.get_obj_cnt() > 0 && start_key.get_obj_cnt() == end_key.get_obj_cnt()) {
        if (OB_UNLIKELY(NULL == start_key_obj_ptr || NULL == end_key_obj_ptr)) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(ERROR, "key obj ptr is NULL", K(ret), K(start_key_obj_ptr), K(end_key_obj_ptr));
        } else if (start_key_obj_ptr[0] == end_key_obj_ptr[0]
                   && ObIntType == start_key_obj_ptr[0].get_type()) {
          database_id_ = start_key_obj_ptr[0].get_int();
        }
      }
    }//for
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(!is_valid_id(database_id_))) {
        // FIXME(tingshuai.yts):暂时定为显示该错误信息，只有直接查询该虚拟表才可能出现
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "select a table which is used for show clause");
      } else {
        if (OB_FAIL(schema_guard_->get_table_schemas_in_database(tenant_id_,
                                                               database_id_,
                                                               table_schemas_))) {
          SERVER_LOG(WARN, "fail to get table schemas in database", K(ret), K(tenant_id_), K(database_id_));
        } else if (table_schemas_.empty()) {
          // do nothing
        } else if (OB_FAIL(seq_values_.create(FETCH_SEQ_NUM_ONCE,
                                              ObModIds::OB_AUTOINCREMENT,
                                              ObModIds::OB_AUTOINCREMENT))) {
          SERVER_LOG(WARN, "failed to create seq values ObHashMap", K(ret), LITERAL_K(FETCH_SEQ_NUM_ONCE));
        } else if (OB_FAIL(tables_statistics_.create(table_schemas_.count(), "TableStat", "TableStat"))) {
          LOG_WARN("failed to create table stat ObHashMap", K(ret), K(table_schemas_.count()));
        } else {
          bool fetch_inc = false;
          bool fetch_stat = false;
          for (int64_t i = 0; OB_SUCC(ret) && (!fetch_inc || !fetch_stat) && i < output_column_ids_.count(); ++i) {
            uint64_t col_id = output_column_ids_.at(i);
            switch(col_id) {
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
                  if (OB_FAIL(get_table_stats())) {
                    LOG_WARN("failed to fetch table stats", K(ret));
                  }
                  fetch_stat = true;
                }
                break;
              }
              default: {
                break;//do nothing
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
  ObArray<AutoincKey> order_autokeys;
  ObArray<AutoincKey> noorder_autokeys;
  ObArray<int64_t> order_autoinc_versions;
  ObArray<int64_t> noorder_autoinc_versions;
  int64_t autoinc_version = OB_INVALID_VERSION;
  AutoincKey key;
  for(int64_t i = 0; OB_SUCC(ret) && i < table_schemas_.count(); ++i) {
    const ObTableSchema *table_schema = table_schemas_.at(i);
    if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "table schema is NULL", K(ret), K(i), K(tenant_id_), K(database_id_));
    } else if (table_schema->get_autoinc_column_id() != 0) {
      key.reset();
      key.tenant_id_ = table_schema->get_tenant_id();//always same as tenant_id_
      key.table_id_  = table_schema->get_table_id();
      key.column_id_ = table_schema->get_autoinc_column_id();
      autoinc_version = table_schema->get_truncate_version();
      if (table_schema->is_order_auto_increment_mode()) {
        if (OB_FAIL(order_autokeys.push_back(key))) {
          SERVER_LOG(WARN, "failed to push back AutoincKey", K(ret));
        } else if (OB_FAIL(order_autoinc_versions.push_back(autoinc_version))) {
          SERVER_LOG(WARN, "failed to push back autoinc_version", KR(ret), K(key));
        }
      } else if (!table_schema->is_order_auto_increment_mode()) {
        if (OB_FAIL(noorder_autokeys.push_back(key))) {
          SERVER_LOG(WARN, "failed to push back AutoincKey is order", K(ret));
        } else if (OB_FAIL(noorder_autoinc_versions.push_back(autoinc_version))) {
          SERVER_LOG(WARN, "failed to push back autoinc_version", KR(ret), K(key));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (order_autokeys.count() != order_autoinc_versions.count()
        || noorder_autokeys.count() != noorder_autoinc_versions.count()) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "autokeys count is not equal to truncate versions count", KR(ret), K(order_autokeys.count()), K(order_autoinc_versions.count()),
                                                                                 K(noorder_autokeys.count()), K(noorder_autoinc_versions.count()));
    } else if (OB_FAIL(share::ObAutoincrementService::get_instance().get_sequence_values(
              tenant_id_, order_autokeys, noorder_autokeys,
              order_autoinc_versions, noorder_autoinc_versions, seq_values_))) {
      SERVER_LOG(WARN, "failed to get sequence value", K(ret));
    }
  }
  return ret;
}

int ObTenantAllTables::get_table_stats()
{
  int ret = OB_SUCCESS;
  for(int64_t i = 0; OB_SUCC(ret) && i < table_schemas_.count(); ++i) {
    const ObTableSchema *table_schema = table_schemas_.at(i);
    if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "table schema is NULL", K(ret), K(i), K(tenant_id_), K(database_id_));
    } else {
      TableStatistics tab_stat;
      ObSQLClientRetryWeak sql_client_retry_weak(sql_proxy_, false, OB_INVALID_TIMESTAMP, false);
      ObSqlString sql;
      if (OB_ISNULL(session_) || OB_ISNULL(sql_proxy_) || OB_ISNULL(sql_proxy_->get_pool())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(ret), K(session_), K(sql_proxy_));
      } else if (OB_FAIL(sql.append_fmt(TABLE_STATUS_SQL, table_schema->get_table_id()))) {
        LOG_WARN("failed to append sql", K(ret));
      } else {
        SMART_VAR(ObMySQLProxy::MySQLResult, res) {
          sqlclient::ObMySQLResult *result = NULL;
          if (OB_FAIL(sql_client_retry_weak.read(res, tenant_id_, sql.ptr()))) {
            LOG_WARN("execute sql failed", "sql", sql.ptr(), K(ret));
          } else if (OB_ISNULL(result = res.get_result())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("fail to execute ", "sql", sql.ptr(), K(ret));
          }
          while (OB_SUCC(ret)) {
            if (OB_FAIL(result->next())) {
              if (OB_ITER_END == ret) {
                ret = OB_SUCCESS;
                break;
              } else {
                LOG_WARN("get next row failed", K(ret));
              }
            } else {
              EXTRACT_UINT_FIELD_TO_CLASS_MYSQL(*result, table_rows, tab_stat, int64_t);
              EXTRACT_UINT_FIELD_TO_CLASS_MYSQL(*result, data_length, tab_stat, int64_t);
              EXTRACT_LAST_DDL_TIME_FIELD_TO_INT_MYSQL(*result, create_time, tab_stat, int64_t);
              EXTRACT_LAST_DDL_TIME_FIELD_TO_INT_MYSQL(*result, update_time, tab_stat, int64_t);
            }
          }
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(tables_statistics_.set_refactored(table_schema->get_table_id(), tab_stat))) {
            if (ret == OB_HASH_EXIST) {
              ret = OB_SUCCESS;
              LOG_WARN("the table stat is already fetched", K(table_schema->get_table_id()), K(tab_stat));
            } else {
              LOG_WARN("failed to set table stat", K(ret));
            }
          }
        }
        LOG_TRACE("succeed to get table stats", K(table_schema->get_table_id()), K(tab_stat));
      }
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

int ObTenantAllTables::inner_get_next_row(common::ObNewRow *&row)
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
  const ObTableSchema *table_schema = NULL;
  ObObj *cells = NULL;
  const int64_t col_count = output_column_ids_.count();
  ObString database_name;
  if (OB_UNLIKELY(NULL == allocator_
                  || NULL == schema_guard_
                  || NULL == session_
                  || NULL == (cells = cur_row_.cells_)
                  || NULL == option_buf_
                  || !is_valid_id(tenant_id_)
                  || !is_valid_id(database_id_))) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "data member dosen't init", K(ret), K(allocator_), K(schema_guard_),
               K(session_), K(cells), K(option_buf_), K(tenant_id_), K(database_id_));
  } else {
    ObSchemaPrinter schema_printer(*schema_guard_);
    const ObDatabaseSchema *db_schema = NULL;
    if (OB_FAIL(schema_guard_->get_database_schema(tenant_id_, database_id_, db_schema))) {
      SERVER_LOG(WARN, "Failed to get database schema", K(ret), K_(tenant_id));
    } else if (OB_ISNULL(db_schema)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "db_schema should not be null", K(ret), K_(tenant_id), K_(database_id));
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
              switch(col_id) {
                case DATABASE_ID: {
                  cells[cell_idx].set_int(database_id_);
                  break;
                }
                case TABLE_NAME: {
                  cells[cell_idx].set_varchar(table_schema->get_table_name_str());
                  cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                  break;
                }
                case TABLE_TYPE: {
                  if (is_mysql_database_id(table_schema->get_database_id())) {
                    cells[cell_idx].set_varchar(table_schema->is_user_view() ?
                    ObString::make_string("VIEW") : ObString::make_string("BASE TABLE"));
                  } else {
                    cells[cell_idx].set_varchar(ObString::make_string(ob_mysql_table_type_str(table_schema->get_table_type())));
                  }
                  cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                  break;
                }
                case ENGINE: {
                  if (table_schema->is_user_view()) {
                    cells[cell_idx].set_null();
                  } else {
                    cells[cell_idx].set_varchar(ObString::make_string("oceanbase"));
                    cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                  }
                  break;
                }
                case TABLE_VERSION: {
                  if (table_schema->is_user_view()) {
                    cells[cell_idx].set_null();
                  } else {
                    if (OB_SUCCESS == stat_ret) {
                      cells[cell_idx].set_uint64(tstat.data_version_);
                    } else {
                      cells[cell_idx].set_null();
                    }
                  }
                  break;
                }
                case ROW_FORMAT: {//use compress function instead
                  if (table_schema->is_user_view()) {
                    cells[cell_idx].set_null();
                  } else {
                    if (ObStoreFormat::is_store_format_valid(table_schema->get_store_format())) {
                      cells[cell_idx].set_varchar(ObString::make_string(ObStoreFormat::get_store_format_name(table_schema->get_store_format())));
                      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
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
                case MAX_DATA_LENGTH: {//TODO
                  cells[cell_idx].set_null();
                  break;
                }
                case INDEX_LENGTH: {//TODO
                  cells[cell_idx].set_null();
                  break;
                }
                case DATA_FREE: {//TODO
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
                    key.table_id_  = table_schema->get_table_id();
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
                      cells[cell_idx].set_varchar(ObString::make_string(ObCharset::collation_name(table_schema->get_collation_type())));
                      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
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
                  memset(option_buf_, 0, MAX_TABLE_STATUS_CREATE_OPTION_LENGTH);
                  if (table_schema->is_user_view()) {
                    cells[cell_idx].set_null();
                  } else {
                    if (OB_FAIL(schema_printer.print_table_definition_table_options(*table_schema,
                                                                                    option_buf_,
                                                                                    MAX_TABLE_STATUS_CREATE_OPTION_LENGTH,
                                                                                    pos,
                                                                                    true))) {
                      SERVER_LOG(WARN, "print table definition table options failed", K(option_buf_), K(MAX_TABLE_STATUS_CREATE_OPTION_LENGTH), K(pos));
                    } else if (strlen(option_buf_) > 0) {
                      cells[cell_idx].set_varchar(ObString::make_string(option_buf_));
                      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                    } else {
                      cells[cell_idx].set_null();
                    }
                  }
                  break;
                }
                case COMMENT: {
                  //对于comment列mysql中system view和base table均正常显示;而user view仅显示VIEW即可
                  if (table_schema->is_user_view()) {
                    cells[cell_idx].set_varchar(ObString::make_string("VIEW"));
                    cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                  } else {
                    if (table_schema->get_comment_str().length() > 0) {
                      cells[cell_idx].set_varchar(table_schema->get_comment_str());
                      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                    } else {
                      cells[cell_idx].set_varchar(ObString::make_string(""));
                      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
                    }
                  }
                  break;
                }
                default: {
                  ret = OB_ERR_UNEXPECTED;
                  SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx),
                             K(j), K(output_column_ids_), K(col_id));
                  break;
                }
              }
              if (OB_SUCC(ret)) {
                cell_idx++;
              }
            }
            if (OB_SUCC(ret)) {
              // skip table that is neither normal table, system table nor view table
              if (!table_schema->is_user_table()
                  && !table_schema->is_sys_table()
                  && !table_schema->is_view_table()
                  && !table_schema->is_external_table()
                  && !is_information_schema_database_id(table_schema->get_database_id())
                  && !is_mysql_database_id(table_schema->get_database_id())) {
                is_allow = false;
              } else {
                priv_info.reset();
                session_->get_session_priv_info(priv_info);
                if (OB_FAIL(schema_guard_->check_table_show(priv_info, database_name,
                                                            table_schema->get_table_name_str(), is_allow))) {
                  SERVER_LOG(WARN, "check show table priv failed", K(ret));
                }
              }
            }
          }
          table_schema_idx_++;
        }
      } while(!is_allow && OB_SUCCESS == ret);
    }
  }
  return ret;
}

}/* ns observer*/
}/* ns oceanbase */
