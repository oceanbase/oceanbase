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

#define USING_LOG_PREFIX STORAGE

#include "ob_all_virtual_ddl_diagnose_info.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_ddl_common.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_ddl_sim_point_define.h"
#include "deps/oblib/src/lib/string/ob_string.h"
#include "deps/oblib/src/lib/utility/ob_macro_utils.h"
#include "deps/oblib/src/lib/alloc/alloc_assist.h"


namespace oceanbase
{
using namespace storage;
using namespace common;
using namespace share;

namespace observer
{

int ObAllVirtualDDLDiagnoseInfo::init(ObMySQLProxy *sql_proxy)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "ObAllVirtualDDLDiagnoseInfo has been inited", K(ret));
  } else if (OB_ISNULL(sql_proxy) || !sql_proxy->is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "sql proxy is NULL or not inited", K(ret));
  } else if (FALSE_IT(sql_proxy_ = sql_proxy)) {
  } else if (OB_FAIL(process())) {
    SERVER_LOG(WARN, "Fail to process!", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObAllVirtualDDLDiagnoseInfo::process()
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 32> tenant_ids;
  share::schema::ObSchemaGetterGuard schema_guard;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid GCTX", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(tenant_ids))) {
    LOG_WARN("fail to get tenant ids", KR(ret));
  } else if (OB_FAIL(tenant_ids.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("empty tenant ids", KR(ret));
  } else {
    ObSqlString scan_sql;
    if (OB_FAIL(scan_sql.assign_fmt("SELECT task_id, tenant_id, object_id, ddl_type, execution_id, time_to_usec(gmt_create) as GMT_CREATE, time_to_usec(gmt_modified) as GMT_MODIFIED FROM %s "
                                    "WHERE ddl_type IN (5, 10, 1001, 1002, 1004, 1005, 1010) ",
                                    OB_ALL_VIRTUAL_DDL_TASK_STATUS_TNAME))) {
      LOG_WARN("failed to assign sql", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
        const uint64_t tenant_id = tenant_ids.at(i);
        if (OB_FAIL(scan_sql.append_fmt("UNION ALL "
                                      "SELECT task_id, tenant_id, object_id, ddl_type, NULL as execution_id, GMT_CREATE, GMT_MODIFIED FROM "
                                      "(SELECT task_id, tenant_id, object_id, ddl_type, time_to_usec(gmt_create) as GMT_CREATE, time_to_usec(gmt_modified) as GMT_MODIFIED "
                                      "FROM %s "
                                      "WHERE user_message = 'Successful ddl' "
                                      "AND TENANT_ID = %ld "
                                      "AND target_object_id = -1 "
                                      "AND ddl_type IN (5, 10, 1001, 1002, 1004, 1005, 1010) "
                                      "ORDER BY task_id DESC "
                                      "LIMIT 100) AS subquery ",
                                      OB_ALL_VIRTUAL_DDL_ERROR_MESSAGE_TNAME,
                                      tenant_id))) {
          LOG_WARN("failed to assign sql", K(ret));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(scan_sql.append_fmt("ORDER BY task_id DESC, tenant_id DESC"))) {
        LOG_WARN("failed to assign sql", K(ret));
      } else if (OB_FAIL(collect_ddl_info(scan_sql))) {
        LOG_WARN("failed to collect ddl info", K(ret));
      }
    }
  }
  return ret;
}

int ObAllVirtualDDLDiagnoseInfoI1::process()
{
  int ret = OB_SUCCESS;
  ObSqlString scan_sql;
  ObSqlString task_id;
  int index_count = 0;
  if (OB_FAIL(set_index_ids(key_ranges_))) {
    LOG_WARN("failed to set index ids", K(ret));
  } else {
    int64_t index_id = -1;
    for (int64_t i = 0; OB_SUCC(ret) && i < get_index_ids().count(); ++i) {
      index_id = get_index_ids().at(i);
      if (0 < index_id) {
        if (index_count == 0) {
          if (OB_FAIL(task_id.assign_fmt("(%ld", index_id))) {
            LOG_WARN("failed to assign sql", K(ret));
          } else {
            index_count++;
          }
        } else {
          if (OB_FAIL(task_id.append_fmt(", %ld", index_id))) {
            LOG_WARN("failed to assign sql", K(ret));
          } else {
            index_count++;
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (index_count <= 0) {
    } else if (OB_FAIL(task_id.append_fmt(") "))) {
      LOG_WARN("failed to assign sql", K(ret));
    } else if (OB_FAIL(scan_sql.assign_fmt("SELECT task_id, tenant_id, object_id, ddl_type, execution_id, time_to_usec(gmt_create) as GMT_CREATE, time_to_usec(gmt_modified) as GMT_MODIFIED "
                                          "FROM %s WHERE task_id in %s "
                                          "UNION "
                                          "SELECT task_id, tenant_id, object_id, ddl_type, NULL AS execution_id, time_to_usec(gmt_create) as GMT_CREATE, time_to_usec(gmt_modified) as GMT_MODIFIED "
                                          "FROM %s WHERE user_message = 'Successful ddl' AND task_id in %s "
                                          "ORDER BY task_id DESC, tenant_id DESC ",
                                          OB_ALL_VIRTUAL_DDL_TASK_STATUS_TNAME,
                                          task_id.ptr(),
                                          OB_ALL_VIRTUAL_DDL_ERROR_MESSAGE_TNAME,
                                          task_id.ptr()))) {
      LOG_WARN("failed to assign sql", K(ret));
    } else if (OB_FAIL(collect_ddl_info(scan_sql))) {
      LOG_WARN("failed to collect ddl info", K(ret));
    }
  }
  return ret;
}

int ObAllVirtualDDLDiagnoseInfoI1::inner_open()
{
  return set_index_ids(key_ranges_);
}

int ObAllVirtualDDLDiagnoseInfo::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAllVirtualCompactionDiagnoseInfo has not been inited", K(ret));
  } else if (OB_FAIL(get_next_diagnose_info_row())) {
    if (OB_ITER_END != ret) {
      LOG_WARN("Fail to get next diagnose info row", K(ret));
    }
  } else if (OB_FAIL(fill_cells())) {
    LOG_WARN("Fail to fill cells", K(ret), K(value_));
  } else {
    row = &cur_row_;
  }
  return ret;
}

int ObAllVirtualDDLDiagnoseInfo::fill_cells()
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
    uint64_t col_id = output_column_ids_.at(i);
    switch (col_id) {
      case OB_APP_MIN_COLUMN_ID:
        // tenant_id
        cur_row_.cells_[i].set_int(value_.tenant_id_);
        break;
      case OB_APP_MIN_COLUMN_ID + 1:
        // ddl_task_id
        cur_row_.cells_[i].set_int(value_.ddl_task_id_);
        break;
      case OB_APP_MIN_COLUMN_ID + 2:
        // object_id
        cur_row_.cells_[i].set_int(value_.object_id_);
        break;
      case OB_APP_MIN_COLUMN_ID + 3:
        // op_type
        cur_row_.cells_[i].set_varchar(value_.op_name_);
        cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      case OB_APP_MIN_COLUMN_ID + 4:
        // create_time
        cur_row_.cells_[i].set_timestamp(value_.start_time_);
        break;
      case OB_APP_MIN_COLUMN_ID + 5:
        // finish_time
        cur_row_.cells_[i].set_timestamp(value_.finish_time_);
        break;
      case OB_APP_MIN_COLUMN_ID + 6:
        // diagnose_info
        cur_row_.cells_[i].set_varchar(message_);
        cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid col_id", K(ret), K(col_id));
        break;
    }
  }
  return ret;
}

int ObAllVirtualDDLDiagnoseInfo::get_next_diagnose_info_row()
{
  int ret = OB_SUCCESS;
  bool is_valid_diagnose = false;
  ObSqlMonitorStats sql_monitor_stats;
  while (OB_SUCC(ret)) {
    if (idx_ >= diagnose_values_.count() && OB_FAIL(collect_task_info())) {
      if (OB_ITER_END == ret) {
        break;
      } else {
        LOG_WARN("failed to collect task info", K(ret));
      }
    }
    while (OB_SUCC(ret) && idx_ < diagnose_values_.count()) {
      value_ = diagnose_values_.at(idx_);
      idx_++;
      message_[0] = '\0';
      pos_ = 0;
      diagnose_info_.reuse();
      sql_monitor_stats.reuse();
      if (OB_FAIL(sql_monitor_stats.init(value_.tenant_id_, value_.ddl_task_id_, value_.ddl_type_))) {
        LOG_WARN("failed to init sql monitor stats", K(ret), K(value_.tenant_id_), K(value_.ddl_task_id_), K(value_.ddl_type_));
      } else if (OB_FAIL(diagnose_info_.init(value_.tenant_id_, value_.ddl_task_id_, value_.ddl_type_, value_.execution_id_))) {
        LOG_WARN("failed to init sql monitor stats", K(ret), K(value_.tenant_id_), K(value_.ddl_task_id_), K(value_.ddl_type_));
      } else if (OB_FAIL(sql_monitor_stats_collector_.get_next_sql_plan_monitor_stat(sql_monitor_stats))) {
        LOG_WARN("failed to collect sql monitor data", K(ret));
      } else if (OB_FAIL(diagnose_info_.diagnose(sql_monitor_stats))) {
        if (OB_EMPTY_RESULT == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to collect sql monitor data", K(ret));
        }
      } else if (OB_FAIL(databuff_printf(message_, common::OB_DIAGNOSE_INFO_LENGTH, pos_, "%s", diagnose_info_.get_diagnose_info()))) {
        LOG_WARN("failed to print diagnose info", K(ret));
      } else {
        is_valid_diagnose = true;
        break;
      }
    }
    if (OB_SUCC(ret) && is_valid_diagnose) {
      break;
    }
  }
  return ret;
}

int ObAllVirtualDDLDiagnoseInfo::collect_ddl_info(const ObSqlString &scan_sql)
{
  int ret = OB_SUCCESS;
  sqlclient::ObMySQLResult *scan_result = nullptr;
  if (!scan_sql.is_valid() || OB_ISNULL(sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("assign get invalid argument", K(ret), K(scan_sql), KP(sql_proxy_));
  } else if (OB_FAIL(sql_proxy_->read(ddl_scan_result_, common::OB_SYS_TENANT_ID, scan_sql.ptr()))) {
    LOG_WARN("fail to execute sql", K(ret), K(scan_sql));
  } else {
    sql_result_iter_end_ = false;
  }
  return ret;
}

int ObAllVirtualDDLDiagnoseInfo::collect_task_info()
{
  int ret = OB_SUCCESS;
  idx_ = 0;
  diagnose_values_.reset();
  value_.reset();
  sqlclient::ObMySQLResult *scan_result = nullptr;
  if (sql_result_iter_end_) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(scan_result = ddl_scan_result_.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, query result must not be NULL", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < MAX_SQL_MONITOR_BATCH_SIZE; ++i) {
      if (OB_FAIL(scan_result->next())) {
        if (OB_ITER_END == ret) {
          sql_result_iter_end_ = true;
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next row", K(ret));
        }
      } else {
        int32_t optype = 0;
        EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(*scan_result, "execution_id", value_.execution_id_, int64_t, true/*skip_null_error*/, true/*skip_column_error*/, -2/*default_value*/);
        EXTRACT_INT_FIELD_MYSQL(*scan_result, "tenant_id", value_.tenant_id_, uint64_t);
        EXTRACT_INT_FIELD_MYSQL(*scan_result, "task_id", value_.ddl_task_id_, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*scan_result, "object_id", value_.object_id_, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*scan_result, "GMT_CREATE", value_.start_time_, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*scan_result, "GMT_MODIFIED", value_.finish_time_, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*scan_result, "ddl_type", optype, int32_t);
        value_.ddl_type_ = static_cast<share::ObDDLType>(optype);
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to get mysql field result", K(ret));
        } else if (OB_FAIL(databuff_printf(value_.op_name_, common::MAX_LONG_OPS_NAME_LENGTH, "%s",  share::get_ddl_type(value_.ddl_type_)))) {
          LOG_WARN("failed to print ddl type", K(ret), K(value_.ddl_type_));
        } else if (OB_FAIL(diagnose_values_.push_back(value_))) {
          LOG_WARN("failed to push back value", K(ret), K(value_));
        } else if (OB_FAIL(sql_monitor_stats_collector_.scan_task_id_.push_back(value_.ddl_task_id_))) {
          LOG_WARN("failed to push back task id", K(ret), K(value_.ddl_task_id_));
        } else if (OB_FAIL(sql_monitor_stats_collector_.scan_tenant_id_.push_back(value_.tenant_id_))) {
          LOG_WARN("failed to push back tenant id", K(ret), K(value_.tenant_id_));
        }
      }
    }
    if (OB_SUCC(ret) && diagnose_values_.count() > 0) {
      if (OB_FAIL(sql_monitor_stats_collector_.init(sql_proxy_))) {
        LOG_WARN("failed to get sql monitor stats batch", K(ret));
      } else if (OB_FAIL(collect_task_gmt_create_time())) {
        LOG_WARN("failed to collect task gmt create time", K(ret));
      }
    }
  }
  return ret;
}

int ObAllVirtualDDLDiagnoseInfo::collect_task_gmt_create_time()
{
  int ret = OB_SUCCESS;
  if (diagnose_values_.count() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(diagnose_values_.count()));
  } else {
    ObSqlString scan_sql;
    ObSqlString cond_sql;
    for (int64 i = 0; OB_SUCC(ret) && i < diagnose_values_.count(); i++) {
      if (i == 0 && OB_FAIL(cond_sql.assign_fmt("(tenant_id = %ld AND table_id = %ld)", diagnose_values_.at(i).tenant_id_, diagnose_values_.at(i).object_id_))) {
        LOG_WARN("failed to assign sql", K(ret));
      } else if (i > 0 && OB_FAIL(cond_sql.append_fmt(" OR (tenant_id = %ld AND table_id = %ld)", diagnose_values_.at(i).tenant_id_, diagnose_values_.at(i).object_id_))) {
        LOG_WARN("failed to append sql", K(ret));
      }
    }
    sqlclient::ObMySQLResult *scan_result = nullptr;
    uint64_t index = 0;
    SMART_VAR(ObMySQLProxy::MySQLResult, scan_res) {
      if (OB_FAIL(scan_sql.assign_fmt("SELECT table_id, tenant_id, TIME_TO_USEC(gmt_create) as GMT_CREATE "
                                      "FROM ( "
                                        "SELECT table_id, tenant_id, gmt_create, "
                                        "ROW_NUMBER() OVER (PARTITION BY tenant_id, table_id ORDER BY gmt_create) AS rn "
                                        "FROM %s WHERE %s"
                                      ") sub "
                                      "WHERE rn = 1",
                                     OB_ALL_VIRTUAL_TABLE_HISTORY_TNAME, cond_sql.ptr()))) {
        LOG_WARN("failed to assign sql", K(ret));
      } else if (!scan_sql.is_valid() || OB_ISNULL(sql_proxy_)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", K(ret), K(scan_sql), KP(sql_proxy_));
      } else if (OB_FAIL(sql_proxy_->read(scan_res, common::OB_SYS_TENANT_ID, scan_sql.ptr()))) {
        LOG_WARN("fail to execute sql", K(ret));
      } else if (OB_ISNULL(scan_result = scan_res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, query result must not be NULL", K(ret));
      } else {
        while (OB_SUCC(ret)) {
          if (OB_FAIL(scan_result->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            } else {
              LOG_WARN("failed to get next row", K(ret));
            }
          } else {
            int64_t gmt_create_tmp = 0;
            EXTRACT_INT_FIELD_MYSQL(*scan_result, "GMT_CREATE", gmt_create_tmp, int64_t);

            int64_t table_id_tmp = 0;
            EXTRACT_INT_FIELD_MYSQL(*scan_result, "table_id", table_id_tmp, int64_t);

            int64_t tenant_id_tmp;
            EXTRACT_INT_FIELD_MYSQL(*scan_result, "tenant_id", tenant_id_tmp, int64_t);

            if (OB_FAIL(ret)) {
              LOG_WARN("failed to get mysql field result", K(ret));
            } else {
              for (int64_t i = 0; OB_SUCC(ret) && i < diagnose_values_.count(); ++i) {
                if (diagnose_values_.at(i).object_id_ == table_id_tmp && diagnose_values_.at(i).tenant_id_ == tenant_id_tmp && diagnose_values_.at(i).execution_id_ == -2) {
                  diagnose_values_.at(i).start_time_ = gmt_create_tmp;
                  break;
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

}// observer

}// oceanbase