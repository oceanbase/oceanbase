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

#define USING_LOG_PREFIX RS

#include "share/table/ob_ttl_util.h"
#include "share/ob_max_id_fetcher.h"
#include "share/ob_srv_rpc_proxy.h"
#include "common/ob_unit_info.h"
#include "share/ob_server_status.h"
#include "share/schema/ob_schema_utils.h"

namespace oceanbase
{
namespace common
{

bool ObTTLTime::is_same_day(int64_t ttl_time1, int64_t ttl_time2)
{
  time_t param1 = static_cast<time_t>(ttl_time1 / 1000000l);
  time_t param2 = static_cast<time_t>(ttl_time2 / 1000000l);
  
  struct tm tm1, tm2;
  ::localtime_r(&param1, &tm1);
  ::localtime_r(&param2, &tm2);

  return (tm1.tm_yday == tm2.tm_yday);
}

bool ObTTLUtil::extract_val(const char* ptr, uint64_t len, int& val)
{
  char buffer[16] = {0};
  bool bool_ret = false;
  for (int i = 0; i < len; ++i) {
    if (ptr[i] == ' ') {
      continue;
    } else if (ptr[i] >= '0' && ptr[i] <= '9') {
      bool_ret = true;
      MEMCPY(buffer, ptr + i, len - i > 2 ? len - i : 2);
      break;
    }
  }
  val = atoi(buffer);
  return bool_ret;
}

int ObTTLUtil::parse_ttl_daytime(ObString& in, ObTTLDayTime& daytime)
{
  int ret = OB_SUCCESS;

  const char* first_split = in.find(':');
  const char* second_split = in.reverse_find(':');

  if (in.contains(first_split) && 
      in.contains(second_split) && 
      first_split < second_split) {
    if (extract_val(in.ptr(), first_split - in.ptr(), daytime.hour_) &&
        extract_val(first_split + 1, second_split - first_split - 1, daytime.min_) && 
        extract_val(second_split + 1, in.length() + in.ptr() - second_split, daytime.sec_)) {
    } else {
      ret = OB_INVALID_CONFIG;
      LOG_WARN("illegal input string", K(ret), K(in));  
    }
  } else {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("illegal input string", K(ret), K(in));  
  }

  return ret;
}

int ObTTLUtil::parse(const char* str, ObTTLDutyDuration& duration)
{
  int ret = OB_SUCCESS;
  
  if (OB_ISNULL(str)) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("fail to parse str", K(ret));
  } else {
    ObString in_str(str);
    const char* begin = in_str.find('[');
    const char* split = in_str.find(',');
    const char* end = in_str.reverse_find(']');

    if (OB_ISNULL(begin) || OB_ISNULL(split) || OB_ISNULL(end)) {
      ret = OB_INVALID_CONFIG;
      LOG_WARN("fail to parse str", K(ret));
    } else {
      ObString first_param, second_param;
      first_param.assign_ptr(begin + 1, split - begin - 1);
      second_param.assign_ptr(split + 1, end - split - 1);

      if (OB_FAIL(parse_ttl_daytime(first_param, duration.begin_)) ||
          OB_FAIL(parse_ttl_daytime(second_param, duration.end_))) {
        LOG_WARN("fail to parse daytime", K(ret));
      }
    }
  }

  return ret;
}

bool ObTTLUtil::current_in_duration(ObTTLDutyDuration& duration)
{
  time_t now;
  time(&now);
  struct tm *t = localtime(&now);
  uint32_t begin = duration.begin_.sec_ + 60 * (duration.begin_.min_ + 60 * duration.begin_.hour_);
  uint32_t end = duration.end_.sec_ + 60 * (duration.end_.min_ + 60 * duration.end_.hour_);
  uint32_t current = t->tm_sec + 60 * (t->tm_min + 60 * t->tm_hour);
  return ((begin <= current) & ( current <= end));
}

int ObTTLUtil::insert_ttl_task(uint64_t tenant_id,
                               const char* tname,
                               common::ObISQLClient& proxy,
                               ObTTLStatus& task)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affect_rows = 0;

  if (OB_FAIL(sql.assign_fmt("INSERT INTO %s "
              "(gmt_create, gmt_modified, tenant_id, table_id, partition_id, "
              "task_id, task_start_time, task_update_time, trigger_type, status,"
              " ttl_del_cnt, max_version_del_cnt, scan_cnt, row_key, ret_code)"
              " VALUE "
              "(now(), now(), %ld, %ld, %ld,"
              " %ld, %ld, %ld, %ld, %ld, "
              " %ld, %ld, %ld,'%.*s', '%.*s')", // 12
              tname, // 0
              tenant_id, task.table_id_, task.partition_id_,
              task.task_id_, task.task_start_time_, task.task_update_time_, task.trigger_type_, task.status_,
              task.ttl_del_cnt_, task.max_version_del_cnt_,
              task.scan_cnt_, task.row_key_.length(), task.row_key_.ptr(),
              task.ret_code_.length(), task.ret_code_.ptr()))) {
    LOG_WARN("sql assign fmt failed", K(ret));
  } else if (OB_FAIL(proxy.write(tenant_id, sql.ptr(), affect_rows))) {
    LOG_WARN("fail to execute sql", K(ret), K(sql));
  } else if (affect_rows != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_INFO("execute sql, affect rows != 1", K(ret), K(sql));
  } else {
    LOG_INFO("success to execute sql", K(ret), K(sql));
  }

  return ret;
}

int ObTTLUtil::update_ttl_task_all_fields(uint64_t tenant_id,
                                          const char* tname,
                                          common::ObMySQLProxy& proxy, 
                                          ObTTLStatusKey& key,
                                          ObTTLStatus& task)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affect_rows = 0;
  int64_t start = ObTimeUtility::current_time();

  if (OB_FAIL(sql.assign_fmt("UPDATE %s SET "
              "task_start_time = %ld, task_update_time = %ld, trigger_type = %ld, status = %ld,"
              " ttl_del_cnt = %ld, max_version_del_cnt = %ld, scan_cnt = %ld, row_key = '%*.s', ret_code = '%*.s'"
              " WHERE tenant_id = %ld AND table_id = %ld AND partition_id = %ld AND task_id = %ld ",
              tname, // 0
              task.task_start_time_, task.task_update_time_, task.trigger_type_, task.status_,
              task.ttl_del_cnt_, task.max_version_del_cnt_, task.scan_cnt_,
              task.row_key_.length(), task.row_key_.ptr(),
              task.ret_code_.length(), task.ret_code_.ptr(),
              tenant_id, task.table_id_, key.partition_id_, key.task_id_))) {
    LOG_WARN("sql assign fmt failed", K(ret));
  } else if (OB_FAIL(proxy.write(tenant_id, sql.ptr(), affect_rows))) {
    LOG_WARN("fail to execute sql", K(ret), K(sql));
  } else {
    LOG_INFO("success to execute sql", K(ret), K(sql));
  }

  return ret;
}

int ObTTLUtil::update_ttl_task(uint64_t tenant_id,
                               const char* tname,
                               common::ObISQLClient& proxy, 
                               ObTTLStatusKey& key,
                               ObTTLStatusFieldArray& update_fields)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;

  if (OB_FAIL(sql.assign_fmt("UPDATE %s SET ", tname))) {
    LOG_WARN("sql assign fmt failed", K(ret));
  }

  // FILED_NAME = value string construct
  for (size_t i = 0; OB_SUCC(ret) && i < update_fields.count(); ++i) {
    ObTTLStatusField& field = update_fields.at(i);

    if (OB_FAIL(sql.append_fmt("%s =", field.field_name_.ptr()))) {
      LOG_WARN("sql assign fmt failed", K(ret));
    } else if (field.type_ == ObTTLStatusField::INT_TYPE) { // todo@dazhi: not alway cstring
      if (OB_FAIL(sql.append_fmt("%ld", field.data_.int_))) {
        LOG_WARN("sql append fmt failed", K(ret));
      }
    } else if (field.type_ == ObTTLStatusField::UINT_TYPE) {
      if (OB_FAIL(sql.append_fmt("%ld", field.data_.uint_))) {
        LOG_WARN("sql append fmt failed", K(ret));
      }
    } else if (field.type_ == ObTTLStatusField::STRING_TYPE) {
      if (OB_FAIL(sql.append_fmt("%s", field.data_.str_.ptr()))) { // todo@dazhi: not alway cstring
        LOG_WARN("sql append fmt failed", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql append fmt failed", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(sql.append_fmt("%s", i == update_fields.count() - 1 ? " " : ","))) {
        LOG_WARN("sql append fmt failed", K(ret));
      }
    }
  }

  // WHERE FILTER
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql.append_fmt(" WHERE "
                    "tenant_id = %ld AND table_id = %ld AND partition_id = %ld AND task_id = %ld",
                    key.tenant_id_, key.table_id_, key.partition_id_, key.task_id_))) {
    LOG_WARN("sql append fmt failed", K(ret));
  }

  int64_t affect_rows = 0;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(proxy.write(tenant_id, sql.ptr(), affect_rows))) {
    LOG_WARN("fail to execute sql", K(ret), K(sql));
  } else if (affect_rows != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_INFO("execute sql, affect rows != 1", K(ret), K(sql));
  } else {
    LOG_INFO("success to execute sql", K(ret), K(sql));
  }

  return ret;
}

int ObTTLUtil::update_ttl_task_all_fields(uint64_t tenant_id,
                                          const char* tname,
                                          common::ObISQLClient& proxy, 
                                          ObTTLStatus& task)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affect_rows = 0;

  if (OB_FAIL(sql.assign_fmt("UPDATE %s SET "
              "task_start_time = %ld, task_update_time = %ld, trigger_type = %ld, status = %ld,"
              " ttl_del_cnt = %ld, max_version_del_cnt = %ld, scan_cnt = %ld, row_key = '%*.s', ret_code = '%*.s'"
              " WHERE "
              "tenant_id = %ld AND table_id = %ld AND partition_id = %ld AND task_id = %ld ",
              tname, // 0
              task.task_start_time_, task.task_update_time_, task.trigger_type_, task.status_,
              task.ttl_del_cnt_, task.max_version_del_cnt_, task.scan_cnt_,
              task.row_key_.length(), task.row_key_.ptr(), task.ret_code_.length(), task.ret_code_.ptr(),
              tenant_id, task.table_id_, task.partition_id_, task.task_id_))) {
    LOG_WARN("sql assign fmt failed", K(ret));
  } else if (OB_FAIL(proxy.write(tenant_id, sql.ptr(), affect_rows))) {
    LOG_WARN("fail to execute sql", K(ret), K(sql));
  } else {
    LOG_INFO("success to execute sql", K(ret), K(sql));
  }

  return ret;
}

int ObTTLUtil::delete_ttl_task(uint64_t tenant_id,
                               const char* tname,
                               common::ObISQLClient& proxy,
                               ObTTLStatusKey& key,
                               int64_t &affect_rows)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;

  if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE "
                             "tenant_id = %ld AND table_id = %ld "
                             "AND partition_id = %ld AND task_id = %ld",
                             tname,
                             tenant_id, key.table_id_,
                             key.partition_id_, key.task_id_))) {
    LOG_WARN("sql assign fmt failed", K(ret));
  } else if (OB_FAIL(proxy.write(tenant_id, sql.ptr(), affect_rows))) {
    LOG_WARN("fail to execute sql", K(ret), K(sql));
  } else {
    LOG_INFO("success to execute sql", K(ret), K(sql));
  }

  return ret;
}

int ObTTLUtil::read_ttl_tasks(uint64_t tenant_id,
                              const char* tname,
                              common::ObISQLClient& proxy,
                              ObTTLStatusFieldArray& filters, 
                              ObTTLStatusArray& result_arr,
                              bool for_update /*false*/,
                              common::ObIAllocator *allocator /*NULL*/)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;

  if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s where ", tname))) {
    LOG_WARN("sql assign fmt failed", K(ret));
  }

  // FILED_NAME = value string construct
  for (size_t i = 0; OB_SUCC(ret) && i < filters.count(); ++i) {
    ObTTLStatusField& field = filters.at(i);

    if (OB_FAIL(sql.append_fmt("%s = ", field.field_name_.ptr()))) {
      LOG_WARN("sql assign fmt failed", K(ret));
    } else if (field.type_ == ObTTLStatusField::INT_TYPE) {
      if (OB_FAIL(sql.append_fmt("%ld", field.data_.int_))) {
        LOG_WARN("sql append fmt failed", K(ret));
      }
    } else if (field.type_ == ObTTLStatusField::UINT_TYPE) {
      if (OB_FAIL(sql.append_fmt("%ld", field.data_.uint_))) {
        LOG_WARN("sql append fmt failed", K(ret));
      }
    } else if (field.type_ == ObTTLStatusField::STRING_TYPE) {
      if (OB_FAIL(sql.append_fmt("%s", field.data_.str_.ptr()))) {
        LOG_WARN("sql append fmt failed", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql append fmt failed", K(ret));
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(sql.append_fmt("%s", i == filters.count() - 1 ? "" : " AND "))) {
        LOG_WARN("sql append fmt failed", K(ret));
      }
    }
  }
 
  if (OB_SUCC(ret) && for_update) {
    if (OB_FAIL(sql.append_fmt(" for update"))) {
      LOG_WARN("sql append fmt failed", K(ret));
    }
  }


  if (OB_SUCC(ret)) {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult* result = NULL;
      if (OB_FAIL(proxy.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("sql assign fmt failed", K(ret));
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
            size_t idx = result_arr.count();
            ObTTLStatus task;
            if (OB_FAIL(result_arr.push_back(task))) {
              LOG_WARN("fail to push back task", K(ret), K(result_arr.count()));
            } else {
              
              EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", result_arr.at(idx).tenant_id_, uint64_t);
              EXTRACT_INT_FIELD_MYSQL(*result, "table_id", result_arr.at(idx).table_id_, uint64_t);
              
              EXTRACT_INT_FIELD_MYSQL(*result, "partition_id", result_arr.at(idx).partition_id_, uint64_t);
              EXTRACT_INT_FIELD_MYSQL(*result, "task_id", result_arr.at(idx).task_id_, uint64_t);
              
              EXTRACT_INT_FIELD_MYSQL(*result, "task_start_time", result_arr.at(idx).task_start_time_, int64_t);
              EXTRACT_INT_FIELD_MYSQL(*result, "task_update_time", result_arr.at(idx).task_update_time_, int64_t);
              EXTRACT_INT_FIELD_MYSQL(*result, "trigger_type", result_arr.at(idx).trigger_type_, int64_t);
              EXTRACT_INT_FIELD_MYSQL(*result, "status", result_arr.at(idx).status_, int64_t);

              EXTRACT_INT_FIELD_MYSQL(*result, "ttl_del_cnt", result_arr.at(idx).ttl_del_cnt_, uint64_t);
              EXTRACT_INT_FIELD_MYSQL(*result, "max_version_del_cnt", result_arr.at(idx).max_version_del_cnt_, uint64_t);
              EXTRACT_INT_FIELD_MYSQL(*result, "scan_cnt", result_arr.at(idx).scan_cnt_, uint64_t);
              if (OB_NOT_NULL(allocator)) {
                ObString rowkey; 
                char *rowkey_buf = nullptr;
                EXTRACT_VARCHAR_FIELD_MYSQL(*result, "row_key", rowkey);
                if (OB_SUCC(ret) && !rowkey.empty()) {
                  if (OB_ISNULL(rowkey_buf = static_cast<char *>(allocator->alloc(rowkey.length())))) {
                    LOG_WARN("failt to allocate memory", K(ret), K(rowkey));
                  } else {
                    MEMCPY(rowkey_buf, rowkey.ptr(), rowkey.length());
                    result_arr.at(idx).row_key_.assign(rowkey_buf, rowkey.length());
                  }
                }
              }

              if (OB_NOT_NULL(allocator)) {
                ObString err_msg; 
                char *err_buf = nullptr;
                EXTRACT_VARCHAR_FIELD_MYSQL(*result, "ret_code", err_msg);
                if (OB_SUCC(ret) && !err_msg.empty()) {
                  if (OB_ISNULL(err_buf = static_cast<char *>(allocator->alloc(err_msg.length())))) {
                    LOG_WARN("failt to allocate memory", K(ret), K(err_msg));
                  } else {
                    MEMCPY(err_buf, err_msg.ptr(), err_msg.length());
                    result_arr.at(idx).ret_code_.assign(err_buf, err_msg.length());
                  }
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

bool ObTTLUtil::check_can_do_work() {
  bool bret = true;
  if (GCTX.is_standby_cluster()) {
    bret = false;
  }
  return bret;
}


bool ObTTLUtil::check_can_process_tenant_tasks(uint64_t tenant_id)
{
  bool bret = false;

  if (OB_INVALID_TENANT_ID == tenant_id) {
    LOG_WARN("invalid tenant id");
  } else if (check_can_do_work()) {
    int ret = OB_SUCCESS;
    bool is_restore = true;
    if (OB_FAIL(share::schema::ObMultiVersionSchemaService::get_instance().
                  check_tenant_is_restore(NULL, tenant_id, is_restore))) {
      if (OB_TENANT_NOT_EXIST != ret) {
        LOG_WARN("fail to check tenant is restore", KR(ret), K(tenant_id), K(common::lbt()));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      bret = !is_restore;
    }
  }
  return bret;
}

int ObTTLUtil::remove_all_task_to_history_table(uint64_t tenant_id, uint64_t task_id, common::ObISQLClient& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affect_rows = 0;
  if (OB_FAIL(sql.assign_fmt("insert into %s select * from %s "
              " where task_id = %ld and partition_id != -1 and table_id != -1",
              share::OB_ALL_KV_TTL_TASK_HISTORY_TNAME,
              share::OB_ALL_KV_TTL_TASK_TNAME,
              task_id))) {
    LOG_WARN("sql assign fmt failed", K(ret));
  } else if (OB_FAIL(proxy.write(tenant_id, sql.ptr(), affect_rows))) {
    LOG_WARN("fail to execute sql", K(ret), K(sql), K(tenant_id));
  } else {
    LOG_INFO("success to execute sql", K(ret), K(tenant_id), K(sql), K(affect_rows));
  }

  return ret;
} 

int ObTTLUtil::replace_ttl_task(uint64_t tenant_id,
                               const char* tname,
                               common::ObISQLClient& proxy,
                               ObTTLStatus& task)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affect_rows = 0;

  if (OB_FAIL(sql.assign_fmt("REPLACE INTO %s "
              "(gmt_create, gmt_modified, tenant_id, table_id, partition_id, "
              "task_id, task_start_time, task_update_time, trigger_type, status,"
              " ttl_del_cnt, max_version_del_cnt, scan_cnt, row_key, ret_code)"
              " VALUE "
              "(now(), now(), %ld, %ld, %ld,"
              " %ld, %ld, %ld, %ld, %ld, "
              " %ld, %ld, %ld,'%.*s', '%.*s')", // 12
              tname, // 0
              tenant_id, task.table_id_, task.partition_id_,
              task.task_id_, task.task_start_time_, task.task_update_time_, task.trigger_type_, task.status_,
              task.ttl_del_cnt_, task.max_version_del_cnt_,
              task.scan_cnt_, task.row_key_.length(), task.row_key_.ptr(),
              task.ret_code_.length(), task.ret_code_.ptr()))) {
    LOG_WARN("sql assign fmt failed", K(ret));
  } else if (OB_FAIL(proxy.write(tenant_id, sql.ptr(), affect_rows))) {
    LOG_WARN("fail to execute sql", K(ret), K(sql));
  } else {
    LOG_INFO("success to execute sql", K(ret), K(sql));
  }

  return ret;
}

} // end namespace rootserver
} // end namespace oceanbase