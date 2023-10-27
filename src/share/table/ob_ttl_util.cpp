/**
 * Copyright (c) 2023 OceanBase
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

#include "share/table/ob_ttl_util.h"
#include "share/ob_max_id_fetcher.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/ob_server_status.h"
#include "share/schema/ob_schema_utils.h"
#include "rootserver/ob_root_service.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "lib/stat/ob_diagnose_info.h"
#include "share/location_cache/ob_location_service.h"

using namespace oceanbase::share;

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

  if (OB_ISNULL(str) || strlen(str) == 0) {
    duration.not_set_ = true;
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
      } else {
        duration.not_set_ = false;
      }
    }
  }

  return ret;
}

bool ObTTLUtil::current_in_duration(ObTTLDutyDuration& duration)
{
  bool bret = false;
  if (!duration.not_set_) {
    time_t now;
    time(&now);
    struct tm *t = localtime(&now);
    uint32_t begin = duration.begin_.sec_ + 60 * (duration.begin_.min_ + 60 * duration.begin_.hour_);
    uint32_t end = duration.end_.sec_ + 60 * (duration.end_.min_ + 60 * duration.end_.hour_);
    uint32_t current = t->tm_sec + 60 * (t->tm_min + 60 * t->tm_hour);
    bret = (begin <= current) & ( current <= end);
  }
  return bret;
}

int ObTTLUtil::transform_tenant_state(const common::ObTTLTaskStatus& tenant_status,
                                      common::ObTTLTaskStatus& status)
{
  int ret = OB_SUCCESS;
  if (tenant_status == OB_RS_TTL_TASK_CREATE) {
    status = OB_TTL_TASK_RUNNING;
  } else if (tenant_status == OB_RS_TTL_TASK_SUSPEND) {
    status = OB_TTL_TASK_PENDING;
  } else if (tenant_status == OB_RS_TTL_TASK_CANCEL) {
    status = OB_TTL_TASK_CANCEL;
  } else if (tenant_status == OB_RS_TTL_TASK_MOVE) {
    status = OB_TTL_TASK_MOVING;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid type", K(tenant_status), K(status));
  }
  return ret;
}

int ObTTLUtil::check_tenant_state(uint64_t tenant_id,
                                  common::ObISQLClient& proxy,
                                  const ObTTLTaskStatus local_state,
                                  const int64_t local_task_id)
{
  int ret = OB_SUCCESS;

  ObTTLStatus tenant_task;
  ObTTLTaskStatus tenant_state;
  if (OB_FAIL(ObTTLUtil::read_tenant_ttl_task(tenant_id, proxy, tenant_task, true))) {
    if (OB_ITER_END == ret) {
      // tenant task maybe remove
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lock tenant task for update failed, tenant task maybe removed", K(ret), K(tenant_id), K(local_state));
    } else {
      LOG_WARN("failed to lock tenant task for update", KR(ret), K(tenant_id), K(local_state));
    }
  } else if (local_task_id != tenant_task.task_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant task id is different from local task id", KR(ret), K(local_task_id), K(tenant_task.task_id_));
  } else if (OB_FAIL(transform_tenant_state(static_cast<ObTTLTaskStatus>(tenant_task.status_), tenant_state))) {
    LOG_WARN("fail to transform ttl tenant task status", KR(ret), K(tenant_task.status_));
  } else if (tenant_state != local_state) {
    ret = OB_EAGAIN;
    FLOG_INFO("state of tenant task is different from local task state", K(ret), K(tenant_id), K(tenant_task.task_id_ ), K(local_state));
  }

  return ret;
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
              "(gmt_create, gmt_modified, tenant_id, table_id, tablet_id, "
              "task_id, task_start_time, task_update_time, trigger_type, status,"
              " ttl_del_cnt, max_version_del_cnt, scan_cnt, ret_code, row_key)"
              " VALUE "
              "(now(), now(), %ld, %ld, %ld,"
              " %ld, %ld, %ld, %ld, %ld, "
              " %ld, %ld, %ld,'%.*s', ",
              tname,
              tenant_id, task.table_id_, task.tablet_id_,
              task.task_id_, task.task_start_time_, task.task_update_time_, task.trigger_type_,
              task.status_, task.ttl_del_cnt_, task.max_version_del_cnt_,
              task.scan_cnt_, task.ret_code_.length(), task.ret_code_.ptr()))) {
    LOG_WARN("sql assign fmt failed", K(ret));
  } else if (OB_FAIL(sql_append_hex_escape_str(task.row_key_, sql))) {
    LOG_WARN("fail to append rowkey", K(ret));
  } else if (OB_FAIL(sql.append(")"))) {
    LOG_WARN("fail to append");
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affect_rows))) {
    LOG_WARN("fail to execute sql", K(ret), K(sql));
  } else if (affect_rows != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_INFO("execute sql, affect rows != 1", K(ret), K(sql));
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
      if (OB_FAIL(sql.append_fmt("%s", i == update_fields.count() - 1 ? " " : ","))) {
        LOG_WARN("sql append fmt failed", K(ret));
      }
    }
  }

  // WHERE FILTER
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sql.append_fmt(" WHERE "
                    "tenant_id = %ld AND table_id = %ld AND tablet_id = %ld AND task_id = %ld",
                    key.tenant_id_, key.table_id_, key.tablet_id_, key.task_id_))) {
    LOG_WARN("sql append fmt failed", K(ret));
  }

  int64_t affect_rows = 0;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affect_rows))) {
    LOG_WARN("fail to execute sql", K(ret), K(sql));
    if (ret == OB_ERR_EXCLUSIVE_LOCK_CONFLICT) {
      FLOG_INFO("fail to execute sql, this task/rowkey is locked by other thread, pls try again", K(ret), K(sql));
    }
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
              " ttl_del_cnt = %ld, max_version_del_cnt = %ld, scan_cnt = %ld, ret_code = '%.*s',"
              " row_key = ",
              tname, task.task_start_time_, task.task_update_time_, task.trigger_type_, task.status_,
              task.ttl_del_cnt_, task.max_version_del_cnt_, task.scan_cnt_, task.ret_code_.length(),
              task.ret_code_.ptr()))) {
    LOG_WARN("sql assign fmt failed", K(ret));
  } else if (OB_FAIL(sql_append_hex_escape_str(task.row_key_, sql))) {
    LOG_WARN("fail to append rowkey", K(ret));
  } else if (OB_FAIL(sql.append_fmt(" WHERE tenant_id = %ld AND table_id = %ld"
              " AND tablet_id = %ld AND task_id = %ld ",
              tenant_id, task.table_id_, task.tablet_id_, task.task_id_))) {
    LOG_WARN("sql assign fmt failed", K(ret));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affect_rows))) {
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
                             "AND tablet_id = %ld AND task_id = %ld",
                             tname,
                             tenant_id, key.table_id_,
                             key.tablet_id_, key.task_id_))) {
    LOG_WARN("sql assign fmt failed", K(ret));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affect_rows))) {
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
      sqlclient::ObMySQLResult* result = nullptr;
      if (OB_FAIL(proxy.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(sql));
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

              EXTRACT_INT_FIELD_MYSQL(*result, "tablet_id", result_arr.at(idx).tablet_id_, uint64_t);
              EXTRACT_INT_FIELD_MYSQL(*result, "task_id", result_arr.at(idx).task_id_, uint64_t);

              EXTRACT_INT_FIELD_MYSQL(*result, "task_start_time", result_arr.at(idx).task_start_time_, int64_t);
              EXTRACT_INT_FIELD_MYSQL(*result, "task_update_time", result_arr.at(idx).task_update_time_, int64_t);
              EXTRACT_INT_FIELD_MYSQL(*result, "trigger_type", result_arr.at(idx).trigger_type_, int64_t);
              EXTRACT_INT_FIELD_MYSQL(*result, "status", result_arr.at(idx).status_, int64_t);

              EXTRACT_INT_FIELD_MYSQL(*result, "ttl_del_cnt", result_arr.at(idx).ttl_del_cnt_, uint64_t);
              EXTRACT_INT_FIELD_MYSQL(*result, "max_version_del_cnt", result_arr.at(idx).max_version_del_cnt_, uint64_t);
              EXTRACT_INT_FIELD_MYSQL(*result, "scan_cnt", result_arr.at(idx).scan_cnt_, uint64_t);
              if (OB_SUCC(ret) && OB_NOT_NULL(allocator)) {
                ObString rowkey;
                char *rowkey_buf = nullptr;
                EXTRACT_VARCHAR_FIELD_MYSQL(*result, "row_key", rowkey);
                if (OB_SUCC(ret) && !rowkey.empty()) {
                  if (OB_ISNULL(rowkey_buf = static_cast<char *>(allocator->alloc(rowkey.length())))) {
                    ret = OB_ALLOCATE_MEMORY_FAILED;
                    LOG_WARN("failt to allocate memory", K(ret), K(rowkey));
                  } else {
                    MEMCPY(rowkey_buf, rowkey.ptr(), rowkey.length());
                    result_arr.at(idx).row_key_.assign(rowkey_buf, rowkey.length());
                  }
                }
              }

              if (OB_SUCC(ret) && OB_NOT_NULL(allocator)) {
                ObString err_msg;
                char *err_buf = nullptr;
                EXTRACT_VARCHAR_FIELD_MYSQL(*result, "ret_code", err_msg);
                if (OB_SUCC(ret) && !err_msg.empty()) {
                  if (OB_ISNULL(err_buf = static_cast<char *>(allocator->alloc(err_msg.length())))) {
                    ret = OB_ALLOCATE_MEMORY_FAILED;
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

int ObTTLUtil::read_tenant_ttl_task(uint64_t tenant_id,
                                    common::ObISQLClient& sql_client,
                                    ObTTLStatus& ttl_record,
                                    const bool for_update,
                                    ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE table_id = '%ld'%s", OB_ALL_KV_TTL_TASK_TNAME, TTL_TENNAT_TASK_TABLE_ID, for_update ? " FOR UPDATE" : ""))) {
    LOG_WARN("fail to append sql", KR(ret), K(tenant_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql_client.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get sql result", KR(ret), K(tenant_id), K(sql));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next result");
        }
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", ttl_record.tenant_id_, uint64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "table_id", ttl_record.table_id_, uint64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "tablet_id", ttl_record.tablet_id_, uint64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "task_id", ttl_record.task_id_, uint64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "task_start_time", ttl_record.task_start_time_, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "task_update_time", ttl_record.task_update_time_, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "trigger_type", ttl_record.trigger_type_, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "status", ttl_record.status_, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "ttl_del_cnt", ttl_record.ttl_del_cnt_, uint64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "max_version_del_cnt", ttl_record.max_version_del_cnt_, uint64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "scan_cnt", ttl_record.scan_cnt_, uint64_t);
        if (OB_SUCC(ret) && OB_NOT_NULL(allocator)) {
          ObString rowkey;
          char *rowkey_buf = nullptr;
          EXTRACT_VARCHAR_FIELD_MYSQL(*result, "row_key", rowkey);
          if (OB_SUCC(ret) && !rowkey.empty()) {
            if (OB_ISNULL(rowkey_buf = static_cast<char *>(allocator->alloc(rowkey.length())))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("failt to allocate memory", K(ret), K(rowkey));
            } else {
              MEMCPY(rowkey_buf, rowkey.ptr(), rowkey.length());
              ttl_record.row_key_.assign(rowkey_buf, rowkey.length());
            }
          }
        }
        if (OB_SUCC(ret) && OB_NOT_NULL(allocator)) {
          ObString err_msg;
          char *err_buf = nullptr;
          EXTRACT_VARCHAR_FIELD_MYSQL(*result, "ret_code", err_msg);
          if (OB_SUCC(ret) && !err_msg.empty()) {
            if (OB_ISNULL(err_buf = static_cast<char *>(allocator->alloc(err_msg.length())))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("failt to allocate memory", K(ret), K(err_msg));
            } else {
              MEMCPY(err_buf, err_msg.ptr(), err_msg.length());
              ttl_record.ret_code_.assign(err_buf, err_msg.length());
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
  int ret = OB_SUCCESS;
  int64_t tenant_id = MTL_ID();
  uint64_t tenant_data_version = 0;;
  if (GCTX.is_standby_cluster()) {
    bret = false;
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
    bret = false;
    LOG_WARN("get tenant data version failed", K(ret));
  } else if (tenant_data_version < DATA_VERSION_4_2_1_0) {
    bret = false;
    LOG_DEBUG("TTL can not work with data version less than 4_2_1", K(tenant_data_version));
  } else if (is_user_tenant(tenant_id)) {
    if (OB_FAIL(GET_MIN_DATA_VERSION(gen_meta_tenant_id(tenant_id), tenant_data_version))) {
      bret = false;
      LOG_WARN("get tenant data version failed", K(ret));
    } else if (tenant_data_version < DATA_VERSION_4_2_1_0) {
      bret = false;
      LOG_DEBUG("TTL can not work with data version less than 4_2_1", K(tenant_data_version));
    }
  }
  return bret;
}


bool ObTTLUtil::check_can_process_tenant_tasks(uint64_t tenant_id)
{
  bool bret = false;

  if (OB_INVALID_TENANT_ID == tenant_id) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "invalid tenant id");
  } else {
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

int ObTTLUtil::move_task_to_history_table(uint64_t tenant_id, uint64_t task_id,
                                          common::ObMySQLTransaction& proxy,
                                          int64_t batch_size, int64_t &move_rows)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t insert_rows = 0;
  int64_t delete_rows = 0;
  if (OB_FAIL(sql.assign_fmt("replace into %s select * from %s "
              " where task_id = %ld and tablet_id != -1  and table_id != -1"
              " order by tenant_id, task_id, table_id, tablet_id LIMIT %ld",
              share::OB_ALL_KV_TTL_TASK_HISTORY_TNAME,
              share::OB_ALL_KV_TTL_TASK_TNAME,
              task_id, batch_size))) {
    LOG_WARN("sql assign fmt failed", K(ret));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), insert_rows))) {
    LOG_WARN("fail to execute sql", K(ret), K(sql), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("delete from %s"
          " where task_id = %ld and tablet_id != -1 and table_id != -1"
          " order by tenant_id, task_id, table_id, tablet_id LIMIT %ld ",
          share::OB_ALL_KV_TTL_TASK_TNAME,
          task_id, batch_size))) {
    LOG_WARN("sql assign fmt failed", K(ret));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), delete_rows))) {
    LOG_WARN("fail to execute sql", K(ret), K(sql), K(tenant_id));
  } else {
    move_rows = delete_rows;
    LOG_INFO("success to execute sql", K(ret), K(tenant_id), K(sql), K(insert_rows), K(delete_rows));
  }

  return ret;
}

// only one record left in this situation
int ObTTLUtil::move_tenant_task_to_history_table(uint64_t tenant_id, uint64_t task_id,
                                                 common::ObMySQLTransaction& proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t insert_rows = 0;
  int64_t delete_rows = 0;
  if (OB_FAIL(sql.assign_fmt("insert into %s select * from %s "
              " where task_id = %ld and tablet_id = %ld",
              share::OB_ALL_KV_TTL_TASK_HISTORY_TNAME,
              share::OB_ALL_KV_TTL_TASK_TNAME,
              task_id, TTL_TENNAT_TASK_TABLET_ID))) {
    LOG_WARN("sql assign fmt failed", K(ret));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), insert_rows))) {
    LOG_WARN("fail to execute sql", K(ret), K(sql), K(tenant_id));
  } else if (OB_FAIL(sql.assign_fmt("delete from %s"
          " where task_id = %ld and tablet_id = %ld",
          share::OB_ALL_KV_TTL_TASK_TNAME,
          task_id, TTL_TENNAT_TASK_TABLET_ID))) {
    LOG_WARN("sql assign fmt failed", K(ret));
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), delete_rows))) {
    LOG_WARN("fail to execute sql", K(ret), K(sql), K(tenant_id));
  } else {
    LOG_INFO("success to execute sql", K(ret), K(tenant_id), K(sql), K(insert_rows), K(delete_rows));
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
              "(gmt_create, gmt_modified, tenant_id, table_id, tablet_id, "
              "task_id, task_start_time, task_update_time, trigger_type, status,"
              " ttl_del_cnt, max_version_del_cnt, scan_cnt, ret_code, row_key)"
              " VALUE "
              "(now(), now(), %ld, %ld, %ld,"
              " %ld, %ld, %ld, %ld, %ld, "
              " %ld, %ld, %ld,'%.*s', ",
              tname, // 0
              tenant_id, task.table_id_, task.tablet_id_,
              task.task_id_, task.task_start_time_, task.task_update_time_, task.trigger_type_, task.status_,
              task.ttl_del_cnt_, task.max_version_del_cnt_,
              task.scan_cnt_, task.ret_code_.length(), task.ret_code_.ptr()))) {
    LOG_WARN("sql assign fmt failed", K(ret));
  } else if (OB_FAIL(sql_append_hex_escape_str(task.row_key_, sql))) {
    LOG_WARN("fail to append rowkey", K(ret));
  } else if (OB_FAIL(sql.append(")"))) {
    LOG_WARN("fail to append");
  } else if (OB_FAIL(proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affect_rows))) {
    LOG_WARN("fail to execute sql", K(ret), K(sql));
  } else {
    LOG_INFO("success to execute sql", K(ret), K(sql));
  }

  return ret;
}

// example: kv_attributes = {hbase: {maxversions: 3}}
int ObTTLUtil::parse_kv_attributes(const ObString &kv_attributes, int32_t &max_versions, int32_t &time_to_live)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  json::Parser json_parser;
  json::Value *ast = nullptr;
  if (kv_attributes.empty()) {
    // skip
  } else if (OB_FAIL(json_parser.init(&allocator))) {
    LOG_WARN("failed to init json parser", K(ret));
  } else if (OB_FAIL(json_parser.parse(kv_attributes.ptr(), kv_attributes.length(), ast))) {
    LOG_WARN("failed to parse kv attributes", K(ret), K(kv_attributes));
  } else if (NULL != ast
             && ast->get_type() == json::JT_OBJECT
             && ast->get_object().get_size() == 1) {
    json::Pair *kv = ast->get_object().get_first();
    if (NULL != kv && kv != ast->get_object().get_header()) {
      if (kv->name_.case_compare("HBASE") == 0) {
        ast = kv->value_;
        if (NULL == ast) {
          // do nothing
        } else if (ast->get_type() == json::JT_OBJECT) {
          DLIST_FOREACH(elem, ast->get_object()) {
            if (elem->name_.case_compare("TimeToLive") == 0) {
              json::Value *ttl_val = elem->value_;
              if (NULL != ttl_val && ttl_val->get_type() == json::JT_NUMBER) {
                if (ttl_val->get_number() <= 0) {
                  ret = OB_INVALID_ARGUMENT;
                  LOG_WARN("time to live should greater than 0", K(ret), K(ttl_val));
                  LOG_USER_ERROR(OB_INVALID_ARGUMENT, "time to live, which should greater than 0");
                } else {
                  time_to_live = static_cast<int32_t>(ttl_val->get_number());
                }
              }
            } else if (elem->name_.case_compare("MaxVersions") == 0) {
              json::Value *max_versions_val = elem->value_;
              if (NULL != max_versions_val && max_versions_val->get_type() == json::JT_NUMBER) {
                if (max_versions_val->get_number() <= 0) {
                  ret = OB_INVALID_ARGUMENT;
                  LOG_WARN("max versions should greater than 0", K(ret), K(max_versions_val));
                  LOG_USER_ERROR(OB_INVALID_ARGUMENT, "max versions, which should greater than 0");
                } else {
                  max_versions = static_cast<int32_t>(max_versions_val->get_number());
                }
              } else {
                ret = OB_NOT_SUPPORTED;
                LOG_WARN("not supported kv attribute", K(ret));
                LOG_USER_ERROR(OB_NOT_SUPPORTED, "kv attributes with wrong format");
              }
            } else {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("not supported kv attribute", K(ret));
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "kv attributes with wrong format");
            }
          }  // end foreach
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not supported kv attribute", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "kv attributes with wrong format");
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("only hbase mode is supported currently", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "kv attributes with wrong format");
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("not supported kv attribute", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "kv attributes with wrong format");
  }
  return ret;
}

int ObTTLUtil::dispatch_ttl_cmd(const ObTTLParam &param)
{
  int ret = OB_SUCCESS;
  int final_ret = OB_SUCCESS;
  ObSEArray<ObSimpleTTLInfo, 32> ttl_info_array;
  if (OB_UNLIKELY(!param.is_valid()
                  || (!param.ttl_all_ && param.ttl_info_array_.empty()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(param), KR(ret));
  } else if (OB_FAIL(get_ttl_info(param, ttl_info_array))) {
    LOG_WARN("fail to get tenant id", KR(ret), K(param));
  } else if (!ttl_info_array.empty()) {
    const int64_t ttl_info_count = ttl_info_array.count();
    for (int i = 0; i < ttl_info_count && OB_SUCC(ret); ++i) {
      const uint64_t tenant_id = ttl_info_array.at(i).tenant_id_;
      if (OB_FAIL(dispatch_one_tenant_ttl(param.type_, *param.transport_, ttl_info_array.at(i)))) {
        LOG_WARN("fail dispatch one tenant ttl", KR(ret), K(ttl_info_count), "ttl_info", ttl_info_array.at(i));
      }
    }
  }
  return ret;
}

int ObTableTTLChecker::init(const schema::ObTableSchema &table_schema, bool in_full_column_order)
{
  int ret = OB_SUCCESS;
  int64_t tenant_id = table_schema.get_tenant_id();
  bool has_datetime_col = false;
  if (tenant_id == OB_INVALID_TENANT_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    ObString ttl_definition = table_schema.get_ttl_definition();
    if (ttl_definition.empty()) {
      // do nothing
    } else {
      ObString right = ttl_definition;
      bool is_end = false;
      int64_t i = 0;
      while (OB_SUCC(ret) && !is_end) {
        ObString left = right.split_on(',');
        if (left.empty()) {
          left = right;
          is_end = true;
        }
        ObTableTTLExpr ttl_expr;
        ObString column_str = left.split_on('+').trim();
        left = left.trim();
        left += strlen("INTERVAL");
        left = left.trim();
        ObString interval_str = left.split_on(' ');
        left.trim();
        ObString time_unit_str = left;

        ttl_expr.column_name_ = column_str;
        ttl_expr.interval_ = atol(interval_str.ptr());
        if (time_unit_str.case_compare("SECOND") == 0) {
          ttl_expr.time_unit_ = ObTableTTLTimeUnit::SECOND;
        } else if (time_unit_str.case_compare("MINUTE") == 0) {
          ttl_expr.time_unit_ = ObTableTTLTimeUnit::MINUTE;
        } else if (time_unit_str.case_compare("HOUR") == 0) {
          ttl_expr.time_unit_ = ObTableTTLTimeUnit::HOUR;
        } else if (time_unit_str.case_compare("DAY") == 0) {
          ttl_expr.time_unit_ = ObTableTTLTimeUnit::DAY;
        } else if (time_unit_str.case_compare("MONTH") == 0) {
          ttl_expr.time_unit_ = ObTableTTLTimeUnit::MONTH;
        } else if (time_unit_str.case_compare("YEAR") == 0) {
          ttl_expr.time_unit_ = ObTableTTLTimeUnit::YEAR;
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("unepxected time unit", K(ret));
        }

        // 2. get delta second and month
        int64_t nsecond = 0;
        int64_t nmonth = 0;
        switch (ttl_expr.time_unit_) {
          case ObTableTTLTimeUnit::SECOND: {
            nsecond = ttl_expr.interval_;
            break;
          }
          case ObTableTTLTimeUnit::MINUTE: {
            nsecond = ttl_expr.interval_ * 60;
            break;
          }
          case ObTableTTLTimeUnit::HOUR: {
            nsecond = ttl_expr.interval_ * 60 * 60;
            break;
          }
          case ObTableTTLTimeUnit::DAY: {
            nsecond = ttl_expr.interval_ * 60 * 60 * 24;
            break;
          }
          case ObTableTTLTimeUnit::MONTH: {
            nmonth = ttl_expr.interval_;
            break;
          }
          case ObTableTTLTimeUnit::YEAR: {
            nmonth = ttl_expr.interval_ * 12;
            break;
          }
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected time unit", K(ret), K_(ttl_expr.time_unit));
        }


        if (OB_SUCC(ret)) {
          ttl_expr.nsecond_ = nsecond;
          ttl_expr.nmonth_ = nmonth;
          if (ttl_expr.column_name_.empty()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null column name", K(ret));
          } else if (OB_FAIL(ttl_definition_.push_back(ttl_expr))) {
            LOG_WARN("fail to add ttl expr", K(ret), K(ttl_expr));
          } else if (in_full_column_order) {
            schema::ObTableSchema::const_column_iterator iter = table_schema.column_begin();
            schema::ObTableSchema::const_column_iterator end = table_schema.column_end();
            const schema::ObColumnSchemaV2 *col_schema = nullptr;
            bool find_col = false;
            for (int idx = 0; OB_SUCC(ret) && iter != end && !find_col; ++iter, idx++) {
              col_schema = *iter;
              if (OB_ISNULL(col_schema)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("invalid column schema", K(ret));
              } else if (ttl_expr.column_name_.case_compare(col_schema->get_column_name_str()) == 0) {
                find_col = true;
                if (OB_FAIL(row_cell_ids_.push_back(idx))) {
                  LOG_WARN("fail to push back", K(ret), K(idx));
                } else if (col_schema->get_data_type() == ObDateTimeType) {
                  has_datetime_col = true;
                }
              }
            }
          }
        }
      }
    }
  }

  if (OB_SUCC(ret) && has_datetime_col) {
    ObSchemaGetterGuard schema_guard;
    ObTimeZoneInfoWrap tz_info_wrap;
    const ObSysVariableSchema *sys_variable_schema = nullptr;
    const ObSysVarSchema *system_timezone = nullptr;
    ObTZMapWrap tz_map_wrap;
    if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("get schema guard failed", K(ret), K(tenant_id));
    } else if (OB_FAIL(schema_guard.get_sys_variable_schema(tenant_id, sys_variable_schema))) {
      LOG_WARN("get sys variable schema failed", K(ret), K(tenant_id));
    } else if (NULL == sys_variable_schema) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sys variable schema is NULL", K(ret));
    } else if (OB_FAIL(sys_variable_schema->get_sysvar_schema(SYS_VAR_TIME_ZONE, system_timezone))) {
      LOG_WARN("fail to get system timezone", K(ret));
    } else if (OB_FAIL(OTTZ_MGR.get_tenant_tz(tenant_id, tz_map_wrap))) {
      LOG_WARN("get tenant timezone map failed", K(ret), K(tenant_id));
    } else if (OB_FAIL(tz_info_wrap_.init_time_zone(system_timezone->get_value(), OB_INVALID_VERSION, const_cast<ObTZInfoMap &>(*tz_map_wrap.get_tz_map())))) {
      LOG_WARN("fail to init time zone info wrap", K(ret), K(system_timezone->get_value()));
    }
  }

  return ret;
}

int ObTableTTLChecker::check_row_expired(const common::ObNewRow &row, bool &is_expired)
{
  int ret = OB_SUCCESS;
  is_expired = false;
  for (int i = 0; OB_SUCC(ret) && !is_expired && i < ttl_definition_.count(); i++) {
    ObTableTTLExpr ttl_expr = ttl_definition_.at(i);
    ObObj column = row.get_cell(row_cell_ids_.at(i));
    int64_t column_ts = column.get_timestamp();
    if (column.is_null()) {
      continue;
    } else if (column.get_type() == ObDateTimeType) {
      // todo: get tz info from system var
      const ObTimeZoneInfo *tz_info = tz_info_wrap_.get_time_zone_info();
      if (OB_FAIL(ObTimeConverter::datetime_to_timestamp(column_ts, tz_info, column_ts))) {
        LOG_WARN("fail to convert datetime to utc ts", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      int64_t expire_ts = column_ts;
      int64_t cur_ts = ObTimeUtility::current_time();
      if (ttl_expr.nsecond_ > 0 && OB_FAIL(ObTimeConverter::date_add_nsecond(column_ts, ttl_expr.nsecond_, 0, expire_ts))) {
        LOG_WARN("fail to add nsecond", K(ret), K(column_ts), K(ttl_expr.nsecond_));
      } else if (ttl_expr.nsecond_ > 0 && OB_FAIL(ObTimeConverter::date_add_nmonth(column_ts, ttl_expr.nmonth_, expire_ts, true))) {
        LOG_WARN("fail to add month", K(ret), K(column_ts), K(ttl_expr.nmonth_));
      } else if (expire_ts <= cur_ts) {
        is_expired = true;
      }
    }
  }
  return ret;
}

int ObTTLParam::add_ttl_info(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSimpleTTLInfo info(tenant_id);
  if (OB_FAIL(ttl_info_array_.push_back(info))) {
    LOG_WARN("fail to push_back", K(ret), K(info));
  }
  return ret;
}

int ObTTLUtil::get_ttl_info(const ObTTLParam &param, ObIArray<ObSimpleTTLInfo> &ttl_info_array)
{
  int ret = OB_SUCCESS;

  ObArray<ObSimpleTTLInfo> tmp_info_array;
  if (param.ttl_all_) {
    if (OB_FAIL(get_all_user_tenant_ttl(tmp_info_array))) {
      LOG_WARN("fail to get all tenant ttl info", KR(ret));
    }
  } else {
    if (OB_FAIL(tmp_info_array.assign(param.ttl_info_array_))) {
      LOG_WARN("fail to assign", K(param), KR(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (tmp_info_array.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ttl info array should not be empty", KR(ret), K(param));
  } else {
    const int64_t info_cnt = tmp_info_array.count();
    for (int64_t i = 0; OB_SUCC(ret) && (i < info_cnt); ++i) {
      share::ObAllTenantInfo tenant_info;
      bool is_restore = false;
      const uint64_t tenant_id = tmp_info_array.at(i).tenant_id_;
      if (OB_FAIL(share::schema::ObMultiVersionSchemaService::get_instance().
                  check_tenant_is_restore(NULL, tenant_id, is_restore))) {
        LOG_WARN("fail to check tenant is restore", KR(ret), K(i), "ttl_info", tmp_info_array.at(i));
      } else if (is_restore) {
        LOG_INFO("skip restoring tenant to do ttl task", K(tenant_id));
      } else if (OB_FAIL(share::ObAllTenantInfoProxy::load_tenant_info(tenant_id, GCTX.sql_proxy_,
                                                                false, tenant_info))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS; // ignore ret, so as to process the next tenant
          LOG_WARN("tenant may be deleted, skip ttl task for this tenant", K(tenant_id));
        } else {
          LOG_WARN("fail to load tenant info", KR(ret), K(tenant_id));
        }
      } else if (tenant_info.is_standby()) { // Skip major freeze for standby tenants
        LOG_INFO("skip do ttl task for standby tenant", K(tenant_info));
      } else if (OB_FAIL(ttl_info_array.push_back(tmp_info_array.at(i)))) {
        LOG_WARN("fail to push back ttl info", KR(ret), K(i), "ttl_info", tmp_info_array.at(i));
      }
    }
  }

  return ret;
}

int ObTTLUtil::dispatch_one_tenant_ttl(obrpc::ObTTLRequestArg::TTLRequestType type,
                                       const rpc::frame::ObReqTransport &transport,
                                       const ObSimpleTTLInfo &ttl_info)
{
  int ret = OB_SUCCESS;
  if (!ttl_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ttl_info));
  } else {
    const int64_t launch_start_time = ObTimeUtility::current_time();
    obrpc::ObSrvRpcProxy proxy;
    ObAddr leader;
    obrpc::ObTTLRequestArg req;
    obrpc::ObTTLResponseArg resp;
    uint64_t tenant_id = ttl_info.tenant_id_;
    req.tenant_id_ = tenant_id;
    req.cmd_code_ = type;
    req.trigger_type_ = TRIGGER_TYPE::USER_TRIGGER;
    if (OB_ISNULL(GCTX.location_service_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid GCTX", KR(ret));
    } else if (OB_FAIL(proxy.init(&transport))) {
      LOG_WARN("fail to init", KR(ret));
    } else {
      const int64_t MAX_RETRY_COUNT = 5;
      bool ttl_done = false;
      static const int64_t MAX_PROCESS_TIME_US = 10 * 1000 * 1000L;
      for (int64_t i = 0; OB_SUCC(ret) && (!ttl_done) && (i < MAX_RETRY_COUNT); ++i) {
        if (OB_FAIL(GCTX.location_service_->get_leader_with_retry_until_timeout(GCONF.cluster_id,
                    tenant_id, share::SYS_LS, leader))) {
          LOG_WARN("fail to get ls locaiton leader", KR(ret), K(tenant_id));
        } else if (OB_FAIL(proxy.to(leader)
                                .trace_time(true)
                                .max_process_handler_time(MAX_PROCESS_TIME_US)
                                .by(tenant_id)
                                .dst_cluster_id(GCONF.cluster_id)
                                .dispatch_ttl(req, resp))) {
          LOG_WARN("tenant ttl rpc failed", KR(ret), K(tenant_id), K(leader), K(ttl_info));
        } else if (FALSE_IT(ret = resp.err_code_)) {
        } else if (OB_FAIL(ret)) {
          if (OB_LEADER_NOT_EXIST == ret || OB_EAGAIN == ret) {
            const int64_t RESERVED_TIME_US = 600 * 1000; // 600 ms
            const int64_t timeout_remain_us = THIS_WORKER.get_timeout_remain();
            const int64_t idle_time_us = 200 * 1000 * (i + 1);
            if (timeout_remain_us - idle_time_us > RESERVED_TIME_US) {
              LOG_WARN("leader may switch or ddl confilict, will retry", KR(ret), K(tenant_id), K(ttl_info),
                "ori_leader", leader, K(timeout_remain_us), K(idle_time_us), K(RESERVED_TIME_US));
              USLEEP(static_cast<int>(idle_time_us));
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("leader may switch or ddl confilict, will not retry cuz timeout_remain is "
                "not enough", KR(ret), K(tenant_id), K(ttl_info), "ori_leader", leader,
                K(timeout_remain_us), K(idle_time_us), K(RESERVED_TIME_US));
            }
          }
        } else {
          ttl_done = true;
        }
      }

      if (OB_SUCC(ret) && !ttl_done) {
        ret = OB_EAGAIN;
        LOG_WARN("fail to retry ttl cuz switching role", KR(ret), K(MAX_RETRY_COUNT));
      }
    }

    const int64_t launch_cost_time = ObTimeUtility::current_time() - launch_start_time;
    LOG_INFO("do tenant ttl", KR(ret), K(tenant_id), K(leader), K(ttl_info), K(launch_cost_time));
  }
  return ret;
}

int ObTTLUtil::get_all_user_tenant_ttl(ObIArray<ObSimpleTTLInfo> &ttl_info_array)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 32> tenant_ids;
  if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid GCTX", KR(ret));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_ids(tenant_ids))) {
    LOG_WARN("fail to get tenant ids", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
    if (is_user_tenant(tenant_ids[i])) {
      ObSimpleTTLInfo info(tenant_ids[i]);
      if(OB_FAIL(ttl_info_array.push_back(info))) {
        LOG_WARN("fail to push back", KR(ret), "tenant_id", tenant_ids[i]);
      }
    }
  }
  return ret;
}

int ObTTLUtil::get_tenant_table_ids(const uint64_t tenant_id, ObIArray<uint64_t> &table_id_array)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_ids_in_tenant(tenant_id, table_id_array))) {
    LOG_WARN("fail to get table ids in tenant", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObTTLUtil::check_is_ttl_table(const ObTableSchema &table_schema, bool &is_ttl_table)
{
  int ret = OB_SUCCESS;
  is_ttl_table = false;
  if (table_schema.is_user_table() && !table_schema.is_in_recyclebin() &&
    (!table_schema.get_kv_attributes().empty() || !table_schema.get_ttl_definition().empty())) {
    is_ttl_table = true;
  }
  return ret;
}

int ObTTLUtil::check_ttl_task_exists(uint64_t tenant_id, common::ObISQLClient& proxy,
                                     const uint64_t& task_id, const uint64_t& table_id,
                                     ObTabletID& tablet_id, bool &is_exists)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  uint64_t result_cnt = 0;
  if (OB_FAIL(sql.assign_fmt("SELECT (SELECT COUNT(*) FROM %s WHERE table_id = %ld"
    " AND tablet_id = %ld AND task_id = %ld) + (SELECT COUNT(*) FROM %s WHERE"
    " table_id = %ld AND tablet_id = %ld AND task_id = %ld) AS cnt",
      share::OB_ALL_KV_TTL_TASK_HISTORY_TNAME, table_id, tablet_id.id(), task_id,
      share::OB_ALL_KV_TTL_TASK_TNAME, table_id, tablet_id.id(), task_id))) {
    LOG_WARN("sql assign fmt failed", K(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult* result = nullptr;
      if (OB_FAIL(proxy.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, query result must not be NULL", K(ret));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("fail to get next row", K(ret));
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "cnt", result_cnt, uint64_t);
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (result_cnt > 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected ttl task record count", KR(ret), K(tenant_id), K(task_id), K(table_id), K(tablet_id));
    } else {
      is_exists = (result_cnt > 0);
    }
  }

  return ret;
}


} // end namespace rootserver
} // end namespace oceanbase