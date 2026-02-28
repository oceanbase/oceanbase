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

#include "observer/dbms_scheduler/ob_dbms_sched_job_utils.h"
#include "storage/routine_load/ob_routine_load_dbms_sched_util.h"

namespace oceanbase
{
using namespace dbms_scheduler;
using namespace sql;
namespace storage
{
int ObRoutineLoadSchedUtil::create_routine_load_sched_job(
    common::ObISQLClient &trans,
    const uint64_t tenant_id,
    const int64_t job_id,
    const ObString &job_name,
    const ObString &job_action,
    const int64_t create_time,
    const ObString &exec_env,
    const ObString &repeat_interval_str)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("must be user tenant", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(OB_INVALID_ID == job_id
                         || job_name.empty()
                         || job_action.empty()
                         || OB_INVALID_TIMESTAMP == create_time)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid job attr", KR(ret), K(job_id), K(job_name), K(job_action), K(create_time));
  } else {
    HEAP_VAR(dbms_scheduler::ObDBMSSchedJobInfo, job_info) {
      job_info.tenant_id_ = tenant_id;
      //和routine load job共用一个job id
      job_info.job_ = job_id;
      job_info.job_name_ = job_name;
      job_info.job_action_ = job_action;
      job_info.lowner_ = ObString("oceanbase");
      job_info.cowner_ = ObString("oceanbase");
      job_info.powner_ = lib::is_oracle_mode() ? ObString("SYS") : ObString("root@%");
      job_info.job_style_ = ObString("REGULAR");
      job_info.job_type_ = ObString("PLSQL_BLOCK");
      job_info.job_class_ = ObString("DEFAULT_JOB_CLASS");
      job_info.start_date_ = create_time;
      job_info.end_date_ = 64060560000000000; // 4000-01-01
      //设置为不间断执行（1秒间隔）
      job_info.repeat_interval_ = repeat_interval_str;
      job_info.enabled_ = true;
      job_info.auto_drop_ = false;
      job_info.max_run_duration_ = 24 * 60 * 60; // set to 1 day
      job_info.exec_env_ = exec_env;
      job_info.comments_ = ObString("used to load data from kafka periodically");
      job_info.func_type_ = dbms_scheduler::ObDBMSSchedFuncType::ROUTINE_LOAD_KAFKA_JOB;

      if (OB_FAIL(dbms_scheduler::ObDBMSSchedJobUtils::create_dbms_sched_job(trans, tenant_id, job_id, job_info))) {
        LOG_WARN("fail to create routine load dbms sched job", KR(ret), K(job_info));
      } else {
        LOG_INFO("finish create routine load dbms sched job", K(job_info));
      }
    } else {
      LOG_WARN("fail to alloc dbms_scheduled_job_info for routine load kafka", KR(ret));
    }
  }

  return ret;
}

template<typename T>
int ObRoutineLoadSchedUtil::split_string_to_array(
    const ObString &input_str,
    const char delimiter,
    ObIArray<T> &result_array)
{
  int ret = OB_SUCCESS;
  result_array.reset();
  if (OB_UNLIKELY(input_str.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(input_str));
  } else {
    ObString remain_str = input_str;
    ObString item_str;

    while (OB_SUCC(ret) && !remain_str.empty()) {
      item_str = remain_str.split_on(delimiter).trim_space_only();
      if (item_str.empty()) {
        // 如果分割后为空，说明没有找到分隔符，直接处理剩余字符串
        item_str = remain_str.trim_space_only();
        remain_str.reset();
      }

      if (OB_SUCC(ret)) {
        char *end_ptr = NULL;
        if (std::is_same<T, int64_t>::value) {
          bool valid = false;
          int64_t value = ObFastAtoi<int64_t>::atoi(item_str.ptr(), item_str.ptr() + item_str.length(), valid);
          if (!valid) {
            ret = OB_INVALID_DATA;
            LOG_WARN("fail to convert string to int64_t", KR(ret), K(item_str), K(value));
          } else if (OB_FAIL(result_array.push_back(value))) {
            LOG_WARN("fail to push back to array", KR(ret), K(value));
          }
        } else if (std::is_same<T, int32_t>::value) {
          bool valid = false;
          int32_t value = ObFastAtoi<int32_t>::atoi(item_str.ptr(), item_str.ptr() + item_str.length(), valid);
          if (!valid) {
            ret = OB_INVALID_DATA;
            LOG_WARN("fail to convert string to int32_t", KR(ret), K(item_str), K(value));
          } else if (OB_FAIL(result_array.push_back(value))) {
            LOG_WARN("fail to push back to array", KR(ret), K(value));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected type", KR(ret), K(item_str));
        }
      }
    }
  }
  return ret;
}

int ObRoutineLoadSchedUtil::parser_and_check_kafka_partitions(
    const ObString &input_str,
    const char delimiter,
    int64_t &partition_cnt)
{
  int ret = OB_SUCCESS;
  partition_cnt = 0;
  if (OB_UNLIKELY(input_str.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(input_str));
  } else {
    ObString remain_str = input_str;
    ObString item_str;

    while (OB_SUCC(ret) && !remain_str.empty()) {
      char *end_ptr = NULL;
      item_str = remain_str.split_on(delimiter).trim_space_only();
      if (item_str.empty()) {
        // 如果分割后为空，说明没有找到分隔符，直接处理剩余字符串
        item_str = remain_str.trim_space_only();
        remain_str.reset();
      }
      if (OB_UNLIKELY(!item_str.is_numeric())) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid kafka partitions", KR(ret), K(item_str));
      } else {
        partition_cnt++;
      }
    }
    if (OB_UNLIKELY(0 >= partition_cnt)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid kafka partitions", KR(ret), K(input_str));
    }
  }
  return ret;
}

//NOTE: "kafka_offsets" = "101,0,OFFSET_BEGINNING,OFFSET_END"或者"kafka_offsets" = "2021-05-22 11:00:00,2021-05-22 11:00:00"
//      时间格式不能和 OFFSET 格式混用。
int ObRoutineLoadSchedUtil::parser_and_check_kafka_offsets(
    const ObString &input_str,
    const char delimiter,
    int64_t &partition_cnt)
{
  int ret = OB_SUCCESS;
  partition_cnt = 0;
  static const char* OFFSET_BEGINNING = "OFFSET_BEGINNING";
  static const char* OFFSET_END = "OFFSET_END";
  int64_t normal_format_cnt = 0;
  int64_t time_format_cnt = 0;
  if (OB_UNLIKELY(input_str.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(input_str));
  } else {
    ObString remain_str = input_str;
    ObString item_str;

    while (OB_SUCC(ret) && !remain_str.empty()) {
      char *end_ptr = NULL;
      item_str = remain_str.split_on(delimiter).trim_space_only();
      if (item_str.empty()) {
        // 如果分割后为空，说明没有找到分隔符，直接处理剩余字符串
        item_str = remain_str.trim_space_only();
        remain_str.reset();
      }
      if (item_str.is_numeric()) {
        normal_format_cnt++;
      } else if (0 == item_str.case_compare(OFFSET_BEGINNING)) {
        normal_format_cnt++;
      } else if (0 == item_str.case_compare(OFFSET_END)) {
        normal_format_cnt++;
      //TODO: 现在不支持时间格式
      // } else if (is_valid_datetime_str(item_str)) {
      //   time_format_cnt++;
      } else {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid kafka offsets", KR(ret), K(item_str));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(0 < normal_format_cnt && 0 < time_format_cnt)) {
        ret = OB_INVALID_DATA;
        LOG_WARN("invalid kafka offsets", KR(ret), K(normal_format_cnt), K(time_format_cnt));
      } else {
        partition_cnt = normal_format_cnt > 0 ? normal_format_cnt : time_format_cnt;
        if (OB_UNLIKELY(0 >= partition_cnt)) {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid kafka offsets", KR(ret), K(normal_format_cnt), K(time_format_cnt));
        }
      }
    }
  }
  return ret;
}

bool ObRoutineLoadSchedUtil::is_valid_datetime_str(const ObString &time_str)
{
  int64_t datetime_value = 0;
  bool is_valid = false;
  ObTimeConvertCtx cvrt_ctx(NULL, false);
  int ret = ObTimeConverter::str_to_datetime(time_str, cvrt_ctx, datetime_value);
  if (OB_SUCCESS == ret) {
    is_valid = ObTimeConverter::is_valid_datetime(datetime_value);
  }
  return is_valid;
}

// Explicit template instantiations
template int ObRoutineLoadSchedUtil::split_string_to_array<int32_t>(
    const ObString &input_str,
    const char delimiter,
    ObIArray<int32_t> &result_array);

template int ObRoutineLoadSchedUtil::split_string_to_array<int64_t>(
    const ObString &input_str,
    const char delimiter,
    ObIArray<int64_t> &result_array);

}
}