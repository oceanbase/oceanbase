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

#ifndef OCEANBASE_ROUTINE_LOAD_DBMS_SCHED_UTIL_H_
#define OCEANBASE_ROUTINE_LOAD_DBMS_SCHED_UTIL_H_

#include "lib/ob_define.h"
#include "sql/parser/parse_node.h"
#include "share/scn.h"
#include "share/schema/ob_mview_info.h"
#include "share/schema/ob_mlog_info.h"

namespace oceanbase
{

namespace dbms_scheduler
{
class ObDBMSSchedJobInfo;
}
namespace storage
{
class ObRoutineLoadSchedUtil
{
public:
  static int create_routine_load_sched_job(common::ObISQLClient &trans,
                                           const uint64_t tenant_id,
                                           const int64_t job_id,
                                           const ObString &job_name,
                                           const ObString &job_action,
                                           const int64_t create_time,
                                           const ObString &exec_env,
                                           const ObString &repeat_interval);

public:
  template<typename T>
  static int split_string_to_array(const ObString &input_str,
                                   const char delimiter,
                                   ObIArray<T> &result_array);
  static int parser_and_check_kafka_partitions(const ObString &input_str,
                                               const char delimiter,
                                               int64_t &partition_cnt);
  static int parser_and_check_kafka_offsets(const ObString &input_str,
                                            const char delimiter,
                                            int64_t &partition_cnt);
  static bool is_valid_datetime_str(const ObString &time_str);

public:
  static constexpr const char* FAST_REPEAT_INTERVAL = "FREQ=SECONDLY;INTERVAL=1";
  static constexpr const char* SLOW_REPEAT_INTERVAL = "FREQ=SECONDLY;INTERVAL=5";

};
} // namespace storage
} // namespace oceanbase

#endif