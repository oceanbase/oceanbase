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

#define USING_LOG_PREFIX PL

#include "pl/sys_package/ob_dbms_routine_load_mysql.h"
#include "storage/routine_load/ob_routine_load_consume_executor.h"

namespace oceanbase
{
namespace pl
{
using namespace common;
using namespace sql;
using namespace storage;

/*
PROCEDURE consume_kafka(
    IN     job_id                  BIGINT,
    IN     partition_pos           INT,
    IN     partition_len           INT,
    IN     offsets_pos             INT,
    IN     offsets_len             INT,
    IN     job_name                VARCHAR(128),
    IN     exec_sql                VARCHAR(65535));
*/
int ObDBMSRoutineLoadMysql::consume_kafka(ObExecContext &ctx, ParamStore &params, ObObj &result)
{
  UNUSED(result);
  int ret = OB_SUCCESS;
  if (7 != params.count()
     || !params.at(0).is_int()    // job_id is BIGINT
     || !params.at(1).is_int32()
     || !params.at(2).is_int32()
     || !params.at(3).is_int32()
     || !params.at(4).is_int32()
     || !params.at(5).is_varchar()
     || !params.at(6).is_varchar()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for routine load consume kafka", KR(ret));
  }
  if (OB_SUCC(ret)) {
    ObRoutineLoadConsumeArg consume_params;
    ObRoutineLoadConsumeExecutor consume_executor;
    consume_params.job_id_ = params.at(0).get_int();  // Use get_int() for BIGINT
    consume_params.par_str_pos_ = params.at(1).get_int32();
    consume_params.par_str_len_ = params.at(2).get_int32();
    consume_params.off_str_pos_ = params.at(3).get_int32();
    consume_params.off_str_len_ = params.at(4).get_int32();
    consume_params.job_name_ = params.at(5).get_varchar();
    consume_params.exec_sql_ = params.at(6).get_varchar();
    if (OB_FAIL(consume_executor.execute(ctx, consume_params))) {
      LOG_WARN("fail to execute kafka consume", KR(ret), K(consume_params));
    }
  }
  return ret;
}

} // namespace pl
} // namespace oceanbase
