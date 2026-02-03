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

#ifndef OCEANBASE_ROUTINE_LOAD_EXECUTOR_H_
#define OCEANBASE_ROUTINE_LOAD_EXECUTOR_H_

#include "sql/engine/ob_exec_context.h"


namespace oceanbase
{
namespace sql
{

class ObCreateRoutineLoadStmt;
class ObPauseRoutineLoadStmt;
class ObResumeRoutineLoadStmt;
class ObStopRoutineLoadStmt;

class ObCreateRoutineLoadExecutor
{
public:
  ObCreateRoutineLoadExecutor() {}
  virtual ~ObCreateRoutineLoadExecutor() {}
  int execute(ObExecContext &ctx, ObCreateRoutineLoadStmt &stmt);
private:
  int build_sql_(const int64_t job_id,
                 ObCreateRoutineLoadStmt &stmt,
                 common::ObSqlString &sql,
                 uint32_t &par_str_pos,
                 uint32_t &off_str_pos,
                 uint32_t &par_str_len,
                 uint32_t &off_str_len);
  int add_kafka_partitions_and_offsets_str_(const ObCreateRoutineLoadStmt &stmt,
                                            common::ObSqlString &sql,
                                            uint32_t &par_str_pos,
                                            uint32_t &off_str_pos,
                                            uint32_t &par_str_len,
                                            uint32_t &off_str_len);
  int check_kafka_partitions_and_offsets_(ObCreateRoutineLoadStmt &stmt);
  int add_dest_table_info_(const ObCreateRoutineLoadStmt &stmt, ObSqlString &sql);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObCreateRoutineLoadExecutor);
  // function members
private:
  // data members
};

class ObPauseRoutineLoadExecutor
{
public:
  ObPauseRoutineLoadExecutor() {}
  virtual ~ObPauseRoutineLoadExecutor() {}
  int execute(ObExecContext &ctx, ObPauseRoutineLoadStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObPauseRoutineLoadExecutor);
};

class ObResumeRoutineLoadExecutor
{
public:
  ObResumeRoutineLoadExecutor() {}
  virtual ~ObResumeRoutineLoadExecutor() {}
  int execute(ObExecContext &ctx, ObResumeRoutineLoadStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObResumeRoutineLoadExecutor);
};

class ObStopRoutineLoadExecutor
{
public:
  ObStopRoutineLoadExecutor() {}
  virtual ~ObStopRoutineLoadExecutor() {}
  int execute(ObExecContext &ctx, ObStopRoutineLoadStmt &stmt);
private:
  DISALLOW_COPY_AND_ASSIGN(ObStopRoutineLoadExecutor);
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_ROUTINE_LOAD_EXECUTOR_H_ */
