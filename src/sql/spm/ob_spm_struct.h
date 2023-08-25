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

#ifndef OCEANBASE_SQL_SPM_OB_SPM_STRUCT_H_
#define OCEANBASE_SQL_SPM_OB_SPM_STRUCT_H_
#include "share/ob_define.h"

namespace oceanbase
{
namespace sql
{

class ObEvolutionStat
{
public:
  ObEvolutionStat()
  : executions_(0),
    cpu_time_(0),
    elapsed_time_(0),
    error_cnt_(0),
    last_exec_ts_(0)
  {}
  ObEvolutionStat(const ObEvolutionStat &other)
  : executions_(other.executions_),
    cpu_time_(other.cpu_time_),
    elapsed_time_(other.elapsed_time_),
    error_cnt_(other.error_cnt_),
    last_exec_ts_(other.last_exec_ts_)
  {}
  virtual ~ObEvolutionStat() {}
  inline void reset()
  {
    executions_ = 0;
    cpu_time_ = 0;
    elapsed_time_ = 0;
    error_cnt_ = 0;
    last_exec_ts_ = 0;
  }
  inline ObEvolutionStat& operator=(const ObEvolutionStat &other)
  {
    if (this != &other) {
      executions_ = other.executions_;
      cpu_time_ = other.cpu_time_;
      elapsed_time_ = other.elapsed_time_;
      error_cnt_ = other.error_cnt_;
      last_exec_ts_ = other.last_exec_ts_;
    }
    return *this;
  }
  TO_STRING_KV(K_(executions), K_(cpu_time), K_(elapsed_time), K_(error_cnt), K_(last_exec_ts));
public:
  int64_t  executions_;       // The total number of executions in the evolution process
  int64_t  cpu_time_;         // The total CPU time consumed during the evolution process
  int64_t elapsed_time_;
  int64_t error_cnt_;
  int64_t last_exec_ts_;
};

struct AlterPlanBaselineArg
{
public:
  AlterPlanBaselineArg()
  : sql_id_(),
    with_plan_hash_(false),
    plan_hash_(0),
    attr_name_(),
    attr_value_(),
    alter_flag_(false)
  {}

  ObString sql_id_;
  bool with_plan_hash_;
  uint64_t plan_hash_;
  ObString attr_name_;
  ObString attr_value_;
  bool alter_flag_;
};

} // namespace sql end
} // namespace oceanbase end

#endif
