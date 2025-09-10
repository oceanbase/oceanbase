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

class ObEvolutionRecords {
public:
  ObEvolutionRecords() {
    reset();
  }

  ObEvolutionRecords(const ObEvolutionRecords &other) {
    MEMCPY(use_evo_plan_, other.use_evo_plan_, RECORDS_SIZE * sizeof(bool));
    MEMCPY(receive_ts_, other.receive_ts_, RECORDS_SIZE * sizeof(int64_t));
    MEMCPY(elapsed_t_, other.elapsed_t_, RECORDS_SIZE * sizeof(int64_t));
    ref_count_ = other.ref_count_;
  }

  void set_record_for_get_plan(const int64_t evo_idx, const bool use_evo_plan, const int64_t receive_ts)
  {
    if (evo_idx < RECORDS_SIZE) {
      use_evo_plan_[evo_idx] = use_evo_plan;
      receive_ts_[evo_idx] = receive_ts;
    }
  }

  void set_record_for_finish_plan(const int64_t receive_ts, const int64_t elapsed_t)
  {
    bool find = false;
    for (int i = 0; !find && i < RECORDS_SIZE; ++i) {
      if (receive_ts == receive_ts_[i]) {
        elapsed_t_[i] = elapsed_t;
      }
    }
  }

  void reset()
  {
    MEMSET(use_evo_plan_, 0, RECORDS_SIZE * sizeof(bool));
    MEMSET(receive_ts_, 0, RECORDS_SIZE * sizeof(int64_t));
    MEMSET(elapsed_t_, 0, RECORDS_SIZE * sizeof(int64_t));
    ref_count_ = 0;
  }

  bool check_executing_evo_plan_exists()
  {
    bool find = false;
    for (int i = 0; !find && i < RECORDS_SIZE && 0 != receive_ts_[i]; ++i) {
      if (true == use_evo_plan_[i] && 0 == elapsed_t_[i]) {
        find = true;
      }
    }
    return find;
  }

  int32_t get_serialize_size() const {  return 4 + 8 + RECORDS_SIZE * 9; }
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const {
    int ret = OB_SUCCESS;
    pos = 0;
    int32_t version = 1;
    uint8_t use_evo_plan = 0;
    uint32_t receive_ts = 0;
    uint32_t elapsed_t = 0;
    if (OB_FAIL(common::serialization::encode_i32(buf, buf_len, pos, version))) {
    } else if (OB_FAIL(common::serialization::encode_i64(buf, buf_len, pos, receive_ts_[0]))) {
    }
    for (int i = 0; OB_SUCC(ret) && i < RECORDS_SIZE; ++i) {
      use_evo_plan = use_evo_plan_[i] > 0 ? 1 : 0;
      receive_ts = (0 < i && receive_ts_[i] > receive_ts_[0]) ? (uint32_t)((receive_ts_[i] - receive_ts_[0]) & 0xFFFFFFFF) : 0;
      elapsed_t = UINT32_MAX < elapsed_t_[i] ? UINT32_MAX : elapsed_t_[i];
      if (OB_FAIL(common::serialization::encode_i8(buf, buf_len, pos, use_evo_plan))) {
      } else if (OB_FAIL(common::serialization::encode_i32(buf, buf_len, pos, receive_ts))) {
      } else if (OB_FAIL(common::serialization::encode_i32(buf, buf_len, pos, elapsed_t))) {
      }
    }
    return ret;
  }
  void inc_ref_count()  { ATOMIC_INC(&ref_count_);  }
  void dec_ref_count()  { ATOMIC_DEC(&ref_count_);  }
  bool is_referenced() const { return ATOMIC_LOAD(&ref_count_) > 0; }

  DECLARE_TO_STRING {
    int64_t pos = 0;
    int ret = OB_SUCCESS;
    J_OBJ_START();
    J_ARRAY_START();
    for (int i = 0; OB_SUCC(ret) && i < RECORDS_SIZE; ++i) {
      J_OBJ_START();
      BUF_PRINTO(use_evo_plan_[i]);
      J_COMMA();
      BUF_PRINTO(receive_ts_[i]);
      J_COMMA();
      BUF_PRINTO(elapsed_t_[i]);
      J_OBJ_END();
      if (i + 1 < RECORDS_SIZE) {
        J_COMMA();
      }
    }
    J_ARRAY_END();
    J_OBJ_END();
    return pos;
  }

private:
  static const int64_t RECORDS_SIZE = 150;
  bool use_evo_plan_[RECORDS_SIZE];
  int64_t receive_ts_[RECORDS_SIZE];
  int64_t elapsed_t_[RECORDS_SIZE];
  int64_t ref_count_;
};

class ObEvoRecordsGuard {
public:
  ObEvoRecordsGuard()
  : evo_records_(NULL)
  {}
  ~ObEvoRecordsGuard() { reset(); }

  void set_evo_records(ObEvolutionRecords *evo_records)  {
    reset();
    if (NULL != evo_records)  {
      evo_records->inc_ref_count();
      evo_records_ = evo_records;
    }
  }

  void reset()  {
    if (NULL != evo_records_) {
      evo_records_->dec_ref_count();
      evo_records_ = NULL;
    }
  }
  ObEvolutionRecords *get_evo_records() { return evo_records_;  }
  TO_STRING_KV(KPC_(evo_records));
private:
  ObEvolutionRecords *evo_records_;
};

class ObEvolutionStat
{
public:
  ObEvolutionStat()
  : executions_(0),
    cpu_time_(0),
    elapsed_time_(0),
    error_cnt_(0),
    last_exec_ts_(0),
    records_(NULL)
  {}
  ObEvolutionStat(const ObEvolutionStat &other)
  : executions_(other.executions_),
    cpu_time_(other.cpu_time_),
    elapsed_time_(other.elapsed_time_),
    error_cnt_(other.error_cnt_),
    last_exec_ts_(other.last_exec_ts_),
    records_(NULL)
  {}
  virtual ~ObEvolutionStat() {}
  inline void reset()
  {
    executions_ = 0;
    cpu_time_ = 0;
    elapsed_time_ = 0;
    error_cnt_ = 0;
    last_exec_ts_ = 0;
    records_ = NULL;
  }
  inline ObEvolutionStat& operator=(const ObEvolutionStat &other)
  {
    if (this != &other) {
      executions_ = other.executions_;
      cpu_time_ = other.cpu_time_;
      elapsed_time_ = other.elapsed_time_;
      error_cnt_ = other.error_cnt_;
      last_exec_ts_ = other.last_exec_ts_;
      records_ = NULL;
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
  ObEvolutionRecords *records_;
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
