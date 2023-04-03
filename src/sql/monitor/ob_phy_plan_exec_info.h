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

#ifdef PLAN_MONITOR_INFO_DEF
PLAN_MONITOR_INFO_DEF(QUERY_TYPE, query_type)
PLAN_MONITOR_INFO_DEF(TOTAL_WAIT_TIME, total_wait_time)
PLAN_MONITOR_INFO_DEF(TOTAL_WAIT_COUNT, total_wait_count)
//timestamp
PLAN_MONITOR_INFO_DEF(RPC_SEND_TIME, rpc_send_time)
PLAN_MONITOR_INFO_DEF(RECEIVE_TIME, receive_time)
PLAN_MONITOR_INFO_DEF(ENTER_QUEUE_TIME, enter_queue_time)
PLAN_MONITOR_INFO_DEF(RUN_TIME, run_time)
PLAN_MONITOR_INFO_DEF(BEFORE_PROCESS_TIME, before_process_time)
PLAN_MONITOR_INFO_DEF(SINGLE_PROCESS_TIME, single_process_time)
PLAN_MONITOR_INFO_DEF(PROCESS_EXECUTOR_TIME, process_executor_time)
PLAN_MONITOR_INFO_DEF(EXECUTOR_END_TIME, executor_end_time)
#endif

#ifndef OCEANBASE_SQL_OB_PHY_PLAN_EXEC_INFO_H
#define OCEANBASE_SQL_OB_PHY_PLAN_EXEC_INFO_H
#include "share/ob_time_utility2.h"
#include "sql/monitor/ob_phy_operator_monitor_info.h"
#include "sql/monitor/ob_exec_stat.h"
namespace oceanbase
{
namespace sql
{
enum ObPlanMonitorInfoIds
{
#define PLAN_MONITOR_INFO_DEF(def, name) def,
#include "ob_phy_plan_exec_info.h"
#undef PLAN_MONITOR_INFO_DEF
#define EVENT_INFO(def, name) def,
#include "ob_exec_stat.h"
#undef EVENT_INFO
MAX_EVENT_ID
};

static const MonitorName OB_PLAN_MONITOR_INFOS[] = {
#define PLAN_MONITOR_INFO_DEF(def, name) {def, #name},
#include "ob_phy_plan_exec_info.h"
#undef PLAN_MONITOR_INFO_DEF
#define EVENT_INFO(def, name) {def, #name},
#include "ob_exec_stat.h"
#undef EVENT_INFO
{MAX_EVENT_ID, "max_event"}
};

class ObPhyPlanExecInfo final : public ObPhyOperatorMonitorInfo
{
public:
  ObPhyPlanExecInfo() {}
  inline virtual int64_t print_info(char *buf, int64_t buf_len) const;
  inline int add_exec_record(const ObExecRecord &exec_record);
  inline int add_exec_timestamp(const ObExecTimestamp &exec_timestamp);
  int64_t to_string(char *buf, int64_t buf_len) const
  {
    return print_info(buf, buf_len);
  }
private:
  void set_value(ObPlanMonitorInfoIds index, int64_t value)
  {
    plan_info_array_[index] = value;
  }
  bool is_timestamp(int64_t index) const
  {
    return (index >= RPC_SEND_TIME) && (index <= EXECUTOR_END_TIME);
  }
  DISALLOW_COPY_AND_ASSIGN(ObPhyPlanExecInfo);
private:
  common::ObWaitEventDesc max_wait_event_;
  int64_t plan_info_array_[MAX_EVENT_ID];
};

int64_t ObPhyPlanExecInfo::print_info(char *buf, int64_t buf_len) const
{
  int64_t pos = 0;
  const int64_t time_buf_len = 128;
  char timebuf[time_buf_len];
  int64_t time_buf_pos = 0;

  J_OBJ_START();
  J_KV(N_MAX_WAIT_EVENT, max_wait_event_);
  J_OBJ_END();
  J_COMMA();
  bool first_cell = true;
  for (int64_t i = 0; i < MAX_EVENT_ID; i++) {
    time_buf_pos = 0;
    if (plan_info_array_[i] != 0) {
      if (first_cell) {
        first_cell = false;
      } else {
        J_COMMA();
      }
      J_OBJ_START();
      if (is_timestamp(i)) {
        if (common::OB_SUCCESS != share::ObTimeUtility2::usec_to_str(plan_info_array_[i], timebuf, time_buf_len, time_buf_pos)) {
          SQL_MONITOR_LOG_RET(WARN, common::OB_ERR_UNEXPECTED, "fail to print time as str", K(i));
          J_KV(OB_PLAN_MONITOR_INFOS[i].info_name_, plan_info_array_[i]);
        } else {
          timebuf[time_buf_pos] = '\0';
          J_KV(OB_PLAN_MONITOR_INFOS[i].info_name_, timebuf);
        }

      } else {
        J_KV(OB_PLAN_MONITOR_INFOS[i].info_name_, plan_info_array_[i]);
      }
      J_OBJ_END();
    }
  }
  return pos;
}

int ObPhyPlanExecInfo::add_exec_record(const ObExecRecord &exec_record)
{
  int ret = common::OB_SUCCESS;
  op_id_ = -1;
  job_id_ = -1;
  task_id_ = -1;
  max_wait_event_ = exec_record.max_wait_event_;
  set_value(TOTAL_WAIT_TIME, exec_record.wait_time_end_);
  set_value(TOTAL_WAIT_COUNT, exec_record.wait_count_end_);

#define EVENT_INFO(def, name) \
  set_value(def, exec_record.get_##name());
#include "ob_exec_stat.h"
#undef EVENT_INFO
  return ret;
}
int ObPhyPlanExecInfo::add_exec_timestamp(const ObExecTimestamp &exec_timestamp)
{
  int ret = common::OB_SUCCESS;
  SQL_MONITOR_LOG(DEBUG, "add exec timestamp", K(exec_timestamp.exec_type_),
           K(exec_timestamp.before_process_ts_),
           K(exec_timestamp.process_executor_ts_),
           K(exec_timestamp.executor_end_ts_),
           K(exec_timestamp.receive_ts_),
           K(exec_timestamp.enter_queue_ts_),
           K(exec_timestamp.run_ts_),
           K(exec_timestamp.single_process_ts_));
  set_value(QUERY_TYPE, exec_timestamp.exec_type_);
  set_value(BEFORE_PROCESS_TIME, exec_timestamp.before_process_ts_);
  set_value(PROCESS_EXECUTOR_TIME, exec_timestamp.process_executor_ts_);
  set_value(EXECUTOR_END_TIME, exec_timestamp.executor_end_ts_);
  if (InnerSql != exec_timestamp.exec_type_) {
    set_value(RECEIVE_TIME, exec_timestamp.receive_ts_);
    set_value(ENTER_QUEUE_TIME, exec_timestamp.enter_queue_ts_);
    set_value(RUN_TIME, exec_timestamp.run_ts_);
  }
  if (MpQuery == exec_timestamp.exec_type_
        || PSCursor == exec_timestamp.exec_type_
        || DbmsCursor == exec_timestamp.exec_type_
        || CursorFetch == exec_timestamp.exec_type_) {
    set_value(SINGLE_PROCESS_TIME, exec_timestamp.single_process_ts_);
  }
  if (RpcProcessor == exec_timestamp.exec_type_) {
    set_value(RPC_SEND_TIME, exec_timestamp.rpc_send_ts_);
  }
  return ret;
}
} //namespace sql
} //namespace oceanbase
#endif
