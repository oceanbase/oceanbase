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

#ifndef OCEANBASE_SQL_OB_PHY_PLAN_MONITOR_INFORMATION_H
#define OCEANBASE_SQL_OB_PHY_PLAN_MONITOR_INFORMATION_H
#include "lib/container/ob_se_array.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "sql/monitor/ob_phy_operator_monitor_info.h"
#include "sql/monitor/ob_phy_plan_exec_info.h"
#include "observer/mysql/ob_mysql_request_manager.h"
#include "lib/trace/ob_trace_event.h"
namespace oceanbase
{
namespace sql
{
class ObPhyPlanMonitorInfo final
{
public:
  OB_UNIS_VERSION(1);
public:
  const static int OPERATOR_LOCAL_COUNT = 8;
  explicit ObPhyPlanMonitorInfo(common::ObConcurrentFIFOAllocator &allocator);
  virtual void destroy()
  {
    reset();
    allocator_.free(this);
  }
  void reset()
  {
    operator_infos_.reset();
  }

  int add_operator_info(const ObPhyOperatorMonitorInfo &info);
  int64_t get_operator_count() { return operator_infos_.count(); }
  int set_plan_exec_record(const ObExecRecord &exec_record);
  int set_plan_exec_timestamp(const ObExecTimestamp &exec_timestamp);
  int get_operator_info(int64_t op_id,
                        ObPhyOperatorMonitorInfo &info) const;
  int get_operator_info_by_index(int64_t index,
                                 ObPhyOperatorMonitorInfo *&info);
  int get_plan_info(ObPhyPlanExecInfo *&info)
  { info = &plan_info_; return common::OB_SUCCESS; }
  int set_trace(const common::ObTraceEventRecorder &trace) { return exec_trace_.assign(trace); }
  const common::ObTraceEventRecorder &get_trace() const { return exec_trace_; }
  int64_t to_string(char *buf, int64_t buf_len) const
  {
    int64_t pos = 0;
    J_OBJ_START();
    J_KV(N_QID, request_id_);
    J_KV(N_PLAN_ID, plan_id_);
    J_KV(N_EXECUTION_TIME, execution_time_);
    J_KV(N_PLAN_MONITOR_INFO, plan_info_);
    for (int64_t i = 0; i < operator_infos_.count(); i++) {
         J_KV(N_OPERATOR_MONITOR_INFO, operator_infos_.at(i));
    }
    J_OBJ_END();
    return pos;
  }
  int64_t get_operator_info_memory_size()
  {
    // 本地info内存统计在alloctor_里面，因此只需要计算非本地的内存
    return operator_infos_.count() < OPERATOR_LOCAL_COUNT ? 0 : operator_infos_.get_data_size();
  }
  void set_request_id(int64_t request_id) { request_id_ = request_id; }
  void set_plan_id(int64_t plan_id) {plan_id_ = plan_id; }
  int64_t &get_request_id() { return request_id_; }
  int64_t get_request_id() const { return request_id_; }
  int64_t get_plan_id() const { return plan_id_; }
  int64_t get_execution_time() const { return execution_time_; }
  void set_address(common::ObAddr addr) { scheduler_addr_ = addr; }
  common::ObAddr get_address() const { return scheduler_addr_; }
private:
  DISALLOW_COPY_AND_ASSIGN(ObPhyPlanMonitorInfo);
private:
  common::ObConcurrentFIFOAllocator &allocator_;
  int64_t request_id_;
  common::ObAddr scheduler_addr_;
  int64_t plan_id_;
  int64_t execution_time_;
  common::ObSEArray<ObPhyOperatorMonitorInfo, OPERATOR_LOCAL_COUNT> operator_infos_;
  ObPhyPlanExecInfo plan_info_;
  common::ObTraceEventRecorder exec_trace_;
};
} //namespace sql
} //namespace oceanbase
#endif
