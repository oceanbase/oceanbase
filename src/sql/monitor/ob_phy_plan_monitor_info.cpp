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

#define USING_LOG_PREFIX SQL_MONITOR
#include "sql/monitor/ob_phy_plan_monitor_info.h"
using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{
OB_SERIALIZE_MEMBER(ObPhyPlanMonitorInfo, request_id_, plan_id_, plan_info_, operator_infos_, execution_time_);

ObPhyPlanMonitorInfo::ObPhyPlanMonitorInfo(common::ObConcurrentFIFOAllocator &allocator)
    :allocator_(allocator),
     request_id_(OB_INVALID_ID),
     scheduler_addr_(),
     plan_id_(OB_INVALID_ID),
     execution_time_(0),
     operator_infos_(OB_MALLOC_NORMAL_BLOCK_SIZE),
     exec_trace_(false, ObLatchIds::TRACE_RECORDER_LOCK)
  {
  }

int ObPhyPlanMonitorInfo::add_operator_info(const ObPhyOperatorMonitorInfo &info)
{
  return operator_infos_.push_back(info);
}

int ObPhyPlanMonitorInfo::get_operator_info(int64_t op_id, ObPhyOperatorMonitorInfo &info) const
{
  int ret = OB_ENTRY_NOT_EXIST;
  ARRAY_FOREACH_NORET(operator_infos_, idx) {
    if (operator_infos_.at(idx).get_op_id() == op_id) {
      if (OB_FAIL(info.assign(operator_infos_.at(idx)))) {
        LOG_WARN("fail to assign to phy_operator info", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
      break;
    }
  }
  return ret;
}

int ObPhyPlanMonitorInfo::get_operator_info_by_index(int64_t index,
                                                     ObPhyOperatorMonitorInfo *&info)
{
  int ret = OB_SUCCESS;
  if (0 > index) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid operator index", K(index));
  } else if (index > operator_infos_.count()) {
    ret = OB_ITER_END;
    LOG_WARN("fail to get operator info", K(ret), K(index));
  } else if (index < operator_infos_.count()) {
    info = &(operator_infos_.at(index));
  }
  return ret;
}

int ObPhyPlanMonitorInfo::set_plan_exec_record(const ObExecRecord &exec_record)
{
  return plan_info_.add_exec_record(exec_record);
}
int ObPhyPlanMonitorInfo::set_plan_exec_timestamp(const ObExecTimestamp &exec_timestamp)
{
  execution_time_ = exec_timestamp.run_ts_;
  return plan_info_.add_exec_timestamp(exec_timestamp);
}
} //namespace sql
} //namespace oceanbase
