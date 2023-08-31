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
#include "sql/monitor/ob_exec_stat_collector.h"
#include "sql/monitor/ob_phy_operator_monitor_info.h"
#include "sql/monitor/ob_phy_plan_monitor_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan.h"
#include "observer/ob_inner_sql_result.h"
#include "sql/ob_sql.h"
using namespace oceanbase::common;
using namespace oceanbase::observer;
namespace oceanbase
{
namespace sql
{
template<class T>
int ObExecStatCollector::add_stat(const T *value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(value)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(value));
  } else if (OB_FAIL(serialization::encode_vi32(
              extend_buf_, MAX_STAT_BUF_COUNT, length_, value->get_type()))) {
    LOG_WARN("fail to encode type", K(ret), K(value));
  } else if (OB_FAIL(value->serialize(extend_buf_, MAX_STAT_BUF_COUNT, length_))) {
    LOG_WARN("fail to serialize value", K(ret), K(value));
  }
  return ret;
}

int ObExecStatCollector::add_raw_stat(const common::ObString &str)
{
  int ret = OB_SUCCESS;
  if (length_ + str.length() >= MAX_STAT_BUF_COUNT) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_DEBUG("buffer size not enough", K(ret),K(length_), K(str.length()));
  } else {
    MEMCPY(extend_buf_ + length_, str.ptr(), str.length());
    length_ += str.length();
  }
  return ret;
}

int ObExecStatCollector::get_extend_info(ObIAllocator &allocator, ObString &str)
{
  int ret = OB_SUCCESS;
  const ObString tmp_str(length_, extend_buf_);
  if (OB_FAIL(ob_write_string(allocator, tmp_str, str))) {
    LOG_WARN("fail to write string", K(tmp_str), K(ret));
  }
  return ret;
}
void ObExecStatCollector::reset()
{
  length_ = 0;
}

int ObExecStatCollector::collect_plan_monitor_info(uint64_t job_id,
                                  uint64_t task_id,
                                  ObPhyPlanMonitorInfo *monitor_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(monitor_info)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_MONITOR_LOG(WARN, "invalid argument", K(ret), K(monitor_info));
  } else {
    ObPhyOperatorMonitorInfo *op_info = NULL;
    for (int64_t i = 0; i < monitor_info->get_operator_count() && OB_SUCC(ret); i++) {
      if (OB_FAIL(monitor_info->get_operator_info_by_index(i, op_info))) {
        SQL_MONITOR_LOG(WARN, "fail to get operator info by index", K(ret), K(i));
      } else if (OB_ISNULL(op_info)) {
        ret = OB_ERR_UNEXPECTED;
        SQL_MONITOR_LOG(WARN, "get invalid op_info", K(ret), K(op_info));
      } else if (OB_FAIL(op_info->set_job_id(job_id))) {
        SQL_MONITOR_LOG(WARN, "fail to set job id", K(ret), K(job_id));
      } else if (OB_FAIL(op_info->set_task_id(task_id))) {
        SQL_MONITOR_LOG(WARN, "fail to to set task id", K(ret), K(task_id));
      } else if (OB_FAIL(add_stat<ObPhyOperatorMonitorInfo>(op_info))) {
        SQL_MONITOR_LOG(WARN, "fail to add value", K(ret), K(i));
      } else {
        LOG_DEBUG("collect plan monitor info", K(*op_info));
      }
    }
  }
  return ret;
}

int ObExecStatCollector::collect_monitor_info(uint64_t job_id,
                                              uint64_t task_id,
                                              ObPhyOperatorMonitorInfo &op_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(op_info.set_job_id(job_id))) {
    SQL_MONITOR_LOG(WARN, "fail to set job id", K(ret), K(job_id));
  } else if (OB_FAIL(op_info.set_task_id(task_id))) {
    SQL_MONITOR_LOG(WARN, "fail to to set task id", K(ret), K(task_id));
  } else if (OB_FAIL(add_stat<ObPhyOperatorMonitorInfo>(&op_info))) {
    SQL_MONITOR_LOG(WARN, "fail to add value", K(ret));
  } else {
    LOG_DEBUG("add monitor info", K(op_info));
  }
  return ret;
}
//////////////////////////////////////
int ObExecStatDispatch::set_extend_info(const ObString &stat_buf)
{
  stat_str_.assign_ptr(stat_buf.ptr(), stat_buf.length());
  return OB_SUCCESS;
}

int ObExecStatDispatch::dispatch(bool need_add_monitor,
                                 ObPhyPlanMonitorInfo *monitor_info,
                                 bool need_update_plan,
                                 ObPhysicalPlan *plan)
{
  int ret = OB_SUCCESS;
  StatType type = OB_INVALID_STAT_TYPE;
  if ((need_add_monitor && OB_ISNULL(monitor_info))
      || OB_ISNULL(plan)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(monitor_info), K(plan));
  }
  while (OB_SUCC(ret) && OB_SUCC(get_next_type(type))) {
    switch (type) {
      case PLAN_MONITOR_INFO:
        {
          ObPhyOperatorMonitorInfo op_info;
          if (OB_FAIL(get_value<ObPhyOperatorMonitorInfo>(&op_info))) {
            LOG_WARN("fail to get value", K(ret));
          } else if (need_add_monitor && OB_FAIL(monitor_info->add_operator_info(op_info))) {
            LOG_WARN("fail to add operator info", K(ret), K(op_info));
          } else if (need_update_plan && OB_FAIL(plan->op_stats_.add_op_stat(op_info))) {
            LOG_WARN("fail to add operator info", K(ret), K(op_info));
          }
          break;
        }
      default:
        ret = OB_UNKNOWN_OBJ;
        LOG_WARN("unknown type", K(ret), K(type));
        break;
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObExecStatDispatch::get_next_type(StatType &type)
{
  int ret = OB_SUCCESS;
  int32_t type_value = 0;
  if (pos_ == stat_str_.length()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(serialization::decode_vi32(stat_str_.ptr(), stat_str_.length(), pos_, &type_value))) {
    LOG_WARN("fail to decode type", K(ret), K(pos_));
  } else {
    type = static_cast<StatType>(type_value);
  }
  return ret;
}
template<class T>
int ObExecStatDispatch::get_value(T *value)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(value)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(value));
  } else if (OB_FAIL(value->deserialize(stat_str_.ptr(), stat_str_.length(), pos_))) {
    LOG_WARN("fail to deserialize value", K(ret), K(pos_));
  }
  return ret;
}

}/* ns sql*/
}/* ns oceanbase */
