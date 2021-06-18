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

#define USING_LOG_PREFIX SQL_ENG

#include "ob_px_dtl_msg.h"
#include "sql/engine/ob_physical_plan_ctx.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::sql::dtl;

OB_SERIALIZE_MEMBER(ObPxPartitionInfo, partition_key_, logical_row_count_, physical_row_count_);
OB_SERIALIZE_MEMBER(
    ObPxTaskMonitorInfo, sched_exec_time_start_, sched_exec_time_end_, exec_time_start_, exec_time_end_, metrics_);
OB_SERIALIZE_MEMBER((ObPxTaskChSet, dtl::ObDtlChSet), sqc_id_, task_id_);
OB_SERIALIZE_MEMBER(ObPxPartChMapItem, first_, second_, third_);
OB_SERIALIZE_MEMBER(ObPxReceiveDataChannelMsg, child_dfo_id_, ch_sets_, ch_map_opt_, ch_total_info_);
OB_SERIALIZE_MEMBER(ObPxTransmitDataChannelMsg, ch_sets_, part_affinity_map_, ch_map_opt_, ch_total_info_);
OB_SERIALIZE_MEMBER(ObPxInitSqcResultMsg, dfo_id_, sqc_id_, rc_, task_count_);
OB_SERIALIZE_MEMBER(ObPxFinishSqcResultMsg, dfo_id_, sqc_id_, rc_, trans_result_, task_monitor_info_array_,
    sqc_affected_rows_, dml_row_info_, temp_table_id_, interm_result_ids_);
OB_SERIALIZE_MEMBER(ObPxFinishTaskResultMsg, dfo_id_, sqc_id_, task_id_, rc_);
OB_SERIALIZE_MEMBER(ObPxDmlRowInfo, row_match_count_, row_duplicated_count_, row_deleted_count_);

int ObPxTaskMonitorInfo::add_op_metric(sql::ObOpMetric& metric)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(metrics_.push_back(metric))) {
    LOG_WARN("failed to push back metric", K(ret));
  }
  return ret;
}

int ObPxTaskChSet::assign(const ObPxTaskChSet& other)
{
  int ret = OB_SUCCESS;
  sqc_id_ = other.sqc_id_;
  task_id_ = other.task_id_;
  sm_group_id_ = other.sm_group_id_;
  if (OB_FAIL(dtl::ObDtlChSet::assign(other))) {
    LOG_WARN("fail assign ObPxTaskChSet", K(other), K(ret));
  }
  return ret;
}

void ObPxDmlRowInfo::set_px_dml_row_info(const ObPhysicalPlanCtx& plan_ctx)
{
  row_match_count_ = plan_ctx.get_row_matched_count();
  row_duplicated_count_ = plan_ctx.get_row_duplicated_count();
  row_deleted_count_ = plan_ctx.get_row_deleted_count();
}
int64_t ObPxFinishTaskResultMsg::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(dfo_id), K_(sqc_id), K_(task_id), K_(rc));
  J_OBJ_END();
  return pos;
}
int64_t ObPxFinishSqcResultMsg::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(dfo_id), K_(sqc_id), K_(rc), K_(sqc_affected_rows));
  J_OBJ_END();
  return pos;
}
int64_t ObPxInitSqcResultMsg::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(dfo_id), K_(sqc_id), K_(rc), K_(task_count));
  J_OBJ_END();
  return pos;
}
int64_t ObPxTransmitDataChannelMsg::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(ch_sets), K_(part_affinity_map), K_(ch_total_info), K_(ch_map_opt));
  J_OBJ_END();
  return pos;
}
int64_t ObPxReceiveDataChannelMsg::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(child_dfo_id), K_(ch_sets), K_(ch_map_opt), K_(ch_total_info));
  J_OBJ_END();
  return pos;
}
int64_t ObPxPartChMapItem::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(first), K_(second), K_(third));
  J_OBJ_END();
  return pos;
}
int64_t ObPxTaskMonitorInfo::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(sched_exec_time_start), K_(sched_exec_time_end), K_(exec_time_start), K_(exec_time_end), K(metrics_.count()));
  J_OBJ_END();
  return pos;
}
int64_t ObPxDmlRowInfo::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(row_match_count), K_(row_duplicated_count), K_(row_deleted_count));
  J_OBJ_END();
  return pos;
}
int64_t ObPxPartitionInfo::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(partition_key), K_(logical_row_count), K_(physical_row_count));
  J_OBJ_END();
  return pos;
}
