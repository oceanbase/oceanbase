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
#include "sql/monitor/ob_phy_operator_monitor_info.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/ob_name_def.h"
#include "share/ob_time_utility2.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
namespace oceanbase
{
namespace sql
{
ObPhyOperatorMonitorInfo::ObPhyOperatorMonitorInfo() : ObIValue(PLAN_MONITOR_INFO),
    op_id_(-1),
    job_id_(0),
    task_id_(0),
    op_type_(PHY_INVALID)
{
    memset(info_array_, 0, OB_MAX_INFORMATION_COUNT * sizeof(int64_t));
}

ObPhyOperatorMonitorInfo::~ObPhyOperatorMonitorInfo()
{
}

int ObPhyOperatorMonitorInfo::set_operator_id(int64_t op_id)
{
  int ret = OB_SUCCESS;
  if (op_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(op_id));
  } else {
    op_id_ = op_id;
  }
  return ret;
}

int ObPhyOperatorMonitorInfo::set_job_id(int64_t job_id)
{
  int ret = OB_SUCCESS;
  if (job_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(job_id));
  } else {
    job_id_ = job_id;
  }
  return ret;
}

int ObPhyOperatorMonitorInfo::set_task_id(int64_t task_id)
{
  int ret = OB_SUCCESS;
  if (task_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(task_id));
  } else {
    task_id_ = task_id;
  }
  return ret;
}

int ObPhyOperatorMonitorInfo::assign(const ObPhyOperatorMonitorInfo &other)
{
  int ret = OB_SUCCESS;
  op_id_ = other.op_id_;
  job_id_ = other.job_id_;
  task_id_ = other.task_id_;
  op_type_ = other.op_type_;
  for (int64_t i = 0; i < OB_MAX_INFORMATION_COUNT; i++) {
    info_array_[i] = other.info_array_[i];
  }
  return ret;
}

void ObPhyOperatorMonitorInfo::operator=(const ObPhyOperatorMonitorInfo &other)
{
  if (OB_SUCCESS != assign(other)) {
    LOG_ERROR_RET(OB_ERROR, "fail to assign", K(&other));
  }
}

ObPhyOperatorMonitorInfo::ObPhyOperatorMonitorInfo(const ObPhyOperatorMonitorInfo &other)
: ObIValue(PLAN_MONITOR_INFO), op_id_(other.op_id_),
    job_id_(other.job_id_), task_id_(other.task_id_), op_type_(other.op_type_)
{
  for (int64_t i = 0; i < OB_MAX_INFORMATION_COUNT; i++) {
    info_array_[i] = other.info_array_[i];
  }
}

int64_t ObPhyOperatorMonitorInfo::to_string(char *buf, int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_OP_ID, op_id_,
       N_JOB_ID, job_id_,
       N_TASK_ID, task_id_,
       N_OP, op_type_);
  for (int64_t i = 0; i < OB_MAX_INFORMATION_COUNT; i++) {
    if (info_array_[i] != 0 && NULL != OB_OPERATOR_MONITOR_INFOS[i].info_name_) {
      J_OBJ_START();
      J_KV(OB_OPERATOR_MONITOR_INFOS[i].info_name_, info_array_[i]);
      J_OBJ_END();
      J_COMMA();
    }
  }
  J_OBJ_END();
  return pos;
}

bool ObPhyOperatorMonitorInfo::is_timestamp(int64_t i) const
{
  bool bret = false;
  if (OPEN_TIME == i
      || FIRST_ROW_TIME == i
      || LAST_ROW_TIME == i
      || CLOSE_TIME == i) {
    bret = true;
  }
  return bret;
}

int64_t ObPhyOperatorMonitorInfo::print_info(char* buf, int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  bool first_cell = true;
  const int64_t time_buf_len = 128;
  char timebuf[time_buf_len];
  int64_t time_buf_pos = 0;

  for (int64_t i = 0; i < OB_MAX_INFORMATION_COUNT; i++) {
    time_buf_pos = 0;
    if (info_array_[i] != 0 && NULL != OB_OPERATOR_MONITOR_INFOS[i].info_name_) {
      if (first_cell) {
        first_cell = false;
      } else {
        J_COMMA();
      }
      J_OBJ_START();
      if (is_timestamp(i)) {
        if (OB_SUCCESS != ObTimeUtility2::usec_to_str(info_array_[i], timebuf, time_buf_len, time_buf_pos)) {
          LOG_WARN_RET(OB_ERR_UNEXPECTED, "fail to print time as str", K(i));
          J_KV(OB_OPERATOR_MONITOR_INFOS[i].info_name_, info_array_[i]);
        } else {
          timebuf[time_buf_pos] = '\0';
          J_KV(OB_OPERATOR_MONITOR_INFOS[i].info_name_, timebuf);
        }
      } else {
        J_KV(OB_OPERATOR_MONITOR_INFOS[i].info_name_, info_array_[i]);
      }
      J_OBJ_END();
    }
  }
  J_OBJ_END();
  return pos;
}

void ObPhyOperatorMonitorInfo::set_value(ObOperatorMonitorInfoIds index, int64_t value)
{
  if (index >= 0 && index < OB_MAX_INFORMATION_COUNT) {
    info_array_[index] = value;
  } else {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid index", K(index), K(value));
  }

}

void ObPhyOperatorMonitorInfo::increase_value(ObOperatorMonitorInfoIds index)
{
  if (index >= 0 && index < OB_MAX_INFORMATION_COUNT) {
    info_array_[index]++;
  } else {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid index", K(index));
  }
}

void ObPhyOperatorMonitorInfo::get_value(ObOperatorMonitorInfoIds index, int64_t &value)
{
  if (index >= 0 && index < OB_MAX_INFORMATION_COUNT) {
    value = info_array_[index];
  } else {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid index", K(index));
  }
}

int64_t ObPhyOperatorMonitorInfo::get_valid_info_count() const
{
  int64_t count = 0;
  for (int64_t i = 0; i < OB_MAX_INFORMATION_COUNT; i++) {
    if (info_array_[i] != 0) {
      count ++;
    }
  }
  return count;
}
OB_DEF_SERIALIZE(ObPhyOperatorMonitorInfo)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, op_id_);
  LST_DO_CODE(OB_UNIS_ENCODE, job_id_);
  LST_DO_CODE(OB_UNIS_ENCODE, task_id_);
  LST_DO_CODE(OB_UNIS_ENCODE, op_type_);
  int64_t valid_count = get_valid_info_count();
  LST_DO_CODE(OB_UNIS_ENCODE, valid_count);
  for (int64_t i = 0; i < OB_MAX_INFORMATION_COUNT && OB_SUCC(ret); i++) {
    if (info_array_[i] != 0) {
      if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, i))) {
        LOG_WARN("fail to encode vi64", K(ret), K(i), K(buf), K(buf_len), K(pos));
      } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, info_array_[i]))) {
        LOG_WARN("fail to encode vi64", K(ret), K(i), K(buf), K(buf_len), K(pos), K(info_array_[i]));
      }
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObPhyOperatorMonitorInfo)
{
  int ret = OB_SUCCESS;
  int64_t valid_count = 0;
  LST_DO_CODE(OB_UNIS_DECODE, op_id_);
  LST_DO_CODE(OB_UNIS_DECODE, job_id_);
  LST_DO_CODE(OB_UNIS_DECODE, task_id_);
  LST_DO_CODE(OB_UNIS_DECODE, op_type_);
  LST_DO_CODE(OB_UNIS_DECODE, valid_count);
  int64_t index = 0;
  int64_t value = 0;
  for (int64_t i = 0; i < valid_count && OB_SUCC(ret); i++) {
    if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &index))) {
      LOG_WARN("fail to decode index", K(ret), K(pos));
    } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &value))) {
      LOG_WARN("fail to decode value", K(ret), K(pos));
    } else {
      if (index >= OB_MAX_INFORMATION_COUNT) {
        //nothing to do, 也许收到一个新版本的统计信息
      } else if (index < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("invalid index", K(index), K(i), K(value), K(ret));
      } else {
        set_value(static_cast<ObOperatorMonitorInfoIds>(index), value);
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPhyOperatorMonitorInfo)
{
  int64_t len = 0;
  int64_t valid_count = get_valid_info_count();
  LST_DO_CODE(OB_UNIS_ADD_LEN, op_id_);
  LST_DO_CODE(OB_UNIS_ADD_LEN, job_id_);
  LST_DO_CODE(OB_UNIS_ADD_LEN, task_id_);
  LST_DO_CODE(OB_UNIS_ADD_LEN, op_type_);
  LST_DO_CODE(OB_UNIS_ADD_LEN, valid_count);
  for (int64_t i = 0; i < OB_MAX_INFORMATION_COUNT; i++) {
    if (info_array_[i] != 0) {
      len += serialization::encoded_length_vi64(i);
      len += serialization::encoded_length_vi64(info_array_[i]);
    }
  }
  return len;
}

}
}
