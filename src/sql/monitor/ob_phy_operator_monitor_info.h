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

#ifdef OPERATOR_MONITOR_INFO_DEF
OPERATOR_MONITOR_INFO_DEF(OPEN_TIME, open_time)
OPERATOR_MONITOR_INFO_DEF(FIRST_ROW_TIME, first_row)
OPERATOR_MONITOR_INFO_DEF(LAST_ROW_TIME, last_row)
OPERATOR_MONITOR_INFO_DEF(CLOSE_TIME, close_time)
OPERATOR_MONITOR_INFO_DEF(RESCAN_TIMES, rescan_times)
OPERATOR_MONITOR_INFO_DEF(INPUT_ROW_COUNT, input_row_count)
OPERATOR_MONITOR_INFO_DEF(OUTPUT_ROW_COUNT, output_row_count)
OPERATOR_MONITOR_INFO_DEF(MEMORY_USED, memory_used)
OPERATOR_MONITOR_INFO_DEF(DISK_READ_COUNT, disk_read_count)
OPERATOR_MONITOR_INFO_DEF(MONITOR_INFO_END, monitor_end)
#endif

#ifndef OCEANBASE_SQL_OB_PHY_OPERATOR_MONITOR_INFORMATION_H
#define OCEANBASE_SQL_OB_PHY_OPERATOR_MONITOR_INFORMATION_H
#include "share/ob_define.h"
#include "lib/utility/ob_unify_serialize.h"
#include "sql/monitor/ob_i_collect_value.h"
#include "sql/engine/ob_phy_operator_type.h"
namespace oceanbase
{
namespace sql
{
struct MonitorName
{
  int64_t id_;
  const char * const info_name_;
};
enum ObOperatorMonitorInfoIds
{
#define OPERATOR_MONITOR_INFO_DEF(def, name) def,
#include "ob_phy_operator_monitor_info.h"
#undef OPERATOR_MONITOR_INFO_DEF
};
static const MonitorName OB_OPERATOR_MONITOR_INFOS[] = {
#define OPERATOR_MONITOR_INFO_DEF(def, name) \
  {def, #name},
#include "ob_phy_operator_monitor_info.h"
#undef OPERATOR_MONITOR_INFO_DEF
};

class ObPhyOperatorMonitorInfo : public ObIValue
{
  OB_UNIS_VERSION(1);
public:
  ObPhyOperatorMonitorInfo();
  virtual ~ObPhyOperatorMonitorInfo();
  int set_operator_id(int64_t op_id);
  int set_job_id(int64_t job_id);
  int set_task_id(int64_t task_id);
  void set_operator_type(ObPhyOperatorType type) { op_type_ = type; }
  int64_t get_op_id() const { return op_id_; }
  int64_t get_job_id() const { return job_id_; }
  int64_t get_task_id() const { return task_id_; }
  ObPhyOperatorType get_operator_type() const { return op_type_; }
  int assign(const ObPhyOperatorMonitorInfo &info);
  void operator= (const ObPhyOperatorMonitorInfo&other);
  ObPhyOperatorMonitorInfo(const ObPhyOperatorMonitorInfo &other);
  int64_t to_string(char *buf, int64_t buf_len) const;
  virtual int64_t print_info(char *buf, int64_t buf_len) const;
  void set_value(ObOperatorMonitorInfoIds index, int64_t value);
  void get_value(ObOperatorMonitorInfoIds index, int64_t &value);
  void increase_value(ObOperatorMonitorInfoIds index);
  static const int64_t OB_MAX_INFORMATION_COUNT = MONITOR_INFO_END;
private:
  int64_t get_valid_info_count() const;
  virtual bool is_timestamp(int64_t index) const;
protected:
  int64_t op_id_;
  int64_t job_id_; //在分布式执行计划时有效
  int64_t task_id_; //在分布式执行计划时有效
private:
  ObPhyOperatorType op_type_;
  uint64_t info_array_[OB_MAX_INFORMATION_COUNT];
};
} //namespace sql
} //namespace oceanbase
#endif
