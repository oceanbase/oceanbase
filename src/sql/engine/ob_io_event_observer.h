/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_SQL_IO_EVENT_OBSERVER_H_
#define _OB_SQL_IO_EVENT_OBSERVER_H_

#include "share/diagnosis/ob_sql_plan_monitor_node_list.h"

namespace oceanbase
{
namespace sql
{
class ObIOEventObserver
{
public:
  ObIOEventObserver(ObMonitorNode &monitor_info) : op_monitor_info_(monitor_info), io_time_{0} {}
  inline void on_read_io(uint64_t used_time)
  {
    op_monitor_info_.block_time_ += used_time;
    io_time_ += used_time;
  }
  inline void on_write_io(uint64_t used_time)
  {
    op_monitor_info_.block_time_ += used_time;
    io_time_ += used_time;
  }
  inline uint64_t get_io_time() const { return io_time_; }
private:
  ObMonitorNode &op_monitor_info_;
  uint64_t io_time_;
};
}
}
#endif /* _OB_SQL_IO_EVENT_OBSERVER_H_ */
//// end of header file

