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
  ObIOEventObserver(ObMonitorNode &monitor_info) : op_monitor_info_(monitor_info) {}
  inline void on_read_io(uint64_t used_time)
  {
    op_monitor_info_.block_time_ += used_time;
  }
  inline void on_write_io(uint64_t used_time)
  {
    op_monitor_info_.block_time_ += used_time;
  }
private:
  ObMonitorNode &op_monitor_info_;
};
}
}
#endif /* _OB_SQL_IO_EVENT_OBSERVER_H_ */
//// end of header file

