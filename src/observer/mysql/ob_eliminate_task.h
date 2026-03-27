/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OB_ELIMINATE_TASK_
#define _OB_ELIMINATE_TASK_

#include "lib/task/ob_timer.h"
#include "lib/allocator/ob_fifo_allocator.h"
#include "sql/monitor/flt/ob_flt_span_mgr.h"

namespace oceanbase
{
namespace obmysql
{

class ObMySQLRequestManager;
class ObEliminateTask : public common::ObTimerTask
{
public:
  ObEliminateTask();
  virtual ~ObEliminateTask();

  void runTimerTask();
  int init(const ObMySQLRequestManager *request_manager);
  int check_config_mem_limit(bool &is_change);
  int calc_evict_mem_level(int64_t &low, int64_t &high);

private:
  ObMySQLRequestManager *request_manager_;
  int64_t config_mem_limit_;
  sql::ObFLTSpanMgr* flt_mgr_;
  bool is_tp_trigger_;
};

} // end of namespace obmysql
} // end of namespace oceanbase
#endif
