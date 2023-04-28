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
};

} // end of namespace obmysql
} // end of namespace oceanbase
#endif
