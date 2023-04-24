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

#ifndef _OCEANBASE_WORKLOAD_SCHEDULER_H_
#define _OCEANBASE_WORKLOAD_SCHEDULER_H_

#include "lib/mysqlclient/ob_single_mysql_connection_pool.h"
#include "ob_workload_executor.h"
#include "ob_workload_options.h"
#include "lib/container/ob_se_array.h"

namespace oceanbase
{
  namespace testbench
  {

    class ObSchedulerOptions
    {
    public:
      ObSchedulerOptions() {}
      ~ObSchedulerOptions();
      int parse_options();

    public:
      int opts_cnt_;
      int8_t log_level_;
      const char *home_path_;
      ObArray<ObIWorkloadOptions *> workloads_;
    };

    class ObWorkloadScheduler
    {
    public:
      ObWorkloadScheduler() {}
      ~ObWorkloadScheduler() {}

      int init(ObSchedulerOptions &opts);
      int init_config();
      int init_sql_proxy();
      int init_timer();
      int init_workload_executor();

    private:
      common::sqlclient::ObSingleMySQLConnectionPool sql_conn_pool_;
      common::ObMySQLProxy sql_proxy_;
      ObSchedulerOptions opts_;
    };

  }
}
#endif