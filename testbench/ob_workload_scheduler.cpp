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

#include "lib/oblog/ob_log.h"
#include "ob_workload_scheduler.h"

namespace oceanbase
{
  namespace testbench
  {
    ObSchedulerOptions::~ObSchedulerOptions()
    {
      for (int i = 0; i < opts_cnt_; ++i)
      {
        ObIWorkloadOptions *&opt = workloads_[i];
        if (opt != nullptr)
        {
          OB_DELETE(ObIWorkloadOptions, "", opt);
        }
      }
    }

    int ObSchedulerOptions::parse_options()
    {
      int ret = OB_SUCCESS;
      for (int i = 0; i < opts_cnt_; ++i)
      {
        if (OB_FAIL(workloads_[i]->parse_options()))
        {
        }
      }
      return ret;
    }

    int ObWorkloadScheduler::init(ObSchedulerOptions &opts)
    {
      fprintf(stdout, "init"
                      "\n");
      int ret = OB_SUCCESS;
      opts_ = opts;
      if (OB_FAIL(init_config()))
      {
        TESTBENCH_LOG(WARN, "init user-defined config fail, use default config", K(ret));
      }
      else if (OB_FAIL(init_timer()))
      {
        TESTBENCH_LOG(WARN, "init timer fail", K(ret));
      }
      else if (OB_FAIL(init_sql_proxy()))
      {
        TESTBENCH_LOG(WARN, "init sql proxy fail", K(ret));
      }
      else if (OB_FAIL(init_workload_executor()))
      {
        TESTBENCH_LOG(WARN, "init workload executor fail", K(ret));
      }
      else
      {
        TESTBENCH_LOG(INFO, "init workload scheduler success");
      }
      return ret;
    }

    int ObWorkloadScheduler::init_config()
    {
      fprintf(stdout, "init config"
                      "\n");
      return opts_.parse_options();
    }

    int ObWorkloadScheduler::init_timer()
    {
      int ret = OB_SUCCESS;
      return ret;
    }

    int ObWorkloadScheduler::init_sql_proxy()
    {
      fprintf(stdout, "init sql proxy"
                      "\n");
      int ret = OB_SUCCESS;
      sql_conn_pool_.set_db_param("root@sys", "", "oceanbase");
      common::ObAddr db_addr;
      const char *local_ip_ = "127.0.0.1";
      db_addr.set_ip_addr(local_ip_, 2881);
      ObConnPoolConfigParam param;
      param.sqlclient_wait_timeout_ = 1000;                // 300s
      param.long_query_timeout_ = 300 * 1000 * 1000;       // 120s
      param.connection_refresh_interval_ = 200 * 1000;     // 200ms
      param.connection_pool_warn_time_ = 10 * 1000 * 1000; // 1s
      param.sqlclient_per_observer_conn_limit_ = 1000;
      if (OB_FAIL(sql_conn_pool_.init(db_addr, param)))
      {
      }
      else if (OB_FAIL(sql_proxy_.init(&sql_conn_pool_)))
      {
      }
      return ret;
    }

    int ObWorkloadScheduler::init_workload_executor()
    {
      int ret = OB_SUCCESS;
      return ret;
    }

  } // namespace testbench
} // namespace oceanbase