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
#include "ob_testbench_builder.h"

namespace oceanbase
{
  namespace testbench
  {
    ObTestbenchOptions::ObTestbenchOptions() : opts_cnt_(0), log_level_(OB_LOG_LEVEL_DEBUG), workloads_()
    {
      home_path_[0] = '\0';
      snprintf(log_dir_, OB_MAX_CONTEXT_STRING_LENGTH, "%s", "log");
      snprintf(log_file_, OB_MAX_CONTEXT_STRING_LENGTH, "%s", "log/scheduler.log");
      log_file_size_ = 256 * 1024 * 1024;
      cluster_host_[0] = '\0';
      cluster_user_[0] = '\0';
      cluster_pass_[0] = '\0';
      cluster_db_name_[0] = '\0';
      cluster_port_ = 0;
      duration_ = 1;
    }

    ObTestbenchOptions::~ObTestbenchOptions() {}

    int ObTestbenchOptions::parse_options()
    {
      int ret = OB_SUCCESS;
      for (int i = 0; i < opts_cnt_; ++i)
      {
        if (OB_FAIL(workloads_[i]->parse_options()))
        {
          TESTBENCH_LOG(WARN, "parse_options for workload fail", K(ret));
        }
      }
      return ret;
    }

    void ObTestbenchOptions::destroy_options()
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

    ObTestbenchBuilder::ObTestbenchBuilder() : is_inited_(false), sql_proxy_()
    {
    }

    ObTestbenchBuilder::~ObTestbenchBuilder() {}

    int ObTestbenchBuilder::init(ObTestbenchOptions &opts)
    {
      int ret = OB_SUCCESS;
      opts_ = &opts;
      if (IS_INIT)
      {
        ret = OB_INIT_TWICE;
        TESTBENCH_LOG(WARN, "ObTestbenchBuilder init twice", KR(ret));
      }
      else if (OB_FAIL(init_config()))
      {
        TESTBENCH_LOG(WARN, "ObTestbenchBuilder init_config fail, use default config", KR(ret));
      }
      else if (OB_FAIL(init_sql_proxy()))
      {
        TESTBENCH_LOG(ERROR, "ObTestbenchBuilder init_sql_proxy fail", KR(ret));
      }
      else if (OB_FAIL(init_transaction_executor()))
      {
        TESTBENCH_LOG(ERROR, "ObTestbenchBuilder init_transaction_executor fail", KR(ret));
      }
      else if (OB_FAIL(init_statistics_collector()))
      {
        TESTBENCH_LOG(ERROR, "ObTestbenchBuilder init_workload_executor fail", KR(ret));
      }
      else if (OB_FAIL(init_transaction_scheduler()))
      {
        TESTBENCH_LOG(ERROR, "ObTestbenchBuilder init_transaction_scheduler fail", KR(ret));
      }
      else
      {
        TESTBENCH_LOG(INFO, "ObTestbenchBuilder init success");
      }
      return ret;
    }

    int ObTestbenchBuilder::init_config()
    {
      return opts_->parse_options();
    }

    int ObTestbenchBuilder::init_sql_proxy()
    {
      int ret = OB_SUCCESS;
      common::ObAddr addr;
      if (OB_UNLIKELY(!addr.set_ip_addr(opts_->cluster_host_, opts_->cluster_port_)))
      {
        ret = OB_ERR_UNEXPECTED;
        TESTBENCH_LOG(ERROR, "ObTestbenchBuilder addr set_ip_addr fail", KR(ret));
      }
      else if (FALSE_IT(sql_proxy_.set_db_param(addr, opts_->cluster_user_, opts_->cluster_pass_, opts_->cluster_db_name_)))
      {
        ret = OB_ERR_UNEXPECTED;
        TESTBENCH_LOG(ERROR, "ObTestbenchBuilder sql_proxy_ set_db_param fail", KR(ret));
      }
      else if (OB_FAIL(sql_proxy_.init()))
      {
        TESTBENCH_LOG(ERROR, "ObTestbenchBuilder sql_proxy_ init fail", KR(ret));
      }
      else
      {
        is_inited_ = true;
        TESTBENCH_LOG(INFO, "ObTestbenchBuilder init_sql_proxy success");
      }
      return ret;
    }

    int ObTestbenchBuilder::init_transaction_executor()
    {
      int ret = OB_SUCCESS;
      return ret;
    }

    int ObTestbenchBuilder::init_statistics_collector()
    {
      int ret = OB_SUCCESS;
      return ret;
    }

    int ObTestbenchBuilder::init_transaction_scheduler()
    {
      int ret = OB_SUCCESS;
      return ret;
    }

    int ObTestbenchBuilder::start_service()
    {
      int ret = OB_SUCCESS;
      if (OB_FAIL(sql_proxy_.start_service()))
      {
        TESTBENCH_LOG(ERROR, "ObTestbenchBuilder sql_proxy_ start_service fail", KR(ret));
      }
      else
      {
        TESTBENCH_LOG(INFO, "ObTestbenchBuilder start_service success");
      }
      return ret;
    }

    void ObTestbenchBuilder::stop_service()
    {
      opts_->destroy_options();
      sql_proxy_.stop_and_destroy();
      TESTBENCH_LOG(INFO, "ObTestbenchBuilder stop_service success");
    }

  } // namespace testbench
} // namespace oceanbase