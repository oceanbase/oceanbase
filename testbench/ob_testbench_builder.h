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

#ifndef _OCEANBASE_TESTBENCH_BUILDER_H_
#define _OCEANBASE_TESTBENCH_BUILDER_H_

#include "ob_testbench_transaction_executor_pool.h"
#include "ob_testbench_options.h"
#include "lib/container/ob_se_array.h"
#include "ob_testbench_mysql_proxy.h"

namespace oceanbase
{
  namespace testbench
  {
    struct ObTestbenchOptions
    {
      ObTestbenchOptions();
      ~ObTestbenchOptions();

      int parse_options();
      void destroy_options();

      int opts_cnt_;
      int8_t log_level_;
      int log_file_size_;
      char home_path_[OB_MAX_CONTEXT_STRING_LENGTH];
      char log_dir_[OB_MAX_CONTEXT_STRING_LENGTH];
      char log_file_[OB_MAX_CONTEXT_STRING_LENGTH];
      char cluster_host_[OB_MAX_HOST_NAME_LENGTH];
      char cluster_user_[OB_MAX_USER_NAME_BUF_LENGTH];
      int32_t cluster_port_;
      char cluster_pass_[OB_MAX_PASSWORD_BUF_LENGTH];
      char cluster_db_name_[OB_MAX_DATABASE_NAME_BUF_LENGTH];
      int duration_;
      ObArray<ObIWorkloadOptions *> workloads_;

      TO_STRING_KV(K_(opts_cnt), K_(log_level), KCSTRING_(home_path), KCSTRING_(cluster_user), K_(cluster_port), KCSTRING_(cluster_pass), KCSTRING_(cluster_db_name), K_(duration));
    };

    class ObTestbenchBuilder
    {
    public:
      static ObTestbenchBuilder &get_instance();

    public:
      ObTestbenchBuilder();
      ~ObTestbenchBuilder();

      int init(ObTestbenchOptions &opts);
      int init_config();
      int init_sql_proxy();
      int init_transaction_executor();
      int init_statistics_collector();
      int init_transaction_scheduler();
      int start_service();
      void stop_service();

    private:
      bool is_inited_;
      ObTestbenchMySQLProxy sql_proxy_;
      ObTestbenchOptions *opts_;

    private:
      DISALLOW_COPY_AND_ASSIGN(ObTestbenchBuilder);
    };

    inline ObTestbenchBuilder &ObTestbenchBuilder::get_instance()
    {
      static ObTestbenchBuilder builder;
      return builder;
    }
  }
}
#endif