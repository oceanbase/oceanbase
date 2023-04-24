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
 *
 */

#ifndef _OCEANBASE_WORKLOAD_TRANSACTION_TASK_H_
#define _OCEANBASE_WORKLOAD_TRANSACTION_TASK_H_

#include "ob_testbench_options.h"
#include "ob_testbench_mysql_proxy.h"

namespace oceanbase
{
  namespace testbench
  {
    class ObIWorkloadTransactionTask
    {
    public:
      ObIWorkloadTransactionTask();
      virtual ~ObIWorkloadTransactionTask();
      virtual int parameter_instantiation() = 0;
      virtual int get_database_conn() = 0;
      virtual int execute_commands() = 0;
    };

    template <enum WorkloadType>
    class ObWorkloadTransactionTask;

    template <>
    class ObWorkloadTransactionTask<WorkloadType::DISTRIBUTED_TRANSACTION> : public ObIWorkloadTransactionTask
    {
    public:
      ObWorkloadTransactionTask();
      virtual ~ObWorkloadTransactionTask() override;
      virtual int parameter_instantiation() override;
      virtual int get_database_conn() override;
      virtual int execute_commands() override;
    };
  }
}
#endif