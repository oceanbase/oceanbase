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

#ifndef _OCEANBASE_TESTBENCH_TRANSACTION_EXECUTOR_POOL_H_
#define _OCEANBASE_TESTBENCH_TRANSACTION_EXECUTOR_POOL_H_

#include "lib/thread/thread_mgr_interface.h"
#include "ob_testbench_transaction_task.h"

namespace oceanbase
{
  namespace testbench
  {
    class ObTestbenchTransactionExecutorPool : public lib::TGTaskHandler
    {
    public:
      ObTestbenchTransactionExecutorPool();
      ~ObTestbenchTransactionExecutorPool();

    public:
      int init(const int64_t thread_num, const int64_t task_rate_limit, ObTestbenchMySQLProxy *sql_proxy);
      int start();
      int stop();
      int wait();
      void destroy();
      virtual void handle(void *task);
      inline int get_tg_id() const { return tg_id_; }

    private:
      int tg_id_;
      bool is_inited_;
      ObTestbenchMySQLProxy *sql_proxy_;

    private:
      DISALLOW_COPY_AND_ASSIGN(ObTestbenchTransactionExecutorPool);
    };
  } // end namespace testbench
} // end namespace oceanbase

#endif