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

#include "ob_testbench_transaction_executor_pool.h"
#include "share/ob_thread_define.h"
#include "share/ob_thread_mgr.h"

namespace oceanbase
{
  namespace testbench
  {
    ObTestbenchTransactionExecutorPool::ObTestbenchTransactionExecutorPool() : tg_id_(-1), is_inited_(false), sql_proxy_(NULL) {}

    ObTestbenchTransactionExecutorPool::~ObTestbenchTransactionExecutorPool()
    {
      destroy();
    }

    int ObTestbenchTransactionExecutorPool::init(const int64_t thread_num, const int64_t task_rate_limit, ObTestbenchMySQLProxy *sql_proxy)
    {
      int ret = OB_SUCCESS;
      if (IS_INIT)
      {
        ret = OB_INIT_TWICE;
        TESTBENCH_LOG(ERROR, "ObTestbenchTransactionExecutorPool init twice", KR(ret));
      }
      else if (NULL == sql_proxy)
      {
        ret = OB_INVALID_ARGUMENT;
        TESTBENCH_LOG(ERROR, "invalid argument", KR(ret));
      }
      else if (OB_FAIL(TG_CREATE(lib::TGDefIDs::TransactionExecutorPool, tg_id_)))
      {
        TESTBENCH_LOG(ERROR, "init ObTestbenchTransactionExecutorPool fail", KR(ret), K(tg_id_));
      }
      else if (OB_FAIL(TG_SET_THREAD_CNT(tg_id_, thread_num)))
      {
        TESTBENCH_LOG(WARN, "set ObTestbenchTransactionExecutorPool thread cnt fail", KR(ret), K(thread_num));
      }
      else if (OB_FAIL(TG_SET_QUEUE_SIZE(tg_id_, task_rate_limit)))
      {
        TESTBENCH_LOG(WARN, "set ObTestbenchTransactionExecutorPool queue size fail", KR(ret), K(task_rate_limit));
      }
      else
      {
        sql_proxy_ = sql_proxy;
        is_inited_ = true;
        TESTBENCH_LOG(INFO, "ObTestbenchTransactionExecutorPool init success", K(thread_num), K(task_rate_limit));
      }

      if (OB_FAIL(ret) && OB_INIT_TWICE != ret)
      {
        destroy();
      }
      return ret;
    }

    int ObTestbenchTransactionExecutorPool::start()
    {
      int ret = OB_SUCCESS;
      if (IS_NOT_INIT)
      {
        ret = OB_NOT_INIT;
        TESTBENCH_LOG(ERROR, "ObTestbenchTransactionExecutorPool not init");
      }
      else if (OB_FAIL(TG_SET_HANDLER_AND_START(tg_id_, *this)))
      {
        TESTBENCH_LOG(ERROR, "ObTestbenchTransactionExecutorPool set handle and start fail", KR(ret));
      }
      else
      {
        TESTBENCH_LOG(INFO, "ObTestbenchTransactionExecutorPool start success");
      }
      return ret;
    }

    int ObTestbenchTransactionExecutorPool::stop()
    {
      int ret = OB_SUCCESS;
      if (IS_NOT_INIT)
      {
        ret = OB_NOT_INIT;
        TESTBENCH_LOG(ERROR, "ObTestbenchTransactionExecutorPool not init");
      }
      else
      {
        TG_STOP(tg_id_);
        TESTBENCH_LOG(INFO, "ObTestbenchTransactionExecutorPool stop");
      }
      return ret;
    }

    int ObTestbenchTransactionExecutorPool::wait()
    {
      int ret = OB_SUCCESS;
      if (IS_NOT_INIT)
      {
        ret = OB_NOT_INIT;
        TESTBENCH_LOG(ERROR, "ObTestbenchTransactionExecutorPool not init");
      }
      else
      {
        TG_WAIT(tg_id_);
        TESTBENCH_LOG(INFO, "ObTestbenchTransactionExecutorPool wait");
      }
      return ret;
    }

    void ObTestbenchTransactionExecutorPool::destroy()
    {
      stop();
      wait();
      is_inited_ = false;
      if (-1 != tg_id_)
      {
        TG_DESTROY(tg_id_);
        tg_id_ = -1;
      }
      TESTBENCH_LOG(INFO, "ObTestbenchTransactionExecutorPool destroy");
    }

    void ObTestbenchTransactionExecutorPool::handle(void *task)
    {
      int ret = OB_SUCCESS;
      return;
    }
  }
}