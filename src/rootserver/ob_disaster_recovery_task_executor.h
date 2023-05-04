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

#ifndef OCEANBASE_ROOTSERVER_OB_DISASTER_RECOVERY_TASK_EXECUTOR_H_
#define OCEANBASE_ROOTSERVER_OB_DISASTER_RECOVERY_TASK_EXECUTOR_H_

#include "rootserver/ob_disaster_recovery_task_mgr.h"

namespace oceanbase
{
namespace obrpc
{
class ObSrvRpcProxy;
}

namespace share
{
class ObLSTableOperator;

namespace schema
{
class ObMultiVersionSchemaService;
}
}

namespace rootserver
{
class ObDRTask;

class ObDRTaskExecutor
{
public:
  ObDRTaskExecutor()
    : inited_(false),
      lst_operator_(nullptr),
      rpc_proxy_(nullptr) {}
  virtual ~ObDRTaskExecutor() {}
public:
  // init a ObDRTaskExecutor
  // param [in] lst_operator, to check task
  // param [in] rpc_proxy, to send task execution to dst server
  int init(
      share::ObLSTableOperator &lst_operator,
      obrpc::ObSrvRpcProxy &rpc_proxy);

  // do previous check and execute a task
  // @param [in] task, the task to execute
  // @param [out] ret_code, the result of execution
  virtual int execute(
      const ObDRTask &task,
      int &ret_code,
      ObDRTaskRetComment &ret_comment) const;
private:
  bool inited_;
  share::ObLSTableOperator *lst_operator_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDRTaskExecutor);
};
} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_REBALANCE_TASK_EXECUTOR_H_
