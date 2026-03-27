/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_CONSTRUCT_QUEUE_
#define OB_CONSTRUCT_QUEUE_

#include "lib/task/ob_timer.h"

namespace oceanbase
{
namespace obmysql
{

class ObMySQLRequestManager;

class ObConstructQueueTask : public common::ObTimerTask
{
public:
  ObConstructQueueTask();
  virtual ~ObConstructQueueTask();

  void runTimerTask();
  int init(const ObMySQLRequestManager *request_manager);

private:
  ObMySQLRequestManager *request_manager_;
  bool is_tp_trigger_;
};

} // end of namespace obmysql
} // end of namespace oceanbase
#endif /* OB_CONSTRUCT_QUEUE_ */
