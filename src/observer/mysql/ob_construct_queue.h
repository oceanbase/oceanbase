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
