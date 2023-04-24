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

#ifndef _OCEANBASE_WORKLOAD_EXECUTOR_H_
#define _OCEANBASE_WORKLOAD_EXECUTOR_H_

#include "lib/net/ob_addr.h"
#include "lib/thread/threads.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_single_mysql_connection_pool.h"

namespace oceanbase 
{
namespace observer
{

class ObWorkloadExecutor : public lib::ThreadPool
{
public:
  ObWorkloadExecutor();
  ~ObWorkloadExecutor();
  void run1();
  int init(const int64_t thread_num, const int64_t task_rate_limit);
  common::ObMySQLProxy &get_sql_proxy() { return sql_proxy_; }
private:
  common::ObMySQLProxy sql_proxy_;
};

}
}

#endif