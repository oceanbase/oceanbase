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

#ifndef OCEANBASE_TRANSACTION_OB_SQL_POLL_
#define OCEANBASE_TRANSACTION_OB_SQL_POLL_

#include "share/ob_errno.h"
#include "common/ob_queue_thread.h"
#include "lib/thread/ob_simple_thread_pool.h"
#include "lib/string/ob_sql_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "ob_trans_log.h"

namespace oceanbase {
namespace common {
class ObMySQLProxy;
class ObSqlString;
}  // namespace common

namespace transaction {

class ObSqlTask {
public:
  ObSqlTask()
  {
    reset();
  }
  ~ObSqlTask()
  {
    destroy();
  }
  int init(const ObSqlString& sql);
  int init(const ObSqlString& sql, const int64_t expect_affected_row, const int64_t retry_time);
  bool is_valid() const;
  void reset();
  void destroy();

private:
  bool is_inited_;
  ObSqlString sql_;
  int64_t max_retry_time_;
  int64_t expect_affected_row_;
  bool need_check_affected_row_;
};

class ObSqlPool : public common::ObSimpleThreadPool {
public:
  ObSqlPool();
  ~ObSqlPool()
  {
    destroy();
  }
  int init(const int64_t n_threads, const common::ObMySQLProxy& sql_proxy);
  void reset();
  void destroy();
  int start();
  int stop();
  int wait();
  int add_task(void* task);
  void handle(void* task);

private:
  static const int64_t MAX_TASK_PER_SEC = 100;
  static const int64_t TOTAL_TASK = 10000;

private:
  bool is_inited_;
  bool is_running_;
  common::ObMySQLProxy sql_proxy_;
};

}  // namespace transaction
}  // namespace oceanbase

#endif  // OCEANBASE_TRANSACTION_OB_SQL_POLL_
